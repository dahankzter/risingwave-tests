//! `POST /api/probe/start`: runs `latency/probe.sh` (embedded into the binary — see
//! `embedded.rs` — so this works regardless of the server's CWD, same reasoning as the setup
//! SQL) as a child process, streaming each round's result as `Event::Probe` and the final
//! summary line as `Event::Log`.
//!
//! The one thing this handler must get right that a naive "just run the script" would not:
//! `SENTINEL` in the child's environment. `probe.sh` defaults to `SENTINEL=on`, which lets it
//! advance the watermark itself so it works standalone with no background traffic. But those
//! sentinel rows carry `now()`, which outruns a paced load's event time and releases the load's
//! own matches early — measured on the rig, running the probe alongside a feed dropped that
//! feed's server-side p50 from 7.236s to 3.396s (see `setup_realtime.sql`'s header and
//! `probe.sh`'s own comment). So: `SENTINEL=off` whenever a load is running, `on` otherwise.

use crate::embedded;
use crate::event::Event;
use crate::state::AppState;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// `true` (a load is running) forces the sentinel off; `false` leaves it on so the probe still
/// works with no background traffic. Pulled out as a pure function so the decision is testable
/// without spawning anything.
pub fn sentinel_for(load_running: bool) -> &'static str {
    if load_running { "off" } else { "on" }
}

/// Parses one `probe.sh` stdout line of the form `round 3: 6318 ms` into `(round, latency_ms)`.
/// The script's `TIMEOUT` line goes to stderr, not stdout (see `probe.sh`), so it — and anything
/// else that isn't a round line, in particular the final `rounds=... p50=...` summary — falls
/// through to `None` and is forwarded as `Event::Log` by the caller instead.
pub fn parse_round_line(line: &str) -> Option<(u32, u64)> {
    let rest = line.strip_prefix("round ")?;
    let (round_s, rest) = rest.split_once(": ")?;
    let ms_s = rest.strip_suffix(" ms")?;
    let round: u32 = round_s.trim().parse().ok()?;
    let latency_ms: u64 = ms_s.trim().parse().ok()?;
    Some((round, latency_ms))
}

/// Runs the probe to completion and always clears `state.probe_running` on the way out, however
/// it ends — a stuck `true` here would permanently 409 every future probe request. Spawned as a
/// detached task by `api::probe_start`, which has already returned 200 by the time this runs.
pub async fn run_probe(state: Arc<AppState>, rounds: u32, sentinel: &'static str) {
    state.publish(Event::Log {
        level: "info".to_string(),
        text: format!("probe: starting {rounds} round(s) (sentinel={sentinel})"),
    });

    if let Err(e) = execute(&state, rounds, sentinel).await {
        state.publish(Event::Log { level: "warn".to_string(), text: format!("probe: {e}") });
    }

    *state.probe_running.lock().await = false;
}

async fn execute(state: &Arc<AppState>, rounds: u32, sentinel: &'static str) -> anyhow::Result<()> {
    let script = embedded::probe_script();

    let mut child = Command::new("bash")
        .arg("-s")
        .env("ROUNDS", rounds.to_string())
        .env("SENTINEL", sentinel)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().expect("stdin was requested piped");
    let writer = tokio::spawn(async move {
        // Errors here (a broken pipe if the child already exited) are surfaced by `child.wait()`
        // below, not here — this task's only job is to hand the script over and then close the
        // pipe (by dropping `stdin`) so `bash -s` sees EOF and starts running.
        let _ = stdin.write_all(&script).await;
    });

    let stdout = child.stdout.take().expect("stdout was requested piped");
    let stderr = child.stderr.take().expect("stderr was requested piped");

    let state_out = state.clone();
    let out_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match parse_round_line(&line) {
                Some((round, latency_ms)) => state_out.publish(Event::Probe { round, latency_ms }),
                // Covers the final `rounds=... p50=...ms ...` summary line in particular.
                None => state_out
                    .publish(Event::Log { level: "info".to_string(), text: format!("probe: {line}") }),
            }
        }
    });

    let state_err = state.clone();
    let err_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            state_err.publish(Event::Log { level: "warn".to_string(), text: format!("probe: {line}") });
        }
    });

    let _ = writer.await;
    let _ = out_task.await;
    let _ = err_task.await;
    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("probe.sh exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentinel_is_forced_off_while_a_load_is_running() {
        assert_eq!(sentinel_for(true), "off");
    }

    #[test]
    fn sentinel_stays_on_standalone_so_the_probe_still_works_with_no_traffic() {
        assert_eq!(sentinel_for(false), "on");
    }

    #[test]
    fn parses_a_round_line() {
        assert_eq!(parse_round_line("round 3: 6318 ms"), Some((3, 6318)));
        assert_eq!(parse_round_line("round 0: 42 ms"), Some((0, 42)));
    }

    #[test]
    fn does_not_mistake_the_timeout_line_or_the_summary_line_for_a_round() {
        assert_eq!(
            parse_round_line(
                "round 2: TIMEOUT after 30s+ — pipeline not emitting (check the MV and watermark)"
            ),
            None,
        );
        assert_eq!(parse_round_line("rounds=8 p50=6318ms p95=6318ms min=6318ms max=6318ms"), None);
    }

    #[test]
    fn rejects_garbage_that_merely_starts_with_round() {
        assert_eq!(parse_round_line("round: not really a round line"), None);
        assert_eq!(parse_round_line("roundup: 5 ms"), None);
    }
}
