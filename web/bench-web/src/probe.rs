//! `POST /api/probe/start`: the client-side latency measurement. For each round it drives one
//! chain of its own through the pipeline — deposit, bet, then the completing withdraw — and times
//! how long the match takes to become visible in `mv_rt`, publishing each round as `Event::Probe`
//! and a p50/p95 summary as `Event::Log`.
//!
//! Why this exists next to the server-side numbers the alert stream already produces: the stream's
//! percentiles cover every match the current load happens to produce, whereas this drives a known
//! chain and measures it end to end, including the query that a consumer would actually run. Two
//! measurements that agree are worth more than either alone.
//!
//! This talks to the database directly rather than shelling out to `latency/probe.sh`. The script
//! remains the CLI path (`make latency`), but running it from the server meant depending on a
//! `psql` binary on PATH — which on a Mac with keg-only libpq is not there, and the failure
//! surfaced as `exited with exit status: 127`, a message that says nothing about what was missing.
//! The console already holds a Postgres client; using it removes the dependency and the whole
//! class of environment breakage with it.
//!
//! The one subtlety, unchanged from the script: sentinel rows. The probe can advance the watermark
//! itself so it works with no background traffic — but those rows carry `now()`, which outruns a
//! paced load's event time and releases the load's own matches early (measured on the rig: a
//! feed's server-side p50 fell from 7.236s to 3.396s with a sentinel-emitting probe alongside).
//! So sentinels are only emitted when no load is running.

use crate::event::Event;
use crate::state::AppState;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `true` (a load is running) forces the sentinel off; `false` leaves it on so the probe still
/// works with no background traffic. Pulled out as a pure function so the decision is testable
/// without spawning anything.
pub fn sentinel_for(load_running: bool) -> &'static str {
    if load_running { "off" } else { "on" }
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
    const TABLE: &str = "t_rt";
    const MV: &str = "mv_rt";
    /// Poll interval while waiting for a match to appear. Fine-grained enough not to inflate a
    /// sub-second measurement, coarse enough not to hammer the frontend.
    const POLL: Duration = Duration::from_millis(20);
    /// Give up on a round after this. A probe that hangs forever would look identical to a probe
    /// that is merely slow, and the panel would never say which.
    const ROUND_TIMEOUT: Duration = Duration::from_secs(60);

    let (client, connection) =
        tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Probe partitions must not collide with the generator's (1..=partitions) or with a previous
    // probe's leftovers: a reused key lets an old row satisfy the poll instantly and fake a fast
    // round. Derived from the wall clock, one block of ids per run.
    let base: i32 = 1_000_000
        + ((std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            % 100_000) as i32)
            * 10;

    let mut samples: Vec<u64> = Vec::with_capacity(rounds as usize);
    for round in 0..rounds {
        let pid = base + round as i32;

        // The chain's opening rows. Their own timing does not matter — the measurement starts at
        // the completing row, which is what a detector is waiting for.
        client
            .execute(
                &format!(
                    "insert into {TABLE} (id, ts, kind, amount) values \
                     ($1, now(), 'deposit', 100), ($1, now(), 'bet', 10)"
                ),
                &[&pid],
            )
            .await?;

        let started = Instant::now();
        client
            .execute(
                &format!(
                    "insert into {TABLE} (id, ts, kind, amount) values ($1, now(), 'withdraw', 90)"
                ),
                &[&pid],
            )
            .await?;

        let mut polls: u32 = 0;
        loop {
            let seen: i64 = client
                .query_one(&format!("select count(*) from {MV} where partition_0 = $1"), &[&pid])
                .await?
                .get(0);
            if seen > 0 {
                break;
            }
            if started.elapsed() > ROUND_TIMEOUT {
                anyhow::bail!(
                    "round {round} timed out after {}s with no match — is the pipeline emitting?",
                    ROUND_TIMEOUT.as_secs()
                );
            }
            // Keep the watermark moving when nothing else is: without new event time the sort
            // never releases the withdraw row and the match cannot finalise. Sentinel rows are
            // suppressed while a load is running (see the module doc).
            polls += 1;
            if sentinel == "on" && polls.is_multiple_of(10) {
                client
                    .execute(
                        &format!(
                            "insert into {TABLE} (id, ts, kind, amount) \
                             values (0, now(), 'noop', 0)"
                        ),
                        &[],
                    )
                    .await?;
            }
            tokio::time::sleep(POLL).await;
        }

        let ms = started.elapsed().as_millis() as u64;
        samples.push(ms);
        state.publish(Event::Probe { round, latency_ms: ms });
    }

    samples.sort_unstable();
    let pick = |q: f64| samples[((q * samples.len() as f64) as usize).min(samples.len() - 1)];
    state.publish(Event::Log {
        level: "info".to_string(),
        text: format!(
            "probe: {} round(s) — p50 {}ms p95 {}ms min {}ms max {}ms (sentinel={sentinel})",
            samples.len(),
            pick(0.5),
            pick(0.95),
            samples[0],
            samples[samples.len() - 1]
        ),
    });
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



}
