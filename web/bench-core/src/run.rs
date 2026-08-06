//! The run loop as a driveable unit.
//!
//! The CLI and the web console must produce identical workloads; the only way to guarantee that is
//! for both to drive this one loop. It owns pacing, batching and the sink, and exposes a handle so
//! a caller can change the rate or stop mid-run without reaching inside.

use crate::gen::{Config as GenConfig, Generator};
use crate::pace::Pacer;
use crate::sink::{Direct, Row, Sink, Ts};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Hard limit on bound parameters per statement:
/// https://www.postgresql.org/docs/current/limits.html
const MAX_BIND_PARAMS: usize = 65_535;

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub table: String,
    pub url: String,
    pub realtime: bool,
    pub batch: usize,
    pub rate: f64,
    pub gen: GenConfig,
}

#[derive(Debug, Clone, Default)]
pub struct Progress {
    pub rows_sent: u64,
    pub rows_target: u64,
    pub rate_requested: f64,
    pub open_chains: usize,
    pub done: bool,
}

impl RunConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.gen.validate()?;
        if self.rate <= 0.0 {
            anyhow::bail!("rate must be positive (got {})", self.rate);
        }
        // Direct::write_async binds batch * (4 + payload_cols) parameters; Postgres allows
        // MAX_BIND_PARAMS. Reject a combination that would exceed it up front, instead of
        // letting it fail deep in the driver.
        let per_row = 4 + self.gen.payload_cols;
        let params_per_batch = self.batch.saturating_mul(per_row);
        if params_per_batch > MAX_BIND_PARAMS {
            anyhow::bail!(
                "batch {} with {} payload columns binds {} parameters per statement \
                 ({} * (4 + {})), which exceeds postgres's limit of {} bound parameters per \
                 statement; lower batch or payload_cols",
                self.batch,
                self.gen.payload_cols,
                params_per_batch,
                self.batch,
                self.gen.payload_cols,
                MAX_BIND_PARAMS
            );
        }
        // Tie grouping (a shared timestamp across `ties` consecutive rows) is only implemented
        // for bulk mode's synthetic tick clock. Realtime timestamps are taken from the wall
        // clock at insert time, one per row, so `ties` would be silently ignored there rather
        // than actually grouping anything. Reject the combination instead.
        if self.realtime && self.gen.ties > 1 {
            anyhow::bail!(
                "ties {} has no effect in realtime mode: realtime timestamps are taken from \
                 the wall clock per row and cannot be grouped; use bulk mode for tie density",
                self.gen.ties
            );
        }
        Ok(())
    }
}

pub struct RunHandle {
    rate: Arc<AtomicU64>, // f64 bits
    cancel: CancellationToken,
    progress: watch::Receiver<Progress>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RunHandle {
    pub fn progress(&self) -> watch::Receiver<Progress> {
        self.progress.clone()
    }
    pub fn set_rate(&self, rate: f64) {
        if rate > 0.0 {
            self.rate.store(rate.to_bits(), Ordering::Relaxed);
        }
    }
    /// Requests cancellation. In realtime mode this interrupts the per-row sleep immediately
    /// rather than waiting for the current batch (up to hundreds of rows, which at a low rate
    /// can be tens of seconds) to finish — see `wait_until`.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
    pub async fn join(self) -> anyhow::Result<()> {
        self.task.await?
    }
}

/// Waits until `due`, or returns early if `cancel` fires first. `true` means the sleep ran to
/// completion (the row is due, proceed); `false` means cancellation won the race (stop now).
///
/// This is `select!`ed against the sleep rather than checked only at batch boundaries: a batch
/// can be hundreds of rows, and in realtime mode at a low rate that is tens of seconds of a
/// "Stop" click appearing to do nothing.
async fn wait_until(due: Instant, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep_until(due.into()) => true,
        _ = cancel.cancelled() => false,
    }
}

pub async fn start(cfg: RunConfig) -> anyhow::Result<RunHandle> {
    cfg.validate()?;

    let mut g = Generator::new(cfg.gen.clone())?;
    let payload = vec!["x".repeat(cfg.gen.payload_bytes); cfg.gen.payload_cols];

    let direct = Direct::connect(&cfg.url, cfg.table.clone(), cfg.gen.payload_cols).await?;
    // Realtime wants rows visible as they are produced; bulk does not, and paying a barrier per
    // INSERT there caps ingest near 9k rows/s instead of ~92k.
    let flush = if cfg.realtime { "true" } else { "false" };
    direct
        .client()
        .batch_execute(&format!("set rw_implicit_flush to {flush};"))
        .await?;
    let mut sink = Sink::Direct(direct);

    let rate = Arc::new(AtomicU64::new(cfg.rate.to_bits()));
    let cancel = CancellationToken::new();
    let (tx, rx) = watch::channel(Progress {
        rows_sent: 0,
        rows_target: cfg.gen.rows,
        rate_requested: cfg.rate,
        open_chains: 0,
        done: false,
    });

    let rate_for_task = Arc::clone(&rate);
    let cancel_for_task = cancel.clone();
    let realtime = cfg.realtime;
    let batch = cfg.batch;
    let rows = cfg.gen.rows;
    let ties = cfg.gen.ties;

    let task = tokio::spawn(async move {
        let mut buf: Vec<Row> = Vec::with_capacity(batch);
        let mut tick = 10i64;
        let mut in_group = 0u32;
        let mut current_rate = cfg.rate;
        let mut sent: u64 = 0;

        // Constructed here, after connection setup (`Direct::connect` and the
        // `rw_implicit_flush` round trip) rather than before it — otherwise that setup time
        // becomes schedule backlog and the loop bursts through it at full rate the moment
        // pacing starts, briefly exceeding the requested rate.
        let mut pacer = Pacer::new(Instant::now(), current_rate);

        for i in 0..rows {
            // Bulk mode never sleeps, so there is nothing for a per-row select! to interrupt;
            // a batch-boundary check is cheap enough there and batches are fast regardless.
            if i % batch as u64 == 0 {
                if cancel_for_task.is_cancelled() {
                    break;
                }
                let new_rate = f64::from_bits(rate_for_task.load(Ordering::Relaxed));
                if new_rate != current_rate {
                    pacer.set_rate(i, Instant::now(), new_rate);
                    current_rate = new_rate;
                }
            }
            if realtime {
                // Realtime rows can be tens of seconds apart at a low rate, so cancellation is
                // raced against the sleep itself rather than checked only between batches.
                if !wait_until(pacer.due(i), &cancel_for_task).await {
                    break;
                }
            }
            let e = g.next_event();
            let ts = if realtime {
                // Taken now, from the real clock — never from an accumulated schedule.
                Ts::Wall(time::OffsetDateTime::now_utc())
            } else {
                let t = tick;
                in_group += 1;
                if in_group >= ties {
                    in_group = 0;
                    tick += 1;
                }
                Ts::Tick(t)
            };
            buf.push(Row {
                partition: e.partition,
                ts,
                kind: e.kind,
                amount: e.amount,
                payload: payload.clone(),
            });
            sent += 1;
            if buf.len() >= batch {
                sink.write(&buf).await?;
                buf.clear();
                let _ = tx.send(Progress {
                    rows_sent: sent,
                    rows_target: rows,
                    rate_requested: current_rate,
                    open_chains: g.open_chains(),
                    done: false,
                });
            }
        }
        // Reached whether the loop ran to completion or broke out on cancellation — either way
        // whatever is buffered still needs to reach the sink. A user-initiated stop is not an
        // error, so this returns Ok(()) regardless of which path got here.
        if !buf.is_empty() {
            sink.write(&buf).await?;
        }
        sink.finish().await?;
        let _ = tx.send(Progress {
            rows_sent: sent,
            rows_target: rows,
            rate_requested: current_rate,
            open_chains: g.open_chains(),
            done: true,
        });
        Ok(())
    });

    Ok(RunHandle { rate, cancel, progress: rx, task })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_non_positive_rate_before_constructing_a_pacer() {
        // Pacer::new asserts rate > 0.0. RunConfig must reject it first so a caller gets an
        // error rather than a panic — the web server takes this value from an HTTP request.
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: true,
            batch: 500,
            rate: 0.0,
            gen: crate::gen::Config::default(),
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("rate"), "got: {err}");
    }

    #[test]
    fn rejects_a_batch_that_would_exceed_the_bound_parameter_limit() {
        // Direct::write_async binds batch * (4 + payload_cols) parameters; Postgres allows 65535.
        let gen = crate::gen::Config { payload_cols: 4, ..Default::default() };
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: false,
            batch: 20_000,
            rate: 1.0,
            gen,
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("65535"), "got: {err}");
    }

    #[test]
    fn realtime_rejects_tie_density() {
        let gen = crate::gen::Config { ties: 2, ..Default::default() };
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: true,
            batch: 500,
            rate: 1.0,
            gen,
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ties"), "got: {err}");
    }

    /// The exact bug the fix addresses: a stop must interrupt a sleep in progress, not wait it
    /// out. `due` is an hour out — if `stop()` were only observed at the next batch boundary
    /// (the old behaviour), this test would time out; instead `wait_until` must return `false`
    /// well within the 200ms budget, proving cancellation raced against and won the sleep.
    #[tokio::test]
    async fn stop_interrupts_a_realtime_sleep_immediately() {
        let cancel = CancellationToken::new();
        let due = Instant::now() + std::time::Duration::from_secs(3600);

        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move { wait_until(due, &cancel_for_task).await });

        // Give the spawned task a moment to actually start waiting before cancelling it, so the
        // test proves the sleep is interrupted rather than never having started.
        tokio::task::yield_now().await;
        cancel.cancel();

        let proceed = tokio::time::timeout(std::time::Duration::from_millis(200), handle)
            .await
            .expect("wait_until must return promptly after cancellation, not wait out the sleep")
            .expect("task must not panic");
        assert!(!proceed, "wait_until must report cancellation, not a completed sleep");
    }
}
