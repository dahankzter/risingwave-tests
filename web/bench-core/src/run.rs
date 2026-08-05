//! The run loop as a driveable unit.
//!
//! The CLI and the web console must produce identical workloads; the only way to guarantee that is
//! for both to drive this one loop. It owns pacing, batching and the sink, and exposes a handle so
//! a caller can change the rate or stop mid-run without reaching inside.

use crate::gen::{Config as GenConfig, Generator};
use crate::pace::Pacer;
use crate::sink::{Direct, Row, Ts};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

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
    stop: Arc<AtomicBool>,
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
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
    pub async fn join(self) -> anyhow::Result<()> {
        self.task.await?
    }
}

pub async fn start(cfg: RunConfig) -> anyhow::Result<RunHandle> {
    cfg.validate()?;

    let mut g = Generator::new(cfg.gen.clone())?;
    let payload = vec!["x".repeat(cfg.gen.payload_bytes); cfg.gen.payload_cols];

    let mut direct = Direct::connect(&cfg.url, cfg.table.clone(), cfg.gen.payload_cols).await?;
    // Realtime wants rows visible as they are produced; bulk does not, and paying a barrier per
    // INSERT there caps ingest near 9k rows/s instead of ~92k.
    let flush = if cfg.realtime { "true" } else { "false" };
    direct
        .client()
        .batch_execute(&format!("set rw_implicit_flush to {flush};"))
        .await?;

    let rate = Arc::new(AtomicU64::new(cfg.rate.to_bits()));
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = watch::channel(Progress {
        rows_sent: 0,
        rows_target: cfg.gen.rows,
        rate_requested: cfg.rate,
        open_chains: 0,
        done: false,
    });

    let rate_for_task = Arc::clone(&rate);
    let stop_for_task = Arc::clone(&stop);
    let realtime = cfg.realtime;
    let batch = cfg.batch;
    let rows = cfg.gen.rows;
    let ties = cfg.gen.ties;

    let task = tokio::spawn(async move {
        let mut buf: Vec<Row> = Vec::with_capacity(batch);
        let mut tick = 10i64;
        let mut in_group = 0u32;
        let mut current_rate = cfg.rate;

        // Constructed here, after connection setup (`Direct::connect` and the
        // `rw_implicit_flush` round trip) rather than before it — otherwise that setup time
        // becomes schedule backlog and the loop bursts through it at full rate the moment
        // pacing starts, briefly exceeding the requested rate.
        let mut pacer = Pacer::new(Instant::now(), current_rate);

        for i in 0..rows {
            if i % batch as u64 == 0 {
                if stop_for_task.load(Ordering::Relaxed) {
                    break;
                }
                let new_rate = f64::from_bits(rate_for_task.load(Ordering::Relaxed));
                if new_rate != current_rate {
                    pacer.set_rate(i, Instant::now(), new_rate);
                    current_rate = new_rate;
                }
            }
            if realtime {
                tokio::time::sleep_until(pacer.due(i).into()).await;
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
            if buf.len() >= batch {
                direct.write_async(&buf).await?;
                buf.clear();
                let _ = tx.send(Progress {
                    rows_sent: i + 1,
                    rows_target: rows,
                    rate_requested: current_rate,
                    open_chains: g.open_chains(),
                    done: false,
                });
            }
        }
        if !buf.is_empty() {
            direct.write_async(&buf).await?;
        }
        let _ = tx.send(Progress {
            rows_sent: rows,
            rows_target: rows,
            rate_requested: current_rate,
            open_chains: g.open_chains(),
            done: true,
        });
        Ok(())
    });

    Ok(RunHandle { rate, stop, progress: rx, task })
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
        let mut gen = crate::gen::Config::default();
        gen.payload_cols = 4;
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
        let mut gen = crate::gen::Config::default();
        gen.ties = 2;
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
}
