//! Workload driver for the MATCH_RECOGNIZE bench. Replaces datagen/gen.py.
//!
//! Connects directly by default; `--emit-sql` prints the stream instead, for inspection.

use anyhow::Result;
use bench_core::gen::{Config, Generator};
use bench_core::pace::Pacer;
use bench_core::pipeline::{seal, SealConfig};
use bench_core::sink::{Direct, EmitSql, Row, Sink, Ts};
use clap::{Parser, Subcommand, ValueEnum};
use std::time::Instant;

/// Hard limit on bound parameters per statement:
/// https://www.postgresql.org/docs/current/limits.html
const MAX_BIND_PARAMS: usize = 65_535;

#[derive(Parser)]
#[command(about = "MATCH_RECOGNIZE bench workload driver")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://root@127.0.0.1:4566/dev")]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Mode {
    Bulk,
    Realtime,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate and insert a workload.
    Load {
        #[arg(long)]
        table: String,
        #[arg(long, value_enum, default_value = "bulk")]
        mode: Mode,
        #[arg(long, default_value_t = 100_000)]
        rows: u64,
        #[arg(long, default_value_t = 1_000)]
        partitions: i32,
        #[arg(long, default_value_t = 500)]
        batch: usize,
        #[arg(long, default_value_t = 0)]
        hot_count: i32,
        #[arg(long, default_value_t = 0.5)]
        hot_share: f64,
        #[arg(long, default_value_t = 0.2)]
        abandon_prob: f64,
        #[arg(long, default_value_t = 1)]
        ties: u32,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value_t = 2000.0)]
        rate: f64,
        #[arg(long, default_value_t = 0)]
        payload_cols: usize,
        #[arg(long, default_value_t = 32)]
        payload_bytes: usize,
        /// Print SQL instead of connecting.
        #[arg(long)]
        emit_sql: bool,
    },
    /// Settle, advance the watermark, settle again.
    Seal {
        #[arg(long, default_value = "t_perf")]
        table: String,
        #[arg(long, default_value = "mv_perf")]
        mv: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Seal { table, mv } => {
            let d = Direct::connect(&cli.url, table.clone(), 0).await?;
            let cfg = SealConfig { table, mv, ..Default::default() };
            let out = seal(d.client(), &cfg).await?;
            eprintln!(
                "seal: feed settled at {} matches, seal settled at {}",
                out.settled_before, out.settled_after
            );
            Ok(())
        }
        Cmd::Load {
            table, mode, rows, partitions, batch, hot_count, hot_share, abandon_prob,
            ties, seed, rate, payload_cols, payload_bytes, emit_sql,
        } => {
            let realtime = mode == Mode::Realtime;

            // Postgres allows at most MAX_BIND_PARAMS bound parameters per statement, and
            // Direct::write_async binds batch * (4 + payload_cols) of them. Reject a combination
            // that would exceed it up front, instead of letting it fail deep in the driver.
            let per_row = 4 + payload_cols;
            let params_per_batch = batch.saturating_mul(per_row);
            if params_per_batch > MAX_BIND_PARAMS {
                anyhow::bail!(
                    "--batch {batch} with --payload-cols {payload_cols} binds {params_per_batch} \
                     parameters per statement ({batch} * (4 + {payload_cols})), which exceeds \
                     postgres's limit of {MAX_BIND_PARAMS} bound parameters per statement; lower \
                     --batch or --payload-cols"
                );
            }

            // Pacer::new panics if rate <= 0; validate first so a bad --rate is a clean error
            // rather than a stack trace. Only realtime mode paces, but validate unconditionally
            // so the error is consistent regardless of mode.
            if rate <= 0.0 {
                anyhow::bail!("--rate must be positive, got {rate}");
            }

            let cfg = Config {
                rows, partitions, hot_count, hot_share, abandon_prob, ties, seed,
                payload_cols, payload_bytes, ..Config::default()
            };
            let mut g = Generator::new(cfg.clone())?;
            let payload = vec!["x".repeat(payload_bytes); payload_cols];

            let pacer = Pacer::new(Instant::now(), rate);

            let mut direct = if emit_sql {
                None
            } else {
                Some(Direct::connect(&cli.url, table.clone(), payload_cols).await?)
            };
            if let Some(d) = &direct {
                // Realtime wants rows visible as they are produced; bulk does not, and paying a
                // barrier per INSERT there caps ingest near 9k rows/s instead of ~92k.
                let flush = if realtime { "true" } else { "false" };
                d.client()
                    .batch_execute(&format!("set rw_implicit_flush to {flush};"))
                    .await?;
            }
            // Only take the stdout lock when actually emitting; holding it for the whole run in
            // direct mode would block the progress messages below.
            let stdout = std::io::stdout();
            let mut emit = if emit_sql {
                let flush = if realtime { "true" } else { "false" };
                println!("set rw_implicit_flush to {flush};");
                Some(EmitSql::new(stdout.lock(), table.clone(), payload_cols))
            } else {
                None
            };

            let mut buf: Vec<Row> = Vec::with_capacity(batch);
            let mut tick = 10i64;
            let mut in_group = 0u32;

            for i in 0..rows {
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
                    match (&mut direct, &mut emit) {
                        (Some(d), _) => d.write_async(&buf).await?,
                        (None, Some(e)) => e.write(&buf)?,
                        (None, None) => unreachable!("one sink is always configured"),
                    }
                    buf.clear();
                }
            }
            if !buf.is_empty() {
                match (&mut direct, &mut emit) {
                    (Some(d), _) => d.write_async(&buf).await?,
                    (None, Some(e)) => e.write(&buf)?,
                    (None, None) => unreachable!("one sink is always configured"),
                }
            }
            if let Some(e) = &mut emit {
                e.finish()?;
            }
            eprintln!(
                "-- emitted {rows} rows over {partitions} partitions (hot: {hot_count} @ {hot_share}), \
                 {} chains left open",
                g.open_chains()
            );
            Ok(())
        }
    }
}
