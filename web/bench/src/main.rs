//! Workload driver for the MATCH_RECOGNIZE bench. Replaces datagen/gen.py.
//!
//! Connects directly by default; `--emit-sql` prints the stream instead, for inspection.

use anyhow::Result;
use bench_core::gen::{Config, Generator};
use bench_core::pipeline::{seal, SealConfig};
use bench_core::run::{self, RunConfig};
use bench_core::sink::{Direct, EmitSql, Row, Sink, Ts};
use clap::{Parser, Subcommand, ValueEnum};

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
    ///
    /// Bulk-only: this assumes an integer `ts` column and reads `max(ts)` back as an int4, so
    /// running it against a realtime table (wall-clock `ts`) fails with an opaque type error.
    /// Realtime feeds get their watermark from the wall clock and do not need sealing.
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

            let gen_cfg = Config {
                rows, partitions, hot_count, hot_share, abandon_prob, ties, seed,
                payload_cols, payload_bytes, ..Config::default()
            };

            // The `--emit-sql` path never connects, so it does not go through `run::start` —
            // but it still shares the same validation (rate, batch/param limit, realtime ties)
            // by building a `RunConfig` purely to validate it.
            let run_cfg = RunConfig {
                table: table.clone(),
                url: cli.url.clone(),
                realtime,
                batch,
                rate,
                gen: gen_cfg.clone(),
            };
            run_cfg.validate()?;

            if emit_sql {
                let mut g = Generator::new(gen_cfg)?;
                let payload = vec!["x".repeat(payload_bytes); payload_cols];

                let flush = if realtime { "true" } else { "false" };
                println!("set rw_implicit_flush to {flush};");
                // `Sink::Emit` needs `Box<dyn Write + Send>`: `StdoutLock` holds a
                // `ReentrantLockGuard` and is not `Send`, so this uses the unlocked `Stdout`
                // handle instead (each write re-locks internally, functionally the same
                // output, just without holding the lock across awaits).
                let writer: Box<dyn std::io::Write + Send> = Box::new(std::io::stdout());
                let mut sink =
                    Sink::Emit(EmitSql::new(writer, table.clone(), payload_cols));

                let mut buf: Vec<Row> = Vec::with_capacity(batch);
                let mut tick = 10i64;
                let mut in_group = 0u32;

                for _ in 0..rows {
                    let e = g.next_event();
                    let ts = if realtime {
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
                        sink.write(&buf).await?;
                        buf.clear();
                    }
                }
                if !buf.is_empty() {
                    sink.write(&buf).await?;
                }
                sink.finish().await?;
                eprintln!(
                    "-- emitted {rows} rows over {partitions} partitions (hot: {hot_count} @ {hot_share}), \
                     {} chains left open",
                    g.open_chains()
                );
                return Ok(());
            }

            let handle = run::start(run_cfg).await?;
            let mut progress = handle.progress();
            handle.join().await?;
            let final_progress = progress.borrow_and_update().clone();
            eprintln!(
                "-- emitted {rows} rows over {partitions} partitions (hot: {hot_count} @ {hot_share}), \
                 {} chains left open",
                final_progress.open_chains
            );
            Ok(())
        }
    }
}
