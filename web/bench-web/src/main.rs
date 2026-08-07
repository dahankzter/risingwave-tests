//! `bench-web`: the demo console. Argument parsing only — everything else is in `lib.rs` so it
//! can be exercised by `tests/api.rs` without a database or `podman`.

use bench_web::podman::{DEFAULT_IMAGE, DEFAULT_NAME};
use bench_web::ServeConfig;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "MATCH_RECOGNIZE bench web console")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://root@127.0.0.1:4566/dev")]
    url: String,

    /// Bind address. Defaults to loopback: this service shells out to `podman` and can destroy a
    /// data volume, so it must not be casually exposed. A non-loopback bind prints a warning.
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,

    #[arg(long, env = "RW_IMAGE", default_value = DEFAULT_IMAGE)]
    image: String,

    #[arg(long, default_value = DEFAULT_NAME)]
    container_name: String,

    /// Override for where `POST /api/pipeline/rebuild` finds the realtime pipeline's setup SQL.
    /// Unset by default: the server uses the copy embedded into the binary at compile time, so it
    /// works identically regardless of the process's current directory. Pass this to point at a
    /// file on disk instead — e.g. to iterate on the SQL without rebuilding the binary.
    #[arg(long)]
    pipeline_sql: Option<PathBuf>,

    /// Partition the cores: the cluster gets all but the last two, this process gets those two,
    /// and `streaming_parallelism` is set to match the cluster's cpuset. Off by default — every
    /// number in the README was measured unpinned, so enabling it changes what a run means.
    #[arg(long)]
    pin: bool,

    /// Override the cluster's cpuset (implies `--pin`), e.g. `0-31`.
    #[arg(long)]
    cores_cluster: Option<String>,

    /// Override the bench's cpuset (implies `--pin`), e.g. `62-63`.
    #[arg(long)]
    cores_bench: Option<String>,
}

/// Not `#[tokio::main]`: when pinning is active the runtime needs an `on_thread_start` hook so
/// every worker and blocking-pool thread pins itself to the bench cores as it starts (see
/// `build_runtime` and `pin.rs`'s module doc — `sched_setaffinity` is per-thread, not
/// per-process, and `#[tokio::main]`'s generated runtime offers no way to install the hook).
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if !cli.bind.ip().is_loopback() {
        eprintln!(
            "bench-web: WARNING binding to {} (not loopback) — this service shells out to podman \
             and can destroy the rw-tests-data volume; make sure that's really what you want",
            cli.bind
        );
    }

    let pin_layout = resolve_pinning(&cli)?;
    if let Some(l) = &pin_layout {
        println!("bench-web: {}", l.why);
    }

    let rt = build_runtime(pin_layout.as_ref())?;
    // `on_thread_start` only fires for threads the runtime spawns — it does not retroactively pin
    // the thread that built the runtime and is about to call `block_on` (which, in a multi-thread
    // runtime, also participates in polling). Pin it here so every thread ends up pinned, not just
    // the workers.
    if let Some(layout) = &pin_layout {
        bench_web::pin::apply_to_self(layout)?;
    }
    rt.block_on(bench_web::serve(ServeConfig {
        bind: cli.bind,
        db_url: cli.url,
        container_name: cli.container_name,
        image: cli.image,
        pin_layout,
        pipeline_sql: cli.pipeline_sql,
    }))
}

/// Builds the tokio runtime `main` drives. With no pin layout (the default) this matches what
/// `#[tokio::main]` used to generate: a multi-thread runtime, default worker count, all drivers
/// enabled. With a pin layout active, the worker pool is sized to the bench cpuset — not left at
/// the default (one worker per host core), which on a 64-core box confined to 2 cores would be 64
/// workers oversubscribing 2 — and every worker/blocking-pool thread pins itself to those cores
/// via `on_thread_start` as it starts, since `sched_setaffinity` only affects the calling thread.
fn build_runtime(pin_layout: Option<&bench_web::pin::Layout>) -> anyhow::Result<tokio::runtime::Runtime> {
    use bench_web::pin;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();

    if let Some(layout) = pin_layout {
        if let Some(spec) = &layout.bench {
            let cores = pin::parse_range(spec)?;
            let worker_count = cores.len().max(1);
            builder.worker_threads(worker_count);
            let hook_cores = cores.clone();
            builder.on_thread_start(move || {
                if let Err(e) = pin::pin_current_thread(&hook_cores) {
                    // A thread that fails to pin still runs — just unpinned — so this cannot be a
                    // panic (that would take the whole runtime down over one thread). It is loud
                    // on stderr because a silently-unpinned thread is exactly the bug this file
                    // exists to prevent.
                    eprintln!("bench-web: WARNING failed to pin runtime thread: {e}");
                }
            });
        }
    }

    Ok(builder.build()?)
}

/// `--cores-*` overrides imply `--pin`; the automatic layout comes from the usable core count.
/// An override is validated here rather than at first use, so a typo fails at startup instead of
/// when someone presses "cluster up" in front of an audience.
fn resolve_pinning(cli: &Cli) -> anyhow::Result<Option<bench_web::pin::Layout>> {
    use bench_web::pin;

    let overridden = cli.cores_cluster.is_some() || cli.cores_bench.is_some();
    if !cli.pin && !overridden {
        return Ok(None);
    }
    let mut layout = pin::plan(pin::usable_cores());
    if let Some(spec) = &cli.cores_cluster {
        let cores = pin::parse_range(spec)?;
        layout.parallelism = Some(cores.len() as u32);
        layout.cluster = Some(spec.clone());
        layout.why = format!("cluster cpuset overridden to {spec} ({} cores)", cores.len());
    }
    if let Some(spec) = &cli.cores_bench {
        pin::parse_range(spec)?;
        layout.bench = Some(spec.clone());
        layout.why = format!("{}; bench cpuset overridden to {spec}", layout.why);
    }
    Ok(Some(layout))
}
