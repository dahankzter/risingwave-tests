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

    /// Where `POST /api/pipeline/rebuild` finds the realtime pipeline's setup SQL. Defaults to
    /// the repo layout when run as `cd web && cargo run -p bench-web`.
    #[arg(long, default_value = "../scenarios/perf/setup_realtime.sql")]
    pipeline_sql: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if !cli.bind.ip().is_loopback() {
        eprintln!(
            "bench-web: WARNING binding to {} (not loopback) — this service shells out to podman \
             and can destroy the rw-tests-data volume; make sure that's really what you want",
            cli.bind
        );
    }

    bench_web::serve(ServeConfig {
        bind: cli.bind,
        db_url: cli.url,
        container_name: cli.container_name,
        image: cli.image,
        pipeline_sql: cli.pipeline_sql,
    })
    .await
}
