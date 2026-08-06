//! `bench-web`: the demo console. This is a skeleton — the server, control API, and static
//! asset embedding land in later tasks. For now `main` starts the alert stream reader and prints
//! whatever it publishes, which is enough to verify the reader by hand (see the task report).

mod event;
mod stream;

use clap::Parser;
use tokio::sync::broadcast;

#[derive(Parser)]
#[command(about = "MATCH_RECOGNIZE bench web console (skeleton)")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://root@127.0.0.1:4566/dev")]
    url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let (tx, mut rx) = broadcast::channel(1024);
    let _reader = stream::spawn_reader(cli.url, tx);

    loop {
        match rx.recv().await {
            Ok(ev) => println!("{}", serde_json::to_string(&ev)?),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("bench-web: dropped {n} events (receiver lagged)");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}
