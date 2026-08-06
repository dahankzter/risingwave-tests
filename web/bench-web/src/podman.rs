//! Cluster lifecycle: up/down/clean, shelled out to `podman`.
//!
//! `up` mirrors the Makefile's `up` target exactly (see `/Makefile`'s `up:` recipe), so a
//! container started from the web console behaves identically to one started with `make up` —
//! same name, same platform pin, same published ports, same data volume. It additionally
//! publishes `-p 1222:1222` ahead of Task 7's metrics scrape (the Makefile and `compose.yaml`
//! gain that port in that task; this driver gets it now so the two stay in lockstep once they
//! do).
//!
//! `Cluster` is a trait, not a bare struct, specifically so the control API can be tested without
//! a `podman` binary on PATH: `router_for_test()` (in `lib.rs`) wires up `NullCluster`, and the
//! three rejection-path tests never call any of these methods at all (they fail validation before
//! reaching the driver). Real async fns can't be trait methods on a `dyn` object without either
//! the `async-trait` crate or hand-rolled boxed futures; this uses the latter to avoid adding a
//! dependency for three methods.

use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use tokio::process::Command;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The Makefile's pinned default — see `RW_IMAGE ?=` at the top of `/Makefile`. Kept in sync by
/// hand; there is no single source of truth shared between `make` and this binary.
pub const DEFAULT_IMAGE: &str =
    "ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--bee0fbd--feat-match-recognize-v2";
pub const DEFAULT_NAME: &str = "rw-tests";
pub const DATA_VOLUME: &str = "rw-tests-data";

pub trait Cluster: Send + Sync {
    fn up(&self) -> BoxFuture<'_, anyhow::Result<()>>;
    fn down(&self) -> BoxFuture<'_, anyhow::Result<()>>;
    /// Stops the container and removes the data volume. Unlike the Makefile's `clean` (which
    /// tolerates a missing container/volume via `|| true`), the confirmation gate lives one layer
    /// up in `api.rs` — this method performs the destructive action unconditionally once called,
    /// on the assumption the caller already checked. Keeping the token check in the HTTP layer
    /// rather than here means this trait never needs to know about request bodies.
    fn clean(&self) -> BoxFuture<'_, anyhow::Result<()>>;
}

/// The real driver: shells out to `podman`.
#[derive(Debug, Clone)]
pub struct PodmanDriver {
    name: String,
    image: String,
}

impl PodmanDriver {
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self { name: name.into(), image: image.into() }
    }
}

async fn run(args: &[String]) -> anyhow::Result<()> {
    let status = Command::new("podman")
        .args(args)
        .stdin(Stdio::null())
        .status()
        .await
        .map_err(|e| anyhow::anyhow!("failed to spawn podman: {e}"))?;
    if !status.success() {
        anyhow::bail!("podman {} failed: {status}", args.join(" "));
    }
    Ok(())
}

impl Cluster for PodmanDriver {
    fn up(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let args: Vec<String> = vec![
                "run".into(),
                "-d".into(),
                "--replace".into(),
                "--name".into(),
                self.name.clone(),
                "--platform".into(),
                "linux/amd64".into(),
                "-p".into(),
                "4566:4566".into(),
                "-p".into(),
                "5690:5690".into(),
                // Task 7's metrics scrape needs this; publishing it now keeps the driver and the
                // Makefile/compose.yaml (which gain it in that task) in step from the start.
                "-p".into(),
                "1222:1222".into(),
                "-v".into(),
                format!("{DATA_VOLUME}:/root/.risingwave"),
                self.image.clone(),
                "single_node".into(),
            ];
            run(&args).await
        })
    }

    fn down(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            // Mirrors the Makefile's `podman rm -f $(NAME) 2>/dev/null || true`: a container
            // that is already gone is not an error.
            let _ = run(&["rm".into(), "-f".into(), self.name.clone()]).await;
            Ok(())
        })
    }

    fn clean(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.down().await?;
            let _ = run(&["volume".into(), "rm".into(), DATA_VOLUME.to_string()]).await;
            Ok(())
        })
    }
}

/// A no-op driver for `router_for_test()`. Every method succeeds instantly and touches nothing —
/// the point is that the three rejection-path tests in `tests/api.rs` prove they never even get
/// this far, not that this driver does something clever.
pub struct NullCluster;

impl Cluster for NullCluster {
    fn up(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn down(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn clean(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
