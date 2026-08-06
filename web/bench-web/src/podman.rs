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
    /// Asks podman directly whether the container is running — used by the status poller (see
    /// `status.rs`) so `/api/status` reflects reality instead of "whatever the last action taken
    /// through this API implied." A missing container (never started, or removed) is `Ok(false)`,
    /// not an error: that is the ordinary pre-`up` state, not a failure to report.
    fn is_running(&self) -> BoxFuture<'_, anyhow::Result<bool>>;
}

/// The real driver: shells out to `podman`.
#[derive(Debug, Clone)]
pub struct PodmanDriver {
    name: String,
    image: String,
    /// Cores the container may run on (`--cpuset-cpus`), when pinning is enabled. `None` means
    /// unrestricted — the default, matching every number recorded in the README so far.
    cpuset: Option<String>,
}

impl PodmanDriver {
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self { name: name.into(), image: image.into(), cpuset: None }
    }

    /// Restrict the container to `cpuset` (a podman core list such as `"0-61"`).
    pub fn with_cpuset(mut self, cpuset: Option<String>) -> Self {
        self.cpuset = cpuset;
        self
    }
}

/// Longest chunk of captured output to fold into an error message. podman failures are
/// occasionally a full stack trace or a registry error dump; that must not flood a JSON response
/// (or, worse, a demo screen) with kilobytes of noise.
const MAX_OUTPUT_CHARS: usize = 2000;

fn truncated(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    if s.chars().count() > MAX_OUTPUT_CHARS {
        let head: String = s.chars().take(MAX_OUTPUT_CHARS).collect();
        format!("{head}... (truncated)")
    } else {
        s.to_string()
    }
}

/// Captures stdout/stderr rather than inheriting them (`Command::output`, not `Command::status`)
/// specifically so a failure carries podman's own explanation back to the HTTP caller instead of
/// a bare exit status. podman is inconsistent about which stream it writes an error to, so stderr
/// is preferred but stdout is used as a fallback when stderr is empty.
async fn run(args: &[String]) -> anyhow::Result<()> {
    let output = Command::new("podman")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to spawn podman: {e}"))?;
    if !output.status.success() {
        let stderr = truncated(&output.stderr);
        let detail = if stderr.is_empty() { truncated(&output.stdout) } else { stderr };
        if detail.is_empty() {
            anyhow::bail!("podman {} failed: {}", args.join(" "), output.status);
        }
        anyhow::bail!("podman {} failed: {}: {detail}", args.join(" "), output.status);
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
            ];
            let mut args = args;
            if let Some(cpuset) = &self.cpuset {
                args.push("--cpuset-cpus".into());
                args.push(cpuset.clone());
            }
            args.push(self.image.clone());
            args.push("single_node".into());
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

    fn is_running(&self) -> BoxFuture<'_, anyhow::Result<bool>> {
        Box::pin(async move {
            let output = Command::new("podman")
                .args(["inspect", "-f", "{{.State.Running}}", &self.name])
                .stdin(Stdio::null())
                .output()
                .await
                .map_err(|e| anyhow::anyhow!("failed to spawn podman: {e}"))?;
            if !output.status.success() {
                // No such container (the common case before `cluster up` is ever called, or after
                // `cluster down`/`clean`) is not an error here — it just means "not running".
                return Ok(false);
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.trim() == "true")
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
    fn is_running(&self) -> BoxFuture<'_, anyhow::Result<bool>> {
        Box::pin(async { Ok(false) })
    }
}
