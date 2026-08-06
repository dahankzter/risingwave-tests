//! `AppState`: the single global run, the broadcast sender the WebSocket layer (Task 5) fans out
//! from, and the cluster/pipeline/load status line.
//!
//! There is exactly one of everything here — one load, one cluster — per the plan's global
//! constraint. `run` guards the one `RunHandle` that may exist at a time; `POST /api/load/start`
//! checks it and returns 409 rather than queuing (see `api.rs`).

use crate::event::Event;
use crate::podman::Cluster;
use bench_core::run::RunHandle;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Mirrors `Event::Status`'s fields, so publishing the current status is just wrapping this in
/// that variant.
#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub cluster: String,
    pub pipeline: String,
    pub load: String,
}

impl Default for Status {
    fn default() -> Self {
        Self { cluster: "unknown".to_string(), pipeline: "unknown".to_string(), load: "stopped".to_string() }
    }
}

pub struct AppState {
    /// The one global load, if any is running. A `tokio::sync::Mutex` because handlers hold it
    /// across `.await` (starting/stopping/joining a run).
    pub run: tokio::sync::Mutex<Option<RunHandle>>,
    /// Fan-out for everything the reader, the run loop, and the handlers publish. Task 5's `/ws`
    /// subscribes to this; Task 4 only needs it to exist so `Event::Log`/`Event::Status` from the
    /// control API has somewhere to go.
    pub tx: broadcast::Sender<Event>,
    /// Never held across `.await` — plain `std::sync::Mutex` is enough and cheaper.
    pub status: Mutex<Status>,
    /// Injected so the control API is testable without a `podman` binary — see `podman.rs`.
    pub cluster: Arc<dyn Cluster>,
    /// The live cluster's connection string. Used both to start a load and to open the dedicated
    /// connection `pipeline/rebuild` needs for the subscription drop and the setup SQL.
    pub db_url: String,
    /// Where `pipeline/rebuild` finds `setup_realtime.sql`. A path rather than a hardcoded
    /// literal so it can point at a fixture in tests without touching the real scenario file.
    pub pipeline_sql: PathBuf,
}

impl AppState {
    pub fn new(cluster: Arc<dyn Cluster>, db_url: String, pipeline_sql: PathBuf) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            run: tokio::sync::Mutex::new(None),
            tx,
            status: Mutex::new(Status::default()),
            cluster,
            db_url,
            pipeline_sql,
        }
    }

    pub fn publish(&self, event: Event) {
        // No receiver yet is not an error; the reader has the same rule (see stream.rs).
        let _ = self.tx.send(event);
    }

    pub fn set_status(&self, f: impl FnOnce(&mut Status)) {
        let mut guard = self.status.lock().expect("status mutex poisoned");
        f(&mut guard);
    }

    pub fn status_snapshot(&self) -> Status {
        self.status.lock().expect("status mutex poisoned").clone()
    }
}
