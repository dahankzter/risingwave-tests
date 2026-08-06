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
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// How many alerts a late joiner's `Snapshot` carries. Enough to fill a screen without making
/// the snapshot frame itself large.
const RECENT_CAPACITY: usize = 50;

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
    /// Ring buffer of the last `RECENT_CAPACITY` `Event::Alert`s, kept for `ws.rs`'s
    /// `Event::Snapshot` so a client that connects mid-run sees recent activity immediately
    /// instead of an empty screen until the next alert. Maintained by `ws::spawn_aggregator`,
    /// which sees every alert (unsampled) off the broadcast channel.
    pub recent: Mutex<VecDeque<Event>>,
    /// The most recent `Event::Stats`, if any alert has been measured yet. Also for `Snapshot`.
    pub last_stats: Mutex<Option<Event>>,
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
            recent: Mutex::new(VecDeque::with_capacity(RECENT_CAPACITY)),
            last_stats: Mutex::new(None),
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

    /// Records one alert into the ring buffer, dropping the oldest once at capacity. Called by
    /// the aggregator for every alert it sees off the broadcast channel, not by the sampled
    /// per-client forwarding path — the ring buffer, like the percentiles, must reflect every
    /// alert, not the thinned display feed.
    pub fn record_alert(&self, event: Event) {
        let mut guard = self.recent.lock().expect("recent mutex poisoned");
        if guard.len() == RECENT_CAPACITY {
            guard.pop_front();
        }
        guard.push_back(event);
    }

    /// Snapshot of the ring buffer, oldest first, for `Event::Snapshot`.
    pub fn recent_snapshot(&self) -> Vec<Event> {
        self.recent.lock().expect("recent mutex poisoned").iter().cloned().collect()
    }

    pub fn set_last_stats(&self, event: Event) {
        *self.last_stats.lock().expect("last_stats mutex poisoned") = Some(event);
    }

    pub fn last_stats_snapshot(&self) -> Option<Event> {
        self.last_stats.lock().expect("last_stats mutex poisoned").clone()
    }

    /// Builds the `Event::Snapshot` sent first to every new WebSocket client, and re-sent after
    /// a `RecvError::Lagged` so a slow client resyncs instead of the producer stalling for it.
    pub fn snapshot_event(&self) -> Event {
        let status = self.status_snapshot();
        Event::Snapshot {
            status: Box::new(Event::Status { cluster: status.cluster, pipeline: status.pipeline, load: status.load }),
            recent: self.recent_snapshot(),
            stats: self.last_stats_snapshot().map(Box::new),
        }
    }
}
