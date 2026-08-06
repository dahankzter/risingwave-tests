//! What the server pushes to the browser. One tagged enum so the client can switch on `type`.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// One matched chain. Sampled for display; every alert still reaches the percentiles.
    Alert {
        partition: i32,
        chain_len: i64,
        latency_ms: f64,
        alert_ts: String,
    },
    Rate {
        rows_per_sec_in: f64,
        rows_per_sec_requested: f64,
        alerts_per_sec_out: f64,
    },
    Stats {
        n: usize,
        min_ms: f64,
        p50_ms: f64,
        p95_ms: f64,
        p99_ms: f64,
        max_ms: f64,
    },
    Status {
        cluster: String,
        pipeline: String,
        load: String,
    },
    Metrics {
        matches_emitted: u64,
        evicted_rows: u64,
        scan_budget_exhausted: u64,
    },
    Probe {
        round: u32,
        latency_ms: u64,
    },
    Snapshot {
        status: Box<Event>,
        recent: Vec<Event>,
        stats: Option<Box<Event>>,
    },
    Log {
        level: String,
        text: String,
    },
}
