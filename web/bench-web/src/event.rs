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
        /// When the cluster took ownership of the chain's completing row, in unix milliseconds.
        /// Carried so the aggregator can tell an alert belonging to THIS run from one whose
        /// trigger was ingested in a previous one — see `StatsReset`.
        ingest_ms: f64,
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
        /// Watermark lateness the LIVE table declares, read from the catalog each poll. `None` when
        /// there is no pipeline to describe. The page compares this against the selector so an
        /// unapplied change is visible instead of silently misattributing seconds.
        lateness_secs: Option<u32>,
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
    /// The measurement epoch rolled over (a load started or the pipeline was rebuilt): the
    /// aggregator's percentiles restart so a run's numbers describe that run — accumulated
    /// cross-run samples put a stale p95 two orders of magnitude above p50. Clients clear their
    /// stats history on this.
    ///
    /// `epoch_ms` is the boundary in unix milliseconds. Resetting the accumulator is not enough on
    /// its own: rows left unreleased in the sort by a previous run (their watermark never advanced
    /// because the traffic stopped) are released the moment new traffic arrives, and match
    /// immediately — producing genuine alerts whose trigger was ingested tens of minutes ago. They
    /// are real, but they measure the absence of traffic, not the pipeline, so they are excluded
    /// from this run's percentiles by comparing each alert's `ingest_ms` against this.
    StatsReset { epoch_ms: f64 },
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
