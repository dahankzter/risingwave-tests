# Bench Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single web page that shows MATCH_RECOGNIZE fraud alerts arriving live, with the throughput and latency to back them up, and controls to drive the cluster and the load from a browser.

**Architecture:** A third crate `bench-web` in the existing `web/` workspace: Axum for controls, a WebSocket for the event stream, and static assets embedded in the binary. Alerts reach the server through a RisingWave subscription cursor, not polling. The run loop currently inlined in the CLI moves into `bench-core` first, so the CLI and the server drive identical generator behaviour.

**Tech Stack:** Rust 1.97, axum 0.8, tokio, tokio-postgres, serde/serde_json, rust-embed, plus the existing `bench-core`.

## Global Constraints

- Rust edition 2021. Workspace at `web/`, already containing `bench-core` and `bench`.
- Bind to `127.0.0.1` by default. `--bind` overrides, and a non-loopback bind prints a startup warning. This service shells out to podman and can destroy a data volume.
- `POST /api/cluster/clean` requires `{"confirm":"clean"}` in the request body. A UI-only dialog is not sufficient — the endpoint must be unusable by accident from `curl`.
- One global run: one load, one probe, one cluster. `POST /api/load/start` while a load is running returns 409, never queues.
- The probe forces `SENTINEL=off` whenever a load is active. With the sentinel on it advances the watermark itself, which released the background feed's matches early and made the server-side p50 read 3.4s against the 7.2s the same feed gives when measured alone.
- Realtime event timestamps are taken at insert time from the real clock. Never from an accumulated schedule.
- `make smoke` must pass unchanged after every task — 5 scenarios, all ok.
- CPU pinning defaults to OFF. Every number in the README today was measured unpinned and is not comparable to a pinned run.

## What already exists

`bench-core`'s real public API, verified at plan time — build against this, not against the spec's projection:

| module | items |
|---|---|
| `gen` | `Kind::{Deposit,Bet,Withdraw,Noop}` + `as_str()`, `Event{partition,kind,amount}`, `Config` (fields incl. `rows, partitions, hot_count, hot_share, bets_min, bets_max, abandon_prob, payload_cols, payload_bytes, seed, ties`), `Config::{default,validate}`, `ConfigError`, `Generator::{new,next_event,open_chains}` |
| `pace` | `Pacer::{new,rate,due,set_rate}` |
| `sink` | `Ts::{Tick,Wall}`, `Row`, `column_list`, `Sink` (impl'd only by `EmitSql`), `EmitSql::new`, `Direct::{connect,client,write_async}` |
| `pipeline` | `SealConfig`, `SealOutcome`, `run_sql_file`, `Settle`/`Poll`/`Settle::observe`, `seal` |

`measure.rs` does not exist. The run loop does not exist as a reusable unit — it is inline in `web/bench/src/main.rs`.

The pipeline it drives is `scenarios/perf/setup_realtime.sql`: `t_rt` (with `ingest_ts` as `proctime()`), `mv_rt` (MATCH_RECOGNIZE, carrying `trigger_ingest_ts`), sink `rt_feed`, and `t_rt_alerts` (with `alert_ts` as `proctime()`). `alert_ts - trigger_ingest_ts` is a match's own measured latency.

Subscription cursors are verified working on `bee0fbd`:

```sql
create subscription sub_alerts from t_rt_alerts with (retention = '1 day');
declare cur subscription cursor for sub_alerts;
fetch 10 from cur;   -- yields table columns plus `op` and `rw_timestamp`
```

`FETCH` is non-blocking and returns zero rows when nothing is new, so the reader is a loop with a short sleep. Cursors are session-scoped: the reader needs its own dedicated connection.

---

## File Structure

| file | responsibility |
|---|---|
| `web/bench-core/src/run.rs` | the run loop as a driveable handle: start, set_rate, stop, progress |
| `web/bench-core/src/measure.rs` | latency percentiles and rate windows; pure, no I/O |
| `web/bench-web/src/main.rs` | wiring: config, router, listener |
| `web/bench-web/src/state.rs` | `AppState`: the single global run, broadcast sender, status |
| `web/bench-web/src/event.rs` | the `Event` enum serialised to the browser |
| `web/bench-web/src/api.rs` | POST control handlers |
| `web/bench-web/src/stream.rs` | subscription cursor reader task |
| `web/bench-web/src/podman.rs` | cluster up/down/clean |
| `web/bench-web/src/metrics.rs` | Prometheus scrape of the compute node |
| `web/bench-web/src/pin.rs` | CPU core detection and assignment |
| `web/bench-web/static/{index.html,app.js,style.css}` | the page |

---

### Task 1: Extract the run loop into `bench-core`

The CLI's `main.rs` currently owns the generate-pace-insert loop. The web server cannot drive a loop that lives inside `fn main`, and duplicating it would recreate the divergence this whole port was meant to end.

**Files:**
- Create: `web/bench-core/src/run.rs`
- Modify: `web/bench-core/src/lib.rs` (add `pub mod run;`), `web/bench/src/main.rs` (drive the new handle instead of looping inline)

**Interfaces:**
- Consumes: `gen::{Config, Generator}`, `pace::Pacer`, `sink::{Direct, Row, Ts}`.
- Produces:
  - `pub struct RunConfig { pub table: String, pub url: String, pub realtime: bool, pub batch: usize, pub rate: f64, pub gen: gen::Config }`
  - `pub struct Progress { pub rows_sent: u64, pub rows_target: u64, pub rate_requested: f64, pub open_chains: usize, pub done: bool }`
  - `pub struct RunHandle` with `pub fn progress(&self) -> tokio::sync::watch::Receiver<Progress>`, `pub fn set_rate(&self, rate: f64)`, `pub fn stop(&self)`, `pub async fn join(self) -> anyhow::Result<()>`
  - `pub async fn start(cfg: RunConfig) -> anyhow::Result<RunHandle>`

- [ ] **Step 1: Write the failing test**

Create the test module at the bottom of `web/bench-core/src/run.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_non_positive_rate_before_constructing_a_pacer() {
        // Pacer::new asserts rate > 0.0. RunConfig must reject it first so a caller gets an
        // error rather than a panic — the web server takes this value from an HTTP request.
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: true,
            batch: 500,
            rate: 0.0,
            gen: crate::gen::Config::default(),
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("rate"), "got: {err}");
    }

    #[test]
    fn rejects_a_batch_that_would_exceed_the_bound_parameter_limit() {
        // Direct::write_async binds batch * (4 + payload_cols) parameters; Postgres allows 65535.
        let mut gen = crate::gen::Config::default();
        gen.payload_cols = 4;
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: false,
            batch: 20_000,
            rate: 1.0,
            gen,
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("65535"), "got: {err}");
    }

    #[test]
    fn realtime_rejects_tie_density() {
        let mut gen = crate::gen::Config::default();
        gen.ties = 2;
        let cfg = RunConfig {
            table: "t".into(),
            url: "postgres://x".into(),
            realtime: true,
            batch: 500,
            rate: 1.0,
            gen,
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ties"), "got: {err}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && cargo test -p bench-core run::`
Expected: FAIL — `cannot find type RunConfig in this scope`.

- [ ] **Step 3: Write the implementation**

Write `web/bench-core/src/run.rs`. The loop body must be moved from `web/bench/src/main.rs` — read that file and lift its logic rather than writing a new one from memory, preserving exactly:
- bulk sets `rw_implicit_flush` to `false`, realtime to `true`;
- realtime sleeps until `pacer.due(row_index)` before each row and stamps `Ts::Wall(OffsetDateTime::now_utc())` at insert time;
- bulk stamps `Ts::Tick`, incrementing per tie group.

Structure:

```rust
//! The run loop as a driveable unit.
//!
//! The CLI and the web console must produce identical workloads; the only way to guarantee that is
//! for both to drive this one loop. It owns pacing, batching and the sink, and exposes a handle so
//! a caller can change the rate or stop mid-run without reaching inside.

use crate::gen::{Config as GenConfig, Generator};
use crate::pace::Pacer;
use crate::sink::{Direct, Row, Ts};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub table: String,
    pub url: String,
    pub realtime: bool,
    pub batch: usize,
    pub rate: f64,
    pub gen: GenConfig,
}

#[derive(Debug, Clone, Default)]
pub struct Progress {
    pub rows_sent: u64,
    pub rows_target: u64,
    pub rate_requested: f64,
    pub open_chains: usize,
    pub done: bool,
}

impl RunConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.gen.validate()?;
        if self.rate <= 0.0 {
            anyhow::bail!("rate must be positive (got {})", self.rate);
        }
        let per_row = 4 + self.gen.payload_cols;
        if self.batch * per_row > 65_535 {
            anyhow::bail!(
                "batch {} x {} columns exceeds the PostgreSQL limit of 65535 bound parameters per statement",
                self.batch, per_row
            );
        }
        if self.realtime && self.gen.ties > 1 {
            anyhow::bail!(
                "ties {} has no effect in realtime mode: timestamps come from the wall clock per row",
                self.gen.ties
            );
        }
        Ok(())
    }
}

pub struct RunHandle {
    rate: Arc<AtomicU64>,          // f64 bits
    stop: Arc<AtomicBool>,
    progress: watch::Receiver<Progress>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RunHandle {
    pub fn progress(&self) -> watch::Receiver<Progress> {
        self.progress.clone()
    }
    pub fn set_rate(&self, rate: f64) {
        if rate > 0.0 {
            self.rate.store(rate.to_bits(), Ordering::Relaxed);
        }
    }
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
    pub async fn join(self) -> anyhow::Result<()> {
        self.task.await?
    }
}

pub async fn start(cfg: RunConfig) -> anyhow::Result<RunHandle> {
    cfg.validate()?;
    // ... connect, spawn the loop, return the handle
    todo!("lift the loop body from web/bench/src/main.rs")
}
```

Replace the `todo!` with the lifted loop. The loop reads `rate` from the atomic each batch and calls `pacer.set_rate(rows_sent, Instant::now(), new_rate)` when it changes; it checks `stop` each batch; it publishes `Progress` through the watch channel each batch.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd web && cargo test -p bench-core`
Expected: PASS, including the three new tests.

- [ ] **Step 5: Rewire the CLI to drive the handle**

`web/bench/src/main.rs`'s `Load` arm becomes: build `RunConfig`, call `run::start`, and await `join()`. The `--emit-sql` path stays in the CLI — it does not connect, so it does not use `run`.

- [ ] **Step 6: Verify the CLI is behaviourally unchanged**

```bash
cd web && cargo build --release
cd /home/dahankzter/projects/risingwave-tests
make load-setup && make load PROFILE=small ROWS=20000
web/target/release/bench load --table t_rt --mode realtime --ties 2 --rows 10   # must still error
make smoke
```

Expected: load completes and seals; the `--ties` rejection still fires; smoke 5/5.

- [ ] **Step 7: Commit**

```bash
git add web/bench-core/src/run.rs web/bench-core/src/lib.rs web/bench/src/main.rs
git commit -m "refactor(bench-core): extract the run loop into a driveable handle"
```

---

### Task 2: Measurement

**Files:**
- Create: `web/bench-core/src/measure.rs`
- Modify: `web/bench-core/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct Percentiles { pub n: usize, pub min_ms: f64, pub p50_ms: f64, pub p95_ms: f64, pub p99_ms: f64, pub max_ms: f64 }`
  - `pub struct Latencies` with `new()`, `push(&mut self, ms: f64)`, `percentiles(&self) -> Option<Percentiles>`
  - `pub struct RateWindow` with `new(window: Duration)`, `record(&mut self, at: Instant, n: u64)`, `per_sec(&self, now: Instant) -> f64`

- [ ] **Step 1: Write the failing tests**

Append to `web/bench-core/src/measure.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn percentiles_over_a_known_distribution() {
        let mut l = Latencies::new();
        for i in 1..=100 {
            l.push(i as f64);
        }
        let p = l.percentiles().unwrap();
        assert_eq!(p.n, 100);
        assert_eq!(p.min_ms, 1.0);
        assert_eq!(p.max_ms, 100.0);
        assert!((p.p50_ms - 50.0).abs() <= 1.0, "p50 was {}", p.p50_ms);
        assert!((p.p95_ms - 95.0).abs() <= 1.0, "p95 was {}", p.p95_ms);
    }

    #[test]
    fn percentiles_are_none_until_there_is_data() {
        assert!(Latencies::new().percentiles().is_none());
    }

    #[test]
    fn a_single_sample_is_every_percentile() {
        let mut l = Latencies::new();
        l.push(7.0);
        let p = l.percentiles().unwrap();
        assert_eq!((p.min_ms, p.p50_ms, p.p99_ms, p.max_ms), (7.0, 7.0, 7.0, 7.0));
    }

    #[test]
    fn rate_window_reports_per_second_over_its_window() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(Duration::from_secs(2));
        w.record(t0, 1000);
        w.record(t0 + Duration::from_secs(1), 1000);
        // 2000 rows across a 2s window
        let r = w.per_sec(t0 + Duration::from_secs(2));
        assert!((r - 1000.0).abs() < 50.0, "rate was {r}");
    }

    #[test]
    fn rate_window_forgets_samples_older_than_the_window() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(Duration::from_secs(2));
        w.record(t0, 10_000);
        // Ten seconds later that sample is long outside the window.
        assert_eq!(w.per_sec(t0 + Duration::from_secs(10)), 0.0);
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd web && cargo test -p bench-core measure`
Expected: FAIL — `cannot find type Latencies`.

- [ ] **Step 3: Implement**

```rust
//! Latency and throughput summarisation.
//!
//! Pure: no clock of its own beyond the instants callers pass in, and no I/O. The UI's percentiles
//! are computed here rather than by re-querying latency/report.sql on a timer.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Percentiles {
    pub n: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

/// Every latency sample seen this run. The UI's feed is sampled for display, but percentiles
/// must cover all of them — a percentile over a display sample is not a percentile.
#[derive(Debug, Default)]
pub struct Latencies {
    sorted: Vec<f64>,
    dirty: bool,
    raw: Vec<f64>,
}

impl Latencies {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, ms: f64) {
        self.raw.push(ms);
        self.dirty = true;
    }

    pub fn percentiles(&self) -> Option<Percentiles> {
        if self.raw.is_empty() {
            return None;
        }
        let mut v = self.raw.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pick = |q: f64| -> f64 {
            let idx = ((v.len() - 1) as f64 * q).round() as usize;
            v[idx]
        };
        Some(Percentiles {
            n: v.len(),
            min_ms: v[0],
            p50_ms: pick(0.50),
            p95_ms: pick(0.95),
            p99_ms: pick(0.99),
            max_ms: v[v.len() - 1],
        })
    }
}

/// Rows per second over a sliding window.
#[derive(Debug)]
pub struct RateWindow {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
}

impl RateWindow {
    pub fn new(window: Duration) -> Self {
        Self { window, samples: VecDeque::new() }
    }

    pub fn record(&mut self, at: Instant, n: u64) {
        self.samples.push_back((at, n));
    }

    pub fn per_sec(&self, now: Instant) -> f64 {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        let total: u64 = self
            .samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, n)| n)
            .sum();
        total as f64 / self.window.as_secs_f64()
    }
}
```

Note `per_sec` filters rather than mutating, so it can take `&self`. Add an explicit prune inside `record` to stop `samples` growing without bound — drop entries older than twice the window.

- [ ] **Step 4: Run to verify they pass**

Run: `cd web && cargo test -p bench-core measure`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add web/bench-core/src/measure.rs web/bench-core/src/lib.rs
git commit -m "feat(bench-core): latency percentiles and sliding-window rate"
```

---

### Task 3: The alert stream reader

**Files:**
- Create: `web/bench-web/Cargo.toml`, `web/bench-web/src/main.rs` (skeleton), `web/bench-web/src/event.rs`, `web/bench-web/src/stream.rs`
- Modify: `web/Cargo.toml` (add the member)

**Interfaces:**
- Produces:
  - `event::Event` — a `#[serde(tag = "type")]` enum with variants `Alert`, `Rate`, `Stats`, `Status`, `Metrics`, `Probe`, `Snapshot`, `Log`
  - `stream::spawn_reader(url: String, tx: broadcast::Sender<Event>) -> tokio::task::JoinHandle<()>`

- [ ] **Step 1: Add the crate**

`web/bench-web/Cargo.toml`:

```toml
[package]
name = "bench-web"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
bench-core = { path = "../bench-core" }
anyhow.workspace = true
clap.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "signal", "sync", "process"] }
tokio-postgres.workspace = true
time.workspace = true
axum = { version = "0.8", features = ["ws"] }
tower-http = { version = "0.6", features = ["trace"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rust-embed = "8"
mime_guess = "2"
```

Add `"bench-web"` to `members` in `web/Cargo.toml`.

- [ ] **Step 2: Write the event enum**

`web/bench-web/src/event.rs`:

```rust
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
```

- [ ] **Step 3: Write the reader**

`web/bench-web/src/stream.rs`. Behaviour required:

- Own a dedicated connection — subscription cursors are session-scoped.
- On start: `create subscription if not exists sub_alerts from t_rt_alerts with (retention = '1 day')`, then `declare cur subscription cursor for sub_alerts`.
- Loop: `fetch 100 from cur`. `FETCH` is non-blocking and returns zero rows when nothing is new, so sleep ~200ms on an empty fetch rather than spinning.
- For each row compute `latency_ms` from `alert_ts - trigger_ingest_ts` and publish an `Event::Alert`.
- If the cursor or subscription vanishes — the pipeline was rebuilt — re-declare on the next tick and emit `Event::Log`. **Never let this task die**; a dead reader means a silently empty page.

- [ ] **Step 4: Test it against the live cluster**

There is no unit test here — it is I/O. Verify by hand:

```bash
cd /home/dahankzter/projects/risingwave-tests
make rt-setup
cd web && cargo run -p bench-web &            # once main.rs can start the reader
cd .. && make rt-load ROWS=20000 RATE=2000
```

Expected: the reader's log shows alerts arriving within ~7s of the feed starting. Record the observed count in the report.

- [ ] **Step 5: Commit**

```bash
git add web/Cargo.toml web/bench-web/
git commit -m "feat(bench-web): subscription-cursor alert reader"
```

---

### Task 4: Server, state, and the control API

**Files:**
- Modify: `web/bench-web/src/main.rs`
- Create: `web/bench-web/src/state.rs`, `web/bench-web/src/api.rs`, `web/bench-web/src/podman.rs`

**Interfaces:**
- Produces: `AppState { run: Mutex<Option<RunHandle>>, tx: broadcast::Sender<Event>, status: Mutex<Status> }`, the router, and the endpoints listed in Global Constraints.

- [ ] **Step 1: Write the failing test**

`web/bench-web/tests/api.rs` — these run without a cluster, exercising rejection paths only:

```rust
// Uses axum's testing surface: build the router, send requests, assert status codes.
// No database and no podman required for any assertion here.

#[tokio::test]
async fn clean_without_the_confirmation_token_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(&app, "/api/cluster/clean", serde_json::json!({})).await;
    assert_eq!(res, 400, "clean must refuse without an explicit confirmation");
}

#[tokio::test]
async fn clean_with_the_wrong_token_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(&app, "/api/cluster/clean",
        serde_json::json!({"confirm": "yes"})).await;
    assert_eq!(res, 400);
}

#[tokio::test]
async fn starting_a_load_with_an_invalid_rate_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(&app, "/api/load/start",
        serde_json::json!({"rate": 0, "rows": 100, "partitions": 10})).await;
    assert_eq!(res, 400, "an invalid rate must not reach Pacer::new, which panics on it");
}
```

`router_for_test()` and `post_json()` are small helpers the task adds to `lib.rs`; `bench-web` gains a `src/lib.rs` exposing them plus the modules, with `main.rs` reduced to argument parsing and `serve()`.

- [ ] **Step 2: Run to verify they fail**

Run: `cd web && cargo test -p bench-web`
Expected: FAIL — crate has no `router_for_test`.

- [ ] **Step 3: Implement state, podman, and the handlers**

`podman.rs` shells out with `tokio::process::Command`. `up` mirrors the Makefile exactly, including `--replace`, `--platform linux/amd64`, both published ports, and the `rw-tests-data` volume — plus **`-p 1222:1222`**, which the Makefile and `compose.yaml` must also gain in Task 7 for the metrics scrape.

`clean` must check the confirmation token before doing anything.

Handlers return 409 when the requested transition is impossible (no cluster, load already running). `pipeline/rebuild` stops a running load first, then runs `scenarios/perf/setup_realtime.sql` via `bench_core::pipeline::run_sql_file`, then re-creates the subscription.

- [ ] **Step 4: Run to verify they pass**

Run: `cd web && cargo test -p bench-web`
Expected: PASS, 3 tests.

- [ ] **Step 5: Verify the destructive path by hand, carefully**

```bash
cd web && cargo run -p bench-web &
curl -sS -X POST localhost:3000/api/cluster/clean -H 'content-type: application/json' -d '{}' -w ' -> %{http_code}\n'
curl -sS -X POST localhost:3000/api/cluster/clean -H 'content-type: application/json' -d '{"confirm":"clean"}' -w ' -> %{http_code}\n'
```

Expected: 400 then 200. **Run the second one only when you are willing to lose the data volume**; recreate afterwards with `make up`.

- [ ] **Step 6: Commit**

```bash
git add web/bench-web/
git commit -m "feat(bench-web): control API, app state, podman driver"
```

---

### Task 5: WebSocket fan-out, sampling, and snapshots

**Files:**
- Modify: `web/bench-web/src/main.rs`, `web/bench-web/src/state.rs`
- Create: `web/bench-web/src/ws.rs`

**Interfaces:**
- Produces: `GET /ws`, plus the 250ms aggregation tick that emits `Rate` and `Stats`, and the 2s tick that emits `Metrics`.

- [ ] **Step 1: Write the failing test**

Sampling is the testable part; the socket is not. In `web/bench-web/src/ws.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// At 2000 rows/s the alert rate is roughly 500/s — far more than a browser should render.
    /// Every alert must still reach the percentiles; only the display feed is thinned.
    #[test]
    fn sampler_thins_to_roughly_the_target_rate() {
        let mut s = Sampler::new(20.0); // 20 forwarded per second
        let mut forwarded = 0;
        // 500 alerts arriving over one simulated second
        for i in 0..500 {
            if s.should_forward(i as f64 / 500.0) {
                forwarded += 1;
            }
        }
        assert!((15..=25).contains(&forwarded), "forwarded {forwarded}, want ~20");
    }

    #[test]
    fn sampler_forwards_everything_when_the_rate_is_below_the_target() {
        let mut s = Sampler::new(20.0);
        let mut forwarded = 0;
        for i in 0..5 {
            if s.should_forward(i as f64 / 5.0) {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 5, "a slow feed must not be thinned at all");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd web && cargo test -p bench-web ws`
Expected: FAIL — `cannot find type Sampler`.

- [ ] **Step 3: Implement**

`Sampler` forwards at most `target` items per second of simulated time. The socket handler subscribes to the broadcast, sends a `Snapshot` first (status, the last 50 alerts from a ring buffer, current stats), then forwards. On `RecvError::Lagged` it re-sends a `Snapshot` rather than closing — a slow client resyncs instead of stalling the producer.

- [ ] **Step 4: Run to verify it passes**

Run: `cd web && cargo test -p bench-web`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/bench-web/src/ws.rs web/bench-web/src/main.rs web/bench-web/src/state.rs
git commit -m "feat(bench-web): websocket fan-out with sampling and snapshots"
```

---

### Task 6: The page

**Files:**
- Create: `web/bench-web/static/index.html`, `web/bench-web/static/app.js`, `web/bench-web/static/style.css`
- Modify: `web/bench-web/src/main.rs` (serve embedded assets)

Layout is decided: **the stream is the hero.** The alert feed occupies the left two-thirds; a right rail stacks the rows/s speedometer, alerts/s, and the latency chart. Controls sit in a thin top bar with cluster state. The demo lands because chains visibly resolve — `d b b w` becoming an alert about six seconds after the withdraw — so the feed dominates and the gauges corroborate.

- [ ] **Step 1: Build the demo tab**

Requirements, not a mockup to copy:
- Feed: newest alert at the top, 50 rows max, each showing partition, chain shape, latency. New rows animate in; the list must not reflow the page.
- The feed carries the label `showing ~20 of ~N alerts/s · percentiles cover all`, with N from the `Rate` event. A reviewer who notices the feed rate not matching the alerts/s gauge would otherwise wonder what else is approximated.
- Speedometer: rows/s in, with the requested rate marked, so a generator falling behind is visible rather than inferred.
- Latency chart: last ~120 `Stats` samples, p50 and p95 as two lines.
- Controls: start/stop load, a rate slider that calls `POST /api/load/rate` live, rebuild pipeline, cluster up/down, and clean behind a typed confirmation.
- Reconnect the WebSocket automatically with backoff, and show connection state. A demo that dies silently on a dropped socket is worse than one that says it dropped.

No build step and no CDN: plain ES modules and hand-written CSS, embedded with `rust-embed` so the binary is self-contained.

- [ ] **Step 2: Verify by hand against real traffic**

```bash
cd /home/dahankzter/projects/risingwave-tests && make up && make rt-setup
cd web && cargo run --release -p bench-web
# in the UI: start a load at 2000 rows/s, watch for ~60s
```

Expected and to be recorded in the report: alerts appear within ~7s of starting; the speedometer tracks the requested rate; moving the slider changes the observed rate within a couple of seconds; p50 settles near 6s. Take a screenshot.

- [ ] **Step 3: Commit**

```bash
git add web/bench-web/static web/bench-web/src/main.rs
git commit -m "feat(bench-web): the demo page"
```

---

### Task 7: Details tab, metrics scrape, and port publishing

**Files:**
- Create: `web/bench-web/src/metrics.rs`
- Modify: `web/bench-web/static/*`, `Makefile`, `compose.yaml`, `README.md`

- [ ] **Step 1: Publish the metrics port**

The compute node's Prometheus endpoint is on 1222 inside the container and is not currently published. Add `-p 1222:1222` to the Makefile's `up` target and to `compose.yaml`, keeping the two in step.

Verify: `curl -s localhost:1222/metrics | grep -c match_recognize` returns a non-zero count.

- [ ] **Step 2: Scrape and expose**

`metrics.rs` polls `http://127.0.0.1:1222/metrics` every 2s, sums the three `stream_match_recognize_*` counters across actors, and emits `Event::Metrics`. Counters are cumulative across dropped MVs, so the UI must label them as totals since cluster start, not per-run.

- [ ] **Step 3: Build the details tab**

Four stacked panels: latency percentiles and throughput; the operator metrics; pipeline state (open chains, live partitions, base rows vs matches); and run config (setup SQL, generator arguments, image tag, core layout).

A run that cannot produce trustworthy numbers — emulated on Apple Silicon, unpinned, or too few cores — is labelled as such here, so a screenshot of a Mac run cannot circulate as a measurement.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(bench-web): details tab, operator metrics, publish port 1222"
```

---

### Task 8: CPU pinning

**Files:**
- Create: `web/bench-web/src/pin.rs`
- Modify: `web/bench-web/src/podman.rs`, `web/bench-web/static/*`, `README.md`

**Interfaces:**
- Produces: `pub struct Layout { pub cluster: Option<String>, pub bench: Option<String>, pub parallelism: Option<u32>, pub why: String }`, `pub fn plan(total: usize) -> Layout`, `pub fn apply_to_self(layout: &Layout) -> anyhow::Result<()>`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_few_cores_pins_nothing() {
        let l = plan(4);
        assert!(l.cluster.is_none() && l.bench.is_none());
        assert!(l.why.contains("too few"), "why: {}", l.why);
    }

    #[test]
    fn a_large_box_reserves_two_cores_for_the_bench() {
        let l = plan(64);
        assert_eq!(l.bench.as_deref(), Some("62-63"));
        assert_eq!(l.cluster.as_deref(), Some("0-61"));
        // Parallelism must match the cpuset, or the cluster spawns 64 workers on 62 cores.
        assert_eq!(l.parallelism, Some(62));
    }

    #[test]
    fn the_boundary_case_still_leaves_a_usable_cluster() {
        let l = plan(8);
        assert_eq!(l.bench.as_deref(), Some("6-7"));
        assert_eq!(l.cluster.as_deref(), Some("0-5"));
        assert_eq!(l.parallelism, Some(6));
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd web && cargo test -p bench-web pin`

- [ ] **Step 3: Implement**

```
detect usable cores N   (from the cgroup quota where one exists, not nproc —
                         inside a constrained environment nproc overreports)
  N < 8   -> pin nothing, why = "too few cores to partition (N)"
  N >= 8  -> bench = last 2 cores, cluster = the rest, parallelism = cluster count
platform
  Linux   -> cpuset on the container (podman --cpuset-cpus) + sched_setaffinity on this process
  macOS   -> cpuset on the container only; why records that process affinity is unavailable
```

`apply_to_self` is Linux-only: `sched_setaffinity` has no macOS equivalent — macOS exposes only affinity *tags*, which are scheduler hints and unusable here. On macOS it must return `Ok(())` having done nothing, and say so in `why`. **Compile it behind `#[cfg(target_os = "linux")]` with a no-op fallback**, so the crate still builds on a Mac.

Setting `streaming_parallelism` to match the cpuset is the part that actually buys the isolation: RisingWave sizes its thread pools from the detected core count, so a container pinned to 16 cores may still spawn 64 streaming workers and thrash.

- [ ] **Step 4: Run to verify they pass**

Run: `cd web && cargo test -p bench-web`

- [ ] **Step 5: Wire it in, defaulting to off**

Pinning is opt-in: `--pin` enables the automatic layout, `--cores-cluster`/`--cores-bench` override it. The details tab shows the layout in effect and `why`. Off by default, because every number in the README today was measured unpinned.

- [ ] **Step 6: Verify on this rig**

```bash
cd web && cargo run --release -p bench-web -- --pin
# then, from the details tab or:
podman inspect rw-tests -f '{{.HostConfig.CpusetCpus}}'
```

Expected on this 64-core box: cluster `0-61`, bench `62-63`, parallelism 62. Record whether a 2k rows/s load's p50 changes against the unpinned baseline of ~6.0s.

**The macOS path cannot be tested here.** It must degrade and log; a colleague has to confirm it. Say so in the report rather than implying it was verified.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(bench-web): optional automatic CPU pinning"
```

---

## Self-Review

**Spec coverage.** Demo tab (Task 6), details tab with all four panels (7), the full control surface with the confirmation token and 409 semantics (4), subscription-cursor streaming rather than polling (3), sampling with percentiles over all alerts (2, 5), snapshots for late joiners (5), operator metrics needing port 1222 (7), CPU pinning with safe auto-assignment degrading on macOS (8).

**Deviation from the spec, deliberate.** The spec assumed `bench-core` already exposed a `Run` handle. It does not — the run loop is inline in the CLI, because that is all the CLI needed. Task 1 extracts it before anything consumes it, and rewires the CLI onto it so the two cannot diverge. This also gives the `--rate`, batch-limit and realtime-`--ties` validations a single home, replacing the CLI-local checks added at the end of the previous plan.

**Not covered, deliberately out of scope:** authentication; persisting run history across restarts; multi-cluster orchestration; a Rust port of `latency/probe.sh` (the probe endpoint shells out to the existing script); realtime tie-density support (rejected rather than implemented).

**Known risk.** Task 3's reader has no unit test — it is I/O against a live cluster, and the failure mode that matters (the cursor vanishing on pipeline rebuild) only reproduces against a real database. Task 3 Step 4 verifies it by hand; the reader's must-never-die requirement is the thing to scrutinise in review.
