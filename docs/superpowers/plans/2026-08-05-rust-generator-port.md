# Rust Generator Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `datagen/gen.py` and `datagen/seal.sh` with a Rust workspace whose pacing logic is unit-testable without a database, keeping every existing `make` target working.

**Architecture:** A cargo workspace under `web/`. `bench-core` is a library with no CLI or web dependencies: chain generation, pacing, a sink abstraction with direct-connection and emit-SQL implementations, and pipeline setup/seal. `bench` is a thin CLI over it. The web server (plan 2) will consume the same library.

**Tech Stack:** Rust 1.97, tokio, tokio-postgres, clap 4 (derive), rand + rand_chacha, anyhow, thiserror, time.

## Global Constraints

- Rust edition 2021. Toolchain already present: `cargo 1.97.1`, `rustc 1.97.1`.
- Workspace lives at `web/`. Nothing outside `web/`, the `Makefile`, and `datagen/` changes in this plan.
- `make smoke` must pass unchanged after every task. It exercises scenario SQL, not the generator, and is the regression check that this port changed nothing observable.
- Default connection: `postgres://root@127.0.0.1:4566/dev`. Override with `--url` or `DATABASE_URL`. `PSQLFLAGS` does not apply to the Rust binary.
- Determinism uses `rand_chacha::ChaCha8Rng::seed_from_u64`, not `StdRng` — `StdRng`'s algorithm is explicitly allowed to change between `rand` releases, which would silently break golden tests.
- Realtime event timestamps are taken at insert time from the real clock. Never from an accumulated schedule — that is the drift defect the port exists to eliminate.
- Bulk mode emits no watermark sentinel inline. Sealing is a separate phase that settles first (see `pipeline.rs`).
- The generator emits typed rows. Only `sink.rs` turns rows into SQL text.

---

## File Structure

| file | responsibility |
|---|---|
| `web/Cargo.toml` | workspace manifest, shared dependency versions |
| `web/bench-core/src/lib.rs` | re-exports; module wiring only |
| `web/bench-core/src/pace.rs` | pure pacing: row index → instant. No I/O. |
| `web/bench-core/src/gen.rs` | `Config`, validation, chain shaping, skew, `Event` stream |
| `web/bench-core/src/sink.rs` | `Row`, `Sink` trait, `EmitSql`, `Direct` |
| `web/bench-core/src/pipeline.rs` | setup SQL execution, seal (settle → sentinel → settle) |
| `web/bench-core/tests/golden_sql.rs` | `--emit-sql` golden test |
| `web/bench-core/tests/integration.rs` | live-cluster tests, ignored by default |
| `web/bench/src/main.rs` | clap CLI |
| `Makefile` | targets migrated to the binary |

---

### Task 1: Workspace scaffold

**Files:**
- Create: `web/Cargo.toml`, `web/bench-core/Cargo.toml`, `web/bench-core/src/lib.rs`, `web/.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces: a workspace that `cargo test` runs clean, with `bench_core` as the library crate name.

- [ ] **Step 1: Create the workspace manifest**

`web/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = ["bench-core", "bench"]

[workspace.package]
edition = "2021"
rust-version = "1.97"

[workspace.dependencies]
anyhow = "1"
thiserror = "2"
rand = "0.8"
rand_chacha = "0.3"
time = { version = "0.3", features = ["formatting", "macros"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "signal", "sync"] }
tokio-postgres = { version = "0.7", features = ["with-time-0_3"] }
clap = { version = "4", features = ["derive", "env"] }
```

- [ ] **Step 2: Create the library crate**

`web/bench-core/Cargo.toml`:

```toml
[package]
name = "bench-core"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[lib]
name = "bench_core"
path = "src/lib.rs"

[dependencies]
anyhow.workspace = true
thiserror.workspace = true
rand.workspace = true
rand_chacha.workspace = true
time.workspace = true
tokio.workspace = true
tokio-postgres.workspace = true
```

`web/bench-core/src/lib.rs`:

```rust
//! Workload generation and measurement for the MATCH_RECOGNIZE bench.
//!
//! This crate knows nothing about CLIs or HTTP. It is driven by `bench` (the CLI) and by
//! `bench-web` (the demo console), which must both see identical generator behaviour.

pub mod gen;
pub mod pace;
pub mod pipeline;
pub mod sink;
```

Create empty module files so it compiles:

```bash
cd web/bench-core/src && touch gen.rs pace.rs pipeline.rs sink.rs
```

`web/.gitignore`:

```
target/
```

- [ ] **Step 3: Create a placeholder binary crate so the workspace resolves**

`web/bench/Cargo.toml`:

```toml
[package]
name = "bench"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
bench-core = { path = "../bench-core" }
anyhow.workspace = true
clap.workspace = true
tokio.workspace = true
```

`web/bench/src/main.rs`:

```rust
fn main() {
    println!("bench: not implemented yet");
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cd web && cargo test`
Expected: compiles, 0 tests, no warnings that mention missing modules.

- [ ] **Step 5: Commit**

```bash
git add web/
git commit -m "feat(web): scaffold the bench cargo workspace"
```

---

### Task 2: Pacing

The defect this eliminates: pacing that accumulates a schedule drifts ahead of the wall clock by however long the inserts take, and once it leads by more than the watermark delay, rows inserted with `now()` are dropped as late.

**Files:**
- Modify: `web/bench-core/src/pace.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `Pacer::new(start: Instant, rate: f64) -> Pacer`, `Pacer::due(&self, row_index: u64) -> Instant`, `Pacer::set_rate(&mut self, at_row: u64, at: Instant, rate: f64)`, `Pacer::rate(&self) -> f64`.

- [ ] **Step 1: Write the failing tests**

Append to `web/bench-core/src/pace.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn first_row_is_due_immediately() {
        let start = Instant::now();
        let p = Pacer::new(start, 2000.0);
        assert_eq!(p.due(0), start);
    }

    #[test]
    fn row_n_is_due_at_n_over_rate() {
        let start = Instant::now();
        let p = Pacer::new(start, 2000.0);
        assert_eq!(p.due(2000), start + Duration::from_secs(1));
        assert_eq!(p.due(1000), start + Duration::from_millis(500));
    }

    /// The drift defect: `due` must be a pure function of the row index, never of how many
    /// times it has been called or how long the caller took in between. Interleaving other
    /// work must not move the schedule.
    #[test]
    fn schedule_does_not_drift_with_call_history() {
        let start = Instant::now();
        let p = Pacer::new(start, 1000.0);
        let direct = p.due(5000);
        for i in 0..5000 {
            let _ = p.due(i);
        }
        assert_eq!(p.due(5000), direct);
        assert_eq!(direct, start + Duration::from_secs(5));
    }

    #[test]
    fn set_rate_rebases_from_the_change_point() {
        let start = Instant::now();
        let mut p = Pacer::new(start, 1000.0);
        // One second in, 1000 rows done, double the rate.
        let at = start + Duration::from_secs(1);
        p.set_rate(1000, at, 2000.0);
        assert_eq!(p.rate(), 2000.0);
        // The next 2000 rows now take one second, measured from the change point.
        assert_eq!(p.due(1000), at);
        assert_eq!(p.due(3000), at + Duration::from_secs(1));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && cargo test -p bench-core pace`
Expected: FAIL — `cannot find type Pacer in this scope`.

- [ ] **Step 3: Write the implementation**

Prepend to `web/bench-core/src/pace.rs`:

```rust
//! Wall-clock pacing for realtime workloads.
//!
//! Deliberately free of I/O so the scheduling rule is testable in milliseconds rather than by
//! running a two-minute benchmark and squinting at the result.
//!
//! `due` is a pure function of the row index. It must never accumulate, because a schedule that
//! accumulates ignores the time the inserts themselves take: it creeps ahead of the wall clock,
//! and once it leads by more than the table's watermark delay the watermark sits in the future
//! and anything inserted with `now()` is discarded as late.

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Pacer {
    /// Origin the current rate is measured from.
    epoch: Instant,
    /// Row index that `epoch` corresponds to.
    epoch_row: u64,
    rate: f64,
}

impl Pacer {
    pub fn new(start: Instant, rate: f64) -> Self {
        assert!(rate > 0.0, "rate must be positive");
        Self { epoch: start, epoch_row: 0, rate }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// When row `row_index` should be inserted.
    pub fn due(&self, row_index: u64) -> Instant {
        let ahead = row_index.saturating_sub(self.epoch_row);
        self.epoch + Duration::from_secs_f64(ahead as f64 / self.rate)
    }

    /// Change the rate, rebasing the schedule so rows already emitted are not re-timed.
    /// `at_row` is the next row index to be emitted and `at` is the instant it becomes the new
    /// origin — normally `Instant::now()`.
    pub fn set_rate(&mut self, at_row: u64, at: Instant, rate: f64) {
        assert!(rate > 0.0, "rate must be positive");
        self.epoch = at;
        self.epoch_row = at_row;
        self.rate = rate;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd web && cargo test -p bench-core pace`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add web/bench-core/src/pace.rs
git commit -m "feat(bench-core): wall-clock pacing that cannot drift"
```

---

### Task 3: Chain generation and config validation

**Files:**
- Modify: `web/bench-core/src/gen.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub enum Kind { Deposit, Bet, Withdraw, Noop }` with `Kind::as_str(&self) -> &'static str`
  - `pub struct Event { pub partition: i32, pub kind: Kind, pub amount: i32 }`
  - `pub struct Config` with public fields `rows: u64, partitions: i32, hot_count: i32, hot_share: f64, bets_min: u32, bets_max: u32, abandon_prob: f64, payload_cols: usize, payload_bytes: usize, seed: u64, ties: u32, tick_gap: i64, sentinel_partition: i32`
  - `Config::default()`, `Config::validate(&self) -> Result<(), ConfigError>`
  - `pub struct Generator`, `Generator::new(cfg: Config) -> Result<Generator, ConfigError>`, `Generator::next_event(&mut self) -> Event`, `Generator::open_chains(&self) -> usize`

- [ ] **Step 1: Write the failing tests**

Append to `web/bench-core/src/gen.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config { partitions: 2, bets_min: 2, bets_max: 2, ..Config::default() }
    }

    /// An abandoned chain runs its bets and then stops. It must never emit a withdraw, and the
    /// partition's next event must open a fresh chain — that buffered `d b+` prefix is the
    /// retained state the WITHIN bound is there to bound.
    #[test]
    fn abandoned_chains_never_withdraw() {
        let mut g = Generator::new(Config { abandon_prob: 1.0, ..cfg() }).unwrap();
        let kinds: Vec<Kind> = (0..40).map(|_| g.next_event().kind).collect();
        assert!(!kinds.contains(&Kind::Withdraw), "abandoned chains must not withdraw");
        assert!(kinds.iter().filter(|k| **k == Kind::Deposit).count() > 1,
                "a new chain must open after an abandoned one");
    }

    #[test]
    fn completed_chains_are_deposit_bets_withdraw() {
        let mut g = Generator::new(Config { abandon_prob: 0.0, partitions: 1, ..cfg() }).unwrap();
        let kinds: Vec<Kind> = (0..4).map(|_| g.next_event().kind).collect();
        assert_eq!(kinds, vec![Kind::Deposit, Kind::Bet, Kind::Bet, Kind::Withdraw]);
    }

    #[test]
    fn hot_count_must_leave_cold_partitions() {
        let err = Generator::new(Config { partitions: 10, hot_count: 10, ..Config::default() })
            .unwrap_err();
        assert!(matches!(err, ConfigError::HotCount { .. }), "got {err:?}");
    }

    #[test]
    fn hot_partitions_take_their_share() {
        let cfg = Config {
            rows: 10_000, partitions: 1000, hot_count: 10, hot_share: 0.9,
            ..Config::default()
        };
        let mut g = Generator::new(cfg).unwrap();
        let hot = (0..10_000).filter(|_| g.next_event().partition <= 10).count();
        assert!((8500..9500).contains(&hot), "hot share out of range: {hot}");
    }

    #[test]
    fn same_seed_gives_the_same_stream() {
        let mk = || {
            let mut g = Generator::new(cfg()).unwrap();
            (0..200).map(|_| { let e = g.next_event(); (e.partition, e.amount) }).collect::<Vec<_>>()
        };
        assert_eq!(mk(), mk());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && cargo test -p bench-core gen`
Expected: FAIL — `cannot find type Config in this scope`.

- [ ] **Step 3: Write the implementation**

Prepend to `web/bench-core/src/gen.rs`:

```rust
//! Fraud-shaped workload generation: per partition, a `deposit -> bet{1..n} -> withdraw` chain.
//!
//! A completed chain matches the bench pattern `(d b+ w)`. An abandoned chain runs its bets and
//! then stops, leaving a `d b+` prefix buffered until its WITHIN bound expires — the retained
//! state regime worth measuring. Abandoning at the deposit instead would leave almost nothing
//! behind, which is what the original Python did before it was fixed.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Deposit,
    Bet,
    Withdraw,
    Noop,
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Deposit => "deposit",
            Kind::Bet => "bet",
            Kind::Withdraw => "withdraw",
            Kind::Noop => "noop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub partition: i32,
    pub kind: Kind,
    pub amount: i32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rows: u64,
    pub partitions: i32,
    pub hot_count: i32,
    pub hot_share: f64,
    pub bets_min: u32,
    pub bets_max: u32,
    pub abandon_prob: f64,
    pub payload_cols: usize,
    pub payload_bytes: usize,
    pub seed: u64,
    pub ties: u32,
    pub tick_gap: i64,
    pub sentinel_partition: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: 100_000,
            partitions: 1_000,
            hot_count: 0,
            hot_share: 0.5,
            bets_min: 1,
            bets_max: 4,
            abandon_prob: 0.2,
            payload_cols: 0,
            payload_bytes: 32,
            seed: 42,
            ties: 1,
            tick_gap: 1,
            sentinel_partition: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("--hot-count ({hot_count}) must be < --partitions ({partitions}): no cold partitions would be left")]
    HotCount { hot_count: i32, partitions: i32 },
    #[error("--partitions must be at least 1")]
    Partitions,
    #[error("--ties must be at least 1")]
    Ties,
    #[error("--bets-min ({min}) must be <= --bets-max ({max})")]
    Bets { min: u32, max: u32 },
    #[error("--hot-share and --abandon-prob must be within 0.0..=1.0")]
    Probability,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partitions < 1 {
            return Err(ConfigError::Partitions);
        }
        if self.hot_count > 0 && self.hot_count >= self.partitions {
            return Err(ConfigError::HotCount {
                hot_count: self.hot_count,
                partitions: self.partitions,
            });
        }
        if self.ties < 1 {
            return Err(ConfigError::Ties);
        }
        if self.bets_min > self.bets_max {
            return Err(ConfigError::Bets { min: self.bets_min, max: self.bets_max });
        }
        if !(0.0..=1.0).contains(&self.hot_share) || !(0.0..=1.0).contains(&self.abandon_prob) {
            return Err(ConfigError::Probability);
        }
        Ok(())
    }
}

/// Mid-chain state for one partition: bets still to emit, and whether it ever completes.
#[derive(Debug, Clone, Copy)]
struct Chain {
    bets_left: u32,
    abandoned: bool,
}

pub struct Generator {
    cfg: Config,
    rng: ChaCha8Rng,
    chains: HashMap<i32, Chain>,
}

impl Generator {
    pub fn new(cfg: Config) -> Result<Self, ConfigError> {
        cfg.validate()?;
        let rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        Ok(Self { cfg, rng, chains: HashMap::new() })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Chains holding a buffered `d b+` prefix with no withdraw yet.
    pub fn open_chains(&self) -> usize {
        self.chains.len()
    }

    fn pick_partition(&mut self) -> i32 {
        if self.cfg.hot_count > 0 && self.rng.gen::<f64>() < self.cfg.hot_share {
            self.rng.gen_range(1..=self.cfg.hot_count)
        } else {
            self.rng.gen_range(self.cfg.hot_count + 1..=self.cfg.partitions)
        }
    }

    pub fn next_event(&mut self) -> Event {
        let partition = self.pick_partition();
        self.event_for(partition)
    }

    fn event_for(&mut self, partition: i32) -> Event {
        match self.chains.get(&partition).copied() {
            None => {
                let bets = self.rng.gen_range(self.cfg.bets_min..=self.cfg.bets_max);
                let abandoned = self.rng.gen::<f64>() < self.cfg.abandon_prob;
                self.chains.insert(partition, Chain { bets_left: bets, abandoned });
                Event { partition, kind: Kind::Deposit, amount: self.rng.gen_range(50..500) }
            }
            Some(c) if c.bets_left > 0 => {
                self.chains.insert(
                    partition,
                    Chain { bets_left: c.bets_left - 1, ..c },
                );
                Event { partition, kind: Kind::Bet, amount: self.rng.gen_range(5..50) }
            }
            Some(c) => {
                self.chains.remove(&partition);
                if c.abandoned {
                    // No withdraw: the prefix stays buffered and this partition starts afresh.
                    self.event_for(partition)
                } else {
                    Event { partition, kind: Kind::Withdraw, amount: self.rng.gen_range(40..450) }
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd web && cargo test -p bench-core gen`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add web/bench-core/src/gen.rs
git commit -m "feat(bench-core): fraud chain generation with skew and abandonment"
```

---

### Task 4: Rows and the emit-SQL sink

**Files:**
- Modify: `web/bench-core/src/sink.rs`
- Create: `web/bench-core/tests/golden_sql.rs`, `web/bench-core/tests/golden/bulk_seed42.sql`

**Interfaces:**
- Consumes: `gen::{Event, Kind, Config, Generator}`, `pace::Pacer`.
- Produces:
  - `pub enum Ts { Tick(i64), Wall(time::OffsetDateTime) }`
  - `pub struct Row { pub partition: i32, pub ts: Ts, pub kind: Kind, pub amount: i32, pub payload: Vec<String> }`
  - `pub trait Sink { fn write(&mut self, rows: &[Row]) -> anyhow::Result<()>; fn finish(&mut self) -> anyhow::Result<()>; }`
  - `pub struct EmitSql<W: std::io::Write>`, `EmitSql::new(w: W, table: String, payload_cols: usize) -> Self`
  - `pub fn column_list(payload_cols: usize) -> String`

- [ ] **Step 1: Write the failing test**

Create `web/bench-core/tests/golden_sql.rs`:

```rust
use bench_core::gen::{Config, Generator, Kind};
use bench_core::sink::{column_list, EmitSql, Row, Sink, Ts};

/// The column list must be explicit. The realtime table carries a generated `proctime()` column
/// (`ingest_ts`), so a positional INSERT does not line up with the table shape.
#[test]
fn column_list_is_explicit_and_includes_payload() {
    assert_eq!(column_list(0), "(id, ts, kind, amount)");
    assert_eq!(column_list(2), "(id, ts, kind, amount, p0, p1)");
}

#[test]
fn emitted_sql_matches_the_golden_file() {
    let cfg = Config { rows: 20, partitions: 5, seed: 42, ..Config::default() };
    let mut g = Generator::new(cfg.clone()).unwrap();

    let mut out = Vec::new();
    {
        let mut sink = EmitSql::new(&mut out, "t_perf".to_string(), 0);
        let rows: Vec<Row> = (0..cfg.rows)
            .map(|i| {
                let e = g.next_event();
                Row {
                    partition: e.partition,
                    ts: Ts::Tick(10 + i as i64),
                    kind: e.kind,
                    amount: e.amount,
                    payload: vec![],
                }
            })
            .collect();
        sink.write(&rows).unwrap();
        sink.finish().unwrap();
    }

    let actual = String::from_utf8(out).unwrap();
    let expected = include_str!("golden/bulk_seed42.sql");
    assert_eq!(actual, expected, "emitted SQL drifted from the golden file");
}

#[test]
fn kinds_are_quoted_as_sql_string_literals() {
    let mut out = Vec::new();
    {
        let mut sink = EmitSql::new(&mut out, "t".to_string(), 0);
        sink.write(&[Row {
            partition: 1,
            ts: Ts::Tick(10),
            kind: Kind::Withdraw,
            amount: 90,
            payload: vec![],
        }])
        .unwrap();
    }
    let sql = String::from_utf8(out).unwrap();
    assert!(sql.contains("(1, 10, 'withdraw', 90)"), "got: {sql}");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd web && cargo test -p bench-core --test golden_sql`
Expected: FAIL — unresolved import `bench_core::sink::EmitSql`.

- [ ] **Step 3: Write the implementation**

Prepend to `web/bench-core/src/sink.rs`:

```rust
//! Where generated rows go.
//!
//! The generator emits typed `Row`s; only this module turns them into SQL. `Direct` binds them as
//! parameters against a real connection, `EmitSql` formats them for inspection. Keeping the
//! generator ignorant of SQL text removes a class of quoting bugs by construction.

use crate::gen::Kind;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum Ts {
    /// Bulk mode: integer ticks. Deterministic, and WITHIN bounds are expressed in ticks.
    Tick(i64),
    /// Realtime mode: wall clock, taken at insert time.
    Wall(time::OffsetDateTime),
}

#[derive(Debug, Clone)]
pub struct Row {
    pub partition: i32,
    pub ts: Ts,
    pub kind: Kind,
    pub amount: i32,
    pub payload: Vec<String>,
}

/// Explicit column list. Required because the realtime table has a generated `ingest_ts` column,
/// so positional inserts do not line up.
pub fn column_list(payload_cols: usize) -> String {
    let mut s = String::from("(id, ts, kind, amount");
    for i in 0..payload_cols {
        s.push_str(&format!(", p{i}"));
    }
    s.push(')');
    s
}

pub trait Sink {
    fn write(&mut self, rows: &[Row]) -> anyhow::Result<()>;
    fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct EmitSql<W: Write> {
    out: W,
    table: String,
    columns: String,
}

impl<W: Write> EmitSql<W> {
    pub fn new(out: W, table: String, payload_cols: usize) -> Self {
        let columns = column_list(payload_cols);
        Self { out, table, columns }
    }

    fn ts_literal(ts: &Ts) -> String {
        match ts {
            Ts::Tick(t) => t.to_string(),
            Ts::Wall(t) => {
                let fmt = time::format_description::well_known::Rfc3339;
                format!("'{}'", t.format(&fmt).expect("rfc3339 formatting"))
            }
        }
    }
}

impl<W: Write> Sink for EmitSql<W> {
    fn write(&mut self, rows: &[Row]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        write!(self.out, "insert into {} {} values ", self.table, self.columns)?;
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
                write!(self.out, ", ")?;
            }
            write!(
                self.out,
                "({}, {}, '{}', {}",
                r.partition,
                Self::ts_literal(&r.ts),
                r.kind.as_str(),
                r.amount
            )?;
            for p in &r.payload {
                write!(self.out, ", '{p}'")?;
            }
            write!(self.out, ")")?;
        }
        writeln!(self.out, ";")?;
        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.out.flush()?;
        Ok(())
    }
}
```

- [ ] **Step 4: Generate the golden file, then read it before trusting it**

```bash
cd web
mkdir -p bench-core/tests/golden
cargo test -p bench-core --test golden_sql 2>&1 | head -30
```

The golden test fails because the file does not exist yet. Create it by running a one-off:

```bash
cd web && cat > /tmp/gen_golden.rs <<'EOF'
// scratch: not committed
EOF
cargo test -p bench-core --test golden_sql -- --nocapture 2>&1 | head -5
```

Instead of scripting it, write the file by capturing the assertion's `actual` value: temporarily
change `assert_eq!(actual, expected, ...)` to `std::fs::write("tests/golden/bulk_seed42.sql", &actual).unwrap();`,
run the test once, then restore the assertion.

**Read the generated file before committing it.** It must contain exactly one `insert into t_perf (id, ts, kind, amount) values ...;` line with 20 tuples, ticks 10..29, kinds drawn from `deposit`/`bet`/`withdraw`, and no `p0` columns. A golden file nobody read is not a test.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd web && cargo test -p bench-core --test golden_sql`
Expected: PASS, 3 tests.

- [ ] **Step 6: Commit**

```bash
git add web/bench-core/src/sink.rs web/bench-core/tests/golden_sql.rs web/bench-core/tests/golden/bulk_seed42.sql
git commit -m "feat(bench-core): typed rows and the emit-SQL sink"
```

---

### Task 5: Direct connection sink

**Files:**
- Modify: `web/bench-core/src/sink.rs`
- Create: `web/bench-core/tests/integration.rs`

**Interfaces:**
- Consumes: `Row`, `Sink`, `column_list`.
- Produces: `pub struct Direct`, `Direct::connect(url: &str, table: String, payload_cols: usize) -> anyhow::Result<Direct>` (async), `Direct::write_async(&mut self, rows: &[Row]) -> anyhow::Result<()>`, `Direct::client(&self) -> &tokio_postgres::Client`.

Note: `Sink::write` is synchronous, so `Direct` does not implement `Sink`; the CLI and web server call `write_async` directly. This keeps `EmitSql` usable from sync contexts without dragging in an async runtime.

- [ ] **Step 1: Write the failing integration test**

Create `web/bench-core/tests/integration.rs`:

```rust
//! Live-cluster tests. Ignored by default so `cargo test` stays offline.
//! Run with a cluster up:  cargo test -p bench-core -- --ignored

use bench_core::gen::Kind;
use bench_core::sink::{Direct, Row, Ts};

const URL: &str = "postgres://root@127.0.0.1:4566/dev";

#[tokio::test]
#[ignore]
async fn direct_sink_inserts_rows() {
    let mut d = Direct::connect(URL, "t_direct_test".to_string(), 0).await.unwrap();
    d.client()
        .batch_execute(
            "set rw_implicit_flush to true;
             drop table if exists t_direct_test;
             create table t_direct_test (id int, ts int, kind varchar, amount int) append only;",
        )
        .await
        .unwrap();

    let rows: Vec<Row> = (0..3)
        .map(|i| Row {
            partition: 7,
            ts: Ts::Tick(10 + i),
            kind: Kind::Bet,
            amount: 42,
            payload: vec![],
        })
        .collect();
    d.write_async(&rows).await.unwrap();

    let got: i64 = d
        .client()
        .query_one("select count(*) from t_direct_test", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(got, 3);

    d.client().batch_execute("drop table t_direct_test;").await.unwrap();
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd web && cargo test -p bench-core --test integration -- --ignored`
Expected: FAIL — unresolved import `bench_core::sink::Direct`.

- [ ] **Step 3: Write the implementation**

Append to `web/bench-core/src/sink.rs`:

```rust
/// A real connection. Rows are bound as parameters rather than formatted into SQL text.
pub struct Direct {
    client: tokio_postgres::Client,
    table: String,
    columns: String,
    payload_cols: usize,
}

impl Direct {
    pub async fn connect(
        url: &str,
        table: String,
        payload_cols: usize,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        // The connection future drives the protocol and must be polled for the client to work.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("bench: connection closed: {e}");
            }
        });
        let columns = column_list(payload_cols);
        Ok(Self { client, table, columns, payload_cols })
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub async fn write_async(&mut self, rows: &[Row]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let per_row = 4 + self.payload_cols;
        let mut sql = format!("insert into {} {} values ", self.table, self.columns);
        for i in 0..rows.len() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for c in 0..per_row {
                if c > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("${}", i * per_row + c + 1));
            }
            sql.push(')');
        }

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            Vec::with_capacity(rows.len() * per_row);
        for r in rows {
            params.push(Box::new(r.partition));
            match r.ts {
                Ts::Tick(t) => params.push(Box::new(t as i32)),
                Ts::Wall(t) => params.push(Box::new(t)),
            }
            params.push(Box::new(r.kind.as_str().to_string()));
            params.push(Box::new(r.amount));
            for p in &r.payload {
                params.push(Box::new(p.clone()));
            }
        }
        let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

        self.client.execute(&sql, &refs).await?;
        Ok(())
    }
}
```

- [ ] **Step 4: Run the test with a cluster up**

```bash
cd /home/dahankzter/projects/risingwave-tests && make up
cd web && cargo test -p bench-core --test integration -- --ignored
```

Expected: PASS, 1 test.

- [ ] **Step 5: Verify the offline suite is still offline**

Run: `cd web && cargo test -p bench-core`
Expected: PASS, and the integration test reported as ignored, not failed.

- [ ] **Step 6: Commit**

```bash
git add web/bench-core/src/sink.rs web/bench-core/tests/integration.rs
git commit -m "feat(bench-core): direct connection sink with bound parameters"
```

---

### Task 6: Pipeline setup and sealing

The defect this preserves the fix for: `flush` returns before the materialized view has caught up, and a far-future sentinel delivered inside that window discards the rows still in flight. Measured on the rig: 3917 matches immediately after the final flush, 10624 five seconds later with nothing else inserted; an inline sentinel froze it at 3917 permanently, losing about 63% of matches.

**Files:**
- Modify: `web/bench-core/src/pipeline.rs`
- Modify: `web/bench-core/tests/integration.rs`

**Interfaces:**
- Consumes: `tokio_postgres::Client`.
- Produces:
  - `pub struct SealConfig { pub table: String, pub mv: String, pub sentinel_partition: i32, pub stable_polls: u32, pub poll: std::time::Duration, pub max_polls: u32 }` with `Default`
  - `pub async fn run_sql_file(client: &tokio_postgres::Client, path: &std::path::Path) -> anyhow::Result<()>`
  - `pub async fn seal(client: &tokio_postgres::Client, cfg: &SealConfig) -> anyhow::Result<SealOutcome>`
  - `pub struct SealOutcome { pub settled_before: i64, pub settled_after: i64 }`

- [ ] **Step 1: Write the failing integration test**

Append to `web/bench-core/tests/integration.rs`:

```rust
use bench_core::pipeline::{seal, SealConfig};

/// Sealing must wait for the pipeline to drain before advancing the watermark. If it does not,
/// the far-future sentinel discards the rows still in flight and the match count freezes low.
#[tokio::test]
#[ignore]
async fn seal_waits_for_the_pipeline_to_drain() {
    let d = bench_core::sink::Direct::connect(URL, "t_seal_test".to_string(), 0)
        .await
        .unwrap();
    let c = d.client();
    c.batch_execute(
        "set rw_implicit_flush to true;
         drop materialized view if exists mv_seal_test;
         drop table if exists t_seal_test;
         create table t_seal_test (id int, ts int, kind varchar, amount int,
           watermark for ts as ts - 10) append only;
         create materialized view mv_seal_test as
         select * from t_seal_test match_recognize (
           partition by id order by ts
           measures count(*) as chain_len
           one row per match after match skip past last row
           pattern (d b w) within 5000
           define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw');",
    )
    .await
    .unwrap();

    // 100 complete chains, no sentinel.
    let mut sql = String::from("insert into t_seal_test (id, ts, kind, amount) values ");
    for i in 0..100i32 {
        if i > 0 {
            sql.push_str(", ");
        }
        let base = 10 + i * 10;
        sql.push_str(&format!(
            "({i}, {base}, 'deposit', 100), ({i}, {}, 'bet', 10), ({i}, {}, 'withdraw', 90)",
            base + 1,
            base + 2
        ));
    }
    c.batch_execute(&sql).await.unwrap();

    let cfg = SealConfig { table: "t_seal_test".into(), mv: "mv_seal_test".into(), ..Default::default() };
    let outcome = seal(c, &cfg).await.unwrap();

    assert_eq!(outcome.settled_after, 100, "every complete chain must be released by the seal");

    c.batch_execute("drop materialized view mv_seal_test; drop table t_seal_test;")
        .await
        .unwrap();
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd web && cargo test -p bench-core --test integration seal -- --ignored`
Expected: FAIL — unresolved import `bench_core::pipeline::seal`.

- [ ] **Step 3: Write the implementation**

Prepend to `web/bench-core/src/pipeline.rs`:

```rust
//! Pipeline lifecycle: running setup SQL, and sealing a finished bulk feed.
//!
//! Sealing is a separate phase, not a row appended to the feed, because `flush` returns before the
//! materialized view has caught up. Measured on the rig with a 200k-row feed: 3917 matches
//! immediately after the final flush, 10624 five seconds later with nothing further inserted. A
//! far-future sentinel delivered inside that window froze the count at 3917 permanently — the
//! watermark discards the rows still in flight rather than matching them, and they never come
//! back. Interposing a flush before the sentinel does not help, because flush is precisely what
//! does not wait. So: settle, seal, settle.

use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SealConfig {
    pub table: String,
    pub mv: String,
    pub sentinel_partition: i32,
    /// Consecutive unchanged reads before the count is considered settled. This is a heuristic,
    /// not a guarantee — raise it on a slower box or a heavier feed.
    pub stable_polls: u32,
    pub poll: Duration,
    pub max_polls: u32,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            table: "t_perf".into(),
            mv: "mv_perf".into(),
            sentinel_partition: 0,
            stable_polls: 5,
            poll: Duration::from_secs(1),
            max_polls: 600,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SealOutcome {
    pub settled_before: i64,
    pub settled_after: i64,
}

pub async fn run_sql_file(
    client: &tokio_postgres::Client,
    path: &Path,
) -> anyhow::Result<()> {
    let sql = std::fs::read_to_string(path)?;
    // psql meta-commands (\echo) are not SQL; strip them so scenario files run unmodified.
    let cleaned: String = sql
        .lines()
        .filter(|l| !l.trim_start().starts_with('\\'))
        .collect::<Vec<_>>()
        .join("\n");
    client.batch_execute(&cleaned).await?;
    Ok(())
}

async fn count(client: &tokio_postgres::Client, mv: &str) -> anyhow::Result<i64> {
    let row = client.query_one(&format!("select count(*) from {mv}"), &[]).await?;
    Ok(row.get(0))
}

async fn settle(
    client: &tokio_postgres::Client,
    cfg: &SealConfig,
    what: &str,
) -> anyhow::Result<i64> {
    let mut last = -1i64;
    let mut stable = 0u32;
    let mut polls = 0u32;
    loop {
        let n = count(client, &cfg.mv).await?;
        if n == last {
            stable += 1;
            if stable >= cfg.stable_polls {
                return Ok(n);
            }
        } else {
            stable = 0;
        }
        last = n;
        polls += 1;
        if polls >= cfg.max_polls {
            anyhow::bail!(
                "seal: {what} still moving after {}s (at {n} matches)",
                cfg.max_polls as u64 * cfg.poll.as_secs()
            );
        }
        tokio::time::sleep(cfg.poll).await;
    }
}

pub async fn seal(
    client: &tokio_postgres::Client,
    cfg: &SealConfig,
) -> anyhow::Result<SealOutcome> {
    let settled_before = settle(client, cfg, "feed").await?;

    let max_ts: i32 = client
        .query_one(&format!("select coalesce(max(ts), 0) from {}", cfg.table), &[])
        .await?
        .get(0);

    client
        .batch_execute(&format!(
            "set rw_implicit_flush to true;
             insert into {} (id, ts, kind, amount) values ({}, {}, 'noop', 0);",
            cfg.table,
            cfg.sentinel_partition,
            max_ts as i64 + 1_000_000
        ))
        .await?;

    let settled_after = settle(client, cfg, "seal").await?;
    Ok(SealOutcome { settled_before, settled_after })
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd web && cargo test -p bench-core --test integration seal -- --ignored`
Expected: PASS — `settled_after == 100`.

- [ ] **Step 5: Commit**

```bash
git add web/bench-core/src/pipeline.rs web/bench-core/tests/integration.rs
git commit -m "feat(bench-core): pipeline setup and settle-then-seal"
```

---

### Task 7: CLI, make target migration, and retiring the Python

**Files:**
- Modify: `web/bench/src/main.rs`, `web/bench/Cargo.toml`
- Modify: `Makefile:112-142` (the `GEN`, `load`, `rt-load` block)
- Delete: `datagen/gen.py`, `datagen/seal.sh`
- Modify: `README.md` (the `datagen/` bullets in Layout, and the "Sealing a bulk feed" section)

**Interfaces:**
- Consumes: everything from tasks 2-6.
- Produces: a `bench` binary with subcommands `load` and `seal`.

- [ ] **Step 1: Write the CLI**

`web/bench/src/main.rs`:

```rust
//! Workload driver for the MATCH_RECOGNIZE bench. Replaces datagen/gen.py.
//!
//! Connects directly by default; `--emit-sql` prints the stream instead, for inspection.

use anyhow::Result;
use bench_core::gen::{Config, Generator};
use bench_core::pace::Pacer;
use bench_core::pipeline::{seal, SealConfig};
use bench_core::sink::{Direct, EmitSql, Row, Sink, Ts};
use clap::{Parser, Subcommand, ValueEnum};
use std::time::Instant;

#[derive(Parser)]
#[command(about = "MATCH_RECOGNIZE bench workload driver")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://root@127.0.0.1:4566/dev")]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Mode {
    Bulk,
    Realtime,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate and insert a workload.
    Load {
        #[arg(long)]
        table: String,
        #[arg(long, value_enum, default_value = "bulk")]
        mode: Mode,
        #[arg(long, default_value_t = 100_000)]
        rows: u64,
        #[arg(long, default_value_t = 1_000)]
        partitions: i32,
        #[arg(long, default_value_t = 500)]
        batch: usize,
        #[arg(long, default_value_t = 0)]
        hot_count: i32,
        #[arg(long, default_value_t = 0.5)]
        hot_share: f64,
        #[arg(long, default_value_t = 0.2)]
        abandon_prob: f64,
        #[arg(long, default_value_t = 1)]
        ties: u32,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value_t = 2000.0)]
        rate: f64,
        #[arg(long, default_value_t = 0)]
        payload_cols: usize,
        #[arg(long, default_value_t = 32)]
        payload_bytes: usize,
        /// Print SQL instead of connecting.
        #[arg(long)]
        emit_sql: bool,
    },
    /// Settle, advance the watermark, settle again.
    Seal {
        #[arg(long, default_value = "t_perf")]
        table: String,
        #[arg(long, default_value = "mv_perf")]
        mv: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Seal { table, mv } => {
            let d = Direct::connect(&cli.url, table.clone(), 0).await?;
            let cfg = SealConfig { table, mv, ..Default::default() };
            let out = seal(d.client(), &cfg).await?;
            eprintln!(
                "seal: feed settled at {} matches, seal settled at {}",
                out.settled_before, out.settled_after
            );
            Ok(())
        }
        Cmd::Load {
            table, mode, rows, partitions, batch, hot_count, hot_share, abandon_prob,
            ties, seed, rate, payload_cols, payload_bytes, emit_sql,
        } => {
            let cfg = Config {
                rows, partitions, hot_count, hot_share, abandon_prob, ties, seed,
                payload_cols, payload_bytes, ..Config::default()
            };
            let mut g = Generator::new(cfg.clone())?;
            let payload = vec!["x".repeat(payload_bytes); payload_cols];

            let realtime = mode == Mode::Realtime;
            let pacer = Pacer::new(Instant::now(), rate);

            let mut direct = if emit_sql {
                None
            } else {
                Some(Direct::connect(&cli.url, table.clone(), payload_cols).await?)
            };
            if let Some(d) = &direct {
                // Realtime wants rows visible as they are produced; bulk does not, and paying a
                // barrier per INSERT there caps ingest near 9k rows/s instead of ~92k.
                let flush = if realtime { "true" } else { "false" };
                d.client()
                    .batch_execute(&format!("set rw_implicit_flush to {flush};"))
                    .await?;
            }
            // Only take the stdout lock when actually emitting; holding it for the whole run in
            // direct mode would block the progress messages below.
            let stdout = std::io::stdout();
            let mut emit = if emit_sql {
                let flush = if realtime { "true" } else { "false" };
                println!("set rw_implicit_flush to {flush};");
                Some(EmitSql::new(stdout.lock(), table.clone(), payload_cols))
            } else {
                None
            };

            let mut buf: Vec<Row> = Vec::with_capacity(batch);
            let mut tick = 10i64;
            let mut in_group = 0u32;

            for i in 0..rows {
                if realtime {
                    tokio::time::sleep_until(pacer.due(i).into()).await;
                }
                let e = g.next_event();
                let ts = if realtime {
                    // Taken now, from the real clock — never from an accumulated schedule.
                    Ts::Wall(time::OffsetDateTime::now_utc())
                } else {
                    let t = tick;
                    in_group += 1;
                    if in_group >= ties {
                        in_group = 0;
                        tick += 1;
                    }
                    Ts::Tick(t)
                };
                buf.push(Row {
                    partition: e.partition,
                    ts,
                    kind: e.kind,
                    amount: e.amount,
                    payload: payload.clone(),
                });
                if buf.len() >= batch {
                    match (&mut direct, &mut emit) {
                        (Some(d), _) => d.write_async(&buf).await?,
                        (None, Some(e)) => e.write(&buf)?,
                        (None, None) => unreachable!("one sink is always configured"),
                    }
                    buf.clear();
                }
            }
            if !buf.is_empty() {
                match (&mut direct, &mut emit) {
                    (Some(d), _) => d.write_async(&buf).await?,
                    (None, Some(e)) => e.write(&buf)?,
                    (None, None) => unreachable!("one sink is always configured"),
                }
            }
            if let Some(e) = &mut emit {
                e.finish()?;
            }
            eprintln!(
                "-- emitted {rows} rows over {partitions} partitions (hot: {hot_count} @ {hot_share}), \
                 {} chains left open",
                g.open_chains()
            );
            Ok(())
        }
    }
}
```

- [ ] **Step 2: Build and check the CLI runs**

```bash
cd web && cargo build --release
./target/release/bench load --table t_perf --rows 20 --partitions 5 --emit-sql | head -3
```

Expected: a `set rw_implicit_flush to false;` line followed by one `insert into t_perf (id, ts, kind, amount) values ...;`.

- [ ] **Step 3: Run the structural parity check against the Python**

Byte-identical output is not the gate — that would require reimplementing CPython's MT19937 and
its `_randbelow` rejection loop, permanently coupling this crate to CPython internals. Check
structure and behaviour instead:

```bash
cd /home/dahankzter/projects/risingwave-tests
python3 datagen/gen.py --table t_perf --rows 2000 --partitions 100 --seed 42 2>/dev/null > /tmp/py.sql
web/target/release/bench load --table t_perf --rows 2000 --partitions 100 --seed 42 --emit-sql > /tmp/rs.sql

# same shape: statement count, column list, row count, kind vocabulary
echo "py stmts: $(grep -c '^insert' /tmp/py.sql)   rs stmts: $(grep -c '^insert' /tmp/rs.sql)"
echo "py cols : $(grep -o '(id, ts, kind, amount)' /tmp/py.sql | head -1)"
echo "rs cols : $(grep -o '(id, ts, kind, amount)' /tmp/rs.sql | head -1)"
for f in /tmp/py.sql /tmp/rs.sql; do
  echo "$f rows: $(grep -o "'\(deposit\|bet\|withdraw\)'" $f | wc -l)"
  echo "$f kinds: $(grep -o "'\(deposit\|bet\|withdraw\)'" $f | sort | uniq -c | tr '\n' ' ')"
done
```

Expected: identical statement counts, identical column lists, identical row counts, and kind
distributions within a few percent of each other. Chain grammar is already asserted by the Task 3
unit tests.

- [ ] **Step 4: Compare end-to-end match counts on a live cluster**

This is the check that matters — same workload shape must produce the same number of matches.

```bash
cd /home/dahankzter/projects/risingwave-tests && make up
make load-setup
python3 datagen/gen.py --table t_perf --rows 200000 --partitions 100000 --hot-count 100 \
  --hot-share 0.3 --abandon-prob 0.25 --seed 42 2>/dev/null | psql -h 127.0.0.1 -p 4566 -d dev -U root -q
TABLE=t_perf MV=mv_perf ./datagen/seal.sh
psql -h 127.0.0.1 -p 4566 -d dev -U root -tAc "select count(*) from mv_perf;"   # record this

make load-setup
web/target/release/bench load --table t_perf --rows 200000 --partitions 100000 --hot-count 100 \
  --hot-share 0.3 --abandon-prob 0.25 --seed 42
web/target/release/bench seal --table t_perf --mv mv_perf
psql -h 127.0.0.1 -p 4566 -d dev -U root -tAc "select count(*) from mv_perf;"
```

Expected: the two counts within 5% of each other (different RNG, same shape). The Python run gave
10626 on this rig at these settings. **If they differ by more than 5%, stop and investigate before
deleting anything** — that is a behavioural difference, not RNG noise.

- [ ] **Step 5: Migrate the make targets**

Replace `Makefile` lines 112-142 (the `GEN`/`GENARGS`/`load`/`rt-load` block) with:

```make
PROFILE ?= small
ROWS    ?=
BENCH    = web/target/release/bench

$(BENCH):
	cd web && cargo build --release

ifeq ($(PROFILE),small)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),100000) --abandon-prob 0.2
else ifeq ($(PROFILE),fraud)
GENARGS = --table t_perf --partitions 100000 --rows $(or $(ROWS),1000000) --hot-count 100 --hot-share 0.3 --abandon-prob 0.25 --ties 2
else ifeq ($(PROFILE),hotspot)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),500000) --hot-count 1 --hot-share 0.9 --abandon-prob 0.3
endif

load-setup:
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_bulk.sql

# Feed, then seal. The seal is a separate step because a far-future sentinel delivered while the
# pipeline is still draining discards the in-flight rows instead of matching them.
load: $(BENCH)
	$(BENCH) load $(GENARGS)
	@$(BENCH) seal --table t_perf --mv mv_perf

rt-setup:
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_realtime.sql

rt-load: $(BENCH)
	$(BENCH) load --table t_rt --mode realtime --rate $(or $(RATE),2000) \
		--rows $(or $(ROWS),200000) --partitions 5000 --hot-count 5 --hot-share 0.4
```

Also update `latency/bench.sh` line 38-40, replacing the `python3 datagen/gen.py ... | psql` invocation with:

```bash
web/target/release/bench load --table t_rt --mode realtime --rate "$RATE" --rows "$ROWS" \
  --partitions "$PARTITIONS" --hot-count 5 --hot-share 0.4 &
```

- [ ] **Step 6: Verify every migrated target**

```bash
cd /home/dahankzter/projects/risingwave-tests
make smoke                              # must still be 5 scenario(s) ok
make load-setup && make load PROFILE=small ROWS=20000
make rt-setup && make bench ROUNDS=4
```

Expected: `make smoke` green; `load` prints seal settling lines; `bench` prints both measurements
with the probe completing all rounds (no TIMEOUT).

- [ ] **Step 7: Delete the Python and update the README**

```bash
git rm datagen/gen.py datagen/seal.sh
```

In `README.md`, replace the two `datagen/` bullets in the Layout section with:

```markdown
- `web/` — the Rust workspace. `bench-core` holds workload generation, pacing, the sink
  abstraction and the seal logic; `bench` is the CLI that `make load` and `make rt-load` drive.
  Pacing lives in `bench-core/src/pace.rs` and is unit-tested without a database.
```

In the "Sealing a bulk feed" section, replace `datagen/seal.sh` with `bench seal` and
`gen.py` with `bench load`, leaving the explanation of *why* sealing is separate unchanged.

- [ ] **Step 8: Final verification**

```bash
cd web && cargo test && cargo test -- --ignored
cd .. && make smoke && make bench ROUNDS=4
grep -rn "gen\.py\|seal\.sh" --include="*.md" --include="Makefile" --include="*.sh" --include="*.sql" . | grep -v "^./docs/superpowers/"
```

Expected: all tests pass, smoke green, bench completes, and the final grep returns nothing (docs
under `docs/superpowers/` legitimately reference the old names as history).

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: replace the Python generator with the Rust bench CLI"
```

---

## Self-Review

**Spec coverage.** This plan covers the spec's generator half: workspace layout (task 1), pacing
(2), chain generation (3), the sink abstraction with both implementations (4, 5), pipeline setup and
seal (6), CLI plus make migration plus retiring the Python (7). Unit / golden / integration test
tiers all appear. The spec's `measure.rs` is deliberately deferred to plan 2 — nothing in the CLI
path consumes percentiles, and building it now would be untested scaffolding.

**Deviation from the spec, agreed with the repo owner.** The spec's parity gate said Rust
`--emit-sql` must be diff-clean against the Python. That would require reimplementing CPython's
MT19937 and its `_randbelow` rejection loop, permanently coupling this crate to CPython internals
for a one-time migration check, and has been ruled out. Task 7 uses a structural gate instead
(statement count, column list, row count, kind distribution) plus an end-to-end match-count
comparison within 5%, which is what actually establishes that the port changed nothing observable.

`ChaCha8Rng` is named directly rather than using `StdRng`: they are the same algorithm today, but
`rand` reserves the right to change `StdRng` between releases, which would break the golden-SQL
test on a routine dependency bump.

**Not covered here, belongs to plan 2:** the Axum server, subscription-cursor streaming, the two-tab
UI, CPU pinning, and publishing port 1222.
