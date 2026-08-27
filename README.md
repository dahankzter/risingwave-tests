# risingwave-tests

A test bench for the MATCH_RECOGNIZE contribution to RisingWave
([risingwavelabs/risingwave#26584](https://github.com/risingwavelabs/risingwave/pull/26584)):
scenario scripts, adversarial patterns, and performance probes run against published
`risingwave-mr` images.

## Usage

`make info` prints a getting-started walkthrough — the three paths (demo it, check a build, measure
it) and what to do when something looks wrong. `make help` lists every target.

`make doctor` checks the four prerequisites — podman, Rust (against the version `web/Cargo.toml`
itself declares), `psql` and `python3` — and reports all of them rather than stopping at the first.
Every target that needs a tool depends on its check, so a missing one produces install instructions
for the package manager on that machine (brew, dnf, pacman, apt, zypper, apk) instead of
`podman: command not found`. On macOS it also covers the two traps worth knowing about: podman needs
a VM (`podman machine init && podman machine start`), and Homebrew keeps `libpq` keg-only so `psql`
never lands on `PATH`.

```sh
make up                # start a single-node RisingWave from $RW_IMAGE (podman run)
make psql              # interactive session on :4566
make run S=scenarios/semantics/preference_supersession.sql
make smoke             # run every scenario and assert against expected/
make down              # stop; make clean also removes the data volume
```

Everything shells out to `psql` from `PATH`. Homebrew's libpq is keg-only and does not land on
`PATH`, so on a Mac set it once in your environment:

```sh
export PSQL=/opt/homebrew/opt/libpq/bin/psql
```

`make up` needs nothing but podman. A `compose.yaml` is also provided for anyone who prefers
compose — but note that `podman compose` ships **no** provider of its own and fails with
`looking up compose provider failed` unless docker-compose or podman-compose is installed:

```sh
podman compose up -d      # or: docker compose up -d
podman compose down       # add -v to drop the data volume too
```

The two are interchangeable: same container name, same volume, same pinned image, so `make psql`,
`make smoke`, `make logs`, `make down` and `make clean` all work against a compose-started
cluster. Keep `RW_IMAGE` in step between the two files when repinning.

The image is pinned in the `RW_IMAGE` variable in the Makefile; override per
invocation to compare versions:

```sh
make up RW_IMAGE=ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--231d979--feat-match-recognize-parser
```

## Images

Published at `ghcr.io/dahankzter/risingwave` with tags encoding `<rw-version>--mr--<sha>--<branch>`:

- `…--mr--4afdc2a--feat-match-recognize-v2` — **current PR head**, and what the Makefile and
  `compose.yaml` pin. Note the version prefix is `v3.2.0-alpha`, not `v3.1.0-alpha`: the branch was
  rebased onto a main that bumped the workspace version. Adds, over `0897a4d`: the NFA walk is
  iterative and remembers verdicts, so a long match converges instead of re-deriving; an overflowed
  `WITHIN` deadline reads as a window that never closes; the MATCH_RECOGNIZE grammar words are
  contextual and match only when unquoted; dead `DEFINE` symbols and foreign `PREV` anchors are
  rejected at bind time, along with four further binder holes; walk recursion is bounded,
  epsilon-transitions are metered, and a data error no longer kills the executor; and the scan
  budget is bounded on the data path.
  Verified on the Linux rig: `make smoke` 5/5 against the recorded `expected/` with no re-blessing,
  from a cleaned data volume.
- `…--mr--0897a4d--feat-match-recognize-v2` — the previous head. Adds, over `bee0fbd`: a starved
  visit is allowed to emit and a held match's rows are never pruned; the scan cursor no longer
  advances past a budget-aborted start; the whole-pattern NFA state cap is re-checked on decode; a `WITHIN` bound that widens the `ORDER BY`
  type is rejected at bind time; and CI runs the recovery suite. The branch was rebased, so this is
  not a descendant of `bee0fbd`.
  Verified on the Linux rig: `make smoke` 5/5 against the recorded `expected/` with no re-blessing,
  so none of the above changed the semantics this bench asserts.
- `…--mr--bee0fbd--feat-match-recognize-v2` — the head before that: all six review rounds (incl. the
  budget-truncation and calendar-interval WITHIN fixes, the `_match_id` epoch floor, and the
  allocation/hot-path perf batch). Proto wire format changed vs older tags — do not mix a frontend
  and compute node from different tags.
- `…--mr--0bc2acb--feat-match-recognize-v2` / `…--mr--9e0f3f9--…` — earlier v2 heads.
- `…--mr--5e4ef85--feat-match-recognize-v2` — same architecture, before the metrics commit (the
  `stream_match_recognize_*` counters are absent in this build).
- `…--mr--231d979--feat-match-recognize-parser` — the earlier EOWC-based architecture (PR #25899),
  useful as a comparison baseline (e.g. the backtracking probe behaves very differently).

The package is public; the images are **linux/amd64 only**. On Apple Silicon the images run emulated (the Makefile pins `--platform linux/amd64`): fine for
smoke/semantics runs, not for performance numbers — run `scenarios/perf/` on the Linux rig.

**Always `make clean` when switching between tags with different wire formats** — the data volume
carries meta/stream-plan state, and a build recovering state persisted by an incompatible tag
aborts at startup (barrier recovery bootstrap crash).

## Layout

- `scenarios/semantics/` — cluster-level checks of matching semantics: preference supersession,
  PERMUTE listing order, `EMIT ON WINDOW CLOSE` clause acceptance. These mirror the upstream SLTs
  but run against a released image rather than a source build.
- `scenarios/adversarial/` — patterns designed to hurt: currently the catastrophic-backtracking
  shape `(a? ×16 b)`, which the failure memo and the scan budget must keep from going exponential.
- `scenarios/perf/` — load setups: bulk throughput (`setup_bulk.sql`), realtime with wall-clock
  timestamps (`setup_realtime.sql`), and hot-partition skew (`hot_partition.sql`).
- `latency/` — decision-latency measurement: `probe.sh` (client-side, polls the MV), `report.sql`
  (server-side, from the proctime stamps every match records for itself), and `bench.sh`, which
  runs the pipeline, the traffic and both measurements in one command.
- `web/` — the Rust workspace. `bench-core` holds workload generation, pacing, the sink
  abstraction and the seal logic; `bench` is the CLI that `make load` and `make rt-load` drive.
  Pacing lives in `bench-core/src/pace.rs` and is unit-tested without a database. Dropped from
  the Python generator it replaces, with no equivalent: `--tick-gap`, `--bets-min`, `--bets-max`,
  `--sentinel-partition`. Don't go looking for them.
- `expected/` — recorded scenario output. This is what `make smoke` asserts against.

## Building

The Rust workspace is under `web/` and needs stable Rust 1.95 or newer — nothing in it requires a
nightly or a newer toolchain, deliberately, so a colleague with a stock `rustup` can build the
console. `make test` runs the workspace's unit tests.

## The demo console

```sh
cd web && cargo run --release -p bench-web     # then open http://127.0.0.1:3000
cargo run --release -p bench-web -- --pin      # partition the cores first (see below)
```

Four tabs over the same cluster:

- **live** — the alert feed, rows/s and alerts/s dials, and the latency chart.
- **correctness** — a picker over two groups of scenarios, each showing the prose its own file opens
  with, and its results as tables on the right. *semantics checks* (`scenarios/semantics`) pin down
  one edge of the spec each and clean up after themselves; *demos* (`scenarios/playground`) are
  runnable tours that deliberately leave their tables and views behind, so running one and switching
  to the playground lands you in front of objects worth querying — see
  [`scenarios/playground/README.md`](scenarios/playground/README.md). Each materialized view a check creates also yields a **show graph** toggle:
  the streaming plan it compiles to, as an operator tree with `StreamMatchRecognize` and
  `StreamWatermarkSort` highlighted — the ordering/matching split, visible on the machine rather
  than described. These checks build their own tables and drop them again; they share nothing with
  the load, so running one mid-load disturbs neither.
- **playground** — arbitrary SQL against the same cluster, for the questions a fixed scenario set
  cannot answer. The left column lists your tables, views, sources and sinks (click one to
  `describe` it, and **show data** then reads its first 20 rows) plus `show tables` /
  `show materialized views` / `show sources` / `show sinks` / `show internal tables` shortcuts; the
  right column is an editor (⌘/Ctrl+Enter runs) over the same results renderer the correctness tab
  uses. **show data** is disabled for a sink, which has no rows to read — RisingWave's own message
  for that is a bare "Failed to prepare the statement" with nothing under it, so the button says
  why instead. Statements run one at a time in order, so a failure
  names the statement that failed; every materialized view you create gets the same **show graph**
  plan tree. Note that RisingWave's internal state tables and the pg_catalog compatibility views
  are omitted from the list — `show internal tables` reaches the former when you want them. The list
  is re-read each time you enter the tab, since the cluster changes behind it: a correctness check
  builds its own tables and drops them again, and a check that *fails* leaves them behind (the run
  stops at the failing statement, so its trailing drops never execute) — which is useful, because
  you can then go and poke at the wreckage here. Rerunning that check cleans up first.
- **details** — latency and throughput, the operator metrics, pipeline state, and the run
  environment (which labels its own trustworthiness).

Why a result appears when it does — barrier-gated vs watermark-gated, and the case where a quiet
stream stops emitting altogether — is written up in [`docs/latency-model.md`](docs/latency-model.md),
with a `make latency-model` harness that reproduces its numbers on whatever machine you are on.

The top bar keeps the cluster and load controls: up/down, pipeline rebuild with the watermark
lateness selector, load start/stop with a live rate slider, and the end-to-end timing check.

The top bar also carries two demo levers:

- **lateness** — the watermark tolerance the pipeline is rebuilt with (5s default, or 10/1/0s).
  Changing the dropdown does not change the pipeline: the declaration lives in the table's DDL, so
  it takes a rebuild. The header therefore shows a `watermark` chip read from the *live* table, and
  when the selector disagrees it reads `5s → 1s pending` in amber and the button says
  `rebuild pipeline (1s)` — so an unapplied change cannot quietly misattribute four seconds of
  every latency on screen.
  Since most of the reported latency *is* this number, switching it and rebuilding turns "latency
  is a policy dial, not an engine limit" into something you can show rather than assert. The
  server refuses the rebuild if it cannot find the declaration to rewrite, rather than reporting a
  lateness the pipeline does not actually have.
- **time 3 test alerts** — the client-side latency measurement: drives a few chains of its own through the
  pipeline and times each one from the completing insert to the match being visible in `mv_rt`,
  reporting per-round figures and a p50/p95. It measures the same thing the feed's latency column
  does, but on a chain it controls and through the query a consumer would run — two independent
  measurements that agree are worth more than either alone. While a load is running it suppresses
  its watermark sentinels, because those carry `now()` and would release the load's own matches
  early (on the rig that inflated a feed's apparent speed from 7.2s to 3.4s).
- **correctness check** — run any of the embedded semantics scenarios and read its transcript:
  each query with its column headers and the rows this cluster returned, directly under the
  scenario's own expectation line (`-- expect: …`). The correctness half of a demo, next to the
  throughput half.

The button order matters: **cluster up → rebuild pipeline → start load**. Starting a load before
the pipeline exists is refused with an explanation rather than accepted — a load writing to a
missing table sends nothing, reports no error of its own, and reads on screen as a broken engine.

Two things the console is deliberate about:

- **Percentiles describe the current run.** Starting a load or rebuilding the pipeline resets the
  measurement epoch (server-side `stats_reset`), because accumulating samples across runs put a
  stale p95 two orders of magnitude above p50 — the first thing a reviewer would challenge.
- **The environment panel labels its own trustworthiness.** Emulated (linux/amd64 container on a
  non-x86 host), unpinned, or fewer than 8 cores, and the panel says "shape-check only" with the
  reasons. A screenshot of a laptop run cannot circulate as a measurement.

Operator metrics come from the Prometheus endpoint on port **1260** — where the `single_node`
binary serves them, not the 1222 a multi-node compute node would use (published by `make up`,
`compose.yaml` and the console's podman driver). They are **totals since cluster
start** — the counters survive dropped and recreated pipelines.

### CPU pinning

`--pin` gives the cluster all cores but the last two, keeps those two for the bench process, and
sets `streaming_parallelism` to match the cluster's cpuset — that last part is what buys the
isolation, since RisingWave sizes its thread pools from the core count it detects and would
otherwise spawn workers for cores it cannot use. `--cores-cluster` / `--cores-bench` override the
layout (and imply `--pin`). Off by default: every number recorded here was measured unpinned.

On Linux both halves apply (container cpuset + `sched_setaffinity`). On macOS only the container
cpuset applies — the platform exposes affinity *hints*, not a cpuset — and the details tab says
so rather than implying an isolation it does not have.

## Load & latency

```sh
make load-setup && make load PROFILE=fraud      # 1M rows, 100k partitions, mild skew, 25% open partials
make load PROFILE=hotspot                        # one partition takes 90% of traffic
make bench                                       # whole realtime latency benchmark, both numbers
make bench ROUNDS=20 RATE=5000                   # ... with more probe rounds and heavier traffic
```

Measured on the Linux rig (native amd64, 64 cores, unpinned) against `0897a4d`:

| | |
|---|---|
| bulk ingest, fraud profile | ~81k rows/s (200k rows, 100k partitions, 100 hot @ 30%, 25% abandoned) — the feed alone; `make load` also seals, which polls until the match count stops moving and takes an order of magnitude longer than the feed |
| decision latency [1] client probe | p50 6398 ms, p95 6411 ms, min 6014 ms (8 rounds @ 2k rows/s) |
| decision latency [2] server-side | p50 6248 ms, p95 6252 ms, min 5499 ms (39839 matches @ 2k rows/s) |

The same workload on `bee0fbd` gave ~92k rows/s and p50s of 6318/6048 ms. Do not read the ingest
difference as a regression: 92k was a single timing of a 2.5-second feed, which is short enough
that container and connection warm-up move it several percent, and neither figure was taken with
pinning. The latency figures are within a couple of hundred milliseconds and dominated by the 5s
declared watermark delay either way.

There are two latency measurements because they answer different questions. `make latency` runs a
client probe: insert a chain's completing event, poll the MV until the match shows up. That is
what a consumer polling the MV feels, but it samples only the rounds it drives. `make lat-report`
instead has the cluster measure itself — a `proctime()` column stamps each event on arrival,
MATCH_RECOGNIZE carries that stamp out as a measure, and a sink into a second `proctime()` table
stamps the match on arrival. Every match stores its own delay, so the distribution comes from the
whole workload rather than a handful of synthetic rounds. Both run off one pipeline
(`scenarios/perf/setup_realtime.sql`); `make bench` does the whole sequence.

The two land close together: [2] waits one hop further (across the sink into a table), while [1]
adds a client round trip and 20ms of poll granularity, and those roughly cancel. `make bench` runs
the probe with `SENTINEL=off` so it does not advance the watermark itself — left on, the probe's
`now()` rows release the background traffic's matches early and [2] reads far too low.

Both figures include the 5s watermark delay declared on the realtime table, so the
operator and pipeline account for roughly 1.3s of the probe number; tighten the watermark to trade
late-event tolerance for alert speed. For reference, the same probe on emulated Apple Silicon
reported ~9.4s p50.

### Sealing a bulk feed

`bench load` emits data only. Advancing the watermark is a separate step, because **`flush` returns
before the materialized view has caught up** — measured on a 200k-row feed: 3917 matches
immediately after the final flush, 10624 five seconds later with nothing else inserted. A
far-future sentinel delivered inside that window froze the count at 3917 permanently: the
watermark discards the rows still in flight instead of matching them, losing ~63% of the matches,
and they never come back. Putting a `flush` in front of the sentinel does not help, since `flush`
is precisely what does not wait.

So `bench seal` polls until the match count stops moving, inserts the sentinel, and polls
again. `make load` does both halves. Anything driving a bulk feed by hand must do the same.

Bulk mode also leaves `rw_implicit_flush` off. With it on, every INSERT pays a barrier round trip
and ingest caps near 9k rows/s — that measures barrier latency, not the operator.

## Assertions

Scenario scripts are plain psql SQL files, re-runnable (they drop and recreate their own objects),
with the expected result stated in a comment at each query. Those comments are documentation; the
assertion is `expected/*.out`, recorded from a real run.

```sh
make smoke      # run semantics/ + adversarial/ and diff against expected/
make bless      # re-record expected/ after an intentional change
```

`make smoke` fails on a changed result set, not merely on a SQL error — which is the regression
worth catching when comparing image tags. Review a blessed diff against the in-script comments
before committing it.
