# risingwave-tests

A test bench for the MATCH_RECOGNIZE contribution to RisingWave
([risingwavelabs/risingwave#26584](https://github.com/risingwavelabs/risingwave/pull/26584)):
scenario scripts, adversarial patterns, and performance probes run against published
`risingwave-mr` images.

## Usage

`make info` prints a getting-started walkthrough — the three paths (demo it, check a build, measure
it) and what to do when something looks wrong. `make help` lists every target.

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

- `…--mr--bee0fbd--feat-match-recognize-v2` — current PR head: all six review rounds (incl. the
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

Cluster up/down, pipeline rebuild, load start/stop with a live rate slider, the alert feed, rate
gauges, a latency chart, and a **details** tab with four panels: latency and throughput, the
operator metrics, pipeline state, and the run environment.

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

Measured on the Linux rig (native amd64, 64 cores) against `bee0fbd`:

| | |
|---|---|
| bulk ingest, fraud profile | ~92k rows/s (200k rows, 100k partitions, 100 hot @ 30%, 25% abandoned) |
| decision latency [1] client probe | p50 6318 ms, p95 6462 ms, min 5996 ms (8 rounds @ 2k rows/s) |
| decision latency [2] server-side | p50 6048 ms, p95 6413 ms, min 5104 ms (39801 matches @ 2k rows/s) |

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
