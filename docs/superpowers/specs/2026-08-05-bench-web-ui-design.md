# Bench web UI, and the Rust generator port

Design, 2026-08-05. Status: approved, not yet implemented.

## Purpose

A single web page that makes the MATCH_RECOGNIZE work visible and drivable: fraud chains resolving
into alerts as they happen, with the latency and throughput to back it up. Two audiences at once —
colleagues who want to see streaming pattern matching work, and PR reviewers who want the numbers.
The demo tab serves the first, a details tab behind it serves the second.

Colleagues will run this on Macs to experiment and learn, not to measure. Real measurement happens
on Linux — this rig, and later a large Google Cloud instance for QA. The design must run acceptably
in both places while never presenting a Mac run as a measurement.

The Python generator is replaced by a Rust one in the same change, so there is a single
implementation of the pacing logic that both `make` and the web server drive.

## Why replace `gen.py`

Pacing has produced three separate defects in one day:

1. sleeps emitted as the cumulative target rather than the increment (4000 rows at 2000/s slept
   8.97s instead of 2s);
2. sleeping the exact increment, which ignores INSERT execution time, so event time crept ahead of
   the wall clock until the watermark sat in the future and rows inserted with `now()` were dropped
   as late (probe rounds 6s, 24s, 31s, timeout);
3. the probe advancing the watermark itself while measuring a feed, halving the feed's apparent
   latency (3.4s against 7.2s measured alone).

Each was found by running a benchmark and noticing a number was wrong. With a direct database
connection the mechanism disappears: pacing becomes `sleep_until` on the real clock, and event
timestamps are taken at insert time, so a schedule cannot outrun the clock by construction. What
remains becomes unit-testable without a database.

## Architecture

A cargo workspace under `web/`:

```
web/
├── bench-core/               no Axum, no CLI dependencies
│   ├── gen.rs                chain shaping, skew, abandonment, payload
│   ├── pace.rs               wall-clock pacing; pure, no I/O
│   ├── sink.rs               Direct(tokio-postgres) | EmitSql(io::Write)
│   ├── pipeline.rs           setup/teardown SQL, seal (settle → sentinel → settle)
│   └── measure.rs            latency percentiles, rate windows
├── bench/                    CLI — replaces datagen/gen.py
└── bench-web/                Axum + embedded static assets
    ├── api.rs                POST controls
    ├── stream.rs             subscription cursor → broadcast channel
    ├── podman.rs             cluster up/down/clean
    └── static/               index.html, app.js, style.css
```

`bench-core` does not know whether a CLI or an HTTP request is driving it. It exposes a `Run`
handle — `start(config)`, `set_rate(n)`, `stop()`, and a `tokio::sync::watch` of progress. The CLI
drives it to completion and prints; the web server drives it and forwards progress to WebSocket
clients.

`sink.rs` holds the `--emit-sql` decision as an enum with two implementations behind one trait. The
generator emits *rows*, not SQL strings: `Direct` binds them as parameters, `EmitSql` formats them.
This removes a class of quoting bugs by construction.

`pace.rs` has no I/O at all — a pure function from `(rate, rows_emitted, start_instant)` to the
instant row N should be inserted. Defect 2 above becomes a unit test rather than a two-minute
benchmark run.

`measure.rs` computes percentiles in Rust over the alert stream, so the details tab does not
re-query `latency/report.sql` on a timer. That SQL stays for `make lat-report`.

### Rejected alternatives

- **Single crate, two binaries.** Same code sharing, less structure, but Axum and tower leak into
  the CLI's dependency tree and the generator/web boundary stays informal.
- **Web server spawns the CLI as a child process.** Best isolation, but live rate changes need IPC
  or a restart, and it re-inherits the child-babysitting that `latency/bench.sh` already does.
  Directly at odds with wanting a rate dial you can turn mid-run.

## Data flow

```
 generator task ──insert──▶ t_rt ──▶ mv_rt ──sink──▶ t_rt_alerts
      │                                                   │
      │ progress (rows sent, rate)              CREATE SUBSCRIPTION
      ▼                                                   │
  watch channel                          subscription cursor task
      │                                        │ FETCH n FROM cur
      └──────────────┬─────────────────────────┘
                     ▼
            broadcast::Sender<Event>
                     │
              WebSocket fan-out
```

Alerts reach the server through `CREATE SUBSCRIPTION` plus a subscription cursor, not polling.
Verified working against `bee0fbd`: the cursor yields the table's columns plus `op` and
`rw_timestamp`, and delivers incrementally. It is the correct mechanism and it demonstrates a real
RisingWave capability.

Events, serialised as tagged JSON:

| event | carries | cadence |
|---|---|---|
| `Alert` | partition, chain_len, latency_ms, alert_ts | sampled, ~20/s |
| `Rate` | rows/s in (requested and actual), alerts/s out | 250ms |
| `Stats` | p50, p95, p99, min, max, n | 250ms |
| `Status` | cluster, pipeline, load state | on transition |
| `Metrics` | matches_emitted, evicted_rows, scan_budget_exhausted | 2s |
| `Probe` | round index, latency_ms, and a final p50/p95 summary | per probe round |
| `Snapshot` | status, last 50 alerts, current stats | once, on connect |
| `Log` | level, text | as they occur |

**Cursor task.** `FETCH` is non-blocking and returns zero rows when nothing is new, so this is a
loop with a short sleep, not a blocking read. It holds a dedicated connection because subscription
cursors are session-scoped, re-declares on reconnect, and tolerates its cursor vanishing when the
pipeline is rebuilt.

**Back-pressure.** At 2000 rows/s the alert rate is roughly 500/s, far more than a browser should
render. Every alert goes into `measure.rs`, so percentiles see all of them; the WebSocket forwards
a sample of at most ~20/s. A slow client is lagged off the broadcast channel and resynced from the
snapshot rather than stalling the producer.

**Late joiners** receive a `Snapshot` frame first: current status, the last 50 alerts from a ring
buffer, current stats.

Percentiles shown in the UI therefore cover *what the page has been watching*, not all history, and
reset on reload. The details tab labels this "this run"; `make lat-report` remains the source of
truth over the full table.

## Control surface

| endpoint | does | notes |
|---|---|---|
| `POST /api/cluster/up` | `podman run --replace …`, wait for pgwire | image pin read from env |
| `POST /api/cluster/down` | stop and remove container | volume kept |
| `POST /api/cluster/clean` | down **and destroy the volume** | requires `{"confirm":"clean"}` |
| `POST /api/pipeline/rebuild` | run `setup_realtime.sql`, recreate subscription | stops any running load first |
| `POST /api/load/start` | rate, rows, partitions, hot_count, hot_share, abandon_prob | 409 if already running |
| `POST /api/load/rate` | `{rate}` | live dial; writes the atomic the pacer reads |
| `POST /api/load/stop` | cancels the generator task | |
| `POST /api/probe/start` | `{rounds}`; results stream as events | forces `SENTINEL=off` when a load is running |
| `GET /api/status` | current state for initial render | same shape as `Status` |
| `GET /ws` | the event stream | |

Three deliberate constraints:

**`clean` requires a confirmation token in the request body**, not only a UI dialog. A UI-only
guard is one stray click or one `curl` away from destroying the data volume.

**One global run.** One load, one probe, one cluster; `start` while running returns 409 rather than
queueing or racing. Concurrent loads would make every number on the page meaningless.

**The probe forces `SENTINEL=off` whenever a load is active**, encoding defect 3 in the API rather
than in a comment. Standalone (no load) it keeps the sentinel on so it still works without traffic.

**Bind to `127.0.0.1` by default.** The service can destroy a data volume and shell out to podman.
`--bind` overrides explicitly, with a startup warning when the address is not loopback.

## CPU pinning

The rig has 64 cores, the container's cpuset is unset, and compute node parallelism is 64 — while
the licence caps at 4 RWU (`exceeds the maximum allowed by the license key (4)`, which is already
why ElasticDiskCache is disabled). Latency-sensitive work competes with the cluster: if the
generator is descheduled between taking a timestamp and the insert landing, that delay is
indistinguishable from pipeline latency, and we would be reporting our own scheduling noise.

Automatic, safe assignment:

```
detect usable cores N        (from the cgroup quota where one exists, not nproc)
  N < 8      → pin nothing, log "too few cores to partition (N)"
  N >= 8     → bench = 2 cores, cluster = N-2, streaming_parallelism = N-2
platform
  Linux      → cpuset on the container + sched_setaffinity on the bench process
  macOS      → cpuset on the container only; log that process affinity is unavailable
```

Two cores for the bench side suffice — the generator at 2k rows/s is nearly idle; what it needs is
not to wait behind a runnable queue when it stamps a row.

Setting `streaming_parallelism` to match the cpuset is the part that is easy to miss and is what
actually buys the isolation: RisingWave sizes its thread pools from the detected core count, so a
container pinned to 16 cores may still spawn 64 streaming workers and thrash.

**Portability.** Container cpuset works on both platforms — on a Mac, podman runs a Linux VM and it
applies within the VM's cores. Process affinity is `sched_setaffinity`, which is Linux-only; macOS
exposes only affinity *tags*, which are scheduler hints and not usable for this. The macOS path
must degrade and log, never fail. **This cannot be tested on the Linux rig** and needs a colleague
to verify. On macOS the images run emulated anyway, so pinning buys tidiness there, not measurement
quality.

`cores.cluster` and `cores.bench` override the automatic split. The UI shows the layout in effect
and how it was chosen.

**Existing numbers were measured unpinned** (~92k rows/s, probe p50 6318ms, server-side p50 6048ms
over 39801 matches). Pinned runs are not comparable to them and the README figures need re-measuring
if pinning becomes the default. It does not become the default in this change.

## UI

Two tabs.

**Demo tab** — layout A, "stream is the hero". The alert feed occupies the left two-thirds; a right
rail stacks the rows/s speedometer, alerts/s, and the latency chart. Controls sit in a thin top bar
with cluster state. What sells the feature is watching chains resolve — `d b b w` becoming an alert
about six seconds after the withdraw — so the feed dominates and the gauges corroborate.

The feed carries a label: *showing ~20 of ~500 alerts/s · percentiles cover all*. A reviewer who
notices the feed rate not matching the alerts/s gauge would otherwise reasonably wonder what else
is approximated.

**Details tab** — four stacked panels: latency percentiles and throughput; operator metrics
(`stream_match_recognize_matches_emitted`, `evicted_rows`, `scan_budget_exhausted`); pipeline state
(open chains, live partitions, base rows vs matches); and run config (setup SQL, generator
arguments, image tag, core layout).

Operator metrics require the compute node's Prometheus endpoint, so **the Makefile and
`compose.yaml` must publish port 1222** alongside 4566 and 5690.

A run that cannot produce trustworthy numbers — emulated on Apple Silicon, or unpinned, or too few
cores — is labelled as such on the details tab, so a screenshot of a Mac run cannot circulate as a
measurement.

## Error handling

- **Cluster not up** — every control except `cluster/up` returns 409 with a plain message; the UI
  disables them rather than letting clicks fail.
- **Rebuild while loading** — stops the load first. Feeding a dropped table silently is the
  confusing case.
- **Cursor dies** (pipeline rebuilt, connection dropped) — re-declare on the next tick, emit a
  `Log`. Never takes the server down.
- **Generator behind schedule** — report actual against requested in `Rate` and show it. This is
  defect 1 and 2's signature; it should be visible rather than inferred.
- **podman absent** — detected at startup; cluster controls disabled with the reason shown.

## Testing

1. **Unit, no database.** `pace.rs` against a fake clock: row N is scheduled at `start + N/rate`,
   and cumulative elapsed does not drift ahead of the wall clock. `gen.rs`: an abandoned chain emits
   `d b+` and never `w`; `--hot-count >= --partitions` is rejected.
2. **SQL-text golden tests.** `--emit-sql` for a fixed seed against a checked-in file. Catches
   quoting and column-list regressions without a cluster.
3. **Integration, live cluster.** Reuses the `expected/` harness: run a short fixed-seed load and
   assert the match count. Plus the test that would have caught defect 3 — run the probe with a load
   active and assert the server-side p50 is within tolerance of the probe's.

**Parity gate.** Before `datagen/gen.py` is deleted, Rust `--emit-sql` output must be diff-clean
against the Python for the same seed and arguments. This makes "full parity" checked rather than
asserted.

## What happens to the existing make targets

`gen.py` and `seal.sh` are deleted only once the parity gate passes, and the targets that used them
move to the Rust binary in the same commit:

| target | before | after |
|---|---|---|
| `load` | `gen.py \| psql`, then `seal.sh` | `bench load --profile …` (feed and seal in one process) |
| `rt-load` | `gen.py --mode realtime \| psql` | `bench load --mode realtime …` |
| `bench` | `latency/bench.sh` orchestrates | unchanged; it calls the new binary |
| `latency` | `latency/probe.sh` | unchanged — the probe stays bash for now (see Scope) |
| `lat-report` | `psql -f latency/report.sql` | unchanged |

`make smoke`, `scenarios/check.sh` and `expected/` are untouched: they exercise scenario SQL, not
the generator, and must keep passing across the whole port as the regression check that it changed
nothing observable.

Because the CLI connects directly rather than emitting SQL to a pipe, `PSQLFLAGS` no longer applies
to it. Connection details come from `--url` (or `DATABASE_URL`), defaulting to the same
`127.0.0.1:4566 dev/root` the Makefile uses today.

## Scope

In: the workspace, full generator parity including seal, the two-tab UI, the controls above,
automatic CPU pinning (off by default), publishing port 1222, migrating the make targets above, and
deleting `gen.py` and `seal.sh` once the parity gate passes.

Out: auto-detecting a "good" core split beyond the safe default; authentication; persisting run
history across restarts; multi-cluster or multi-run orchestration; replacing `scenarios/check.sh`
or the `expected/` harness; a Rust port of `probe.sh` beyond what `bench-core` needs to run rounds.
