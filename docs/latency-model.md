# When does a RisingWave result become visible?

Two different clocks decide, and which one applies to a query is the single most useful thing to
know about it. Most surprise in this bench — "the view is empty", "it was fast yesterday", "the test
is flaky" — comes from expecting one clock and getting the other.

Everything below is measured by `scenarios/perf/latency_model.py`, so it can be re-run rather than
believed. See [Re-measuring](#re-measuring).

## The two clocks

**Barrier-gated.** Projections, filters, and aggregates or joins over append-only input emit on the
next checkpoint. Nothing waits for anything except the barrier. This is the fast path, and its cost
is flat and predictable: one checkpoint interval.

**Watermark-gated.** MATCH_RECOGNIZE's final emission, tumbling and hopping window aggregates,
`EMIT ON WINDOW CLOSE`, and interval joins all hold results until the watermark passes the end of
what they are holding. They are not slower operators; they are operators that have been *told* to
wait, and the waiting is a correctness requirement you declared.

The distinction is visible in the plan. A `StreamWatermarkSort` or a window operator under your
node means the second clock applies.

## Measured

| | case | result |
|---|---|---|
| A | barrier-gated: a plain aggregate MV | **1.07s** (1.06–1.10) |
| B | watermark-gated, event time already well in the past | **3.02s** (2.97–3.04) |
| C | watermark-gated, live data, nothing later arriving | **never** (5/5 runs) |
| C' | the same, once one later event arrives | **2.0–7.1s**, bimodal |

`barrier_interval_ms=1000`, `checkpoint_frequency=1`, five runs per case.

> **These absolute numbers are from a single-node linux/amd64 container emulated on Apple Silicon
> and are inflated.** Trust the structure and the ratios; re-run the script on native hardware for
> figures worth quoting. The console's details tab labels its own environment for the same reason.

Reading them:

**A is one checkpoint**, as designed. If a query's answer only needs the rows that have arrived, it
is available on the next barrier and there is no lateness term at all.

**B is about three barriers**, not one — a row arrives, the watermark derived from it propagates, and
the operator then emits. Note what B is *not*: the table declares `interval '2' second` of watermark
tolerance, but the events are timestamped in the past, so that tolerance costs nothing. Lateness is
only paid on data that is actually live.

**C is the one to internalise.** A fully matched pair sat in the operator and stayed invisible
indefinitely — not slow, *stopped* — because nothing later arrived to advance the watermark past it.
One further event released it.

**C' is bimodal** (≈2s or ≈6–7s depending on where the releasing event lands in the barrier cycle),
which is why the script reports raw values alongside the median. A single sample here means nothing;
that is how the demo settle times were first got wrong.

## Four things that generalise

### A quiet stream stops emitting, silently

Watermarks advance from arriving data. No data, no watermark, no emission — and nothing anywhere
reports a problem. This is case C, and it is not an artifact of a test harness:

- a low-traffic partition emits late or not at all while a busy one is fine;
- a source that goes idle freezes every watermark-gated operator downstream of it;
- a quiet Sunday night means detection quietly stops firing.

Same trap in Flink. Production deployments handle it with idle-source watermark advancement or a
periodic heartbeat event. Every demo file in `scenarios/playground` ends with a sentinel row for
exactly this reason, and that sentinel is standing in for what a real deployment must provide.

It is also the motivation for emit-on-update: a provisional match reported as it forms does not wait
on the watermark at all, at the cost of being retracted or superseded later.

### Late rows are dropped, silently

A watermark is monotonic, and a row arriving below it is discarded. No error, no warning in the
query path. Two ways this bites:

- **A sentinel placed far in the future poisons everything after it.** A sentinel at `now() + 999s`
  sets the watermark past the next several batches, and those batches vanish on arrival. This cost
  an afternoon in this repo; see `scenarios/playground/README.md`.
- **Backfilled or replayed history** arriving after live data is late by definition.

Keep a sentinel just past its own batch, and keep watermark tolerance wide enough for the real
disorder of the source.

### Watermark lateness composes across hops

Each watermark-gated stage adds its own tolerance to the floor: two stages at 5s is a 10s floor
before any engine cost. This is why `scenarios/playground/04_match_over_match.sql` is the slowest
thing in the repo — structurally, not because of container overhead.

*Reasoned from the structure rather than measured here; the script covers single-hop cases only.*

### `FLUSH` is not global quiescence

`FLUSH` waits for a checkpoint on the **DML** path. It drains an insert deterministically, which is
why it appears in every scenario file. It does **not** traverse a sink into a table: that table is a
separate streaming job fed by a sink, not by DML.

Consequence for anyone writing tests against a multi-hop pipeline: `insert; flush; select` is sound
for one hop and a race for two. Measured directly — six consecutive flushes left the bridge table
full and the second-level view still empty.

## The dials, in order of leverage

1. **Watermark tolerance in the table's DDL.** Dominates everything else. At 5s tolerance the
   realtime pipeline measures p50 ≈ 5.75s end to end — roughly 750ms of engine work under 5s of
   declared policy. The console's lateness selector exists to make this a demo rather than a claim,
   and it forces a pipeline rebuild because the number lives in the DDL.
2. **Whether the query needs watermark gating at all.** A plain aggregate is one barrier. Reach for
   windows or `EMIT ON WINDOW CLOSE` when the semantics genuinely require completeness, not by
   default.
3. **The number of gated hops.** Collapse levels where the semantics allow; each one adds its
   tolerance to the floor.
4. **`barrier_interval_ms` and `checkpoint_frequency`.** Both mutable at runtime. They set the floor
   for the fast path and the granularity of the slow one. Shorter is lower latency and more
   overhead.
5. **The partition key.** `PARTITION BY` becomes a hash exchange, so one hot key is one actor doing
   the work of many. The bench's hot-partition generator exists to make this visible.

## Parallelism and pinning, on a big box

Two things worth knowing when measuring on real hardware, both verified on the Linux rig:

- Constraining the container with `--cpuset-cpus` is enough on its own: RisingWave's default
  `streaming_parallelism` policy sizes new materialized views to the cpuset's core count, with no
  `ALTER SYSTEM` needed.
- `sched_setaffinity` is **per-thread, not per-process**. Pinning from `main` pins only the calling
  thread and leaves the async runtime's worker and blocking-pool threads roaming every core. The
  console builds its runtime with a `on_thread_start` hook so each thread pins itself as it starts;
  see `web/bench-web/src/pin.rs`.

## Re-measuring

```sh
make console        # in one terminal
make latency-model  # in another
```

The script needs only the standard library — it drives the console's `/api/sql/run` rather than a
Postgres driver, so there is nothing to install and no build step. It creates and drops its own
objects under a `latmodel` prefix, so it will not collide with the demos or the realtime pipeline.

```sh
python3 scenarios/perf/latency_model.py --repeat 5            # more samples
python3 scenarios/perf/latency_model.py --quiet-secs 20       # a longer starvation window
python3 scenarios/perf/latency_model.py --console http://host:3000
```

If case C ever reports that a match emitted with no later event, the script says so loudly: that
would contradict the starvation finding above, and something else was advancing the watermark.
