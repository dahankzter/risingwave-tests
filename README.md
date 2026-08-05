# risingwave-tests

A test bench for the MATCH_RECOGNIZE contribution to RisingWave
([risingwavelabs/risingwave#26584](https://github.com/risingwavelabs/risingwave/pull/26584)):
scenario scripts, adversarial patterns, and performance probes run against published
`risingwave-mr` images.

## Usage

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
- `latency/` — the end-to-end decision-latency probe: time from inserting a match's completing
  event to the alert row appearing in the MV, with p50/p95 over N rounds.
- `datagen/gen.py` — the workload generator: partitions, rows, hot-partition skew
  (`--hot-count/--hot-share`), fraud-shaped event chains with configurable abandonment
  (`--abandon-prob` — abandoned chains become retained open partials, the interesting state
  regime under `WITHIN`), payload width (`--payload-cols/--payload-bytes`), tie density
  (`--ties`), and bulk vs realtime pacing (`--mode realtime --rate N`).
- `datagen/seal.sh` — advances the watermark past a finished bulk feed, once the pipeline has
  drained. Not part of the feed itself; see "Sealing a bulk feed" below.
- `expected/` — recorded scenario output. This is what `make smoke` asserts against.

## Load & latency

```sh
make load-setup && make load PROFILE=fraud      # 1M rows, 100k partitions, mild skew, 25% open partials
make load PROFILE=hotspot                        # one partition takes 90% of traffic
make rt-setup && make rt-load &                  # realtime background load (wall-clock ts)
make latency ROUNDS=20                           # p50/p95 insert->alert delay under that load
```

Measured on the Linux rig (native amd64, 64 cores) against `bee0fbd`:

| | |
|---|---|
| bulk ingest, fraud profile | ~92k rows/s (200k rows, 100k partitions, 100 hot @ 30%, 25% abandoned) |
| decision latency under 2k rows/s | p50 6448 ms, p95 6676 ms, min 5412 ms (20 rounds) |

The latency figure includes the 5s watermark delay declared on the realtime table, so the
operator and pipeline account for roughly 1.4s of it. That is the honest end-to-end number an
alerting consumer sees; tighten the watermark to trade late-event tolerance for alert speed. For
reference, the same probe on emulated Apple Silicon reported ~9.4s p50.

### Sealing a bulk feed

`gen.py` emits data only. Advancing the watermark is a separate step, because **`flush` returns
before the materialized view has caught up** — measured on a 200k-row feed: 3917 matches
immediately after the final flush, 10624 five seconds later with nothing else inserted. A
far-future sentinel delivered inside that window froze the count at 3917 permanently: the
watermark discards the rows still in flight instead of matching them, losing ~63% of the matches,
and they never come back. Putting a `flush` in front of the sentinel does not help, since `flush`
is precisely what does not wait.

So `datagen/seal.sh` polls until the match count stops moving, inserts the sentinel, and polls
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
