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
make smoke             # run every scenario under scenarios/semantics/
make down              # stop; make clean also removes the data volume
```

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
- `scenarios/adversarial/` — patterns designed to hurt: catastrophic-backtracking shapes, scan
  budget probes.
- `scenarios/perf/` — load setups: bulk throughput (`setup_bulk.sql`), realtime with wall-clock
  timestamps (`setup_realtime.sql`), and hot-partition skew (`hot_partition.sql`).
- `latency/` — the end-to-end decision-latency probe: time from inserting a match's completing
  event to the alert row appearing in the MV, with p50/p95 over N rounds.
- `datagen/gen.py` — the workload generator: partitions, rows, hot-partition skew
  (`--hot-count/--hot-share`), fraud-shaped event chains with configurable abandonment
  (`--abandon-prob` — abandoned chains become retained open partials, the interesting state
  regime under `WITHIN`), payload width (`--payload-cols/--payload-bytes`), tie density
  (`--ties`), and bulk vs realtime pacing (`--mode realtime --rate N`).

## Load & latency

```sh
make load-setup && make load PROFILE=fraud      # 1M rows, 100k partitions, mild skew, 25% open partials
make load PROFILE=hotspot                        # one partition takes 90% of traffic
make rt-setup && make rt-load &                  # realtime background load (wall-clock ts)
make latency ROUNDS=20                           # p50/p95 insert->alert delay under that load
```

The latency number includes the watermark delay declared on the table (5s in the realtime setup) —
that is the honest end-to-end figure an alerting consumer sees, and it is dominated by that
declared delay: expect roughly `watermark delay + ~1s processing` under continuous traffic.
Tighten the watermark to trade late-event tolerance for alert speed. Numbers measured on Apple
Silicon (emulated) are shape-checks only (~9.4s p50 observed where native + real traffic should
sit near ~6s) — run the same targets on a native amd64 box for real figures.

Scenario scripts are plain psql SQL files. Expected results are stated in comments at the point of
each query; scripts are written to be re-runnable (they drop and recreate their own objects).
