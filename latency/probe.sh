#!/usr/bin/env bash
# End-to-end decision latency: time from inserting the COMPLETING event of a match until the match
# row is visible in the MV. This is the number an alerting pipeline actually feels.
#
# Requires the realtime perf pipeline (scenarios/perf/setup_realtime.sql) to be running, typically
# with datagen in realtime mode supplying background load in another terminal:
#
#   ROUNDS=20 ./latency/probe.sh
#
# Each round drives one fresh probe partition (keys counting down from 999999) through a complete
# deposit -> bet -> withdraw chain with wall-clock timestamps, then polls until the match appears.
# Reports per-round latency and a p50/p95 summary. Poll granularity ~20ms.

set -euo pipefail

PSQL="${PSQL:-/opt/homebrew/opt/libpq/bin/psql}"
FLAGS=(-h localhost -p 4566 -d dev -U root -q -t -A)
ROUNDS="${ROUNDS:-10}"
TABLE="${TABLE:-t_rt}"
MV="${MV:-mv_rt}"

# Per-run unique probe partitions: reusing pids across runs would let leftover rows from an
# earlier (aborted) run satisfy the poll instantly and fake a fast round.
BASE=$(( (($(date +%s) % 86400)) * 100 + 100000000 ))
lat_ms=()
for ((i = 0; i < ROUNDS; i++)); do
  pid=$((BASE + i))
  # Separate statements per event: now() is fixed for the whole statement, so a single INSERT
  # would give the deposit and the bet an identical ts and leave them unordered under ORDER BY ts
  # — the (d b+ w) pattern would then match by luck, and a miss shows up as a bogus TIMEOUT.
  # Both run before t0, so the extra round trip is outside the measurement.
  "$PSQL" "${FLAGS[@]}" -c \
    "set rw_implicit_flush to true;
     insert into $TABLE values ($pid, now(), 'deposit', 100);" > /dev/null
  "$PSQL" "${FLAGS[@]}" -c \
    "set rw_implicit_flush to true;
     insert into $TABLE values ($pid, now(), 'bet', 10);" > /dev/null
  t0=$(python3 -c 'import time; print(time.time_ns()//1_000_000)')
  "$PSQL" "${FLAGS[@]}" -c \
    "set rw_implicit_flush to true;
     insert into $TABLE values ($pid, now(), 'withdraw', 90);" > /dev/null
  polls=0
  while :; do
    n=$("$PSQL" "${FLAGS[@]}" -c "select count(*) from $MV where partition_0 = $pid;")
    [ "$n" -ge 1 ] && break
    polls=$((polls + 1))
    # Keep the watermark moving ourselves (sentinel partition 0): the probe must not depend on
    # background load for watermark progress — the release delay it causes is part of the
    # measured latency either way.
    if [ $((polls % 10)) -eq 0 ]; then
      "$PSQL" "${FLAGS[@]}" -c "set rw_implicit_flush to true; insert into $TABLE values (0, now(), 'noop', 0);" > /dev/null
    fi
    if [ "$polls" -gt 1500 ]; then
      echo "round $i: TIMEOUT after 30s+ — pipeline not emitting (check the MV and watermark)" >&2
      exit 1
    fi
    sleep 0.02
  done
  t1=$(python3 -c 'import time; print(time.time_ns()//1_000_000)')
  ms=$((t1 - t0))
  lat_ms+=("$ms")
  echo "round $i: ${ms} ms"
done

printf '%s\n' "${lat_ms[@]}" | sort -n | python3 -c '
import sys
xs = [int(l) for l in sys.stdin]
pick = lambda q: xs[min(len(xs) - 1, int(q * len(xs)))]
print(f"rounds={len(xs)} p50={pick(0.5)}ms p95={pick(0.95)}ms min={xs[0]}ms max={xs[-1]}ms")'
