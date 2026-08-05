#!/usr/bin/env bash
# Run the whole realtime latency benchmark and print both measurements side by side.
#
#   make bench                       # defaults: 60k rows at 2000/s, 20 probe rounds
#   make bench ROWS=200000 RATE=5000 ROUNDS=40
#
# Steps, all against the single pipeline in scenarios/perf/setup_realtime.sql:
#   1. rebuild the pipeline (drops and recreates its own objects)
#   2. start the background feed, paced server-side at RATE rows/s
#   3. once traffic is flowing, run the client probe          -> measurement [1]
#   4. let the feed finish and the pipeline settle
#   5. query the stamps every match recorded for itself       -> measurement [2]
#
# The two measure different things on purpose; see the header of setup_realtime.sql for the
# diagram. In short: [1] stops at the MV and reflects a polling consumer, [2] waits one hop
# further across the sink and covers every match rather than the probe's own rounds.

set -uo pipefail

cd "$(dirname "$0")/.."

PSQL="${PSQL:-psql}"
FLAGS=(-h 127.0.0.1 -p 4566 -d dev -U root -v ON_ERROR_STOP=1 -X)
RATE="${RATE:-2000}"
ROUNDS="${ROUNDS:-10}"
PARTITIONS="${PARTITIONS:-5000}"

# The feed has to outlast the probe. The probe runs with SENTINEL=off here (so it does not
# advance the watermark and skew measurement [2]), which means it is the feed that releases its
# matches -- if the feed stops first, the watermark freezes and the probe hangs until it times
# out. A round costs roughly the watermark delay plus change; 12s each plus 20s of slack is
# comfortable. Override ROWS to pin the feed size explicitly.
ROWS="${ROWS:-$(( RATE * (12 * ROUNDS + 20) ))}"

say() { printf '\n=== %s\n' "$*"; }

say "1/5  building the pipeline"
"$PSQL" "${FLAGS[@]}" -q -f scenarios/perf/setup_realtime.sql 2>&1 | grep -vE '^(NOTICE|SET|DROP)' || true

say "2/5  starting background load: $ROWS rows at $RATE rows/s (~$((ROWS / RATE))s of traffic)"
web/target/release/bench load --table t_rt --mode realtime --rate "$RATE" --rows "$ROWS" \
  --partitions "$PARTITIONS" --hot-count 5 --hot-share 0.4 &
load_pid=$!
# Kill the feed if we exit early, so an aborted run does not leave traffic behind.
trap 'kill $load_pid 2>/dev/null' EXIT

# Wait for traffic to actually be arriving before probing: probing an empty pipeline measures
# nothing useful, and the probe's own watermark rows would be the only thing moving.
for _ in $(seq 60); do
  n=$("$PSQL" "${FLAGS[@]}" -t -A -c "select count(*) from t_rt;" 2>/dev/null || echo 0)
  [ "${n:-0}" -gt 1000 ] && break
  sleep 1
done
echo "traffic flowing (${n:-0} rows in)"

say "3/5  measurement [1]: client probe, $ROUNDS rounds (insert -> visible in mv_rt)"
PSQL="$PSQL" ROUNDS="$ROUNDS" TABLE=t_rt MV=mv_rt SENTINEL=off ./latency/probe.sh 2>&1 | tail -3
probe_rc=$?

say "4/5  waiting for the feed to finish and the pipeline to settle"
wait $load_pid 2>/dev/null
trap - EXIT
last=-1; stable=0
for _ in $(seq 120); do
  n=$("$PSQL" "${FLAGS[@]}" -t -A -c "select count(*) from t_rt_alerts;" 2>/dev/null || echo 0)
  if [ "$n" = "$last" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 4 ] && break
  else
    stable=0
  fi
  last="$n"
  sleep 1
done
echo "settled at $last alerts"

say "5/5  measurement [2]: server-side, every match under the load (arrival -> t_rt_alerts)"
"$PSQL" "${FLAGS[@]}" -f latency/report.sql

cat <<'NOTE'

[1] is what a consumer polling the MV feels; it samples only the probe's own rounds, and
    carries its own round trips plus 20ms of poll granularity.
[2] is what a downstream table sees, over every match produced -- one sink hop further.
Both include the 5s watermark delay declared on t_rt, which dominates either figure.
NOTE

exit $probe_rc
