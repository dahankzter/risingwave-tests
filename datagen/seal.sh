#!/usr/bin/env bash
# Seal a completed bulk feed: wait for the pipeline to drain, then advance the watermark past
# every buffered row so the remaining decidable matches are released.
#
#   TABLE=t_perf MV=mv_perf ./datagen/seal.sh
#
# Why this is not just an INSERT at the end of the feed (which is what gen.py used to emit):
# `flush` returns before the materialized view has caught up. On the rig, a 200k-row feed showed
# 3917 matches immediately after its final flush and 10624 five seconds later with nothing else
# inserted. A far-future sentinel delivered inside that window froze the count at 3917 for good --
# the watermark discards the rows still in flight instead of matching them, costing ~63% of the
# matches. So: settle first, seal second, settle again.
#
# "Settled" here means the match count stopped moving, which is a heuristic, not a guarantee --
# STABLE_POLLS consecutive unchanged reads. Raise it on a slower box or a heavier feed.

set -euo pipefail

PSQL="${PSQL:-$(command -v psql || echo /opt/homebrew/opt/libpq/bin/psql)}"
FLAGS=(-h 127.0.0.1 -p 4566 -d dev -U root -q -t -A)
TABLE="${TABLE:-t_perf}"
MV="${MV:-mv_perf}"
SENTINEL_PARTITION="${SENTINEL_PARTITION:-0}"
STABLE_POLLS="${STABLE_POLLS:-5}"
POLL_SECS="${POLL_SECS:-1}"
MAX_POLLS="${MAX_POLLS:-600}"

settle() {
  local what="$1" last=-1 stable=0 polls=0 n
  while :; do
    n=$("$PSQL" "${FLAGS[@]}" -c "select count(*) from $MV;")
    if [ "$n" = "$last" ]; then
      stable=$((stable + 1))
      [ "$stable" -ge "$STABLE_POLLS" ] && break
    else
      stable=0
    fi
    last="$n"
    polls=$((polls + 1))
    if [ "$polls" -ge "$MAX_POLLS" ]; then
      echo "seal: $what still moving after $((MAX_POLLS * POLL_SECS))s (at $n matches); giving up" >&2
      exit 1
    fi
    sleep "$POLL_SECS"
  done
  echo "seal: $what settled at $n matches"
}

settle "feed"

# Past every row in the table, so the watermark releases the whole buffer.
max_ts=$("$PSQL" "${FLAGS[@]}" -c "select coalesce(max(ts), 0) from $TABLE;")
"$PSQL" "${FLAGS[@]}" -c \
  "set rw_implicit_flush to true;
   insert into $TABLE values ($SENTINEL_PARTITION, $max_ts + 1000000, 'noop', 0);" > /dev/null

settle "seal"
