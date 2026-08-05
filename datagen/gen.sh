#!/usr/bin/env bash
# Pipe generated rows into a table via psql. The table must exist with columns (id, ts, v) and a
# watermark on ts.
#
#   TABLE=t_perf PARTITIONS=1000 ROWS=100000 ./datagen/gen.sh
#
# Rows are emitted round-robin over PARTITIONS with monotonically increasing ts, in batched
# multi-row INSERTs. A trailing sentinel row (partition 0, far-future ts) advances the watermark
# past everything.

set -euo pipefail

TABLE="${TABLE:?set TABLE}"
PARTITIONS="${PARTITIONS:-100}"
ROWS="${ROWS:-10000}"
BATCH="${BATCH:-500}"
PSQL="${PSQL:-/opt/homebrew/opt/libpq/bin/psql}"
PSQLFLAGS=(-h localhost -p 4566 -d dev -U root -v ON_ERROR_STOP=1 -q)

awk -v parts="$PARTITIONS" -v rows="$ROWS" -v batch="$BATCH" -v table="$TABLE" 'BEGIN {
  printf "set rw_implicit_flush to true;\n";
  n = 0;
  for (i = 0; i < rows; i++) {
    if (n == 0) printf "insert into %s values ", table;
    else printf ", ";
    # id cycles over partitions, ts strictly increasing, v alternates sign for (a b)-style patterns
    printf "(%d, %d, %d)", (i % parts) + 1, i + 10, (i % 2 == 0) ? 5 : -5;
    n++;
    if (n == batch) { printf ";\n"; n = 0; }
  }
  if (n > 0) printf ";\n";
  printf "insert into %s values (0, %d, 0);\n", table, rows + 1000000;
}' | "$PSQL" "${PSQLFLAGS[@]}"

echo "inserted $ROWS rows over $PARTITIONS partitions into $TABLE (+ watermark sentinel)"
