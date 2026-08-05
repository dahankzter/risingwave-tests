#!/usr/bin/env bash
# Run scenario scripts and diff their output against expected/<name>.out.
#
#   scenarios/check.sh                        # every scenario that has an expected file
#   scenarios/check.sh scenarios/semantics/eowc_clause.sql ...
#   scenarios/check.sh --bless [files ...]    # (re)record the expected output instead
#
# The expectations written in each script's comments are documentation; these recorded files are
# the actual assertion. Without them a scenario only fails on a SQL *error* — a silently wrong
# result set passes, which is precisely the regression this bench exists to catch when comparing
# image tags.
#
# Scenarios with no expected file are setup-only (scenarios/perf/) and are skipped, so --bless
# decides what is under assertion. Always eyeball a blessed diff against the expectations in the
# script comments before committing it.

set -uo pipefail

PSQL="${PSQL:-$(command -v psql || echo /opt/homebrew/opt/libpq/bin/psql)}"
PSQLFLAGS=(-h 127.0.0.1 -p 4566 -d dev -U root -v ON_ERROR_STOP=1 -X)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXPECTED_DIR="$ROOT/expected"

BLESS=0
if [ "${1:-}" = "--bless" ]; then
  BLESS=1
  shift
fi

if [ "$#" -gt 0 ]; then
  scripts=("$@")
else
  scripts=("$ROOT"/scenarios/semantics/*.sql "$ROOT"/scenarios/adversarial/*.sql)
fi

# NOTICEs depend on whether a previous run left objects behind (every script opens with
# `drop ... if exists`), so a first run and a re-run differ; they are not part of the contract.
# Trailing whitespace comes from psql's column padding and varies with value widths.
normalize() {
  grep -v -E '^(NOTICE:|psql:.*: NOTICE:)' | sed -e 's/[[:space:]]*$//'
}

mkdir -p "$EXPECTED_DIR"
fail=0
ran=0

for s in "${scripts[@]}"; do
  name="$(basename "$s" .sql)"
  exp="$EXPECTED_DIR/$name.out"

  if [ "$BLESS" -eq 0 ] && [ ! -f "$exp" ]; then
    echo "skip  $name (setup-only: no expected/$name.out)"
    continue
  fi

  # Capture first, normalize second: piping directly would hand $? to the filter, and `grep -v`
  # reports 1 on empty output, which would read as a psql failure.
  raw="$("$PSQL" "${PSQLFLAGS[@]}" -f "$s" 2>&1)"
  rc=$?
  actual="$(printf '%s\n' "$raw" | normalize)"
  ran=$((ran + 1))

  if [ "$rc" -ne 0 ]; then
    echo "ERROR $name (psql exited $rc)"
    printf '%s\n' "$actual" | tail -20
    fail=$((fail + 1))
    continue
  fi

  if [ "$BLESS" -eq 1 ]; then
    printf '%s\n' "$actual" > "$exp"
    echo "bless $name"
    continue
  fi

  d="$(mktemp)"
  if diff -u "$exp" <(printf '%s\n' "$actual") > "$d" 2>&1; then
    echo "ok    $name"
  else
    echo "FAIL  $name"
    sed -e 's/^/      /' "$d"
    fail=$((fail + 1))
  fi
  rm -f "$d"
done

if [ "$fail" -ne 0 ]; then
  echo "$fail of $ran scenario(s) failed"
  exit 1
fi
echo "$ran scenario(s) ok"
