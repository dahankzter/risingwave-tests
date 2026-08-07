#!/usr/bin/env python3
"""Measure the two clocks that govern when a RisingWave result becomes visible.

Produces the table in `docs/latency-model.md`. Run it on any machine with the console up and
re-measure there; the point of it being a script rather than prose is that the numbers in the doc
can be reproduced rather than trusted.

    make console          # in one terminal
    make latency-model    # in another

Talks to the console's `POST /api/sql/run` instead of a Postgres driver, so it needs nothing beyond
the standard library — no psql on PATH, no psycopg2, no cargo build. The console is already the
thing running on every machine this bench runs on.

Each case is repeated and reported as a median, because the first version of this measurement drew
conclusions from single runs and two of them turned out to be coin flips.
"""

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request

# Each case creates and drops its own objects under this prefix, so a run leaves nothing behind and
# cannot collide with the demos or the realtime pipeline.
PREFIX = "latmodel"


def sql(base, text, timeout=180):
    """Run SQL through the console. Raises on transport failure; returns the parsed blocks."""
    req = urllib.request.Request(
        f"{base}/api/sql/run",
        json.dumps({"sql": text}).encode(),
        {"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def scalar(base, query):
    """The first cell of a single-row query, as text."""
    blocks = sql(base, query)["blocks"]
    rows = blocks[0]["rows"]
    if not rows:
        raise RuntimeError(f"{query!r} returned no rows")
    return rows[0][0]


def must(base, text):
    """Run SQL and fail loudly on the first error. A silently-half-built fixture would produce a
    number that looks like a measurement and is not one."""
    result = sql(base, text)
    for block in result["blocks"]:
        if block["error"]:
            raise RuntimeError(f"setup failed at {block['expect']!r}: {block['error']}")
    return result


def wait_until(base, query, want, limit, poll=0.1):
    """Seconds until `query` returns `want`, or None if it never did within `limit`."""
    start = time.monotonic()
    while time.monotonic() - start < limit:
        if scalar(base, query) == want:
            return time.monotonic() - start
        time.sleep(poll)
    return None


def case_barrier(base):
    """A plain aggregate: nothing watermark-gated, so visibility is one checkpoint away."""
    must(
        base,
        f"""drop materialized view if exists {PREFIX}_agg_mv;
            drop table if exists {PREFIX}_agg;
            create table {PREFIX}_agg (k int, v int) append only;
            create materialized view {PREFIX}_agg_mv as
              select k, count(*) as n from {PREFIX}_agg group by k;""",
    )
    settle(base)
    count = f"select count(*) as n from {PREFIX}_agg_mv;"
    must(base, f"insert into {PREFIX}_agg values (1, 1);")
    took = wait_until(base, count, "1", limit=30)
    must(base, f"drop materialized view {PREFIX}_agg_mv; drop table {PREFIX}_agg;")
    return took


def build_matcher(base):
    """A minimal two-row MATCH_RECOGNIZE over a table with a 2s watermark tolerance."""
    must(
        base,
        f"""drop materialized view if exists {PREFIX}_mr_mv;
            drop table if exists {PREFIX}_mr;
            create table {PREFIX}_mr (id int, ts timestamptz, kind varchar,
              watermark for ts as ts - interval '2' second) append only;
            create materialized view {PREFIX}_mr_mv as
            select * from {PREFIX}_mr
            match_recognize (
              partition by id order by ts
              measures a.ts as t1, b.ts as t2
              one row per match after match skip past last row
              pattern (a b)
              define a as a.kind = 'x', b as b.kind = 'y'
            ) as m;""",
    )
    settle(base)


def drop_matcher(base):
    must(base, f"drop materialized view {PREFIX}_mr_mv; drop table {PREFIX}_mr;")


def case_watermark_satisfied(base):
    """A match whose event time is already well in the past, so the watermark covers it as soon as
    any row arrives. Isolates the operator's own cost from the lateness policy."""
    build_matcher(base)
    count = f"select count(*) as n from {PREFIX}_mr_mv;"
    # Fixed timestamps, not now() twice: two calls to now() in one statement can tie, and a tie is a
    # different experiment from an ordered pair.
    must(
        base,
        f"""insert into {PREFIX}_mr values
              (1, '2026-01-01 10:00:00+00', 'x'), (1, '2026-01-01 10:00:01+00', 'y');
            insert into {PREFIX}_mr values (9, now() - interval '1' second, 'sentinel');""",
    )
    took = wait_until(base, count, "1", limit=60)
    drop_matcher(base)
    return took


def case_starvation(base, quiet_secs):
    """The trap: a complete match on LIVE data, with nothing later arriving to advance the
    watermark. Returns (still_invisible_after_quiet, seconds_once_a_later_event_arrives)."""
    build_matcher(base)
    count = f"select count(*) as n from {PREFIX}_mr_mv;"
    must(
        base,
        f"""insert into {PREFIX}_mr values
              (2, now(), 'x'), (2, now() + interval '1' second, 'y');""",
    )
    time.sleep(quiet_secs)
    starved = scalar(base, count) == "0"
    # One later event is all it takes.
    must(base, f"insert into {PREFIX}_mr values (9, now() + interval '5' second, 'sentinel');")
    took = wait_until(base, count, "1", limit=60)
    drop_matcher(base)
    return starved, took


def settle(base, seconds=4):
    """Let a freshly created view finish coming up, so creation cost is not counted as latency."""
    time.sleep(seconds)


def median(values):
    kept = [v for v in values if v is not None]
    return statistics.median(kept) if kept else None


def fmt(seconds):
    return "never" if seconds is None else f"{seconds:.2f}s"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--console", default="http://127.0.0.1:3000", help="console base URL")
    parser.add_argument("--repeat", type=int, default=3, help="runs per case; the median is reported")
    parser.add_argument(
        "--quiet-secs",
        type=int,
        default=6,
        help="how long to leave the starvation case with no later event",
    )
    args = parser.parse_args()

    try:
        barrier_ms = None
        for row in sql(args.console, "show parameters;")["blocks"][0]["rows"]:
            if row[0] == "barrier_interval_ms":
                barrier_ms = row[1]
        checkpoint_every = None
        for row in sql(args.console, "show parameters;")["blocks"][0]["rows"]:
            if row[0] == "checkpoint_frequency":
                checkpoint_every = row[1]
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"cannot reach the console at {args.console} ({e}) — is `make console` running?")

    print(f"barrier_interval_ms={barrier_ms}  checkpoint_frequency={checkpoint_every}")
    print(f"repeat={args.repeat}  quiet={args.quiet_secs}s  console={args.console}\n")

    barrier = [case_barrier(args.console) for _ in range(args.repeat)]
    print(f"A  barrier-gated (plain aggregate MV)      {fmt(median(barrier))}   raw {[fmt(v) for v in barrier]}")

    satisfied = [case_watermark_satisfied(args.console) for _ in range(args.repeat)]
    print(f"B  watermark already past the event time   {fmt(median(satisfied))}   raw {[fmt(v) for v in satisfied]}")

    starved_flags, released = [], []
    for _ in range(args.repeat):
        starved, took = case_starvation(args.console, args.quiet_secs)
        starved_flags.append(starved)
        released.append(took)
    all_starved = all(starved_flags)
    print(
        f"C  live match, nothing later arriving      "
        f"{'never' if all_starved else 'LEAKED — see below'}"
        f"   (invisible after {args.quiet_secs}s in {sum(starved_flags)}/{len(starved_flags)} runs)"
    )
    print(f"C' the same, once one later event arrives  {fmt(median(released))}   raw {[fmt(v) for v in released]}")

    if not all_starved:
        print(
            "\nNOTE: case C emitted without a later event in at least one run. That contradicts the "
            "starvation claim in docs/latency-model.md and is worth investigating before trusting "
            "the rest of this table — something else was advancing the watermark."
        )


if __name__ == "__main__":
    main()
