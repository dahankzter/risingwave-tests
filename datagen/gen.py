#!/usr/bin/env python3
"""Workload generator for the MATCH_RECOGNIZE bench. Emits SQL INSERTs on stdout; pipe to psql.

The event shape is a fraud-flavored chain per partition: deposit -> bet{1..n} -> withdraw. A chain
that completes forms a match for the standard test pattern (d b+ w); an abandoned chain (no
withdraw) leaves an open partial — under WITHIN, exactly the retained-state regime whose cost the
operator's design doc discloses.

Examples:
  # 1M rows over 10k partitions, 10 hot partitions taking half the traffic, 20% chains abandoned
  ./gen.py --table t_perf --partitions 10000 --rows 1000000 --hot-count 10 --hot-share 0.5 \
           --abandon-prob 0.2 | psql -h localhost -p 4566 -d dev -U root

  # Realtime mode: wall-clock timestamps paced at 2000 rows/s (for latency probing alongside)
  ./gen.py --table t_perf --mode realtime --rate 2000 --rows 100000 | psql ...

Timestamps are integers in BULK mode (fast, deterministic, WITHIN bounds in "ticks") and
timestamptz in REALTIME mode (wall clock; lets a probe measure event->alert delay). The target
table's schema must match --payload-cols (see scenarios/perf/).
"""

import argparse
import random
import sys
import time


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--table", required=True)
    p.add_argument("--rows", type=int, default=100_000, help="total events to emit")
    p.add_argument("--partitions", type=int, default=1_000, help="distinct partition keys")
    p.add_argument("--batch", type=int, default=500, help="rows per INSERT statement")
    p.add_argument("--seed", type=int, default=42, help="RNG seed (deterministic workloads)")
    # Skew
    p.add_argument("--hot-count", type=int, default=0, help="number of hot partitions (0 = uniform)")
    p.add_argument("--hot-share", type=float, default=0.5, help="fraction of traffic to hot partitions")
    # Chain shape
    p.add_argument("--bets-min", type=int, default=1)
    p.add_argument("--bets-max", type=int, default=4)
    p.add_argument("--abandon-prob", type=float, default=0.2,
                   help="probability a chain never completes (open partial; retained under WITHIN)")
    # Data layout
    p.add_argument("--payload-cols", type=int, default=0,
                   help="extra varchar payload columns p0..pN-1 (table schema must match)")
    p.add_argument("--payload-bytes", type=int, default=32, help="width of each payload column")
    # Time
    p.add_argument("--mode", choices=["bulk", "realtime"], default="bulk")
    p.add_argument("--rate", type=int, default=1000, help="rows/s pacing (realtime mode)")
    p.add_argument("--ties", type=int, default=1, help="events sharing each timestamp (tie density)")
    p.add_argument("--tick-gap", type=int, default=1, help="bulk mode: ts increment between tie groups")
    p.add_argument("--sentinel-partition", type=int, default=0,
                   help="partition key reserved for watermark sentinels")
    return p.parse_args()


def main():
    a = parse_args()
    rng = random.Random(a.seed)
    payload = lambda: "'" + ("x" * a.payload_bytes) + "'"
    pay_cols = "".join(f", {payload()}" for _ in range(a.payload_cols))

    # Per-partition chain state: None = idle, list = [remaining_bets] mid-chain.
    state = {}
    hot = set(range(1, a.hot_count + 1)) if a.hot_count else set()
    cold_lo = a.hot_count + 1

    def pick_partition():
        if hot and rng.random() < a.hot_share:
            return rng.randrange(1, a.hot_count + 1)
        return rng.randrange(cold_lo, a.partitions + 1)

    def next_event(pid):
        s = state.get(pid)
        if s is None:
            if rng.random() < a.abandon_prob:
                # Start a chain destined to be abandoned: emit the deposit, then forget it.
                return ("deposit", rng.randrange(50, 500))
            state[pid] = rng.randrange(a.bets_min, a.bets_max + 1)
            return ("deposit", rng.randrange(50, 500))
        if s > 0:
            state[pid] = s - 1
            return ("bet", rng.randrange(5, 50))
        del state[pid]
        return ("withdraw", rng.randrange(40, 450))

    tick = 10
    emitted = 0
    batch = []
    started = time.time()

    def ts_expr():
        if a.mode == "realtime":
            return "now()"
        return str(tick)

    print("set rw_implicit_flush to true;")
    while emitted < a.rows:
        for _ in range(a.ties):
            if emitted >= a.rows:
                break
            pid = pick_partition()
            kind, amount = next_event(pid)
            batch.append(f"({pid}, {ts_expr()}, '{kind}', {amount}{pay_cols})")
            emitted += 1
            if len(batch) >= a.batch:
                print(f"insert into {a.table} values " + ", ".join(batch) + ";")
                batch.clear()
                if a.mode == "realtime":
                    # Pace: sleep so cumulative rate approximates --rate.
                    ahead = emitted / a.rate - (time.time() - started)
                    if ahead > 0:
                        print(f"select pg_sleep({ahead:.3f});")
        tick += a.tick_gap
    if batch:
        print(f"insert into {a.table} values " + ", ".join(batch) + ";")
    # Watermark sentinel far past everything (bulk mode; realtime advances by itself).
    if a.mode == "bulk":
        print(f"insert into {a.table} values ({a.sentinel_partition}, {tick + 1_000_000}, 'noop', 0{pay_cols});")
    print(f"-- emitted {emitted} rows over {a.partitions} partitions "
          f"(hot: {a.hot_count} @ {a.hot_share if a.hot_count else 0}), "
          f"open partials ~{int(a.abandon_prob * 100)}%", file=sys.stderr)


if __name__ == "__main__":
    main()
