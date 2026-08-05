#!/usr/bin/env python3
"""Workload generator for the MATCH_RECOGNIZE bench. Emits SQL INSERTs on stdout; pipe to psql.

The event shape is a fraud-flavored chain per partition: deposit -> bet{1..n} -> withdraw. A chain
that completes forms a match for the standard test pattern (d b+ w); an abandoned chain (no
withdraw) leaves an open partial — under WITHIN, exactly the retained-state regime whose cost the
operator's design doc discloses.

Examples:
  # 1M rows over 10k partitions, 10 hot partitions taking half the traffic, 20% chains abandoned
  ./gen.py --table t_perf --partitions 10000 --rows 1000000 --hot-count 10 --hot-share 0.5 \
           --abandon-prob 0.2 | psql -h 127.0.0.1 -p 4566 -d dev -U root

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
from datetime import datetime, timezone


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
    a = p.parse_args()
    # Cold partitions occupy (hot_count, partitions]; an empty range would blow up inside
    # rng.randrange with a bare ValueError halfway through a run.
    if a.hot_count and a.hot_count >= a.partitions:
        p.error("--hot-count must be < --partitions (no cold partitions would be left)")
    if a.rate <= 0:
        p.error("--rate must be positive")
    if a.ties < 1:
        p.error("--ties must be at least 1")
    return a


def main():
    a = parse_args()
    rng = random.Random(a.seed)
    payload = lambda: "'" + ("x" * a.payload_bytes) + "'"
    pay_cols = "".join(f", {payload()}" for _ in range(a.payload_cols))
    # Name the columns explicitly: the realtime table carries a generated proctime column
    # (ingest_ts), so a positional INSERT would not line up with the table shape.
    col_list = "(id, ts, kind, amount" + "".join(f", p{i}" for i in range(a.payload_cols)) + ")"

    # Per-partition chain state: absent = idle, [remaining_bets, abandoned] = mid-chain.
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
            state[pid] = [rng.randrange(a.bets_min, a.bets_max + 1),
                          rng.random() < a.abandon_prob]
            return ("deposit", rng.randrange(50, 500))
        if s[0] > 0:
            s[0] -= 1
            return ("bet", rng.randrange(5, 50))
        del state[pid]
        if s[1]:
            # Abandoned: the chain runs its bets and then simply stops, so the partition's next
            # event opens a fresh chain and the `d b+` prefix stays buffered until its WITHIN
            # bound expires. That retained partial is the state regime worth measuring —
            # abandoning at the deposit instead would leave almost nothing behind.
            return next_event(pid)
        return ("withdraw", rng.randrange(40, 450))

    tick = 10
    group = 0
    emitted = 0
    batch = []
    started = time.time()

    def ts_expr():
        if a.mode == "realtime":
            # One wall-clock instant per tie group, spaced at the pacing rate. Server-side now()
            # cannot be used here: it is fixed per statement, so an entire --batch of rows would
            # land on a single timestamp — collapsing --ties into batch-sized tie groups and
            # leaving a chain's deposit/bet/withdraw mutually unordered under ORDER BY ts (the
            # pattern then matches by luck or not at all).
            t = started + group * a.ties / a.rate
            return "'" + datetime.fromtimestamp(t, timezone.utc).isoformat(sep=" ") + "'"
        return str(tick)

    if a.mode == "realtime":
        # Per-statement barrier: the rows have to become visible as they are produced, which is
        # the point of realtime mode. At a few statements per second the barrier cost is noise.
        print("set rw_implicit_flush to true;")
    else:
        # Bulk mode explicitly does NOT flush per statement. Doing so costs a barrier round trip
        # per INSERT and caps ingest at roughly 9k rows/s on the rig; without it the same feed
        # runs at ~88k rows/s, so the per-statement flush -- not the operator -- was what the
        # throughput numbers measured.
        print("set rw_implicit_flush to false;")
    while emitted < a.rows:
        for _ in range(a.ties):
            if emitted >= a.rows:
                break
            pid = pick_partition()
            kind, amount = next_event(pid)
            batch.append(f"({pid}, {ts_expr()}, '{kind}', {amount}{pay_cols})")
            emitted += 1
            if len(batch) >= a.batch:
                print(f"insert into {a.table} {col_list} values " + ", ".join(batch) + ";")
                batch.clear()
                if a.mode == "realtime":
                    # Sleep until an ABSOLUTE wall-clock target, evaluated server-side, rather
                    # than for a fixed duration. Event timestamps are the same schedule, so this
                    # keeps event time pinned to real time.
                    #
                    # A fixed pg_sleep cannot do that. Sleeping the exact increment makes total
                    # sleep rows/rate, which ignores the time the INSERTs themselves take, so the
                    # schedule creeps ahead of the wall clock. Once it leads by more than the
                    # watermark delay, the table's watermark sits in the future and anything
                    # inserted with now() -- the latency probe's own rows -- is dropped as late.
                    # That showed up as probe rounds climbing 6s, 24s, 31s, timeout.
                    # (Sleeping the cumulative target instead is worse still: 4000 rows at 2000/s
                    # slept 8.97s rather than 2s.)
                    target = datetime.fromtimestamp(started + emitted / a.rate, timezone.utc)
                    print("select pg_sleep(greatest(0, extract(epoch from ("
                          f"'{target.isoformat(sep=' ')}'::timestamptz - now()))));")
        tick += a.tick_gap
        group += 1
    if batch:
        print(f"insert into {a.table} {col_list} values " + ", ".join(batch) + ";")
    # No watermark sentinel is emitted here, deliberately: sealing a bulk feed is a separate step
    # (datagen/seal.sh), because it is only safe once the pipeline has actually drained.
    #
    # `flush` returns while the MV is still catching up. Measured on the rig with a 200k-row feed:
    # immediately after the final flush the MV held 3917 matches, and five seconds later it held
    # 10624 with nothing further inserted. Sending the far-future sentinel inside that window
    # froze the MV at 3917 permanently -- the watermark discards the rows still in flight rather
    # than matching them, and they never come back. An inline sentinel loses ~63% of the matches.
    #
    # Bulk mode therefore emits the data only; seal.sh waits for the match count to stop moving
    # before advancing the watermark. (scenarios/adversarial/backtracking.sql describes the
    # neighbouring hazard: a sentinel placed too far ahead of rows that have not arrived yet.)
    note = (f"-- emitted {emitted} rows over {a.partitions} partitions "
            f"(hot: {a.hot_count} @ {a.hot_share if a.hot_count else 0}), "
            f"{len(state)} chains left open, ~{int(a.abandon_prob * 100)}% abandon rate")
    if a.mode == "realtime":
        # Pacing is server-side: the stream carries a pg_sleep per batch and psql executes them,
        # so this process cannot observe the ingest rate. Timing generation here would measure the
        # pipe buffer -- a short run is emitted and exits long before psql has run the sleeps.
        note += (f", paced at {a.rate} rows/s server-side "
                 f"(~{emitted / a.rate:.0f}s of traffic once psql has drained it)")
    print(note, file=sys.stderr)


if __name__ == "__main__":
    main()
