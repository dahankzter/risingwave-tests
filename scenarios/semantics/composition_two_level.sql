-- Hierarchical composition: MATCH_RECOGNIZE over MATCH_RECOGNIZE via an intermediate watermarked
-- table (the operator forwards no watermark, so the table re-derives one from the match rows).
-- Verified end-to-end on the PR build 2026-08-05; this pins it against every published image.
--
-- The load-bearing number: the intermediate table's lateness allowance (15 min) must exceed the
-- level-1 WITHIN bound (10 min) plus upstream lateness (1 min) — a WITHIN-held level-1 match can
-- emit up to that long after its own ts measure, and a smaller allowance silently drops exactly
-- those matches at level 2.

set rw_implicit_flush to true;
set streaming_parallelism = 1;

drop materialized view if exists meta_rings;
drop sink if exists feed;
drop materialized view if exists rings;
drop table if exists ring_events;
drop table if exists events;

create table events (card int, ts timestamp, kind varchar, amount int,
  watermark for ts as ts - interval '1' minute) append only;

-- Level 1: per-card deposit -> bets -> withdraw chains within 10 minutes.
create materialized view rings as
select * from events match_recognize (
  partition by card
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within interval '10' minute
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

-- Level-2 source: intermediate table declaring its OWN watermark.
create table ring_events (partition_0 int, ts timestamp, chain_len bigint,
  watermark for ts as ts - interval '15' minute) append only;

create sink feed into ring_events from rings;

-- Level 2: three level-1 matches within an hour on the same card.
create materialized view meta_rings as
select * from ring_events match_recognize (
  partition by partition_0
  order by ts
  measures count(*) as rings, last(m.ts) as last_ring_ts
  one row per match
  after match skip past last row
  pattern (m m m)
  within interval '1' hour
  define m as true
);

-- Four rings on card 1; the fourth advances the intermediate table's watermark past the third,
-- releasing rings 1-3 into the level-2 matcher.
insert into events values
  (1, '2026-08-05 10:00:00', 'deposit', 100), (1, '2026-08-05 10:01:00', 'bet', 40), (1, '2026-08-05 10:02:00', 'withdraw', 90),
  (1, '2026-08-05 10:15:00', 'deposit', 100), (1, '2026-08-05 10:16:00', 'bet', 40), (1, '2026-08-05 10:17:00', 'withdraw', 90),
  (1, '2026-08-05 10:30:00', 'deposit', 100), (1, '2026-08-05 10:31:00', 'bet', 40), (1, '2026-08-05 10:32:00', 'withdraw', 90),
  (1, '2026-08-05 10:50:00', 'deposit', 100), (1, '2026-08-05 10:51:00', 'bet', 40), (1, '2026-08-05 10:52:00', 'withdraw', 90),
  (0, '2026-08-05 11:30:00', 'noop', 0);

-- The internal sink is asynchronous (not covered by implicit flush across the sink boundary).
select pg_sleep(3);

\echo 'expect: 4 level-1 rings'
select * from rings order by ts;

\echo 'expect: one meta match — (1, 3, 2026-08-05 10:32:00)'
select * from meta_rings;

drop materialized view meta_rings;
drop sink feed;
drop materialized view rings;
drop table ring_events;
drop table events;
