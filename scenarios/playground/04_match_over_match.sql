-- MATCH_RECOGNIZE over MATCH_RECOGNIZE: two levels, bridged by a sink into a watermarked table.
--
-- Level 1 reads pairs of consecutive failures. Its output is fed through a sink into a table that
-- declares its own watermark, and level 2 then matches pairs OF THOSE PAIRS. The bridge is the
-- interesting part: a match view carries no watermark of its own, so the intermediate table is what
-- re-establishes the ordering guarantee the second matcher needs.
--
-- Level 2 lags level 1 by a barrier or two, hence the explicit settle before reading it — without
-- that wait the last select reads an empty view and looks like a failure.
--
-- Leaves its objects behind, for poking at in the playground afterwards.

drop materialized view if exists pg_l2;
drop sink if exists pg_l1_feed;
drop table if exists pg_l1_out;
drop materialized view if exists pg_l1;
drop table if exists pg_l0;

create table pg_l0 (
  player int, ts int, kind varchar,
  watermark for ts as ts - 1
) append only;

create materialized view pg_l1 as
select * from pg_l0
match_recognize (
  partition by player order by ts
  measures a.ts as start_ts, b.ts as end_ts
  one row per match after match skip past last row
  pattern (a b)
  define a as a.kind = 'fail', b as b.kind = 'fail'
) as m;

create table pg_l1_out (
  player int, start_ts int, end_ts int,
  watermark for end_ts as end_ts - 1
) append only;

-- `into <table> as <query>` — the `from` form takes a relation name, not a query.
create sink pg_l1_feed into pg_l1_out as
  select partition_0 as player, start_ts, end_ts from pg_l1;

create materialized view pg_l2 as
select * from pg_l1_out
match_recognize (
  partition by player order by end_ts
  measures x.end_ts as first_pair, y.end_ts as second_pair
  one row per match after match skip past last row
  pattern (x y)
  define x as x.player > 0, y as y.player > 0
) as m;

insert into pg_l0 values
  (5,1,'fail'),(5,2,'fail'),(5,3,'fail'),(5,4,'fail'),
  (5,5,'fail'),(5,6,'fail'),(5,7,'fail'),(5,8,'fail'),(5,999,'ok');
flush;
flush;
flush;

-- Level 2 is the slowest thing in this bench to become visible, and it is worth knowing why.
-- `flush` waits for a checkpoint on the DML path, so it drains the insert into pg_l0 — but the
-- bridge table is fed by a SINK, not by DML, and flush does not traverse that hop. The wait for
-- level 2 is therefore barrier-driven and can only be waited out. On a loaded machine this may
-- still read empty; re-run the last select, or read pg_l2 in the playground.
\echo 'expect: a settle long enough for the sink hop — level 2 is the slowest to appear'
select pg_sleep(12) as settling;

\echo 'expect: level 1 — four non-overlapping pairs, (1,2) (3,4) (5,6) (7,8)'
select * from pg_l1 order by start_ts;

\echo 'expect: the sink carried all four pairs into the intermediate table'
select player, start_ts, end_ts from pg_l1_out order by end_ts;

\echo 'expect: level 2 — one pair-of-pairs, first_pair 2 and second_pair 4'
select * from pg_l2 order by first_pair;
