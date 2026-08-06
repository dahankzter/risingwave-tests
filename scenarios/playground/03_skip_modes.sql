-- The same data and the same pattern, twice, differing in one clause: AFTER MATCH SKIP. An
-- escalating stake — 10, 20, 40, 80, 160 — read as strictly-increasing triples.
--
-- SKIP PAST LAST ROW consumes the match and resumes after it, so the triples cannot overlap: one
-- row. SKIP TO NEXT ROW resumes one row in, so every starting position is tried: three rows. Side
-- by side, that turns "which matches does it report" from an argument into an observation.
--
-- Then a third view aggregates the first one, because a match view is an ordinary view — the point
-- being that pattern matching composes with the rest of streaming SQL rather than terminating it.
--
-- Leaves its objects behind, for poking at in the playground afterwards.

drop materialized view if exists pg_risk_by_player;
drop materialized view if exists pg_climb_overlap;
drop materialized view if exists pg_climb_past;
drop table if exists pg_bets;

create table pg_bets (
  player int, ts int, stake int,
  watermark for ts as ts - 1
) append only;

create materialized view pg_climb_past as
select * from pg_bets
match_recognize (
  partition by player order by ts
  measures a.ts as t1, a.stake as s1, c.ts as t3, c.stake as s3
  one row per match
  after match skip past last row
  pattern (a b c)
  define a as a.stake > 0, b as b.stake > a.stake, c as c.stake > b.stake
) as m;

create materialized view pg_climb_overlap as
select * from pg_bets
match_recognize (
  partition by player order by ts
  measures a.ts as t1, a.stake as s1, c.ts as t3, c.stake as s3
  one row per match
  after match skip to next row
  pattern (a b c)
  define a as a.stake > 0, b as b.stake > a.stake, c as c.stake > b.stake
) as m;

create materialized view pg_risk_by_player as
select partition_0 as player, count(*) as escalations, max(s3) as peak_stake
from pg_climb_past group by partition_0;

insert into pg_bets values (7,1,10),(7,2,20),(7,3,40),(7,4,80),(7,5,160),(7,99,0);
flush;

-- `flush` waits for a checkpoint, so it drains the insert deterministically; the sleep covers the
-- barrier hop the three views then need to emit. Both, because either alone has raced here.
\echo 'expect: a brief settle, so the reads below are deterministic'
select pg_sleep(5) as settling;

\echo 'expect: SKIP PAST LAST ROW — one non-overlapping triple, 10 at t1 climbing to 40 at t3'
select * from pg_climb_past order by t1;

\echo 'expect: SKIP TO NEXT ROW — three overlapping triples, starting at 10, 20 and 40'
select * from pg_climb_overlap order by t1;

\echo 'expect: the aggregate over match output — player 7, 1 escalation, peak stake 40'
select * from pg_risk_by_player;
