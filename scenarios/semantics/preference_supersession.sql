-- Preference supersession: a provisional match must be HELD while a more-preferred alternative
-- is still in flight — "a later row exists" is not finality.
-- Mirrors e2e_test/streaming/match_recognize_preference_supersession.slt from the PR branch.

set rw_implicit_flush to true;
set streaming_parallelism = 1;

drop materialized view if exists mv_same_start;
drop materialized view if exists mv_gap;
drop table if exists t_pref;

create table t_pref (id int, ts int, v int, watermark for ts as ts - 1) append only;

create materialized view mv_same_start as
select * from t_pref match_recognize (
  partition by id
  order by ts
  measures count(*) as len, classifier() as last_var
  one row per match
  after match skip past last row
  pattern (a b c d | a b)
  define a as a.v = 1, b as b.v = 2, c as c.v = 3, d as d.v = 4
);

-- Partition 1: a,b,c buffered. Preferred branch (a b c d) is blocked at the buffer boundary,
-- so the followed (a b) must NOT emit.
insert into t_pref values (1, 10, 1), (1, 11, 2), (1, 12, 3), (9, 14, 0);
\echo 'expect: 0 rows'
select * from mv_same_start;

-- The d arrives: the single four-row match is the batch answer.
insert into t_pref values (1, 13, 4), (9, 15, 0);
\echo 'expect: (1, 4, d)'
select * from mv_same_start;

-- Partition 2: killing row decides the held (a b) — it must emit now.
insert into t_pref values (2, 20, 1), (2, 21, 2), (2, 22, 9), (9, 24, 0);
\echo 'expect: (1, 4, d) and (2, 2, b)'
select * from mv_same_start order by partition_0;

create materialized view mv_gap as
select * from t_pref match_recognize (
  partition by id
  order by ts
  measures count(*) as len, classifier() as last_var
  one row per match
  after match skip past last row
  pattern (x n n | n)
  define x as x.v = 7, n as n.v = 8
);

-- Partition 3: x,n buffered. The lone (n) is terminal from its own start, but position 0 is
-- alive inside the preferred branch: nothing may emit.
insert into t_pref values (3, 30, 7), (3, 31, 8), (9, 33, 0);
\echo 'expect: 0 rows'
select * from mv_gap;

-- Second n: batch answer is the single (x n n).
insert into t_pref values (3, 32, 8), (9, 34, 0);
\echo 'expect: (3, 3, n)'
select * from mv_gap;

-- Partition 5: n with no x before it — empty gap, terminal, must emit at arrival (starvation
-- control).
insert into t_pref values (5, 50, 8), (9, 52, 0);
\echo 'expect: (3, 3, n) and (5, 1, n)'
select * from mv_gap order by partition_0;

drop materialized view mv_same_start;
drop materialized view mv_gap;
drop table t_pref;
