-- Catastrophic-backtracking probe: (a? a? ... a? b) over a run of a-rows is the classic
-- exponential shape. The pattern is path-independent (all DEFINEs read only the candidate row),
-- so the failure memo applies and the scan budget backstops the rest: this must complete
-- promptly, not pin a core. Watch stream_match_recognize_scan_budget_exhausted_count while it
-- runs; correctness expectation is at the end.

set rw_implicit_flush to true;
set streaming_parallelism = 1;

drop materialized view if exists mv_bt;
drop table if exists t_bt;

create table t_bt (id int, ts int, v int, watermark for ts as ts - 1) append only;

create materialized view mv_bt as
select * from t_bt match_recognize (
  partition by id
  order by ts
  measures count(*) as len
  one row per match
  after match skip past last row
  pattern (a? a? a? a? a? a? a? a? a? a? a? a? a? a? a? a? b)
  define a as a.v = 1, b as b.v = 2
);

-- 30 a-rows, no b: every start position explores the optional lattice and fails.
insert into t_bt
select 1, g, 1 from generate_series(10, 39) g;
insert into t_bt values (9, 100, 0);

\echo 'expect: 0 rows (no b anywhere), returned promptly'
select * from mv_bt;

-- A closing b: the greedy answer consumes the 16 optionals immediately before it plus b.
insert into t_bt values (1, 40, 2), (9, 200, 0);
\echo 'expect: one row, len = 17'
select * from mv_bt;

drop materialized view mv_bt;
drop table t_bt;
