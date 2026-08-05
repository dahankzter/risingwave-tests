-- EMIT ON WINDOW CLOSE on MATCH_RECOGNIZE is accepted: the operator's output is final-only, so
-- the clause names behavior it already has (it adds nothing). Queries written for the earlier
-- EOWC-required builds (e.g. the Fraud PoC detectors) must run unchanged.

set rw_implicit_flush to true;
set streaming_parallelism = 1;

drop materialized view if exists mv_eowc;
drop table if exists t_eowc;

create table t_eowc (id int, ts int, v int, watermark for ts as ts - 1) append only;

create materialized view mv_eowc as
select * from t_eowc match_recognize (
  partition by id
  order by ts
  measures b.v as dip
  one row per match
  after match skip past last row
  pattern (a b)
  define a as a.v > 0, b as b.v < 0
) emit on window close;

insert into t_eowc values (1, 10, 5), (1, 11, -3), (1, 12, 1), (9, 14, 0);
\echo 'expect: (1, -3)'
select * from mv_eowc;

drop materialized view mv_eowc;
drop table t_eowc;
