-- Partition-cardinality sweep: many partitions, each holding a small live window. Measures the
-- cost the PR discloses as "each watermark visits every partition". Setup only — feed with:
--
--   TABLE=t_perf PARTITIONS=10000 ROWS=200000 ./datagen/gen.sh
--
-- then watch compute-node CPU between inserts (idle-load = the per-watermark sweep) and
-- stream_match_recognize_evicted_rows_count for drain progress. Datagen alternates v = 5 / -5,
-- so the (a b) pattern below matches constantly and the buffer stays small per partition.

set rw_implicit_flush to true;

drop materialized view if exists mv_perf;
drop table if exists t_perf;

create table t_perf (id int, ts int, v int, watermark for ts as ts - 10) append only;

create materialized view mv_perf as
select * from t_perf match_recognize (
  partition by id
  order by ts
  measures b.v as dip
  one row per match
  after match skip past last row
  pattern (a b)
  define a as a.v > 0, b as b.v < 0
);

\echo 'setup done — run datagen, then: select count(*) from mv_perf;'
