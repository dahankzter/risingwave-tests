-- Setup for bulk-mode load runs (integer ticks as event time; datagen supplies the sentinel).
-- Feed with e.g.:
--   python3 datagen/gen.py --table t_perf --partitions 10000 --rows 1000000 \
--     --hot-count 10 --hot-share 0.5 --abandon-prob 0.2 | make psql
--   TABLE=t_perf MV=mv_perf ./datagen/seal.sh
--
-- The seal is a separate step: a far-future sentinel delivered while the pipeline is still
-- draining discards the rows in flight rather than matching them (see datagen/seal.sh). Or just
-- `make load-setup && make load PROFILE=fraud`, which does both.
-- Watch: rows-per-second the cluster absorbs, stream_match_recognize_* counters, and idle-load
-- CPU after the feed stops (that is the per-watermark sweep over retained partials).

set rw_implicit_flush to true;

drop materialized view if exists mv_perf;
drop table if exists t_perf;

create table t_perf (id int, ts int, kind varchar, amount int,
  watermark for ts as ts - 10) append only;

create materialized view mv_perf as
select * from t_perf match_recognize (
  partition by id
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len, sum(b.amount) as staked
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within 5000
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

\echo 'setup done — feed with datagen/gen.py (see header), then: select count(*) from mv_perf;'
