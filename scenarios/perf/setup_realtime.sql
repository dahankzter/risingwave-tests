-- Setup for realtime-mode runs: wall-clock timestamps, so latency/probe.sh can measure the
-- insert->alert delay while datagen supplies background load:
--   python3 datagen/gen.py --table t_rt --mode realtime --rate 2000 --rows 200000 \
--     --partitions 5000 --hot-count 5 --hot-share 0.4 | make psql     # terminal 1
--   ROUNDS=20 ./latency/probe.sh                                      # terminal 2
-- The 5s watermark delay bounds how long the sort holds rows; the probe's reported latency
-- includes it (that is the honest end-to-end number an alerting consumer sees).

set rw_implicit_flush to true;

drop materialized view if exists mv_rt;
drop table if exists t_rt;

create table t_rt (id int, ts timestamptz, kind varchar, amount int,
  watermark for ts as ts - interval '5' second) append only;

create materialized view mv_rt as
select * from t_rt match_recognize (
  partition by id
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within interval '10' minute
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

\echo 'setup done — start datagen (realtime) and run latency/probe.sh'
