-- Hot-partition focus: ordered semantics preclude intra-partition parallelism, so one hot
-- partition serializes its actor's watermark pass and other partitions' emissions wait behind it
-- (design doc, "Parallelism"). Feed with an extreme skew and compare against a uniform run:
--   web/target/release/bench load --table t_hot --partitions 1000 --rows 500000 \
--     --hot-count 1 --hot-share 0.9 --abandon-prob 0.3
--   web/target/release/bench seal --table t_hot --mv mv_hot
-- vs --hot-count 0. Watch per-partition match arrival lag (max(ts) per partition below) and CPU.

set rw_implicit_flush to true;

drop materialized view if exists mv_hot;
drop table if exists t_hot;

create table t_hot (id int, ts int, kind varchar, amount int,
  watermark for ts as ts - 10) append only;

create materialized view mv_hot as
select * from t_hot match_recognize (
  partition by id
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within 5000
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

\echo 'setup done — feed with heavy skew (see header); progress per partition:'
\echo '  select id, count(*), max(ts) from mv_hot group by id order by count(*) desc limit 10;'
