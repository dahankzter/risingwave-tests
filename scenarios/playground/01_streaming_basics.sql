-- Streaming SQL with no MATCH_RECOGNIZE in sight: an append-only table with a watermark, and a
-- materialized view holding a running aggregate per kind. Worth running first because it
-- establishes what a materialized view IS here — a standing computation the engine maintains, not a
-- query re-run on demand — before pattern matching is layered on top.
--
-- The plan is worth expanding: StreamHashAgg carries an [append_only] tag, because a table declared
-- append only lets the aggregate skip the retraction machinery a general changelog would need.
--
-- Unlike a semantics check, this leaves its objects behind on purpose: switch to the playground and
-- the table and view are waiting there to be queried.

drop materialized view if exists pg_by_kind;
drop table if exists pg_tx;

create table pg_tx (
  player int,
  ts timestamptz,
  kind varchar,
  amount int,
  watermark for ts as ts - interval '2' second
) append only;

create materialized view pg_by_kind as
select kind, count(*) as n, sum(amount) as total, max(amount) as biggest
from pg_tx
group by kind;

insert into pg_tx values
  (1, now() - interval '10' second, 'deposit', 500),
  (1, now() - interval '9' second,  'bet',     100),
  (2, now() - interval '8' second,  'deposit', 900),
  (2, now() - interval '7' second,  'withdraw', 850);
flush;

\echo 'expect: three kinds — bet 1/100, deposit 2/1400 (biggest 900), withdraw 1/850'
select * from pg_by_kind order by kind;
