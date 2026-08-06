-- A deposit, any number of bets, then a withdrawal — inside one minute. The shape an analyst
-- describes in a sentence, written as one clause instead of a hand-rolled join graph.
--
-- Two things to point at. Player 2 matches with b* matching ZERO times, so bets = 0 and
-- bet_total = NULL: a quantifier that matched nothing is not the same as no match at all. Player 1
-- deposits and bets but never withdraws, and correctly produces nothing.
--
-- Expand the plan: StreamMatchRecognize over StreamWatermarkSort over StreamExchange. That stack IS
-- the architecture — the exchange puts a partition's rows in one place, the sort puts them in
-- order, and the matcher does nothing but match. Ordering and matching are separate on purpose.
--
-- Leaves its objects behind, for poking at in the playground afterwards.

drop materialized view if exists pg_extraction;
drop table if exists pg_flow;

create table pg_flow (
  player int,
  ts timestamptz,
  kind varchar,
  amount int,
  watermark for ts as ts - interval '2' second
) append only;

create materialized view pg_extraction as
select * from pg_flow
match_recognize (
  partition by player
  order by ts
  measures
    d.ts as deposit_ts,
    d.amount as deposit_amount,
    w.ts as withdraw_ts,
    w.amount as withdraw_amount,
    count(b.amount) as bets,
    sum(b.amount) as bet_total
  one row per match
  after match skip past last row
  pattern (d b* w) within interval '60' second
  define
    d as d.kind = 'deposit',
    b as b.kind = 'bet',
    w as w.kind = 'withdraw'
) as m;

insert into pg_flow values
  (1, now() - interval '10' second, 'deposit',  500),
  (1, now() - interval '9' second,  'bet',      100),
  (2, now() - interval '8' second,  'deposit',  900),
  (2, now() - interval '7' second,  'withdraw', 850);

-- A match emits once the watermark passes its end, so a row beyond the batch releases it. Keep the
-- sentinel just past its own batch: one far in the future makes every LATER insert late, and late
-- rows are dropped silently.
insert into pg_flow values (99, now() + interval '5' second, 'sentinel', 0);
flush;
\echo 'expect: a brief settle, so the reads below are deterministic'
select pg_sleep(8) as settling;

\echo 'expect: one match — player 2, deposit 900 then withdraw 850, bets = 0 and bet_total NULL'
select * from pg_extraction;
