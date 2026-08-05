-- Server-side decision latency: every match carries its own end-to-end delay, measured by the
-- cluster, so the number comes from the real load rather than from synthetic probe rounds.
--
--   make lat-setup && make lat-load          # feed it (realtime pacing, wall-clock ts)
--   make lat-report                          # p50/p95/p99 over every match produced
--
-- How the timing works. `proctime()` is a generated column evaluated when a row enters a table,
-- so two of them bracket the pipeline:
--
--   t_lat.ingest_ts        when the event arrived
--   t_lat_alerts.alert_ts  when the resulting match landed back in a table
--
-- MATCH_RECOGNIZE carries the completing row's ingest_ts out as a measure, and the sink feeds the
-- match into t_lat_alerts, which stamps its own arrival. alert_ts - trigger_ingest_ts is then the
-- insert-to-alert delay for that specific match, stored alongside it.
--
-- Versus latency/probe.sh: the probe times a client round trip (insert, then poll the MV until
-- the row appears), which is what an alerting consumer actually feels, but it only samples the
-- handful of chains it drives itself and its resolution is the poll interval. This measures every
-- match under the real workload -- thousands of samples, no polling.
--
-- This reads HIGHER than the probe, not lower: the probe stops when the row appears in the MV,
-- whereas this waits one hop further, for the match to cross the sink into t_lat_alerts. Measured
-- on the rig at 2k rows/s: probe p50 6448ms over 20 rounds, this p50 7334ms over 7660 matches.
-- The gap is the sink. Neither is wrong -- read the probe as "when a consumer polling the MV sees
-- it" and this as "when a downstream table has it", and keep both.
--
-- The 5s watermark delay on t_lat is included in the measurement, and dominates it.

set rw_implicit_flush to true;

drop materialized view if exists mv_lat_alerts;
drop sink if exists lat_feed;
drop table if exists t_lat_alerts;
drop materialized view if exists mv_lat;
drop table if exists t_lat;

create table t_lat (id int, ts timestamptz, kind varchar, amount int,
  -- Stamped when the event enters the table, not when it was generated: this is the point the
  -- cluster took ownership of the row, which is what the latency should be measured from.
  ingest_ts timestamptz as proctime(),
  watermark for ts as ts - interval '5' second) append only;

create materialized view mv_lat as
select * from t_lat match_recognize (
  partition by id
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len, last(w.ingest_ts) as trigger_ingest_ts
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within interval '10' minute
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

-- The match lands here and is stamped on arrival; the difference is the latency.
create table t_lat_alerts (partition_0 int, ts timestamptz, chain_len bigint,
  trigger_ingest_ts timestamptz, alert_ts timestamptz as proctime()) append only;

create sink lat_feed into t_lat_alerts from mv_lat;

\echo 'setup done — feed with `make lat-load`, then `make lat-report`'
