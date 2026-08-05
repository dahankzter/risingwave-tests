-- The realtime pipeline: wall-clock event time, instrumented so decision latency can be measured
-- two different ways off the same traffic.
--
--   make bench          # does all of it and prints both numbers
--
-- or by hand:
--   make rt-setup                       # this file
--   make rt-load &                      # background traffic (realtime pacing)
--   make latency ROUNDS=20              # [1] client-side measurement
--   make lat-report                     # [2] server-side measurement
--
-- ---------------------------------------------------------------------------------------------
-- The pipeline, and where each measurement is taken:
--
--     insert  ->  t_rt  ->  mv_rt (MATCH_RECOGNIZE)  ->  sink  ->  t_rt_alerts
--                   |             |                                     |
--              ingest_ts     [1] probe stops here                  [2] alert_ts
--              (proctime)        (polls until the row appears)          (proctime)
--
--   [1] latency/probe.sh -- a client inserts a chain's completing event, then polls mv_rt until
--       the match appears. This is what a consumer polling the MV feels, including its own round
--       trips and the 20ms poll granularity. It samples only the chains the probe itself drives.
--
--   [2] latency/report.sql -- proctime() stamps every event on arrival (ingest_ts), the match
--       carries that stamp out as a measure, and t_rt_alerts stamps the match when the sink
--       delivers it (alert_ts). alert_ts - trigger_ingest_ts is that match's own delay, stored
--       beside it, so the distribution covers every match produced under the load.
--
-- The two land close together. Measured on the rig at 2k rows/s: [1] p50 6318ms over 8 rounds,
-- [2] p50 6048ms over 39801 matches. [2] waits one hop further (across the sink into a table),
-- but [1] adds a client round trip and 20ms of poll granularity, and those roughly cancel. Both
-- include the 5s watermark delay declared below, which dominates either figure -- tighten it to
-- trade late-event tolerance for alert speed.
--
-- The two only stay comparable if the probe does not advance the watermark itself. Standalone it
-- does (SENTINEL=on) so that it works with no background load, but those rows carry now(), which
-- outruns the paced generator's event time and releases the BACKGROUND traffic's matches early --
-- with it left on, [2] measured 3.4s against the 7.2s the same feed gives when measured alone.
-- `make bench` therefore runs the probe with SENTINEL=off and lets the feed drive the watermark.
-- ---------------------------------------------------------------------------------------------

set rw_implicit_flush to true;

drop sink if exists rt_feed;
drop table if exists t_rt_alerts;
drop materialized view if exists mv_rt;
drop table if exists t_rt;

create table t_rt (id int, ts timestamptz, kind varchar, amount int,
  -- Stamped when the event enters the table: the point the cluster took ownership of the row,
  -- which is what latency should be measured from. Generated, so writers never supply it --
  -- anything inserting here must name its columns explicitly.
  ingest_ts timestamptz as proctime(),
  watermark for ts as ts - interval '5' second) append only;

create materialized view mv_rt as
select * from t_rt match_recognize (
  partition by id
  order by ts
  measures last(w.ts) as ts, count(*) as chain_len, last(w.ingest_ts) as trigger_ingest_ts
  one row per match
  after match skip past last row
  pattern (d b+ w)
  within interval '10' minute
  define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw'
);

-- Matches land here and are stamped on arrival; the difference is the measured latency.
create table t_rt_alerts (partition_0 int, ts timestamptz, chain_len bigint,
  trigger_ingest_ts timestamptz, alert_ts timestamptz as proctime()) append only;

create sink rt_feed into t_rt_alerts from mv_rt;

\echo 'setup done — `make bench` runs the whole thing; see the header for the manual steps'
