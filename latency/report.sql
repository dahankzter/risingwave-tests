-- Decision-latency distribution over every match the cluster produced, from the proctime stamps
-- set up by scenarios/perf/setup_latency.sql. Seconds.
--
-- The declared watermark delay (5s on t_lat) is part of every figure here and dominates it, so
-- read p50 as "watermark delay + pipeline", not as pipeline alone. min is the useful lower bound
-- on what the pipeline itself costs.

select
  count(*)                                                          as matches,
  round(min(secs)::numeric, 3)                                      as min_s,
  round(percentile_cont(0.50) within group (order by secs)::numeric, 3) as p50_s,
  round(percentile_cont(0.95) within group (order by secs)::numeric, 3) as p95_s,
  round(percentile_cont(0.99) within group (order by secs)::numeric, 3) as p99_s,
  round(max(secs)::numeric, 3)                                      as max_s
from (
  select extract(epoch from (alert_ts - trigger_ingest_ts)) as secs
  from t_lat_alerts
) s;
