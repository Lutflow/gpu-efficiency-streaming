-- 04_datadog_metrics.sql  (OPTIONAL showcase -- only deployed when enable_datadog_sink = true)
-- Reshape each efficiency window into the metric record shape the Confluent Datadog Metrics Sink
-- expects: name, type, timestamp (epoch SECONDS), dimensions ARRAY<ROW<name,value>>, values ARRAY<ROW<value>>.
--
-- [VERIFY] Confirm the exact metric-record field names/types required by the Datadog Metrics Sink
-- against the connector docs before enabling in production; the reshape below follows the documented
-- struct but is the one signature in this PoC not yet validated against a live connector.
--
-- This emits one KPI metric (joules_per_1k_tokens). To emit the full metric set
-- (gpu_util_pct, generation_tokens, is_anomaly, idle_waste) use UNION ALL or a statement set.

CREATE TABLE gpu_efficiency_datadog AS
SELECT
  'lutflow.gpu.efficiency.joules_per_1k_tokens'                AS `name`,
  'gauge'                                                      AS `type`,
  CAST(UNIX_TIMESTAMP(CAST(window_start AS STRING)) AS BIGINT) AS `timestamp`,  -- epoch seconds
  ARRAY[ ROW('deployment_id', deployment_id) ]                 AS dimensions,    -- ARRAY<ROW<name,value>>
  ARRAY[ ROW(joules_per_1k_tokens) ]                           AS `values`       -- ARRAY<ROW<value DOUBLE>>
FROM gpu_efficiency_anomalies
WHERE joules_per_1k_tokens IS NOT NULL;
