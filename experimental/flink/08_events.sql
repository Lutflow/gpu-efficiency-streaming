-- 08_events.sql
-- Append-only stream of efficiency events that feeds the agentic remediation step.
--
-- AI_COMPLETE is a non-deterministic function and Flink only allows it over an APPEND-ONLY
-- ('insert-only') stream -- not over the updating (retract) streams produced by the OVER-window
-- ML functions. A TUMBLE GROUP BY aggregation is naturally append-only (one row per window,
-- emitted once), so we derive idle-window events directly from the governed telemetry and flag
-- the windows whose average utilization is low (idle-but-allocated GPU -- the waste we remediate).
CREATE TABLE gpu_efficiency_events
  DISTRIBUTED BY (`deployment_id`, `window_start`) INTO 1 BUCKETS
AS
SELECT
  deployment_id,
  window_start,
  'IDLE_WINDOW' AS event_type,
  'GPU efficiency event: deployment=' || deployment_id
    || ' avg_util=' || CAST(avg_gpu_util AS STRING)
    || ' window=' || CAST(window_start AS STRING)
    || ' -- idle-but-allocated GPU; recommend a remediation.' AS summary
FROM (
  SELECT
    deployment_id,
    window_start,
    AVG(gpu_util_pct) AS avg_gpu_util
  FROM TUMBLE(TABLE `gpu_telemetry`, DESCRIPTOR(`event_time`), INTERVAL '15' SECOND)
  WHERE deployment_id = 'inference-node-a'
  GROUP BY deployment_id, window_start
)
WHERE avg_gpu_util < 25;
