-- 08_events.sql
-- Unified, deduplicated, append-only event stream that feeds the agentic remediation step.
-- It merges REACTIVE anomalies (detected now) with PREDICTED capacity risk (forecast ahead),
-- normalized to a common shape with a prompt-ready `summary`.
--
-- Why dedup BEFORE the LLM: the upstream alerts/capacity_risk are updating (retract) streams.
-- Deduplicating to one row per (deployment_id, window_start, event_type) makes this append-only
-- so Gemini is invoked exactly once per event -- avoiding duplicate calls, rate-limit, and a
-- failed (red) node in the lineage.
--
-- [VERIFY-ISOLATED] Confirm the UNION ALL + ROW_NUMBER dedup yields an append-only stream.
CREATE TABLE gpu_efficiency_events
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT deployment_id, window_start, event_type, summary
FROM (
  SELECT
    deployment_id, window_start, event_type, summary,
    ROW_NUMBER() OVER (
      PARTITION BY deployment_id, window_start, event_type
      ORDER BY window_start
    ) AS rn
  FROM (
    SELECT
      deployment_id,
      window_start,
      'REACTIVE_' || efficiency_flag AS event_type,
      'Reactive GPU efficiency anomaly:'
        || ' deployment=' || deployment_id
        || ' flag=' || efficiency_flag
        || ' avg_util=' || CAST(avg_gpu_util AS STRING)
        || ' expected_util=' || CAST(expected_util AS STRING) AS summary
    FROM gpu_efficiency_alerts
    UNION ALL
    SELECT
      deployment_id,
      window_start,
      'PREDICTED_IDLE' AS event_type,
      'Predicted GPU idle risk:'
        || ' deployment=' || deployment_id
        || ' predicted_util=' || CAST(predicted_util AS STRING) AS summary
    FROM gpu_efficiency_capacity_risk
  )
)
WHERE rn = 1;
