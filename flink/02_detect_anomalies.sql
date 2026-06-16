-- 02_detect_anomalies.sql
-- Real-time GPU efficiency anomaly detection with Confluent Cloud's built-in ML
-- (ML_DETECT_ANOMALIES, ARIMA) running inline in Flink SQL -- no separate model-serving infra.
--
-- DESIGN: the demo monitors a SINGLE deployment_id. This matches the official
-- ML_DETECT_ANOMALIES example exactly: the function is used as an OVER window function with
-- `OVER (ORDER BY <time> ...)` and NO `PARTITION BY` (no official example uses PARTITION BY).
-- Two consequences, both intentional:
--   1. The statement is guaranteed to validate (it mirrors the documented pattern).
--   2. With a single stream, the within-window counter delta (MAX - MIN) is semantically valid.
-- Multi-deployment is a documented roadmap item (one templated statement per deployment, or
-- PARTITION BY once Confluent supports it for this function) -- see README "Roadmap".
--
-- WARMUP: with a 15s tumbling window and minTrainingSize=30 (enableStl=true needs a couple of
-- seasonal cycles), the first anomalies appear in ~7-8 minutes (30 windows). STL decomposition
-- is meaningful here because the producer emits a diurnal (seasonal) utilization signal.

CREATE TABLE gpu_efficiency_anomalies
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
WITH windowed AS (
  SELECT
    deployment_id,
    window_start,
    window_time,
    AVG(gpu_util_pct)                                              AS avg_gpu_util,
    AVG(num_requests_running)                                      AS avg_running,
    -- Counter deltas within the window. Valid because this is a single deployment / single stream:
    (MAX(generation_tokens_total) - MIN(generation_tokens_total))  AS gen_tokens_win,
    -- Prompt (prefill) tokens within the window -- so downstream "useful work" can count
    -- prefill + decode (goodput-style), not generation alone (see flink/08 + README Related work).
    (MAX(prompt_tokens_total) - MIN(prompt_tokens_total))          AS prompt_tokens_win,
    -- CAST to DOUBLE: the chained DECIMAL division (energy/1000 then / tokens) otherwise
    -- overflows DECIMAL(38) precision and Flink returns NULL for the KPI. DOUBLE avoids it.
    CAST(MAX(energy_mj) - MIN(energy_mj) AS DOUBLE) / 1000.0        AS energy_joules_win
  FROM TUMBLE(TABLE `gpu_telemetry`, DESCRIPTOR(`event_time`), INTERVAL '15' SECOND)
  WHERE deployment_id = 'inference-node-a'   -- demo single-key (multi-deployment = roadmap)
  GROUP BY deployment_id, window_start, window_time
)
SELECT
  deployment_id,
  window_start,
  avg_gpu_util,
  gen_tokens_win,
  prompt_tokens_win,
  -- Energy-efficiency KPI: DCGM energy (joules) per 1k useful generated tokens.
  -- DOUBLE throughout so the division never hits DECIMAL precision overflow (which emits NULL).
  energy_joules_win / NULLIF(CAST(gen_tokens_win AS DOUBLE), 0.0) * 1000.0   AS joules_per_1k_tokens,
  ML_DETECT_ANOMALIES(
    avg_gpu_util,                 -- monitored value (compute efficiency)
    window_time,                  -- timestamp
    JSON_OBJECT(
      'minTrainingSize'      VALUE 30,
      'enableStl'            VALUE true,
      'm'                    VALUE 12,
      'confidencePercentage' VALUE 95.0
    )
  ) OVER (
    ORDER BY window_time          -- doc-exact: NO PARTITION BY
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS anomaly
FROM windowed;
