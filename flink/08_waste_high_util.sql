-- 08_waste_high_util.sql
-- "Utilization lies" waste detector: high GPU utilization but LOW useful throughput = energy wasted
-- on under-batched work -- exactly the pattern the measured efficiency frontier exposed at low
-- concurrency (util ~100% yet ~27x the energy per token). Reads the anomaly stream (which carries
-- avg_gpu_util and the within-window generated-token delta) and flags WASTE_HIGH_UTIL.
--
-- No ML / no model inference -- a cheap, deterministic filter. The downstream remediation agent
-- (flink/09) consumes this alongside the idle/saturation alerts.
CREATE TABLE gpu_efficiency_waste
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  deployment_id,
  window_start,
  avg_gpu_util,
  gen_tokens_win,
  CAST(gen_tokens_win AS DOUBLE) / 15.0   AS tokens_per_sec,   -- 15s tumbling window
  'WASTE_HIGH_UTIL'                       AS waste_flag
FROM gpu_efficiency_anomalies
WHERE avg_gpu_util >= 90
  AND gen_tokens_win <= 1000;   -- high utilization but <~66 useful tokens/s over the window
