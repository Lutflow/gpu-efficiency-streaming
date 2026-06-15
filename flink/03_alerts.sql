-- 03_alerts.sql
-- Project the anomaly stream into an actionable alerts table that the sink connectors consume.
-- The business rule turns a raw statistical anomaly into an efficiency verdict:
--   * util far BELOW the ARIMA forecast lower bound  -> IDLE_WASTE (GPU allocated but idle)
--   * util far ABOVE the forecast upper bound         -> SATURATION (request backpressure)
-- IDLE_WASTE is the core "reclaim wasted GPU spend" story this demo exists to tell.

CREATE TABLE gpu_efficiency_alerts
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  deployment_id,
  window_start,
  avg_gpu_util,
  anomaly.is_anomaly     AS is_anomaly,
  anomaly.forecast_value AS expected_util,
  anomaly.lower_bound    AS lower_bound,
  anomaly.upper_bound    AS upper_bound,
  CASE WHEN avg_gpu_util < anomaly.lower_bound THEN 'IDLE_WASTE'
       WHEN avg_gpu_util > anomaly.upper_bound THEN 'SATURATION'
       ELSE 'OK' END     AS efficiency_flag
FROM gpu_efficiency_anomalies
WHERE anomaly.is_anomaly = TRUE;
