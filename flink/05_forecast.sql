-- 05_forecast.sql
-- Forecast the GPU efficiency trend with Confluent's built-in ML_FORECAST (ARIMA) so the
-- pipeline *predicts* efficiency degradation, not just detects it after the fact. This is a
-- parallel branch off the same governed gpu_telemetry stream (mirrors Confluent's own Lab 3).
--
-- ML_FORECAST is an OVER window function (ORDER BY event time, no PARTITION BY) returning a ROW
-- with forecast_value / lower_bound / upper_bound / actual_value / rmse / aic. enableStl=true uses
-- Seasonal-Trend decomposition, which is meaningful now that the producer emits a diurnal signal.
CREATE TABLE gpu_efficiency_forecast
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
WITH windowed AS (
  SELECT
    deployment_id,
    window_start,
    window_time,
    AVG(gpu_util_pct) AS avg_gpu_util
  FROM TUMBLE(TABLE `gpu_telemetry`, DESCRIPTOR(`event_time`), INTERVAL '15' SECOND)
  WHERE deployment_id = 'inference-node-a'
  GROUP BY deployment_id, window_start, window_time
)
SELECT
  deployment_id,
  window_start,
  avg_gpu_util,
  ML_FORECAST(
    avg_gpu_util,
    window_time,
    JSON_OBJECT('minTrainingSize' VALUE 36, 'enableStl' VALUE TRUE, 'm' VALUE 12, 'horizon' VALUE 1)
  ) OVER (
    ORDER BY window_time
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS forecast
FROM windowed;
