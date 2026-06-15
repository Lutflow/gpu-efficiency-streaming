-- 07_capacity_risk.sql
-- Predictive capacity risk: turn the next-window forecast into an actionable flag. When the
-- forecasted utilization for the upcoming window drops below threshold, raise PREDICTED_IDLE
-- *before* the idle window actually happens. This is the "predict, don't just detect" story.
--
-- [VERIFY-ISOLATED] Confirm the forecast ROW field name (forecast_value) against a live row
-- before wiring; the rule depends on it.
CREATE TABLE gpu_efficiency_capacity_risk
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  deployment_id,
  window_start,
  forecast.forecast_value AS predicted_util,
  'PREDICTED_IDLE'        AS risk_flag
FROM gpu_efficiency_forecast
WHERE forecast.forecast_value < 20;
