-- 09_remediation.sql
-- Rule-based remediation recommender -- reference-aligned with the Confluent Streaming Agents pattern
-- (investigate -> decide -> act), but deterministic (CASE rules, NO LLM). It consumes the governed
-- signals -- idle/saturation alerts (flink/03) and high-util waste (flink/08) -- and emits, per
-- deployment, a recommended action plus an *illustrative* reclaimable-cost estimate (the real number
-- the platform should use is the measured idle/low-efficiency fraction per deployment; see the case
-- study Business-impact section). Closes the loop: detect -> forecast -> govern -> recommend.
CREATE TABLE gpu_remediation
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  deployment_id,
  window_start,
  condition_flag,
  CASE condition_flag
    WHEN 'IDLE_WASTE'      THEN 'Consolidate or scale down: GPU allocated but idle'
    WHEN 'SATURATION'      THEN 'Scale out or rate-limit: GPU saturated, latency at risk'
    WHEN 'WASTE_HIGH_UTIL' THEN 'Raise batch concurrency or right-size the model: high util, low useful throughput'
    ELSE 'Review deployment'
  END                                                          AS recommended_action,
  -- Illustrative: a g2-standard-8 (1x L4) is ~$621/GPU/month; these are projection multipliers, not a
  -- measured saving. The product value is measuring the real reclaimable fraction per deployment.
  CAST(CASE condition_flag
    WHEN 'IDLE_WASTE'      THEN 621.0 * 0.40
    WHEN 'WASTE_HIGH_UTIL' THEN 621.0 * 0.25
    ELSE 0.0
  END AS DOUBLE)                                               AS est_reclaimable_usd_per_mo
FROM (
  SELECT deployment_id, window_start, efficiency_flag AS condition_flag FROM gpu_efficiency_alerts
  UNION ALL
  SELECT deployment_id, window_start, waste_flag      AS condition_flag FROM gpu_efficiency_waste
);
