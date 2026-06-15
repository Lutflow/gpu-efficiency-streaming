-- 06c_remediation.sql
-- Agentic remediation: for each flagged efficiency anomaly, ask Gemini (in-Flink, via the
-- built-in AI_COMPLETE model-inference function) for ONE concise recommendation. The pipeline
-- RECOMMENDS; it does not act/enforce. Only the generic anomaly row is sent to the LLM.
--
-- [VERIFY-ISOLATED] Confirm the AI_COMPLETE / LATERAL TABLE invocation shape against a single
-- test row before wiring into Terraform; only wire if the statement reaches RUNNING.
CREATE TABLE gpu_efficiency_remediations
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  a.deployment_id,
  a.window_start,
  a.efficiency_flag,
  a.avg_gpu_util,
  r.recommendation
FROM gpu_efficiency_alerts AS a,
  LATERAL TABLE(
    AI_COMPLETE(
      'remediation_model',
      'GPU efficiency anomaly.'
        || ' deployment=' || a.deployment_id
        || ' flag=' || a.efficiency_flag
        || ' avg_util=' || CAST(a.avg_gpu_util AS STRING)
        || ' expected_util=' || CAST(a.expected_util AS STRING)
        || ' lower_bound=' || CAST(a.lower_bound AS STRING)
        || ' upper_bound=' || CAST(a.upper_bound AS STRING)
    )
  ) AS r(recommendation);
