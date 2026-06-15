-- 06c_remediation.sql
-- Agentic remediation: for each unified, deduplicated efficiency event, ask Gemini (in-Flink,
-- via the built-in AI_COMPLETE model-inference function) for ONE concise recommendation. The
-- pipeline RECOMMENDS; it does not act or enforce. Only the generic event summary is sent to the
-- LLM -- no proprietary data.
--
-- Runs over gpu_efficiency_events (already deduplicated to append-only) so the LLM is invoked
-- once per event -- safe for Gemini free-tier rate limits.
--
-- [VERIFY-ISOLATED] Confirm the AI_COMPLETE / LATERAL TABLE shape on 1-2 rows and that the
-- statement stays RUNNING (and volume is within rate limits) before wiring into Terraform.
CREATE TABLE gpu_efficiency_remediations
  DISTRIBUTED BY (`deployment_id`) INTO 1 BUCKETS
AS
SELECT
  e.deployment_id,
  e.window_start,
  e.event_type,
  r.recommendation
FROM gpu_efficiency_events AS e,
  LATERAL TABLE(AI_COMPLETE('remediation_model', e.summary)) AS r(recommendation);
