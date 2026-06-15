-- 06b_gemini_model.sql
-- A text-generation model that turns one GPU efficiency anomaly into one concise, actionable
-- remediation recommendation. Only the generic anomaly row is ever sent to the LLM -- no
-- proprietary data, no model internals.
CREATE MODEL `remediation_model`
  INPUT (`text` VARCHAR(2147483647))
  OUTPUT (`recommendation` VARCHAR(2147483647))
  WITH (
    'provider' = 'googleai',
    'googleai.connection' = 'gemini_connection',
    'googleai.system_prompt' = 'You are a GPU FinOps assistant. Given a GPU efficiency anomaly (utilization, forecast bounds, flag), reply with ONE concise remediation recommendation and an estimated action. No preamble.',
    'task' = 'text_generation'
  );
