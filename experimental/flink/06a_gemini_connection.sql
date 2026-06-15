-- 06a_gemini-connection.sql
-- Connection to Google AI (Gemini) for in-Flink model inference. The API key is injected at
-- deploy time from a gitignored variable (terraform.tfvars / TF_VAR_gemini_api_key) -- the
-- ${gemini_api_key} placeholder is NEVER committed with a real value. The connection must live
-- in the same region as the Flink compute pool.
CREATE CONNECTION `gemini-connection` WITH (
  'type' = 'googleai',
  'endpoint' = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',
  'api-key' = '${gemini_api_key}'
);
