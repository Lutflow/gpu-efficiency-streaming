-- 08_waste_high_util.sql
-- "Utilization lies" waste detector: high GPU utilization but LOW *useful* throughput = energy
-- wasted on under-batched work -- exactly the pattern the measured efficiency frontier exposed at
-- low concurrency (util ~100% yet ~27x the energy per useful token).
--
-- USEFUL WORK = prefill + decode. We measure useful_tokens_win = prompt_tokens_win + gen_tokens_win
-- (goodput-style accounting, see README "Related work"), NOT generation alone. This matters for
-- correctness: a long-context / prefill-heavy request (big prompt, few output tokens) does real GPU
-- work during prefill, so counting only generated tokens would FALSE-POSITIVE it as "waste".
--   * Switching the threshold to useful tokens already prevents that (a big prompt makes useful high).
--   * The extra guard below also excludes prefill-dominated windows explicitly: we only flag waste
--     when generation is a non-trivial share of useful work (>= 30%), so genuine prefill/long-context
--     bursts are never mislabelled.
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
  prompt_tokens_win,
  (prompt_tokens_win + gen_tokens_win)                              AS useful_tokens_win,
  CAST(prompt_tokens_win + gen_tokens_win AS DOUBLE) / 15.0         AS useful_tokens_per_sec,  -- 15s window
  'WASTE_HIGH_UTIL'                                                 AS waste_flag
FROM gpu_efficiency_anomalies
WHERE avg_gpu_util >= 90
  -- low USEFUL throughput (prefill + decode): <~66 useful tokens/s over the 15s window
  AND (prompt_tokens_win + gen_tokens_win) <= 1000
  -- prefill guard: do not flag prefill-dominated / long-context windows (generation must be a
  -- non-trivial share of useful work), so legitimate big-prompt requests are not called "waste".
  AND CAST(gen_tokens_win AS DOUBLE) >= 0.3 * CAST(prompt_tokens_win + gen_tokens_win AS DOUBLE);
