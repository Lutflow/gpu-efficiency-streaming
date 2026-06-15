# Sample output

> **REAL hardware results** (IBM Granite 3.3-8B on a real NVIDIA L4, vLLM + DCGM, 100% real telemetry)
> live in the [measured case study](../case-studies/granite-3.3-8b-l4/) with the raw JSONL attached.
> Measured efficiency frontier (concurrency sweep): **173 J/1k @ conc 32 · 311 @ 16 · 612 @ 8 · 4 653
> @ conc 1 · `NULL` (max waste) idle**. The rows below are from the **synthetic quickstart** run (real
> pipeline output, synthetic input).

Captured from a **live run** of this pipeline on Confluent Cloud for Apache Flink. The input is the
synthetic structured signal modeling an IBM Granite 3.3-8B Instruct deployment on NVIDIA L4 — see the
README section [*What's synthetic vs. production*](../README.md#whats-synthetic-vs-production). These
are real `ML_DETECT_ANOMALIES` / `ML_FORECAST` outputs, not hand-written examples.

Nullable fields are shown unwrapped from their Avro-union console encoding; values are otherwise
verbatim.

## `gpu_efficiency_alerts`

Each record carries the windowed `avg_gpu_util`, the model's `expected_util` (ARIMA forecast) and its
`lower_bound` / `upper_bound`, the `is_anomaly` flag, and the derived business `efficiency_flag`.

```json
{"window_start": "2026-06-15T10:21:30-04:00", "avg_gpu_util": 7.33,  "expected_util": 51.21, "lower_bound": 12.96,  "upper_bound": 89.47, "is_anomaly": true, "efficiency_flag": "IDLE_WASTE"}
{"window_start": "2026-06-15T10:21:30-04:00", "avg_gpu_util": 7.16,  "expected_util": 51.21, "lower_bound": 12.96,  "upper_bound": 89.47, "is_anomaly": true, "efficiency_flag": "IDLE_WASTE"}
{"window_start": "2026-06-15T10:20:30-04:00", "avg_gpu_util": 71.96, "expected_util": 15.05, "lower_bound": -16.26, "upper_bound": 46.36, "is_anomaly": true, "efficiency_flag": "SATURATION"}
{"window_start": "2026-06-15T10:20:30-04:00", "avg_gpu_util": 70.59, "expected_util": 15.05, "lower_bound": -16.26, "upper_bound": 46.36, "is_anomaly": true, "efficiency_flag": "SATURATION"}
```

How to read it:

- **IDLE_WASTE** — measured `avg_gpu_util = 7.33` while ARIMA expected `≈ 51.2` (normal range
  `[12.96, 89.47]`). The value falls **below** the lower bound → an allocated-but-idle GPU burning money.
- **SATURATION** — measured `avg_gpu_util = 71.96` against an expected `≈ 15.0` (range
  `[-16.26, 46.36]`); **above** the upper bound → an unexpected load spike worth investigating.

> The bounds are ARIMA's raw prediction interval and are **not** clamped to `[0, 100]` — that's why a
> `lower_bound` can be negative (e.g. `-16.26`). It is the model's output, not an error.

## `gpu_efficiency_capacity_risk`

Forward-looking: `predicted_util` is `ML_FORECAST`'s projection for the next window; a value below the
idle threshold raises `PREDICTED_IDLE` **before** the waste happens.

```json
{"window_start": "2026-06-15T10:20:15-04:00", "predicted_util": 14.04, "risk_flag": "PREDICTED_IDLE"}
{"window_start": "2026-06-15T10:20:15-04:00", "predicted_util": 14.06, "risk_flag": "PREDICTED_IDLE"}
```

## `gpu_efficiency_anomalies` — energy-efficiency KPI

The headline KPI `joules_per_1k_tokens` (DCGM energy per 1,000 useful generated tokens) lives here,
computed per window from the energy and token counter deltas. It is the project's differentiator: a
**cost-per-useful-work** unit, not just a utilization gauge. Real rows:

```json
{"window_start": "2026-06-15T18:03:30-04:00", "avg_gpu_util": 7.0,  "gen_tokens_win": 0,    "joules_per_1k_tokens": null}
{"window_start": "2026-06-15T18:03:45-04:00", "avg_gpu_util": 8.49, "gen_tokens_win": 401,  "joules_per_1k_tokens": 95.7}
{"window_start": "2026-06-15T18:03:30-04:00", "avg_gpu_util": 14.7, "gen_tokens_win": 5925, "joules_per_1k_tokens": 71.3}
{"window_start": "2026-06-15T18:03:15-04:00", "avg_gpu_util": 55.3, "gen_tokens_win": 3407, "joules_per_1k_tokens": 29.0}
```

How to read it (cost of useful work falls as utilization rises):

- **Efficient** — `avg_gpu_util = 55.3` → **29.0 J / 1k tokens**. The GPU is doing real work, so the
  fixed power draw is amortized over many tokens. (At full saturation this trends to ~20-25 J/1k,
  consistent with an L4 at ~72 W sustaining a few thousand tokens/s.)
- **Low utilization** — `avg_gpu_util = 14.7` → **71.3 J/1k**, and `8.49` → **95.7 J/1k**: the same
  energy floor spread over far fewer useful tokens, so cost-per-token climbs sharply — waste.
- **Fully idle** — `gen_tokens_win = 0` → `joules_per_1k_tokens = NULL`. **This is a feature, not a gap:**
  the GPU is burning energy while producing *zero* useful tokens, so energy-per-useful-work is
  *undefined* — the maximum-waste case. No number can express "infinitely inefficient"; `NULL` is the
  honest signal.

These are produced by the deployed `flink/02_detect_anomalies.sql`, `flink/03_alerts.sql`,
`flink/05_forecast.sql`, and `flink/07_capacity_risk.sql` statements.
