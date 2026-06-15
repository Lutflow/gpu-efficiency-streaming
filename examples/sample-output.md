# Sample output

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

## `gpu_efficiency_capacity_risk`

Forward-looking: `predicted_util` is `ML_FORECAST`'s projection for the next window; a value below the
idle threshold raises `PREDICTED_IDLE` **before** the waste happens.

```json
{"window_start": "2026-06-15T10:20:15-04:00", "predicted_util": 14.04, "risk_flag": "PREDICTED_IDLE"}
{"window_start": "2026-06-15T10:20:15-04:00", "predicted_util": 14.06, "risk_flag": "PREDICTED_IDLE"}
```

These are produced by the deployed `flink/02_detect_anomalies.sql`, `flink/03_alerts.sql`,
`flink/05_forecast.sql`, and `flink/07_capacity_risk.sql` statements.
