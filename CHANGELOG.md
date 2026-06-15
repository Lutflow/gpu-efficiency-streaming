# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-15

Measured case study + real-source bridge. Reframed from monitoring to **GPU cost governance**.

### Added

- **Measured case study** ([`case-studies/granite-3.3-8b-l4/`](case-studies/granite-3.3-8b-l4/)):
  100% real telemetry from **IBM Granite 3.3-8B Instruct** (Red Hat AI FP16 distribution) served by
  **vLLM** on a real **NVIDIA L4**, through the same Flink pipeline. Measured `joules_per_1k_tokens`:
  **173 (busy) / 1 182 (low concurrency) / `NULL` (idle)**, with a raw-telemetry audit cross-check,
  physical gates (≤ 72 W·15 s/window; busy<low<idle), real vLLM serving metrics, provenance, and the
  raw JSONL for reproducibility.
- **Real-source bridge** ([`src/gpu_efficiency_streaming/bridge.py`](src/gpu_efficiency_streaming/bridge.py),
  `uv run bridge`): maps live vLLM + NVIDIA DCGM Prometheus metrics 1:1 onto the `gpu_telemetry` Avro
  contract — no synthetic values. Offline-tested in CI.
- **Business-impact** framing (illustrative reclaimable-spend projection grounded in the measured
  idle/low-efficiency fraction) and headline reframe to real-time GPU cost governance.

### Changed

- The synthetic producer is now positioned as the **reproducible quickstart (no GPU required)**;
  measured hardware results live in the case study.

## [0.1.0] - 2026-06-15

Initial release — real-time GPU efficiency anomaly detection and forecasting on Confluent Cloud.

### Added

- **Structured synthetic producer** (`uv run produce`) that models an IBM Granite 3.3-8B Instruct
  inference deployment on NVIDIA L4 GPUs: a diurnal/sawtooth duty cycle with injected idle episodes and
  physically-correlated fields (it models, it does not measure real hardware).
- **Flink SQL pipeline** (single source of truth in [`flink/`](flink/)): computed event-time column +
  watermark, `ML_DETECT_ANOMALIES` (ARIMA with STL, `m=12`, `minTrainingSize=30`), an IDLE_WASTE /
  SATURATION alert rule, `ML_FORECAST`, and a predictive `capacity_risk` (PREDICTED_IDLE) branch.
- **Three Amazon S3 sinks** — efficiency-alerts lake, capacity-risk lake, and raw-telemetry archive —
  so every branch lands in a governed destination.
- **Terraform IaC** ([`terraform/`](terraform/)): Standard Kafka cluster, Schema Registry subject pinned
  to `BACKWARD`, Flink compute pool and statements, and least-privilege topic-scoped RBAC.
- **Standards-grounded Avro schema** ([`schemas/`](schemas/)): every field maps 1:1 to a public metric
  from vLLM, the NVIDIA DCGM exporter, and OpenTelemetry semantic conventions.
- **CI**: ruff, pytest, `terraform fmt`/`validate`, markdownlint, and a gitleaks secret scan.
- **Experimental** ([`experimental/`](experimental/)): an `AI_COMPLETE` (Gemini) agentic-remediation
  exploration, documented honestly and **not deployed** (Flink determinism constraint over changelog
  streams).

[0.2.0]: https://github.com/Lutflow/gpu-efficiency-streaming/releases/tag/v0.2.0
[0.1.0]: https://github.com/Lutflow/gpu-efficiency-streaming/releases/tag/v0.1.0
