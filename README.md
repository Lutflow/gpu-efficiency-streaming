# Real-Time GPU Efficiency Anomaly Detection on Confluent Cloud

[![CI](https://github.com/Lutflow/gpu-efficiency-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/Lutflow/gpu-efficiency-streaming/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Built with Confluent](https://img.shields.io/badge/built%20with-Confluent%20Cloud-0B1FA0.svg)](https://www.confluent.io/)

Detect **idle-but-allocated GPU** and **saturation** in your LLM inference fleet **in real time**,
using 100% Confluent Cloud: **Datagen Source → Flink (`TUMBLE` + `ML_DETECT_ANOMALIES`, ARIMA) →
Amazon S3 Sink**, governed by **Schema Registry** and visualized in **Stream Lineage**.

The anomaly-detection model runs *inside* Flink SQL — there is no separate model-serving
infrastructure to operate.

## Why this matters

A large share of GPU inference spend is wasted on GPUs that are **allocated but idle**. By the time a
nightly FinOps batch job surfaces it, the money is already gone. This pipeline flags the waste the
moment it appears in the telemetry stream, and lands an actionable anomaly record you can route to a
data lake, an observability platform, or an alert webhook.

The efficiency KPI it emits — `joules_per_1k_tokens`, derived from the GPU's real DCGM energy counter
divided by useful generated tokens — is the kind of unit a platform team can put a dollar figure on.

## Architecture

![Architecture: Datagen Source to Flink TUMBLE to ML_DETECT_ANOMALIES to S3 / webhook / Datadog](assets/architecture.svg)

```text
[Datagen Source] → gpu_telemetry (Avro, Schema Registry)
        │
[Flink TUMBLE 15s]            → per-window efficiency (avg gpu_util, joules_per_1k_tokens)
[Flink ML_DETECT_ANOMALIES]   → ARIMA, OVER (ORDER BY window_time) → is_anomaly + forecast bounds
        │
[Flink alerts]  → gpu_efficiency_alerts (IDLE_WASTE / SATURATION / OK)
        ├─→ [Amazon S3 Sink]            efficiency data lake (FinOps / billing reconciliation)
        ├─→ [HTTP Sink]      (optional) anomaly alerts to a webhook
        └─→ [Datadog Sink]   (optional) real-time efficiency observability
```

This `Source → Flink → Flink → Sink` graph is exactly what **Stream Lineage** renders in the Confluent
Cloud console.

## Run it (one command)

Prerequisites: a [Confluent Cloud](https://confluent.cloud) account, [Terraform](https://www.terraform.io/) ≥ 1.6,
[`uv`](https://docs.astral.sh/uv/), and an AWS account with an S3 bucket.

```bash
# 1. Provide credentials (never committed — terraform.tfvars is gitignored)
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
$EDITOR terraform/terraform.tfvars

# 2. Stand the whole pipeline up
uv run deploy

# 3. ...screenshot the printed Stream Lineage URL once data flows (~5 min warmup)

# 4. Tear it all down
uv run destroy
```

`uv run deploy` runs `terraform apply`. **Everything** — environment, Kafka cluster, Schema Registry
subject, Flink compute pool, connectors, and the Flink SQL statements that run `ML_DETECT_ANOMALIES` —
is declared as Terraform. The Flink statements read their SQL from [`flink/`](flink/) via `file()`, so
the SQL in this repo is the single source of truth.

> **Warmup:** with a 15s tumbling window and `minTrainingSize=20`, the first anomalies appear after
> roughly 20 windows (~5 minutes).

## What's synthetic vs. production

The telemetry in this demo is **synthetic but structured**. A small producer
([`src/gpu_efficiency_streaming/produce.py`](src/gpu_efficiency_streaming/produce.py), run with
`uv run produce`) emits a **temporally structured signal** — a diurnal/sawtooth utilization duty cycle
plus noise, with randomly injected *idle episodes* — and the dependent fields are **physically
correlated** (power tracks utilization toward the L4 TDP, token counters advance faster when busy, the
energy counter integrates power, latency rises under saturation). This is what gives the ML something
real to detect and forecast — but it is still a **synthetic signal, not a measurement**. The **schema is
standards-grounded**: every field maps 1:1 to a real, public metric from **vLLM** (Prometheus v1), the
**NVIDIA DCGM exporter**, and **OpenTelemetry** semantic conventions (GenAI + Hardware/GPU). See the
provenance table below.

To run against a **real fleet**, replace the producer with an **OpenTelemetry Collector** (Prometheus
scrape of vLLM `/metrics` + the DCGM exporter) or a Prometheus→Kafka bridge. **The schema and the entire
Flink/ML/Sink pipeline stay identical** — that is what makes this translation-ready rather than a toy.

### Schema provenance

| Avro field | vLLM (Prometheus v1) | NVIDIA DCGM exporter | OpenTelemetry semconv |
|---|---|---|---|
| `gpu_util_pct` | — | `DCGM_FI_DEV_GPU_UTIL` | `hw.gpu.utilization` |
| `sm/tensor/dram_active_ratio` | — | `DCGM_FI_PROF_{SM,PIPE_TENSOR,DRAM}_ACTIVE` | (hw.gpu.* extended) |
| `power_watts` / `energy_mj` | — | `DCGM_FI_DEV_POWER_USAGE` / `_TOTAL_ENERGY_CONSUMPTION` | `hw.power` / `hw.energy` |
| `temp_celsius` | — | `DCGM_FI_DEV_GPU_TEMP` | `hw.temperature` |
| `num_requests_running/waiting` | `vllm:num_requests_{running,waiting}` | — | — |
| `kv_cache_usage_perc` | `vllm:kv_cache_usage_perc` | — | — |
| `prompt/generation_tokens_total` | `vllm:{prompt,generation}_tokens_total` | — | `gen_ai.client.token.usage` |
| `ttft_seconds` | `vllm:time_to_first_token_seconds` | — | `gen_ai.server.time_to_first_token` |
| `inter_token_latency_s` (TPOT) | `vllm:inter_token_latency_seconds` | — | `gen_ai.server.time_per_output_token` |
| `e2e_latency_seconds` | `vllm:e2e_request_latency_seconds` | — | `gen_ai.server.request.duration` |

## Repository layout

```text
schemas/gpu_telemetry.avsc      # public, standards-grounded Avro schema
scripts/datagen_schema.json     # Datagen Source generator (bimodal gpu_util seeds anomalies)
flink/                          # the SQL pipeline (single source of truth)
  01a_add_event_time.sql        #   computed event_time column (TO_TIMESTAMP_LTZ)
  01b_set_watermark.sql         #   event-time watermark on event_time
  02_detect_anomalies.sql       #   TUMBLE 15s + ML_DETECT_ANOMALIES (ARIMA)
  03_alerts.sql                 #   IDLE_WASTE / SATURATION business rule
  04_datadog_metrics.sql        #   (optional) reshape for the Datadog Metrics Sink
terraform/                      # all infrastructure + connectors + Flink statements
src/gpu_efficiency_streaming/   # `uv run deploy` / `uv run destroy`
tests/                          # schema + datagen validation (pytest)
```

## Design notes

- **Single deployment in the demo.** `ML_DETECT_ANOMALIES` is used as an `OVER (ORDER BY window_time …)`
  window function with **no `PARTITION BY`** — matching the documented pattern exactly. This guarantees
  the statement validates, and makes the within-window counter delta (`MAX − MIN`) semantically valid on
  a single stream.
- **15-second window** keeps the ARIMA warmup to ~5 minutes for a live demo.
- **Robust event time.** `ts` is carried as a plain epoch-millis `long` (no Avro logical type, so the
  pipeline never depends on the source connector preserving it). Flink derives an event-time attribute
  with a computed column — `ALTER TABLE gpu_telemetry ADD event_time AS TO_TIMESTAMP_LTZ(ts, 3)` — and a
  separate `MODIFY WATERMARK FOR event_time` (Confluent Flink runs one statement at a time, so these are
  two statements). `TUMBLE` then windows on `DESCRIPTOR(event_time)`.
- **Changelog mode.** `ML_DETECT_ANOMALIES` as an unbounded `OVER` aggregation emits an updating
  (retract) changelog, so the result tables are created **without** forcing `changelog.mode = 'append'`
  (Confluent infers the correct mode). They are distributed by `deployment_id` so the Kafka message key
  is a real column rather than the implicit raw `key BYTES`.

## Security & governance

- **Least-privilege RBAC (Standard cluster).** The pipeline service account is **not** a
  `CloudClusterAdmin` or `EnvironmentAdmin`. It receives only: `ResourceOwner` scoped to the specific
  pipeline topics (Flink's `ALTER TABLE` needs topic ownership), `ResourceOwner` on the `dlq-*`,
  `transactional-id=*`, and `group=*` resources that the Flink exactly-once sink and managed connectors
  require, `ResourceOwner` on the specific Schema Registry subjects (value + key), and `FlinkDeveloper`
  on the environment. Topic-scoped resource roles require a **Standard** cluster (Basic does not support
  them). This is Lutflow's default security posture — grant the minimum each workload needs.
- **Governed schema.** The canonical Avro schema is registered by Terraform (the registrant — not
  ad-hoc connector auto-registration) and the subject compatibility is pinned to **`BACKWARD`**, so
  schema evolution can't silently break consumers. The Datagen Source produces the identical base schema.
  CLI equivalent:

  ```bash
  confluent schema-registry schema create \
    --subject gpu_telemetry-value --schema schemas/gpu_telemetry.avsc --type avro
  confluent schema-registry subject update gpu_telemetry-value --compatibility BACKWARD
  ```

- **No secrets in the repo.** Credentials live in gitignored `terraform.tfvars` / `TF_VAR_*`; CI runs
  a gitleaks scan.

## Roadmap

- **Multi-deployment:** one templated statement per `deployment_id`, or `PARTITION BY` once Confluent
  supports it for `ML_DETECT_ANOMALIES`.
- **Physically-correlated synthetic data:** the Datagen Random Generator produces independent fields; a
  custom producer or a Flink shaping view would correlate `tokens ∝ util`, etc.
- **Production source:** documented OpenTelemetry Collector / Prometheus→Kafka swap-in.

> ### Built by Lutflow
>
> Lutflow does this at **GPU-attribution depth** — real DCGM + vLLM-internal telemetry, calibrated
> pre-hoc cost prediction, and an efficiency-intelligence layer that *prevents* waste before it
> compounds. This repo shows the streaming pattern; the production version goes deeper.
>
> **Running LLM inference at scale and want the production version? → [lutflow.dev](https://lutflow.dev)**

## License

[Apache-2.0](LICENSE).
