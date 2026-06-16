# Flink SQL pipeline

These statements are the single source of truth for the stream processing. Terraform
([`../terraform/flink.tf`](../terraform/flink.tf)) submits each one with `file()`, so what you read
here is exactly what runs. Confluent Cloud for Apache Flink executes one statement at a time, so the
DAG is expressed as separate, ordered statements with explicit dependencies.

## DAG

```text
01a_add_event_time ─→ 01b_set_watermark ─┬─→ 02_detect_anomalies ─→ gpu_efficiency_anomalies
                                         │        ├─→ 03_alerts ─────────────→ S3 (efficiency lake)
                                         │        └─→ 08_waste_high_util ─┐
                                         │   03_alerts (idle / saturation)─┴─→ 09_remediation ─→ gpu_remediation
                                         └─→ 05_forecast ─→ 07_capacity_risk ─→ S3 (capacity lake)

gpu_telemetry ───────────────────────────────────────────────────────────────→ S3 (raw archive)
```

The detection path closes a loop: `02 → anomalies → {03 alerts, 08 waste} → 09 remediation`.

## Statements

| File | What it does |
|---|---|
| `01a_add_event_time.sql` | `ALTER TABLE gpu_telemetry ADD event_time AS TO_TIMESTAMP_LTZ(ts, 3)` — a computed event-time column derived from the plain epoch-millis `ts`. |
| `01b_set_watermark.sql` | `ALTER TABLE … MODIFY WATERMARK FOR event_time` — the event-time watermark (a separate statement, since Flink runs one at a time). |
| `02_detect_anomalies.sql` | `TUMBLE` 15s aggregation + `ML_DETECT_ANOMALIES` (ARIMA, `enableStl=true`, `m=12`, `minTrainingSize=30`) → `gpu_efficiency_anomalies`. |
| `03_alerts.sql` | Business rule over the anomaly stream → `IDLE_WASTE` / `SATURATION` records in `gpu_efficiency_alerts`. |
| `05_forecast.sql` | `ML_FORECAST` (`horizon=1`, `enableStl=true`, `m=12`) off `gpu_telemetry` → `gpu_efficiency_forecast` (output column aliased `fc`, an `ARRAY<ROW<…>>`). |
| `07_capacity_risk.sql` | Reads `fc[1].forecast_value`; emits a `PREDICTED_IDLE` record to `gpu_efficiency_capacity_risk` when the projected next-window utilization is low. |
| `08_waste_high_util.sql` | "Utilization-lies" detector — high util (`avg_gpu_util >= 90`) but low useful throughput (`gen_tokens_win <= 1000`) → `gpu_efficiency_waste`. Cheap deterministic filter, no ML. |
| `09_remediation.sql` | Rule-based remediation recommender (deterministic `CASE`, **no LLM**; reference-aligned with Confluent Streaming Agents): alerts ∪ waste → `gpu_remediation` (action + illustrative reclaimable $). |

The `gpu_efficiency_alerts` / `gpu_efficiency_capacity_risk` topics and the raw `gpu_telemetry` topic
are each consumed by an **Amazon S3 sink** (see [`../terraform/connectors.tf`](../terraform/connectors.tf)).

## Numbering gaps (intentional)

The numbers are stable identifiers, not a contiguous sequence:

- **04** — reserved; there is no intermediate transform between detection (`02`) and alerts (`03`).
- **06** (`*_gemini_connection`, `*_gemini_model`, `*_remediation`) — an `AI_COMPLETE` (Gemini)
  agentic-remediation *exploration*. **Not deployed**; kept in [`../experimental/`](../experimental/)
  with an honest explanation (Flink rejects the non-deterministic `AI_COMPLETE` over the changelog
  streams this pipeline produces). The deployed remediation (`09`) is the rule-based version; an
  `AI_COMPLETE` upgrade is a documented roadmap item.
