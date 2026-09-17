# Case-study data — provenance notes

This directory holds the raw and derived artifacts for the Granite-3.3-8B / L4 case study.

## `anomalies_inpipeline.jsonl.gz` — historical snapshot, **generation-only denominator**

- **What it is.** A capture of the live Flink KPI stream from `flink/02_detect_anomalies.sql`,
  emitted during the case-study run.
- **Denominator label: GENERATION-ONLY.** This snapshot was produced by the earlier version of
  `flink/02_detect_anomalies.sql`, whose `joules_per_1k_tokens` KPI divided DCGM energy by
  **generation tokens alone** (`gen_tokens_win`). That was a defect: useful work is
  prefill + decode (`prompt_tokens + generation_tokens`), matching the published frontier. The SQL
  was corrected on **2026-09-17** to divide by the useful-token denominator (see CHANGELOG (Unreleased));
  this snapshot predates that fix.
- **Preserved, not recomputed.** This file is retained **byte-for-byte as captured**. It is historical
  evidence and is **not** rewritten, recalculated, or regenerated to match the corrected SQL. Any J/1k
  values inside it therefore reflect the old generation-only denominator and read **higher** than the
  useful-token KPI.
- **Integrity pin (SHA-256):**
  `9c988be400e5e0aeda62c4fe7fbe06c73a229539ee8a1dc17b6a3f340f43eaa1`
- **The published headline is unaffected.** The ~27× batching span and the 4 192 → 154 J/1k frontier
  are an **offline recompute** from the raw telemetry topic (`recompute_frontier.py`,
  `analysis.ipynb`), using the useful-token definition — never derived from this Flink snapshot.

## Other files (unchanged)

- `sweep_telemetry_raw.jsonl.gz` — append-only raw `gpu_telemetry` records; source of the published
  frontier via `recompute_frontier.py`.
- `sweep_telemetry_modelb.jsonl.gz` — raw telemetry for the second model (Mistral) comparison.
- `sweep_results.csv`, `sweep_results_modelb.csv` — derived sweep tables (retain both useful and
  generation-only columns).
