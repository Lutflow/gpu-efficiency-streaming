# Measured case study — IBM Granite 3.3-8B on NVIDIA L4

**100% real hardware telemetry**, captured live through the *same* Confluent Flink pipeline as the
synthetic quickstart. No synthetic values appear in this case study — every number below comes from a
real vLLM server + NVIDIA DCGM exporter, bridged to Kafka, and the raw data is attached for audit.

> Context: this models an **IBM Granite, served on open vLLM** workload — IBM Cloud itself
> documents deploying [granite-3.3-8b on a single NVIDIA L4](https://cloud.ibm.com/docs/solution-tutorials?topic=solution-tutorials-rhoai-deploy).

## What was measured

A real **Red Hat AI distribution of IBM Granite 3.3-8B Instruct**
([`RedHatAI/granite-3.3-8b-instruct`](https://huggingface.co/RedHatAI/granite-3.3-8b-instruct), FP16 —
Red Hat has not published an FP8 build for 3.3 yet) served by **vLLM** on a single **NVIDIA L4** GPU,
under a controlled closed-loop load (fixed-concurrency phases), with **NVIDIA DCGM** energy/utilization
telemetry and **vLLM** serving metrics streamed 1/s into `gpu_telemetry` and processed by the deployed
Flink statements (`flink/02_detect_anomalies.sql` etc.).

## Scope — what this is, and when it matters

This is a **reproducible reference** for measuring and governing GPU inference cost from real
telemetry. Two honest notes on scope:

- **For a one-off measurement of a single deployment, you don't need a streaming platform.** NVIDIA
  DCGM + a model server's `/metrics` + a short script will reproduce the numbers below — and we kept a
  no-GPU synthetic quickstart plus the committed raw data precisely so anyone can.
- **The pattern earns its keep at fleet scale:** continuous, governed, multi-deployment cost governance
  — consistent telemetry via Schema Registry, auditability via Stream Lineage, real-time detection and
  forecasting *inside* Flink (no separate model-serving infrastructure), and a closed action loop. That
  is what this project demonstrates end-to-end on Confluent Cloud for Apache Flink.

What we measured is a known principle made **precise, auditable, and reproducible**: at ~100 % GPU
utilization, energy per *useful* token still varied ~27× across batching regimes. **Utilization is a
misleading cost signal; energy-per-useful-work is the honest one.**

## Related work & positioning

**We did not invent this metric, nor the "utilization lies" finding — and we say so plainly.** Both are
established in the literature; our contribution is *operationalizing* them online, in the data plane.

- **"Utilization is misleading" is a known result.** Model FLOPs / Bandwidth Utilization (MFU/MBU) are
  the accepted "useful work" metrics, and inference typically runs at only **~5-15 % MFU** because it is
  memory-bandwidth bound ([zettabyte — *Measuring useful GPU work with MFU and MBU*](https://www.zettabyte.space/blog/gpu-utilization-mfu-mbu)).
  Google's **ML Productivity Goodput** explicitly targets *"the insufficiency of utilization-based
  metrics"* across a TPU fleet ([arXiv:2502.06982](https://arxiv.org/abs/2502.06982)).
- **"Energy per token" is an established metric.** It has been proposed independently as a first-class
  efficiency metric ([*Advocating Energy-per-Token in LLM Inference*, arXiv:2603.20224](https://arxiv.org/abs/2603.20224)),
  and there are public leaderboards/benchmarks for it: **Hugging Face AI Energy Score**
  ([huggingface.github.io/AIEnergyScore](https://huggingface.github.io/AIEnergyScore)) and the
  **ML.ENERGY** benchmark ([arXiv:2505.06371](https://arxiv.org/abs/2505.06371)).
- **"Useful work" = goodput.** Counting prefill + decode (not generation alone) follows the serving
  community's **goodput** notion of useful, SLO-bounded throughput
  ([*Revisiting SLO and Goodput Metrics in LLM Serving*, arXiv:2410.14257](https://arxiv.org/abs/2410.14257)).
- **GPU-cost idle detection + recommendations already ship.** **Kubecost** (GPU Optimization) and
  **OpenCost** detect idle/under-used GPU spend and recommend savings — on the *same* NVIDIA DCGM sensor
  this project consumes ([Kubecost GPU Optimization](https://docs.kubecost.com/using-kubecost/navigating-the-kubecost-ui/savings/gpu-optimization)
  · [OpenCost](https://github.com/opencost/opencost)).
- **Offline benchmarking** of serving throughput/power is well covered by **GuideLLM**
  ([github.com/vllm-project/guidellm](https://github.com/vllm-project/guidellm)) and MLPerf Inference.

**Our delta (honest):** we **operationalize these recognized metrics (MFU/goodput/energy-per-token) in
the data plane** — computed *online, per deployment, over schema-governed streams, multi-deployment,
with a closed real-time action loop* — rather than as an offline benchmark or a periodic FinOps batch.
The finding and the metric are not ours; the *streaming governance pattern* is what we demonstrate.

### Build vs buy — when does the streaming approach earn its place?

| | Kubecost / OpenCost + DCGM + Grafana | This project (Confluent Flink) |
|---|---|---|
| GPU idle / under-use detection | ✅ mature, production-grade | ✅ same DCGM signal |
| Cost allocation & chargeback | ✅ rich (k8s-native) | ➖ not the focus |
| Latency to signal | minutes–hours (scrape/batch) | **next Flink window (~seconds)** |
| Schema-governed telemetry contract | ➖ Prometheus labels | ✅ **Schema Registry (BACKWARD)** |
| Multi-cloud / non-k8s deployments | ➖ k8s-centric | ✅ **anything that can produce to Kafka** |
| Lineage / audit of the cost signal | ➖ limited | ✅ **Stream Lineage** |
| Real-time **action loop** (detect→recommend→act) | ➖ dashboards/alerts | ✅ **in-pipeline remediation stream** |
| Maturity / ecosystem | ✅ established | ➖ Flink ML functions are recent (see *Roadmap*) |
| Operating cost | Prometheus/k8s overhead | **billed in CFUs** (6-statement loop ⇒ `max_cfu=10`) |

**Honest verdict:** for k8s cost allocation on a single cluster, the incumbents are the pragmatic
choice. The streaming approach wins when you need **governed, sub-window, multi-deployment cost
governance with an action loop** feeding downstream agents — and you accept the Flink-ML maturity and
CFU cost trade-offs.

## Results — the efficiency frontier (real, audited)

The headline KPI is **`joules_per_1k_tokens`** — GPU energy per 1,000 *useful* tokens. **"Useful" now
counts `prompt_tokens + generation_tokens`** (prefill + decode), not generation alone — i.e. all the
tokens the GPU actually processed, the standard *goodput*-style accounting (see *Related work*). We
swept **fixed concurrency 1 → 32** (each level held ≥ 3.5 min) and measured the KPI two independent
ways (see *Rigor*). The frontier is **recomputed offline** by
[`recompute_frontier.py`](recompute_frontier.py) from the committed raw telemetry:

| Concurrency | GPU util | GPU power | Useful throughput | **J/1k (power÷tput)** | J/1k (ΔE÷Δtok) |
|---|---|---|---|---|---|
| 1  | ~100 % | 71.9 W | 17.2 tok/s  | **4 192** | 4 614 |
| 2  | ~100 % | 71.9 W | 33.1 tok/s  | **2 173** | 2 507 |
| 4  | ~100 % | 71.9 W | 66.1 tok/s  | **1 089** | 1 301 |
| 8  | ~100 % | 71.9 W | 130.5 tok/s | **551** | 640 |
| 16 | ~100 % | 72.0 W | 256.9 tok/s | **280** | 333 |
| 32 | ~100 % | 71.9 W | 466.1 tok/s | **154** | — (window too short) |
| idle | ~0 % | 34.9 W | 0 | **NULL** | NULL |

*(Useful throughput counts prompt + generation tokens; the generation-only throughput and the
generation-only J/1k are also in [`data/sweep_results.csv`](data/sweep_results.csv) for comparison —
e.g. conc=32 is 414.8 gen tok/s → 173 J/1k generation-only.)*

![Efficiency frontier](../../assets/efficiency-frontier.png)

How to read it:

- **The cost is throughput-dominated.** `power_watts` was **measured flat at the L4 TDP (~71.9-72.0 W)
  at every loaded level, including conc=1** — so `J/1k ≈ TDP / useful-throughput`. Useful throughput
  scales ~linearly with batching (17 → 466 tok/s), so energy per useful token collapses **~27×
  (4 192 → 154 J/1k)**. The driver of the frontier is **throughput (batching), not a swing in power** —
  we say this explicitly rather than implying an independent energy effect.
- **Utilization lies; cost-per-useful-work tells the truth.** GPU utilization was **~100 % at every
  loaded level** — yet the *cost* of that work ranged ~27×. A utilization dashboard calls conc=1 and
  conc=32 equally "busy"; only J/1k exposes that conc=1 costs ~27× the energy per useful token. (This is
  a known result — MFU/goodput literature, see *Related work* — made precise and auditable here.)
- **Idle = maximum waste, and it's `NULL` on purpose.** Idle still drew **34.9 W** producing **zero**
  useful tokens — energy-per-useful-work is *undefined* (division by zero). `NULL` is the honest
  signal for "infinitely inefficient", not a gap.
- **Including prefill lowered every J/1k ~10 % vs a generation-only count** (e.g. conc=32: 154 vs 173)
  but left the ~27× batching span unchanged — prefill is a roughly constant fraction (~10-12 %) of work
  across the sweep. We report the prefill-inclusive number as primary because it is the more honest
  measure of useful work.

## Rigor — dual-method power, cross-run consistency, sanity

- **Two independent measurement methods, reported honestly.** Every point is computed both ways from
  the **append-only** `gpu_telemetry` topic
  ([`data/sweep_telemetry_raw.jsonl.gz`](data/sweep_telemetry_raw.jsonl.gz), 5 356 records):
  **(i)** `mean(power_watts) / useful-throughput` — *primary*, since instantaneous DCGM power is
  rock-steady at **71.9-72.0 W (the L4 TDP) at every level**; and **(ii)** the counter delta
  `Δenergy_mJ / Δtokens`, computed between energy-counter update instants. The two **agree in shape and
  order of magnitude**, but method (ii) runs **~5-20 % above** method (i) (median ~16 %) — the DCGM
  energy counter integrates short power transients that the 1/s instantaneous sampling under-counts, so
  (ii) is effectively a slightly higher, independent bound. We **publish (i)** and show (ii) as the
  independent cross-check rather than averaging them. `recompute_frontier.py` regenerates both.
- **Measurement caveat (be explicit).** GPU power/energy telemetry from NVIDIA's stack is known to be
  sampling-sensitive: an SC'24 study found that on some data-center GPUs **only ~25 % of the runtime is
  actually sampled**, which can bias energy readings
  ([Yang et al., *nvidia-smi's Lack of Attention*, arXiv:2312.02741](https://arxiv.org/abs/2312.02741)).
  Our **conc=32** counter-delta point is **omitted** for exactly this reason — its phase is too short
  (~60-85 s) for the coarse, stepped energy counter to integrate cleanly; the steady instantaneous-power
  method (i) is what we report there.
- **Cross-run reproducibility.** The generation-only conc=32 number reproduces across independent runs
  (71.9 W ÷ ~415 gen tok/s → **173 J/1k generation-only**, matched by an earlier separate deployment);
  the prefill-inclusive primary at conc=32 is **154 J/1k**.
- **Sanity (not exclusion).** Interval power stays within the L4 envelope (~72 W) and J/1k is monotonic
  in concurrency with idle `NULL`. All six loaded points are physically valid and retained — no points
  dropped.
- **In-pipeline cross-check.** `flink/02_detect_anomalies.sql` computes the identical `Δenergy/Δtokens`
  formula and emits the populated KPI live ([`data/anomalies_inpipeline.jsonl.gz`](data/anomalies_inpipeline.jsonl.gz),
  captured during the run). Its per-15 s windows carry the same DCGM-cadence noise as method (ii), so
  the published frontier uses the steadier method (i) over the retained raw topic.

## Limitations

Stated plainly, because honest scope is the point:

- **Single GPU, single model, two runs.** One NVIDIA L4, one `RedHatAI/granite-3.3-8b-instruct`
  (FP16 — FP8 not published for 3.3 at capture time), `--max-model-len 4096`, concurrency ≤ 32. The
  sweep is one run; conc=32 is corroborated by one independent earlier run. Not a large multi-GPU /
  multi-model statistical study.
- **Controlled synthetic load, not a production workload.** A closed-loop generator with a fixed
  prompt and `max_tokens=200` — it exercises the mechanism (batching efficiency, idle waste); it is
  *not* a representative production traffic mix.
- **Energy-counter cadence.** The DCGM energy counter updates coarsely relative to the bridge sampling,
  so the counter-delta method (ii) runs **~5-20 % above** the primary instantaneous-power method (i)
  rather than within a tight symmetric band (an earlier draft said "±13 %"; the recomputed,
  prefill-inclusive frontier shows a systematic positive offset, documented in *Rigor*). The conc=32
  counter point is omitted as its window is too short for the coarse counter.
- **The business-impact figures below are an illustrative projection**, not a measured production
  saving — see that section.
- **Comparative conc=32 has a short window.** Both conc=32 phases are short (Granite ~85 s, Mistral
  ~62 s after the bridge stopped), so each rests on fewer samples than the ~3.5 min lower-concurrency
  phases. The primary (instantaneous-power) method keeps them robust, and the counter-delta point is
  omitted there; the lower-concurrency points are unaffected.
- **The waste detector is a heuristic, fixed to be prefill-aware.** `WASTE_HIGH_UTIL` now measures
  **useful** throughput (`prompt_tokens + generation_tokens`) and **guards against prefill-heavy /
  long-context windows** (it requires the generated fraction to be non-trivial), so a big-prompt /
  few-output request is *not* mis-flagged as waste. It is a deterministic rule, not a learned model.
- **Cost note:** running the closed loop required raising the Flink compute pool to `max_cfu = 10` (the
  6th statement needed CFU headroom); CFUs are billed by usage and the environment was torn down
  immediately after capture.

## Reproduce it

```bash
# 1. VM: 1x L4 (g2-standard-8), Deep Learning VM image (CUDA driver preinstalled)
gcloud compute instances create granite-l4-casestudy --zone=us-central1-c \
  --machine-type=g2-standard-8 \
  --image-family=common-cu129-ubuntu-2204-nvidia-580 --image-project=deeplearning-platform-release \
  --maintenance-policy=TERMINATE --boot-disk-size=150GB --scopes=cloud-platform

# 2. On the VM (Docker + NVIDIA container toolkit): DCGM exporter + vLLM
docker run -d --gpus all --cap-add SYS_ADMIN -p 9400:9400 \
  nvcr.io/nvidia/k8s/dcgm-exporter:3.3.5-3.4.1-ubuntu22.04
docker run -d --gpus all -p 8000:8000 vllm/vllm-openai:latest \
  --model RedHatAI/granite-3.3-8b-instruct --served-model-name granite-3.3-8b-instruct \
  --max-model-len 4096 --gpu-memory-utilization 0.92

# 3. Deploy the Flink pipeline (same as the quickstart) and run the real-source bridge
uv run deploy
BOOTSTRAP_SERVERS=... KAFKA_API_KEY=... uv run bridge --rate-per-sec 1 \
  --model-id granite-3.3-8b-instruct --deployment-id inference-node-a

# 4. Drive a fixed-concurrency SWEEP (each level >=3.5 min, >=10 clean 15s windows):
#    IDLE -> conc 1 -> 2 -> 4 -> 8 -> 16 -> 32 -> IDLE; log phase epoch timestamps,
#    then audit data/sweep_telemetry_raw.jsonl.gz with the interval method (see plot.py / sweep_results.csv).
```

The bridge is [`src/gpu_efficiency_streaming/bridge.py`](../../src/gpu_efficiency_streaming/bridge.py)
(`uv run bridge`) — it maps vLLM + DCGM Prometheus metrics 1:1 onto the `gpu_telemetry` Avro contract;
no synthetic values.

## Comparison — Granite-3.3-8B vs Mistral-7B-Instruct-v0.3 on the same L4

To close the single-model limitation, we ran the **identical sweep** (same NVIDIA L4, same concurrency
phases 1→32, same prompt, same `max_tokens=200`, same bridge) against a second model —
[`mistralai/Mistral-7B-Instruct-v0.3`](https://huggingface.co/mistralai/Mistral-7B-Instruct-v0.3)
(Apache-2.0). Only the model changed.

| Concurrency | Granite-3.3-8B — J/1k | Mistral-7B-v0.3 — J/1k | Mistral useful tput | Δ (Mistral vs Granite) |
|---|---|---|---|---|
| 1  | 4 192 | 3 552 | 19.9 tok/s  | −15 % |
| 2  | 2 173 | 1 876 | 38.4 tok/s  | −14 % |
| 4  | 1 089 | 944   | 76.1 tok/s  | −13 % |
| 8  | 551   | 478   | 150.4 tok/s | −13 % |
| 16 | 280   | 243   | 296.6 tok/s | −13 % |
| 32 | 154   | 135   | 531.2 tok/s | −12 % |

![Efficiency frontier comparison](../../assets/efficiency-frontier-comparison.png)

Honest reading:

- **Mistral-7B-v0.3 is ~12-15 % more energy-efficient per useful token at every concurrency level** on
  this L4 (e.g. 135 vs 154 J/1k at conc=32). Both models draw the same ~72 W TDP, so the difference is
  **throughput**: the smaller 7B model sustains more useful tokens/s (531 vs 466 at conc=32) than the
  8B, and `J/1k = power ÷ useful-throughput`.
- **This is a model-size effect (7B vs 8B), not an architecture verdict.** The comparison is
  apples-to-apples on *infrastructure* (same GPU, sweep, prompt, pipeline) but the models differ in
  size; a fair "which architecture is more efficient" study would size-match. The point here is that
  the KPI **measures real, model-specific efficiency differences** — exactly what a platform team needs
  to choose and right-size a model.
- Both frontiers use the same prefill-inclusive useful-throughput accounting and dual-method audit; raw
  data: [`data/sweep_telemetry_modelb.jsonl.gz`](data/sweep_telemetry_modelb.jsonl.gz) +
  [`data/sweep_results_modelb.csv`](data/sweep_results_modelb.csv).

### Closed-loop governance (live)

The deployed pipeline closes the loop from detection to a remediation recommendation, entirely in
Flink SQL:

- **`flink/08_waste_high_util.sql`** — a "utilization-lies" waste detector. It flags windows with
  **high GPU utilization but low *useful* throughput**, where useful = `prompt_tokens + generation_tokens`
  (prefill + decode, goodput-style): `avg_gpu_util >= 90` and `useful_tokens_win <= 1000` (< ~66
  useful tok/s over the 15 s window) → `gpu_efficiency_waste`. It also **guards against prefill-heavy /
  long-context windows** (requires generation ≥ 30 % of useful work), so a big-prompt / few-output
  request is not mis-flagged as waste. On the measured hardware the trigger is the **low-concurrency
  regime** (util pinned ~100 % while useful throughput is a fraction of peak — the exact pattern the
  frontier exposed).
- **`flink/09_remediation.sql`** — a **rule-based** remediation recommender (deterministic `CASE`
  rules, **no LLM** — a reference aligned with the Confluent *Streaming Agents* pattern). It consumes
  the governed signals (idle/saturation alerts + high-util waste) and emits, per deployment, a
  `recommended_action` and an *illustrative* `est_reclaimable_usd_per_mo`. Real captured rows in
  [`evidence/remediation-sample.txt`](evidence/remediation-sample.txt), e.g. `WASTE_HIGH_UTIL → "Raise
  batch concurrency or right-size the model" → $155.25/mo` and `SATURATION → "Scale out or rate-limit"
  → $0`.

### Live pipeline evidence

Captured **live** from the Confluent Cloud environment during the run — a full component-by-component
walkthrough (Stream Lineage, the topics, each Flink statement, the S3 connector, the remediation
output, and Schema Registry `BACKWARD`) is in **[`pipeline/PIPELINE.md`](../../pipeline/PIPELINE.md)**.

![Stream Lineage — closed governance loop](../../pipeline/1-lineage.png)

*Stream Lineage (captured live): one producer fans out into detect→alerts→S3, detect→waste→remediation,
forecast→capacity→S3, and a raw-archive S3 sink. CLI evidence (Schema Registry `BACKWARD`, the
`gpu_telemetry-value` schema, topic config) is in [`evidence/`](evidence/).*

## Business impact — real-time GPU cost governance

GPU inference is among the most expensive line items in an AI platform, and a large share is spent on
GPUs that are **allocated but idle or under-batched** — exactly what this run reproduced (idle drawing
~34.9 W for zero useful output; under-batched conc=1 costing ~27× the energy per useful token vs peak
batching).

A g2-standard-8 (1× L4) is **≈ $0.85/hr on-demand ≈ $623/GPU/month**
([GCP Compute pricing](https://cloud.google.com/products/compute/gpus-pricing); corroborated by public
calculators). Reclaimable spend scales with the **idle/low-efficiency fraction `F`** that this pipeline
*measures per deployment*:

> **Illustrative projection** (mechanism, not a measured production figure): if a fleet runs at
> `F = 40%` idle/low-efficiency time, reclaimable ≈ `$623 × 0.40 ≈ $249 / GPU / month`, or
> **≈ $300k/yr on a 100-GPU fleet**. The honest contribution of this project is not this number — it is
> that it **measures the real `F`** (and the J/1k that drives it) per deployment, in real time, so the
> savings are grounded rather than guessed.

This reframes the project from *monitoring* to **real-time GPU cost governance**: the `ML_FORECAST`
capacity-risk branch flags `PREDICTED_IDLE` *before* the waste is incurred (cost → faster
right-sizing), and the `SATURATION` alerts protect customer experience under load.

**Measured unit economics** (price × *measured* useful throughput — this part is measured, not
projected). At the same $0.8508/hr node, cost-per-useful-work mirrors the energy frontier:

| | Granite-3.3-8B (peak) | Mistral-7B-v0.3 (peak) |
|---|---|---|
| Useful throughput | 466 tok/s | 531 tok/s |
| **$ / 1M useful tokens (peak batching)** | **≈ $0.51** | **≈ $0.44** |
| $ / 1M useful tokens (conc=1, under-batched) | ≈ $13.7 | ≈ $11.9 |

So the same dollar buys ~**27× more useful tokens** at peak batching than under-batched — the cost
frontier *is* the energy frontier. (Per-1M-token figures = `price_per_hr / (useful_tok/s × 3600) × 1e6`;
`recompute_frontier.py` + the notebook regenerate them from the committed data.)

**Confluent-native & time-to-market.** The entire governance layer is **Flink SQL deployed in minutes**
(`uv run deploy`) — ARIMA/STL detection and forecasting run **in the data plane**, with **no separate
model-serving or monitoring stack** to stand up, and the loop is closed by a rule-based remediation
recommender aligned with the Confluent Streaming Agents pattern. That is the time-to-market argument:
governed, real-time GPU cost control without assembling Prometheus + scripts + a warehouse + an
alerting + an action tier.

## Provenance

| Field | Value |
|---|---|
| Model | `RedHatAI/granite-3.3-8b-instruct` (IBM Granite 3.3-8B Instruct, Red Hat AI distribution) |
| Quantization | FP16 (BF16 weights; FP8 not published for 3.3 at capture time) |
| Server | `vllm/vllm-openai:latest` (pulled 2026-06-15), OpenAI-compatible, `--max-model-len 4096` |
| GPU | NVIDIA L4 24 GB, `GPU-2e9a88f9-0e65-771b-d391-e09984261540`, driver 580.159.03 |
| Host | GCE `g2-standard-8`, us-central1-c |
| Telemetry | NVIDIA DCGM exporter 3.3.5 + vLLM Prometheus `/metrics`, bridged 1/s |
| Pipeline | Confluent Cloud for Apache Flink — `flink/02,03,05,07,08,09` |
| Captured | 2026-06-15 |
| Raw data | [`data/sweep_telemetry_raw.jsonl.gz`](data/sweep_telemetry_raw.jsonl.gz) (5 356 records) · [`data/anomalies_inpipeline.jsonl.gz`](data/anomalies_inpipeline.jsonl.gz) · [`data/sweep_results.csv`](data/sweep_results.csv) |

## Roadmap / Future directions

This is an open reference; the following are natural extensions (not yet implemented) that line up
with where the platform is heading:

- **LLM-assisted remediation in the data plane** — evolve the rule-based remediation recommender into
  an in-Flink `AI_COMPLETE` / model-inference call that reasons over an alert plus deployment context
  to draft a remediation, fitting the Confluent Streaming Agents pattern. *(Today's recommender is
  deterministic and rule-based, on purpose.)*
- **Lakehouse for historical efficiency analysis (Tableflow → Apache Iceberg)** — materialize the
  governed topics as Iceberg tables for long-horizon trend analysis, regression tracking, and cost
  reporting, with no separate ETL.
- **Fleet matrix (multi-GPU, multi-model, multi-quantization)** — extend the concurrency sweep across
  GPU classes (e.g., L4 / L40S / A100 / H100) and model sizes/quantizations (FP16 / FP8) to map an
  efficiency frontier per `(model, GPU)`. Requires validating per-deployment partitioning
  (`PARTITION BY deployment_id`) for the detection/forecast statements.
- **Production-traffic validation** — replace the closed-loop synthetic generator with mirrored real
  traffic to measure the idle / low-efficiency fraction `F` on representative workloads (today's
  business figures are an illustrative projection grounded in the measured per-token unit economics).
- **Calibrated decision bounds** — attach confidence/uncertainty bands to the forecast so
  `PREDICTED_IDLE` actions carry a calibrated risk, not a point estimate.
- **Closed-loop actuation with guardrails** — connect remediation recommendations to an actuator
  (e.g., scale-to-zero / autoscaler) via a Streaming Agent, moving from *recommend* to *act* behind
  human-approval or policy guardrails.
- **Maturity & cost hardening** — the built-in Flink ML functions (`ML_DETECT_ANOMALIES`,
  `ML_FORECAST`) are relatively recent; a production rollout should pin their SLA/version expectations
  and budget the **CFU cost** of the full loop (this 6-statement pipeline needs `max_cfu = 10`),
  trading detector richness against compute-pool spend.

*Built by **Lutflow** — real-time AI infrastructure governance. [lutflow.dev](https://lutflow.dev)*

## References

- [vLLM production metrics](https://docs.vllm.ai/en/latest/usage/metrics.html) ·
  [NVIDIA DCGM exporter](https://github.com/NVIDIA/dcgm-exporter)
- [Red Hat AI Inference Server (vLLM) + GuideLLM benchmarking](https://developers.redhat.com/articles/2025/12/24/how-deploy-and-benchmark-vllm-guidellm-kubernetes)
- [IBM Research — efficient inference (speculative decoding), arXiv:2404.19124](https://arxiv.org/abs/2404.19124)

*Trademarks: IBM® and Granite are trademarks of IBM Corp.; NVIDIA® and DCGM are trademarks of NVIDIA
Corporation; Red Hat® is a trademark of Red Hat, Inc. Independent, unaffiliated project.*
