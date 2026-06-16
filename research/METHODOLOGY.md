# Methodology — measured efficiency frontier (v0.4.0)

How the GPU efficiency frontier in [`../case-studies/granite-3.3-8b-l4/`](../case-studies/granite-3.3-8b-l4/README.md)
is measured and recomputed, what it does and does not claim, and the prior art it builds on. Every
number is recomputed offline from the committed raw telemetry by
[`recompute_frontier.py`](../case-studies/granite-3.3-8b-l4/recompute_frontier.py) — re-run it and you
get the tables below.

## What "useful work" means

The KPI is **`joules_per_1k_tokens`** — GPU energy per 1,000 **useful** tokens, where
**useful = `prompt_tokens + generation_tokens`** (prefill + decode). Counting prefill as useful work is
the standard *goodput*-style accounting; measuring only generated tokens would penalise legitimate
prefill-heavy / long-context requests. (v0.3.0 used generation-only; v0.4.0 recomputes with prefill
included — see the before/after below.)

## Measurement: two independent methods

Telemetry is the append-only `gpu_telemetry` topic (NVIDIA DCGM energy/power + vLLM token counters,
1/s). Each phase of a fixed-concurrency sweep (1 → 32, each level held ≥ 3.5 min) is computed two ways:

- **(i) Power method — primary.** `mean(power_watts) / useful-throughput`. `power_watts` is measured
  **flat at the L4 TDP (~71.9–72.0 W) at every loaded level, including conc=1**, so the frontier is
  **throughput-dominated**: `J/1k ≈ TDP / useful-throughput`.
- **(ii) Counter-delta method — independent check.** `Δenergy_mJ / Δtokens`, taken between DCGM
  energy-counter update instants. It runs **~5–20 % above** method (i) (median ~16 %): the coarse,
  stepped energy counter integrates short power transients that the 1/s instantaneous sampling
  under-counts. We **publish (i)** and show (ii) as the cross-check rather than averaging them.

**Physical sanity gates (kept, not used to drop points):** interval power within the L4 envelope
(~72 W), `J/1k` monotonic in concurrency, and idle → `NULL` (zero useful tokens ⇒ undefined cost). All
six loaded points are physically valid and retained.

## Phase segmentation

Phases are assigned by the monotonically increasing concurrency sweep (`num_requests_running` reaching
1, 2, 4, 8, 16, 32); each phase trims 15 s off both edges to drop ramp transients. The
generation-only throughput this produces reproduces the v0.3.0 CSV (e.g. Granite conc=32 ≈ 415 gen
tok/s), confirming that only the *token definition* changed — not the segmentation.

## Results — before (generation-only) → after (prefill-inclusive)

Power is measured flat at the L4 TDP under load (idle is a separate, unloaded bookend):

| Model | conc=1 power | conc=32 power | idle |
|---|---|---|---|
| Granite-3.3-8B | 71.9 W | 71.9 W | ~33–36 W |
| Mistral-7B-v0.3 | 70.8 W | 71.8 W | ~30 W |

**Granite-3.3-8B (J/1k):**

| conc | generation-only (v0.3.0) | **useful, primary (v0.4.0)** | counter-delta (useful) |
|---|---|---|---|
| 1  | 4 639 | **4 192** | 4 614 |
| 2  | 2 413 | **2 173** | 2 507 |
| 4  | 1 221 | **1 089** | 1 301 |
| 8  | 611   | **551**   | 640 |
| 16 | 311   | **280**   | 333 |
| 32 | 173   | **154**   | omitted (window too short) |
| span | ~27× | **27.2×** | — |

**Mistral-7B-Instruct-v0.3 (J/1k):**

| conc | generation-only | **useful (primary)** | counter-delta |
|---|---|---|---|
| 1  | 3 972 | **3 552** | 3 748 |
| 2  | 2 096 | **1 876** | 2 257 |
| 4  | 1 056 | **944**   | 1 101 |
| 8  | 535   | **478**   | 569 |
| 16 | 271   | **243**   | 291 |
| 32 | 149   | **135**   | omitted |

**Counter-delta vs power offset (independent-method agreement):**

| conc | Granite | Mistral |
|---|---|---|
| 1  | +10.1 % | +5.5 % |
| 2  | +15.4 % | +20.3 % |
| 4  | +19.5 % | +16.6 % |
| 8  | +16.2 % | +19.0 % |
| 16 | +18.9 % | +19.8 % |

**Effect:** including prefill lowers every `J/1k` by ~10 % (prefill is ~10–12 % of total tokens across
the sweep) and leaves the ~27× batching span unchanged. The conc=32 counter-delta point is omitted for
both models — its phase is too short (~60–85 s) for the coarse, stepped DCGM energy counter.

## Prior art — what is established, and our delta

**We did not invent this metric, nor the "utilization is misleading" finding.** Both are established;
the contribution is *operationalizing* them online, in the data plane.

| Claim it supports | Source | Quote |
|---|---|---|
| Utilization-based metrics are insufficient (fleet scale) | [arXiv:2502.06982](https://arxiv.org/abs/2502.06982) | "We identify critical gaps in conventional utilization-based performance metrics and propose 'ML Productivity Goodput' (MPG)" |
| GPU power telemetry is sampling-biased | [arXiv:2312.02741](https://arxiv.org/abs/2312.02741) | "on the A100 and H100 GPUs only 25% of the runtime is sampled for power consumption … can lead to a drastic under/overestimation of energy consumed" (SC'24) |
| Energy-per-token is an established metric | [arXiv:2603.20224](https://arxiv.org/abs/2603.20224) | "we propose energy efficiency metrics, including Energy-per-Token, as complements to traditional accuracy benchmarks" |
| "Useful work" = goodput (SLO-bounded) | [arXiv:2410.14257](https://arxiv.org/abs/2410.14257) | "we revisit SLO and goodput metrics in LLM serving and propose a unified metric framework smooth goodput" |
| Public energy-efficiency leaderboard | [HF AI Energy Score](https://huggingface.github.io/AIEnergyScore) | AI Energy Score — standardized energy-efficiency leaderboard across tasks/modalities |
| Automated energy-optimization recommendations exist | [arXiv:2505.06371](https://arxiv.org/abs/2505.06371) | "automated optimization recommendations can lead to significant (sometimes more than 40%) energy savings without changing what is being computed" |
| MFU/MBU; inference is memory-bound; util ≠ useful work | [zettabyte — MFU/MBU](https://www.zettabyte.space/blog/gpu-utilization-mfu-mbu) | "GPU utilization … mostly answers whether the GPU was busy, not whether it was doing the work your model needs" |
| Incumbent GPU idle-cost + recommendations (on DCGM) | [Kubecost GPU Optimization](https://docs.kubecost.com/using-kubecost/navigating-the-kubecost-ui/savings/gpu-optimization) | "proactively identifies ways in which you can save money … collects and processes GPU utilization metrics" |
| Open-source cost-monitoring incumbent | [OpenCost](https://github.com/opencost/opencost) | OpenCost — CNCF open-source cost monitoring for cloud-native workloads |
| Offline serving benchmark (prior art) | [GuideLLM](https://github.com/vllm-project/guidellm) | GuideLLM — vLLM / Red Hat serving benchmark |

**Delta:** this project computes the recognized metrics (MFU / goodput / energy-per-token) **online,
per deployment, over schema-governed streams, with a real-time action loop** — not as an offline
benchmark or a periodic FinOps batch. The finding and the metric are not ours; the streaming-governance
pattern is what we demonstrate.

> **Engaging the prior art on our own data:** [`metrics_cross_check.ipynb`](metrics_cross_check.ipynb)
> *implements* MFU, MBU, TTFT-goodput and energy-per-token on our measured `.gz` and shows the same
> qualitative result (single-digit MFU, ~70–85 % MBU → memory-bandwidth bound). It does **not** compare
> our numbers to any paper's (different hardware/workload); constants (L4 dense FP16 = 121 TFLOP/s,
> 300 GB/s; `2·N` FLOPs/token) are cited from the NVIDIA L4 datasheet and model cards.

## Limitations

- Single NVIDIA L4, single model family (`granite-3.3-8b-instruct` FP16; Mistral-7B-v0.3 FP16),
  `--max-model-len 4096`, concurrency ≤ 32. One run; conc=32 corroborated by an independent earlier
  run (generation-only). Not a multi-GPU / multi-model statistical study.
- Controlled synthetic closed-loop load (fixed prompt, `max_tokens=200`) — exercises the mechanism,
  not a production traffic mix.
- DCGM energy counter is coarse, so the counter-delta method runs ~5–20 % above the primary method and
  the short conc=32 counter point is omitted (see arXiv:2312.02741).
- Fleet dollar figures in the case study are an **illustrative projection**, not a measured production
  saving; the per-token unit economics are measured.

## Discards — what we did not adopt, and why

- **"Energy varies only ~3–5×, so the 27× is inflated."** Not adopted: it assumes power rises with
  load, but `power_watts` is **measured flat at ~72 W at every concurrency including conc=1** under
  continuous load (idle 26–36 W is a separate unloaded bookend). The ~27× is a real, throughput-driven
  span, not a measurement artifact — so we reframed it as throughput-dominated rather than inventing a
  lower number.
- **Switching the headline to MFU numerically.** We anchor "useful work" to goodput and cite MFU/MBU,
  but keep the energy-per-useful-token KPI as the governed signal; a FLOP-based MFU per `(model, GPU)`
  is listed as future work, not estimated.
- **A benchmark reference that bot-blocks automated link checks** was dropped from the citations in
  favour of equivalent sources that resolve cleanly.
- **conc=32 counter-delta** is omitted for both models — its window is too short for the coarse counter
  to integrate cleanly; the steady power method is reported there instead.

## Reproduce

```bash
# From the case-study directory; reads the committed raw telemetry and rewrites the CSVs.
uv run python case-studies/granite-3.3-8b-l4/recompute_frontier.py
```

See the [case study](../case-studies/granite-3.3-8b-l4/README.md) for the full write-up, plots, and the
lab notebook.
