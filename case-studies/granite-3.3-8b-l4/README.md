# Measured case study — IBM Granite 3.3-8B on NVIDIA L4

**100% real hardware telemetry**, captured live through the *same* Confluent Flink pipeline as the
synthetic quickstart. No synthetic values appear in this case study — every number below comes from a
real vLLM server + NVIDIA DCGM exporter, bridged to Kafka, and the raw data is attached for audit.

> Context: IBM **completed its acquisition of Confluent** on 2026-03-17
> ([Confluent press release](https://www.confluent.io/press-release/ibm-completes-acquisition-of-confluent/),
> [IBM newsroom, Dec 2025](https://newsroom.ibm.com/2025-12-08-ibm-to-acquire-confluent-to-create-smart-data-platform-for-enterprise-generative-ai)),
> making "IBM Granite, served on open vLLM, governed in real time on Confluent" a first-class
> IBM/Red Hat/Confluent story. IBM Cloud itself documents deploying
> [granite-3.3-8b on a single L4](https://cloud.ibm.com/docs/solution-tutorials?topic=solution-tutorials-rhoai-deploy).

## What was measured

A real **Red Hat AI distribution of IBM Granite 3.3-8B Instruct**
([`RedHatAI/granite-3.3-8b-instruct`](https://huggingface.co/RedHatAI/granite-3.3-8b-instruct), FP16 —
Red Hat has not published an FP8 build for 3.3 yet) served by **vLLM** on a single **NVIDIA L4** GPU,
under a controlled closed-loop load (fixed-concurrency phases), with **NVIDIA DCGM** energy/utilization
telemetry and **vLLM** serving metrics streamed 1/s into `gpu_telemetry` and processed by the deployed
Flink statements (`flink/02_detect_anomalies.sql` etc.).

## Results — the efficiency frontier (real, audited)

The headline KPI is **`joules_per_1k_tokens`** — GPU energy per 1,000 *useful* generated tokens (the
energy cost of useful work, not just utilization). We swept **fixed concurrency 1 → 32** (each level
held ≥ 3.5 min) and measured the KPI two independent ways (see *Rigor*):

| Concurrency | GPU util | GPU power | Throughput | **J/1k (power÷tput)** | J/1k (ΔE÷Δtok) |
|---|---|---|---|---|---|
| 1  | ~100 % | 71.9 W | 15 tok/s  | **4 653** | 4 071 |
| 2  | ~100 % | 71.9 W | 30 tok/s  | **2 412** | 2 561 |
| 4  | ~100 % | 71.9 W | 59 tok/s  | **1 221** | 1 296 |
| 8  | ~100 % | 71.9 W | 118 tok/s | **612** | 589 |
| 16 | ~100 % | 72.0 W | 232 tok/s | **311** | 294 |
| 32 | ~100 % | 71.9 W | 415 tok/s | **173** | 152 |
| idle | ~0 % | 34.9 W | 0 | **NULL** | NULL |

![Efficiency frontier](../../assets/efficiency-frontier.png)

How to read it:

- **The efficiency frontier.** Batching throughput scales ~linearly (15 → 415 tok/s) while power stays
  flat at the **L4 TDP (~72 W) across every loaded level**, so energy per useful token collapses
  **~27× (4 653 → 173 J/1k)**. Because power is constant, the frontier is essentially
  `J/1k ≈ TDP / throughput` — it is *throughput-driven*. That curve is the real, measured efficiency
  frontier for Granite-3.3-8B on an L4.
- **Utilization lies; energy-per-useful-work tells the truth.** GPU utilization was **~100 % at every
  loaded level** — yet the *cost* of that work ranged ~27×. A utilization dashboard calls conc=1 and
  conc=32 equally "busy"; only J/1k exposes that conc=1 wastes ~27× the energy per token.
- **Idle = maximum waste, and it's `NULL` on purpose.** Idle still drew **34.9 W** producing **zero**
  useful tokens — energy-per-useful-work is *undefined* (division by zero). `NULL` is the honest
  signal for "infinitely inefficient", not a gap.
- **The real number is higher than a back-of-envelope ~20-30 J/1k** — FP16 8B on an L4 peaks at ~415
  tok/s, not the thousands a faster accelerator would; `J/1k = power ÷ throughput`. The point is to
  *measure* per deployment, not estimate.

## Rigor — dual-method power, cross-run consistency, sanity

- **Two independent measurement methods agree.** Every point is computed both ways from the
  **append-only** `gpu_telemetry` topic ([`data/sweep_telemetry_raw.jsonl.gz`](data/sweep_telemetry_raw.jsonl.gz),
  5 356 records): **(i)** `mean(power_watts) / throughput` — *primary*, since instantaneous DCGM power
  is rock-steady at **71.9-72.0 W (the L4 TDP) at every level**; and **(ii)** the counter delta
  `Δenergy_mJ / Δtokens` — reported for transparency. The two **agree within ±13 %**; the spread in (ii)
  is jitter from DCGM's energy-counter update cadence over the interval, not a physical effect. We
  publish (i). `data/sweep_results.csv` + `plot.py` regenerate the plot.
- **Cross-run reproducibility.** conc=32 here gives **173 J/1k** (71.9 W ÷ 415 tok/s); an *independent
  earlier single run* measured 72.2 W ÷ 416 tok/s → **173 J/1k** — the same number from a separate
  deployment. Independent runs reproduce.
- **Sanity (not exclusion).** We sanity-check that interval power stays within the L4 power envelope
  (~72 W) and that J/1k is monotonic in concurrency with idle `NULL`. All six points are physically
  valid (power 71.9-72.0 W) and retained — no points are dropped.
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
  so the counter-delta method (ii) carries ±13 % jitter; the primary method (i) uses the steady
  instantaneous power reading.
- **The business-impact figures below are an illustrative projection**, not a measured production
  saving — see that section.

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

## Business impact — real-time GPU cost governance

GPU inference is among the most expensive line items in an AI platform, and a large share is spent on
GPUs that are **allocated but idle or under-batched** — exactly what this run reproduced (idle drawing
26.3 W for zero useful output; low-concurrency costing 7× the energy per token).

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

## Provenance

| Field | Value |
|---|---|
| Model | `RedHatAI/granite-3.3-8b-instruct` (IBM Granite 3.3-8B Instruct, Red Hat AI distribution) |
| Quantization | FP16 (BF16 weights; FP8 not published for 3.3 at capture time) |
| Server | `vllm/vllm-openai:latest` (pulled 2026-06-15), OpenAI-compatible, `--max-model-len 4096` |
| GPU | NVIDIA L4 24 GB, `GPU-2e9a88f9-0e65-771b-d391-e09984261540`, driver 580.159.03 |
| Host | GCE `g2-standard-8`, us-central1-c |
| Telemetry | NVIDIA DCGM exporter 3.3.5 + vLLM Prometheus `/metrics`, bridged 1/s |
| Pipeline | Confluent Cloud for Apache Flink — `flink/02,03,05,07` |
| Captured | 2026-06-15 |
| Raw data | [`data/sweep_telemetry_raw.jsonl.gz`](data/sweep_telemetry_raw.jsonl.gz) (5 356 records) · [`data/anomalies_inpipeline.jsonl.gz`](data/anomalies_inpipeline.jsonl.gz) · [`data/sweep_results.csv`](data/sweep_results.csv) |

## References

- [vLLM production metrics](https://docs.vllm.ai/en/latest/usage/metrics.html) ·
  [NVIDIA DCGM exporter](https://github.com/NVIDIA/dcgm-exporter)
- [Red Hat AI Inference Server (vLLM) + GuideLLM benchmarking](https://developers.redhat.com/articles/2025/12/24/how-deploy-and-benchmark-vllm-guidellm-kubernetes)
- [IBM Research — efficient inference (speculative decoding), arXiv:2404.19124](https://arxiv.org/abs/2404.19124)

*Trademarks: IBM® and Granite are trademarks of IBM Corp.; NVIDIA® and DCGM are trademarks of NVIDIA
Corporation; Red Hat® is a trademark of Red Hat, Inc. Independent, unaffiliated project.*
