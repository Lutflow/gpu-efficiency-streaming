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

## Results (real, audited)

The headline KPI is **`joules_per_1k_tokens`** — DCGM energy per 1,000 *useful* generated tokens, i.e.
the energy cost of useful work (not just utilization).

| Phase | Concurrency | GPU util | GPU power | Throughput | **J / 1k tokens** |
|---|---|---|---|---|---|
| **BUSY** | 32 (fixed) | ~100 % | **72.2 W** | **416 tok/s** | **173** |
| **LOW**  | 4 (fixed)  | ~94 %  | 70.5 W | 60 tok/s | **1 182** |
| **IDLE** | 0          | ~0 %   | 26.3 W | 0 | **NULL** |

Real vLLM serving metrics over the run (n = 876 requests): **TTFT mean 221 ms**, end-to-end mean
15.2 s, ≈ 76 ms/token, peak aggregate **416 tok/s**.

How to read it:

- **Utilization lies; energy-per-useful-work tells the truth.** The LOW phase ran at ~94 % GPU
  utilization yet cost **~7× more energy per token** than BUSY (1 182 vs 173 J/1k) — because at low
  concurrency the same ~71 W is amortized over far fewer batched tokens. A utilization dashboard would
  call both "busy"; the J/1k KPI exposes the waste.
- **Idle = maximum waste, and it's `NULL` on purpose.** At idle the L4 still drew **26.3 W** while
  producing **zero** useful tokens — energy-per-useful-work is *undefined* (division by zero). `NULL`
  is the honest signal for "infinitely inefficient", not a gap.
- **The real number is higher than a back-of-envelope ~20-30 J/1k** because FP16 8B on an L4 sustains
  ~416 tok/s, not the thousands a faster accelerator would; J/1k = power ÷ throughput. This is the
  point of *measuring* rather than estimating.

## Rigor — cross-check + physical gates

- **Audit cross-check (primary).** All numbers above are computed directly from the **append-only**
  `gpu_telemetry` topic (`data/gpu_telemetry_raw_sample.jsonl`, 852 rows): for each phase,
  `Δenergy_J / Δgenerated_tokens` over the sustained interval. The same `Δenergy/Δtokens` formula is
  what `flink/02_detect_anomalies.sql` computes per 15 s window in-pipeline (the live pipeline emits
  the populated KPI; its changelog topic compacts historical windows, so the reproducible audit reads
  the retained raw topic).
- **Physical gate.** Any window implying energy `> 72 W × 15 s = 1 080 J` is discarded as an
  edge/aggregation artifact (the L4 TDP is 72 W). Measured BUSY power 72.2 W sits exactly at TDP.
- **Ordering gate.** `busy (173) < low (1 182) < idle (NULL)` — the expected, physically necessary
  ordering holds.

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

# 4. Drive sustained fixed-concurrency load: IDLE(>=3m) -> LOW(conc 4, >=4m)
#    -> BUSY(conc 32, >=6m) -> IDLE(>=3m); then audit data/gpu_telemetry_raw_sample.jsonl
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
| Raw data | [`data/gpu_telemetry_raw_sample.jsonl`](data/gpu_telemetry_raw_sample.jsonl) (852 records) |

## References

- [vLLM production metrics](https://docs.vllm.ai/en/latest/usage/metrics.html) ·
  [NVIDIA DCGM exporter](https://github.com/NVIDIA/dcgm-exporter)
- [Red Hat AI Inference Server (vLLM) + GuideLLM benchmarking](https://developers.redhat.com/articles/2025/12/24/how-deploy-and-benchmark-vllm-guidellm-kubernetes)
- [IBM Research — efficient inference (speculative decoding), arXiv:2404.19124](https://arxiv.org/abs/2404.19124)

*Trademarks: IBM® and Granite are trademarks of IBM Corp.; NVIDIA® and DCGM are trademarks of NVIDIA
Corporation; Red Hat® is a trademark of Red Hat, Inc. Independent, unaffiliated project.*
