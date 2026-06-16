# VERIFICATION MANIFEST — v0.4.0 hardening

> Audit artifact for the double-verification gate. Branch `hardening/v0.4.0` (local only, **not
> pushed, not tagged**). Floor `a9aa495` intact. This documents every claim, the recompute, the diff,
> what was deliberately **not** incorporated, and the local gates. Reviewer: Kiro.

## 1. Scope of changes

P0 correctness + honest framing · P1 positioning · P2 roadmap. Additive; no floor regression.

| Area | File(s) |
|---|---|
| Recompute (useful = prefill+decode, dual-method) | `case-studies/granite-3.3-8b-l4/recompute_frontier.py` (new), `data/sweep_results.csv`, `data/sweep_results_modelb.csv` |
| Plots relabelled + regenerated | `plot.py`, `plot_comparison.py`, `assets/efficiency-frontier.png`, `assets/efficiency-frontier-comparison.png` |
| Prefill-aware detector | `flink/02_detect_anomalies.sql` (exposes `prompt_tokens_win`), `flink/08_waste_high_util.sql` |
| Case study (results, rigor, related work, build-vs-buy, comparison, unit econ, limitations, roadmap) | `case-studies/granite-3.3-8b-l4/README.md` |
| Notebook (rebuilt, moat-clean, re-executed) | `case-studies/granite-3.3-8b-l4/analysis.ipynb` |
| Main README callout + comparative line | `README.md` |
| Version + changelog | `CHANGELOG.md`, `pyproject.toml`, `CITATION.cff` |

## 2. Data verification (prefill recompute)

**Source telemetry (committed, raw):** `data/sweep_telemetry_raw.jsonl.gz` (Granite, 5 356 rows) and
`data/sweep_telemetry_modelb.jsonl.gz` (Mistral-7B, 5 257 rows). **Both** contain the required fields,
verified by reading row 0:

`prompt_tokens_total` ✅ · `generation_tokens_total` ✅ · `energy_mj` (cumulative mJ) ✅ ·
`power_watts` ✅ · `num_requests_running` ✅ · `gpu_util_pct` ✅.

**Useful work redefined:** `useful = prompt_tokens_total + generation_tokens_total` (was: generation
only). Recomputed offline by `recompute_frontier.py` (phases = monotonic concurrency sweep; 15 s edge
trim; idempotent — re-running leaves the CSVs byte-identical).

**Power is measured flat at the L4 TDP under load** (idle is a separate bookend), which is what makes
the frontier throughput-dominated:

| Model | conc=1 power | conc=32 power | idle |
|---|---|---|---|
| Granite | 71.9 W | 71.9 W | ~33-36 W |
| Mistral-7B | 70.8 W | 71.8 W | ~30 W |

### Before → after (J/1k), Granite-3.3-8B

| conc | gen-only (old, v0.3.0) | **useful (new, primary)** | counter-delta (useful) |
|---|---|---|---|
| 1  | 4 639 | **4 192** | 4 614 |
| 2  | 2 413 | **2 173** | 2 507 |
| 4  | 1 221 | **1 089** | 1 301 |
| 8  | 611   | **551**   | 640 |
| 16 | 311   | **280**   | 333 |
| 32 | 173   | **154**   | — (window too short) |
| span | 26.8× | **27.2×** | — |

### Before → after (J/1k), Mistral-7B-Instruct-v0.3

| conc | gen-only (old) | **useful (new)** | counter-delta |
|---|---|---|---|
| 1  | 3 972 | **3 552** | 3 748 |
| 2  | 2 096 | **1 876** | 2 257 |
| 4  | 1 056 | **944**   | 1 101 |
| 8  | 535   | **478**   | 569 |
| 16 | 271   | **243**   | 291 |
| 32 | 149   | **135**   | — (omitted) |

**Effect:** including prefill lowers every J/1k by ~10 % (prefill is ~10-12 % of total tokens across the
sweep) and leaves the ~27× batching span unchanged. The generation-only throughput reproduces the
original v0.3.0 CSV (e.g. Granite conc=32 = 414.8 gen tok/s ≈ original 415), validating that the phase
segmentation matches the prior methodology — i.e. the only thing that changed is the token definition.

## 3. Dual-method (power vs counter-delta) — measured agreement

Method (i) `mean(power_watts)/useful-throughput` is **primary**. Method (ii) `Δenergy_mJ/Δtokens`
(between energy-counter update instants, ≥4 steps required) is the **independent** signal.

| conc | Granite offset (ii vs i) | Mistral offset |
|---|---|---|
| 1  | +10.1 % | +5.5 % |
| 2  | +15.4 % | +20.3 % |
| 4  | +19.5 % | +16.6 % |
| 8  | +16.2 % | +19.0 % |
| 16 | +18.9 % | +19.8 % |
| 32 | omitted (short window) | omitted |

**Correction logged:** v0.3.0 described the two methods as agreeing within a symmetric **±13 %**. The
recomputed, step-aligned counter-delta runs **systematically ~5-20 % above** the power method (median
~16 %) — the coarse DCGM energy counter integrates transients the 1/s instantaneous sampling
under-counts. The docs now state this honestly (Rigor + Limitations) rather than the symmetric ±13 %.

## 4. Citations (claim | URL | HTTP | quote)

All URLs checked with `curl -IL`. Approved set pre-verified by Kiro; quotes confirmed here.

| Claim it supports | URL | HTTP | Quote (verbatim from source) |
|---|---|---|---|
| Utilization-based metrics are insufficient (fleet) | <https://arxiv.org/abs/2502.06982> | 200 | "We identify critical gaps in conventional utilization-based performance metrics and propose 'ML Productivity Goodput' (MPG)" |
| DCGM/nvidia-smi power sampling is biased | <https://arxiv.org/abs/2312.02741> | 200 | "on the A100 and H100 GPUs only 25% of the runtime is sampled for power consumption … can lead to a drastic under/overestimation of energy consumed" (SC'24; Related DOI 10.1109/SC41406.2024.00028) |
| Energy-per-token is an established metric | <https://arxiv.org/abs/2603.20224> | 200 | "we propose energy efficiency metrics, including Energy-per-Token, as complements to traditional accuracy benchmarks" |
| "Useful work" = goodput (SLO-bounded) | <https://arxiv.org/abs/2410.14257> | 200 | "we revisit SLO and goodput metrics in LLM serving and propose a unified metric framework smooth goodput" |
| Public energy-efficiency leaderboard | <https://huggingface.github.io/AIEnergyScore> | 200 | AI Energy Score leaderboard (landing page; companion: "compares the energy efficiency of AI models across 10 tasks") |
| Automated energy-optimization recommendations exist | <https://arxiv.org/abs/2505.06371> | 200 | "automated optimization recommendations can lead to significant (sometimes more than 40%) energy savings without changing what is being computed" |
| MFU/MBU; inference is memory-bound; util ≠ useful work | <https://www.zettabyte.space/blog/gpu-utilization-mfu-mbu> | 200 | "GPU utilization … mostly answers whether the GPU was busy, not whether it was doing the work your model needs" |
| Incumbent GPU idle-cost + recommendations (DCGM) | <https://docs.kubecost.com/using-kubecost/navigating-the-kubecost-ui/savings/gpu-optimization> | 200 | "proactively identifies ways in which you can save money … collects and processes GPU utilization metrics" |
| Open-source cost-monitoring incumbent | <https://github.com/opencost/opencost> | 200 | OpenCost — CNCF open-source cost monitoring (project repo) |
| Offline serving benchmark (prior art) | <https://github.com/vllm-project/guidellm> | 200 | GuideLLM — vLLM/Red Hat serving benchmark (project repo) |

No new/unapproved citations were introduced beyond the approved set + the two repo links
(OpenCost, GuideLLM) and the DCGM-exporter/vLLM/GCP links already in the repo.

## 5. Discards — what from the external audit was NOT incorporated, and why

- **"Energy varies only ~3-5×, so 27× is inflated" (arXiv 2601.22076): REJECTED — contradicted by our
  measured data.** That hypothesis assumed power rises with load; our `power_watts` is **measured flat
  at ~72 W at every concurrency including conc=1** under continuous load (idle 26-36 W is a separate
  unloaded bookend). The ~27× is therefore a real, throughput-driven span, not a pinning artifact. We
  did **not** invent a lower number (no over-correction); we reframed it as throughput-dominated.
- **Did not switch the headline to MFU/goodput numerically.** We anchor "useful work" to goodput
  conceptually and cite it, but keep the energy-per-useful-token KPI (it is the project's governed
  signal). MFU would need per-model FLOP accounting — listed as future work, not faked.
- **Removed the MLCommons/MLPerf link** from Related work (it bot-blocks to 403 and was not in the
  approved set); GuideLLM (200) is retained as the offline-benchmark exemplar.
- **conc=32 counter-delta omitted** (both models): the phase is too short (~60-85 s) for the coarse,
  stepped DCGM energy counter — reported as a limitation, not patched with a dubious number.

## 6. Local gates (CI parity)

| Gate | Command | Result |
|---|---|---|
| Unit tests | `uv run pytest -q` | **21 passed** |
| Lint | `uv run ruff check .` | **All checks passed** |
| Terraform | `terraform fmt -check -recursive` + `validate` | **fmt clean · valid** |
| Secret scan | `gitleaks git .` (incl. `pipeline/*.png`) | **no leaks** |
| Leak/moat words | `git grep -niE "sentinel\|pcpo\|conformal\|moat\|acquisition"` | **0** |
| Markdown lint | `markdownlint-cli2` (docker `davidanson/markdownlint-cli2`, 18 files) | **0 errors** |
| Notebook reproducibility | `jupyter nbconvert --execute --inplace analysis.ipynb` | **executes, 295 049 bytes** |
| Broken image refs | local scan of all `![](…)` | **0** |
| External links | `curl -IL` on changed docs | **all 200** except Red Hat dev article **403** (bot-block, browser-valid, pre-existing; CI does not check links) |
| Recompute idempotent | re-run `recompute_frontier.py`, diff CSV | **byte-identical** |

## 7. Gate status

**PAUSED for Kiro's audit.** No push, no tag, `main` untouched, floor `a9aa495` intact. On Kiro's
explicit OK: merge `hardening/v0.4.0` → `main`, push, confirm CI green (5 jobs), move/annotate tag
`v0.4.0`. Open question for Kiro: whether this `VERIFICATION-MANIFEST.md` ships in the public repo or
is stripped at merge (it is an internal audit artifact).
