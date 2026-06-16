#!/usr/bin/env python3
"""Recompute the GPU efficiency frontier offline from the raw telemetry (v0.4.0).

Two honesty-driven changes vs the original ``sweep_results*.csv``:

1. **Useful work now includes prefill.** Throughput is ``prompt_tokens_total +
   generation_tokens_total`` per second, not generation tokens only. Counting prompt (prefill)
   tokens as useful work is the standard serving accounting (goodput) and stops the cost metric
   from penalising prefill-heavy / long-context requests.
2. **Both power methods are reported side by side:**
   - ``j_per_1k_power``  = mean(power_watts) / useful_throughput * 1000   (primary; power_watts is
     measured ~flat at the L4 TDP under load, so the frontier is *throughput-dominated*).
   - ``j_per_1k_counter`` = Δenergy_mj / Δuseful_tokens, computed **between DCGM energy-counter
     update instants** (the independent energy signal; the counter is coarse/stepped, hence the
     ~±13% jitter, worst on the short conc=32 window).

Phases are detected by the monotonically increasing concurrency sweep (num_requests_running reaching
1, 2, 4, 8, 16, 32); edges are trimmed. 100% offline from committed raw data.

    uv run python case-studies/granite-3.3-8b-l4/recompute_frontier.py
"""
from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
TARGETS = (1, 2, 4, 8, 16, 32)
TRIM_S = 15.0  # seconds trimmed from each phase edge to drop ramp transients

SOURCES = {
    "sweep_results.csv": "sweep_telemetry_raw.jsonl.gz",          # IBM Granite-3.3-8B
    "sweep_results_modelb.csv": "sweep_telemetry_modelb.jsonl.gz",  # Model B (Mistral-7B-v0.3)
}


def _load(path: Path) -> list[dict]:
    rows = [json.loads(line) for line in gzip.open(path, "rt")]
    rows.sort(key=lambda r: r["ts"])
    return rows


def _phases(rows: list[dict]) -> dict[int, list[dict]]:
    """Assign each loaded sample to a concurrency phase via the monotonic sweep."""
    cur = 0
    out: dict[int, list[dict]] = {}
    for r in rows:
        run = r["num_requests_running"]
        if run in TARGETS and run > cur:
            cur = run
        if cur > 0:
            out.setdefault(cur, []).append(r)
    return out


def _trim(rows: list[dict], sec: float) -> list[dict]:
    t0, t1 = rows[0]["ts"] / 1000, rows[-1]["ts"] / 1000
    inner = [r for r in rows if t0 + sec <= r["ts"] / 1000 <= t1 - sec]
    return inner or rows


def _kpi(rows: list[dict]) -> dict:
    rows = _trim(rows, TRIM_S)
    dt = (rows[-1]["ts"] - rows[0]["ts"]) / 1000
    d_gen = rows[-1]["generation_tokens_total"] - rows[0]["generation_tokens_total"]
    d_prompt = rows[-1]["prompt_tokens_total"] - rows[0]["prompt_tokens_total"]
    d_useful = d_gen + d_prompt
    power_w = sum(r["power_watts"] for r in rows) / len(rows)
    tput_gen = d_gen / dt
    tput_useful = d_useful / dt

    # Counter-delta method: align to energy-counter update instants so the coarse, stepped
    # DCGM counter is not truncated mid-step. Require >= 4 update steps, else the window is too
    # short for the coarse counter to be meaningful (this excludes the brief conc=32 phase).
    steps = [i for i in range(1, len(rows)) if rows[i]["energy_mj"] != rows[i - 1]["energy_mj"]]
    j_counter = None
    if len(steps) >= 4:
        lo, hi = steps[0] - 1, steps[-1]
        d_e = rows[hi]["energy_mj"] - rows[lo]["energy_mj"]  # millijoules
        d_u = (rows[hi]["generation_tokens_total"] + rows[hi]["prompt_tokens_total"]) - (
            rows[lo]["generation_tokens_total"] + rows[lo]["prompt_tokens_total"]
        )
        # mJ per token == J per 1000 tokens
        j_counter = d_e / d_u if d_u else None

    return {
        "power_w": round(power_w, 1),
        "tput_gen": round(tput_gen, 1),
        "tput_useful": round(tput_useful, 1),
        "j_per_1k_gen": round(power_w / tput_gen * 1000),
        "j_per_1k_power": round(power_w / tput_useful * 1000),
        "j_per_1k_counter": round(j_counter) if j_counter else None,
    }


def main() -> None:
    for out_name, gz_name in SOURCES.items():
        rows = _load(HERE / "data" / gz_name)
        ph = _phases(rows)
        out_path = HERE / "data" / out_name
        with open(out_path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(
                ["concurrency", "power_w", "useful_tok_s", "gen_tok_s",
                 "j_per_1k", "j_per_1k_gen_only", "j_per_1k_counter"]
            )
            print(f"\n{out_name}  (<- {gz_name})")
            print("  conc  power_w  useful_tps  gen_tps  J/1k(pwr)  J/1k(gen)  J/1k(cnt)")
            for c in TARGETS:
                k = _kpi(ph[c])
                w.writerow([c, k["power_w"], k["tput_useful"], k["tput_gen"],
                            k["j_per_1k_power"], k["j_per_1k_gen"], k["j_per_1k_counter"]])
                print(f"  {c:4d}  {k['power_w']:7.1f}  {k['tput_useful']:10.1f}  "
                      f"{k['tput_gen']:7.1f}  {k['j_per_1k_power']:18d}  "
                      f"{k['j_per_1k_gen']:14d}  {str(k['j_per_1k_counter']):>13}")
        print(f"  -> wrote {out_path}")


if __name__ == "__main__":
    main()
