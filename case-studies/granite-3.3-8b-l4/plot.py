#!/usr/bin/env python3
"""Plot the GPU efficiency frontier from the measured sweep.

Reads ``data/sweep_results.csv`` (columns: concurrency, throughput_tok_s, j_per_1k) and writes
``../../assets/efficiency-frontier.png``. 100% from the measured run -- no synthetic points.

    uv run python case-studies/granite-3.3-8b-l4/plot.py
"""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
OUT = HERE.parents[1] / "assets" / "efficiency-frontier.png"


def main() -> None:
    rows = []
    with open(HERE / "data" / "sweep_results.csv") as fh:
        for r in csv.DictReader(fh):
            rows.append((int(r["concurrency"]), float(r["throughput_tok_s"]), float(r["j_per_1k"])))
    rows.sort(key=lambda x: x[1])
    tps = [r[1] for r in rows]
    jpk = [r[2] for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(tps, jpk, "-o", color="#7e57c2", linewidth=2, markersize=7)
    for conc, t, j in rows:
        ax.annotate(f"conc={conc}", (t, j), textcoords="offset points", xytext=(6, 6), fontsize=9)
    ax.set_xlabel("Throughput (generated tokens / s)")
    ax.set_ylabel("Energy per useful work (J / 1k tokens)")
    ax.set_title("GPU efficiency frontier — IBM Granite 3.3-8B on NVIDIA L4 (measured)")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    fig.text(0.5, 0.01,
             "Real DCGM energy + vLLM tokens via Confluent Flink. Lower-right = more efficient.",
             ha="center", fontsize=8, color="#5b6b7a")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    OUT.parent.mkdir(exist_ok=True)
    fig.savefig(OUT, dpi=130)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
