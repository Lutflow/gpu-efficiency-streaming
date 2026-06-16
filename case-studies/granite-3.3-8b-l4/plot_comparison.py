#!/usr/bin/env python3
"""Overlay the two measured efficiency frontiers (same NVIDIA L4): Granite-3.3-8B vs Model B.

Reads ``data/sweep_results.csv`` (Granite) and ``data/sweep_results_modelb.csv`` (Model B) and
writes ``../../assets/efficiency-frontier-comparison.png``. 100% from the two measured runs.

    uv run --with matplotlib python case-studies/granite-3.3-8b-l4/plot_comparison.py
"""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
OUT = HERE.parents[1] / "assets" / "efficiency-frontier-comparison.png"
MODEL_B_NAME = "Mistral-7B-Instruct-v0.3"


def load(name):
    rows = []
    with open(HERE / "data" / name) as fh:
        for r in csv.DictReader(fh):
            rows.append((int(r["concurrency"]), float(r["useful_tok_s"]), float(r["j_per_1k"])))
    rows.sort(key=lambda x: x[1])
    return rows


def main() -> None:
    a = load("sweep_results.csv")            # Granite-3.3-8B
    b = load("sweep_results_modelb.csv")     # Model B
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    for rows, label, color in [(a, "IBM Granite-3.3-8B (FP16)", "#7e57c2"),
                               (b, f"{MODEL_B_NAME} (FP16)", "#0b8043")]:
        ax.plot([r[1] for r in rows], [r[2] for r in rows], "-o",
                color=color, lw=2, ms=7, label=label)
        for conc, t, j in rows:
            ax.annotate(f"{conc}", (t, j), textcoords="offset points",
                        xytext=(5, 5), fontsize=8, color=color)
    ax.set_xlabel("Useful throughput (prompt + generation tokens / s)")
    ax.set_ylabel("Energy per useful work (J / 1k tokens)")
    ax.set_title("Efficiency frontier on one NVIDIA L4 — Granite-3.3-8B vs Model B (measured)")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    ax.legend()
    fig.text(0.5, 0.01,
             "Same L4, same sweep (conc 1-32), same prompt. Lower-right = more efficient.",
             ha="center", fontsize=8, color="#5b6b7a")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    OUT.parent.mkdir(exist_ok=True)
    fig.savefig(OUT, dpi=130)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
