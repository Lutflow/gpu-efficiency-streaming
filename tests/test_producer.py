"""Offline tests for the structured-signal producer (no Kafka required)."""

from __future__ import annotations

import json
from pathlib import Path

from gpu_efficiency_streaming.produce import SignalState

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "schemas" / "gpu_telemetry.avsc"


def _schema_field_names() -> set[str]:
    schema = json.loads(SCHEMA_PATH.read_text())
    return {f["name"] for f in schema["fields"]}


def test_record_has_exactly_schema_fields() -> None:
    state = SignalState("inference-node-a", "llm-7b")
    rec = state.next_record(t0=0.0, interval_s=1.0)
    assert set(rec.keys()) == _schema_field_names()


def test_counters_are_monotonic() -> None:
    state = SignalState("inference-node-a", "llm-7b")
    prev = state.next_record(0.0, 1.0)
    for _ in range(200):
        cur = state.next_record(0.0, 1.0)
        assert cur["energy_mj"] >= prev["energy_mj"]
        assert cur["generation_tokens_total"] >= prev["generation_tokens_total"]
        assert cur["prompt_tokens_total"] >= prev["prompt_tokens_total"]
        prev = cur


def test_values_in_plausible_ranges() -> None:
    state = SignalState("inference-node-a", "llm-7b")
    for _ in range(500):
        r = state.next_record(0.0, 1.0)
        assert 0.0 <= r["gpu_util_pct"] <= 100.0
        assert 0.0 <= r["power_watts"] <= 90.0
        assert 0.0 <= r["kv_cache_usage_perc"] <= 1.0
        assert r["num_requests_running"] >= 0
        assert r["num_requests_waiting"] >= 0


def test_signal_is_structured_not_uniform() -> None:
    """Power should correlate with utilization (busy windows draw more watts),
    proving the fields are correlated rather than independently drawn."""
    state = SignalState("inference-node-a", "llm-7b")
    recs = [state.next_record(0.0, 1.0) for _ in range(800)]
    busy = [r["power_watts"] for r in recs if r["gpu_util_pct"] > 80]
    idle = [r["power_watts"] for r in recs if r["gpu_util_pct"] < 20]
    assert busy and idle, "expected both busy and idle windows from the diurnal+idle signal"
    assert sum(busy) / len(busy) > sum(idle) / len(idle)
