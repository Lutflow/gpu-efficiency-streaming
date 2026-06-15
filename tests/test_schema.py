"""Validate the public Avro telemetry schema.

These tests assert the schema parses, carries the standards-grounded fields the
README and Flink SQL depend on, and contains no leftover real/internal names.
"""

from __future__ import annotations

import json
from pathlib import Path

import fastavro
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "schemas" / "gpu_telemetry.avsc"

# Fields the Flink SQL (TUMBLE + ML_DETECT_ANOMALIES + KPI) relies on.
REQUIRED_FIELDS = {
    "deployment_id",
    "model_id",
    "gpu_uuid",
    "gpu_model",
    "ts",
    "gpu_util_pct",
    "num_requests_running",
    "generation_tokens_total",
    "energy_mj",
}


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def test_schema_file_exists() -> None:
    assert SCHEMA_PATH.is_file(), f"missing schema at {SCHEMA_PATH}"


def test_schema_parses_with_fastavro(schema: dict) -> None:
    # Raises if the Avro schema is structurally invalid.
    parsed = fastavro.parse_schema(schema)
    assert parsed["name"].endswith("GpuInferenceTelemetry")


def test_record_identity(schema: dict) -> None:
    assert schema["type"] == "record"
    assert schema["name"] == "GpuInferenceTelemetry"
    assert schema["namespace"] == "io.lutflow.demo.v1"


def test_required_fields_present(schema: dict) -> None:
    names = {f["name"] for f in schema["fields"]}
    missing = REQUIRED_FIELDS - names
    assert not missing, f"schema is missing required fields: {sorted(missing)}"


def test_ts_is_epoch_millis_long(schema: dict) -> None:
    """ts is a plain epoch-millis long (no logicalType): Flink reads it as BIGINT so
    TO_TIMESTAMP_LTZ(ts, 3) can build the event-time column for the watermark."""
    ts = next(f for f in schema["fields"] if f["name"] == "ts")
    assert ts["type"] == "long"


def test_every_field_is_documented(schema: dict) -> None:
    undocumented = [f["name"] for f in schema["fields"] if not f.get("doc")]
    assert not undocumented, f"fields without a doc string: {undocumented}"


def test_only_standards_grounded_fields(schema: dict) -> None:
    """Guardrail: the public schema contains exactly the standards-grounded fields and
    nothing else, so no internal/derived field can leak in. Uses an allowlist (it names
    no internal concepts)."""
    allowed = {
        "deployment_id", "model_id", "gpu_uuid", "gpu_model", "ts",
        "gpu_util_pct", "sm_active_ratio", "tensor_active_ratio", "dram_active_ratio",
        "fb_used_mib", "power_watts", "energy_mj", "temp_celsius",
        "num_requests_running", "num_requests_waiting", "kv_cache_usage_perc",
        "prompt_tokens_total", "generation_tokens_total",
        "ttft_seconds", "inter_token_latency_s", "e2e_latency_seconds",
    }
    names = {f["name"] for f in schema["fields"]}
    unexpected = names - allowed
    assert not unexpected, f"unexpected (non-standards-grounded) fields leaked: {unexpected}"
