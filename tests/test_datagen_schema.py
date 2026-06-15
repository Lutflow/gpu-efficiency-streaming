"""Validate the Datagen Source generator schema.

Asserts the generator mirrors the public schema field-for-field, uses only generic
(non-real) identifiers, and seeds a bimodal gpu_util distribution so the demo has
idle-waste anomalies for ML_DETECT_ANOMALIES to flag.
"""

from __future__ import annotations

import json
from pathlib import Path

import fastavro
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
AVSC_PATH = REPO_ROOT / "schemas" / "gpu_telemetry.avsc"
DATAGEN_PATH = REPO_ROOT / "scripts" / "datagen_schema.json"


@pytest.fixture(scope="module")
def avsc() -> dict:
    return json.loads(AVSC_PATH.read_text())


@pytest.fixture(scope="module")
def datagen() -> dict:
    return json.loads(DATAGEN_PATH.read_text())


def test_datagen_is_valid_json_and_avro(datagen: dict) -> None:
    # arg.properties are non-standard extension keys; fastavro ignores unknown keys
    # on field types, so a successful parse confirms the base Avro shape is valid.
    parsed = fastavro.parse_schema(datagen)
    assert parsed["name"].endswith("GpuInferenceTelemetry")


def test_datagen_matches_public_schema_fields(avsc: dict, datagen: dict) -> None:
    avsc_fields = [f["name"] for f in avsc["fields"]]
    datagen_fields = [f["name"] for f in datagen["fields"]]
    assert datagen_fields == avsc_fields, (
        "datagen generator fields must match the public schema exactly "
        f"(public={avsc_fields}, datagen={datagen_fields})"
    )


def test_uses_generic_identifiers_only(datagen: dict) -> None:
    fields = {f["name"]: f for f in datagen["fields"]}
    deployments = fields["deployment_id"]["type"]["arg.properties"]["options"]
    models = fields["model_id"]["type"]["arg.properties"]["options"]
    assert all(d.startswith("inference-node-") for d in deployments), deployments
    # model_id must be a public, open-source model identifier (no internal/code names).
    allowed_models = {"granite-3.3-8b-instruct"}
    assert set(models) <= allowed_models, models


def test_single_deployment_id(datagen: dict) -> None:
    """Fix A4: global iteration counters require a single deployment so the
    within-window MAX-MIN delta is not interleaved across deployments."""
    fields = {f["name"]: f for f in datagen["fields"]}
    options = fields["deployment_id"]["type"]["arg.properties"]["options"]
    assert options == ["inference-node-a"], options


def test_ts_is_epoch_millis_long(datagen: dict) -> None:
    """Fix A1: ts is a plain epoch-millis long (no logicalType) so Flink reads it as
    BIGINT and the watermark is built from TO_TIMESTAMP_LTZ(ts, 3), not from a logical
    type the Datagen connector might not preserve."""
    fields = {f["name"]: f for f in datagen["fields"]}
    ts_type = fields["ts"]["type"]
    assert ts_type["type"] == "long"
    assert "logicalType" not in ts_type
    assert "iteration" in ts_type["arg.properties"]


def test_gpu_util_is_bimodal_for_anomalies(datagen: dict) -> None:
    fields = {f["name"]: f for f in datagen["fields"]}
    options = fields["gpu_util_pct"]["type"]["arg.properties"]["options"]
    idle = [v for v in options if v < 20]
    busy = [v for v in options if v > 80]
    assert idle, "expected some idle (low) gpu_util values to create anomalies"
    assert busy, "expected some busy (high) gpu_util baseline values"


def test_counters_use_iteration(datagen: dict) -> None:
    """Monotonic counters must increase so within-window deltas are meaningful."""
    fields = {f["name"]: f for f in datagen["fields"]}
    for counter in ("energy_mj", "generation_tokens_total", "prompt_tokens_total"):
        props = fields[counter]["type"]["arg.properties"]
        assert "iteration" in props, f"{counter} should use an iteration (monotonic) generator"
        assert props["iteration"]["step"] > 0
