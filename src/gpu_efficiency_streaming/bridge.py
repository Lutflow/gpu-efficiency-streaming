"""`uv run bridge` -- real-source telemetry bridge (vLLM + NVIDIA DCGM -> Avro -> Kafka).

This is the **measured** counterpart to ``produce.py`` (the synthetic quickstart). It scrapes the
Prometheus ``/metrics`` endpoints of a live **vLLM** server and the **NVIDIA DCGM exporter**, maps
every field 1:1 onto the *same* ``gpu_telemetry`` Avro contract, and produces to the *same*
Confluent topic -- so the identical Flink/ML/sink pipeline runs over real hardware telemetry with
**no synthetic values whatsoever**.

Field mapping (real metric -> Avro field):

    DCGM_FI_DEV_GPU_UTIL                  -> gpu_util_pct
    DCGM_FI_DEV_POWER_USAGE               -> power_watts
    DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION  -> energy_mj          (cumulative mJ counter)
    DCGM_FI_DEV_GPU_TEMP                  -> temp_celsius
    DCGM_FI_DEV_FB_USED                   -> fb_used_mib
    DCGM_FI_PROF_SM_ACTIVE                -> sm_active_ratio
    DCGM_FI_PROF_PIPE_TENSOR_ACTIVE       -> tensor_active_ratio
    DCGM_FI_PROF_DRAM_ACTIVE              -> dram_active_ratio
    (DCGM label UUID)                     -> gpu_uuid
    vllm:num_requests_running             -> num_requests_running
    vllm:num_requests_waiting             -> num_requests_waiting
    vllm:gpu_cache_usage_perc|kv_cache..  -> kv_cache_usage_perc
    vllm:prompt_tokens_total              -> prompt_tokens_total      (cumulative counter)
    vllm:generation_tokens_total          -> generation_tokens_total  (cumulative counter)
    vllm:time_to_first_token_seconds      -> ttft_seconds          (histogram mean = sum/count)
    vllm:time_per_output_token_seconds    -> inter_token_latency_s  (histogram mean)
    vllm:e2e_request_latency_seconds      -> e2e_latency_seconds    (histogram mean)

Configuration (env, never hard-coded secrets) -- same producer creds as ``produce.py`` plus:

    VLLM_METRICS_URL  (default http://localhost:8000/metrics)
    DCGM_METRICS_URL  (default http://localhost:9400/metrics)
"""

from __future__ import annotations

import argparse
import os
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "schemas" / "gpu_telemetry.avsc"

GPU_MODEL = "NVIDIA L4"


def parse_prometheus(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse Prometheus text exposition into {metric: [(labels, value), ...]}.

    Tolerant by design: ignores ``#`` comment/HELP/TYPE lines and unparseable samples.
    """
    out: dict[str, list[tuple[dict[str, str], float]]] = defaultdict(list)
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # name{labels} value [timestamp]
        if "{" in line:
            name, rest = line.split("{", 1)
            label_str, _, val_str = rest.partition("}")
            labels = {}
            for part in _split_labels(label_str):
                if "=" in part:
                    k, v = part.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
            val_field = val_str.strip().split()
        else:
            parts = line.split()
            if len(parts) < 2:
                continue
            name, labels, val_field = parts[0], {}, parts[1:]
        try:
            value = float(val_field[0])
        except (ValueError, IndexError):
            continue
        out[name.strip()].append((labels, value))
    return out


def _split_labels(s: str) -> list[str]:
    """Split a label string on commas that are not inside quotes."""
    parts, buf, in_q = [], [], False
    for ch in s:
        if ch == '"':
            in_q = not in_q
            buf.append(ch)
        elif ch == "," and not in_q:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return parts


def _sum(metrics: dict, name: str, default: float = 0.0) -> float:
    return sum(v for _, v in metrics.get(name, [])) or default


def _first(metrics: dict, name: str, default: float = 0.0) -> float:
    series = metrics.get(name)
    return series[0][1] if series else default


def _hist_mean(metrics: dict, base: str, default: float = 0.0) -> float:
    """Cumulative mean of a Prometheus histogram = sum / count."""
    s = _sum(metrics, base + "_sum")
    c = _sum(metrics, base + "_count")
    return (s / c) if c > 0 else default


def _vllm(metrics: dict, name: str, default: float = 0.0) -> float:
    """vLLM metrics are exposed as ``vllm:<name>``; sum across model-label series."""
    return _sum(metrics, f"vllm:{name}", default)


def _gpu_uuid(dcgm: dict) -> str:
    for series in dcgm.values():
        for labels, _ in series:
            if labels.get("UUID"):
                return labels["UUID"]
            if labels.get("uuid"):
                return labels["uuid"]
    return "GPU-unknown"


def build_record(dcgm: dict, vllm: dict, deployment_id: str, model_id: str, ts_ms: int) -> dict:
    """Map real vLLM + DCGM Prometheus metrics onto the gpu_telemetry Avro record."""
    kv = vllm.get("vllm:gpu_cache_usage_perc") or vllm.get("vllm:kv_cache_usage_perc")
    kv_val = (kv[0][1] if kv else 0.0)
    return {
        "deployment_id": deployment_id,
        "model_id": model_id,
        "gpu_uuid": _gpu_uuid(dcgm),
        "gpu_model": GPU_MODEL,
        "ts": int(ts_ms),
        "gpu_util_pct": round(_first(dcgm, "DCGM_FI_DEV_GPU_UTIL"), 2),
        "sm_active_ratio": round(_first(dcgm, "DCGM_FI_PROF_SM_ACTIVE"), 4),
        "tensor_active_ratio": round(_first(dcgm, "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE"), 4),
        "dram_active_ratio": round(_first(dcgm, "DCGM_FI_PROF_DRAM_ACTIVE"), 4),
        "fb_used_mib": round(_first(dcgm, "DCGM_FI_DEV_FB_USED"), 1),
        "power_watts": round(_first(dcgm, "DCGM_FI_DEV_POWER_USAGE"), 2),
        "energy_mj": int(_first(dcgm, "DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION")),
        "temp_celsius": round(_first(dcgm, "DCGM_FI_DEV_GPU_TEMP"), 2),
        "num_requests_running": int(_vllm(vllm, "num_requests_running")),
        "num_requests_waiting": int(_vllm(vllm, "num_requests_waiting")),
        "kv_cache_usage_perc": round(kv_val, 4),
        "prompt_tokens_total": int(_vllm(vllm, "prompt_tokens_total")),
        "generation_tokens_total": int(_vllm(vllm, "generation_tokens_total")),
        "ttft_seconds": round(_hist_mean(vllm, "vllm:time_to_first_token_seconds"), 4),
        "inter_token_latency_s": round(_hist_mean(vllm, "vllm:time_per_output_token_seconds"), 5),
        "e2e_latency_seconds": round(_hist_mean(vllm, "vllm:e2e_request_latency_seconds"), 3),
    }


def _scrape(url: str, timeout: float = 5.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (trusted localhost)
        return resp.read().decode("utf-8", "replace")


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"error: required environment variable {name} is not set")
    return val


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bridge",
        description="Bridge real vLLM + NVIDIA DCGM telemetry to Kafka (Avro); no synthetic data.",
    )
    parser.add_argument("--deployment-id", default="inference-node-a")
    parser.add_argument("--model-id", default="granite-3.3-8b-instruct")
    parser.add_argument(
        "--rate-per-sec", type=float, default=float(os.environ.get("RATE_PER_SEC", "1.0"))
    )
    parser.add_argument(
        "--duration-sec", type=int, default=int(os.environ.get("DURATION_SEC", "0"))
    )
    parser.add_argument(
        "--vllm-url", default=os.environ.get("VLLM_METRICS_URL", "http://localhost:8000/metrics")
    )
    parser.add_argument(
        "--dcgm-url", default=os.environ.get("DCGM_METRICS_URL", "http://localhost:9400/metrics")
    )
    args = parser.parse_args(argv)

    try:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
            StringSerializer,
        )
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(f"error: confluent-kafka is required ({exc}).") from exc

    topic = os.environ.get("TOPIC", "gpu_telemetry")
    sr_key = _require_env("SCHEMA_REGISTRY_API_KEY")
    sr_secret = _require_env("SCHEMA_REGISTRY_API_SECRET")
    sr = SchemaRegistryClient(
        {
            "url": _require_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": f"{sr_key}:{sr_secret}",
        }
    )
    avro_serializer = AvroSerializer(sr, SCHEMA_PATH.read_text())
    key_serializer = StringSerializer("utf_8")
    producer = Producer(
        {
            "bootstrap.servers": _require_env("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": _require_env("KAFKA_API_KEY"),
            "sasl.password": _require_env("KAFKA_API_SECRET"),
            "client.id": "gpu-telemetry-bridge",
        }
    )

    interval = 1.0 / max(args.rate_per_sec, 0.1)
    t0 = time.time()
    sent = 0
    print(
        f"Bridging REAL telemetry to '{topic}' at {args.rate_per_sec}/s "
        f"(vLLM={args.vllm_url}, DCGM={args.dcgm_url}). Ctrl-C to stop."
    )
    try:
        while True:
            dcgm = parse_prometheus(_scrape(args.dcgm_url))
            vllm = parse_prometheus(_scrape(args.vllm_url))
            rec = build_record(dcgm, vllm, args.deployment_id, args.model_id, time.time() * 1000)
            producer.produce(
                topic=topic,
                key=key_serializer(rec["deployment_id"]),
                value=avro_serializer(rec, SerializationContext(topic, MessageField.VALUE)),
            )
            sent += 1
            if sent % 20 == 0:
                producer.poll(0)
                print(
                    f"  sent={sent} util={rec['gpu_util_pct']} "
                    f"gen_tokens={rec['generation_tokens_total']} energy_mj={rec['energy_mj']}"
                )
            if args.duration_sec and (time.time() - t0) >= args.duration_sec:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        producer.flush(10)
    print(f"done. total sent={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
