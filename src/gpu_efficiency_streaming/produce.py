"""`uv run produce` -- structured synthetic GPU-inference telemetry producer.

Unlike the Datagen Source connector (which draws every field independently), this producer
emits a **temporally structured** signal so that the downstream ML actually has something to
learn and forecast:

* a diurnal/sawtooth duty cycle for ``gpu_util_pct`` (busy peaks, quiet valleys),
* additive Gaussian noise,
* randomly injected *idle episodes* (sustained ramps down to ~5-10% -- the idle-but-allocated
  waste the demo is about),
* and **physically correlated** dependent fields (power, tokens, energy counter, requests,
  latency, temperature all move with utilization).

The data is still **synthetic** -- it is a structured signal, not a real measurement. The schema
is the same standards-grounded Avro contract (vLLM/DCGM/OpenTelemetry) registered in Schema
Registry, so it drops onto the same Flink/ML/sink pipeline. Configuration comes from environment
variables (never hard-coded secrets):

    BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET,
    SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET,
    TOPIC (default gpu_telemetry), RATE_PER_SEC (default 1.0), DURATION_SEC (0 = run forever)
"""

from __future__ import annotations

import argparse
import math
import os
import random
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "schemas" / "gpu_telemetry.avsc"

PERIOD_SEC = 180.0  # one diurnal "day" compressed to 3 minutes for a live demo
GPU_MODEL = "NVIDIA L4"
L4_TDP_W = 72.0
L4_IDLE_W = 36.0


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


class SignalState:
    """Holds monotonic counters and idle-episode state across ticks."""

    def __init__(self, deployment_id: str, model_id: str) -> None:
        self.deployment_id = deployment_id
        self.model_id = model_id
        self.gpu_uuid = f"GPU-{random.randint(0, 0xffffffff):08x}"
        self.energy_mj = 0
        self.prompt_tokens_total = 0
        self.generation_tokens_total = 0
        self.idle_ticks_left = 0  # >0 while inside an injected idle episode
        self.tick = 0  # logical tick; drives the diurnal phase (independent of wall clock)

    def next_record(self, t0: float, interval_s: float) -> dict:
        now = time.time()
        # Phase advances by simulated elapsed time (tick * interval), so the diurnal
        # cycle is deterministic and observable regardless of wall-clock timing.
        elapsed = self.tick * interval_s
        self.tick += 1
        phase = (elapsed % PERIOD_SEC) / PERIOD_SEC
        # Diurnal duty cycle: ~17%..93% utilization.
        base_util = 55.0 + 38.0 * math.sin(2.0 * math.pi * phase)

        # Inject idle episodes: ~3% chance per tick to start a 12-22s idle ramp.
        if self.idle_ticks_left <= 0 and random.random() < 0.03:
            self.idle_ticks_left = random.randint(int(12 / interval_s), int(22 / interval_s))
        if self.idle_ticks_left > 0:
            util = _clamp(8.0 + random.gauss(0, 1.5), 0, 100)
            self.idle_ticks_left -= 1
        else:
            util = _clamp(base_util + random.gauss(0, 3.0), 0, 100)

        frac = util / 100.0
        running = max(0, round(frac * 220 + random.gauss(0, 6)))
        waiting = max(0, round((util - 75) / 25.0 * 120)) if util > 75 else 0
        power = _clamp(L4_IDLE_W + frac * (L4_TDP_W - L4_IDLE_W) + random.gauss(0, 1.5), 0, 90)

        # Monotonic counters advance faster when the GPU is busy.
        self.generation_tokens_total += max(0, int(frac * 900 + random.gauss(0, 40)))
        self.prompt_tokens_total += max(0, int(frac * 1300 + random.gauss(0, 60)))
        self.energy_mj += int(power * interval_s * 1000)  # W * s -> mJ

        saturated = util > 85
        ttft = _clamp(0.05 + (waiting / 120.0) * 8.0 + random.gauss(0, 0.05), 0.01, 30)
        tpot = _clamp(0.012 + (0.04 if saturated else 0.0) + random.gauss(0, 0.004), 0.003, 0.2)
        e2e = _clamp(0.3 + (waiting / 120.0) * 40.0 + random.gauss(0, 0.2), 0.1, 120)
        temp = _clamp(42.0 + frac * 38.0 + random.gauss(0, 1.0), 30, 95)
        return {
            "deployment_id": self.deployment_id,
            "model_id": self.model_id,
            "gpu_uuid": self.gpu_uuid,
            "gpu_model": GPU_MODEL,
            "ts": int(now * 1000),
            "gpu_util_pct": round(util, 2),
            "sm_active_ratio": round(_clamp(frac + random.gauss(0, 0.03), 0, 1), 4),
            "tensor_active_ratio": round(_clamp(frac * 0.9 + random.gauss(0, 0.04), 0, 1), 4),
            "dram_active_ratio": round(_clamp(frac * 0.8 + random.gauss(0, 0.04), 0, 1), 4),
            "fb_used_mib": round(3000.0 + frac * 18000.0, 1),
            "power_watts": round(power, 2),
            "energy_mj": self.energy_mj,
            "temp_celsius": round(temp, 2),
            "num_requests_running": running,
            "num_requests_waiting": waiting,
            "kv_cache_usage_perc": round(_clamp(frac * 0.9 + random.gauss(0, 0.03), 0, 1), 4),
            "prompt_tokens_total": self.prompt_tokens_total,
            "generation_tokens_total": self.generation_tokens_total,
            "ttft_seconds": round(ttft, 4),
            "inter_token_latency_s": round(tpot, 5),
            "e2e_latency_seconds": round(e2e, 3),
        }


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"error: required environment variable {name} is not set")
    return val


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="produce",
        description="Produce structured synthetic GPU efficiency telemetry to Kafka (Avro).",
    )
    parser.add_argument("--deployment-id", default="inference-node-a")
    parser.add_argument("--model-id", default="llm-7b")
    parser.add_argument(
        "--rate-per-sec", type=float, default=float(os.environ.get("RATE_PER_SEC", "1.0"))
    )
    parser.add_argument(
        "--duration-sec", type=int, default=int(os.environ.get("DURATION_SEC", "0"))
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
    except ImportError as exc:  # pragma: no cover - dependency hint
        raise SystemExit(
            "error: confluent-kafka is required. Install deps with `uv sync` "
            f"(import error: {exc})."
        ) from exc

    topic = os.environ.get("TOPIC", "gpu_telemetry")
    schema_str = SCHEMA_PATH.read_text()

    sr_key = _require_env("SCHEMA_REGISTRY_API_KEY")
    sr_secret = _require_env("SCHEMA_REGISTRY_API_SECRET")
    sr = SchemaRegistryClient(
        {
            "url": _require_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": f"{sr_key}:{sr_secret}",
        }
    )
    avro_serializer = AvroSerializer(sr, schema_str)
    key_serializer = StringSerializer("utf_8")
    producer = Producer(
        {
            "bootstrap.servers": _require_env("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": _require_env("KAFKA_API_KEY"),
            "sasl.password": _require_env("KAFKA_API_SECRET"),
            "client.id": "gpu-telemetry-producer",
        }
    )

    interval = 1.0 / max(args.rate_per_sec, 0.1)
    state = SignalState(args.deployment_id, args.model_id)
    t0 = time.time()
    sent = 0
    print(f"Producing structured telemetry to '{topic}' at {args.rate_per_sec}/s "
          f"(deployment={args.deployment_id}). Ctrl-C to stop.")
    try:
        while True:
            rec = state.next_record(t0, interval)
            producer.produce(
                topic=topic,
                key=key_serializer(rec["deployment_id"]),
                value=avro_serializer(rec, SerializationContext(topic, MessageField.VALUE)),
            )
            sent += 1
            if sent % 50 == 0:
                producer.poll(0)
                print(f"  sent={sent} last_util={rec['gpu_util_pct']}")
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
