"""Offline tests for the real-source bridge: Prometheus parsing + Avro field mapping.

These use representative vLLM and NVIDIA DCGM exposition samples (no network, no GPU), so the
field mapping that feeds the live case study is validated in CI.
"""

from gpu_efficiency_streaming.bridge import build_record, parse_prometheus

DCGM_SAMPLE = """\
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %).
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{gpu="0",UUID="GPU-abc123",modelName="NVIDIA L4"} 87
DCGM_FI_DEV_POWER_USAGE{gpu="0",UUID="GPU-abc123"} 71.4
DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION{gpu="0",UUID="GPU-abc123"} 123456789
DCGM_FI_DEV_GPU_TEMP{gpu="0",UUID="GPU-abc123"} 64
DCGM_FI_DEV_FB_USED{gpu="0",UUID="GPU-abc123"} 18250
DCGM_FI_PROF_SM_ACTIVE{gpu="0",UUID="GPU-abc123"} 0.83
DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{gpu="0",UUID="GPU-abc123"} 0.71
DCGM_FI_PROF_DRAM_ACTIVE{gpu="0",UUID="GPU-abc123"} 0.55
"""

VLLM_SAMPLE = """\
# HELP vllm:num_requests_running Number of requests currently running.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="granite"} 12.0
vllm:num_requests_waiting{model_name="granite"} 3.0
vllm:gpu_cache_usage_perc{model_name="granite"} 0.62
vllm:prompt_tokens_total{model_name="granite"} 1500000.0
vllm:generation_tokens_total{model_name="granite"} 845000.0
vllm:time_to_first_token_seconds_sum{model_name="granite"} 120.0
vllm:time_to_first_token_seconds_count{model_name="granite"} 1000.0
vllm:time_per_output_token_seconds_sum{model_name="granite"} 200.0
vllm:time_per_output_token_seconds_count{model_name="granite"} 10000.0
vllm:e2e_request_latency_seconds_sum{model_name="granite"} 5000.0
vllm:e2e_request_latency_seconds_count{model_name="granite"} 1000.0
"""


def test_parse_prometheus_handles_labels_and_comments():
    m = parse_prometheus(DCGM_SAMPLE)
    assert m["DCGM_FI_DEV_GPU_UTIL"][0][1] == 87
    assert m["DCGM_FI_DEV_GPU_UTIL"][0][0]["UUID"] == "GPU-abc123"
    # comment/HELP/TYPE lines are ignored
    assert all(not k.startswith("#") for k in m)


def test_build_record_maps_real_metrics():
    dcgm = parse_prometheus(DCGM_SAMPLE)
    vllm = parse_prometheus(VLLM_SAMPLE)
    rec = build_record(dcgm, vllm, "inference-node-a", "granite-3.3-8b-instruct", 1_700_000_000_000)

    assert rec["deployment_id"] == "inference-node-a"
    assert rec["model_id"] == "granite-3.3-8b-instruct"
    assert rec["gpu_model"] == "NVIDIA L4"
    assert rec["gpu_uuid"] == "GPU-abc123"
    assert rec["gpu_util_pct"] == 87
    assert rec["power_watts"] == 71.4
    assert rec["energy_mj"] == 123456789  # cumulative mJ counter, integer
    assert rec["fb_used_mib"] == 18250
    assert rec["num_requests_running"] == 12
    assert rec["num_requests_waiting"] == 3
    assert rec["kv_cache_usage_perc"] == 0.62
    assert rec["prompt_tokens_total"] == 1500000
    assert rec["generation_tokens_total"] == 845000
    # histogram means = sum / count
    assert rec["ttft_seconds"] == 0.12
    assert rec["inter_token_latency_s"] == 0.02
    assert rec["e2e_latency_seconds"] == 5.0


def test_build_record_tolerates_missing_metrics():
    rec = build_record({}, {}, "inference-node-a", "granite-3.3-8b-instruct", 1_700_000_000_000)
    assert rec["gpu_uuid"] == "GPU-unknown"
    assert rec["energy_mj"] == 0
    assert rec["generation_tokens_total"] == 0
    assert rec["ttft_seconds"] == 0.0
