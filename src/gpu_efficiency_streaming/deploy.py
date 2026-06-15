"""`uv run deploy` -- provision the whole demo on Confluent Cloud via Terraform.

This is a thin, dependable wrapper around Terraform. All infrastructure (cluster,
Schema Registry, Flink compute pool, connectors, and the Flink SQL statements that
run ML_DETECT_ANOMALIES) is declared under ``terraform/``; the Flink statements read
their SQL from ``flink/*.sql`` via ``file()``. So this single command stands the
entire pipeline up reproducibly, then prints the Stream Lineage URL to screenshot.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    """Walk upward from ``start`` (or cwd) until a directory with pyproject.toml."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "terraform").is_dir():
            return candidate
    raise SystemExit(
        "error: could not locate the repo root (a directory containing both "
        "pyproject.toml and terraform/). Run this from inside the repository."
    )


def run(cmd: list[str], cwd: Path) -> None:
    print(f"\n$ {' '.join(cmd)}  (in {cwd})", flush=True)
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def capture(cmd: list[str], cwd: Path) -> str:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="deploy",
        description="Provision the GPU efficiency streaming demo on Confluent Cloud via Terraform.",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Only run 'terraform plan' (no changes applied).",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Pass -auto-approve to 'terraform apply' (skip the interactive prompt).",
    )
    parser.add_argument(
        "--skip-init",
        action="store_true",
        help="Skip 'terraform init' (use when providers are already installed).",
    )
    args = parser.parse_args(argv)

    if shutil.which("terraform") is None:
        raise SystemExit("error: terraform is not installed or not on PATH.")

    repo_root = find_repo_root()
    tf_dir = repo_root / "terraform"

    if not (tf_dir / "terraform.tfvars").is_file():
        print(
            "warning: terraform/terraform.tfvars not found. Copy "
            "terraform/terraform.tfvars.example to terraform/terraform.tfvars and fill it in,\n"
            "         or provide credentials via TF_VAR_* environment variables.",
            file=sys.stderr,
        )

    if not args.skip_init:
        run(["terraform", "init", "-input=false"], cwd=tf_dir)

    if args.plan:
        run(["terraform", "plan", "-input=false"], cwd=tf_dir)
        return 0

    apply_cmd = ["terraform", "apply", "-input=false"]
    if args.auto_approve:
        apply_cmd.append("-auto-approve")
    run(apply_cmd, cwd=tf_dir)

    lineage = capture(["terraform", "output", "-raw", "stream_lineage_url"], cwd=tf_dir)
    print("\n" + "=" * 70)
    print("Deployment complete.")
    if lineage:
        print(f"Stream Lineage (screenshot this for the challenge form):\n  {lineage}")
    print(
        "Allow ~7-8 minutes for ML_DETECT_ANOMALIES to warm up "
        "(minTrainingSize=30, enableStl, m=12); ML_FORECAST starts a little later."
    )
    print("Tear everything down with:  uv run destroy")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
