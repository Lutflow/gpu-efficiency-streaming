"""`uv run destroy` -- tear down all demo infrastructure with `terraform destroy`."""

from __future__ import annotations

import argparse
import shutil

from gpu_efficiency_streaming.deploy import find_repo_root, run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="destroy",
        description="Destroy the GPU efficiency streaming demo infrastructure.",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Pass -auto-approve to 'terraform destroy' (skip the interactive prompt).",
    )
    args = parser.parse_args(argv)

    if shutil.which("terraform") is None:
        raise SystemExit("error: terraform is not installed or not on PATH.")

    tf_dir = find_repo_root() / "terraform"
    cmd = ["terraform", "destroy", "-input=false"]
    if args.auto_approve:
        cmd.append("-auto-approve")
    run(cmd, cwd=tf_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
