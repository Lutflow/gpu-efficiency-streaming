# Contributing

Thanks for your interest in improving this project! It's a focused demo of a real-time GPU
efficiency anomaly-detection pattern on Confluent Cloud, so contributions that sharpen the
pattern, the documentation, or the developer experience are especially welcome.

## Ways to contribute

- **Bugs / inaccuracies** — open an issue (especially anything where the Flink SQL, schema, or
  Terraform doesn't match current Confluent Cloud behavior).
- **Docs** — clarity on the synthetic-vs-production story, the schema provenance, or setup.
- **Roadmap items** — see the Roadmap section of the [README](README.md) (multi-deployment,
  physically-correlated synthetic data, the OpenTelemetry Collector production source).

## Development setup

```bash
uv sync            # create the environment
uv run pytest -q   # run the test suite
uv run ruff check .
terraform -chdir=terraform fmt -recursive
terraform -chdir=terraform init -backend=false && terraform -chdir=terraform validate
```

## Before you open a PR

- `uv run ruff check .` is clean.
- `uv run pytest -q` passes.
- `terraform fmt` and `terraform validate` pass.
- **No secrets.** Credentials go in `terraform/terraform.tfvars` (gitignored) or `TF_VAR_*`
  environment variables — never in tracked files. CI runs a gitleaks scan.
- Commits are small, atomic, and use [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `chore:`, …) in English.

By contributing, you agree that your contributions are licensed under the
[Apache-2.0](LICENSE) license.
