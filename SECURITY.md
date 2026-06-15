# Security Policy

## Reporting a vulnerability

Please report security issues privately via GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository, or by email to **<oscarmatiasg@lutflow.com>**. Please do not open a public issue for
security reports. We aim to acknowledge reports within 3 business days.

## Secrets and credentials

This repository is designed to contain **zero secrets**:

- All credentials (Confluent Cloud API keys, AWS access keys) are supplied at
  deploy time through `terraform/terraform.tfvars` (gitignored) or `TF_VAR_*` environment variables.
- `terraform/terraform.tfvars.example` contains **placeholders only**.
- `.gitignore` excludes `*.tfvars` (except the `.example`), `.env`, `*.tfstate`, key material, and
  credential files.
- CI runs [gitleaks](https://github.com/gitleaks/gitleaks) on every push and pull request.

If you believe a secret was committed, treat it as compromised: rotate it immediately and open a
private report so history can be scrubbed.

## Scope

This is a demonstration project using synthetic data. It is not intended to be run as a
production service as-is; review the IAM/network posture of any real deployment you derive from it.
