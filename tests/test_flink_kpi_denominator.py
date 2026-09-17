"""Guard the in-pipeline efficiency KPI against a generation-only denominator regression.

The published frontier defines useful work as prefill + decode (goodput-style):
``useful = prompt_tokens + generation_tokens`` (see recompute_frontier.py and CHANGELOG
0.4.0). The Flink KPI ``joules_per_1k_tokens`` in ``flink/02_detect_anomalies.sql`` must
divide DCGM energy by that SAME useful-token denominator.

A prior capture divided by generation tokens ALONE, which under-counts the denominator and
inflates J/1k. These tests parse the KPI expression out of the SQL and assert the denominator
counts prompt tokens too. Mutation check: revert the denominator to generation-only (drop
``prompt_tokens_win`` from the division) and ``test_kpi_denominator_counts_useful_tokens``
turns red.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "flink" / "02_detect_anomalies.sql"


@pytest.fixture(scope="module")
def sql_text() -> str:
    return SQL_PATH.read_text()


@pytest.fixture(scope="module")
def kpi_denominator(sql_text: str) -> str:
    """Extract the denominator of the joules_per_1k_tokens KPI expression.

    Grammar in the SQL is:  energy_joules_win / NULLIF(CAST(<DENOM> AS DOUBLE), 0.0) * 1000.0
    We isolate <DENOM> so the assertions test exactly what J/1k is divided by, independent of
    whitespace or line breaks.
    """
    # Collapse whitespace so the regex is newline-insensitive.
    flat = re.sub(r"\s+", " ", sql_text)
    m = re.search(
        r"energy_joules_win\s*/\s*NULLIF\(\s*CAST\(\s*(?P<denom>.+?)\s+AS\s+DOUBLE\s*\)\s*,\s*0\.0\s*\)"
        r"\s*\*\s*1000\.0\s+AS\s+joules_per_1k_tokens",
        flat,
        flags=re.IGNORECASE,
    )
    assert m is not None, "could not locate the joules_per_1k_tokens KPI expression in the SQL"
    return m.group("denom").strip()


def test_sql_file_exists() -> None:
    assert SQL_PATH.is_file(), f"missing SQL at {SQL_PATH}"


def test_kpi_denominator_counts_useful_tokens(kpi_denominator: str) -> None:
    """The KPI denominator MUST include prompt (prefill) tokens, i.e. useful = prefill + decode.

    This is the regression guard: a generation-only denominator (only gen_tokens_win) makes this
    fail.
    """
    denom = kpi_denominator.lower()
    assert "prompt_tokens_win" in denom, (
        "joules_per_1k_tokens divides by a denominator that omits prompt (prefill) tokens: "
        f"{kpi_denominator!r}. Useful work is prefill + decode; dividing by generation alone "
        "is the historical defect this test exists to catch."
    )
    assert "gen_tokens_win" in denom, (
        "denominator must still include generation tokens (useful = prompt + generation): "
        f"{kpi_denominator!r}"
    )


def test_kpi_denominator_sums_prefill_and_decode(kpi_denominator: str) -> None:
    """The two windowed token deltas must be ADDED (not, e.g., subtracted or one dropped)."""
    normalized = re.sub(r"\s+", "", kpi_denominator.lower())
    assert normalized in {
        "prompt_tokens_win+gen_tokens_win",
        "gen_tokens_win+prompt_tokens_win",
    }, (
        "useful-token denominator must be exactly (prompt_tokens_win + gen_tokens_win); got "
        f"{kpi_denominator!r}"
    )
