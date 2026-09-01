"""CONTRACT.md and domain/contract.py must not disagree.

A contract that lives in two places drifts. Here the document is the human-readable half and
the module is the machine-readable one, and this test is the join between them.
"""

from __future__ import annotations

from pathlib import Path

from samegold.domain.contract import (
    ACCOUNTING_TIMEZONE,
    CONTRACT_VERSION,
    RETURN_WINDOW_DAYS,
    QuarantineReason,
)

CONTRACT = (Path(__file__).resolve().parents[2] / "CONTRACT.md").read_text(encoding="utf-8")


def test_the_version_matches() -> None:
    assert f"Version {CONTRACT_VERSION}" in CONTRACT


def test_the_window_matches() -> None:
    assert f"{RETURN_WINDOW_DAYS} days" in CONTRACT


def test_the_timezone_matches() -> None:
    assert ACCOUNTING_TIMEZONE in CONTRACT


def test_the_sql_reference_enforces_the_same_window() -> None:
    sql = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"
    ).read_text(encoding="utf-8")
    assert f"INTERVAL {RETURN_WINDOW_DAYS} DAY" in sql


def test_every_quarantine_reason_is_reachable_in_the_spark_rules() -> None:
    """A reason nobody can produce is a reason nobody maintains."""
    source = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "pipelines" / "transform.py"
    ).read_text(encoding="utf-8")
    generator = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "generator" / "events.py"
    ).read_text(encoding="utf-8")
    for reason in QuarantineReason:
        assert str(reason) in source or reason.name in generator, (
            f"{reason} is declared in the contract but no implementation can emit it"
        )
