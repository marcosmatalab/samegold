"""The consumption layer: a page a person reads, and the alert rule behind it."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from samegold.generator.events import FAST, generate
from samegold.oracle.duckdb_gold import revenue_versions
from samegold.serve.freshness import evaluate_freshness
from samegold.serve.report import render_report

NOW = dt.datetime(2026, 3, 10, 12, 0, tzinfo=dt.UTC)


def test_a_fresh_pipeline_raises_nothing() -> None:
    assert (
        evaluate_freshness(
            newest_arrival=NOW - dt.timedelta(minutes=5),
            closed_months=["2026-02"],
            now=NOW,
        )
        == []
    )


def test_a_stale_pipeline_is_reported_with_the_lag() -> None:
    breaches = evaluate_freshness(NOW - dt.timedelta(hours=3), ["2026-02"], NOW)
    assert [b.kind for b in breaches] == ["ingestion_lag"]
    assert breaches[0].lag_seconds > breaches[0].threshold_seconds


def test_a_missing_close_is_a_different_alert_from_a_stale_pipeline() -> None:
    """They fail for different reasons and get fixed by different people, so they are not one
    alert with two causes."""
    breaches = evaluate_freshness(NOW - dt.timedelta(minutes=1), [], NOW)
    assert [b.kind for b in breaches] == ["close_overdue"]


def test_no_data_at_all_is_not_reported_as_staleness() -> None:
    breaches = evaluate_freshness(None, ["2026-02"], NOW)
    assert breaches and breaches[0].kind == "no_data"


def test_the_report_shows_every_version_and_marks_the_restatements(tmp_path: Path) -> None:
    result = generate(tmp_path / "g", seed=42, profile=FAST)
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    versions = revenue_versions(tmp_path / "g" / "bronze", closes)
    page = render_report(versions, NOW)
    assert page.startswith("<!doctype html>")
    assert page.count("<tr") >= len(versions)
    assert 'class="restated"' in page, "no restatement is shown, and the data has some"
    # Self-contained: nothing to fetch, nothing to execute.
    for forbidden in ("<script", "http://", "https://", "cdn"):
        assert forbidden not in page.lower(), f"the report reaches for {forbidden}"


def test_the_report_does_not_leak_an_identifier(tmp_path: Path) -> None:
    from samegold.governance.policy import check_gold_exposure

    result = generate(tmp_path / "g", seed=5, profile=FAST)
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    versions = revenue_versions(tmp_path / "g" / "bronze", closes)
    assert check_gold_exposure(versions) == []
