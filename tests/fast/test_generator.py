"""The generator is reproducible, its ledger is consistent, and it reaches the boundaries."""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path

import pytest

from samegold.generator.events import FAST, generate
from samegold.oracle.duckdb_gold import DuckDBWitness, reference_counts
from samegold.verify.invariants import conservation


def _tree_digest(root: Path) -> str:
    hasher = hashlib.blake2b(digest_size=16)
    for path in sorted(root.rglob("*.json")):
        hasher.update(str(path.relative_to(root)).encode())
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def test_same_seed_produces_byte_identical_files(tmp_path: Path) -> None:
    a = generate(tmp_path / "a", seed=1234, profile=FAST)
    b = generate(tmp_path / "b", seed=1234, profile=FAST)
    assert a.event_count == b.event_count
    assert _tree_digest(tmp_path / "a" / "bronze") == _tree_digest(tmp_path / "b" / "bronze")


def test_different_seeds_produce_different_data(tmp_path: Path) -> None:
    generate(tmp_path / "a", seed=1, profile=FAST)
    generate(tmp_path / "b", seed=2, profile=FAST)
    assert _tree_digest(tmp_path / "a" / "bronze") != _tree_digest(tmp_path / "b" / "bronze")


def test_every_boundary_case_is_present(tmp_path: Path) -> None:
    """The mutation campaign is only as good as the boundaries the data reaches.

    Every tag here was added *because* mutants survived without it; asserting their presence
    keeps a future refactor from quietly removing one and inflating the score. The set is
    compared for EQUALITY rather than containment so that a tag which stops being emitted
    fails here instead of only showing up, months later, as a surviving mutant nobody can
    explain.
    """
    generate(tmp_path / "g", seed=99, profile=FAST)
    tags = set()
    for path in (tmp_path / "g" / "bronze").rglob("*.json"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if '"boundary"' in line:
                tags.add(line.split('"boundary": "')[1].split('"')[0])
    assert tags == {
        "free_line",
        "zero_qty",
        "return_at_45d",
        "return_past_45d",
        "return_at_sale_instant",
        "zero_qty_return",
        "arrives_at_close_instant",
        "arrives_after_close",
        "amendment_after_close",
        "amendment_tie",
        # Boundary cases 11-14: the rules contract 1.3.0 added, and the two windows whose
        # ordering nothing exercised. Fifteen mutants survived until these existed.
        "price_at_bound",
        "price_past_bound",
        "qty_at_bound",
        "qty_past_bound",
        "return_qty_at_bound",
        "return_qty_past_bound",
        "amend_qty_at_bound",
        "amend_qty_past_bound",
        "duplicate_line_key_by_time",
        "duplicate_line_key_by_event_id",
        "return_cumulative_window",
        "return_tie_on_instant",
        "amendment_to_zero",
    }


def test_conservation_holds_on_the_generated_data(tmp_path: Path) -> None:
    generate(tmp_path / "g", seed=7, profile=FAST)
    counts = reference_counts(tmp_path / "g" / "bronze")
    assert (
        conservation(
            ingested=counts["raw_lines"],
            accepted=counts["accepted"],
            quarantined=counts["rejected_by_rule"] + counts["unparseable"],
            rescued=0,
            deduplicated=counts["duplicates"],
        )
        == []
    )


def test_a_closed_month_actually_moves(tmp_path: Path) -> None:
    """If no month ever reopened, the whole bitemporal model would be untested decoration."""
    result = generate(tmp_path / "g", seed=42, profile=FAST)
    by_month: dict[str, list[int]] = {}
    for (month, _as_of), values in sorted(result.ledger.revenue.items()):
        by_month.setdefault(month, []).append(values["net_cents"])
    assert any(len(set(series)) > 1 for series in by_month.values())


def test_the_witness_reproduces_the_ledger_at_every_close(tmp_path: Path) -> None:
    result = generate(tmp_path / "g", seed=5, profile=FAST)
    witness = DuckDBWitness()
    for close in result.ledger.closes:
        expected = {m: v for (m, a), v in result.ledger.revenue.items() if a == close}
        assert (
            witness.revenue(tmp_path / "g" / "bronze", dt.datetime.fromisoformat(close)) == expected
        )


@pytest.mark.parametrize("seed", [11, 12, 13])
def test_quarantine_reasons_are_all_from_the_closed_enum(tmp_path: Path, seed: int) -> None:
    from samegold.domain.contract import QuarantineReason

    result = generate(tmp_path / f"g{seed}", seed=seed, profile=FAST)
    known = {str(r) for r in QuarantineReason}
    assert set(result.ledger.quarantine) <= known
