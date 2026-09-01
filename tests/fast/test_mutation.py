"""The mutation engine generates, classifies and kills."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from samegold.generator.events import FAST, generate
from samegold.mutation.equivalents import classify
from samegold.mutation.operators import mutate_python, mutate_sql
from samegold.mutation.runner import run_mutation_campaign
from samegold.mutation.spec_mutants import SPEC_MUTANTS

REFERENCE = Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"


def test_the_generator_produces_the_operator_families_we_rely_on() -> None:
    operators = {m.operator for m in mutate_sql(REFERENCE.read_text(encoding="utf-8"))}
    # The interval family is here by name because it silently produced nothing once:
    # sqlglot parses "INTERVAL 45 DAY" with a *string* literal, so an is_number check
    # skipped every window-boundary mutant in the project.
    assert any(o.startswith("interval") for o in operators)
    assert any(o.startswith("cmp:") for o in operators)
    assert "join:kind-swap" in operators
    assert "agg:sum->max" in operators


def test_mutant_ids_are_stable_across_runs() -> None:
    sql = REFERENCE.read_text(encoding="utf-8")
    first = [(m.mutant_id, m.operator, m.original) for m in mutate_sql(sql)]
    second = [(m.mutant_id, m.operator, m.original) for m in mutate_sql(sql)]
    assert first == second


def test_python_mutants_change_the_source() -> None:
    source = "def f(a, b):\n    return a <= b\n"
    mutants = mutate_python(source, "f.py")
    assert mutants and "a < b" in mutants[0].source


def test_every_specification_mutant_still_finds_its_anchor() -> None:
    """A spec mutant whose anchor drifted would silently stop being tested."""
    sql = REFERENCE.read_text(encoding="utf-8")
    for spec in SPEC_MUTANTS:
        mutated = spec.apply(sql)
        assert mutated != sql, f"{spec.mutant_id} produced no change"


def test_equivalence_classification_is_written_not_guessed() -> None:
    assert classify("join:kind-swap", "FULL OUTER JOIN refunds AS r ON x") is not None
    assert classify("cmp:LTE->LT", "a <= b") is None


def test_the_campaign_kills_the_imputation_mutant(tmp_path: Path) -> None:
    """SPEC-01 is the mutant this whole project exists to catch. If it ever survives, the
    two implementations have agreed on the same misunderstanding."""
    result = generate(tmp_path / "g", seed=42, profile=FAST)
    ledger = json.loads((tmp_path / "g" / "truth" / "ledger.json").read_text(encoding="utf-8"))
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    run = run_mutation_campaign(
        REFERENCE.read_text(encoding="utf-8"), tmp_path / "g" / "bronze", ledger, closes
    )
    assert run.detail["SPEC-01"]["killed_by"], "SPEC-01 survived: the witnesses are blind to it"
    assert "ledger" in run.detail["SPEC-01"]["killed_by"]
