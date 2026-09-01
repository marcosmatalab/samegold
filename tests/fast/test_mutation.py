"""The mutation engine generates, classifies honestly, and kills what it should."""

from __future__ import annotations

from pathlib import Path

from samegold.generator.events import FAST, generate
from samegold.mutation.assumption_probe import probe_data_assumption, probe_structural_assumption
from samegold.mutation.equivalents import ASSUMPTIONS, EQUIVALENCES, classify
from samegold.mutation.operators import mutate_python, mutate_sql
from samegold.mutation.spec_mutants import SPEC_MUTANTS

REFERENCE = Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"


def test_the_generator_produces_the_operator_families_we_rely_on() -> None:
    operators = {m.operator for m in mutate_sql(REFERENCE.read_text(encoding="utf-8"))}
    # The number family is here by name because it replaced the interval family when the
    # return window moved from `INTERVAL 45 DAY` to a comparison in seconds. Without a
    # replacement, the whole class of window-boundary mistakes would have stopped being
    # tested the moment the SQL changed, and the mutation score would have gone UP.
    assert "number:+1" in operators
    assert any(o.startswith("cmp:") for o in operators)
    assert "join:kind-swap" in operators
    assert "agg:sum->max" in operators
    assert "coalesce:drop-default" in operators


def test_mutants_are_not_generated_for_function_arguments() -> None:
    """read_json(format = 'newline_delimited') parses as an equality.

    Mutating it produces a binder error, which the campaign then counted as a kill: eight
    mutants were being credited to the SQL parser working. They are not generated any more.
    """
    mutants = mutate_sql(REFERENCE.read_text(encoding="utf-8"))
    assert not any("format" in m.original and m.operator.startswith("cmp") for m in mutants)
    assert not any("union_by_name" in m.original for m in mutants)


def test_mutant_ids_and_contexts_are_stable_across_runs() -> None:
    sql = REFERENCE.read_text(encoding="utf-8")
    first = [(m.mutant_id, m.operator, m.context, m.original) for m in mutate_sql(sql)]
    second = [(m.mutant_id, m.operator, m.context, m.original) for m in mutate_sql(sql)]
    assert first == second


def test_every_mutant_knows_which_cte_it_lives_in() -> None:
    contexts = {m.context for m in mutate_sql(REFERENCE.read_text(encoding="utf-8"))}
    assert {"dedup", "amendments", "final"} <= contexts


def test_python_mutants_change_the_source() -> None:
    source = "def f(a, b):\n    return a <= b\n"
    mutants = mutate_python(source, "f.py")
    assert mutants and "a < b" in mutants[0].source


def test_every_specification_mutant_still_finds_its_anchor() -> None:
    """A spec mutant whose anchor drifted would silently stop being tested."""
    sql = REFERENCE.read_text(encoding="utf-8")
    for spec in SPEC_MUTANTS:
        assert spec.apply(sql) != sql, f"{spec.mutant_id} produced no change"


def test_a_specification_mutant_may_not_anchor_on_a_comment() -> None:
    """When the window moved to seconds, the old anchor survived only in the comment that
    explained the change: the mutant applied, changed nothing executable, and was reported as
    a surviving specification mutant. A mutant that edits prose looks like a finding."""
    from samegold.mutation.spec_mutants import SpecMutant

    sql = "-- the window is 45 days\nSELECT 1;\n"
    mutant = SpecMutant("X", "r", "why", find="45 days", replace="60 days")
    try:
        mutant.apply(sql)
    except ValueError as exc:
        assert "comment" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("a comment-only anchor must be refused")


def test_equivalence_classification_has_no_wildcards() -> None:
    """The bug this prevents: one entry matched every order:flip mutant regardless of where
    it was, and filed four row-SELECTING mutants as "row order does not matter"."""
    assert all(entry.context for entry in EQUIVALENCES)
    # Same operator, different CTE, different verdict.
    assert classify("order:flip", "1", "final") is not None
    assert classify("order:flip", "x", "dedup") is not None
    assert classify("order:flip", "x", "amendments") is None


def test_every_assumption_cited_by_an_equivalence_is_documented() -> None:
    for entry in EQUIVALENCES:
        if entry.assumption:
            assert entry.assumption in ASSUMPTIONS, entry.assumption


def test_the_data_assumption_is_falsifiable_and_falsified() -> None:
    """The negative control for the classification itself.

    If the mutants classified as "equivalent because the input satisfies X" behave the same
    when X is false, then X is not the reason they are equivalent, and the classification is
    a convenience. Most of them must change their answer.
    """
    probe = probe_data_assumption(REFERENCE.read_text(encoding="utf-8"))
    assert probe["mutants_checked"], "the probe covers no mutants"
    assert probe["mutants_that_diverge_when_it_is_false"], probe["verdict"]


def test_the_structural_assumption_holds_on_generated_data(tmp_path: Path) -> None:
    result = generate(tmp_path / "g", seed=3, profile=FAST)
    probe = probe_structural_assumption(tmp_path / "g" / "bronze", result.ledger.closes[-1])
    assert probe["orphan_months"] == 0, probe


def test_the_campaign_kills_the_imputation_mutant(campaign) -> None:  # type: ignore[no-untyped-def]
    """SPEC-01 is the mutant this whole project exists to catch. If it ever survives, the two
    implementations have agreed on the same misunderstanding."""
    assert campaign.detail["SPEC-01"]["killed_by"], "SPEC-01 survived: the witnesses are blind"
    assert "ledger" in campaign.detail["SPEC-01"]["killed_by"]


def test_the_campaign_kills_every_specification_mutant(campaign) -> None:  # type: ignore[no-untyped-def]
    survivors = [
        spec.mutant_id
        for spec in SPEC_MUTANTS
        if not campaign.detail.get(spec.mutant_id, {}).get("killed_by")
    ]
    assert survivors == [], f"specification mutants nobody noticed: {survivors}"


def test_no_mutant_is_killed_by_the_harness_falling_over(campaign) -> None:  # type: ignore[no-untyped-def]
    """A kill has to come from a witness noticing, never from this code crashing.

    Three mutants used to be recorded as "killed by the runtime" because removing a COALESCE
    made a column NULL and the result mapper called int(None).
    """
    for mutant_id, detail in campaign.detail.items():
        reason = str(detail.get("reason", ""))
        assert "TypeError" not in reason and "KeyError" not in reason, (
            f"{mutant_id} was recorded as killed because the harness crashed: {reason}"
        )


def test_the_survivors_are_named_and_the_score_is_published_both_ways(campaign) -> None:  # type: ignore[no-untyped-def]
    matrix = campaign.matrix.to_json()
    strict = matrix["killed"] / matrix["mutants_total"]
    assert 0.0 < strict <= 1.0
    assert matrix["mutants_scored"] + len(matrix["equivalent"]) == matrix["mutants_total"]
    for mutant_id in matrix["equivalent"]:
        assert matrix["equivalent"][mutant_id].strip(), f"{mutant_id} is equivalent with no reason"
