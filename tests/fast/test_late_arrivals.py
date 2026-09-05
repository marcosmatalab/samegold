"""The late-arrival population, pinned, because the Databricks lane's second close rests on it.

That close is the project's thesis running in a workspace: January was signed off at 14 198 046
cents and then moved to 25 582 615, because events for January arrived after it closed. The
population that made it happen was produced by a script in `/tmp` on one machine. Nothing in
this repository could regenerate it, so every figure the second close published rested on data
no reader could reproduce, which is the premise of the project inverted.

`samegold generate-late --seed 20260901 --late-seed 20260904` produces it now, and these tests
are what make that a claim rather than a hope: the same four counts the workspace ingested, from
two seeds, on any machine.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections import Counter
from pathlib import Path

import pytest

from samegold.generator.events import FAST
from samegold.generator.late import (
    LATE_PREFIX,
    late_arrivals,
    late_batch_prefix,
    population_digest,
    population_for,
)

BASE_SEED, LATE_SEED = 20260901, 20260904
# The second late arrival, which the third close rests on. Chosen as a date, like the other
# two, and NOT chosen for the shape of the close it produces - see
# `docs/predictions-2026-09-05.md`. Picking a seed because of the months it happens to restate
# is fitting the data to the demonstration.
THIRD_SEED = 20260905

# What the workspace ingested on 4 September 2026, counted from the volume it read.
EVENTS = 573
BATCHES = 269
BY_TYPE = {
    "customer_upserted": 21,
    "order_line_amended": 63,
    "order_placed": 420,
    "return_registered": 69,
}
BY_MONTH = {"2026-01": 553, "2026-02": 16, "2026-03": 4}
# The two populations, end to end. 755 + 573 = 1328, which is what `rows.bronze_events` in
# `evidence/databricks/SG-DBX-01.json` has to say for the second close.
BASE_EVENTS, FULL_EVENTS = 755, 1328


@pytest.fixture(scope="module")
def produced() -> tuple[Path, object]:
    root = Path(tempfile.mkdtemp(prefix="late-"))
    result = late_arrivals(root, base_seed=BASE_SEED, late_seed=LATE_SEED, profile=FAST)
    return root, result


def _records(bronze: Path) -> list[dict]:
    out = []
    for path in sorted(bronze.rglob("part-*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
    return out


def test_two_seeds_produce_the_population_the_workspace_ingested(produced) -> None:  # type: ignore[no-untyped-def]
    """The four counts, from the two seeds, with nothing hand-written in between."""
    root, result = produced
    records = _records(root / "bronze")
    assert len(records) == EVENTS, len(records)
    assert result.events == EVENTS
    assert result.batches == BATCHES, result.batches
    assert Counter(r["event_type"] for r in records) == Counter(BY_TYPE)
    assert Counter(str(r["event_ts"])[:7] for r in records) == Counter(BY_MONTH)


def test_it_is_deterministic(produced) -> None:  # type: ignore[no-untyped-def]
    """Twice from the same seeds is the same bytes.

    A procedure that is only reproducible on the machine that wrote it is what this replaces,
    so "deterministic" is executed rather than asserted in a docstring.
    """
    root, _ = produced
    again = Path(tempfile.mkdtemp(prefix="late-again-"))
    late_arrivals(again, base_seed=BASE_SEED, late_seed=LATE_SEED, profile=FAST)

    def contents(base: Path) -> dict[str, str]:
        return {
            str(path.relative_to(base)).replace("\\", "/"): path.read_text(encoding="utf-8")
            for path in sorted(base.rglob("part-*.json"))
        }

    assert contents(root / "bronze") == contents(again / "bronze")


def test_no_late_event_was_already_in_the_base_population(produced) -> None:  # type: ignore[no-untyped-def]
    """The definition, checked rather than trusted.

    "Late arrival" means an event the first close never saw. An id that was in both would be a
    re-delivery, which is SG-02's subject and not this one, and it would be counted twice in
    every conservation figure the second close published.
    """
    root, result = produced
    from samegold.generator.events import generate
    from samegold.generator.late import base_event_ids

    base = Path(tempfile.mkdtemp(prefix="base-"))
    generate(base, seed=BASE_SEED, profile=FAST)
    known = base_event_ids(base / "bronze")
    assert len(known) > 0
    late_ids = [r["event_id"] for r in _records(root / "bronze")]
    assert not (set(late_ids) & known), sorted(set(late_ids) & known)[:5]
    assert result.already_present == 185, result.already_present


def test_the_late_batches_cannot_collide_with_the_base_ones(produced) -> None:  # type: ignore[no-untyped-def]
    """`batch=late-<stamp>`, and the reason is Auto Loader listing one directory.

    The two generations bucket arrivals into the same instants, so `batch=202601010000` exists
    in both. Uploaded under one name into one volume, the second upload replaces the first and
    the events that were meant to arrive late arrive not at all.
    """
    root, _ = produced
    names = {path.parent.name for path in (root / "bronze").rglob("part-*.json")}
    assert names, "no batches written"
    assert all(name.startswith(f"batch={LATE_PREFIX}") for name in names), sorted(names)[:5]


# ------------------------------------------------- the arrival that lands on another arrival

# The first arrival's files, hashed over their PATHS AND their bytes. Everything else in this
# file counts events; this pins the names, and the names are the part that a generalisation
# could quietly change.
#
# Those 269 directories are in the workspace's landing volume and were ingested on 4 September
# 2026. The population this repository regenerates is only a reproduction while it reproduces
# what the workspace actually read - so a refactor that renames them has not moved a detail, it
# has broken the tie between the committed record and the events behind it.
FIRST_ARRIVAL_TREE = "a32de69960a0275fe81751b3b32fa1ab0f63c793fcbdb9cf3fcbfb2688359f6b"
# The composed third population: base plus both arrivals.
THIRD_POPULATION_EVENTS = 1883
THIRD_POPULATION_DIGEST = "f5415ee2f90c77a88a7e3a0e185ef458e20932bfee226a207419bbc16deeebac"


def _tree_digest(bronze: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(
        bronze.rglob("part-*.json"),
        key=lambda p: str(p.relative_to(bronze)).replace("\\", "/"),
    ):
        digest.update(str(path.relative_to(bronze)).replace("\\", "/").encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def test_the_first_arrival_is_byte_for_byte_the_one_the_workspace_ingested(produced) -> None:  # type: ignore[no-untyped-def]
    """Names included, which is the half `test_it_is_deterministic` cannot see.

    That test compares one generation against another generation of the same code, so a change
    that renames every batch directory passes it twice. This compares against a constant, which
    is the only thing that can notice.
    """
    root, _ = produced
    names = {path.parent.name for path in (root / "bronze").rglob("part-*.json")}
    assert all(name.startswith(f"batch={LATE_PREFIX}") for name in names), sorted(names)[:5]
    assert not any(name.startswith("batch=late2-") for name in names), sorted(names)[:5]
    assert _tree_digest(root / "bronze") == FIRST_ARRIVAL_TREE, (
        "the first late arrival's files are not the ones the workspace read. This is not a "
        "detail: those directories are in the landing volume, `evidence/databricks/"
        "SG-DBX-01.json` describes what Auto Loader made of them, and a reproduction that "
        "produces different names or different bytes is reproducing a different population."
    )


def test_the_numbering_starts_at_the_second_arrival_and_says_why() -> None:
    """`late-`, then `late2-`, `late3-`. The first is unnumbered because it is already spent."""
    assert late_batch_prefix(1) == LATE_PREFIX
    assert late_batch_prefix(2) == "late2-"
    assert late_batch_prefix(7) == "late7-"
    with pytest.raises(ValueError):
        late_batch_prefix(0)


def test_a_second_late_arrival_cannot_land_on_the_first() -> None:
    """The finding, executed: a prefix that separates two populations does not separate three.

    `late-` was introduced so a late arrival could not overwrite a BASE batch, and it was
    checked against exactly that. Every late arrival got the same prefix, and the stamp comes
    from the generating population - so two late seeds collide with each other exactly as
    freely as a late seed once collided with the base. Nothing could see it, because a second
    late arrival did not exist.

    MEASURED, with the arrivals forced to share a prefix: 112 of the 278 directories the second
    arrival writes are names the first already occupies. In the landing volume those 112 replace
    files Auto Loader has already ingested.
    """
    root = Path(tempfile.mkdtemp(prefix="arrivals-"))
    first = late_arrivals(root / "one", base_seed=BASE_SEED, late_seed=LATE_SEED, profile=FAST)
    second = late_arrivals(
        root / "two",
        base_seed=BASE_SEED,
        late_seed=THIRD_SEED,
        profile=FAST,
        base_bronze=population_for(
            root / "already", base_seed=BASE_SEED, late_seeds=(LATE_SEED,), profile=FAST
        ),
        arrival=2,
    )
    stamps = {path.parent.name.split("-", 1)[1] for path in first.files}
    would_have_collided = {path.parent.name.split("-", 1)[1] for path in second.files} & stamps
    assert len(would_have_collided) == 112, len(would_have_collided)

    names = {path.parent.name for path in first.files} & {path.parent.name for path in second.files}
    assert not names, f"two arrivals write the same directory: {sorted(names)[:5]}"


def test_the_third_population_is_the_two_arrivals_composed() -> None:
    """One recipe for the whole tree, and every arrival filtered against the ones before it.

    An event the FIRST arrival delivered is not late for the second: it is a re-delivery, which
    is SG-02's subject. The composition is what makes that true without a caller remembering
    to make it true.
    """
    root = Path(tempfile.mkdtemp(prefix="third-"))
    bronze = population_for(
        root, base_seed=BASE_SEED, late_seeds=(LATE_SEED, THIRD_SEED), profile=FAST
    )
    lines = [
        line
        for path in sorted(bronze.rglob("part-*.json"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(lines) == THIRD_POPULATION_EVENTS, len(lines)
    assert population_digest(bronze).digest == THIRD_POPULATION_DIGEST

    # NOT "every id in the tree is unique": the base population deliberately delivers some
    # events twice, which is SG-02's subject and has to survive. What must hold is that a LATER
    # arrival delivers nothing an EARLIER one already did - otherwise a re-delivery would be
    # counted as a late arrival and every conservation figure the close publishes would carry
    # it twice.
    def ids_under(prefix: str) -> set[str]:
        out = set()
        for path in sorted(bronze.rglob("part-*.json")):
            if not path.parent.name.startswith(f"batch={prefix}"):
                continue
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    out.add(json.loads(line)["event_id"])
        return out

    everything_before = set()
    for path in sorted(bronze.rglob("part-*.json")):
        if path.parent.name.startswith("batch=late2-"):
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                everything_before.add(json.loads(line)["event_id"])
            except json.JSONDecodeError:
                continue  # a truncated line carries no id and cannot be deduplicated
    second = ids_under("late2-")
    assert second, "the second arrival wrote nothing"
    assert not (second & everything_before), sorted(second & everything_before)[:5]


def test_the_corrupt_lines_are_dropped_and_that_is_a_decision(produced) -> None:  # type: ignore[no-untyped-def]
    """Three lines per population carry no `event_id`, and they do not come across.

    "Not already present" cannot be decided for a record with no id, and keeping them would
    re-deliver a corrupt line the base population already has - the quarantine counts would
    charge one fault twice. The run's own arithmetic is the check: quarantine stayed at 28,
    all of them from the base population, and 727 + 573 = 1300 accepted.
    """
    root, result = produced
    assert result.dropped_without_id == 3, result.dropped_without_id
    # And nothing corrupt reached the files: every line written parses and carries an id.
    for record in _records(root / "bronze"):
        assert record.get("event_id"), record


def test_the_full_population_is_the_one_the_second_close_read() -> None:
    """755 + 573 = 1328, composed by the function the parity fixture uses.

    `population_for` exists so that "the population the workspace read" has one definition in
    this repository rather than one per caller, and the count it produces is what
    `rows.bronze_events` in the record has to match.
    """
    root = Path(tempfile.mkdtemp(prefix="full-"))
    base_only = population_for(root / "a", base_seed=BASE_SEED, late_seeds=(), profile=FAST)
    full = population_for(root / "b", base_seed=BASE_SEED, late_seeds=(LATE_SEED,), profile=FAST)

    def lines(bronze: Path) -> int:
        return sum(
            1
            for path in sorted(bronze.rglob("part-*.json"))
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    assert lines(base_only) == BASE_EVENTS
    assert lines(full) == FULL_EVENTS
    assert lines(full) - lines(base_only) == EVENTS
