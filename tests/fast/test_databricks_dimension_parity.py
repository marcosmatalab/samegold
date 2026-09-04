"""The two Type 2 dimensions, compared - which is what `gold_close.py` says it exists for.

The Databricks lane maintains `dim_customer_scd2` with AUTO CDC, a Databricks-only primitive;
the OSS lane maintains the same dimension with a hand-written two-pass MERGE, and a third
implementation recomputes it in DuckDB. PARITY.md has said for rounds that comparing them is
the point of having both.

Nothing compared them until the lane ran. It ran on 3 September 2026 and they disagreed:
**78 versions and 18 closed rows on Databricks, 75 and 15 on the OSS side.** Sixty customers
and sixty open rows on both, so `open_rows = customers` held and the difference was exactly
three versions.

It ran again the same day with `track_history_column_list=["segment", "country"]` set, and the
workspace produced **75 / 60 / 60 / 15** - the OSS lane's shape exactly. That record is in this
repository (`evidence/databricks/SG-DBX-01.json`), so the comparison below is no longer a
description of a divergence: it is a check that runs.

The cause is measured below rather than asserted: three of the population's 78
`customer_upserted` events are HEARTBEATS - an upsert that repeats the segment and country the
customer already had - and AUTO CDC's default is a new version whenever ANY column changes,
while the source view carries `event_ts` and `event_id`, which change on every upsert by
construction. So the default was guaranteed to produce one version per event.

Which is right is a contract question, and the contract answers it in
`samegold.pipelines.transform.dim_customer_scd2`: "A Type 2 dimension records CHANGES, not
heartbeats." `track_history_column_list=["segment", "country"]` was set on the lane, the lane
was re-run, and the workspace's own rows are now in this repository at
`evidence/databricks/dim_customer_scd2.json`.

**They agree on all seventy-five rows.** The same sixty customers, the same intervals, the same
attributes, the same instants - as multisets and as per-customer histories. Nothing appeared row
by row that the four aggregates could not see, which is a result rather than a formality: two
dimensions can match on every total and disagree about which customer changed when.

What DID appear is in the comparison itself. Its first version reduced every timestamp with
`str(value)[:19]`, which is what made a workspace string and a generator string comparable - by
cutting the zone off the end of both. Falsified against the committed capture: every timestamp
moved to `+01:00`, an hour's shift in every instant, and the test passed. It also asked
`row not in theirs`, which is a set question, so seventy-seven rows containing two repeated
versions passed too. A comparison written about THREE EXTRA VERSIONS could not see extra
versions.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from collections import Counter
from itertools import pairwise
from pathlib import Path

import pytest

from samegold.generator.events import FAST
from samegold.generator.late import population_digest, population_for
from samegold.oracle.duckdb_gold import scd2_as_of

REPO = Path(__file__).resolve().parents[2]
# What the seed step writes: `samegold generate --profile fast --seed 20260901`, and then, for
# the second close, `samegold generate-late --seed 20260901 --late-seed 20260904`.
SEED, LATE_SEED, PROFILE = 20260901, 20260904, FAST
TRACKED = ("segment", "country")
# The two populations this repository documents, as the bronze line count each produces. They
# are CANDIDATES, not answers: the fixture builds each one and counts it, because a key here
# that no longer matches what the generator emits is exactly the drift this file exists to
# catch.
DOCUMENTED_POPULATIONS = {
    "the base seed alone, which is the first close": None,
    "the base seed plus the late arrivals, which is the second": LATE_SEED,
}
# What each population is, measured. The numbers are not the point - the arithmetic is:
# versions + heartbeats = upserts, in both, which is what "a Type 2 dimension records changes,
# not heartbeats" means when it is counted rather than asserted.
POPULATION_FACTS = {
    755: {
        "upserts": 78,
        "heartbeats": 3,
        "versions": 75,
        "closed_rows": 15,
        "heartbeat_ids": ["cu-C000028-1", "cu-C000038-1", "cu-C000043-1"],
    },
    1328: {
        "upserts": 96,
        "heartbeats": 4,
        "versions": 92,
        "closed_rows": 32,
        "heartbeat_ids": [
            "cu-C000028-1",
            "cu-C000038-1",
            "cu-C000039-1",
            "cu-C000043-1",
        ],
    },
}
# The sections a record produced by the DEPLOYED notebook does not carry yet. A CLOSED LIST,
# not a tolerance: `population` was added to `publish_evidence.py` in the same change as this
# check, and `databricks bundle run` runs what was DEPLOYED - so until the lane is deployed and
# the evidence task re-run, the committed record has no digest to compare against. Listing it
# by name means the absence is a dated fact rather than a check that silently does not run, and
# the run that lands the digest turns this test RED, which is what forces the set to be
# emptied and the comparison below to become mandatory.
SECTIONS_THE_DEPLOYED_NOTEBOOK_DOES_NOT_WRITE_YET = {"population"}
# Captured from the workspace and COMMITTED, so its absence is a failure rather than a skip:
# the row-by-row comparison is the one `gold_close.py` names as its reason for existing, and a
# suite that quietly stops running it is how it came not to exist for nine rounds.
CAPTURED = REPO / "evidence" / "databricks" / "dim_customer_scd2.json"
# The record the workspace produced. It carries the dimension's SHAPE - versions, customers,
# open and closed rows - which is what the divergence showed up in, and not its rows.
RECORD = REPO / "evidence" / "databricks" / "SG-DBX-01.json"


def rows_in_record(record: dict) -> int:
    """`rows.bronze_events`, which the digest's arithmetic has to reach."""
    return int((record.get("rows") or {})["bronze_events"])


def _bronze_lines(bronze: Path) -> int:
    return sum(
        1
        for path in sorted(bronze.rglob("part-*.json"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


@pytest.fixture(scope="module")
def ingested(record: dict) -> int:
    """How many bronze events the workspace read, according to the workspace."""
    count = (record.get("rows") or {}).get("bronze_events")
    assert isinstance(count, int), f"the record does not say how many events it ingested: {count!r}"
    return count


@pytest.fixture(scope="module")
def selection(ingested: int) -> tuple[Path, list[dict], list[dict], int]:
    """The bronze tree, the upserts, the reference dimension over them, and the size.

    THE POPULATION IS CHOSEN BY THE RECORD, and this is the round's finding turned into code.
    The fixture used to generate the base seed and nothing else, so when the workspace ingested
    a second, later population the two halves of the comparison stopped describing the same
    events - and the comparison reported it as AUTO CDC and the hand-written MERGE disagreeing,
    92 versions against 75. They did not disagree. They were looking at different data.

    The guard that was supposed to prevent exactly this - the capture's `update_id` against the
    record's - PASSED, because both files did come from the same update. It protects a capture
    from outliving its run. It says nothing about which events that run read. A check that is
    well defined and answers a different question than the one being asked, which is the class
    this repository has now found four times.

    So the population is built from the record's own `rows.bronze_events`: each documented
    population is generated and COUNTED, and the one whose count matches is the one the OSS half
    is computed over. A record whose count matches neither fails here, by name, rather than
    reappearing downstream as a parity failure that is not one.
    """
    root = Path(tempfile.mkdtemp(prefix="dimparity-"))
    tried: dict[str, int] = {}
    bronze: Path | None = None
    for description, late_seed in DOCUMENTED_POPULATIONS.items():
        candidate = population_for(
            root / str(late_seed), base_seed=SEED, late_seed=late_seed, profile=PROFILE
        )
        tried[description] = _bronze_lines(candidate)
        if tried[description] == ingested:
            bronze = candidate
            break
    assert bronze is not None, (
        f"the record says the workspace ingested {ingested} bronze events, and no population "
        f"this repository documents produces that number: {tried}. Either the generator has "
        f"moved under a committed record, or the volume was seeded with something "
        f"docs/databricks-run.md does not describe. Both are worse than a parity failure and "
        f"neither should be read as one."
    )

    by_id: dict[str, dict] = {}
    for path in sorted(bronze.rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or not line.lstrip().startswith("{"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("event_type") == "customer_upserted" and record.get("customer_id"):
                by_id.setdefault(record["event_id"], record)
    upserts = sorted(by_id.values(), key=lambda r: (r["customer_id"], r["event_ts"], r["event_id"]))
    dimension = list(scd2_as_of(bronze, dt.datetime(2030, 1, 1, tzinfo=dt.UTC)))
    return bronze, upserts, dimension, ingested


@pytest.fixture(scope="module")
def population(selection: tuple[Path, list[dict], list[dict], int]) -> tuple[list, list, int]:  # type: ignore[type-arg]
    """What every comparison below reads. The bronze path is asked for separately."""
    _, upserts, dimension, ingested = selection
    return upserts, dimension, ingested


@pytest.fixture(scope="module")
def bronze(selection: tuple[Path, list[dict], list[dict], int]) -> Path:
    """The files the population was generated into, for the fingerprint below."""
    return selection[0]


def _heartbeats(upserts: list[dict]) -> list[dict]:
    """Upserts whose tracked attributes are identical to the customer's previous one."""
    out, previous = [], {}
    for record in upserts:
        key = record["customer_id"]
        now = tuple(record.get(a) for a in TRACKED)
        if key in previous and previous[key] == now:
            out.append(record)
        previous[key] = now
    return out


def test_the_reference_dimension_records_changes_and_not_heartbeats(population) -> None:  # type: ignore[no-untyped-def]
    """The arithmetic that explains the divergence, pinned FOR THE POPULATION IT RAN ON.

    It used to pin 78 / 3 / 75 with no population attached, because there was only one. There
    are two now - the first close read 755 events, the second read 1328 - and the numbers move
    with them:

      755 events   78 upserts   3 heartbeats   75 versions   15 closed
      1328 events  96 upserts   4 heartbeats   92 versions   32 closed

    The arithmetic does not move, and it is the actual claim: **versions + heartbeats =
    upserts**, in both. 75 + 3 = 78 and 92 + 4 = 96. One version per EVENT is what AUTO CDC's
    default produced; one per CHANGE is what the contract asks for; the two differ by exactly
    the heartbeats.

    The late population adds 18 distinct customer upserts (21 lines, two of those ids delivered
    more than once, which is what the deduplication is for). Seventeen of the eighteen change a
    tracked attribute and one, `cu-C000039-1`, repeats what the customer already had - so 75+17
    = 92 versions and 15+17 = 32 closed rows. Measured, not inferred from the difference.
    """
    upserts, dimension, ingested = population
    facts = POPULATION_FACTS[ingested]
    heartbeats = _heartbeats(upserts)
    assert len(upserts) == facts["upserts"], len(upserts)
    assert len(heartbeats) == facts["heartbeats"], [r["event_id"] for r in heartbeats]
    assert len(dimension) == facts["versions"], len(dimension)

    open_rows = [r for r in dimension if r["valid_to"] is None]
    assert len(open_rows) == 60
    assert len({r["customer_id"] for r in dimension}) == 60
    assert len(dimension) - len(open_rows) == facts["closed_rows"]

    # The invariant, which holds on both populations and is what the numbers above are for.
    assert len(dimension) + len(heartbeats) == len(upserts)

    # And they are nameable, which is what makes the divergence a finding rather than a delta.
    assert sorted(r["event_id"] for r in heartbeats) == facts["heartbeat_ids"]


def test_no_two_consecutive_versions_of_a_customer_are_identical(population) -> None:  # type: ignore[no-untyped-def]
    """The PROPERTY, which is what actually has to hold whatever the counts are.

    A version that repeats its predecessor's attributes is not a version; it is the same fact
    with a second row. This is the rule stated as a property rather than as a number, so it
    keeps meaning something on a population these fixtures do not describe.
    """
    _, dimension, _ = population
    by_customer: dict[str, list[dict]] = {}
    for row in dimension:
        by_customer.setdefault(row["customer_id"], []).append(row)
    duplicates = []
    for customer, rows in by_customer.items():
        ordered = sorted(rows, key=lambda r: str(r["valid_from"]))
        for previous, nxt in pairwise(ordered):
            if all(previous[a] == nxt[a] for a in TRACKED):
                duplicates.append((customer, previous["valid_from"], nxt["valid_from"]))
    assert not duplicates, (
        f"consecutive versions with identical {TRACKED}: {duplicates}. A Type 2 dimension "
        f"records changes, not heartbeats."
    )


def _instant(value: object, where: str) -> dt.datetime | None:
    """One ISO-8601 timestamp as an INSTANT, refusing anything that does not carry a zone.

    The comparison below used to reduce both sides with `str(value)[:19]`. That is how a
    workspace's `2026-01-01T00:00:00+00:00` and the generator's `2026-01-01T00:00:00.000000Z`
    were made comparable: by cutting off the part where they differ. It also cut off the part
    where a WRONG one would differ. Doctored to `+01:00` - every row an hour out - the old
    comparison passed.

    A naive timestamp is refused rather than assumed to be UTC, because assuming is the same
    mistake with a friendlier face: it would let a capture taken in workspace-local time compare
    equal to a dimension computed in UTC.
    """
    if value is None:
        return None
    moment = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    assert moment.utcoffset() is not None, (
        f"{where} is {value!r}, which carries no time zone. Two dimensions cannot be compared "
        f"as instants unless both sides say which instant they mean."
    )
    return moment


def _versions(rows: list[dict], start: str, end: str) -> Counter[tuple]:
    """The dimension as a MULTISET of versions, so a repeated row is a difference.

    A Counter and not a set. `row not in other` was the old question, and it answers "no
    difference" for a dimension holding the same versions plus three more of them - which is
    the exact divergence this file was written about.
    """
    return Counter(
        (
            str(row["customer_id"]),
            str(row["segment"]),
            str(row["country"]),
            _instant(row[start], f"{row['customer_id']}.{start}"),
            _instant(row[end], f"{row['customer_id']}.{end}"),
        )
        for row in rows
    )


def _history(rows: list[dict], start: str, end: str) -> dict[str, list[tuple]]:
    """Each customer's versions in order: the sequence, not just the collection."""
    out: dict[str, list[tuple]] = {}
    for row in rows:
        out.setdefault(str(row["customer_id"]), []).append(
            (
                _instant(row[start], f"{row['customer_id']}.{start}"),
                _instant(row[end], f"{row['customer_id']}.{end}"),
                str(row["segment"]),
                str(row["country"]),
            )
        )
    return {c: sorted(v, key=lambda t: (t[0], t[2], t[3])) for c, v in out.items()}


@pytest.fixture(scope="module")
def capture() -> dict:
    """The capture file: a provenance header and the workspace's rows under it.

    Missing is a FAILURE, and the failure carries the query. The file is committed; a clone
    that does not have it has had it deleted.
    """
    assert CAPTURED.exists(), (
        f"no capture at {CAPTURED.relative_to(REPO)}. `scripts/databricks_run.sh fetch` brings "
        f"it down from the evidence volume, where publish_evidence.py writes it with:\n\n"
        f"  SELECT customer_id, segment, country, __START_AT, __END_AT\n"
        f"  FROM samegold.main.dim_customer_scd2 ORDER BY customer_id, __START_AT;\n\n"
        f"It is committed, so this file going missing means somebody deleted the only evidence "
        f"that the two implementations were ever compared."
    )
    document = json.loads(CAPTURED.read_text(encoding="utf-8"))
    assert isinstance(document, dict) and isinstance(document.get("rows"), list), (
        f"{CAPTURED.name} is not a capture document. It must be an object with a `provenance` "
        f"header and a `rows` array; a bare array is the shape it had before it could say "
        f"which run produced it, which is the whole point of the header. A `rows` that is an "
        f"OBJECT is publish_evidence.py reporting that the query failed - the error is in it, "
        f"and the record's `incomplete` list names the section."
    )
    assert document["rows"], f"{CAPTURED.name} holds no rows"
    return document


@pytest.fixture(scope="module")
def captured(capture: dict) -> list[dict]:
    """Just the rows, for the comparisons that do not care where they came from."""
    return capture["rows"]


@pytest.fixture(scope="module")
def record() -> dict:
    """The record the same run published."""
    assert RECORD.exists(), f"no record at {RECORD.relative_to(REPO)}"
    return json.loads(RECORD.read_text(encoding="utf-8"))


def test_the_capture_names_the_run_the_record_names(capture: dict, record: dict) -> None:
    """The capture and the record beside it must come from the same update.

    Without this the capture is a file that outlives its run in silence. A later run replaces
    `SG-DBX-01.json`; nothing replaces the capture; the row-by-row comparison goes on passing
    against rows the workspace no longer holds, and it passes GREEN, which is worse than
    failing. This is that failure, made loud, with the query that fixes it.

    `update_id` is the tie because it is the one thing both files learn from the same read of
    the same event log in the same session - the record publishes it, and publish_evidence.py
    writes it into the capture's header from the same value, in the same task, before either
    file leaves the workspace.
    """
    provenance = capture.get("provenance") or {}
    update = (record.get("update") or [{}])[0]
    assert provenance.get("update_id") and update.get("update_id"), (
        f"one of the two files does not name an update: capture "
        f"{provenance.get('update_id')!r}, record {update.get('update_id')!r}"
    )
    assert provenance["update_id"] == update["update_id"], (
        f"the capture and the record describe DIFFERENT updates.\n"
        f"  {CAPTURED.name}: {provenance['update_id']}\n"
        f"  {RECORD.name}: {update['update_id']}\n"
        f"The record has been replaced by a later run and the capture has not, so every "
        f"comparison below is reading rows the workspace no longer holds. Bring it down with "
        f"`scripts/databricks_run.sh fetch`, or re-run the job if that run predates "
        f"publish_evidence.py writing the capture:\n\n  {capture.get('query')}"
    )
    assert provenance.get("pipeline_id") == record.get("pipeline_id"), (
        f"different pipelines: capture {provenance.get('pipeline_id')!r}, record "
        f"{record.get('pipeline_id')!r}"
    )


def test_the_record_says_which_events_it_read_or_says_it_cannot_yet(record: dict) -> None:
    """The digest is only a tie once a deployed notebook has written one.

    `population` was added to `publish_evidence.py` in the same change as the comparison that
    reads it, and `databricks bundle run` runs what was DEPLOYED. So there is a window in which
    the committed record cannot answer, and a check that quietly skips through that window is
    how a comparison comes not to run for nine rounds.

    The window is a CLOSED LIST instead. This test is red the moment a run from the fixed
    notebook lands, and emptying the set is what that run requires - after which the
    fingerprint comparison below stops being conditional.
    """
    missing = {name for name in ("population",) if not record.get(name)}
    assert missing == SECTIONS_THE_DEPLOYED_NOTEBOOK_DOES_NOT_WRITE_YET, (
        f"the record is missing {sorted(missing)} and this file expects "
        f"{sorted(SECTIONS_THE_DEPLOYED_NOTEBOOK_DOES_NOT_WRITE_YET)}. If the set got SMALLER, "
        f"a run from a notebook that publishes the population digest has landed: empty this "
        f"set, and the comparison in "
        f"test_both_halves_of_the_comparison_describe_the_same_population stops being "
        f"conditional on the record carrying one."
    )


def test_both_halves_of_the_comparison_describe_the_same_population(
    population, bronze: Path, capture: dict, record: dict
) -> None:  # type: ignore[no-untyped-def]
    """The guard the update_id check could not be.

    On 4 September 2026 the workspace ingested a second, later population and this file
    reported it as AUTO CDC and the hand-written MERGE producing different dimensions, 92
    versions against 75. They agreed. The OSS half was computed over 755 events and the
    workspace had read 1328.

    `test_the_capture_names_the_run_the_record_names` passed throughout, and it was doing its
    job: both files DID come from the same update. Sameness of run is not sameness of
    population, and nothing here was asking the second question.

    So it is asked, three ways, and each is a different thing that can go wrong:

      * the population the OSS half was built from has as many events as the record says the
        workspace read - which is what the fixture selects on, asserted here so the failure
        says "population" rather than arriving as a parity difference;
      * the capture holds as many rows as the record's own `rows.dim_customer_scd2` - the two
        come from different queries in the same run;
      * the OSS dimension has as many versions as the capture has rows, which is the shape
        comparison stated as the precondition of the row-by-row one below.
    """
    _, dimension, ingested = population
    # FIRST, because it is the only one of these that compares the events themselves. The
    # three below compare counts, and a count is what let a moved generator through: reordering
    # a list literal in the generator leaves every published number identical and gives thirty
    # customers a different history.
    published = (record.get("population") or [{}])[0]
    if published:
        ours = population_digest(bronze, str(published["columns"]).split(","))
        assert ours.digest == published["digest"], (
            f"the events this repository generates are not the events the record says the "
            f"workspace read.\n"
            f"  this repository: {ours.digest} over {ours.digest_rows} rows\n"
            f"  the record:      {published['digest']} over {published['digest_rows']} rows\n"
            f"This assert cannot say WHICH side moved, and the two are repaired differently:\n"
            f"  * the generator moved under a committed record - a change to "
            f"`samegold/generator/` that this record predates. Nothing below is a parity "
            f"result, and the repair is to regenerate the evidence, not to touch a test;\n"
            f"  * the workspace ingested something else - a volume re-seeded by hand, or a "
            f"population `docs/databricks-run.md` does not describe. The repair is in the "
            f"workspace.\n"
            f"What it CAN say is that nothing further down this file is comparing two "
            f"implementations over one population, whichever of those it is. Do not read the "
            f"row-by-row failures below as a difference between AUTO CDC and the MERGE: they "
            f"are what a moved population looks like from there."
        )
        assert ours.digest_rows == published["digest_rows"], (
            ours.digest_rows,
            published["digest_rows"],
        )
        # The domain, checked rather than trusted: what the digest leaves out has to be the
        # three corrupt lines and not a hole that grew.
        assert ours.rows_outside_the_digest == published["rows_outside_the_digest"], (
            f"the two halves put a different number of rows outside the digest "
            f"({ours.rows_outside_the_digest} here, {published['rows_outside_the_digest']} "
            f"there), so they are hashing different domains and the digests above agreed by "
            f"accident of arithmetic."
        )
        assert ours.digest_rows + ours.rows_outside_the_digest == rows_in_record(record), (
            f"{ours.digest_rows} + {ours.rows_outside_the_digest} does not reach the "
            f"record's own `rows.bronze_events` ({rows_in_record(record)}). The domain is "
            f"not covering the table."
        )

    rows = (record.get("rows") or {}).get("bronze_events")
    assert ingested == rows, (
        f"the OSS half was computed over {ingested} events and the record says the workspace "
        f"read {rows}. Nothing below is a parity result until these two agree."
    )
    captured_rows = len(capture["rows"])
    in_record = (record.get("rows") or {}).get("dim_customer_scd2")
    assert captured_rows == in_record, (
        f"the capture holds {captured_rows} rows and the record counts {in_record} in the same "
        f"table. One of the two files is from a different run of the same update."
    )
    assert len(dimension) == captured_rows, (
        f"the OSS lane computes {len(dimension)} versions over the population whose event "
        f"count matches the record, and the capture holds {captured_rows}. This assert cannot "
        f"say which of two things that is, and the two call for opposite responses:\n"
        f"  * AUTO CDC and the hand-written MERGE disagreeing over the same events, which is "
        f"a parity difference and is what the three tests below would then locate;\n"
        f"  * the record not describing a single run - `rows.bronze_events` is what chose "
        f"the population above and `rows.dim_customer_scd2` is what the capture was "
        f"checked against, and if those two counts came from different runs then the "
        f"population above is not the one the capture was read from and nothing below is "
        f"a parity result.\n"
        f"Rule the second out first. It costs nothing, and it is the more expensive way "
        f"round to be wrong: reading a population mismatch as parity is how the last one "
        f"nearly got 'fixed' by writing the other lane's number in as the expected one."
    )


def test_a_capture_that_claims_the_workspace_measured_it_carries_what_only_a_run_knows(
    capture: dict,
) -> None:
    """The header can be typed by hand, so what cannot be typed convincingly is checked.

    The capture committed on 3 September 2026 was exported by hand and its header says so:
    `measured_in_the_workspace: false`, with every field copied from the record beside it. That
    is honest and it is weak - a copied field agrees by construction - and the reason it is
    written anyway is that it stops agreeing the moment a later run publishes a different
    update.

    A header that claims to have been measured has to carry the three things only a running job
    has: the job run, the task run, and the workspace's own clock at capture time. None of them
    exist in a record a person can copy from.
    """
    provenance = capture.get("provenance") or {}
    assert "measured_in_the_workspace" in provenance, (
        "the header does not say whether the workspace measured it, which is the difference "
        "between provenance and a note"
    )
    if not provenance["measured_in_the_workspace"]:
        assert provenance.get("why"), (
            "a capture that was not measured in the workspace has to say how it came to exist"
        )
        return
    missing = [
        field
        for field in ("job_run_id", "task_run_id", "captured_at", "commit")
        if not provenance.get(field)
    ]
    assert not missing, (
        f"the header claims the workspace measured it and does not carry {missing}. Those come "
        f"from the job that read the rows; a header written afterwards cannot have them."
    )


def test_the_records_own_aggregates_recompute_from_the_captured_rows(
    capture: dict, record: dict
) -> None:
    """The tie that needs no header at all, and the one that holds today.

    `publish_evidence.py` reads the dimension TWICE in the same run: once as six aggregates for
    the record, once row by row for the capture. Two queries, two files - so recomputing the
    six from the rows is not circular, and a capture from a different run than the record beside
    it will disagree here even if somebody has typed a matching update id into its header.

    `first_start` and `last_start` are the sharp ones: they are MIN and MAX over the workspace's
    own `__START_AT`, and no two populations agree on them by accident.
    """
    rows = capture["rows"]
    shape = (record.get("dim_customer_scd2") or [{}])[0]
    assert shape, "the record carries no dim_customer_scd2 section"

    starts = sorted(str(row["__START_AT"]) for row in rows)
    ours = {
        "versions": len(rows),
        "customers": len({row["customer_id"] for row in rows}),
        "open_rows": sum(1 for row in rows if row["__END_AT"] is None),
        "closed_rows": sum(1 for row in rows if row["__END_AT"] is not None),
        "first_start": starts[0],
        "last_start": starts[-1],
    }
    theirs = {key: str(shape[key]) if "start" in key else shape[key] for key in ours}
    assert ours == theirs, (
        f"the captured rows do not add up to the aggregates the record published.\n"
        f"  from the rows   {ours}\n"
        f"  from the record {theirs}\n"
        f"The two were read from the same table in the same run, so a disagreement means one "
        f"of the two files came from a different one."
    )
    assert len(rows) == (record.get("rows") or {}).get("dim_customer_scd2"), (
        f"{len(rows)} captured rows against {(record.get('rows') or {}).get('dim_customer_scd2')} "
        f"in the record's own row counts"
    )


def test_the_two_dimensions_agree_row_by_row(population, captured) -> None:  # type: ignore[no-untyped-def]
    """The comparison `gold_close.py` declares as its reason for existing.

    One half comes from a workspace, so it has to be CAPTURED - there is no way to compute AUTO
    CDC's output without Databricks, and inventing an expected shape here would be a second
    implementation of the primitive rather than a comparison with it. The other half is
    computed, here, from the same seed.

    Multisets, so three extra versions are three differences and not none. Instants, so an hour
    is an hour. Both of those were falsified against this capture before being written.
    """
    _, dimension, _ = population
    ours = _versions(dimension, "valid_from", "valid_to")
    theirs = _versions(captured, "__START_AT", "__END_AT")

    assert sum(theirs.values()) == sum(ours.values()), (
        f"row counts differ: the OSS lane computes {sum(ours.values())} versions, the capture "
        f"holds {sum(theirs.values())}."
    )
    only_ours = sorted((ours - theirs).elements())
    only_theirs = sorted((theirs - ours).elements())
    assert not only_ours and not only_theirs, (
        f"the hand-written MERGE and AUTO CDC produced different dimensions.\n"
        f"  only in the OSS lane   ({len(only_ours)}): {only_ours[:5]}\n"
        f"  only on Databricks     ({len(only_theirs)}): {only_theirs[:5]}\n"
        f"If the Databricks side has extra versions whose attributes repeat their "
        f"predecessor's, `track_history_column_list` did not take effect on that run."
    )


def test_every_customer_has_the_same_history_in_the_same_order(population, captured) -> None:  # type: ignore[no-untyped-def]
    """The same versions is not the same as the same history.

    The multiset above would be satisfied by two dimensions holding the same seventy-five rows
    attached to a different sixty customers, or in a different order where two versions start at
    the same instant. This asks the stronger question, per customer: the same intervals,
    carrying the same attributes, in the same sequence.
    """
    _, dimension, _ = population
    ours = _history(dimension, "valid_from", "valid_to")
    theirs = _history(captured, "__START_AT", "__END_AT")

    assert set(ours) == set(theirs), (
        f"different customers.\n  only in the OSS lane: {sorted(set(ours) - set(theirs))[:5]}\n"
        f"  only on Databricks:  {sorted(set(theirs) - set(ours))[:5]}"
    )
    differing = {c: (ours[c], theirs[c]) for c in sorted(ours) if ours[c] != theirs[c]}
    assert not differing, (
        f"{len(differing)} customers have a different history. First: {list(differing.items())[:2]}"
    )


def test_the_captured_history_is_well_formed_on_its_own_terms(captured) -> None:
    """What has to be true of the workspace's dimension whatever the OSS lane says.

    A comparison against a second implementation cannot catch a defect both share, and both
    lanes read the same generated events. These are the Type 2 properties themselves: one open
    version per customer, each version starting exactly where the previous one ended, and no
    version repeating its predecessor's tracked attributes - the last being the heartbeat rule
    this whole file exists because of.
    """
    history = _history(captured, "__START_AT", "__END_AT")
    assert history, "the capture holds no customers"

    faults: list[str] = []
    for customer, versions in history.items():
        if sum(1 for _, end, *_ in versions if end is None) != 1:
            faults.append(f"{customer}: not exactly one open version")
        if versions[-1][1] is not None:
            faults.append(f"{customer}: the last version is closed")
        for previous, nxt in pairwise(versions):
            if previous[1] != nxt[0]:
                faults.append(f"{customer}: a gap or overlap at {previous[1]} -> {nxt[0]}")
            if previous[2:] == nxt[2:]:
                faults.append(f"{customer}: consecutive versions repeat {previous[2:]}")
    assert not faults, "\n".join(faults[:10])


def test_the_workspace_dimension_has_the_shape_the_oss_lane_computes(population) -> None:  # type: ignore[no-untyped-def]
    """The cross-runtime comparison, running for real against a committed record.

    This is the check that did not exist when it mattered. AUTO CDC and the hand-written MERGE
    disagreed by three versions on the first run and nothing in the repository noticed; the
    difference showed up in a terminal, in four aggregates, which is exactly what this asserts.

    It is WEAKER than the row-by-row comparison below, and the difference is worth naming: four
    matching totals do not prove the same sixty customers have the same seventy-five intervals.
    Two dimensions could agree on every count and disagree on which customer changed when. The
    row-level capture is what closes that, and until it exists this is the half that can run.
    """
    import json

    _, dimension, _ = population
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    shape = (record.get("dim_customer_scd2") or [{}])[0]
    assert shape, "the record carries no dim_customer_scd2 section"

    open_rows = [r for r in dimension if r["valid_to"] is None]
    ours = {
        "versions": len(dimension),
        "customers": len({r["customer_id"] for r in dimension}),
        "open_rows": len(open_rows),
        "closed_rows": len(dimension) - len(open_rows),
    }
    theirs = {k: shape[k] for k in ours}
    assert ours == theirs, (
        f"the hand-written MERGE and AUTO CDC produced differently shaped dimensions.\n"
        f"  OSS lane   {ours}\n"
        f"  Databricks {theirs}\n"
        f"If Databricks has MORE versions, `track_history_column_list` is not in force on the "
        f"run that wrote the record: AUTO CDC's default is a new version whenever any column "
        f"changes, and the source view carries `event_ts` and `event_id`, which change on "
        f"every upsert."
    )
