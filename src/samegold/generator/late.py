"""The late-arrival population, produced from two seeds instead of from somebody's `/tmp`.

The Databricks lane's second close is the project's whole thesis running in a workspace: a
month that finance had already signed off moved, because events for it arrived after the close.
Producing that needed a second population of events that were NOT in the first, and the first
time it was done it was done by a script in `/tmp` on one machine. Nothing in this repository
could regenerate it, so every figure the second run published rested on a population no reader
could reproduce - which is the premise of the project inverted.

The procedure is deterministic given two seeds, so it belongs here:

  1. generate the base population from `base_seed`, and collect its event ids;
  2. generate a second population from `late_seed`;
  3. keep the lines that parse, carry an `event_id`, and whose id is NOT in the base;
  4. write them under `batch=late-<stamp>` so they cannot collide with the base batches in the
     landing volume, and so a reader of the volume can see which arrival is which.

WHAT STEP 3 DROPS, because it is a property of the procedure and not an accident. The generator
emits three lines per population that are not JSON or carry no `event_id`, and they are
deliberately corrupt: they exist so the `unparseable_json` door is exercised. They are dropped
here, because "not already present" cannot be decided for a record with no id - keeping them
would re-deliver a corrupt line the base population already carries, and the quarantine counts
would double-count a fault that arrived once. So the late batch carries no corrupt records at
all, and the run's own arithmetic shows it: quarantine stayed at 28, all of them from the base
population, and every one of the 573 late events was accepted.

MEASURED against the run of 4 September 2026: 573 events in 269 batch directories, by type
{order_placed 420, order_line_amended 63, customer_upserted 21, return_registered 69} and by
event month {2026-01 553, 2026-02 16, 2026-03 4}. `tests/fast/test_late_arrivals.py` pins all
four, so a change in the generator that moves the population is a failure here rather than a
surprise the next time somebody uploads a volume.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from samegold.generator.events import FAST, Profile, generate

# The prefix that keeps arrivals apart in one landing volume. Auto Loader lists the directory;
# two `batch=202601010000` directories from two generations would be one directory with one file
# in it, and the second upload would silently replace the first.
LATE_PREFIX = "late-"


def late_batch_prefix(arrival: int) -> str:
    """The batch-directory prefix for the nth late arrival, counting from 1.

    THE DEFECT THIS EXISTS FOR, because the prefix above already claimed to have fixed it.
    `late-` separates a late arrival from the BASE population, and it was checked against
    exactly that: `test_the_late_batches_cannot_collide_with_the_base_ones`. It does not
    separate a late arrival from ANOTHER late arrival, and could not - every arrival got the
    same prefix and the stamp comes from the generating population, so two late seeds collide
    with each other exactly as freely as a late seed once collided with the base.

    Measured, for the third population this repository is about to read: of the 278 batch
    directories the second late arrival writes, 112 are names the first arrival already
    occupies. Uploaded into the volume as they are, those 112 replace files Auto Loader has
    already ingested.

    Nothing could see it, because a second late arrival did not exist. That is the whole shape
    of the finding, and `FINDINGS.md` carries it: a fix verified against the case that
    motivated it and never against the case after that.

    THE FIRST ARRIVAL KEEPS THE UNNUMBERED NAME, and that is a compatibility constraint rather
    than a special case. Those directories are in the workspace's landing volume and were
    ingested on 4 September 2026; the population this repository regenerates has to be the
    population the workspace read, directory names included, or the reproduction reproduces
    something else. So the numbering starts at the second arrival, which is the first one whose
    name nothing has committed to yet.
    """
    if arrival < 1:
        raise ValueError(f"arrivals are counted from 1, got {arrival}")
    return LATE_PREFIX if arrival == 1 else f"late{arrival}-"


@dataclass(frozen=True)
class LateArrivalResult:
    """What the filter produced, in the shape a reader can check it in."""

    events: int
    files: list[Path] = field(default_factory=list)
    by_event_type: dict[str, int] = field(default_factory=dict)
    by_event_month: dict[str, int] = field(default_factory=dict)
    base_events: int = 0
    late_events: int = 0
    already_present: int = 0
    dropped_without_id: int = 0

    @property
    def batches(self) -> int:
        return len({path.parent.name for path in self.files})


def _lines(bronze: Path) -> Iterator[tuple[Path, str]]:
    for path in sorted(bronze.rglob("part-*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield path, line


def _event_id(line: str) -> str | None:
    """The id, or None for a line that does not parse or does not carry one.

    Both shapes go through the same door on purpose: `unparseable_json` in the contract covers
    a line that is not JSON and a line with no `event_id`, and this function has to agree with
    that or the two definitions of "corrupt" would drift.
    """
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    identifier = record.get("event_id") if isinstance(record, dict) else None
    return str(identifier) if identifier is not None else None


def base_event_ids(bronze: Path) -> set[str]:
    """Every event id already in a bronze tree."""
    return {found for _, line in _lines(bronze) if (found := _event_id(line)) is not None}


def late_arrivals(
    out_dir: Path,
    *,
    base_seed: int,
    late_seed: int,
    profile: Profile = FAST,
    base_bronze: Path | None = None,
    arrival: int = 1,
) -> LateArrivalResult:
    """Write the late batches under ``out_dir/bronze`` and return what they contain.

    `base_bronze` is an optimisation with a correctness condition attached: pass the bronze
    tree the base seed already produced and this does not regenerate it. Pass one produced by a
    DIFFERENT seed and the answer is silently a different population, so callers that cannot
    prove which seed made a tree should pass nothing and let it be generated here. It is also
    how arrivals compose: an arrival after the first is filtered against the base PLUS every
    arrival before it, which is a tree that already contains them.

    `arrival` is this arrival's ordinal, and all it decides is the batch prefix - see
    `late_batch_prefix`. It defaults to 1, which is the population already in the workspace.

    No ledger is written. The generator's ledger is the by-construction answer for the
    population it generated, and this is a filtered subset of a second one: composing the two
    would need the base ledger's arithmetic re-derived over a population it never saw. The
    close is checked against the OSS lane recomputing the same events instead, which is what
    `tests/fast/test_databricks_dimension_parity.py` does.
    """
    out_dir = Path(out_dir)
    work = Path(tempfile.mkdtemp(prefix="samegold-late-"))
    try:
        if base_bronze is None:
            generate(work / "base", seed=base_seed, profile=profile)
            base_bronze = work / "base" / "bronze"
        known = base_event_ids(base_bronze)
        generate(work / "late", seed=late_seed, profile=profile)

        kept: dict[str, list[str]] = {}
        by_type: Counter[str] = Counter()
        by_month: Counter[str] = Counter()
        late_total = already_present = dropped = 0
        for path, line in _lines(work / "late" / "bronze"):
            late_total += 1
            identifier = _event_id(line)
            if identifier is None:
                dropped += 1
                continue
            if identifier in known:
                already_present += 1
                continue
            record = json.loads(line)
            by_type[str(record.get("event_type"))] += 1
            by_month[str(record.get("event_ts"))[:7]] += 1
            kept.setdefault(path.parent.name, []).append(line)

        written: list[Path] = []
        for batch, batch_lines in sorted(kept.items()):
            prefix = late_batch_prefix(arrival)
            target = out_dir / "bronze" / f"batch={prefix}{batch.split('=', 1)[-1]}"
            target.mkdir(parents=True, exist_ok=True)
            destination = target / "part-00000.json"
            destination.write_text("\n".join(batch_lines) + "\n", encoding="utf-8", newline="\n")
            written.append(destination)

        return LateArrivalResult(
            events=sum(len(v) for v in kept.values()),
            files=written,
            by_event_type=dict(sorted(by_type.items())),
            by_event_month=dict(sorted(by_month.items())),
            base_events=sum(1 for _ in _lines(base_bronze)),
            late_events=late_total,
            already_present=already_present,
            dropped_without_id=dropped,
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def population_for(
    out_dir: Path,
    *,
    base_seed: int,
    late_seeds: Sequence[int] = (),
    profile: Profile = FAST,
) -> Path:
    """The whole bronze tree a lane ingested: the base population, plus every late arrival.

    One function, because the alternative is every caller composing the arrivals by hand and
    one of them getting it wrong. `late_seeds=()` is the first close, one seed is the second,
    two is the third - and the parameter is a SEQUENCE rather than an optional seed because a
    third close is where "the late population" stopped being a single thing.

    Each arrival is filtered against the tree as it stands, which by then holds the base and
    every earlier arrival: an event already delivered is not delivered again, whichever arrival
    delivered it. And each is written under its own prefix, so no arrival can land on the files
    of another - which is what `late_batch_prefix` is for and what nothing checked while there
    were only two populations.
    """
    out_dir = Path(out_dir)
    generate(out_dir, seed=base_seed, profile=profile)
    for arrival, late_seed in enumerate(late_seeds, start=1):
        late_arrivals(
            out_dir,
            base_seed=base_seed,
            late_seed=late_seed,
            profile=profile,
            base_bronze=out_dir / "bronze",
            arrival=arrival,
        )
    return out_dir / "bronze"


# ---------------------------------------------------------------------------------------
# The population's FINGERPRINT, and the reason it exists.
#
# `population_for` gives the two halves of the Databricks comparison the same recipe. It does
# not give them the same DATA, and nothing checked that it had: the parity fixture selected
# its population by BRONZE LINE COUNT, so any change to the generator that preserved the
# number of events was invisible to it.
#
# Measured, twice, both count-preserving and rng-consumption-preserving:
#
#   * reordering `countries` in `generator/events.py` from ["ES","PT","FR","IT"] to
#     ["ES","PT","IT","FR"] leaves 1328 lines, 96 upserts, 4 heartbeats, 92 versions, 60
#     customers, 60 open and 32 closed rows - every published count identical - and gives
#     THIRTY customers a different history. The comparison reported that as AUTO CDC and the
#     hand-written MERGE producing different dimensions. They had not. The generator had moved
#     under a committed capture, and the failure sent its reader to look for a difference
#     between two runtimes that did not exist.
#   * renaming the skus changes 1216 values and **all nineteen** parity tests still pass -
#     dimension, close and late arrivals - because gross is `qty * unit_price_cents` and no
#     dimension carries a sku. Nothing in this repository could see it at all.
#
# So the tie is over CONTENT, over the WHOLE population rather than over the events that feed
# the dimension: the second measurement is the argument, not ambition. A dimension-scoped
# digest catches the first drift and not the second, and costs exactly the same.
#
# THE DOMAIN, which is the part that has to be written where it is read.
#
# Three of the 1328 lines are deliberately corrupt, and they are TRUNCATED objects:
#
#     {"event_id": "bad-0000009", "event_type": "order_placed",
#
# Python's `json.loads` raises on those and yields no record at all. MEASURED in local Spark,
# reading the same files with the declared schema: 1328 rows, of which three have a NULL
# `event_id` - so that reader nulls the whole row rather than keeping the fields before the
# truncation, and the two halves happen to exclude the same three lines.
#
# HAPPEN TO is why the domain asks for two columns and not one. Whether a partially parsed
# record keeps its leading fields is a reader OPTION - `spark.sql.json.enablePartialResults`
# is a boolean setting - and the reader that actually fills this table is Auto Loader with
# `schemaEvolutionMode=rescue`, which nothing in this repository can execute. A domain of
# "rows that have an event_id" would therefore rest on a behaviour measured from a DIFFERENT
# reader than the one in the workspace.
#
# So the domain is **rows carrying both an `event_id` and an `arrival_ts`**: an id and the
# time it arrived are what make a bronze row a complete event, and a line truncated after
# `event_type` has no arrival time under the reader measured here NOR under one that returned
# every field it managed to parse. It costs nothing and it does not depend on the answer to a
# question this repository cannot ask. What falls outside is COUNTED and published beside the
# digest rather than dropped silently:
#
#     digest_rows + rows_outside_the_digest = rows.bronze_events
#
# so a reader that started DROPPING the corrupt lines instead of keeping them would break that
# arithmetic rather than quietly shrink the population.
#
# THE VALUES ARE THE ONES THE TABLE HOLDS, not the ones the JSON text carries, and that
# distinction is load-bearing. `qty`, `new_qty` and `unit_price_cents` are declared BIGINT, and
# the generator emits 9223372036854775808 - two to the sixty-third, one past the top of the
# range - for `bad-0000008` and `bad-0000017`. Python reads that as an int; the workspace
# cannot store it and holds NULL. This is not inferred: `bad_events` in the committed record
# names those two ids with `unit_price_cents: null` beside `bad-0000007` and `bad-0000016`,
# which carry 9223372036854775807 and fit. So the renderer applies the declared range, because
# the digest is over the rows the workspace ingested and the schema is part of what they are.
#
# WHAT IT DOES NOT COVER, said here rather than found later:
#
#   * the CONTENT of the three truncated lines. Only their count is tied.
#   * a NEW field the generator starts emitting. The workspace's bronze schema is declared and
#     sixteen columns wide, so a new key lands in `_rescued_data` and neither side would see
#     it - the digests would agree while the populations differed. `_render` REFUSES a record
#     carrying a key outside the projection for exactly that reason: an unseen field becomes a
#     red test naming the field instead of a fingerprint that is quietly blind.
#   * whether either population is CORRECT. It says they are the same, and nothing else.
_DIGEST_FIELD_SEPARATOR = "\x1f"
_DIGEST_ROW_SEPARATOR = "\n"
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1

# The projection, in the order `samegold.pipelines.schema.bronze_schema` declares it, minus
# `_rescued_data` - which is Auto Loader's own column and has no counterpart on this side.
# `tests/fast/test_databricks_bundle.py` fails if this order drifts from the declared schema
# or from the statement in `publish_evidence.py`. The order is part of what is hashed.
BRONZE_DIGEST_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_ts",
    "arrival_ts",
    "order_id",
    "customer_id",
    "sku",
    "qty",
    "new_qty",
    "unit_price_cents",
    "currency",
    "return_id",
    "reason",
    "segment",
    "country",
    "boundary",
)
# The three the bronze schema declares BIGINT, which is what makes a value outside the signed
# 64-bit range NULL in the table rather than a large number.
BRONZE_DIGEST_BIGINT_COLUMNS: tuple[str, ...] = ("qty", "new_qty", "unit_price_cents")
# The two columns a row must carry to be a complete event, and therefore to be in the domain.
BRONZE_DIGEST_REQUIRED_COLUMNS: tuple[str, ...] = ("event_id", "arrival_ts")


@dataclass(frozen=True)
class PopulationDigest:
    """What both halves publish about the events they read."""

    digest: str
    digest_rows: int
    rows_outside_the_digest: int
    columns: tuple[str, ...]


def _render(record: dict[str, object], columns: Sequence[str]) -> str:
    """One bronze row as one line, by the same rule the workspace's SQL applies.

    Every step has a counterpart in `publish_evidence.py`, and the two are executed against
    each other in `tests/spark/test_databricks_population_digest.py` - which is the only
    reason this can be called a tie rather than a hope.

      * an absent value renders as the empty string, because the column is NULL in the table
        and `concat_ws` SKIPS nulls rather than emitting an empty field: without the coalesce
        an order with no `sku` and a sku with no `order_id` render to the same line;
      * a BIGINT column holding a value outside the signed 64-bit range renders as empty,
        because that is what the table holds - see the note above and `bad_events` in the
        record;
      * integers render as decimal, which `CAST(x AS STRING)` also does;
      * nothing else is allowed. A float renders differently in the two engines, and a digest
        whose value depends on which engine computed it is not a digest.
    """
    unexpected = sorted(set(record) - set(columns))
    if unexpected:
        raise ValueError(
            f"the generator emits {unexpected}, which is outside the digest's projection. The "
            f"workspace's bronze schema is sixteen columns wide, so a new field lands in "
            f"`_rescued_data` and BOTH digests would ignore it - they would agree while the "
            f"populations differed. Add the column to the bronze schema, to SCHEMA_HINTS, to "
            f"BRONZE_DIGEST_COLUMNS and to the statement in publish_evidence.py, or the "
            f"fingerprint is blind to it."
        )
    fields = []
    for column in columns:
        value = record.get(column)
        if value is None:
            fields.append("")
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError(
                f"{column} is {type(value).__name__} ({value!r}); the digest renders only "
                f"strings and integers, because those are the two things this side and "
                f"Spark's `CAST(x AS STRING)` are known to render identically"
            )
        # Out of range for the declared BIGINT, so the table holds NULL. The generator emits
        # 2**63 on purpose; `bad_events` in the record shows the workspace's own answer for
        # those two ids.
        if (
            isinstance(value, int)
            and column in BRONZE_DIGEST_BIGINT_COLUMNS
            and not _INT64_MIN <= value <= _INT64_MAX
        ):
            fields.append("")
            continue
        rendered = value if isinstance(value, str) else str(value)
        if not rendered.isascii():
            raise ValueError(
                f"{column} is not ASCII ({rendered!r}). The lines are sorted before hashing, "
                f"and Spark orders strings by their UTF-8 bytes while Python orders them by "
                f"code point; on ASCII the two orders are the same and off it they are not."
            )
        if _DIGEST_FIELD_SEPARATOR in rendered or _DIGEST_ROW_SEPARATOR in rendered:
            raise ValueError(
                f"{column} contains a digest separator ({rendered!r}), so two different rows "
                f"could render to the same line. The separators are U+001F and U+000A "
                f"precisely because no value this generator emits contains them - which is "
                f"checked here rather than asserted in a comment."
            )
        fields.append(rendered)
    return _DIGEST_FIELD_SEPARATOR.join(fields)


def population_digest(
    bronze: Path, columns: Sequence[str] = BRONZE_DIGEST_COLUMNS
) -> PopulationDigest:
    """The fingerprint of a generated bronze tree, in the workspace's own terms.

    `columns` is taken from the record when there is one, so a projection that drifted between
    the two sides shows up as a digest that does not match rather than as two halves hashing
    different things and never being compared.
    """
    columns = tuple(columns)
    lines: list[str] = []
    outside = 0
    for path in sorted(bronze.rglob("part-*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # A truncated line, which this reader cannot see at all. It is outside the
                # domain here and outside it in the table too - see the note above for why
                # that is asked of two columns rather than one.
                outside += 1
                continue
            if not isinstance(record, dict) or any(
                not record.get(column) for column in BRONZE_DIGEST_REQUIRED_COLUMNS
            ):
                outside += 1
                continue
            lines.append(_render(record, columns))
    digest = hashlib.sha256(_DIGEST_ROW_SEPARATOR.join(sorted(lines)).encode("utf-8")).hexdigest()
    return PopulationDigest(
        digest=digest,
        digest_rows=len(lines),
        rows_outside_the_digest=outside,
        columns=columns,
    )


def describe(result: LateArrivalResult) -> str:
    """The counts, in the form docs/databricks-run.md quotes them."""
    stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%d")
    types = ", ".join(f"{name} {count}" for name, count in result.by_event_type.items())
    months = ", ".join(f"{name} {count}" for name, count in result.by_event_month.items())
    return (
        f"{result.events} late events in {result.batches} batch directories "
        f"({len(result.files)} files), written {stamp}\n"
        # "already delivered", not "already in the base population": after the first
        # arrival the tree an arrival is filtered against is the base PLUS every arrival
        # before it, and calling that "the base population" would misname the 1328 events
        # the second arrival is measured against.
        f"  from {result.late_events} generated, of which {result.already_present} had "
        f"already been delivered by the {result.base_events} events before them, and "
        f"{result.dropped_without_id} carried no event_id\n"
        f"  by type : {types}\n"
        f"  by month: {months}"
    )
