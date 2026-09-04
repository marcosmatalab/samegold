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
import json
import shutil
import tempfile
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from samegold.generator.events import FAST, Profile, generate

# The prefix that keeps the two arrivals apart in one landing volume. Auto Loader lists the
# directory; two `batch=202601010000` directories from two generations would be one directory
# with one file in it, and the second upload would silently replace the first.
LATE_PREFIX = "late-"


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
) -> LateArrivalResult:
    """Write the late batches under ``out_dir/bronze`` and return what they contain.

    `base_bronze` is an optimisation with a correctness condition attached: pass the bronze
    tree the base seed already produced and this does not regenerate it. Pass one produced by a
    DIFFERENT seed and the answer is silently a different population, so callers that cannot
    prove which seed made a tree should pass nothing and let it be generated here.

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
            target = out_dir / "bronze" / f"batch={LATE_PREFIX}{batch.split('=', 1)[-1]}"
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
    late_seed: int | None,
    profile: Profile = FAST,
) -> Path:
    """The whole bronze tree a lane ingested: the base population, plus the late one if any.

    One function, because the alternative is every caller composing the two by hand and one of
    them getting it wrong. `late_seed=None` is the first close; a seed is the second.
    """
    out_dir = Path(out_dir)
    generate(out_dir, seed=base_seed, profile=profile)
    if late_seed is not None:
        late_arrivals(
            out_dir,
            base_seed=base_seed,
            late_seed=late_seed,
            profile=profile,
            base_bronze=out_dir / "bronze",
        )
    return out_dir / "bronze"


def describe(result: LateArrivalResult) -> str:
    """The counts, in the form docs/databricks-run.md quotes them."""
    stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%d")
    types = ", ".join(f"{name} {count}" for name, count in result.by_event_type.items())
    months = ", ".join(f"{name} {count}" for name, count in result.by_event_month.items())
    return (
        f"{result.events} late events in {result.batches} batch directories "
        f"({len(result.files)} files), written {stamp}\n"
        f"  from {result.late_events} generated, of which {result.already_present} were already "
        f"in the base population of {result.base_events} and {result.dropped_without_id} "
        f"carried no event_id\n"
        f"  by type : {types}\n"
        f"  by month: {months}"
    )
