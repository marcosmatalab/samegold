"""Turning a series of closes into an immutable version history.

The close is bitemporal: a month has a value *as of* each close, and later arrivals never
rewrite an earlier close, they add a version. This module contains the pure bookkeeping that
turns "what the close said at each instant" into "the versions of that month", with no engine
and no I/O, so the rule can be tested in microseconds and mutated cheaply.

The rules, all four of them consequential:

  * A version is recorded only when the value CHANGES. A close that repeats the previous
    figures is not a restatement and must not create a version, or every month would
    accumulate one version per subsequent close for ever.
  * Version numbers are dense and start at zero, per month. A gap means a version was
    deleted, which is the one thing an accounting history may not allow.
  * ``restated_at`` is the close instant that produced the version, never the wall clock.
    Using ``now()`` would make the table non-deterministic and unhashable, and would make a
    re-run of history produce different data from the original run.
  * "Is this month closed yet?" is answered in the ACCOUNTING timezone. This used to slice
    the ISO string (``as_of[:7]``), which reads the month in whatever offset the caller's
    string happened to carry: a close at 2026-02-01 00:30 Europe/Madrid is
    ``2026-01-31T23:30:00+00:00``, whose prefix is "2026-01", so January was declared still
    open and the whole January close vanished. Passing the SAME INSTANT rendered as a
    Madrid-local string produced a version. An answer that depends on the representation of
    a timestamp rather than on the timestamp is not an answer.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

from samegold.domain.contract import ACCOUNTING_TIMEZONE

VALUE_COLUMNS = (
    "gross_cents",
    "returns_cents",
    "net_cents",
    "line_count",
    "return_count",
    "returns_rejected_count",
)

FIRST_CLOSE = "first close"
RESTATED = "late arrivals after close"


def instant_of(text: str) -> dt.datetime:
    """Parse an ISO timestamp into an aware instant.

    Accepts a trailing ``Z`` because that is what the pipeline writes, and treats a naive
    string as UTC because that is what every producer in this project emits.
    """
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=dt.UTC)


def accounting_month_of(instant: str) -> str:
    """The accounting month an ISO instant falls in, in the accounting timezone."""
    return instant_of(instant).astimezone(ZoneInfo(ACCOUNTING_TIMEZONE)).strftime("%Y-%m")


def versions_from_snapshots(
    snapshots: Sequence[tuple[str, Mapping[str, Mapping[str, int]]]],
    month_is_closed: Any = None,
) -> list[dict[str, Any]]:
    """Collapse per-close snapshots into the versioned close table.

    ``snapshots`` is a sequence of ``(as_of, {month: values})``, sorted here by ``as_of`` so
    the result is a function of the set rather than of the caller's order. ``month_is_closed``
    is an optional predicate ``(month, as_of) -> bool``; the default treats a month as closed
    from the first close that happens after the month ends, which is what makes the partial
    view of the current month stop generating spurious "restatements".
    """
    if month_is_closed is None:

        def month_is_closed(month: str, as_of: str) -> bool:
            return accounting_month_of(as_of) > month

    versions: list[dict[str, Any]] = []
    last: dict[str, tuple[int, ...]] = {}
    counters: dict[str, int] = {}
    # Sorted by INSTANT, not taken as given and not sorted as text. Two bugs, one after the
    # other:
    #
    #   * the function iterated the caller's list while the Spark twin ordered inside a
    #     window, so passing the same closes in another order gave a history running
    #     backwards from Python and forwards from Spark;
    #   * sorting the ISO STRINGS then fixed the wrong thing. "2026-02-01T00:30:00+01:00" is
    #     EARLIER than "2026-01-31T23:45:00+00:00" as an instant and later as text, and
    #     "...Z" and "...+00:00" denote the same instant and sort apart. A close is an
    #     instant; its spelling is not a fact about it.
    # (instant, text). The Spark twin orders by (as_of::timestamp, as_of) and this one sorted
    # by the instant alone, relying on `sorted` being stable - which makes the tie-break the
    # caller's input order rather than a property of the data. Both sides were total; they
    # were not the SAME total order, which is the distinction the dedup tie-break in
    # transform.py spends a paragraph on.
    for as_of, months in sorted(snapshots, key=lambda item: (instant_of(item[0]), item[0])):
        for month in sorted(months):
            if not month_is_closed(month, as_of):
                continue
            values = months[month]
            key = tuple(int(values[column]) for column in VALUE_COLUMNS)
            if last.get(month) == key:
                continue
            number = counters.get(month, 0)
            versions.append(
                {
                    "accounting_month": month,
                    "close_version": number,
                    **{column: int(values[column]) for column in VALUE_COLUMNS},
                    "restated_at": as_of,
                    "restatement_reason": FIRST_CLOSE if number == 0 else RESTATED,
                }
            )
            counters[month] = number + 1
            last[month] = key
    return versions


def current_versions(versions: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The latest version of every month: what a dashboard shows."""
    newest: dict[str, dict[str, Any]] = {}
    for row in versions:
        month = str(row["accounting_month"])
        if month not in newest or int(row["close_version"]) > int(newest[month]["close_version"]):
            newest[month] = dict(row)
    return [newest[month] for month in sorted(newest)]


def scd2_from_versions(
    versions: Sequence[Mapping[str, Any]],
    attributes: Sequence[str] = ("segment", "country"),
) -> list[dict[str, Any]]:
    """Build the Type 2 dimension from the SET of source versions. Pure, and order-free.

    This signature is the fix for a bug an adversarial review found in the previous one,
    which took (current_dimension, batch) and folded the batch in. That fold LOSES
    information: a version whose attributes match the open row is not recorded, and if a
    later correction changes an earlier interval, the discarded version becomes a real change
    that no longer exists anywhere. The result then depended on how the input had been cut
    into batches - the same versions, applied one at a time or all at once, produced different
    dimensions, and every structural invariant passed in both.

    A dimension that is a function of the SET of versions cannot have that bug. The price is
    that the caller has to keep the versions, which is why `gold_scd2_merge` maintains an
    append-only source table and recomputes the affected keys from it. That is also what a
    lakehouse is good at.

    Rules, in order:
      * two versions with the same key and the same valid_from are the same fact arriving
        twice; the last one by event_id wins;
      * adjacent versions with identical attributes are one version, keeping the earlier
        valid_from: a Type 2 dimension records changes, not heartbeats;
      * intervals are closed-open and contiguous, and exactly one row per key is open.
    """
    by_key: dict[Any, dict[str, dict[str, Any]]] = {}
    for version in versions:
        key = version["customer_id"]
        valid_from = str(version["valid_from"])
        slot = by_key.setdefault(key, {})
        previous = slot.get(valid_from)
        if previous is None or str(version.get("event_id", "")) >= str(
            previous.get("event_id", "")
        ):
            slot[valid_from] = dict(version)

    out: list[dict[str, Any]] = []
    for key in sorted(by_key):
        ordered = [by_key[key][valid_from] for valid_from in sorted(by_key[key])]
        collapsed: list[dict[str, Any]] = []
        for version in ordered:
            if collapsed and all(collapsed[-1][name] == version[name] for name in attributes):
                continue
            collapsed.append(version)
        for index, version in enumerate(collapsed):
            is_last = index == len(collapsed) - 1
            out.append(
                {
                    "customer_id": key,
                    "valid_from": str(version["valid_from"]),
                    "valid_to": None if is_last else str(collapsed[index + 1]["valid_from"]),
                    **{name: version[name] for name in attributes},
                    "is_current": is_last,
                }
            )
    return out


def scd2_apply(
    current: Sequence[Mapping[str, Any]],
    batch: Sequence[Mapping[str, Any]],
    attributes: Sequence[str] = ("segment", "country"),
) -> list[dict[str, Any]]:
    """Incremental application, defined as a recomputation over the accumulated versions.

    ``current`` is the SOURCE VERSIONS seen so far, not the materialised dimension. Keeping
    the source is what makes the operation associative and commutative; folding a batch into
    a materialised dimension is not, and the difference is a lost interval.
    """
    return scd2_from_versions([*current, *batch], attributes)


def collapse_versions(
    versions: Sequence[Mapping[str, Any]],
    attributes: Sequence[str] = ("segment", "country"),
) -> list[dict[str, Any]]:
    """The two collapses a Type 2 source series is subject to, as one pure function.

    Extracted from the generator's ledger because a test that re-implements the code it is
    protecting protects nothing: the first version of this rule lived inline in
    ``generator/events.py`` and its regression test copied the loop into the test body,
    asserted on its own copy, and passed against the buggy implementation when a review
    reverted it.

    The two rules, in order, and the order is the bug:

      * versions sharing a ``valid_from`` are one fact arriving twice; the one with the
        greatest ``event_id`` wins, which is the same tie-break ``scd2_from_versions`` uses
        and is what makes the result a function of the SET rather than of the caller's order.
        Without it "the last one wins" means "whichever the caller passed last";
      * ADJACENT versions with identical attributes are one version. A Type 2 dimension
        records changes, not heartbeats.

    Doing both in a single pass replaces the previous entry in place for the first rule and
    then never compares the replacement with its NEW predecessor: A(t1,X), B(t2,Y), C(t2,X)
    comes out as [A(X), C(X)], an adjacent identical pair surviving the collapse the loop is
    performing. Two passes cannot have that bug.
    """
    ordered = sorted(
        versions,
        key=lambda v: (instant_of(str(v["valid_from"])), str(v.get("event_id", ""))),
    )
    deduped: list[dict[str, Any]] = []
    for version in ordered:
        if deduped and deduped[-1]["valid_from"] == version["valid_from"]:
            deduped[-1] = dict(version)
        else:
            deduped.append(dict(version))
    collapsed: list[dict[str, Any]] = []
    for version in deduped:
        if collapsed and all(collapsed[-1][name] == version[name] for name in attributes):
            continue
        collapsed.append(version)
    return collapsed
