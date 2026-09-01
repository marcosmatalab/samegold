"""Canonical digests, and the projection that makes them meaningful.

A digest over a table is only as honest as the projection it is taken over. Two mistakes
kill this kind of evidence, and both are made silently:

  1. hashing a column whose value depends on the wall clock or on the physical layout, so
     the digest never matches and the author quietly starts excluding columns until it does;
  2. hashing without a total order, so the digest depends on the shuffle and the author
     quietly adds a sort until the run they wanted is green.

So the projection is a value with rules, not a list of strings passed at the call site:
it refuses to be built over a non-deterministic column, and it refuses to be built without
a total order. ``CanonicalDigest`` cannot be constructed from a hex string at all, which is
the point: a report cannot be edited into agreement, it can only be recomputed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from samegold.domain.contract import NON_DETERMINISTIC_COLUMNS

_TOKEN: Final = object()


class ProjectionError(ValueError):
    """Raised when a projection would produce a digest that cannot mean anything."""


@dataclass(frozen=True, slots=True)
class Projection:
    """The columns a digest is taken over, and the total order it is taken in.

    ``allow`` is an escape hatch with teeth: a column in NON_DETERMINISTIC_COLUMNS can be
    digested only by naming it explicitly here, and the name then travels inside the
    projection into every evidence record, so the exception is published rather than hidden.
    """

    table: str
    columns: tuple[str, ...]
    order_by: tuple[str, ...]
    allow_non_deterministic: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.columns:
            raise ProjectionError(f"{self.table}: a projection needs at least one column")
        if not self.order_by:
            raise ProjectionError(
                f"{self.table}: a projection needs an explicit total order; "
                f"add order_by=(<primary key columns>,)"
            )
        missing = [c for c in self.order_by if c not in self.columns]
        if missing:
            raise ProjectionError(
                f"{self.table}: order_by columns not in the projection: {missing}"
            )
        banned = [
            c
            for c in self.columns
            if c in NON_DETERMINISTIC_COLUMNS and c not in self.allow_non_deterministic
        ]
        if banned:
            raise ProjectionError(
                f"{self.table}: refusing to digest non-deterministic columns {banned}. "
                f"Remove them, or declare them in allow_non_deterministic and say why in "
                f"the evidence record."
            )
        if len(set(self.columns)) != len(self.columns):
            raise ProjectionError(f"{self.table}: duplicate columns in projection")

    @property
    def spec(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "columns": list(self.columns),
            "order_by": list(self.order_by),
            "allow_non_deterministic": list(self.allow_non_deterministic),
        }


def _canonical(value: Any) -> str:
    """One value, one unambiguous string, with its TYPE in the string.

    An adversarial review found four collisions in the first version of this function, and
    all four came from the same mistake: rendering different values to the same text and
    trusting a separator to keep fields apart.

      * ``None`` and the literal string ``"\x00NULL"`` produced the same bytes;
      * ``1`` and ``"1"`` produced the same bytes;
      * ``True`` and ``"true"`` produced the same bytes;
      * a value containing the field separator moved the boundary between two fields, so
        ``("x\x1fy", "z")`` and ``("x", "y\x1fz")`` hashed identically.

    The fix is the standard one: a type tag, and a length prefix instead of a separator. The
    encoding is ``<tag>:<byte length>:<text>``, which no value can forge because the length
    is counted, not delimited. The four collisions are regression tests in
    tests/fast/test_digest.py and each names the value pair it came from.
    """
    if value is None:
        return "n:0:"
    if isinstance(value, bool):
        text, tag = ("true" if value else "false"), "b"
    elif isinstance(value, int):
        text, tag = str(value), "i"
    elif isinstance(value, Decimal):
        text, tag = format(value.normalize(), "f"), "d"
    elif isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ProjectionError(f"non-finite float in a digest: {value!r}")
        # Money is cents (int) by contract; a float here is a modelling mistake, but if it
        # gets this far we make it deterministic rather than engine dependent.
        text, tag = format(Decimal(repr(value)).normalize(), "f"), "f"
    elif isinstance(value, dt.datetime):
        v = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        text, tag = v.astimezone(dt.UTC).isoformat(timespec="microseconds"), "t"
    elif isinstance(value, dt.date):
        text, tag = value.isoformat(), "D"
    elif isinstance(value, bytes):
        text, tag = value.hex(), "x"
    else:
        text, tag = str(value), "s"
    return f"{tag}:{len(text.encode())}:{text}"


@dataclass(frozen=True, slots=True)
class CanonicalDigest:
    """A digest that knows what it is a digest of.

    There is no public constructor: ``CanonicalDigest("deadbeef")`` raises. The only ways
    to get one are ``of()`` (compute it) and ``parse()`` (read one back from evidence,
    which keeps the projection alongside it so a comparison across different projections
    fails loudly instead of returning False).
    """

    hexdigest: str
    projection: Projection
    row_count: int
    _token: Any = None

    def __post_init__(self) -> None:
        if self._token is not _TOKEN:
            raise ProjectionError(
                "CanonicalDigest cannot be constructed directly; use CanonicalDigest.of()"
            )

    @classmethod
    def of(cls, rows: Iterable[Mapping[str, Any]], projection: Projection) -> CanonicalDigest:
        materialised: list[Mapping[str, Any]] = list(rows)
        for row in materialised:
            missing = [c for c in projection.columns if c not in row]
            if missing:
                raise ProjectionError(
                    f"{projection.table}: rows are missing projected columns {missing}"
                )

        def key(row: Mapping[str, Any]) -> tuple[str, ...]:
            return tuple(_canonical(row[c]) for c in projection.order_by)

        ordered = sorted(materialised, key=key)
        seen: set[tuple[str, ...]] = set()
        hasher = hashlib.blake2b(digest_size=16)
        # The header is length-prefixed too: joining column names with a separator made
        # ("a|b", "c") and ("a", "b|c") hash to the same header.
        hasher.update(f"samegold/v2\x00{projection.table}\x00{len(projection.columns)}".encode())
        for column in projection.columns:
            hasher.update(f"\x00{len(column.encode())}:{column}".encode())
        for row in ordered:
            k = key(row)
            if k in seen:
                raise ProjectionError(
                    f"{projection.table}: order_by {projection.order_by} is not a total order "
                    f"(duplicate key {k}); the digest would depend on the shuffle"
                )
            seen.add(k)
            encoded = "".join(_canonical(row[c]) for c in projection.columns)
            hasher.update(f"\x00{len(encoded.encode())}:{encoded}".encode())
        return cls(hasher.hexdigest(), projection, len(ordered), _TOKEN)

    @classmethod
    def parse(cls, hexdigest: str, projection: Projection, row_count: int) -> CanonicalDigest:
        """Rebuild a digest read from an evidence file, keeping its projection attached."""
        if len(hexdigest) != 32 or any(c not in "0123456789abcdef" for c in hexdigest):
            raise ProjectionError(f"not a samegold digest: {hexdigest!r}")
        return cls(hexdigest, projection, row_count, _TOKEN)

    def agrees_with(self, other: CanonicalDigest) -> bool:
        """Agreement means same projection, same row count and same hash.

        Row count is part of it because it is free and because it caught a real collision:
        two rows whose encoding happened to join into one row's encoding hashed the same but
        counted differently.
        """
        if self.projection != other.projection:
            raise ProjectionError(
                f"comparing digests taken over different projections: "
                f"{self.projection.spec} vs {other.projection.spec}"
            )
        return self.hexdigest == other.hexdigest and self.row_count == other.row_count

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.projection.table}:{self.hexdigest[:12]}({self.row_count} rows)"


REVENUE_PROJECTION = Projection(
    table="revenue_by_month",
    columns=(
        "accounting_month",
        "close_version",
        "gross_cents",
        "returns_cents",
        "net_cents",
        "line_count",
        "return_count",
        "returns_rejected_count",
        "restated_at",
        "restatement_reason",
    ),
    order_by=("accounting_month", "close_version"),
    # restated_at is the CLOSE INSTANT that produced the version, not a wall clock, so it is
    # deterministic and belongs in the digest. Declaring it here is the difference between a
    # column that is reproducible and one that merely looks like a timestamp.
    allow_non_deterministic=("restated_at",),
)

# One close, before the version bookkeeping. Separate from REVENUE_PROJECTION on purpose: a
# snapshot has no close_version and no restated_at, and an earlier version of this file
# declared those columns on the snapshot and let the tests fill them with a literal zero.
SNAPSHOT_PROJECTION = Projection(
    table="revenue_snapshot",
    columns=(
        "accounting_month",
        "gross_cents",
        "returns_cents",
        "net_cents",
        "line_count",
        "return_count",
        "returns_rejected_count",
    ),
    order_by=("accounting_month",),
)

SCD2_PROJECTION = Projection(
    table="dim_customer_scd2",
    columns=("customer_id", "valid_from", "valid_to", "segment", "country", "is_current"),
    order_by=("customer_id", "valid_from"),
)

FACT_PROJECTION = Projection(
    table="fct_order_line",
    columns=("order_id", "sku", "customer_id", "qty", "unit_price_cents", "sale_ts"),
    order_by=("order_id", "sku"),
)


def digest_rows(rows: Sequence[Mapping[str, Any]], projection: Projection) -> CanonicalDigest:
    return CanonicalDigest.of(rows, projection)
