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
    """One value, one unambiguous string.

    The cases below are exactly the ones that made two engines disagree while being right:
    ``None`` vs empty string, ``Decimal('1.10')`` vs ``1.1``, a date with and without an
    offset, and an integer that arrived as a float.
    """
    if value is None:
        return "\x00NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ProjectionError(f"non-finite float in a digest: {value!r}")
        # Money is cents (int) by contract; a float here is a modelling mistake, but if it
        # gets this far we make it deterministic rather than engine dependent.
        return format(Decimal(repr(value)).normalize(), "f")
    if isinstance(value, dt.datetime):
        v = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return v.astimezone(dt.UTC).isoformat(timespec="microseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


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
        hasher.update(("|".join(projection.columns) + "\n").encode())
        for row in ordered:
            k = key(row)
            if k in seen:
                raise ProjectionError(
                    f"{projection.table}: order_by {projection.order_by} is not a total order "
                    f"(duplicate key {k}); the digest would depend on the shuffle"
                )
            seen.add(k)
            hasher.update(
                ("\x1f".join(_canonical(row[c]) for c in projection.columns) + "\x1e").encode()
            )
        return cls(hasher.hexdigest(), projection, len(ordered), _TOKEN)

    @classmethod
    def parse(cls, hexdigest: str, projection: Projection, row_count: int) -> CanonicalDigest:
        """Rebuild a digest read from an evidence file, keeping its projection attached."""
        if len(hexdigest) != 32 or any(c not in "0123456789abcdef" for c in hexdigest):
            raise ProjectionError(f"not a samegold digest: {hexdigest!r}")
        return cls(hexdigest, projection, row_count, _TOKEN)

    def agrees_with(self, other: CanonicalDigest) -> bool:
        if self.projection != other.projection:
            raise ProjectionError(
                f"comparing digests taken over different projections: "
                f"{self.projection.spec} vs {other.projection.spec}"
            )
        return self.hexdigest == other.hexdigest

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.projection.table}:{self.hexdigest[:12]}({self.row_count} rows)"


REVENUE_PROJECTION = Projection(
    table="revenue_by_month",
    columns=("accounting_month", "close_version", "gross_cents", "returns_cents", "net_cents"),
    order_by=("accounting_month", "close_version"),
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
