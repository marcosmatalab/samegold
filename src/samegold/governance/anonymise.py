"""The four anonymisation techniques the exam guide names, implemented and tested.

Each one has a different failure mode, and the difference is the point:

  * **pseudonymisation by hashing** is deterministic and irreversible, which makes it safe to
    publish and useless for support: nobody can answer "which customer is this". It is only as
    strong as the salt, and an unsalted hash of a low-cardinality identifier is a rainbow
    table away from being the identifier itself, so the salt is required, not optional.
  * **tokenisation** is deterministic and REVERSIBLE by whoever holds the vault. That is what
    makes it useful and what makes it a liability: the vault is now the crown jewels.
  * **suppression** is the only one with no residual risk and no utility. It is the right
    answer more often than people like.
  * **generalisation** keeps utility (a region, a month) at a stated cost in precision. It is
    also the one that silently fails: generalising to a bucket with one member in it does not
    anonymise anything, so the function refuses buckets that would not be k-anonymous when it
    is given the group sizes.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
from collections.abc import Mapping
from zoneinfo import ZoneInfo

from samegold.domain.contract import ACCOUNTING_TIMEZONE

SUPPRESSED = "[suppressed]"

# A demonstration region map. Real ones come from the business, and the point of having it
# here at all is that generalisation is a decision someone made, not a library call.
_REGIONS: Mapping[str, str] = {
    "ES": "Iberia",
    "PT": "Iberia",
    "FR": "Western Europe",
    "IT": "Southern Europe",
}


def pseudonymise(value: str, salt: str) -> str:
    """Salted SHA-256. Deterministic, irreversible, and worthless without the salt."""
    if not salt:
        raise ValueError(
            "pseudonymise needs a salt: an unsalted hash of an identifier with few possible "
            "values can be reversed by hashing all of them"
        )
    return hashlib.sha256(f"{salt}\x00{value}".encode()).hexdigest()[:32]


def tokenise(value: str, key: bytes, vault: dict[str, str] | None = None) -> str:
    """Keyed token, reversible by whoever holds the vault. HMAC, not a plain hash."""
    token = hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:24]
    if vault is not None:
        vault[token] = value
    return token


def detokenise(token: str, vault: Mapping[str, str]) -> str | None:
    return vault.get(token)


def suppress(_: object) -> str:
    return SUPPRESSED


def generalize_country(country: str | None) -> str:
    return _REGIONS.get(country or "", "Other")


def generalize_timestamp(value: dt.datetime, precision: str = "month") -> str:
    """Reduce a timestamp to a period. 'day' keeps more utility and less protection.

    The period is taken in the ACCOUNTING timezone, like every other period in this project.
    Reading the calendar fields off the raw value instead put `2026-03-31T22:30Z` in March
    while `rules.accounting_month` put it in April, so an anonymised aggregate would fail to
    reconcile with the close at every month boundary - twice a day for two hours, every
    month, in exactly the timezone the contract names. A naive value is treated as UTC,
    which is what every producer here emits.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    local = value.astimezone(ZoneInfo(ACCOUNTING_TIMEZONE))
    if precision == "month":
        return f"{local.year:04d}-{local.month:02d}"
    if precision == "day":
        return local.date().isoformat()
    if precision == "year":
        return f"{local.year:04d}"
    raise ValueError(f"unknown precision {precision!r}")


def k_anonymity(group_sizes: Mapping[str, int]) -> int:
    """The smallest group after generalisation. A k of 1 is not anonymisation."""
    return min(group_sizes.values()) if group_sizes else 0
