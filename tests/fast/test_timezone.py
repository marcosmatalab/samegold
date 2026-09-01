"""The close must not depend on the machine's timezone.

This file exists because a document claimed it did. An adversarial review checked, found no
such test anywhere, and was right: the reference pins `SET TimeZone='UTC'` on its connection,
which is the fix, but nothing verified that the fix works or that it stays.

The bug it guards against was real. `INTERVAL 45 DAY` over a TIMESTAMPTZ is calendar
arithmetic in the session timezone, so under Europe/Madrid - the accounting timezone this
project declares - the 45-day window is 44h23 or 45h01 long across a daylight-saving
boundary, and the two implementations disagreed on a real seed.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pytest

from samegold.generator.events import FAST, Profile, generate
from samegold.oracle.duckdb_gold import DuckDBWitness, revenue_versions
from samegold.verify.digest import REVENUE_PROJECTION, CanonicalDigest

# Late-return heavy and starting a fortnight before the spring clock change, which is where
# the calendar-versus-elapsed difference actually bites.
DST_PROFILE = Profile(
    days=50,
    start_date=dt.date(2026, 2, 1),
    customers=40,
    skus=20,
    orders_per_day=12,
    return_rate=1.0,
    late_return_share=1.0,
)

TIMEZONES = ("UTC", "Europe/Madrid", "America/New_York")


@pytest.mark.parametrize("timezone", TIMEZONES)
def test_the_reference_agrees_with_the_ledger_under_any_timezone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timezone: str
) -> None:
    monkeypatch.setenv("TZ", timezone)
    if hasattr(os, "tzset"):
        os.tzset()
    result = generate(tmp_path / f"g-{timezone.replace('/', '-')}", seed=4, profile=DST_PROFILE)
    witness = DuckDBWitness()
    for close in result.ledger.closes:
        as_of = dt.datetime.fromisoformat(close)
        expected = {m: v for (m, a), v in result.ledger.revenue.items() if a == close}
        assert witness.revenue(tmp_path / f"g-{timezone.replace('/', '-')}" / "bronze", as_of) == (
            expected
        ), f"the close depends on the machine timezone ({timezone}) at {close}"


def test_the_versioned_close_digest_is_the_same_in_three_timezones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digests = []
    for timezone in TIMEZONES:
        monkeypatch.setenv("TZ", timezone)
        if hasattr(os, "tzset"):
            os.tzset()
        root = tmp_path / timezone.replace("/", "-")
        result = generate(root, seed=9, profile=FAST)
        closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
        digests.append(
            CanonicalDigest.of(revenue_versions(root / "bronze", closes), REVENUE_PROJECTION)
        )
    assert digests[0].agrees_with(digests[1])
    assert digests[1].agrees_with(digests[2])
