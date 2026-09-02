"""Seeded event generator with a by-construction ledger.

Design decision that matters more than any other in this file: the generator does not
derive the truth from the events it wrote. It decides the intent first (orders, amendments,
returns, customer changes), computes the ledger from that intent, and only then serialises
the intent into events, adding noise that is chosen so it cannot move the ledger:

  * duplicates are byte-identical copies under a different file path, so a pipeline that
    deduplicates by the producer's event_id sees no change;
  * corrupt records are ADDITIONAL records, never mutations of good ones, so every one of
    them has a known quarantine reason and a known count;
  * arrival delay and out-of-order arrival move ``arrival_ts`` only.

That is what makes the ledger an oracle rather than a second implementation: it is a
record of what was emitted, not a recomputation of it. It is still written by the same
author as the pipeline, so it shares the author's understanding of the contract; that
residual is what the DuckDB witness and the specification mutants are for.
"""

from __future__ import annotations

import datetime as dt
import json
import random
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from samegold.domain.bitemporal import collapse_versions
from samegold.domain.contract import (
    ACCOUNTING_TIMEZONE,
    CURRENCY,
    MAX_LINE_QUANTITY,
    MAX_UNIT_PRICE_CENTS,
    QuarantineReason,
)
from samegold.domain.rules import accounting_month, is_return_within_window

_TZ = ZoneInfo(ACCOUNTING_TIMEZONE)


@dataclass(frozen=True, slots=True)
class Profile:
    """Every knob of the simulation, with the defaults used by the published claims.

    The rates are deliberately high compared with a real shop (a real return rate is 8-10%,
    not 18%): the harness needs enough of the rare paths per run to say anything, and the
    cost of a higher rate is that the numbers describe the simulation, not the retail sector.
    We say so in the README instead of pretending the numbers are industry figures.
    """

    days: int = 90
    start_date: dt.date = dt.date(2026, 1, 1)
    customers: int = 500
    skus: int = 120
    orders_per_day: int = 40
    max_lines_per_order: int = 4
    return_rate: float = 0.18
    # Fraction of returns that land in the long tail (30-60 days): these are the ones that
    # reopen a closed month, and a few of them fall outside the 45-day window on purpose.
    late_return_share: float = 0.35
    # Fraction of returns that claim MORE units than were sold. Small, and not zero: the
    # contract has a reason for it, all three implementations have a branch for it, and
    # until this knob existed no run could reach any of them.
    over_return_rate: float = 0.04
    amend_rate: float = 0.10
    customer_change_rate: float = 0.25
    duplicate_rate: float = 0.12
    # Fraction of duplicates that arrive far later than the watermark. These are the ones a
    # stateful dedup can miss; measuring that escape rate is a published claim, not a bug.
    duplicate_late_share: float = 0.25
    corrupt_rate: float = 0.04
    arrival_delay_mean_minutes: float = 6.0
    arrival_delay_tail_share: float = 0.03
    arrival_delay_tail_hours: float = 30.0
    batch_minutes: int = 60

    def scaled(self, factor: float) -> Profile:
        """A smaller or larger version of the same shape, for the fast lane."""
        return Profile(
            days=max(1, int(self.days * factor)),
            start_date=self.start_date,
            customers=max(2, int(self.customers * factor)),
            skus=max(2, int(self.skus * factor)),
            orders_per_day=max(1, int(self.orders_per_day * factor)),
            max_lines_per_order=self.max_lines_per_order,
            return_rate=self.return_rate,
            late_return_share=self.late_return_share,
            over_return_rate=self.over_return_rate,
            amend_rate=self.amend_rate,
            customer_change_rate=self.customer_change_rate,
            duplicate_rate=self.duplicate_rate,
            duplicate_late_share=self.duplicate_late_share,
            corrupt_rate=self.corrupt_rate,
            arrival_delay_mean_minutes=self.arrival_delay_mean_minutes,
            arrival_delay_tail_share=self.arrival_delay_tail_share,
            arrival_delay_tail_hours=self.arrival_delay_tail_hours,
            batch_minutes=self.batch_minutes,
        )


FAST = Profile(days=14, customers=60, skus=25, orders_per_day=12)
CI = Profile(days=45, customers=200, skus=60, orders_per_day=25)
FULL = Profile()


@dataclass
class Ledger:
    """What the pipeline must produce, known by construction."""

    revenue: dict[tuple[str, str], dict[str, int]] = field(default_factory=dict)
    """(accounting_month, as_of_close_iso) -> {gross_cents, returns_cents, net_cents}"""
    business_revenue: dict[tuple[str, str], dict[str, int]] = field(default_factory=dict)
    """The same, over the simulated shop ONLY: every boundary fixture excluded.

    Two projections of one arithmetic, not two ledgers. ``revenue`` is the close - every
    accepted line, fixtures included - and it is what the pipelines are compared against,
    because a fixture the close drops is a bug and a fixture it miscounts is a bug.

    ``business_revenue`` exists because SG-04 publishes a PERCENTAGE OF A MONTH on the front
    page of the README, and a boundary case is scaffolding. The point is structural rather
    than a matter of degree: a case that tests the price bound must sit exactly on it, so it
    is by construction the largest single line the contract allows, and it lands in whatever
    month the fixtures use. At the bounds this project shipped for one round - a hundred
    million euros a unit - that one line was 168 times the whole simulated business of its
    month, it moved the published restatement figure from 6.48% to 3.38% and it moved which
    month was worst. Nothing about the pipeline had changed. The bounds are business-sized
    now (see domain/contract.py), which takes the distortion from 168x to under 2%, and under
    2% of a headline business number is still the harness measuring itself.
    """
    dim_customer: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    """customer_id -> ordered SCD2 versions"""
    quarantine: dict[str, int] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    closes: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "revenue": [
                {"accounting_month": m, "as_of": a, **v}
                for (m, a), v in sorted(self.revenue.items())
            ],
            "business_revenue": [
                {"accounting_month": m, "as_of": a, **v}
                for (m, a), v in sorted(self.business_revenue.items())
            ],
            "dim_customer": self.dim_customer,
            "quarantine": self.quarantine,
            "counts": self.counts,
            "closes": self.closes,
        }


@dataclass
class GenerationResult:
    ledger: Ledger
    files: list[Path]
    profile: Profile
    seed: int

    @property
    def event_count(self) -> int:
        return int(self.ledger.counts["events_written"])


def _close_instants(start: dt.date, days: int) -> list[dt.datetime]:
    """Month-end closes at 23:59:59 on day 5 of the following month, Europe/Madrid.

    T+5 rather than T+0 because a close that happens at midnight on the 1st has no late
    data to absorb and the whole restatement story disappears.
    """
    out: list[dt.datetime] = []
    end = start + dt.timedelta(days=days + 75)
    y, m = start.year, start.month
    while dt.date(y, m, 1) <= end:
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        close_day = dt.datetime(ny, nm, 5, 23, 59, 59, tzinfo=_TZ)
        out.append(close_day.astimezone(dt.UTC))
        y, m = ny, nm
    return out


def _delay(rng: random.Random, p: Profile) -> dt.timedelta:
    if rng.random() < p.arrival_delay_tail_share:
        return dt.timedelta(hours=rng.uniform(2.0, p.arrival_delay_tail_hours))
    minutes = rng.expovariate(1.0 / p.arrival_delay_mean_minutes)
    return dt.timedelta(minutes=min(minutes, 240.0))


def _scopes(entry: dict[str, Any]) -> tuple[str, ...]:
    """Which ledger projections a fact or a return belongs to.

    Everything is part of the close. Only the entries a boundary case created carry a
    ``boundary`` tag, and only those are held out of the business projection.
    """
    return ("all",) if "boundary" in entry else ("all", "business")


def generate(out_dir: Path, seed: int, profile: Profile = FAST) -> GenerationResult:
    """Write bronze JSONL files under ``out_dir`` and return the ledger.

    Files are named by arrival batch (``batch=YYYYmmddHHMM/part-*.json``) so that the
    directory listing order and the arrival order agree, which is what a file-source
    reader sees in production and what makes the arrival-permutation experiment meaningful.
    """
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    (out_dir / "bronze").mkdir(parents=True, exist_ok=True)

    closes = _close_instants(profile.start_date, profile.days)
    ledger = Ledger(closes=[c.isoformat() for c in closes])
    events: list[tuple[dt.datetime, dict[str, Any]]] = []  # (arrival_ts, record)
    quarantine_counts: dict[str, int] = defaultdict(int)

    customers = [f"C{idx:06d}" for idx in range(profile.customers)]
    skus = [f"SKU-{idx:05d}" for idx in range(profile.skus)]
    prices = {sku: rng.randrange(199, 24999) for sku in skus}
    segments = ["retail", "pro", "vip"]
    countries = ["ES", "PT", "FR", "IT"]

    # ---- customer dimension (SCD2 source) -------------------------------------------
    base_ts = dt.datetime.combine(profile.start_date, dt.time(0, 0), tzinfo=dt.UTC)
    for cid in customers:
        version_ts = base_ts
        attrs = {"segment": rng.choice(segments), "country": rng.choice(countries)}
        versions = [{"valid_from": version_ts.isoformat(), **attrs}]
        events.append(
            (
                version_ts + _delay(rng, profile),
                {
                    "event_id": f"cu-{cid}-0",
                    "event_type": "customer_upserted",
                    "event_ts": version_ts.isoformat(),
                    "customer_id": cid,
                    "segment": attrs["segment"],
                    "country": attrs["country"],
                },
            )
        )
        n_changes = 0
        while rng.random() < profile.customer_change_rate and n_changes < 3:
            n_changes += 1
            version_ts = base_ts + dt.timedelta(
                days=rng.randrange(1, max(2, profile.days)), hours=rng.randrange(0, 24)
            )
            attrs = {
                "segment": rng.choice(segments),
                "country": attrs["country"] if rng.random() < 0.7 else rng.choice(countries),
            }
            versions.append({"valid_from": version_ts.isoformat(), **attrs})
            events.append(
                (
                    version_ts + _delay(rng, profile),
                    {
                        "event_id": f"cu-{cid}-{n_changes}",
                        "event_type": "customer_upserted",
                        "event_ts": version_ts.isoformat(),
                        "customer_id": cid,
                        "segment": attrs["segment"],
                        "country": attrs["country"],
                    },
                )
            )
        # The rule lives in domain/bitemporal.py, not here. It is the same rule the two
        # engines apply, it has an ordering subtlety worth one function's worth of
        # explanation, and while it was inline its regression test re-implemented it in the
        # test body and passed against the buggy version.
        ledger.dim_customer[cid] = collapse_versions(versions)

    # ---- orders, amendments, returns -------------------------------------------------
    # facts[(order_id, sku)] = dict with qty, price, sale_ts, arrival_ts of the sale
    facts: dict[tuple[str, str], dict[str, Any]] = {}
    returns: list[dict[str, Any]] = []
    # Returns the contract refuses but that still belong to a month: outside the 45-day
    # window, or for more units than were sold. They are reported in gold, so a rule that
    # silently widens or narrows changes a published number instead of vanishing.
    rejected_returns: list[dict[str, Any]] = []
    order_seq = 0

    for day in range(profile.days):
        day_start = base_ts + dt.timedelta(days=day)
        for _ in range(profile.orders_per_day):
            order_seq += 1
            order_id = f"O{order_seq:08d}"
            cid = rng.choice(customers)
            sale_ts = day_start + dt.timedelta(
                hours=rng.randrange(0, 24), minutes=rng.randrange(0, 60)
            )
            n_lines = rng.randrange(1, profile.max_lines_per_order + 1)
            chosen = rng.sample(skus, k=min(n_lines, len(skus)))
            for sku in chosen:
                qty = rng.randrange(1, 5)
                price = prices[sku]
                arrival = sale_ts + _delay(rng, profile)
                events.append(
                    (
                        arrival,
                        {
                            "event_id": f"op-{order_id}-{sku}",
                            "event_type": "order_placed",
                            "event_ts": sale_ts.isoformat(),
                            "order_id": order_id,
                            "customer_id": cid,
                            "sku": sku,
                            "qty": qty,
                            "unit_price_cents": price,
                            "currency": CURRENCY,
                        },
                    )
                )
                facts[(order_id, sku)] = {
                    "customer_id": cid,
                    "qty0": qty,
                    "qty": qty,
                    "unit_price_cents": price,
                    "sale_ts": sale_ts,
                    "arrival_ts": arrival,
                }

                # Amendments: up to three per line, each replacing the effective quantity.
                # More than one on purpose. With a single amendment per line the tie-break in
                # "last amendment wins" is never exercised, and a mutation campaign reported
                # the ORDER BY of that window as an equivalent mutant - it was not equivalent,
                # it was untested. All of them land within 72 hours of the sale, which keeps
                # them ahead of any return (returns start on day 5) and keeps the validity of
                # a return decidable against a settled quantity.
                amendments: list[dict[str, Any]] = []
                current_qty = qty
                for k in range(3):
                    if rng.random() >= profile.amend_rate:
                        break
                    current_qty = max(1, current_qty + rng.choice([-1, 1, 2]))
                    amend_ts = sale_ts + dt.timedelta(
                        hours=12 * k + rng.randrange(1, 13), minutes=rng.randrange(0, 60)
                    )
                    amend_arrival = amend_ts + _delay(rng, profile)
                    events.append(
                        (
                            amend_arrival,
                            {
                                "event_id": f"am-{order_id}-{sku}-{k}",
                                "event_type": "order_line_amended",
                                "event_ts": amend_ts.isoformat(),
                                "order_id": order_id,
                                "sku": sku,
                                "new_qty": current_qty,
                            },
                        )
                    )
                    amendments.append(
                        {
                            "event_id": f"am-{order_id}-{sku}-{k}",
                            "event_ts": amend_ts,
                            "arrival_ts": amend_arrival,
                            "qty": current_qty,
                        }
                    )
                if amendments:
                    facts[(order_id, sku)]["qty"] = current_qty
                    facts[(order_id, sku)]["amendments"] = amendments

                # return: the interesting one
                if rng.random() < profile.return_rate:
                    if rng.random() < profile.late_return_share:
                        offset_days = rng.randrange(30, 61)  # some fall outside the window
                    else:
                        offset_days = rng.randrange(5, 30)
                    return_ts = sale_ts + dt.timedelta(days=offset_days, hours=rng.randrange(0, 24))
                    r_arrival = return_ts + _delay(rng, profile)
                    eff_qty = int(facts[(order_id, sku)]["qty"])
                    # A small share of returns is for MORE units than were sold, so that
                    # `return_exceeds_sold_qty` is a reason some run actually produces. It
                    # was unreachable by construction: `randrange(1, eff_qty + 1)` makes
                    # `r_qty <= eff_qty` a tautology, so the branch existed in all three
                    # implementations and was exercised by none of them, while the test that
                    # checks "every reason is reachable" passed by grepping the source for
                    # the literal string. A reason nobody can produce is a reason nobody
                    # maintains, and grepping for its name is not producing it.
                    if eff_qty > 0 and rng.random() < profile.over_return_rate:
                        r_qty = eff_qty + rng.randrange(1, 4)
                    else:
                        r_qty = rng.randrange(1, eff_qty + 1) if eff_qty > 0 else 1
                    events.append(
                        (
                            r_arrival,
                            {
                                "event_id": f"rt-{order_id}-{sku}",
                                "event_type": "return_registered",
                                "event_ts": return_ts.isoformat(),
                                "return_id": f"R{order_seq:08d}-{sku}",
                                "order_id": order_id,
                                "sku": sku,
                                "qty": r_qty,
                                "reason": rng.choice(["size", "damaged", "changed_mind"]),
                            },
                        )
                    )
                    valid = is_return_within_window(sale_ts, return_ts) and r_qty <= eff_qty
                    if valid:
                        returns.append(
                            {
                                "order_id": order_id,
                                "sku": sku,
                                "qty": r_qty,
                                "unit_price_cents": price,
                                "sale_ts": sale_ts,
                                "arrival_ts": r_arrival,
                            }
                        )
                    else:
                        reason = (
                            QuarantineReason.RETURN_OUTSIDE_WINDOW
                            if not is_return_within_window(sale_ts, return_ts)
                            else QuarantineReason.RETURN_EXCEEDS_SOLD_QTY
                        )
                        quarantine_counts[str(reason)] += 1
                        rejected_returns.append(
                            {"sale_ts": sale_ts, "arrival_ts": r_arrival, "reason": str(reason)}
                        )

    # ---- noise: duplicates and corrupt records --------------------------------------
    originals = list(events)
    n_dup = int(len(originals) * profile.duplicate_rate)
    duplicates_late = 0
    for _ in range(n_dup):
        arrival, rec = originals[rng.randrange(len(originals))]
        if rng.random() < profile.duplicate_late_share:
            extra = dt.timedelta(days=rng.uniform(1.0, 20.0))
            duplicates_late += 1
        else:
            extra = dt.timedelta(minutes=rng.uniform(1.0, 90.0))
        events.append((arrival + extra, dict(rec)))

    n_corrupt = int(len(originals) * profile.corrupt_rate)
    corrupt_kinds = [
        (QuarantineReason.UNPARSEABLE_JSON, "unparseable"),
        (QuarantineReason.UNKNOWN_EVENT_TYPE, "unknown_type"),
        (QuarantineReason.MISSING_REQUIRED_FIELD, "missing_field"),
        (QuarantineReason.NON_POSITIVE_QUANTITY, "negative_qty"),
        (QuarantineReason.NEGATIVE_PRICE, "negative_price"),
        (QuarantineReason.RETURN_WITHOUT_ORDER, "orphan_return"),
        (QuarantineReason.UNKNOWN_CURRENCY, "bad_currency"),
        (QuarantineReason.AMOUNT_OUT_OF_RANGE, "huge_amount"),
        # A number that does not FIT the column, as opposed to one that fits and breaks a
        # business rule. `huge_amount` above is Long.MaxValue: a legal BIGINT, read into the
        # column, refused by the contract's bound. This one is Long.MaxValue plus one, and no
        # rule ever sees it, because the READER cannot put it in a BIGINT column.
        #
        # Every lane does the same thing with it, measured rather than assumed: Spark reading
        # a declared schema in PERMISSIVE mode nulls that ONE column and copies the raw line
        # into `_rescued_data` (the rest of the record survives); Auto Loader with the schema
        # hints does the same into its rescued column; DuckDB reads it as JSON, `json_type`
        # calls it UBIGINT and `TRY_CAST(... AS BIGINT)` returns NULL. So the value is gone and
        # the column is NULL in all three - and `missing_required_field` catches it, because
        # after the rescue the field IS missing.
        #
        # It is generated for the failure it is one edit away from: the door out of here is a
        # NULL column, which is the quietest thing a pipeline can produce. Nothing counted the
        # rescue at all - `claims.py` passed `rescued=0` to the conservation invariant and
        # called the term structurally zero - so a value that vanished into the rescue column
        # was accounted for only by the presence rule that happened to be standing behind it.
        # `values_beyond_bigint` in the ledger is now counted at write time and compared
        # against the reference's own recount, so the row is counted as WELL as classified.
        (QuarantineReason.MISSING_REQUIRED_FIELD, "beyond_bigint"),
    ]
    for i in range(n_corrupt):
        reason, kind = corrupt_kinds[i % len(corrupt_kinds)]
        arrival, _ = originals[rng.randrange(len(originals))]
        ts = (arrival - dt.timedelta(minutes=1)).isoformat()
        eid = f"bad-{i:07d}"
        if kind == "unparseable":
            # A line that is not valid JSON at all: the closing brace is missing on purpose.
            rec = {"__raw__": '{"event_id": "' + eid + '", "event_type": "order_placed",'}
        elif kind == "unknown_type":
            rec = {"event_id": eid, "event_type": "warehouse_pinged", "event_ts": ts}
        elif kind == "missing_field":
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "sku": "SKU-00001",
            }
        elif kind == "negative_qty":
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "order_id": f"OBAD{i}",
                "customer_id": customers[0],
                "sku": skus[0],
                "qty": -3,
                "unit_price_cents": 1000,
                "currency": CURRENCY,
            }
        elif kind == "negative_price":
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "order_id": f"OBAD{i}",
                "customer_id": customers[0],
                "sku": skus[0],
                "qty": 1,
                "unit_price_cents": -500,
                "currency": CURRENCY,
            }
        elif kind == "huge_amount":
            # A price that is a legal BIGINT and outside the contract's bound. Three of these
            # in one close used to end it outright: Spark refused to produce any month with an
            # ARITHMETIC_OVERFLOW and DuckDB published a gross that does not fit its column.
            # The reason exists because of that, and it is generated so that it is REACHED.
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "order_id": f"OBAD{i}",
                "customer_id": customers[0],
                "sku": skus[0],
                "qty": 1,
                "unit_price_cents": 9223372036854775807,
                "currency": CURRENCY,
            }
        elif kind == "beyond_bigint":
            # 2^63, one past the largest BIGINT. Written as a JSON NUMBER on purpose: quoting
            # it would test the reader's string handling instead, and the shape that reached
            # production was a number.
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "order_id": f"OBAD{i}",
                "customer_id": customers[0],
                "sku": skus[0],
                "qty": 1,
                "unit_price_cents": 2**63,
                "currency": CURRENCY,
            }
        elif kind == "orphan_return":
            rec = {
                "event_id": eid,
                "event_type": "return_registered",
                "event_ts": ts,
                "return_id": f"RBAD{i}",
                "order_id": "O99999999",
                "sku": skus[0],
                "qty": 1,
            }
        else:
            rec = {
                "event_id": eid,
                "event_type": "order_placed",
                "event_ts": ts,
                "order_id": f"OBAD{i}",
                "customer_id": customers[0],
                "sku": skus[0],
                "qty": 1,
                "unit_price_cents": 1000,
                "currency": "XXX",
            }
        quarantine_counts[str(reason)] += 1
        events.append((arrival, rec))

    # ---- boundary cases ---------------------------------------------------------------
    # These are not decoration. The mutation campaign showed that without them, six
    # generated mutants survived - not because the gate was weak but because the data never
    # reached the boundary they moved (a zero quantity, a free line, a return exactly on the
    # 45th day, an event arriving exactly at the close instant). A generator that never
    # produces a boundary cannot detect a mistake at that boundary, and the mutation score
    # was measuring the generator, not the pipeline. See
    # docs/adr/0006-mutants-are-generated-not-planted.md and the README note on how the score
    # moved once the boundaries existed.
    #
    # Cases 11 to 14 are the same lesson learned a second time, and the way it came back is
    # the part worth keeping. Contract 1.3.0 added rules - two bounds on the money arithmetic,
    # and which of two sales sharing a line key is the line - and this block was not extended
    # with them. Both implementations grew the rules; nothing grew the data that reaches them,
    # so fifteen mutants of those rules produced the original's numbers exactly and the
    # campaign fell to 52 of 67. A rule can therefore be correct in both lanes and untested
    # from the day it lands. The rule this block now follows: a change to the contract that
    # adds a comparison adds a case here in the same commit, or the campaign quietly stops
    # measuring the pipeline again.
    boundary_seq = 0

    def _boundary_order(sale_ts: dt.datetime, qty: int, price: int, tag: str) -> tuple[str, str]:
        nonlocal boundary_seq
        boundary_seq += 1
        order_id = f"B{boundary_seq:06d}"
        sku = skus[boundary_seq % len(skus)]
        arrival = sale_ts + dt.timedelta(minutes=5)
        events.append(
            (
                arrival,
                {
                    "event_id": f"op-{order_id}-{sku}",
                    "event_type": "order_placed",
                    "event_ts": sale_ts.isoformat(),
                    "order_id": order_id,
                    "customer_id": customers[0],
                    "sku": sku,
                    "qty": qty,
                    "unit_price_cents": price,
                    "currency": CURRENCY,
                    "boundary": tag,
                },
            )
        )
        # The branches, in the order the contract applies them - the same order as the CASE in
        # src/samegold/pipelines/transform.py and as the WHERE of the `lines` CTE in
        # gold_revenue.sql. The first that matches is the reason, so a line that is both
        # zero-quantity and out of range leaves through `non_positive_quantity`; getting that
        # order wrong here would make the ledger disagree with both implementations about a
        # record neither of them accepts.
        if qty <= 0:
            quarantine_counts[str(QuarantineReason.NON_POSITIVE_QUANTITY)] += 1
        elif price < 0:
            quarantine_counts[str(QuarantineReason.NEGATIVE_PRICE)] += 1
        elif qty > MAX_LINE_QUANTITY or price > MAX_UNIT_PRICE_CENTS:
            # The door the bounds opened in contract 1.3.0. Until boundary case 11 below
            # existed nothing walked through it from this helper, because no boundary case
            # asked for an amount anywhere near a bound.
            quarantine_counts[str(QuarantineReason.AMOUNT_OUT_OF_RANGE)] += 1
        else:
            facts[(order_id, sku)] = {
                "customer_id": customers[0],
                "qty0": qty,
                "qty": qty,
                "unit_price_cents": price,
                "sale_ts": sale_ts,
                "arrival_ts": arrival,
                # Marks this line as SCAFFOLDING. It is part of the close like any other line
                # and every witness must reproduce it; it is kept out of the business
                # projection of the ledger, because a fixture chosen to sit on a contract
                # bound describes the contract and not the shop. See Ledger.business_revenue.
                "boundary": tag,
            }
        return order_id, sku

    def _boundary_amendment(
        order_id: str,
        sku: str,
        suffix: str,
        amend_ts: dt.datetime,
        new_qty: int,
        arrival: dt.datetime,
        tag: str,
        outcome: str,
    ) -> None:
        """One amendment, and the outcome the contract gives it, written down rather than
        derived.

        ``outcome`` is a decision, not a computation. That is the whole design of this file:
        the ledger records what was INTENDED, so it is an oracle rather than a second copy of
        the rules that would agree with the pipeline by sharing its mistakes. Every caller
        says in a comment why the contract gives its event the outcome it passes.
        """
        events.append(
            (
                arrival,
                {
                    "event_id": f"am-{order_id}-{sku}-{suffix}",
                    "event_type": "order_line_amended",
                    "event_ts": amend_ts.isoformat(),
                    "order_id": order_id,
                    "sku": sku,
                    "new_qty": new_qty,
                    "boundary": tag,
                },
            )
        )
        if outcome != "accepted":
            quarantine_counts[str(outcome)] += 1
            return
        fact = facts[(order_id, sku)]
        fact["qty"] = new_qty
        fact.setdefault("amendments", []).append(
            {
                "event_id": f"am-{order_id}-{sku}-{suffix}",
                "event_ts": amend_ts,
                "arrival_ts": arrival,
                "qty": new_qty,
            }
        )

    def _boundary_return(
        order_id: str,
        sku: str,
        suffix: str,
        sale_ts: dt.datetime,
        return_ts: dt.datetime,
        qty: int,
        price: int,
        arrival: dt.datetime,
        tag: str,
        outcome: str,
    ) -> None:
        """One return, and the outcome the contract gives it. Same rule as above.

        The two kinds of refusal are NOT interchangeable and the branch below is the whole
        reason this helper exists. A return the RETURN STAGE refuses - outside the window, or
        past what the line sold - is reported per month in gold, so it belongs in
        ``rejected_returns`` and is compared against the reference by
        verify/invariants.returns_accounted_by_reason. One refused at INGEST, for a quantity
        outside the contract's bounds, never reaches that stage in either implementation, so
        counting it there would make the generator's accounting disagree with both.
        """
        events.append(
            (
                arrival,
                {
                    "event_id": f"rt-{order_id}-{sku}-{suffix}",
                    "event_type": "return_registered",
                    "event_ts": return_ts.isoformat(),
                    "return_id": f"R-{order_id}-{suffix}",
                    "order_id": order_id,
                    "sku": sku,
                    "qty": qty,
                    "reason": "size",
                    "boundary": tag,
                },
            )
        )
        if outcome == "accepted":
            returns.append(
                {
                    "order_id": order_id,
                    "sku": sku,
                    "qty": qty,
                    "unit_price_cents": price,
                    "sale_ts": sale_ts,
                    "arrival_ts": arrival,
                    "boundary": tag,
                }
            )
            return
        quarantine_counts[str(outcome)] += 1
        if outcome in (
            str(QuarantineReason.RETURN_OUTSIDE_WINDOW),
            str(QuarantineReason.RETURN_EXCEEDS_SOLD_QTY),
        ):
            rejected_returns.append(
                {"sale_ts": sale_ts, "arrival_ts": arrival, "reason": outcome, "boundary": tag}
            )

    mid = base_ts + dt.timedelta(days=max(1, profile.days // 3), hours=9)

    # 1. A line sold for zero cents: legal (a promotional gift), and it must be counted as a
    #    line even though it adds nothing to revenue. Kills the ">= 0 becomes > 0" mutant.
    _boundary_order(mid, qty=2, price=0, tag="free_line")
    # 2. A line with quantity zero: never valid, must be quarantined, must not be counted.
    _boundary_order(mid, qty=0, price=1999, tag="zero_qty")
    # 3. A return exactly on the 45th day: inside the window, by contract.
    oid, sku = _boundary_order(mid, qty=3, price=5000, tag="return_at_45d")
    r_ts = mid + dt.timedelta(days=45)
    events.append(
        (
            r_ts + dt.timedelta(minutes=5),
            {
                "event_id": f"rt-{oid}-{sku}",
                "event_type": "return_registered",
                "event_ts": r_ts.isoformat(),
                "return_id": f"R-{oid}",
                "order_id": oid,
                "sku": sku,
                "qty": 1,
                "reason": "size",
                "boundary": "return_at_45d",
            },
        )
    )
    returns.append(
        {
            "order_id": oid,
            "sku": sku,
            "qty": 1,
            "unit_price_cents": 5000,
            "sale_ts": mid,
            "arrival_ts": r_ts + dt.timedelta(minutes=5),
            "boundary": "return_at_45d",
        }
    )
    # 4. A return one microsecond past the 45th day: outside, by contract.
    oid, sku = _boundary_order(mid, qty=3, price=5000, tag="return_past_45d")
    r_ts = mid + dt.timedelta(days=45, microseconds=1)
    events.append(
        (
            r_ts + dt.timedelta(minutes=5),
            {
                "event_id": f"rt-{oid}-{sku}",
                "event_type": "return_registered",
                "event_ts": r_ts.isoformat(),
                "return_id": f"R-{oid}",
                "order_id": oid,
                "sku": sku,
                "qty": 1,
                "reason": "size",
                "boundary": "return_past_45d",
            },
        )
    )
    quarantine_counts[str(QuarantineReason.RETURN_OUTSIDE_WINDOW)] += 1
    rejected_returns.append(
        {
            "sale_ts": mid,
            "arrival_ts": r_ts + dt.timedelta(minutes=5),
            "reason": str(QuarantineReason.RETURN_OUTSIDE_WINDOW),
            "boundary": "return_past_45d",
        }
    )
    # 5. A return at the very instant of the sale: inside, by contract (>=, not >).
    oid, sku = _boundary_order(mid, qty=1, price=7700, tag="return_at_sale_instant")
    events.append(
        (
            mid + dt.timedelta(minutes=6),
            {
                "event_id": f"rt-{oid}-{sku}",
                "event_type": "return_registered",
                "event_ts": mid.isoformat(),
                "return_id": f"R-{oid}",
                "order_id": oid,
                "sku": sku,
                "qty": 1,
                "reason": "changed_mind",
                "boundary": "return_at_sale_instant",
            },
        )
    )
    returns.append(
        {
            "order_id": oid,
            "sku": sku,
            "qty": 1,
            "unit_price_cents": 7700,
            "sale_ts": mid,
            "arrival_ts": mid + dt.timedelta(minutes=6),
            "boundary": "return_at_sale_instant",
        }
    )
    # 6. A return of quantity zero: never valid.
    oid, sku = _boundary_order(mid, qty=1, price=1234, tag="zero_qty_return")
    events.append(
        (
            mid + dt.timedelta(hours=2),
            {
                "event_id": f"rt-{oid}-{sku}",
                "event_type": "return_registered",
                "event_ts": (mid + dt.timedelta(hours=1)).isoformat(),
                "return_id": f"R-{oid}",
                "order_id": oid,
                "sku": sku,
                "qty": 0,
                "reason": "size",
                "boundary": "zero_qty_return",
            },
        )
    )
    quarantine_counts[str(QuarantineReason.NON_POSITIVE_QUANTITY)] += 1
    # 7. A sale whose event arrives exactly at a close instant: the close includes it,
    #    because the as-of cut is inclusive. Kills the "<= becomes <" mutant on that cut.
    if closes:
        close0 = closes[0]
        boundary_seq += 1
        oid, sku = f"B{boundary_seq:06d}", skus[boundary_seq % len(skus)]
        sale_ts = close0 - dt.timedelta(days=2)
        events.append(
            (
                close0,
                {
                    "event_id": f"op-{oid}-{sku}",
                    "event_type": "order_placed",
                    "event_ts": sale_ts.isoformat(),
                    "order_id": oid,
                    "customer_id": customers[0],
                    "sku": sku,
                    "qty": 1,
                    "unit_price_cents": 9999,
                    "currency": CURRENCY,
                    "boundary": "arrives_at_close_instant",
                },
            )
        )
        facts[(oid, sku)] = {
            "customer_id": customers[0],
            "qty0": 1,
            "qty": 1,
            "unit_price_cents": 9999,
            "sale_ts": sale_ts,
            "arrival_ts": close0,
            "boundary": "arrives_at_close_instant",
        }

        # 8. A sale that HAPPENED before a close but ARRIVED after it. This is the only
        #    shape of data that can tell an as-of cut on arrival time apart from one on
        #    event time, and without it specification mutant SPEC-04 survives every witness
        #    - which is exactly what happened before this case existed. It is also the
        #    shape that creates a restatement, so it is not an artificial case: it is the
        #    reason the whole bitemporal model is there.
        boundary_seq += 1
        oid, sku = f"B{boundary_seq:06d}", skus[boundary_seq % len(skus)]
        sale_ts = close0 - dt.timedelta(days=1)
        events.append(
            (
                close0 + dt.timedelta(hours=1),
                {
                    "event_id": f"op-{oid}-{sku}",
                    "event_type": "order_placed",
                    "event_ts": sale_ts.isoformat(),
                    "order_id": oid,
                    "customer_id": customers[0],
                    "sku": sku,
                    "qty": 4,
                    "unit_price_cents": 12345,
                    "currency": CURRENCY,
                    "boundary": "arrives_after_close",
                },
            )
        )
        facts[(oid, sku)] = {
            "customer_id": customers[0],
            "qty0": 4,
            "qty": 4,
            "unit_price_cents": 12345,
            "sale_ts": sale_ts,
            "arrival_ts": close0 + dt.timedelta(hours=1),
            "boundary": "arrives_after_close",
        }

        # 9. An AMENDMENT that arrives after a close and changes a quantity that close had
        #    already reported. This is the only shape that can tell "the quantity known at
        #    the close" apart from "the final quantity", and without it specification mutant
        #    SPEC-06 survives at the small profile while dying at the large one - a mutation
        #    score that depends on how much data you happened to generate is a score that
        #    measures the data.
        #
        #    The comment used to say "after the close OF THE MONTH ITS LINE BELONGS TO",
        #    which is not what the arithmetic below does: the sale is three days before the
        #    first close and therefore in the month BEFORE it, whose own close is a month
        #    later. The case works - the quantity differs between close 0 and close 1, which
        #    is what SPEC-06 needs - and the sentence explaining it described a different
        #    case. A comment that is nearly right about a boundary case is the reason the
        #    next reader trusts the next one.
        boundary_seq += 1
        oid, sku = f"B{boundary_seq:06d}", skus[boundary_seq % len(skus)]
        sale_ts = close0 - dt.timedelta(days=3)
        events.append(
            (
                sale_ts + dt.timedelta(minutes=5),
                {
                    "event_id": f"op-{oid}-{sku}",
                    "event_type": "order_placed",
                    "event_ts": sale_ts.isoformat(),
                    "order_id": oid,
                    "customer_id": customers[0],
                    "sku": sku,
                    "qty": 2,
                    "unit_price_cents": 20000,
                    "currency": CURRENCY,
                    "boundary": "amendment_after_close",
                },
            )
        )
        amend_ts = sale_ts + dt.timedelta(hours=6)
        events.append(
            (
                close0 + dt.timedelta(hours=2),
                {
                    "event_id": f"am-{oid}-{sku}",
                    "event_type": "order_line_amended",
                    "event_ts": amend_ts.isoformat(),
                    "order_id": oid,
                    "sku": sku,
                    "new_qty": 7,
                    "boundary": "amendment_after_close",
                },
            )
        )
        facts[(oid, sku)] = {
            "customer_id": customers[0],
            "qty0": 2,
            "qty": 7,
            "unit_price_cents": 20000,
            "sale_ts": sale_ts,
            "arrival_ts": sale_ts + dt.timedelta(minutes=5),
            "boundary": "amendment_after_close",
            "amendments": [
                {
                    # The id the EVENT carries, not a suffixed one. The ledger said
                    # "am-...-0" while the event written said "am-...", which is harmless
                    # only because this line has a single amendment and nothing ties on it:
                    # a ledger that records a different key from the data it describes is one
                    # tie away from being a wrong answer nobody can explain.
                    "event_id": f"am-{oid}-{sku}",
                    "event_ts": amend_ts,
                    "arrival_ts": close0 + dt.timedelta(hours=2),
                    "qty": 7,
                }
            ],
        }

        # 10. Two amendments for the same line at the SAME event time, with different
        #     quantities. Only the tie-break on event_id decides which one wins, and until
        #     this case existed a mutant that flipped exactly that tie-break survived the
        #     whole campaign. A tie-break nothing exercises is a coin toss with a comment.
        boundary_seq += 1
        oid, sku = f"B{boundary_seq:06d}", skus[boundary_seq % len(skus)]
        sale_ts = base_ts + dt.timedelta(days=1, hours=10)
        tie_ts = sale_ts + dt.timedelta(hours=6)
        events.append(
            (
                sale_ts + dt.timedelta(minutes=3),
                {
                    "event_id": f"op-{oid}-{sku}",
                    "event_type": "order_placed",
                    "event_ts": sale_ts.isoformat(),
                    "order_id": oid,
                    "customer_id": customers[0],
                    "sku": sku,
                    "qty": 1,
                    "unit_price_cents": 30000,
                    "currency": CURRENCY,
                    "boundary": "amendment_tie",
                },
            )
        )
        for suffix, tie_qty in (("a", 3), ("b", 9)):
            events.append(
                (
                    tie_ts + dt.timedelta(minutes=4),
                    {
                        "event_id": f"am-{oid}-{sku}-{suffix}",
                        "event_type": "order_line_amended",
                        "event_ts": tie_ts.isoformat(),
                        "order_id": oid,
                        "sku": sku,
                        "new_qty": tie_qty,
                        "boundary": "amendment_tie",
                    },
                )
            )
        # The contract says the last amendment wins, ordered by event time and then by event
        # id descending, so "am-...-b" (9 units) is the effective quantity.
        facts[(oid, sku)] = {
            "customer_id": customers[0],
            "qty0": 1,
            "qty": 9,
            "unit_price_cents": 30000,
            "sale_ts": sale_ts,
            "arrival_ts": sale_ts + dt.timedelta(minutes=3),
            "boundary": "amendment_tie",
            "amendments": [
                {
                    "event_id": f"am-{oid}-{sku}-a",
                    "event_ts": tie_ts,
                    "arrival_ts": tie_ts + dt.timedelta(minutes=4),
                    "qty": 3,
                },
                {
                    "event_id": f"am-{oid}-{sku}-b",
                    "event_ts": tie_ts,
                    "arrival_ts": tie_ts + dt.timedelta(minutes=4),
                    "qty": 9,
                },
            ],
        }

    # 11. The money bounds, exactly ON them and exactly one past them, at all four places the
    #     contract applies them: a line's quantity, a line's unit price, an amendment's new
    #     quantity and a return's quantity. Eight mutants lived here - SQL-041, 047, 048, 052,
    #     059, 067, 068 and 072 - four of which move a bound by one and four of which turn its
    #     `<=` into `<`. Every one of them survived the whole campaign.
    #
    #     Not because the witnesses are weak: because no record ever came near a bound. The
    #     bounds joined the contract at version 1.3.0 and no data joined with them, so the
    #     generator kept emitting what it always had - quantities of one to four, prices under
    #     250 euros - plus the single deliberately absurd 2^63-1 in the corrupt block, which is
    #     so far past both bounds that moving either by one cannot change how it is classified.
    #     A bound that no record sits on is a bound whose exact position is unobservable, and
    #     eight mutants said so.
    #
    #     The constants are IMPORTED from domain/contract.py, not written out here. If a bound
    #     ever moves, the data that tests it moves in the same commit; spelling the number in
    #     would make this block agree with a bound it had copied rather than with the bound the
    #     two implementations enforce. That mattered one round later: the bounds moved from ten
    #     million units and a hundred million euros a unit to ten thousand units and ten
    #     thousand euros a unit, and these four calls followed without being touched.
    #
    #     The size of a bound is the size of the fixture that tests it, and that is not a
    #     detail of this block: it is why the bounds moved. A line at a hundred million euros
    #     is a legitimate boundary case and was also a hundred and sixty-eight times the
    #     simulated business of the month it landed in, which moved a published business
    #     figure. The fixture below is the largest legal line the contract admits, and it has
    #     to be, so the contract had to be a number the close can carry.

    # A line AT the price bound is legal and must be counted; one cent past it is
    # amount_out_of_range. Without the first, `unit_price_cents <= MAX` and
    # `unit_price_cents < MAX` classify every row identically (SQL-041 survives); without the
    # second, `<= MAX` and `<= MAX + 1` do (SQL-047 survives).
    _boundary_order(mid, qty=1, price=MAX_UNIT_PRICE_CENTS, tag="price_at_bound")
    _boundary_order(mid, qty=1, price=MAX_UNIT_PRICE_CENTS + 1, tag="price_past_bound")
    # The same pair for the quantity bound (SQL-059 and SQL-067), priced at ONE CENT: the case
    # has to reach the bound, not dominate the close it is measured in. The price bound above
    # gets no such discount - a line at the price bound costs the price bound, whatever
    # quantity it carries - which is the whole reason the bound itself had to be a number a
    # retail close can absorb.
    bound_oid, bound_sku = _boundary_order(mid, qty=MAX_LINE_QUANTITY, price=1, tag="qty_at_bound")
    _boundary_order(mid, qty=MAX_LINE_QUANTITY + 1, price=1, tag="qty_past_bound")
    # A return AT the quantity bound, against the line that sold exactly that many units, so
    # it is accepted and the whole line comes back: without it `d.qty <= MAX` and
    # `d.qty < MAX` agree on every return ever generated (SQL-048 survives). And one unit past
    # the bound, which leaves at INGEST through amount_out_of_range and never reaches the
    # return stage at all - so under `d.qty <= MAX + 1` the reference admits a return the
    # contract refuses and the month's returns_rejected_count moves (SQL-052 survives without
    # it). Both arrive at one instant: they sit on one line, and a close that saw only one of
    # them would apply the cumulative rule to a different set of returns than the one the
    # ledger recorded.
    bound_return_arrival = mid + dt.timedelta(days=11, minutes=5)
    _boundary_return(
        bound_oid,
        bound_sku,
        "atbound",
        sale_ts=mid,
        return_ts=mid + dt.timedelta(days=10),
        qty=MAX_LINE_QUANTITY,
        price=1,
        arrival=bound_return_arrival,
        tag="return_qty_at_bound",
        outcome="accepted",
    )
    _boundary_return(
        bound_oid,
        bound_sku,
        "pastbound",
        sale_ts=mid,
        return_ts=mid + dt.timedelta(days=11),
        qty=MAX_LINE_QUANTITY + 1,
        price=1,
        arrival=bound_return_arrival,
        tag="return_qty_past_bound",
        outcome=str(QuarantineReason.AMOUNT_OUT_OF_RANGE),
    )
    # And the same pair for an amendment's new quantity (SQL-068 and SQL-072). An amendment
    # carries `new_qty`, which is a different column under a different branch of the
    # classification, so the two rows the line needed do not test it: it needs its own two.
    amend_oid, amend_sku = _boundary_order(mid, qty=2, price=1, tag="amend_qty_at_bound")
    _boundary_amendment(
        amend_oid,
        amend_sku,
        "atbound",
        amend_ts=mid + dt.timedelta(hours=2),
        new_qty=MAX_LINE_QUANTITY,
        arrival=mid + dt.timedelta(hours=2, minutes=5),
        tag="amend_qty_at_bound",
        outcome="accepted",
    )
    amend_oid, amend_sku = _boundary_order(mid, qty=3, price=100, tag="amend_qty_past_bound")
    _boundary_amendment(
        amend_oid,
        amend_sku,
        "pastbound",
        amend_ts=mid + dt.timedelta(hours=2),
        new_qty=MAX_LINE_QUANTITY + 1,
        arrival=mid + dt.timedelta(hours=2, minutes=5),
        tag="amend_qty_past_bound",
        outcome=str(QuarantineReason.AMOUNT_OUT_OF_RANGE),
    )

    # 12. Two order_placed events sharing one (order_id, sku). The key of a sale is its
    #     event_id, so this is contract-legal, and gold keeps exactly ONE of them: the first
    #     by (sale_ts, event_id). SQL-042 and SQL-043 flip the two halves of that order and
    #     both survived, because no seed has ever produced the shape - `rng.sample` draws each
    #     sku at most once per order, so every partition of that window held a single row, and
    #     ranking one row is the same job in any order.
    #
    #     TWO pairs, and the second is not belt and braces. The order has two keys and one
    #     pair can only exercise one of them: with different sale timestamps the event_id half
    #     is never consulted, so flipping it changes nothing and SQL-043 lives; with equal
    #     timestamps the sale_ts half decides nothing, so flipping THAT changes nothing and
    #     SQL-042 lives. It is the lesson of boundary case 10 one level up - a tie-break is
    #     only tested by a tie.
    #
    #     Both copies of a pair arrive at ONE instant on purpose. A close that saw only the
    #     loser would elect the loser, correctly, while the ledger recorded the winner: the
    #     generator would then be describing a close that never happened, and the failure
    #     would read as a pipeline bug.
    for tag, second_hours in (
        # Different sale instants: the earlier sale is the line. Kills SQL-042.
        ("duplicate_line_key_by_time", 4),
        # The same sale instant, so only the event_id decides. Kills SQL-043.
        ("duplicate_line_key_by_event_id", 0),
    ):
        boundary_seq += 1
        oid, sku = f"B{boundary_seq:06d}", skus[boundary_seq % len(skus)]
        pair_arrival = mid + dt.timedelta(minutes=5)
        # Named rather than positional, and the ledger below reads the SAME tuple the event
        # does. `winner` earns the name in both pairs: it is the earlier sale in the first and
        # the lower event_id in the second, which is what (sale_ts, event_id) ascending means.
        # Boundary case 9 records what it costs when a ledger repeats a value instead of
        # sharing it - the two agree until someone edits one of them.
        winner = ("a", 0, 2, 1100)
        loser = ("b", second_hours, 5, 700)
        for suffix, hours, pair_qty, pair_price in (winner, loser):
            events.append(
                (
                    pair_arrival,
                    {
                        "event_id": f"op-{oid}-{sku}-{suffix}",
                        "event_type": "order_placed",
                        "event_ts": (mid + dt.timedelta(hours=hours)).isoformat(),
                        "order_id": oid,
                        "customer_id": customers[0],
                        "sku": sku,
                        "qty": pair_qty,
                        "unit_price_cents": pair_price,
                        "currency": CURRENCY,
                        "boundary": tag,
                    },
                )
            )
        # The winner, and only the winner. The loser is not quarantined either: no reason in
        # the closed enum covers a duplicated line key, both implementations simply rank it
        # second and drop it, and inventing a counter here would make the ledger disagree with
        # both. The quantities and the prices differ so that electing the other one is visible
        # in cents and not only in a row count.
        facts[(oid, sku)] = {
            "customer_id": customers[0],
            "qty0": winner[2],
            "qty": winner[2],
            "unit_price_cents": winner[3],
            "sale_ts": mid + dt.timedelta(hours=winner[1]),
            "arrival_ts": pair_arrival,
            "boundary": tag,
        }

    # 13. One line, three returns, and the cumulative window that decides which of them takes
    #     units off it. Three mutants of that window survived - SQL-055 turns its SUM into a
    #     MAX, SQL-064 and SQL-065 flip the two halves of its ORDER BY - and a fourth,
    #     SQL-069, turns the window's `ELSE 0` into `ELSE 1`, which lets a REFUSED return
    #     consume a unit of the line. All four were invisible for one reason: no line had ever
    #     carried more than one return. With a single row per partition a running SUM and a
    #     running MAX are the same number, every ordering of one row is the same ordering, and
    #     there is no refused return for the `ELSE` to give anything to.
    #
    #     The line sells 3 units at 5 000 cents and takes:
    #       * one return dated BEFORE the sale, refused as return_outside_window. It takes
    #         nothing - that is what the `ELSE 0` says - and SQL-069, which gives it one unit,
    #         then pushes the return below past the quantity sold and refuses a refund the
    #         contract owes;
    #       * one return of 3 units inside the window, which fits EXACTLY. Exactly matters: at
    #         2 units the extra unit SQL-069 invents would still fit, and a case written to
    #         kill that mutant would not kill it;
    #       * one return of 2 units after it, which no longer fits and leaves as
    #         return_exceeds_sold_qty even though 2 units on their own would have fitted.
    #     Read in the other direction (SQL-064) the 2 is accepted and the 3 refused; taking
    #     their MAX instead of their SUM (SQL-055) accepts both. Both move the month's
    #     returns_cents, which is the number a reader of gold would have watched move.
    cumulative_oid, cumulative_sku = _boundary_order(
        mid, qty=3, price=5000, tag="return_cumulative_window"
    )
    # One arrival for all three, for the reason given in case 11: the cumulative rule is a
    # property of the SET of returns that has arrived, so a close that splits the set is a
    # close the ledger's single classification cannot describe.
    cumulative_arrival = mid + dt.timedelta(days=2, minutes=5)
    for suffix, offset_days, return_qty, outcome in (
        ("before", -1, 1, str(QuarantineReason.RETURN_OUTSIDE_WINDOW)),
        ("fits", 1, 3, "accepted"),
        ("overflows", 2, 2, str(QuarantineReason.RETURN_EXCEEDS_SOLD_QTY)),
    ):
        _boundary_return(
            cumulative_oid,
            cumulative_sku,
            suffix,
            sale_ts=mid,
            return_ts=mid + dt.timedelta(days=offset_days),
            qty=return_qty,
            price=5000,
            arrival=cumulative_arrival,
            tag="return_cumulative_window",
            outcome=outcome,
        )
    # And the tie the three returns above cannot produce: two returns of one line at the SAME
    # instant, where only `return_event_id` decides which of them takes the units. The three
    # above have distinct return timestamps, so the second half of that ORDER BY is never
    # consulted and SQL-065 survives them exactly as SQL-043 survives a pair of sales at
    # different times. The line sells 3; the first by event_id takes 1 and is accepted, the
    # second wants 3 and is refused. Read the other way the 3 is accepted and the 1 refused,
    # which is 12 000 cents of refund instead of 4 000.
    tie_oid, tie_sku = _boundary_order(mid, qty=3, price=4000, tag="return_tie_on_instant")
    tie_return_ts = mid + dt.timedelta(days=3)
    for suffix, return_qty, outcome in (
        ("a", 1, "accepted"),
        ("b", 3, str(QuarantineReason.RETURN_EXCEEDS_SOLD_QTY)),
    ):
        _boundary_return(
            tie_oid,
            tie_sku,
            suffix,
            sale_ts=mid,
            return_ts=tie_return_ts,
            qty=return_qty,
            price=4000,
            arrival=tie_return_ts + dt.timedelta(minutes=5),
            tag="return_tie_on_instant",
            outcome=outcome,
        )

    # 14. An amendment to a quantity of ZERO, which is non_positive_quantity: the contract
    #     admits an amendment that changes how many units were sold, not one that unsells the
    #     line. SQL-071 turns the amendments filter's `new_qty > 0` into `new_qty >= 0` and
    #     survived because no amendment has ever been zero - the random path builds each new
    #     quantity with `max(1, ...)`, and the corrupt block's non-positive quantity is an
    #     order_placed carrying `qty`, not an amendment carrying `new_qty`. The mutant widened
    #     a door nobody was standing at.
    #
    #     With this case the difference is a booked figure: the amendment is refused, the line
    #     keeps the 4 units it was sold with, and under the mutant it wins its window and the
    #     COALESCE takes the zero - 10 000 cents of gross the close simply stops reporting,
    #     with the line still counted. It is the same failure an amendment to -5 once caused
    #     in the other direction, which is why the branch exists at all.
    zero_oid, zero_sku = _boundary_order(mid, qty=4, price=2500, tag="amendment_to_zero")
    _boundary_amendment(
        zero_oid,
        zero_sku,
        "zero",
        amend_ts=mid + dt.timedelta(hours=3),
        new_qty=0,
        arrival=mid + dt.timedelta(hours=3, minutes=5),
        tag="amendment_to_zero",
        outcome=str(QuarantineReason.NON_POSITIVE_QUANTITY),
    )

    # ---- the ledger ------------------------------------------------------------------
    # Two projections, ONE pass and one piece of arithmetic. "all" is the close: every
    # accepted line and every accepted return, boundary fixtures included, and it is what
    # SG-01, SG-02, SG-03 and SG-05 compare the implementations against, because a fixture the
    # pipeline drops or miscounts is a bug like any other. "business" drops the fixtures, and
    # SG-04 - the one claim that publishes a percentage of a month's revenue as a business
    # statement - reads that one instead. See Ledger.business_revenue for what it cost to
    # learn that a fixture belongs in the close and not in a sentence about the shop.
    #
    # Accumulated together rather than by running this loop twice over a filtered input: two
    # passes would be two copies of "the quantity known at this close", and the whole argument
    # for the ledger is that it records intent once instead of recomputing it.
    counters = ("gross", "refunds", "line_count", "return_count", "rejected_count")
    for close in closes:
        totals: dict[str, dict[str, dict[str, int]]] = {
            scope: {name: defaultdict(int) for name in counters} for scope in ("all", "business")
        }
        for (_oid, _sku), f in facts.items():
            if f["arrival_ts"] > close:
                continue  # the sale itself is not known yet at this close
            # The quantity reported at a close is the one known at that close: if the
            # amendment has not arrived, the original quantity is what finance saw, and the
            # late amendment becomes a restatement. Using the final quantity here would
            # quietly assume perfect foresight and would hide half of the restatements.
            # The quantity known at this close: the latest amendment (by event time) among
            # those that had ARRIVED by the close. An amendment still in flight has not
            # happened as far as finance is concerned, and it becomes a restatement later.
            arrived_amendments = [a for a in f.get("amendments", []) if a["arrival_ts"] <= close]
            # Latest by (event_ts, event_id). The event_id half is not decoration: two
            # amendments can share an event time, and without a deterministic tie-break the
            # answer depends on the order rows happen to be read in. A mutation that flipped
            # exactly this tie-break survived the campaign until the generator started
            # producing the case (boundary case 10).
            qty = (
                int(max(arrived_amendments, key=lambda a: (a["event_ts"], a["event_id"]))["qty"])
                if arrived_amendments
                else int(f["qty0"])
            )
            month = accounting_month(f["sale_ts"])
            for scope in _scopes(f):
                totals[scope]["gross"][month] += qty * int(f["unit_price_cents"])
                totals[scope]["line_count"][month] += 1
        for r in returns:
            if r["arrival_ts"] <= close:
                month = accounting_month(r["sale_ts"])
                for scope in _scopes(r):
                    totals[scope]["refunds"][month] += int(r["qty"]) * int(r["unit_price_cents"])
                    totals[scope]["return_count"][month] += 1
        for r in rejected_returns:
            if r["arrival_ts"] <= close:
                month = accounting_month(r["sale_ts"])
                for scope in _scopes(r):
                    totals[scope]["rejected_count"][month] += 1
        for scope, target in (("all", ledger.revenue), ("business", ledger.business_revenue)):
            t = totals[scope]
            for month in sorted(set(t["gross"]) | set(t["refunds"])):
                g, rr = t["gross"][month], t["refunds"][month]
                target[(month, close.isoformat())] = {
                    "gross_cents": g,
                    "returns_cents": rr,
                    "net_cents": g - rr,
                    "line_count": t["line_count"][month],
                    "return_count": t["return_count"][month],
                    "returns_rejected_count": t["rejected_count"][month],
                }

    # ---- write files ------------------------------------------------------------------
    events.sort(key=lambda e: (e[0], json.dumps(e[1], sort_keys=True)))
    files: list[Path] = []
    bucket = profile.batch_minutes * 60
    current_key: int | None = None
    handle = None
    written = 0
    # Counted at WRITE time, over the lines that actually reach the files, because that is
    # the quantity the reference can recount independently. `len(originals)` looked like the
    # same number and is not: it is taken before the corrupt records are appended, so it
    # under-counts every parseable-but-invalid record by construction. The cross-check in
    # verify/invariants.conservation_against_ledger found the gap the first time it ran,
    # which is the entire argument for making an invariant compare two derivations rather
    # than one derivation with itself.
    parseable_event_ids: set[str] = set()
    duplicate_lines = 0
    unparseable_lines = 0
    # Counted the same way and for the same reason as the three above: at write time, over the
    # lines that actually reach the files, so that the reference can recount it from the bytes
    # and the two derivations are independent. A value outside BIGINT is dropped by every
    # reader in this project into a NULL column, which is the one fault shape that leaves no
    # trace of itself in the record - so the count is the trace.
    beyond_bigint_lines = 0
    try:
        for arrival, rec in events:
            key = int(arrival.timestamp()) // bucket
            if key != current_key:
                if handle is not None:
                    handle.close()
                current_key = key
                stamp = dt.datetime.fromtimestamp(key * bucket, tz=dt.UTC).strftime("%Y%m%d%H%M")
                path = out_dir / "bronze" / f"batch={stamp}" / "part-00000.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = path.open("w", encoding="utf-8")
                files.append(path)
            raw = rec.pop("__raw__", None)
            line = (
                raw if raw is not None else json.dumps({**rec, "arrival_ts": arrival.isoformat()})
            )
            assert handle is not None
            handle.write(line + "\n")
            written += 1
            # `unparseable_json` in the contract covers two shapes: a line that is not JSON,
            # and a line that carries no event_id. The reference counts both through that one
            # door and so does the Spark pipeline; the ledger counted only the first, so a
            # well-formed record with no event_id made the cross-check disagree by one.
            if raw is not None or rec.get("event_id") is None:
                unparseable_lines += 1
                continue
            if any(
                isinstance(rec.get(column), int)
                and not isinstance(rec.get(column), bool)
                and not (-(2**63) <= int(rec[column]) <= 2**63 - 1)
                for column in ("qty", "new_qty", "unit_price_cents")
            ):
                beyond_bigint_lines += 1
            event_id = str(rec["event_id"])
            if event_id in parseable_event_ids:
                duplicate_lines += 1
            else:
                parseable_event_ids.add(event_id)
    finally:
        if handle is not None:
            handle.close()

    ledger.quarantine = dict(sorted(quarantine_counts.items()))
    ledger.counts = {
        "events_written": written,
        "unique_events": len(parseable_event_ids),
        "duplicates": duplicate_lines,
        # The two figures the noise generator INTENDED, kept beside the two that were
        # actually written. They differ, and the difference is informative: a duplicate is
        # drawn with replacement, so drawing the same original twice writes three copies of
        # one event rather than two copies of two.
        "unparseable_lines": unparseable_lines,
        "values_beyond_bigint": beyond_bigint_lines,
        "duplicates_planned": n_dup,
        "unique_originals": len(originals),
        "duplicates_late": duplicates_late,
        "corrupt": n_corrupt,
        "order_lines": len(facts),
        "valid_returns": len(returns),
        "files": len(files),
    }
    (out_dir / "truth").mkdir(parents=True, exist_ok=True)
    (out_dir / "truth" / "ledger.json").write_text(
        json.dumps(ledger.to_json(), indent=2, sort_keys=True), encoding="utf-8"
    )
    (out_dir / "truth" / "profile.json").write_text(
        json.dumps(
            {**asdict(profile), "start_date": profile.start_date.isoformat(), "seed": seed},
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    return GenerationResult(ledger=ledger, files=files, profile=profile, seed=seed)
