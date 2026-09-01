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

from samegold.domain.contract import ACCOUNTING_TIMEZONE, CURRENCY, QuarantineReason
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
        versions.sort(key=lambda v: str(v["valid_from"]))
        # Collapse versions that share a valid_from (the pipeline keeps the last one).
        collapsed: list[dict[str, Any]] = []
        for v in versions:
            if collapsed and collapsed[-1]["valid_from"] == v["valid_from"]:
                collapsed[-1] = v
            else:
                collapsed.append(v)
        ledger.dim_customer[cid] = collapsed

    # ---- orders, amendments, returns -------------------------------------------------
    # facts[(order_id, sku)] = dict with qty, price, sale_ts, arrival_ts of the sale
    facts: dict[tuple[str, str], dict[str, Any]] = {}
    returns: list[dict[str, Any]] = []
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

                # amendment: changes the effective quantity before shipment
                if rng.random() < profile.amend_rate:
                    new_qty = max(1, qty + rng.choice([-1, 1, 2]))
                    amend_ts = sale_ts + dt.timedelta(hours=rng.randrange(1, 48))
                    amend_arrival = amend_ts + _delay(rng, profile)
                    events.append(
                        (
                            amend_arrival,
                            {
                                "event_id": f"am-{order_id}-{sku}",
                                "event_type": "order_line_amended",
                                "event_ts": amend_ts.isoformat(),
                                "order_id": order_id,
                                "sku": sku,
                                "new_qty": new_qty,
                            },
                        )
                    )
                    facts[(order_id, sku)]["qty"] = new_qty
                    facts[(order_id, sku)]["amend_arrival"] = amend_arrival

                # return: the interesting one
                if rng.random() < profile.return_rate:
                    if rng.random() < profile.late_return_share:
                        offset_days = rng.randrange(30, 61)  # some fall outside the window
                    else:
                        offset_days = rng.randrange(5, 30)
                    return_ts = sale_ts + dt.timedelta(days=offset_days, hours=rng.randrange(0, 24))
                    r_arrival = return_ts + _delay(rng, profile)
                    eff_qty = int(facts[(order_id, sku)]["qty"])
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
    # was measuring the generator, not the pipeline. See docs/adr/0006 and the README note
    # on how the score went from 0.71 to its published value.
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
        if qty > 0 and price >= 0:
            facts[(order_id, sku)] = {
                "customer_id": customers[0],
                "qty0": qty,
                "qty": qty,
                "unit_price_cents": price,
                "sale_ts": sale_ts,
                "arrival_ts": arrival,
            }
        else:
            quarantine_counts[
                str(
                    QuarantineReason.NON_POSITIVE_QUANTITY
                    if qty <= 0
                    else QuarantineReason.NEGATIVE_PRICE
                )
            ] += 1
        return order_id, sku

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
        }

        # 9. An AMENDMENT that arrives after the close of the month its line belongs to. This
        #    is the only shape that can tell "the quantity known at the close" apart from
        #    "the final quantity", and without it specification mutant SPEC-06 survives at
        #    the small profile while dying at the large one - a mutation score that depends
        #    on how much data you happened to generate is a score that measures the data.
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
            "amend_arrival": close0 + dt.timedelta(hours=2),
        }

    # ---- the ledger ------------------------------------------------------------------
    for close in closes:
        gross: dict[str, int] = defaultdict(int)
        refunds: dict[str, int] = defaultdict(int)
        line_count: dict[str, int] = defaultdict(int)
        return_count: dict[str, int] = defaultdict(int)
        for (_oid, _sku), f in facts.items():
            if f["arrival_ts"] > close:
                continue  # the sale itself is not known yet at this close
            # The quantity reported at a close is the one known at that close: if the
            # amendment has not arrived, the original quantity is what finance saw, and the
            # late amendment becomes a restatement. Using the final quantity here would
            # quietly assume perfect foresight and would hide half of the restatements.
            known_amendment: dt.datetime | None = f.get("amend_arrival")
            qty = (
                int(f["qty"])
                if known_amendment is not None and known_amendment <= close
                else int(f["qty0"])
            )
            month = accounting_month(f["sale_ts"])
            gross[month] += qty * int(f["unit_price_cents"])
            line_count[month] += 1
        for r in returns:
            if r["arrival_ts"] <= close:
                month = accounting_month(r["sale_ts"])
                refunds[month] += int(r["qty"]) * int(r["unit_price_cents"])
                return_count[month] += 1
        for month in sorted(set(gross) | set(refunds)):
            g, rr = gross[month], refunds[month]
            ledger.revenue[(month, close.isoformat())] = {
                "gross_cents": g,
                "returns_cents": rr,
                "net_cents": g - rr,
                "line_count": line_count[month],
                "return_count": return_count[month],
            }

    # ---- write files ------------------------------------------------------------------
    events.sort(key=lambda e: (e[0], json.dumps(e[1], sort_keys=True)))
    files: list[Path] = []
    bucket = profile.batch_minutes * 60
    current_key: int | None = None
    handle = None
    written = 0
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
    finally:
        if handle is not None:
            handle.close()

    ledger.quarantine = dict(sorted(quarantine_counts.items()))
    ledger.counts = {
        "events_written": written,
        "unique_events": len(originals),
        "duplicates": n_dup,
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
