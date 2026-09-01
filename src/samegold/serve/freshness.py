"""Freshness against the SLA in the contract, and the alert rule that goes with it.

Section 5 of the exam guide asks for SQL alerts on data quality and for job notifications.
Neither is demonstrable on Free Edition without a workspace, so the RULE lives here where it
can be executed and tested, and the Databricks lane wires the same thresholds into an alert.

The interesting part is what counts as late. Two things are measured separately because they
fail for different reasons and get fixed by different people:

  * INGESTION lag: the newest event that has arrived, against the wall clock. Late means the
    producer stopped or the pipeline stopped, and the answer is a page.
  * CLOSE lag: a month whose close is overdue. Late means a scheduled job did not run, and the
    answer is a job repair, not a page at three in the morning.

Two bugs an adversarial review found here, both of the "only ever tested the happy month"
kind:

  * the rule derived exactly ONE month key, the one before ``now``, so a backlog reported the
    most recent gap and silently forgot every older one. With January closed and April
    running, February's missing close was unreportable by any call. It now walks back through
    every month whose deadline has passed.
  * the deadline was ``datetime(now.year, now.month, close_day)`` in NOW'S timezone. The
    close is an accounting artefact and falls at the end of the close day in the ACCOUNTING
    timezone, so a UTC ``now`` overstated the lag by about 22 hours, and between 22:00Z and
    midnight on the last day of a Madrid month the arithmetic picked the wrong month
    entirely.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from samegold.domain.contract import ACCOUNTING_TIMEZONE, FRESHNESS_SLA


@dataclass(frozen=True, slots=True)
class FreshnessBreach:
    kind: str
    detail: str
    lag_seconds: float
    threshold_seconds: float

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "detail": self.detail,
            "lag_seconds": round(self.lag_seconds, 1),
            "threshold_seconds": round(self.threshold_seconds, 1),
        }


def evaluate_freshness(
    newest_arrival: dt.datetime | None,
    closed_months: Sequence[str],
    now: dt.datetime,
    close_day: int = 5,
    sla: dt.timedelta = FRESHNESS_SLA,
) -> list[FreshnessBreach]:
    """Return the breaches. An empty list is the only healthy answer.

    ``closed_months`` are the months that already have a close recorded. A month is overdue
    once the close day of the following month has passed without one.
    """
    breaches: list[FreshnessBreach] = []

    if newest_arrival is None:
        breaches.append(
            FreshnessBreach(
                "no_data",
                "no event has ever arrived: the pipeline has never run, which is a different "
                "problem from a stale one and should not be reported as freshness",
                lag_seconds=float("inf"),
                threshold_seconds=sla.total_seconds(),
            )
        )
        return breaches

    lag = (now - newest_arrival).total_seconds()
    if lag > sla.total_seconds():
        breaches.append(
            FreshnessBreach(
                "ingestion_lag",
                f"the newest event arrived at {newest_arrival.isoformat()}, which is "
                f"{lag / 60:.0f} minutes ago against an SLA of "
                f"{sla.total_seconds() / 60:.0f} minutes",
                lag_seconds=lag,
                threshold_seconds=sla.total_seconds(),
            )
        )

    recorded = set(closed_months)
    for month_key, deadline in overdue_months(now, closed_months, close_day):
        if month_key in recorded:  # pragma: no cover - overdue_months already filters
            continue
        breaches.append(
            FreshnessBreach(
                "close_overdue",
                f"{month_key} has no close and its deadline "
                f"({deadline.date().isoformat()}) has passed: a scheduled job did not run, "
                f"which is a repair rather than a page",
                lag_seconds=(now - deadline).total_seconds(),
                threshold_seconds=0.0,
            )
        )
    return breaches


def close_deadline(month_key: str, close_day: int) -> dt.datetime:
    """The instant by which ``month_key`` must be closed, in the accounting timezone.

    End of the close day of the FOLLOWING month, local to the entity that signs the close.
    Computing it in the caller's timezone (usually UTC) moved the deadline by the Madrid
    offset and overstated every lag by an hour or two.
    """
    zone = ZoneInfo(ACCOUNTING_TIMEZONE)
    year, month = (int(part) for part in month_key.split("-"))
    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    start_of_next_day = dt.datetime(year, month, close_day, tzinfo=zone) + dt.timedelta(days=1)
    return start_of_next_day.astimezone(dt.UTC)


def overdue_months(
    now: dt.datetime, closed_months: Sequence[str], close_day: int = 5, horizon: int = 36
) -> list[tuple[str, dt.datetime]]:
    """Every month whose close deadline has passed and which has no close, oldest first.

    The window is bounded from below by the OLDEST close on record, because that is the only
    evidence available that the pipeline was supposed to be running then: reporting every
    month back to the epoch on a fresh deployment is noise, and noise is how an alert stops
    being read. With no close on record at all there is no history to define a backlog
    against, so only the most recent overdue month is reported.

    ``horizon`` is a hard stop on how far back the walk goes, so a single very old close
    cannot turn one missing job into hundreds of alerts.
    """
    recorded = set(closed_months)
    floor = min(recorded) if recorded else None
    zone = ZoneInfo(ACCOUNTING_TIMEZONE)
    local = now.astimezone(zone)
    year, month = local.year, local.month
    out: list[tuple[str, dt.datetime]] = []
    for _ in range(horizon):
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
        month_key = f"{year:04d}-{month:02d}"
        if floor is not None and month_key < floor:
            break
        deadline = close_deadline(month_key, close_day)
        if deadline > now:
            continue
        if month_key not in recorded:
            out.append((month_key, deadline))
        if floor is None:
            break
    return sorted(out)
