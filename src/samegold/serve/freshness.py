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
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from samegold.domain.contract import FRESHNESS_SLA


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
    month = dt.date(now.year, now.month, 1) - dt.timedelta(days=1)
    month_key = f"{month.year:04d}-{month.month:02d}"
    if now.day > close_day and month_key not in recorded:
        overdue = (
            now - dt.datetime(now.year, now.month, close_day, tzinfo=now.tzinfo)
        ).total_seconds()
        breaches.append(
            FreshnessBreach(
                "close_overdue",
                f"{month_key} has no close and the close day ({close_day}) has passed: a "
                f"scheduled job did not run, which is a repair rather than a page",
                lag_seconds=overdue,
                threshold_seconds=0.0,
            )
        )
    return breaches
