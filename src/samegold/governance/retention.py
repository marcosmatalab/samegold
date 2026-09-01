"""Retention: purging data the policy says may no longer be held.

The exam guide asks for "data purging solutions for data retention policy compliance", and
the interesting part is not the DELETE. It is that on a lakehouse a DELETE does not delete:
the rows stay in the previous version of the table, reachable by time travel, until VACUUM
removes the files. A purge that stops at the DELETE is a purge that does not comply, and
this module does both and reports both.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from deltalake import DeltaTable


def purge_expired(
    table_uri: str,
    column: str,
    retention_days: int,
    now: dt.datetime,
    vacuum_retention_hours: int = 0,
) -> dict[str, Any]:
    """Delete rows past the retention horizon, then vacuum the files that held them."""
    cutoff = (now - dt.timedelta(days=retention_days)).date().isoformat()
    table = DeltaTable(table_uri)
    before_version = table.version()
    deleted = table.delete(f"{column} < '{cutoff}'")

    table = DeltaTable(table_uri)
    # A DELETE alone leaves the old files in place, and time travel still returns the purged
    # rows: `DeltaTable(uri, version=before).to_pyarrow_table()` reads them back. VACUUM is
    # what makes the purge real, and dry_run=False with a zero retention window is only
    # acceptable because this is a purge - it is exactly the flag a retention job needs and
    # exactly the flag nobody should use for tidying up.
    vacuumed = table.vacuum(
        retention_hours=vacuum_retention_hours,
        dry_run=False,
        enforce_retention_duration=False,
    )
    return {
        "cutoff": cutoff,
        "rows_deleted": int(deleted.get("num_deleted_rows", 0)),
        "files_removed_by_vacuum": len(vacuumed),
        "version_before": before_version,
        "version_after": DeltaTable(table_uri).version(),
        "note": (
            "the DELETE alone would have left the rows readable through time travel; the "
            "vacuum is what makes the purge a purge"
        ),
    }


def residual_in_transaction_log(table_uri: str, values: Iterable[str]) -> list[str]:
    """Values that survive the purge inside the Delta transaction log.

    This exists because of a finding, not a hypothesis. VACUUM removes data files; it does not
    touch the log, and the log carries per-file min/max STATISTICS. After purging a table
    keyed by customer, an adversarial review found real customer identifiers sitting in the
    `minValues` and `maxValues` of a committed log entry: the rows were gone and the
    identifiers were not.

    The fix is on the write side, and it is one table property:
    ``delta.dataSkippingStatsColumns`` restricts statistics to the columns that need them, so
    the identifier never enters the log in the first place. This function is what proves it,
    and what fails the claim if a future writer forgets.
    """
    log_dir = Path(table_uri) / "_delta_log"
    if not log_dir.exists():
        return []
    haystack = "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for path in sorted(log_dir.iterdir())
        if path.is_file()
    )
    return sorted({value for value in values if value and str(value) in haystack})
