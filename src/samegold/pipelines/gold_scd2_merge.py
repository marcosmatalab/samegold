"""SCD Type 2 on Delta: a thin MERGE over a decision made in pure Python.

The logic that decides what the dimension should look like lives in
``samegold.domain.bitemporal.scd2_apply``, which has no engine in it and is tested in
milliseconds. This module only applies that decision to a Delta table.

The split is not tidiness. When the whole thing was one MERGE, an adversarial review found
three bugs in it that no structural invariant could see:

  * two versions of one customer in the same batch lost the middle one, because the MERGE
    closed the open row with the LAST valid_from and inserted only that version. The
    dimension stayed disjoint, contiguous and single-current, and was simply missing a period;
  * a version with the same valid_from as the open row produced a zero-length interval and a
    duplicate primary key;
  * a version older than the open row was counted as a "late correction" and dropped, while
    the docstring called silent dropping unacceptable.

All three are now regression tests over the pure function, and the MERGE below cannot
reintroduce them because it no longer decides anything.

Delta specifics that remain load-bearing:

  * the source is deduplicated on the primary key before the MERGE, or Delta fails at runtime
    with a multiple-matches error. That failure is the good outcome; a silent last-writer-wins
    would be worse;
  * ``whenMatchedUpdateAll`` plus ``whenNotMatchedInsertAll`` on the key (customer_id,
    valid_from) makes the operation idempotent: replaying the same batch after a crash
    rewrites the same rows rather than appending new ones. That is the property the crash
    campaign checks;
  * ``CLUSTER BY (customer_id)`` rather than partitioning: the cardinality of customer_id is
    far too high for partitions, and liquid clustering is the answer the exam expects and the
    one that does not create a directory per customer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from samegold.domain.bitemporal import scd2_from_versions

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

VERSIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_id STRING NOT NULL,
    valid_from  STRING NOT NULL,
    segment     STRING,
    country     STRING,
    event_id    STRING NOT NULL
) USING DELTA
CLUSTER BY (customer_id)
"""

SCD2_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_id   STRING  NOT NULL,
    valid_from    STRING  NOT NULL,
    valid_to      STRING,
    segment       STRING,
    country       STRING,
    is_current    BOOLEAN NOT NULL
) USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.enableDeletionVectors = true
)
"""

TARGET_COLUMNS = ("customer_id", "valid_from", "valid_to", "segment", "country", "is_current")


def F_col_in(column: str, values: list[str]) -> Any:
    from pyspark.sql import functions as F

    return F.col(column).isin(values) if values else F.lit(False)


def upsert_scd2(spark: Any, batch: DataFrame, table: str) -> dict[str, int]:
    """Apply one batch of customer versions to the Type 2 dimension.

    Returns counters, because a MERGE that silently does nothing looks exactly like a MERGE
    that worked.
    """
    from delta.tables import DeltaTable

    spark.sql(SCD2_TABLE_DDL.format(table=table))
    target = DeltaTable.forName(spark, table)

    # The source versions are kept in their own append-only table, and the dimension is
    # recomputed from them for the keys the batch touches. Folding a batch into the
    # materialised dimension loses information - a version that matched the open row is not
    # recorded, and a later correction turns it into a change that no longer exists - and the
    # result then depends on how the input was cut into batches. See domain/bitemporal.py.
    versions_table = f"{table}_versions"
    spark.sql(VERSIONS_TABLE_DDL.format(table=versions_table))
    versions = DeltaTable.forName(spark, versions_table)
    incoming = [row.asDict() for row in batch.collect()]
    keys = sorted({row["customer_id"] for row in incoming})
    (
        versions.alias("v")
        .merge(
            batch.alias("s"),
            "v.customer_id = s.customer_id AND v.valid_from = s.valid_from "
            "AND v.event_id = s.event_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    known = [
        row.asDict()
        for row in spark.table(versions_table).where(F_col_in("customer_id", keys)).collect()
    ]
    current = [row.asDict() for row in target.toDF().where(F_col_in("customer_id", keys)).collect()]
    desired = scd2_from_versions(known)

    # Only the rows that actually changed are written. Writing the whole dimension every time
    # would work and would also make every run look like a full rewrite in the Delta history,
    # which destroys the change data feed as a source of information.
    before = {(row["customer_id"], row["valid_from"]): row for row in current}
    changed = [row for row in desired if before.get((row["customer_id"], row["valid_from"])) != row]
    if not changed:
        return {"applied": 0, "rows_written": 0, "dimension_rows": len(desired)}

    source = spark.createDataFrame(
        [{column: row[column] for column in TARGET_COLUMNS} for row in changed]
    )
    (
        target.alias("t")
        .merge(
            source.alias("s"),
            "t.customer_id = s.customer_id AND t.valid_from = s.valid_from",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {
        "applied": len(incoming),
        "rows_written": len(changed),
        "dimension_rows": len(desired),
        "keys_touched": len(keys),
    }
