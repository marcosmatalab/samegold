"""The bronze schema, declared rather than inferred.

Inferring the schema of the raw events would make the pipeline's behaviour depend on which
files happened to be in the first batch, which is the classic way a pipeline that worked in
development changes its answer in production. The schema is declared here once and used by
both the Spark reader and the streaming reader; unexpected fields land in the corrupt-record
column rather than silently widening the table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql.types import StructType

RESCUED_COLUMN = "_rescued_data"


def bronze_schema() -> StructType:
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("arrival_ts", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("qty", LongType(), True),
            StructField("new_qty", LongType(), True),
            StructField("unit_price_cents", LongType(), True),
            StructField("currency", StringType(), True),
            StructField("return_id", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("boundary", StringType(), True),
            # Timestamps arrive as ISO strings and are cast explicitly downstream. Letting
            # the reader parse them would hand timezone handling to a reader option, which
            # is exactly the kind of decision that must not be invisible in an accounting
            # pipeline.
            StructField(RESCUED_COLUMN, StringType(), True),
        ]
    )
