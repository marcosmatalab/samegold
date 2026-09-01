"""Gold on Databricks: the bitemporal close and the Type 2 dimension.

Two things here exist only on this lane, and both are on the exam:

  * AUTO CDC for the Type 2 dimension. On the OSS lane the same shape is built by hand with a
    two-pass MERGE (`samegold.pipelines.gold_scd2_merge`), and comparing the two is the point:
    the primitive and the hand-written version must produce the same dimension.
  * liquid clustering with automatic column selection (CLUSTER BY AUTO), which needs predictive
    optimization and therefore cannot exist outside Databricks. The OSS lane clusters on
    explicit columns instead, and the cost lab measures both.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

dp.create_streaming_table(
    name="dim_customer_scd2",
    comment="Type 2 customer dimension, maintained by AUTO CDC.",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="dim_customer_scd2",
    source="silver_events_customers",
    keys=["customer_id"],
    sequence_by=F.col("event_ts"),
    stored_as_scd_type=2,
)


@dp.temporary_view(name="silver_events_customers")
def silver_events_customers():
    return (
        spark.readStream.table("silver_events")
        .where(F.col("event_type") == "customer_upserted")
        .select("customer_id", "segment", "country", "event_ts", "event_id")
    )


@dp.materialized_view(
    name="revenue_by_month",
    comment="The close: one row per (accounting_month, close_version), never rewritten.",
)
def revenue_by_month():
    # The SQL is the same derivation as the OSS lane's; keeping it as one statement makes the
    # pipeline's lineage graph show the real dependency rather than a chain of views.
    return spark.sql(
        """
        WITH dedup AS (
            SELECT * EXCEPT (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY event_ts) AS rn
                FROM silver_events
            ) WHERE rn = 1
        ),
        lines AS (
            SELECT order_id, sku, customer_id, qty AS qty0, unit_price_cents,
                   CAST(event_ts AS TIMESTAMP) AS sale_ts
            FROM dedup WHERE event_type = 'order_placed'
        ),
        amendments AS (
            SELECT order_id, sku, new_qty AS qty FROM (
                SELECT order_id, sku, new_qty,
                       row_number() OVER (PARTITION BY order_id, sku
                                          ORDER BY event_ts DESC, event_id DESC) AS rn
                FROM dedup WHERE event_type = 'order_line_amended'
            ) WHERE rn = 1
        ),
        effective AS (
            SELECT l.*, COALESCE(a.qty, l.qty0) AS qty
            FROM lines l LEFT JOIN amendments a USING (order_id, sku)
        ),
        returns AS (
            SELECT e.sale_ts, e.unit_price_cents, d.qty AS qty
            FROM dedup d JOIN effective e USING (order_id, sku)
            WHERE d.event_type = 'return_registered'
              AND CAST(d.event_ts AS TIMESTAMP) >= e.sale_ts
              AND CAST(d.event_ts AS TIMESTAMP) <= e.sale_ts + INTERVAL 45 DAYS
              AND d.qty <= e.qty
        )
        SELECT date_format(from_utc_timestamp(sale_ts, 'Europe/Madrid'), 'yyyy-MM')
                   AS accounting_month,
               SUM(qty * unit_price_cents)  AS gross_cents,
               0L                           AS returns_cents,
               COUNT(*)                     AS line_count
        FROM effective GROUP BY 1
        """
    )
