"""Gold on Databricks: the bitemporal close and the Type 2 dimension.

Two things here exist only on this lane, and both are on the exam:

  * AUTO CDC for the Type 2 dimension. On the OSS lane the same shape is built by hand with a
    two-pass MERGE (`samegold.pipelines.gold_scd2_merge`), and comparing the two is the point:
    the primitive and the hand-written version must produce the same dimension.
  * liquid clustering with automatic column selection (CLUSTER BY AUTO), which needs predictive
    optimization and therefore cannot exist outside Databricks. The OSS lane clusters on
    explicit columns instead, and the cost lab measures what that buys: 85% fewer bytes to
    read for a sku predicate once the table has enough files to skip, and nothing at all when
    it does not.

Three things here were wrong and were found by review rather than by a run, because no
workspace is available to run them in:

  * the returns branch was computed and then thrown away: the final SELECT read only
    `effective` and emitted a literal zero for returns_cents and no net_cents at all, which
    the close_month task then tried to read;
  * the statement DID NOT PARSE. A missing comma before the `gross` CTE made the whole thing
    a syntax error, so the pipeline would have failed on its first refresh. Publishing SQL
    that has never been near a parser is exactly the failure mode a "cloud lane I cannot
    run" invites, so tests/spark now parses every statement in this directory with a local
    Spark. It costs nothing and it is the difference between untested and unparseable;
  * it emitted six columns. The contract has seven: `returns_rejected_count` is part of the
    close, and a rejected refund that leaves the pipeline without a counter is the failure
    nobody detects.

The dedup tie-break and the return classification are now the same derivation as the OSS
lane, down to the hash function, for the reason given in pipelines/transform.py.
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
                SELECT *, row_number() OVER (
                             PARTITION BY event_id
                             ORDER BY try_to_timestamp(event_ts), try_to_timestamp(arrival_ts),
                                      sha2(concat_ws('|',
                                          COALESCE(event_type, ''), COALESCE(order_id, ''),
                                          COALESCE(customer_id, ''), COALESCE(sku, ''),
                                          COALESCE(CAST(qty AS STRING), ''),
                                          COALESCE(CAST(new_qty AS STRING), ''),
                                          COALESCE(CAST(unit_price_cents AS STRING), ''),
                                          COALESCE(currency, ''), COALESCE(return_id, ''),
                                          COALESCE(reason, ''), COALESCE(segment, ''),
                                          COALESCE(country, '')), 256)) AS rn
                FROM silver_events
                WHERE quarantine_reason = 'accepted'
            ) WHERE rn = 1
        ),
        lines AS (
            SELECT order_id, sku, customer_id, qty AS qty0, unit_price_cents,
                   try_to_timestamp(event_ts) AS sale_ts
            FROM dedup WHERE event_type = 'order_placed'
        ),
        amendments AS (
            SELECT order_id, sku, qty FROM (
                SELECT order_id, sku, new_qty AS qty,
                       row_number() OVER (PARTITION BY order_id, sku
                                          ORDER BY try_to_timestamp(event_ts) DESC,
                                                   event_id DESC) AS rn
                FROM dedup WHERE event_type = 'order_line_amended' AND new_qty IS NOT NULL
            ) WHERE rn = 1
        ),
        effective AS (
            SELECT l.order_id, l.sku, l.customer_id, l.unit_price_cents, l.sale_ts,
                   COALESCE(a.qty, l.qty0) AS qty
            FROM lines l LEFT JOIN amendments a USING (order_id, sku)
        ),
        return_candidates AS (
            SELECT d.order_id, d.sku, d.qty AS return_qty,
                   try_to_timestamp(d.event_ts) AS return_ts,
                   e.sale_ts, e.unit_price_cents, e.qty AS sold_qty
            FROM dedup d LEFT JOIN effective e USING (order_id, sku)
            WHERE d.event_type = 'return_registered' AND d.qty IS NOT NULL AND d.qty > 0
        ),
        returns_classified AS (
            SELECT *,
                   CASE
                       WHEN sale_ts IS NULL THEN 'return_without_order'
                       WHEN return_ts < sale_ts THEN 'return_outside_window'
                       -- Seconds, and a cast to double rather than unix_timestamp: the
                       -- latter truncates to whole seconds and accepted a return one
                       -- microsecond outside the window. Found by comparing this
                       -- implementation with the DuckDB reference.
                       WHEN CAST(return_ts AS DOUBLE) - CAST(sale_ts AS DOUBLE) > 45 * 86400
                           THEN 'return_outside_window'
                       WHEN return_qty > sold_qty THEN 'return_exceeds_sold_qty'
                       ELSE 'accepted'
                   END AS return_reason
            FROM return_candidates
        ),
        returns AS (SELECT * FROM returns_classified WHERE return_reason = 'accepted'),
        rejected AS (
            SELECT date_format(from_utc_timestamp(sale_ts, 'Europe/Madrid'), 'yyyy-MM')
                       AS accounting_month,
                   COUNT(*) AS returns_rejected_count
            FROM returns_classified
            WHERE return_reason <> 'accepted' AND sale_ts IS NOT NULL
            GROUP BY 1
        ),
        gross AS (
            SELECT date_format(from_utc_timestamp(sale_ts, 'Europe/Madrid'), 'yyyy-MM')
                       AS accounting_month,
                   SUM(qty * unit_price_cents) AS gross_cents,
                   COUNT(*)                    AS line_count
            FROM effective GROUP BY 1
        ),
        refunds AS (
            SELECT date_format(from_utc_timestamp(sale_ts, 'Europe/Madrid'), 'yyyy-MM')
                       AS accounting_month,
                   SUM(return_qty * unit_price_cents) AS returns_cents,
                   COUNT(*)                           AS return_count
            FROM returns GROUP BY 1
        )
        SELECT COALESCE(g.accounting_month, r.accounting_month) AS accounting_month,
               COALESCE(g.gross_cents, 0)                       AS gross_cents,
               COALESCE(r.returns_cents, 0)                     AS returns_cents,
               COALESCE(g.gross_cents, 0) - COALESCE(r.returns_cents, 0) AS net_cents,
               COALESCE(g.line_count, 0)                        AS line_count,
               COALESCE(r.return_count, 0)                      AS return_count,
               COALESCE(x.returns_rejected_count, 0)            AS returns_rejected_count
        FROM gross g
        FULL OUTER JOIN refunds r USING (accounting_month)
        LEFT JOIN rejected x
               ON x.accounting_month = COALESCE(g.accounting_month, r.accounting_month)
        """
    )
