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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# `active()`, not `getActiveSession()`. The second returns `SparkSession | None`, and every
# use below then reads an attribute off a value that may be None - which mypy says plainly once
# pyspark is installed, and said to nobody for eleven rounds because the fast lane that runs
# mypy does not install it. `active()` returns a session or raises, which is the behaviour this
# file wants: there is no sensible way to run a pipeline source with no session.
spark = SparkSession.active()

# The two calls below are the ones this lane exists for, and both are refused by the
# open-source API that ships in pyspark 4.2.0. That is not a mistake in this file and it is
# not something to work around: it is the parity boundary, and it is recorded in PARITY.md
# with the signatures that prove it. It was found by type-checking, not by running, because
# no workspace is available to run it in - which is exactly the kind of claim about a cloud
# lane that is usually left as prose.
#
#   * `cluster_by_auto` does not exist on the open-source `create_streaming_table`, which
#     takes `cluster_by` (explicit columns) only. Automatic liquid clustering is the
#     Databricks-only half of that objective.
#   * `stored_as_scd_type` is typed `Literal[1, "1"] | None` in the open-source AUTO CDC API.
#     SCD Type 2 through AUTO CDC is Databricks-only, which is precisely why
#     `src/samegold/pipelines/gold_scd2_merge.py` maintains the same dimension by hand and why
#     comparing the two is on the exam.
dp.create_streaming_table(
    name="dim_customer_scd2",
    comment="Type 2 customer dimension, maintained by AUTO CDC.",
    cluster_by_auto=True,  # type: ignore[call-arg]
)

# TRACK HISTORY ON THE ATTRIBUTES, and this is the fix for the first divergence this lane's
# whole reason for existing has ever produced.
#
# The first successful run put 78 versions and 18 closed rows in this table where the OSS
# lane's hand-written MERGE produces 75 and 15. Sixty customers and sixty open rows on both
# sides, so `open_rows = customers` held and the difference was exactly three versions.
#
# MEASURED, not assumed. The population contains 78 distinct `customer_upserted` event ids and
# exactly THREE of them are heartbeats - an upsert that repeats the segment and country the
# customer already had:
#
#     C000028  cu-C000028-1  2026-01-14T15:00:00Z  vip / PT
#     C000038  cu-C000038-1  2026-01-05T19:00:00Z  vip / IT
#     C000043  cu-C000043-1  2026-01-13T14:00:00Z  pro / FR
#
# 75 + 3 = 78 and 15 + 3 = 18. AUTO CDC was producing one version per EVENT; the OSS lane
# produces one version per CHANGE.
#
# WHICH IS RIGHT IS A CONTRACT QUESTION, and the contract already answers it, in
# `samegold.pipelines.transform.dim_customer_scd2`: "A Type 2 dimension records CHANGES, not
# heartbeats. An upsert that repeats the attributes the customer already had is not a new
# version." Three implementations on the OSS side agree on that - the domain rule in
# `domain/bitemporal.scd2_from_versions`, the full recomputation, and the DuckDB reference -
# and a round was spent making them agree. So this lane was the one that was wrong.
#
# WHY it was wrong is worth keeping, because nothing about it is obvious: AUTO CDC's default
# for SCD Type 2 is to create a new version when ANY column changes, and the source view below
# carries `event_ts` and `event_id`, which change on every upsert by construction. The default
# was therefore guaranteed to produce one version per event on this input, and no amount of
# care in the rules would have altered it.
#
# `track_history_column_list` names the columns a version is ABOUT - stated positively, the
# same way acceptance is a conjunction of the rules rather than the leftover branch - so a
# change to `event_ts` or `event_id` alone updates the current row in place instead of opening
# a new version. It is the same pair of columns the OSS lane compares with `lag()`.
#
# It is also a FOURTH Databricks-only primitive, and it is pinned like the other three:
# open-source `pyspark.pipelines.create_auto_cdc_flow` takes `column_list` and
# `except_column_list` and has no history-tracking parameter at all. mypy on pyspark 4.2.0:
#
#     error: Unexpected keyword argument "track_history_column_list" for
#     "create_auto_cdc_flow"  [call-arg]
#
# NOT YET RUN with this setting. The 78/18 above is what the workspace produced WITHOUT it;
# whether it produces 75/15 with it is the first thing the next run has to check, and
# docs/databricks-run.md says so beside the two anchors that are still in dispute.
dp.create_auto_cdc_flow(  # type: ignore[call-arg]
    target="dim_customer_scd2",
    source="silver_events_customers",
    keys=["customer_id"],
    sequence_by=F.col("event_ts"),
    stored_as_scd_type=2,  # type: ignore[arg-type]
    track_history_column_list=["segment", "country"],
)


@dp.temporary_view(name="silver_events_customers")
def silver_events_customers() -> DataFrame:
    return (
        spark.readStream.table("silver_classified")
        .where(
            (F.col("event_type") == "customer_upserted")
            & (F.col("quarantine_reason") == "accepted")
        )
        .select("customer_id", "segment", "country", "event_ts", "event_id")
    )


@dp.materialized_view(
    name="revenue_by_month",
    comment="The close: one row per (accounting_month, close_version), never rewritten.",
)
def revenue_by_month() -> DataFrame:
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
                                          COALESCE(country, ''),
                                          COALESCE(boundary, '')), 256)) AS rn
                FROM silver_classified
            ) WHERE rn = 1
        ),
        lines AS (
            -- ONE line per (order_id, sku): two sales sharing that pair are contract-legal
            -- (an order_placed is keyed by event_id) and used to fan the returns out across
            -- the join, so one return was both refunded and counted as refused.
            SELECT * EXCEPT (line_rn) FROM (
                SELECT order_id, sku, customer_id, qty AS qty0, unit_price_cents,
                       try_to_timestamp(event_ts) AS sale_ts,
                       row_number() OVER (PARTITION BY order_id, sku
                                          ORDER BY try_to_timestamp(event_ts), event_id) AS line_rn
                FROM dedup
                WHERE event_type = 'order_placed' AND quarantine_reason = 'accepted'
            ) WHERE line_rn = 1
        ),
        amendments AS (
            SELECT order_id, sku, qty FROM (
                SELECT order_id, sku, new_qty AS qty,
                       row_number() OVER (PARTITION BY order_id, sku
                                          ORDER BY try_to_timestamp(event_ts) DESC,
                                                   event_id DESC) AS rn
                FROM dedup
                WHERE event_type = 'order_line_amended' AND quarantine_reason = 'accepted'
                  AND new_qty IS NOT NULL AND new_qty > 0
            ) WHERE rn = 1
        ),
        effective AS (
            SELECT l.order_id, l.sku, l.customer_id, l.unit_price_cents, l.sale_ts,
                   COALESCE(a.qty, l.qty0) AS qty
            FROM lines l LEFT JOIN amendments a USING (order_id, sku)
        ),
        return_candidates AS (
            SELECT d.order_id, d.sku, d.qty AS return_qty, d.event_id AS return_event_id,
                   try_to_timestamp(d.event_ts) AS return_ts,
                   e.sale_ts, e.unit_price_cents, e.qty AS sold_qty
            FROM dedup d LEFT JOIN effective e USING (order_id, sku)
            WHERE d.event_type = 'return_registered' AND d.quarantine_reason = 'accepted'
              AND d.qty IS NOT NULL AND d.qty > 0
        ),
        -- Eligibility first, then the cumulative rule over the ELIGIBLE returns only, and
        -- ordered by the TIMESTAMP rather than by the text of it. The first version of this
        -- window ordered by the raw event_ts string while the dedup and amendments CTEs above
        -- both parse it, so two returns spelled with different UTC offsets were applied in the
        -- wrong order and this lane accepted a different set of returns from the other two:
        -- 2000 cents of net revenue, introduced by the fix.
        eligibility AS (
            SELECT *,
                   CASE
                       WHEN sale_ts IS NULL THEN 'return_without_order'
                       WHEN return_ts < sale_ts THEN 'return_outside_window'
                       -- Seconds, and a cast to double rather than unix_timestamp: the
                       -- latter truncates to whole seconds and accepted a return one
                       -- microsecond outside the window.
                       WHEN CAST(return_ts AS DOUBLE) - CAST(sale_ts AS DOUBLE) > 45 * 86400
                           THEN 'return_outside_window'
                   END AS ineligible_reason
            FROM return_candidates
        ),
        returns_classified AS (
            SELECT * EXCEPT (ineligible_reason, returned_including_this),
                   COALESCE(
                       ineligible_reason,
                       CASE WHEN returned_including_this > sold_qty
                            THEN 'return_exceeds_sold_qty' END,
                       'accepted'
                   ) AS return_reason
            FROM (
                SELECT *,
                       SUM(CASE WHEN ineligible_reason IS NULL THEN return_qty ELSE 0 END)
                           OVER (PARTITION BY order_id, sku
                                 ORDER BY return_ts, return_event_id
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                           AS returned_including_this
                FROM eligibility
            )
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
