# Databricks notebook source
"""Record a close: read gold as of an instant and append a new immutable version.

Why a notebook task and not part of the pipeline: a close is a decision, not a
transformation. It happens on a schedule, it is signed off, and it must be recorded exactly
once per (month, version). Putting it in the pipeline would recompute it on every refresh.
"""

# COMMAND ----------
dbutils.widgets.text("as_of", "")
as_of = dbutils.widgets.get("as_of")
catalog = spark.conf.get("samegold.catalog", "samegold")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.main.revenue_closed (
    accounting_month STRING NOT NULL,
    close_version    INT    NOT NULL,
    gross_cents      BIGINT,
    returns_cents    BIGINT,
    net_cents        BIGINT,
    restated_at      TIMESTAMP,
    restatement_reason STRING
) USING DELTA CLUSTER BY (accounting_month)
""")

# COMMAND ----------
# The version is (max existing) + 1 per month, and the insert is idempotent on
# (accounting_month, close_version): a retried job must not create version 2 twice.
spark.sql(f"""
MERGE INTO {catalog}.main.revenue_closed AS t
USING (
    SELECT r.accounting_month,
           COALESCE(v.next_version, 0) AS close_version,
           r.gross_cents, r.returns_cents, r.net_cents,
           TIMESTAMP'{as_of}' AS restated_at,
           CASE WHEN COALESCE(v.next_version, 0) = 0 THEN 'first close'
                ELSE 'late arrivals after close' END AS restatement_reason
    FROM {catalog}.main.revenue_by_month r
    LEFT JOIN (
        SELECT accounting_month, MAX(close_version) + 1 AS next_version
        FROM {catalog}.main.revenue_closed GROUP BY accounting_month
    ) v USING (accounting_month)
) AS s
ON t.accounting_month = s.accounting_month AND t.close_version = s.close_version
WHEN NOT MATCHED THEN INSERT *
""")
