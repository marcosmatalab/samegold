# Databricks notebook source
"""Record a close: read gold as of an instant and append a new immutable version.

Why a notebook task and not part of the pipeline: a close is a decision, not a
transformation. It happens on a schedule, it is signed off, and it must be recorded exactly
once per (month, version). Putting it in the pipeline would recompute it on every refresh.
"""

# The two names below are injected by the Databricks runtime, not imported: a notebook task
# runs with `spark` and `dbutils` already bound. Declaring them here would be a lie about how
# this file executes there, so the check is switched off for the file and the reason is
# written where a reader of the file will find it. Same decision, and the same sentence, as
# the `F821` per-file ignore in pyproject.toml.
# mypy: disable-error-code="name-defined"

# COMMAND ----------
import re

dbutils.widgets.text("as_of", "")
as_of = dbutils.widgets.get("as_of")
# Validated, not trusted. The value is interpolated into a TIMESTAMP literal below, and a
# quote in it would break out of that literal in the one job that writes the signed-off close
# table. A widget is user input like any other.
if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?", as_of):
    raise ValueError(f"as_of must be an ISO timestamp without a zone offset, got {as_of!r}")
catalog = spark.conf.get("samegold.catalog", "samegold")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.main.revenue_closed (
    accounting_month STRING NOT NULL,
    close_version    INT    NOT NULL,
    gross_cents      BIGINT,
    returns_cents    BIGINT,
    net_cents        BIGINT,
    line_count       BIGINT,
    return_count     BIGINT,
    returns_rejected_count BIGINT,
    restated_at      TIMESTAMP,
    restatement_reason STRING
) USING DELTA CLUSTER BY (accounting_month)
""")

# COMMAND ----------
# The version is (max existing) + 1 per month, and the insert is idempotent on
# (accounting_month, close_version): a retried job must not create version 2 twice.
#
# Two conditions in the source below carry the whole bitemporal rule, and both were missing
# from the first version of this task:
#
#   * a month is closed only once the close instant is in a LATER accounting month. Without
#     it, the partial view of the month in progress is signed off as version 0 and every
#     subsequent close restates it, for ever.
#   * a version is recorded only when a VALUE CHANGED. Without it, every scheduled close
#     appends a version that says exactly what the previous one said, and "restatement"
#     stops meaning anything. This is the rule domain/bitemporal.py states in Python and
#     pipelines/transform.revenue_versions derives in Spark; this is the third derivation.
spark.sql(f"""
MERGE INTO {catalog}.main.revenue_closed AS t
USING (
    WITH latest AS (
        SELECT accounting_month, gross_cents, returns_cents, net_cents,
               line_count, return_count, returns_rejected_count, close_version
        FROM (
            SELECT *, row_number() OVER (PARTITION BY accounting_month
                                         ORDER BY close_version DESC) AS rn
            FROM {catalog}.main.revenue_closed
        ) WHERE rn = 1
    )
    SELECT r.accounting_month,
           COALESCE(l.close_version + 1, 0) AS close_version,
           r.gross_cents, r.returns_cents, r.net_cents,
           r.line_count, r.return_count, r.returns_rejected_count,
           TIMESTAMP'{as_of}' AS restated_at,
           CASE WHEN l.close_version IS NULL THEN 'first close'
                ELSE 'late arrivals after close' END AS restatement_reason
    FROM {catalog}.main.revenue_by_month r
    LEFT JOIN latest l USING (accounting_month)
    WHERE date_format(from_utc_timestamp(TIMESTAMP'{as_of}', 'Europe/Madrid'), 'yyyy-MM')
              > r.accounting_month
      AND (l.accounting_month IS NULL
           OR l.gross_cents           <> r.gross_cents
           OR l.returns_cents         <> r.returns_cents
           OR l.net_cents             <> r.net_cents
           OR l.line_count            <> r.line_count
           OR l.return_count          <> r.return_count
           OR l.returns_rejected_count <> r.returns_rejected_count)
) AS s
ON t.accounting_month = s.accounting_month AND t.close_version = s.close_version
WHEN NOT MATCHED THEN INSERT *
""")
