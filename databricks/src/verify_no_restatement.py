# Databricks notebook source
"""The other branch: a close that wrote nothing, checked on the claim that implies.

This runs when `close_month` decided NOT to restate. That decision is a claim - "no month
eligible to close has moved since its last version" - and it is a claim nothing checked. It is
also the more dangerous of the two: a wrong restatement is visible as a version somebody can
compare, and a MISSING restatement is visible as nothing at all.

So the two branches check different things and neither subsumes the other:

  * `verify_month`, per restated month, asks whether what was written is right;
  * this asks whether writing nothing was right.

WHY THIS IS NOT A `for_each`, since the branch above it is one. The claim here is an aggregate
over every eligible month - "none of them drifted" - and it has one answer. Splitting it per
month would give a task per month whose combined verdict is the same boolean, with no failure
to isolate and nothing to repair separately, which is the decoration the round set out to
avoid. The contrast is the point: `for_each` is used where the domain is per-month and a
single task where the claim is one.

The eligibility boundary comes from a task value rather than from a timestamp parameter. That
removes the only value here that would have been interpolated into a TIMESTAMP literal - the
close instant - and replaces it with the accounting month `close_month` derived from it, which
is a `YYYY-MM` string compared as a string.
"""

# The two names below are injected by the Databricks runtime, not imported: a notebook task
# runs with `spark` and `dbutils` already bound. Same decision, and the same sentence, as
# close_month.py and the `F821` per-file ignore in pyproject.toml.
# mypy: disable-error-code="name-defined"

# COMMAND ----------
import re

for _widget in ("catalog", "as_of_month", "job_run_id", "task_run_id", "fail_task"):
    dbutils.widgets.text(_widget, "")

catalog = dbutils.widgets.get("catalog") or "samegold"
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"catalog must be a bare Unity Catalog identifier, got {catalog!r}")

# The accounting month of the close instant, published by `close_month` as a task value. A
# month is eligible to close once this is strictly greater than it.
as_of_month = dbutils.widgets.get("as_of_month").strip()
if not re.fullmatch(r"\d{4}-\d{2}", as_of_month):
    raise ValueError(
        f"as_of_month must look like 2026-03, got {as_of_month!r}. It is the task value "
        f"`close_month` publishes; a value of the wrong shape means this task is reading "
        f"something other than the close's own eligibility boundary."
    )

job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

# COMMAND ----------
# The deliberate failure. See the same block in verify_month.py for why it is a parameter and
# not a temporary edit: `require_fresh_deployment` would make a temporary edit a COMMIT, and
# the record produced would then carry the sha of a tree containing a deliberate bug.
fail_task = dbutils.widgets.get("fail_task").strip()
if fail_task == "verify_no_restatement":
    raise RuntimeError(
        f"fail_task={fail_task!r}: failing verify_no_restatement on purpose. It is off unless "
        f"somebody passes it."
    )

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.main.close_verification (
    job_run_id       STRING,
    task_run_id      STRING,
    checked_at       TIMESTAMP,
    check_name       STRING,
    accounting_month STRING,
    close_version    INT,
    ok               BOOLEAN,
    detail           STRING
) USING DELTA
""")

# COMMAND ----------
# The rows are SELECTED here and appended by the writer below, rather than written with
# `INSERT INTO ... SELECT`. That is not a style choice: `tests/spark/test_databricks_lane_parses.py`
# resolves every statement in this lane against views with the real column names, and a temp
# view cannot be the target of an INSERT - so an `INSERT INTO` here would be a statement the
# resolution check has to skip, and a skipped statement is how this repository last shipped a
# missing comma. As a SELECT, every column, join and predicate below is analysed.
verified = spark.sql(f"""
SELECT '{job_run_id}' AS job_run_id,
       '{task_run_id}' AS task_run_id,
       current_timestamp() AS checked_at,
       check_name,
       accounting_month,
       close_version,
       ok,
       detail
FROM (
    WITH eligible AS (
        SELECT * FROM {catalog}.main.revenue_by_month
        WHERE accounting_month < '{as_of_month}'
    ),
    latest AS (
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY accounting_month
                                         ORDER BY close_version DESC) AS rn
            FROM {catalog}.main.revenue_closed
        ) WHERE rn = 1
    )
    SELECT 'every_eligible_month_has_a_version' AS check_name,
           e.accounting_month AS accounting_month,
           l.close_version AS close_version,
           l.accounting_month IS NOT NULL AS ok,
           concat('month ', e.accounting_month, ' eligible against ', '{as_of_month}',
                  ', latest version ',
                  COALESCE(CAST(l.close_version AS STRING), 'NONE')) AS detail
    FROM eligible e LEFT JOIN latest l ON e.accounting_month = l.accounting_month
    UNION ALL
    SELECT 'no_eligible_month_drifted',
           e.accounting_month,
           l.close_version,
           l.gross_cents            = e.gross_cents
       AND l.returns_cents          = e.returns_cents
       AND l.net_cents              = e.net_cents
       AND l.line_count             = e.line_count
       AND l.return_count           = e.return_count
       AND l.returns_rejected_count = e.returns_rejected_count,
           concat('source gross ', e.gross_cents, ' lines ', e.line_count,
                  ' / version ', l.close_version, ' gross ', l.gross_cents,
                  ' lines ', l.line_count)
    FROM eligible e JOIN latest l ON e.accounting_month = l.accounting_month
)
""")
# Appended by position into the table declared above, whose column order this SELECT
# follows deliberately.
verified.write.mode("append").saveAsTable(f"{catalog}.main.close_verification")


# COMMAND ----------
failed = spark.sql(f"""
    SELECT check_name, accounting_month FROM {catalog}.main.close_verification
    WHERE job_run_id = '{job_run_id}' AND NOT COALESCE(ok, FALSE)
    ORDER BY check_name, accounting_month
""").collect()
names = [f"{row['check_name']}:{row['accounting_month']}" for row in failed]

# No task value here either, for the reason written out in verify_month.py: the verdicts go to
# `close_verification` where `publish_evidence` reads them with their detail.

if names:
    raise AssertionError(
        f"the close wrote nothing and {len(names)} check(s) say it should have: {names}. A "
        f"missing restatement is the silent half of this failure - there is no wrong version "
        f"to notice, only a right one that was never written. The `detail` column of "
        f"{catalog}.main.close_verification for job run {job_run_id} carries the numbers."
    )
print(f"no restatement was needed, and every month eligible against {as_of_month} agrees")
