# Databricks notebook source
"""One restated month, checked against the claims the close made about it.

This is the body of a `for_each` task. Its input is one accounting month, and the list of
months comes from a task value `close_month` publishes - so the cardinality is set by the
DATA rather than by a list somebody maintains here.

WHAT IT DOES NOT CHECK, because something else already does. The revenue arithmetic is tied to
the OSS lane to the cent by `tests/fast/test_databricks_close_parity.py`, which recomputes
every published version from the same events with a DuckDB reference sharing no code with this
lane. Re-deriving gross and net here would be a third implementation of a figure that already
has two, and a differential test cannot catch a defect both halves share.

WHAT NOTHING CHECKED UNTIL NOW is the close's own VERSIONING - the part that makes this a
bitemporal close rather than an overwrite:

  * `net_cents = gross_cents - returns_cents` on the row that was written;
  * the versions of this month are exactly 0..n with no gaps, so nothing skipped a number;
  * every earlier version was stamped strictly before the newest, which is what "a version
    that was signed off is never rewritten" looks like from inside a single run;
  * the six figures written equal the source row the MERGE read, so the close copied the month
    it said it copied and not its neighbour;
  * the month was eligible to close at all - the recorded close instant falls in a LATER
    accounting month than the one being closed.

The eligibility check reads `restated_at` off the row rather than taking the close instant as a
parameter. That is deliberate twice over: it removes a widget that would be interpolated into a
TIMESTAMP literal, and it checks the instant that was actually RECORDED rather than the one
this task happened to be told about.

Each check writes one row to `close_verification`, keyed by this job run, so `publish_evidence`
publishes per-month verdicts instead of one pass/fail for the whole close.
"""

# The two names below are injected by the Databricks runtime, not imported: a notebook task
# runs with `spark` and `dbutils` already bound. Same decision, and the same sentence, as
# close_month.py and the `F821` per-file ignore in pyproject.toml.
# mypy: disable-error-code="name-defined"

# COMMAND ----------
import re

for _widget in ("catalog", "accounting_month", "job_run_id", "task_run_id", "fail_task"):
    dbutils.widgets.text(_widget, "")

catalog = dbutils.widgets.get("catalog") or "samegold"
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"catalog must be a bare Unity Catalog identifier, got {catalog!r}")

# The `for_each` input. It is interpolated into SQL as a string literal, and it is the one
# value here that arrives from a TASK VALUE rather than from the bundle, so it is checked
# against the shape a month has rather than trusted for coming from upstream.
accounting_month = dbutils.widgets.get("accounting_month").strip()
if not re.fullmatch(r"\d{4}-\d{2}", accounting_month):
    raise ValueError(
        f"accounting_month must look like 2026-01, got {accounting_month!r}. This is the "
        f"for_each input; a value of the wrong shape means the task value `close_month` "
        f"published is not the list of months it is supposed to be."
    )

job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

# COMMAND ----------
# The deliberate failure, and why it is shipped rather than improvised.
#
# A repair run cannot be demonstrated without a failed task, and waiting for an organic one is
# waiting indefinitely. The alternative was to break the code and redeploy - which
# `require_fresh_deployment` now forces you to COMMIT first, so the deployed commit would name
# a tree containing a deliberate bug and the record it produced would carry that sha as its
# provenance. A parameter keeps the deployed code honest and the failure explicit.
#
# It is a JOB parameter defaulting to the empty string, so producing a failure takes a
# deliberate `--params fail_task=...` at launch. `tests/fast/test_databricks_bundle.py`
# asserts the default is empty and that every notebook task is handed it.
fail_task = dbutils.widgets.get("fail_task").strip()
if fail_task in {"verify_month", f"verify_month:{accounting_month}"}:
    raise RuntimeError(
        f"fail_task={fail_task!r}: failing verify_month for {accounting_month} on purpose. "
        f"This is the switch that produces a real failed task run to repair, and it is off "
        f"unless somebody passes it."
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
       '{accounting_month}' AS accounting_month,
       close_version,
       ok,
       detail
FROM (
    WITH versions AS (
        SELECT * FROM {catalog}.main.revenue_closed
        WHERE accounting_month = '{accounting_month}'
    ),
    -- ONE row, even when the version numbers are broken. `WHERE close_version = MAX(...)`
    -- returns two rows if a version number is duplicated, and every scalar subquery below then
    -- raises instead of returning a verdict - so the task failed with a Spark error rather
    -- than with `versions_have_no_gaps: false`, which is the check written for exactly that
    -- shape. Found by falsifying that check rather than by reading this.
    newest AS (
        SELECT * FROM (
            SELECT *, row_number() OVER (ORDER BY close_version DESC, restated_at DESC) AS rn
            FROM versions
        ) WHERE rn = 1
    ),
    source AS (
        SELECT * FROM {catalog}.main.revenue_by_month
        WHERE accounting_month = '{accounting_month}'
    )
    SELECT 'net_is_gross_minus_returns' AS check_name,
           (SELECT close_version FROM newest) AS close_version,
           (SELECT net_cents = gross_cents - returns_cents FROM newest) AS ok,
           (SELECT concat('gross ', gross_cents, ' - returns ', returns_cents, ' = ',
                          gross_cents - returns_cents, ', net says ', net_cents)
            FROM newest) AS detail
    UNION ALL
    SELECT 'versions_have_no_gaps',
           (SELECT close_version FROM newest),
           (SELECT COUNT(*) = MAX(close_version) + 1
                   AND COUNT(DISTINCT close_version) = COUNT(*) FROM versions),
           (SELECT concat(COUNT(*), ' rows for versions 0..', MAX(close_version))
            FROM versions)
    UNION ALL
    SELECT 'earlier_versions_are_older',
           (SELECT close_version FROM newest),
           (SELECT COALESCE(MAX(restated_at) < (SELECT restated_at FROM newest), TRUE)
            FROM versions WHERE close_version < (SELECT MAX(close_version) FROM versions)),
           (SELECT concat('newest ', (SELECT CAST(restated_at AS STRING) FROM newest),
                          ', latest earlier ',
                          COALESCE(CAST(MAX(restated_at) AS STRING), 'none'))
            FROM versions WHERE close_version < (SELECT MAX(close_version) FROM versions))
    UNION ALL
    SELECT 'written_equals_the_source_month',
           (SELECT close_version FROM newest),
           (SELECT n.gross_cents            = s.gross_cents
               AND n.returns_cents          = s.returns_cents
               AND n.net_cents              = s.net_cents
               AND n.line_count             = s.line_count
               AND n.return_count           = s.return_count
               AND n.returns_rejected_count = s.returns_rejected_count
            FROM newest n JOIN source s ON n.accounting_month = s.accounting_month),
           (SELECT concat('written gross ', n.gross_cents, ' lines ', n.line_count,
                          ' / source gross ', s.gross_cents, ' lines ', s.line_count)
            FROM newest n JOIN source s ON n.accounting_month = s.accounting_month)
    UNION ALL
    SELECT 'month_was_eligible_to_close',
           (SELECT close_version FROM newest),
           (SELECT date_format(from_utc_timestamp(restated_at, 'Europe/Madrid'), 'yyyy-MM')
                   > '{accounting_month}' FROM newest),
           (SELECT concat('recorded close instant ', CAST(restated_at AS STRING),
                          ' against month {accounting_month}') FROM newest)
)
""")
# Appended by position into the table declared above, whose column order this SELECT
# follows deliberately.
verified.write.mode("append").saveAsTable(f"{catalog}.main.close_verification")


# COMMAND ----------
failed = spark.sql(f"""
    SELECT check_name FROM {catalog}.main.close_verification
    WHERE job_run_id = '{job_run_id}' AND accounting_month = '{accounting_month}'
      AND NOT COALESCE(ok, FALSE)
    ORDER BY check_name
""").collect()
names = [row["check_name"] for row in failed]

# NO TASK VALUE IS SET HERE, and that is the round's own rule applied to its own code.
#
# The first draft published `month` and `failed_checks` from each iteration, described in the
# comment as "read by publish_evidence". They were not: `publish_evidence` reads the verdicts
# out of `close_verification`, filtered by job run, because it needs every row WITH its detail
# and a task value per iteration would be a second, thinner copy of the same fact. Two sources
# for one fact is how they come to disagree, and a value written on the chance somebody wants
# it is exactly what this round set out to remove.
#
# `tests/fast/test_databricks_bundle.py` enforces it: every `taskValues.set` in this lane must
# have a named reader, and these two had none.

if names:
    # FAIL the task. A verification that writes "not ok" into a table and then returns success
    # is a check nobody is forced to look at, which is the class this repository keeps
    # finding. Failing here also makes it repairable: `databricks jobs repair-run
    # --rerun-tasks` re-runs this month alone, and the other months are not spent again.
    raise AssertionError(
        f"{accounting_month} failed {len(names)} close check(s): {names}. The `detail` column "
        f"of {catalog}.main.close_verification for job run {job_run_id} carries the numbers "
        f"that decided each one."
    )
print(f"{accounting_month}: all close checks passed")
