# Databricks notebook source
"""Export the run's metrics as an evidence record the OSS lane can read.

The pipeline event log is the only observability Free Edition offers: there is no
system.billing and no system.lakeflow without an account console. So the numbers this
notebook publishes are pipeline-level (rows written per table, expectation pass and fail
counts per rule, the state of the update, the shape of the AUTO CDC dimension), and they are
labelled as such rather than dressed up as cost.

Four things were wrong here, and none of them could have been found by reading this file,
because nothing had ever deployed it:

  * it read the pipeline id from `spark.conf.get("pipelines.id")`. That key exists inside a
    pipeline SOURCE. This is a notebook task in a job - a different process, with no pipeline
    around it - so the call was `event_log('')` and the whole record was an exception;
  * the same for the catalog, from `spark.conf.get("samegold.catalog")`: a pipeline's
    `configuration:` block is not visible to a job's notebook tasks, so it silently returned
    the default and the record would have described a catalog nobody deployed to;
  * it summed nothing and scoped nothing, so an expectation's counts were reported once per
    flow_progress EVENT and across every update the pipeline had ever run. The number a
    reader wants is per rule, for THIS update. Both are in the query now;
  * the record only ever existed as stdout and a task value. A record that cannot leave the
    workspace is not evidence anyone can check, so it is also written to a Unity Catalog
    volume, which is what `scripts/databricks_run.sh` copies down into evidence/databricks/.
"""

# The two names below are injected by the Databricks runtime, not imported: a notebook task
# runs with `spark` and `dbutils` already bound. Declaring them here would be a lie about how
# this file executes there, so the check is switched off for the file and the reason is
# written where a reader of the file will find it. Same decision, and the same sentence, as
# the `F821` per-file ignore in pyproject.toml.
# mypy: disable-error-code="name-defined"

# COMMAND ----------
import json
import re
from collections.abc import Callable
from typing import Any

for _widget in ("catalog", "pipeline_id", "out_dir", "job_run_id", "task_run_id"):
    dbutils.widgets.text(_widget, "")

catalog = dbutils.widgets.get("catalog") or "samegold"
pipeline_id = dbutils.widgets.get("pipeline_id")
out_dir = dbutils.widgets.get("out_dir")

# Each of the three is interpolated into SQL or into a path, so each is checked against the
# shape it is allowed to have. A job parameter is input like any other, and an identifier
# position cannot be parameterised away.
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"catalog must be a bare Unity Catalog identifier, got {catalog!r}")
if not re.fullmatch(r"[0-9a-fA-F-]{8,64}", pipeline_id):
    raise ValueError(
        f"pipeline_id must be the value the bundle passes as "
        f"resources.pipelines.samegold_pipeline.id, got {pipeline_id!r}. Inside a pipeline "
        f"source this would be spark.conf.get('pipelines.id'); this is a notebook task, "
        f"where that key does not exist."
    )
if out_dir and not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", out_dir):
    raise ValueError(f"out_dir must be a Unity Catalog volume path, got {out_dir!r}")

# COMMAND ----------
incomplete: list[str] = []


def _read(section: str, produce: Callable[[], Any]) -> Any:
    """Run one query, and record a failure AS a value rather than losing the record.

    A single missing table must not turn the whole run into a stack trace with no evidence at
    all - but a section that could not be read has to SAY so, in the record, by name. The
    alternative (a try/except returning an empty list) publishes "zero rows" and "could not
    read the table" as the same number, which is the failure this repository is about.
    """
    try:
        return produce()
    except Exception as error:
        incomplete.append(section)
        return {"error": f"{type(error).__name__}: {error}"}


def _rows(query: str) -> list[dict[str, Any]]:
    """The only door to Spark in this notebook, so the parse test has one place to watch.

    Every statement below is a literal passed to this function, and
    tests/spark/test_databricks_lane_parses.py reads `_rows(...)` exactly as it reads
    `spark.sql(...)`: a helper that the parse test did not know about would be a way to ship
    SQL nothing had ever parsed, which is the defect that test was written for.
    """
    return [row.asDict(recursive=True) for row in spark.sql(query).collect()]


# COMMAND ----------
# The event log holds every update this pipeline has ever run. Summing across all of them
# reports a rule's failures for the life of the pipeline and calls it this run's number, so
# both queries below are scoped to the LAST update id.
expectations = _read(
    "expectations",
    lambda: _rows(f"""
        WITH last_update AS (
            SELECT origin.update_id AS update_id
            FROM event_log('{pipeline_id}')
            WHERE origin.update_id IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
        ),
        exploded AS (
            SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                   'array<struct<name:string, dataset:string,
                                 passed_records:bigint, failed_records:bigint>>')) AS e
            FROM event_log('{pipeline_id}')
            WHERE event_type = 'flow_progress'
              AND origin.update_id = (SELECT update_id FROM last_update)
        )
        SELECT e.dataset AS dataset, e.name AS rule,
               SUM(e.passed_records) AS passed,
               SUM(e.failed_records) AS failed
        FROM exploded GROUP BY 1, 2 ORDER BY 1, 2
    """),
)

update_state = _read(
    "update_state",
    lambda: _rows(f"""
        WITH last_update AS (
            SELECT origin.update_id AS update_id
            FROM event_log('{pipeline_id}')
            WHERE origin.update_id IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
        )
        SELECT origin.update_id                                 AS update_id,
               MAX(details:update_progress.state)               AS last_state,
               MIN(timestamp)                                   AS started_at,
               MAX(timestamp)                                   AS ended_at,
               SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS error_events
        FROM event_log('{pipeline_id}')
        WHERE origin.update_id = (SELECT update_id FROM last_update)
        GROUP BY 1
    """),
)

# COMMAND ----------
# One statement rather than one per table: a per-table loop would have to interpolate a table
# NAME into the query, and a name the parse test cannot resolve is a statement the parse test
# cannot check. The seven tables are the whole lane, bronze to signed-off close.
row_counts = _read(
    "rows",
    lambda: _rows(f"""
        SELECT 'bronze_events'     AS table_name, COUNT(*) AS n
          FROM {catalog}.main.bronze_events
        UNION ALL SELECT 'silver_classified', COUNT(*) FROM {catalog}.main.silver_classified
        UNION ALL SELECT 'silver_events',      COUNT(*) FROM {catalog}.main.silver_events
        UNION ALL SELECT 'silver_quarantine',  COUNT(*) FROM {catalog}.main.silver_quarantine
        UNION ALL SELECT 'dim_customer_scd2',  COUNT(*) FROM {catalog}.main.dim_customer_scd2
        UNION ALL SELECT 'revenue_by_month',   COUNT(*) FROM {catalog}.main.revenue_by_month
        UNION ALL SELECT 'revenue_closed',     COUNT(*) FROM {catalog}.main.revenue_closed
        ORDER BY table_name
    """),
)
rows = (
    {row["table_name"]: row["n"] for row in row_counts}
    if isinstance(row_counts, list)
    else row_counts
)

# The conservation invariant in this lane's own terms: every classified event is either
# accepted or carries exactly one quarantine reason, and the reasons are the contract's closed
# enum. This is what makes the expectation counts checkable against something rather than
# merely reported - `silver_events` should hold exactly the accepted ones.
quarantine = _read(
    "quarantine_by_reason",
    lambda: _rows(
        f"SELECT quarantine_reason AS reason, COUNT(*) AS n "
        f"FROM {catalog}.main.silver_classified GROUP BY 1 ORDER BY 1"
    ),
)

# Rows whose classification was decided by a rule that could not answer. This should always be
# zero: bronze declares its types, so every predicate is total. It is REPORTED rather than
# assumed because the alternative is what happened on the first deployment - an undecidable
# predicate quietly turning into `accepted`, and 2.7e19 of revenue nobody questioned until
# `close_month` happened to overflow a BIGINT on the way out.
undecided = _read(
    "undecided_rules",
    lambda: _rows(
        f"SELECT undecided_rules AS rule, COUNT(*) AS n "
        f"FROM {catalog}.main.silver_classified "
        f"WHERE undecided_rules IS NOT NULL AND undecided_rules <> '' "
        f"GROUP BY 1 ORDER BY 2 DESC"
    ),
)

# The bound the contract puts on a single line, checked against what the lane actually booked.
# A gross that exceeds (rows x max_qty x max_price) cannot be a business number, and saying so
# in the record costs one query. The deployed run would have failed this by six orders.
bounds = _read(
    "gross_within_contract_bounds",
    lambda: _rows(
        f"SELECT accounting_month, gross_cents, line_count, "
        f"       gross_cents > line_count * 10000 * 1000000 AS above_contract_ceiling "
        f"FROM {catalog}.main.revenue_by_month ORDER BY accounting_month"
    ),
)

# AUTO CDC is the Databricks-only primitive this lane exists to show, so what it produced is
# reported in the shape the OSS lane's hand-written MERGE is checked in: one open row per key,
# and closed rows that meet the open ones. `__START_AT` and `__END_AT` are the columns AUTO CDC
# adds to a Type 2 target; they are not columns this repository chose.
dimension = _read(
    "dim_customer_scd2",
    lambda: _rows(f"""
        SELECT COUNT(*)                                              AS versions,
               COUNT(DISTINCT customer_id)                           AS customers,
               SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END)     AS open_rows,
               SUM(CASE WHEN __END_AT IS NOT NULL THEN 1 ELSE 0 END) AS closed_rows,
               MIN(__START_AT)                                       AS first_start,
               MAX(__START_AT)                                       AS last_start
        FROM {catalog}.main.dim_customer_scd2
    """),
)

close = _read(
    "revenue_closed",
    lambda: _rows(
        f"SELECT accounting_month, close_version, gross_cents, returns_cents, net_cents, "
        f"line_count, return_count, returns_rejected_count, restatement_reason "
        f"FROM {catalog}.main.revenue_closed ORDER BY accounting_month, close_version"
    ),
)

# COMMAND ----------
record = {
    # Its own id, not one of the OSS chain's. This record is produced inside a workspace,
    # cannot be reproduced by a reader with a clone, and is not appended to
    # evidence/history.jsonl; giving it "SG-09" (which is the cost lab) made it look like the
    # same claim measured somewhere else. It is a different claim on a different runtime.
    "claim_id": "SG-DBX-01",
    "title": "the Databricks lane ran and its expectations reported",
    "runtime": "databricks-free",
    "catalog": catalog,
    "pipeline_id": pipeline_id,
    # Recorded exactly as the job handed them over. A dynamic value reference the runtime does
    # not recognise is passed through as its own text rather than rejected, so a literal
    # "{{job.run_id}}" surviving into this record means the reference in resources/jobs.yml is
    # wrong - which is worth seeing rather than worth hiding behind a default.
    "job_run_id": dbutils.widgets.get("job_run_id"),
    "task_run_id": dbutils.widgets.get("task_run_id"),
    # Said in the record itself, not only in the document that quotes it. A reader handed this
    # file alone has to be able to see that it is not part of the chain, and why.
    "chain": {
        "chained": False,
        "why": (
            "produced inside a Databricks workspace by a deploy, not by `samegold evidence` "
            "on a commit of this repository. It derives no seed from a commit sha and "
            "nobody with a clone can recompute it, so appending it to evidence/history.jsonl "
            "would put an unverifiable link in a chain whose only value is that every link "
            "is verifiable."
        ),
    },
    "update": update_state,
    "expectations": expectations,
    "quarantine_by_reason": quarantine,
    "undecided_rules": undecided,
    "gross_within_contract_bounds": bounds,
    "rows": rows,
    "dim_customer_scd2": dimension,
    "revenue_closed": close,
    # Named sections that could not be read. An empty list is a stronger statement than a
    # record with holes in it that nothing points at.
    "incomplete": sorted(set(incomplete)),
}
payload = json.dumps(record, indent=2, sort_keys=True, default=str)
print(payload)

# COMMAND ----------
# Out of the workspace. A task value is readable by the next task and by the Jobs API; a file
# on a Unity Catalog volume is readable by `databricks fs cp`, which is what turns this into
# something a reader outside the account can hold. Both, because they cost nothing.
dbutils.jobs.taskValues.set("evidence", payload)
if out_dir:
    with open(f"{out_dir}/SG-DBX-01.json", "w", encoding="utf-8") as handle:
        handle.write(payload)
    print(f"wrote {out_dir}/SG-DBX-01.json")
