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

for _widget in (
    "catalog",
    "pipeline_id",
    "out_dir",
    "job_run_id",
    "task_run_id",
    "deploy_commit",
    "deploy_tree_dirty",
    # What `close_month` decided, carried here as dynamic value references. See
    # `_task_value` below for what happens when they do not resolve.
    "close_decision",
    "close_months_written",
    "close_versions_written",
    # What the runtime says each upstream task DID, as `{{tasks.<key>.result_state}}`. The
    # widget is named after the task it reports, so the correspondence with resources/jobs.yml
    # is mechanical rather than remembered. See `TASK_STATE_WIDGETS` below.
    "state_close_month",
    "state_verify_each_restated_month",
    "state_verify_no_restatement",
    "fail_task",
):
    dbutils.widgets.text(_widget, "")

catalog = dbutils.widgets.get("catalog") or "samegold"
pipeline_id = dbutils.widgets.get("pipeline_id")
out_dir = dbutils.widgets.get("out_dir")
# The commit the bundle was deployed from, carried in by the deploy rather than written
# afterwards by whoever fetched the files. Everything this notebook publishes could say WHICH
# tables it read and WHEN, and could not say which code produced them.
deploy_commit = dbutils.widgets.get("deploy_commit") or "unknown"
deploy_tree_dirty = dbutils.widgets.get("deploy_tree_dirty") or "unknown"

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
# Checked rather than published as given, because the whole value of this field is that a
# reader can look the commit up. "unknown" is allowed and is honest - a deploy by hand, without
# the script - but a truncated sha or a leftover "${var.deploy_commit}" is not: it would look
# like provenance and resolve to nothing.
if not re.fullmatch(r"unknown|[0-9a-f]{40}", deploy_commit):
    raise ValueError(
        f"deploy_commit must be a full 40-character sha or the word 'unknown', got "
        f"{deploy_commit!r}. scripts/databricks_run.sh passes it with "
        f'--var="deploy_commit=$(git rev-parse HEAD)"; a value that is neither means the '
        f"bundle variable did not resolve."
    )
if deploy_tree_dirty not in ("true", "false", "unknown"):
    raise ValueError(
        f"deploy_tree_dirty must be 'true', 'false' or 'unknown', got {deploy_tree_dirty!r}"
    )
# A BOOLEAN in the record, not the string it arrives as. A bundle variable is a string and a
# job widget is a string, so this crossed the wire as "false" - and `if
# record["deploy"]["tree_dirty"]:` is TRUE for the string "false", on a clean tree, for every
# reader who writes the obvious thing. `None` for "unknown", because two states cannot carry
# three and a value nobody supplied must not read as "clean": `deploy.commit` is the
# discriminator, and it is the word "unknown" in exactly that case.
tree_dirty: bool | None = {"true": True, "false": False}.get(deploy_tree_dirty)

# COMMAND ----------
# The deliberate failure. See the same block in verify_month.py: a repair run needs a real
# failed task, and the alternative to a parameter is committing a deliberate bug and letting
# the record name that tree as its provenance.
if dbutils.widgets.get("fail_task").strip() == "publish_evidence":
    raise RuntimeError("fail_task='publish_evidence': failing the evidence task on purpose")


def _task_value(widget: str) -> Any:
    """A task value that reached this notebook as a dynamic value reference, or None.

    `run_if: ALL_DONE` means this task runs even when `close_month` failed - and a task that
    failed published no task values, so `{{tasks.close_month.values.decision}}` arrives as its
    own literal text. The runtime passes an unresolved reference through rather than rejecting
    it, which is the same property the record already relies on to make a wrong
    `{{job.run_id}}` visible instead of blank.

    So the unresolved form is DETECTED and reported as a hole, not parsed as data. A value that
    is absent because the upstream never ran and a value that is genuinely empty are different
    facts, and this is the function that refuses to collapse them.
    """
    raw = dbutils.widgets.get(widget).strip()
    if not raw or (raw.startswith("{{") and raw.endswith("}}")):
        unresolved.append(widget)
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


unresolved: list[str] = []

# COMMAND ----------
# `get_json_object(details, '$.path')`, not `details:path`, in every query below.
#
# The `:` operator on a STRING column is Databricks SQL. MEASURED on pyspark 4.2.0: it raises,
# while `get_json_object` returns the value and `parse_json(details):path` returns a variant.
# Both engines have `get_json_object`, and that is the whole reason the update-state query can
# be executed by `tests/spark/test_databricks_event_log_query.py` against a synthetic event log
# rather than merely parsed - which is what it took to find that `MAX` on a state string
# publishes WAITING_FOR_RESOURCES for every update that ever ran.
#
# `event_log()` itself is still Databricks-only and is substituted for a view in that test.
# One name substituted is a different thing from a query rewritten.
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
            SELECT explode(from_json(
                   get_json_object(details, '$.flow_progress.data_quality.expectations'),
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
            -- The most recent update that reached a TERMINAL state, not simply the most
            -- recent update to leave an event. What this record describes is a set of TABLES,
            -- and an update that has not finished has not produced them. In this job the
            -- notebook runs after the pipeline task, so the pipeline's update is terminal by
            -- the time this executes and the two coincide - but they coincide by the shape of
            -- the job rather than by anything this query asserted, and after a morning in
            -- which one launch produced six updates, "the latest one that left an event" is
            -- not a sentence worth relying on.
            --
            -- WHAT THIS STILL DOES NOT GUARANTEE, said here rather than left to be assumed:
            -- that the update it names is the one whose output the close read. Nothing in the
            -- Jobs API hands a notebook task the update id its upstream pipeline task
            -- produced, so if a second update is started by hand while the job is running,
            -- this names that one. The id is published so a reader can check it against
            -- `databricks pipelines get`.
            SELECT origin.update_id AS update_id
            FROM event_log('{pipeline_id}')
            WHERE origin.update_id IS NOT NULL
              AND get_json_object(details, '$.update_progress.state')
                  IN ('COMPLETED', 'FAILED', 'CANCELED')
            ORDER BY timestamp DESC LIMIT 1
        )
        SELECT origin.update_id                                 AS update_id,
               -- max_by(state, timestamp), NEVER MAX(state). `MAX` on a string is the
               -- alphabetical maximum, and over the states an update passes through -
               -- CREATED, WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING,
               -- COMPLETED, FAILED, CANCELED - W sorts last. So this field published
               -- WAITING_FOR_RESOURCES for update 44a237b3, which `databricks pipelines get`
               -- reports as COMPLETED, and it would have published the same word for the
               -- update that FAILED that morning. A constant with the shape of a measurement,
               -- in the field that says whether the lane worked, and the `dbx:update.
               -- last_state` anchor would have taken it.
               max_by(get_json_object(details, '$.update_progress.state'),
                      timestamp)                  AS last_state,
               MIN(timestamp)                                   AS started_at,
               MAX(timestamp)                                   AS ended_at,
               SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS error_events
        FROM event_log('{pipeline_id}')
        WHERE origin.update_id = (SELECT update_id FROM last_update)
        GROUP BY 1
    """),
)

# Every update that reached a terminal state, most recent first. One `bundle run` produced SIX
# failed updates in fourteen minutes on 2 September 2026 - five automatic retries - and nothing
# in the record said so: it described one update and the retry loop was invisible to every
# reader of it. `pipelines.numUpdateRetryAttempts` is set to 0 now, and this is how a reader
# checks that rather than trusting it.
update_history = _read(
    "update_history",
    lambda: _rows(f"""
        SELECT origin.update_id                                AS update_id,
               max_by(get_json_object(details, '$.update_progress.state'),
                      timestamp)                  AS final_state,
               MAX(timestamp)                                   AS ended_at
        FROM event_log('{pipeline_id}')
        WHERE origin.update_id IS NOT NULL
          AND get_json_object(details, '$.update_progress.state')
              IN ('COMPLETED', 'FAILED', 'CANCELED')
        GROUP BY 1
        ORDER BY ended_at DESC
        LIMIT 10
    """),
)

# COMMAND ----------
# WHICH EVENTS THIS RUN READ, as a fingerprint the OSS half can recompute.
#
# `tests/fast/test_databricks_dimension_parity.py` compares the workspace's dimension against
# one this repository generates from a seed, and it chose WHICH population to generate by
# matching `rows.bronze_events` - a COUNT. That is not a tie. Measured: reordering the
# `countries` list in the OSS generator leaves every published number identical (1328 rows,
# 96 upserts, 4 heartbeats, 92 versions, 60 customers, 32 closed, the close unchanged to the
# cent) and gives thirty customers a different history, and the comparison then reports it as
# AUTO CDC and the hand-written MERGE producing different dimensions. Renaming the skus
# changes 1216 values and every parity test in the repository still passes.
#
# So the workspace publishes what it actually ingested, and the other half has to reproduce it.
#
# THE DOMAIN. Three of the lines are deliberately corrupt and they are TRUNCATED objects -
# `{"event_id": "bad-0000009", "event_type": "order_placed",`. Python's `json.loads` cannot
# see them at all; local Spark, reading the same files with the declared schema, nulls the
# whole row. The two halves therefore exclude the same three lines - but they do so by
# accident of one reader's behaviour, and whether a partially parsed record keeps its leading
# fields is a setting (`spark.sql.json.enablePartialResults`). The reader that fills THIS
# table is Auto Loader in rescue mode, which nothing outside a workspace can run. So the
# domain asks for an `event_id` AND an `arrival_ts`: a line truncated after `event_type` has
# no arrival time under either behaviour, which makes the domain independent of a question
# this repository cannot ask. What falls outside is counted, not dropped:
#
#     digest_rows + rows_outside_the_digest = rows.bronze_events
#
# so a reader that started dropping those lines breaks the arithmetic instead of quietly
# shrinking the population.
#
# THE RENDERING has a counterpart in `samegold.generator.late._render`, and
# `tests/spark/test_databricks_population_digest.py` runs the two against each other over the
# real population - which is the only reason this is a tie and not a hope:
#
#   * `coalesce(..., '')` on EVERY column. `concat_ws` SKIPS nulls rather than emitting an
#     empty field, so without it an order with no `sku` and a sku with no `order_id` render to
#     the same line.
#   * `chr(31)` and `chr(10)` rather than escape sequences, so the separators do not depend on
#     how a SQL string literal is unescaped. The OSS side refuses a value containing either.
#   * `CAST(x AS STRING)` on the three BIGINT columns, which is also where a value the table
#     could not hold shows up as NULL: the generator emits 9223372036854775808 for two events
#     and `bad_events` below reports `unit_price_cents: null` for exactly those two ids.
#   * `sort_array` before hashing, because neither side can promise an order otherwise.
#
# The column list is written out twice - as the projection and as the `columns` field a reader
# gets - and `tests/fast/test_databricks_bundle.py` fails if the two orders drift from each
# other or from the declared bronze schema. The order is part of what is hashed.
population = _read(
    "population",
    lambda: _rows(f"""
        SELECT sha2(concat_ws(chr(10), sort_array(collect_list(line))), 256) AS digest,
               COUNT(line)                                   AS digest_rows,
               COUNT(*) - COUNT(line)                        AS rows_outside_the_digest,
               'event_id,event_type,event_ts,arrival_ts,order_id,customer_id,sku,qty,new_qty,unit_price_cents,currency,return_id,reason,segment,country,boundary'
                                                             AS columns
        FROM (
            SELECT CASE
                       WHEN event_id IS NULL OR arrival_ts IS NULL THEN NULL
                       ELSE concat_ws(chr(31),
                            coalesce(CAST(event_id AS STRING), ''),
                            coalesce(CAST(event_type AS STRING), ''),
                            coalesce(CAST(event_ts AS STRING), ''),
                            coalesce(CAST(arrival_ts AS STRING), ''),
                            coalesce(CAST(order_id AS STRING), ''),
                            coalesce(CAST(customer_id AS STRING), ''),
                            coalesce(CAST(sku AS STRING), ''),
                            coalesce(CAST(qty AS STRING), ''),
                            coalesce(CAST(new_qty AS STRING), ''),
                            coalesce(CAST(unit_price_cents AS STRING), ''),
                            coalesce(CAST(currency AS STRING), ''),
                            coalesce(CAST(return_id AS STRING), ''),
                            coalesce(CAST(reason AS STRING), ''),
                            coalesce(CAST(segment AS STRING), ''),
                            coalesce(CAST(country AS STRING), ''),
                            coalesce(CAST(boundary AS STRING), ''))
                   END AS line
            FROM {catalog}.main.bronze_events
        )
    """),
)

# COMMAND ----------
# WHAT THE JOB DID, as opposed to what the tables say.
#
# The close either appends versions or decides not to, and until this round that decision was
# invisible: `close_month` set no task value, the job had no branch, and the record could not
# say which of the two had happened. `publish_evidence` set a task value that nothing read.
#
# Now the decision governs the graph - a condition task reads `versions_written`, a for_each
# iterates `months_written`, and the false branch checks the claim that writing nothing implies
# - and this is where that shows up in the evidence, so the documents render it rather than
# somebody typing it.
close_months = _task_value("close_months_written")
close_versions = _task_value("close_versions_written")
close_decision = _task_value("close_decision")
# Which side of the condition ran. Derived from the decision rather than from a fourth task
# value, because two names for one fact is how they come to disagree.
branch = {
    "restated": "verify_each_restated_month",
    "no_op": "verify_no_restatement",
}.get(str(close_decision), "unknown")

# THE UPSTREAM TASKS' OWN OUTCOMES, and why they are read at all.
#
# `{{tasks.<key>.result_state}}` is a documented dynamic value reference. Its vocabulary is
# success, failed, excluded, canceled, evicted, timedout, upstream_canceled, upstream_evicted
# and upstream_failed, and `excluded` is what the side of the condition that did not run
# reports. Same pass-through rule as every other reference: one the runtime does not recognise
# arrives as its own text, so this cannot invent a state it was not told.
#
# RECORDED, AND NEVER USED TO SUPPRESS A HOLE. A state this record could not learn is `None`,
# and a `None` must not read as "the task was fine" - which is the exact shape of the defect
# below. The hole is derived from the branch, which the record already knows; these states only
# let a reader say WHY it is there without opening the run page.
TASK_STATE_WIDGETS = {
    "close_month": "state_close_month",
    "verify_each_restated_month": "state_verify_each_restated_month",
    "verify_no_restatement": "state_verify_no_restatement",
}


def _state(widget: str) -> Any:
    """A sibling task's `result_state`, or None when the reference did not resolve."""
    raw = dbutils.widgets.get(widget).strip()
    if not raw or (raw.startswith("{{") and raw.endswith("}}")):
        return None
    return raw


task_states = {task: _state(widget) for task, widget in TASK_STATE_WIDGETS.items()}

# WHICH CHECKS THE BRANCH THAT RAN OWED, so that their ABSENCE is derivable.
#
# THE DEFECT THIS CLOSES, which nothing above can see. `unresolved_task_values` catches a failed
# `close_month`: that task published no values, the references arrive as their own text, and the
# record says so by name. It does NOT catch a failed VERIFICATION. When `verify_no_restatement`
# fails, `close_month` succeeded, every task value resolves, the branch is derived correctly -
# and `close_verification` holds zero rows for this job run, because the task that writes them
# died before writing any. The record then comes out IDENTICAL to a healthy one but for a
# section with no rows in it, and zero rows reads as "nothing to report".
#
# Same class as the one the fetch guard is written against: an absence carrying the weight of an
# assertion. In a repository whose documents are RENDERED from this record it is the more
# expensive half - `orch.checks_run = 0`, `orch.checks_failed = 0` is what a page would then
# have printed about a run whose verification never executed.
#
# So the branch is asked what it owed. The names below are the `check_name` literals the two
# verification notebooks SELECT, and `tests/fast/test_databricks_bundle.py` fails if this list
# and those files stop agreeing: a second copy of a fact is only safe while something compares
# it against the first.
CHECKS_BY_BRANCH = {
    "verify_each_restated_month": (
        "earlier_versions_are_older",
        "month_was_eligible_to_close",
        "net_is_gross_minus_returns",
        "versions_have_no_gaps",
        "written_equals_the_source_month",
    ),
    "verify_no_restatement": (
        "every_eligible_month_has_a_version",
        "no_eligible_month_drifted",
    ),
}


def _verification_holes(branch: str, months: Any, rows: Any) -> Any:
    """What the branch owed and what of it is not in the table: `(expected, missing)`.

    The true branch is checked PER MONTH, because `months_written` says exactly how many
    iterations the for_each had and one failed iteration leaves every other month's rows in
    place - a check on check NAMES alone would read that run as complete. The false branch is
    one task with one answer, over eligible months this notebook does not know, so it is checked
    on names.

    An `expected` that comes back empty means the branch itself is unknown, which the caller
    reports rather than reading as "nothing was owed".
    """
    checks = CHECKS_BY_BRANCH.get(str(branch), ())
    names, pairs = set(), set()
    if isinstance(rows, list):
        for row in rows:
            names.add(str(row.get("check_name")))
            pairs.add(f"{row.get('check_name')}:{row.get('accounting_month')}")
    if branch == "verify_each_restated_month" and isinstance(months, list) and months:
        expected = sorted(f"{check}:{month}" for check in checks for month in months)
        seen = pairs
    else:
        expected = sorted(checks)
        seen = names
    return expected, [name for name in expected if name not in seen]


# The per-month verdicts the two verification tasks wrote, for THIS run. The job run id filters
# them: the table accumulates across runs, and a record that published every row ever written
# would be describing the workspace's history rather than this run.
job_run_id_raw = dbutils.widgets.get("job_run_id")
if re.fullmatch(r"\d+", job_run_id_raw or ""):
    close_verification = _read(
        "close_verification",
        lambda: _rows(f"""
            SELECT check_name, accounting_month, close_version, ok, detail
            FROM {catalog}.main.close_verification
            WHERE job_run_id = '{job_run_id_raw}'
            ORDER BY check_name, accounting_month
        """),
    )
else:
    # The id is interpolated into SQL above, so it is checked against the shape it must have.
    # A `{{job.run_id}}` that did not resolve would otherwise become a query for rows nothing
    # wrote, which returns zero rows and looks like "every check passed".
    incomplete.append("close_verification")
    close_verification = {"error": f"job_run_id is not a run id: {job_run_id_raw!r}"}

expected_checks, missing_checks = _verification_holes(branch, close_months, close_verification)
if missing_checks:
    # NAMED BY THE TASK THAT OWED THEM, and not by the section. "close_verification" already
    # means "the table could not be read"; a verification that never ran is a different fact
    # from a query that failed, and collapsing two facts into one name is what this file exists
    # to refuse. `incomplete` is where the record names its holes, so the hole goes there.
    incomplete.append(branch)
elif not expected_checks:
    # The branch is unknown - `close_month` failed, or its decision did not resolve - so this
    # record cannot say what the verification owed, let alone whether it was paid. Which is
    # exactly when the section must not be readable as complete.
    incomplete.append("close_verification")

orchestration = [
    {
        "decision": close_decision,
        "months_written": close_months if isinstance(close_months, list) else None,
        "versions_written": close_versions if isinstance(close_versions, int) else None,
        "branch": branch,
        # Task values that did not resolve, by name. Non-empty means an upstream task did not
        # publish them - which under `run_if: ALL_DONE` is exactly the case this record exists
        # to describe rather than to hide.
        "unresolved_task_values": sorted(unresolved),
        # What the branch owed, and what of it is missing. `expected_checks` is published as
        # well as `missing_checks` because "nothing is missing" and "nothing was expected" are
        # two different sentences, and a reader - `samegold.evidence.databricks_doc` included -
        # has to be able to tell them apart before quoting a count of checks as a result.
        "expected_checks": expected_checks,
        "missing_checks": missing_checks,
        # What the runtime said each upstream task did. Explanation, never permission.
        "task_states": task_states,
    }
]

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

# THE THREE CHECKS THE RECORD COULD NOT ANSWER.
#
# `docs/databricks-run.md` carries a six-item checklist with an expected value beside every
# query, written before the run. When the first successful record was compared against it,
# every figure the record CARRIED matched - and three of the six items could not be checked at
# all, because nothing in the record spoke to them. They had to be read off a terminal by the
# person who ran it, which is the same standing as prose.
#
# A checklist and a record that do not cover the same ground is a gap that gets filled by
# somebody remembering. These are the three, and they cost one query each.

# 1. The money columns are integers. This is the defect the whole lane was rebuilt around -
#    Auto Loader inferred every column as STRING, `qty * unit_price_cents` promoted to DOUBLE,
#    and the close died writing a double into a BIGINT. `typeof()` reads the type off the table
#    itself, and unlike `DESCRIBE` it is a SELECT the parse tests can analyse.
column_types = _read(
    "column_types",
    lambda: _rows(f"""
        SELECT typeof(qty)              AS qty,
               typeof(new_qty)          AS new_qty,
               typeof(unit_price_cents) AS unit_price_cents
        FROM {catalog}.main.bronze_events LIMIT 1
    """),
)

money_types = _read(
    "money_types",
    lambda: _rows(f"""
        SELECT typeof(gross_cents)   AS gross_cents,
               typeof(returns_cents) AS returns_cents,
               typeof(net_cents)     AS net_cents
        FROM {catalog}.main.revenue_by_month LIMIT 1
    """),
)

# 2. The four events the generator emits in order to be rejected, by name. Two carry
#    Long.MaxValue - a legal BIGINT outside the contract's bound - and two carry 2^63, which
#    does not fit the column at all and is rescued, leaving it NULL. The per-reason totals can
#    match while these four are wrong; on the failed run two of them were `accepted` and the
#    totals looked plausible until `close_month` overflowed.
bad_events = _read(
    "bad_events",
    lambda: _rows(f"""
        SELECT event_id, qty, unit_price_cents, quarantine_reason, undecided_rules
        FROM {catalog}.main.silver_classified
        WHERE event_id IN ('bad-0000007', 'bad-0000008', 'bad-0000016', 'bad-0000017')
        ORDER BY event_id
    """),
)

# 3. How many rows arrived with a value too wide for their column. The reader nulls that one
#    field and copies the raw line into the rescue column, after which the record is
#    indistinguishable from one whose producer never sent the field - so the count is the only
#    trace that a value was LOST rather than absent.
rescued = _read(
    "rescued_rows",
    lambda: _rows(
        f"SELECT COUNT(*) AS n FROM {catalog}.main.bronze_events WHERE _rescued_data IS NOT NULL"
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

# THE DIMENSION ITSELF, row by row, and not only its six aggregates.
#
# `tests/fast/test_databricks_dimension_parity.py` compares AUTO CDC's output against the OSS
# lane's hand-written MERGE on the same seed. Half of that comparison cannot be computed outside
# a workspace, so it has to be captured here - and it was captured BY HAND for one run, which
# is a file that cannot say which run it came from. A later run replacing the record would have
# left it comparing green against rows nothing produced any more.
#
# So it is written by the same task, in the same session, from the same tables the aggregates
# above were read from. That is the point: the header below is not a field copied out of the
# record afterwards, it is what this run knows about itself.
#
# Lower case, and that is not a slip. `_module_namespace` in the parse test evaluates every
# ALL-CAPS module-level assignment in these lane files so that a SQL fragment built from
# another one cannot go unparsed, and it refuses to skip anything it cannot evaluate. This
# string interpolates `catalog`, which is a job widget and exists only in a workspace - so it
# belongs with the other query strings in this file, which are all built from widgets, and not
# with the literal constants that test can evaluate. The statement itself is still extracted
# and still parsed: `_sql_calls` follows the name to its assignment.
dimension_capture_sql = f"""
    SELECT customer_id, segment, country, __START_AT, __END_AT
    FROM {catalog}.main.dim_customer_scd2
    ORDER BY customer_id, __START_AT
"""
dimension_rows = _read("dim_customer_scd2_rows", lambda: _rows(dimension_capture_sql))

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
    # Which code produced all of this. `evidence/databricks/fetch.json` has carried a commit
    # since the lane first ran, but that one is written by the laptop AFTER the fact: it says
    # what HEAD was when somebody copied the files down, which is a different fact and can be
    # re-stamped onto stale files by a later fetch. This one travels with the deploy.
    "deploy": {"commit": deploy_commit, "tree_dirty": tree_dirty},
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
    # Ten terminal updates, most recent first. A retry loop is a shape, not a number, and it
    # is invisible in a record that describes one update.
    "update_history": update_history,
    "expectations": expectations,
    "quarantine_by_reason": quarantine,
    "undecided_rules": undecided,
    "column_types": column_types,
    "money_types": money_types,
    "bad_events": bad_events,
    "rescued_rows": rescued,
    "gross_within_contract_bounds": bounds,
    "rows": rows,
    # WHICH events those rows are, and not only how many. See the statement above.
    "population": population,
    # What the JOB did: the decision, the branch it took, and the per-month verdicts.
    "orchestration": orchestration,
    "close_verification": close_verification,
    "dim_customer_scd2": dimension,
    "revenue_closed": close,
    # Named sections that could not be read. An empty list is a stronger statement than a
    # record with holes in it that nothing points at.
    "incomplete": sorted(set(incomplete)),
}
payload = json.dumps(record, indent=2, sort_keys=True, default=str)
print(payload)

# COMMAND ----------
# THE TASK VALUE THAT USED TO BE SET HERE IS GONE.
#
# `dbutils.jobs.taskValues.set("evidence", payload)` published the whole record as a task
# value, and nothing has ever read it. This is the last task in the job, so nothing in the
# graph CAN read it - a value written for a reader that cannot exist, which is the same class
# as a message announcing what it does not do. The record's actual delivery is the file written
# to the evidence volume above and copied down by `scripts/databricks_run.sh fetch`.
#
# It was also a latent bug: a task value is capped at 48 KiB and this payload is the entire
# record, 7 KB today and growing with every section added to it.
if out_dir:
    with open(f"{out_dir}/SG-DBX-01.json", "w", encoding="utf-8") as handle:
        handle.write(payload)
    print(f"wrote {out_dir}/SG-DBX-01.json")

# COMMAND ----------
# The capture, with a header that names the run rather than describing it.
#
# `update_id` is the same value the record publishes because it is the same read, in the same
# session, of the same event log - which is what makes it provenance rather than a copy. The
# thing it protects against is the file outliving the run: a later update replaces
# SG-DBX-01.json, this file does not change, and the comparison that reads it goes on passing
# against rows the workspace no longer holds. Then the two update ids differ and the test says
# so, by name, with the query.
#
# `query` is in the header because a reader who wants to check these rows needs the statement
# that produced them, and reading it out of this file is one step; finding it in a notebook in
# a repository is several.

# The WORKSPACE's clock, not the fetching machine's - and guarded, because this cell runs after
# the record has already been written and a failure here must not be what loses the capture.
# `_read` answers with a dict describing the error rather than raising, so the shape is checked
# rather than indexed into: a header whose captured_at is an error object would be a header that
# looks measured and is not.
_now = _read("captured_at", lambda: _rows("SELECT current_timestamp() AS now"))
captured_at = _now[0].get("now") if isinstance(_now, list) and _now else None
capture_update = update_state[0] if isinstance(update_state, list) and update_state else {}
capture = {
    "capture": "dim_customer_scd2",
    "provenance": {
        "measured_in_the_workspace": True,
        "update_id": capture_update.get("update_id") if capture_update else None,
        "pipeline_id": pipeline_id,
        "job_run_id": dbutils.widgets.get("job_run_id"),
        "task_run_id": dbutils.widgets.get("task_run_id"),
        "commit": deploy_commit,
        "tree_dirty": tree_dirty,
        "captured_at": captured_at,
        "catalog": catalog,
    },
    "query": " ".join(dimension_capture_sql.split()),
    "rows": dimension_rows,
}
if out_dir:
    with open(f"{out_dir}/dim_customer_scd2.json", "w", encoding="utf-8") as handle:
        handle.write(json.dumps(capture, indent=2, sort_keys=True, default=str))
    print(f"wrote {out_dir}/dim_customer_scd2.json")
