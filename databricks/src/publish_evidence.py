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
