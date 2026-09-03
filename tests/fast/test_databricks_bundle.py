"""The Databricks bundle, checked against the Free Edition limits it has to live inside.

The lane was written, parsed, type-checked and reviewed, and never deployed. Everything a
reader can check about it without an account, they should be able to check here; everything
they cannot, `docs/limits.md` has to say out loud. This file is the first half.

The rules below are not style. Each one is a way the first deploy or the first run fails, and
several of them are defects this file was written by finding:

  * the pipeline read `/Volumes/samegold/raw/landing` and nothing in the bundle created it;
  * the notebook tasks read their catalog from `spark.conf`, which a pipeline's
    `configuration:` block populates for the pipeline's own sources and for nothing else;
  * the schedule was deployed UNPAUSED, on an account where going over the daily quota stops
    all compute until midnight.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks"
BUNDLE = yaml.safe_load((LANE / "databricks.yml").read_text(encoding="utf-8"))
RESOURCES = {
    path.name: yaml.safe_load(path.read_text(encoding="utf-8"))
    for path in sorted((LANE / "resources").glob("*.yml"))
}
RUN_DOC = REPO / "docs" / "databricks-run.md"
RECORD = REPO / "evidence" / "databricks" / "SG-DBX-01.json"


def _merged() -> dict[str, Any]:
    """The bundle as the CLI assembles it: the target's resources plus every include."""
    out: dict[str, dict[str, Any]] = {}
    trees = [BUNDLE["targets"]["free"].get("resources", {})]
    trees.extend(document.get("resources", {}) for document in RESOURCES.values())
    for tree in trees:
        for kind, declared in tree.items():
            out.setdefault(kind, {}).update(declared)
    return out


MERGED = _merged()
PIPELINES = MERGED.get("pipelines", {})
JOBS = MERGED.get("jobs", {})
VOLUMES = MERGED.get("volumes", {})
SCHEMAS = MERGED.get("schemas", {})


# ------------------------------------------------------------------ the Free Edition limits


def test_there_is_exactly_one_pipeline() -> None:
    """One active pipeline per pipeline type. A second one cannot start."""
    assert len(PIPELINES) == 1, sorted(PIPELINES)


def test_the_pipeline_is_serverless_and_triggered() -> None:
    """Serverless is the only compute, and it refuses a time-based streaming trigger.

    `continuous: true` there fails with INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED, and an
    always-on pipeline would spend the daily quota by lunchtime whatever it did.
    """
    pipeline = next(iter(PIPELINES.values()))
    assert pipeline.get("serverless") is True
    assert pipeline.get("continuous") is False
    # Serverless compute IS Photon; declaring it is a claim about the deploy the deploy does
    # not honour, and a rejected field is a failed deploy.
    assert "photon" not in pipeline
    # A production pipeline retries a failed update. On Free Edition that is the quota.
    assert pipeline.get("development") is True


def test_the_schedule_is_deployed_paused() -> None:
    """The one setting that can take the whole account down while nobody is watching.

    It was UNPAUSED, on a 03:00 nightly cron. Exceeding the Free Edition quota stops all
    compute for the rest of the day, so an unattended schedule is not a convenience here: it
    is the thing that makes every other lane on this account unavailable tomorrow morning.
    """
    for name, job in JOBS.items():
        schedule = job.get("schedule")
        if schedule is not None:
            assert schedule.get("pause_status") == "PAUSED", name


def test_no_job_can_exceed_the_concurrent_task_ceiling() -> None:
    """Five concurrent job tasks per account."""
    for name, job in JOBS.items():
        tasks = job.get("tasks", [])
        assert len(tasks) <= 5, f"{name} has {len(tasks)} tasks"
        assert job.get("max_concurrent_runs", 1) == 1, name


def test_nothing_needs_a_sql_warehouse() -> None:
    """One 2X-Small warehouse exists, but the bundle cannot create it or learn its id.

    A `sql_task` needs `warehouse_id`, which is not a bundle resource here, so a task that
    used one could not be deployed from a clean account. That is why
    `databricks/sql/policies.sql` is declared and not applied, and docs/limits.md says so.
    """
    text = json.dumps({"bundle": BUNDLE, "resources": RESOURCES})
    assert "sql_task" not in text
    assert "warehouse_id" not in text


def test_no_volume_asks_for_an_external_location() -> None:
    """Free Edition has none, so a volume with a storage_location cannot be created."""
    assert VOLUMES, "the bundle declares no volumes at all"
    for name, volume in VOLUMES.items():
        assert "storage_location" not in volume, name
        assert volume.get("volume_type", "MANAGED") == "MANAGED", name


def test_the_target_does_not_pin_a_host() -> None:
    """`host:` in the target would override DATABRICKS_HOST and hardcode one workspace."""
    assert "host" not in BUNDLE["targets"]["free"]


# ------------------------------------------- the things that make the first deploy work


def test_every_volume_path_in_the_bundle_is_a_volume_the_bundle_creates() -> None:
    """The defect this file exists for.

    `samegold.landing: /Volumes/samegold/raw/landing` was read by the Auto Loader source, and
    nothing in the bundle created that volume: `bundle deploy` succeeded and the first update
    failed on a path that had never been declared anywhere. A path is only as deployed as the
    resource behind it.
    """
    declared = {
        f"/Volumes/{volume['catalog_name']}/{volume['schema_name']}/{volume['name']}".replace(
            "${var.catalog}", str(BUNDLE["variables"]["catalog"]["default"])
        )
        for volume in VOLUMES.values()
    }
    text = json.dumps({"bundle": BUNDLE, "resources": RESOURCES})
    used = {
        path.replace("${var.catalog}", str(BUNDLE["variables"]["catalog"]["default"]))
        for path in re.findall(r"/Volumes/[A-Za-z0-9_${}./-]+", text)
    }
    orphans = sorted(path for path in used if not any(path.startswith(v) for v in declared))
    assert not orphans, f"paths under no declared volume: {orphans}; declared: {sorted(declared)}"


def test_every_schema_a_volume_lives_in_is_declared() -> None:
    names = {schema.get("name") for schema in SCHEMAS.values()}
    for name, volume in VOLUMES.items():
        assert volume["schema_name"] in names, f"volume {name} is in an undeclared schema"


def test_the_catalog_is_referenced_through_the_variable_everywhere() -> None:
    """One source of truth, or `--var catalog=other` deploys half a lane into the wrong place.

    The landing path used to spell the catalog out (`/Volumes/samegold/raw/landing`) beside a
    `catalog: ${var.catalog}` two lines above it, so overriding the variable moved the tables
    and left the data behind.
    """
    default = str(BUNDLE["variables"]["catalog"]["default"])
    text = json.dumps({"bundle": BUNDLE, "resources": RESOURCES})
    # The variable's own declaration is the one legitimate mention of the literal name.
    text = text.replace(json.dumps(BUNDLE["variables"]), "")
    assert f'"{default}."' not in text
    assert f"/Volumes/{default}/" not in text, (
        "the catalog name is spelled out in a path; use ${var.catalog} so one override "
        "moves the whole lane"
    )


def test_every_variable_whose_default_means_nobody_said_is_said_by_the_deploy() -> None:
    """A default of "unknown" is a hole the deploy has to fill, or the record inherits it.

    `deploy_commit` exists so that what the run publishes can name the code that produced it.
    Declared and not passed, it resolves to "unknown" on every deploy, the record publishes
    "unknown", and nothing anywhere goes red - the field is simply always the same and always
    useless. That is the failure mode of every value with a plausible default, and it is why
    the catalog was read from `spark.conf` for four rounds.

    So the rule is a property of the DECLARATION: a variable whose default is the word
    "unknown" is one nobody can supply after the fact, and `scripts/databricks_run.sh` must
    pass it. A test in test_databricks_catalog_step.py then runs the script and reads the argv;
    this one is what makes a NEW variable of that shape fail until somebody wires it.
    """
    script = (REPO / "scripts" / "databricks_run.sh").read_text(encoding="utf-8")
    unsupplied = sorted(
        name
        for name, declaration in BUNDLE.get("variables", {}).items()
        if str(declaration.get("default")) == "unknown"
        and f'--var="{name}=' not in script
        and f"--var={name}=" not in script
    )
    assert not unsupplied, (
        f'these bundle variables default to "unknown" and nothing passes them, so every '
        f"deploy publishes that word: {unsupplied}. Either the deploy supplies it or the "
        f"default should say what it really is."
    )


def test_the_notebook_validates_the_commit_it_is_handed() -> None:
    """A widget that is not checked is a widget that publishes whatever it was given.

    `${var.deploy_commit}` surviving into the record as its own text is what a variable that
    did not resolve looks like, and it would sit in the provenance header of every capture
    looking like provenance. The notebook already refuses a catalog that is not an identifier
    and a pipeline id that is not a uuid, for the same reason.
    """
    source = (LANE / "src" / "publish_evidence.py").read_text(encoding="utf-8")
    assert "deploy_commit" in source, "the notebook does not read the commit at all"
    assert "unknown|[0-9a-f]{40}" in source, (
        "the notebook does not check the shape of deploy_commit, so a value that is neither a "
        "sha nor the word 'unknown' would be published as provenance"
    )


@pytest.mark.parametrize(
    "path",
    sorted(
        {
            *(
                library["file"]["path"]
                for pipeline in PIPELINES.values()
                for library in pipeline.get("libraries", [])
                if "file" in library
            ),
        }
    ),
)
def test_every_pipeline_library_exists(path: str) -> None:
    """`../src` from the bundle root pointed outside the bundle at nothing."""
    assert (LANE / path).is_file(), f"{path} does not resolve from databricks/"


@pytest.mark.parametrize(
    ("job_name", "task_key", "notebook"),
    [
        (job_name, task["task_key"], task["notebook_task"]["notebook_path"])
        for job_name, job in JOBS.items()
        for task in job.get("tasks", [])
        if "notebook_task" in task
    ],
)
def test_every_notebook_task_points_at_a_file(job_name: str, task_key: str, notebook: str) -> None:
    assert (LANE / "resources" / notebook).resolve().is_file(), f"{job_name}/{task_key}"


# ---------------------------------------------- pipeline configuration versus job parameters


def _widgets(source: str) -> set[str]:
    """Every `dbutils.widgets.text("name", ...)` a notebook declares, including in a loop."""
    names: set[str] = set()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "text" or not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            names.add(first.value)
        elif isinstance(first, ast.Name):
            # `for w in (...): dbutils.widgets.text(w, "")` - read the tuple it iterates.
            for loop in ast.walk(tree):
                if isinstance(loop, ast.For) and isinstance(loop.target, ast.Name):
                    if loop.target.id != first.id:
                        continue
                    if isinstance(loop.iter, ast.Tuple | ast.List):
                        names.update(
                            element.value
                            for element in loop.iter.elts
                            if isinstance(element, ast.Constant) and isinstance(element.value, str)
                        )
    return names


NOTEBOOK_TASKS = [
    (task["task_key"], task["notebook_task"])
    for job in JOBS.values()
    for task in job.get("tasks", [])
    if "notebook_task" in task
]


@pytest.mark.parametrize(("task_key", "task"), NOTEBOOK_TASKS, ids=[t for t, _ in NOTEBOOK_TASKS])
def test_every_widget_a_notebook_reads_is_a_parameter_the_job_passes(
    task_key: str, task: dict[str, Any]
) -> None:
    """A widget with no parameter behind it silently takes its default, for ever.

    This is the shape of the bug that made both notebook tasks read the catalog from
    `spark.conf`: a value that LOOKS configured, is not, and produces a plausible number in
    the wrong place rather than an error anyone would see.
    """
    source = (LANE / "resources" / task["notebook_path"]).resolve().read_text(encoding="utf-8")
    passed = set(task.get("base_parameters", {}))
    missing = sorted(_widgets(source) - passed)
    assert not missing, f"{task_key} reads widgets the job does not pass: {missing}"


def _conf_keys(source: str) -> set[str]:
    """Every literal key read through `spark.conf.get`, found with the Python parser.

    A regex over the text matched the sentence in close_month.py that EXPLAINS why the call
    was removed. A construct named in a comment is not a construct - the same trap, and the
    same fix, as `_is_databricks_only` in tests/spark/test_databricks_lane_parses.py.
    """
    keys: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        chain = node.func
        if chain.attr != "get" or not isinstance(chain.value, ast.Attribute):
            continue
        if chain.value.attr != "conf":
            continue
        first = node.args[0] if node.args else None
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            keys.add(first.value)
    return keys


def test_no_notebook_task_reads_the_pipelines_configuration() -> None:
    """`spark.conf.get("samegold.*")` in a notebook is always the default.

    A pipeline's `configuration:` block reaches the pipeline's own source files. A notebook
    task in the job is a different process with no pipeline around it, so both notebooks were
    reading a key nothing had set - one of them to decide which catalog to write the
    signed-off close into, the other to build an `event_log('')` call out of a pipeline id
    that only exists inside a pipeline.
    """
    for task_key, task in NOTEBOOK_TASKS:
        source = (LANE / "resources" / task["notebook_path"]).resolve().read_text(encoding="utf-8")
        leaked = sorted(
            key for key in _conf_keys(source) if key.startswith(("samegold.", "pipelines."))
        )
        assert not leaked, (
            f"{task_key} reads {leaked} from spark.conf, which a notebook task cannot see; "
            f"pass it in base_parameters and read it from a widget"
        )


def test_the_pipeline_sources_only_read_configuration_the_bundle_declares() -> None:
    """The mirror image: a pipeline source may use spark.conf, but only for declared keys."""
    pipeline = next(iter(PIPELINES.values()))
    declared = set(pipeline.get("configuration", {}))
    for library in pipeline.get("libraries", []):
        source = (LANE / library["file"]["path"]).read_text(encoding="utf-8")
        for key in _conf_keys(source):
            if not key.startswith("samegold."):
                continue
            assert key in declared, (
                f"{library['file']['path']} reads {key}, which databricks.yml does not declare "
                f"in the pipeline's configuration: block, so it always takes its default"
            )


# ------------------------------------------------------------------ the run document


ANCHOR = re.compile(r"<!--dbx:([\w.]+)-->(.*?)<!--/dbx-->", re.DOTALL)
# A closed list, so an inconvenient figure cannot be removed from the document by deleting
# its anchor: the run has to fill these in or the document has to stop claiming them.
REQUIRED_ANCHORS = {
    "update.last_state",
    "update.error_events",
    "rows.bronze_events",
    "rows.silver_classified",
    "rows.silver_events",
    "rows.silver_quarantine",
    "rows.dim_customer_scd2",
    "rows.revenue_by_month",
    "rows.revenue_closed",
    "dim.versions",
    "dim.customers",
    "dim.open_rows",
    "dim.closed_rows",
    "expectations.table",
    "quarantine.table",
}
# Documents that may quote the record. The run document must carry the whole closed list above;
# any other may carry a SUBSET, and every anchor it does carry has to agree.
#
# The README was the reason for this. Its Databricks section held nine figures typed in by hand
# from a terminal - 14 198 046, 425, 755, 727, 28, 75, 60 - and the renderer's hand-typed-number
# check never looked at them, because that check fires on lines carrying an `SG-nn` claim id and
# `SG-DBX-01` is not one. So the most-read page in the repository was the one place a run's
# figures could go stale silently.
QUOTING_DOCUMENTS = (RUN_DOC, REPO / "README.md")


def _anchors(document: Path = RUN_DOC) -> dict[str, str]:
    return {
        name: body.strip() for name, body in ANCHOR.findall(document.read_text(encoding="utf-8"))
    }


def _matches(body: str, value: Any) -> bool:
    """Whether an anchor body states `value`.

    Digit groups are separated for reading - `14 198 046` - and the record holds `14198046`,
    so the spaces are removed before comparing. ONLY the spaces, and only when what is left is
    entirely digits: a normalisation that can turn two different values into the same string is
    the defect this repository found in its own dimension comparison one round ago, and
    deleting spaces from a number cannot change which number it is.
    """
    stripped = body.replace(" ", "").replace("\u00a0", "").replace("\u202f", "")
    if stripped.isdigit():
        return stripped == str(value)
    return body == str(value)


def test_the_run_document_carries_every_anchor_it_is_supposed_to() -> None:
    assert set(_anchors()) == REQUIRED_ANCHORS, sorted(set(_anchors()) ^ REQUIRED_ANCHORS)


def test_the_run_document_holds_no_figure_the_run_has_not_produced() -> None:
    """The whole point of the anchors, and the lesson of round twelve in one assertion.

    While `evidence/databricks/SG-DBX-01.json` does not exist, nothing in this repository has
    ever deployed the lane, and every figure in the document must say so. A number typed in by
    hand ahead of the run is exactly what "not executed here" was.
    """
    if RECORD.exists():
        pytest.skip("the lane has run; test_the_run_document_agrees_with_the_record checks it")
    # Every document that quotes the record, not only the run document. The README acquired
    # dbx anchors after the lane had already run, so this branch has never executed against it;
    # a check that covers one of two files is how the second one gets ahead of its run.
    wrong = {
        f"{document.name}:{name}": body
        for document in QUOTING_DOCUMENTS
        for name, body in _anchors(document).items()
        if body != "NOT RUN"
    }
    assert not wrong, (
        f"these anchors hold values while {RECORD.relative_to(REPO)} does not exist, so no run "
        f"produced them: {wrong}"
    )


def test_the_run_document_agrees_with_the_record() -> None:
    """Once the run has happened, the document may only say what the record says."""
    if not RECORD.exists():
        pytest.skip("the lane has not been deployed yet")
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    update = record.get("update") or [{}]
    scalars: dict[str, Any] = {}
    if isinstance(update, list) and update:
        scalars["update.last_state"] = update[0].get("last_state")
        scalars["update.error_events"] = update[0].get("error_events")
    for table, count in (record.get("rows") or {}).items():
        scalars[f"rows.{table}"] = count
    dimension = record.get("dim_customer_scd2") or [{}]
    if isinstance(dimension, list) and dimension:
        for field in ("versions", "customers", "open_rows", "closed_rows"):
            scalars[f"dim.{field}"] = dimension[0].get(field)
    # `2026-01` is not a legal anchor name (`[\w.]+`), so the month is spelled with an
    # underscore. Derived from the record's own accounting_month rather than positionally: a
    # run over different months would produce anchors nothing in the documents claims, which
    # the closed-list check below turns into a failure rather than a silent pass.
    for row in record.get("gross_within_contract_bounds") or []:
        if not isinstance(row, dict) or not row.get("accounting_month"):
            continue
        month = str(row["accounting_month"]).replace("-", "_")
        scalars[f"revenue.{month}.gross_cents"] = row.get("gross_cents")
        scalars[f"revenue.{month}.line_count"] = row.get("line_count")

    for document in QUOTING_DOCUMENTS:
        anchors = _anchors(document)
        unknown = sorted(set(anchors) - set(scalars) - {"expectations.table", "quarantine.table"})
        assert not unknown, (
            f"{document.name} carries dbx anchors the record cannot answer: {unknown}. An "
            f"anchor nothing checks is a hand-typed number with extra punctuation."
        )
        for name, value in scalars.items():
            if value is None or name not in anchors:
                continue  # a hole the record names in `incomplete`, or a figure this document
                # does not quote - only the run document has to quote them all
            assert _matches(anchors[name], value), (
                f"{document.name} {name}: document says {anchors[name]!r}, record says {value!r}"
            )

    # The two tables are pasted rather than anchored per cell, so what is checked is that every
    # row of the record survived the paste. Only the run document carries them.
    expectations = record.get("expectations")
    if isinstance(expectations, list):
        block = _anchors()["expectations.table"]
        for row in expectations:
            assert str(row["rule"]) in block, f"rule {row['rule']} is missing from the table"
            assert str(row["passed"]) in block and str(row["failed"]) in block, row


# ------------------------------------------ the fields the API requires and validate does not
#
# `databricks bundle validate -t free` answered `Validation OK!` on a bundle whose very first
# POST came back `name must be set (400 INVALID_PARAMETER_VALUE)`, and the CI job that runs
# validate as its default action had been green on that bundle for a round. Validate checks
# syntax, `include:` and variable resolution, and warns about properties it does not
# recognise. It does not check that the request body it is about to send is one the API will
# accept, and the bundle reference says as much: a resource declaration "uses the
# corresponding object's create operation's request payload", and what that payload requires
# is documented in the REST API reference, not enforced by the linter.
#
# So the required fields are asserted here, from that reference, for every resource type this
# bundle declares - not just for the one that failed. A test written to the shape of the
# defect that was found is a test that finds that defect again and nothing else.
REQUIRED_FIELDS = {
    # POST /api/2.0/pipelines
    "pipelines": ("name",),
    # POST /api/2.2/jobs/create
    "jobs": ("name", "tasks"),
    # POST /api/2.1/unity-catalog/schemas
    "schemas": ("name", "catalog_name"),
    # POST /api/2.1/unity-catalog/volumes
    "volumes": ("name", "catalog_name", "schema_name"),
}


@pytest.mark.parametrize("kind", sorted(REQUIRED_FIELDS))
def test_every_resource_carries_the_fields_its_create_api_requires(kind: str) -> None:
    """The key a resource is declared under is the BUNDLE's id for it, not a name field.

    That is the whole trap: `resources.pipelines.samegold_pipeline` reads like a name, is not
    one, and nothing between the editor and the workspace said so.
    """
    declared = MERGED.get(kind, {})
    missing = {
        resource_id: [field for field in REQUIRED_FIELDS[kind] if field not in resource]
        for resource_id, resource in declared.items()
        if any(field not in resource for field in REQUIRED_FIELDS[kind])
    }
    assert not missing, (
        f"these {kind} are missing fields their create API requires: {missing}. "
        f"`databricks bundle validate` passes on this and the deploy fails on the first POST."
    )


def test_the_pipeline_sets_exactly_one_of_schema_and_target() -> None:
    """An either/or the API enforces at POST time and validate does not read.

    "Exactly one of `schema` or `target` must be specified" - the create-pipeline reference.
    Both, or neither, is a 400 on a bundle that validated cleanly.
    """
    for name, pipeline in PIPELINES.items():
        present = [field for field in ("schema", "target") if field in pipeline]
        assert len(present) == 1, f"{name} sets {present or 'neither'} of schema/target"


def test_every_resource_type_the_bundle_declares_has_a_required_field_rule() -> None:
    """A new resource type must be looked up in the reference before it can be deployed.

    Without this, the check above silently covers only the four types that existed when it was
    written - which is how `NOT_ANALYSABLE` in the Spark lane stopped covering a statement,
    and how `ruff check src tests` stopped covering two directories.
    """
    unknown = sorted(set(MERGED) - set(REQUIRED_FIELDS))
    assert not unknown, (
        f"the bundle declares {unknown}, and nothing here says what their create API "
        f"requires. Read the REST API reference for each and add it to REQUIRED_FIELDS."
    )


# ------------------------------------------------------------------ the deploy script
#
# `scripts/databricks_run.sh` is the one command a reader is told to run, so what it does is
# checked here rather than left to be discovered against a real workspace. These assertions
# are about the SHAPE of the calls, which is all a test with no account can see; the calls
# themselves were found by a deploy that failed.
SCRIPT = (REPO / "scripts" / "databricks_run.sh").read_text(encoding="utf-8")
# A construct named in a comment is not a construct - the same rule, and the same reason, as
# `_is_databricks_only` in tests/spark/test_databricks_lane_parses.py. The script explains at
# length why it does NOT call `catalogs create`, and that sentence must not read as the call.
CODE = "\n".join(line for line in SCRIPT.splitlines() if not line.lstrip().startswith("#"))


def test_the_catalog_is_not_created_through_the_unity_catalog_api() -> None:
    """`databricks catalogs create` cannot work on Free Edition.

    It goes to the Unity Catalog API, which wants a storage root on the metastore. Free
    Edition uses Default Storage and has none, so it fails with `Metastore storage root URL
    does not exist` (databricks/cli#4513). This was found by running the deploy, not by
    reading it: the call looks perfectly reasonable.
    """
    assert "catalogs create" not in CODE, (
        "the script calls `databricks catalogs create`, which cannot succeed on a Free "
        "Edition metastore; create the catalog with SQL instead"
    )


def test_the_catalog_is_created_through_the_sql_statements_api() -> None:
    """The path that works with Default Storage, and a wait that can outlast a cold start.

    `wait_timeout` accepts "0s" or "5s" to "50s", so NO value of it covers a serverless
    warehouse booting, which takes 40s to 2 minutes and is the NORMAL case on Free Edition.
    The first version asked for 30s with `on_wait_timeout: CANCEL`, and the wait expired on a
    cold warehouse. The answer is not a bigger number; it is CONTINUE plus polling.
    """
    assert "/api/2.0/sql/statements" in CODE
    assert "CREATE CATALOG IF NOT EXISTS" in CODE
    assert '"on_wait_timeout": "CONTINUE"' in CODE
    assert '"on_wait_timeout": "CANCEL"' not in CODE, (
        "CANCEL ends the client's wait and reports CANCELED while the DDL the warehouse has "
        "already admitted goes on running - which is how this step reported a failure that "
        "had in fact created the catalog"
    )
    assert "api get" in CODE and "/api/2.0/sql/statements/" in CODE
    assert "SAMEGOLD_SQL_TIMEOUT_SECONDS:-300" in CODE
    assert "SAMEGOLD_SQL_POLL_SECONDS:-5" in CODE


def test_no_failure_path_asserts_the_world_without_looking_again() -> None:
    """The rule this round is about, as a property of the code.

    CANCELED does not mean "it did not happen", it means "I stopped waiting". Every exit from
    a non-SUCCEEDED state has to re-ask `catalogs get` before it says anything, and the same
    goes for the fetch step, where a failed `fs cp` was being reported as a missing record.
    """
    assert "catalog_exists()" in CODE
    body = SCRIPT.split("statement ended in state", 1)[1].split("    die ", 1)[0]
    assert "catalog_exists" in body, (
        "the non-success path reaches its die without re-checking whether the catalog exists"
    )
    fetch = SCRIPT.split("step_fetch()", 1)[1].split(chr(10) + chr(125), 1)[0]
    assert "fs ls" in fetch, "a failed `fs cp` is still being read as a missing record"


def test_the_failure_message_is_generated_from_the_state_that_was_observed() -> None:
    """A hand-written taxonomy that does not cover what it prints is worse than none.

    The list that came before this one enumerated PENDING, RUNNING and FAILED. The state it
    printed on the day it mattered was CANCELED, which was not in it - the same shape as
    round 14's by-ordinal exclusion list.
    """
    assert "explain_statement_state" in CODE
    explain = SCRIPT.split("explain_statement_state() {", 1)[1].split(chr(10) + chr(125), 1)[0]
    for state in ("TIMED_OUT_WAITING", "FAILED", "CANCELED", "CLOSED", "PENDING | RUNNING"):
        assert state in explain, f"{state} has no note, and this script can print it"
    assert "no note about" in explain


def test_the_catalog_name_is_validated_before_it_reaches_sql() -> None:
    """It is interpolated into an identifier position, which cannot be parameterised."""
    assert "^[A-Za-z_][A-Za-z0-9_]*$" in CODE


def test_authentication_accepts_a_configured_profile() -> None:
    """The script must not be stricter than the CLI it drives.

    `~/.databrickscfg` is how `databricks configure` stores credentials, and a machine set up
    that way is correctly set up. Requiring the two environment variables aborted on it. The
    question asked first is now "can the CLI authenticate", and the variables are only named
    when the answer is no - where their absence is almost always the reason.
    """
    body = SCRIPT.split("require_auth()", 1)[1].split("\n}", 1)[0]
    checks = [
        line.strip()
        for line in body.splitlines()
        if "current-user me" in line or "DATABRICKS_HOST" in line
    ]
    assert checks, "require_auth checks nothing"
    assert "current-user me" in checks[0], (
        f"require_auth tests the environment before it tests whether the CLI can "
        f"authenticate, so a valid ~/.databrickscfg profile is rejected: {checks[0]}"
    )


# ------------------------------------------------ the types, from one declaration


# READ, not imported. `bronze_schema()` builds a pyspark StructType, and importing pyspark in
# the FAST lane breaks the one promise that lane makes: no JVM, no Spark, no network. The
# first version of these two tests called it, passed on both machines it was written on -
# because both have the spark extras installed - and turned the `fast` workflow red on the
# push, which is `tests/fast/test_architecture.py`'s own rule violated by a test.
#
# The declaration is a literal list of `StructField("name", Type(), ...)`, so the parser can
# read it without executing it. That keeps the single source (the same file the OSS reader
# uses) and costs nothing.
_SPARK_TO_SQL = {"StringType": "STRING", "LongType": "BIGINT", "TimestampType": "TIMESTAMP"}


def _declared_bronze_types() -> dict[str, str]:
    """{column: SQL type} from `samegold.pipelines.schema.bronze_schema`, by parsing it."""
    source = (REPO / "src" / "samegold" / "pipelines" / "schema.py").read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        if node.func.id != "StructField" or len(node.args) < 2:
            continue
        name, spark_type = node.args[0], node.args[1]
        if not (isinstance(name, ast.Constant) and isinstance(name.value, str)):
            continue
        # `StructField(RESCUED_COLUMN, StringType(), True)` names its column by constant.
        if not (isinstance(spark_type, ast.Call) and isinstance(spark_type.func, ast.Name)):
            continue
        out[name.value] = _SPARK_TO_SQL[spark_type.func.id]
    # The rescued column is declared through a module constant rather than a literal.
    if "_rescued_data" not in out and "RESCUED_COLUMN" in source:
        out["_rescued_data"] = "STRING"
    assert len(out) > 10, f"only parsed {len(out)} fields out of the bronze schema"
    return out


def test_the_auto_loader_hints_are_the_declared_bronze_schema() -> None:
    """One schema, two consumers, and a test that breaks if they drift.

    Auto Loader reading JSON with no `cloudFiles.schemaHints` infers every column as STRING,
    and on the first real deployment it did: `DESCRIBE silver_classified` came back with 21
    STRING columns, `gross_cents` came out DOUBLE because `qty * unit_price_cents` on two
    strings promotes to double, and `close_month` died writing that double into a BIGINT.

    The OSS lane never had the problem because it DECLARES the schema
    (`samegold.pipelines.schema.bronze_schema`). That declaration is now the single source:
    the hints in `databricks/src/bronze_autoloader.py` must be the same fields with the same
    types, so changing one and not the other fails here rather than in a workspace.
    """
    source = (LANE / "src" / "bronze_autoloader.py").read_text(encoding="utf-8")
    block = source.split("SCHEMA_HINTS = (", 1)[1].split(")", 1)[0]
    hinted = {
        part.strip().split()[0]: part.strip().split()[1].upper()
        for part in "".join(re.findall(r'"([^"]*)"', block)).split(",")
        if part.strip()
    }
    # `_rescued_data` is Auto Loader's own column and is not hinted; everything else is.
    declared = {
        name: sql_type
        for name, sql_type in _declared_bronze_types().items()
        if name != "_rescued_data"
    }
    mismatched = {
        name: (hinted[name], declared[name])
        for name in hinted.keys() & declared.keys()
        if hinted[name] != declared[name]
    }
    assert hinted == declared, (
        f"the Auto Loader hints and the declared bronze schema disagree.\n"
        f"  only in the hints:  {sorted(set(hinted) - set(declared))}\n"
        f"  only in the schema: {sorted(set(declared) - set(hinted))}\n"
        f"  different types:    {mismatched}"
    )


def test_the_money_columns_are_declared_as_integers() -> None:
    """The thesis of the repository, asserted where it can be violated silently.

    Money is an integer number of cents. A STRING column multiplied by a STRING column is a
    DOUBLE, and floating point money in an accounting pipeline is the defect this project
    exists to argue against - it reached production on this lane and nothing said a word.
    """
    types = _declared_bronze_types()
    for column in ("qty", "new_qty", "unit_price_cents"):
        assert types[column] == "BIGINT", f"{column} is {types[column]}, not BIGINT"
