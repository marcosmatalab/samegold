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


def _anchors() -> dict[str, str]:
    return {
        name: body.strip() for name, body in ANCHOR.findall(RUN_DOC.read_text(encoding="utf-8"))
    }


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
    wrong = {name: body for name, body in _anchors().items() if body != "NOT RUN"}
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

    anchors = _anchors()
    for name, value in scalars.items():
        if value is None:
            continue  # the record says that section could not be read; `incomplete` names it
        assert anchors[name] == str(value), (
            f"{name}: document says {anchors[name]!r}, record says {value!r}"
        )

    # The two tables are pasted rather than anchored per cell, so what is checked is that every
    # row of the record survived the paste.
    expectations = record.get("expectations")
    if isinstance(expectations, list):
        block = anchors["expectations.table"]
        for row in expectations:
            assert str(row["rule"]) in block, f"rule {row['rule']} is missing from the table"
            assert str(row["passed"]) in block and str(row["failed"]) in block, row
