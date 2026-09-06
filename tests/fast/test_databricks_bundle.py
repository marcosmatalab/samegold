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
import itertools
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from samegold.evidence.databricks_doc import NOT_RUN, scalars_from, tables_from
from samegold.generator.late import (
    BRONZE_DIGEST_BIGINT_COLUMNS,
    BRONZE_DIGEST_COLUMNS,
    BRONZE_DIGEST_REQUIRED_COLUMNS,
)

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks"
BUNDLE = yaml.safe_load((LANE / "databricks.yml").read_text(encoding="utf-8"))
RESOURCES = {
    path.name: yaml.safe_load(path.read_text(encoding="utf-8"))
    for path in sorted((LANE / "resources").glob("*.yml"))
}
RUN_DOC = REPO / "docs" / "databricks-run.md"
RECORD = REPO / "evidence" / "databricks" / "SG-DBX-01.json"
# The row-level capture the same task writes. Beside RECORD rather than beside the test
# that reads it, because tests/fast/test_architecture.py attributes a module constant to
# whichever test body precedes it - and a path into evidence/ decides whether a test has to
# carry the evidence_dependent marker.
CAPTURE = REPO / "evidence" / "databricks" / "dim_customer_scd2.json"


def _merged() -> dict[str, Any]:
    """The bundle as the CLI assembles it: the target's resources plus every include."""
    out: dict[str, dict[str, Any]] = {}
    trees = [BUNDLE["targets"]["free"].get("resources", {})]
    trees.extend(document.get("resources", {}) for document in RESOURCES.values())
    for tree in trees:
        for kind, declared in tree.items():
            out.setdefault(kind, {}).update(declared)
    return out


def _all_tasks(job: dict[str, Any]) -> list[dict[str, Any]]:
    """Every task in a job, INCLUDING the body of a for_each.

    A `for_each_task` carries its real work in `for_each_task.task`, one level down. Every
    check in this file used to walk `job["tasks"]` and stop there, so the notebook a for_each
    runs would have had no path check, no widget check and no parameter check - a new
    construct arriving outside the reach of every existing guard, which is this repository's
    most-repeated defect and the reason `_merged()` above exists at all.

    `test_the_flattening_reaches_every_notebook_in_the_bundle` is the guard on this guard.
    """
    out: list[dict[str, Any]] = []
    for task in job.get("tasks", []):
        out.append(task)
        nested = (task.get("for_each_task") or {}).get("task")
        if isinstance(nested, dict):
            out.append(nested)
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


def _widest_concurrency(job: dict[str, Any]) -> tuple[int, list[str]]:
    """An upper bound on how many tasks of this job can be running at once, and which.

    The ceiling is five CONCURRENT tasks per account, and the previous version of this check
    counted DECLARED ones - so a chain of six tasks that runs one at a time failed it, and the
    number would have been bumped to six with nothing learned. Counting the wrong quantity in
    the check that guards a limit is the same defect as the guard named for the population that
    measured a row count, one file away.

    What is computed is the width of the dependency DAG: the largest set of tasks none of which
    must wait for another. Two tasks can run together exactly when neither is reachable from
    the other, so that set is the maximum possible simultaneity. A `for_each` counts as its own
    `concurrency`, because that is how many task slots its iterations occupy.

    It OVERESTIMATES for conditional branches - the two outcomes of a condition are an
    antichain and can never both run - and overestimating is the safe direction for a ceiling.
    """
    tasks = job.get("tasks", [])
    keys = [t["task_key"] for t in tasks]
    direct = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks}
    # Transitive closure, so "waits for" is the full ordering and not just the edges drawn.
    waits: dict[str, set[str]] = {k: set(direct.get(k, set())) for k in keys}
    for _ in range(len(keys)):
        for key in keys:
            waits[key] |= {n for d in list(waits[key]) for n in waits.get(d, set())}

    def slots(key: str) -> int:
        task = next(t for t in tasks if t["task_key"] == key)
        return int((task.get("for_each_task") or {}).get("concurrency", 1) or 1)

    best, widest = 0, []
    for size in range(1, len(keys) + 1):
        for group in itertools.combinations(keys, size):
            if any(b in waits[a] or a in waits[b] for a in group for b in group if a != b):
                continue
            total = sum(slots(key) for key in group)
            if total > best:
                best, widest = total, list(group)
    return best, widest


def test_no_job_can_exceed_the_concurrent_task_ceiling() -> None:
    """Five concurrent job tasks per account, measured as concurrency and not as a task count.

    A chain of any length is fine here; what is not fine is a shape that can put six tasks in
    flight at once. `for_each` concurrency counts, because that is where this job could
    plausibly grow past the limit without a task being added to the file.
    """
    for name, job in JOBS.items():
        width, which = _widest_concurrency(job)
        assert width <= 5, (
            f"{name} can have {width} tasks running at once ({sorted(which)}), and Free "
            f"Edition allows five per ACCOUNT - which this job would then be using entirely. "
            f"Lower a for_each `concurrency`, or serialise a branch."
        )
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


def test_the_name_the_run_step_looks_the_job_up_by_is_the_name_the_bundle_deploys() -> None:
    """One literal in two files, and the drift would be silent.

    `step_run` refuses to run a deployment older than HEAD, and it finds the deployed job with
    `databricks jobs list --name "..."`, which filters on an EXACT name. Rename the job in
    resources/jobs.yml and that lookup stops matching - so the guard would report "nothing is
    deployed" about a job sitting in the workspace, and the fix somebody would reach for is to
    deploy again, which changes nothing.

    A guard whose failure mode is a confident wrong diagnosis is worse than no guard, so the
    two spellings are tied here rather than left to agree.
    """
    script = (REPO / "scripts" / "databricks_run.sh").read_text(encoding="utf-8")
    names = {job.get("name") for job in JOBS.values() if job.get("name")}
    assert names, "no job in the bundle declares a name"
    looked_up = re.findall(r'^JOB_NAME="([^"]*)"', script, flags=re.MULTILINE)
    assert len(looked_up) == 1, (
        f"expected exactly one JOB_NAME in scripts/databricks_run.sh, found {looked_up}"
    )
    assert looked_up[0] in names, (
        f'scripts/databricks_run.sh looks the deployed job up as "{looked_up[0]}" and the '
        f"bundle deploys {sorted(names)}. `jobs list --name` is an exact match, so the "
        f"freshness guard would find nothing and say nothing is deployed."
    )


def test_the_commit_the_run_step_compares_against_is_a_parameter_the_job_carries() -> None:
    """The guard reads `deploy_commit` off the DEPLOYED job, so a task has to carry it.

    Move it to a job-level parameter, or drop it from base_parameters because the notebook
    started reading it from somewhere else, and the lookup returns NOPARAM on a perfectly
    fresh deployment - a refusal with a wrong reason attached.
    """
    carriers = [
        (name, task["task_key"])
        for name, job in JOBS.items()
        for task in job.get("tasks", [])
        if "deploy_commit" in ((task.get("notebook_task") or {}).get("base_parameters") or {})
    ]
    assert carriers, (
        "no notebook task in the bundle passes `deploy_commit` in its base_parameters, which "
        "is where scripts/databricks_run.sh reads the deployed commit from"
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
        for task in _all_tasks(job)
        if "notebook_task" in task
    ],
)
def test_every_notebook_task_points_at_a_file(job_name: str, task_key: str, notebook: str) -> None:
    assert (LANE / "resources" / notebook).resolve().is_file(), f"{job_name}/{task_key}"


# ------------------------------------------------------------------ real orchestration
#
# Three tasks in a straight line is not orchestration, it is a script with extra YAML. What is
# checked below is that each construct added to this job SERVES something - a value that is
# read, a decision with two different consequences, a fan-out whose width is set by data - and
# not that the construct is present.


def _lane_sources() -> dict[str, str]:
    return {
        path.name: path.read_text(encoding="utf-8") for path in sorted((LANE / "src").glob("*.py"))
    }


def _task_values_set(source: str) -> list[str]:
    """Every `dbutils.jobs.taskValues.set("name", ...)` a notebook really executes."""
    out = []
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "set" or not node.args:
            continue
        owner = node.func.value
        if not (isinstance(owner, ast.Attribute) and owner.attr == "taskValues"):
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            out.append(first.value)
    return out


def _resource_text() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((LANE / "resources").glob("*.yml"))
    )


def test_every_task_value_written_has_a_named_reader() -> None:
    """The rule this round adopted, enforced rather than intended.

    `publish_evidence.py` used to end with `taskValues.set("evidence", payload)`, and nothing
    had ever read it - it is the LAST task, so nothing in the graph can. A value written for a
    reader that cannot exist is the same class as a message announcing what it does not do,
    which is the defect this lane's own history is built around.

    A reader is one of two things: a `{{tasks.<key>.values.<name>}}` reference in the bundle,
    or a `taskValues.get` in another notebook. Anything else set is dead weight that will be
    read as a contract by the next person to touch this job.
    """
    resources = _resource_text()
    sources = _lane_sources()
    unread = []
    for name, source in sources.items():
        # The AST, not the text. The first version matched a `taskValues.set(...)` written
        # inside the COMMENT that explains why that call was removed, and reported the value it
        # was documenting as still being written. A check that reads prose as code is the same
        # mistake as a comment that describes what the code does not do.
        for value in _task_values_set(source):
            referenced = f"values.{value}}}}}" in resources
            fetched = any(
                re.search(rf'taskValues\.get\([^)]*"{re.escape(value)}"', other)
                for other_name, other in sources.items()
                if other_name != name
            )
            if not (referenced or fetched):
                unread.append(f"{name}:{value}")
    assert not unread, (
        f"these task values are written and nothing reads them: {unread}. Either wire a "
        f"reader - a `{{{{tasks.<key>.values.<name>}}}}` reference in resources/, or a "
        f"`taskValues.get` - or delete the write. A value nobody reads still looks like a "
        f"contract to whoever changes this job next."
    )


def test_the_condition_decides_on_something_the_close_publishes() -> None:
    """A condition over a constant is a comment. This one reads the close's own decision."""
    conditions = [
        (task["task_key"], task["condition_task"])
        for job in JOBS.values()
        for task in _all_tasks(job)
        if "condition_task" in task
    ]
    assert conditions, "the job declares no condition task"
    for key, condition in conditions:
        left = str(condition.get("left", ""))
        assert re.fullmatch(r"\{\{tasks\.\w+\.values\.\w+\}\}", left), (
            f"{key} compares {left!r}, which is not a task value. A condition whose operands "
            f"are both fixed at deploy time takes the same branch for ever."
        )
        producer, value = left.split(".")[1], left.split(".")[3].rstrip("}")
        source = (LANE / "src" / f"{producer}.py").read_text(encoding="utf-8")
        assert f'taskValues.set("{value}"' in source, (
            f"{key} reads {left}, and {producer}.py never sets {value!r}. The reference would "
            f"arrive as its own literal text and the comparison would be against a string."
        )


def test_both_outcomes_of_every_condition_lead_somewhere() -> None:
    """A branch with nothing on it is a decision with one consequence, which is no decision.

    This is what stops the condition task being decoration: if the false side had no task, the
    close that changed nothing would produce nothing, and "nothing changed" and "nothing ran"
    would be the same evidence again.
    """
    for job_name, job in JOBS.items():
        for task in _all_tasks(job):
            if "condition_task" not in task:
                continue
            outcomes = {
                str(dep.get("outcome"))
                for other in _all_tasks(job)
                for dep in other.get("depends_on", [])
                if dep.get("task_key") == task["task_key"] and "outcome" in dep
            }
            assert outcomes == {"true", "false"}, (
                f"{job_name}/{task['task_key']} has tasks on {sorted(outcomes)} only. Both "
                f"outcomes need one, or the condition is a switch with one position."
            )


def test_the_fan_out_is_over_data_and_not_over_a_list_someone_maintains() -> None:
    """`for_each` earns its place only if the cardinality comes from the run.

    A hand-written list of months would be a loop with extra machinery, and it would go stale
    the first time the close touched a month nobody had added to it.
    """
    for job_name, job in JOBS.items():
        for task in job.get("tasks", []):
            if "for_each_task" not in task:
                continue
            inputs = str(task["for_each_task"].get("inputs", ""))
            assert re.fullmatch(r"\{\{tasks\.\w+\.values\.\w+\}\}", inputs), (
                f"{job_name}/{task['task_key']} iterates {inputs!r}. If that is a literal "
                f"list, its length is a decision made at deploy time about data that changes "
                f"per run."
            )
            concurrency = task["for_each_task"].get("concurrency")
            assert isinstance(concurrency, int) and concurrency >= 1, (
                f"{job_name}/{task['task_key']} does not declare a concurrency. Left out it "
                f"defaults to running every iteration at once, and the account ceiling is five."
            )


def test_the_failure_switch_is_off_by_default_and_reaches_every_notebook() -> None:
    """The one parameter in this bundle that can make a task fail deliberately.

    It exists because a repair run needs a real failure and the alternative was committing a
    deliberate bug, which `require_fresh_deployment` would then have put in the provenance of
    every record the run produced. What must be true of it: the default is OFF, every notebook
    is handed it, and every notebook actually consults it - a task that ignores it would be a
    hole in the demonstration rather than a safe one.
    """
    parameters = {p["name"]: p for p in JOBS["samegold_close"].get("parameters", [])}
    assert "fail_task" in parameters, "the job declares no fail_task parameter"
    assert parameters["fail_task"].get("default") == "", (
        f"fail_task defaults to {parameters['fail_task'].get('default')!r}. A switch that "
        f"breaks a task must be off unless somebody asks for it at launch."
    )
    for key, task in NOTEBOOK_TASKS:
        assert task.get("base_parameters", {}).get("fail_task") == "{{job.parameters.fail_task}}", (
            f"{key} is not handed fail_task, so it cannot be made to fail and cannot be part "
            f"of a repair demonstration"
        )
    # The NOTEBOOKS the job runs, not every file in the lane: `bronze_autoloader.py` and the
    # other pipeline sources are declarative-pipeline files with no widgets and no job task,
    # and demanding the parameter of them would be this file asserting something about a
    # program the job never launches.
    for key, task in NOTEBOOK_TASKS:
        notebook = Path(task["notebook_path"]).name
        source = (LANE / "src" / notebook).read_text(encoding="utf-8")
        assert 'widgets.get("fail_task")' in source, (
            f"{key} runs {notebook}, which is handed fail_task and never reads it - so the "
            f"parameter is a promise that task does not keep, and a repair demonstration that "
            f"targets it would silently succeed"
        )


def test_every_task_that_executes_something_declares_a_timeout() -> None:
    """The ceiling exists because of the daily quota, not because of neatness.

    Free Edition stops ALL compute for the rest of the day when the quota runs out, so a task
    that hangs does not cost one run - it costs every lane on the account until midnight.
    `docs/limits.md` used to say a timeout was deliberately unset for want of a run to size it
    from; runs exist now.

    Condition tasks are excluded because they evaluate an expression and start no compute, and
    a `for_each` wrapper is excluded because the thing that runs is its body, which is checked.
    """
    for job_name, job in JOBS.items():
        assert job.get("timeout_seconds"), f"{job_name} has no job-level timeout"
        for task in _all_tasks(job):
            if "condition_task" in task or "for_each_task" in task:
                continue
            assert task.get("timeout_seconds"), (
                f"{job_name}/{task['task_key']} runs compute with no timeout. On this account "
                f"a hung task is the whole day."
            )


def test_no_task_relies_on_max_retries_to_stop_a_serverless_retry() -> None:
    """The lever that governs, beside the one that only looks like it does.

    `max_retries: 0` was declared on every task with a comment saying it stopped the job
    retrying them. On 5 September 2026 the deployed job came back from `databricks jobs get`
    with max_retries NOT DECLARED on any task while `timeout_seconds` survived - and in run
    44869473800771 `verify_no_restatement` ran twice inside the original run anyway, 6s then
    12s, 57 seconds apart.

    Both halves matter and they are separate. The declaration did not arrive, which is a
    serializer question and is open. And `max_retries: 0` is the API's own DEFAULT - "the value
    0 means to never retry" - so arriving would have changed nothing: what retried that task is
    serverless auto-optimization, which is on by default, which the UI calls "may include
    additional retries", and whose field is `disable_auto_optimization`.

    So a task declaring only `max_retries: 0` is a task whose retry behaviour is not decided by
    anything in this bundle. That is the fourth instance of a declaration that does not govern,
    after `development: true`, `--full-refresh-all` and the bash flag, and the first where the
    comment beside it asserted the opposite.
    """
    for job_name, job in JOBS.items():
        for task in _all_tasks(job):
            if "condition_task" in task or "for_each_task" in task:
                continue
            key = f"{job_name}/{task['task_key']}"
            assert task.get("max_retries") == 0, f"{key} does not declare max_retries: 0"
            assert task.get("disable_auto_optimization") is True, (
                f"{key} declares max_retries: 0 and not `disable_auto_optimization: true`. "
                f"max_retries 0 is the API's default, so on its own it decides nothing; "
                f"serverless auto-optimization is what retried a failed task on 5 September "
                f"2026, and this is the field that turns it off."
            )


def test_the_evidence_is_written_whatever_the_rest_of_the_job_did() -> None:
    """`run_if` in the use that is not a way of hiding a red run.

    Two independent reasons, and either alone is enough: one branch of the condition is ALWAYS
    skipped, so under the default this task would never run at all; and a failure upstream must
    still leave a record, because a lane whose subject is that a green tick must mean something
    cannot have its worst outcome be the one that leaves no evidence.
    """
    tasks = {t["task_key"]: t for t in _all_tasks(JOBS["samegold_close"])}
    evidence = tasks["publish_evidence"]
    assert evidence.get("run_if") == "ALL_DONE", (
        f"publish_evidence has run_if={evidence.get('run_if')!r}. One of its dependencies is "
        f"always skipped, so anything stricter means the record is never written."
    )
    depends = {d["task_key"] for d in evidence.get("depends_on", [])}
    branches = {
        task["task_key"]
        for task in _all_tasks(JOBS["samegold_close"])
        for dep in task.get("depends_on", [])
        if "outcome" in dep
    }
    assert branches <= depends, (
        f"publish_evidence waits for {sorted(depends)} and the condition's branches are "
        f"{sorted(branches)}. A branch it does not wait for is a branch whose verdicts may "
        f"not be in the table yet when the record is read."
    )


def test_the_verification_table_is_declared_the_same_way_in_both_places() -> None:
    """The lane CREATEs it; the spark resolution harness restates it. Two spellings drift.

    `tests/spark/test_databricks_lane_parses.py` resolves every statement against views built
    from `LANE_TABLES`. If that copy and the CREATE TABLE disagree, the resolution check passes
    against a table shape the workspace does not have - which is the check reporting the scope
    of its own fixture.
    """
    create = re.search(
        r"CREATE TABLE IF NOT EXISTS \{catalog\}\.main\.close_verification \((.*?)\)\s*USING DELTA",
        (LANE / "src" / "verify_month.py").read_text(encoding="utf-8"),
        re.DOTALL,
    )
    assert create, "verify_month.py no longer creates close_verification"
    declared = [
        tuple(part.split()) for part in (p.strip() for p in create.group(1).split(",")) if part
    ]
    harness = (REPO / "tests" / "spark" / "test_databricks_lane_parses.py").read_text(
        encoding="utf-8"
    )
    restated = re.search(r'"close_verification": \((.*?)\),\n', harness, re.DOTALL)
    assert restated, "the spark harness no longer knows close_verification"
    columns = re.findall(r'"([^"]*)"', restated.group(1))
    restated_pairs = [
        tuple(part.split()) for part in (p.strip() for p in "".join(columns).split(",")) if part
    ]
    assert declared == restated_pairs, (
        f"the lane creates {declared} and the spark harness resolves against {restated_pairs}"
    )


# ------------------------------------- the verification that did not run, and left no trace
#
# `run_if: ALL_DONE` is what makes the record survive a failure, and it is also what lets a
# failure through unremarked. `unresolved_task_values` sees a failed `close_month`: no task
# values were published, the references arrive as their own text. It cannot see a failed
# VERIFICATION - the close succeeded, every value resolves, and the only difference between
# that record and a healthy one is a `close_verification` section with no rows in it.
#
# Zero rows reads as "every check passed". These are the checks that make it read as a hole.

# The `check_name` literals each verification notebook actually SELECTs. A `SELECT '<name>'` is
# how every one of them is written; the record's own catalogue is compared against this rather
# than against a list restated here, so the comparison cannot be satisfied by editing the test.
CHECK_NAME = re.compile(r"SELECT\s+'([a-z][a-z0-9_]*)'")


def _check_names(file_name: str) -> set[str]:
    return set(CHECK_NAME.findall((LANE / "src" / file_name).read_text(encoding="utf-8")))


def _lane_definitions(file_name: str, *names: str) -> dict[str, Any]:
    """Evaluate named module-level definitions out of a lane notebook, and nothing else.

    These notebooks cannot be imported: `spark` and `dbutils` are injected by a runtime that
    does not exist here. The alternative to lifting the definition out is to restate it in the
    test, which tests the restatement - the defect this repository has already found in a
    document, in a fixture and in a spark harness. So the PURE definitions are executed by
    name, the way `tests/spark/test_databricks_lane_parses.py` executes the SQL out of the same
    files, and a name that is not there raises rather than leaving the test with nothing to do.
    """
    path = LANE / "src" / file_name
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted = list(names)
    body: list[ast.stmt] = []
    found: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            body.append(node)
            found.append(node.name)
        elif (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in wanted
        ):
            body.append(node)
            found.append(node.targets[0].id)
    missing = [name for name in wanted if name not in found]
    assert not missing, f"{file_name} no longer defines {missing} at module level"
    namespace: dict[str, Any] = {}
    exec(compile(ast.Module(body=body, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def test_the_checks_the_record_expects_are_the_checks_the_lane_writes() -> None:
    """One fact in two files, held together by the only thing that can hold it.

    `publish_evidence.py` names what each branch owes so that an absence is derivable. That is
    a second copy of what the two verify notebooks SELECT, and a second copy nothing compares
    is how the two come to disagree: add a sixth check to `verify_month.py` and the record
    would go on treating five as the whole debt, which is the guard reporting the scope of its
    own list.
    """
    catalogue = _lane_definitions("publish_evidence.py", "CHECKS_BY_BRANCH")["CHECKS_BY_BRANCH"]
    written = {
        "verify_each_restated_month": _check_names("verify_month.py"),
        "verify_no_restatement": _check_names("verify_no_restatement.py"),
    }
    assert {branch: sorted(names) for branch, names in catalogue.items()} == {
        branch: sorted(names) for branch, names in written.items()
    }, (
        f"publish_evidence.py expects {catalogue} and the notebooks write {written}. The record "
        f"derives a missing verification from the first; a check the second writes and the "
        f"first does not name is a check whose absence nothing would notice."
    )


def test_a_verification_that_wrote_nothing_is_a_hole_and_not_a_clean_report() -> None:
    """The defect, at the function that closes it, in the four shapes a run can take.

    The middle two are the ones that matter. A `verify_no_restatement` that fails writes no
    rows, and every other field of the record is what a healthy run produces; a for_each whose
    second month fails leaves the FIRST month's five rows in place, so a check on check names
    alone would find all five names present and call the run complete.
    """
    holes = _lane_definitions("publish_evidence.py", "CHECKS_BY_BRANCH", "_verification_holes")[
        "_verification_holes"
    ]
    no_op_rows = [
        {"check_name": "every_eligible_month_has_a_version", "accounting_month": "2026-01"},
        {"check_name": "no_eligible_month_drifted", "accounting_month": "2026-01"},
    ]
    expected, missing = holes("verify_no_restatement", [], no_op_rows)
    assert expected and not missing, (expected, missing)

    # The run this fix exists for: the task failed, so the table has no rows for it.
    expected, missing = holes("verify_no_restatement", [], [])
    assert (
        missing
        == expected
        == [
            "every_eligible_month_has_a_version",
            "no_eligible_month_drifted",
        ]
    ), (expected, missing)

    months = ["2026-01", "2026-02"]
    every = [
        {"check_name": name, "accounting_month": month}
        for month in months
        for name in _check_names("verify_month.py")
    ]
    expected, missing = holes("verify_each_restated_month", months, every)
    assert len(expected) == 10 and not missing, (expected, missing)

    # One iteration of the for_each failed. Every check NAME is still present, in the month
    # that passed - which is why the true branch is checked per month and not per name.
    january_only = [row for row in every if row["accounting_month"] == "2026-01"]
    expected, missing = holes("verify_each_restated_month", months, january_only)
    february = sorted(f"{name}:2026-02" for name in _check_names("verify_month.py"))
    assert sorted(missing) == february, missing

    # The close itself failed, so there is no branch to derive a debt from. Saying "nothing was
    # owed" here would be the same absence wearing the same disguise one level up.
    expected, missing = holes("unknown", None, [])
    assert expected == [] and missing == [], (expected, missing)


def test_every_task_state_the_record_reports_is_a_reference_the_job_passes() -> None:
    """The states are read from the job, so the job has to hand them over, by task key.

    The widget is named after the task it reports and the reference is built from the same
    key, which makes the correspondence checkable instead of remembered. A `result_state` for a
    task key the job does not have would resolve to nothing and be recorded as `None` - a state
    the record could not learn, which is indistinguishable from a task that never ran.
    """
    widgets = _lane_definitions("publish_evidence.py", "TASK_STATE_WIDGETS")["TASK_STATE_WIDGETS"]
    tasks = {task["task_key"] for task in _all_tasks(JOBS["samegold_close"])}
    evidence = next(
        task
        for task in _all_tasks(JOBS["samegold_close"])
        if task["task_key"] == "publish_evidence"
    )
    parameters = evidence["notebook_task"]["base_parameters"]
    for task_key, widget in sorted(widgets.items()):
        assert task_key in tasks, f"{widget} reports a task the job does not declare: {task_key}"
        assert parameters.get(widget) == f"{{{{tasks.{task_key}.result_state}}}}", (
            f"publish_evidence reads {widget} and the job passes "
            f"{parameters.get(widget)!r}. A state nobody passes is a state the record cannot "
            f"learn, and it is written down as `None` rather than as 'fine'."
        )


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
    for task in _all_tasks(job)
    if "notebook_task" in task
]


def test_the_flattening_reaches_every_notebook_in_the_bundle() -> None:
    """The guard on `_all_tasks`, which is the guard on everything below it.

    If a future task type nests its notebook somewhere `_all_tasks` does not look, every check
    in this file goes quiet about that notebook and nothing says so. So the flattened count is
    compared against the RAW TEXT of the resource files: one `notebook_task:` key each.
    """
    declared = sum(
        path.read_text(encoding="utf-8").count("notebook_task:")
        for path in sorted((LANE / "resources").glob("*.yml"))
    )
    assert declared == len(NOTEBOOK_TASKS), (
        f"the resource files declare {declared} notebook tasks and _all_tasks() found "
        f"{len(NOTEBOOK_TASKS)} ({[key for key, _ in NOTEBOOK_TASKS]}). A notebook the "
        f"flattening does not reach is a notebook with no path check, no widget check and no "
        f"parameter check."
    )


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
    # What the JOB did, which the round that made it a graph is the reason for. These landed
    # with the three runs of 5 September 2026; before that no record could answer them, and the
    # renderer would have written NOT RUN into all six.
    "orch.decision",
    "orch.branch",
    "orch.versions_written",
    "orch.months_written",
    "orch.checks_run",
    "orch.checks_failed",
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


def test_the_orchestration_facts_reach_the_anchor_map() -> None:
    """The record carries what the job DID; this is the map that lets a document quote it.

    Tested against a synthetic record rather than the committed one, because the committed one
    predates the orchestration and a check that waits for a deployment is a check that does not
    run. When a run does publish these, `samegold readme` fills the anchors and `samegold
    check` holds them to the record - which is the whole point of putting orchestration facts
    in the evidence rather than in prose.
    """
    scalars = dict(scalars_from(_healthy_record()))
    assert scalars["orch.decision"] == "restated"
    assert scalars["orch.branch"] == "verify_each_restated_month"
    assert scalars["orch.versions_written"] == 2
    assert scalars["orch.months_written"] == 2
    assert scalars["orch.checks_run"] == 2
    assert scalars["orch.checks_failed"] == 1


def _healthy_record() -> dict[str, Any]:
    """A record from a run whose verification reported: the only shape that may be quoted.

    `expected_checks` and `missing_checks` are what say so. They are part of the fixture
    because they are part of the fact - a run that published a count of checks without saying
    what it OWED has published a number, not a result.
    """
    return {
        "orchestration": [
            {
                "decision": "restated",
                "branch": "verify_each_restated_month",
                "versions_written": 2,
                "months_written": ["2026-01", "2026-02"],
                "expected_checks": ["a:2026-01", "b:2026-01"],
                "missing_checks": [],
                "task_states": {"verify_each_restated_month": "success"},
            }
        ],
        "close_verification": [
            {"check_name": "a", "ok": True},
            {"check_name": "b", "ok": False},
        ],
        "incomplete": [],
    }


def test_a_record_whose_verification_did_not_report_offers_no_check_anchors() -> None:
    """The trap this round was written to remove, at the reader's end of it.

    A run whose `verify_no_restatement` failed publishes a `close_verification` with no rows
    in it, and nothing else about the record differs from a healthy one. Rendered, that is
    `orch.checks_run = 0` and `orch.checks_failed = 0` on a page describing a run whose
    verification never executed - two figures that look like a clean result and are the
    absence of one.

    So the anchors are offered only against a record that POSITIVELY says the branch paid what
    it owed. Everything else - a record that names the hole, and a record that does not speak
    to it at all - renders NOT RUN, which is what every other figure this repository cannot
    answer already does.
    """
    healthy = dict(scalars_from(_healthy_record()))
    assert healthy["orch.checks_run"] == 2 and healthy["orch.checks_failed"] == 1

    # The failed run, as the notebook now publishes it.
    named = _healthy_record()
    named["close_verification"] = []
    named["orchestration"][0].update(
        branch="verify_no_restatement",
        decision="no_op",
        months_written=[],
        expected_checks=["every_eligible_month_has_a_version", "no_eligible_month_drifted"],
        missing_checks=["every_eligible_month_has_a_version", "no_eligible_month_drifted"],
        task_states={"verify_no_restatement": "failed"},
    )
    named["incomplete"] = ["verify_no_restatement"]
    offered = dict(scalars_from(named))
    assert "orch.checks_run" not in offered and "orch.checks_failed" not in offered, offered
    # The branch it took is still quotable: what is withheld is the RESULT of a verification
    # that produced none, not the record of which one was owed.
    assert offered["orch.branch"] == "verify_no_restatement"

    # The same failed run through a notebook that does not derive the hole - which is every
    # record committed before this round, and would be every record again if the derivation
    # were deleted. "The record did not mention a problem" is not evidence of none.
    silent = _healthy_record()
    silent["close_verification"] = []
    for field in ("expected_checks", "missing_checks", "task_states"):
        silent["orchestration"][0].pop(field)
    quiet = dict(scalars_from(silent))
    assert "orch.checks_run" not in quiet and "orch.checks_failed" not in quiet, quiet


def test_a_record_without_orchestration_offers_no_orchestration_anchors() -> None:
    """The other half, and the one that keeps a document from getting ahead of its run.

    A record produced before the orchestration existed must not make `orch.*` anchors
    available, or a document could quote a figure no run produced - which is the failure
    `test_the_run_document_holds_no_figure_the_run_has_not_produced` exists for.
    """
    scalars = dict(scalars_from({"rows": {"bronze_events": 1}}))
    assert not [name for name in scalars if name.startswith("orch.")], scalars


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


def test_the_committed_record_answers_every_anchor_the_documents_require() -> None:
    """WHICH RECORD MAY BE COMMITTED, as a check rather than as a judgement somebody repeats.

    `evidence/databricks/SG-DBX-01.json` is the canonical record: every `dbx:` anchor in every
    document is rendered from it. So a run whose record cannot answer the closed list above
    must not replace it, and the case is not hypothetical - a run that ingests NOTHING (the
    lane started with no new files in the landing volume) produces no `flow_progress` event
    carrying data quality, and its `expectations` comes back empty. Committing that record
    would turn a measured table into `NOT RUN` and the diff would read like an update.

    A regression disguised as an update is exactly what an unwritten norm lets through, so it
    is written down twice: in `evidence/databricks/README.md`, and here where it fails.

    The other runs are not thrown away. `scripts/databricks_run.sh fetch <label>` keeps one
    beside the canonical record under a name of its own, and nothing renders or compares those.
    """
    if not RECORD.exists():
        pytest.skip("the lane has not been deployed yet")
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    answerable = set(scalars_from(record)) | set(tables_from(record))
    missing = sorted(REQUIRED_ANCHORS - answerable)
    assert not missing, (
        f"the committed record cannot answer {missing}, so rendering the documents from it "
        f"would replace those figures with '{NOT_RUN}'.\n"
        f"If this is a run that ingested nothing, or one whose verification did not report, it "
        f"is not the canonical record: fetch it under a label instead -\n"
        f"    scripts/databricks_run.sh fetch <label>\n"
        f"and restore the record the documents are rendered from. "
        f"evidence/databricks/README.md says which file is canonical and why."
    )


def test_a_record_kept_beside_the_canonical_one_can_still_name_its_run() -> None:
    """A labelled record is evidence too, and evidence that cannot name its run is prose.

    There is no test that a sidecar EXISTS - runs 1 and 2 may be read and thrown away, that is
    the operator's call. What is not allowed is a file sitting in `evidence/databricks/` that
    nobody can trace to a job run, because the reason for keeping it at all is that it
    describes one.
    """
    for path in sorted(RECORD.parent.glob("SG-DBX-01.*.json")):
        kept = json.loads(path.read_text(encoding="utf-8"))
        assert str(kept.get("job_run_id", "")).isdigit(), (
            f"{path.name} is kept beside the canonical record and does not name a job run "
            f"({kept.get('job_run_id')!r}). Fetch it again from the run it came from, or "
            f"delete it: a record nobody can look up is not evidence of anything."
        )


def test_the_run_document_agrees_with_the_record() -> None:
    """Once the run has happened, the document may only say what the record says."""
    if not RECORD.exists():
        pytest.skip("the lane has not been deployed yet")
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    # ONE derivation of the mapping, imported from the renderer that fills the anchors. It
    # used to be restated here, and two derivations of the same mapping is how a document and
    # its test come to agree with each other and not with the record.
    scalars = dict(scalars_from(record))

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


def _digest_statement() -> str:
    """The one statement in the notebook that computes the population fingerprint."""
    source = (LANE / "src" / "publish_evidence.py").read_text(encoding="utf-8")
    start = source.index("population = _read(")
    return source[start : source.index('"""),', start)]


def test_the_digest_projection_is_the_declared_bronze_schema() -> None:
    """The order is part of what is hashed, so it has one definition and three spellings.

    `BRONZE_DIGEST_COLUMNS` renders the OSS half, the `coalesce(CAST(...))` list renders the
    workspace's, and the `columns` literal is what a reader of the record is told was hashed.
    Any two of those drifting produces a digest mismatch that looks like a moved population -
    the failure this fingerprint exists to make legible, arriving with the wrong explanation
    attached.

    So all three are tied to `samegold.pipelines.schema.bronze_schema`, minus `_rescued_data`,
    which is Auto Loader's own column and has no counterpart on the OSS side.
    """
    declared = [name for name in _declared_bronze_types() if name != "_rescued_data"]
    assert list(BRONZE_DIGEST_COLUMNS) == declared, (
        f"BRONZE_DIGEST_COLUMNS is {list(BRONZE_DIGEST_COLUMNS)} and the declared bronze "
        f"schema is {declared}. A column in the table and not in the projection is a column "
        f"the fingerprint is blind to."
    )

    statement = _digest_statement()
    rendered = re.findall(r"coalesce\(CAST\((\w+) AS STRING\), ''\)", statement)
    assert rendered == declared, (
        f"the notebook hashes {rendered} and the declared schema is {declared}. The order is "
        f"part of the digest, so this is not cosmetic."
    )

    published = re.findall(r"'((?:\w+,){5,}\w+)'", statement)
    assert len(published) == 1, f"expected one column list literal, found {published}"
    assert published[0].split(",") == declared, (
        f"the notebook TELLS a reader it hashed {published[0]} and it hashed "
        f"{','.join(rendered)}. The OSS half renders whatever the record says, so this is the "
        f"spelling that would silently make the two hash different things."
    )


def test_the_columns_the_digest_clamps_are_the_declared_bigints() -> None:
    """The BIGINT range is a property of the TABLE, and this is where the two agree on which.

    The generator emits 9223372036854775808 for two events - one past the top of a BIGINT - so
    the workspace holds NULL and Python holds an integer. The OSS renderer applies the range to
    the columns declared BIGINT; declare a fourth one and forget this tuple, and the two halves
    disagree on exactly the rows nobody looks at.
    """
    declared = {name for name, sql_type in _declared_bronze_types().items() if sql_type == "BIGINT"}
    assert set(BRONZE_DIGEST_BIGINT_COLUMNS) == declared, (
        f"the digest clamps {sorted(BRONZE_DIGEST_BIGINT_COLUMNS)} and the schema declares "
        f"{sorted(declared)} as BIGINT"
    )


def test_the_domain_the_digest_covers_is_columns_the_schema_declares() -> None:
    """A domain over a column the table does not have would exclude every row, silently."""
    declared = set(_declared_bronze_types())
    missing = sorted(set(BRONZE_DIGEST_REQUIRED_COLUMNS) - declared)
    assert not missing, missing
    statement = _digest_statement()
    for column in BRONZE_DIGEST_REQUIRED_COLUMNS:
        assert f"{column} IS NULL" in statement, (
            f"the OSS half requires {column} to be present and the notebook's domain does "
            f"not mention it, so the two are hashing different sets of rows"
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


# The remaining string booleans in the committed evidence, by name and BY FILE.
#
# It used to be a set of paths in the record alone, and the capture beside it was not walked at
# all - so `dim_customer_scd2.json`'s own `provenance.tree_dirty` carried the same `"false"`
# and nothing said so. One file covered out of two is the scope defect this repository has
# found in its own linting, its own mutation campaign and its own preflight; here it is again,
# in the test written to catch the class.
#
# `publish_evidence.py` has converted both since `8c9faa7`, and for a while the committed files
# still carried the strings: the notebook that wrote them was deployed from `4d13a13`, before
# the conversion, and `databricks bundle run` runs what was DEPLOYED. That gap is the reason
# this list existed, and it is why the entries were named rather than tolerated - a run from
# the fixed notebook had to turn this test red, and it did.
# CLOSED by the run from `ad936aa` on 4 September 2026, fetched in `65df0bc`: both files now
# carry a real `false`. The set is empty and stays declared, because an empty closed list is a
# stronger statement than no list - it says the exception was retired rather than forgotten,
# and a NEW string boolean fails against `set()` immediately.
STILL_STRINGS_IN_THE_COMMITTED_EVIDENCE: set[str] = set()


def _tree_dirty(value: Any) -> bool | None:
    """True, False or None, from a boolean or from the string a widget used to publish.

    The string form is refused elsewhere; it is READ here anyway, because a test that only
    understands the fixed shape would go quiet on exactly the regression it should catch.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return None


def test_no_committed_evidence_came_from_a_deploy_that_was_not_a_commit() -> None:
    """The gate at the point of entry, and this round produced its own instance.

    `require_fresh_deployment` was written and committed; the FINDINGS.md entry describing it
    was not. The next deploy went out from that tree, and the record it produced said
    `tree_dirty: true`. Nobody caught it by reading - the provenance field caught it, which is
    the field doing precisely what it was added for.

    Deploying a dirty tree while working is legitimate. COMMITTING what it produced is not:
    `deploy.commit` then names a commit that does not contain the code that ran, so nobody
    with a clone can tie the record to anything, and tying it to something is the only job
    that field has. A record like that is worse than no record, because it looks chained.

    `scripts/databricks_run.sh fetch` says so at the moment the files land. It cannot do more
    than say it: the files are already on disk by then and the commit happens later, possibly
    on another machine. THIS is the half that governs, because it runs in `make fast`, in
    `make preflight` and in CI - at the moment the evidence would actually enter the
    repository.
    """
    if not RECORD.exists():
        pytest.skip("the lane has not been deployed yet")

    offenders = {}
    for document, path in (
        (RECORD, ("deploy", "tree_dirty")),
        (CAPTURE, ("provenance", "tree_dirty")),
    ):
        if not document.exists():
            continue
        node: Any = json.loads(document.read_text(encoding="utf-8"))
        for key in path:
            node = (node or {}).get(key)
        state = _tree_dirty(node)
        if state is not False:
            offenders[document.name] = "unknown" if state is None else "dirty"

    assert not offenders, (
        f"committed evidence whose deploy was not a commit: {offenders}. A record produced by "
        f"a deploy from a tree with uncommitted code names a commit that does not contain the "
        f"code that ran, so it can be tied to nothing - and a record that cannot be tied to "
        f"anything still looks like evidence. Commit the code, deploy again, re-run the "
        f"evidence task, and fetch:\n"
        f"    scripts/databricks_run.sh deploy\n"
        f"    scripts/databricks_run.sh run publish_evidence\n"
        f"    scripts/databricks_run.sh fetch\n"
        f"'unknown' is the same verdict for a different reason: a deploy by hand, without the "
        f"variables, leaves a record that cannot say what it came from."
    )


def test_a_boolean_in_the_record_is_a_boolean() -> None:
    """`"false"` is true.

    `deploy.tree_dirty` crossed three string-typed layers to reach the record - a bundle
    variable, a job parameter, a notebook widget - and arrived as the STRING "false". Every
    reader who writes `if record["deploy"]["tree_dirty"]:` gets True on a clean tree, and the
    field exists precisely so that a reader can decide whether the commit beside it describes
    what ran.

    The fix is at the source, in `publish_evidence.py`, where the three states become
    `True` / `False` / `None`: two states cannot carry three, and a value nobody supplied must
    not read as "clean". `deploy.commit` is the discriminator, and it is the word "unknown" in
    exactly that case.

    Checked over EVERY boolean-named field the record carries rather than the one that was
    wrong, because a type that crossed a string boundary once will cross it again.
    """
    if not RECORD.exists():
        pytest.skip("the lane has not been deployed yet")

    def walk(node: Any, path: str = "") -> list[tuple[str, Any]]:
        found: list[tuple[str, Any]] = []
        if isinstance(node, dict):
            for key, value in node.items():
                found.extend(walk(value, f"{path}.{key}" if path else str(key)))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                found.extend(walk(value, f"{path}[{index}]"))
        else:
            found.append((path, node))
        return found

    # BOTH files the run publishes. The capture is not a lesser document: it is the one the
    # row-by-row comparison reads, and its header is what says the rows were measured in a
    # workspace rather than typed.
    wrong = {
        f"{document.name}:{path}"
        for document in (RECORD, CAPTURE)
        if document.exists()
        for path, value in walk(json.loads(document.read_text(encoding="utf-8")))
        if isinstance(value, str) and value.lower() in {"true", "false"}
    }
    # A CLOSED LIST, not a tolerance. The record committed here was produced by a notebook
    # deployed from `4d13a13`, which is before the conversion above existed - `bundle run` runs
    # what was deployed - so it still carries the one field. Listing it by name means two
    # things: a NEW string boolean fails immediately, and the next run from a deployed fix
    # fails too, because this set will no longer match. Emptying it is what that run requires.
    assert wrong == STILL_STRINGS_IN_THE_COMMITTED_EVIDENCE, (
        f"string booleans in the committed evidence: {sorted(wrong)}, expected "
        f"{sorted(STILL_STRINGS_IN_THE_COMMITTED_EVIDENCE)}. A field that carries a boolean as a "
        f"string is truthy either way; convert at the source, not at each reader. If this "
        f"failure says the set got SMALLER, a run from the fixed notebook has landed and the "
        f"list above should be emptied."
    )
