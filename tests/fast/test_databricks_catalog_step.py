"""The catalog step, driven against a stub CLI, because its bugs are in its WAITING.

Two rounds of this step were wrong in ways no amount of reading found:

  * `databricks catalogs create` cannot work on a Free Edition metastore at all;
  * then `wait_timeout: 30s` with `on_wait_timeout: CANCEL` timed out on a cold warehouse and
    the script announced `CANCELED` as "the catalog is not there". It was there. The cancel
    ends the CLIENT's wait; the DDL the warehouse had already admitted went on and created it.
    A false claim about the world, proved false in the user's terminal, inside an error
    message.

So the thing under test is not "does it call the right endpoint". It is: what does this step
CONCLUDE, from each shape of answer the API can give it. That needs the API to answer, so
these tests put a stub `databricks` on PATH and run the real script.

The stub records its calls, so a test can also assert what the script did NOT do - which is
how "it re-checked before giving up" is checked rather than hoped for.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "databricks_run.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None,
    # Stated rather than passed over in silence: without a shell there is nothing to run, and
    # a green tick here would be reporting the absence of bash rather than the behaviour of
    # the script. Every environment this repository supports has one - Linux CI, WSL2, and
    # Git Bash on the machine it is written on.
    reason="no bash on PATH, so the shell script cannot be executed at all",
)

# Every stub is the same program with a different script of answers. `$SG_STEPS` is a file the
# stub appends one line to per call, and the per-verb answers come from files the test writes,
# so no test has to embed shell logic of its own.
STUB = """#!/bin/sh
echo "$*" >> "$SG_CALLS"
case "$1 $2" in
  "current-user me") exit 0 ;;
  "catalogs get")
      # Counted, not flagged. The scenario that matters is a catalog that is ABSENT on the
      # first look and PRESENT on the re-check, so "exists" has to be a function of which
      # call this is: a marker file that exists from the start would make the step return
      # early and never submit a statement at all.
      SG_N=$(cat "$SG_GET_COUNT" 2>/dev/null || echo 0)
      SG_N=$((SG_N + 1))
      echo "$SG_N" > "$SG_GET_COUNT"
      if [ "${SG_EXISTS_FROM:-0}" -gt 0 ] && [ "$SG_N" -ge "${SG_EXISTS_FROM:-0}" ]; then
        exit 0
      fi
      exit 1 ;;
  "jobs list")
      # `step_run` asks the workspace which commit the deployed job was deployed from before
      # it spends anything. The stub reads the answer out of a file so a test can say what is
      # deployed - the same commit, an older one, or no job at all. $SG_JOBS_FAIL makes the
      # CLI FAIL instead of answering, which is a different thing again and used to kill the
      # script with no message at all.
      if [ -n "${SG_JOBS_FAIL:-}" ]; then exit 1; fi
      cat "$SG_JOBS" ; exit 0 ;;
  "warehouses list") cat "$SG_WAREHOUSES" ; exit 0 ;;
  "warehouses start") exit 0 ;;
  "fs cp")
      # `databricks fs cp --overwrite SRC DEST`. A copy that writes nothing is not a copy, and
      # the step's next action is to READ what it copied - so the stub actually produces the
      # destination file. $SG_FS_CP_MISSING names a file the volume does not have, which is
      # how the "the record came down and the capture did not" path gets exercised.
      SG_SRC="$4" ; SG_DEST="$5"
      if [ -n "${SG_FS_CP_MISSING:-}" ]; then
        case "$SG_SRC" in
          *"$SG_FS_CP_MISSING") exit 1 ;;
        esac
      fi
      cp "$SG_FS_CP_PAYLOAD" "$SG_DEST" 2>/dev/null || echo '{}' > "$SG_DEST"
      exit 0 ;;
  "api post")
      # The first answer of the script of answers.
      head -n 1 "$SG_ANSWERS" ; exit 0 ;;
  "api get")
      # Each poll consumes the next line; the last line repeats for ever.
      SG_N=$(cat "$SG_POLL_COUNT" 2>/dev/null || echo 1)
      SG_N=$((SG_N + 1))
      echo "$SG_N" > "$SG_POLL_COUNT"
      LINE=$(sed -n "${SG_N}p" "$SG_ANSWERS")
      if [ -z "$LINE" ]; then LINE=$(tail -n 1 "$SG_ANSWERS"); fi
      echo "$LINE" ; exit 0 ;;
  *) exit 0 ;;
esac
"""

RUNNING_WAREHOUSE = '[{"id": "wh-1", "state": "RUNNING"}]'
STOPPED_WAREHOUSE = '[{"id": "wh-1", "state": "STOPPED"}]'


def _bash_path(path: Path) -> str:
    """`C:/Users/x` -> `/c/Users/x`, which is the only form Git Bash SEARCHES on PATH.

    Found the hard way: with a `C:/...` entry the stub sat there unfound and `command -v
    databricks` resolved the real CLI in /usr/local/bin instead, so these tests drove the real
    tool against whatever credentials this machine had. A test that silently talks to a live
    workspace is worse than one that fails.
    """
    text = path.as_posix()
    if len(text) > 1 and text[1] == ":":
        return "/" + text[0].lower() + text[2:]
    return text


def _head() -> str:
    """This checkout's HEAD, which is what `step_run` compares the deployment against."""
    done = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return done.stdout.strip() if done.returncode == 0 else "unknown"


def _deployed_job(commit: str, *, wrapped: bool = False) -> str:
    """What `databricks jobs list --name ... --expand-tasks -o json` answers with.

    `wrapped` is the Jobs API's own envelope, `{"jobs": [...]}`; the CLI prints the bare
    array. Which of the two arrives cannot be established without a workspace, so the parser
    reads both and both shapes are exercised here rather than one being assumed.
    """
    job = {
        "job_id": 1,
        "settings": {
            "name": "samegold monthly close",
            "tasks": [
                {"task_key": "ingest_and_transform", "pipeline_task": {"pipeline_id": "p-1"}},
                {
                    "task_key": "publish_evidence",
                    "notebook_task": {
                        "notebook_path": "../src/publish_evidence.py",
                        "base_parameters": {"catalog": "samegold", "deploy_commit": commit},
                    },
                },
            ],
        },
    }
    return json.dumps({"jobs": [job]} if wrapped else [job])


def _state(state: str, statement_id: str = "stmt-1", error: str = "") -> str:
    body = f'{{"statement_id": "{statement_id}", "status": {{"state": "{state}"'
    if error:
        body += f', "error": {{"message": "{error}"}}'
    return body + "}}"


def _run(
    tmp_path: Path,
    answers: list[str],
    *,
    warehouses: str = RUNNING_WAREHOUSE,
    exists_from_call: int = 0,
    subcommand: str = "catalog",
    second: str = "",
    jobs: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run `scripts/databricks_run.sh catalog` against a stub that answers `answers` in order.

    `exists_from_call=2` makes `catalogs get` fail the first time and succeed from the second,
    which is the real sequence: absent when the step starts, present when it looks again after
    a non-success state.
    """
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "databricks"
    stub.write_text(STUB, encoding="utf-8", newline="\n")
    stub.chmod(0o755)

    calls = tmp_path / "calls"
    calls.write_text("", encoding="utf-8")
    (tmp_path / "answers").write_text("\n".join(answers) + "\n", encoding="utf-8", newline="\n")
    (tmp_path / "warehouses").write_text(warehouses, encoding="utf-8", newline="\n")
    # A deployment made from THIS checkout, so that the steps that are not about the freshness
    # guard are not blocked by it. A test that is about the guard passes its own.
    (tmp_path / "jobs").write_text(
        jobs if jobs is not None else _deployed_job(_head()), encoding="utf-8", newline="\n"
    )

    environment = {
        **os.environ,
        "SG_CALLS": calls.as_posix(),
        "SG_ANSWERS": (tmp_path / "answers").as_posix(),
        "SG_WAREHOUSES": (tmp_path / "warehouses").as_posix(),
        "SG_JOBS": (tmp_path / "jobs").as_posix(),
        "SG_POLL_COUNT": (tmp_path / "poll_count").as_posix(),
        "SG_GET_COUNT": (tmp_path / "get_count").as_posix(),
        "SG_EXISTS_FROM": str(exists_from_call),
        # The waits are what this file is about, so they are turned down to keep the suite in
        # the fast lane. The DEFAULTS are asserted separately, in test_databricks_bundle.py.
        "SAMEGOLD_SQL_POLL_SECONDS": "0",
        "SAMEGOLD_SQL_TIMEOUT_SECONDS": "2",
        "SAMEGOLD_CATALOG": "probe_catalog",
        **(extra_env or {}),
    }
    # PATH is exported INSIDE bash rather than set in the parent environment, and that is not
    # a detail. A Windows parent hands bash a `;`-separated PATH which MSYS then converts and
    # reorders, so a stub prepended out here ended up BEHIND the real `databricks` CLI in
    # /usr/local/bin - and these tests silently drove the real tool against whatever
    # credentials the machine had. Exporting it in the shell that runs the script is the only
    # ordering this test controls.
    #
    # The script path is relative to `cwd` for a related reason: an absolute Windows path
    # handed to Git Bash comes out the other side with its separators eaten.
    # And the directory has to be in the form bash SEARCHES. Git Bash does not look in a
    # `C:/Users/...` PATH entry at all - it wants `/c/Users/...` - so the stub sat there
    # unfound while `command -v databricks` happily resolved the real CLI. `cygpath` is the
    # conversion, and it exists only where the conversion is needed.
    # `chmod` runs through bash, not through Python. `Path.chmod(0o755)` on Windows toggles
    # the read-only attribute and nothing else, so the stub would sit there unexecutable and
    # `databricks` would resolve to the real CLI further down PATH - a test quietly talking to
    # a live workspace. Bash's chmod sets the bit MSYS actually reads.
    # Before anything else: can the shell that will run the script actually SEE the fixture?
    # In some sandboxed environments (this repository's agent harness is one) the Python
    # process and `bash` have different views of the Windows temp directory, and every test
    # below then fails with the script talking to the real CLI further down PATH. That is an
    # environment fact, not a defect in the step under test, and it is worth saying in those
    # words rather than as six assertion errors - or, worse, as a skip that says nothing.
    visible = subprocess.run(
        ["bash", "-c", f'test -r "{_bash_path(stub)}"'],
        capture_output=True,
        check=False,
    )
    if visible.returncode != 0:
        pytest.skip(
            f"bash cannot read the stub this test just wrote ({stub}), so the script would "
            f"run against the real databricks CLI instead. The Python process and the shell "
            f"disagree about the filesystem here; these tests are meaningful where they do "
            f"not - Linux, WSL2, and a normal Windows checkout."
        )
    launch = (
        f'chmod +x "{stub}" 2>/dev/null || true; '
        f'export PATH="{_bash_path(stub_dir)}:$PATH"; '
        f"exec scripts/databricks_run.sh {subcommand} {second}".rstrip()
    )
    return subprocess.run(
        ["bash", "-c", launch],
        capture_output=True,
        text=True,
        env=environment,
        cwd=REPO,
        timeout=120,
        check=False,
    )


def test_a_statement_that_succeeds_immediately_is_accepted(tmp_path: Path) -> None:
    result = _run(tmp_path, [_state("SUCCEEDED")])
    assert result.returncode == 0, result.stdout + result.stderr
    assert "created" in result.stdout


def test_it_polls_through_pending_and_running_to_success(tmp_path: Path) -> None:
    """The cold-start case, which is the NORMAL case on Free Edition.

    A warehouse that stops itself after a few minutes idle means almost every run of this
    script starts one. The old code could not express this at all: its longest possible wait
    was one `wait_timeout`, and the API caps that at 50s.
    """
    result = _run(
        tmp_path,
        [_state("PENDING"), _state("PENDING"), _state("RUNNING"), _state("SUCCEEDED")],
        warehouses=STOPPED_WAREHOUSE,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "created" in result.stdout
    # It said what it was waiting for. A script that looks hung gets killed halfway, and
    # killed halfway is how the catalog and the report disagree.
    assert "cold start" in result.stdout
    assert "still PENDING" in result.stdout
    calls = (tmp_path / "calls").read_text(encoding="utf-8")
    assert "api get /api/2.0/sql/statements/stmt-1" in calls


def test_a_non_success_state_with_the_catalog_present_is_a_success(tmp_path: Path) -> None:
    """THE test. This is what happened on the first real workspace.

    The statement reported CANCELED, the script said "the catalog is not there", and the
    catalog was there - created by the very statement whose wait had been cancelled. CANCELED
    means "I stopped waiting", not "it did not happen", and the only way to tell the
    difference is to look.
    """
    result = _run(tmp_path, [_state("CANCELED")], exists_from_call=2)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CANCELED" in result.stdout
    assert "the catalog is there" in result.stdout
    assert "not there" not in result.stdout


def test_the_ceiling_expiring_still_re_checks_before_concluding(tmp_path: Path) -> None:
    """A ceiling that expires says nothing about the world, so it may not claim anything."""
    result = _run(tmp_path, [_state("PENDING")], exists_from_call=2)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "the catalog is there" in result.stdout


def test_a_real_failure_names_the_state_it_saw_and_says_it_looked(tmp_path: Path) -> None:
    result = _run(tmp_path, [_state("FAILED", error="PERMISSION_DENIED: no CREATE CATALOG")])
    assert result.returncode == 1
    output = result.stdout + result.stderr
    assert "FAILED" in output
    assert "PERMISSION_DENIED" in output
    # It claims absence only as something it checked, and it does not recite states it did
    # not see.
    assert "checked just now, not\nassumed" in output or "checked just now" in output
    assert "CANCELED" not in output


def test_a_state_nobody_anticipated_still_produces_a_correct_message(tmp_path: Path) -> None:
    """The by-ordinal-list defect of round 14, in its error-message form.

    A hand-written taxonomy that does not cover what it prints is worse than no taxonomy: the
    old one listed PENDING, RUNNING and FAILED, and printed CANCELED.
    """
    result = _run(tmp_path, [_state("SOMETHING_NEW")])
    assert result.returncode == 1
    output = result.stdout + result.stderr
    assert "SOMETHING_NEW" in output
    assert "no note about" in output


def test_the_catalog_is_never_created_through_the_unity_catalog_api(tmp_path: Path) -> None:
    """Belt and braces on the text check: the run itself must not make that call."""
    _run(tmp_path, [_state("SUCCEEDED")])
    calls = (tmp_path / "calls").read_text(encoding="utf-8")
    assert "catalogs create" not in calls, calls


# --------------------------------------------------------- the flag that never arrived
#
# `run-full-refresh` printed a full-refresh banner and then ran WITHOUT `--full-refresh-all`.
# The dispatch was `SAMEGOLD_FULL_REFRESH=1 require_cli; require_auth; step_run`, and a bash
# assignment prefixed to a command applies to THAT command: the variable lived for the
# duration of `require_cli` and was gone by the time `step_run` looked. One Free Edition run
# spent on a refresh that did not happen, and the update failed on
# DELTA_MERGE_INCOMPATIBLE_DATATYPE - the exact schema conflict the refresh exists to clear.
#
# Reading the `case` block would not have caught it. The text was right; the SEMANTICS were
# wrong. So these tests run the script and read the argv the CLI was actually called with.


def _captured_calls(tmp_path: Path) -> str:
    return (tmp_path / "calls").read_text(encoding="utf-8")


def test_run_full_refresh_actually_passes_the_flag(tmp_path: Path) -> None:
    """The argv, not the source. This is the whole point of the test."""
    result = _run(tmp_path, [_state("SUCCEEDED")], subcommand="run-full-refresh")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _captured_calls(tmp_path)
    assert "bundle run samegold_close" in calls, calls
    assert "--full-refresh-all" in calls, (
        f"`run-full-refresh` did not pass --full-refresh-all to the CLI. The captured "
        f"invocation was:\n{calls}"
    )
    # And the job-shaped spelling of the same intent. `databricks bundle run --help` groups
    # `--full-refresh-all` under "Pipeline Flags", and the KEY this script passes is a JOB;
    # `--pipeline-params` is the one the help attaches to "jobs with pipeline tasks". Which of
    # the two the CLI acts on cannot be observed without a workspace - both parse, and the CLI
    # goes to authentication before it would say - so both are sent and both are asserted.
    assert "full_refresh=true" in calls, calls
    # And it said so, which is the half that used to be true on its own.
    assert "FULL REFRESH" in result.stdout


def test_plain_run_does_not_pass_the_flag(tmp_path: Path) -> None:
    """The symmetric half: a refresh nobody asked for is a re-read of the whole landing zone.

    On Free Edition that is quota, and quota exhaustion stops all compute for the day.
    """
    result = _run(tmp_path, [_state("SUCCEEDED")], subcommand="run")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _captured_calls(tmp_path)
    assert "bundle run samegold_close" in calls, calls
    assert "--full-refresh-all" not in calls, calls
    assert "full_refresh=true" not in calls, calls
    assert "FULL REFRESH" not in result.stdout


def test_the_banner_governs_the_command_rather_than_describing_it(tmp_path: Path) -> None:
    """If the announcement and the invocation disagree, the script dies instead of running.

    The defect this guards was not "the flag was missing" but "the message and the command
    came from two different places, and only the message was checked". So the check is that
    they are the same object: with SAMEGOLD_FULL_REFRESH set, the flag has to be in the argv
    about to be executed, or nothing is executed at all.
    """
    # Set from the OUTSIDE, which is how the user worked around the bug - it must still work.
    result = _run(
        tmp_path,
        [_state("SUCCEEDED")],
        subcommand="run",
        extra_env={"SAMEGOLD_FULL_REFRESH": "1"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--full-refresh-all" in _captured_calls(tmp_path)

    # And the guard itself is present in the code path, phrased as a refusal rather than a
    # warning: a script that notices the disagreement and proceeds has not noticed anything.
    source = (REPO / "scripts" / "databricks_run.sh").read_text(encoding="utf-8")
    body = source.split("step_run() {", 1)[1].split("\n}", 1)[0]
    assert "Refusing to spend a Free Edition run" in body


def test_job_parameters_reach_the_cli_through_the_guarded_path(tmp_path: Path) -> None:
    """The second hole of the shape `--only` was the first instance of.

    `require_fresh_deployment` guards the run by being inside `step_run`, so every option this
    script does not wrap is a reason to type `databricks bundle run` by hand - and a hand-typed
    run goes straight past the guard. That is not hypothetical: this repository's own record
    was produced by a hand-typed run with `--only`, which is the finding that put the selection
    into the script. `--params` was the next one, and the three runs of 5 September 2026 needed
    it: `fail_task=verify_no_restatement` is how the failed run was made to fail.
    """
    result = _run(
        tmp_path,
        [_state("SUCCEEDED")],
        subcommand="run",
        extra_env={"SAMEGOLD_PARAMS": "fail_task=verify_no_restatement"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _captured_calls(tmp_path)
    assert "--params" in calls and "fail_task=verify_no_restatement" in calls, calls
    assert "PARAMS: fail_task=verify_no_restatement" in result.stdout, result.stdout
    # Through the guard, not around it: the freshness check ran before the run did.
    assert "bundle run samegold_close" in calls, calls


def test_a_plain_run_passes_no_parameters(tmp_path: Path) -> None:
    """`fail_task` defaults to empty for a reason; a run that carries it by accident fails a
    task on purpose and spends the quota doing it."""
    result = _run(tmp_path, [_state("SUCCEEDED")], subcommand="run")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--params" not in _captured_calls(tmp_path)
    assert "PARAMS:" not in result.stdout


def test_the_guard_says_what_it_does_not_cover(tmp_path: Path) -> None:
    """A guard people believe covers more than it does is worse than a smaller one.

    Wrapping `--only` and `--params` makes the convenient path the guarded path. It does not
    and cannot cover a `databricks bundle run` typed into a terminal, an API call, or Run now
    in the workspace - the workspace has no way to know which commit a laptop is sitting on -
    and the script says so where the wrapping is done rather than leaving a reader to assume.
    """
    source = (REPO / "scripts" / "databricks_run.sh").read_text(encoding="utf-8")
    assert "WRAPS the CLI, it does not replace it" in source
    assert "Run now button" in source


# ------------------------------------------------- the run that executed code nobody deployed
#
# `databricks bundle run` runs what was DEPLOYED. On 4 September 2026 a run of this lane
# executed a notebook from two commits earlier and ended SUCCESS: the pipeline was green, the
# close was correct, and the record it published carried no `deploy` key and no row-level
# capture, because the deployed notebook predated both. One fault, two symptoms, and no error
# anywhere - nothing compared the deployed code with the tree.
#
# `step_deploy` already carries the commit INTO the deploy, as a `base_parameters` field on the
# publish_evidence task, so the workspace can be asked. These tests are that question being
# asked, driven against the stub CLI rather than reasoned about: what the step CONCLUDES from
# each answer, and - the half that matters most - whether it spent the run anyway.


def test_a_deployment_older_than_head_is_refused_rather_than_run(tmp_path: Path) -> None:
    """The finding, closed. A refusal, and no `bundle run` in the argv."""
    stale = "0" * 40
    result = _run(tmp_path, [], subcommand="run", jobs=_deployed_job(stale))

    assert result.returncode != 0, result.stdout + result.stderr
    message = result.stdout + result.stderr
    # Both shas, because "your deployment is stale" without them is a puzzle: the next question
    # anybody asks is which commit is up there.
    assert stale in message, message
    assert _head() in message, message
    # And the thing that actually costs: it did not run.
    assert "bundle run samegold_close" not in _captured_calls(tmp_path), _captured_calls(tmp_path)


def test_the_refusal_names_the_way_to_run_the_deployment_on_purpose(tmp_path: Path) -> None:
    """A gate with no legitimate exit is a gate people learn to delete.

    Reproducing what an older deployment published is a real thing to want, and the answer to
    it should be a decision somebody made in one visible word rather than a commented-out line
    in this script.
    """
    result = _run(tmp_path, [], subcommand="run", jobs=_deployed_job("0" * 40))
    assert "SAMEGOLD_RUN_STALE=1" in result.stdout + result.stderr


def test_the_override_runs_the_stale_deployment_and_says_that_is_what_it_did(
    tmp_path: Path,
) -> None:
    """The escape hatch, exercised - and the announcement checked against the argv again.

    An override that is honoured silently is how somebody ends up reading a record from an
    older commit as though it described this tree.
    """
    stale = "0" * 40
    result = _run(
        tmp_path,
        [],
        subcommand="run",
        jobs=_deployed_job(stale),
        extra_env={"SAMEGOLD_RUN_STALE": "1"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "bundle run samegold_close" in _captured_calls(tmp_path)
    assert stale in result.stdout, result.stdout


def test_a_deployment_from_head_is_run(tmp_path: Path) -> None:
    """The symmetric half, without which the guard could be `exit 1` and pass everything above."""
    result = _run(tmp_path, [], subcommand="run", jobs=_deployed_job(_head()))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "bundle run samegold_close" in _captured_calls(tmp_path)


def test_the_jobs_api_envelope_is_read_as_well_as_the_bare_array(tmp_path: Path) -> None:
    """Both shapes, because which one the CLI prints cannot be checked from outside a workspace.

    The parser accepts `[...]` and `{"jobs": [...]}`. Asserting one of them here would be this
    repository's own recurring defect - a check that is well defined and answers a question
    nobody asked - so the test says the same thing the parser does: either shape is an answer.
    """
    result = _run(tmp_path, [], subcommand="run", jobs=_deployed_job(_head(), wrapped=True))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "bundle run samegold_close" in _captured_calls(tmp_path)


def test_a_workspace_with_no_such_job_is_refused_rather_than_left_to_the_cli(
    tmp_path: Path,
) -> None:
    """Nothing deployed at all is a different fact from a stale deployment, and says so.

    `databricks bundle run` would fail here too. It would fail later, after authentication and
    a bundle load, with a message about a resource key rather than about a deploy that never
    happened.
    """
    result = _run(tmp_path, [], subcommand="run", jobs="[]")
    assert result.returncode != 0, result.stdout + result.stderr
    assert "nothing" in (result.stdout + result.stderr).lower()
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_a_deployment_that_cannot_name_its_own_commit_is_refused(tmp_path: Path) -> None:
    """`bundle deploy` by hand leaves `deploy_commit` at its default, the word "unknown".

    That is not "stale" and it is not "fresh": it is a deployment that cannot be compared, and
    reporting it as either would be inventing a fact. The refusal says which of the two it
    could not tell.
    """
    result = _run(tmp_path, [], subcommand="run", jobs=_deployed_job("unknown"))
    assert result.returncode != 0, result.stdout + result.stderr
    assert "unknown" in result.stdout + result.stderr
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_a_deployment_predating_the_parameter_is_refused(tmp_path: Path) -> None:
    """A job whose tasks carry no `deploy_commit` at all - which is what the deployment that
    caused this finding actually looked like, since the parameter was added after it."""
    job = json.dumps(
        [
            {
                "job_id": 1,
                "settings": {
                    "name": "samegold monthly close",
                    "tasks": [
                        {
                            "task_key": "publish_evidence",
                            "notebook_task": {
                                "notebook_path": "../src/publish_evidence.py",
                                "base_parameters": {"catalog": "samegold"},
                            },
                        }
                    ],
                },
            }
        ]
    )
    result = _run(tmp_path, [], subcommand="run", jobs=job)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_an_unreadable_answer_is_refused_and_not_read_as_nothing_deployed(
    tmp_path: Path,
) -> None:
    """The distinction the `MAX`-over-a-state-string finding is about.

    A CLI that answers with something this cannot parse has said nothing, and "it said nothing"
    is not "no job exists". Collapsing the two would make the message name the wrong cause and
    send the reader to deploy something that is already there.
    """
    result = _run(tmp_path, [], subcommand="run", jobs="not json at all")
    assert result.returncode != 0, result.stdout + result.stderr
    message = result.stdout + result.stderr
    assert "could not read" in message.lower(), message
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_a_cli_that_fails_the_lookup_dies_with_a_message_rather_than_bare(
    tmp_path: Path,
) -> None:
    """The failure mode the guard itself introduced, and it is a shell one.

    The lookup was written as `databricks jobs list ... | python -c ...`. This script runs
    under `set -o pipefail`, so when the first half of that pipeline fails the whole command
    substitution fails, and `set -e` then kills the script AT THE ASSIGNMENT - exit 1, no
    output, at the one point in the script whose entire purpose is to say what went wrong.

    A guard that fails silently is worse than the failure it guards, so the CLI call is its own
    statement now and this is the test that says so.
    """
    result = _run(tmp_path, [], subcommand="run", extra_env={"SG_JOBS_FAIL": "1"})
    assert result.returncode != 0, result.stdout + result.stderr
    message = result.stdout + result.stderr
    assert message.strip(), "the script died without saying anything, which is the defect"
    assert "jobs list" in message, message
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_a_task_selection_reaches_the_cli(tmp_path: Path) -> None:
    """The hole this closes is the one the guard was written for.

    The command that produced this repository's evidence was typed by hand -
    `databricks bundle run samegold_close -t free --var="catalog=samegold" --only
    publish_evidence` - and a hand-typed `bundle run` goes straight past
    `require_fresh_deployment`. Leaving the convenient path outside the guard is leaving the
    guard optional.
    """
    result = _run(tmp_path, [], subcommand="run", second="publish_evidence")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _captured_calls(tmp_path)
    assert "--only publish_evidence" in calls, calls
    assert "ONLY: publish_evidence" in result.stdout


def test_a_task_selection_goes_through_the_freshness_guard(tmp_path: Path) -> None:
    """The point of routing it through the script rather than typing it."""
    result = _run(
        tmp_path,
        [],
        subcommand="run",
        second="publish_evidence",
        jobs=_deployed_job("0" * 40),
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)


def test_a_plain_run_selects_no_tasks(tmp_path: Path) -> None:
    """The symmetric half: `--only` with nothing after it would run nothing at all."""
    result = _run(tmp_path, [], subcommand="run")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--only" not in _captured_calls(tmp_path)
    assert "ONLY:" not in result.stdout


def test_a_subcommand_that_takes_no_selection_refuses_one(tmp_path: Path) -> None:
    """An argument nobody reads is an argument that looks obeyed.

    `deploy publish_evidence` must not deploy everything and say nothing about the word it was
    handed - that is the shape of the full-refresh banner that announced a flag it did not
    pass.
    """
    result = _run(tmp_path, [_state("SUCCEEDED")], subcommand="deploy", second="publish_evidence")
    assert result.returncode != 0, result.stdout + result.stderr
    assert "takes no task selection" in result.stdout + result.stderr


def test_run_full_refresh_is_guarded_too(tmp_path: Path) -> None:
    """The other door into `step_run`, which is the one that spends most.

    A full refresh re-reads the whole landing zone. Doing that against code from two commits
    ago is the expensive version of the failure this guard exists for, so the dispatch is
    checked rather than assumed to share the path.
    """
    result = _run(tmp_path, [], subcommand="run-full-refresh", jobs=_deployed_job("0" * 40))
    assert result.returncode != 0, result.stdout + result.stderr
    assert "bundle run samegold_close" not in _captured_calls(tmp_path)
    # And it named the override for the subcommand actually being run, not for a different one.
    assert "SAMEGOLD_RUN_STALE=1 scripts/databricks_run.sh run-full-refresh" in (
        result.stdout + result.stderr
    )


def test_no_shell_script_leaks_an_assignment_into_a_later_function(tmp_path: Path) -> None:
    """The CLASS, swept, rather than the one line that cost a run.

    `VAR=1 some_function; other_function` is legal bash and reads as "set VAR, then call
    both". It means "call some_function with VAR set, then call other_function without it".
    Every reader of `SAMEGOLD_FULL_REFRESH=1 require_cli; require_auth; step_run` saw an
    assignment doing what it says; what it did was scope the variable to `require_cli` and
    drop it before `step_run`, and the run that followed spent Free Edition compute on a
    refresh that did not happen.

    The form is only dangerous when the prefixed command is a FUNCTION IN THE SAME FILE and
    the statement continues with more commands: prefixing an external command
    (`SAMEGOLD_BIN=$(BIN) scripts/databricks_run.sh all`, in the Makefile) is the correct and
    intended use, because the variable is meant for that process and nothing after it.
    """
    import re

    scripts = sorted((REPO / "scripts").glob("*.sh"))
    assert scripts, "no shell scripts found to sweep, so this test is checking nothing"
    offenders: list[str] = []
    for script in scripts:
        text = script.read_text(encoding="utf-8")
        functions = set(re.findall(r"^([a-z_][a-z0-9_]*)\s*\(\)\s*\{", text, re.MULTILINE))
        for number, line in enumerate(text.splitlines(), start=1):
            code = line.split("#", 1)[0]
            match = re.search(r"\b[A-Z_][A-Z0-9_]*=\S*\s+([a-z_][a-z0-9_]*)\b", code)
            if not match or match.group(1) not in functions:
                continue
            # Harmless when it is the whole statement: the variable is scoped to the one call
            # and nothing follows that could expect it.
            if ";" in code[match.end() :] or "&&" in code[match.end() :]:
                offenders.append(f"{script.name}:{number}: {line.strip()}")
    assert not offenders, (
        "an environment assignment prefixed to a shell FUNCTION scopes to that function "
        "alone, and these lines continue with more commands that will not see it:\n  "
        + "\n  ".join(offenders)
    )


def test_the_fetch_step_does_not_call_its_own_output_an_uncommitted_change(tmp_path: Path) -> None:
    """`fetch.json` said `tree_dirty: true` on every fetch that ever worked.

    `step_fetch` copies `SG-DBX-01.json` into `evidence/databricks/` and then records whether
    the tree is dirty with `git status --porcelain`. The record it had just written is in that
    output, so the field was structurally true and said nothing about the code that was
    deployed - which is the field a reader uses to decide whether the commit named beside it
    describes what ran.

    Round 19 found this exact class in `samegold.generator.seeds.current_tree` (the evidence
    sweep counting its own output) and fixed it in Python. The shell script one directory away
    kept the bare version for two more rounds.

    The filter is EXTRACTED from the script and run, rather than restated here: the defect was
    never in what the script meant, and a copy of the awk program in this file would be a
    second implementation of it.
    """
    import re
    import subprocess

    source = SCRIPT.read_text(encoding="utf-8")
    match = re.search(r"awk '(\{ p = substr.*?\})'", source, re.S)
    assert match, "the code_changes filter is no longer where this test looks for it"
    program = match.group(1)

    porcelain = (
        " M evidence/databricks/SG-DBX-01.json\n"
        "?? evidence/databricks/fetch.json\n"
        " M src/samegold/cli.py\n"
        "?? notes.txt\n"
    )
    out = subprocess.run(
        ["awk", program],
        input=porcelain,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.splitlines() == ["src/samegold/cli.py", "notes.txt"], out.stdout

    # And a tree whose ONLY changes are the record it just fetched is clean.
    only_evidence = " M evidence/databricks/SG-DBX-01.json\n?? evidence/databricks/fetch.json\n"
    out = subprocess.run(
        ["awk", program], input=only_evidence, capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "", out.stdout


# A record with just enough in it for the fetch step's own summary to read.
def _record_fixture(tree_dirty: object = False) -> str:
    return json.dumps(
        {
            "claim_id": "SG-DBX-01",
            "update": [{"update_id": "u-1", "last_state": "COMPLETED"}],
            "incomplete": [],
            "expectations": [],
            "quarantine_by_reason": [],
            "deploy": {"commit": "a" * 40, "tree_dirty": tree_dirty},
        }
    )


RECORD_FIXTURE = _record_fixture()


def _fetch(
    tmp_path: Path, *, missing: str = "", record: str | None = None, label: str = ""
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Run the fetch step for real, with the evidence directory pointed at a temp dir."""
    out = tmp_path / "evidence"
    out.mkdir()
    payload = tmp_path / "payload.json"
    payload.write_text(record or RECORD_FIXTURE, encoding="utf-8", newline="\n")
    result = _run(
        tmp_path,
        [_state("SUCCEEDED")],
        subcommand="fetch",
        second=label,
        extra_env={
            "SAMEGOLD_EVIDENCE_OUT": out.as_posix(),
            "SG_FS_CP_PAYLOAD": payload.as_posix(),
            "SG_FS_CP_MISSING": missing,
        },
    )
    return result, out


def test_a_labelled_fetch_keeps_the_run_beside_the_canonical_record_and_not_over_it(
    tmp_path: Path,
) -> None:
    """The run whose record is worth keeping and must not be the one documents render from.

    Run 2 of this round FAILS on purpose, and the record it produces is the artefact: it is
    what shows that `run_if: ALL_DONE` keeps evidence alive through a failure and that the
    record says the verification did not report. `step_fetch` writes to one path, so fetching
    it would REPLACE the record every page in this repository is rendered from - a failed run
    becoming the canonical description of the lane.

    So the label puts it under a name of its own. The canonical files are not touched, and
    nothing that renders or compares reads a labelled one: `evidence/databricks/README.md`
    says which file is canonical and why, and this is the switch that makes obeying it
    possible.
    """
    out = tmp_path / "evidence"
    out.mkdir()
    canonical = out / "SG-DBX-01.json"
    canonical.write_text('{"claim_id": "the one the documents render from"}', encoding="utf-8")
    payload = tmp_path / "payload.json"
    payload.write_text(RECORD_FIXTURE, encoding="utf-8", newline="\n")
    result = _run(
        tmp_path,
        [_state("SUCCEEDED")],
        subcommand="fetch",
        second="run-2-failed",
        extra_env={
            "SAMEGOLD_EVIDENCE_OUT": out.as_posix(),
            "SG_FS_CP_PAYLOAD": payload.as_posix(),
            "SG_FS_CP_MISSING": "",
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (out / "SG-DBX-01.run-2-failed.json").exists(), result.stdout
    assert (out / "dim_customer_scd2.run-2-failed.json").exists(), result.stdout
    assert (out / "fetch.run-2-failed.json").exists(), result.stdout
    assert canonical.read_text(encoding="utf-8") == (
        '{"claim_id": "the one the documents render from"}'
    ), "a labelled fetch overwrote the canonical record, which is what it exists to avoid"
    assert "LABELLED" in result.stdout, result.stdout
    # And the provenance report is about the file that was just fetched, not about whatever
    # happens to be sitting at the canonical path.
    assert "clean tree at" in result.stdout, result.stdout


def test_a_label_that_is_not_a_filename_is_refused(tmp_path: Path) -> None:
    """The label becomes part of a path, so it is checked rather than interpolated."""
    result, out = _fetch(tmp_path, label="../../etc/passwd")
    assert result.returncode != 0, result.stdout
    assert "label made of lower-case letters" in result.stdout + result.stderr
    assert not list(out.glob("*.json")), sorted(p.name for p in out.glob("*"))


def test_the_fetch_says_when_the_evidence_must_not_be_committed(tmp_path: Path) -> None:
    """The round's own incident, as a test.

    `require_fresh_deployment` was committed and the FINDINGS.md entry describing it was not,
    so the next deploy went out dirty and the record said `tree_dirty: true`. The provenance
    caught it; no person did. This is the step saying it out loud at the moment the files land.

    It does NOT die, and that is a decision rather than an omission: the files are already on
    disk by the time this can read the field, the commit happens later and possibly elsewhere,
    and a message that cannot govern what it announces is this script's oldest defect. The
    refusal lives in `test_databricks_bundle.py`, where the commit is. This is the part that
    tells you now.
    """
    result, out = _fetch(tmp_path, record=_record_fixture(tree_dirty=True))
    assert result.returncode == 0, (
        f"the fetch died on a dirty deploy. It should report and finish: the files are "
        f"already down, and the rest of this step's output is what you read to check the "
        f"run.\n{result.stdout}{result.stderr}"
    )
    assert (out / "SG-DBX-01.json").exists()
    assert "DO NOT COMMIT THIS EVIDENCE" in result.stdout, result.stdout
    # And it says WHY, not only that. A bare refusal teaches nothing and gets worked around.
    assert "does not contain the code that ran" in result.stdout, result.stdout
    # And it says what to do instead, with the selection this script now takes.
    assert "run publish_evidence" in result.stdout, result.stdout


def test_a_clean_deploy_is_reported_as_clean_and_says_nothing_alarming(tmp_path: Path) -> None:
    """The symmetric half, without which the message above could be printed unconditionally."""
    result, _ = _fetch(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DO NOT COMMIT" not in result.stdout, result.stdout
    assert "clean tree at" in result.stdout, result.stdout


def test_a_record_that_cannot_say_is_not_read_as_clean(tmp_path: Path) -> None:
    """`unknown` is a third state, and defaulting it to "clean" is how a hole becomes a tick.

    A deploy by hand without `--var="deploy_tree_dirty=..."` leaves the word "unknown". That
    record cannot be tied to a commit either, and the two reasons are different enough that
    the message says which.
    """
    result, _ = _fetch(tmp_path, record=_record_fixture(tree_dirty="unknown"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "does not say whether the tree" in result.stdout, result.stdout
    assert "clean tree at" not in result.stdout, result.stdout


def test_the_string_true_is_read_as_dirty(tmp_path: Path) -> None:
    """A notebook deployed before the boolean conversion publishes `"true"`, which is truthy
    either way - and that truthiness is the reason the conversion happened. The reader here
    understands the old shape so a regression to it cannot go quiet."""
    result, _ = _fetch(tmp_path, record=_record_fixture(tree_dirty="true"))
    assert "DO NOT COMMIT THIS EVIDENCE" in result.stdout, result.stdout


def test_the_fetch_brings_down_the_row_level_capture(tmp_path: Path) -> None:
    """The capture is fetched by the same step as the record, or it goes stale by hand.

    It was exported by hand for one run, which produced a file that could not say which run
    it came from: a later run replaced the record, nothing replaced the capture, and the
    row-by-row comparison would have gone on passing against rows the workspace no longer
    held. The query already existed - it was written into docs/databricks-run.md for a person
    to paste - so the fix is that nobody has to remember.

    Executed against a stub, not read: `run-full-refresh` is the standing example of a case
    statement that read correctly and did something else.
    """
    result, out = _fetch(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (out / "SG-DBX-01.json").exists(), result.stdout
    assert (out / "dim_customer_scd2.json").exists(), (
        f"the fetch step did not bring down the row-level capture. What it ran was:\n"
        f"{_captured_calls(tmp_path)}"
    )
    assert (out / "fetch.json").exists()
    calls = _captured_calls(tmp_path)
    assert "dim_customer_scd2.json" in calls, calls


def test_a_missing_capture_is_reported_and_does_not_take_the_record_down(tmp_path: Path) -> None:
    """A run from before publish_evidence.py wrote the capture has none.

    That must not fail the fetch - the record is what the step exists to bring down, and it
    arrived - but it must not pass in silence either, because any capture already in the
    repository now describes an OLDER run than the record beside it. The loud version of that
    is the provenance test in tests/fast/test_databricks_dimension_parity.py; this is the
    warning that tells you before you get there.
    """
    result, out = _fetch(tmp_path, missing="dim_customer_scd2.json")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (out / "SG-DBX-01.json").exists()
    assert not (out / "dim_customer_scd2.json").exists()
    assert "WARNING" in result.stdout and "dim_customer_scd2.json" in result.stdout, result.stdout


def test_the_deploy_tells_the_workspace_which_commit_it_is_deploying(tmp_path: Path) -> None:
    """The commit travels WITH the deploy, so what the run publishes can name it.

    The only commit anywhere near this lane used to be the one `step_fetch` writes into
    fetch.json afterwards - this machine's HEAD when somebody copied the files down. That is a
    different fact from "the code that produced these tables", and a later fetch can stamp it
    onto files it did not produce. A bundle variable cannot: it is read at deploy time and
    baked into the job the run executes.

    The argv again, not the source.
    """
    result = _run(tmp_path, [_state("SUCCEEDED")], subcommand="deploy")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _captured_calls(tmp_path)
    assert "bundle deploy" in calls, calls

    head = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert f"--var=deploy_commit={head}" in calls, (
        f"the deploy did not pass the commit it was deploying. What it ran was:\n{calls}"
    )
    assert re.search(r"--var=deploy_tree_dirty=(true|false)\b", calls), (
        f"a commit alone is a claim the deploy does not honour when the tree has "
        f"uncommitted code. What it ran was:\n{calls}"
    )
