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

import os
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
  "warehouses list") cat "$SG_WAREHOUSES" ; exit 0 ;;
  "warehouses start") exit 0 ;;
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

    environment = {
        **os.environ,
        "SG_CALLS": calls.as_posix(),
        "SG_ANSWERS": (tmp_path / "answers").as_posix(),
        "SG_WAREHOUSES": (tmp_path / "warehouses").as_posix(),
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
        f"exec scripts/databricks_run.sh {subcommand}"
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
