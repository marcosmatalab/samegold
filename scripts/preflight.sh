#!/usr/bin/env bash
#
# Everything CI runs, in one command, before a push.
#
# This script exists because of a specific failure, twice. `make ci-local` ran the FAST
# workflow and was named as though it ran CI, so a change to `tests/spark/` could be pushed
# with a green local run and a red `spark` workflow. That is how round 12 found a Delta job
# that had been red for two days while the documents called the lane unexecuted - and it is
# how round 13, the round that wrote the ADR about it, pushed a red Spark lane of its own.
# The author's machine is not the repository. The gate has to be the whole of CI or it is a
# gate that reports the scope of the command.
#
# Two rules it will not break:
#
#   * it never reports success for a lane it did not run. Native Windows cannot run Spark at
#     all (Hadoop's NativeIO calls winutils.exe), so on Windows this exits non-zero and says
#     so in one sentence. A partial run that exits 0 is the thing being fixed;
#   * every command below is the command CI runs, written out literally rather than built
#     from variables, and tests/fast/test_preflight.py fails if the two drift apart.
#
# What it deliberately does NOT run, and why:
#
#   evidence.yml    an hour of compute on a weekly schedule; it WRITES evidence, and a
#                   pre-push gate that rewrites the evidence chain is not a gate.
#   databricks.yml  needs an account, a token and a Free Edition workspace, and a run there
#                   can spend that account's quota for the day. `make databricks` is that one.
#
# Both of those exclusions are named in tests/fast/test_preflight.py, so a NEW workflow forces
# a decision here rather than being silently uncovered.

set -uo pipefail  # not -e: every step runs, and the failures are reported together

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# The venv's executables go on PATH, so the commands below can be written exactly as CI
# writes them: CI installs the package into the runner's environment and calls `pytest`,
# `ruff` and `mypy` by name.
# SAMEGOLD_BIN FIRST, because it is the explicit override and an explicit override that
# loses to a default is not an override. It matters on exactly the machine this repository is
# written on: `.venv/bin` there is a symlink to `.venv/Scripts`, so running this script inside
# WSL2 with `.venv` checked first would put Windows .exe files on a Linux PATH.
for candidate in "${SAMEGOLD_BIN:-}" "$REPO/.venv/bin" "$REPO/.venv/Scripts"; do
    if [ -n "$candidate" ] && [ -d "$candidate" ]; then
        PATH="$candidate:$PATH"
        break
    fi
done
export PATH
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
# CI runs on Linux, where Python's default text encoding is already UTF-8. On Windows it is
# cp1252, and several readers in this repository call `read_text()` without naming an
# encoding, so the same test that is green on a runner raises UnicodeDecodeError on an em
# dash locally. Turning UTF-8 mode on makes a local run read files the way CI reads them,
# which is the only thing this gate is for. It is a workaround, not a fix: the fix is an
# `encoding=` argument at each of those call sites.
export PYTHONUTF8=1

failed=()
skipped=()

bold() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

run() {
    local label="$1" command="$2"
    bold "$label: $command"
    if eval "$command"; then
        return 0
    fi
    failed+=("$label ($command)")
    return 1
}

# Said BEFORE the three minutes of fast lane rather than after them: on a machine that cannot
# run the JVM lanes, the answer is already decided, and finding that out at the end is how
# people learn to skip the gate.
case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
        printf '
[33mnote: this is native Windows, where the Spark lanes cannot run at all.
'
        printf 'This gate will run everything else and then exit non-zero. Run it in WSL2 or
'
        printf 'Linux with a JDK 21 for a verdict you can push on - see CONTRIBUTING.md.[0m
'
        ;;
esac

# ------------------------------------------------------------------ .github/workflows/fast.yml
run "fast/tests"        "pytest tests/fast -q"
run "fast/lint"         "ruff check src tests databricks pipelines"
run "fast/format"       "ruff format --check src tests databricks pipelines"
run "fast/types"        "mypy"
run "fast/check"        "samegold check"

# ------------------------------------------------------------------ .github/workflows/spark.yml
#
# Two jobs, and the second is not a repeat of the first: `spark-no-delta` runs with
# SAMEGOLD_STORAGE=parquet and proves the transformations agree with the reference without
# ever reaching Maven Central; the `delta` job runs the same tests with the real Delta jars
# and then the Delta-specific lane. They are separate PROCESSES on purpose - a Spark session
# is a per-process singleton, so a parquet session created by the first would be handed to the
# second by getOrCreate.
jvm_lanes_can_run() {
    case "$(uname -s)" in
        MINGW* | MSYS* | CYGWIN*)
            skipped+=("spark and delta: native Windows cannot run Spark (Hadoop's NativeIO needs winutils.exe/hadoop.dll); run this script inside WSL2 or Linux with a JDK 21")
            return 1
            ;;
    esac
    if ! command -v java >/dev/null 2>&1; then
        skipped+=("spark and delta: no java on PATH; the lanes need a JDK 21 (Temurin), see docs/adr/0002-version-pinning.md")
        return 1
    fi
    if ! python -c "import pyspark" >/dev/null 2>&1; then
        skipped+=("spark and delta: pyspark is not installed; run 'make install-spark' first")
        return 1
    fi
    return 0
}

if jvm_lanes_can_run; then
    run "spark-no-delta" "SAMEGOLD_STORAGE=parquet pytest tests/spark -q -m spark"
    run "delta/spark" "pytest tests/spark -q -m spark"
    run "delta/delta" "pytest tests/delta -q"
fi

# ------------------------------------------------------------------ the verdict
echo
if [ ${#skipped[@]} -gt 0 ]; then
    for reason in "${skipped[@]}"; do
        printf '\033[33mNOT RUN  %s\033[0m\n' "$reason"
    done
fi
if [ ${#failed[@]} -gt 0 ]; then
    for reason in "${failed[@]}"; do
        printf '\033[31mFAILED   %s\033[0m\n' "$reason"
    done
fi

if [ ${#failed[@]} -eq 0 ] && [ ${#skipped[@]} -eq 0 ]; then
    printf '\n\033[32mpreflight: everything the fast and spark workflows run is green here.\033[0m\n\n'
    exit 0
fi
# Non-zero for a lane that did not run, not only for a lane that failed. "It passed on my
# machine" has to mean the same thing as "it passed in CI", and a lane this machine cannot
# execute is a lane this machine cannot vouch for.
printf '\n\033[31mpreflight: NOT green. %d failed, %d not run.\033[0m\n\n' \
    "${#failed[@]}" "${#skipped[@]}"
exit 1
