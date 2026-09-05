#!/usr/bin/env bash
#
# The Databricks lane, end to end, from two environment variables.
#
#   export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
#   export DATABRICKS_TOKEN=dapi...
#   make databricks
#
# It does six things, in this order, and any one of them can be run alone (plus
# `run-full-refresh`, which is `run` with Auto Loader's cached schema thrown away):
#
#   catalog   create the Unity Catalog catalog if it is missing, with SQL. A bundle cannot:
#             there is no `catalogs` resource type, and a schema whose catalog does not exist
#             fails at DEPLOY time. Neither can `databricks catalogs create` on Free Edition:
#             that API wants a metastore storage root, and Default Storage has none.
#   validate  databricks bundle validate -t free
#   deploy    databricks bundle deploy   -t free   (schemas, volumes, pipeline, job)
#   seed      generate events with the OSS generator and upload them to the landing volume,
#             because a pipeline over an empty directory reports nothing and "no expectation
#             failed" and "no row was read" would arrive as the same evidence
#   run       databricks bundle run samegold_close -t free, after REFUSING to run when the
#             deployed job says it was deployed from a commit that is not HEAD. That is
#             what `bundle run` actually executes, and a run against an older deployment
#             is green about code this repository cannot see. SAMEGOLD_RUN_STALE=1 is the
#             way to say you meant it. A second argument selects tasks -
#             `run publish_evidence` - so that a partial run goes through the same guard
#             instead of round the side of it.
#   fetch     copy the SG-DBX-01 record out of the workspace into evidence/databricks/
#
# Every failure here should say what to do about it. A stack trace from a CLI that was never
# installed, or a 403 from a token that expired, is not a message: it is a puzzle.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE="$REPO/databricks"
TARGET="${SAMEGOLD_DBX_TARGET:-free}"
# The subcommand, captured HERE rather than read as "$1" inside a function, which is the
# function's own first argument and not the script's. The first version of the freshness
# guard below did read it that way and told everybody who ran `run-full-refresh` to retry
# with `run` - a message that names the wrong command, which is this script's own oldest
# recurring defect wearing a third set of clothes. A test asserts the message names the
# subcommand actually being run, and that test is how this was found.
SUBCOMMAND="${1:-all}"
# The task keys to run, for `run` and `run-full-refresh`. It exists because the command
# that produced this repository's own evidence was typed by hand -
#   databricks bundle run samegold_close -t free --var="catalog=samegold" --only publish_evidence
# - and a hand-typed `bundle run` goes straight past `require_fresh_deployment`. That is the
# exact hole the guard was written for, left open by the one command in the lane that is
# convenient enough to reach for. A selection is now something this script does, so the
# guard covers it.
ONLY_TASKS="${2:-}"
# `fetch` takes the same second argument, and it means something else there: a LABEL, which
# puts the run's files under names of their own instead of over the canonical ones.
#
# The reason is the run this is being written for. A deliberately failed run produces a record
# whose whole value is that it says the run failed, and `step_fetch` writes to one path - so
# fetching it REPLACES the record every document in this repository renders from. An artefact
# of a failed run cannot be the canonical record of a repository whose pages are generated from
# it; that is the same argument as the dirty-tree guard a few functions below, one step later
# in the same pipeline.
#
# `evidence/databricks/README.md` says which file is canonical and why. This is the switch that
# makes it possible to obey.
FETCH_LABEL=""
if [ "$SUBCOMMAND" = "fetch" ] && [ -n "$ONLY_TASKS" ]; then
    FETCH_LABEL="$ONLY_TASKS"
    ONLY_TASKS=""
fi
CATALOG="${SAMEGOLD_CATALOG:-samegold}"
BIN="${SAMEGOLD_BIN:-$REPO/.venv/bin}"
# Overridable so that `step_fetch` can be RUN in a test instead of read. Everything this
# script does to the workspace is stubbed in tests/fast/test_databricks_catalog_step.py by
# putting a fake `databricks` on PATH; the one thing that was not stubbable was where the
# files land, because it was this repository's own evidence directory - so a test that
# executed the step would have overwritten the committed record with a fixture. Reading the
# source instead is what round twenty-one established does not count.
OUT="${SAMEGOLD_EVIDENCE_OUT:-$REPO/evidence/databricks}"
LANDING="dbfs:/Volumes/$CATALOG/raw/landing"
EVIDENCE_VOLUME="dbfs:/Volumes/$CATALOG/raw/evidence"

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die()  { printf '\n\033[31mmake databricks: %s\033[0m\n\n' "$1" >&2; exit 1; }

require_cli() {
    command -v databricks >/dev/null 2>&1 || die \
"the databricks CLI is not on PATH.

  macOS/Linux:  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
  or:           brew tap databricks/tap && brew install databricks

This lane deploys from OUTSIDE the workspace on purpose: Free Edition restricts outbound
traffic, so a bundle deploy started from a notebook in the workspace is unreliable."
}

require_auth() {
    # The CLI resolves credentials from several places, and the environment is only one of
    # them: a profile in ~/.databrickscfg is the way the CLI itself tells you to store them
    # (`databricks configure`). Demanding the two variables made this script STRICTER than the
    # tool it drives, and it aborted on machines that were correctly configured.
    #
    # So the question asked is the one that matters - can the CLI authenticate - and the
    # variables are only checked when the answer is no, because then their absence is almost
    # always the reason and the message should say so.
    if databricks current-user me >/dev/null 2>&1; then
        return 0
    fi
    [ -n "${DATABRICKS_HOST:-}" ] || die \
"the CLI cannot authenticate, and DATABRICKS_HOST is not set.

  export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
  export DATABRICKS_TOKEN=dapi...

That is the workspace URL, with the scheme and no trailing path. Free Edition has no account
console, so there is no account-level host to use instead. A configured profile works too:
  databricks configure --host https://<your-workspace>.cloud.databricks.com --token"
    [ -n "${DATABRICKS_TOKEN:-}" ] || die \
"the CLI cannot authenticate, and DATABRICKS_TOKEN is not set.

  export DATABRICKS_TOKEN=dapi...

A personal access token, from Settings -> Developer -> Access tokens in the workspace. It is a
PAT because Free Edition has no account console and therefore no OAuth machine-to-machine
service principals - docs/limits.md says so, and this is the line where you feel it."
    case "$DATABRICKS_HOST" in
        https://*) ;;
        *) die "DATABRICKS_HOST must start with https://, got '$DATABRICKS_HOST'" ;;
    esac
    die \
"the CLI could not authenticate against $DATABRICKS_HOST.

  databricks current-user me

failed with both variables set. The usual causes are an expired PAT, a token from a different
workspace, or a host with a trailing slash or path on it."
}

python_bin() {
    for candidate in python3 python; do
        command -v "$candidate" >/dev/null 2>&1 && { echo "$candidate"; return 0; }
    done
    return 1
}

step_catalog() {
    say "catalog $CATALOG"
    # Interpolated into SQL below, in an identifier position that cannot be parameterised, so
    # it is checked against the shape of an identifier first. Same rule as close_month.py.
    printf '%s' "$CATALOG" | grep -Eq '^[A-Za-z_][A-Za-z0-9_]*$' || die \
"catalog must match ^[A-Za-z_][A-Za-z0-9_]*\$, got '$CATALOG'"

    if catalog_exists; then
        echo "  exists"
        return 0
    fi

    # NOT `databricks catalogs create`. That goes to the Unity Catalog API, which wants a
    # storage root on the metastore; Free Edition uses Default Storage and has none, so it
    # fails with `Metastore storage root URL does not exist` (databricks/cli#4513). The same
    # `CREATE CATALOG` in SQL works, because the SQL path resolves the location through
    # Default Storage. A catalog is also not a bundle resource type at all, so this is the one
    # piece of the lane created imperatively, and saying so beats a deploy that dies on a
    # parent nobody declared.
    local py warehouse warehouse_state submitted statement_id state error deadline started
    py="$(python_bin)" || die "no python on PATH, which this step needs to read the CLI's JSON"

    read -r warehouse warehouse_state <<EOF
$(databricks warehouses list -o json 2>/dev/null | "$py" -c "$WAREHOUSE_FIELDS")
EOF

    [ -n "$warehouse" ] || die \
"catalog '$CATALOG' does not exist and there is no SQL warehouse to create it with.

Free Edition includes one 2X-Small warehouse; if it has been deleted, recreate it from
SQL Warehouses in the workspace, or just run this once in the SQL Editor:

  CREATE CATALOG IF NOT EXISTS $CATALOG;

The Unity Catalog API cannot do it here: it wants a metastore storage root, and Free Edition
uses Default Storage and has none (databricks/cli#4513)."

    # A Free Edition warehouse stops itself after a few minutes idle, so a COLD START is the
    # normal case for this script rather than the exception, and it costs 40s to 2 minutes.
    # Saying so on screen is not decoration: a script that looks hung for ninety seconds is a
    # script people kill halfway through, and killing it halfway is exactly how you end up with
    # a catalog that exists and a tool that reported failure.
    if [ "$warehouse_state" != "RUNNING" ]; then
        echo "  warehouse $warehouse is $warehouse_state; starting it"
        echo "  a serverless 2X-Small cold start takes 40s-2min. This is a wait, not a hang."
        databricks warehouses start "$warehouse" --no-wait >/dev/null 2>&1 || true
    fi

    echo "  creating via SQL on warehouse $warehouse"
    # `on_wait_timeout: CONTINUE`, not CANCEL, and this is the whole lesson of the round.
    # The API accepts "0s" or "5s" to "50s" for wait_timeout, and a cold start can take longer
    # than 50s, so NO value of that parameter covers the normal case: a timeout tuned against
    # execution time cannot cover a wait dominated by start-up time. With CANCEL, the client's
    # wait expiring killed the wait AND reported CANCELED, and this script then announced the
    # catalog was not there - while the DDL the warehouse had already admitted went on to
    # create it. With CONTINUE the statement stays alive and the response carries a
    # statement_id to poll.
    submitted="$(databricks api post /api/2.0/sql/statements --json "$(cat <<JSON
{
  "warehouse_id": "$warehouse",
  "statement": "CREATE CATALOG IF NOT EXISTS $CATALOG COMMENT 'samegold: created by scripts/databricks_run.sh, not by the bundle'",
  "wait_timeout": "50s",
  "on_wait_timeout": "CONTINUE"
}
JSON
)")" || die "the CREATE CATALOG statement could not be submitted; the CLI output is above"

    read -r state statement_id error <<EOF
$(printf '%s' "$submitted" | "$py" -c "$STATEMENT_FIELDS")
EOF

    # The ceiling is on the WHOLE wait rather than on one request, because what is being waited
    # for is a machine booting and not a query running. Five minutes is a cold start with room.
    started=$(date +%s)
    deadline=$(( started + ${SAMEGOLD_SQL_TIMEOUT_SECONDS:-300} ))
    while [ "$state" = "PENDING" ] || [ "$state" = "RUNNING" ]; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            state="TIMED_OUT_WAITING"
            break
        fi
        # The GET is documented as taking up to 5 seconds to reflect the latest status, so
        # polling faster than that buys nothing and spends request quota.
        sleep "${SAMEGOLD_SQL_POLL_SECONDS:-5}"
        echo "  still $state after $(( $(date +%s) - started ))s (statement $statement_id)"
        read -r state statement_id error <<EOF
$(databricks api get "/api/2.0/sql/statements/$statement_id" 2>/dev/null | "$py" -c "$STATEMENT_FIELDS")
EOF
    done

    if [ "$state" = "SUCCEEDED" ]; then
        echo "  created"
        return 0
    fi

    # EVERY path out of a non-SUCCEEDED state goes through here, and the first thing it does is
    # LOOK. CANCELED does not mean "it did not happen", it means "I stopped waiting": the
    # cancel ends the client's wait, not a DDL the warehouse has already admitted. CLOSED is
    # documented as a successful execution whose results are no longer fetchable. And a ceiling
    # that expires says nothing whatsoever about the world. The version before this one
    # asserted "Either way the catalog is not there" and was proved wrong on the first real
    # workspace it met - inside its own error message.
    echo "  statement ended in state $state; asking the catalog whether it exists"
    if catalog_exists; then
        echo "  created - the statement reported $state and the catalog is there."
        echo "  The wait was cut short; the DDL was not."
        return 0
    fi

    # Only now, having looked, may this say what is and is not there. And it reports the state
    # it actually saw rather than reciting a list of the states it expected: the list that came
    # before this one enumerated PENDING, RUNNING and FAILED, and the state it printed on the
    # day it mattered was CANCELED, which was not in it.
    die \
"CREATE CATALOG did not succeed, and the catalog is still not there - checked just now, not
assumed.

  last observed state: $state
  statement id:        ${statement_id:-none}
  workspace error:     ${error:-none reported}

$(explain_statement_state "$state")

You can do it by hand in the SQL Editor, which is one line:

  CREATE CATALOG IF NOT EXISTS $CATALOG;"
}

# Generated from the state that was observed, with a fallback that stays correct for a state
# this script has never seen. A hand-maintained taxonomy that does not cover what it prints is
# round 14's by-ordinal exclusion list wearing different clothes.
explain_statement_state() {
    case "$1" in
        TIMED_OUT_WAITING)
            echo "The warehouse did not finish within ${SAMEGOLD_SQL_TIMEOUT_SECONDS:-300}s. It may still be starting, and that statement may yet complete on its own - which is why this step re-checks rather than concluding." ;;
        FAILED)
            echo "The workspace refused the statement; the error above is its reason." ;;
        CANCELED)
            echo "Something cancelled it. This script no longer cancels on timeout, so the cancel came from somewhere else." ;;
        CLOSED)
            echo "CLOSED is documented as a successful execution whose results are no longer fetchable, so seeing it here with no catalog is surprising and worth reporting upstream." ;;
        PENDING | RUNNING)
            echo "It was still going when the wait ended, which should be unreachable: the loop leaves those states only at the ceiling, and that is reported as TIMED_OUT_WAITING." ;;
        *)
            echo "This script has no note about '$1'. It is a state the API returned and nothing here anticipated, which is worth reading in the workspace rather than guessing at." ;;
    esac
}

# One question, one place, so that every caller asks it the same way and no caller is tempted
# to infer the answer instead.
catalog_exists() {
    databricks catalogs get "$CATALOG" >/dev/null 2>&1
}

# Reads a statement-execution response and prints `state statement_id error` on one line.
# Shared by the submit and by the poll, so the two readings cannot drift apart.
STATEMENT_FIELDS='
import json, sys
try:
    body = json.load(sys.stdin)
except Exception:
    body = {}
status = body.get("status") or {}
error = (status.get("error") or {}).get("message", "")
print(
    status.get("state", "UNREADABLE"),
    body.get("statement_id", ""),
    " ".join(str(error).split()),
)
'

# Prints `id state` for the warehouse to use: a RUNNING one if there is one, because that
# skips the cold start, and otherwise whichever exists. Free Edition gives exactly one.
WAREHOUSE_FIELDS='
import json, sys
try:
    warehouses = json.load(sys.stdin)
except Exception:
    warehouses = []
running = [w for w in warehouses if str(w.get("state", "")).upper() == "RUNNING"]
chosen = running or warehouses
if chosen:
    print(chosen[0].get("id", ""), str(chosen[0].get("state", "UNKNOWN")).upper())
else:
    print("", "NONE")
'

step_validate() { say "bundle validate -t $TARGET"; (cd "$BUNDLE" && databricks bundle validate -t "$TARGET"); }
# The commit, carried INTO the deploy so that what the run publishes can name the code that
# produced it. Until now the only commit anywhere near this lane was the one `step_fetch` writes
# into fetch.json afterwards - this machine's HEAD when somebody copied the files down, which is
# a different fact, and one a later fetch can re-stamp onto files it did not produce.
#
# A tree with uncommitted code is deployed as that commit PLUS whatever is not in it, so the
# commit alone would be a claim the deploy does not honour. `code_changes` is the same filter
# `step_fetch` uses, and it excludes evidence/ for the same reason.
deploy_commit() { git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown; }

step_deploy() {
    say "bundle deploy -t $TARGET"
    local commit dirty
    commit="$(deploy_commit)"
    dirty=$(test -n "$(code_changes)" && echo true || echo false)
    echo "  deploying $commit (tree_dirty=$dirty)"
    (cd "$BUNDLE" && databricks bundle deploy -t "$TARGET" \
        --var="catalog=$CATALOG" \
        --var="deploy_commit=$commit" \
        --var="deploy_tree_dirty=$dirty")
}

step_seed() {
    say "seed $LANDING"
    if [ -z "${SAMEGOLD_RESEED:-}" ] && [ -n "$(databricks fs ls "$LANDING" 2>/dev/null || true)" ]; then
        echo "  the landing volume already has files; not seeding again."
        echo "  SAMEGOLD_RESEED=1 make databricks forces another batch."
        return
    fi
    # Three candidates, because there are three shapes this repository is checked out in: a
    # POSIX venv, a Windows venv (executables in Scripts/, with a .exe suffix), and a CI
    # runner where pip put the entry point straight on PATH. Testing only `$BIN/samegold`
    # made the seed step fail on the second of those with the venv sitting right there.
    local samegold=""
    for candidate in "$BIN/samegold" "$BIN/samegold.exe"; do
        if [ -x "$candidate" ]; then samegold="$candidate"; break; fi
    done
    if [ -z "$samegold" ] && command -v samegold >/dev/null 2>&1; then
        samegold="$(command -v samegold)"
    fi
    [ -n "$samegold" ] || die \
"no samegold executable: not at $BIN/samegold, and not on PATH.

  make install

builds it. On Windows the executables land in .venv/Scripts; SAMEGOLD_BIN=... points this
script at whichever directory holds them."
    local work
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' RETURN
    # The SAME generator the OSS lane runs, at the same profile, so the two lanes are being
    # asked about the same population - which is the only reason comparing their numbers is
    # worth anything. The seed is fixed here and printed, so the run is repeatable.
    "$samegold" generate --out "$work" --profile "${SAMEGOLD_PROFILE:-fast}" \
        --seed "${SAMEGOLD_SEED:-20260901}"
    databricks fs mkdir "$LANDING" 2>/dev/null || true
    databricks fs cp -r --overwrite "$work/bronze" "$LANDING"
    echo "  uploaded $(find "$work/bronze" -name '*.json' | wc -l | tr -d ' ') files"
    # The ledger goes up too, unread by the pipeline: it is what a later comparison of this
    # lane's close against the by-construction truth would need, and leaving it behind on a
    # laptop is how that comparison becomes impossible to make later.
    databricks fs cp --overwrite "$work/truth/ledger.json" "$EVIDENCE_VOLUME/ledger.json"
}

# The name the bundle deploys this job under. Asserted against resources/jobs.yml by
# tests/fast/test_databricks_bundle.py, because a literal repeated in two files is a literal
# that drifts, and the way this one would drift is silent: `jobs list --name` filters on an
# exact name, so a renamed job simply stops being found and the guard below would say "nothing
# is deployed" about a job sitting right there.
JOB_NAME="samegold monthly close"

# Reads the deployed job and prints what it was DEPLOYED FROM. One call.
#
# `step_deploy` passes `deploy_commit` into the bundle and resources/jobs.yml puts it in the
# `publish_evidence` task's `base_parameters`, so the deployed job carries the sha of the tree
# that deployed it in a field the Jobs API returns. That is the only thing in this lane that
# can answer "which code is up there", and until now nothing asked it.
#
# It prints one word, and the words are different FACTS rather than degrees of one:
#
#   <40 hex>    the job says it was deployed from that commit
#   unknown     it is deployed and does not say - `bundle deploy` run by hand without the vars
#   NONE        no job of that name exists: nothing has been deployed
#   NOPARAM     a job exists and carries no `deploy_commit` at all, which is what a deployment
#               from before that parameter existed looks like
#   AMBIGUOUS   two tasks disagree, which no deploy this bundle performs can produce
#   UNREADABLE  the CLI answered with something this cannot parse
#   NOPYTHON    there is no interpreter here to read the answer with
#
# Collapsing any two of those into one value is `MAX` over a state string again: the caller
# would have to guess which it had, and the guesses point in opposite directions.
DEPLOYED_COMMIT_FIELDS='
import json, sys
try:
    body = json.load(sys.stdin)
except Exception:
    print("UNREADABLE"); raise SystemExit(0)
# The Jobs list API wraps its rows in {"jobs": [...]}; the CLI prints a bare array. Which of
# the two arrives here cannot be established from outside a workspace, so both are read rather
# than one being asserted - and an answer in neither shape says UNREADABLE rather than NONE,
# because "the CLI said something else" and "nothing is deployed" are not the same fact and
# reporting the second for the first is how this repository has been wrong before.
if isinstance(body, dict):
    jobs = body.get("jobs")
elif isinstance(body, list):
    jobs = body
else:
    jobs = None
if jobs is None:
    print("UNREADABLE"); raise SystemExit(0)
if not jobs:
    print("NONE"); raise SystemExit(0)
found = set()
for job in jobs:
    settings = job.get("settings") if isinstance(job.get("settings"), dict) else job
    for task in (settings.get("tasks") or []):
        parameters = (task.get("notebook_task") or {}).get("base_parameters") or {}
        if "deploy_commit" in parameters:
            found.add(str(parameters["deploy_commit"]))
if not found:
    print("NOPARAM")
elif len(found) > 1:
    print("AMBIGUOUS " + " ".join(sorted(found)))
else:
    print(found.pop())
'

deployed_commit() {
    local py answer
    py="$(python_bin)" || { echo NOPYTHON; return 0; }
    # The CLI call and the parse are two statements rather than one pipeline, and that is
    # not a style choice: this script runs under `set -o pipefail`, so a `databricks |
    # python` pipeline whose first half fails makes the whole substitution fail, and `set
    # -e` then kills the script at the assignment - with no message, at the one point whose
    # entire purpose is to produce one. `|| true` keeps the failure INSIDE this function,
    # where an empty answer parses as UNREADABLE and the caller says so and names the
    # command to run by hand.
    answer="$(databricks jobs list --name "$JOB_NAME" --expand-tasks -o json 2>/dev/null || true)"
    printf '%s' "$answer" | "$py" -c "$DEPLOYED_COMMIT_FIELDS"
}

# The check that closes "the workspace ran what was deployed, and nobody had deployed".
#
# That one cost a morning. `databricks bundle run` runs the DEPLOYED code; no deploy had
# happened since the commits that added the `deploy` block and the row-level capture; and the
# run ended SUCCESS with a green pipeline and a correct close. One fault, two symptoms - a
# missing key and a missing file - and no error anywhere. Nothing compared the deployed code
# with the tree, so a run was green about code the repository could not see.
#
# What this does NOT cover, written here rather than discovered later: a DIRTY tree deploys as
# HEAD plus whatever is not committed, so the sha matches while the code does not. That is the
# ordinary development loop and refusing it would break what this script is for; `tree_dirty`
# in the record is the field that carries that other claim.
#
# It REFUSES rather than warning. The command it guards is the one command in this repository
# that can spend a Free Edition account's compute for the rest of the day, and a banner printed
# above a run that then happens anyway is the exact defect the refusal below it exists for.
require_fresh_deployment() {
    local head deployed advice
    head="$(deploy_commit)"
    deployed="$(deployed_commit)"

    if [ -n "${SAMEGOLD_RUN_STALE:-}" ]; then
        say "SAMEGOLD_RUN_STALE: running what is DEPLOYED without requiring it to be HEAD"
        echo "  deployed: $deployed"
        echo "  HEAD:     $head"
        echo "  What this run publishes describes the deployed commit, not this tree."
        return 0
    fi

    advice="
  scripts/databricks_run.sh deploy

deploys this tree. To run what is up there ON PURPOSE - to reproduce what an older deployment
published, say - name the decision:

  SAMEGOLD_RUN_STALE=1 scripts/databricks_run.sh $SUBCOMMAND"

    case "$deployed" in
        NONE)
            die "no job named \"$JOB_NAME\" exists in this workspace, so there is nothing
deployed for this to run. The bundle run would fail too, later and less clearly.$advice" ;;
        NOPARAM)
            die "the deployed job carries no \`deploy_commit\` parameter at all, which is what
a deployment made before that parameter existed looks like. It cannot name its own code, so
this cannot tell you whether it is stale.$advice" ;;
        unknown)
            die "the deployed job says it was deployed from \"unknown\", which is what
\`databricks bundle deploy\` run by hand without --var=\"deploy_commit=...\" leaves behind. The
deployment cannot name its own code, so this cannot compare it with yours.$advice" ;;
        NOPYTHON)
            die "no python on PATH, so the deployed job's commit cannot be read and this run
cannot be checked against HEAD. Refusing rather than spending a Free Edition run on a
deployment nobody has identified.$advice" ;;
        UNREADABLE | AMBIGUOUS*)
            die "could not read which commit the deployed job was deployed from ($deployed).

  databricks jobs list --name \"$JOB_NAME\" --expand-tasks -o json

is the call this makes; run it and look at what comes back.$advice" ;;
    esac

    if [ "$head" = unknown ]; then
        die "the deployed job was deployed from $deployed and this working copy cannot say what
HEAD is - \`git rev-parse HEAD\` failed - so the two cannot be compared.$advice"
    fi

    if [ "$deployed" != "$head" ]; then
        die "the deployed job was deployed from

  $deployed

and HEAD here is

  $head

\`databricks bundle run\` runs what was DEPLOYED. Running now would execute that older code and
publish a record naming it, which is exactly what happened on 4 September 2026: a green run, a
correct close, and a notebook from two commits earlier.$advice"
    fi

    echo "  the deployed job was deployed from HEAD ($head)"
}

step_run() {
    # Before anything that can cost compute: is what is about to run the code in this tree?
    require_fresh_deployment
    # `cloudFiles.schemaLocation` CACHES the schema Auto Loader inferred the first time. The
    # lane's first deployment inferred every column as STRING, and adding `schemaHints` to
    # bronze_autoloader.py changes NOTHING for an existing checkpoint until the schema is
    # inferred again. Without this the type fix looks like it did not work - or, as actually
    # happened, the update dies with DELTA_MERGE_INCOMPATIBLE_DATATYPE (StringType and
    # LongType on `new_qty`), which is precisely the conflict the refresh exists to avoid.
    #
    # The command is built as an ARRAY and then INSPECTED, because the first version of this
    # printed a full-refresh banner and ran without the flag. `SAMEGOLD_FULL_REFRESH=1
    # require_cli; ...; step_run` sets the variable for the duration of `require_cli` ONLY -
    # a bash assignment prefixed to a command applies to that command - so `step_run` never
    # saw it. The banner appeared because the banner was printed somewhere else. A message
    # that announces what is about to happen and does not govern what happens is an assertion
    # nobody verifies, which is this repository's oldest recurring defect.
    local -a command=(databricks bundle run samegold_close -t "$TARGET" --var="catalog=$CATALOG")
    if [ -n "$ONLY_TASKS" ]; then
        # `--only <keys>` runs those tasks and skips the rest of the graph. The evidence task
        # reads no task value from its upstreams - it only SETS one - so running it alone is
        # safe, and it is what re-publishes a record without spending another pipeline update.
        command+=(--only "$ONLY_TASKS")
        say "ONLY: $ONLY_TASKS"
        echo "  the rest of the job's tasks are skipped, so what they produce is not refreshed"
    fi
    if [ -n "${SAMEGOLD_FULL_REFRESH:-}" ]; then
        # TWO flags for one intent, and the reason is that neither can be checked from here.
        #
        # `databricks bundle run --help` (CLI v1.14.1) groups its flags by the kind of
        # resource they apply to. `--full-refresh-all` ("Perform a full graph reset and
        # recompute") is under **Pipeline Flags**. The KEY this command passes is
        # `samegold_close`, which is a JOB - and the flag the same help attaches to jobs is
        # `--pipeline-params`, "A map from keys to values for jobs with pipeline tasks", whose
        # `full_refresh` field is what the Jobs API carries on a pipeline task.
        #
        # Which of the two the CLI acts on for a job key cannot be determined without a
        # workspace, and that was MEASURED rather than assumed: an unknown flag is rejected at
        # parse time (`Error: unknown flag: --not-a-real-flag`), but both of these parse and
        # the CLI then goes straight to authentication - so the earliest point at which their
        # semantics could be observed is after credentials, on the one command that spends the
        # thing being protected.
        #
        # So both are sent. Dropping `--full-refresh-all` would assert it is inert; keeping
        # only it would assert it works. Sending both asserts neither, costs nothing if one is
        # ignored, and fails at parse time - before any compute - if the combination is ever
        # rejected. The check that closes this is the first item of the post-run checklist in
        # docs/databricks-run.md: if `qty` comes back STRING, no refresh happened.
        command+=(--full-refresh-all --pipeline-params "full_refresh=true")
        say "FULL REFRESH: the pipeline will re-read the landing zone from scratch"
        echo "  needed after a schemaHints change, because the inferred schema is cached"
    fi

    # Same rule as the refresh flag below, for the same reason: a selection that was announced
    # and not passed would run the WHOLE job, which on Free Edition is a pipeline update
    # nobody asked for.
    if [ -n "$ONLY_TASKS" ] && ! printf '%s\n' "${command[@]}" | grep -qx -- "$ONLY_TASKS"; then
        die \
"a task selection was given and --only is not in the command about to run:

  ${command[*]}

Refusing to run the whole job when part of it was asked for."
    fi

    # The announcement and the invocation are now the same object, and this is the check that
    # they cannot drift apart again: if a full refresh was asked for, the flag has to be in
    # the argv about to be executed. Not in the source, not in a variable - in the argv.
    if [ -n "${SAMEGOLD_FULL_REFRESH:-}" ] &&
       { ! printf '%s\n' "${command[@]}" | grep -qx -- "--full-refresh-all" ||
         ! printf '%s\n' "${command[@]}" | grep -qx -- "full_refresh=true"; }; then
        die \
"SAMEGOLD_FULL_REFRESH is set and --full-refresh-all is not in the command about to run:

  ${command[*]}

Refusing to spend a Free Edition run pretending to refresh. This is the exact failure that
cost one: the banner printed, the flag did not arrive, and the update died on the schema
conflict the refresh was supposed to clear."
    fi

    say "${command[*]}"
    # Free Edition stops ALL compute for the rest of the day when the quota runs out, so this
    # is the one command in the repository that can cost you the afternoon. It is never
    # triggered by a push: the schedule in resources/jobs.yml is deployed PAUSED.
    (cd "$BUNDLE" && "${command[@]}")
}

# The working tree's changes, EXCLUDING this repository's own evidence output.
#
# The same rule as `samegold.generator.seeds._code_changes`, and here for the same reason: the
# `tree_dirty` field written below was computed with a bare `git status --porcelain`, and this
# function runs immediately after copying `SG-DBX-01.json` into `evidence/databricks/`. It was
# therefore structurally TRUE on every fetch that ever succeeded - the record said the deploy
# came from a dirty tree because the fetch had just written the record.
#
# Round 19 found and fixed exactly this in Python (`evidence/` is output, not input) and did
# not look one file further. `substr($0, 4)` is the porcelain layout - two status characters
# and a space - and unlike the Python side there is no upstream `.strip()` to eat the first
# line's leading space.
code_changes() {
    git -C "$REPO" status --porcelain 2>/dev/null \
        | awk '{ p = substr($0, 4); if (p !~ /^"?evidence\//) print p }'
}

# What the record says about the tree it was deployed from, read back out of the file that
# just landed.
#
# THE FINDING THIS CLOSES, and it is this round's own: `require_fresh_deployment` was written,
# committed, and the FINDINGS.md entry describing it was not. The next deploy therefore went
# out from a tree with that document uncommitted, and published `tree_dirty: true`. Nobody
# noticed by reading; the provenance field noticed. The tree was committed, the lane
# redeployed, and the record that is in this repository came from a clean one.
#
# Deploying a dirty tree WHILE DEVELOPING is legitimate and this does not interfere with it.
# Committing the evidence it produces is not: `deploy.commit` then names a commit that does not
# contain the code that ran, so the record cannot be tied to anything, which is the one job
# that field has.
#
# IT REPORTS, AND IT DOES NOT DIE. Three reasons, because "refuse rather than warn" is this
# script's rule elsewhere and departing from it needs one:
#
#   * by the time this can read the field the files are already on disk. Dying prevents
#     nothing that has happened - it only skips the rest of this step's own output;
#   * the thing to prevent is the COMMIT, which happens later and possibly elsewhere. A
#     message here cannot govern it, and this repository's own rule is that a message which
#     announces something it does not govern is worthless;
#   * so the refusal lives where the commit is: `test_databricks_bundle.py` fails on committed
#     evidence whose deploy was not a commit. That runs in `make fast`, in `make preflight` and
#     in CI - at the moment the record would actually enter the repository, not before it.
#
# This is the part that tells you now, so you find out here rather than from a red gate.
report_uncommittable_provenance() {
    local py verdict record="${1:-$OUT/SG-DBX-01.json}"
    py="$(python_bin)" || return 0
    verdict="$("$py" - "$record" <<'PY'
import json, sys

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        deploy = (json.load(handle) or {}).get("deploy") or {}
except Exception as error:
    print(f"UNREADABLE {type(error).__name__}")
    raise SystemExit(0)
dirty, commit = deploy.get("tree_dirty"), deploy.get("commit", "unknown")
# The string forms are what a notebook deployed before the boolean conversion published, and
# "true" is truthy either way - which is the whole reason that conversion happened.
if dirty is True or (isinstance(dirty, str) and dirty.lower() == "true"):
    print(f"DIRTY {commit}")
elif dirty is False or (isinstance(dirty, str) and dirty.lower() == "false"):
    print(f"CLEAN {commit}")
else:
    print(f"UNKNOWN {commit}")
PY
)"
    case "$verdict" in
        CLEAN*)
            echo "  deploy provenance: clean tree at ${verdict#CLEAN }" ;;
        DIRTY*)
            echo
            echo "  DO NOT COMMIT THIS EVIDENCE."
            echo
            echo "  The record says it was produced by a deploy from ${verdict#DIRTY }"
            echo "  with UNCOMMITTED CHANGES outside evidence/."
            echo
            echo "  So the commit it names does not contain the code that ran, and nobody"
            echo "  with a clone can tie the two together - which is the only thing"
            echo "  \`deploy.commit\` is for."
            echo
            echo "  Deploying a dirty tree while you work is fine. Committing what it produced"
            echo "  is not. Commit the code, deploy again, and re-run the evidence task:"
            echo
            echo "    scripts/databricks_run.sh deploy"
            echo "    scripts/databricks_run.sh run publish_evidence"
            echo "    scripts/databricks_run.sh fetch"
            echo
            echo "  tests/fast/test_databricks_bundle.py refuses it if you commit it anyway."
            echo ;;
        UNKNOWN*)
            echo
            echo "  The record does not say whether the tree it was deployed from was clean"
            echo "  (deploy.tree_dirty is neither true nor false). A record that cannot say"
            echo "  cannot be tied to a commit either; treat it as uncommittable."
            echo ;;
        *)
            echo "  (could not read the deploy provenance out of the record: $verdict)" ;;
    esac
}

step_fetch() {
    say "fetch the evidence"
    mkdir -p "$OUT"
    # The three names this fetch writes. Unlabelled they are the canonical ones; labelled, they
    # are a run kept beside them under a name that says what it is, and nothing this repository
    # renders or compares reads them.
    local record="SG-DBX-01.json" capture="dim_customer_scd2.json" fetched="fetch.json"
    if [ -n "$FETCH_LABEL" ]; then
        case "$FETCH_LABEL" in
            *[!a-z0-9-]* | -* | "")
                die \
"\`fetch\` takes a label made of lower-case letters, digits and hyphens, and got '$FETCH_LABEL'.

The label becomes part of a filename: scripts/databricks_run.sh fetch run-2-failed" ;;
        esac
        record="SG-DBX-01.$FETCH_LABEL.json"
        capture="dim_customer_scd2.$FETCH_LABEL.json"
        fetched="fetch.$FETCH_LABEL.json"
        echo "  LABELLED: writing $record, and leaving the canonical record alone."
    fi
    # A failed copy is not the same fact as a missing file, and the previous version of this
    # message asserted the second from the first: "the run produced no SG-DBX-01.json ... its
    # absence means the task did not reach the end". A `cp` can fail because the token expired,
    # because the volume is not readable, or because the network dropped. Same defect as the
    # catalog step's CANCELED, one function away: turning "the call failed" into a claim about
    # the world without looking. So it LOOKS, and reports what it found either way.
    if ! databricks fs cp --overwrite "$EVIDENCE_VOLUME/SG-DBX-01.json" "$OUT/$record"; then
        if databricks fs ls "$EVIDENCE_VOLUME/SG-DBX-01.json" >/dev/null 2>&1; then
            die \
"the record EXISTS at $EVIDENCE_VOLUME/SG-DBX-01.json and could not be copied down.

So the run reached the end and produced it; what failed is this machine's ability to fetch it.
Look at credentials and network before you look at the job. You can also read it in the
workspace, or retry with: scripts/databricks_run.sh fetch"
        fi
        die \
"no record at $EVIDENCE_VOLUME/SG-DBX-01.json - checked just now, not inferred from the copy
failing.

publish_evidence.py writes that file as its last step, so this is consistent with the task not
reaching the end. The run's output is in the workspace under Jobs -> samegold monthly close."
    fi
    # Written HERE, by this machine, about this deploy - and kept in a separate file from the
    # record the workspace produced, so that nothing this laptop asserts can be mistaken for
    # something the workspace measured.
    cat > "$OUT/$fetched" <<JSON
{
  "fetched_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "bundle_target": "$TARGET",
  "catalog": "$CATALOG",
  "deployed_from_commit": "$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)",
  "tree_dirty": $(test -n "$(code_changes)" && echo true || echo false),
  "databricks_cli": "$(databricks version 2>/dev/null | head -1)",
  "note": "Written by scripts/databricks_run.sh on the machine that ran the deploy. Nothing here was measured inside the workspace; SG-DBX-01.json is. Neither file is part of evidence/history.jsonl - see evidence/databricks/README.md."
}
JSON
    # The row-level dimension capture, written by the same task in the same run as the record
    # above. It used to be exported by hand, which produced a file that could not say which run
    # it came from - so a later run replacing the record left it comparing green against rows
    # the workspace no longer held. Not fatal when absent: a run from before publish_evidence.py
    # wrote it has none, and the record is still the thing this step exists to bring down. Loud,
    # though, because the comparison that reads it is then reading an older workspace than the
    # record beside it - and `tests/fast/test_databricks_dimension_parity.py` says so by
    # comparing the two update ids.
    if databricks fs cp --overwrite \
        "$EVIDENCE_VOLUME/dim_customer_scd2.json" "$OUT/$capture"; then
        echo "  evidence/databricks/$capture"
    else
        echo
        echo "  WARNING: no dim_customer_scd2.json at $EVIDENCE_VOLUME."
        echo "  The record came down; the row-level capture did not. If a capture is already in"
        echo "  the repository it now describes an OLDER run than SG-DBX-01.json beside it, and"
        echo "  tests/fast/test_databricks_dimension_parity.py will fail on the two update ids"
        echo "  rather than compare against rows nothing produced. Re-run the job with a"
        echo "  publish_evidence.py that writes it."
        echo
    fi
    echo "  evidence/databricks/$record"
    echo "  evidence/databricks/$fetched"
    report_uncommittable_provenance "$OUT/$record"
    # The two per-rule tables, rendered ready to paste into the anchored blocks in
    # docs/databricks-run.md. The scalars there are filled in by hand from the record; these
    # two are tables, and a table typed out by hand is a table with a transcription error in
    # it. tests/fast/test_databricks_bundle.py checks the paste against the record afterwards.
    local py=""
    command -v python3 >/dev/null 2>&1 && py=python3
    [ -z "$py" ] && command -v python >/dev/null 2>&1 && py=python
    [ -z "$py" ] && { echo "  (no python on PATH: read the JSON yourself)"; return; }
    "$py" - "$OUT/$record" <<'PY'
import json, sys

record = json.load(open(sys.argv[1], encoding="utf-8"))
update = record.get("update")
state = update[0] if isinstance(update, list) and update else update
print(f"\n  claim_id {record.get('claim_id')}   update {state}")

missing = record.get("incomplete") or []
if missing:
    print(f"\n  SECTIONS THAT COULD NOT BE READ: {missing}")
    print("  Those are holes in the record, not zeros. Put the error in the anchor, not a 0.")


def table(rows, columns, headings):
    print("\n" + "| " + " | ".join(headings) + " |")
    print("|" + "|".join("---" for _ in headings) + "|")
    for row in rows:
        print("| " + " | ".join(str(row.get(c, "")) for c in columns) + " |")


expectations = record.get("expectations")
if isinstance(expectations, list) and expectations:
    print("\n--- paste into the dbx:expectations.table anchor ---")
    table(expectations, ["rule", "dataset", "passed", "failed"],
          ["rule", "dataset", "passed", "failed"])

quarantine = record.get("quarantine_by_reason")
if isinstance(quarantine, list) and quarantine:
    print("\n--- paste into the dbx:quarantine.table anchor ---")
    table(quarantine, ["reason", "n"], ["quarantine reason", "rows"])

rows = record.get("rows")
if isinstance(rows, dict):
    print("\n--- the scalar anchors ---")
    for name, value in sorted(rows.items()):
        print(f"  dbx:rows.{name} = {value}")
dimension = record.get("dim_customer_scd2")
if isinstance(dimension, list) and dimension:
    for field in ("versions", "customers", "open_rows", "closed_rows"):
        print(f"  dbx:dim.{field} = {dimension[0].get(field)}")
if isinstance(state, dict):
    print(f"  dbx:update.last_state = {state.get('last_state')}")
    print(f"  dbx:update.error_events = {state.get('error_events')}")
PY
}

usage() {
    echo "usage: scripts/databricks_run.sh [all|catalog|validate|deploy|seed|run|run-full-refresh|fetch]" >&2
    echo "       run and run-full-refresh take an optional comma-separated list of task keys:" >&2
    echo "         scripts/databricks_run.sh run publish_evidence" >&2
    echo "       fetch takes an optional label, which keeps the run beside the canonical" >&2
    echo "       record instead of over it:" >&2
    echo "         scripts/databricks_run.sh fetch run-2-failed" >&2
    exit 2
}

# An argument nobody reads is an argument that looks obeyed. Only the two run subcommands take
# a second one, and the rest reject it rather than ignoring it - `deploy publish_evidence`
# should not deploy everything and say nothing about the word it was handed.
case "$SUBCOMMAND" in
    run | run-full-refresh | fetch) ;;
    *)
        [ -z "$ONLY_TASKS" ] || die \
"\`$SUBCOMMAND\` takes no task selection, and one was given: '$ONLY_TASKS'.

Only \`run\` and \`run-full-refresh\` select tasks."
        ;;
esac

case "${1:-all}" in
    all)      require_cli; require_auth; step_catalog; step_validate; step_deploy; step_seed; step_run; step_fetch ;;
    catalog)  require_cli; require_auth; step_catalog ;;
    # validate is the one step that needs no workspace beyond authentication, which is why it
    # is also what .github/workflows/databricks.yml runs by default.
    validate) require_cli; require_auth; step_validate ;;
    deploy)   require_cli; require_auth; step_catalog; step_validate; step_deploy ;;
    seed)     require_cli; require_auth; step_seed ;;
    run)      require_cli; require_auth; step_run ;;
    # The same step with the schema cache thrown away. Spelled as its own word rather than a
    # flag on `all`, so that it is a decision someone made.
    # The assignment is its OWN statement. `SAMEGOLD_FULL_REFRESH=1 require_cli` sets the
    # variable for the duration of `require_cli` and nothing else, so `step_run` ran without
    # it and without the flag - one wasted Free Edition run, and the update failed on the very
    # schema conflict the refresh was meant to clear.
    run-full-refresh)
              SAMEGOLD_FULL_REFRESH=1
              export SAMEGOLD_FULL_REFRESH
              require_cli; require_auth; step_run ;;
    fetch)    require_cli; require_auth; step_fetch ;;
    *)        usage ;;
esac
