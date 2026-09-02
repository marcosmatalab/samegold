#!/usr/bin/env bash
#
# The Databricks lane, end to end, from two environment variables.
#
#   export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
#   export DATABRICKS_TOKEN=dapi...
#   make databricks
#
# It does six things, in this order, and any one of them can be run alone:
#
#   catalog   create the Unity Catalog catalog if it is missing (a bundle cannot: there is no
#             `catalogs` resource type, and a schema whose catalog does not exist fails at
#             DEPLOY time, which is the first thing that would have gone wrong here)
#   validate  databricks bundle validate -t free
#   deploy    databricks bundle deploy   -t free   (schemas, volumes, pipeline, job)
#   seed      generate events with the OSS generator and upload them to the landing volume,
#             because a pipeline over an empty directory reports nothing and "no expectation
#             failed" and "no row was read" would arrive as the same evidence
#   run       databricks bundle run samegold_close -t free
#   fetch     copy the SG-DBX-01 record out of the workspace into evidence/databricks/
#
# Every failure here should say what to do about it. A stack trace from a CLI that was never
# installed, or a 403 from a token that expired, is not a message: it is a puzzle.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE="$REPO/databricks"
TARGET="${SAMEGOLD_DBX_TARGET:-free}"
CATALOG="${SAMEGOLD_CATALOG:-samegold}"
BIN="${SAMEGOLD_BIN:-$REPO/.venv/bin}"
OUT="$REPO/evidence/databricks"
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
    [ -n "${DATABRICKS_HOST:-}" ] || die \
"DATABRICKS_HOST is not set.

  export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com

That is the workspace URL, with the scheme and no trailing path. Free Edition has no account
console, so there is no account-level host to use instead."
    [ -n "${DATABRICKS_TOKEN:-}" ] || die \
"DATABRICKS_TOKEN is not set.

  export DATABRICKS_TOKEN=dapi...

A personal access token, from Settings -> Developer -> Access tokens in the workspace. It is a
PAT because Free Edition has no account console and therefore no OAuth machine-to-machine
service principals - docs/limits.md says so, and this is the line where you feel it."
    case "$DATABRICKS_HOST" in
        https://*) ;;
        *) die "DATABRICKS_HOST must start with https://, got '$DATABRICKS_HOST'" ;;
    esac
    databricks current-user me >/dev/null 2>&1 || die \
"the CLI could not authenticate against $DATABRICKS_HOST.

  databricks current-user me

failed. The usual causes are an expired PAT, a token from a different workspace, or a host
with a trailing slash or path on it."
}

step_catalog() {
    say "catalog $CATALOG"
    if databricks catalogs get "$CATALOG" >/dev/null 2>&1; then
        echo "  exists"
    else
        # Not a bundle resource: Declarative Automation Bundles have no `catalogs` type, so
        # this is the one piece of the lane that is created imperatively, and saying that out
        # loud is better than a deploy that fails on a missing parent nobody declared.
        databricks catalogs create "$CATALOG" \
            --comment "samegold: created by scripts/databricks_run.sh, not by the bundle"
        echo "  created"
    fi
}

step_validate() { say "bundle validate -t $TARGET"; (cd "$BUNDLE" && databricks bundle validate -t "$TARGET"); }
step_deploy()   { say "bundle deploy -t $TARGET";   (cd "$BUNDLE" && databricks bundle deploy   -t "$TARGET" --var="catalog=$CATALOG"); }

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

step_run() {
    say "bundle run samegold_close -t $TARGET"
    # Free Edition stops ALL compute for the rest of the day when the quota runs out, so this
    # is the one command in the repository that can cost you the afternoon. It is never
    # triggered by a push: the schedule in resources/jobs.yml is deployed PAUSED.
    (cd "$BUNDLE" && databricks bundle run samegold_close -t "$TARGET" --var="catalog=$CATALOG")
}

step_fetch() {
    say "fetch the evidence"
    mkdir -p "$OUT"
    databricks fs cp --overwrite "$EVIDENCE_VOLUME/SG-DBX-01.json" "$OUT/SG-DBX-01.json" || die \
"the run produced no $EVIDENCE_VOLUME/SG-DBX-01.json.

Look at the publish_evidence task in the run: the notebook writes that file as its last step,
so its absence means the task did not reach the end. The run's output is in the workspace
under Jobs -> samegold monthly close."
    # Written HERE, by this machine, about this deploy - and kept in a separate file from the
    # record the workspace produced, so that nothing this laptop asserts can be mistaken for
    # something the workspace measured.
    cat > "$OUT/fetch.json" <<JSON
{
  "fetched_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "bundle_target": "$TARGET",
  "catalog": "$CATALOG",
  "deployed_from_commit": "$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)",
  "tree_dirty": $(test -n "$(git -C "$REPO" status --porcelain 2>/dev/null)" && echo true || echo false),
  "databricks_cli": "$(databricks version 2>/dev/null | head -1)",
  "note": "Written by scripts/databricks_run.sh on the machine that ran the deploy. Nothing here was measured inside the workspace; SG-DBX-01.json is. Neither file is part of evidence/history.jsonl - see evidence/databricks/README.md."
}
JSON
    echo "  evidence/databricks/SG-DBX-01.json"
    echo "  evidence/databricks/fetch.json"
    # The two per-rule tables, rendered ready to paste into the anchored blocks in
    # docs/databricks-run.md. The scalars there are filled in by hand from the record; these
    # two are tables, and a table typed out by hand is a table with a transcription error in
    # it. tests/fast/test_databricks_lane.py checks the paste against the record afterwards.
    local py=""
    command -v python3 >/dev/null 2>&1 && py=python3
    [ -z "$py" ] && command -v python >/dev/null 2>&1 && py=python
    [ -z "$py" ] && { echo "  (no python on PATH: read the JSON yourself)"; return; }
    "$py" - "$OUT/SG-DBX-01.json" <<'PY'
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
    echo "usage: scripts/databricks_run.sh [all|catalog|validate|deploy|seed|run|fetch]" >&2
    exit 2
}

case "${1:-all}" in
    all)      require_cli; require_auth; step_catalog; step_validate; step_deploy; step_seed; step_run; step_fetch ;;
    catalog)  require_cli; require_auth; step_catalog ;;
    # validate is the one step that needs no workspace beyond authentication, which is why it
    # is also what .github/workflows/databricks.yml runs by default.
    validate) require_cli; require_auth; step_validate ;;
    deploy)   require_cli; require_auth; step_catalog; step_validate; step_deploy ;;
    seed)     require_cli; require_auth; step_seed ;;
    run)      require_cli; require_auth; step_run ;;
    fetch)    require_cli; require_auth; step_fetch ;;
    *)        usage ;;
esac
