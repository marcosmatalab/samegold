# Runbook: the alert has fired

It is three in the morning and `samegold_close_not_sound` has triggered. This page is what to
do, in order, and it assumes nothing about what you remember.

Everything here is either a measurement made in this repository or a behaviour of the platform
recorded with its date. Where a claim came out of an incident, the incident is in `FINDINGS.md`
and this page points at it rather than restating it.

---

## 0. What fired, and what it does NOT mean

`samegold_close_not_sound` fires when the most recent row of `<catalog>.main.job_run_status`
has `ok = false`, **or when there is no row at all**.

**It deliberately does not watch the job's terminal state, and that is the whole point.** The
evidence task runs under `run_if: ALL_DONE` and is last in the graph, so it succeeds whatever
happened upstream - which means a run whose close verification FAILED reports
`SUCCESS_WITH_FAILURES`, not `FAILED`. An alert on "not SUCCESS" would have stayed silent
through exactly the failure this lane exists to make visible. That is measured, from run 2 of
5 September 2026; `FINDINGS.md` carries it with its class.

So:

- **the job's terminal state is not a signal here.** Do not conclude anything from a green run.
- **an empty result is a trigger**, not an all-clear. `job_run_status` is written by the last
  task of every run, so no rows means no run ever finished writing one.

`ok` is derived in `databricks/src/publish_evidence.py` from three things together: no section
incomplete, no expected check missing, and no upstream task in a state this graph does not
produce on a healthy run.

---

## 1. Read the record before touching anything

```sh
scripts/databricks_run.sh fetch
```

This copies `SG-DBX-01.json` out of the workspace into `evidence/databricks/`. Read these
fields, in this order:

| field | what it tells you |
|---|---|
| `incomplete` | which sections the notebook **could not read**. See §2 - this is the one people misread |
| `missing_checks` | checks the branch owed and never wrote |
| `orchestration.decision` / `.branch` | what the close decided, and which branch it took |
| `close_verification` | per-month check results, with the numbers that decided each |
| `task_states` | the state of every task in the graph |
| `update.last_state` | the pipeline update's terminal state |

If `fetch` prints `SECTIONS THAT COULD NOT BE READ`, those sections are holes and the anchors
in `docs/databricks-run.md` take the error message rather than a zero.

---

## 2. A hole is not a zero

**`incomplete` is the most dangerous field on this page, because an empty section and a
failed section look identical once they reach a table.**

- `expectations: []` with `incomplete: []` beside it means **the run genuinely reported no
  expectations** - which is what run 1 of 5 September produced, because it ingested nothing.
- `expectations: []` with `"expectations"` in `incomplete` means **the notebook could not read
  that section**. The number is unknown, not zero.

Zero rows reads as "nothing to report" to a human and to a chart. That is why the record names
its holes explicitly, and why an anchor over a hole renders `NOT RUN` rather than a blank or a
`0`. When you are deciding whether the close is sound, treat every name in `incomplete` as
**unmeasured**, and do not let a downstream table quietly turn it into a fact.

`missing_checks` is the same idea one level in: a branch that was supposed to write a check and
did not. Both lists being empty is part of what `ok` means.

---

## 3. Is it the data, or is it the platform?

Decide this before re-running anything, because the two have opposite responses: a data
problem is a finding to keep, and a platform problem is a run to repeat.

**It is the DATA if:**

- `close_verification` has rows with `ok = false` and a populated `detail`. The check ran, read
  numbers, and those numbers failed. `detail` carries them.
- the quarantine counts moved but the conservation identity still closes
  (`silver_classified = silver_events + silver_quarantine`). The lane is working; the
  population changed.
- `undecided_rules` is non-empty. A rule returned NULL rather than true or false - that is the
  ANSI-mode defect's signature, and `docs/limits.md` has the measurement table.
- the close restated a month you did not expect. Check whether the arrivals are returns
  against sales in an earlier month: `gold_close.py` books a return into the month of its
  **sale**, so a February return can move January and leave February alone.

**It is the PLATFORM if:**

- `task_states` shows a task in a state this graph does not produce, or a task that ran twice.
  Serverless auto-optimization retries tasks; `max_retries: 0` does not stop it, and
  `disable_auto_optimization: true` is what does. `FINDINGS.md` carries that one.
- `update.last_state` is `FAILED` with ERROR-level events in the pipeline event log.
- the record is absent entirely, or `job_run_id` reads the literal `{{job.run_id}}`.
- everything is zero and the landing volume is empty - check with
  `databricks fs ls dbfs:/Volumes/<catalog>/raw/landing` before blaming the pipeline.
- **quota.** Free Edition stops all compute for the day when the quota is exhausted, and a
  failed update spends it like a successful one.

---

## 4. Re-running without spending the day

**Read this before typing anything, because the wrong command costs the account's whole daily
compute quota and there is no second attempt until tomorrow.**

### Repair the failed task, do not re-run the job

```sh
databricks jobs repair-run --run-id <run-id> --rerun-tasks <task-name>
```

**`--rerun-tasks` re-runs the tasks you name and NOT their dependents.** That is the property
worth knowing at three in the morning, and it cuts both ways:

- it is why a repair is cheap: `verify_month` failing for one month is repaired by re-running
  that month alone, and the other months are not spent again
  (`databricks/src/verify_month.py` says so where it raises);
- it is also why a repair can leave you with a **stale record**. `publish_evidence` is
  downstream, so repairing an upstream task does not by itself rewrite the record in the
  volume. Measured on 5 September 2026: after the repair, `publish_evidence` stayed at
  `attempt_number: 0` and did not overwrite it. If you need the record to describe the repaired
  state, `publish_evidence` has to be in the `--rerun-tasks` list too.

### Do not reach for `run-full-refresh` reflexively

`scripts/databricks_run.sh run` is an incremental update. `run-full-refresh` re-reads the whole
landing volume from scratch and recomputes the close - which will produce **one correct close
and no restatement at all**, destroying the very thing a second close demonstrates. It is the
right command after a `schemaHints` change and almost never otherwise, because
`cloudFiles.schemaLocation` caches the inferred schema.

If you do full-refresh, the schema cache must be deleted **together with** the refresh and never
on its own: a re-inferred schema under an existing checkpoint fails on a schema change instead
of on the type you were trying to fix.

### Deploy before you run, if anything changed

`databricks bundle run` runs what was **deployed**, not what is in your tree. On 4 September
2026 that silently cost two things: `publish_evidence.py` did not write
`dim_customer_scd2.json`, and the record carried no `deploy` key - both because the commits
that added them had never been deployed. **The task ended SUCCESS.** `scripts/databricks_run.sh
all` deploys before it runs, for this reason.

---

## 5. If the next command fails

These are the fields predicted to break, kept as written, and moved here from
`docs/databricks-run.md` - which is a description of what a run returned, not a page anybody
reads while something is on fire. One of them has since been made explicit rather than left to
be inferred: `volume_type: MANAGED` is spelled out, because the reference does not mark it
required or optional and "the documentation is ambiguous" is not a reason to find out at POST
time on someone else's workspace.

| symptom | field | what it means |
|---|---|---|
| `validate` rejects an unknown field | `development: true` on the pipeline, or `resources.volumes` | the CLI is older than the field. Upgrade the CLI rather than deleting the field: `development` is what stops a failed update retrying into the quota |
| `deploy` fails on a missing catalog | none | `scripts/databricks_run.sh catalog` did not run, or the token cannot create a catalog. A bundle cannot declare one |
| the pipeline fails at import with `ModuleNotFoundError: pyspark.pipelines` | the three `libraries.file` sources | the runtime on `channel: CURRENT` does not expose the Spark 4 declarative API under that name. The lane is written against it deliberately - `PARITY.md` explains why - and this is the honest way to find out |
| `cluster_by_auto` is rejected | `gold_close.py` | automatic liquid clustering needs predictive optimization on the metastore. If Free Edition does not enable it, that belongs in `docs/databricks-run.md` as a limit, not as a workaround |
| the run finishes and every count is 0 | the `seed` step | nothing was in the landing volume. `databricks fs ls dbfs:/Volumes/<catalog>/raw/landing` says whether the upload happened |
| `job_run_id` in the record reads `{{job.run_id}}` | `resources/jobs.yml` | the runtime did not recognise that dynamic value reference and passed the text through. The record shows it rather than hiding it behind a blank |
| `validate` fails with `cannot configure default credentials` | none | `databricks bundle validate` resolves authentication and calls `/api/2.0/preview/scim/v2/Me` before it finishes. It needs a reachable workspace and working credentials; measured 8 September 2026, a fake host and token fail the same way |
| the `databricks` workflow was green and is now red at the first step, with an authentication error | the `databricks` GitHub environment | **the token has expired, and that is its expected end.** Free Edition issues personal access tokens with an expiry and has no service principals to replace them with - `docs/limits.md` says why. Issue a new PAT in the workspace, update `DATABRICKS_TOKEN` in the `databricks` environment (Settings -> Environments -> databricks), and re-dispatch with `validate` before `deploy`. Nothing in the repository changes; do not commit a token, and do not move the secret to repository secrets to make it easier |
| the workflow log stops after `==> catalog` | none | the catalog step failed, so the deploy never started and the bundle is not the problem. A bundle cannot declare a catalog - there is no `catalogs` resource type, and on Default Storage `databricks catalogs create` fails - so it is created with SQL out of band by the same script. Check the token can create a catalog |

---

## 6. When it is over

- If it was a **data** problem: the finding goes in `FINDINGS.md` with its defect class, and any
  figure it moved is re-rendered from the record by `samegold readme` rather than edited.
- If it was a **platform** problem: the measurement goes in `docs/limits.md` with its date.
- Either way, `scripts/databricks_run.sh fetch` then `samegold readme` then `make check`. A
  document that disagrees with the record fails the fast lane, which is the point.
