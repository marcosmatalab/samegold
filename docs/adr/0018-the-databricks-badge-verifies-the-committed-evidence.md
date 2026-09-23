# ADR 0018 - the Databricks badge verifies the committed evidence, on every push

**Status** accepted, 2026-09-23

## Context

The front pages showed a `databricks` badge pointing at `.github/workflows/databricks.yml`. That
workflow runs only when dispatched by hand. It validates or deploys definitions and does not start
the job, and that is deliberate: a run spends the free tier's daily quota. So the badge's colour
recorded whether the last manual dispatch validated and whether the credentials worked that day.
A reader took it for the state of the Databricks lane, and it measured nothing about the close.

## Decision

**The badge points at `.github/workflows/databricks-evidence.yml`**, which runs on every push and
every pull request and verifies, offline, the Databricks evidence that is committed.

- The record and its capture match the digests pinned in `PINNED_DIGESTS`, and every figure the
  documents quote from them matches (`tests/fast/test_front_page_figures.py`).
- Every closed version the workspace published is recomputed to the cent by the DuckDB reference,
  over the population the late-arrival generator reproduces, and no signed-off version was
  rewritten (`tests/fast/test_databricks_close_parity.py`).

**`databricks.yml` stays as it was**: dispatch only, validate by default, no compute, and no
badge.

**`tests/fast/test_badges.py`** fails for any workflow badge on either front page whose workflow
runs only when dispatched. It also fails for a Databricks badge whose workflow does not run on
push and pull request, or does not run both of those tests.

## Alternatives rejected

- **Run the Databricks job from CI on push.** This would make the badge measure the cloud close,
  and it would spend the daily quota on every commit. It would also put a workspace credential
  in reach of every push.
- **Remove the badge.** This is honest but hides the part of the Databricks lane that can be
  verified from a clone, and verifying that part on every push is cheap.
- **Keep the badge on the deploy workflow and explain it in prose.** The prose would be the fix
  declared instead of made.

## Consequences

**Green now means that the Databricks figures on this repository's pages are the ones the
workspace measured, and that the open-source lane recomputes each of them.** It does not mean
the job ran recently. That is a fact about a workspace this repository cannot reach, and
`docs/limits.md` says so.

**The two tests also run in the fast lane.** The separate workflow exists so that the badge
names what it measures, and so that a failure there reads as "the Databricks evidence" rather
than as one line among many in the fast lane.
