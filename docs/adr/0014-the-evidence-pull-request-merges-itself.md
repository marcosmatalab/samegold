# ADR 0014 - the weekly evidence pull request closes itself

**Status** accepted, 2026-09-22

## Context

`evidence.yml` runs on `cron: "23 4 * * 1"`, spends about a quarter of an hour recomputing
every claim on a runner, pushes the records to a branch and opens a pull request. That design
is right and its comments argue it well: a record is evidence, and it should arrive somewhere a
person looks at it before it becomes the number on the front page.

Measured on 22 September 2026, over four fires of that cron:

| run | branch | opened | merged |
|---|---|---|---|
| 33815591122 | `evidence/run-33815591122` | 3 Sep | yes |
| 34106077889 | `evidence/run-34106077889` | 7 Sep | **no** |
| 34830672517 | `evidence/run-34830672517` | 14 Sep | **no** |
| 35586535706 | `evidence/run-35586535706` | 21 Sep | **no** |

One of four. The cost was not theoretical. `evidence/run-34830672517` and
`evidence/run-35586535706` were each **ahead 1, behind 0** of `main`: green runs over that very
HEAD, carrying the correction that moved `SG-00` from `local run, not reproduced in CI,
98c980b44` to `CI, 9e52f158e` and the other nine rows from a commit eighteen behind. So the
front page of a repository whose entire thesis is that unregenerated documents rot was itself
eighteen commits stale, while the regeneration sat in a branch.

Opening the pull request was never the hard part. Closing it was.

## Decision

**`gh pr merge --auto --squash --delete-branch` immediately after the `gh pr create` that is
already there**, with a warning rather than a failure if auto-merge is not enabled on the
repository: the evidence is already pushed and the pull request is already open, so a refused
auto-merge loses nothing except the automation.

**And the compensation moves into the producing job.** `samegold verify-latest` runs as a step
of `evidence.yml`, before the commit and push, not as a check on the pull request.

## Why the compensation is where it is

Handing the merge to a machine means a bad record could land unattended. The obvious place to
compensate is a required status check on the pull request, and it cannot be: a pull request
opened by `GITHUB_TOKEN` has its checks queued at `action_required` and they wait for a person
to click. Measured on run 35587784478, the `fast` check of the 21 September pull request, which
sat at `action_required` for a week. A required check that cannot report is not a gate, it is a
deadlock, and with `--auto` waiting on it the branch would never merge at all.

So the recompute runs inside the job that produces the record, on the runner that measured it,
before anything is pushed. A record whose rate does not reproduce never becomes a branch.

## Alternatives rejected

**Push straight to `main` and skip the pull request.** `main` is not protected - `gh api
repos/.../branches/main/protection` answers 404 - so this is technically available. It throws
away the human review, which is half of what the workflow is for, and it would still be true
that nobody ever looked.

**Protect `main` and require `fast`.** The same `action_required` rule makes it a deadlock for
exactly the pull requests it would be protecting, and it would block the author's own pushes
for the benefit of a check that cannot run.

**Leave it and merge by hand every Monday.** That is what the last three weeks were. Section 10
of the audit that prompted this makes the honest version of the argument: eight Mondays of
visible activity cannot be bought with hours today, and the only thing that can is a repository
that keeps a pulse when nobody is looking at it.

## Consequences

A bad record can now land on `main` without a person seeing it first. What stands between it
and the front page is [ADR 0011](0011-the-gate-recomputes-the-record.md): the step that
recomputes each claim from the seeds its own record names, in the job, before the push.

The two decisions were made in the same round on purpose. Auto-merge without the recompute
would be a bad idea; the recompute without auto-merge leaves the front page stale for weeks at
a time. Each is the reason the other is safe.
