# ADR 0014 - the evidence pull request is merged by the job that opened it

**Status** accepted, 2026-09-22 (superseding the version of this ADR that described `--auto`);
amended 2026-09-23: the merge now waits for the repository's own checks on the evidence branch

## Context

`evidence.yml` runs on `cron: "23 4 * * 1"`, spends about a quarter of an hour recomputing
every claim on a runner, pushes the records to a branch and opens a pull request. That design
is right and its comments argue it well: a record is evidence, and it should arrive somewhere a
person can look at it before it becomes the number on the front page.

Measured over four fires of that cron: run 33815591122 (3 September) was merged, and runs
34106077889, 34830672517 and 35586535706 were not. One of four. The cost was not theoretical -
two of those branches were **ahead 1, behind 0** of `main`, green runs over that very HEAD
carrying the correction that moved `SG-00` from `local run, not reproduced in CI, 98c980b44` to
`CI, 9e52f158e`. The front page of a repository whose thesis is that unregenerated documents
rot named a commit eighteen behind HEAD, while the regeneration sat in a branch.

Opening the pull request was never the hard part. Closing it was.

### The first version of this ADR described a mechanism that does not exist here

It said the fix was `gh pr merge --auto --squash --delete-branch`. That command was never added
to `evidence.yml`, and this document sat on `main` asserting that it had been - which is the
defect `tests/fast/test_prose_gate.py` now has a fourth rule for, and which is recorded in
`docs/findings/the-gate-found-three-things-and-two-were-its-own.md`.

It also would not have worked. Asked on 22 September 2026, GitHub answers:

```console
$ gh api graphql -f query='{ repository(owner:"marcosmatalab", name:"samegold") {
    autoMergeAllowed
    pullRequests(states: OPEN, first: 5) {
      nodes { number viewerCanEnableAutoMerge mergeable } } } }'

autoMergeAllowed            false
#2  viewerCanEnableAutoMerge false   mergeable CONFLICTING
#3  viewerCanEnableAutoMerge false   mergeable CONFLICTING
#4  viewerCanEnableAutoMerge false   mergeable MERGEABLE
```

Two reasons, and the second survives fixing the first. The repository setting is off, so the
mutation is refused outright. And auto-merge is a **queue for a pull request that is waiting on
something**: `gh api repos/marcosmatalab/samegold/branches/main/protection` answers 404 and
`gh api repos/marcosmatalab/samegold/rulesets` answers `[]`, so nothing is ever pending on a
pull request here and a queue has nothing to wait for. `viewerCanEnableAutoMerge` is false even
on #4, which GitHub itself reports as `MERGEABLE`.

`--auto` in this repository is a command that fails, wrapped in a warning that says merge it by
hand. That is the state the last three Mondays were already in.

## Decision

**The job merges the pull request it just opened, in the same step, with
`gh pr merge --squash --delete-branch`** - immediately, not queued.

**And the recompute gate moves into the job, before the push.** `samegold verify-latest` is a
step of `evidence.yml`, between rendering the documents and committing them. ADR 0011 said this
was so before it was; it is so now.

## Why not the alternatives

**Create a ruleset with `fast`, `spark` and `evidence` as required checks, and keep `--auto`.**
This is the option that makes `--auto` legal, and it is worse than doing nothing, for a reason
that is measured rather than argued: a pull request opened by `GITHUB_TOKEN` does not get its
checks run. The only check ever queued on an evidence pull request was run 35587784478, which
ended `action_required` without executing, and was still sitting there a week later. Make
`fast` required and the evidence pull request waits on a check that will never report, so
`--auto` never fires and the branch accumulates exactly as today - with the added cost that
every direct push to `main` is now blocked by the same ruleset.

It can be made to work by having the job open the pull request with a personal access token
instead of `GITHUB_TOKEN`, because a PAT-opened pull request does run its checks. That means a
long-lived PAT with `repo` scope in a public repository's secrets, to buy a review nobody has
performed in four weeks. Rejected on both counts.

**Push straight to `main` and drop the pull request.** This is what the decision does in
substance, and the pull request is kept anyway because it costs one API call and leaves a
permalink with a readable diff. A squash-merged pull request is a better audit trail than a
commit, and the branch is deleted either way.

**Leave it and merge by hand every Monday.** That is what the last three Mondays were.

## Consequences

A record can now reach `main` without a person seeing it first. What stands between it and the
front page is [ADR 0011](0011-the-gate-recomputes-the-record.md), running on the runner that
produced the record, before the push - which is strictly earlier than any check on the pull
request could have bitten, and which is why the two decisions were made together.

The merge is conditional on the steps before it in the job - `samegold check`, then
`samegold verify-latest`, either of which fails the job before the branch is pushed - AND, since
23 September 2026, on the repository's own checks passing on the evidence branch.

That second condition is the amendment, and what it corrects is a sentence that used to stand
here: that `fast.yml` "then runs on the resulting push to `main`". It did not. The branch, the
pull request and the merge are all made with GITHUB_TOKEN, and GitHub starts no workflow for a
push or a pull request made with that token, so no check ever ran on an evidence pull request
or on the commit it left on `main`. A `workflow_dispatch` made with the same token is the
documented exception, so the job now runs `gh workflow run fast.yml --ref "$branch"` and the
same for `databricks-evidence.yml`, waits for both with `gh run watch`, and merges only if both
are green; a red one leaves the pull request open and fails the job. The cost is the length of
the job, by the few minutes those two take. `tests/fast/test_evidence_pr_checks.py` holds the
order.

The same amendment added a `commit` input to the dispatch, so a run can measure a named commit
- a release's own - after `main` has moved past it. The seeds still derive from that commit's
sha: the input chooses what is measured, not how.

**What this does not buy: a human reading the diff.** It never did. The honest description of
the previous design is not "review" but "a branch nobody closed", and this ADR exists because
saying otherwise for three weeks cost the front page eighteen commits of accuracy.
