# samegold v0.2.0

The release in which the cloud half stopped being a claim about a laptop. The Databricks
workflow runs in CI against a real Free Edition workspace, the fast lane runs on two
architectures, and the three things that broke on the way are written up rather than fixed
quietly.

[v0.1.0](release-notes-v0.1.0.md) is the description of what this project is. This page is what
changed.

## What is new

- **The Databricks lane runs from CI.** `databricks.yml` takes `workflow_dispatch` only,
  defaults to `validate`, and offers no option that starts compute. Its token lives in a GitHub
  environment rather than a repository secret, and the job is guarded to this repository so a
  fork's pull request cannot reach it. The badge on both front pages says whether it has run -
  and a green tick there means the bundle resolved and the credentials worked, and nothing
  about the close.
- **`deploy-definitions`**, which is what CI runs when asked to deploy. Plain `deploy` creates a
  missing catalog with a SQL statement, and a SQL statement starts the one 2X-Small warehouse
  Free Edition gives you. The new subcommand asks `databricks catalogs get` - a REST call - and
  refuses with the command to run by hand. "This lane never starts compute" is now a property
  of the command instead of a fact about the workspace that week.
- **The fast lane runs on `ubuntu-24.04-arm` as well as `ubuntu-latest`**, so every published
  figure is recomputed from the seeds its own record names on x86_64 and on aarch64 and has to
  agree on both. Every wheel this project needs has a manylinux aarch64 build; nothing had to
  change. The figures are still PRODUCED on one architecture and now VERIFIED on two, and
  [`limits.md`](limits.md) keeps those two sentences apart.
- **[`join-skew.md`](join-skew.md)**, which answers the question every Spark interview asks and
  then declines to publish the answer as a claim. A key holding 30% of the rows gives a skew
  factor of **1.00** at two hundred thousand rows and at two million, because adaptive execution
  coalesces the shuffle into one partition; turn coalescing off and the same data gives
  **5.62**. The only configuration in which the number is interesting is the one
  [ADR 0005](adr/0005-adaptive-execution-stays-on.md) calls tuning the experiment to fit the
  claim, so it is a page with a script - `make skew` - and not a row in the chain.
- **[`docs/adr/README.md`](adr/README.md)**, because seven of the fourteen decisions were linked
  by no document in the repository and the directory had no index.

## The three findings

All three are in [`FINDINGS.md`](../FINDINGS.md), and none of them was found by a gate.

- **A green that expires.** The deploy lane went red with no line of the tree changed: the
  Databricks CLI fetches a Terraform binary at run time and verifies HashiCorp's signature over
  its checksums, and that key expired. The SHA pin added in September did not help and could not
  have - it fixes which CLI runs, and what changed is what that CLI fetches when it runs.
- **The freshness guard called a clean runner dirty**, for the third time on the same rule, and
  the comment above the previous fix had already written "did not look one file further" - and
  named the shape of the file it had not looked at.
- **Three sentences a reader caught**, including one in the v0.1.0 release notes that was false
  in the minute it was written, and one claim made in a handoff document without measuring it.

## What is NOT in this version

- **`bundle deploy` has not succeeded from CI.** It reaches the workspace and fails on a
  pipeline name collision with the pipeline the September deploys created. The diagnosis is
  under way and is deliberately not guessed at: the deployment state lives in the workspace, not
  in the checkout, and nothing in a clone can read it.
- **The exposure that produced the first finding is open, on purpose.** A deploy lane that
  downloads a binary while it runs can fail again, and the alternative is a pair of pinned
  versions that must agree with nothing checking that they do. [`limits.md`](limits.md) carries
  the reasoning.
- **No skew claim, and no join claim of any kind.** The measurement is published, the script is
  in the repository, and the number it produces under the configuration this project is allowed
  to use is 1.00.
- **Still one architecture in the chain.** Two verify; one produces.
- **Still no workspace anybody else can reach.** The bundle is checked from a clone; the
  workspace is not, and those remain different claims.

[**`docs/limits.md`**](limits.md) is the full list, and it is longer than this page on purpose.

---

The figures quoted above are the ones this release measured, and this file is not rendered from
the evidence chain: a release note describes a release, so it is frozen deliberately. Every
live figure is anchored on the front page, and where the two disagree the front page is right.
