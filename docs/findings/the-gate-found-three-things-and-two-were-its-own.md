# The recompute gate's first run found three things, and two of them were the gate

**22 September 2026.** `samegold verify-latest` was built to close the one attack the evidence
chain could not stop: appending a well-formed record with a rate nobody computed
([ADR 0011](../adr/0011-the-gate-recomputes-the-record.md)). Its first run against the real
evidence reported three mismatches. One was a badly defined claim. The other two were defects
in the gate, and it was accusing good evidence of being forged.

This page exists because that is the more useful half of the story, and it was in a chat
transcript rather than in the repository.

## What it printed

```console
$ samegold verify-latest
MISMATCH SG-01: the published rate is 9/9 and recomputing it gives 15/15.
MISMATCH SG-04: the published rate is 1/1 and recomputing it gives 2/2.
MISMATCH SG-06: the published rate is 203/203 and recomputing it gives 204/204.
5 claims recomputed from their own seeds; 3 DID NOT REPRODUCE; 2 not recomputed
```

Every one of those records was genuine.

## One: the gate ignored the profile the record names

**What.** SG-01 and SG-04 were written by a sweep at the `fast` profile. `verify-latest`
re-ran them at `ci`, because `ci` is its own default. The two profiles generate different
populations - different day counts, different order volumes - so SG-01's fifteen comparisons
over five closes became nine over three. Nothing was wrong with either number.

**Why it was invisible.** The command's own docstring said it recomputed each claim "from the
seeds its own record names", and it did: the seeds were pinned correctly, to the right commit,
through `SAMEGOLD_SEED_COMMIT`. The seeds were the part everybody thought about, because the
seeds are what this repository's whole argument is about. The profile is the other half of what
decides the population, it lives in the same `runs` block two fields away, and it was read by
nobody.

**What protects it now.**
`tests/fast/test_reproduce.py::test_the_profile_the_record_names_is_the_one_it_is_recomputed_at`
hands the gate a record written at `fast` and fails unless the runner is asked for `fast`.

**And the shape of the fix mattered more than the fix.** The runner's signature grew from
`(claim_id, sha)` to `(claim_id, sha, profile)` and was about to grow again for the third
finding below. A signature that has to grow every time somebody discovers another field the
record already carries is a signature that should have been the record. It takes the record
now.

## Two: SG-06 was a claim about "now"

**What.** SG-06's rate was `records - breaks` over `records`, measured over the evidence chain
as it stood. The chain grows: this claim's own record is appended to it the moment the claim
finishes. So the run that published 203/203 was immediately followed by a chain of 204, and
re-running it gave 204/204 - and would have given a different answer every time, forever.

**Why this is not a limitation.** It was called one for about an hour, and listed among the
claims the gate does not recompute, with a reason attached. That was wrong, and the reasoning
is worth keeping: **a measurement whose denominator is "now" cannot be checked by anybody,
including whoever took it.** "Not reproducible, with a reason" reads like honesty and is
actually a claim that was never about a fixed thing. The third option - redefine it or retire
it - is the only one that leaves a claim behind.

**What it says now.** The record names the head it verified, in `chain_head`, and
`claim_seed_provenance(evidence_dir, up_to=head)` verifies the prefix ending at that head. Run
it with no head and it asks about the chain today, which is what `make evidence` wants; run it
with the head its record names and it asks the same question it asked then. A head that is no
longer in the chain is a **failure**, not a missing input: it means the history was rewritten
rather than appended to, which is the event the chain exists to make visible.

**What protects it now.** Four tests in `tests/fast/test_evidence_gate.py`, of which the one
that matters builds a chain, measures it, appends two more records underneath, and requires the
same answer from the same head.

## Three: the one real finding, and it was the gate working

Nothing. The other seven claims reproduced exactly. That is the result the command was built
for, and it is worth writing down beside the two failures, because a gate whose first run finds
only its own bugs is a gate that has not been pointed at anything yet.

## The same week, in prose: two ADRs described changes that were not there

Not found by this gate - it reads evidence records, not documents - but the same defect class
and the same week, so it belongs here.

**ADR 0014** was committed to `main` saying the fix for the unmerged evidence branches was
`gh pr merge --auto --squash --delete-branch`, "immediately after the `gh pr create` that is
already there". The command had been written, refused at commit time by a permission guard, and
the ADR went in without it. It also would not have worked: measured afterwards,
`autoMergeAllowed` is false on this repository and `viewerCanEnableAutoMerge` is false on every
open pull request, because auto-merge is a queue for a pull request waiting on something and
`main` has no protection and no rulesets. The ADR is rewritten and the mechanism it now
describes - an immediate `gh pr merge --squash` after the recompute gate has run in the same
job - is the one that exists.

**ADR 0011** said `samegold verify-latest` ran "as a step in `fast.yml`, in `evidence.yml`
before the record is pushed, and in `make preflight`". Two of those three were true. It is a
step of `evidence.yml` now.

**What protects it now.** A fourth rule in `samegold.evidence.prose`: an ADR whose status is
`accepted` may not quote a command, in the sections where it asserts rather than recounts, that
the implementation does not run. Two versions of that rule found nothing first, and both are
regression tests:

  * the first read every tracked file, so the ADR was its own evidence - the command it claimed
    to have added appears in the tree, in that ADR;
  * the second excluded `docs/` and still passed, because the comment in `prose.py` explaining
    the defect quotes the command as an example. **A comment is prose that happens to live in
    source.** Python is reduced to its code by an AST round-trip now, and `#` comes off the
    YAML and the shell.

**What it still does not catch**, stated here rather than left to be found: ADR 0011's lie.
`samegold verify-latest` existed in `fast.yml`, so a rule that asks "is this command anywhere"
answers yes. Checking that a command is in the *file the ADR names* is a further rule that has
not been written. [`docs/limits.md`](../limits.md) carries it.

## The lesson, which is the one this repository keeps having

Every one of these is the same shape as the defects in [`FINDINGS.md`](../../FINDINGS.md): a
check whose **name** describes the right thing and whose **measurement** describes something
next to it. "From the seeds its own record names" measured the seeds and not the profile. "The
chain verifies" measured a chain that was a different length each time. "An accepted ADR" was
read by three rules that had nothing to say about the claim it was making.

The difference this time is that the gate was new enough that its first run was still being
read carefully. A gate installed and then trusted would have published these as findings about
the evidence.
