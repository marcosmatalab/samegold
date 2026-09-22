# Three things no gate found, and the reader who did

**22 September 2026.** Every other write-up in this directory is about a check that caught
something. This one is about three claims that went into the repository, or nearly did, with
nothing in the repository able to contradict them, and a person reading a diff who could.

It is here because a repository whose thesis is "no assertion is taken on trust" should say
which of its assertions were.

## One: a sentence in the release notes that was false the moment it was written

`docs/release-notes-v0.1.0.md` said, under what the version does NOT contain:

> **No figure here was measured on more than one machine.** All ten claims in this tag's table
> were produced by a single run of the evidence workflow on `ubuntu-latest`, which is one
> runner, one Python and one architecture.

It replaced an earlier bullet that the first CI evidence run had made stale, and it was written
in the same minute as the run that falsified it. Measured against the chain it was sitting next
to:

| | |
|---|---|
| `environment.platform` values in `evidence/history.jsonl` | **4**: Windows 11, WSL2, another Linux, the Azure runner |
| `environment.python` values | **4**: 3.11.15, 3.11.16, 3.12.3, 3.13.2 |
| claims measured both on Windows and on the runner | **10** |
| of those, same rate on both | **8** |

The two that differed are `SG-00` and `SG-06`, which count this repository's tests and this
chain's records rather than anything about the data, and whose denominators are a property of
the machine by definition. So the true statement is the opposite of what was published: **every
claim about the data was measured on two operating systems and agreed.**

**Why no gate saw it.** The drift gates compare a FIGURE against the record that produced it,
and a COMMAND against the tree that runs it. This was a sentence about the shape of the whole
chain, quantified in words - "a single run", "one architecture" - with no anchor to be stale
and no command to be missing. There is no rule in `samegold.evidence.prose` for "counts
something the evidence can count", and writing one is not obviously possible: the space of
sentences that imply a number is the space of sentences.

The bullet now says what is actually missing, which is a different **architecture**: every
record in the chain is x86_64, and one DuckDB version appears in all of them.

## Two: a branch called safe to delete, without measuring it

Asked what to do with the `stale-deploy-guard` branch, a handoff document said:

> `stale-deploy-guard` no necesita cherry-pick: su contenido ya está en `main` por otros commits.

That was not measured. It was inferred from the branch being old and the feature it describes
being present, and it was handed over as a fact, in a document about what to do next, in the
repository whose whole argument is that an unmeasured claim is only true by accident.

Measured afterwards, at the reader's insistence:

```
d2e3a54 adds 14 lines to FINDINGS.md and removes 2
of the added lines, 14 are present VERBATIM in main; 0 are not
the two removed lines: gone from main too
```

**The conclusion was right and the method was wrong**, and those are separate facts. The
content is in `main` because of `ad936aa`, and the finding that commit records is
[the one about a document written and not committed](../../FINDINGS.md) - the entry that says
the FINDINGS entry closing the stale-deploy finding "was written and **not committed**", so the
next deploy went out of a tree carrying it and the record said `deploy.tree_dirty: true`.
`d2e3a54` is that uncommitted document, committed on a side branch. The branch is the artefact
of a finding the repository already holds.

**What found it.** `git diff`. Not a gate: a person who did not believe the sentence.

## Three: fourteen summaries written from the titles

`docs/adr/README.md` was added the same afternoon, because seven of the fourteen ADRs were
linked by no document in the repository. It has a column saying what each decision rejected.

The first draft of that column was written from the ADR **titles**, because a title is right
there and the alternatives section is three screens down. Checking the fourteen against the
documents changed **fifteen cells across two passes**: `0002` rejected giving up version
agility and not "pinning per lane"; `0008` rejected making any statement about latency or
money and not "timing"; `0013` rejected vendoring 30 MB of Delta jars and not "skipping",
which is what the decision replaced rather than what it was competing with. The index also
described a supersession policy - superseded files pointing at the ones that replaced them -
that this repository has never used: all fourteen are `Accepted`, and the one reversal is
inside `0014`, which rewrote itself and says so in its status line.

None of that reached `main`. It is here because it is the same defect two hours later, and the
only reason it was checked is that the second finding above had just happened.

## What the three have in common

Each is a sentence **about the repository** rather than about the data: how many machines the
chain has seen, whether a branch's content is in `main`, what an ADR rejected. Every gate here
reads in the other direction - from a document to a record, or from a document to the tree -
and none of them reads a document against another document's contents or against the history.

That is not an argument for building a fourth kind of gate today. It is an argument for knowing
where the gates stop, which is what [`docs/limits.md`](../limits.md) is for, and for the thing
that actually worked three times in one afternoon: somebody reading the claim and not believing
it.
