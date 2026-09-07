# The prose audit, 7 September 2026

A reviewer read the front page and found three false sentences. This is what happened when the
question "how many more are there?" was asked properly, and the answer is the reason this
document exists rather than a line in a commit message.

## What was done

Two passes, deliberately different in kind.

**The mechanical gate.** `samegold.evidence.prose` checks the three shapes of claim this
repository can falsify from its own files: an exhaustive enumeration of a directory, a claim of
the form "has never run" against the committed run records, and a claim that a path does not
exist. It found **five** - the three that were reported, plus the sentence in `PARITY.md`
reading "this repository has never deployed one", of bundles it deploys with one command,
and `docs/milestones.md` calling four record sections "never run" two rounds after they
were in the record.

**The adversarial sweep.** Five agents read every document under a different lens - claims of
absence, claims that something has never happened, enumerations and counts, claims about tooling
and CI, and loose figures outside the anchors - each extracting candidate sentences and checking
them against the repository with real commands. Every candidate then went to a separate agent
whose instruction was to REFUTE it, defaulting to "not false" and looking specifically for a
scope or a date that would rescue the sentence.

**Sixty-seven statements survived that refutation.** Not sixty-seven suspicions: sixty-seven
sentences where a second reader, trying to defend them, could not.

## Where they are

| document | confirmed false statements referencing it |
|---|---|
| `docs/databricks-run.md` | 38 |
| `docs/milestones.md` | 22 |
| `README.md` | 17 |
| `docs/limits.md` | 15 |
| `PARITY.md` | 10 |
| `EXAM_MAP.md` | 7 |
| `FINDINGS.md` | 6 |
| `CONTRIBUTING.md` | 5 |

The counts overlap: one sentence can reference several documents, and several sentences say the
same wrong thing in different files. What they cluster into is smaller than the number and worse
than it looks:

1. **"every figure below still reads `NOT RUN`."** The header of `docs/databricks-run.md`, the
   equivalent in `docs/limits.md`, and the bullets repeating it. The same file carried twenty
   rendered anchors holding measured values while its own first paragraph said none of them did.
   The anchors are filled by a command; the sentence above them was typed by a person.
2. **The M12 tally.** "4 of 7 done", "three not started", "the dashboard is not started" - in the
   README, in `docs/milestones.md`, and in the summary line under the table those numbers are
   supposed to describe. The dashboard was declared, deployed and verified on 6 September.
3. **"three Databricks-only primitives."** `PARITY.md` pins four, under a heading that says four,
   and three other documents count three.
4. **Claims about what the tooling checks.** `CONTRIBUTING.md` promising that a test fails if a
   check CI runs is missing from `scripts/preflight.sh`, and the `make databricks` step list.
5. **Stale figures in prose rather than in anchors** - dimension version counts, capture
   provenance, round numbers.

## What was fixed in this round

The five the gate found, the two header blocks in cluster 1, the M12 tally in cluster 2, the
primitive count in cluster 3, and the round count in `FINDINGS.md`. The README was rewritten from
389 lines to about a hundred, which removed most of its seventeen by removing the sentences.

## What is still open, said plainly

**The rest of cluster 4 and 5, and the long tail inside `docs/databricks-run.md`.** That document
is nine hundred lines of narrative written across seven rounds, and a third of the confirmed
statements are in it: paragraphs describing what a future run will show, written before runs that
have since happened. Fixing them properly means re-reading the document against the record it now
renders from, which is a round of its own rather than a paragraph in this one.

The honest position is the one this repository takes everywhere else: the number is published,
the list is not pretended to be closed, and the gate that would have caught the mechanical part
of it exists now and runs on every push. What the gate does **not** cover - a false sentence
phrased in any way other than its three shapes - is the reason this audit was done by reading
rather than by matching, and the reason it will have to be done again.
