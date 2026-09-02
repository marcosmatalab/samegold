# ADR 0006 - mutants are generated; the specification ones are not, and say so

**Status** accepted, 2026-09-01

## Context

"We planted 15 bugs and the gate caught 13" is a number produced by the person who wrote the
gate, about bugs that person imagined. It is marketing.

## Decision

Two families, kept apart in the reporting.

**Generated** mutants come from the AST: comparison swaps, join kind swaps, interval bumps,
aggregate swaps, coalesce removals, order flips. The author does not choose them and some are
ones nobody would have thought of.

**Specification** mutants are written by hand and labelled, because no generator knows that
"a return belongs to the month of the sale" is a rule. There are six, each with a written
rationale, and they are the only experiment capable of falsifying the project's own
independence claim.

Survivors are enumerated. Equivalent mutants are classified **in writing**, one entry at a
time, in `mutation/equivalents.py`, and the score is published twice: accepting the
classification and refusing it.

## What this decision produced

The first campaign scored 0.71 and its survivors turned out to be holes in the **generator**,
not in the harness: no zero-cent line, no return exactly on the 45th day, no sale arriving
after a close. Nine boundary cases were added, and two specification mutants that had
survived (SPEC-04 and SPEC-06, both about what a close knew at the time) started dying. A
hand-picked mutant set would have found none of that.

It happened again, and the second time is the more useful one, because it shows how the hole
reopens. Contract 1.3.0 added two bounds on the money arithmetic and a rule about which of two
sales sharing a line key wins, and added **no data that reaches them**. The next campaign
dropped to 52 of 67 with fifteen survivors, every one of them a rule the generator had never
exercised: no record anywhere near a bound, no line with two sales, no line with two returns,
no amendment to zero. Not one of them was a weakness in a witness, and every one of them
would have been closed by writing "equivalent" beside it.

The lesson the first round taught is that a mutation score measures the data as much as the
code. The lesson this one adds is that the score **decays silently**: a rule can be added to
the contract, implemented correctly in both lanes, and be untested from the day it lands,
because nothing in the campaign asks whether the data can tell the new rule from its
neighbours. The survivors are the only thing that says so, which is why an unexplained
survivor is a finding and not a number to close out.

The same shape, one level up, and it is logged here because it is the same mistake and not a
neighbouring one. `ruff check src tests` printed "All checks passed" for ten rounds while
`databricks/` and `pipelines/` were in neither that command nor mypy's `files`. A whole
directory was unchecked, the green tick reported the scope of the command rather than the
state of the repository, and the thing that finally surfaced it was a formatting error
committed into `databricks/src/silver_expectations.py` by the round that added the note about
unchecked things rotting. Both directories are in `ruff` and `mypy` now.

And a third, found by pulling the same thread. The contract's money bounds carried a comment
claiming a close would need "a hundred billion" maximum-value lines before the BIGINT sum
overflowed. The division gives ninety-two. The bounds were introduced to stop three unbounded
lines from ending a close and they moved the threshold to ninety-three, while the sentence
defending them asserted a margin nine orders of magnitude larger than the one they gave. A
test performs that division now.

The general form of all three is one sentence: **a measurement's scope is part of its result,
and a result that does not carry its scope is read as if the scope were everything.** The
mutation score's scope was the data it ran on; the lint command's scope was two directories;
the bound rationale's scope was an arithmetic nobody performed. Each read as a statement about
the whole.
