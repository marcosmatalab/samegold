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
