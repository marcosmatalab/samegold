# ADR 0011 - the gate recomputes the record, it does not only validate it

**Status** accepted, 2026-09-22

## Context

ADR 0007 built the evidence gate and ADR 0010 made the chain append-only. Between them they
answer four attacks, and an adversarial review on 20 September 2026 measured all four holding
against a clean clone of `9e52f15`:

| attack | what the gate said |
|---|---|
| edit a figure in `README.md` | `exit 2`, `DRIFT stale-render: README.md does not match the evidence` |
| edit `verdict.rate` inside a record | `exit 2`, `CHAIN line 185 (SG-00): hash is '3d1772d0...' but the content hashes to '2c5aef23...'` |
| append a record with an invented `ci_run_url` | `exit 2`, `its ci_commit_sha is '000...' while its seeds come from 98c980b44512` |
| append a consistent forgery with an old timestamp | `exit 2`, `this record started at 2026-09-03..., before the previous one...: the history has been reordered` |

The fifth attack passed. It is fifteen lines and it edits nothing:

```python
from samegold.evidence.store import record_hash   # the repository's own hasher

forged = copy_of(latest["SG-03"])                 # a REAL record, with real seeds
forged["verdict"]["rate"] = {"successes": 999, "trials": 999, "point": 1.0}
forged["verdict"]["runs"]["started_at"] = <now>   # so the history stays in order
forged["prev"] = rows[-1]["hash"]                 # so the chain stays linked
forged["hash"] = record_hash(forged)              # so the hash verifies
append(forged)
```

Measured outcome: `README.md` and `CLAIMS.md` published
`SG-03 mutation campaign | PASS | 999/999 (95% CI 99.6%-100.0%)`, `samegold check` exited **0**
printing "evidence chain verified (186 records) and the documents match it", and
`pytest tests/fast` reported **594 passed**. In a git diff it looks like one more legitimate
evidence run.

`store.py`'s own docstring said that forging "requires rewriting the chain, which is a visible
act in the git history". That was the load-bearing sentence of the whole design and it was
false: appending is enough, and appending is what the chain is *for*.

The reason it was false is worth naming, because it is the same shape as most of `FINDINGS.md`.
Every check in the repository asked whether a record was **well formed** - hashed, linked,
ordered, seeded from a commit, provenance-shaped. Not one asked whether the number in it was
**true**. Those are different questions, and four layers of the first do not add up to one
layer of the second.

## Decision

**A published figure must be recomputable, and something must actually recompute it.**

Two gates, because they fail differently and cost differently.

**One: `samegold verify-latest` re-runs the claims.** For each claim's most recent record it
re-runs the claim from the seeds *that record names* and compares `successes` and `trials`.
Exact, costs minutes, and it runs as a step in `fast.yml`, in `evidence.yml` before the record
is pushed, and in `make preflight` - never inside the fast lane's pytest.

**Two: a fast-lane test compares each rate against the artifacts of its own record.** The rate
is a function of numbers the record already publishes: `SG-03`'s is `per_witness.ledger` over
`mutants_total - equivalent`, which is 67 over 94 - 27. The forgery moved the rate and left
`mutants_total: 94, equivalent: 27, per_witness.ledger: 67` two lines underneath it. That is
arithmetic over one file, it costs nothing, and it is what turns `make fast` red on the attack.
`reproduce.RATE_RULES` holds one rule per claim and a test fails when a new claim has none, so
the coverage of this gate is computed rather than believed.

### Three decisions inside it that were not obvious

**The seeds are pinned to the record's commit, not re-derived from HEAD.** Seeds come from the
commit sha. A published record is never at HEAD: the evidence job measures at C, commits the
record on top of it, and the merge makes HEAD a third commit. Re-running a claim at HEAD draws
different seeds over a different population, so every comparison would be a false alarm.
`SAMEGOLD_SEED_COMMIT` pins the derivation. It cannot be used to choose a favourable number:
`seed_source()` reports `"pinned"` whenever the pin differs from the real HEAD, and
`EvidenceStore._validate` refuses every source that is not `"commit"` or `"override"`, so a
record produced under a pin cannot enter the chain. `verify-latest` writes nothing in any case.

**A record whose code has moved is "not recomputed", not "failed".** If `src/samegold` differs
between the commit a record names and HEAD, a different answer is a different measurement
rather than a disagreement. The run says which files moved and that `make evidence` re-measures
it. The alternative - treating it as a failure - turns the lane red on every commit that
touches the generator, and a gate that is red by default is a gate somebody switches off. This
is the same "not run" versus "failed" distinction `scripts/preflight.sh` exists to make, and
ADR 0013's Delta lane makes again.

**Two claims are not recomputed by default, and they are named in the output.** `SG-00` would
run the entire fast lane a second time inside the lane already running it; its artifacts are
checked against the repository by `test_documentation.py` instead.
`SG-07` needs a JVM and ten minutes, so it belongs to the Spark lane. Both are listed in
`reproduce.NOT_BY_DEFAULT` with their reason, both can be forced with `--claims`, and the
summary line counts what was recomputed before it says anything reassuring - it cannot print
"every published figure reproduces" having recomputed nothing.

## Alternatives rejected

**Sign the records with a key.** A signature proves who wrote a record. The forged record was
written by the repository's own hasher on the author's own machine, so a signature would have
been perfectly valid. It answers "who" and the open question was "is it true".

**Rely on the build attestation.** `evidence.yml` already calls `attest-build-provenance` over
`history.jsonl`. It is real provenance and **no gate in this repository verifies it**, which
makes it documentation. Verifying it would prove the file came out of a runner; it would not
prove the number inside came out of a run. The same objection as signing.

**Make `main` a protected branch with `fast` required.** It would gate the evidence pull
requests, and it cannot: a pull request opened by `GITHUB_TOKEN` has its checks queued at
`action_required` and they stay there until a human clicks - measured on run 35587784478, which
waited a week. A required check that cannot report is a gate that cannot bite. This is why
the recompute step is inside the evidence job, before the push, rather than on the pull
request.

## Consequences

`fast.yml` grows from about 150 s to about 400 s, inside its existing `timeout-minutes: 10`.

The residual hole is stated rather than left to be found: a forger who appends a bad record
**and** changes a file under `src/samegold` in the same commit gets "not recomputed" instead of
"MISMATCH" from the expensive gate. The cheap gate still catches them unless they also rewrite
the artifacts consistently - and the artifacts are what `CLAIMS.md` and `FINDINGS.md` are
written from, so that is a document to rewrite too. It is narrower than what was there before,
which was nothing, and `docs/limits.md` carries it.

ADR 0014 turns auto-merge on for the weekly evidence pull request. That would be a bad idea on
its own, because a bad record could then land unattended. It is a reasonable one with this ADR
in place, and the two were deliberately decided together.
