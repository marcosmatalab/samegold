# Changelog

What each release changed, and why. Not a history of versions that never existed: this file
starts at the first tag, and the eighty-nine commits before it are in `git log` and in
[`FINDINGS.md`](FINDINGS.md), which is where the interesting part of them lives.

The format is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the versions are
[semantic](https://semver.org/spec/v2.0.0.html). `samegold` is not published to PyPI, so
"version" means the state of the repository at a tag, not a package a dependency resolver
will see.

## [0.1.0] - 2026-09-22

The first tagged close. Ten claims, every published figure recomputed from the seeds its own
record names, and no number on the front page typed by a person.

### Added

- **`samegold verify-latest`**, the gate that recomputes a record instead of validating it.
  Four defences answered "is this record well formed" and none answered "is this number true":
  an adversarial review published `SG-03 | PASS | 999/999` on the front page by APPENDING one
  well-formed record, with real seeds, a fresh timestamp and a hash computed by this
  repository's own hasher, and `samegold check` exited 0 on it with the fast lane green. The
  command re-runs each claim from the seeds its record names and compares.
  [ADR 0011](docs/adr/0011-the-gate-recomputes-the-record.md) carries the decision, the three
  choices inside it that were not obvious, and the hole it still leaves.
- **A fast-lane test that compares every published rate against the artifacts of its own
  record.** The expensive gate above costs minutes; this one costs nothing and is what turns
  `make fast` red on that forgery, because moving a rate to 999/999 leaves `mutants_total: 94`
  and `equivalent: 27` sitting two lines underneath it.
- **`SAMEGOLD_SEED_COMMIT`**, which pins the commit the seeds derive from so that a record can
  be recomputed at all. A published record is never at HEAD. `seed_source()` reports `pinned`
  whenever it is set to anything but the real HEAD, and the store refuses every source but
  `commit` and `override`, so a pinned run cannot be published.
- **`docs/databricks-run-evidence.md`**: what the Free Edition workspace measured, rendered
  from the committed records - the Type 2 dimension row by row with its `__START_AT` and
  `__END_AT`, the four closed versions of two months, the expectations with their pass and fail
  counts, the ten pipeline updates including the three that failed. It is the one part of this
  repository that cannot be recomputed from a clone, so it is at least readable from one.
- **`docs/img/job-graph-light.svg`** and **`docs/img/job-graph-dark.svg`**: the six tasks of the close job and the condition
  that branches them, checked against `databricks/resources/jobs.yml` three ways - it may not
  name a task the bundle does not declare, may not draw a dependency the job does not have, and
  must label both outcomes of the branch.
- **Tests for `src/samegold/faults/`**, which was 198 statements at 0% in every lane. The crash
  harness had nothing checking it: not the bound arithmetic that once printed 1.4979 as a
  probability, not the schedule that once asked for batches the run never produced, not the
  accounting that separates a missed injection from a pass. 84% now, with no JVM.
- **A function-size gate** in `tests/fast/test_architecture.py`, with the guard on the guard: a
  walk that finds fewer than 200 functions fails before the limit is applied to them.
- ADRs [0011](docs/adr/0011-the-gate-recomputes-the-record.md),
  [0012](docs/adr/0012-what-this-repository-is-mostly-made-of.md),
  [0013](docs/adr/0013-the-delta-lane-fails-when-it-cannot-verify.md) and
  [0014](docs/adr/0014-the-evidence-pull-request-merges-itself.md).

### Changed

- **The demo transcript on the front page is rendered, not pasted.** It announced 780 events
  and seed 6569293562773694097 while the program printed 694 and 9769305124036219406, and had
  been wrong for eighteen commits. It was the only defect here a reviewer could find by running
  the command the first screen tells them to run. `samegold demo` and the README block now go
  through one function over figures SG-00 measures.
- **The provenance column links the run.** `grep -rno 'actions/runs/[0-9]*' --include=*.md .`
  over this repository used to answer with nothing, so the word "CI" on the front page was the
  least checkable thing on it.
- **`generate` was split from 1 295 lines into nineteen functions.** The seeds derive from the
  commit sha, so any split that moves one RNG draw moves every published figure; it was
  verified by generating at a fixed seed before and after and comparing a digest of every byte
  written, at both profiles. Identical, both.
- **The Delta lane fails when it cannot reach Maven Central**, instead of skipping five tests
  and exiting 0 under a reason that named the wrong component.
  [ADR 0013](docs/adr/0013-the-delta-lane-fails-when-it-cannot-verify.md).
- **`scripts/preflight.sh` counts an unreachable Maven Central as "not run", not "failed".**
  It used to report `FAILED delta/spark` with 143 errors, which anyone behind a corporate proxy
  reads as "this repository is broken".
- **The coverage gate moves from 58% to 65%.**
- The README says in its own words, and with measured figures, what share of this repository is
  Spark and Delta and what share is the harness.
  [ADR 0012](docs/adr/0012-what-this-repository-is-mostly-made-of.md).

### Fixed

- The claims table had been six days stale while three green evidence runs over that very HEAD
  sat in branches nobody merged. The cron opened a pull request every Monday and exactly one of
  the four was ever closed.
- `unused`, a function whose own comment said it existed to keep an import meaningful, is gone
  with the import.

[0.1.0]: https://github.com/marcosmatalab/samegold/releases/tag/v0.1.0
