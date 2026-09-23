# ADR 0015 - the version has one source, and the changelog is held to it

**Status** accepted, 2026-09-23

## Context

At tag `v0.2.0` the version was written in three places by three hands: `pyproject.toml` and
`src/samegold/__init__.py` both said `0.1.0`, and `CHANGELOG.md` opened with `## [0.2.0]`.
Nothing compared them, so `pip show samegold` on a checkout of the 0.2.0 release reported 0.1.0,
and so did `samegold.__version__`. It is the defect this repository exists to find - a figure
typed twice, drifting - in the one place nobody had pointed a gate at.

## Decision

**`pyproject.toml` declares the version, and nothing else states it.** `samegold.__version__` is
read from the installed distribution with `importlib.metadata`, so it cannot disagree with what
`pip show` reports.

**The changelog is held to it by a test, not by a habit.**
`tests/fast/test_version.py` fails when the declared version is not the newest `## [x.y.z]`
heading of `CHANGELOG.md`, when the module assigns a literal instead of reading the metadata,
and when the installed distribution is not the declared version.

**Tags that already exist are not rewritten.** `v0.1.0` and `v0.2.0` keep the metadata they
shipped with; the next release is the first whose package reports its own number, and it is
`0.3.0`.

## Alternatives rejected

- **Derive the version from the git tag** (`hatch-vcs`, `setuptools-scm`). One source as well,
  and it ties the version to a tag that exists only after the release commit, so every commit
  between releases reports a synthetic `0.2.1.devN+g<sha>` and a clone without tags reports
  `0.0.0`. The number a reader sees would depend on how they fetched the repository.
- **Keep the literal in `__init__.py` and compare the two copies.** It catches the drift and
  keeps the duplication that caused it.

## Consequences

A version bump is one line in `pyproject.toml` plus a changelog section; the fast lane refuses
either without the other. The cost is on editable installs: the distribution records the
version when it is installed, so after a bump the environment reports the old number until it
is reinstalled with `make install` - and the third test says exactly that instead of letting
`pip show` quietly disagree with the checkout.
