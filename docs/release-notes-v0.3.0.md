# samegold v0.3.0

The release in which the front pages stopped being able to say something the repository does
not check. Four defects, found on 23 September 2026 by installing and running a clean clone of
`cf7bf5a` end to end, each fixed at its cause and each held shut by a test that failed before
the fix.

[v0.1.0](https://github.com/marcosmatalab/samegold/blob/v0.3.0/docs/release-notes-v0.1.0.md) is
the description of what this project is. This page is what changed.

## Fixed

- **The package reports its own version.** At tag `v0.2.0` the package still said `0.1.0`, in
  `pyproject.toml`, in `samegold.__version__` and in `pip show`. The version now has one
  source, `pyproject.toml`; `__version__` reads it through `importlib.metadata`; and
  `tests/fast/test_version.py` fails if the declared version, the installed one and the newest
  release in `CHANGELOG.md` disagree. The existing tags are not rewritten, so this is the first
  release whose metadata carries its own number.
  [ADR 0015](https://github.com/marcosmatalab/samegold/blob/v0.3.0/docs/adr/0015-the-version-has-one-source.md).
- **A figure edited by hand on a front page fails `samegold check`.** It used to pass for the
  Databricks row, for January's closed versions, for the return window, for the two numbers
  beside the GIF and for the Databricks record itself. Now:
  - every `dbx:` anchor is compared with the record;
  - the record and its capture are pinned by content digest, without joining the chain;
  - the return window and the GIF's figures are rendered from the contract, the GIF and the
    Makefile;
  - the stack badges are rendered from `pyproject.toml`;
  - any other number in the prose of either page is reported with its line.

  [ADR 0016](https://github.com/marcosmatalab/samegold/blob/v0.3.0/docs/adr/0016-every-front-page-figure-has-a-source.md).
- **The demo block is what `make demo` prints**, byte for byte. The demo used a seed derived from
  the commit and its block came from an older evidence record, so the page showed another
  commit's output. The demo now uses a fixed seed, and `samegold readme` renders the block by
  running it. `samegold check` runs it again and compares. The claims keep their commit-derived
  seeds.
  [ADR 0017](https://github.com/marcosmatalab/samegold/blob/v0.3.0/docs/adr/0017-the-demo-has-a-fixed-seed.md).
- **The Databricks badge measures something.** It pointed at a workflow that runs only when
  dispatched by hand and does not start the job. It now points at `databricks-evidence.yml`,
  which runs on every push and pull request and verifies the committed Databricks evidence
  offline: the pinned record, the figures that quote it, and every closed version recomputed to
  the cent on DuckDB.
  [ADR 0018](https://github.com/marcosmatalab/samegold/blob/v0.3.0/docs/adr/0018-the-databricks-badge-verifies-the-committed-evidence.md).

## Install

`samegold` is not published to PyPI. Install it from the tag:

```bash
pip install "git+https://github.com/marcosmatalab/samegold@v0.3.0"
```

or clone the tag and run `make install`, which is what the README's first section does.

The full list is in the
[changelog](https://github.com/marcosmatalab/samegold/blob/v0.3.0/CHANGELOG.md).
