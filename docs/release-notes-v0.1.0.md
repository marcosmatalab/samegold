# samegold v0.1.0

A bitemporal month-end close on Delta Lake and Spark, computed three times by three engines
that have to agree on one canonical digest. Around it, a harness whose only job is to prove the
close wrong, and an append-only hash chain holding every figure it published and every time one
of them turned out to be false. No account, no credentials and no network beyond PyPI are
needed to check any of it.

## The three numbers on the front page, measured

- **Ten claims, all passing, and eight of the ten recomputed from the seeds their own records
  name.** `samegold verify-latest` re-runs a claim from its record rather than validating the
  record's shape, which is the difference that matters: an adversarial review had published a
  perfect mutation score by appending one well-formed record with real seeds and a hash from
  this repository's own hasher, and every check in the repository passed on it. The two claims
  it does not recompute by default cost minutes of runtime, not honesty, and
  [ADR 0011](adr/0011-the-gate-recomputes-the-record.md) names them.
- **694 tests in the fast lane: 175 that exercise the pipeline and 519 that check this
  repository's own claims about itself.** Zero test files are in neither class, because a file
  nobody has classified fails the lane rather than being assigned a side. The lane needs no
  JVM and no credentials.
- **24.8% of the code is Spark, Delta and Databricks; 75.2% is the harness** - 7 567 lines
  against 22 911. That ratio is the point rather than an accident. Anybody can write a
  month-end close; knowing whether the one you wrote is right is the work.

## What is in this version

- **`samegold verify-latest`**, the gate that recomputes a published record instead of
  validating it, running inside the evidence workflow before anything is pushed.
- **The evidence chain**: 223 records, append-only and hash-chained, with every seed derived
  from a commit sha so that a published number can be recomputed at the commit that published
  it. `make refute SEED=...` runs every claim about the data on a seed nobody chose, and the
  chain refuses the result as evidence.
- **Three engines**: the PySpark pipeline, a DuckDB reference implementation and a
  by-construction ledger, compared on the whole versioned close.
- **The Databricks lane**, deployed and run end to end against a Free Edition workspace, whose
  records are committed and whose figures are rendered into the documents rather than
  screenshotted. 126 tests drive the bundle from a clone against a stub CLI.
- **Drift gates on the prose, not only on the figures**: four rules that fail the lane when a
  sentence, an exhaustive enumeration, a "this has never run" or an accepted ADR describes
  something the tree does not do. Two of the fourteen ADRs were caught lying by the fourth.
- **Two front pages, English and Spanish, both rendered from the same records**, with a gate
  that fails when they stop carrying the same figures, commands, links and assertions.
- **A recorded GIF**, not a drawn one: `docs/refute.tape` is what `vhs` executes and `make gif`
  regenerates.

## What is NOT in this version

- **The Databricks workspace is not reproducible from a clone.** The bundle is checked here;
  the workspace is not, and those are different claims. The records under
  `evidence/databricks/` say so themselves, with `"chain": {"chained": false}`.
- **Nothing here has run on anything but x86_64.** The chain holds records from four platforms
  - Windows 11, WSL2, another Linux and the `ubuntu-latest` runner - and four Python versions,
  and eight of the ten claims were measured on Windows and on the runner and gave the same
  rate. The two that did not are `SG-00` and `SG-06`, which count this repository's tests and
  this chain's records rather than anything about the data, and both of those denominators are
  a property of the machine by definition. What is missing is a different ARCHITECTURE, and one
  DuckDB version appears in every record in the chain.

  An earlier draft of this bullet said no figure here had been measured on more than one
  machine. It was false when it was written, and the chain it was written beside says so in
  four `environment.platform` values. It is in `docs/findings/`.
- **The Spark and Delta lanes do not run on Windows.** They need WSL2 or Linux; `make doctor`
  says what a given machine can run, and `make preflight` refuses to exit 0 on one that cannot.
- **No performance claim of any kind.** Nothing here has been benchmarked against anything,
  and `SG-09` measures what layout costs in files and bytes, not in time.
- **This is not published to PyPI.** "Version" means the state of the repository at a tag.

[**`docs/limits.md`**](limits.md) is the full list: what this repository has verified by running
it, what it has written and not executed, and what a reader should distrust. It is deliberately
longer than this page.

---

The figures above are the ones measured at the tagged commit, and this is the one document here
whose numbers are NOT rendered from the evidence chain: a release note describes a release, so
it is frozen on purpose. Every figure in it is anchored and live on the front page, and if the
two ever disagree, the front page is the one that is right.
