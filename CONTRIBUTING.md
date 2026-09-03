# Contributing

## The one command to pass before a push

```sh
make preflight
```

That is the gate. It runs everything the `fast` and `spark` workflows run - the fast lane,
ruff, ruff format, mypy, `samegold check`, and both Spark jobs including the Delta lane - and
it **exits non-zero for a lane it could not run**, not only for a lane that failed.

That last property is the whole point of it, and it is there because of a specific failure
that happened twice. The command it replaced, `make ci-local`, ran the fast workflow and was
named as though it ran CI. A change under `tests/spark/` could pass it and arrive red in the
`spark` workflow, and did: round 12 found a Delta job that had been red in CI for two days
while `docs/limits.md` described the lane as unexecuted, and round 13 - the round that wrote
the ADR about that - pushed a red Spark lane of its own the same way.

So:

- `tests/fast/test_preflight.py` fails if a check CI runs is missing from
  `scripts/preflight.sh`, or if a new workflow is neither covered by the gate nor explicitly
  excluded from it with a reason.
- The gate deliberately does not run `evidence.yml` (an hour of compute, and it *writes* the
  evidence chain) or `databricks.yml` (needs an account and a token, and a run can spend a
  Free Edition account's daily quota). Those two are named in the script and in that test.

## The container, if you would rather not provision anything

```sh
docker build -f .devcontainer/Dockerfile -t samegold .
docker run --rm samegold                  # make demo
docker run --rm samegold make preflight   # the gate: fast + spark + delta
```

Python 3.11 and a Temurin 21 JDK, pinned, with the dependencies already installed into
`/opt/venv` - which is the `VENV` the Makefile reads, so nothing re-resolves on each run.
`.devcontainer/devcontainer.json` points VS Code at the same Dockerfile; nothing here needs
VS Code.

**Built and run**, on Windows 11 with Docker Desktop's WSL2 backend and Ubuntu 24.04, over a
domestic connection: **160.4 s** cold and **2.74 GB**, and `docker run --rm samegold make demo`
takes **0.4 s** because the venv is already resolved inside the image. The Dockerfile's header
carries the five-line breakdown and says where each number was taken, which matters more than
the numbers: the estimate it replaced was 2.6x too pessimistic on time and 37% too optimistic
on size, and one of its two "measured" figures had been measured on a GitHub runner rather than
here.

2.74 GB is a real download and it is stated rather than buried: the JDK and the pyspark wheel
are almost all of it, and both are why the image exists at all.

## If your machine cannot run the Spark lanes

**Native Windows cannot run Spark at all.** Hadoop's `NativeIO` calls `winutils.exe`, and
every file read fails with `UnsatisfiedLinkError`. Do not chase it. `make preflight` detects
this, says so in one sentence, and exits non-zero - it will not hand you a green tick for a
half-run.

Run the gate inside WSL2 or Linux with a JDK 21 instead. What that needs:

```sh
# inside WSL2, with the repository visible at /mnt/c/... or cloned locally
export JAVA_HOME=/path/to/temurin-21
export PATH="$JAVA_HOME/bin:$PATH"
make install-spark
make preflight
```

`docs/adr/0002-version-pinning.md` has the versions. The Spark and Delta lanes take
about seventy seconds each; the first Delta run also resolves jars from Maven Central.

## The lanes, individually

| command | what it needs | what it is for |
|---|---|---|
| `make demo` | nothing but Python 3.11 | ten seconds, one business finding |
| `make fast` | nothing but Python 3.11 | the fast lane: no JVM, no network, no credentials |
| `make lint` | nothing | ruff and mypy over `src`, `tests`, `databricks`, `pipelines` |
| `make spark` | a JVM | the Spark lane without Delta |
| `make delta` | a JVM and Maven Central | the full Spark + Delta lane |
| `make preflight` | a JVM | **all of the above, as CI runs it** |
| `make evidence` | nothing | regenerates the claims and the evidence chain |
| `make databricks` | a Free Edition workspace and two env vars | the one lane that needs an account |

## Two house rules that are not style

**A surviving mutant is a finding.** Never close one by calling it equivalent without an
entry in `mutation/equivalents.py` and a written rationale; the score is published both ways,
accepting the classification and refusing it. Most survivors so far were holes in the
generator, not in the harness.

**A number in a document belongs in an anchor.** `samegold readme` renders them from the
evidence and `samegold check` fails when they drift. A figure typed in by hand is a figure
that will be stale two commits later, because the seeds derive from the commit SHA.
