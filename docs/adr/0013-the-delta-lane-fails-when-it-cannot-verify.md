# ADR 0013 - the Delta lane fails when it cannot reach the jars, instead of skipping

**Status** accepted, 2026-09-22

## Context

Measured on 20 September 2026 on a machine whose proxy blocked `repo1.maven.org`:

```
SPARK_LOCAL_IP=127.0.0.1 pytest tests/delta -q -rs
1 passed, 5 skipped in 3.49s
exit 0
```

with the skip reason
`no Spark session available: PySparkRuntimeError: [JAVA_GATEWAY_EXITED]`.

**That reason was false.** Spark worked perfectly on that machine: `pytest tests/spark` with
`SAMEGOLD_STORAGE=parquet` gave 143 passed in 212 s. The real cause was a hundred lines further
down the captured stderr:

```
Server access error at url https://repo1.maven.org/maven2/io/delta/delta-spark_4.2_2.13/4.4.0/...
  (java.io.IOException: Unable to tunnel through proxy. Proxy returns "HTTP/1.1 403 Forbidden")
Exception in thread "main" java.lang.RuntimeException:
  [unresolved dependency: io.delta#delta-spark_4.2_2.13;4.4.0: not found]
```

So the whole Delta lane exited 0 having verified nothing, under a diagnosis that named the
wrong component. Five skipped tests and exit 0 read as "the Delta claims hold" to everything
downstream - to a human, to `make preflight`, to a CI summary.

This repository's argument, made in `scripts/preflight.sh`'s own header and in most of
`FINDINGS.md`, is that a gate which does not bite is worse than no gate. This was one of its
own gates going green on zero verification, and it had been there the whole time.

There was an asymmetry that made it worse. The same missing jars made
`pytest tests/spark -q` (without `SAMEGOLD_STORAGE=parquet`) explode with **143 errors**, while
`pytest tests/delta -q` skipped in silence. `make preflight` then reported
`FAILED delta/spark`, which anyone behind a corporate proxy reads as "this repository is
broken". It was not broken; their network could not reach the jars. `preflight.sh` exists
precisely to tell "could not run" from "ran and failed", and this was the one place it got that
distinction backwards, in both directions at once.

## Decision

**`tests/delta/conftest.py` distinguishes three outcomes, and only one of them is a skip.**

| what is true | verdict | why |
|---|---|---|
| no JDK on PATH, or no pyspark | **skip** | a runtime this machine does not have is a lane that did not run, which is what a skip means |
| a JDK, pyspark, and no route to `repo1.maven.org` | **fail** | the claims this lane exists to verify have not been verified, and exit 0 would say they had |
| a Spark session already exists without the Delta extension | **skip**, with the fix | `getOrCreate` hands the parquet lane's session to this one; the answer is to run the lane on its own |

The reachability probe is a `urlopen` of `https://repo1.maven.org/maven2/io/delta/` with an
eight-second timeout, **not** a search of the exception text. The exception says
`JAVA_GATEWAY_EXITED` and carries no mention of ivy at all; the line that names the coordinate
is in the JVM's stderr, which pytest has already captured somewhere else. Parsing for
"unresolved dependency" would have been a guess that looked like a measurement.

**`scripts/preflight.sh` counts that case as "not run", not as "failed".** It guards the two
Delta-dependent lanes with the same reachability probe and puts one line in the `NOT RUN`
section naming the URL. `spark-no-delta` is deliberately outside that guard: it runs in parquet
mode and needs no network, so a machine with no route to Maven Central still gets a real answer
out of it.

The verdict is still non-zero. A lane this machine cannot execute is a lane this machine cannot
vouch for, and that rule does not move. What moves is the word, and on a diagnostic the word is
the entire content.

## Alternatives rejected

**Vendor the Delta jars in the repository.** About 30 MB of binaries in a 4.7 MB repository,
stale the moment the version moves, and precisely the kind of shortcut this repository spends
`FINDINGS.md` criticising elsewhere.

**Keep the skip and improve its message.** A better sentence attached to exit 0 changes nothing
about what consumes the exit code. The reason the old message was misleading is that it was
attached to a pass.

**Make the probe a hard precondition for the whole Spark lane.** It would stop
`spark-no-delta` running on an offline machine, and that lane verifies the transformations
against the DuckDB reference with no network at all. Narrowing the guard to the two lanes that
genuinely need the jars keeps the most useful offline result available.

## Consequences

On a network without a route to Maven Central, `pytest tests/delta` now fails with a message
whose first line says Maven Central is unreachable and that the Delta claims have NOT been
verified, and whose last line carries what the session actually said. `make preflight` reports
`0 failed, N not run` with the URL in the reason, instead of `FAILED delta/spark` and 143
errors.

A CI runner reaches Maven Central, so `spark.yml` is unaffected. The case this changes is a
developer behind a proxy, which is the case the old behaviour handled worst and reported best.
