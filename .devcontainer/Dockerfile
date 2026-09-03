# A clean-room for the OSS lanes: Python 3.11 and a JDK 21, so that a clone plus one build
# runs `make demo`, `make fast`, `make spark` and `make delta` with nothing installed on the
# host.
#
# It exists because getting there by hand cost two afternoons. Native Windows cannot run Spark
# at all - Hadoop's NativeIO calls winutils.exe and every file read raises UnsatisfiedLinkError
# - so the lanes needed WSL2, a hand-provisioned Temurin, a separate venv, and three
# environment variables, none of which was written down anywhere a stranger would find.
#
#   docker build -f .devcontainer/Dockerfile -t samegold .
#   docker run --rm samegold                  # make demo
#   docker run --rm samegold make fast
#   docker run --rm samegold make preflight   # the gate: fast + spark + delta
#
# It is a plain Dockerfile on purpose. VS Code's devcontainer.json points at it, but nothing
# here needs VS Code, and a container that only works inside one editor is a container that
# does not work.
#
# TIME AND SIZE BUDGET, with the provenance of each number, because a budget nobody can check
# is a wish:
#
#   ~4 min   building the pyspark 4.2.0 wheel. MEASURED, and it is why the spark workflow
#            caches it: pyspark publishes an sdist, not a wheel (see .github/workflows/spark.yml).
#   ~450 MB  that same wheel. Measured, same place.
#   ~1 min   base images and the JDK, over a normal connection. NOT measured - an estimate
#            from the published image sizes.
#   ~2 min   the remaining dependencies. NOT measured.
#
# So: roughly seven minutes and about 2 GB for a cold build, of which the four measured
# minutes dominate. The whole figure has NOT been verified by a build - see the note at the
# bottom of this file - and the first person to run it should replace this paragraph with what
# it actually took.

# The JDK comes from Temurin's own image rather than from apt, which on bookworm would give
# JDK 17. ADR 0002 pins 21 for the Spark lane. The patch level is deliberately not pinned:
# pinning a tag that was never built is how you ship a Dockerfile that cannot build at all,
# and this one has not been built here. Pin it once a build has succeeded.
FROM eclipse-temurin:21-jdk-jammy AS jdk

FROM python:3.11-slim-bookworm

COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

# `git` is not a convenience. Every seed in this repository derives from the commit SHA, and
# `samegold` reads it with `git rev-parse`; without git in the image the demo has no seed and
# the evidence records have no anchor. `make` runs the lanes; `procps` is what Spark's scripts
# use to find their own processes.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git make procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

# The venv lives OUTSIDE the workspace so that mounting a working copy over /workspace (which
# is what devcontainer.json does) does not hide it. `VENV` is the variable the Makefile already
# reads, so `make demo` finds `$(VENV)/bin/python` present and skips the install step entirely
# rather than re-resolving 450 MB on every run. Verified: `make -n demo VENV=...` emits only
# the demo command.
ENV VENV=/opt/venv
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir -U pip \
 && /opt/venv/bin/pip install --no-cache-dir -e ".[spark,rust,dev]"

# Spark binds to a hostname the container cannot resolve otherwise, and UTF-8 mode makes the
# document readers behave the same here as on a Linux CI runner.
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONUTF8=1
# The Delta lane resolves jars into this directory; naming it keeps them inside the container
# rather than in a home directory that may be mounted from elsewhere.
ENV SAMEGOLD_IVY_HOME=/opt/ivy

CMD ["make", "demo"]

# NOT BUILT HERE. There was no Docker engine available on the machine this was written on -
# Docker Desktop was not running and WSL integration was off - so this file has been reviewed
# and not executed, which is the exact category this repository spends its rounds finding.
# `docs/limits.md` says so too. What HAS been checked: that `make demo VENV=<existing venv>`
# runs the demo without reinstalling, which is the one mechanism the image depends on.
