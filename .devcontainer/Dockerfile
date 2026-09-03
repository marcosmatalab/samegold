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
# TIME AND SIZE, MEASURED. This paragraph used to be a budget with two estimates in it and a
# note saying nobody had built the image. It has been built, and every line below is what that
# build did:
#
#   160.4 s  `docker build -f .devcontainer/Dockerfile -t samegold .`, cold, of which
#              5.1 s  pulling python:3.11-slim-bookworm
#              9.5 s  pulling eclipse-temurin:21-jdk-jammy
#             16.2 s  apt-get: git, make, procps
#             82.9 s  venv + `pip install -e ".[spark,rust,dev]"`
#             57.3 s  export and unpack
#            The five lines add up to 171.0 s and the build took 160.4: BuildKit pulls the two
#            base images concurrently, so they overlap rather than sum.
#     2.74 GB  `docker images samegold`.
#      0.4 s  `docker run --rm samegold make demo`, which is the `CMD` below.
#
# WHERE, because that is half of every measurement here: Windows 11, Docker Desktop with the
# WSL2 backend, Ubuntu 24.04, a domestic connection. A different machine will move the pulls
# and the export; the 82.9 s is dependency resolution and compilation and will move least.
#
# The estimate this replaces said ~7 min and ~2 GB. The time was 2.6x too pessimistic and the
# size 37% too optimistic, and the interesting error is the third one: "~4 min building the
# pyspark 4.2.0 wheel. MEASURED" was a real measurement taken on a GitHub runner in
# .github/workflows/spark.yml, and the whole venv step here took 82.9 s. A number measured
# somewhere else is an estimate here, however carefully it was measured there.
#
# WHY 2.74 GB, since a contributor on a slow line deserves to know what is arriving: the JDK
# and the pyspark wheel are almost all of it. Both are load-bearing - the Spark and Delta lanes
# are the point of this image - so the size is reported rather than apologised for. The obvious
# way to reduce it, NOT taken here: a second stage that copies only `/opt/venv`, `/opt/java` and
# the workspace out of the build, which would drop the apt lists and pip's build tree. That is a
# decision to make after measuring what it actually saves, not before.

# The JDK comes from Temurin's own image rather than from apt, which on bookworm would give
# JDK 17. ADR 0002 pins 21 for the Spark lane. The patch level is still a floating tag, and the
# reason has changed: it used to be that pinning a tag nobody had resolved is how you ship a
# Dockerfile that cannot build. The build has happened and both floating tags resolved without
# incident, so a digest pin is now available and is a decision nobody has made - it buys a
# reproducible image and costs the security updates that come with the moving tag.
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

# BUILT AND RUN. `docker build` and `docker run --rm samegold make demo` both succeeded; the
# numbers are at the top of this file. The mechanism the image depends on - `make demo` finding
# `$(VENV)/bin/python` already present and skipping the install - is what the 0.4 s shows:
# resolving the dependencies again would have taken the 82.9 s the build spent on them.
