# samegold - make targets, ordered by how long they take.
#
# fast      no JVM, no network, no credentials.        ~45 s
# spark     adds a local Spark session (JVM).           ~2 min
# delta     adds the Delta jars from Maven Central.     ~3 min first time
# faults    the crash campaign, ten repetitions.        ~20 min
# evidence  every claim except SG-07.                   ~2 min
# databricks  a Free Edition workspace and two env vars.  ~6 min for the whole job, measured
#             on run 592180158314216 (5 September 2026): 81s of pipeline, 51s of close, and
#             four short notebook tasks. It can spend that account's compute quota for the
#             rest of the day - see docs/databricks-run.md.
#
# The durations are the ones measured on the machine that wrote this file, not a target.
# `make doctor` prints what the fast lane actually took on yours.
#
# Every target here is runnable by a stranger with a clone and a Python 3.11, with exactly
# two exceptions, and they are named rather than left to be discovered: `databricks` and
# `databricks-validate` need an account, a network and credentials.

PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
PROFILE ?= ci
SEED ?= 20260901

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# A STAMP, AND NOT THE INTERPRETER, and the difference cost this round a red preflight.
#
# This rule used to be `$(BIN)/python:` - a file target that exists the moment the venv does. So
# once anybody had a venv, `install` was a no-op FOR EVER: adding a dependency to pyproject.toml
# changed nothing, and every target that depends on `install` went on running against an
# environment built before the dependency existed.
#
# Measured on 7 September 2026: `pytest-cov` was added to the `dev` extra and the fast lane's
# command grew `--cov`, `make preflight` ran `install` first exactly as it is supposed to, and
# the WSL2 clone still failed with `unrecognized arguments: --cov` because its venv was older
# than the declaration. A dependency declared and not installed is the same class as every other
# declaration in this repository that did not govern - and this one had the shortest possible
# feedback loop between the declaration and the failure, which is why it was worth fixing rather
# than working around with a manual `pip install`.
#
# The stamp depends on `pyproject.toml`, so the dependency list is what decides whether the
# environment is stale.
$(VENV)/.installed: pyproject.toml
	test -x $(BIN)/python || $(PY) -m venv $(VENV)
	$(BIN)/pip install -q -U pip
	$(BIN)/pip install -q -e ".[dev]"
	@touch $@

.PHONY: install
install: $(VENV)/.installed ## create the virtualenv with the fast lane only

.PHONY: install-spark
install-spark: $(VENV)/.installed ## add pyspark 4.2.0 + delta-spark 4.4.0 (about 500 MB)
	$(BIN)/pip install -q -e ".[spark,rust,dev]"

.PHONY: demo
demo: install ## 0.4 s in the container / 4.0 s on Windows, once installed; one finding
	@# Silent, so stdout is the program's alone and matches the README block byte for byte.
	@$(BIN)/samegold demo

.PHONY: fast
fast: install ## the fast lane: no JVM, no network, no credentials
	$(BIN)/pytest tests/fast -q --cov=src/samegold --cov-report=term-missing:skip-covered --cov-fail-under=65

.PHONY: spark
spark: install-spark ## the Spark lane without Delta (works with no route to Maven Central)
	SAMEGOLD_STORAGE=parquet $(BIN)/pytest tests/spark -q -m spark

.PHONY: delta
delta: install-spark ## the full Spark + Delta lane (needs Maven Central)
	# Separate processes on purpose: a Spark session is a per-process singleton, so a parquet
	# session created by the spark lane would be handed to the delta lane by getOrCreate.
	$(BIN)/pytest tests/spark -q -m spark
	$(BIN)/pytest tests/delta -q

.PHONY: faults
faults: install-spark ## the crash campaign, with its negative control (about 8 minutes)
	SAMEGOLD_STORAGE=parquet $(BIN)/samegold evidence --claims SG-07 --repetitions 10

.PHONY: cost
cost: install ## the layout experiments: compaction, clustering, partitioning, delete cost
	$(BIN)/samegold evidence --claims SG-09

.PHONY: privacy
privacy: install ## masking, the exposure check and a retention purge that really purges
	$(BIN)/samegold evidence --claims SG-08

.PHONY: evidence
evidence: install ## run every claim except the crash campaign (which needs a JVM)
	$(BIN)/samegold evidence --profile $(PROFILE)

.PHONY: evidence-full
evidence-full: install-spark ## every claim including SG-07; about fifteen minutes
	$(BIN)/samegold evidence --profile $(PROFILE)
	SAMEGOLD_STORAGE=parquet $(BIN)/samegold evidence --claims SG-07 --repetitions 10

.PHONY: readme
readme: install ## render README.md and CLAIMS.md from the evidence
	$(BIN)/samegold readme

.PHONY: check
check: install ## fail if the documents and the evidence disagree
	$(BIN)/samegold check

.PHONY: refute
refute: install ## run every claim with a seed of your choosing: make refute SEED=12345
	$(BIN)/samegold refute --seed $(SEED) --profile ci

.PHONY: skew
skew: ## measure what a skewed join does, and print the table docs/join-skew.md publishes
	# NOT a claim, and docs/join-skew.md says why: with adaptive execution on - which
	# ADR 0005 requires - two million rows coalesce into one shuffle partition, and one
	# partition has no skew. The only configuration where the thing exists is the one that
	# ADR refuses to test, so this is run by hand and the page reports it as a measurement
	# rather than as evidence.
	#
	# Needs pyspark and a JVM: same machine as the Spark lanes, so WSL2 or Linux.
	$(BIN)/python scripts/measure_join_skew.py

.PHONY: gif
gif: ## re-record docs/img/refute.gif by RUNNING make refute, not by drawing it
	# `vhs` drives a real terminal and films what comes out, so the GIF on the front page is
	# an execution. It needs vhs, ttyd and ffmpeg, which are NOT dependencies of this project
	# and are not installed by `make install`: nothing in CI runs this target, the artifact is
	# committed, and this is how it is redone when `make refute` starts printing something
	# else. Recorded on WSL2; the Spark lanes want the same machine.
	vhs docs/refute.tape
	# And then the PLAYBACK is sped up, which is the only thing here that is not real time.
	# The run takes 73,1 s and a front page is closed long before that; 4x puts it at 18,3 s.
	# `setpts` changes when the frames are shown and not what is in them, the tape is
	# untouched, and BOTH numbers are printed beside the image - so a reader is told the
	# acceleration rather than left to assume the program is fast.
	# Neither number is typed on the front pages: `make readme` renders both from the frame
	# delays of the GIF and from the `setpts=PTS/N` on the next line, and `samegold check` fails
	# when either stops matching. Change the divisor here and the pages follow on `make readme`.
	ffmpeg -v error -i docs/img/refute.gif -filter_complex "[0:v]setpts=PTS/4,fps=15,split[a][b];[a]palettegen=max_colors=256[p];[b][p]paletteuse=dither=none" -y docs/img/.refute-fast.gif
	mv docs/img/.refute-fast.gif docs/img/refute.gif

.PHONY: report
report: install ## render the close as one self-contained HTML page
	$(BIN)/samegold report --out close-report.html

.PHONY: preflight
preflight: install ## THE ONE TO PASS BEFORE A PUSH: everything the fast and spark workflows run
	# This replaces `ci-local`, which ran the FAST workflow and was named as though it ran
	# CI. A change under tests/spark/ could pass it and arrive red in the spark workflow,
	# and that happened twice: round 12 found a Delta job that had been red for two days,
	# and round 13 - the round that wrote the ADR about it - pushed a red Spark lane of its
	# own. See docs/adr/0006-mutants-are-generated-not-planted.md.
	#
	# It exits NON-ZERO for a lane it could not run, not only for a lane that failed. On
	# native Windows the Spark lanes cannot run at all, and it says that in one sentence
	# instead of printing a green half-run.
	SAMEGOLD_BIN=$(BIN) scripts/preflight.sh

.PHONY: doctor
doctor: install ## what is installed and what each lane needs
	$(BIN)/samegold doctor

.PHONY: databricks
databricks: install ## deploy the Databricks lane to a Free Edition workspace and fetch its evidence
	# The only target here that needs an account. It reads DATABRICKS_HOST and
	# DATABRICKS_TOKEN from the environment and tells you which one is missing rather than
	# handing you a stack trace from a CLI. What it does, in order: create the catalog (a
	# bundle cannot), validate, deploy, seed the landing volume with generated events, run
	# the job, and copy the SG-DBX-01 record into evidence/databricks/.
	SAMEGOLD_BIN=$(BIN) scripts/databricks_run.sh all

.PHONY: databricks-validate
databricks-validate: ## `databricks bundle validate -t free`, without deploying anything
	SAMEGOLD_BIN=$(BIN) scripts/databricks_run.sh validate

.PHONY: lint
lint: install ## ruff + mypy strict, over every directory that holds code
	# `databricks` and `pipelines` are in the list because for ten rounds they were not, and
	# "All checks passed" was reporting the scope of the command. See docs/adr/0006-mutants-are-generated-not-planted.md.
	$(BIN)/ruff check src tests databricks pipelines
	$(BIN)/ruff format --check src tests databricks pipelines
	$(BIN)/mypy

.PHONY: all
all: preflight ## an alias for preflight, which is the gate

.PHONY: clean
clean: ## remove build artefacts and scratch data (never touches evidence/)
	rm -rf .pytest_cache .ruff_cache .mypy_cache build dist *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
