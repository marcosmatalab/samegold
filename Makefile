# samegold - make targets, ordered by how long they take.
#
# fast      no JVM, no network, no credentials.        ~15 s
# spark     adds a local Spark session (JVM).           ~70 s
# delta     adds the Delta jars from Maven Central.     ~2 min first time
# faults    the crash campaign.                         ~4 min for one repetition
# evidence  runs every claim and appends evidence.      ~90 s
#
# Every target is runnable by a stranger with a clone and a Python 3.11. Nothing here needs
# a Databricks account; the Databricks lane is a separate target and is clearly marked.

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

$(BIN)/python:
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -q -U pip
	$(BIN)/pip install -q -e ".[dev]"

.PHONY: install
install: $(BIN)/python ## create the virtualenv with the fast lane only

.PHONY: install-spark
install-spark: $(BIN)/python ## add pyspark 4.2.0 + delta-spark 4.4.0 (about 500 MB)
	$(BIN)/pip install -q -e ".[spark,rust,dev]"

.PHONY: demo
demo: install ## ten seconds, no credentials, one business finding
	$(BIN)/samegold demo

.PHONY: fast
fast: install ## the fast lane: no JVM, no network, no credentials
	$(BIN)/pytest tests/fast -q

.PHONY: spark
spark: install-spark ## the Spark lane without Delta (works with no route to Maven Central)
	SAMEGOLD_STORAGE=parquet $(BIN)/pytest tests/spark -q -m spark

.PHONY: delta
delta: install-spark ## the full Spark + Delta lane (needs Maven Central)
	$(BIN)/pytest tests/spark tests/delta -q

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

.PHONY: doctor
doctor: install ## what is installed and what each lane needs
	$(BIN)/samegold doctor

.PHONY: lint
lint: install ## ruff + mypy strict
	$(BIN)/ruff check src tests
	$(BIN)/ruff format --check src tests
	$(BIN)/mypy

.PHONY: all
all: fast lint check ## what CI runs on every push

.PHONY: clean
clean: ## remove build artefacts and scratch data (never touches evidence/)
	rm -rf .pytest_cache .ruff_cache .mypy_cache build dist *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
