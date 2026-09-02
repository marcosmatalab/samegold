"""Shared fixtures for the fast lane.

The mutation campaign is the expensive thing in this lane (about fifteen seconds), and three
tests need one. Running it once per session keeps `make fast` under half a minute, which is
the difference between a lane people run and a lane people skip.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from samegold.generator.events import FAST, generate
from samegold.mutation.runner import MutationRun, run_mutation_campaign

REFERENCE = Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"


@pytest.fixture(scope="session")
def campaign(tmp_path_factory: pytest.TempPathFactory) -> MutationRun:
    root = tmp_path_factory.mktemp("campaign")
    result = generate(root / "g", seed=42, profile=FAST)
    ledger = json.loads((root / "g" / "truth" / "ledger.json").read_text(encoding="utf-8"))
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    return run_mutation_campaign(
        REFERENCE.read_text(encoding="utf-8"), root / "g" / "bronze", ledger, closes
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """The fast lane must finish without ever having imported Spark. Checked at the END.

    `tests/fast/test_architecture.py::test_the_fast_lane_does_not_need_pyspark` asks the same
    question, but it asks it AT THE MOMENT IT RUNS, so whether it notices depends on the order
    the tests happen to execute in. It did not notice a test added in round 17 that called
    `bronze_schema()` - which builds a pyspark StructType - because the machines it was
    written on both have the spark extras installed, and the check happened to run first. The
    `fast` workflow, which installs `.[dev]` and no Spark, went red on the push.

    This hook runs once, after everything, so no ordering can hide it. It is a session
    finaliser rather than a test because the property is about the whole session: "nothing in
    this lane touched Spark", not "nothing had touched it by the time I looked".
    """
    import sys

    heavy = sorted(
        name
        for name in ("pyspark", "delta", "py4j")
        if name in sys.modules or any(m.startswith(f"{name}.") for m in sys.modules)
    )
    if not heavy:
        return
    session.exitstatus = 1
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    message = (
        f"the fast lane imported {heavy}. That lane's whole promise is that it runs with no "
        f"JVM, no Spark and no network, and the `fast` workflow installs `.[dev]`, which has "
        f"none of them - so an import that works here fails there. Read the declaration "
        f"instead of executing it, or move the test to tests/spark."
    )
    if reporter is not None:
        reporter.write_sep("=", "fast lane dependency violation", red=True)
        reporter.write_line(message)
    else:  # pragma: no cover - only when the terminal plugin is disabled
        print(message)
