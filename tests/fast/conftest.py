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
