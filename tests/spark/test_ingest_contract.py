"""The contract both ingestors must satisfy, run against whichever one is available.

Auto Loader only exists inside Databricks, so this file checks the file-source implementation
here and the same test runs against Auto Loader on the Databricks lane. What it asserts is
exactly the shared part: the declared schema, and that nothing is dropped.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from samegold.generator.events import FAST, generate
from samegold.ingest.adapter import INGESTORS, IngestSpec

pytestmark = pytest.mark.spark


def test_the_file_source_ingestor_preserves_every_line(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    result = generate(tmp_path / "g", seed=21, profile=FAST)
    raw_lines = sum(
        len([line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()])
        for path in (tmp_path / "g" / "bronze").rglob("*.json")
    )
    ingestor = INGESTORS["file-source"]
    stream = ingestor.read_stream(spark, IngestSpec(path=str(tmp_path / "g" / "bronze")))
    out = tmp_path / "out"
    query = (
        stream.writeStream.format("parquet")
        .option("path", str(out / "data"))
        .option("checkpointLocation", str(out / "_ck"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    ingested = spark.read.parquet(str(out / "data")).count()
    assert ingested == raw_lines, (
        f"the ingestor lost {raw_lines - ingested} of {raw_lines} lines; a record that "
        f"disappears without a counter is the failure nobody detects"
    )
    assert result.event_count == raw_lines


def test_the_two_ingestors_declare_their_differences() -> None:
    """The differences are part of the code, not part of a README nobody re-reads."""
    for name in ("file-source", "auto-loader"):
        assert INGESTORS[name].differences, f"{name} claims to differ from nothing"
    assert INGESTORS["file-source"].differences != INGESTORS["auto-loader"].differences
