"""CONTRACT.md and domain/contract.py must not disagree.

A contract that lives in two places drifts. Here the document is the human-readable half and
the module is the machine-readable one, and this test is the join between them.
"""

from __future__ import annotations

from pathlib import Path

from samegold.domain.contract import (
    ACCOUNTING_TIMEZONE,
    CONTRACT_VERSION,
    RETURN_WINDOW_DAYS,
    QuarantineReason,
)

CONTRACT = (Path(__file__).resolve().parents[2] / "CONTRACT.md").read_text(encoding="utf-8")


def test_the_version_matches() -> None:
    assert f"Version {CONTRACT_VERSION}" in CONTRACT


def test_the_window_matches() -> None:
    assert f"{RETURN_WINDOW_DAYS} days" in CONTRACT


def test_the_timezone_matches() -> None:
    assert ACCOUNTING_TIMEZONE in CONTRACT


def test_the_sql_reference_enforces_the_same_window() -> None:
    sql = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"
    ).read_text(encoding="utf-8")
    assert f"INTERVAL {RETURN_WINDOW_DAYS} DAY" in sql


def test_every_quarantine_reason_is_reachable_in_the_spark_rules() -> None:
    """A reason nobody can produce is a reason nobody maintains."""
    source = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "pipelines" / "transform.py"
    ).read_text(encoding="utf-8")
    generator = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "generator" / "events.py"
    ).read_text(encoding="utf-8")
    for reason in QuarantineReason:
        assert str(reason) in source or reason.name in generator, (
            f"{reason} is declared in the contract but no implementation can emit it"
        )


def test_the_sql_reference_enforces_the_window_in_seconds() -> None:
    """The previous version of this test asserted `INTERVAL 45 DAY in sql`, which was true
    only because the phrase survived in the COMMENT that explains why it is no longer used.
    A test that passes on a comment is a test that stopped watching."""
    sql = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"
    ).read_text(encoding="utf-8")
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert f"{RETURN_WINDOW_DAYS} * 86400" in code
    assert "INTERVAL 45 DAY" not in code


def test_the_spark_side_enforces_the_same_window_in_seconds() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "pipelines" / "transform.py"
    ).read_text(encoding="utf-8")
    assert "RETURN_WINDOW_DAYS * 86400" in source
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    assert "unix_timestamp" not in code, (
        "unix_timestamp truncates to whole seconds and accepted a return one microsecond "
        "outside the window; the two implementations disagreed on exactly that row"
    )


def test_the_delta_coordinate_lives_in_one_place() -> None:
    """ADR 0002 claims there is a single constant and a test that enforces it. This is it.

    The claim was false when it was written: the coordinate was also spelled out in the
    declarative pipeline spec, and nothing checked that the two agreed.
    """
    from samegold.pipelines.session import DELTA_COORDINATE

    root = Path(__file__).resolve().parents[2]
    spec = (root / "pipelines" / "spark-pipeline.yml").read_text(encoding="utf-8")
    assert DELTA_COORDINATE in spec, (
        "the declarative pipeline spec must use the same coordinate as pipelines/session.py"
    )
    sources = [
        path
        for path in (root / "src").rglob("*.py")
        if "__pycache__" not in str(path) and path.name != "session.py"
    ]
    for path in sources:
        assert "io.delta:delta-spark" not in path.read_text(encoding="utf-8"), (
            f"{path.relative_to(root)} spells out the Delta coordinate; it belongs in "
            f"pipelines/session.py alone"
        )


def test_the_declarative_pipeline_spec_is_consistent_with_the_code() -> None:
    """The SDP spec is a file nothing executes in this container, which is exactly the kind of
    file that rots. These are the properties that can be checked without a JVM."""
    import yaml

    root = Path(__file__).resolve().parents[2]
    spec = yaml.safe_load((root / "pipelines" / "spark-pipeline.yml").read_text(encoding="utf-8"))
    assert spec["name"] == "samegold"
    configuration = spec["configuration"]
    # Without these three, open-source SDP writes Parquet and every Delta-specific claim in
    # this repository has nothing to work with.
    assert configuration["spark.sql.sources.default"] == "delta"
    assert "DeltaSparkSessionExtension" in configuration["spark.sql.extensions"]
    assert "DeltaCatalog" in configuration["spark.sql.catalog.spark_catalog"]
    transformations = root / "pipelines" / "transformations"
    assert list(transformations.glob("*.py")), "the spec globs transformations/** and it is empty"
