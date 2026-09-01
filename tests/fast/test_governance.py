"""Anonymisation, classification and purging, executed rather than declared."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from samegold.governance.anonymise import (
    detokenise,
    generalize_country,
    generalize_timestamp,
    k_anonymity,
    pseudonymise,
    suppress,
    tokenise,
)
from samegold.governance.policy import Classification, apply_policy, check_gold_exposure


def test_pseudonymisation_is_deterministic_and_salt_dependent() -> None:
    assert pseudonymise("C000001", "salt") == pseudonymise("C000001", "salt")
    assert pseudonymise("C000001", "salt") != pseudonymise("C000001", "other-salt")
    assert pseudonymise("C000001", "salt") != "C000001"


def test_pseudonymisation_refuses_to_run_without_a_salt() -> None:
    """An unsalted hash of a low-cardinality identifier is the identifier with extra steps."""
    with pytest.raises(ValueError, match="needs a salt"):
        pseudonymise("C000001", "")


def test_tokenisation_is_reversible_only_with_the_vault() -> None:
    vault: dict[str, str] = {}
    token = tokenise("C000001", key=b"k", vault=vault)
    assert token != "C000001"
    assert detokenise(token, vault) == "C000001"
    assert detokenise(token, {}) is None


def test_tokenisation_is_keyed_not_just_hashed() -> None:
    assert tokenise("C1", key=b"a") != tokenise("C1", key=b"b")


def test_generalisation_keeps_utility_and_loses_precision() -> None:
    assert generalize_country("ES") == generalize_country("PT") == "Iberia"
    assert generalize_country(None) == "Other"
    stamp = dt.datetime(2026, 3, 17, 4, 30, tzinfo=dt.UTC)
    assert generalize_timestamp(stamp) == "2026-03"
    assert generalize_timestamp(stamp, "day") == "2026-03-17"


def test_k_anonymity_reports_the_smallest_bucket() -> None:
    """Generalising into a bucket with one member in it anonymises nothing."""
    assert k_anonymity({"Iberia": 40, "Western Europe": 1}) == 1


def test_suppression_removes_the_value_entirely() -> None:
    assert suppress("anything") == "[suppressed]"


def test_the_policy_masks_a_direct_identifier_on_the_way_into_gold() -> None:
    row = {"customer_id": "C000042", "country": "PT", "segment": "vip", "sku": "SKU-1"}
    masked = apply_policy(row, salt="pepper")
    assert "customer_id" not in masked
    assert masked["customer_id_pseudonym"] != "C000042"
    assert masked["country_region"] == "Iberia"
    assert masked["segment"] == "vip"  # an attribute, kept, and the policy says why


def test_the_exposure_check_catches_an_identifier_that_reached_gold() -> None:
    violations = check_gold_exposure([{"customer_id": "C000042", "net_cents": 1}])
    assert violations and violations[0]["kind"] == "direct_identifier_in_gold"


def test_the_exposure_check_catches_an_identifier_hiding_under_another_name() -> None:
    """The check that earns its keep: masking is applied by someone who remembers to apply it,
    but a new column carrying the same value under a new name fails the build anyway."""
    violations = check_gold_exposure([{"customer_ref": "C000042", "net_cents": 1}])
    assert violations and violations[0]["kind"] == "identifier_shaped_value_in_gold"


def test_masked_gold_passes_the_exposure_check() -> None:
    rows = [apply_policy({"customer_id": f"C{i:06d}", "country": "ES"}, "pepper") for i in range(5)]
    assert check_gold_exposure(rows) == []


def test_every_column_in_the_policy_declares_a_rationale() -> None:
    from samegold.governance.policy import COLUMN_POLICY

    for policy in COLUMN_POLICY:
        assert policy.rationale, f"{policy.column} is classified with no reason given"
        assert policy.classification in set(Classification)


def test_purging_removes_the_rows_and_the_files_that_held_them(tmp_path: Path) -> None:
    """A DELETE on a lakehouse does not delete: time travel still returns the rows until the
    files are vacuumed. The purge has to do both, and this test fails if it stops at the
    DELETE."""
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from samegold.governance.retention import purge_expired

    table_uri = str(tmp_path / "events")
    rows = pa.table(
        {
            "event_id": [f"e{i}" for i in range(100)],
            "event_day": [f"2026-0{(i % 3) + 1}-01" for i in range(100)],
        }
    )
    write_deltalake(table_uri, rows, mode="overwrite")
    report = purge_expired(
        table_uri, "event_day", retention_days=60, now=dt.datetime(2026, 4, 15, tzinfo=dt.UTC)
    )
    assert report["rows_deleted"] > 0
    remaining = DeltaTable(table_uri).to_pyarrow_table().to_pylist()
    assert all(row["event_day"] >= report["cutoff"] for row in remaining)
    assert report["files_removed_by_vacuum"] >= 1, (
        "the rows were deleted but their files were left behind, so time travel still "
        "returns them and the retention policy is not met"
    )


def test_the_exposure_check_scans_every_row_and_every_column() -> None:
    """Four false negatives an adversarial review found, as four assertions.

    The first version read the column names from rows[0], sampled the first 200 rows, skipped
    any column that appeared in the policy, and matched exactly "C" plus six digits.
    """
    from samegold.governance.policy import check_gold_exposure

    assert check_gold_exposure([{"net_cents": 1}, {"customer_id": "C000042"}])
    assert check_gold_exposure([{"sku": "C000042"}])
    assert check_gold_exposure([{"x": "ok"}] * 240 + [{"x": "C000042"}])
    for shape in ("C0000123", "c000123", "CUST-C000123"):
        assert check_gold_exposure([{"note": shape}]), shape


def test_the_purge_leaves_no_identifier_in_the_transaction_log(tmp_path: Path) -> None:
    """VACUUM removes data files and leaves the log alone, and the log carries per-file
    min/max statistics. After a purge, real customer identifiers were sitting in the
    minValues of a committed log entry: the rows were gone and the identifiers were not.

    The fix is a table property, and this is the test that keeps it.
    """
    import pyarrow as pa
    from deltalake import write_deltalake

    from samegold.governance.retention import purge_expired, residual_in_transaction_log

    table_uri = str(tmp_path / "events")
    identifiers = [f"C{i:06d}" for i in range(50)]
    write_deltalake(
        table_uri,
        pa.table(
            {
                "customer_id": identifiers,
                "event_day": [f"2026-0{(i % 3) + 1}-01" for i in range(50)],
            }
        ),
        mode="overwrite",
        configuration={"delta.dataSkippingStatsColumns": "event_day"},
    )
    report = purge_expired(
        table_uri, "event_day", retention_days=60, now=dt.datetime(2026, 4, 15, tzinfo=dt.UTC)
    )
    assert report["rows_deleted"] > 0
    assert residual_in_transaction_log(table_uri, identifiers) == []


def test_the_residual_check_actually_finds_something_when_stats_are_not_restricted(
    tmp_path: Path,
) -> None:
    """The negative control for the test above: without the table property, the identifiers
    ARE in the log, and the checker says so. A check that can never fire proves nothing."""
    import pyarrow as pa
    from deltalake import write_deltalake

    from samegold.governance.retention import purge_expired, residual_in_transaction_log

    table_uri = str(tmp_path / "unprotected")
    identifiers = [f"C{i:06d}" for i in range(50)]
    write_deltalake(
        table_uri,
        pa.table(
            {
                "customer_id": identifiers,
                "event_day": [f"2026-0{(i % 3) + 1}-01" for i in range(50)],
            }
        ),
        mode="overwrite",
    )
    purge_expired(
        table_uri, "event_day", retention_days=60, now=dt.datetime(2026, 4, 15, tzinfo=dt.UTC)
    )
    assert residual_in_transaction_log(table_uri, identifiers), (
        "without delta.dataSkippingStatsColumns the identifiers survive in the log, and this "
        "test exists to prove the checker can see them"
    )
