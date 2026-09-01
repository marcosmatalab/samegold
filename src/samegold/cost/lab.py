"""The cost lab: measured layout experiments on real Delta tables, with no cloud.

The metric is deliberately NOT wall time. Wall time on a laptop measures the laptop, and a
performance claim backed by a stopwatch in a container is the kind of number a reviewer
discounts on sight. What is measured instead is what the layout actually determines and what
the engine actually uses to skip work:

  * how many files a predicate CANNOT skip, computed from the per-file min/max statistics in
    the Delta log rather than from a query plan;
  * how many bytes those files hold;
  * for a delete, how many rows had to be copied to rewrite the surviving data.

All three are deterministic: the same input produces the same number on any machine, which is
what makes them publishable. Each experiment declares what its result may NOT be attributed
to, because "clustering made it faster" is usually "the files got bigger".

Everything here runs on delta-rs (the Rust implementation), so the whole lab executes with no
JVM and no Maven. That is also a second engine reading tables in the same format the Spark
lane writes, which is the interoperability the Delta protocol exists for.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


@dataclass(frozen=True, slots=True)
class Experiment:
    experiment_id: str
    question: str
    treatment: str
    control: str
    metric: str
    not_attributable: str


EXPERIMENTS: tuple[Experiment, ...] = (
    Experiment(
        "COST-01",
        "What does compacting many small files buy?",
        "OPTIMIZE (bin-packing compaction) over the same rows",
        "identical row content, identical predicate",
        "files not skippable for a month predicate, and the bytes in them",
        "not a statement about write cost: compaction rewrote every row once",
    ),
    Experiment(
        "COST-02",
        "Does clustering by (accounting_month, sku) help a sku predicate?",
        "Z-ORDER on (accounting_month, sku)",
        "same rows, both arms compacted to the same target size, same predicate",
        "files not skippable for a sku predicate",
        "not attributable to file size: both arms are compacted with the same target",
    ),
    Experiment(
        "COST-03",
        "Partitioning by month versus clustering: which predicate does each one serve?",
        "PARTITION BY accounting_month",
        "same rows, both compacted, two predicates (month, sku)",
        "files not skippable for each predicate",
        "not a general recommendation: it is a statement about these two predicates",
    ),
    Experiment(
        "COST-04",
        "What does a delete cost when the survivors have to be rewritten?",
        "DELETE of one month",
        "same table, same predicate",
        "rows copied to rewrite the survivors, and files added and removed",
        "not a deletion-vector measurement: with deletion vectors the metric changes shape, "
        "and that comparison belongs to the Databricks lane",
    ),
)


def lab_dataset(rows: int = 400_000, months: int = 12, skus: int = 400, seed: int = 7) -> pa.Table:
    """A deterministic table sized so that layout is actually visible.

    The business dataset the rest of the project uses is a few thousand rows, and a few
    thousand rows compact into ONE file: every layout comparison then reads "1 file versus 1
    file", which measures nothing. Layout experiments need enough data to produce several
    files after compaction, and the numbers below describe file layout, not retail.

    Rows are laid out in arrival order (interleaved months and skus), which is what an
    append-only ingest actually produces and what makes clustering non-trivial.
    """
    import random

    rng = random.Random(seed)
    month_values = [f"2026-{(i % 12) + 1:02d}" for i in range(months)]
    sku_values = [f"SKU-{i:05d}" for i in range(skus)]
    return pa.table(
        {
            "accounting_month": [rng.choice(month_values) for _ in range(rows)],
            "sku": [rng.choice(sku_values) for _ in range(rows)],
            "order_id": [f"O{i:09d}" for i in range(rows)],
            "qty": [rng.randrange(1, 5) for _ in range(rows)],
            "unit_price_cents": [rng.randrange(199, 24999) for _ in range(rows)],
        }
    )


def _assert_value_exists(rows: pa.Table, column: str, value: str) -> None:
    """Refuse to probe a value the table does not contain.

    Probing a sku that does not exist reports "no file has to be read", which looks like
    perfect skipping and is nothing of the kind. The lab used to accept it silently.
    """
    present = set(rows.column(column).to_pylist())
    if value not in present:
        raise ValueError(
            f"the probe value {value!r} does not appear in column {column!r}: the experiment "
            f"would report perfect data skipping for a predicate that matches nothing. "
            f"Pick one of {sorted(present)[:3]}..."
        )


def _files_not_skippable(table: DeltaTable, column: str, value: str) -> dict[str, int]:
    """Count the files whose statistics do not let the reader skip them.

    This reads the min/max the writer recorded in the Delta log, which is exactly what data
    skipping uses, and it is why the number is identical on every machine.
    """
    # delta-rs returns its own Arrow table type (arro3), not a pyarrow one, so it is
    # converted rather than assumed. Two Arrow implementations in one process is the price of
    # having a second engine, and it is worth paying.
    actions = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    considered = 0
    total_bytes = 0
    for action in actions:
        low = action.get(f"min.{column}")
        high = action.get(f"max.{column}")
        if low is None or high is None or (low <= value <= high):
            considered += 1
            total_bytes += int(action.get("size_bytes", 0))
    return {
        "files_total": len(actions),
        "files_not_skippable": considered,
        "bytes_not_skippable": total_bytes,
        "bytes_total": sum(int(a.get("size_bytes", 0)) for a in actions),
    }


def _write(rows: pa.Table, path: Path, partition_by: list[str] | None = None) -> None:
    shutil.rmtree(path, ignore_errors=True)
    write_deltalake(str(path), rows, mode="overwrite", partition_by=partition_by)


def _append_in_chunks(rows: pa.Table, path: Path, chunks: int) -> None:
    """Write the same rows as many small files: the small-file problem, on purpose."""
    shutil.rmtree(path, ignore_errors=True)
    size = max(1, rows.num_rows // chunks)
    for offset in range(0, rows.num_rows, size):
        write_deltalake(
            str(path),
            rows.slice(offset, size),
            mode="overwrite" if offset == 0 else "append",
        )


def run_lab(
    rows: pa.Table, workdir: Path, month: str, sku: str, target_size: int = 2_000_000
) -> dict[str, Any]:
    """Run every experiment over one dataset and return the measurements."""
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    _assert_value_exists(rows, "accounting_month", month)
    _assert_value_exists(rows, "sku", sku)
    results: dict[str, Any] = {"rows": rows.num_rows, "month_probed": month, "sku_probed": sku}

    small = workdir / "small_files"
    _append_in_chunks(rows, small, chunks=40)
    before = _files_not_skippable(DeltaTable(str(small)), "accounting_month", month)
    metrics = DeltaTable(str(small)).optimize.compact(target_size=target_size)
    after = _files_not_skippable(DeltaTable(str(small)), "accounting_month", month)
    results["COST-01"] = {
        "before": before,
        "after": after,
        "optimize": {k: metrics[k] for k in ("numFilesAdded", "numFilesRemoved") if k in metrics},
        "files_removed_pct": round(
            100.0 * (before["files_total"] - after["files_total"]) / before["files_total"], 2
        ),
        "bytes_not_skippable_change_pct": round(
            100.0
            * (after["bytes_not_skippable"] - before["bytes_not_skippable"])
            / max(1, before["bytes_not_skippable"]),
            2,
        ),
    }

    # COST-02 is run at TWO file sizes on purpose. At one file size the answer came back
    # "clustering changed nothing", which is true and useless: with three files covering the
    # whole sku range there is nothing to skip. Measuring at two sizes turns a null result
    # into a threshold, and the null result is published next to it rather than dropped.
    results["COST-02"] = {}
    for label, size in (("large_files", target_size), ("small_files", target_size // 16)):
        plain = workdir / f"plain_{label}"
        _append_in_chunks(rows, plain, chunks=40)
        DeltaTable(str(plain)).optimize.compact(target_size=size)
        clustered = workdir / f"clustered_{label}"
        _append_in_chunks(rows, clustered, chunks=40)
        DeltaTable(str(clustered)).optimize.compact(target_size=size)
        DeltaTable(str(clustered)).optimize.z_order(["accounting_month", "sku"], target_size=size)
        unclustered = _files_not_skippable(DeltaTable(str(plain)), "sku", sku)
        clustered_stats = _files_not_skippable(DeltaTable(str(clustered)), "sku", sku)
        results["COST-02"][label] = {
            "target_size_bytes": size,
            "unclustered": unclustered,
            "clustered": clustered_stats,
            # The honest denominator. The two arms do not end up with the same number of
            # files - Z-ORDER rewrites and recompresses - so comparing raw bytes mixes
            # skipping with granularity and compression. The share of the table each arm has
            # to read is comparable; the raw byte counts are published next to it so a reader
            # can see the confound rather than take the ratio on trust.
            "share_unclustered": round(
                unclustered["bytes_not_skippable"] / max(1, unclustered["bytes_total"]), 4
            ),
            "share_clustered": round(
                clustered_stats["bytes_not_skippable"] / max(1, clustered_stats["bytes_total"]),
                4,
            ),
        }
    clustered = workdir / "clustered_large_files"

    partitioned = workdir / "partitioned"
    _write(rows, partitioned, partition_by=["accounting_month"])
    # Compact the partitioned arm too, or the comparison is "one big file versus twelve
    # unoptimised ones" and says nothing about partitioning.
    DeltaTable(str(partitioned)).optimize.compact(target_size=target_size)
    results["COST-03"] = {
        "partitioned_files_for_month": len(
            DeltaTable(str(partitioned)).file_uris([("accounting_month", "=", month)])
        ),
        "partitioned_files_total": len(DeltaTable(str(partitioned)).file_uris()),
        "clustered_month_predicate": _files_not_skippable(
            DeltaTable(str(clustered)), "accounting_month", month
        ),
        "clustered_sku_predicate": _files_not_skippable(DeltaTable(str(clustered)), "sku", sku),
        "partitioned_sku_predicate": _files_not_skippable(DeltaTable(str(partitioned)), "sku", sku),
    }

    deletable = workdir / "delete"
    _append_in_chunks(rows, deletable, chunks=8)
    DeltaTable(str(deletable)).optimize.compact(target_size=target_size)
    delete_metrics = DeltaTable(str(deletable)).delete(f"accounting_month = '{month}'")
    results["COST-04"] = {
        k: delete_metrics[k]
        for k in ("num_added_files", "num_removed_files", "num_deleted_rows", "num_copied_rows")
        if k in delete_metrics
    }
    results["COST-04"]["rows_copied_per_row_deleted"] = round(
        delete_metrics.get("num_copied_rows", 0)
        / max(1, delete_metrics.get("num_deleted_rows", 1)),
        2,
    )

    results["experiments"] = [
        {
            "experiment_id": e.experiment_id,
            "question": e.question,
            "treatment": e.treatment,
            "control": e.control,
            "metric": e.metric,
            "not_attributable": e.not_attributable,
        }
        for e in EXPERIMENTS
    ]
    return results
