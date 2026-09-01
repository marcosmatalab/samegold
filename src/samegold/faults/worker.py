"""A single streaming run of the silver stage, crashable at a named point.

Run as a subprocess by faults/harness.py, because the whole point is that the process dies:
``os._exit`` cannot be undone inside the test runner.

The stage chosen for the fault experiment is bronze -> silver, written by ``foreachBatch``,
because that is where a partial write is possible and where idempotency has to be earned.
The writer is idempotent by construction (it overwrites the partition for its own batch id),
which is the property the crash points are there to falsify.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from samegold.faults.barrier import CrashBarrier
from samegold.pipelines.schema import RESCUED_COLUMN, bronze_schema
from samegold.pipelines.session import StorageMode, build_session
from samegold.pipelines.transform import classify


def run(bronze: Path, out: Path, files_per_trigger: int = 40) -> int:
    barrier = CrashBarrier.from_env()
    spark = build_session("samegold-faults", mode=StorageMode.from_env())
    checkpoint = out / "_checkpoint"
    silver_path = out / "silver"

    stream = (
        spark.readStream.format("json")
        .schema(bronze_schema())
        .option("columnNameOfCorruptRecord", RESCUED_COLUMN)
        .option("maxFilesPerTrigger", files_per_trigger)
        .load(str(bronze))
    )

    # The writer is a knob, and the knob is the negative control. "overwrite" is idempotent
    # by construction: a batch owns its own directory and rewrites it, so replaying it after a
    # crash cannot double the rows. "append" is the hopeful version that most pipelines
    # actually ship. The campaign runs the harness against BOTH, and a harness that cannot
    # tell them apart is not measuring anything - which is exactly what an adversarial review
    # demonstrated against the first version of this file.
    write_mode = os.environ.get("SAMEGOLD_WRITER", "overwrite")

    def write_batch(batch_df, batch_id: int) -> None:  # type: ignore[no-untyped-def]
        barrier.reach("before_batch_write", batch_id)
        target = silver_path / f"batch_id={batch_id}"
        (
            classify(batch_df)
            .write.mode(write_mode)
            .format(str(StorageMode.from_env()))
            .save(str(target))
        )
        barrier.reach("after_batch_write_before_commit", batch_id)

    query = (
        stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="samegold-faults-worker")
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--files-per-trigger", type=int, default=40)
    parser.add_argument("--reset", action="store_true", help="delete the output and checkpoint")
    args = parser.parse_args(argv)
    out = Path(args.out)
    if args.reset:
        shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True, exist_ok=True)
    return run(Path(args.bronze), out, args.files_per_trigger)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
