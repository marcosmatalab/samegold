"""Where the pipeline is killed, and why those places and not others.

Killing a job at a uniformly random instant samples the DURATION of the job, not its
STRUCTURE: nearly every kill lands in the long read-and-shuffle phase and almost none in the
microsecond window that actually matters, which is between writing the data files and making
the commit visible. Two hundred random kills with no hits in that window is a large number
that means nothing.

So the points are enumerated. Each one is a place where a partial state is possible, and the
list says plainly which of them this harness can reach and which it cannot:

  * REACHABLE from the writer we own: everything around a micro-batch - before it writes,
    after it writes and before the checkpoint advances, between two batches.
  * NOT REACHABLE without patching the engine: the inside of a Delta commit, the inside of a
    RocksDB state-store commit, the middle of a multi-part commit. Those belong to the
    engine, and a portfolio project that claims to have tested them is claiming to have
    tested somebody else's transaction log. They are listed here with ``reachable=False`` and
    the README reports them as NOT COVERED rather than leaving them out of the list.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CrashPoint:
    name: str
    description: str
    reachable: bool
    expectation: str
    stage: str = "silver"

    def to_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "reachable": self.reachable,
            "expectation": self.expectation,
            "stage": self.stage,
        }


CRASH_POINTS: tuple[CrashPoint, ...] = (
    CrashPoint(
        "before_batch_write",
        "the micro-batch has been computed but nothing has been written",
        True,
        "the batch is replayed in full on restart; the result is unchanged",
    ),
    CrashPoint(
        "after_batch_write_before_commit",
        "the data files are on disk, the checkpoint has not advanced",
        True,
        "the batch is replayed; whatever the write left behind must not be double counted "
        "(this is the point that separates an idempotent writer from a hopeful one)",
    ),
    # "between batches" is deliberately NOT a separate point. In a foreachBatch writer it is
    # indistinguishable from before_batch_write: by the time the function is entered for
    # batch k, batch k-1 is committed and batch k has written nothing. Listing it separately
    # would have inflated the coverage number by one point that tests the same instant, and
    # the first campaign run reported it as a missed injection - which is how it was caught.
    CrashPoint(
        "mid_merge",
        "half of an SCD2 MERGE has been applied",
        True,
        "the MERGE is atomic at the table level, so the restart sees either the old or the "
        "new version, never a half-applied dimension",
        stage="gold",
    ),
    CrashPoint(
        "after_gold_before_close_marker",
        "gold is written, the accounting close marker has not been recorded",
        True,
        "the close is recomputed; the close marker is the only thing that must not be "
        "written twice, and it is keyed by (month, version)",
        stage="gold",
    ),
    CrashPoint(
        "inside_delta_commit",
        "between writing the parquet files and making the commit visible in _delta_log",
        False,
        "guaranteed by the Delta protocol, not by this project; reaching it requires "
        "instrumenting the engine's LogStore, which changes the program under test",
    ),
    CrashPoint(
        "inside_state_store_commit",
        "in the middle of a RocksDB state-store checkpoint",
        False,
        "guaranteed by the engine; not reachable from a foreachBatch writer",
    ),
    CrashPoint(
        "inside_multipart_commit",
        "between the parts of a multi-part commit on object storage",
        False,
        "cloud-storage specific and not observable on a local filesystem",
    ),
)

REACHABLE = tuple(point for point in CRASH_POINTS if point.reachable)
SILVER_POINTS = tuple(p for p in REACHABLE if p.stage == "silver")
GOLD_POINTS = tuple(p for p in REACHABLE if p.stage == "gold")
