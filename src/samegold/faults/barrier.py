"""The crash barrier: how a run is killed at a named point, reproducibly.

Three mechanisms were tried. Two of them do not work, and knowing why saves a day:

  * Raising from a ``StreamingQueryListener`` does NOT kill the job. The listener bus is
    asynchronous and best-effort; the exception is logged by the bus and the query finishes
    normally. Anything that relies on a listener to stop a stream is testing nothing.
  * Hooking Delta's commit coordinator is not possible from PySpark: the interfaces are
    internal Scala and there is no public extension point.
  * ``os._exit`` inside ``foreachBatch`` DOES work, and is the mechanism used here.
    ``foreachBatch`` runs on the driver, and ``os._exit`` skips ``finally`` blocks, shutdown
    hooks and the checkpoint commit, which is exactly the state a real crash leaves behind.

The honesty constraint that shapes the design: the barrier lives in the WRITER, never in a
transformation. The transformations are byte-identical between a clean run and a fault run,
and ``samegold evidence`` records the digest of the transformation modules in both, so a
reader can check that the program under test did not change. Crash points inside the engine
(the Delta commit, the state store) are NOT reachable this way and are reported as not
covered rather than quietly claimed.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

EXIT_CODE = 77


@dataclass(frozen=True, slots=True)
class CrashBarrier:
    """Kills the process the first time ``point`` is reached on batch ``batch_id``."""

    point: str | None = None
    batch: int = 1

    @classmethod
    def from_env(cls) -> CrashBarrier:
        """Built from the environment so that enabling it never edits the pipeline."""
        point = os.environ.get("SAMEGOLD_CRASH_POINT") or None
        batch = int(os.environ.get("SAMEGOLD_CRASH_BATCH", "1"))
        return cls(point=point, batch=batch)

    @property
    def armed(self) -> bool:
        return self.point is not None

    def reach(self, point: str, batch_id: int) -> None:
        """Called by the writer at a named point. Returns, or never returns."""
        if self.point != point or batch_id != self.batch:
            return
        print(
            f"samegold: crashing at point={point} batch={batch_id} (exit {EXIT_CODE})",
            file=sys.stderr,
            flush=True,
        )
        sys.stderr.flush()
        sys.stdout.flush()
        # _exit, not exit(): no finally blocks, no atexit, no shutdown hooks, no checkpoint
        # commit. A SystemExit would be caught by Spark and turned into a graceful stop,
        # which is the opposite of the thing being tested.
        os._exit(EXIT_CODE)
