"""Append-only evidence, red runs included.

``history.jsonl`` is never rewritten and never filtered. A repository that only ever shows
green runs is showing a selection, not a history, so the renderer prints the number of red
records next to the green ones and the fast lane fails if the file ever shrinks.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

from samegold.evidence.record import EvidenceRecord


class EvidenceStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.runs_dir = self.root / "runs"
        self.history = self.root / "history.jsonl"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.history.touch(exist_ok=True)

    def append(self, record: EvidenceRecord) -> None:
        with self.history.open("a", encoding="utf-8") as handle:
            handle.write(record.to_line() + "\n")
        latest = self.runs_dir / f"{record.claim_id}.json"
        latest.write_text(json.dumps(record.to_json(), indent=2, sort_keys=True), encoding="utf-8")

    def read_history(self) -> Iterator[dict[str, object]]:
        if not self.history.exists():
            return iter(())
        return (
            json.loads(line)
            for line in self.history.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    def latest(self) -> dict[str, dict[str, object]]:
        """The most recent record per claim, which is what the README renders."""
        out: dict[str, dict[str, object]] = {}
        for entry in self.read_history():
            out[str(entry["claim_id"])] = entry
        return out

    def counts(self) -> dict[str, int]:
        green = red = 0
        for entry in self.read_history():
            verdict = entry.get("verdict", {})
            if isinstance(verdict, dict) and verdict.get("outcome") == "pass":
                green += 1
            else:
                red += 1
        return {"pass": green, "fail": red, "total": green + red}
