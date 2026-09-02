"""A deliberately empty stand-in for numpy, so that mypy can run at all.

Nothing in this repository imports numpy. It arrives as a transitive dependency (duckdb and
pyarrow both expose it), and numpy 2.x ships stubs written with `type X = ...`, which is 3.12
syntax. This project declares `python_version = "3.11"` because 3.11 is the floor it supports
and what CI runs, and mypy refuses to PARSE that syntax under 3.11 - a blocking error, before
it checks a single line of ours.

That made mypy's answer depend on which extras happened to be installed: green with `.[dev]`,
which is what CI installs, and unable to start after `make install-spark`, which pulls numpy in
behind pandas. `follow_imports = "skip"` and `ignore_errors` do not help, because a parse
failure happens before either applies. Shadowing the package on `mypy_path` does, and it is
visible in the repository rather than buried in a flag.

The cost is stated rather than hidden: if this project ever uses numpy directly, this file
turns its API into `Any` and must be deleted. `tests/fast/test_architecture.py` is where an
import of it would be refused.
"""

from typing import Any

def __getattr__(name: str) -> Any: ...
