"""samegold - a month-end close you can falsify.

The package is deliberately split so that every layer can be attacked on its own:

- ``domain``     the data contract and the business rules, as pure Python. No engine.
- ``generator``  seeded event generator; seeds are derived from the git commit SHA.
- ``oracle``     two independent recomputations of gold (DuckDB SQL, analytic Python).
- ``verify``     canonical digests, typed verdicts, invariants, interval statistics.
- ``mutation``   mechanical mutation of the transformation code (Python AST and SQL AST).
- ``faults``     structural fault injection points and the barrier that fires them.
- ``evidence``   append-only evidence records; every number in the README comes from here.
- ``pipelines``  the Spark / Spark Declarative Pipelines implementation.
- ``ingest``     the ingestion adapter: Auto Loader (Databricks) vs file source (OSS).
- ``cost``       measured cost and performance experiments.
"""

from importlib.metadata import version as _distribution_version

# ONE SOURCE. `pyproject.toml` declares the version and the installed distribution carries it;
# this reads it back instead of restating it. It used to be a second literal, and at tag v0.2.0
# both copies still said 0.1.0. tests/fast/test_version.py holds pyproject, the installed
# distribution and the newest CHANGELOG release to one number.
__version__ = _distribution_version("samegold")
