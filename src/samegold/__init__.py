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

__version__ = "0.1.0"
