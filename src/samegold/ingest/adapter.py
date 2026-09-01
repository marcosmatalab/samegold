"""The ingestion adapter, and the honest statement of what its implementations do not share.

Auto Loader is proprietary. There is no open-source equivalent, so the two lanes of this
project cannot run the same ingestion code, and pretending otherwise would put a false claim
at the very bottom of the stack.

What they CAN share is a contract: given a landing directory, produce a stream of rows with
the declared bronze schema, a rescued column for anything that did not fit, and an
``arrival_ts``. That contract is what ``tests/spark/test_ingest_contract.py`` checks against
whichever implementation is available, and the guarantees that differ - file discovery, state
scaling, schema evolution - are written into ``differences`` so they travel with the code
instead of living in a README nobody re-reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


@dataclass(frozen=True, slots=True)
class IngestSpec:
    path: str
    max_files_per_trigger: int = 40
    schema_location: str | None = None  # Auto Loader only


class Ingestor(Protocol):
    name: str
    differences: tuple[str, ...]

    def read_stream(self, spark: Any, spec: IngestSpec) -> DataFrame: ...


@dataclass(frozen=True)
class FileSourceIngestor:
    """Apache Spark's file source. Works everywhere, discovers by listing."""

    name: str = "file-source"
    differences: tuple[str, ...] = (
        "discovers new files by listing the directory on every trigger: cost grows with the "
        "number of objects, not with the number of new ones",
        "no cloud file-notification mode",
        "seen-file state lives in the checkpoint; there is no RocksDB index behind it",
        "no schema evolution modes and no schema hints; the schema is declared or nothing",
        "malformed records land in the corrupt-record column, whose semantics differ per "
        "format, rather than in a rescued-data column with a stable shape",
    )

    def read_stream(self, spark: Any, spec: IngestSpec) -> DataFrame:
        from samegold.pipelines.schema import RESCUED_COLUMN, bronze_schema

        return (
            spark.readStream.format("json")
            .schema(bronze_schema())
            .option("columnNameOfCorruptRecord", RESCUED_COLUMN)
            .option("maxFilesPerTrigger", spec.max_files_per_trigger)
            .load(spec.path)
        )


@dataclass(frozen=True)
class AutoLoaderIngestor:
    """Databricks Auto Loader. Only exists inside Databricks."""

    name: str = "auto-loader"
    differences: tuple[str, ...] = (
        "discovers new files by listing OR by cloud notifications",
        "seen-file state is RocksDB backed and scales to millions of objects",
        "schema evolution modes, schema hints and a stable `_rescued_data` column",
        "on Free Edition there are no external locations, so it can only read a UC volume "
        "and file-notification mode is not reachable",
    )

    def read_stream(self, spark: Any, spec: IngestSpec) -> DataFrame:
        from samegold.pipelines.schema import bronze_schema

        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.maxFilesPerTrigger", spec.max_files_per_trigger)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .schema(bronze_schema())
        )
        if spec.schema_location:
            reader = reader.option("cloudFiles.schemaLocation", spec.schema_location)
        return reader.load(spec.path)


INGESTORS: dict[str, Any] = {
    FileSourceIngestor.name: FileSourceIngestor(),
    AutoLoaderIngestor.name: AutoLoaderIngestor(),
}

SHARED_GUARANTEES: tuple[str, ...] = (
    "every row carries the declared bronze schema",
    "a row that does not fit the schema is preserved, never dropped",
    "a file is not re-read once its batch has been committed",
)
