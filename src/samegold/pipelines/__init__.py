"""The Spark implementation: bronze, silver, gold, and the session that runs them."""

from samegold.pipelines.session import DELTA_COORDINATE, StorageMode, build_session

__all__ = ["DELTA_COORDINATE", "StorageMode", "build_session"]
