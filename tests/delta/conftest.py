from __future__ import annotations

import pytest

from samegold.pipelines.session import StorageMode, build_session


@pytest.fixture(scope="session")
def delta_spark():  # type: ignore[no-untyped-def]
    """A session with the Delta jars. Skips, loudly, when they cannot be resolved.

    A skip here means Maven Central is unreachable, not that Delta is broken, and the message
    says so: a silent skip would let the delta lane look green in an environment where it
    never ran.
    """
    try:
        session = build_session("samegold-delta", mode=StorageMode.DELTA)
        session.sql("SELECT 1").collect()
    except Exception as exc:
        pytest.skip(
            f"the Delta jars could not be resolved ({type(exc).__name__}). This lane needs "
            f"Maven Central; run `make delta` on a machine that can reach it. Detail: {exc}"[:400]
        )
    yield session
    session.stop()
