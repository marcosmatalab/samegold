from __future__ import annotations

import pytest

from samegold.pipelines.session import StorageMode, build_session


@pytest.fixture(scope="session")
def spark():  # type: ignore[no-untyped-def]
    """One session for the whole module: ``getOrCreate`` costs about 7 seconds."""
    session = build_session("samegold-tests", mode=StorageMode.from_env())
    yield session
    session.stop()
