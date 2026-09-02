"""Same stand-in as the package: see typings/numpy/__init__.pyi."""

from typing import Any

def __getattr__(name: str) -> Any: ...
