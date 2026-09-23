"""The version has one source, and the release it names is the one the changelog describes.

WHAT THIS EXISTS BECAUSE OF. At tag `v0.2.0`, `pyproject.toml` and `src/samegold/__init__.py`
both said 0.1.0, so `pip show samegold` reported 0.1.0 for a checkout of the 0.2.0 release, and
`CHANGELOG.md` said 0.2.0. Two hand-typed copies of the number and a heading written by a third
hand, with nothing comparing them: the defect this repository hunts, in its own metadata.

So there is one copy. `pyproject.toml` declares the version, the installed distribution carries
it, and `samegold.__version__` READS it from the distribution rather than restating it. These
tests hold the three places together: the declaration, what is installed, and the newest
heading of the changelog.
"""

from __future__ import annotations

import re
import tomllib
from importlib.metadata import version
from pathlib import Path

import samegold

REPO = Path(__file__).resolve().parents[2]

#: A release heading in Keep a Changelog form: `## [0.3.0] - 2026-09-23`, or `- Unreleased`
#: while the section is being written. The date is not compared: the version is the claim.
HEADING = re.compile(r"^## \[(?P<version>\d+\.\d+\.\d+)\]", re.MULTILINE)


def _declared() -> str:
    with (REPO / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def _newest_changelog_version() -> str:
    text = (REPO / "CHANGELOG.md").read_text(encoding="utf-8")
    match = HEADING.search(text)
    assert match, "CHANGELOG.md has no `## [x.y.z]` release heading"
    return match.group("version")


def test_the_package_version_is_the_newest_changelog_release() -> None:
    """The defect itself: pyproject said 0.1.0 while the changelog's newest release was 0.2.0."""
    assert _declared() == _newest_changelog_version(), (
        f"pyproject.toml declares {_declared()} and the newest release in CHANGELOG.md is "
        f"{_newest_changelog_version()}. Bump one of them: a release is both or neither."
    )


def test_the_module_reads_the_version_instead_of_restating_it() -> None:
    """`__version__` comes from the installed distribution, so it cannot be a second copy."""
    assert samegold.__version__ == version("samegold")
    source = (REPO / "src" / "samegold" / "__init__.py").read_text(encoding="utf-8")
    assert not re.search(r"__version__\s*=\s*[\"']", source), (
        "src/samegold/__init__.py assigns a literal version; read it with importlib.metadata"
    )


def test_what_is_installed_is_what_is_declared() -> None:
    """`pip show samegold` is the figure a user sees, and an editable install can go stale.

    An editable install records the version at install time. After a bump, the checkout says
    one number and the environment another until `pip install -e .` runs again - which is
    exactly how `pip show` came to say 0.1.0 on a 0.2.0 checkout.
    """
    assert version("samegold") == _declared(), (
        f"the installed distribution is {version('samegold')} and pyproject.toml declares "
        f"{_declared()}; reinstall with `pip install -e .` (or `make install`)"
    )
