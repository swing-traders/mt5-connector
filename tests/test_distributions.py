"""The two distributions: one version, and each one's import chain kept to its own side."""

import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent

# Run in a fresh interpreter: the test process has already imported both distributions.
_IMPORT_ALL = """
import importlib
import pkgutil
import sys

for name in sys.argv[1:]:
    package = importlib.import_module(name)
    for module in pkgutil.walk_packages(package.__path__, f"{name}."):
        importlib.import_module(module.name)
print("\\n".join(sorted(sys.modules)))
"""


def _loaded_by_importing(*packages: str) -> list[str]:
    result = subprocess.run(
        [sys.executable, "-c", _IMPORT_ALL, *packages],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.split()


def _within(module: str, package: str) -> bool:
    return module == package or module.startswith(f"{package}.")


def test_both_distributions_report_the_version_file():
    declared = (_REPO / "VERSION").read_text(encoding="utf-8").strip()

    assert version("mt5-connector-client") == declared
    assert version("mt5-connector-server") == declared


@pytest.mark.parametrize(
    ("packages", "foreign"),
    [
        (
            ("mt5connector.client", "mt5connector.wire"),
            ("mt5connector.server", "flask", "waitress"),
        ),
        (
            ("mt5connector.server",),
            ("mt5connector.client", "mt5connector.wire", "nautilus_trader"),
        ),
    ],
    ids=["client", "server"],
)
def test_a_distribution_imports_nothing_of_the_other(packages, foreign):
    loaded = _loaded_by_importing(*packages)

    assert all(any(_within(module, package) for module in loaded) for package in packages)
    assert [module for module in loaded if any(_within(module, f) for f in foreign)] == []
