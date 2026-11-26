import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "source",
    [
        "import pytest\ndef test_blocked(): pytest.skip('missing native history')",
        "import pytest\n@pytest.mark.skip(reason='missing native history')\n"
        "def test_blocked(): pass",
        "import pytest\npytest.skip('missing native history', allow_module_level=True)",
        "import pytest\n@pytest.mark.xfail(reason='missing native history')\n"
        "def test_blocked(): assert False",
    ],
    ids=["call", "setup", "collection", "xfail"],
)
def test_a_skipped_certification_case_fails_with_its_name_and_reason(tmp_path, source):
    (tmp_path / "test_case.py").write_text(source)
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "functional.pytest_gate", "-q", "test_case.py"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode != 0
    assert "Certification forbids a skip: test_case.py" in done.stdout
    assert "missing native history" in done.stdout


def test_a_complete_certification_case_passes(tmp_path):
    (tmp_path / "test_case.py").write_text("def test_complete(): assert True")
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "functional.pytest_gate", "-q", "test_case.py"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode == 0, done.stdout + done.stderr
