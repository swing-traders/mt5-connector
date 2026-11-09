"""Each hub has its own history volume."""

import pytest


@pytest.fixture(autouse=True)
def history_store(tmp_path, monkeypatch):
    monkeypatch.setenv("MT5_HISTORY_STORE", str(tmp_path))
