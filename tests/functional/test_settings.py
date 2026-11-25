import json
import stat

import pytest

from functional.settings import Redactor, load, write


def test_secret_values_are_redacted_in_native_and_escaped_output():
    values = {
        "MT5_LOGIN": "12345",
        "MT5_PASSWORD": 'pass\\"word',
        "MT5_SERVER": "Example-Demo",
    }
    redact = Redactor(values)
    for value in values.values():
        assert redact(value) == "[redacted]"
        assert redact(json.dumps(value)) == '"[redacted]"'
    assert redact("trading allowed: terminal=true, expert=true") == (
        "trading allowed: terminal=true, expert=true"
    )


def test_environment_values_remain_literal_and_the_copy_is_private(tmp_path):
    source = tmp_path / ".env"
    source.write_text("MT5_LOGIN=1\nMT5_PASSWORD=p$word=literal\nMT5_SERVER=Example-Demo\n")
    values = load(source)
    assert values["MT5_PASSWORD"] == "p$word=literal"
    destination = tmp_path / "private.env"
    write(destination, values)
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert load(destination) == values


def test_a_missing_secret_is_reported_by_variable_name(tmp_path):
    path = tmp_path / ".env"
    path.write_text("MT5_LOGIN=1\n")
    with pytest.raises(ValueError, match="MT5_PASSWORD is missing"):
        load(path)
