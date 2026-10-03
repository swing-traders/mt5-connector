import pytest

from mt5connect.config import MT5Config, derive_ws_url
from mt5connect.errors import MT5ConfigError


def _base(**kw):
    return MT5Config(account=1, password="p", server="s", symbols=["EURUSD"], **kw)


def test_a_config_without_a_server_url_is_refused_naming_the_field():
    with pytest.raises(MT5ConfigError, match="server_url"):
        _base()


def test_an_empty_server_url_is_refused_naming_the_field():
    with pytest.raises(MT5ConfigError, match="server_url"):
        _base(server_url="")


def test_server_url_is_kept():
    c = _base(server_url="http://192.168.1.10:5000")
    assert c.server_url == "http://192.168.1.10:5000"


def test_ws_url_derived_when_omitted():
    c = _base(server_url="http://192.168.1.10:5000")
    assert c.ws_url == "ws://192.168.1.10:9000"


def test_ws_url_explicit_wins():
    c = _base(server_url="http://192.168.1.10:5000", ws_url="ws://192.168.1.10:9001")
    assert c.ws_url == "ws://192.168.1.10:9001"


def test_derive_ws_url_http():
    assert derive_ws_url("http://localhost:5000") == "ws://localhost:9000"


def test_derive_ws_url_already_ws():
    assert derive_ws_url("ws://localhost:9000") == "ws://localhost:9000"
