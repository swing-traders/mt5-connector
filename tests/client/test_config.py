import pytest

from mt5connector.client.config import MT5Config, derive_ws_url
from mt5connector.client.errors import MT5ConfigError


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


def test_the_execution_settings_default():
    c = _base(server_url="http://192.168.1.10:5000")
    assert c.deviation_points == 20
    assert c.account_refresh_seconds == 10
    assert c.history_lookback_mins == 60


@pytest.mark.parametrize(
    ("field", "value"),
    [("deviation_points", -1), ("account_refresh_seconds", 0), ("history_lookback_mins", 0)],
)
def test_an_execution_setting_out_of_range_is_refused_naming_it(field, value):
    with pytest.raises(ValueError, match=field):
        _base(server_url="http://192.168.1.10:5000", **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [("deviation_points", 0), ("account_refresh_seconds", 1), ("history_lookback_mins", 1)],
)
def test_an_execution_setting_at_its_bound_is_kept(field, value):
    c = _base(server_url="http://192.168.1.10:5000", **{field: value})
    assert getattr(c, field) == value
