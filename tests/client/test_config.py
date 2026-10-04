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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_s", 0),
        ("timeout_s", -1.0),
        ("timeout_s", float("nan")),
        ("timeout_s", float("inf")),
        ("reconnect_max_attempts", 0),
        ("reconnect_initial_delay_s", -0.5),
        ("reconnect_initial_delay_s", float("nan")),
        ("reconnect_initial_delay_s", float("inf")),
        ("reconnect_max_delay_s", -1.0),
        ("reconnect_max_delay_s", float("nan")),
        ("reconnect_max_delay_s", float("inf")),
    ],
)
def test_a_connection_setting_out_of_range_is_refused_naming_it(field, value):
    with pytest.raises(ValueError, match=field):
        _base(server_url="http://192.168.1.10:5000", **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_s", 0.001),
        ("reconnect_max_attempts", 1),
        ("reconnect_initial_delay_s", 0),
        ("reconnect_max_delay_s", 0),
    ],
)
def test_a_connection_setting_at_its_bound_is_kept(field, value):
    c = _base(server_url="http://192.168.1.10:5000", **{field: value})
    assert getattr(c, field) == value


@pytest.mark.parametrize("symbols", [["EURUSD", "   "], [""], ["\t"]])
def test_a_blank_symbol_is_refused(symbols):
    with pytest.raises(ValueError, match="symbols"):
        MT5Config(
            account=1,
            password="p",
            server="s",
            symbols=symbols,
            server_url="http://192.168.1.10:5000",
        )


@pytest.mark.parametrize(
    "server_url", ["nonsense", "localhost:5000", "http://", "//192.168.1.10:5000"]
)
def test_a_server_url_without_a_scheme_and_host_is_refused_naming_it(server_url):
    with pytest.raises(MT5ConfigError, match="server_url"):
        _base(server_url=server_url)


@pytest.mark.parametrize(
    "ws_url",
    [
        "",
        "nonsense",
        "localhost:9000",
        "ws://",
        "//192.168.1.10:9000",
        "http://192.168.1.10:9000",
        "ftp://192.168.1.10:1",
        "ws://[::1",
    ],
)
def test_a_ws_url_that_is_no_websocket_url_with_a_host_is_refused_naming_it(ws_url):
    with pytest.raises(MT5ConfigError, match="ws_url"):
        _base(server_url="http://192.168.1.10:5000", ws_url=ws_url)


@pytest.mark.parametrize("ws_url", ["ws://192.168.1.10:9001", "wss://hub.example.com/push"])
def test_a_ws_or_wss_url_with_a_host_is_kept(ws_url):
    assert _base(server_url="http://192.168.1.10:5000", ws_url=ws_url).ws_url == ws_url


def test_a_bracketed_ipv6_host_keeps_its_brackets_in_the_derived_ws_url():
    assert derive_ws_url("http://[::1]:5000") == "ws://[::1]:9000"
    assert _base(server_url="http://[fe80::1]:5000/api").ws_url == "ws://[fe80::1]:9000/api"
