import traceback

import pytest
from nautilus_trader.model.identifiers import Venue

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import MT5ConfigError

_WS_URL = "ws://192.168.1.10:9001"


def _base(**kw):
    return MT5Config(account=1, server="s", **kw)


def _config(**kw):
    return _base(**{"ws_url": _WS_URL, **kw})


def test_a_config_without_a_ws_url_is_refused_naming_the_field():
    with pytest.raises(MT5ConfigError, match=r"^MT5Config\.ws_url cannot be empty\.$"):
        _base()


def test_an_empty_ws_url_is_refused_naming_the_field():
    with pytest.raises(MT5ConfigError, match=r"^MT5Config\.ws_url cannot be empty\.$"):
        _base(ws_url="")


def test_a_config_keeps_its_ws_url():
    assert _base(ws_url=_WS_URL).ws_url == _WS_URL


def test_the_venue_defaults_to_mt5():
    assert _config().venue == Venue("MT5")


@pytest.mark.parametrize("label", ["MT5", "MT5_ALPHA", "MT5_BETA", "MT5_A", "MT5_12345678"])
def test_a_venue_of_mt5_or_mt5_and_up_to_eight_capitals_or_digits_is_kept(label):
    assert _config(venue=Venue(label)).venue == Venue(label)


@pytest.mark.parametrize(
    "label", ["MT5-ALPHA", "MT5_", "MT5_ABCDEFGHI", "MT5_ftmo", "MT5ALPHA", "ALPHA", "MT5_FT-MO"]
)
def test_a_venue_of_any_other_shape_is_refused_naming_it(label):
    with pytest.raises(MT5ConfigError, match=f"venue {label} "):
        _config(venue=Venue(label))


def test_a_venue_given_as_a_string_is_refused():
    with pytest.raises(MT5ConfigError, match="venue 'MT5_ALPHA' is not a Venue"):
        _config(venue="MT5_ALPHA")


def test_the_execution_settings_default():
    c = _config()
    assert c.deviation_points == 20
    assert c.account_refresh_seconds == 10
    assert c.history_lookback_mins == 60


@pytest.mark.parametrize(
    ("field", "value"),
    [("deviation_points", -1), ("account_refresh_seconds", 0), ("history_lookback_mins", 0)],
)
def test_an_execution_setting_out_of_range_is_refused_naming_it(field, value):
    with pytest.raises(ValueError, match=field):
        _config(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [("deviation_points", 0), ("account_refresh_seconds", 1), ("history_lookback_mins", 1)],
)
def test_an_execution_setting_at_its_bound_is_kept(field, value):
    c = _config(**{field: value})
    assert getattr(c, field) == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_s", 0),
        ("timeout_s", -1.0),
        ("timeout_s", float("nan")),
        ("timeout_s", float("inf")),
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
        _config(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_s", 0.001),
        ("reconnect_initial_delay_s", 0.001),
    ],
)
def test_a_connection_setting_at_its_bound_is_kept(field, value):
    c = _config(**{field: value})
    assert getattr(c, field) == value


@pytest.mark.parametrize(
    ("delays", "named"),
    [
        ({"reconnect_initial_delay_s": 0.0}, r"reconnect_initial_delay_s 0\.0 is below 1 ms$"),
        (
            {"reconnect_initial_delay_s": 0.0005},
            r"reconnect_initial_delay_s 0\.0005 is below 1 ms$",
        ),
        (
            {"reconnect_initial_delay_s": 0.001, "reconnect_max_delay_s": 0.0},
            r"reconnect_max_delay_s 0\.0 is below 1 ms$",
        ),
        (
            {"reconnect_initial_delay_s": 2.0, "reconnect_max_delay_s": 1.0},
            r"reconnect_initial_delay_s 2\.0 is above reconnect_max_delay_s 1\.0$",
        ),
    ],
)
def test_a_reconnect_delay_under_a_millisecond_or_an_initial_one_past_the_max_is_refused(
    delays, named
):
    with pytest.raises(MT5ConfigError, match=named):
        _config(**delays)


def test_reconnect_delays_of_one_millisecond_are_kept():
    c = _config(reconnect_initial_delay_s=0.001, reconnect_max_delay_s=0.001)
    assert (c.reconnect_initial_delay_s, c.reconnect_max_delay_s) == (0.001, 0.001)


@pytest.mark.parametrize(
    "ws_url",
    [
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
        _config(ws_url=ws_url)


@pytest.mark.parametrize("ws_url", ["ws://192.168.1.10:9001", "wss://hub.example.com/push"])
def test_a_ws_or_wss_url_with_a_host_is_kept(ws_url):
    assert _config(ws_url=ws_url).ws_url == ws_url


@pytest.mark.parametrize(
    "ws_url",
    ["ws://127.0.0.1:bad", "ws://127.0.0.1:70000", "ws://127.0.0.1:0", "wss://127.0.0.1:-1"],
)
def test_a_ws_url_that_names_no_tcp_port_is_refused_naming_it(ws_url):
    with pytest.raises(MT5ConfigError, match="ws_url"):
        _config(ws_url=ws_url)


# The parser refuses a full-width colon naming the whole authority, credentials included.
@pytest.mark.parametrize(
    "ws_url",
    [
        "ws://review:SYNTHETIC_SECRET@127.0.0.1：9000",
        "ws://review:SYNTHETIC_SECRET@127.0.0.1:bad",
    ],
)
def test_a_malformed_url_is_refused_without_echoing_its_credentials(ws_url):
    with pytest.raises(MT5ConfigError, match="ws_url") as refused:
        _config(ws_url=ws_url)
    assert "SYNTHETIC_SECRET" not in "".join(traceback.format_exception(refused.value))


@pytest.mark.parametrize("ws_url", ["ws://127.0.0.1:1", "wss://127.0.0.1:65535", "ws://127.0.0.1"])
def test_a_ws_url_with_a_port_in_range_or_none_is_kept(ws_url):
    assert _config(ws_url=ws_url).ws_url == ws_url


@pytest.mark.parametrize(
    ("ws_url", "shown"),
    [
        ("ws://hub:SYNTHETIC_SECRET@127.0.0.1:9000/push", "ws://127.0.0.1:9000/push"),
        ("wss://hub@[::1]:9000/push?token=SYNTHETIC_SECRET#top", "wss://[::1]:9000/push"),
        ("ws://127.0.0.1:9000", "ws://127.0.0.1:9000"),
    ],
)
def test_the_hubs_display_url_is_its_scheme_host_port_and_path(ws_url, shown):
    assert _config(ws_url=ws_url).ws_display_url == shown


def test_an_instrument_refresh_interval_of_nan_is_refused_naming_it():
    with pytest.raises(MT5ConfigError, match="update_instruments_interval_mins"):
        _config(update_instruments_interval_mins=float("nan"))
