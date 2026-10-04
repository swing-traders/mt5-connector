"""The server's and the hub's settings, the server's start, liveness and relayed commission
schedules."""

from zoneinfo import ZoneInfo

import pytest
from chart_posts import ChartPosts, publishers_on
from mirror_samples import CLOCK

from mt5connector.server.app import TerminalStartError, connect_terminal, create_app
from mt5connector.server.commissions import CommissionRule, CommissionSchedule, CommissionTier
from mt5connector.server.history import FloorStore, History
from mt5connector.server.repeated_hours import RepeatedHours
from mt5connector.server.settings import (
    Settings,
    SettingsError,
    read_hub_settings,
    read_settings,
)
from mt5connector.server.terminal import Terminal
from mt5connector.server.wire.push_wire import (
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
)

ENVIRONMENT = {
    "MT5_TERMINAL_PATH": "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
    "MT5_LOGIN": "12345678",
    "MT5_PASSWORD": "secret-password",
    "MT5_SERVER": "example-server",
}


def test_settings_read_the_environment_with_defaults():
    settings = read_settings(ENVIRONMENT)
    assert settings.terminal_path == "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
    assert settings.login == 12345678
    assert settings.password == "secret-password"
    assert settings.server == "example-server"
    assert settings.login_timeout_ms == 60000
    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 5000
    assert settings.api_threads == 5
    assert settings.broker_tz == ZoneInfo("America/New_York")
    assert settings.broker_offset_hours == 7
    assert settings.clock_check_seconds == 300
    assert settings.clock_sample_max_age_seconds == 30
    assert settings.clock_bootstrap_seconds == 120
    assert settings.history_retry_seconds == 5
    assert settings.floor_ttl_seconds == 900
    assert settings.hub_port == 9000
    assert settings.chart_idle_seconds == 900


def test_settings_take_overrides():
    settings = read_settings(
        ENVIRONMENT
        | {
            "MT5_LOGIN_TIMEOUT_MS": "90000",
            "MT5_API_HOST": "127.0.0.1",
            "MT5_API_PORT": "5100",
            "MT5_API_THREADS": "8",
            "MT5_BROKER_TZ": "Europe/Helsinki",
            "MT5_BROKER_OFFSET_HOURS": "0",
            "MT5_CLOCK_CHECK_SECONDS": "60",
            "MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS": "15",
            "MT5_CLOCK_BOOTSTRAP_SECONDS": "600",
            "MT5_HISTORY_RETRY_SECONDS": "2",
            "MT5_FLOOR_TTL_SECONDS": "60",
            "MT5_HUB_PORT": "9100",
            "MT5_CHART_IDLE_SECONDS": "60",
        }
    )
    assert (settings.login_timeout_ms, settings.api_host, settings.api_port) == (
        90000,
        "127.0.0.1",
        5100,
    )
    assert settings.api_threads == 8
    assert settings.broker_tz == ZoneInfo("Europe/Helsinki")
    assert settings.broker_offset_hours == 0
    assert settings.clock_check_seconds == 60
    assert settings.clock_sample_max_age_seconds == 15
    assert settings.clock_bootstrap_seconds == 600
    assert settings.history_retry_seconds == 2
    assert settings.floor_ttl_seconds == 60
    assert (settings.hub_port, settings.chart_idle_seconds) == (9100, 60)


@pytest.mark.parametrize("name", ["MT5_TERMINAL_PATH", "MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"])
def test_settings_refuse_a_missing_variable(name):
    environment = dict(ENVIRONMENT)
    del environment[name]
    with pytest.raises(SettingsError, match=name):
        read_settings(environment)


@pytest.mark.parametrize(
    ("variable", "value", "field"),
    [
        ("MT5_API_THREADS", "0", "api_threads"),
        ("MT5_API_THREADS", "1", "api_threads"),
        ("MT5_API_PORT", "0", "api_port"),
        ("MT5_API_PORT", "65536", "api_port"),
        ("MT5_LOGIN_TIMEOUT_MS", "0", "login_timeout_ms"),
        ("MT5_LOGIN_TIMEOUT_MS", "-1", "login_timeout_ms"),
        ("MT5_CLOCK_CHECK_SECONDS", "0", "clock_check_seconds"),
        ("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "0", "clock_sample_max_age_seconds"),
        ("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "-1", "clock_sample_max_age_seconds"),
        ("MT5_CLOCK_BOOTSTRAP_SECONDS", "0", "clock_bootstrap_seconds"),
        ("MT5_CLOCK_BOOTSTRAP_SECONDS", "-1", "clock_bootstrap_seconds"),
        ("MT5_HISTORY_RETRY_SECONDS", "0", "history_retry_seconds"),
        ("MT5_HISTORY_RETRY_SECONDS", "-1", "history_retry_seconds"),
        ("MT5_FLOOR_TTL_SECONDS", "0", "floor_ttl_seconds"),
        ("MT5_FLOOR_TTL_SECONDS", "-1", "floor_ttl_seconds"),
        ("MT5_HUB_PORT", "0", "hub_port"),
        ("MT5_HUB_PORT", "65536", "hub_port"),
        ("MT5_CHART_IDLE_SECONDS", "0", "chart_idle_seconds"),
        ("MT5_CHART_IDLE_SECONDS", "-1", "chart_idle_seconds"),
    ],
)
def test_settings_refuse_an_out_of_range_value(variable, value, field):
    with pytest.raises(SettingsError, match=field):
        read_settings(ENVIRONMENT | {variable: value})


def test_settings_accept_two_api_threads():
    assert read_settings(ENVIRONMENT | {"MT5_API_THREADS": "2"}).api_threads == 2


def test_settings_built_directly_refuse_an_out_of_range_value():
    with pytest.raises(SettingsError, match="api_threads"):
        Settings(
            terminal_path="C:\\Program Files\\MetaTrader 5\\terminal64.exe",
            login=12345678,
            password="secret-password",
            server="example-server",
            login_timeout_ms=60000,
            api_host="0.0.0.0",
            api_port=5000,
            api_threads=0,
            broker_tz=ZoneInfo("America/New_York"),
            broker_offset_hours=7,
            clock_check_seconds=300,
            clock_sample_max_age_seconds=30,
            clock_bootstrap_seconds=120,
            history_retry_seconds=5,
            floor_ttl_seconds=900,
            hub_port=9000,
            chart_idle_seconds=900,
        )


@pytest.mark.parametrize("value", ["Mars/Olympus", "", "../etc/zone"])
def test_settings_refuse_an_unknown_broker_zone(value):
    with pytest.raises(SettingsError, match="MT5_BROKER_TZ"):
        read_settings(ENVIRONMENT | {"MT5_BROKER_TZ": value})


@pytest.mark.parametrize(
    "variable",
    [
        "MT5_BROKER_OFFSET_HOURS",
        "MT5_CLOCK_CHECK_SECONDS",
        "MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS",
        "MT5_CLOCK_BOOTSTRAP_SECONDS",
    ],
)
@pytest.mark.parametrize("value", ["7.5", "seven"])
def test_settings_refuse_a_clock_setting_that_is_not_an_integer(variable, value):
    with pytest.raises(SettingsError, match=f"{variable} is not an integer"):
        read_settings(ENVIRONMENT | {variable: value})


@pytest.mark.parametrize("variable", ["MT5_HISTORY_RETRY_SECONDS", "MT5_FLOOR_TTL_SECONDS"])
@pytest.mark.parametrize("value", ["2.5", "five", ""])
def test_settings_refuse_a_history_setting_that_is_not_an_integer(variable, value):
    with pytest.raises(SettingsError, match=f"{variable} is not an integer"):
        read_settings(ENVIRONMENT | {variable: value})


def test_settings_refuse_a_login_that_is_not_an_integer_without_echoing_it():
    with pytest.raises(SettingsError, match="MT5_LOGIN is not an integer") as refusal:
        read_settings(ENVIRONMENT | {"MT5_LOGIN": "acct-98765"})
    assert "98765" not in str(refusal.value)


def test_settings_repr_carries_no_login_values():
    text = repr(read_settings(ENVIRONMENT))
    assert "12345678" not in text
    assert "secret-password" not in text
    assert "example-server" not in text


def test_hub_settings_read_the_environment_with_defaults():
    settings = read_hub_settings({})
    assert (settings.hub_port, settings.api_port) == (9000, 5000)
    assert settings.broker_tz == ZoneInfo("America/New_York")
    assert settings.broker_offset_hours == 7
    assert settings.chart_idle_seconds == 900


def test_hub_settings_take_overrides():
    settings = read_hub_settings(
        {
            "MT5_HUB_PORT": "9100",
            "MT5_API_PORT": "5100",
            "MT5_BROKER_TZ": "Europe/Helsinki",
            "MT5_BROKER_OFFSET_HOURS": "0",
            "MT5_CHART_IDLE_SECONDS": "60",
        }
    )
    assert (settings.hub_port, settings.api_port) == (9100, 5100)
    assert (settings.broker_tz, settings.broker_offset_hours) == (ZoneInfo("Europe/Helsinki"), 0)
    assert settings.chart_idle_seconds == 60


def test_the_server_and_the_hub_read_the_hubs_port_and_idle_period_from_one_variable_each():
    environment = ENVIRONMENT | {"MT5_HUB_PORT": "9100", "MT5_CHART_IDLE_SECONDS": "60"}
    server = read_settings(environment)
    hub = read_hub_settings(environment)
    assert (server.hub_port, server.chart_idle_seconds) == (hub.hub_port, hub.chart_idle_seconds)
    assert (hub.hub_port, hub.chart_idle_seconds) == (9100, 60)


@pytest.mark.parametrize(
    ("variable", "value", "message"),
    [
        ("MT5_HUB_PORT", "0", "hub_port"),
        ("MT5_API_PORT", "65536", "api_port"),
        ("MT5_HUB_PORT", "nine", "MT5_HUB_PORT is not an integer"),
        ("MT5_CHART_IDLE_SECONDS", "0", "chart_idle_seconds"),
        ("MT5_BROKER_OFFSET_HOURS", "7.5", "MT5_BROKER_OFFSET_HOURS is not an integer"),
        ("MT5_BROKER_TZ", "Mars/Olympus", "MT5_BROKER_TZ"),
    ],
)
def test_hub_settings_refuse_a_bad_value(variable, value, message):
    with pytest.raises(SettingsError, match=message):
        read_hub_settings({variable: value})


def test_start_initializes_the_configured_terminal_once(
    stub, commissions, server_times, clock_status
):
    stub.initialize.return_value = True
    stub.positions_total.return_value = 0
    terminal = Terminal(stub)

    repeated_hours = RepeatedHours()
    history = History(terminal, CLOCK, repeated_hours, FloorStore(), retry_s=0.01, floor_ttl_s=900)

    connect_terminal(terminal, read_settings(ENVIRONMENT))
    client = create_app(
        terminal,
        commissions,
        CLOCK,
        repeated_hours,
        server_times,
        clock_status,
        history,
        publishers_on(ChartPosts()),
        workers=3,
        retry_s=5,
    ).test_client()
    for _ in range(3):
        assert client.get("/health").status_code == 200
        assert client.post("/mt5/positions_total").status_code == 200

    stub.initialize.assert_called_once_with(
        "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
        login=12345678,
        password="secret-password",
        server="example-server",
        timeout=60000,
    )


def test_start_fails_with_the_packages_last_error(stub):
    stub.initialize.return_value = False
    stub.last_error.return_value = (-6, "Terminal: Authorization failed")
    with pytest.raises(TerminalStartError, match="-6") as failure:
        connect_terminal(Terminal(stub), read_settings(ENVIRONMENT))
    assert "Terminal: Authorization failed" in str(failure.value)
    assert "12345678" not in str(failure.value)


def test_health_answers_the_verification_without_calling_the_terminal(client, stub):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json == {
        "ok": True,
        "result": {
            "symbol": "EURUSD",
            "trade_server": 1_752_570_000,
            "current": 1_752_569_998,
            "gmt": 1_752_570_000,
            "skew_s": -30,
            "offset_s": 10_800,
            "in_flight": 0,
            "peak_in_flight": 0,
            "refusals": 0,
            "workers": 3,
        },
    }
    assert stub.mock_calls == []


def test_health_is_unavailable_while_the_clock_is_not_verified(client, stub, clock_status):
    clock_status.clear()

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": "the broker clock is not verified"},
    }
    assert stub.mock_calls == []


def test_commissions_without_a_relayed_schedule_defer_the_read(client):
    response = client.get("/commissions/EURUSD.a")
    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert response.json == {
        "ok": False,
        "error": {"code": -20001, "message": "no commission schedule relayed for EURUSD.a"},
    }


def test_commissions_answer_the_relayed_schedule(client, commissions):
    commissions.write(
        "EURUSD.a",
        CommissionSchedule(
            ret=1,
            last_error=0,
            rules=(
                CommissionRule(
                    currency="USD",
                    mode_range=CommissionRangeMode.VOLUME,
                    mode_charge=CommissionChargeMode.INSTANT,
                    mode_entry=CommissionEntryMode.INOUT,
                    mode_direction=CommissionDirectionMode.BOTH,
                    mode_profit=CommissionProfitMode.ALL,
                    tiers=(
                        CommissionTier(
                            mode=CommissionMode.MONEY_DEPOSIT,
                            volume_type=CommissionVolumeType.VOLUME,
                            value=3.5,
                            min_value=0.0,
                            max_value=0.0,
                            range_from=0.0,
                            range_to=1000000.0,
                            currency="USD",
                        ),
                    ),
                ),
            ),
        ),
    )

    response = client.get("/commissions/EURUSD.a")

    assert response.json == {
        "ok": True,
        "result": {
            "ret": 1,
            "last_error": 0,
            "rules": [
                {
                    "currency": "USD",
                    "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
                    "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
                    "mode_entry": "SYMBOL_COMMISSION_ENTRY_INOUT",
                    "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
                    "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
                    "tiers": [
                        {
                            "mode": "SYMBOL_COMMISSION_MONEY_DEPOSIT",
                            "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
                            "value": 3.5,
                            "min_value": 0,
                            "max_value": 0,
                            "range_from": 0,
                            "range_to": 1000000,
                            "currency": "USD",
                        }
                    ],
                }
            ],
        },
    }
