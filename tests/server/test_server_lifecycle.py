"""The server's settings, start, liveness and relayed commission schedules."""

import logging

import pytest
from mirror_samples import expected_json, struct_sample

from mt5connect import mirror
from mt5server.app.app import TerminalStartError, connect_terminal, create_app
from mt5server.app.commissions import CommissionRule, CommissionSchedule, CommissionTier
from mt5server.app.settings import Settings, SettingsError, read_settings
from mt5server.app.terminal import Terminal

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
    assert settings.api_threads == 4


def test_settings_take_overrides():
    settings = read_settings(
        ENVIRONMENT
        | {
            "MT5_LOGIN_TIMEOUT_MS": "90000",
            "MT5_API_HOST": "127.0.0.1",
            "MT5_API_PORT": "5100",
            "MT5_API_THREADS": "8",
        }
    )
    assert (settings.login_timeout_ms, settings.api_host, settings.api_port) == (
        90000,
        "127.0.0.1",
        5100,
    )
    assert settings.api_threads == 8


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
        ("MT5_API_PORT", "0", "api_port"),
        ("MT5_API_PORT", "65536", "api_port"),
        ("MT5_LOGIN_TIMEOUT_MS", "0", "login_timeout_ms"),
        ("MT5_LOGIN_TIMEOUT_MS", "-1", "login_timeout_ms"),
    ],
)
def test_settings_refuse_an_out_of_range_value(variable, value, field):
    with pytest.raises(SettingsError, match=field):
        read_settings(ENVIRONMENT | {variable: value})


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
        )


def test_settings_refuse_a_login_that_is_not_an_integer_without_echoing_it():
    with pytest.raises(SettingsError, match="MT5_LOGIN is not an integer") as refusal:
        read_settings(ENVIRONMENT | {"MT5_LOGIN": "acct-98765"})
    assert "98765" not in str(refusal.value)


def test_settings_repr_carries_no_login_values():
    text = repr(read_settings(ENVIRONMENT))
    assert "12345678" not in text
    assert "secret-password" not in text
    assert "example-server" not in text


def test_start_initializes_the_configured_terminal_once(stub, commissions):
    stub.initialize.return_value = True
    stub.terminal_info.return_value = struct_sample(mirror.StructName.TERMINAL_INFO)
    terminal = Terminal(stub)

    connect_terminal(terminal, read_settings(ENVIRONMENT))
    client = create_app(terminal, commissions).test_client()
    for _ in range(3):
        assert client.get("/health").status_code == 200

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


def test_health_answers_the_terminal_info(client, stub):
    info = struct_sample(mirror.StructName.TERMINAL_INFO)._replace(connected=True)
    stub.terminal_info.return_value = info

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json == {
        "ok": True,
        "result": expected_json(info),
        "last_error": [1, "Success"],
    }
    stub.initialize.assert_not_called()


def test_health_is_unavailable_when_the_terminal_does_not_answer(client, stub):
    stub.terminal_info.return_value = None
    stub.last_error.return_value = (-10004, "No IPC connection")

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json == {
        "ok": False,
        "error": {"code": -10004, "message": "No IPC connection"},
        "last_error": [-10004, "No IPC connection"],
    }
    stub.initialize.assert_not_called()


def test_health_logs_the_broker_connection_when_it_changes(client, stub, caplog):
    info = struct_sample(mirror.StructName.TERMINAL_INFO)
    caplog.set_level(logging.INFO, logger="mt5server.app.app")
    for connected in (True, True, False, False, True):
        stub.terminal_info.return_value = info._replace(connected=connected)
        client.get("/health")
    assert [record.getMessage() for record in caplog.records] == [
        "terminal broker connection: connected=True",
        "terminal broker connection: connected=False",
        "terminal broker connection: connected=True",
    ]


def test_commissions_without_a_relayed_schedule_answer_not_found(client):
    response = client.get("/commissions/EURUSD.a")
    assert response.status_code == 200
    assert response.json == {
        "ok": False,
        "error": {"code": -4, "message": "no commission schedule relayed for EURUSD.a"},
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
                    mode_range=0,
                    mode_charge=2,
                    mode_entry=0,
                    mode_direction=0,
                    mode_profit=0,
                    tiers=(
                        CommissionTier(
                            mode=0,
                            volume_type=1,
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
                    "mode_range": 0,
                    "mode_charge": 2,
                    "mode_entry": 0,
                    "mode_direction": 0,
                    "mode_profit": 0,
                    "tiers": [
                        {
                            "mode": 0,
                            "volume_type": 1,
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
