"""The hub's settings, read from the environment."""

from zoneinfo import ZoneInfo

import pytest

from mt5connector.server.settings import SettingsError, read_hub_settings


def test_hub_settings_read_the_environment_with_defaults():
    settings = read_hub_settings({})
    assert settings.hub_port == 9000
    assert settings.broker_tz == ZoneInfo("America/New_York")
    assert settings.broker_offset_hours == 7
    assert settings.chart_idle_seconds == 900


def test_hub_settings_take_overrides():
    settings = read_hub_settings(
        {
            "MT5_HUB_PORT": "9100",
            "MT5_BROKER_TZ": "Europe/Helsinki",
            "MT5_BROKER_OFFSET_HOURS": "0",
            "MT5_CHART_IDLE_SECONDS": "60",
        }
    )
    assert settings.hub_port == 9100
    assert (settings.broker_tz, settings.broker_offset_hours) == (ZoneInfo("Europe/Helsinki"), 0)
    assert settings.chart_idle_seconds == 60


@pytest.mark.parametrize("value", ["Mars/Olympus", "", "../etc/zone"])
def test_hub_settings_refuse_an_unknown_broker_zone(value):
    with pytest.raises(SettingsError, match="MT5_BROKER_TZ"):
        read_hub_settings({"MT5_BROKER_TZ": value})


@pytest.mark.parametrize(
    "variable",
    [
        "MT5_BROKER_OFFSET_HOURS",
        "MT5_CLOCK_CHECK_SECONDS",
        "MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS",
        "MT5_CLOCK_BOOTSTRAP_SECONDS",
        "MT5_HISTORY_RETRY_SECONDS",
    ],
)
@pytest.mark.parametrize("value", ["7.5", "seven", ""])
def test_hub_settings_refuse_a_clock_setting_that_is_not_an_integer(variable, value):
    with pytest.raises(SettingsError, match=f"^{variable}=.* is not an integer$"):
        read_hub_settings({variable: value})


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("MT5_HUB_PORT", "0"),
        ("MT5_HUB_PORT", "65536"),
        ("MT5_HUB_PORT", "nine"),
        ("MT5_BROKER_TZ", "Mars/Olympus"),
        ("MT5_BROKER_OFFSET_HOURS", "7.5"),
        ("MT5_CHART_IDLE_SECONDS", "0"),
        ("MT5_CHART_IDLE_SECONDS", "idle"),
        ("MT5_HISTORY_RETRY_SECONDS", "0"),
        ("MT5_HISTORY_RETRY_SECONDS", "-5"),
        ("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "0"),
        ("MT5_CLOCK_BOOTSTRAP_SECONDS", "0"),
        ("MT5_CLOCK_CHECK_SECONDS", "0"),
        ("MT5_HUB_CONSUMER_QUEUE_BOUND", "0"),
        ("MT5_HUB_CONSUMER_QUEUE_BOUND", "many"),
        ("MT5_HUB_EA_QUEUE_BOUND", "0"),
        ("MT5_HUB_PENDING_BOUND", "0"),
    ],
)
def test_hub_settings_name_the_variable_and_its_value_when_refusing(variable, value):
    with pytest.raises(SettingsError) as refused:
        read_hub_settings({variable: value})
    assert str(refused.value).startswith(f"{variable}=")
    assert value in str(refused.value)
