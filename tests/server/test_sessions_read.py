"""The shim's read of the trading sessions the server relays, through waitress and the real routes,
and the instrument info the provider builds from them."""

from unittest.mock import MagicMock

import pytest
from mirror_samples import struct_sample
from nautilus_trader.common.component import TestClock

from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.errors import MT5InstrumentError
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.server.commissions import CommissionSchedule
from mt5connector.server.sessions import TradingSessions
from mt5connector.server.wire import mirror
from mt5connector.server.ws_server import SESSIONS_RELAY_PATH, post_to

XAUUSD_RELAY_PATH = f"{SESSIONS_RELAY_PATH}/XAUUSD.a"
# Sunday first: a session from 18:00 Sunday to 16:57 Friday, broken from 16:59 to 18:00 each day.
QUOTE = [
    [[64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61020]],
    [],
]
TRADE = [
    [[65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 60900]],
    [],
]
FRAME = {
    "v": 1,
    "type": "sessions",
    "symbol": "XAUUSD.a",
    "quote": QUOTE,
    "trade": TRADE,
    "last_error": 0,
}
KEPT = TradingSessions(
    quote=tuple(tuple(tuple(session) for session in day) for day in QUOTE),
    trade=tuple(tuple(tuple(session) for session in day) for day in TRADE),
    last_error=0,
)


def test_a_symbol_with_no_relayed_sessions_retries_twice_then_reads_them(
    remote, sessions, monkeypatch
):
    sleeps = []
    get = MagicMock(wraps=remote._session.get)
    monkeypatch.setattr(remote._session, "get", get)

    def wait(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            sessions.write("XAUUSD.a", KEPT)

    monkeypatch.setattr("time.sleep", wait)
    before = remote.last_error()

    assert remote.trading_sessions("XAUUSD.a") == {
        "quote": QUOTE,
        "trade": TRADE,
        "last_error": 0,
    }
    assert sleeps == [5.0, 5.0]
    assert get.call_count == 3
    assert remote.last_error() == before


@pytest.fixture
def provider(stub, remote, commissions):
    """A provider over the served app, XAUUSD.a charging no commission."""
    commissions.write("XAUUSD.a", CommissionSchedule(ret=0, last_error=0, rules=()))
    info = struct_sample(mirror.StructName.SYMBOL_INFO)._replace(
        name="XAUUSD.a",
        digits=2,
        point=0.01,
        trade_tick_size=0.01,
        volume_min=0.01,
        volume_max=100.0,
        volume_step=0.01,
        trade_contract_size=100.0,
        currency_base="XAU",
        currency_profit="USD",
        currency_margin="XAU",
        trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        trade_mode=mirror.SYMBOL_TRADE_MODE_FULL,
        chart_mode=mirror.SYMBOL_CHART_MODE_BID,
    )
    stub.symbol_select.return_value = True
    stub.symbol_info.return_value = info
    stub.symbols_get.return_value = (info,)
    connection = MagicMock(mt5=remote)
    connection.get_account_info.return_value = MagicMock(currency="USD", currency_digits=2)
    return MT5InstrumentProvider(connection=connection, venue=MT5_VENUE, clock=TestClock())


def test_a_load_waits_for_the_relayed_sessions_and_carries_them_in_its_info(
    served, provider, monkeypatch
):
    sleeps = []

    def wait(seconds):
        sleeps.append(seconds)
        assert provider.get_instrument("XAUUSD.a") is None
        post_to(served)(XAUUSD_RELAY_PATH, FRAME)

    monkeypatch.setattr("time.sleep", wait)
    instrument = provider.load_symbol("XAUUSD.a")

    assert sleeps == [5.0]
    assert instrument.info["quote_sessions"] == QUOTE
    assert instrument.info["trade_sessions"] == TRADE
    assert (
        instrument.info["chart_mode"],
        instrument.info["bar_tz"],
        instrument.info["bar_day_open"],
    ) == ("BID", "America/New_York", "17:00")


def test_a_refused_relay_fails_the_load_naming_the_symbol_until_a_frame_is_accepted(
    served, provider
):
    post_to(served)(XAUUSD_RELAY_PATH, FRAME | {"quote": QUOTE[:6]})

    with pytest.raises(MT5InstrumentError) as refused:
        provider.load_symbol("XAUUSD.a")
    post_to(served)(XAUUSD_RELAY_PATH, FRAME)
    instrument = provider.load_symbol("XAUUSD.a")

    assert str(refused.value) == (
        "XAUUSD.a: sessions: the sessions relay for XAUUSD.a was refused: "
        "quote holds 6 days, not 7"
    )
    assert instrument.info["quote_sessions"] == QUOTE
