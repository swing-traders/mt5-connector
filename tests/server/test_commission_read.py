"""The shim's read of the commission schedules the server relays, through waitress and the real
routes, and the taker fee the client derives from one."""

import dataclasses
import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from mirror_samples import struct_sample
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.identifiers import InstrumentId

from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.errors import MT5InstrumentError, ServerBusy, ServerUnreachable
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.server.commissions import CommissionRule, CommissionSchedule, CommissionTier
from mt5connector.server.wire import mirror
from mt5connector.server.wire.push_wire import (
    ChartState,
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
)
from mt5connector.server.ws_server import COMMISSIONS_RELAY_PATH, SERVER_TIME_RELAY_PATH, post_to

EURUSD_RELAY_PATH = f"{COMMISSIONS_RELAY_PATH}/EURUSD.a"
IC_FRAME = {
    "v": 1,
    "type": "commissions",
    "symbol": "EURUSD.a",
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
}
EURUSD_SERVER_TIME = {
    "v": 1,
    "type": "server_time",
    "symbol": "EURUSD.a",
    "trade_server": 1_752_580_800,
    "current": 1_752_580_798,
    "gmt": 1_752_570_000,
    "connected": 1,
}

SCHEDULE = CommissionSchedule(
    ret=1,
    last_error=0,
    rules=(
        CommissionRule(
            currency="UST",
            mode_range=CommissionRangeMode.VOLUME,
            mode_charge=CommissionChargeMode.INSTANT,
            mode_entry=CommissionEntryMode.IN,
            mode_direction=CommissionDirectionMode.BOTH,
            mode_profit=CommissionProfitMode.ALL,
            tiers=(
                CommissionTier(
                    mode=CommissionMode.MONEY_DEPOSIT,
                    volume_type=CommissionVolumeType.VOLUME,
                    value=6.0,
                    min_value=0.0,
                    max_value=0.0,
                    range_from=0.0,
                    range_to=1000.0,
                    currency="UST",
                ),
            ),
        ),
    ),
)


def test_a_relayed_schedule_is_read_as_the_server_keeps_it(remote, commissions):
    commissions.write("EURUSD+", SCHEDULE)
    as_served = json.loads(json.dumps(dataclasses.asdict(SCHEDULE)))
    assert remote.commission_schedule("EURUSD+") == as_served


def test_a_symbol_with_no_relayed_schedule_retries_twice_then_reads_it(
    remote, commissions, monkeypatch
):
    sleeps = []
    get = MagicMock(wraps=remote._session.get)
    monkeypatch.setattr(remote._session, "get", get)

    def wait(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            commissions.write("EURUSD.a", SCHEDULE)

    monkeypatch.setattr("time.sleep", wait)
    before = remote.last_error()
    assert remote.commission_schedule("EURUSD.a") == json.loads(
        json.dumps(dataclasses.asdict(SCHEDULE))
    )
    assert sleeps == [5.0, 5.0]
    assert get.call_count == 3
    assert remote.last_error() == before


@pytest.mark.parametrize("retry_after", [None, "-1", "nan", "inf", "later"])
def test_a_deferred_read_without_a_valid_delay_raises(remote, monkeypatch, retry_after):
    response = MagicMock(status_code=503)
    response.json.return_value = {
        "ok": False,
        "error": {"code": -20001, "message": "no commission schedule relayed for EURUSD.a"},
    }
    response.headers = {} if retry_after is None else {"Retry-After": retry_after}
    get = MagicMock(return_value=response)
    monkeypatch.setattr(remote._session, "get", get)

    with pytest.raises(ServerUnreachable, match="Retry-After"):
        remote.commission_schedule("EURUSD.a")
    assert get.call_count == 1


def test_a_read_while_the_clock_is_not_verified_raises_server_unreachable(remote, clock_status):
    clock_status.clear()
    with pytest.raises(ServerUnreachable):
        remote.commission_schedule("EURUSD+")


@pytest.mark.parametrize("workers", [2])
def test_a_read_the_server_is_busy_for_raises_server_busy_and_leaves_last_error(
    remote, commissions, held
):
    commissions.write("EURUSD+", SCHEDULE)
    held.take(1, "/mt5/positions_total")
    before = remote.last_error()

    with pytest.raises(ServerBusy, match="^commissions: server busy$"):
        remote.commission_schedule("EURUSD+")
    assert remote.last_error() == before


@pytest.fixture
def provider(stub):
    info = struct_sample(mirror.StructName.SYMBOL_INFO)._replace(
        name="EURUSD.a",
        digits=5,
        point=0.00001,
        trade_tick_size=0.00001,
        volume_min=0.01,
        volume_max=500.0,
        volume_step=0.01,
        trade_contract_size=100000.0,
        currency_base="EUR",
        currency_profit="USD",
        currency_margin="EUR",
        trade_calc_mode=mirror.SYMBOL_CALC_MODE_FOREX,
        trade_mode=mirror.SYMBOL_TRADE_MODE_FULL,
        chart_mode=mirror.SYMBOL_CHART_MODE_BID,
    )
    stub.symbol_select.return_value = True
    stub.symbol_info.return_value = info
    stub.symbols_get.return_value = (info,)
    stub.symbol_info_tick.return_value = struct_sample(mirror.StructName.TICK)._replace(
        bid=1.085, ask=1.085
    )
    connection = MagicMock()
    connection.get_account_info.return_value = MagicMock(currency="USD", currency_digits=2)
    return MT5InstrumentProvider(connection=connection, venue=MT5_VENUE, clock=TestClock())


def test_a_relayed_empty_schedule_loads_a_zero_fee(remote, served, provider):
    post_to(served)(
        EURUSD_RELAY_PATH,
        {
            "v": 1,
            "type": "commissions",
            "symbol": "EURUSD.a",
            "ret": 0,
            "last_error": 0,
            "rules": [],
        },
    )

    assert remote.commission_schedule("EURUSD.a") == {"ret": 0, "last_error": 0, "rules": []}
    assert provider.load_symbol("EURUSD.a").taker_fee == Decimal(0)


def test_a_relayed_ic_schedule_derives_its_fee_per_fill_from_its_names(
    remote, served, provider, monkeypatch
):
    sleeps = []

    def wait(seconds):
        sleeps.append(seconds)
        assert provider.get_instrument("EURUSD.a") is None
        post_to(served)(EURUSD_RELAY_PATH, IC_FRAME)

    monkeypatch.setattr("time.sleep", wait)
    fee = provider.load_symbol("EURUSD.a").taker_fee

    assert sleeps == [5.0]
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")


async def test_a_load_of_a_symbol_no_ea_publishes_waits_for_its_chart_and_loads_the_relayed_fee(
    remote, served, provider, chart_posts, monkeypatch
):
    chart_posts.state = ChartState.REQUESTED
    sleeps = []

    def wait(seconds):
        # The chart the post requested opens, and its EA relays its sample and schedule.
        sleeps.append(seconds)
        post_to(served)(SERVER_TIME_RELAY_PATH, EURUSD_SERVER_TIME)
        post_to(served)(EURUSD_RELAY_PATH, IC_FRAME)

    monkeypatch.setattr("time.sleep", wait)
    await provider.load_ids_async([InstrumentId.from_str("EURUSD.a.MT5")])
    fee = provider.get_instrument("EURUSD.a").taker_fee

    assert chart_posts.posted == ["EURUSD.a"]
    assert sleeps == [5.0]
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")


def test_a_refused_relay_fails_the_load_naming_the_symbol_until_a_frame_is_accepted(
    remote, served, provider
):
    post_to(served)(EURUSD_RELAY_PATH, IC_FRAME | {"rules": None})

    with pytest.raises(MT5InstrumentError, match="EURUSD.a") as refused:
        provider.load_symbol("EURUSD.a")
    post_to(served)(EURUSD_RELAY_PATH, IC_FRAME)
    fee = provider.load_symbol("EURUSD.a").taker_fee

    assert str(refused.value) == (
        "EURUSD.a: commissions: the commission relay for EURUSD.a was refused: "
        "rules is not a list: None"
    )
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")
