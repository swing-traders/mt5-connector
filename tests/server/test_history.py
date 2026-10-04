"""The history protocol, its routes and its client, against a package double serving one symbol's
history as the terminal does."""

import calendar
import logging
import threading
import time
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPMethod, HTTPStatus
from unittest.mock import MagicMock

import numpy as np
import pytest
from mirror_samples import CLOCK, struct_sample
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from saturation import Held, counters, until

import mt5connector.client.history as history_client
import mt5connector.wire.history_wire as wire
from mt5connector.client.downloader import MT5DataDownloader
from mt5connector.client.errors import ServerUnreachable
from mt5connector.server import history as history_module
from mt5connector.server.history import FloorOrigin
from mt5connector.server.server_time import ServerTimeSample, ServerTimeSink
from mt5connector.server.wire import mirror
from mt5connector.server.wire.history_wire import Series

RATES = np.dtype(list(mirror.RATES.dtype))
TICKS = np.dtype(list(mirror.TICKS.dtype))
SUCCESS = (1, "Success")
MINUTE = 60
HOUR = 3_600
DAY = 86_400
EDT = 10_800
EST = 7_200
H1 = mirror.TIMEFRAME_H1
M1 = mirror.TIMEFRAME_M1
INFO = mirror.COPY_TICKS_INFO
UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}


def broker(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    """A broker epoch: the broker's wall time written as if it were UTC."""
    return calendar.timegm((year, month, day, hour, minute, 0))


# The trade-server time the relay last carried: broker 2026-01-15 12:00, under EST.
NOW = broker(2026, 1, 15, 12)
NOW_UTC = NOW - EST
# The span of one read at the double's MaxBars of 100,000.
H1_SPAN = 99_989 * HOUR
M1_SPAN = 99_989 * MINUTE


def rates(*times: int) -> np.ndarray:
    return np.array([(t, 1.1, 1.2, 1.0, 1.15, 10, 2, 0) for t in times], dtype=RATES)


def ticks(*times_msc: int) -> np.ndarray:
    return np.array([(t // 1000, 1.1, 1.1002, 0.0, 0, t, 6, 0.0) for t in times_msc], dtype=TICKS)


def bar_json(utc: int) -> dict[str, object]:
    return {
        "time": utc,
        "open": 1.1,
        "high": 1.2,
        "low": 1.0,
        "close": 1.15,
        "tick_volume": 10,
        "spread": 2,
        "real_volume": 0,
    }


def tick_json(utc_msc: int) -> dict[str, object]:
    return {
        "time": utc_msc // 1000,
        "bid": 1.1,
        "ask": 1.1002,
        "last": 0.0,
        "volume": 0,
        "time_msc": utc_msc,
        "flags": 6,
        "volume_real": 0.0,
    }


def ok(rows: list) -> dict[str, object]:
    return {"ok": True, "result": rows, "last_error": list(SUCCESS)}


def syncing(message: str) -> dict[str, object]:
    code = int(wire.ServerCode.SYNCING)
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


def forbidden_sleep(seconds: float) -> None:
    raise AssertionError(f"slept {seconds} s")


@dataclass(frozen=True)
class Call:
    name: str
    arguments: tuple
    at: float


class Package:
    """A MetaTrader5 package double serving one symbol's history the way the terminal does, and
    any answers scripted ahead of it."""

    def __init__(self) -> None:
        self.rates: dict[int, np.ndarray] = {}
        self.ticks = ticks()
        self.maxbars = 100_000
        self.last_quote = 0
        self.delay_s = 0.0
        self.syncing = lambda: False
        self.scripted: dict[str, list] = defaultdict(list)
        self.calls: list[Call] = []

    def script(self, name: str, *answers: tuple) -> None:
        self.scripted[name].extend(answers)

    def windows(self, name: str) -> list[tuple]:
        return [call.arguments for call in self.calls if call.name == name]

    def names(self) -> list[str]:
        return [call.name for call in self.calls]

    def symbol_select(self, symbol, enable):
        return self._answer("symbol_select", (symbol, enable), lambda: True)

    def terminal_info(self):
        info = struct_sample(mirror.StructName.TERMINAL_INFO)._replace(maxbars=self.maxbars)
        return self._answer("terminal_info", (), lambda: info)

    def symbol_info_tick(self, symbol):
        tick = struct_sample(mirror.StructName.TICK)._replace(
            time=self.last_quote, time_msc=self.last_quote * 1000
        )
        return self._answer("symbol_info_tick", (symbol,), lambda: tick)

    def version(self):
        return self._answer("version", (), lambda: (500, 4321, "27 Sep 2026"))

    def copy_rates_range(self, symbol, timeframe, date_from, date_to):
        lo, hi = int(date_from.timestamp()), int(date_to.timestamp())
        return self._answer(
            "copy_rates_range", (timeframe, lo, hi), lambda: self._rates(timeframe, lo, hi)
        )

    def copy_rates_from(self, symbol, timeframe, date_from, count):
        at = int(date_from.timestamp())
        return self._answer(
            "copy_rates_from",
            (timeframe, at, count),
            lambda: self._rates_from(timeframe, at, count),
        )

    def copy_ticks_range(self, symbol, date_from, date_to, flags):
        lo, hi = round(date_from.timestamp() * 1000), round(date_to.timestamp() * 1000)
        return self._answer("copy_ticks_range", (lo, hi, flags), lambda: self._ticks(lo, hi))

    def copy_ticks_from(self, symbol, date_from, count, flags):
        lo = round(date_from.timestamp() * 1000)
        return self._answer(
            "copy_ticks_from", (lo, count, flags), lambda: self._ticks(lo, None)[:count]
        )

    def last_error(self):
        return self._error

    def _answer(self, name, arguments, natural):
        self.calls.append(Call(name, arguments, time.monotonic()))
        if len(self.calls) > 60:
            raise AssertionError("the protocol kept calling the package")
        if self.delay_s:
            time.sleep(self.delay_s)
        if self.scripted[name]:
            value, self._error = self.scripted[name].pop(0)
        else:
            value, self._error = natural(), SUCCESS
        return value

    def _rates(self, timeframe, lo, hi):
        series = self.rates.get(timeframe, rates())
        period = {M1: MINUTE, H1: HOUR}[timeframe]
        if self.syncing() or hi - lo > (self.maxbars - 11) * period:
            return series[:0]
        elif len(series) > 0 and hi < series["time"][0]:
            return series[:1]
        else:
            return series[(series["time"] >= lo) & (series["time"] <= hi)]

    def _rates_from(self, timeframe, at, count):
        # The package answers the last `count` bars opened at or before the date.
        series = self.rates.get(timeframe, rates())
        if self.syncing():
            return series[:0]
        else:
            opened = series[series["time"] <= at]
            return opened[max(0, len(opened) - count) :]

    def _ticks(self, lo, hi):
        times = self.ticks["time_msc"]
        if self.syncing():
            return self.ticks[:0]
        elif hi is None:
            return self.ticks[times >= lo]
        else:
            return self.ticks[(times >= lo) & (times <= hi)]


@pytest.fixture
def stub():
    return Package()


@pytest.fixture
def server_times():
    """The relay's latest sample, carrying the trade server at NOW."""
    sink = ServerTimeSink(max_age_s=30)
    sink.write(ServerTimeSample("EURUSD", NOW, NOW, NOW - EST, True))
    return sink


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", forbidden_sleep)


@pytest.fixture
def cold_reads(served, stub):
    """Window reads held in the terminal until released, as a cold read holds it, the first in the
    package and the rest waiting on the terminal behind it."""
    held = Held(served)
    stub.syncing = lambda: not held.released.wait(5)
    yield held
    held.release()


def post_bars(client, series: str, start: int, end: int):
    body = {"symbol": "EURUSD", "timeframe": series, "start": start, "end": end}
    return client.post("/history/bars", json=body)


def post_ticks(client, start: int, end: int):
    return client.post("/history/ticks", json={"symbol": "EURUSD", "start": start, "end": end})


def kept(floors, series: Series, value: int) -> None:
    floors.write("EURUSD", series, value, int(time.time()), FloorOrigin.STUB)


def advertised(client, series: str) -> dict[str, int]:
    return client.get("/history/ranges?symbol=EURUSD").json["result"]["ranges"][series]


# ── Chunking ──────────────────────────────────────────────────────────────────


def test_a_window_one_period_over_the_cap_is_read_in_two_spans_within_it(client, stub, floors):
    first = broker(2025, 7, 15, 12)
    last = first + 99_990 * HOUR
    middle = first + 49_995 * HOUR
    times = [first, first + HOUR, *(middle + k * HOUR for k in range(-2, 2)), last - HOUR, last]
    stub.rates[H1] = rates(*times)
    kept(floors, Series.H1, first)

    response = post_bars(client, "H1", CLOCK.to_utc(first), CLOCK.to_utc(last))

    reads = stub.windows("copy_rates_range")
    assert len(reads) == 2
    assert all(hi - lo <= 99_989 * HOUR for _, lo, hi in reads)
    assert (reads[0][1], reads[0][2] + 1, reads[1][2]) == (first, reads[1][1], last)
    assert response.json == ok([bar_json(CLOCK.to_utc(t)) for t in times])


def test_a_window_at_the_cap_is_read_whole(client, stub, floors):
    first = broker(2025, 7, 15, 12)
    last = first + 99_989 * HOUR
    stub.rates[H1] = rates(first, first + HOUR, last)
    kept(floors, Series.H1, first)

    response = post_bars(client, "H1", CLOCK.to_utc(first), CLOCK.to_utc(last))

    assert stub.windows("copy_rates_range") == [(H1, first, last)]
    assert len(response.json["result"]) == 3


def test_a_minute_window_one_period_over_a_million_bar_cap_is_read_in_two_spans(
    client, stub, floors
):
    stub.maxbars = 1_000_000
    first = broker(2025, 7, 15, 12)
    last = first + 999_990 * MINUTE
    stub.rates[M1] = rates(first, first + MINUTE, last)
    kept(floors, Series.M1, first)

    response = post_bars(client, "M1", CLOCK.to_utc(first), CLOCK.to_utc(last))

    reads = stub.windows("copy_rates_range")
    assert len(reads) == 2
    assert all(hi - lo <= 999_989 * MINUTE for _, lo, hi in reads)
    assert len(response.json["result"]) == 3


# ── Floor ─────────────────────────────────────────────────────────────────────


def test_a_window_before_the_floor_is_answered_empty_and_the_floor_advertised(client, stub):
    # Broker 1,735,689,600 is 2025-01-01 00:00, under EST: 2024-12-31T22:00:00Z.
    stub.rates[H1] = rates(1_735_689_600, 1_735_689_600 + HOUR)
    start, end = 1_704_067_200, 1_704_067_200 + DAY

    response = post_bars(client, "H1", start, end)

    assert response.json == ok([])
    # The probe alone: one H1 period from 1970-01-02, under EST.
    assert stub.windows("copy_rates_range") == [(H1, 93_600, 97_200)]
    ranges = client.get("/history/ranges?symbol=EURUSD").json["result"]
    assert ranges["maxbars"] == 100_000
    assert (ranges["ranges"]["H1"]["floor"], ranges["ranges"]["H1"]["generation"]) == (
        1_735_682_400,
        1,
    )
    assert abs(ranges["ranges"]["H1"]["measured_at"] - time.time()) < 5


def test_a_window_straddling_the_floor_answers_its_rows_from_the_floor_on(client, stub):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR, floor + 2 * HOUR, floor + 3 * HOUR)

    response = post_bars(client, "H1", broker(2025, 7, 14) - EDT, floor + 2 * HOUR - EDT)

    assert response.json == ok([bar_json(floor + k * HOUR - EDT) for k in range(3)])
    assert advertised(client, "H1")["floor"] == floor - EDT


def test_a_fresh_floor_is_not_measured_again_and_a_stale_one_is(client, stub, monkeypatch):
    now = [1_760_000_000.0]
    monkeypatch.setattr(history_module, "time", types.SimpleNamespace(time=lambda: now[0]))
    first = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(*(first + k * HOUR for k in range(6)))
    start, end = first - EDT, first + 5 * HOUR - EDT
    post_bars(client, "H1", start, end)
    stub.rates[H1] = rates(*(first + k * HOUR for k in range(2, 6)))
    probes = len(stub.windows("copy_rates_range"))

    now[0] += 899
    post_bars(client, "H1", start, end)
    fresh = advertised(client, "H1")
    now[0] += 2
    post_bars(client, "H1", start, end)
    stale = advertised(client, "H1")

    assert len(stub.windows("copy_rates_range")) == probes + 1 + 2
    assert fresh == {"floor": first - EDT, "measured_at": 1_760_000_000, "generation": 1}
    assert stale == {"floor": first + 2 * HOUR - EDT, "measured_at": 1_760_000_901, "generation": 2}


def test_ranges_list_each_series_measured_and_maxbars(client, stub):
    hour_floor = broker(2025, 7, 14, 1)
    minute_floor = broker(2025, 7, 14, 0, 30)
    tick_floor = broker(2025, 7, 14, 0, 5) * 1000 + 250
    stub.rates[H1] = rates(hour_floor, hour_floor + HOUR)
    stub.rates[M1] = rates(minute_floor, minute_floor + MINUTE)
    stub.ticks = ticks(tick_floor, tick_floor + 1_000)
    before = broker(2025, 7, 1) - EDT
    post_bars(client, "H1", before, before + HOUR)
    post_bars(client, "M1", before, before + HOUR)
    post_ticks(client, before, before + HOUR)

    response = client.get("/history/ranges?symbol=EURUSD").json

    entries = response["result"]["ranges"]
    assert list(entries) == ["M1", "H1", "ticks"]
    assert response["result"]["maxbars"] == 100_000
    assert {series: entry["floor"] for series, entry in entries.items()} == {
        "M1": minute_floor - EDT,
        "H1": hour_floor - EDT,
        "ticks": (tick_floor - EDT * 1000) // 1000,
    }
    assert {entry["generation"] for entry in entries.values()} == {1}
    assert client.get("/history/ranges?symbol=GBPUSD").json["result"]["ranges"] == {}


def test_a_window_past_a_kept_floor_but_before_the_terminals_is_answered_empty(
    client, stub, floors
):
    first = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(first, first + HOUR)
    kept(floors, Series.H1, broker(2025, 7, 1))

    response = post_bars(client, "H1", broker(2025, 7, 7) - EDT, broker(2025, 7, 8) - EDT)

    assert response.json == ok([])
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        first - EDT,
        2,
    )
    assert "symbol_info_tick" not in stub.names()


def test_a_tick_day_past_a_kept_floor_but_before_the_terminals_is_answered_empty(
    client, stub, floors
):
    first = broker(2025, 7, 17, 6) * 1000
    stub.ticks = ticks(*(first + k * 600_000 for k in range(3 * 144)))
    kept(floors, Series.TICKS, broker(2025, 7, 1) * 1000)
    start = calendar.timegm((2025, 7, 16, 0, 0, 0))

    response = post_ticks(client, start, start + DAY - 1)

    assert response.json == ok([])
    assert advertised(client, "ticks")["floor"] == first // 1000 - EDT


def test_a_tick_window_covers_the_whole_of_its_last_second(client, stub, floors):
    second = broker(2025, 7, 16, 12)
    stub.ticks = ticks(
        second * 1000 - 500, second * 1000 + 250, second * 1000 + 999, second * 1000 + 1000
    )
    kept(floors, Series.TICKS, broker(2025, 7, 1) * 1000)

    response = post_ticks(client, second - EDT - 1, second - EDT)

    assert [row["time_msc"] for row in response.json["result"]] == [
        (second - EDT) * 1000 - 500,
        (second - EDT) * 1000 + 250,
        (second - EDT) * 1000 + 999,
    ]


def test_a_tick_window_ending_on_the_autumn_rollback_covers_its_last_second(client, stub, floors):
    # 2025-11-02 05:00:00Z is 01:00 EDT and 06:00:00Z is 01:00 EST: one UTC second past the window
    # the broker's clock falls back an hour.
    start = calendar.timegm((2025, 11, 2, 5, 0, 0))
    stub.ticks = ticks(*((start + EDT) * 1000 + k * 1_200_000 for k in range(3)))
    kept(floors, Series.TICKS, broker(2025, 10, 1) * 1000)

    response = post_ticks(client, start, start + HOUR - 1)

    assert [row["time_msc"] for row in response.json["result"]] == [
        start * 1000 + k * 1_200_000 for k in range(3)
    ]


def test_the_downloader_writes_every_tick_across_midnight_once(remote, stub, floors):
    midnight = calendar.timegm((2025, 7, 17, 0, 0, 0))
    utc_msc = [
        midnight * 1000 - 1_250,
        midnight * 1000 - 750,
        midnight * 1000,
        midnight * 1000 + 500,
    ]
    stub.ticks = ticks(*(t + EDT * 1000 for t in utc_msc))
    kept(floors, Series.TICKS, broker(2025, 7, 1) * 1000)
    provider = MagicMock()
    provider.get_instrument.return_value = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    catalog = MagicMock()
    downloader = MT5DataDownloader(MagicMock(), provider, catalog)

    result = downloader.download_ticks(
        "EURUSD",
        datetime.fromtimestamp(midnight - HOUR, UTC),
        datetime.fromtimestamp(midnight + HOUR, UTC),
    )

    written = [tick for call in catalog.write_data.call_args_list for tick in call[0][0]]
    assert sorted(tick.ts_event for tick in written) == [t * 1_000_000 for t in utc_msc]
    assert result.success


# ── A read that proves nothing ────────────────────────────────────────────────


def test_a_bar_window_nothing_proves_answers_503_once_its_evidence_reads_are_made(
    client, stub, no_sleep
):
    window = broker(2025, 7, 15, 12)
    stub.last_quote = broker(2025, 7, 16)
    start, end = window - EDT, window + HOUR - EDT

    response = post_bars(client, "H1", start, end)

    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert response.json == syncing(f"EURUSD H1 {start}..{end}: syncing")
    assert stub.names() == [
        "symbol_select",
        "terminal_info",
        "copy_rates_range",
        "copy_rates_range",
        "symbol_info_tick",
        "copy_rates_range",
    ]
    assert stub.windows("copy_rates_range") == [
        (H1, 93_600, 97_200),
        (H1, window, window + HOUR),
        (H1, window + HOUR, window + HOUR + H1_SPAN),
    ]
    stub.rates[H1] = rates(window, window + HOUR)

    answered = post_bars(client, "H1", start, end)

    assert answered.json == ok([bar_json(start), bar_json(start + HOUR)])


def test_a_tick_window_nothing_proves_answers_503_once_its_evidence_reads_are_made(
    client, stub, no_sleep
):
    stub.last_quote = broker(2025, 7, 18)
    start = calendar.timegm((2025, 7, 16, 0, 0, 0))
    end = start + DAY - 1

    response = post_ticks(client, start, end)

    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert response.json == syncing(f"EURUSD ticks {start}..{end}: syncing")
    assert stub.names() == [
        "symbol_select",
        "copy_ticks_from",
        "copy_ticks_range",
        "symbol_info_tick",
        "copy_ticks_from",
    ]
    # The probe from 1970-01-02 under EST, then the first tick from the window's end.
    assert stub.windows("copy_ticks_from") == [(93_600_000, 1, INFO), ((end + EDT) * 1000, 1, INFO)]


def test_a_window_read_the_terminal_failed_answers_503_after_that_read(
    client, stub, floors, no_sleep
):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    stub.script("copy_rates_range", (None, (-1, "Terminal: Call failed")))
    start, end = floor - EDT, floor + HOUR - EDT

    response = post_bars(client, "H1", start, end)

    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert response.json == syncing(f"EURUSD H1 {start}..{end}: syncing")
    assert stub.names() == ["symbol_select", "terminal_info", "copy_rates_range"]


def test_rows_answered_under_an_error_answer_503(client, stub, floors, no_sleep):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    stub.script("copy_rates_range", (rates(floor), (-10005, "IPC timeout")))

    response = post_bars(client, "H1", floor - EDT, floor + HOUR - EDT)

    assert response.status_code == 503
    assert len(stub.windows("copy_rates_range")) == 1


def test_a_window_answered_with_rows_is_read_once_and_needs_no_evidence(client, stub, floors):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)

    post_bars(client, "H1", floor - EDT, floor + 3 * HOUR - EDT)

    assert stub.names() == ["symbol_select", "terminal_info", "copy_rates_range"]


def test_a_terminal_without_ipc_answers_503_for_bars_and_fails_the_ranges(client, stub):
    stub.script("terminal_info", (None, (-10004, "No IPC connection")), (None, (-10004, "No IPC")))

    bars = post_bars(client, "H1", 1_752_570_000, 1_752_573_600)
    ranges = client.get("/history/ranges?symbol=EURUSD")

    assert (bars.status_code, bars.json) == (
        503,
        syncing("EURUSD H1 1752570000..1752573600: syncing"),
    )
    assert (ranges.status_code, ranges.json["error"]) == (
        200,
        {"code": -10004, "message": "No IPC"},
    )
    assert stub.windows("copy_rates_range") == []


# ── Bracketed ─────────────────────────────────────────────────────────────────


def test_an_empty_minute_window_with_bars_on_both_sides_is_answered_empty(client, stub):
    friday = [broker(2025, 7, 18, 22, minute) for minute in range(60)]
    monday = [broker(2025, 7, 21, 0, minute) for minute in range(60)]
    stub.rates[M1] = rates(*friday, *monday)
    stub.last_quote = monday[-1]
    lo, hi = broker(2025, 7, 19, 7), broker(2025, 7, 19, 12)

    response = post_bars(client, "M1", lo - EDT, hi - EDT)

    assert response.json == ok([])
    # The probe, the window, the last quote, the span after the window and the last bar opened at
    # or before its start.
    assert stub.names() == [
        "symbol_select",
        "terminal_info",
        "copy_rates_range",
        "copy_rates_range",
        "symbol_info_tick",
        "copy_rates_range",
        "copy_rates_from",
    ]
    assert stub.windows("copy_rates_range") == [
        (M1, 93_600, 93_660),
        (M1, lo, hi),
        (M1, hi, hi + M1_SPAN),
    ]
    assert stub.windows("copy_rates_from") == [(M1, lo, 1)]


@pytest.mark.parametrize("last_bar", ["none", "at the start"])
def test_an_empty_minute_window_whose_last_bar_from_its_start_is_not_before_it_answers_503(
    client, stub, no_sleep, last_bar
):
    friday = [broker(2025, 7, 18, 22, minute) for minute in range(60)]
    monday = [broker(2025, 7, 21, 0, minute) for minute in range(60)]
    stub.rates[M1] = rates(*friday, *monday)
    stub.last_quote = monday[-1]
    lo, hi = broker(2025, 7, 19, 7), broker(2025, 7, 19, 12)
    if last_bar == "none":
        stub.script("copy_rates_from", (rates(), SUCCESS))
    else:
        stub.script("copy_rates_from", (rates(lo), SUCCESS))

    response = post_bars(client, "M1", lo - EDT, hi - EDT)

    assert response.status_code == 503
    assert stub.windows("copy_rates_from") == [(M1, lo, 1)]


def a_tuesday_of_ticks() -> list[int]:
    """A tick every ten minutes, broker 2025-07-15 00:00 to 23:50."""
    return [broker(2025, 7, 15) * 1000 + k * 600_000 for k in range(144)]


def test_an_empty_tick_day_with_a_tick_before_it_in_its_week_and_one_after_it_is_answered_empty(
    client, stub
):
    thursday = [broker(2025, 7, 17, 6) * 1000 + k * 600_000 for k in range(36)]
    stub.ticks = ticks(*a_tuesday_of_ticks(), *thursday)
    stub.last_quote = broker(2025, 7, 18)
    start = calendar.timegm((2025, 7, 16, 0, 0, 0))
    end = start + DAY - 1

    response = post_ticks(client, start, end)

    assert response.json == ok([])
    # The window is its own UTC day, so the widening goes straight to its week; each read runs to
    # the second after its window, where the package ends one at its first millisecond.
    week = calendar.timegm((2025, 7, 14, 0, 0, 0))
    assert stub.windows("copy_ticks_range") == [
        ((start + EDT) * 1000, (end + 1 + EDT) * 1000, INFO),
        ((week + EDT) * 1000, (week + 7 * DAY + EDT) * 1000, INFO),
    ]
    assert stub.windows("copy_ticks_from")[1:] == [((end + EDT) * 1000, 1, INFO)]


def test_an_empty_tick_day_with_no_tick_after_it_while_the_last_quote_is_later_answers_503(
    client, stub, no_sleep
):
    stub.ticks = ticks(*a_tuesday_of_ticks())
    stub.last_quote = broker(2025, 7, 18)
    start = calendar.timegm((2025, 7, 16, 0, 0, 0))

    response = post_ticks(client, start, start + DAY - 1)

    assert response.status_code == 503
    assert stub.names()[-2:] == ["symbol_info_tick", "copy_ticks_from"]


# ── The live edge ─────────────────────────────────────────────────────────────


def a_friday_of_ticks() -> list[int]:
    """A tick every ten minutes, broker 2025-07-18 00:00 to 23:50."""
    return [broker(2025, 7, 18) * 1000 + k * 600_000 for k in range(144)]


def test_a_saturday_tick_window_after_the_last_quote_is_answered_empty_at_once(client, stub):
    stub.ticks = ticks(*a_friday_of_ticks())
    stub.last_quote = broker(2025, 7, 18, 23, 59)
    saturday = calendar.timegm((2025, 7, 19, 0, 0, 0))

    response = post_ticks(client, saturday, saturday + DAY - 1)

    assert response.json == ok([])
    assert stub.names() == [
        "symbol_select",
        "copy_ticks_from",
        "copy_ticks_range",
        "symbol_info_tick",
    ]


def test_the_zero_tick_dates_no_live_edge(client, stub, no_sleep):
    stub.ticks = ticks(*a_friday_of_ticks())
    saturday = calendar.timegm((2025, 7, 19, 0, 0, 0))

    response = post_ticks(client, saturday, saturday + DAY - 1)

    assert response.status_code == 503
    assert stub.names()[-2:] == ["symbol_info_tick", "copy_ticks_from"]


def test_an_empty_weekday_window_before_the_last_quote_with_nothing_after_it_answers_503(
    client, stub, floors, no_sleep
):
    floor = broker(2025, 7, 1)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    stub.last_quote = broker(2025, 7, 18)
    lo = broker(2025, 7, 16, 10)

    response = post_bars(client, "H1", lo - EDT, lo + 3 * HOUR - EDT)

    assert response.status_code == 503
    # The window, the kept floor measured again, the last quote, and the span after the window.
    assert stub.names()[2:] == [
        "copy_rates_range",
        "copy_rates_range",
        "symbol_info_tick",
        "copy_rates_range",
    ]
    assert stub.windows("copy_rates_range")[-1] == (H1, lo + 3 * HOUR, lo + 3 * HOUR + H1_SPAN)


def test_a_window_ending_now_answers_its_ticks_up_to_the_last_quote(client, stub):
    first = NOW - HOUR
    stub.ticks = ticks(*((first + k * MINUTE) * 1000 + 250 for k in range(50)))
    stub.last_quote = first + 49 * MINUTE

    response = post_ticks(client, NOW_UTC - HOUR, NOW_UTC)

    assert response.json == ok(
        [tick_json((NOW_UTC - HOUR + k * MINUTE) * 1000 + 250) for k in range(50)]
    )


def test_an_empty_window_ending_now_while_the_last_quote_is_an_hour_past_it_answers_503(
    client, stub, no_sleep
):
    stub.ticks = ticks(*((NOW - 3 * HOUR + k * MINUTE) * 1000 for k in range(60)))
    stub.last_quote = NOW + HOUR

    response = post_ticks(client, NOW_UTC - HOUR, NOW_UTC)

    assert response.status_code == 503
    assert stub.windows("copy_ticks_from")[-1] == (NOW * 1000, 1, INFO)


# ── A window in the future ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("/history/bars", {"timeframe": "H1"}),
        ("/history/ticks", {}),
    ],
)
def test_a_window_starting_after_the_terminals_time_is_refused_without_a_package_call(
    client, stub, route, body
):
    start = NOW_UTC + HOUR

    response = client.post(
        route, json={"symbol": "EURUSD", "start": start, "end": start + HOUR} | body
    )

    message = f"start {start} is after the terminal's time {NOW_UTC}"
    assert (response.status_code, response.json) == (
        400,
        {"ok": False, "error": {"code": -2, "message": message}, "last_error": [-2, message]},
    )
    assert stub.calls == []


def relayed_seconds_ago(server_times, monkeypatch, seconds: int) -> None:
    """Relays the sample at NOW again, `seconds` before the monotonic clock's present, which then
    stands still."""
    monotonic = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: monotonic[0])
    server_times.write(ServerTimeSample("EURUSD", NOW, NOW, NOW - EST, True))
    monotonic[0] += seconds


def test_a_window_starting_within_the_time_since_the_sample_arrived_is_answered(
    client, stub, floors, server_times, monkeypatch
):
    relayed_seconds_ago(server_times, monkeypatch, 20)
    tick = (NOW + 10) * 1000 + 250
    stub.ticks = ticks(tick)
    kept(floors, Series.TICKS, tick)

    response = post_ticks(client, NOW_UTC + 10, NOW_UTC + 10)

    assert response.json == ok([tick_json(tick - EST * 1000)])


def test_a_window_starting_past_the_time_since_the_sample_arrived_is_refused(
    client, stub, server_times, monkeypatch
):
    relayed_seconds_ago(server_times, monkeypatch, 20)

    response = post_ticks(client, NOW_UTC + 60, NOW_UTC + 120)

    message = f"start {NOW_UTC + 60} is after the terminal's time {NOW_UTC + 20}"
    assert (response.status_code, response.json["error"]["message"]) == (400, message)
    assert stub.calls == []


def test_a_window_merely_ending_after_the_terminals_time_is_answered(client, stub, floors):
    stub.rates[H1] = rates(NOW - HOUR, NOW)
    kept(floors, Series.H1, NOW - HOUR)

    response = post_bars(client, "H1", NOW_UTC - HOUR, NOW_UTC + HOUR)

    assert response.json == ok([bar_json(NOW_UTC - HOUR), bar_json(NOW_UTC)])


# ── Grid ──────────────────────────────────────────────────────────────────────


def test_a_bar_off_the_grid_fails_the_answer_naming_it(client, stub, floors):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR, floor + HOUR + 1_800)
    kept(floors, Series.H1, floor)

    response = post_bars(client, "H1", floor - EDT, floor + 2 * HOUR - EDT)

    message = f"EURUSD H1: row {floor + HOUR + 1_800 - EDT} is off the grid"
    assert response.status_code == 200
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": message},
        "last_error": [-1, message],
    }


def a_daily_era_then_hours() -> tuple[list[int], list[int]]:
    """Ten H1 bars a day apart, broker 2025-07-07 to 07-18, then six an hour apart from 07-21."""
    days = [broker(2025, 7, day) for day in (7, 8, 9, 10, 11, 14, 15, 16, 17, 18)]
    hours = [broker(2025, 7, 21) + k * HOUR for k in range(6)]
    return days, hours


def test_a_daily_spaced_prefix_is_dropped_and_the_floor_moves_past_it(client, stub):
    days, hours = a_daily_era_then_hours()
    stub.rates[H1] = rates(*days, *hours)
    post_bars(client, "H1", broker(2025, 7, 1) - EDT, broker(2025, 7, 2) - EDT)
    measured = advertised(client, "H1")

    response = post_bars(client, "H1", broker(2025, 7, 6) - EDT, hours[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in hours])
    moved = advertised(client, "H1")
    assert (measured["floor"], measured["generation"]) == (days[0] - EDT, 1)
    assert (moved["floor"], moved["generation"]) == (hours[0] - EDT, 2)


def test_a_stub_measured_again_never_moves_a_coarse_floor_back(client, stub, monkeypatch):
    now = [1_760_000_000.0]
    monkeypatch.setattr(history_module, "time", types.SimpleNamespace(time=lambda: now[0]))
    days, hours = a_daily_era_then_hours()
    stub.rates[H1] = rates(*days, *hours)
    post_bars(client, "H1", broker(2025, 7, 6) - EDT, hours[-1] - EDT)
    now[0] += 901

    response = post_bars(client, "H1", hours[2] - EDT, hours[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in hours[2:]])
    # The floor measured again, which the terminal answers with the daily era's first bar.
    assert stub.windows("copy_rates_range")[-2] == (H1, 93_600, 97_200)
    assert advertised(client, "H1") == {
        "floor": hours[0] - EDT,
        "measured_at": 1_760_000_901,
        "generation": 2,
    }


def test_a_window_beginning_at_a_coarse_floor_answers_its_rows_whole(client, stub):
    days, hours = a_daily_era_then_hours()
    stub.rates[H1] = rates(*days, *hours)
    post_bars(client, "H1", broker(2025, 7, 6) - EDT, hours[-1] - EDT)

    response = post_bars(client, "H1", hours[0] - EDT, hours[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in hours])
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        hours[0] - EDT,
        2,
    )


def test_a_lead_a_whole_number_of_days_apart_at_the_floor_is_a_coarse_prefix(client, stub, floors):
    first = broker(2025, 7, 11, 23)
    later = broker(2025, 7, 13, 23)
    stub.rates[H1] = rates(first, later, later + HOUR, later + 2 * HOUR)
    kept(floors, Series.H1, first)

    response = post_bars(client, "H1", first - EDT, later + 2 * HOUR - EDT)

    assert [row["time"] for row in response.json["result"]] == [
        later - EDT,
        later + HOUR - EDT,
        later + 2 * HOUR - EDT,
    ]
    assert advertised(client, "H1")["floor"] == later - EDT


def test_a_lead_no_whole_number_of_days_apart_is_not_a_coarse_prefix(client, stub, floors):
    first = broker(2025, 7, 11, 21)
    later = broker(2025, 7, 14)
    times = [first, later, later + HOUR, later + 2 * HOUR]
    stub.rates[H1] = rates(*times)
    kept(floors, Series.H1, first)

    response = post_bars(client, "H1", first - EDT, times[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in times])


def test_a_whole_day_gap_in_a_window_after_the_floor_is_answered_whole_and_moves_no_floor(
    client, stub, floors
):
    floor = broker(2020, 1, 1)
    friday = broker(2025, 7, 11, 23)
    sunday = broker(2025, 7, 13, 23)
    week = [friday, sunday, sunday + HOUR, sunday + 2 * HOUR]
    stub.rates[H1] = rates(floor, *week)
    kept(floors, Series.H1, floor)

    response = post_bars(client, "H1", friday - EDT, week[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in week])
    # Broker 2020-01-01 00:00 is under EST.
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        floor - EST,
        1,
    )


def test_a_read_from_the_floor_answered_from_a_later_row_is_answered_whole_and_moves_no_floor(
    client, stub, floors
):
    floor = broker(2020, 1, 1)
    friday = broker(2025, 7, 11, 23)
    sunday = broker(2025, 7, 13, 23)
    week = [friday, sunday, sunday + HOUR, sunday + 2 * HOUR]
    stub.rates[H1] = rates(floor, *week)
    kept(floors, Series.H1, floor)
    # The read from the floor answers only the rows the terminal has synced.
    stub.script("copy_rates_range", (rates(*week), SUCCESS))

    response = post_bars(client, "H1", floor - EST, week[-1] - EDT)

    assert response.json == ok([bar_json(t - EDT) for t in week])
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        floor - EST,
        1,
    )
    assert post_bars(client, "H1", floor - EST, floor - EST).json == ok([bar_json(floor - EST)])


def test_a_first_span_at_the_floor_answering_nothing_answers_503_and_moves_no_floor(
    client, stub, floors, no_sleep
):
    floor = broker(2014, 1, 6)
    last = floor + 99_990 * HOUR
    # The window is read in two spans; the second begins half way.
    second = floor + 49_995 * HOUR
    later = second + 2 * DAY
    stub.rates[H1] = rates(floor, second, later, later + HOUR, later + 2 * HOUR)
    kept(floors, Series.H1, floor)
    stub.script("copy_rates_range", (rates(), SUCCESS))

    response = post_bars(client, "H1", CLOCK.to_utc(floor), CLOCK.to_utc(last))

    assert response.status_code == 503
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        CLOCK.to_utc(floor),
        1,
    )


def test_a_coarse_prefix_at_a_floor_a_gap_proof_measured_again_is_dropped(client, stub, floors):
    kept_floor = broker(2014, 1, 6)
    last = kept_floor + 99_990 * HOUR
    # The terminal's own floor lies in the second of the window's two spans, so the first answers
    # nothing and the kept floor is measured again for it.
    days = [kept_floor + 49_995 * HOUR + 5 * HOUR + k * DAY for k in range(5)]
    hours = [days[-1] + DAY + k * HOUR for k in range(3)]
    stub.rates[H1] = rates(*days, *hours)
    kept(floors, Series.H1, kept_floor)

    response = post_bars(client, "H1", CLOCK.to_utc(kept_floor), CLOCK.to_utc(last))

    assert response.json == ok([bar_json(CLOCK.to_utc(t)) for t in hours])
    assert (advertised(client, "H1")["floor"], advertised(client, "H1")["generation"]) == (
        CLOCK.to_utc(hours[0]),
        3,
    )


# ── The package's own failures, the gate and the refusals ─────────────────────


def test_a_symbol_the_terminal_cannot_select_fails_with_its_error(client, stub):
    stub.script("symbol_select", (False, (-4, "Terminal: Not found")))

    response = post_bars(client, "H1", 1_752_570_000, 1_752_573_600)

    assert (response.status_code, response.json) == (
        200,
        {
            "ok": False,
            "error": {"code": -4, "message": "Terminal: Not found"},
            "last_error": [-4, "Terminal: Not found"],
        },
    )
    assert stub.names() == ["symbol_select"]


def test_the_history_routes_are_unavailable_while_the_clock_is_not_verified(
    client, stub, clock_status
):
    clock_status.clear()

    responses = [
        post_bars(client, "H1", 1_752_570_000, 1_752_573_600),
        post_ticks(client, 1_752_570_000, 1_752_573_600),
        client.get("/history/ranges?symbol=EURUSD"),
    ]

    assert [(response.status_code, response.json) for response in responses] == [
        (503, UNVERIFIED)
    ] * 3
    assert stub.calls == []


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({}, "missing parameter: symbol, timeframe, start, end"),
        (
            {"symbol": "EURUSD", "timeframe": "H1", "start": 1, "end": 2, "bogus": 1},
            "unknown parameter: bogus",
        ),
        ({"symbol": "", "timeframe": "H1", "start": 1, "end": 2}, "not a symbol: ''"),
        (
            {"symbol": "EURUSD", "timeframe": "MN1", "start": 1, "end": 2},
            "not a bar timeframe: 'MN1'",
        ),
        (
            {"symbol": "EURUSD", "timeframe": 16385, "start": 1, "end": 2},
            "not a bar timeframe: 16385",
        ),
        (
            {"symbol": "EURUSD", "timeframe": "ticks", "start": 1, "end": 2},
            "not a bar timeframe: 'ticks'",
        ),
        (
            {"symbol": "EURUSD", "timeframe": "H1", "start": 1.5, "end": 2},
            "not an integer epoch: start",
        ),
        ({"symbol": "EURUSD", "timeframe": "H1", "start": 3, "end": 2}, "start is after end"),
    ],
)
def test_a_bars_body_out_of_shape_is_refused(client, stub, body, message):
    response = client.post("/history/bars", json=body)

    assert response.status_code == 400
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": message},
        "last_error": [-2, message],
    }
    assert stub.calls == []


@pytest.mark.parametrize("flags", [999, 1, "trade", None])
def test_a_tick_selection_the_package_does_not_have_is_refused(client, stub, flags):
    body = {"symbol": "EURUSD", "start": 1, "end": 2, "flags": flags}

    response = client.post("/history/ticks", json=body)

    assert (response.status_code, response.json["error"]["message"]) == (
        400,
        f"not a tick selection: {flags!r}",
    )
    assert stub.calls == []


def test_a_tick_selection_travels_by_name(client, stub, floors):
    first = broker(2025, 7, 14) * 1000
    stub.ticks = ticks(first, first + 1_000)
    kept(floors, Series.TICKS, first)
    body = {"symbol": "EURUSD", "start": first // 1000 - EDT, "end": first // 1000 - EDT + 1}

    client.post("/history/ticks", json=body | {"flags": "ALL"})

    assert stub.windows("copy_ticks_range")[-1][2] == mirror.COPY_TICKS_ALL


def test_ranges_without_a_symbol_are_refused(client, stub):
    ranges = client.get("/history/ranges")

    assert (ranges.status_code, ranges.json["error"]["message"]) == (
        400,
        "missing parameter: symbol",
    )
    assert stub.calls == []


# ── The repeated hour ─────────────────────────────────────────────────────────

# Broker 08:30 on 2025-11-02 and on 2024-11-03 is New York's repeated 01:30, each first occurring
# under EDT.
REPEATED = broker(2025, 11, 2, 8, 30)
REPEATED_A_YEAR_BEFORE = broker(2024, 11, 3, 8, 30)


def mirror_ticks(client, start: int, end: int):
    body = {"symbol": "EURUSD", "date_from": start, "date_to": end, "flags": INFO}
    return client.post("/mt5/copy_ticks_range", json=body)


def repeated_hour_warning(epoch: int) -> str:
    return (
        f"copy_ticks_range: ticks.time {epoch} is in the broker's repeated hour, read as its first "
        "occurrence"
    )


def encoding_warnings(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "mt5connector.server.encoding" and record.levelno == logging.WARNING
    ]


def test_each_repeated_broker_hour_is_warned_of_once_across_answers_and_routes(
    client, stub, floors, caplog
):
    stub.ticks = ticks(REPEATED_A_YEAR_BEFORE * 1000, REPEATED * 1000, (REPEATED + MINUTE) * 1000)
    kept(floors, Series.TICKS, broker(2024, 10, 1) * 1000)
    caplog.set_level(logging.WARNING, logger="mt5connector.server.encoding")

    answers = [
        mirror_ticks(client, REPEATED - EDT, REPEATED - EDT),
        mirror_ticks(client, REPEATED + MINUTE - EDT, REPEATED + MINUTE - EDT),
        post_ticks(client, REPEATED - EDT, REPEATED + MINUTE - EDT),
    ]
    in_one_hour = encoding_warnings(caplog)
    answers.append(mirror_ticks(client, REPEATED_A_YEAR_BEFORE - EDT, REPEATED_A_YEAR_BEFORE - EDT))
    answers.append(post_ticks(client, REPEATED - EDT, REPEATED - EDT))

    assert [len(answer.json["result"]) for answer in answers] == [1, 1, 2, 1, 1]
    assert in_one_hour == [repeated_hour_warning(REPEATED)]
    assert encoding_warnings(caplog) == [
        repeated_hour_warning(REPEATED),
        repeated_hour_warning(REPEATED_A_YEAR_BEFORE),
    ]


def test_an_answer_spanning_two_repeated_hours_warns_of_each(client, stub, caplog):
    stub.ticks = ticks(REPEATED_A_YEAR_BEFORE * 1000, REPEATED * 1000)
    caplog.set_level(logging.WARNING, logger="mt5connector.server.encoding")

    answer = mirror_ticks(client, REPEATED_A_YEAR_BEFORE - EDT, REPEATED - EDT)

    assert len(answer.json["result"]) == 2
    assert encoding_warnings(caplog) == [
        repeated_hour_warning(REPEATED_A_YEAR_BEFORE),
        repeated_hour_warning(REPEATED),
    ]


# ── The client ────────────────────────────────────────────────────────────────


def test_the_client_answers_a_window_in_the_packages_types(remote, stub, floors):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)

    answered = history_client.bars("EURUSD", wire.Series.H1, floor - DAY - EDT, floor + HOUR - EDT)

    assert answered.dtype == RATES
    assert answered["time"].tolist() == [floor - EDT, floor + HOUR - EDT]
    ranges = history_client.ranges("EURUSD")
    assert ranges == history_client.HistoryRanges(
        maxbars=100_000,
        series={
            wire.Series.H1: history_client.SeriesRange(
                floor - EDT, ranges.series[wire.Series.H1].measured_at, 1
            )
        },
    )


def test_the_client_answers_none_for_a_failure_and_records_its_error(remote, stub):
    stub.script("symbol_select", (False, (-4, "Terminal: Not found")))

    assert history_client.ticks("EURUSD", 1_752_570_000, 1_752_573_600) is None
    assert remote.last_error() == (-4, "Terminal: Not found")


def test_the_client_answers_none_for_a_window_in_the_future_and_records_the_refusal(remote, stub):
    start = NOW_UTC + HOUR

    assert history_client.ticks("EURUSD", start, start + HOUR) is None
    assert remote.last_error() == (-2, f"start {start} is after the terminal's time {NOW_UTC}")
    assert stub.calls == []


def test_the_client_waits_past_the_mirrors_read_timeout(remote, stub, floors, monkeypatch):
    monkeypatch.setattr(remote, "READ_TIMEOUT_S", 0.2)
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor)
    kept(floors, Series.H1, floor)
    stub.delay_s = 0.3

    answered = history_client.bars("EURUSD", wire.Series.H1, floor - EDT, floor - EDT)

    assert len(answered) == 1


def test_the_client_asks_again_after_each_syncing_answers_retry_after_until_the_rows(
    remote, stub, floors, monkeypatch
):
    slept = []
    monkeypatch.setattr(history_client, "time", types.SimpleNamespace(sleep=slept.append))
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    stub.syncing = lambda: len(stub.windows("symbol_select")) <= 2

    answered = history_client.bars("EURUSD", wire.Series.H1, floor - EDT, floor + HOUR - EDT)

    assert answered["time"].tolist() == [floor - EDT, floor + HOUR - EDT]
    assert len(stub.windows("symbol_select")) == 3
    assert slept == [5.0, 5.0]


def test_a_cancellation_between_retries_stops_the_client(remote, stub, floors):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    stub.syncing = lambda: True
    cancel = threading.Event()
    answered = []
    request = threading.Thread(
        target=lambda: answered.append(
            history_client.bars(
                "EURUSD", wire.Series.H1, floor - EDT, floor + HOUR - EDT, cancel=cancel
            )
        )
    )

    request.start()
    deadline = time.monotonic() + 5
    while not stub.windows("symbol_select") and time.monotonic() < deadline:
        time.sleep(0.01)
    cancel.set()
    request.join(5)

    assert not request.is_alive()
    assert answered == [None]
    assert len(stub.windows("symbol_select")) == 1
    assert remote.last_error()[0] == wire.ServerCode.SYNCING


def asked_routes(remote, monkeypatch) -> list[str]:
    """The routes the client asks from here on, in order."""
    asked = []
    call_route = remote.call_route

    def asking(name, method, path, **kwargs):
        asked.append(path)
        return call_route(name, method, path, **kwargs)

    monkeypatch.setattr(remote, "call_route", asking)
    return asked


def releasing_sleep(cold_reads: Held, monkeypatch) -> list[float]:
    """The client's sleeps from here on, the first releasing the cold reads."""
    slept = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        cold_reads.release()

    monkeypatch.setattr(history_client, "time", types.SimpleNamespace(sleep=sleep))
    return slept


def test_the_client_asks_a_window_again_after_a_busy_answers_retry_after(
    remote, stub, served, floors, cold_reads, monkeypatch
):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    window = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "start": floor - EDT,
        "end": floor + HOUR - EDT,
    }
    cold_reads.take(2, "/history/bars", window)
    asked = asked_routes(remote, monkeypatch)
    slept = releasing_sleep(cold_reads, monkeypatch)

    answered = history_client.bars("EURUSD", wire.Series.H1, floor - EDT, floor + HOUR - EDT)

    assert answered["time"].tolist() == [floor - EDT, floor + HOUR - EDT]
    assert asked == ["/history/bars", "/history/bars"]
    assert slept == [5.0]
    assert counters(served)["refusals"] == 1


def test_the_client_asks_the_ranges_again_after_a_busy_answers_retry_after(
    remote, stub, served, floors, cold_reads, monkeypatch
):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    window = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "start": floor - EDT,
        "end": floor + HOUR - EDT,
    }
    cold_reads.take(2, "/history/bars", window)
    asked = asked_routes(remote, monkeypatch)
    slept = releasing_sleep(cold_reads, monkeypatch)

    answered = history_client.ranges("EURUSD")

    assert answered.maxbars == 100_000
    assert set(answered.series) == {wire.Series.H1}
    assert asked == ["/history/ranges", "/history/ranges"]
    assert slept == [5.0]
    assert counters(served)["refusals"] == 1


@pytest.mark.parametrize(
    ("route", "read"),
    [
        (
            "/history/bars",
            lambda cancel: history_client.bars(
                "EURUSD", wire.Series.H1, 1_752_570_000, 1_752_573_600, cancel=cancel
            ),
        ),
        ("/history/ranges", lambda cancel: history_client.ranges("EURUSD", cancel=cancel)),
    ],
    ids=["bars", "ranges"],
)
def test_a_cancellation_while_the_server_is_busy_stops_the_client(
    remote, stub, served, floors, cold_reads, monkeypatch, route, read
):
    floor = broker(2025, 7, 15, 12)
    stub.rates[H1] = rates(floor, floor + HOUR)
    kept(floors, Series.H1, floor)
    window = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "start": floor - EDT,
        "end": floor + HOUR - EDT,
    }
    cold_reads.take(2, "/history/bars", window)
    asked = asked_routes(remote, monkeypatch)
    cancel = threading.Event()
    answered = []
    request = threading.Thread(target=lambda: answered.append(read(cancel)))

    request.start()
    until(lambda: counters(served)["refusals"] == 1)
    cancel.set()
    request.join(5)

    assert not request.is_alive()
    assert answered == [None]
    assert asked == [route]
    assert remote.last_error() == (wire.ServerCode.BUSY, f"{route}: the server is busy")
    assert remote.last_error()[0] is wire.ServerCode.BUSY


def test_a_syncing_reply_carries_its_status_and_code_as_members_and_its_retry_after(remote, stub):
    body = {"symbol": "EURUSD", "timeframe": "H1", "start": 1_752_570_000, "end": 1_752_573_600}

    reply = remote.call_route("history/bars", HTTPMethod.POST, "/history/bars", json=body)

    assert (reply.status, reply.retry_after) == (HTTPStatus.SERVICE_UNAVAILABLE, "5")
    assert type(reply.status) is HTTPStatus
    assert reply.envelope["error"]["code"] is wire.ServerCode.SYNCING
    assert remote.last_error()[0] is wire.ServerCode.SYNCING


def test_the_client_raises_server_unreachable_while_the_server_is_not_ready(
    remote, stub, clock_status
):
    clock_status.clear()

    with pytest.raises(
        ServerUnreachable,
        match="^history/bars: server not ready — the broker clock is not verified$",
    ):
        history_client.bars("EURUSD", wire.Series.H1, 1_752_570_000, 1_752_573_600)
    with pytest.raises(ServerUnreachable, match="^history/ranges: server not ready"):
        history_client.ranges("EURUSD")
