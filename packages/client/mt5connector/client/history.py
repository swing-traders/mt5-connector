"""The MT5 server's history routes, called through the shim's session (`remote_mt5`): the rows of a
bar or tick window once the server vouches for them, and the floors it advertises."""

import threading
import time
from dataclasses import dataclass
from http import HTTPMethod, HTTPStatus

import numpy as np

from mt5connector.client import remote_mt5
from mt5connector.client.errors import ServerUnreachable
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import (
    BARS_PATH,
    RANGES_PATH,
    TICKS_PATH,
    Series,
    ServerCode,
    TickFlags,
)

_BARS = "history/bars"
_TICKS = "history/ticks"
_RANGES = "history/ranges"
_RANGE_FIELDS = {"floor", "measured_at", "generation"}


@dataclass(frozen=True)
class SeriesRange:
    """A series' floor in true UTC, when the server measured it, and its generation."""

    floor: int
    measured_at: int
    generation: int


@dataclass(frozen=True)
class HistoryRanges:
    """The terminal's MaxBars and the floor of every series the server has measured for a symbol."""

    maxbars: int
    series: dict[Series, SeriesRange]


def bars(
    symbol: str, series: Series, start: int, end: int, cancel: threading.Event | None = None
) -> np.ndarray | None:
    """The bars opened in [start, end], true-UTC epoch seconds, each stamped at its open; None for a
    failure, or when `cancel` is set while the server defers it, either leaving last_error set."""
    body = {"symbol": symbol, "timeframe": series.value, "start": start, "end": end}
    rows = _rows(_BARS, BARS_PATH, body, cancel)
    if rows is None:
        return None
    else:
        return remote_mt5.decode_array(_BARS, mirror.RATES, rows)


def ticks(
    symbol: str,
    start: int,
    end: int,
    flags: TickFlags = TickFlags.INFO,
    cancel: threading.Event | None = None,
) -> np.ndarray | None:
    """The ticks from the start of second `start` through the end of second `end`, true-UTC epoch
    seconds; None for a failure, or when `cancel` is set while the server defers them, either
    leaving last_error set."""
    body = {"symbol": symbol, "start": start, "end": end, "flags": flags.value}
    rows = _rows(_TICKS, TICKS_PATH, body, cancel)
    if rows is None:
        return None
    else:
        return remote_mt5.decode_array(_TICKS, mirror.TICKS, rows)


def ranges(symbol: str, cancel: threading.Event | None = None) -> HistoryRanges | None:
    """The terminal's MaxBars and the floors the server has measured for the symbol; None for a
    failure, or when `cancel` is set while the server defers them, either leaving last_error set."""
    reply = _reply(_RANGES, HTTPMethod.GET, RANGES_PATH, cancel, params={"symbol": symbol})
    if reply.envelope["ok"]:
        return _ranges(reply.envelope["result"])
    else:
        return None


def _rows(
    name: str, path: str, body: dict[str, object], cancel: threading.Event | None
) -> list | None:
    """The rows a window route answers."""
    reply = _reply(name, HTTPMethod.POST, path, cancel, json=body)
    if not reply.envelope["ok"]:
        return None
    elif isinstance(reply.envelope["result"], list):
        return reply.envelope["result"]
    else:
        raise ServerUnreachable(f"{name}: the result is not a list of rows")


def _reply(
    name: str,
    method: HTTPMethod,
    path: str,
    cancel: threading.Event | None,
    *,
    json: dict[str, object] | None = None,
    params: dict[str, str] | None = None,
) -> remote_mt5.Reply:
    """A route's answer, asked again each time the server defers it — the terminal syncing, or every
    slot taken — after the delay that answer gives, until it answers otherwise or `cancel` is
    set."""
    reply = remote_mt5.call_route(name, method, path, json=json, params=params)
    while _is_deferred(reply) and _waits_out(
        remote_mt5.retry_after_s(name, reply.retry_after), cancel
    ):
        reply = remote_mt5.call_route(name, method, path, json=json, params=params)
    return reply


def _is_deferred(reply: remote_mt5.Reply) -> bool:
    return reply.status is HTTPStatus.SERVICE_UNAVAILABLE and reply.envelope["error"]["code"] in (
        ServerCode.SYNCING,
        ServerCode.BUSY,
    )


def _waits_out(seconds: float, cancel: threading.Event | None) -> bool:
    """Waits the seconds out unless `cancel` is set first; whether it was not."""
    if cancel is None:
        time.sleep(seconds)
        return True
    else:
        return not cancel.wait(seconds)


def _ranges(result: object) -> HistoryRanges:
    """The ranges a result carries; raises ServerUnreachable for any other shape."""
    if not (
        isinstance(result, dict)
        and set(result) == {"maxbars", "ranges"}
        and _is_integer(result["maxbars"])
        and isinstance(result["ranges"], dict)
        and all(name in set(Series) for name in result["ranges"])
        and all(_is_range(entry) for entry in result["ranges"].values())
    ):
        raise ServerUnreachable(f"{_RANGES}: the result is not the history ranges")
    series = {}
    for name, entry in result["ranges"].items():
        series[Series(name)] = SeriesRange(**entry)
    return HistoryRanges(maxbars=result["maxbars"], series=series)


def _is_range(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _RANGE_FIELDS
        and all(_is_integer(item) for item in value.values())
    )


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
