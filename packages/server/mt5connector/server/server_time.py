"""The trade-server time the terminal's EA relays through the hub, and the latest sample of it.

State: the latest sample, the monotonic time it arrived, and when the terminal's current unbroken
run of connected samples began; a disconnected sample, or a gap of the maximum age between two
samples, breaks the run. Nothing is persisted."""

import threading
import time
from dataclasses import dataclass

from mt5connector.server.ws_server import FrameType

_FRAME_VERSION = 1
_FIELDS = ("v", "type", "symbol", "trade_server", "current", "gmt", "connected")
_EPOCH_FIELDS = ("trade_server", "current", "gmt")


@dataclass(frozen=True)
class ServerTimeSample:
    """The trade server's time as one chart's EA relayed it."""

    symbol: str
    trade_server: int
    current: int
    gmt: int
    connected: bool


@dataclass(frozen=True)
class Received:
    sample: ServerTimeSample
    arrived: float
    connected_since: float | None


class ServerTimeSink:
    """The latest relayed sample, with the time it arrived on the monotonic clock and the start of
    the connected run it belongs to."""

    def __init__(self, *, max_age_s: int) -> None:
        self._max_age_s = max_age_s
        self._latest: Received | None = None
        self._arrival = threading.Condition()

    def write(self, sample: ServerTimeSample) -> None:
        with self._arrival:
            arrived = time.monotonic()
            previous = self._latest
            if not sample.connected:
                connected_since = None
            elif (
                previous is not None
                and previous.connected_since is not None
                and arrived - previous.arrived < self._max_age_s
            ):
                connected_since = previous.connected_since
            else:
                connected_since = arrived
            self._latest = Received(sample, arrived, connected_since)
            self._arrival.notify_all()

    def latest(self) -> Received | None:
        with self._arrival:
            return self._latest

    def wait_newer(self, than: Received | None, timeout: float | None) -> Received | None:
        """The latest sample once it is not `than`, or whichever it is when the timeout ends the
        wait; a None timeout waits without end."""
        with self._arrival:
            self._arrival.wait_for(lambda: self._latest is not than, timeout)
            return self._latest


def server_time_refusal(frame: object) -> str | None:
    """Why a relayed body is not a server_time frame, or None when it is."""
    if not isinstance(frame, dict):
        return "request body is not a JSON object"
    missing = [name for name in _FIELDS if name not in frame]
    unknown = sorted(name for name in frame if name not in _FIELDS)
    not_epochs = [name for name in _EPOCH_FIELDS if name in frame and not _is_integer(frame[name])]
    if missing:
        return f"missing field: {', '.join(missing)}"
    elif unknown:
        return f"unknown field: {', '.join(unknown)}"
    elif not (_is_integer(frame["v"]) and frame["v"] == _FRAME_VERSION):
        return f"unsupported frame version: {frame['v']!r}"
    elif frame["type"] != FrameType.SERVER_TIME:
        return f"not a {FrameType.SERVER_TIME} frame: {frame['type']!r}"
    elif not (isinstance(frame["symbol"], str) and frame["symbol"]):
        return f"not a symbol: {frame['symbol']!r}"
    elif not_epochs:
        return f"not an integer epoch: {', '.join(not_epochs)}"
    elif not (_is_integer(frame["connected"]) and frame["connected"] in (0, 1)):
        return f"connected is not 0 or 1: {frame['connected']!r}"
    else:
        return None


def server_time_sample(frame: dict) -> ServerTimeSample:
    """The sample a server_time frame carries; the frame is one server_time_refusal accepts."""
    return ServerTimeSample(
        symbol=frame["symbol"],
        trade_server=frame["trade_server"],
        current=frame["current"],
        gmt=frame["gmt"],
        connected=frame["connected"] == 1,
    )


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
