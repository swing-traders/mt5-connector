"""The MT5 server: the MetaTrader5 package mirrored over HTTP with every epoch in true UTC, the
history windows it vouches for, its liveness, and what the terminal's EAs relay — the trade-server
time and each symbol's commission schedule.

State: the calls that can reach the terminal in flight now, the most in flight at once and the calls
refused since start; nothing is persisted."""

import dataclasses
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from http import HTTPStatus

import waitress
from flask import Flask, make_response, request

from mt5connector.server.charts import ChartFailed, HubError, Publishers, hub_charts
from mt5connector.server.clock_check import ClockCheck, ClockStatus
from mt5connector.server.commissions import (
    CommissionStore,
    RelayRefusal,
    commission_schedule,
    commissions_refusal,
)
from mt5connector.server.encoding import encode, non_epochs, package_arguments
from mt5connector.server.history import FloorStore, History, Syncing, bars_refusal, ticks_refusal
from mt5connector.server.server_time import ServerTimeSink, server_time_refusal, server_time_sample
from mt5connector.server.settings import Settings, read_settings
from mt5connector.server.terminal import Answered, Failed, Terminal
from mt5connector.server.wire import mirror
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import (
    BARS_PATH,
    RANGES_PATH,
    TICKS_PATH,
    Series,
    ServerCode,
    TickFlags,
)
from mt5connector.server.wire.push_wire import ChartState
from mt5connector.server.ws_server import COMMISSIONS_RELAY_PATH, SERVER_TIME_RELAY_PATH

logger = logging.getLogger(__name__)

_LOOPBACK = "127.0.0.1"


class _Endpoint(StrEnum):
    HEALTH = "health"
    COMMISSIONS = "commissions"
    SERVER_TIME_RELAY = "server_time_relay"
    COMMISSIONS_RELAY = "commissions_relay"
    HISTORY_BARS = "history_bars"
    HISTORY_TICKS = "history_ticks"
    HISTORY_RANGES = "history_ranges"


# The routes that answer while the broker clock is not verified: the liveness, the relay that
# verifies it, and the commission relay, which carries no time.
_UNGATED = frozenset({_Endpoint.HEALTH, _Endpoint.SERVER_TIME_RELAY, _Endpoint.COMMISSIONS_RELAY})


class TerminalStartError(Exception):
    """Raised when the package cannot initialize its connection to the terminal."""


def connect_terminal(terminal: Terminal, settings: Settings) -> None:
    """Initializes the package against the configured terminal and logs in to its account."""
    outcome = terminal.call(
        mirror.FUNCTIONS[mirror.FunctionName.INITIALIZE],
        {
            "path": settings.terminal_path,
            "login": settings.login,
            "password": settings.password,
            "server": settings.server,
            "timeout": settings.login_timeout_ms,
        },
    )
    if isinstance(outcome, Failed):
        raise TerminalStartError(f"initialize failed: {outcome.last_error}")


@dataclass(frozen=True)
class _Unserved:
    """Why a read of a symbol is not served: no EA publishes it yet, the hub did not take the post,
    or the spawner failed to open the symbol's chart."""

    status: HTTPStatus
    code: int
    message: str
    headers: dict[str, str]


class _Concurrency:
    """The cap on calls that can reach the terminal, one below the server's workers so one is always
    free for /health and the relay, and its counters."""

    def __init__(self, workers: int) -> None:
        self._workers = workers
        self._slots = threading.BoundedSemaphore(workers - 1)
        self._lock = threading.Lock()
        self._in_flight = 0
        self._peak_in_flight = 0
        self._refusals = 0

    def enter(self, route: str) -> bool:
        """Takes a slot without waiting for one; whether one was free. A refusal is counted and
        logged."""
        if self._slots.acquire(blocking=False):
            with self._lock:
                self._in_flight += 1
                self._peak_in_flight = max(self._peak_in_flight, self._in_flight)
            return True
        else:
            with self._lock:
                self._refusals += 1
                in_flight = self._in_flight
            logger.warning("%s refused: %d calls to the terminal in flight", route, in_flight)
            return False

    def leave(self) -> None:
        """Gives back the slot enter() took."""
        # Counted out before the slot is free, so in_flight never exceeds the cap.
        with self._lock:
            self._in_flight -= 1
        self._slots.release()

    def read(self) -> dict[str, int]:
        with self._lock:
            return {
                "in_flight": self._in_flight,
                "peak_in_flight": self._peak_in_flight,
                "refusals": self._refusals,
                "workers": self._workers,
            }


def create_app(
    terminal: Terminal,
    commissions: CommissionStore,
    clock: BrokerClock,
    server_times: ServerTimeSink,
    clock_status: ClockStatus,
    history: History,
    publishers: Publishers,
    *,
    workers: int,
    retry_s: int,
) -> Flask:
    """The server's routes — the package mirror, /health, the commission read, the relays and the
    history routes — behind the clock gate, the publisher check and the cap of `workers` − 1
    terminal-bound calls; `retry_s` is the Retry-After of every deferring answer."""
    concurrency = _Concurrency(workers)
    app = Flask(__name__, static_folder=None)
    app.json.sort_keys = False
    app.before_request(_clock_gate(clock_status))
    for function in mirror.FUNCTIONS.values():
        app.add_url_rule(
            f"/mt5/{function.name}",
            endpoint=function.name.value,
            view_func=_capped(concurrency, retry_s, _mirror_view(terminal, function, clock)),
            methods=["POST"],
        )
    app.add_url_rule(
        "/health",
        endpoint=_Endpoint.HEALTH.value,
        view_func=_health_view(clock_status, concurrency),
        methods=["GET"],
    )
    app.add_url_rule(
        "/commissions/<symbol>",
        endpoint=_Endpoint.COMMISSIONS.value,
        view_func=_commissions_view(commissions, publishers, retry_s),
        methods=["GET"],
    )
    app.add_url_rule(
        SERVER_TIME_RELAY_PATH,
        endpoint=_Endpoint.SERVER_TIME_RELAY.value,
        view_func=_server_time_relay_view(server_times, publishers),
        methods=["POST"],
    )
    app.add_url_rule(
        f"{COMMISSIONS_RELAY_PATH}/<symbol>",
        endpoint=_Endpoint.COMMISSIONS_RELAY.value,
        view_func=_commissions_relay_view(commissions),
        methods=["POST"],
    )
    app.add_url_rule(
        BARS_PATH,
        endpoint=_Endpoint.HISTORY_BARS.value,
        view_func=_charted(
            publishers,
            retry_s,
            _capped(concurrency, retry_s, _history_bars_view(history, server_times, clock)),
        ),
        methods=["POST"],
    )
    app.add_url_rule(
        TICKS_PATH,
        endpoint=_Endpoint.HISTORY_TICKS.value,
        view_func=_charted(
            publishers,
            retry_s,
            _capped(concurrency, retry_s, _history_ticks_view(history, server_times, clock)),
        ),
        methods=["POST"],
    )
    app.add_url_rule(
        RANGES_PATH,
        endpoint=_Endpoint.HISTORY_RANGES.value,
        view_func=_capped(concurrency, retry_s, _history_ranges_view(history)),
        methods=["GET"],
    )
    return app


def _clock_gate(clock_status: ClockStatus) -> Callable:
    def gate():
        if request.endpoint not in _UNGATED and clock_status.read() is None:
            return _clock_unverified(), 503

    return gate


def _charted(publishers: Publishers, retry_s: int, view: Callable) -> Callable:
    """The view behind the publisher check: a read naming a symbol no EA publishes yet is deferred
    while its chart is requested, and refused once the spawner failed to open it."""

    def charted():
        symbol = _named_symbol(_body_arguments())
        if symbol is None:
            return view()
        else:
            unserved = _ensure_published(publishers, symbol, retry_s)
            if unserved is None:
                return view()
            else:
                failure = _failure(unserved.code, unserved.message)
                return failure, unserved.status, unserved.headers

    return charted


def _ensure_published(publishers: Publishers, symbol: str, retry_s: int) -> _Unserved | None:
    """None once an EA publishes the symbol, else why its read is not served; posts the symbol to
    the hub when due."""
    try:
        state = publishers.ensure_chart(symbol)
    except HubError as failure:
        return _Unserved(HTTPStatus.SERVICE_UNAVAILABLE, mirror.RES_E_FAIL, str(failure), {})
    except ChartFailed as failure:
        message = f"the chart of {symbol} failed to open: {failure}"
        return _Unserved(HTTPStatus.BAD_REQUEST, ServerCode.CHART_FAILED, message, {})
    if state == ChartState.PUBLISHED:
        return None
    else:
        message = f"no publisher for {symbol}; chart requested"
        headers = {"Retry-After": str(retry_s)}
        return _Unserved(HTTPStatus.SERVICE_UNAVAILABLE, ServerCode.SYNCING, message, headers)


def _named_symbol(body: object) -> str | None:
    """The symbol a read's body names, or None for a body that names none."""
    if isinstance(body, dict) and isinstance(body.get("symbol"), str) and body["symbol"]:
        return body["symbol"]
    else:
        return None


def _capped(concurrency: _Concurrency, retry_s: int, view: Callable) -> Callable:
    """The view run in a slot of the cap, or refused at once when none is free: a call never waits
    on a worker for one."""

    def capped():
        if concurrency.enter(request.path):
            try:
                # Built in the slot, so serializing a large answer counts against the cap.
                return make_response(view())
            finally:
                concurrency.leave()
        else:
            message = f"{request.path}: the server is busy"
            headers = {"Retry-After": str(retry_s)}
            return _failure(ServerCode.BUSY, message), HTTPStatus.SERVICE_UNAVAILABLE, headers

    return capped


def _mirror_view(terminal: Terminal, function: mirror.Function, clock: BrokerClock) -> Callable:
    def view():
        arguments = _body_arguments()
        refusal = _refusal(function, arguments)
        if refusal is not None:
            return _failure(mirror.RES_E_INVALID_PARAMS, refusal), 400
        elif function.server_session:
            # Every client shares the server's terminal session; one client's shutdown() ending it
            # would end it for all of them.
            logger.info("%s from %s keeps the server's session", function.name, request.remote_addr)
            return _answer(None, mirror.SUCCESS), 200
        else:
            outcome = terminal.call(function, package_arguments(function, arguments, clock))
            if isinstance(outcome, Failed):
                # terminal_info() failing is the terminal's IPC being down.
                if function.name is mirror.FunctionName.TERMINAL_INFO:
                    status = 503
                else:
                    status = 200
                return _failure(*outcome.last_error), status
            else:
                return _answer(encode(function, outcome.value, clock), outcome.last_error), 200

    return view


def _health_view(clock_status: ClockStatus, concurrency: _Concurrency) -> Callable:
    # The clock's status and the cap's counters answer it, so it never waits on the terminal's lock;
    # no package call answers it, so its envelope carries no last_error.
    def view():
        verification = clock_status.read()
        if verification is None:
            return _clock_unverified(), 503
        else:
            return {
                "ok": True,
                "result": dataclasses.asdict(verification) | concurrency.read(),
            }, 200

    return view


def _commissions_view(
    commissions: CommissionStore, publishers: Publishers, retry_s: int
) -> Callable:
    # No package call answers a relayed schedule, so these envelopes carry no last_error.
    def view(symbol: str):
        unserved = _ensure_published(publishers, symbol, retry_s)
        relay = commissions.read(symbol)
        if unserved is not None:
            error = {"code": unserved.code, "message": unserved.message}
            return {"ok": False, "error": error}, unserved.status, unserved.headers
        elif relay is None:
            message = f"no commission schedule relayed for {symbol}"
            error = {"code": ServerCode.SYNCING, "message": message}
            return (
                {"ok": False, "error": error},
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"Retry-After": str(retry_s)},
            )
        elif isinstance(relay, RelayRefusal):
            message = f"the commission relay for {symbol} was refused: {relay.reason}"
            error = {"code": ServerCode.RELAY_REFUSED, "message": message}
            return {"ok": False, "error": error}, HTTPStatus.UNPROCESSABLE_ENTITY
        else:
            return {"ok": True, "result": dataclasses.asdict(relay)}, 200

    return view


def _server_time_relay_view(server_times: ServerTimeSink, publishers: Publishers) -> Callable:
    # No package call answers a relayed frame, so these envelopes carry no last_error.
    def view():
        frame = _body_arguments()
        refusal = server_time_refusal(frame)
        if request.remote_addr != _LOOPBACK:
            return _not_loopback()
        elif refusal is not None:
            error = {"code": mirror.RES_E_INVALID_PARAMS, "message": refusal}
            return {"ok": False, "error": error}, 400
        else:
            sample = server_time_sample(frame)
            server_times.write(sample)
            publishers.saw(sample.symbol)
            return {"ok": True, "result": None}, 200

    return view


def _commissions_relay_view(commissions: CommissionStore) -> Callable:
    # No package call answers a relayed frame, so these envelopes carry no last_error.
    def view(symbol: str):
        frame = _body_arguments()
        refusal = commissions_refusal(frame, symbol)
        if request.remote_addr != _LOOPBACK:
            return _not_loopback()
        elif refusal is not None:
            commissions.write(symbol, RelayRefusal(refusal))
            error = {"code": mirror.RES_E_INVALID_PARAMS, "message": refusal}
            return {"ok": False, "error": error}, 400
        else:
            commissions.write(symbol, commission_schedule(frame))
            return {"ok": True, "result": None}, 200

    return view


def _not_loopback() -> tuple[dict[str, object], int]:
    """The answer to a relayed frame that did not arrive over loopback."""
    message = f"{request.remote_addr} is not loopback"
    return {"ok": False, "error": {"code": mirror.RES_E_FAIL, "message": message}}, 403


def _history_bars_view(
    history: History, server_times: ServerTimeSink, clock: BrokerClock
) -> Callable:
    def view():
        body = _body_arguments()
        refusal = bars_refusal(body, _terminal_time(server_times, clock))
        if refusal is not None:
            return _failure(mirror.RES_E_INVALID_PARAMS, refusal), 400
        else:
            return _outcome(
                history.bars(body["symbol"], Series(body["timeframe"]), body["start"], body["end"])
            )

    return view


def _history_ticks_view(
    history: History, server_times: ServerTimeSink, clock: BrokerClock
) -> Callable:
    def view():
        body = _body_arguments()
        refusal = ticks_refusal(body, _terminal_time(server_times, clock))
        if refusal is not None:
            return _failure(mirror.RES_E_INVALID_PARAMS, refusal), 400
        else:
            flags = TickFlags(body.get("flags", TickFlags.INFO))
            return _outcome(history.ticks(body["symbol"], body["start"], body["end"], flags))

    return view


def _history_ranges_view(history: History) -> Callable:
    def view():
        symbol = request.args.get("symbol", "")
        if not symbol:
            return _failure(mirror.RES_E_INVALID_PARAMS, "missing parameter: symbol"), 400
        else:
            return _outcome(history.ranges(symbol))

    return view


def _terminal_time(server_times: ServerTimeSink, clock: BrokerClock) -> int:
    """The terminal's current time in true UTC: the trade-server time of the relay's latest sample,
    which a verified clock always has, aged by the time since it arrived."""
    received = server_times.latest()
    return clock.to_utc(received.sample.trade_server) + int(time.monotonic() - received.arrived)


def _clock_unverified() -> dict[str, object]:
    """The envelope of a route refused while the broker clock is not verified."""
    message = "the broker clock is not verified"
    return {"ok": False, "error": {"code": mirror.RES_E_FAIL, "message": message}}


def _body_arguments() -> object:
    """The request's JSON body; an empty body is no arguments."""
    if not request.get_data():
        return {}
    return request.get_json(force=True, silent=True)


def _refusal(function: mirror.Function, arguments: object) -> str | None:
    """Why these arguments cannot be passed to the function, or None when they can."""
    if not isinstance(arguments, dict):
        return "request body is not a JSON object"
    names = {param.name for param in function.params}
    unknown = sorted(name for name in arguments if name not in names)
    missing = [
        param.name for param in function.params if param.required and param.name not in arguments
    ]
    in_a_form = not function.forms or any(
        all(name in arguments for name in form) for form in function.forms
    )
    not_epochs = non_epochs(function, arguments)
    if unknown:
        return f"unknown parameter: {', '.join(unknown)}"
    elif missing:
        return f"missing parameter: {', '.join(missing)}"
    elif not in_a_form:
        return f"missing parameter: {', or '.join(' and '.join(form) for form in function.forms)}"
    elif not_epochs:
        return f"not an integer epoch: {', '.join(not_epochs)}"
    else:
        return None


def _answer(result: object, last_error: tuple[int, str]) -> dict[str, object]:
    """A package call's success envelope, carrying the last_error() read after the call."""
    return {"ok": True, "result": result, "last_error": list(last_error)}


def _outcome(outcome: Answered | Failed | Syncing) -> tuple:
    """The response to an answer the server composed from package calls: one it cannot give yet is
    HTTP 503, its Retry-After the delay before the client asks again."""
    if isinstance(outcome, Syncing):
        return _failure(*outcome.last_error), 503, {"Retry-After": str(outcome.retry_after_s)}
    elif isinstance(outcome, Failed):
        return _failure(*outcome.last_error), 200
    else:
        return _answer(outcome.value, outcome.last_error), 200


def _failure(code: int, message: str) -> dict[str, object]:
    """A package call's failure envelope, or a refusal's standing in for one; its error is also the
    last_error() it leaves."""
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = read_settings(os.environ)
    clock = BrokerClock(settings.broker_tz, timedelta(hours=settings.broker_offset_hours))
    logger.info(
        "broker clock: %s %+d h, verified against the relayed trade-server time every %d s",
        settings.broker_tz.key,
        settings.broker_offset_hours,
        settings.clock_check_seconds,
    )
    import MetaTrader5

    terminal = Terminal(MetaTrader5)
    connect_terminal(terminal, settings)
    server_times = ServerTimeSink(max_age_s=settings.clock_sample_max_age_seconds)
    clock_check = ClockCheck(
        server_times,
        clock,
        max_age_s=settings.clock_sample_max_age_seconds,
        check_s=settings.clock_check_seconds,
        bootstrap_s=settings.clock_bootstrap_seconds,
    )
    threading.Thread(target=clock_check.run, name="broker-clock-check", daemon=True).start()
    history = History(
        terminal,
        clock,
        FloorStore(),
        retry_s=settings.history_retry_seconds,
        floor_ttl_s=settings.floor_ttl_seconds,
    )
    publishers = Publishers(
        hub_charts(f"http://127.0.0.1:{settings.hub_port}"),
        fresh_s=settings.clock_sample_max_age_seconds,
        touch_s=settings.chart_idle_seconds / 2,
        retry_s=settings.history_retry_seconds,
    )
    app = create_app(
        terminal,
        CommissionStore(),
        clock,
        server_times,
        clock_check.status,
        history,
        publishers,
        workers=settings.api_threads,
        retry_s=settings.history_retry_seconds,
    )
    waitress.serve(
        app,
        host=settings.api_host,
        port=settings.api_port,
        threads=settings.api_threads,
    )


if __name__ == "__main__":
    main()
