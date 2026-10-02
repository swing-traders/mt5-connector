"""The MT5 server: the MetaTrader5 package mirrored over HTTP, its liveness, and the commission
schedules the terminal's EA relays."""

import dataclasses
import logging
import os
import threading
from collections.abc import Callable

import waitress
from flask import Flask, request

from mt5connect import mirror
from mt5server.app.commissions import CommissionStore
from mt5server.app.encoding import encode
from mt5server.app.settings import Settings, read_settings
from mt5server.app.terminal import Failed, Terminal

logger = logging.getLogger(__name__)

_TERMINAL_INFO = mirror.FUNCTIONS[mirror.FunctionName.TERMINAL_INFO]


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


def create_app(terminal: Terminal, commissions: CommissionStore) -> Flask:
    """The server's routes: POST /mt5/<function> for every package function, GET /health and GET
    /commissions/<symbol>."""
    app = Flask(__name__, static_folder=None)
    app.json.sort_keys = False
    for function in mirror.FUNCTIONS.values():
        app.add_url_rule(
            f"/mt5/{function.name}",
            endpoint=function.name.value,
            view_func=_mirror_view(terminal, function),
            methods=["POST"],
        )
    app.add_url_rule(
        "/health", endpoint="health", view_func=_health_view(terminal), methods=["GET"]
    )
    app.add_url_rule(
        "/commissions/<symbol>",
        endpoint="commissions",
        view_func=_commissions_view(commissions),
        methods=["GET"],
    )
    return app


def _mirror_view(terminal: Terminal, function: mirror.Function) -> Callable:
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
            outcome = terminal.call(function, arguments)
            if isinstance(outcome, Failed):
                # terminal_info() failing is the terminal's IPC being down.
                if function.name is mirror.FunctionName.TERMINAL_INFO:
                    status = 503
                else:
                    status = 200
                return _failure(*outcome.last_error), status
            else:
                return _answer(encode(function, outcome.value), outcome.last_error), 200

    return view


def _health_view(terminal: Terminal) -> Callable:
    watch = _ConnectionWatch()

    def view():
        outcome = terminal.call(_TERMINAL_INFO, {})
        if isinstance(outcome, Failed):
            return _failure(*outcome.last_error), 503
        else:
            info = encode(_TERMINAL_INFO, outcome.value)
            watch.observe(info["connected"])
            return _answer(info, outcome.last_error), 200

    return view


def _commissions_view(commissions: CommissionStore) -> Callable:
    # No package call answers a relayed schedule, so these envelopes carry no last_error.
    def view(symbol: str):
        schedule = commissions.read(symbol)
        if schedule is None:
            message = f"no commission schedule relayed for {symbol}"
            return {"ok": False, "error": {"code": mirror.RES_E_NOT_FOUND, "message": message}}, 200
        else:
            return {"ok": True, "result": dataclasses.asdict(schedule)}, 200

    return view


class _ConnectionWatch:
    """Logs the terminal's broker connection at INFO each time it changes."""

    def __init__(self) -> None:
        self._connected: bool | None = None
        self._lock = threading.Lock()

    def observe(self, connected: bool) -> None:
        with self._lock:
            changed = connected != self._connected
            self._connected = connected
        if changed:
            logger.info("terminal broker connection: connected=%s", connected)


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
    if unknown:
        return f"unknown parameter: {', '.join(unknown)}"
    elif missing:
        return f"missing parameter: {', '.join(missing)}"
    elif not in_a_form:
        return f"missing parameter: {', or '.join(' and '.join(form) for form in function.forms)}"
    else:
        return None


def _answer(result: object, last_error: tuple[int, str]) -> dict[str, object]:
    """A package call's success envelope, carrying the last_error() read after the call."""
    return {"ok": True, "result": result, "last_error": list(last_error)}


def _failure(code: int, message: str) -> dict[str, object]:
    """A package call's failure envelope, or a refusal's standing in for one; its error is also the
    last_error() it leaves."""
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = read_settings(os.environ)
    import MetaTrader5

    terminal = Terminal(MetaTrader5)
    connect_terminal(terminal, settings)
    waitress.serve(
        create_app(terminal, CommissionStore()),
        host=settings.api_host,
        port=settings.api_port,
        threads=settings.api_threads,
    )


if __name__ == "__main__":
    main()
