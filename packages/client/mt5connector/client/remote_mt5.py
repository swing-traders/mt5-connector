"""The MetaTrader5 package's call surface served by the MT5 server, every epoch in true UTC: its
functions, structs as namedtuples, arrays as numpy structured arrays, constants and Buy/Sell/Close;
and the commission schedules the server relays from the terminal.

State: the server this module is configured against with its HTTP session, and the last_error() pair
the last answered call carried."""

from __future__ import annotations

import inspect
from collections import namedtuple
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from http import HTTPMethod, HTTPStatus
from urllib.parse import quote

import numpy as np
import requests
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError

from mt5connector.client.errors import MT5ConfigError, ResponseLost, ServerBusy, ServerUnreachable
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import ServerCode

__version__ = mirror.PACKAGE_VERSION

CONNECT_TIMEOUT_S = 10.0
# The server serializes every package call, so a request's wait includes the calls queued ahead.
READ_TIMEOUT_S = 60.0

_ENVELOPE_STATUSES = frozenset(
    {HTTPStatus.OK, HTTPStatus.BAD_REQUEST, HTTPStatus.SERVICE_UNAVAILABLE}
)
_SERVER_CODES = frozenset(ServerCode)

_server_url: str | None = None
_ws_url: str | None = None
_session: requests.Session | None = None
_last_error: tuple[int, str] = mirror.SUCCESS

globals().update(mirror.CONSTANTS)

STRUCT_TYPES: dict[mirror.StructName, type] = {
    name: namedtuple(name.value, struct.fields) for name, struct in mirror.STRUCTS.items()
}
globals().update({name.value: struct_type for name, struct_type in STRUCT_TYPES.items()})


def configure(server_url: str, ws_url: str | None = None) -> None:
    global _server_url, _ws_url, _session
    if _session is not None:
        _session.close()
    _server_url = server_url.rstrip("/")
    _ws_url = ws_url
    _session = requests.Session()


def last_error() -> tuple[int, str]:
    """The (code, message) the package's last_error() reported after the last call."""
    return _last_error


def _mirror_function(function: mirror.Function) -> Callable:
    signature = _signature(function)

    def call(*args, **kwargs):
        arguments = signature.bind(*args, **kwargs).arguments
        body = {}
        for param in function.params:
            if param.name in arguments:
                body[param.name] = _wire_value(param, arguments[param.name])
        return _call(function, body)

    call.__name__ = function.name.value
    call.__qualname__ = function.name.value
    call.__module__ = __name__
    call.__signature__ = signature
    return call


def _signature(function: mirror.Function) -> inspect.Signature:
    """The package function's Python signature; its METH_VARARGS functions take no keywords."""
    if function.calling is mirror.Calling.POSITIONAL:
        kind = inspect.Parameter.POSITIONAL_ONLY
    else:
        kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    parameters = []
    for param in function.params:
        if param.required:
            parameters.append(inspect.Parameter(param.name, kind))
        else:
            parameters.append(inspect.Parameter(param.name, kind, default=None))
    return inspect.Signature(parameters)


def _wire_value(param: mirror.Param, value: object) -> object:
    # The package reads a datetime through its timestamp(), so the wire carries that epoch.
    if param.kind is mirror.ParamKind.DATETIME and isinstance(value, datetime):
        return int(value.timestamp())
    else:
        return value


@dataclass(frozen=True)
class Reply:
    """A server's answer to one request, as the shim received it."""

    status: HTTPStatus
    envelope: dict
    retry_after: str | None


def _call(function: mirror.Function, body: dict[str, object]) -> object:
    reply = _exchange(
        function.name,
        HTTPMethod.POST,
        f"/mt5/{function.name}",
        _read_timeout_s(function, body),
        json=body,
    )
    if reply.envelope["ok"]:
        return _decode(function, reply.envelope["result"])
    elif (
        reply.status is HTTPStatus.SERVICE_UNAVAILABLE
        and reply.envelope["error"]["code"] is ServerCode.BUSY
    ):
        raise ServerBusy(f"{function.name}: server busy")
    else:
        return mirror.failure_value(function)


def call_route(
    name: str,
    method: HTTPMethod,
    path: str,
    *,
    json: dict[str, object] | None = None,
    params: dict[str, str] | None = None,
) -> Reply:
    """Calls one of the server's own routes and sets last_error to the pair its envelope carries.
    No read timeout bounds it: a cold history read holds the terminal as long as the package's own
    call takes."""
    return _exchange(name, method, path, None, json=json, params=params)


def _exchange(
    name: str,
    method: HTTPMethod,
    path: str,
    read_timeout_s: float | None,
    *,
    json: dict[str, object] | None = None,
    params: dict[str, str] | None = None,
) -> Reply:
    """The server's reply to a request, its last_error recorded as the shim's."""
    global _last_error
    if _session is None:
        raise MT5ConfigError("remote_mt5: no server is configured")
    try:
        response = _session.request(
            method,
            f"{_server_url}{path}",
            json=json,
            params=params,
            timeout=(CONNECT_TIMEOUT_S, read_timeout_s),
        )
    except requests.RequestException as exc:
        raise _unanswered(name, exc) from exc
    envelope = _envelope(name, response)
    code, message = envelope["last_error"]
    _last_error = (code, message)
    return Reply(HTTPStatus(response.status_code), envelope, response.headers.get("Retry-After"))


def commission_schedule(symbol: str) -> dict | None:
    """The commission schedule the terminal's EA relayed for a symbol, as the server keeps it; None
    while none has been relayed. Leaves last_error as it was: no package call answers it."""
    name = "commissions"
    if _session is None:
        raise MT5ConfigError("remote_mt5: no server is configured")
    try:
        response = _session.get(
            f"{_server_url}/commissions/{quote(symbol, safe='')}",
            timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
        )
    except requests.RequestException as exc:
        raise _unanswered(name, exc) from exc
    try:
        envelope = response.json()
    except requests.JSONDecodeError as exc:
        raise ResponseLost(f"{name}: HTTP {response.status_code} body is not JSON") from exc
    if response.status_code == HTTPStatus.OK and _is_relayed(envelope):
        return envelope["result"]
    elif (
        response.status_code == HTTPStatus.OK
        and _is_server_failure(envelope)
        and envelope["error"]["code"] == mirror.RES_E_NOT_FOUND
    ):
        return None
    elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE and _is_server_failure(envelope):
        raise ServerUnreachable(f"{name}: server not ready — {envelope['error']['message']}")
    else:
        raise ResponseLost(f"{name}: HTTP {response.status_code} body is not an envelope")


def _unanswered(name: str, exc: requests.RequestException) -> ServerUnreachable:
    """The error for a request no reply answered: ServerUnreachable when the connection was never
    made, so the server cannot have seen it, else ResponseLost."""
    cause = exc.args[0] if exc.args else None
    if isinstance(cause, MaxRetryError) and isinstance(cause.reason, ConnectTimeoutError):
        return ServerUnreachable(f"{name}: {exc}")
    else:
        return ResponseLost(f"{name}: {exc}")


def _read_timeout_s(function: mirror.Function, body: dict[str, object]) -> float:
    """The HTTP read timeout: a call carrying the terminal's own timeout waits that long on top."""
    for param in function.params:
        value = body.get(param.name)
        if param.kind is mirror.ParamKind.TIMEOUT_MS and isinstance(value, int):
            return READ_TIMEOUT_S + value / 1000
    return READ_TIMEOUT_S


def _envelope(name: str, response: requests.Response) -> dict:
    """The response's envelope; raises ServerUnreachable for the server refusing the request before
    serving it, ResponseLost for any other answer outside the contract."""
    if response.status_code not in _ENVELOPE_STATUSES:
        raise ResponseLost(f"{name}: HTTP {response.status_code}")
    try:
        envelope = response.json()
    except requests.JSONDecodeError as exc:
        raise ResponseLost(f"{name}: HTTP {response.status_code} body is not JSON") from exc
    if response.status_code == HTTPStatus.OK and _is_answer(envelope):
        return envelope
    elif _is_error(envelope):
        return _with_server_code(envelope)
    elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE and _is_server_failure(envelope):
        raise ServerUnreachable(f"{name}: server not ready — {envelope['error']['message']}")
    else:
        raise ResponseLost(f"{name}: HTTP {response.status_code} body is not an envelope")


def _with_server_code(envelope: dict) -> dict:
    """A failure envelope whose code, when it is one of the server's own, is its ServerCode member;
    a package code stays the package's integer."""
    code, message = envelope["last_error"]
    if code in _SERVER_CODES:
        member = ServerCode(code)
        return envelope | {
            "error": envelope["error"] | {"code": member},
            "last_error": [member, message],
        }
    else:
        return envelope


def _is_answer(envelope: object) -> bool:
    return (
        isinstance(envelope, dict)
        and envelope.get("ok") is True
        and "result" in envelope
        and _is_last_error(envelope.get("last_error"))
    )


def _is_error(envelope: object) -> bool:
    if not isinstance(envelope, dict) or envelope.get("ok") is not False:
        return False
    error = envelope.get("error")
    return (
        isinstance(error, dict)
        and _is_code_and_message(error.get("code"), error.get("message"))
        and _is_last_error(envelope.get("last_error"))
        and envelope.get("last_error") == [error.get("code"), error.get("message")]
    )


def _is_relayed(envelope: object) -> bool:
    """Whether a body is the server's answer from what the terminal relayed: a result no package
    call answered, so it carries no last_error."""
    return (
        isinstance(envelope, dict)
        and envelope.get("ok") is True
        and "result" in envelope
        and "last_error" not in envelope
    )


def _is_server_failure(envelope: object) -> bool:
    """Whether a body is a failure of the server's own — a route it is not ready to serve, or a
    relayed value it does not hold: an error no package call left, so it carries no last_error."""
    if not isinstance(envelope, dict) or envelope.get("ok") is not False:
        return False
    error = envelope.get("error")
    return (
        isinstance(error, dict)
        and _is_code_and_message(error.get("code"), error.get("message"))
        and "last_error" not in envelope
    )


def _is_last_error(value: object) -> bool:
    return isinstance(value, list) and len(value) == 2 and _is_code_and_message(*value)


def _is_code_and_message(code: object, message: object) -> bool:
    return isinstance(code, int) and not isinstance(code, bool) and isinstance(message, str)


def _decode(function: mirror.Function, result: object) -> object:
    """The result in the package's types; raises ResponseLost for any other shape."""
    if not _has_result_shape(function.result, result):
        raise ResponseLost(f"{function.name}: the result is not a {function.result} answer")
    if function.result is mirror.ResultKind.STRUCT:
        return _struct(function.name, mirror.STRUCTS[function.struct], result)
    elif function.result is mirror.ResultKind.STRUCTS:
        struct = mirror.STRUCTS[function.struct]
        return tuple(_struct(function.name, struct, item) for item in result)
    elif function.result is mirror.ResultKind.ARRAY:
        return decode_array(function.name, mirror.ARRAYS[function.array], result)
    elif function.result is mirror.ResultKind.TUPLE:
        return tuple(result)
    else:
        return result


def _has_result_shape(kind: mirror.ResultKind, result: object) -> bool:
    if kind is mirror.ResultKind.STRUCT:
        return isinstance(result, dict)
    elif kind in (mirror.ResultKind.STRUCTS, mirror.ResultKind.ARRAY, mirror.ResultKind.TUPLE):
        return isinstance(result, list)
    elif kind is mirror.ResultKind.BOOL:
        return isinstance(result, bool)
    elif kind is mirror.ResultKind.SCALAR:
        return isinstance(result, int | float) and not isinstance(result, bool)
    else:
        return result is None


def _struct(name: str, struct: mirror.Struct, data: object) -> tuple:
    if not isinstance(data, dict) or set(data) != set(struct.fields):
        raise ResponseLost(f"{name}: the {struct.name} fields are not the package's")
    values = {}
    for field in struct.fields:
        if field in struct.nested:
            values[field] = _struct(name, mirror.STRUCTS[struct.nested[field]], data[field])
        else:
            values[field] = data[field]
    return STRUCT_TYPES[struct.name](**values)


def decode_array(name: str, array: mirror.Array, rows: list) -> np.ndarray:
    """The rows as the package's structured array; raises ResponseLost for a row that does not fit
    its dtype."""
    records = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != set(array.names)
            or not all(_fits(row[field], numpy_type) for field, numpy_type in array.dtype)
        ):
            raise ResponseLost(f"{name}: a {array.name} row does not fit the package's dtype")
        records.append(tuple(row[field] for field in array.names))
    return np.array(records, dtype=np.dtype(list(array.dtype)))


def _fits(value: object, numpy_type: str) -> bool:
    """Whether a JSON value has a numpy field's kind: for a float field a float or an integer within
    its range, for an integer field an integer within its range, and never a bool."""
    if isinstance(value, bool):
        return False
    elif np.issubdtype(numpy_type, np.floating):
        bounds = np.finfo(numpy_type)
        in_range = isinstance(value, int) and float(bounds.min) <= value <= float(bounds.max)
        return isinstance(value, float) or in_range
    else:
        bounds = np.iinfo(numpy_type)
        return isinstance(value, int) and bounds.min <= value <= bounds.max


_MIRRORED: dict[mirror.FunctionName, Callable] = {
    name: _mirror_function(function)
    for name, function in mirror.FUNCTIONS.items()
    if name is not mirror.FunctionName.LAST_ERROR
}
globals().update({name.value: function for name, function in _MIRRORED.items()})

# Buy, Sell and Close behave as the package's own helpers of the same names: a market order at the
# given price, or at the current quote re-sent while the server answers a requote or no prices.
_MARKET_ORDER_ATTEMPTS = 10
_MARKET_ORDER_DEVIATION = 10
_RETRY_RETCODES = (mirror.TRADE_RETCODE_REQUOTE, mirror.TRADE_RETCODE_PRICE_OFF)


def Buy(symbol, volume, price=None, *, comment=None, ticket=None):
    """A market buy at price, or at the current ask when price is None."""
    if price is not None:
        return _market_order(mirror.ORDER_TYPE_BUY, symbol, volume, price, comment, ticket)
    for _ in range(_MARKET_ORDER_ATTEMPTS):
        tick = _MIRRORED[mirror.FunctionName.SYMBOL_INFO_TICK](symbol)
        result = _market_order(mirror.ORDER_TYPE_BUY, symbol, volume, tick.ask, comment, ticket)
        if result is None or result.retcode not in _RETRY_RETCODES:
            break
    return result


def Sell(symbol, volume, price=None, *, comment=None, ticket=None):
    """A market sell at price, or at the current bid when price is None."""
    if price is not None:
        return _market_order(mirror.ORDER_TYPE_SELL, symbol, volume, price, comment, ticket)
    for _ in range(_MARKET_ORDER_ATTEMPTS):
        tick = _MIRRORED[mirror.FunctionName.SYMBOL_INFO_TICK](symbol)
        result = _market_order(mirror.ORDER_TYPE_SELL, symbol, volume, tick.bid, comment, ticket)
        if result is None or result.retcode not in _RETRY_RETCODES:
            break
    return result


def Close(symbol, *, comment=None, ticket=None):
    """Closes the symbol's buy and sell positions, or only the one with ticket: True when every one
    closed, "Partially" when some did, False when none did, None when a quote or a send failed."""
    if ticket is not None:
        positions = _MIRRORED[mirror.FunctionName.POSITIONS_GET](ticket=ticket)
    else:
        positions = _MIRRORED[mirror.FunctionName.POSITIONS_GET](symbol=symbol)
    tried = 0
    done = 0
    for position in positions:
        if position.type in (mirror.ORDER_TYPE_BUY, mirror.ORDER_TYPE_SELL):
            tried += 1
            result = _close_position(symbol, position, comment)
            if result is None:
                return None
            elif result.retcode == mirror.TRADE_RETCODE_DONE:
                done += 1
    if done == 0:
        return False
    elif done == tried:
        return True
    else:
        return "Partially"


def _close_position(symbol, position, comment):
    """The last answer to the opposite market order closing one position; None when a quote or a
    send failed."""
    for _ in range(_MARKET_ORDER_ATTEMPTS):
        tick = _MIRRORED[mirror.FunctionName.SYMBOL_INFO_TICK](symbol)
        if tick is None:
            result = None
        elif position.type == mirror.ORDER_TYPE_BUY:
            result = _market_order(
                mirror.ORDER_TYPE_SELL, symbol, position.volume, tick.bid, comment, position.ticket
            )
        else:
            result = _market_order(
                mirror.ORDER_TYPE_BUY, symbol, position.volume, tick.ask, comment, position.ticket
            )
        if result is None or result.retcode not in _RETRY_RETCODES:
            return result
    return result


def _market_order(order_type, symbol, volume, price, comment, ticket):
    request = {
        "action": mirror.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": _MARKET_ORDER_DEVIATION,
    }
    if comment is not None:
        request["comment"] = comment
    if ticket is not None:
        request["position"] = ticket
    return _MIRRORED[mirror.FunctionName.ORDER_SEND](request)


def tick_from_ws(payload: dict) -> tuple:
    """Convert an EA WebSocket tick message (all-string JSON) to a Tick.

    The EA message has no ``type`` key, stringifies all numerics, and names the epoch millisecond
    field ``time_msec``. The formatted ``time`` string is ignored; epoch seconds are derived from
    ``time_msec``.
    """
    time_msc = int(payload.get("time_msec", "0"))
    return STRUCT_TYPES[mirror.StructName.TICK](
        time=time_msc // 1000,
        bid=float(payload.get("bid", "0.0")),
        ask=float(payload.get("ask", "0.0")),
        last=float(payload.get("last", "0.0")),
        volume=int(float(payload.get("volume", "0"))),
        time_msc=time_msc,
        flags=int(payload.get("flags", "0")),
        volume_real=float(payload.get("volume_real", "0.0")),
    )
