"""The MetaTrader5 package's call surface served by an MT5 server, every epoch in true UTC, and the
commission schedules the server relays from the terminal.

State: per RemoteMT5, its server's URL, its HTTP session, and the last_error() pair its last
answered call carried."""

from __future__ import annotations

import inspect
import time
from collections import namedtuple
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from http import HTTPMethod, HTTPStatus
from urllib.parse import quote

import numpy as np
import requests
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError

from mt5connector.client.errors import (
    MT5InstrumentError,
    ResponseLost,
    ServerBusy,
    ServerUnreachable,
)
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

globals().update(mirror.CONSTANTS)

STRUCT_TYPES: dict[mirror.StructName, type] = {
    name: namedtuple(name.value, struct.fields) for name, struct in mirror.STRUCTS.items()
}
globals().update({name.value: struct_type for name, struct_type in STRUCT_TYPES.items()})


def _mirror_function(function: mirror.Function) -> Callable:
    """The package function as a RemoteMT5 method: bound, it takes the package's own signature."""
    signature = _signature(function)

    def call(self: RemoteMT5, *args, **kwargs):
        arguments = signature.bind(*args, **kwargs).arguments
        body = {}
        for param in function.params:
            if param.name in arguments:
                body[param.name] = _wire_value(param, arguments[param.name])
        return self._call(function, body)

    call.__name__ = function.name.value
    call.__qualname__ = f"RemoteMT5.{function.name.value}"
    call.__module__ = __name__
    call.__signature__ = signature.replace(
        parameters=[
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_ONLY),
            *signature.parameters.values(),
        ]
    )
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


def _mirrored(cls: type) -> type:
    """The class with every package function but last_error as a method calling the server."""
    for function in mirror.FUNCTIONS.values():
        if function.name is not mirror.FunctionName.LAST_ERROR:
            setattr(cls, function.name.value, _mirror_function(function))
    return cls


# Buy, Sell and Close behave as the package's own helpers of the same names: a market order at the
# given price, or at the current quote re-sent while the server answers a requote or no prices.
_MARKET_ORDER_ATTEMPTS = 10
_MARKET_ORDER_DEVIATION = 10
_RETRY_RETCODES = (mirror.TRADE_RETCODE_REQUOTE, mirror.TRADE_RETCODE_PRICE_OFF)


@_mirrored
class RemoteMT5:
    """The MetaTrader5 package as one MT5 server serves it, every call on this object's own HTTP
    session."""

    def __init__(self, server_url: str) -> None:
        self._server_url = server_url.rstrip("/")
        self._session = requests.Session()
        self._last_error: tuple[int, str] = mirror.SUCCESS

    def close_session(self) -> None:
        """Closes the HTTP session's pooled connections; a later call opens a fresh one."""
        self._session.close()

    def last_error(self) -> tuple[int, str]:
        """The (code, message) the package's last_error() reported after this object's last answered
        call."""
        return self._last_error

    def call_route(
        self,
        name: str,
        method: HTTPMethod,
        path: str,
        *,
        json: dict[str, object] | None = None,
        params: dict[str, str] | None = None,
    ) -> Reply:
        """Calls one of the server's own routes and sets last_error to the pair its envelope
        carries. No read timeout bounds it: a cold history read holds the terminal as long as the
        package's own call takes."""
        return self._exchange(name, method, path, None, json=json, params=params)

    def commission_schedule(self, symbol: str) -> dict:
        """Waits for the symbol's relayed schedule, retrying after the server's advertised delay;
        raises MT5InstrumentError when the server refused the symbol's last relay or the symbol's
        chart failed to open, and ServerBusy when it refuses the read with every slot taken. Leaves
        last_error as it was: no package call answers it."""
        name = "commissions"
        while True:
            try:
                response = self._session.get(
                    f"{self._server_url}/commissions/{quote(symbol, safe='')}",
                    timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                )
            except requests.RequestException as exc:
                raise _unanswered(name, exc) from exc
            try:
                envelope = response.json()
            except requests.JSONDecodeError as exc:
                raise ResponseLost(f"{name}: HTTP {response.status_code} body is not JSON") from exc
            if (
                response.status_code == HTTPStatus.OK
                and _is_relayed(envelope)
                and isinstance(envelope["result"], dict)
            ):
                return envelope["result"]
            elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE and _is_server_failure(
                envelope
            ):
                if envelope["error"]["code"] == ServerCode.SYNCING:
                    time.sleep(retry_after_s(name, response.headers.get("Retry-After")))
                else:
                    raise ServerUnreachable(
                        f"{name}: server not ready — {envelope['error']['message']}"
                    )
            elif (
                response.status_code == HTTPStatus.SERVICE_UNAVAILABLE
                and _is_error(envelope)
                and envelope["error"]["code"] == ServerCode.BUSY
            ):
                raise ServerBusy(
                    f"{name}: server busy", retry_after_s(name, response.headers.get("Retry-After"))
                )
            elif (
                response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
                and _is_server_failure(envelope)
                and envelope["error"]["code"] == ServerCode.RELAY_REFUSED
            ):
                raise MT5InstrumentError(f"{name}: {envelope['error']['message']}")
            elif (
                response.status_code == HTTPStatus.BAD_REQUEST
                and _is_server_failure(envelope)
                and envelope["error"]["code"] == ServerCode.CHART_FAILED
            ):
                raise MT5InstrumentError(f"{name}: {envelope['error']['message']}")
            else:
                raise ResponseLost(f"{name}: HTTP {response.status_code} body is not an envelope")

    def Buy(self, symbol, volume, price=None, *, comment=None, ticket=None):
        """A market buy at price, or at the current ask when price is None."""
        if price is not None:
            return self._market_order(mirror.ORDER_TYPE_BUY, symbol, volume, price, comment, ticket)
        for _ in range(_MARKET_ORDER_ATTEMPTS):
            tick = self.symbol_info_tick(symbol)
            result = self._market_order(
                mirror.ORDER_TYPE_BUY, symbol, volume, tick.ask, comment, ticket
            )
            if result is None or result.retcode not in _RETRY_RETCODES:
                break
        return result

    def Sell(self, symbol, volume, price=None, *, comment=None, ticket=None):
        """A market sell at price, or at the current bid when price is None."""
        if price is not None:
            return self._market_order(
                mirror.ORDER_TYPE_SELL, symbol, volume, price, comment, ticket
            )
        for _ in range(_MARKET_ORDER_ATTEMPTS):
            tick = self.symbol_info_tick(symbol)
            result = self._market_order(
                mirror.ORDER_TYPE_SELL, symbol, volume, tick.bid, comment, ticket
            )
            if result is None or result.retcode not in _RETRY_RETCODES:
                break
        return result

    def Close(self, symbol, *, comment=None, ticket=None):
        """Closes the symbol's buy and sell positions, or only the one with ticket: True when every
        one closed, "Partially" when some did, False when none did, None when a quote or a send
        failed."""
        if ticket is not None:
            positions = self.positions_get(ticket=ticket)
        else:
            positions = self.positions_get(symbol=symbol)
        tried = 0
        done = 0
        for position in positions:
            if position.type in (mirror.ORDER_TYPE_BUY, mirror.ORDER_TYPE_SELL):
                tried += 1
                result = self._close_position(symbol, position, comment)
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

    def _close_position(self, symbol, position, comment):
        """The last answer to the opposite market order closing one position; None when a quote or a
        send failed."""
        for _ in range(_MARKET_ORDER_ATTEMPTS):
            tick = self.symbol_info_tick(symbol)
            if tick is None:
                result = None
            elif position.type == mirror.ORDER_TYPE_BUY:
                result = self._market_order(
                    mirror.ORDER_TYPE_SELL,
                    symbol,
                    position.volume,
                    tick.bid,
                    comment,
                    position.ticket,
                )
            else:
                result = self._market_order(
                    mirror.ORDER_TYPE_BUY,
                    symbol,
                    position.volume,
                    tick.ask,
                    comment,
                    position.ticket,
                )
            if result is None or result.retcode not in _RETRY_RETCODES:
                return result
        return result

    def _market_order(self, order_type, symbol, volume, price, comment, ticket):
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
        return self.order_send(request)

    def _call(self, function: mirror.Function, body: dict[str, object]) -> object:
        reply = self._exchange(
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
            raise ServerBusy(
                f"{function.name}: server busy", retry_after_s(function.name, reply.retry_after)
            )
        else:
            return mirror.failure_value(function)

    def _exchange(
        self,
        name: str,
        method: HTTPMethod,
        path: str,
        read_timeout_s: float | None,
        *,
        json: dict[str, object] | None = None,
        params: dict[str, str] | None = None,
    ) -> Reply:
        """The server's reply to a request, its last_error recorded as this object's."""
        try:
            response = self._session.request(
                method,
                f"{self._server_url}{path}",
                json=json,
                params=params,
                timeout=(CONNECT_TIMEOUT_S, read_timeout_s),
            )
        except requests.RequestException as exc:
            raise _unanswered(name, exc) from exc
        envelope = _envelope(name, response)
        code, message = envelope["last_error"]
        self._last_error = (code, message)
        return Reply(
            HTTPStatus(response.status_code), envelope, response.headers.get("Retry-After")
        )


def retry_after_s(name: str, retry_after: str | None) -> int:
    """The delay in seconds a deferring answer's Retry-After gives, HTTP's whole non-negative
    seconds; raises ServerUnreachable for none."""
    if retry_after is not None and retry_after.isascii() and retry_after.isdigit():
        return int(retry_after)
    else:
        raise ServerUnreachable(f"{name}: a deferring answer's Retry-After is {retry_after!r}")


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
