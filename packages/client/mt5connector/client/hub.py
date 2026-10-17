"""A consumer's session with the hub on NT's WebSocketClient: the calls it asks, each answered under
its correlation id, and the streams it subscribes to, sent whole again on every reconnect, since the
hub keeps nothing for a consumer that left.

State: the subscriptions wanted, in the order they were first wanted; the next op id and request id;
each request in flight with its call, its answer's parts so far, its deadline and the future its
caller awaits; each command sent and not yet settled, by its MQL5 request id, with its symbol, kept
across a disconnect; the reconnects NT's client has made, and the one the last hello greeted. An op
or a request waits for its connection's hello, which the hub needs before any other frame. A change
of the wanted set and the resend hold one lock across their sends, so the hub receives them in the
order the set changed. A request in flight when NT's client reconnects fails at once: the hub
answers nothing asked on a connection that left."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from nautilus_trader.core.nautilus_pyo3 import (
    WebSocketClient,
    WebSocketClientError,
    WebSocketConfig,
)

from mt5connector.client.constants import RECONNECT_MULTIPLIER
from mt5connector.client.errors import (
    Deferred,
    EaError,
    EaGone,
    HubAnswerError,
    HubTimeout,
    InvalidWindow,
    NoPublisher,
    NotReady,
    Overflow,
    ProtocolError,
    ResponseLost,
    ServerUnreachable,
    Syncing,
)
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import bar_series
from mt5connector.wire.push_wire import PING_INTERVAL_S, Subscription, TransactionType

if TYPE_CHECKING:
    from nautilus_trader.common.component import Logger

    from mt5connector.client.config import MT5Config

# The session's deadline for a request stands this far past the hub's, so the hub's own answer
# arrives first.
SESSION_MARGIN_S = 5.0
_DEADLINES = {
    calls.Wait.READ: calls.READ_DEADLINE_S,
    calls.Wait.COMMAND: calls.COMMAND_DEADLINE_S,
    calls.Wait.HISTORY: calls.HISTORY_DEADLINE_S,
    calls.Wait.TICKS: calls.TICK_BUSY_S,
}


@dataclass(frozen=True)
class Answered:
    """A request's result, typed as the registry gives it, and the MQL5 error its answer folds
    in."""

    result: object
    last_error: int


@dataclass
class _InFlight:
    row: calls.Row
    future: asyncio.Future
    timer: asyncio.TimerHandle
    parts: list = field(default_factory=list)
    # An order_send's symbol, under which its answer that it was sent leaves it outstanding.
    symbol: str | None = None


def _session_call(row: calls.Row) -> Callable:
    """The call as a HubSession method taking its arguments by name; None leaves one out."""
    parameters = []
    for argument in row.arguments:
        if argument.required:
            parameters.append(
                inspect.Parameter(argument.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            )
        else:
            parameters.append(
                inspect.Parameter(
                    argument.name, inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None
                )
            )
    signature = inspect.Signature(parameters)
    name = row.call.name.lower()

    async def call(self: HubSession, *args, **kwargs) -> Answered:
        bound = signature.bind(*args, **kwargs).arguments
        arguments = {key: value for key, value in bound.items() if value is not None}
        return await self.request(row.call, **arguments)

    call.__name__ = name
    call.__qualname__ = f"HubSession.{name}"
    call.__signature__ = signature.replace(
        parameters=[
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_ONLY),
            *signature.parameters.values(),
        ]
    )
    return call


def _with_calls(cls: type) -> type:
    """The class with every registry call as a method asking it."""
    for row in calls.CALLS.values():
        setattr(cls, row.call.name.lower(), _session_call(row))
    return cls


@_with_calls
class HubSession:
    """One consumer's connection to the hub."""

    def __init__(
        self,
        config: MT5Config,
        loop: asyncio.AbstractEventLoop,
        on_frame: Callable[[binary.Ticks | binary.Bar | binary.TradeTransaction], None],
        on_reconnect: Callable[[], None] | None,
        log: Logger,
    ) -> None:
        self._config = config
        self._loop = loop
        self._on_frame = on_frame
        self._on_reconnect = on_reconnect
        self._log = log
        self._ws: WebSocketClient | None = None
        self._wanted: list[Subscription] = []
        self._wanting = asyncio.Lock()
        self._greeting = asyncio.Event()
        self._next_op_id = 1
        self._next_request_id = 1
        self._in_flight: dict[int, _InFlight] = {}
        self._outstanding: dict[int, str] = {}
        self._reconnects = 0
        self._greeted: int | None = None
        self._tasks: set[asyncio.Task] = set()

    async def connect(self) -> None:
        """Connects to the hub, says hello, subscribes everything wanted and asks the outcomes of
        the commands still outstanding; NT's client reconnects with backoff from then on. Raises
        RuntimeError on a connected session."""
        if self._ws is not None:
            raise RuntimeError("hub session: already connected")
        config = WebSocketConfig(
            url=self._config.ws_url,
            headers=[],
            heartbeat=PING_INTERVAL_S,
            reconnect_delay_initial_ms=int(self._config.reconnect_initial_delay_s * 1000),
            reconnect_delay_max_ms=int(self._config.reconnect_max_delay_s * 1000),
            reconnect_backoff_factor=RECONNECT_MULTIPLIER,
        )
        self._ws = await WebSocketClient.connect(
            self._loop,
            config,
            self._on_message,
            post_reconnection=self._on_reconnected_off_loop,
        )
        await self._send_wanted()
        self._track(self._loop.create_task(self._ask_outstanding()))

    async def disconnect(self) -> None:
        """Closes the connection, failing every request in flight; the subscriptions stay wanted.
        Raises RuntimeError on a session never connected."""
        if self._ws is None:
            raise RuntimeError("hub session: not connected")
        for task in self._tasks:
            task.cancel()
        self._fail_in_flight(ResponseLost, "unanswered: the session disconnected")
        await self._ws.disconnect()
        self._ws = None
        self._greeted = None
        self._greeting.clear()

    async def subscribe(self, subscription: Subscription) -> None:
        """Wants a stream, and subscribes it once the connection it rides has had its hello; the
        connect or reconnect sends the wanted set otherwise."""
        async with self._wanting:
            if subscription not in self._wanted:
                self._wanted.append(subscription)
                if self._is_greeted():
                    await self._send(binary.encode(binary.Subscribe(self._op_id(), subscription)))

    async def unsubscribe(self, subscription: Subscription) -> None:
        """Stops wanting a stream, and unsubscribes it once the connection it rides has had its
        hello."""
        async with self._wanting:
            if subscription in self._wanted:
                self._wanted.remove(subscription)
                if self._is_greeted():
                    await self._send(binary.encode(binary.Unsubscribe(self._op_id(), subscription)))

    async def request(self, call: calls.Call, **arguments: object) -> Answered:
        """The answer to a call, its parts assembled; raises the HubAnswerError of the answer's
        error kind, ResponseLost when no answer comes within the session's deadline or across a
        reconnect, and ValueError for arguments the call does not take, a bar read's timeframe among
        them. An order_send's request names the MqlTradeRequest fields it sets; every other member
        is zero."""
        if self._ws is None:
            raise RuntimeError("hub session: not connected")
        row = calls.CALLS[call]
        if call is calls.Call.ORDER_SEND:
            arguments = arguments | {"request": calls.trade_request(arguments["request"])}
        count, body = calls.encode_arguments(row, arguments)
        if row.shape is calls.Shape.RATES:
            bar_series(arguments["timeframe"])
        await self._greeting.wait()
        request_id = self._next_request_id
        self._next_request_id += 1
        timer = self._loop.call_later(
            _DEADLINES[row.wait] + SESSION_MARGIN_S, self._expired, request_id
        )
        flight = _InFlight(row, self._loop.create_future(), timer)
        if call is calls.Call.ORDER_SEND:
            flight.symbol = arguments["request"]["symbol"]
        self._in_flight[request_id] = flight
        try:
            await self._send(binary.encode(binary.Request(request_id, call, count, body)))
        except asyncio.CancelledError:
            # A flight whose request may not have gone out is withdrawn; one whose request went out
            # is the session's to finish, its answer taken as any other once its caller has gone.
            if request_id in self._in_flight:
                self._end(request_id)
            flight.future.cancel()
            raise
        return await flight.future

    async def deferring(self, call: calls.Call, **arguments: object) -> Answered:
        """The answer to a call, asked again after the delay each deferring answer gives until it is
        answered otherwise or the caller cancels."""
        while True:
            try:
                return await self.request(call, **arguments)
            except Deferred as deferred:
                self._log.debug(f"hub session: {deferred}; asking again")
                await asyncio.sleep(deferred.retry_after_s)

    def _op_id(self) -> int:
        op_id = self._next_op_id
        self._next_op_id += 1
        return op_id

    def _is_greeted(self) -> bool:
        """Whether the connection an op would ride has had its hello."""
        return self._greeted == self._reconnects and self._ws.is_active()

    async def _send_wanted(self) -> None:
        async with self._wanting:
            reconnects = self._reconnects
            await self._send(binary.encode(binary.AdapterHello()))
            for subscription in self._wanted:
                await self._send(binary.encode(binary.Subscribe(self._op_id(), subscription)))
            self._greeted = reconnects
            # A reconnect while this resend ran leaves the next connection to its own hello.
            if reconnects == self._reconnects:
                self._greeting.set()

    async def _send(self, raw: bytes) -> None:
        """Sends a frame; one the connection drops is lost with it, and the reconnect resends the
        wanted set and fails every request in flight. NT's client turns a reconnected socket active
        before it calls back, and sends a frame it held through the outage at once, so that frame
        can precede the hello: the hub refuses it and closes, and the reconnect that follows starts
        over."""
        try:
            await self._ws.send(raw)
        except (WebSocketClientError, RuntimeError) as exc:
            self._log.info(f"hub session: a frame not sent, the reconnect starts over: {exc}")

    def _on_message(self, raw: bytes) -> None:
        """Takes a frame from the hub: an answer to its request, a stream frame to the owner; logs a
        refusal and a frame it cannot decode, so the session goes on."""
        try:
            frame = binary.decode(raw, binary.Sender.HUB)
        except binary.UnknownKind as unknown:
            self._log.info(f"hub session: skipping a frame: {unknown}")
        except binary.CodecError as failure:
            self._log.error(f"hub session: a frame it cannot decode: {failure}")
            self._undecodable(failure)
        else:
            if isinstance(frame, binary.Answer):
                self._answer(frame)
            elif isinstance(frame, binary.Busy):
                self._busy(frame)
            elif isinstance(frame, binary.Refusal):
                self._log.error(f"hub session: the hub refused a frame: {frame.message}")
            elif isinstance(frame, binary.Ack):
                self._log.debug(f"hub session: op {frame.op_id} acknowledged")
            else:
                if isinstance(frame, binary.TradeTransaction):
                    self._settle(frame)
                self._handed(frame)

    def _undecodable(self, failure: binary.CodecError) -> None:
        """Fails the request an undecodable answer answers, or every request in flight for a frame
        whose header breaks; an undecodable stream frame fails nothing."""
        if failure.kind is None:
            self._fail_in_flight(ProtocolError, str(failure))
        elif failure.kind is binary.FrameKind.ANSWER and failure.correlation in self._in_flight:
            flight = self._in_flight[failure.correlation]
            self._end(failure.correlation)
            message = f"{flight.row.call.name.lower()}: answer frame: {failure}"
            _fail(flight, ProtocolError(f"hub session: {message}"))

    def _answer(self, answer: binary.Answer) -> None:
        """Resolves a request with its answer once the last part arrives, or fails it with its
        error; an answer to no request in flight is dropped with a warning."""
        flight = self._in_flight.get(answer.request_id)
        if flight is None:
            self._log.warning(f"hub session: dropping an answer to request {answer.request_id}")
        elif answer.error is not None:
            self._end(answer.request_id)
            _fail(flight, _raised(flight.row.call, answer.error, answer.last_error))
        elif answer.part != len(flight.parts):
            self._end(answer.request_id)
            failure = ProtocolError(
                f"hub session: part {answer.part} of request {answer.request_id}, not "
                f"{len(flight.parts)}"
            )
            _fail(flight, failure)
        else:
            try:
                flight.parts.append(calls.read_hub_result(flight.row, answer))
                if answer.last:
                    joined = calls.joined(flight.row, flight.parts)
                    answered = Answered(calls.typed(flight.row, joined), answer.last_error)
            except binary.CodecError as failure:
                self._end(answer.request_id)
                _fail(flight, ProtocolError(f"hub session: {failure}"))
            else:
                if answer.last:
                    self._end(answer.request_id)
                    # Outstanding before the next frame is taken: it may be the transaction that
                    # settles the command.
                    if (
                        flight.row.call is calls.Call.ORDER_SEND
                        and answered.result.retcode == mirror.TRADE_RETCODE_PLACED
                    ):
                        self._outstanding[answered.result.request_id] = flight.symbol
                    if not flight.future.done():
                        flight.future.set_result(answered)

    def _busy(self, busy: binary.Busy) -> None:
        """Moves a request's deadline to the busy's bound past now, with the hub's margin and the
        session's."""
        flight = self._in_flight.get(busy.request_id)
        if flight is not None:
            flight.timer.cancel()
            delay = busy.bound_s + calls.BUSY_MARGIN_S + SESSION_MARGIN_S
            flight.timer = self._loop.call_later(delay, self._expired, busy.request_id)

    def _expired(self, request_id: int) -> None:
        flight = self._in_flight.pop(request_id)
        failure = ResponseLost(
            f"hub session: {flight.row.call.name.lower()} unanswered past the session's deadline"
        )
        _fail(flight, failure)

    def _end(self, request_id: int) -> None:
        self._in_flight.pop(request_id).timer.cancel()

    def _fail_in_flight(self, error: type[ServerUnreachable], why: str) -> None:
        """Fails every request in flight with the error, its message naming the request's call."""
        for request_id, flight in list(self._in_flight.items()):
            self._end(request_id)
            _fail(flight, error(f"hub session: {flight.row.call.name.lower()}: {why}"))

    def _settle(self, frame: binary.TradeTransaction) -> None:
        """Settles a command whose final result the transaction carries."""
        if frame.transaction["type"] == TransactionType.REQUEST:
            self._outstanding.pop(frame.result["request_id"], None)

    def _handed(self, frame: binary.Ticks | binary.Bar | binary.TradeTransaction):
        """Hands the owner a frame, logging one it fails on, so the stream goes on."""
        try:
            self._on_frame(frame)
        except Exception as exc:
            self._log.exception(f"hub session: a {type(frame).__name__} frame failed", exc)

    def _on_reconnected_off_loop(self) -> None:
        # NT calls this on its own thread.
        self._loop.call_soon_threadsafe(self._on_reconnected)

    def _on_reconnected(self) -> None:
        """Holds back every op and request until the new connection's hello, fails the requests in
        flight, tells the owner, then sends the hello and the whole wanted set again and asks the
        outcomes of the commands not yet settled."""
        self._reconnects += 1
        self._greeting.clear()
        self._fail_in_flight(ResponseLost, "unanswered: the hub connection was lost")
        self._log.info("hub session: reconnected, subscribing again what is wanted")
        if self._on_reconnect is not None:
            self._on_reconnect()
        self._track(self._loop.create_task(self._resume()))

    def _track(self, task: asyncio.Task) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _resume(self) -> None:
        await self._send_wanted()
        await self._ask_outstanding()

    async def _ask_outstanding(self) -> None:
        symbols: dict[str, list[int]] = {}
        for request_id, symbol in self._outstanding.items():
            symbols.setdefault(symbol, []).append(request_id)
        await asyncio.gather(
            *(self._ask_outcomes(symbol, tuple(ids)) for symbol, ids in symbols.items())
        )

    async def _ask_outcomes(self, symbol: str, request_ids: tuple[int, ...]) -> None:
        """Asks the publisher of a symbol for the REQUEST transactions of its unsettled commands and
        hands the owner each one that settles a command still outstanding, as a live one arrives;
        the commands the publisher holds no transaction of are dropped with one warning. A command
        whose outcomes go unanswered stays outstanding for the next connect."""
        try:
            answered = await self.deferring(
                calls.Call.OUTCOMES, symbol=symbol, request_ids=request_ids
            )
        except (HubAnswerError, ServerUnreachable) as failure:
            self._log.error(f"hub session: the outcomes of {symbol} were not answered: {failure}")
        else:
            held = {transaction.result["request_id"] for transaction in answered.result}
            for transaction in answered.result:
                # A live transaction may have settled the command while its outcome was asked.
                if transaction.result["request_id"] in self._outstanding:
                    self._settle(transaction)
                    self._handed(transaction)
            dropped = [
                request_id
                for request_id in request_ids
                if request_id not in held and request_id in self._outstanding
            ]
            for request_id in dropped:
                self._outstanding.pop(request_id, None)
            if dropped:
                ids = ", ".join(str(request_id) for request_id in dropped)
                self._log.warning(
                    f"hub session: the publisher of {symbol} holds no outcome of requests {ids}; "
                    "dropped"
                )


def _fail(flight: _InFlight, failure: Exception) -> None:
    """Fails a request its caller still awaits; one it cancelled is ended quietly."""
    if not flight.future.done():
        flight.future.set_exception(failure)


def _raised(call: calls.Call, error: binary.AnswerError, last_error: int) -> HubAnswerError:
    """The error a request's answer raises."""
    message = f"{call.name.lower()}: {error.message}"
    if error.kind is ErrorKind.SYNCING:
        return Syncing(message, last_error, error.retry_after_ms / 1000)
    elif error.kind is ErrorKind.NOT_READY:
        return NotReady(message, last_error, error.retry_after_ms / 1000)
    elif error.kind is ErrorKind.NO_PUBLISHER:
        return NoPublisher(message, last_error, error.retry_after_ms / 1000)
    elif error.kind is ErrorKind.INVALID_WINDOW:
        return InvalidWindow(message, last_error)
    elif error.kind is ErrorKind.OVERFLOW:
        return Overflow(message, last_error)
    elif error.kind is ErrorKind.EA_GONE:
        return EaGone(message, last_error)
    elif error.kind is ErrorKind.TIMEOUT:
        return HubTimeout(message, last_error)
    else:
        return EaError(message, last_error)
