"""Every epoch a hub frame carries converted at the era of the timestamp itself: a request's
arguments from true UTC to the broker's clock on their way to the EA, and what an EA sends — a
published tick, bar or transaction, an answer's results — from the broker's clock to true UTC. A
zero epoch stays zero both ways. A range read asks the broker bounds its window reads across, and
keeps the records that read into it at either occurrence of a repeated hour.

State: a Conversion's verdict on each broker hour it met, whether the clock repeats or skips it, for
one frame or answer part; and a request's Results, for the life of its answer, the true-UTC window
it keeps records of and the true-UTC open of the last bar it passed on, so a bar opening in the same
period in a later part is dropped."""

from dataclasses import dataclass

import numpy as np

from mt5connector.server.encoding import bar_close, bar_closes
from mt5connector.server.eras import Eras
from mt5connector.server.repeated_hours import RepeatedHours
from mt5connector.server.windows import broker_window, reading_into
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import BAR_CLOSE, BARS, Series, bar_series
from mt5connector.server.wire.push_wire import (
    TRANSACTION_EPOCHS,
    Stream,
    Subscription,
    TransactionType,
)

_HOUR_S = 3_600
# The reads of a true-UTC window, which their EA reads across its broker window.
_WINDOWS = frozenset({calls.Call.COPY_RATES_RANGE, calls.Call.COPY_TICKS_RANGE})
_BARS = np.dtype(list(BARS.dtype))


class Conversion:
    """Epochs converted to true UTC, each in an hour the broker's clock repeats or skips warned of
    through the process's RepeatedHours; an hour is judged once per conversion."""

    def __init__(self, clock: BrokerClock, repeated_hours: RepeatedHours) -> None:
        self._clock = clock
        self._repeated_hours = repeated_hours
        self._hours: dict[int, tuple[bool, bool]] = {}
        self.eras = Eras(clock)

    def utc(self, unit: mirror.EpochUnit, value: int, where: str, field: str) -> int:
        if unit is mirror.EpochUnit.SECONDS:
            converted = self._clock.to_utc(value)
            seconds = value
        else:
            converted = self._clock.to_utc_msc(value)
            seconds = value // 1000
        if value != 0:
            hour = seconds // _HOUR_S
            if hour not in self._hours:
                self._hours[hour] = (
                    self._clock.is_ambiguous(seconds),
                    self._clock.is_skipped(seconds),
                )
            ambiguous, skipped = self._hours[hour]
            if ambiguous:
                self._repeated_hours.warn(where, field, value, hour)
            elif skipped:
                self._repeated_hours.warn_skipped(where, field, value, hour)
        return converted

    def record(
        self, epochs: dict[str, mirror.EpochUnit], record: dict[str, object], where: str
    ) -> dict[str, object]:
        return record | {
            name: self.utc(unit, record[name], where, name) for name, unit in epochs.items()
        }

    def rows(self, epochs: dict[str, mirror.EpochUnit], rows: np.ndarray, where: str) -> np.ndarray:
        """Each epoch field of the rows in true UTC, the clock read once per broker hour."""
        converted = rows.copy()
        for name, unit in epochs.items():
            values = rows[name].astype(np.int64)
            if unit is mirror.EpochUnit.SECONDS:
                converted[name] = self.eras.to_utc(values)
                seconds = values
            else:
                converted[name] = self.eras.to_utc_msc(values)
                seconds = values // 1000
            self._warn_hours(values[values != 0], seconds[values != 0], where, name)
        return converted

    def _warn_hours(self, values: np.ndarray, seconds: np.ndarray, where: str, field: str) -> None:
        """Warns of each repeated or skipped broker hour the values fall in, naming the first value
        in it the clock repeats or skips."""
        if len(seconds) == 0:
            return
        hours = seconds // _HOUR_S
        # A stable sort keeps each hour's rows in their order, and the hours in their first rows'.
        order = np.argsort(hours, kind="stable")
        _, starts = np.unique(hours[order], return_index=True)
        groups = np.split(order, starts[1:])
        for rows in sorted(groups, key=lambda group: int(group[0])):
            flagged = self.eras.first_flagged(seconds[rows])
            if flagged is not None:
                position, repeated, skipped = flagged
                value = int(values[rows[position]])
                hour = int(hours[rows[position]])
                if repeated:
                    self._repeated_hours.warn(where, field, value, hour)
                else:
                    self._repeated_hours.warn_skipped(where, field, value, hour)


@dataclass(frozen=True)
class Published:
    """A frame an EA published, as its consumers receive it: the subscription it reaches and the
    symbol it is of — a tick's or a bar's, a request transaction's request's, any other
    transaction's own, empty for one that names none."""

    subscription: Subscription
    symbol: str
    frame: binary.Ticks | binary.Bar | binary.TradeTransaction


def published(
    frame: binary.Ticks | binary.RatesBar | binary.TradeTransaction,
    clock: BrokerClock,
    repeated_hours: RepeatedHours,
) -> Published:
    """A published frame with every epoch in true UTC, and a bar by its series with its close;
    raises ValueError for a bar of a timeframe no series serves."""
    conversion = Conversion(clock, repeated_hours)
    if isinstance(frame, binary.Ticks):
        ticks = tuple(
            conversion.record(mirror.TICKS.epochs, tick, binary.FrameKind.TICK.name.lower())
            for tick in frame.ticks
        )
        subscription = Subscription(Stream.TICKS, frame.symbol)
        passed = Published(subscription, frame.symbol, binary.Ticks(frame.symbol, ticks))
    elif isinstance(frame, binary.RatesBar):
        series = bar_series(frame.timeframe)
        where = binary.FrameKind.BAR.name.lower()
        bar = conversion.record(mirror.RATES.epochs, frame.rates, where)
        bar[BAR_CLOSE] = bar_close(clock, series, frame.rates["time"])
        subscription = Subscription(Stream.BARS, frame.symbol, series)
        passed = Published(subscription, frame.symbol, binary.Bar(frame.symbol, series, bar))
    else:
        # MQL5 fills only the type of a request transaction; its request carries the rest.
        if frame.transaction["type"] == TransactionType.REQUEST:
            symbol = frame.request["symbol"]
        else:
            symbol = frame.transaction["symbol"]
        transaction = _transaction(
            conversion, frame, binary.FrameKind.TRADE_TRANSACTION.name.lower()
        )
        passed = Published(Subscription(Stream.TRADE_TRANSACTIONS), symbol, transaction)
    return passed


def _transaction(
    conversion: Conversion, frame: binary.TradeTransaction, where: str
) -> binary.TradeTransaction:
    """A transaction with its own and its request's epochs in true UTC."""
    return binary.TradeTransaction(
        conversion.record(TRANSACTION_EPOCHS, frame.transaction, f"{where}.transaction"),
        conversion.record(binary.TRADE_REQUEST.epochs, frame.request, f"{where}.request"),
        frame.result,
    )


def broker_arguments(
    row: calls.Row, arguments: dict[str, object], clock: BrokerClock
) -> dict[str, object]:
    """A request's arguments as its EA reads them: each epoch on the broker's clock, a trade
    request's included, and a range read's window as the broker bounds it reads across."""
    converted = dict(arguments)
    if row.call in _WINDOWS:
        lo, hi = broker_window(clock, arguments["date_from"], arguments["date_to"]).fetch
        # A zero bound is no time and stays zero.
        if arguments["date_from"] != 0:
            converted["date_from"] = lo
        if arguments["date_to"] != 0:
            converted["date_to"] = hi
    else:
        for argument in row.arguments:
            if argument.name in arguments:
                if argument.type is calls.ArgType.EPOCH:
                    converted[argument.name] = clock.to_broker(arguments[argument.name])
                elif argument.type is calls.ArgType.TRADE_REQUEST:
                    request = dict(arguments[argument.name])
                    for name in binary.TRADE_REQUEST.epochs:
                        request[name] = clock.to_broker(request[name])
                    converted[argument.name] = request
    return converted


class Results:
    """One request's answer parts converted to true UTC as a consumer receives them: a range read's
    records reading into its window, and of two bars opening at one true-UTC second the first."""

    def __init__(
        self,
        row: calls.Row,
        arguments: dict[str, object],
        clock: BrokerClock,
        repeated_hours: RepeatedHours,
    ) -> None:
        self._row = row
        self._clock = clock
        self._repeated_hours = repeated_hours
        # The series of a bar read's timeframe.
        self.series: Series | None = None
        if row.shape is calls.Shape.RATES:
            self.series = bar_series(arguments["timeframe"])
        self._window: tuple[int, int] | None = None
        if row.call in _WINDOWS:
            self._window = (arguments["date_from"], arguments["date_to"])
        self._last_open: int | None = None

    def part(self, value: object) -> object:
        """A part's results in true UTC."""
        conversion = Conversion(self._clock, self._repeated_hours)
        where = self._row.call.name.lower()
        if self._row.shape is calls.Shape.STRUCT:
            converted = conversion.record(self._row.layout.epochs, value, where)
        elif self._row.shape is calls.Shape.STRUCTS:
            converted = [
                conversion.record(self._row.layout.epochs, record, where) for record in value
            ]
        elif self._row.shape is calls.Shape.TICK:
            converted = conversion.record(mirror.TICKS.epochs, value, where)
        elif self._row.shape is calls.Shape.TICKS:
            kept = self._in_window(value, value["time_msc"] // 1000)
            converted = conversion.rows(mirror.TICKS.epochs, kept, where)
        elif self._row.shape is calls.Shape.RATES:
            converted = self._bars(self._in_window(value, value["time"]), conversion, where)
        elif self._row.shape is calls.Shape.TRANSACTIONS:
            converted = [_transaction(conversion, transaction, where) for transaction in value]
        else:
            converted = value
        return converted

    def _in_window(self, rows: np.ndarray, seconds: np.ndarray) -> np.ndarray:
        """The rows whose broker second reads into a range read's window; every row of any other
        read."""
        if self._window is None:
            return rows
        else:
            return rows[reading_into(self._clock, seconds, *self._window)]

    def _bars(self, rates: np.ndarray, conversion: Conversion, where: str) -> np.ndarray:
        """The bars with their opens and closes in true UTC, of two opening at one true-UTC second
        the first."""
        opened = conversion.rows(mirror.RATES.epochs, rates, where)
        opens = opened["time"].astype(np.int64)
        kept = np.ones(len(opens), dtype=bool)
        kept[1:] = opens[1:] != opens[:-1]
        if len(opens) > 0 and self._last_open is not None:
            kept[0] = opens[0] != self._last_open
        dropped = rates["time"][~kept].astype(np.int64)
        _, firsts = np.unique(dropped // _HOUR_S, return_index=True)
        for position in sorted(firsts.tolist()):
            broker_open = int(dropped[position])
            self._repeated_hours.warn_dropped_bar(
                where, "time", broker_open, broker_open // _HOUR_S
            )
        if kept.any():
            self._last_open = int(opens[kept][-1])
        bars = np.zeros(int(kept.sum()), dtype=_BARS)
        for name in mirror.RATES.names:
            bars[name] = opened[name][kept]
        bars[BAR_CLOSE] = bar_closes(conversion.eras, self.series, rates["time"][kept])
        return bars
