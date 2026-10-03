"""The wire's conversions, shaped by the inventory: package answers to JSON and client arguments to
the package's, with true-UTC epochs on the wire and the broker's clock at the package."""

import logging
from datetime import UTC, datetime

import numpy as np

from mt5connect import mirror
from mt5connect.broker_clock import BrokerClock

logger = logging.getLogger(__name__)

_TRADE_REQUEST = mirror.STRUCTS[mirror.StructName.TRADE_REQUEST]


class ShapeError(Exception):
    """Raised when the package answers in a shape the inventory does not describe."""


def encode(function: mirror.Function, value: object, clock: BrokerClock) -> object:
    """The JSON value of a successful answer: structs as objects in field order, arrays as a list of
    objects keyed by the dtype's fields, numpy scalars as plain numbers, and epochs in true UTC."""
    answer = _Answer(clock)
    if function.result is mirror.ResultKind.STRUCT:
        encoded = answer.struct(mirror.STRUCTS[function.struct], value)
    elif function.result is mirror.ResultKind.STRUCTS:
        encoded = []
        for item in value:
            encoded.append(answer.struct(mirror.STRUCTS[function.struct], item))
    elif function.result is mirror.ResultKind.ARRAY:
        encoded = answer.array(mirror.ARRAYS[function.array], value)
    elif function.result is mirror.ResultKind.TUPLE:
        encoded = [_plain(item) for item in value]
    else:
        encoded = _plain(value)
    if answer.ambiguous is not None:
        field, epoch = answer.ambiguous
        logger.warning(
            "%s: %s %d is in the broker's repeated hour, read as its first occurrence",
            function.name,
            field,
            epoch,
        )
    return encoded


def package_arguments(
    function: mirror.Function, arguments: dict[str, object], clock: BrokerClock
) -> dict[str, object]:
    """The arguments as the package reads them: each time window a datetime whose timestamp() is the
    broker epoch, and a request's epochs on the broker's clock."""
    params = {param.name: param for param in function.params}
    converted = {}
    for name, value in arguments.items():
        if params[name].kind is mirror.ParamKind.DATETIME:
            converted[name] = broker_datetime(clock.to_broker(value))
        elif params[name].kind is mirror.ParamKind.REQUEST and isinstance(value, dict):
            converted[name] = _broker_request(value, clock)
        else:
            converted[name] = value
    return converted


def broker_datetime(broker_epoch: int) -> datetime:
    """The datetime the package reads as a broker epoch, through its timestamp()."""
    return datetime.fromtimestamp(broker_epoch, tz=UTC)


def non_epochs(function: mirror.Function, arguments: dict[str, object]) -> list[str]:
    """The time arguments that are not integer epochs: time windows, and a request's epochs."""
    names = []
    for param in function.params:
        value = arguments.get(param.name)
        if param.kind is mirror.ParamKind.DATETIME and param.name in arguments:
            if not _is_integer(value):
                names.append(param.name)
        elif param.kind is mirror.ParamKind.REQUEST and isinstance(value, dict):
            names.extend(
                f"{param.name}.{key}"
                for key in _TRADE_REQUEST.epochs
                if key in value and not _is_integer(value[key])
            )
    return names


def _broker_request(request: dict[str, object], clock: BrokerClock) -> dict[str, object]:
    converted = dict(request)
    for key in _TRADE_REQUEST.epochs:
        if key in converted:
            converted[key] = clock.to_broker(converted[key])
    return converted


class _Answer:
    """One answer's encoding; remembers the first epoch it read in the broker's repeated hour."""

    def __init__(self, clock: BrokerClock) -> None:
        self._clock = clock
        self.ambiguous: tuple[str, int] | None = None

    def struct(self, struct: mirror.Struct, value: tuple) -> dict[str, object]:
        if len(value) != len(struct.fields):
            raise ShapeError(
                f"{struct.name}: {len(value)} fields, the inventory has {len(struct.fields)}"
            )
        encoded = {}
        for name in struct.fields:
            if name in struct.nested:
                nested = mirror.STRUCTS[struct.nested[name]]
                encoded[name] = self.struct(nested, getattr(value, name))
            elif name in struct.epochs:
                field = f"{struct.name}.{name}"
                encoded[name] = self._utc(field, struct.epochs[name], _plain(getattr(value, name)))
            else:
                encoded[name] = _plain(getattr(value, name))
        return encoded

    def array(self, array: mirror.Array, records: np.ndarray) -> list[dict[str, object]]:
        if records.dtype.names != array.names:
            raise ShapeError(
                f"{array.name}: fields {records.dtype.names}, the inventory has {array.names}"
            )
        rows = []
        for row in records.tolist():
            encoded = dict(zip(array.names, row, strict=True))
            for name, unit in array.epochs.items():
                encoded[name] = self._utc(f"{array.name}.{name}", unit, encoded[name])
            rows.append(encoded)
        return rows

    def _utc(self, field: str, unit: mirror.EpochUnit, value: object) -> int:
        if not _is_integer(value):
            raise ShapeError(f"{field}: {value!r} is not an integer epoch")
        elif unit is mirror.EpochUnit.SECONDS:
            utc = self._clock.to_utc(value)
            seconds = value
        else:
            utc = self._clock.to_utc_msc(value)
            seconds = value // 1000
        if self.ambiguous is None and value != 0 and self._clock.is_ambiguous(seconds):
            self.ambiguous = (field, value)
        return utc


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _plain(value: object) -> object:
    if isinstance(value, np.generic):
        return value.item()
    return value
