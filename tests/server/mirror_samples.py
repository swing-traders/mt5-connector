"""Sample arguments and package answers generated from the inventory, with the JSON and the call
shapes the mirror contract expects for them."""

from collections import namedtuple
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np

from mt5connector.server.wire import mirror
from mt5connector.server.wire.broker_clock import BrokerClock

PACKAGE_TYPES = {
    name: namedtuple(name.value, struct.fields) for name, struct in mirror.STRUCTS.items()
}

CLOCK = BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7))
# Sample epochs sit in mid-July 2025, under EDT, where the broker's clock runs 10,800 s ahead of
# UTC: broker 1,752,580,800 is 2025-07-15T09:00:00Z.
BROKER_EPOCH = 1_752_580_800
UTC_EPOCH = 1_752_570_000
EDT_OFFSET_S = 10_800


def argument_samples(function: mirror.Function) -> dict[str, object]:
    """A distinct JSON-ready value for every parameter of the function."""
    return {
        param.name: _argument_sample(param.kind, index)
        for index, param in enumerate(function.params)
    }


def _argument_sample(kind: mirror.ParamKind, index: int) -> object:
    if kind is mirror.ParamKind.INT:
        return 100 + index
    elif kind is mirror.ParamKind.STR:
        return f"text-{index}"
    elif kind is mirror.ParamKind.FLOAT:
        return 0.5 + index
    elif kind is mirror.ParamKind.BOOL:
        return True
    elif kind is mirror.ParamKind.DATETIME:
        return UTC_EPOCH + index
    elif kind is mirror.ParamKind.TIMEOUT_MS:
        return 90000 + index
    else:
        return {"action": 1, "symbol": f"text-{index}", "volume": 0.1}


def expected_call(
    function: mirror.Function, arguments: dict[str, object]
) -> tuple[tuple[object, ...], dict[str, object]]:
    """The package call for a full argument set: unnamed parameters positionally in the inventory's
    order, named ones by keyword, each time window a datetime on the broker's clock."""
    package_arguments = {}
    for param in function.params:
        if param.kind is mirror.ParamKind.DATETIME:
            broker = arguments[param.name] + EDT_OFFSET_S
            package_arguments[param.name] = datetime.fromtimestamp(broker, UTC)
        else:
            package_arguments[param.name] = arguments[param.name]
    positional = tuple(
        package_arguments[param.name] for param in function.params if not param.named
    )
    keywords = {
        param.name: package_arguments[param.name] for param in function.params if param.named
    }
    return positional, keywords


def struct_sample(name: mirror.StructName, seed: int = 0) -> tuple:
    struct = mirror.STRUCTS[name]
    values = {}
    for index, field in enumerate(struct.fields):
        if field in struct.nested:
            values[field] = struct_sample(struct.nested[field], seed)
        elif field in struct.epochs:
            values[field] = _epoch_sample(struct.epochs[field], seed * 1000 + index)
        elif index % 3 == 0:
            values[field] = seed * 1000 + index
        elif index % 3 == 1:
            values[field] = seed * 1000 + index + 0.25
        else:
            values[field] = f"{field}-{seed}"
    return PACKAGE_TYPES[name](**values)


def array_sample(array: mirror.Array, rows: int = 2) -> np.ndarray:
    dtype = np.dtype(list(array.dtype))
    records = []
    for row in range(rows):
        values = []
        for index, (field, numpy_type) in enumerate(array.dtype):
            if field in array.epochs:
                values.append(_epoch_sample(array.epochs[field], 1000 * row + index))
            elif np.issubdtype(numpy_type, np.floating):
                values.append(1000 * row + index + 0.5)
            else:
                values.append(1000 * row + index)
        records.append(tuple(values))
    return np.array(records, dtype=dtype)


def _epoch_sample(unit: mirror.EpochUnit, step: int) -> int:
    if unit is mirror.EpochUnit.SECONDS:
        return BROKER_EPOCH + step
    else:
        return (BROKER_EPOCH + step) * 1000 + step % 1000


def in_true_utc(value: object) -> object:
    """A sample answer with each of its non-zero epochs moved off the broker's clock under EDT."""
    if isinstance(value, np.ndarray):
        array = next(array for array in mirror.ARRAYS.values() if array.names == value.dtype.names)
        converted = value.copy()
        for field, unit in array.epochs.items():
            converted[field] = [_utc(unit, epoch) for epoch in value[field].tolist()]
        return converted
    elif hasattr(value, "_asdict"):
        struct = mirror.STRUCTS[mirror.StructName(type(value).__name__)]
        changes = {}
        for field, item in value._asdict().items():
            if field in struct.nested:
                changes[field] = in_true_utc(item)
            elif field in struct.epochs:
                changes[field] = _utc(struct.epochs[field], item)
        return value._replace(**changes)
    elif isinstance(value, tuple):
        return tuple(in_true_utc(item) for item in value)
    else:
        return value


def _utc(unit: mirror.EpochUnit, epoch: int) -> int:
    if epoch == 0:
        return 0
    elif unit is mirror.EpochUnit.SECONDS:
        return epoch - EDT_OFFSET_S
    else:
        return epoch - EDT_OFFSET_S * 1000


def result_sample(function: mirror.Function) -> object:
    if function.result is mirror.ResultKind.STRUCT:
        return struct_sample(function.struct)
    elif function.result is mirror.ResultKind.STRUCTS:
        return (struct_sample(function.struct, 0), struct_sample(function.struct, 1))
    elif function.result is mirror.ResultKind.ARRAY:
        return array_sample(mirror.ARRAYS[function.array])
    elif function.name is mirror.FunctionName.LAST_ERROR:
        return (-1, "Terminal: Call failed")
    elif function.result is mirror.ResultKind.TUPLE:
        return (500, 4321, "27 Sep 2026")
    elif function.result is mirror.ResultKind.BOOL:
        return True
    elif function.result is mirror.ResultKind.SCALAR:
        return 3
    else:
        return None


def expected_json(value: object) -> object:
    """A package answer as the mirror contract serializes it."""
    if isinstance(value, np.ndarray):
        return [dict(zip(value.dtype.names, row, strict=True)) for row in value.tolist()]
    elif hasattr(value, "_asdict"):
        return {field: expected_json(item) for field, item in value._asdict().items()}
    elif isinstance(value, tuple):
        return [expected_json(item) for item in value]
    else:
        return value
