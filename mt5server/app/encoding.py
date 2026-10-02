"""JSON values for what the MetaTrader5 package answers, shaped by the inventory."""

import numpy as np

from mt5connect import mirror


class ShapeError(Exception):
    """Raised when the package answers in a shape the inventory does not describe."""


def encode(function: mirror.Function, value: object) -> object:
    """The JSON value of a successful answer: structs as objects in field order, arrays as a list of
    objects keyed by the dtype's fields, numpy scalars as plain numbers."""
    if function.result is mirror.ResultKind.STRUCT:
        return encode_struct(mirror.STRUCTS[function.struct], value)
    elif function.result is mirror.ResultKind.STRUCTS:
        return [encode_struct(mirror.STRUCTS[function.struct], item) for item in value]
    elif function.result is mirror.ResultKind.ARRAY:
        return _encode_array(mirror.ARRAYS[function.array], value)
    elif function.result is mirror.ResultKind.TUPLE:
        return [_plain(item) for item in value]
    else:
        return _plain(value)


def encode_struct(struct: mirror.Struct, value: tuple) -> dict[str, object]:
    if len(value) != len(struct.fields):
        raise ShapeError(
            f"{struct.name}: {len(value)} fields, the inventory has {len(struct.fields)}"
        )
    encoded = {}
    for name in struct.fields:
        if name in struct.nested:
            encoded[name] = encode_struct(mirror.STRUCTS[struct.nested[name]], getattr(value, name))
        else:
            encoded[name] = _plain(getattr(value, name))
    return encoded


def _encode_array(array: mirror.Array, records: np.ndarray) -> list[dict[str, object]]:
    if records.dtype.names != array.names:
        raise ShapeError(
            f"{array.name}: fields {records.dtype.names}, the inventory has {array.names}"
        )
    return [dict(zip(array.names, row, strict=True)) for row in records.tolist()]


def _plain(value: object) -> object:
    if isinstance(value, np.generic):
        return value.item()
    return value
