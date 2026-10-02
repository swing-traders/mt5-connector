"""Sample arguments and package answers generated from the inventory, with the JSON and the call
shapes the mirror contract expects for them."""

from collections import namedtuple

import numpy as np

from mt5connect import mirror

PACKAGE_TYPES = {
    name: namedtuple(name.value, struct.fields) for name, struct in mirror.STRUCTS.items()
}


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
        return 1704067200 + index
    elif kind is mirror.ParamKind.TIMEOUT_MS:
        return 90000 + index
    else:
        return {"action": 1, "symbol": f"text-{index}", "volume": 0.1}


def expected_call(
    function: mirror.Function, arguments: dict[str, object]
) -> tuple[tuple[object, ...], dict[str, object]]:
    """The package call for a full argument set: unnamed parameters positionally in the inventory's
    order, named ones by keyword."""
    positional = tuple(arguments[param.name] for param in function.params if not param.named)
    keywords = {param.name: arguments[param.name] for param in function.params if param.named}
    return positional, keywords


def struct_sample(name: mirror.StructName, seed: int = 0) -> tuple:
    struct = mirror.STRUCTS[name]
    values = {}
    for index, field in enumerate(struct.fields):
        if field in struct.nested:
            values[field] = struct_sample(struct.nested[field], seed)
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
        for index, (_, numpy_type) in enumerate(array.dtype):
            if np.issubdtype(numpy_type, np.floating):
                values.append(1000 * row + index + 0.5)
            else:
                values.append(1000 * row + index)
        records.append(tuple(values))
    return np.array(records, dtype=dtype)


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
