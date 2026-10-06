"""The serialized executor: each MetaTrader5 package call runs under one lock together with the
last_error() read that follows it, so the error an answer reports is always its own."""

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from types import ModuleType

from mt5connector.server.wire import mirror


@dataclass(frozen=True)
class Answered:
    value: object
    last_error: tuple[int, str]


@dataclass(frozen=True)
class Failed:
    last_error: tuple[int, str]


class Terminal:
    """The MetaTrader5 package behind the one lock every call takes."""

    def __init__(self, package: ModuleType) -> None:
        self._package = package
        self._lock = threading.Lock()

    def call(self, function: mirror.Function, arguments: Mapping[str, object]) -> Answered | Failed:
        """Calls the package function with arguments keyed by parameter name."""
        args, kwargs = package_call(function, arguments)
        with self._lock:
            package_function = getattr(self._package, function.name)
            # The package's trade calls refuse positional arguments beside any **kwargs, even {}.
            if kwargs:
                value = package_function(*args, **kwargs)
            else:
                value = package_function(*args)
            code, message = self._package.last_error()
        if mirror.is_failure(function, value):
            return Failed((code, message))
        else:
            return Answered(value, (code, message))


def package_call(
    function: mirror.Function, arguments: Mapping[str, object]
) -> tuple[tuple[object, ...], dict[str, object]]:
    """The call the package documents for these arguments: its unnamed parameters positionally while
    they run unbroken from the first, everything else by keyword."""
    positional = []
    keywords = {}
    unbroken = True
    for param in function.params:
        if param.name not in arguments:
            if not param.named:
                unbroken = False
        elif param.named or not unbroken:
            keywords[param.name] = arguments[param.name]
        else:
            positional.append(arguments[param.name])
    return tuple(positional), keywords
