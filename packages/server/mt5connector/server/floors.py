"""The floors measured for each symbol's series: where its rows begin.

State: each (symbol, series)'s floor on the broker's clock — seconds for bars, milliseconds for
ticks — with the true-UTC time it was measured, a generation that steps on every change of value,
and whether the stub or a coarse prefix at the series' start set it; a stub measured again never
moves a coarse floor back. Nothing is persisted, so floors are measured again after a restart."""

import threading
from dataclasses import dataclass

from mt5connector.server.wire.history_wire import FloorOrigin, Series


@dataclass(frozen=True)
class Floor:
    """Where a series' rows begin, as the store holds it."""

    # The broker's clock: seconds for bars, milliseconds for ticks.
    value: int
    # True UTC seconds.
    measured_at: int
    generation: int
    origin: FloorOrigin


class FloorStore:
    """The floor measured for each (symbol, series)."""

    def __init__(self) -> None:
        self._floors: dict[tuple[str, Series], Floor] = {}
        self._lock = threading.Lock()

    def read(self, symbol: str, series: Series) -> Floor | None:
        with self._lock:
            return self._floors.get((symbol, series))

    def of_symbol(self, symbol: str) -> dict[Series, Floor]:
        with self._lock:
            return {
                series: floor for (owner, series), floor in self._floors.items() if owner == symbol
            }

    def write(
        self, symbol: str, series: Series, value: int, measured_at: int, origin: FloorOrigin
    ) -> Floor:
        """Records a measurement and answers the floor it leaves in force, whose generation steps
        when the value changes."""
        with self._lock:
            last = self._floors.get((symbol, series))
            if last is None:
                floor = Floor(value, measured_at, 1, origin)
            elif (
                last.origin is FloorOrigin.COARSE
                and origin is FloorOrigin.STUB
                and value <= last.value
            ):
                # The stub of a series served with a coarse prefix is that prefix's first row.
                floor = Floor(last.value, measured_at, last.generation, last.origin)
            elif last.value != value:
                floor = Floor(value, measured_at, last.generation + 1, origin)
            else:
                floor = Floor(value, measured_at, last.generation, origin)
            self._floors[(symbol, series)] = floor
            return floor
