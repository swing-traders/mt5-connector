"""The floors measured for each symbol's series: where its rows begin.

State: each symbol's series as its latest ranges answer gave them; for each (symbol, series), its
last floor in broker seconds, kept while answers omit the series, with the true-UTC time the hub
learned it and a generation that steps when its value changes or the series returns after an answer
omitted it. Nothing is persisted, so floors are learned again after a restart."""

import threading
from dataclasses import dataclass

from mt5connector.server.wire.history_wire import Series


@dataclass(frozen=True)
class Floor:
    """Where a series' rows begin, as the store holds it."""

    # Broker seconds.
    value: int
    # True UTC seconds.
    measured_at: int
    generation: int


class FloorStore:
    """The floors each symbol's latest ranges answer gave."""

    def __init__(self) -> None:
        self._floors: dict[tuple[str, Series], Floor] = {}
        self._advertised: dict[str, frozenset[Series]] = {}
        self._lock = threading.Lock()

    def of_symbol(self, symbol: str) -> dict[Series, Floor]:
        with self._lock:
            return {
                series: self._floors[(symbol, series)]
                for series in self._advertised.get(symbol, frozenset())
            }

    def replace(self, symbol: str, floors: dict[Series, int], now: int) -> None:
        """Takes a ranges answer's floors as the symbol's whole set."""
        with self._lock:
            advertised = self._advertised.get(symbol, frozenset())
            for series, value in floors.items():
                last = self._floors.get((symbol, series))
                if last is None:
                    generation = 1
                elif last.value != value or series not in advertised:
                    generation = last.generation + 1
                else:
                    generation = last.generation
                self._floors[(symbol, series)] = Floor(value, now, generation)
            self._advertised[symbol] = frozenset(floors)
