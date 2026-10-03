"""Requests held in flight against a served app, each holding a slot of its terminal-bound cap, and
the counters its /health reports."""

import threading
import time
from collections.abc import Callable

import requests

COUNTERS = ("in_flight", "peak_in_flight", "refusals", "workers")


def until(condition: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("the condition never held")
        time.sleep(0.01)


def counters(served: str) -> dict[str, int]:
    """The cap's counters /health reports."""
    result = requests.get(f"{served}/health", timeout=1).json()["result"]
    return {name: result[name] for name in COUNTERS}


class Held:
    """POSTs held in flight by a package double that blocks until `released` is set."""

    def __init__(self, served: str) -> None:
        self.served = served
        self.released = threading.Event()
        self.answers: list[requests.Response] = []
        self._threads: list[threading.Thread] = []

    def take(self, count: int, path: str, json: dict[str, object] | None = None) -> None:
        """Starts `count` POSTs to the path and waits until every one held holds a slot."""
        for _ in range(count):
            thread = threading.Thread(target=self._post, args=(path, json))
            thread.start()
            self._threads.append(thread)
        until(lambda: counters(self.served)["in_flight"] == len(self._threads))

    def release(self) -> None:
        self.released.set()
        for thread in self._threads:
            thread.join(10)

    def _post(self, path: str, json: dict[str, object] | None) -> None:
        self.answers.append(requests.post(f"{self.served}{path}", json=json, timeout=10))
