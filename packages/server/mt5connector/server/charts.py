"""The symbols the terminal's EAs publish, as their server-time samples name them, and the posts
that tell the hub a symbol is in use and request the chart of one no EA publishes.

State: per symbol, when a sample last named it; when the server last posted it to the hub or, if
later, when a run of its samples began — no post once the hub answers that its chart failed to open,
so its next read asks the hub again; and whether the hub's last answer requested its chart. A symbol
is published while a sample named it within the sample max age. Nothing is persisted."""

import http.client
import json
import threading
import time
import urllib.request
from collections.abc import Callable
from urllib.parse import quote

from mt5connector.server.wire.push_wire import CHARTS_PATH, ChartState

_HUB_TIMEOUT_S = 2


class HubError(Exception):
    """Raised when the hub does not answer a chart post within its contract."""


class ChartFailed(Exception):
    """Raised when the hub answers that the spawner failed to open the symbol's chart; the message
    is the spawner's reason."""


class Publishers:
    """Which symbols an EA publishes, with the server's posts to the hub of the symbols it reads."""

    def __init__(
        self,
        post: Callable[[str], ChartState],
        *,
        fresh_s: float,
        touch_s: float,
        retry_s: float,
    ) -> None:
        self._post = post
        self._fresh_s = fresh_s
        self._touch_s = touch_s
        self._retry_s = retry_s
        self._lock = threading.Lock()
        self._seen: dict[str, float] = {}
        self._posted: dict[str, float] = {}
        self._requested: set[str] = set()

    def saw(self, symbol: str) -> None:
        """Records a server-time sample naming the symbol."""
        now = time.monotonic()
        with self._lock:
            if not self._is_fresh(symbol, now):
                # A run's start counts as a post the hub answered published: the hub counts the
                # symbol in use from its EA's hello.
                self._posted[symbol] = now
                self._requested.discard(symbol)
            self._seen[symbol] = now

    def ensure_chart(self, symbol: str) -> ChartState:
        """Whether an EA publishes the symbol. Posts it to the hub — marking it in use, and
        requesting its chart while no EA publishes it — when it has gone a retry interval without a
        post while the hub's last answer requested its chart, and a touch interval otherwise; the
        hub's answer then decides. Raises HubError for a post the hub does not answer, and
        ChartFailed when it answers that the spawner failed to open the symbol's chart."""
        now = time.monotonic()
        with self._lock:
            fresh = self._is_fresh(symbol, now)
            last = self._posted.get(symbol)
            if symbol in self._requested:
                # Due again by a reader's next retry, so it meets a failure to open the chart.
                interval = self._retry_s
            else:
                interval = self._touch_s
            due = last is None or now - last >= interval
            if due:
                self._posted[symbol] = now
        if due:
            try:
                state = self._post(symbol)
            except HubError:
                with self._lock:
                    if last is None:
                        self._posted.pop(symbol, None)
                    else:
                        self._posted[symbol] = last
                raise
            except ChartFailed:
                with self._lock:
                    self._seen.pop(symbol, None)
                    self._posted.pop(symbol, None)
                    self._requested.discard(symbol)
                raise
            with self._lock:
                if state == ChartState.PUBLISHED:
                    self._seen[symbol] = max(self._seen.get(symbol, now), now)
                    self._requested.discard(symbol)
                else:
                    self._seen.pop(symbol, None)
                    self._requested.add(symbol)
            return state
        elif fresh:
            return ChartState.PUBLISHED
        else:
            return ChartState.REQUESTED

    def _is_fresh(self, symbol: str, now: float) -> bool:
        seen = self._seen.get(symbol)
        return seen is not None and now - seen < self._fresh_s


def hub_charts(base_url: str) -> Callable[[str], ChartState]:
    """A post of a symbol to the hub's chart route under `base_url`, answering the hub's state of
    it; raises ChartFailed with the spawner's reason for a chart that failed to open, and HubError
    for any other answer."""
    # No proxy: the hub answers the route over loopback alone.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post(symbol: str) -> ChartState:
        url = f"{base_url}{CHARTS_PATH}/{quote(symbol, safe='')}"
        request = urllib.request.Request(url, data=b"", method="POST")
        try:
            with opener.open(request, timeout=_HUB_TIMEOUT_S) as response:
                body = json.loads(response.read())
            if not (isinstance(body, dict) and body.get("ok") is True):
                raise ValueError(f"not a chart answer: {body!r}")
            state = ChartState(body.get("result"))
            if state == ChartState.FAILED and not isinstance(body.get("reason"), str):
                raise ValueError(f"not a chart failure's reason: {body!r}")
        except (OSError, http.client.HTTPException, ValueError) as failure:
            raise HubError(f"chart post to {url} failed: {failure}") from failure
        if state == ChartState.FAILED:
            raise ChartFailed(body["reason"])
        else:
            return state

    return post
