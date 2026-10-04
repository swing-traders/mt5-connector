"""A double of the hub's chart route, and the server's view of the published symbols built on it."""

from mt5connector.server.charts import Publishers
from mt5connector.server.wire.push_wire import ChartState

# The settings' defaults: the sample max age, half the chart idle period, and the history retry.
FRESH_S = 30
TOUCH_S = 450
RETRY_S = 5


class ChartPosts:
    """Records each symbol the server posts to the hub, and answers `state`, or raises `failure`
    when one is set."""

    def __init__(self) -> None:
        self.posted: list[str] = []
        self.state = ChartState.PUBLISHED
        self.failure: Exception | None = None

    def __call__(self, symbol: str) -> ChartState:
        self.posted.append(symbol)
        if self.failure is not None:
            raise self.failure
        return self.state


def publishers_on(posts: ChartPosts) -> Publishers:
    return Publishers(posts, fresh_s=FRESH_S, touch_s=TOUCH_S, retry_s=RETRY_S)
