"""The adapter's exceptions."""


class MT5Error(Exception):
    """The base of the adapter's exceptions."""


class MT5ConfigError(MT5Error):
    """Raised for a configuration, or the account it points at, the adapter cannot run."""


class MT5ConnectionError(MT5Error):
    """Raised when the adapter cannot establish or keep its connection to the MT5 terminal."""


class ServerUnreachable(MT5ConnectionError):
    """Raised when the hub cannot be reached or answers outside its contract."""


class ResponseLost(ServerUnreachable):
    """Raised when a request reached the hub but no answer within its contract came back, so it may
    have been acted on."""


class ProtocolError(ServerUnreachable):
    """Raised for a request whose answer, or a frame of the hub's no request can be told from,
    breaks the hub protocol's layout."""


class HubAnswerError(MT5ConnectionError):
    """Raised for a request the hub or its EA answered without a result, carrying the MQL5 error the
    answer folds in."""

    def __init__(self, message: str, last_error: int) -> None:
        super().__init__(message)
        self.last_error = last_error


class Deferred(HubAnswerError):
    """Raised for a request the hub defers, carrying the delay before asking again."""

    def __init__(self, message: str, last_error: int, retry_after_s: float) -> None:
        super().__init__(message, last_error)
        self.retry_after_s = retry_after_s


class Syncing(Deferred):
    """Raised for a history window the terminal's answers do not prove yet."""


class NotReady(Deferred):
    """Raised while the hub's broker clock is not verified."""


class NoPublisher(Deferred):
    """Raised while no EA serves the request's role and symbol and its chart is requested."""


class InvalidWindow(HubAnswerError):
    """Raised for a history window starting after the terminal's time."""


class Overflow(HubAnswerError):
    """Raised for a request a bounded queue had no room for."""


class EaGone(HubAnswerError):
    """Raised for a request whose EA left before answering it, so it may have acted on it."""


class HubTimeout(HubAnswerError):
    """Raised for a request its EA did not answer within the hub's deadline."""


class EaError(HubAnswerError):
    """Raised for a request its EA could not perform, with the MQL5 error it left."""


class MT5SymbolNotFoundError(MT5Error):
    """Raised when the venue cannot select a symbol or read its definition."""

    def __init__(self, symbol: str):
        super().__init__(f"symbol {symbol!r} not found")
        self.symbol = symbol


class MT5OrderError(MT5Error):
    """Raised when an order submission, modification or cancellation fails."""


class MT5InstrumentError(MT5Error):
    """Raised when an MT5 symbol cannot be converted into a NautilusTrader instrument definition."""
