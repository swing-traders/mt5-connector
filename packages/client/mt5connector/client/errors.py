"""The adapter's exceptions."""


class MT5Error(Exception):
    """Base exception for all nautilus-mt5 errors."""


class MT5ConfigError(MT5Error):
    """Raised when the configuration, or the account it points at, cannot be run (e.g. no
    server_url, an account that does not hedge)."""


class MT5ConnectionError(MT5Error):
    """Raised when the adapter cannot establish or keep its connection to the MT5 terminal."""


class ServerUnreachable(MT5ConnectionError):
    """Raised when the remote MT5 server cannot be reached or answers outside its contract."""


class ResponseLost(ServerUnreachable):
    """Raised when a request reached the remote MT5 server but no answer within its contract came
    back, so the server may have acted on it."""


class ServerBusy(MT5ConnectionError):
    """Raised when the remote MT5 server refuses a call because every slot of its cap is taken,
    carrying the delay its Retry-After gives."""

    def __init__(self, message: str, retry_after_s: int) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class MT5LoginError(MT5Error):
    """Raised when the login to the broker account fails."""


class MT5SymbolNotFoundError(MT5Error):
    """Raised when the venue cannot select a symbol or read its definition."""

    def __init__(self, symbol: str):
        super().__init__(f"symbol {symbol!r} not found")
        self.symbol = symbol


class MT5OrderError(MT5Error):
    """Raised when an order submission, modification or cancellation fails, carrying the MT5 retcode
    when the venue answered one."""

    def __init__(self, message: str, retcode: int | None = None):
        full_message = message
        if retcode is not None:
            full_message = f"{message} (MT5 retcode: {retcode})"
        super().__init__(full_message)
        self.retcode = retcode


class MT5InstrumentError(MT5Error):
    """Raised when an MT5 symbol cannot be converted into a NautilusTrader instrument definition."""
