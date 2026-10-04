"""The venue's id and the client's defaults for its account loop and its reconnect backoff."""

from nautilus_trader.model.identifiers import Venue

# ─────────────────────────────────────────────────────────────────────────────
# VENUE
# ─────────────────────────────────────────────────────────────────────────────

MT5_VENUE = Venue("MT5")


# ─────────────────────────────────────────────────────────────────────────────
# POLLING
# ─────────────────────────────────────────────────────────────────────────────

# Interval (milliseconds) between the execution client's account turns.
DEFAULT_EXEC_POLL_INTERVAL_MS: int = 250


# ─────────────────────────────────────────────────────────────────────────────
# RECONNECT
# ─────────────────────────────────────────────────────────────────────────────

RECONNECT_INITIAL_DELAY_S: float = 1.0
RECONNECT_MAX_DELAY_S: float = 60.0
RECONNECT_MULTIPLIER: float = 2.0
RECONNECT_MAX_ATTEMPTS: int = 20
