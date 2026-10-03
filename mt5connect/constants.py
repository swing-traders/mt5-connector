"""
nautilus_mt5/constants.py

All fixed values for the nautilus-mt5 adapter.
No logic here — just constants referenced across all modules.
"""

from nautilus_trader.model.identifiers import Venue

# ─────────────────────────────────────────────────────────────────────────────
# VENUE
# ─────────────────────────────────────────────────────────────────────────────

MT5_VENUE = Venue("MT5")


# ─────────────────────────────────────────────────────────────────────────────
# POLLING
# ─────────────────────────────────────────────────────────────────────────────

# Interval (milliseconds) for polling open positions to detect fills.
DEFAULT_EXEC_POLL_INTERVAL_MS: int = 250


# ─────────────────────────────────────────────────────────────────────────────
# RECONNECT
# ─────────────────────────────────────────────────────────────────────────────

RECONNECT_INITIAL_DELAY_S: float = 1.0
RECONNECT_MAX_DELAY_S: float = 60.0
RECONNECT_MULTIPLIER: float = 2.0
RECONNECT_MAX_ATTEMPTS: int = 20


# ─────────────────────────────────────────────────────────────────────────────
# MT5 ORDER FILLING MODES
#
# NOTE: There is no single correct filling mode — it depends on the broker
# AND the account type (Raw Spread, Pro, Standard, Zero all differ).
# The correct mode is auto-detected per-symbol at runtime in execution.py
# via _get_filling_mode(), which reads symbol_info().filling_mode.
# No fixed constant is defined here on purpose.
# ─────────────────────────────────────────────────────────────────────────────
