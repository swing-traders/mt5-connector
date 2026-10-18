"""The broker clock the hub tests run under, and a sample epoch on it."""

from datetime import timedelta
from zoneinfo import ZoneInfo

from mt5connector.server.wire.broker_clock import BrokerClock

CLOCK = BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7))
# Mid-July 2025, under EDT, the broker's clock runs 10,800 s ahead of UTC: broker 1,752,580,800 is
# 2025-07-15T09:00:00Z.
BROKER_EPOCH = 1_752_580_800
UTC_EPOCH = 1_752_570_000
