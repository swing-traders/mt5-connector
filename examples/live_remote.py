"""
examples/live_remote.py

Connect to the MT5 server and stream live ticks for the configured symbols.

    python examples/live_remote.py

Requirements
------------
- An MT5 server (mt5-connector-server, see packages/server/mt5connector/server/README.md) is
  running, its terminal logged in to a broker.
- A local ``.env`` with:
      MT5_ACCOUNT=12345678
      MT5_PASSWORD=your_password
      MT5_SERVER=YourBroker-Demo
      MT5_SYMBOLS=EURUSD,GBPUSD
      MT5_SERVER_URL=http://localhost:5000
- The adapter installed in this env:  pip install -e packages/client
"""

import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from mt5connector.client.config import MT5Config
from mt5connector.client.factories import (
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
    build_mt5_node_config,
)

load_dotenv(Path(__file__).parent.parent / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        sys.exit(f"ERROR: '{key}' is not set. Add it to your .env file.")
    return val


MT5_ACCOUNT = int(_require("MT5_ACCOUNT"))
MT5_PASSWORD = _require("MT5_PASSWORD")
MT5_SERVER = _require("MT5_SERVER")
MT5_SYMBOLS = [s.strip() for s in _require("MT5_SYMBOLS").split(",")]
MT5_SERVER_URL = os.getenv("MT5_SERVER_URL", "http://localhost:5000")
INSTRUMENT_ID = "EURUSDp.MT5"


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

config = MT5Config(
    account=MT5_ACCOUNT,
    password=MT5_PASSWORD,
    server=MT5_SERVER,
    symbols=MT5_SYMBOLS,
    server_url=MT5_SERVER_URL,
)
print(f"Server: {config.server_url}, ws: {config.ws_url}")


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY — subscribes to quote ticks and logs them
# ─────────────────────────────────────────────────────────────────────────────


class TickPrintStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: str


class TickPrintStrategy(Strategy):
    """Subscribes to quote ticks for one instrument and logs each tick."""

    def __init__(self, config: TickPrintStrategyConfig):
        super().__init__(config)
        self.instrument_id = InstrumentId.from_str(config.instrument_id)
        self.log.info(f"initazed with instrument_id {self.config.instrument_id}")

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.log.info(
            f"{tick.instrument_id} bid={tick.bid_price} ask={tick.ask_price} " f"@ {tick.ts_event}",
            color=LogColor.GREEN,
        )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    node_config = build_mt5_node_config(mt5_config=config)
    node = TradingNode(config=node_config)

    node.add_data_client_factory("MT5", MT5LiveDataClientFactory)
    node.add_exec_client_factory("MT5", MT5LiveExecClientFactory)
    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    print(f"MT5_SYMBOS {MT5_SYMBOLS}")
    for symbol in MT5_SYMBOLS:
        node.trader.add_strategy(
            TickPrintStrategy(
                TickPrintStrategyConfig(instrument_id=f"{symbol}.MT5"),
            )
        )

    def shutdown(sig, frame):
        print("\nShutting down...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Starting node — connecting to remote server {MT5_SERVER_URL}...\n")
    node.build()
    node.run()


if __name__ == "__main__":
    main()
