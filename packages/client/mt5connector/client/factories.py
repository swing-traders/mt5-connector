"""NT's live data and execution client factories for MT5 — sharing one connection and instrument
provider per account and server for the process's life — and the node config that wires them."""

from __future__ import annotations

import asyncio
import logging

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataClientConfig,
    LiveExecClientConfig,
    LiveRiskEngineConfig,
    RoutingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.factories import LiveDataClientFactory, LiveExecClientFactory
from nautilus_trader.model.identifiers import InstrumentId, Symbol

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.data import MT5DataClient
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.providers import MT5InstrumentProvider

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SHARED CONNECTION REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

_connection_registry: dict[int, tuple[MT5Connection, MT5InstrumentProvider]] = {}

# The MT5Config of each venue's node config: NT's client configs are frozen msgspec structs that
# carry no adapter config.
_mt5_config_registry: dict[str, MT5Config] = {}


def _get_or_create_connection(
    config: MT5Config,
    clock: LiveClock,
) -> tuple[MT5Connection, MT5InstrumentProvider]:
    """The connection and instrument provider of the config's account and server, created and
    connected once, so the clients of one account share one terminal session and one instrument
    cache."""
    registry_key = hash((config.account, config.server))

    if registry_key not in _connection_registry:
        logger.info("MT5 factories: creating the account's connection")
        conn = MT5Connection(config)
        provider = MT5InstrumentProvider(conn, clock=clock, config=_provider_config(config))

        conn.connect()

        _connection_registry[registry_key] = (conn, provider)
        logger.info(f"MT5 factories: connection established — {conn}")

    return _connection_registry[registry_key]


def _provider_config(config: MT5Config) -> InstrumentProviderConfig:
    """The provider loads exactly the symbols the config names."""
    return InstrumentProviderConfig(
        load_ids=frozenset(InstrumentId(Symbol(symbol), MT5_VENUE) for symbol in config.symbols)
    )


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLIENT FACTORY
# ─────────────────────────────────────────────────────────────────────────────


class MT5LiveDataClientFactory(LiveDataClientFactory):
    """Builds the MT5 data client of a node."""

    @classmethod
    def create(
        cls,
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LiveDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> MT5DataClient:
        """A data client on the shared connection of the MT5Config the client config carries in its
        `custom`, else the one the node config was built with."""
        mt5_config: MT5Config = (
            config.custom["mt5_config"]
            if hasattr(config, "custom") and isinstance(getattr(config, "custom", None), dict)
            else _mt5_config_registry.get(MT5_VENUE.value)
        )
        if mt5_config is None:
            raise RuntimeError("MT5LiveDataClientFactory: no MT5Config found.")
        conn, provider = _get_or_create_connection(mt5_config, clock)

        return MT5DataClient(
            loop=loop,
            connection=conn,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=mt5_config,
        )


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION CLIENT FACTORY
# ─────────────────────────────────────────────────────────────────────────────


class MT5LiveExecClientFactory(LiveExecClientFactory):
    """Builds the MT5 execution client of a node."""

    @classmethod
    def create(
        cls,
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LiveExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> MT5LiveExecutionClient:
        """An execution client on the shared connection of the MT5Config the client config carries
        in its `custom`, else the one the node config was built with."""
        mt5_config: MT5Config = (
            config.custom["mt5_config"]
            if hasattr(config, "custom") and isinstance(getattr(config, "custom", None), dict)
            else _mt5_config_registry.get(MT5_VENUE.value)
        )
        if mt5_config is None:
            raise RuntimeError("MT5LiveExecClientFactory: no MT5Config found.")
        conn, provider = _get_or_create_connection(mt5_config, clock)

        return MT5LiveExecutionClient(
            loop=loop,
            connection=conn,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=mt5_config,
        )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE BUILDER
# ─────────────────────────────────────────────────────────────────────────────


def build_mt5_node_config(
    mt5_config: MT5Config,
    risk_engine_config: LiveRiskEngineConfig | None = None,
    logging_config=None,
) -> TradingNodeConfig:
    """The node config of one MT5 venue: its data and execution clients, each the default route and
    loading the config's symbols, and the MT5Config the factories build them from."""
    _mt5_config_registry[MT5_VENUE.value] = mt5_config

    data_client_cfg = LiveDataClientConfig(
        instrument_provider=_provider_config(mt5_config),
        routing=RoutingConfig(default=True, venues=frozenset({MT5_VENUE.value})),
    )
    exec_client_cfg = LiveExecClientConfig(
        instrument_provider=_provider_config(mt5_config),
        routing=RoutingConfig(default=True, venues=frozenset({MT5_VENUE.value})),
    )

    kwargs: dict = {
        "data_clients": {MT5_VENUE.value: data_client_cfg},
        "exec_clients": {MT5_VENUE.value: exec_client_cfg},
    }
    if risk_engine_config is not None:
        kwargs["risk_engine"] = risk_engine_config
    if logging_config is not None:
        kwargs["logging"] = logging_config

    return TradingNodeConfig(**kwargs)
