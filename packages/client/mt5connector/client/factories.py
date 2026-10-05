"""NT's live data and execution client factories for MT5 — sharing one connection per account and
server, and one instrument provider per connection, venue and instrument-provider config, for the
process's life — and the node config that wires a venue's clients."""

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
from nautilus_trader.model.identifiers import InstrumentId, Venue

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.data import MT5DataClient
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.providers import MT5InstrumentProvider

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SHARED CONNECTIONS AND INSTRUMENT PROVIDERS
# ─────────────────────────────────────────────────────────────────────────────

_connection_registry: dict[tuple[int, str], MT5Connection] = {}

_provider_registry: dict[
    tuple[MT5Connection, Venue, InstrumentProviderConfig], MT5InstrumentProvider
] = {}

# The MT5Config of each venue's node config, by the venue's value: NT's client configs are frozen
# msgspec structs that carry no adapter config.
_mt5_config_registry: dict[str, MT5Config] = {}


def _ensure_connection(config: MT5Config) -> MT5Connection:
    """The connection of the config's account and server, created and connected on first use."""
    registry_key = (config.account, config.server)

    if registry_key not in _connection_registry:
        logger.info("MT5 factories: creating the account's connection")
        conn = MT5Connection(config)
        conn.connect()
        _connection_registry[registry_key] = conn
        logger.info(f"MT5 factories: connection established — {conn}")

    return _connection_registry[registry_key]


def _ensure_provider(
    connection: MT5Connection,
    venue: Venue,
    config: InstrumentProviderConfig,
    clock: LiveClock,
) -> MT5InstrumentProvider:
    """The provider of a connection, a venue and an instrument-provider config, created on first
    use."""
    registry_key = (connection, venue, config)

    if registry_key not in _provider_registry:
        _provider_registry[registry_key] = MT5InstrumentProvider(
            connection, venue=venue, clock=clock, config=config
        )

    return _provider_registry[registry_key]


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
        `custom`, else the one the node config of the venue `name` was built with, loading what the
        client config's `instrument_provider` names."""
        mt5_config: MT5Config = (
            config.custom["mt5_config"]
            if hasattr(config, "custom") and isinstance(getattr(config, "custom", None), dict)
            else _mt5_config_registry.get(name)
        )
        if mt5_config is None:
            raise RuntimeError(f"MT5LiveDataClientFactory: no MT5Config for {name}")
        conn = _ensure_connection(mt5_config)

        return MT5DataClient(
            loop=loop,
            connection=conn,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=_ensure_provider(
                conn, mt5_config.venue, config.instrument_provider, clock
            ),
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
        in its `custom`, else the one the node config of the venue `name` was built with, loading
        what the client config's `instrument_provider` names."""
        mt5_config: MT5Config = (
            config.custom["mt5_config"]
            if hasattr(config, "custom") and isinstance(getattr(config, "custom", None), dict)
            else _mt5_config_registry.get(name)
        )
        if mt5_config is None:
            raise RuntimeError(f"MT5LiveExecClientFactory: no MT5Config for {name}")
        conn = _ensure_connection(mt5_config)

        return MT5LiveExecutionClient(
            loop=loop,
            connection=conn,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=_ensure_provider(
                conn, mt5_config.venue, config.instrument_provider, clock
            ),
            config=mt5_config,
        )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE BUILDER
# ─────────────────────────────────────────────────────────────────────────────


def build_mt5_node_config(
    mt5_config: MT5Config,
    risk_engine_config: LiveRiskEngineConfig | None = None,
    logging_config=None,
    *,
    data_instruments: frozenset[InstrumentId],
    exec_instruments: frozenset[InstrumentId],
) -> TradingNodeConfig:
    """The node config of the MT5Config's venue: its data client loading `data_instruments` and its
    execution client loading `exec_instruments`, each through its `InstrumentProviderConfig` and the
    default route, and the MT5Config the factories build them from."""
    _mt5_config_registry[mt5_config.venue.value] = mt5_config

    data_client_cfg = LiveDataClientConfig(
        instrument_provider=InstrumentProviderConfig(load_ids=data_instruments),
        routing=RoutingConfig(default=True, venues=frozenset({mt5_config.venue.value})),
    )
    exec_client_cfg = LiveExecClientConfig(
        instrument_provider=InstrumentProviderConfig(load_ids=exec_instruments),
        routing=RoutingConfig(default=True, venues=frozenset({mt5_config.venue.value})),
    )

    kwargs: dict = {
        "data_clients": {mt5_config.venue.value: data_client_cfg},
        "exec_clients": {mt5_config.venue.value: exec_client_cfg},
    }
    if risk_engine_config is not None:
        kwargs["risk_engine"] = risk_engine_config
    if logging_config is not None:
        kwargs["logging"] = logging_config

    return TradingNodeConfig(**kwargs)
