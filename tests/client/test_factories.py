"""The data and execution client factories, the connection and the instrument providers they share,
and the node config that wires them."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Venue
from nautilus_trader.model.objects import Currency
from venue_doubles import account_info, symbol_info

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.data import MT5DataClient
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.factories import (
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
    _connection_registry,
    _ensure_connection,
    _ensure_provider,
    _mt5_config_registry,
    _provider_registry,
    build_mt5_node_config,
)

EURUSD = InstrumentId.from_str("EURUSD.MT5")
GBPUSD = InstrumentId.from_str("GBPUSD.MT5")


def make_config(account=12345678, server="Exness-MT5Trial1", venue=MT5_VENUE):
    return MT5Config(
        account=account,
        password="test_password",
        server=server,
        server_url="http://127.0.0.1:5000",
        venue=venue,
    )


def make_nt_components():
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.test_kit.stubs.component import TestComponentStubs

    return TestComponentStubs.msgbus(), TestComponentStubs.cache(), LiveClock()


def make_live_data_config(mt5_config, instrument_ids=frozenset({EURUSD})):
    """A data client config carrying `mt5_config`, its instrument provider loading
    `instrument_ids`."""
    cfg = MagicMock()
    cfg.custom = {"mt5_config": mt5_config}
    cfg.instrument_provider = InstrumentProviderConfig(load_ids=frozenset(instrument_ids))
    return cfg


def make_live_exec_config(mt5_config, instrument_ids=frozenset({EURUSD})):
    """An execution client config carrying `mt5_config`, its instrument provider loading
    `instrument_ids`."""
    cfg = MagicMock()
    cfg.custom = {"mt5_config": mt5_config}
    cfg.instrument_provider = InstrumentProviderConfig(load_ids=frozenset(instrument_ids))
    return cfg


def node_config(mt5_config, **kwargs):
    """The node config of `mt5_config` whose clients both load EURUSD."""
    return build_mt5_node_config(
        mt5_config,
        data_instruments=frozenset({EURUSD}),
        exec_instruments=frozenset({EURUSD}),
        **kwargs,
    )


@pytest.fixture(autouse=True)
def clean_registry():
    from mt5connector.client.factories import _mt5_config_registry

    _connection_registry.clear()
    _provider_registry.clear()
    _mt5_config_registry.clear()
    yield
    _connection_registry.clear()
    _provider_registry.clear()
    _mt5_config_registry.clear()


@pytest.fixture
def mock_mt5_conn():
    with patch("mt5connector.client.factories.MT5Connection") as MockConn:
        instance = MagicMock()
        instance.connect = MagicMock()
        instance.disconnect = MagicMock()
        instance.ensure_connected = MagicMock()
        instance.get_account_info.return_value = AccountSnapshot.from_mt5(
            account_info(currency="EUR")
        )
        MockConn.return_value = instance
        yield MockConn, instance


@pytest.fixture
def mock_provider():
    from nautilus_trader.common.providers import InstrumentProvider

    from mt5connector.client.providers import MT5InstrumentProvider as RealProvider

    real_instance = RealProvider.__new__(RealProvider)
    InstrumentProvider.__init__(real_instance)
    real_instance._conn = MagicMock()
    real_instance.get_instrument = MagicMock(return_value=None)

    with patch("mt5connector.client.factories.MT5InstrumentProvider") as MockProv:
        MockProv.return_value = real_instance
        yield MockProv, real_instance


class TestEnsureConnection:
    def test_creates_connection_on_first_call(self, mock_mt5_conn):
        MockConn, _ = mock_mt5_conn
        config = make_config()
        _ensure_connection(config)
        MockConn.assert_called_once_with(config)

    def test_calls_connect_on_first_call(self, mock_mt5_conn):
        _, conn_inst = mock_mt5_conn
        _ensure_connection(make_config())
        conn_inst.connect.assert_called_once()

    def test_reuses_connection_on_second_call(self, mock_mt5_conn):
        config = make_config()
        assert _ensure_connection(config) is _ensure_connection(config)

    def test_connect_called_only_once_for_same_config(self, mock_mt5_conn):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        _ensure_connection(config)
        _ensure_connection(config)
        conn_inst.connect.assert_called_once()

    def test_returns_the_connection(self, mock_mt5_conn):
        _, conn_inst = mock_mt5_conn
        assert _ensure_connection(make_config()) is conn_inst


class TestConnectionRegistryIsolation:
    def test_different_accounts_get_separate_connections(self, mock_mt5_conn):
        MockConn, _ = mock_mt5_conn
        _ensure_connection(make_config(account=11111111))
        _ensure_connection(make_config(account=22222222))
        assert MockConn.call_count == 2

    def test_different_servers_get_separate_connections(self, mock_mt5_conn):
        MockConn, _ = mock_mt5_conn
        _ensure_connection(make_config(server="BrokerA-Demo"))
        _ensure_connection(make_config(server="BrokerB-Demo"))
        assert MockConn.call_count == 2

    def test_same_account_different_server_is_separate(self, mock_mt5_conn):
        MockConn, _ = mock_mt5_conn
        _ensure_connection(make_config(account=12345678, server="ServerA"))
        _ensure_connection(make_config(account=12345678, server="ServerB"))
        assert MockConn.call_count == 2

    def test_registry_grows_with_each_new_config(self, mock_mt5_conn):
        _ensure_connection(make_config(account=10000001))
        _ensure_connection(make_config(account=10000002))
        _ensure_connection(make_config(account=10000003))
        assert len(_connection_registry) == 3


class TestDataClientFactory:
    def test_create_returns_mt5_data_client(self, mock_mt5_conn, mock_provider):
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        assert isinstance(client, MT5DataClient)

    def test_create_reads_mt5_config_from_custom(self, mock_mt5_conn, mock_provider):
        config = make_config(account=99998888)
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        assert client._config.account == 99998888

    def test_create_uses_registry_connection(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        assert client._conn is conn_inst

    def test_create_twice_uses_same_connection(self, mock_mt5_conn, mock_provider):
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        c1 = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        msgbus2, cache2, clock2 = make_nt_components()
        c2 = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus2, cache2, clock2
        )
        assert c1._conn is c2._conn


class TestExecClientFactory:
    def test_create_returns_mt5_exec_client(self, mock_mt5_conn, mock_provider):
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus, cache, clock
        )
        assert isinstance(client, MT5LiveExecutionClient)

    def test_create_reads_mt5_config_from_custom(self, mock_mt5_conn, mock_provider):
        config = make_config(account=77776666)
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus, cache, clock
        )
        assert client._config.account == 77776666

    def test_create_uses_registry_connection(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus, cache, clock
        )
        assert client._conn is conn_inst

    def test_the_client_books_in_the_currency_of_the_account_connected(
        self, mock_mt5_conn, mock_provider
    ):
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(make_config()), msgbus, cache, clock
        )
        assert client.base_currency == Currency.from_str("EUR")

    def test_the_account_id_waits_for_the_accounts_own_login(self, mock_mt5_conn, mock_provider):
        config = make_config(account=55554444)
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus, cache, clock
        )
        assert client.account_id is None


class TestFactoriesShareConnection:
    def test_data_and_exec_factories_share_connection(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        msgbus2, cache2, clock2 = make_nt_components()
        data_client = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        exec_client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus2, cache2, clock2
        )
        assert data_client._conn is exec_client._conn

    def test_connection_created_only_once_for_both_factories(self, mock_mt5_conn, mock_provider):
        MockConn, _ = mock_mt5_conn
        config = make_config()
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        msgbus2, cache2, clock2 = make_nt_components()
        MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(config), msgbus, cache, clock
        )
        MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(config), msgbus2, cache2, clock2
        )
        assert MockConn.call_count == 1


class TestBuildMt5NodeConfig:

    def test_returns_trading_node_config(self):
        from nautilus_trader.config import TradingNodeConfig

        result = node_config(make_config())
        assert isinstance(result, TradingNodeConfig)

    def test_registry_keyed_by_venue_string(self):
        from mt5connector.client.factories import _mt5_config_registry

        node_config(make_config())
        assert "MT5" in _mt5_config_registry

    def test_mt5_config_stored_in_registry(self):
        from mt5connector.client.factories import _mt5_config_registry

        config = make_config()
        node_config(config)
        assert _mt5_config_registry["MT5"] is config

    def test_optional_risk_engine_config_is_wired(self):
        from nautilus_trader.config import LiveRiskEngineConfig

        risk_config = LiveRiskEngineConfig()
        result = node_config(make_config(), risk_engine_config=risk_config)
        assert result.risk_engine is risk_config

    def test_strategies_kwarg_not_accepted(self):
        # A node takes its strategies through node.trader.add_strategy once it is built.
        with pytest.raises(TypeError, match="strategies"):
            node_config(make_config(), strategies=[MagicMock()])

    def test_node_config_has_no_strategies_by_default(self):
        # The caller adds strategies through node.trader.add_strategy().
        result = node_config(make_config())
        assert not result.strategies

    def test_data_clients_keyed_by_venue(self):
        result = node_config(make_config())
        assert "MT5" in result.data_clients

    def test_exec_clients_keyed_by_venue(self):
        result = node_config(make_config())
        assert "MT5" in result.exec_clients

    def test_data_client_routing_is_default(self):
        result = node_config(make_config())
        assert result.data_clients["MT5"].routing.default is True

    def test_exec_client_routing_is_default(self):
        result = node_config(make_config())
        assert result.exec_clients["MT5"].routing.default is True

    def test_registry_updated_on_repeated_call(self):
        from mt5connector.client.factories import _mt5_config_registry

        config1 = make_config(account=11111111)
        config2 = make_config(account=22222222)
        node_config(config1)
        node_config(config2)
        assert _mt5_config_registry["MT5"] is config2


@pytest.fixture
def terminal():
    """The package behind the provider's shim, serving EURUSD and GBPUSD, neither charging a
    commission."""
    definitions = {
        "EURUSD": symbol_info(name="EURUSD"),
        "GBPUSD": symbol_info(name="GBPUSD", currency_base="GBP"),
    }
    package = MagicMock()
    package.symbol_select.side_effect = lambda name, enable: name in definitions
    package.symbol_info.side_effect = definitions.get
    package.symbols_get.return_value = tuple(definitions.values())
    package.commission_schedule.return_value = {"ret": 0, "last_error": 0, "rules": []}
    with patch("mt5connector.client.providers.mt5", package):
        yield package


def loaded_ids(provider) -> set[InstrumentId]:
    return {instrument.id for instrument in provider.list_all()}


class TestInstrumentProviders:
    def test_the_node_config_gives_each_client_its_own_instruments_as_load_ids(self):
        result = build_mt5_node_config(
            make_config(),
            data_instruments=frozenset({EURUSD, GBPUSD}),
            exec_instruments=frozenset({EURUSD}),
        )
        assert result.data_clients["MT5"].instrument_provider == InstrumentProviderConfig(
            load_ids=frozenset({EURUSD, GBPUSD})
        )
        assert result.exec_clients["MT5"].instrument_provider == InstrumentProviderConfig(
            load_ids=frozenset({EURUSD})
        )

    async def test_each_client_of_a_node_loads_what_its_config_names_on_one_connection(
        self, mock_mt5_conn, terminal
    ):
        node = build_mt5_node_config(
            make_config(),
            data_instruments=frozenset({EURUSD, GBPUSD}),
            exec_instruments=frozenset({EURUSD}),
        )
        loop = asyncio.get_running_loop()
        msgbus, cache, clock = make_nt_components()
        msgbus2, cache2, clock2 = make_nt_components()
        data_client = MT5LiveDataClientFactory.create(
            loop, "MT5", node.data_clients["MT5"], msgbus, cache, clock
        )
        exec_client = MT5LiveExecClientFactory.create(
            loop, "MT5", node.exec_clients["MT5"], msgbus2, cache2, clock2
        )

        await data_client._provider.initialize()
        await exec_client._provider.initialize()

        assert loaded_ids(data_client._provider) == {EURUSD, GBPUSD}
        assert loaded_ids(exec_client._provider) == {EURUSD}
        assert data_client._conn is exec_client._conn

    def test_clients_naming_the_same_instruments_share_one_provider(self, mock_mt5_conn):
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        msgbus2, cache2, clock2 = make_nt_components()
        data_client = MT5LiveDataClientFactory.create(
            loop, "MT5", make_live_data_config(make_config(), {EURUSD}), msgbus, cache, clock
        )
        exec_client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(make_config(), {EURUSD}), msgbus2, cache2, clock2
        )
        assert data_client._provider is exec_client._provider

    def test_clients_naming_different_instruments_have_their_own_providers(self, mock_mt5_conn):
        MockConn, _ = mock_mt5_conn
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = make_nt_components()
        msgbus2, cache2, clock2 = make_nt_components()
        data_client = MT5LiveDataClientFactory.create(
            loop,
            "MT5",
            make_live_data_config(make_config(), {EURUSD, GBPUSD}),
            msgbus,
            cache,
            clock,
        )
        exec_client = MT5LiveExecClientFactory.create(
            loop, "MT5", make_live_exec_config(make_config(), {EURUSD}), msgbus2, cache2, clock2
        )
        assert data_client._provider is not exec_client._provider
        assert data_client._conn is exec_client._conn
        assert MockConn.call_count == 1


ALPHA = Venue("MT5_ALPHA")
BETA = Venue("MT5_BETA")


class TestNamedVenues:
    def test_the_node_config_of_a_named_venue_is_keyed_and_routed_by_it(self):
        config = make_config(venue=ALPHA)
        result = node_config(config)
        assert _mt5_config_registry == {"MT5_ALPHA": config}
        assert set(result.data_clients) == {"MT5_ALPHA"}
        assert set(result.exec_clients) == {"MT5_ALPHA"}
        assert result.data_clients["MT5_ALPHA"].routing.venues == frozenset({"MT5_ALPHA"})
        assert result.exec_clients["MT5_ALPHA"].routing.venues == frozenset({"MT5_ALPHA"})

    def test_two_venues_node_configs_coexist_each_factory_finding_its_own_config(
        self, mock_mt5_conn, mock_provider
    ):
        ftmo = make_config(account=11111111, venue=ALPHA)
        icm = make_config(account=22222222, venue=BETA)
        ftmo_node = node_config(ftmo)
        icm_node = node_config(icm)
        assert _mt5_config_registry == {"MT5_ALPHA": ftmo, "MT5_BETA": icm}

        loop = asyncio.new_event_loop()
        clients = {}
        for name, node in (("MT5_ALPHA", ftmo_node), ("MT5_BETA", icm_node)):
            msgbus, cache, clock = make_nt_components()
            msgbus2, cache2, clock2 = make_nt_components()
            clients[name] = (
                MT5LiveDataClientFactory.create(
                    loop, name, node.data_clients[name], msgbus, cache, clock
                ),
                MT5LiveExecClientFactory.create(
                    loop, name, node.exec_clients[name], msgbus2, cache2, clock2
                ),
            )

        for name, config in (("MT5_ALPHA", ftmo), ("MT5_BETA", icm)):
            data_client, exec_client = clients[name]
            assert data_client._config is config
            assert exec_client._config is config
            assert (data_client.id, data_client.venue) == (ClientId(name), Venue(name))
            assert (exec_client.id, exec_client.venue) == (ClientId(name), Venue(name))

    def test_a_factory_of_a_venue_no_node_config_names_is_refused(self):
        node = node_config(make_config(venue=ALPHA))
        msgbus, cache, clock = make_nt_components()
        with pytest.raises(RuntimeError, match="no MT5Config for MT5_BETA"):
            MT5LiveDataClientFactory.create(
                asyncio.new_event_loop(),
                "MT5_BETA",
                node.data_clients["MT5_ALPHA"],
                msgbus,
                cache,
                clock,
            )

    def test_one_connection_serves_two_venues_through_a_provider_each(self, mock_mt5_conn):
        _, conn_inst = mock_mt5_conn
        load = InstrumentProviderConfig(load_ids=frozenset({EURUSD}))
        _, _, clock = make_nt_components()
        ftmo = _ensure_provider(conn_inst, ALPHA, load, clock)
        icm = _ensure_provider(conn_inst, BETA, load, clock)
        assert ftmo is not icm
        assert ftmo is _ensure_provider(conn_inst, ALPHA, load, clock)

    async def test_two_venues_on_one_login_load_their_instruments_each_at_its_own_venue(
        self, mock_mt5_conn, terminal
    ):
        MockConn, _ = mock_mt5_conn
        loop = asyncio.get_running_loop()
        loaded_by_venue = {}
        for venue in (ALPHA, BETA):
            node = build_mt5_node_config(
                make_config(venue=venue),
                data_instruments=frozenset({EURUSD}),
                exec_instruments=frozenset({EURUSD}),
            )
            msgbus, cache, clock = make_nt_components()
            data_client = MT5LiveDataClientFactory.create(
                loop, venue.value, node.data_clients[venue.value], msgbus, cache, clock
            )
            await data_client._provider.initialize()
            loaded_by_venue[venue.value] = loaded_ids(data_client._provider)
            assert data_client._provider.get_instrument("EURUSD").id.venue == venue

        assert loaded_by_venue == {
            "MT5_ALPHA": {InstrumentId.from_str("EURUSD.MT5_ALPHA")},
            "MT5_BETA": {InstrumentId.from_str("EURUSD.MT5_BETA")},
        }
        assert MockConn.call_count == 1
