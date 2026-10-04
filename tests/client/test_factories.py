"""The data and execution client factories, the connection and provider they share, and the node
config that wires them."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from nautilus_trader.common.component import LiveClock
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency
from venue_doubles import account_info

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.data import MT5DataClient
from mt5connector.client.errors import MT5ConfigError
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.factories import (
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
    _connection_registry,
    _get_or_create_connection,
    build_mt5_node_config,
)


def make_config(account=12345678, server="Exness-MT5Trial1", symbols=None):
    return MT5Config(
        account=account,
        password="test_password",
        server=server,
        symbols=["EURUSD"] if symbols is None else symbols,
        server_url="http://127.0.0.1:5000",
    )


def make_nt_components():
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.test_kit.stubs.component import TestComponentStubs

    return TestComponentStubs.msgbus(), TestComponentStubs.cache(), LiveClock()


def make_live_data_config(mt5_config):
    cfg = MagicMock()
    cfg.custom = {"mt5_config": mt5_config}
    return cfg


def make_live_exec_config(mt5_config):
    cfg = MagicMock()
    cfg.custom = {"mt5_config": mt5_config}
    return cfg


@pytest.fixture(autouse=True)
def clean_registry():
    from mt5connector.client.factories import _mt5_config_registry

    _connection_registry.clear()
    _mt5_config_registry.clear()
    yield
    _connection_registry.clear()
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


class TestGetOrCreateConnection:
    def test_creates_connection_on_first_call(self, mock_mt5_conn, mock_provider):
        MockConn, _ = mock_mt5_conn
        config = make_config()
        clock = LiveClock()
        _get_or_create_connection(config, clock)
        MockConn.assert_called_once_with(config)

    def test_calls_connect_on_first_call(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        clock = LiveClock()
        _get_or_create_connection(config, clock)
        conn_inst.connect.assert_called_once()

    def test_reuses_connection_on_second_call(self, mock_mt5_conn, mock_provider):
        config = make_config()
        clock = LiveClock()
        conn1, prov1 = _get_or_create_connection(config, clock)
        conn2, prov2 = _get_or_create_connection(config, clock)
        assert conn1 is conn2
        assert prov1 is prov2

    def test_connect_called_only_once_for_same_config(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        config = make_config()
        clock = LiveClock()
        _get_or_create_connection(config, clock)
        _get_or_create_connection(config, clock)
        conn_inst.connect.assert_called_once()

    def test_returns_tuple_of_connection_and_provider(self, mock_mt5_conn, mock_provider):
        _, conn_inst = mock_mt5_conn
        _, prov_inst = mock_provider
        config = make_config()
        clock = LiveClock()
        conn, provider = _get_or_create_connection(config, clock)
        assert conn is conn_inst
        assert provider is prov_inst


class TestConnectionRegistryIsolation:
    def test_different_accounts_get_separate_connections(self, mock_mt5_conn, mock_provider):
        MockConn, _ = mock_mt5_conn
        clock = LiveClock()
        _get_or_create_connection(make_config(account=11111111), clock)
        _get_or_create_connection(make_config(account=22222222), clock)
        assert MockConn.call_count == 2

    def test_different_servers_get_separate_connections(self, mock_mt5_conn, mock_provider):
        MockConn, _ = mock_mt5_conn
        clock = LiveClock()
        _get_or_create_connection(make_config(server="BrokerA-Demo"), clock)
        _get_or_create_connection(make_config(server="BrokerB-Demo"), clock)
        assert MockConn.call_count == 2

    def test_same_account_different_server_is_separate(self, mock_mt5_conn, mock_provider):
        MockConn, _ = mock_mt5_conn
        clock = LiveClock()
        _get_or_create_connection(make_config(account=12345678, server="ServerA"), clock)
        _get_or_create_connection(make_config(account=12345678, server="ServerB"), clock)
        assert MockConn.call_count == 2

    def test_registry_grows_with_each_new_config(self, mock_mt5_conn, mock_provider):
        clock = LiveClock()
        _get_or_create_connection(make_config(account=10000001), clock)
        _get_or_create_connection(make_config(account=10000002), clock)
        _get_or_create_connection(make_config(account=10000003), clock)
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

        result = build_mt5_node_config(make_config(symbols=["EURUSD", "XAUUSD"]))
        assert isinstance(result, TradingNodeConfig)

    def test_registry_keyed_by_venue_string(self):
        from mt5connector.client.factories import _mt5_config_registry

        build_mt5_node_config(make_config())
        assert "MT5" in _mt5_config_registry

    def test_mt5_config_stored_in_registry(self):
        from mt5connector.client.factories import _mt5_config_registry

        config = make_config()
        build_mt5_node_config(config)
        assert _mt5_config_registry["MT5"] is config

    def test_optional_risk_engine_config_is_wired(self):
        from nautilus_trader.config import LiveRiskEngineConfig

        risk_config = LiveRiskEngineConfig()
        result = build_mt5_node_config(make_config(), risk_engine_config=risk_config)
        assert result.risk_engine is risk_config

    def test_strategies_kwarg_not_accepted(self):
        # A node takes its strategies through node.trader.add_strategy once it is built.
        with pytest.raises(TypeError, match="strategies"):
            build_mt5_node_config(make_config(), strategies=[MagicMock()])

    def test_node_config_has_no_strategies_by_default(self):
        # The caller adds strategies through node.trader.add_strategy().
        result = build_mt5_node_config(make_config())
        assert not result.strategies

    def test_data_clients_keyed_by_venue(self):
        result = build_mt5_node_config(make_config())
        assert "MT5" in result.data_clients

    def test_exec_clients_keyed_by_venue(self):
        result = build_mt5_node_config(make_config())
        assert "MT5" in result.exec_clients

    def test_data_client_routing_is_default(self):
        result = build_mt5_node_config(make_config())
        assert result.data_clients["MT5"].routing.default is True

    def test_exec_client_routing_is_default(self):
        result = build_mt5_node_config(make_config())
        assert result.exec_clients["MT5"].routing.default is True

    def test_registry_updated_on_repeated_call(self):
        from mt5connector.client.factories import _mt5_config_registry

        config1 = make_config(account=11111111)
        config2 = make_config(account=22222222)
        build_mt5_node_config(config1)
        build_mt5_node_config(config2)
        assert _mt5_config_registry["MT5"] is config2


class TestInstrumentLoading:
    def test_the_node_config_names_the_configured_symbols_as_load_ids(self):
        result = build_mt5_node_config(make_config(symbols=["EURUSD.a", "XAUUSD+"]))
        for client in (result.data_clients["MT5"], result.exec_clients["MT5"]):
            assert client.instrument_provider.load_all is False
            assert client.instrument_provider.load_ids == frozenset(
                {InstrumentId.from_str("EURUSD.a.MT5"), InstrumentId.from_str("XAUUSD+.MT5")}
            )

    def test_the_node_config_refuses_a_config_naming_no_symbols(self):
        with pytest.raises(MT5ConfigError, match="symbols"):
            build_mt5_node_config(make_config(symbols=[]))

    def test_a_client_factory_refuses_a_config_naming_no_symbols_before_connecting(
        self, mock_mt5_conn
    ):
        _, conn_inst = mock_mt5_conn
        msgbus, cache, clock = make_nt_components()
        with pytest.raises(MT5ConfigError, match="symbols"):
            MT5LiveDataClientFactory.create(
                asyncio.new_event_loop(),
                "MT5",
                make_live_data_config(make_config(symbols=[])),
                msgbus,
                cache,
                clock,
            )
        conn_inst.connect.assert_not_called()

    def test_the_shared_provider_loads_the_configured_symbols(self, mock_mt5_conn):
        with patch("mt5connector.client.factories.MT5InstrumentProvider") as provider:
            _get_or_create_connection(make_config(symbols=["EURUSDm", "GBPUSDm"]), LiveClock())
        config = provider.call_args.kwargs["config"]
        assert config.load_ids == frozenset(
            {InstrumentId.from_str("EURUSDm.MT5"), InstrumentId.from_str("GBPUSDm.MT5")}
        )
