"""Chart-count health follows the serving spawner's connection."""

from ea_wire import FakeEa
from hub_doubles import eventually, health_of, run_hub, verified, verified_by

from mt5connector.server.wire import binary
from mt5connector.server.wire.push_wire import ChartRole


async def test_chart_count_is_forgotten_when_the_spawner_leaves_and_reports_fresh_on_reconnect():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        account = await FakeEa.connect(port, ChartRole.ACCOUNT, "EURUSD")
        await spawner.send(binary.ChartStatus(100, 100))
        await eventually(lambda: hub._chart_status == binary.ChartStatus(100, 100))
        status, body = await health_of(port)
        assert status == 200
        assert (body["result"]["chart_count"], body["result"]["chart_limit"]) == (100, True)

        await spawner.close()
        await eventually(lambda: hub._spawner is None)
        status, body = await health_of(port)
        assert status == 503
        assert (body["result"]["chart_count"], body["result"]["chart_limit"]) == (None, False)

        replacement = await FakeEa.connect(port, ChartRole.SPAWNER, "EURUSD")
        async with verified_by(replacement):
            _status, body = await health_of(port)
            assert (body["result"]["chart_count"], body["result"]["chart_limit"]) == (None, False)
            await replacement.send(binary.ChartStatus(2, 100))
            await eventually(lambda: hub._chart_status == binary.ChartStatus(2, 100))
            status, body = await health_of(port)
            assert status == 200
            assert (body["result"]["chart_count"], body["result"]["chart_limit"]) == (2, False)
            await replacement.close()
        await account.close()
    server.close()
    await server.wait_closed()
