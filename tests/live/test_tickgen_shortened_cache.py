"""The candidate hub tests/live/prepare_tickgen.py starts with `--shorten-cache`, on a history copy
whose M1 frame cache the collector rewrote short, answering the floors and the bars of the
untouched copy. It runs only when MT5_TICK_RAW_DIR is set; MT5_TICK_ACCEPTANCE_REQUIRED=1 fails
every case whose inputs or hub are missing instead of skipping it."""

import asyncio
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mt5connector.client.config import MT5Config
from mt5connector.client.hub import HubSession
from mt5connector.wire import calls, mirror
from mt5connector.wire.history_wire import Series

CHECKOUT = Path(__file__).resolve().parents[2]
SYMBOL = os.environ.get("MT5_LIVE_HUB_SYMBOL")
HUB_URL = os.environ.get("MT5_LIVE_HUB_URL", "ws://127.0.0.1:9000")
RAW_DIR = os.environ.get("MT5_TICK_RAW_DIR")
REQUIRED = os.environ.get("MT5_TICK_ACCEPTANCE_REQUIRED") == "1"
EA_SOURCE = "packages/server/mt5connector/server/mql5/Expert/ticks.mq5"
SYNCED_S = 2 * calls.HISTORY_BUSY_S
# The floors and the January 2023 M1 row count a hub on an untouched copy of the history answers.
M1_FLOOR = int(datetime(2018, 9, 14, 1, 58, tzinfo=UTC).timestamp())
QUOTE_FLOOR = int(datetime(2018, 9, 26, 16, 16, tzinfo=UTC).timestamp())
JANUARY = (
    int(datetime(2023, 1, 2, tzinfo=UTC).timestamp()),
    int(datetime(2023, 1, 14, tzinfo=UTC).timestamp()),
)
JANUARY_ROWS = 14_251


def unavailable(reason: str):
    if REQUIRED:
        pytest.fail(reason)
    else:
        pytest.skip(reason)


class Log:
    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        print(message)

    def error(self, message: str) -> None:
        print(message)

    def exception(self, message: str, failure: BaseException) -> None:
        print(f"{message}: {failure!r}")


@pytest.fixture(scope="module")
def provenance() -> dict:
    import mt5connector.client

    if SYMBOL is None or RAW_DIR is None:
        unavailable("MT5_LIVE_HUB_SYMBOL and MT5_TICK_RAW_DIR must be set")
    assert Path(mt5connector.client.__file__).resolve().is_relative_to(CHECKOUT)
    path = Path(RAW_DIR) / "provenance.json"
    if not path.is_file():
        unavailable(f"{path} is missing")
    prepared = json.loads(path.read_text())
    if "m1_first_utc" not in prepared:
        unavailable(f"{path} was not prepared with --shorten-cache")
    running = subprocess.run(
        ["podman", "inspect", "--format", "{{.Image}} {{.State.Running}}", prepared["container"]],
        capture_output=True,
        text=True,
    )
    if running.returncode != 0:
        unavailable(f"the candidate container {prepared['container']} is not there")
    assert running.stdout.split() == [prepared["image_id"], "true"]
    assert prepared["image_ticks_sha256"] == prepared["source_sha256"][EA_SOURCE]
    assert HUB_URL.rstrip("/").endswith(f":{prepared['port']}")
    assert prepared["symbol"] == SYMBOL
    return prepared


def test_a_shortened_m1_cache_answers_the_untouched_floors_and_january_2023_m1_rows(provenance):
    assert provenance["m1_first_utc"] > JANUARY[1]

    async def reads():
        session = HubSession(
            MT5Config(account=1, server="unused", ws_url=HUB_URL),
            asyncio.get_running_loop(),
            lambda frame: None,
            None,
            Log(),
        )
        await session.connect()
        try:
            ranges = await asyncio.wait_for(
                session.deferring(calls.Call.RANGES, symbol=SYMBOL), SYNCED_S
            )
            january = await asyncio.wait_for(
                session.deferring(
                    calls.Call.COPY_RATES_RANGE,
                    symbol=SYMBOL,
                    timeframe=mirror.TIMEFRAME_M1,
                    date_from=JANUARY[0],
                    date_to=JANUARY[1],
                ),
                SYNCED_S,
            )
        finally:
            await session.disconnect()
        return ranges.result, january.result

    try:
        ranges, january = asyncio.run(reads())
    except OSError as failure:
        unavailable(f"no hub answers at {HUB_URL}: {failure}")

    assert ranges.series[Series.M1].floor == M1_FLOOR
    assert ranges.series[Series.TICKS].floor == QUOTE_FLOOR
    assert len(january) == JANUARY_ROWS
