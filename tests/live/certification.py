"""Health observations and explicit controls for certification's throwaway server."""

import asyncio
import csv
import io
import json
import os
import shutil
import time
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from http import HTTPStatus
from pathlib import PurePosixPath
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen

from mt5connector.wire.push_wire import ChartRole


async def health(url):
    parts = urlsplit(url)
    endpoint = urlunsplit(
        ("https" if parts.scheme == "wss" else "http", parts.netloc, "/health", "", "")
    )

    def read():
        try:
            response = urlopen(endpoint, timeout=5)
        except HTTPError as failure:
            response = failure
        with response:
            return HTTPStatus(response.status), json.load(response)

    return await asyncio.to_thread(read)


async def until(read, accepts, seconds, description, interval=0.02):
    deadline = time.monotonic() + seconds
    while True:
        value = await read()
        if accepts(value):
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"{description} not observed within {seconds} s")
        await asyncio.sleep(interval)


async def observed_pending(url, coroutines):
    """The completed answers and roles observed while those reads were pending."""
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    roles = set()
    try:
        while not all(task.done() for task in tasks):
            status, body = await health(url)
            assert status is HTTPStatus.OK, body
            roles.update(ChartRole[row["role"].upper()] for row in body["result"]["pending"])
            await asyncio.sleep(0)
        return [task.result() for task in tasks], roles
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class ProbeEvent(StrEnum):
    START = "START"
    CHART_ADDED = "CHART_ADDED"
    CHART_GONE = "CHART_GONE"
    WATCH_SELECTED = "WATCH_SELECTED"
    WATCH_GONE = "WATCH_GONE"
    WATCH_HELD = "WATCH_HELD"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    ACK = "ACK"
    FAILED = "FAILED"


class ProbeCommand(IntEnum):
    OPEN_STRAY = 1
    CLOSE_SYMBOL = 2
    SELECT_SYMBOL = 3


class RelayOperation(StrEnum):
    PAUSE = "pause"
    RELEASE = "release"
    STATUS = "status"


@dataclass(frozen=True)
class Observation:
    microseconds: int
    event: ProbeEvent
    symbol: str
    chart: int
    selected: bool
    visible: bool
    positions: int
    orders: int
    token: str


class Throwaway:
    """An explicitly named certification container and its terminal probe."""

    def __init__(self):
        self.container = os.environ["MT5_CERT_CONTAINER"]
        self.network = os.environ["MT5_CERT_NETWORK"]
        self.probe_file = os.environ["MT5_CERT_PROBE_FILE"]
        self.control_file = str(
            PurePosixPath(self.probe_file).with_name("connector-chart-probe.commands.tsv")
        )
        self.idle_seconds = float(os.environ["MT5_CERT_IDLE_SECONDS"])
        self.resync_symbol = os.environ["MT5_CERT_RESYNC_SYMBOL"]
        self.relay_script = os.environ["MT5_CERT_RELAY_SCRIPT"]
        self.runtime = shutil.which("docker") or shutil.which("podman")
        if self.runtime is None:
            raise RuntimeError("certification needs docker or podman")
        if not 0 < self.idle_seconds <= 10:
            raise ValueError("MT5_CERT_IDLE_SECONDS must be in (0, 10]")
        self.network_attached = True

    async def run(self, *arguments, input_text=None):
        process = await asyncio.create_subprocess_exec(
            self.runtime,
            *arguments,
            stdin=asyncio.subprocess.PIPE if input_text is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(None if input_text is None else input_text.encode()), 240
        )
        if process.returncode:
            raise AssertionError(f"container control failed: {stderr.decode()}")
        return stdout.decode()

    async def logs(self, since):
        return await self.run("logs", "--timestamps", "--since", str(since), self.container)

    async def observations(self):
        contents = await self.run(
            "exec",
            self.container,
            "python3",
            "-c",
            "import pathlib,sys; data=pathlib.Path(sys.argv[1]).read_bytes(); "
            "sys.stdout.buffer.write(data[:data.rfind(b'\\n')+1])",
            self.probe_file,
        )
        rows = csv.DictReader(io.StringIO(contents), delimiter="\t")
        result = [
            Observation(
                int(row["microseconds"]),
                ProbeEvent(row["event"]),
                row["symbol"],
                int(row["chart"]),
                row["selected"] == "1",
                row["visible"] == "1",
                int(row["positions"]),
                int(row["orders"]),
                row["token"],
            )
            for row in rows
        ]
        assert result and result[0].event is ProbeEvent.START, "terminal probe is not running"
        assert not any(row.event is ProbeEvent.FAILED for row in result), "terminal probe failed"
        return result

    async def command(self, operation, symbol):
        token = str(time.monotonic_ns())
        await self.run(
            "exec",
            "-i",
            self.container,
            "python3",
            "-c",
            "import pathlib,sys; p=pathlib.Path(sys.argv[1]); t=p.with_suffix('.next'); "
            "t.write_text(sys.stdin.read(),encoding='utf-8'); t.replace(p)",
            self.control_file,
            input_text=f"{token}\t{operation.value}\t{symbol}\n",
        )
        rows = await until(
            self.observations,
            lambda rows: any(row.event is ProbeEvent.ACK and row.token == token for row in rows),
            5,
            "terminal probe command acknowledgement",
        )
        return next(row for row in rows if row.event is ProbeEvent.ACK and row.token == token)

    async def relay(self, operation=RelayOperation.STATUS):
        answer = await self.run(
            "exec",
            self.container,
            "python3",
            "-c",
            "import json,sys,urllib.request; "
            "r=urllib.request.Request('http://127.0.0.1:9002/control',"
            "data=json.dumps({'operation':sys.argv[1]}).encode()); "
            "print(urllib.request.urlopen(r,timeout=5).read().decode())",
            operation.value,
        )
        return json.loads(answer)

    async def cut_network(self):
        await self.run("network", "disconnect", self.network, self.container)
        self.network_attached = False

    async def restore_network(self):
        if not self.network_attached:
            await self.run("network", "connect", self.network, self.container)
            self.network_attached = True

    async def start(self):
        await self.run("start", self.container)
        await self.run("exec", "-d", self.container, "python3", self.relay_script)


def listed_charts(rows):
    charts = {}
    for row in rows:
        if row.event is ProbeEvent.CHART_ADDED:
            charts[row.chart] = row.symbol
        elif row.event is ProbeEvent.CHART_GONE:
            charts.pop(row.chart, None)
    return charts


def selected_symbols(rows):
    symbols = {}
    for row in rows:
        if row.event in (ProbeEvent.WATCH_SELECTED, ProbeEvent.WATCH_HELD):
            symbols[row.symbol] = row
        elif row.event is ProbeEvent.WATCH_GONE:
            symbols.pop(row.symbol, None)
    return symbols
