"""Certification-only EA relay; chart-control frames can be released as one queued burst."""

import asyncio
import json
import time
from enum import StrEnum

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from mt5connector.server.wire import binary
from mt5connector.server.wire.push_wire import ChartRole


class Operation(StrEnum):
    PAUSE = "pause"
    RELEASE = "release"
    STATUS = "status"


class Relay:
    def __init__(self):
        self.paused = False
        self.queued = []
        self.events = []

    def record(self, kind, symbol, role):
        event = {
            "at": time.monotonic(),
            "kind": kind.name,
            "symbol": symbol,
            "role": role.name,
        }
        self.events.append(event)
        return event

    async def peer(self, frontend):
        first = await frontend.recv()
        hello = binary.decode_hello(first)
        if not isinstance(hello, binary.EaHello):
            raise ValueError("the certification relay accepts EAs alone")
        self.record(binary.FrameKind.HELLO, hello.symbol, hello.chart_role)
        async with connect("ws://127.0.0.1:9001", max_size=None) as backend:
            await backend.send(first)

            async def to_hub():
                async for payload in frontend:
                    kind = binary.FrameKind(payload[1])
                    if kind in (binary.FrameKind.CHART_OPENED, binary.FrameKind.CHART_FAILED):
                        frame = binary.decode(payload, binary.Sender.EA)
                        self.record(kind, frame.symbol, frame.chart_role)
                    await backend.send(payload)

            async def to_ea():
                async for payload in backend:
                    kind = binary.FrameKind(payload[1])
                    if hello.chart_role is ChartRole.SPAWNER and kind in (
                        binary.FrameKind.OPEN_CHART,
                        binary.FrameKind.CLOSE_CHART,
                    ):
                        frame = binary.decode(payload, binary.Sender.HUB)
                        event = self.record(kind, frame.symbol, frame.chart_role)
                        if self.paused:
                            self.queued.append((frontend, payload, event))
                            continue
                    await frontend.send(payload)

            tasks = [asyncio.create_task(to_hub()), asyncio.create_task(to_ea())]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def control(self, reader, writer):
        header = await reader.readuntil(b"\r\n\r\n")
        lengths = [
            line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")
            if line.lower().startswith(b"content-length:")
        ]
        if len(lengths) != 1 or not 0 < int(lengths[0]) <= 1024:
            raise ValueError("control request needs one bounded Content-Length")
        request = json.loads(await reader.readexactly(int(lengths[0])))
        operation = Operation(request["operation"])
        if operation is Operation.PAUSE:
            self.paused = True
        elif operation is Operation.RELEASE:
            self.paused = False
            queued, self.queued = self.queued, []
            for frontend, payload, _ in queued:
                await frontend.send(payload)
        body = json.dumps(
            {"queued": [event for _, _, event in self.queued], "events": self.events}
        ).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def main():
    relay = Relay()
    async with serve(relay.peer, "127.0.0.1", 9000, max_size=None):
        server = await asyncio.start_server(relay.control, "127.0.0.1", 9002)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
