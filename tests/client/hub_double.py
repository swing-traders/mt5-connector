"""A hub double speaking the client's wire to its sessions, and waits on what it received."""

import asyncio

from websockets.exceptions import ConnectionClosed

from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import Sender


def zeroed(layout: binary.Layout, **values) -> dict:
    record = {}
    for member in layout.members:
        if member.declared is mirror.MqlType.STRING:
            record[member.name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[member.name] = 0.0
        elif member.declared is mirror.MqlType.BOOL:
            record[member.name] = False
        elif member.declared is not None:
            record[member.name] = 0
    return record | values


class HubDouble:
    """A hub for a client's sessions to connect to, recording what each connection sends and
    answering what a test hands it."""

    def __init__(self):
        self.received: list[list] = []
        self.connections = []
        self.answers: dict[calls.Call, list] = {}

    async def handler(self, ws):
        self.connections.append(ws)
        frames = []
        self.received.append(frames)
        try:
            async for raw in ws:
                if not frames:
                    frame = binary.decode_hello(raw)
                else:
                    frame = binary.decode(raw, Sender.ADAPTER)
                frames.append(frame)
                if isinstance(frame, binary.Subscribe | binary.Unsubscribe):
                    await ws.send(binary.encode(binary.Ack(frame.op_id)))
                elif isinstance(frame, binary.Request) and calls.Call(frame.call) in self.answers:
                    await self._answered(ws, frame)
        except ConnectionClosed:
            pass

    async def _answered(self, ws, request: binary.Request) -> None:
        """Answers a request with its call's next answer, an AnswerError failing it; the last answer
        stands for every later request."""
        queued = self.answers[calls.Call(request.call)]
        if len(queued) > 1:
            answer = queued.pop(0)
        else:
            answer = queued[0]
        if isinstance(answer, binary.AnswerError):
            await ws.send(
                binary.encode(binary.Answer(request.request_id, 0, True, 0, error=answer))
            )
        else:
            count, body = calls.write_result(calls.CALLS[calls.Call(request.call)], answer)
            await ws.send(binary.encode(binary.Answer(request.request_id, 0, True, 0, count, body)))

    async def push(self, frame):
        await self.connections[-1].send(binary.encode(frame))

    def requests(self, connection=-1) -> list[binary.Request]:
        return [frame for frame in self.received[connection] if isinstance(frame, binary.Request)]

    def asked(self, call: calls.Call) -> list[binary.Request]:
        """Every request of the call, over every connection."""
        return [
            frame
            for frames in self.received
            for frame in frames
            if isinstance(frame, binary.Request) and frame.call == call
        ]

    async def answer(self, request: binary.Request, value, *, part=0, last=True, last_error=0):
        count, body = calls.write_result(calls.CALLS[calls.Call(request.call)], value)
        await self.push(binary.Answer(request.request_id, part, last, last_error, count, body))

    async def fail(self, request: binary.Request, error: binary.AnswerError, last_error=0):
        await self.push(binary.Answer(request.request_id, 0, True, last_error, error=error))


async def eventually(predicate, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("the condition never held")
        await asyncio.sleep(0.01)


async def asked(hub, count=1, connection=-1) -> list[binary.Request]:
    await eventually(lambda: len(hub.received) > 0 and len(hub.requests(connection)) >= count)
    return hub.requests(connection)
