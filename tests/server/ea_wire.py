"""The EA's half of the hub protocol, which only MQL5 speaks in production: the frames an EA sends
encoded, the frames the hub sends it decoded, and an EA's answer results; and a fake EA on a real
websockets connection."""

import asyncio
from dataclasses import dataclass

import numpy as np
import websockets

from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.binary import FrameKind, Reader, Writer
from mt5connector.server.wire.history_wire import FloorOrigin
from mt5connector.server.wire.push_wire import ChartRole, Role

_RATES = np.dtype(list(mirror.RATES.dtype))
_TICKS = np.dtype(list(mirror.TICKS.dtype))


def frame(kind: FrameKind, payload: bytes, *, correlation=0, count=0, flags=0, version=2) -> bytes:
    return binary.HEADER.pack(version, kind, flags, correlation, count, len(payload)) + payload


def encode_ea(sent) -> bytes:
    """An EA-sent frame's bytes, laid out as the MQL5 EA packs it."""
    writer = Writer()
    if isinstance(sent, binary.EaHello):
        writer.string(Role.EA)
        writer.string(sent.chart_role)
        writer.string(sent.symbol)
        return frame(FrameKind.HELLO, writer.payload())
    elif isinstance(sent, binary.ChartOpened):
        writer.string(sent.chart_role)
        writer.string(sent.symbol)
        return frame(FrameKind.CHART_OPENED, writer.payload())
    elif isinstance(sent, binary.ChartFailed):
        writer.string(sent.chart_role)
        writer.string(sent.symbol)
        writer.i32(sent.error)
        return frame(FrameKind.CHART_FAILED, writer.payload())
    elif isinstance(sent, binary.ChartKept):
        writer.string(sent.chart_role)
        writer.string(sent.symbol)
        writer.i32(sent.positions)
        writer.i32(sent.orders)
        return frame(FrameKind.CHART_KEPT, writer.payload())
    elif isinstance(sent, binary.RatesBar):
        writer.string(sent.symbol)
        writer.i32(sent.timeframe)
        binary.write_pod(writer, binary.RATES, mirror.RATES, sent.rates)
        return frame(FrameKind.BAR, writer.payload())
    elif isinstance(sent, binary.ServerTime):
        writer.string(sent.symbol)
        writer.i64(sent.trade_server)
        writer.i64(sent.current)
        writer.i64(sent.gmt)
        writer.boolean(sent.connected)
        return frame(FrameKind.SERVER_TIME, writer.payload())
    elif isinstance(sent, binary.Commissions):
        writer.string(sent.symbol)
        binary.write_schedule(writer, sent.schedule)
        return frame(FrameKind.COMMISSIONS, writer.payload())
    elif isinstance(sent, binary.Sessions):
        writer.string(sent.symbol)
        binary.write_sessions(writer, sent.sessions)
        return frame(FrameKind.SESSIONS, writer.payload())
    else:
        # A tick, a transaction, an answer and a busy: the layouts the hub sends a consumer.
        return binary.encode(sent)


def decode_to_ea(data: bytes):
    """A frame the hub sent an EA."""
    version, code, flags, correlation, count, length = binary.HEADER.unpack_from(data)
    assert version == binary.VERSION and length == len(data) - binary.HEADER.size
    reader = Reader(data, binary.HEADER.size)
    kind = FrameKind(code)
    if kind is FrameKind.WANTED:
        symbol = reader.string("symbol")
        ticks = reader.boolean("ticks")
        decoded = binary.Wanted(symbol, ticks, tuple(reader.i32("tf") for _ in range(count)))
    elif kind is FrameKind.OPEN_CHART:
        decoded = binary.OpenChart(ChartRole(reader.string("role")), reader.string("symbol"))
    elif kind is FrameKind.CLOSE_CHART:
        decoded = binary.CloseChart(ChartRole(reader.string("role")), reader.string("symbol"))
    elif kind is FrameKind.DUPLICATE:
        decoded = binary.Duplicate(ChartRole(reader.string("role")), reader.string("symbol"))
    elif kind is FrameKind.REQUEST:
        decoded = binary.Request(correlation, reader.u16("call"), count, reader.rest())
    elif kind is FrameKind.ERROR:
        decoded = binary.Refusal(reader.string("message"))
    else:
        raise AssertionError(f"the hub sent an EA a {kind.name} frame")
    reader.done()
    return decoded


def arguments_of(request: binary.Request) -> tuple[calls.Call, dict]:
    row = calls.CALLS[calls.Call(request.call)]
    return row.call, calls.decode_arguments(row, request)


def ea_result(call: calls.Call, value, floor: calls.FloorFact | None = None, *, last=True):
    """The count and bytes of an EA's answer part: rows as the EA packs them, and on a history
    read's last part its floor fact."""
    row = calls.CALLS[call]
    if row.shape is calls.Shape.RATES:
        rows = np.array([tuple(r[name] for name in mirror.RATES.names) for r in value], _RATES)
        count, body = len(rows), rows.tobytes()
    elif row.shape is calls.Shape.TICKS:
        rows = np.array([tuple(r[name] for name in mirror.TICKS.names) for r in value], _TICKS)
        count, body = len(rows), rows.tobytes()
    else:
        count, body = calls.write_result(row, value)
    if row.role is ChartRole.HISTORY and last:
        writer = Writer()
        if floor is None:
            writer.u8(0)
        else:
            writer.u8(1)
            writer.i64(floor.floor)
            writer.string(floor.origin)
        body += writer.payload()
    return count, body


def answer(
    request_id: int, call: calls.Call, value, floor=None, *, part=0, last=True, last_error=0
):
    count, body = ea_result(call, value, floor, last=last)
    return binary.Answer(request_id, part, last, last_error, count, body)


def stub_floor(value: int) -> calls.FloorFact:
    return calls.FloorFact(value, FloorOrigin.STUB)


@dataclass
class FakeEa:
    """An EA on a real connection to the hub."""

    ws: object
    chart_role: ChartRole
    symbol: str

    @classmethod
    async def connect(cls, port: int, chart_role: ChartRole, symbol: str) -> "FakeEa":
        ws = await websockets.connect(f"ws://127.0.0.1:{port}")
        await ws.send(encode_ea(binary.EaHello(chart_role, symbol)))
        return cls(ws, chart_role, symbol)

    async def send(self, sent) -> None:
        await self.ws.send(encode_ea(sent))

    async def received(self, timeout=1.0):
        return decode_to_ea(await asyncio.wait_for(self.ws.recv(), timeout=timeout))

    async def request(self, timeout=1.0) -> binary.Request:
        """The next request the hub sends, skipping its wanted frames."""
        while True:
            received = await self.received(timeout)
            if isinstance(received, binary.Request):
                return received

    async def close(self) -> None:
        await self.ws.close()


def zeroed(layout: binary.Layout, **values) -> dict[str, object]:
    """A record of the layout with every member zero, as MQL5 initialises a struct, but `values`."""
    record = {}
    for name in layout.fields:
        member = next(member for member in layout.members if member.name == name)
        if member.declared is None or member.declared is mirror.MqlType.STRING:
            record[name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[name] = 0.0
        elif member.declared is mirror.MqlType.BOOL:
            record[name] = False
        else:
            record[name] = 0
    return record | values
