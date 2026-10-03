"""The remote shim's transport against a stand-in HTTP server: what it sends, and how it tells a
server it cannot use from a package failure."""

import calendar
import json
import socket
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import mt5connect.remote_mt5 as rmt5
from mt5connect import errors
from mt5connect.errors import MT5ConfigError, MT5ConnectionError, ServerBusy, ServerUnreachable

JSON = "application/json"


def _answer(result, last_error=(1, "Success")) -> dict:
    return {"ok": True, "result": result, "last_error": list(last_error)}


def _failure(code: int, message: str) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.stand_in.received.append((self.path, json.loads(body), self.client_address))
        if self.server.stand_in.drop:
            # The request arrived; the connection closes before any reply.
            self.close_connection = True
            return
        status, content_type, payload, delay = self.server.stand_in.answer
        time.sleep(delay)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class _StandIn:
    """The requests a stand-in server received and the answer it gives next."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.received = []
        self.answer = (200, JSON, json.dumps(_answer(None)).encode(), 0.0)
        self.drop = False

    def reply(self, status: int, body, content_type: str = JSON, delay: float = 0.0) -> None:
        payload = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.answer = (status, content_type, payload, delay)


@pytest.fixture
def stand_in(monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.daemon_threads = True
    server.stand_in = _StandIn(f"http://127.0.0.1:{server.server_address[1]}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(rmt5, "_session", None)
    monkeypatch.setattr(rmt5, "_server_url", None)
    monkeypatch.setattr(rmt5, "_ws_url", None)
    monkeypatch.setattr(rmt5, "_last_error", rmt5._last_error)
    rmt5.configure(server.stand_in.url)
    yield server.stand_in
    rmt5._session.close()
    server.shutdown()
    server.server_close()


def test_call_posts_its_parameters_by_name_to_its_route(stand_in):
    stand_in.reply(200, _answer([]))
    assert rmt5.positions_get(symbol="EURUSD") == ()
    path, body, _ = stand_in.received[0]
    assert path == "/mt5/positions_get"
    assert body == {"symbol": "EURUSD"}


def test_order_send_posts_the_request_as_its_parameter(stand_in):
    stand_in.reply(200, _failure(-2, "Invalid arguments"))
    request = {"action": 1, "symbol": "EURUSD", "volume": 0.1}
    rmt5.order_send(request)
    assert stand_in.received[0][:2] == ("/mt5/order_send", {"request": request})


def test_datetime_arguments_travel_as_epoch_seconds(stand_in):
    stand_in.reply(200, _answer([]))
    rmt5.copy_rates_range(
        "EURUSD", 16385, datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)
    )
    assert stand_in.received[0][1] == {
        "symbol": "EURUSD",
        "timeframe": 16385,
        "date_from": calendar.timegm((2024, 1, 1, 0, 0, 0)),
        "date_to": calendar.timegm((2024, 1, 2, 0, 0, 0)),
    }


def test_calls_share_one_kept_alive_connection(stand_in):
    stand_in.reply(200, _answer(0))
    rmt5.positions_total()
    rmt5.orders_total()
    first, second = (client for _, _, client in stand_in.received)
    assert first == second


def test_signatures_are_the_packages():
    with pytest.raises(TypeError):
        rmt5.symbol_info(symbol="EURUSD")
    with pytest.raises(TypeError):
        rmt5.copy_rates_range("EURUSD", 16385)


def test_refused_connection_raises_server_unreachable(monkeypatch):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    monkeypatch.setattr(rmt5, "_session", None)
    monkeypatch.setattr(rmt5, "_server_url", None)
    rmt5.configure(f"http://127.0.0.1:{port}")
    with pytest.raises(ServerUnreachable) as refused:
        rmt5.order_send({"action": 1})
    assert not isinstance(refused.value, errors.ResponseLost)
    assert issubclass(ServerUnreachable, MT5ConnectionError)
    rmt5._session.close()


def test_html_server_error_raises_response_lost(stand_in):
    stand_in.reply(500, "<html><body>Internal Server Error</body></html>", content_type="text/html")
    with pytest.raises(errors.ResponseLost, match="HTTP 500"):
        rmt5.order_send({"action": 1})
    assert issubclass(errors.ResponseLost, ServerUnreachable)


def test_non_json_answer_raises_response_lost(stand_in):
    stand_in.reply(200, "not json", content_type="text/plain")
    with pytest.raises(errors.ResponseLost, match="not JSON"):
        rmt5.order_send({"action": 1})


def test_a_connection_closed_after_the_request_arrived_raises_response_lost(stand_in):
    stand_in.drop = True
    with pytest.raises(errors.ResponseLost, match="order_send"):
        rmt5.order_send({"action": 1})
    assert stand_in.received[0][:2] == ("/mt5/order_send", {"request": {"action": 1}})


def test_the_unverified_clock_gate_raises_server_unreachable_not_a_lost_response(stand_in):
    message = "the broker clock is not verified"
    stand_in.reply(503, {"ok": False, "error": {"code": -1, "message": message}})
    with pytest.raises(ServerUnreachable, match="server not ready") as refused:
        rmt5.order_send({"action": 1})
    assert not isinstance(refused.value, errors.ResponseLost)


def test_a_busy_server_raises_server_busy_not_a_lost_response(stand_in):
    stand_in.reply(503, _failure(-20_002, "/mt5/order_send: the server is busy"))
    with pytest.raises(ServerBusy):
        rmt5.order_send({"action": 1})


@pytest.mark.parametrize("status", [200, 400, 503])
@pytest.mark.parametrize(
    "last_error",
    [[1, "Success"], [-10005, "IPC send failed"], [-10005.0, "IPC timeout"]],
    ids=["other-code", "other-message", "float-code"],
)
def test_failure_whose_error_is_not_its_last_error_raises_server_unreachable(
    stand_in, status, last_error
):
    error = {"code": -10005, "message": "IPC timeout"}
    stand_in.reply(status, {"ok": False, "error": error, "last_error": last_error})
    with pytest.raises(ServerUnreachable, match="not an envelope"):
        rmt5.positions_get()


@pytest.mark.parametrize(
    "body",
    [
        {"status": "healthy"},
        {"ok": True, "result": True},
        {"ok": True, "result": True, "last_error": [1]},
        {"ok": False, "error": {"code": -1, "message": "Terminal: Call failed"}},
    ],
    ids=[
        "no-envelope",
        "answer-without-last-error",
        "short-last-error",
        "failure-without-last-error",
    ],
)
def test_json_that_is_not_an_envelope_raises_response_lost(stand_in, body):
    stand_in.reply(200, body)
    with pytest.raises(errors.ResponseLost, match="not an envelope"):
        rmt5.initialize()


def test_every_answer_sets_last_error(stand_in):
    stand_in.reply(200, _failure(-10005, "IPC timeout"))
    assert rmt5.positions_total() is None
    stand_in.reply(200, _answer(3, last_error=(1, "Success")))
    assert rmt5.positions_total() == 3
    assert rmt5.last_error() == (1, "Success")


def test_answer_outside_a_200_raises_server_unreachable(stand_in):
    stand_in.reply(400, _answer(True))
    with pytest.raises(ServerUnreachable):
        rmt5.initialize()


def test_struct_with_fields_that_are_not_the_packages_raises_server_unreachable(stand_in):
    stand_in.reply(200, _answer({"bid": 1.08, "ask": 1.0801}))
    with pytest.raises(ServerUnreachable, match="Tick"):
        rmt5.symbol_info_tick("EURUSD")


TICK_ROW = {
    "time": 1704067200,
    "bid": 1.1,
    "ask": 1.2,
    "last": 0.0,
    "volume": 0,
    "time_msc": 1704067200123,
    "flags": 6,
    "volume_real": 0.0,
}


@pytest.mark.parametrize(
    "row",
    [
        TICK_ROW | {"bid": None},
        TICK_ROW | {"bid": 10**400},
        TICK_ROW | {"time": 1704067200.5},
        TICK_ROW | {"flags": "6"},
        TICK_ROW | {"volume": True},
    ],
    ids=["null-float", "int-beyond-float", "float-in-int", "string-in-int", "bool-in-int"],
)
def test_array_value_outside_its_dtype_raises_server_unreachable(stand_in, row):
    stand_in.reply(200, _answer([row]))
    with pytest.raises(ServerUnreachable, match="ticks"):
        rmt5.copy_ticks_range("EURUSD", 1704067200, 1704070800, -1)


@pytest.mark.parametrize(
    ("function", "result"),
    [
        ("positions_get", None),
        ("positions_get", {"ticket": 1}),
        ("positions_total", None),
        ("positions_total", "3"),
        ("positions_total", True),
        ("initialize", 1),
        ("version", "500"),
        ("shutdown", True),
    ],
    ids=[
        "structs-null",
        "structs-object",
        "scalar-null",
        "scalar-string",
        "scalar-bool",
        "bool-int",
        "tuple-string",
        "none-bool",
    ],
)
def test_result_outside_its_kind_raises_response_lost(stand_in, function, result):
    stand_in.reply(200, _answer(result))
    with pytest.raises(errors.ResponseLost, match=function):
        getattr(rmt5, function)()


def test_an_order_send_result_outside_its_struct_raises_response_lost(stand_in):
    stand_in.reply(200, _answer({"retcode": 10009}))
    with pytest.raises(errors.ResponseLost, match="OrderSendResult"):
        rmt5.order_send({"action": 1})


def test_read_timeout_raises_response_lost(stand_in, monkeypatch):
    monkeypatch.setattr(rmt5, "READ_TIMEOUT_S", 0.2)
    stand_in.reply(200, _answer(None), delay=0.6)
    with pytest.raises(errors.ResponseLost):
        rmt5.order_send({"action": 1})


def test_login_timeout_reaches_the_terminal_and_extends_the_read_timeout(stand_in, monkeypatch):
    monkeypatch.setattr(rmt5, "READ_TIMEOUT_S", 0.2)
    stand_in.reply(200, _answer(True), delay=0.6)
    assert rmt5.login(12345678, password="pw", server="example-server", timeout=1000) is True
    assert stand_in.received[0][:2] == (
        "/mt5/login",
        {"login": 12345678, "password": "pw", "server": "example-server", "timeout": 1000},
    )


def test_package_failure_answers_the_failure_value_and_sets_last_error(stand_in):
    stand_in.reply(200, _failure(-10005, "IPC timeout"))
    assert rmt5.positions_get() is None
    assert rmt5.last_error() == (-10005, "IPC timeout")
    stand_in.reply(200, _failure(-6, "Authorization failed"))
    assert rmt5.login(12345678, password="pw", server="example-server") is False
    assert rmt5.last_error() == (-6, "Authorization failed")


def test_parameter_refusal_answers_the_failure_value_and_sets_last_error(stand_in):
    stand_in.reply(400, _failure(-2, "missing parameter: symbol"))
    assert rmt5.symbol_info("EURUSD") is None
    assert rmt5.last_error() == (-2, "missing parameter: symbol")


def test_unavailable_terminal_answers_the_failure_value_and_sets_last_error(stand_in):
    stand_in.reply(503, _failure(-10004, "No IPC connection"))
    assert rmt5.terminal_info() is None
    assert rmt5.last_error() == (-10004, "No IPC connection")


def test_unconfigured_shim_refuses_to_call(monkeypatch):
    monkeypatch.setattr(rmt5, "_session", None)
    with pytest.raises(MT5ConfigError):
        rmt5.positions_total()


def test_constants_are_the_packages():
    assert rmt5.ORDER_TYPE_BUY == 0
    assert rmt5.ORDER_FILLING_IOC == 1
    assert rmt5.TRADE_ACTION_DEAL == 1
    assert rmt5.TRADE_RETCODE_DONE == 10009
    assert rmt5.TIMEFRAME_H1 == 16385
    assert rmt5.COPY_TICKS_ALL == -1


def test_tick_struct_attribute_access():
    t = rmt5.Tick(
        time=123, bid=1.0, ask=1.1, last=0.0, volume=5, time_msc=123000, flags=2, volume_real=5.0
    )
    assert t.bid == 1.0 and t.ask == 1.1 and t.time_msc == 123000
    assert t._asdict()["bid"] == 1.0


def test_tick_from_ws_converts_new_format():
    tick = rmt5.tick_from_ws(
        {
            "symbol": "EURUSD",
            "time": "2024.06.01 10:00:00",
            "ask": "1.0854",
            "bid": "1.0852",
            "volume": "0",
            "last": "0.0",
            "time_msec": "1712345678123",
            "flags": "2",
        }
    )
    assert tick.time == 1712345678  # time_msec // 1000
    assert tick.time_msc == 1712345678123
    assert tick.bid == 1.0852 and tick.ask == 1.0854
    assert tick.last == 0.0 and tick.volume == 0 and tick.flags == 2


def test_tick_from_ws_missing_fields_default():
    tick = rmt5.tick_from_ws({"bid": "1.1"})
    assert tick.bid == 1.1 and tick.ask == 0.0
    assert tick.time == 0 and tick.time_msc == 0
    assert tick.volume == 0 and tick.flags == 0 and tick.volume_real == 0.0
