"""Tick relay hub: the MQL5 EA publishes ticks, adapters subscribe, and the EA's trade-server time
goes on to the HTTP server."""

import asyncio
import http.client
import json
import logging
import os
import urllib.request
from collections.abc import Callable
from enum import StrEnum

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedError

TICK_FLAG_BID = 2
TICK_FLAG_ASK = 4
SERVER_TIME_RELAY_PATH = "/relay/server_time"
_RELAY_TIMEOUT_S = 5

logger = logging.getLogger(__name__)


class FrameType(StrEnum):
    """The `type` of a frame the EA sends; its tick frames carry none."""

    HELLO = "hello"
    SERVER_TIME = "server_time"


class TickHub:
    def __init__(self, relay: Callable[[dict], None]):
        self._relay = relay
        self._eas = set()
        self._adapters = set()
        self._symbols = set()

    async def handler(self, ws):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                kind = msg.get("type")
                if kind == FrameType.HELLO and msg.get("role") == "ea":
                    self._eas.add(ws)
                    logger.info("ea connected")
                elif kind == FrameType.HELLO and msg.get("role") == "adapter":
                    self._adapters.add(ws)
                    logger.info("adapter connected")
                elif kind == "subscribe":
                    self._symbols.update(msg.get("symbols", []))
                    logger.info("subscribe")
                elif kind == "unsubscribe":
                    self._symbols.difference_update(msg.get("symbols", []))
                    logger.info("unsubscribe")
                elif kind == FrameType.SERVER_TIME and ws in self._eas:
                    # Run off the loop and not awaited, so a slow server never holds back the ticks.
                    asyncio.get_running_loop().run_in_executor(None, self._relay, msg)
                elif self._is_tick(msg):
                    flags = int(msg.get("flags"))
                    # we only need to send the ticks if the bid or the ask price has changed
                    if bool(flags & TICK_FLAG_BID) or bool(flags & TICK_FLAG_ASK):
                        await self._broadcast(raw)
                else:
                    logger.info(f"unknown kind {kind}")
        except ConnectionClosedError as closed:
            logger.info("connection %s closed: %s", ws.remote_address, closed)
        except Exception:
            logger.warning(
                "closing connection %s on a frame the hub cannot handle",
                ws.remote_address,
                exc_info=True,
            )
        finally:
            self._adapters.discard(ws)
            self._eas.discard(ws)

    @staticmethod
    def _is_tick(msg: dict) -> bool:
        return "symbol" in msg and ("bid" in msg or "ask" in msg or "time_msec" in msg)

    async def _broadcast(self, raw):
        for a in list(self._adapters):
            try:
                await a.send(raw)
            except Exception:
                self._adapters.discard(a)


def post_to(url: str) -> Callable[[dict], None]:
    """A relay that POSTs each frame to the URL as JSON and logs a failed post as a warning."""
    # No proxy: the server refuses a relayed frame that does not arrive over loopback.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post(frame: dict) -> None:
        request = urllib.request.Request(
            url,
            data=json.dumps(frame).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with opener.open(request, timeout=_RELAY_TIMEOUT_S):
                pass
        except (OSError, http.client.HTTPException) as failure:
            logger.warning("%s relay to %s failed: %s", FrameType.SERVER_TIME, url, failure)

    return post


async def main():
    port = int(os.environ.get("WS_PORT", "9000"))
    api_port = int(os.environ.get("MT5_API_PORT", "5000"))
    hub = TickHub(post_to(f"http://127.0.0.1:{api_port}{SERVER_TIME_RELAY_PATH}"))
    async with serve(hub.handler, "0.0.0.0", port):
        await asyncio.Future()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting WSHUB")
    asyncio.run(main())
