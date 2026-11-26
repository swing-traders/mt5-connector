"""Verify the authenticated login and execute one minimum-volume market round trip."""

import asyncio
from enum import IntFlag

from mt5connector.wire import calls, mirror

MAGIC = 7_490_001
CALL_SECONDS = 120
SETTLE_SECONDS = 30
ACCEPTED = frozenset(
    (mirror.TRADE_RETCODE_PLACED, mirror.TRADE_RETCODE_DONE, mirror.TRADE_RETCODE_DONE_PARTIAL)
)


class FillCapability(IntFlag):
    FOK = 1
    IOC = 2


class CertificationError(RuntimeError):
    """A release certification condition failed."""


async def _ask(session, call, **arguments):
    return (await asyncio.wait_for(session.deferring(call, **arguments), CALL_SECONDS)).result


async def _close_and_confirm_flat(session, symbol, filling, report):
    for attempt in range(2):
        try:
            orders = await _ask(session, calls.Call.ORDERS_GET)
            for order in orders:
                if order.magic == MAGIC:
                    removed = await _ask(
                        session,
                        calls.Call.ORDER_SEND,
                        request={
                            "action": mirror.TRADE_ACTION_REMOVE,
                            "order": order.ticket,
                            "symbol": order.symbol,
                        },
                    )
                    if removed.retcode not in ACCEPTED:
                        raise CertificationError("certification order removal was rejected")
            positions = await _ask(session, calls.Call.POSITIONS_GET)
            owned = [position for position in positions if position.magic == MAGIC]
            for position in owned:
                if position.symbol != symbol or position.type != mirror.POSITION_TYPE_BUY:
                    raise CertificationError("the certification position has an unexpected shape")
                tick = await _ask(session, calls.Call.SYMBOL_INFO_TICK, symbol=symbol)
                closed = await _ask(
                    session,
                    calls.Call.ORDER_SEND,
                    request={
                        "action": mirror.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "position": position.ticket,
                        "volume": position.volume,
                        "type": mirror.ORDER_TYPE_SELL,
                        "price": tick.bid,
                        "deviation": 50,
                        "type_filling": filling,
                        "magic": MAGIC,
                    },
                )
                if closed.retcode not in ACCEPTED:
                    raise CertificationError("the certification close was rejected")
            async with asyncio.timeout(SETTLE_SECONDS):
                while True:
                    positions = await _ask(session, calls.Call.POSITIONS_GET)
                    orders = await _ask(session, calls.Call.ORDERS_GET)
                    if not positions and not orders:
                        return
                    if any(position.magic != MAGIC for position in positions) or any(
                        order.magic != MAGIC for order in orders
                    ):
                        raise CertificationError("the login is not flat after certification")
                    await asyncio.sleep(0.25)
        except CertificationError:
            raise
        except Exception as failure:
            if attempt == 1:
                raise CertificationError(f"close failed after 2 attempts: {failure}") from failure
            report(
                f"First close attempt failed: {type(failure).__name__}: {failure}; retrying once."
            )


async def certify(session, *, login, server, symbol, report):
    account = await _ask(session, calls.Call.ACCOUNT_INFO)
    if account.login != login or account.server != server:
        raise CertificationError("the authenticated login or server does not match")
    if account.trade_mode != mirror.ACCOUNT_TRADE_MODE_DEMO:
        raise CertificationError("certification requires a demo login")
    if not account.trade_allowed:
        raise CertificationError("the account does not allow trading")
    terminal = await _ask(session, calls.Call.TERMINAL_INFO)
    if not terminal.connected or not terminal.trade_allowed:
        raise CertificationError("the terminal is disconnected or does not allow trading")
    positions = await _ask(session, calls.Call.POSITIONS_GET)
    orders = await _ask(session, calls.Call.ORDERS_GET)
    if positions or orders:
        raise CertificationError("the login is not flat before certification")
    info = await _ask(session, calls.Call.SYMBOL_INFO, symbol=symbol)
    if info.filling_mode & FillCapability.FOK:
        filling = mirror.ORDER_FILLING_FOK
    elif info.filling_mode & FillCapability.IOC:
        filling = mirror.ORDER_FILLING_IOC
    else:
        raise CertificationError("the symbol has no supported market filling mode")
    tick = await _ask(session, calls.Call.SYMBOL_INFO_TICK, symbol=symbol)
    if tick.ask <= 0 or tick.bid <= 0 or info.volume_min <= 0:
        raise CertificationError("the symbol has no quote or minimum volume")
    try:
        sent = await _ask(
            session,
            calls.Call.ORDER_SEND,
            request={
                "action": mirror.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": info.volume_min,
                "type": mirror.ORDER_TYPE_BUY,
                "price": tick.ask,
                "deviation": 50,
                "type_filling": filling,
                "magic": MAGIC,
            },
        )
        if sent.retcode not in ACCEPTED:
            raise CertificationError("the minimum-volume market buy was rejected")
        async with asyncio.timeout(SETTLE_SECONDS):
            while True:
                positions = await _ask(session, calls.Call.POSITIONS_GET)
                owned = [position for position in positions if position.magic == MAGIC]
                if owned:
                    if len(owned) != 1 or owned[0].volume != info.volume_min:
                        raise CertificationError("the filled volume is not the minimum lot")
                    break
                await asyncio.sleep(0.25)
    finally:
        await _close_and_confirm_flat(session, symbol, filling, report)
