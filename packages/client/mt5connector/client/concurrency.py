"""Concurrent reads whose children share one lifetime."""

import asyncio


async def _fan_out(*coroutines):
    """Return results in input order; on failure, cancel siblings and raise the original error."""
    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(coroutine) for coroutine in coroutines]
    except ExceptionGroup as failure:
        raise failure.exceptions[0] from None
    return [task.result() for task in tasks]
