"""Concurrent reads whose children share one lifetime."""

import asyncio


async def _fan_out(*coroutines):
    """Return results in input order; on failure, cancel siblings and raise the original error."""
    tasks = []
    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(coroutine) for coroutine in coroutines]
    except ExceptionGroup:
        failed = [task for task in tasks if not task.cancelled() and task.exception() is not None]
        raise failed[0].exception() from None
    return [task.result() for task in tasks]
