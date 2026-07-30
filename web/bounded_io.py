# -*- coding: utf-8 -*-
"""Concurrency-bounded off-loop execution (2026-07-30).

Why this is not just ``async with semaphore: await run.io_bound(...)``
--------------------------------------------------------------------
That obvious form leaks permits under cancellation, which is exactly when these
loaders get cancelled -- a visitor navigating away tears the client down mid
fetch.

``nicegui.run.io_bound`` swallows cancellation::

    except asyncio.CancelledError:
        pass
    return   # -> None

A running thread cannot be cancelled, so on teardown ``io_bound`` returns
immediately while its executor thread carries on with the Supabase request. The
enclosing ``async with`` then exits and releases the permit *while the work is
still in flight*, so replacement visitors are free to start a full quota more.
Cancelling four in-flight loaders can therefore put eight concurrent reads on a
pool the code claims to hold at four.

The fix is to stop tying the permit to the caller's lifetime and tie it to the
WORKER's instead: release from the future's done-callback. The permit is then
held for exactly as long as a thread is occupied, whatever happens to whoever
started it.

We submit to ``nicegui.run.thread_pool`` on purpose -- that shared pool is the
resource being bounded, so using a private pool here would defeat the point.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, Optional

from nicegui import run as nicegui_run

logger = logging.getLogger(__name__)


async def bounded_io_bound(semaphore: asyncio.Semaphore,
                           fn: Callable[..., Any],
                           *args: Any,
                           **kwargs: Any) -> Optional[Any]:
    """Run ``fn`` in NiceGUI's shared thread pool, holding one permit.

    The permit is acquired before submitting and released when the worker
    actually finishes -- NOT when this coroutine returns. Cancelling the caller
    propagates ``CancelledError`` (unlike ``run.io_bound``, which swallows it)
    while the permit stays held until the thread is genuinely done.

    Returns ``None`` if the executor is shutting down, matching
    ``run.io_bound``'s contract so existing ``if result is None: return`` guards
    keep working.
    """
    await semaphore.acquire()

    try:
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(
            nicegui_run.thread_pool, functools.partial(fn, *args, **kwargs)
        )
    except RuntimeError as e:
        # Nothing was submitted, so nothing will release the permit for us.
        semaphore.release()
        if 'cannot schedule new futures after shutdown' not in str(e):
            raise
        logger.debug('bounded_io_bound: executor shutting down, skipping %r', getattr(fn, '__name__', fn))
        return None
    except BaseException:
        semaphore.release()
        raise

    # From here the done-callback owns the permit. It fires exactly once, on
    # success, exception OR cancellation of the future, so the permit cannot be
    # stranded -- and it is NOT released early just because we stop awaiting.
    future.add_done_callback(lambda _: semaphore.release())

    # shield() so cancelling the caller does not try to cancel a future that a
    # running thread cannot honour anyway; the worker finishes and releases.
    return await asyncio.shield(future)
