# -*- coding: utf-8 -*-
"""Bounded replacement for ``asyncio.run()`` in the render-smoke harness.

Why this exists (PR #410, windows-latest): a render-smoke test printed ``F``
and then the job sat ~29 minutes until it was cancelled, never printing the
failure. ``asyncio.run()`` ends by cancelling every task still on the loop
and gathering them WITHOUT a timeout (``asyncio.runners._cancel_all_tasks``).
A page leaves such tasks behind -- the browse page's notes/joins/version
loaders, ``run.io_bound`` workers -- and NiceGUI's ``run._run`` swallows the
``CancelledError`` (``except asyncio.CancelledError: pass``) and returns
``None``, so a cancelled loader carries on and can await again. One such task
holds the process forever; whether it does is timing, so it shows on slow
Windows runners and not on Linux.

``run_bounded(coro)`` runs the coroutine on a fresh loop like ``asyncio.run``
but every wait on the way out is bounded: leftover tasks are cancelled and
waited for at most ``teardown_timeout`` seconds, then abandoned (named in a
warning, or in a note on the test's own exception). The coroutine's own
outcome -- its exception above all -- is always what the caller sees.

``run_user_driver(context_factory, driver)`` is the User-simulation form:
the driver runs under ``driver_timeout``, and if it raises, that exception is
raised once the context's teardown has had ``teardown_timeout`` seconds,
whether or not the teardown finished.
"""

from __future__ import annotations

import asyncio
import io
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncContextManager, Awaitable, Callable, Coroutine

# Generous: the slowest driver in this package runs in well under 30 s on CI.
DRIVER_TIMEOUT = 180.0
# NiceGUI's own background_tasks.teardown() waits 2 s per round; a clean
# teardown is far faster than this.
TEARDOWN_TIMEOUT = 15.0
# How long a cancelled task gets to finish before it is abandoned.
_CANCEL_GRACE = 2.0


def _describe(tasks) -> str:
    parts = []
    for task in tasks:
        buf = io.StringIO()
        task.print_stack(limit=8, file=buf)
        parts.append(f'{task.get_name()}:\n{buf.getvalue().rstrip()}')
    return '\n'.join(parts)


async def _wait_bounded(task: asyncio.Task, timeout: float) -> tuple[bool, str]:
    """Wait for ``task`` at most ``timeout`` s; on expiry cancel it and give it
    ``_CANCEL_GRACE`` s more. Never raises.

    Return ``(finished, expired_at)``: whether the task is done, and -- when
    the timeout expired -- where it was waiting at that moment ('' if it
    finished in time). A task that stops when cancelled has *finished*, but
    it timed out, and the caller must say so rather than report the cancel.
    """
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if done:
        return True, ''
    expired_at = _describe([task])
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=_CANCEL_GRACE)
    return bool(done), expired_at


def _cleanup_loop(loop: asyncio.AbstractEventLoop, teardown_timeout: float) -> list:
    """Cancel what is left on ``loop`` and wait for it, bounded. Return the
    tasks that did not finish (they are abandoned with the loop).

    Cancelling repeats until the loop is empty, because finishing one task
    can start another: NiceGUI's ``create_lazy`` starts the queued
    user-storage write from the done-callback of the one before it.
    """
    deadline = loop.time() + teardown_timeout
    cancelled: set = set()
    seen: set = set()
    stuck: list = []
    while True:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        seen.update(pending)
        if not pending:
            break
        for task in pending:
            if task not in cancelled:
                task.cancel()
                cancelled.add(task)
        remaining = deadline - loop.time()
        if remaining <= 0:
            stuck = pending
            break
        loop.run_until_complete(asyncio.wait(pending, timeout=remaining))
    for task in seen:
        if task.done() and not task.cancelled():
            task.exception()  # mark retrieved: no "never retrieved" noise
    try:
        loop.run_until_complete(asyncio.wait_for(loop.shutdown_asyncgens(), _CANCEL_GRACE))
    except (asyncio.TimeoutError, RuntimeError):
        pass
    # Not loop.shutdown_default_executor(): on 3.11 it ends in an untimed
    # thread.join(), so a blocked run_in_executor worker would hold the run
    # past every bound here (Codex, PR #413). run_bounded installs its own
    # default executor and shuts it down without waiting instead.
    return stuck


def _forget_nicegui_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """Drop ``loop``'s tasks from NiceGUI's process-wide task registries.

    ``background_tasks.running_tasks`` and ``lazy_tasks_running`` outlive the
    loop. A task abandoned there makes every LATER lifespan teardown fail:
    ``cancel()`` raises "Event loop is closed", or ``gather()`` raises because
    the task belongs to another loop and ``teardown()`` retries it forever.
    """
    try:
        from nicegui import background_tasks as bt
    except ImportError:
        return
    for task in [t for t in bt.running_tasks if t.get_loop() is loop]:
        bt.running_tasks.discard(task)
    for name, task in list(bt.lazy_tasks_running.items()):
        if task.get_loop() is loop:
            bt.lazy_tasks_running.pop(name, None)
            waiting = bt.lazy_coroutines_waiting.pop(name, None)
            if waiting is not None:
                waiting.close()


def run_bounded(
    coro: Coroutine[Any, Any, Any],
    *,
    timeout: float | None = None,
    teardown_timeout: float = TEARDOWN_TIMEOUT,
) -> Any:
    """``asyncio.run(coro)`` with every wait bounded (see module docstring).

    ``timeout`` bounds ``coro`` itself (default: driver + teardown budget).
    """
    if timeout is None:
        timeout = DRIVER_TIMEOUT + teardown_timeout
    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(thread_name_prefix='render-smoke')
    loop.set_default_executor(executor)
    stuck: list = []
    try:
        asyncio.set_event_loop(loop)
        main = loop.create_task(coro, name='render-smoke main')
        finished, expired_at = loop.run_until_complete(_wait_bounded(main, timeout))
        stuck = _cleanup_loop(loop, teardown_timeout)
        if expired_at:
            raise TimeoutError(
                f'render-smoke: coroutine did not finish within {timeout:.0f} s'
                f'{"" if finished else " (and did not stop when cancelled)"}; '
                f'it was waiting here:\n{expired_at}'
            )
        if main.cancelled():
            raise asyncio.CancelledError()
        exc = main.exception()
        if exc is not None:
            if stuck:
                exc.add_note(
                    f'render-smoke: {len(stuck)} leftover task(s) did not stop within '
                    f'{teardown_timeout:.0f} s of cancellation and were abandoned:\n'
                    f'{_describe(stuck)}'
                )
            raise exc
        if stuck:
            warnings.warn(
                f'render-smoke: {len(stuck)} leftover task(s) did not stop within '
                f'{teardown_timeout:.0f} s of cancellation and were abandoned:\n'
                f'{_describe(stuck)}',
                RuntimeWarning,
                stacklevel=2,
            )
        return main.result()
    finally:
        _forget_nicegui_tasks(loop)
        # A worker still running is abandoned, not joined (see _cleanup_loop).
        executor.shutdown(wait=False, cancel_futures=True)
        asyncio.set_event_loop(None)
        loop.close()


def run_user_driver(
    context_factory: Callable[[], AsyncContextManager[Any]],
    driver: Callable[[Any], Awaitable[Any]],
    *,
    driver_timeout: float = DRIVER_TIMEOUT,
    teardown_timeout: float = TEARDOWN_TIMEOUT,
) -> None:
    """Enter ``context_factory()``, run ``driver(user)`` inside it, leave it --
    with the driver bounded by ``driver_timeout`` and everything after the
    driver (context exit + loop cleanup) bounded by ``teardown_timeout``.

    The driver's own exception wins: it is raised even if the teardown that
    follows it hangs or raises.
    """
    outcome: dict[str, BaseException] = {}

    async def _main() -> None:
        driver_done = asyncio.Event()

        async def _body() -> None:
            async with context_factory() as user:
                task = asyncio.ensure_future(driver(user))
                try:
                    _finished, expired_at = await _wait_bounded(task, driver_timeout)
                    if expired_at or task.cancelled():
                        outcome['error'] = TimeoutError(
                            f'render-smoke: driver did not finish within '
                            f'{driver_timeout:.0f} s; it was waiting here:\n'
                            f'{expired_at or _describe([task])}'
                        )
                    elif task.exception() is not None:
                        outcome['error'] = task.exception()
                finally:
                    driver_done.set()
                if 'error' in outcome:
                    raise outcome['error']

        body = asyncio.ensure_future(_body())
        waiter = asyncio.ensure_future(driver_done.wait())
        await asyncio.wait({body, waiter}, return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        if not body.done() and not (await _wait_bounded(body, teardown_timeout))[0]:
            if 'error' not in outcome:
                raise TimeoutError(
                    f'render-smoke: teardown did not finish within '
                    f'{teardown_timeout:.0f} s:\n{_describe([body])}'
                )
            outcome['error'].add_note(
                f'render-smoke: teardown did not finish within {teardown_timeout:.0f} s '
                f'after this failure and was abandoned.'
            )
        if body.done() and not body.cancelled():
            exc = body.exception()
            if exc is not None and exc is not outcome.get('error'):
                if 'error' in outcome:
                    # The teardown raised too; the driver's failure is the story.
                    outcome['error'].add_note(f'render-smoke: teardown also raised {exc!r}')
                else:
                    raise exc
        if 'error' in outcome:
            raise outcome['error']  # via run_bounded, which notes stuck leftovers

    try:
        run_bounded(
            _main(),
            # Setup + driver + teardown, plus slack for the cancel grace periods.
            timeout=driver_timeout + teardown_timeout + 4 * _CANCEL_GRACE + 60.0,
            teardown_timeout=teardown_timeout,
        )
    except BaseException as exc:
        error = outcome.get('error')
        if error is None or exc is error:
            raise
        error.add_note(f'render-smoke: then the run failed with {exc!r}')
        raise error from None
