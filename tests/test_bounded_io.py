# -*- coding: utf-8 -*-
"""Tests for web/bounded_io.py -- permit accounting under cancellation.

The bug this module exists to prevent (Codex PR #319 review):

    async with semaphore:
        await run.io_bound(blocking_call)

`nicegui.run.io_bound` swallows CancelledError (`except asyncio.CancelledError:
pass`) and returns, but a running thread cannot be cancelled, so the blocking
call carries on. The `async with` then releases the permit while the work is
still in flight, and replacement callers can start a fresh quota -- cancelling
four in-flight loaders yields eight concurrent reads against a "bound" of four.

`bounded_io_bound` ties the permit to the WORKER (a future done-callback) rather
than to the caller's stack, so it is held for exactly as long as a thread is busy.
"""

import asyncio
import threading
import time as real_time

import pytest

from web.bounded_io import bounded_io_bound


def test_returns_the_workers_value():
    async def main():
        sem = asyncio.Semaphore(2)
        return await bounded_io_bound(sem, lambda x: x * 2, 21)

    assert asyncio.run(main()) == 42


def test_permit_released_on_success():
    async def main():
        sem = asyncio.Semaphore(1)
        await bounded_io_bound(sem, lambda: 'ok')
        return sem._value

    assert asyncio.run(main()) == 1


def test_permit_released_when_the_worker_raises():
    def boom():
        raise ValueError('worker failed')

    async def main():
        sem = asyncio.Semaphore(1)
        with pytest.raises(ValueError, match='worker failed'):
            await bounded_io_bound(sem, boom)
        return sem._value

    assert asyncio.run(main()) == 1, 'permit stranded after a worker exception'


def test_cancelling_the_caller_keeps_the_permit_until_the_worker_finishes():
    """THE regression test.

    Cancel the awaiting task while the worker is mid-flight. The permit must NOT
    come back yet -- if it does, a replacement caller can double the real
    concurrency while the original thread is still working.
    """
    started = threading.Event()
    finished = threading.Event()

    def slow():
        started.set()
        real_time.sleep(0.4)
        finished.set()
        return 'done'

    async def main():
        sem = asyncio.Semaphore(1)
        task = asyncio.create_task(bounded_io_bound(sem, slow))

        await asyncio.get_running_loop().run_in_executor(None, started.wait, 2.0)
        assert sem._value == 0, 'permit was never taken'

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Cancellation propagated (unlike run.io_bound, which swallows it) and the
        # worker is STILL running, so the permit must still be held.
        permit_right_after_cancel = sem._value
        assert not finished.is_set(), 'worker finished too early to prove anything'

        # Once the thread genuinely completes, the done-callback returns it.
        await asyncio.get_running_loop().run_in_executor(None, finished.wait, 3.0)
        await asyncio.sleep(0.05)  # let the callback run on the loop
        return permit_right_after_cancel, sem._value

    right_after_cancel, eventually = asyncio.run(main())
    assert right_after_cancel == 0, (
        'permit was released while the worker was still running -- replacement '
        'callers could exceed the bound'
    )
    assert eventually == 1, 'permit was never returned after the worker finished'


def test_bound_holds_when_callers_are_cancelled_and_replaced():
    """End-to-end: cancel the in-flight holders, start replacements, watch the peak.

    With the permit tied to the caller this reaches 2x the bound; tied to the
    worker it cannot.
    """
    bound = 2
    lock = threading.Lock()
    stats = {'in_flight': 0, 'peak': 0}

    def work():
        with lock:
            stats['in_flight'] += 1
            stats['peak'] = max(stats['peak'], stats['in_flight'])
        real_time.sleep(0.25)
        with lock:
            stats['in_flight'] -= 1
        return 1

    async def main():
        sem = asyncio.Semaphore(bound)
        first = [asyncio.create_task(bounded_io_bound(sem, work)) for _ in range(bound)]
        await asyncio.sleep(0.08)  # let them occupy the pool

        for t in first:
            t.cancel()
        await asyncio.gather(*first, return_exceptions=True)

        # Replacements try to start immediately, while the originals' threads run.
        second = [asyncio.create_task(bounded_io_bound(sem, work)) for _ in range(bound)]
        await asyncio.gather(*second, return_exceptions=True)

    asyncio.run(main())
    assert stats['peak'] <= bound, (
        f"peak {stats['peak']} exceeded the bound {bound} -- cancelled callers "
        'released their permits while their workers were still running'
    )


def test_shutdown_releases_the_permit_and_returns_none(monkeypatch):
    """If the executor is gone, nothing will run the done-callback for us."""
    import web.bounded_io as bio

    class _DeadLoopProxy:
        def run_in_executor(self, *a, **k):
            raise RuntimeError('cannot schedule new futures after shutdown')

    monkeypatch.setattr(bio.asyncio, 'get_running_loop', lambda: _DeadLoopProxy())

    async def main():
        sem = asyncio.Semaphore(1)
        result = await bounded_io_bound(sem, lambda: 'never runs')
        return result, sem._value

    result, permits = asyncio.run(main())
    assert result is None, 'shutdown should mirror run.io_bound and return None'
    assert permits == 1, 'permit stranded on executor shutdown'


def test_unexpected_runtime_error_propagates_and_releases(monkeypatch):
    import web.bounded_io as bio

    class _BadLoopProxy:
        def run_in_executor(self, *a, **k):
            raise RuntimeError('something else entirely')

    monkeypatch.setattr(bio.asyncio, 'get_running_loop', lambda: _BadLoopProxy())

    async def main():
        sem = asyncio.Semaphore(1)
        with pytest.raises(RuntimeError, match='something else entirely'):
            await bounded_io_bound(sem, lambda: None)
        return sem._value

    assert asyncio.run(main()) == 1, 'permit stranded on an unexpected RuntimeError'
