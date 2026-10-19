# -*- coding: utf-8 -*-
"""The render-smoke harness fails promptly when a driver fails (PR #410).

On windows-latest a failed assertion in ``test_round5_triage_restored_on_reopen``
printed ``F`` and then hung ~29 minutes at ``asyncio.run()``'s exit, because a
task the page left behind did not finish when cancelled. These tests rebuild
that situation deliberately -- with NiceGUI's own ``run.io_bound``, whose
``_run`` swallows ``CancelledError`` -- and require the run to end in seconds
with the driver's assertion as the error raised.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import pytest

from tests.render_smoke._bounded_run import run_bounded, run_user_driver
from tests.render_smoke.conftest import run_joins_lab_smoke

# Small budgets so a regression costs seconds, not the CI job.
_TEARDOWN = 1.0
# Bound for "promptly": teardown budget + cancel graces + loop cleanup, with slack.
_PROMPT = 12.0


async def _swallows_cancel_then_waits() -> None:
    """The shape of a page loader that outlives its test: cancellation is
    eaten once (as nicegui.run._run does) and the task waits again."""
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(3600)


def test_run_bounded_raises_the_coroutine_error_despite_a_stuck_task():
    async def main():
        asyncio.get_running_loop().create_task(_swallows_cancel_then_waits())
        await asyncio.sleep(0)
        assert False, 'DELIBERATE driver failure'

    started = time.monotonic()
    with pytest.raises(AssertionError, match='DELIBERATE') as info:
        run_bounded(main(), teardown_timeout=_TEARDOWN)
    assert time.monotonic() - started < _PROMPT
    assert any('leftover task' in n for n in getattr(info.value, '__notes__', []))


def test_run_bounded_does_not_wait_for_a_blocked_default_executor_worker():
    """asyncio's shutdown_default_executor() joins its shutdown thread with no
    timeout on 3.11, so a blocked worker would hold the run past every bound."""
    async def main():
        asyncio.get_running_loop().run_in_executor(None, time.sleep, 20)
        await asyncio.sleep(0.05)
        assert False, 'DELIBERATE driver failure'

    started = time.monotonic()
    with pytest.raises(AssertionError, match='DELIBERATE'):
        run_bounded(main(), teardown_timeout=_TEARDOWN)
    assert time.monotonic() - started < _PROMPT


def test_a_task_started_during_cleanup_is_cancelled_and_nicegui_forgets_the_loop(monkeypatch):
    """NiceGUI's create_lazy starts the queued storage write from the previous
    write's done-callback, i.e. AFTER the first cancel pass. It must still be
    cancelled, and no task of the closed loop may stay in NiceGUI's
    process-wide registries -- a later test's lifespan teardown would trip
    over it ("Event loop is closed", or a gather() retried forever)."""
    from nicegui import background_tasks as bt, core

    async def write():
        await asyncio.sleep(3600)

    async def main():
        monkeypatch.setattr(core, 'loop', asyncio.get_running_loop())
        bt.create_lazy(write(), name='storage-user-test')
        bt.create_lazy(write(), name='storage-user-test')  # queued behind the first
        await asyncio.sleep(0)
        return asyncio.get_running_loop()

    loop = run_bounded(main(), teardown_timeout=_TEARDOWN)
    left = [t for t in bt.running_tasks | set(bt.lazy_tasks_running.values())
            if t.get_loop() is loop]
    assert not left, left
    assert 'storage-user-test' not in bt.lazy_coroutines_waiting


def test_run_bounded_warns_but_passes_when_only_a_leftover_is_stuck():
    async def main():
        asyncio.get_running_loop().create_task(_swallows_cancel_then_waits())
        await asyncio.sleep(0)
        return 'ok'

    with pytest.warns(RuntimeWarning, match='leftover task'):
        assert run_bounded(main(), teardown_timeout=_TEARDOWN) == 'ok'


def test_driver_error_wins_over_a_teardown_that_never_ends():
    @asynccontextmanager
    async def context():
        try:
            yield object()
        finally:
            # A teardown that ignores cancellation, like a lifespan shutdown
            # waiting on a task that will not die.
            await _swallows_cancel_then_waits()

    async def driver(_user):
        assert False, 'DELIBERATE driver failure'

    started = time.monotonic()
    with pytest.raises(AssertionError, match='DELIBERATE') as info:
        run_user_driver(context, driver, teardown_timeout=_TEARDOWN)
    assert time.monotonic() - started < _PROMPT
    assert any('teardown did not finish' in n for n in getattr(info.value, '__notes__', []))


def test_a_driver_that_never_ends_times_out():
    @asynccontextmanager
    async def context():
        yield object()

    async def driver(_user):
        await _swallows_cancel_then_waits()

    with pytest.raises(TimeoutError, match='driver did not finish'):
        run_user_driver(context, driver, driver_timeout=0.5, teardown_timeout=_TEARDOWN)


def test_joins_lab_harness_fails_promptly_with_a_page_task_left_running(monkeypatch):
    """The real harness, real lifespan, real ``run.io_bound``: a task left on
    the loop survives cancellation, and the driver's assertion still comes
    back in seconds."""
    import tests.render_smoke._bounded_run as br
    from nicegui import run

    monkeypatch.setattr(br, 'TEARDOWN_TIMEOUT', _TEARDOWN)
    monkeypatch.setattr(br.run_user_driver, '__kwdefaults__',
                        {**br.run_user_driver.__kwdefaults__, 'teardown_timeout': _TEARDOWN})

    async def loader():
        await run.io_bound(time.sleep, 3)  # cancel is swallowed here -> None
        await asyncio.sleep(3600)

    async def driver(user):
        await user.open('/joins-lab')
        asyncio.get_running_loop().create_task(loader(), name='leftover loader')
        await asyncio.sleep(0.05)
        assert False, 'DELIBERATE driver failure'

    started = time.monotonic()
    with pytest.raises(AssertionError, match='DELIBERATE') as info:
        run_joins_lab_smoke(driver)
    assert time.monotonic() - started < _PROMPT + 10
    assert any('leftover loader' in n for n in getattr(info.value, '__notes__', []))
