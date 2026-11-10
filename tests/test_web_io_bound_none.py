# -*- coding: utf-8 -*-
"""The website survives ``nicegui.run.io_bound`` returning None.

NiceGUI 3.8's ``run.io_bound`` does not raise when the awaiting task is
cancelled, or when the app is stopping: it swallows the ``CancelledError`` and
returns None. Production (2026-10-07, after the v9.6.0 deploy) logged the
search page's early-rows painter crashing on exactly that, 19 times::

    File "web/pages/search_results.py", line 691, in create_result_card
      _title_info = search_state.title_translations.get(sys_id) if sys_id else None
    AttributeError: 'NoneType' object has no attribute 'get'

The search cancels the painter when it ends; if the painter is then waiting on
its title lookup, the lookup "returns" None, the painter stored that as the
page's titles and rendered with them.

These tests drive the real /search handlers (NiceGUI User simulation) with a
job queue in place of the worker processes. No index, no network.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'io-bound-none-test-secret-0123456789abcdef')

from tests.test_web_search_within_cutoff import (  # noqa: E402,F401  (page is a fixture)
    WORD, AnsweringQueue, _label_texts, _row, _wait_for, page,
)
from tests.test_web_variant_settings_per_visitor import (  # noqa: E402
    run, submit, wait_for_payloads,
)

PREVIEW_FAILED = 'Search preview render failed'


def test_nicegui_io_bound_returns_none_when_cancelled_and_under_wait_for():
    """The premise of every None guard on the website: a cancelled run.io_bound,
    and an asyncio.wait_for around one that times out, both RETURN None (the Joins
    Lab's search time limit therefore never raised TimeoutError). If NiceGUI ever
    raises instead, the guards are harmless and this test says so."""
    from nicegui import run as nicegui_run

    async def main():
        gate = threading.Event()
        try:
            task = asyncio.ensure_future(nicegui_run.io_bound(gate.wait, 5))
            await asyncio.sleep(0.05)
            task.cancel()
            cancelled = await task
            timed_out = await asyncio.wait_for(nicegui_run.io_bound(gate.wait, 5), timeout=0.05)
            return cancelled, timed_out
        finally:
            gate.set()

    assert asyncio.run(main()) == (None, None)


class HeldQueue(AnsweringQueue):
    """WORD's search hands over early rows and stays running until the test ends it."""

    def __init__(self, early_rows):
        super().__init__({})
        self.early_rows = early_rows
        self.held = None

    def submit(self, payload):
        from web.research_jobs import Job
        with self.lock:
            self.payloads.append(payload)
        job = Job(payload)
        job.preview = [dict(r) for r in self.early_rows]
        job.preview_sequence = 1
        self.held = job
        return job

    def finish(self, rows):
        self.held.future.set_result({'value': [dict(r) for r in rows],
                                     'cutoff': {'capped': False, 'interrupted': False},
                                     'time_limit': False})


class SlowFirstTitles:
    """TranslationService whose FIRST title lookup waits until released."""
    entered = None
    release = None
    calls = 0

    def __init__(self, thread_safe=True):
        pass

    def titles_available(self):
        return True

    def get_title_translations_batch(self, sys_ids):
        cls = type(self)
        cls.calls += 1
        if cls.calls == 1:
            cls.entered.set()
            cls.release.wait(10)
        return {}

    def close(self):
        pass


class _Errors(logging.Handler):
    def __init__(self):
        super().__init__(logging.ERROR)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


# The page's own logger, and NiceGUI's, which logs an exception a handler raised.
_LOGGERS = ('web.pages.search', 'nicegui')


def _capture_errors():
    handler = _Errors()
    for name in _LOGGERS:
        logging.getLogger(name).addHandler(handler)
    return handler


def _release(handler):
    for name in _LOGGERS:
        logging.getLogger(name).removeHandler(handler)


def _none_errors(handler):
    return [m for m in handler.messages if 'NoneType' in m or m == PREVIEW_FAILED]


def _shows(user, text):
    return any(text in t for t in _label_texts(user))


def _card_shown(user, shelfmark):
    """A result card for *shelfmark* is on the page (its label is bidi-isolated HTML)."""
    with user._client:
        return any(shelfmark in str(getattr(e, 'content', '') or getattr(e, 'text', '') or '')
                   for e in user._client.elements.values())


def test_a_cancelled_preview_title_lookup_does_not_crash_the_painter(page, monkeypatch):  # noqa: F811 (the imported fixture)
    """The production crash, reproduced with the real cancel: the search ends while
    its painter waits on the preview title lookup; NiceGUI's io_bound swallows the
    painter's cancel and returns None. The painter must stop, not render with None."""
    from shared import translation_service
    from web import research_jobs
    queue = HeldQueue(early_rows=[_row('M1')])
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    SlowFirstTitles.entered, SlowFirstTitles.release = threading.Event(), threading.Event()
    SlowFirstTitles.calls = 0
    monkeypatch.setattr(translation_service, 'TranslationService', SlowFirstTitles)
    errors = _capture_errors()

    async def driver(a, b):
        try:
            await a.open('/search')
            submit(a, WORD)
            await wait_for_payloads(queue, 1)
            # The painter got the early rows and is waiting on their titles.
            await _wait_for(SlowFirstTitles.entered.is_set)
            queue.finish([_row('M1')])          # the search ends: it cancels the painter
            await _wait_for(lambda: _shows(a, '1 Results'), user=a)
            await asyncio.sleep(0.3)            # the painter's last step, if any
        finally:
            SlowFirstTitles.release.set()

    try:
        run(driver)
    finally:
        _release(errors)
    assert _none_errors(errors) == []


def _io_bound_returning_none_for(monkeypatch, *names, pages=('web.pages.search',)):
    """Make run.io_bound return None for the named callbacks, as NiceGUI does for a
    cancelled task or a stopping app; everything else runs. Patched in the given
    page modules and in web.io_bound_result, whose wrapper keeps the callback's
    name -- so a call reaches the patch whichever way the page makes it."""
    import importlib
    from types import SimpleNamespace

    from nicegui import run as nicegui_run

    from web import io_bound_result
    seen = []

    async def io_bound(fn, *args, **kwargs):
        if getattr(fn, '__name__', '') in names:
            seen.append(fn.__name__)
            return None
        return await nicegui_run.io_bound(fn, *args, **kwargs)

    fake = SimpleNamespace(io_bound=io_bound, cpu_bound=nicegui_run.cpu_bound)
    for module in [importlib.import_module(name) for name in pages] + [io_bound_result]:
        monkeypatch.setattr(module, 'run', fake, raising=False)
    return seen


def test_the_result_renders_when_its_title_lookup_returns_none(page, monkeypatch):  # noqa: F811 (the imported fixture)
    """Stage 0's title lookup returning None stored None as the page's titles, and
    the result list then failed to render."""
    queue = page({(WORD, False): ([_row('M1')], None)})
    seen = _io_bound_returning_none_for(monkeypatch, '_fetch_titles_fast')
    errors = _capture_errors()

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: _card_shown(a, 'S-M1'), user=a)

    try:
        run(driver)
    finally:
        _release(errors)
    assert seen == ['_fetch_titles_fast']
    # The card's shelfmark is drawn before its title: the card can show and still
    # have failed half-way. What tells is the handler's exception.
    assert _none_errors(errors) == []


def test_a_filter_lookup_that_returns_none_never_searches_without_the_filters(page, monkeypatch):  # noqa: F811 (the imported fixture)
    """With a filter active the lookup never answers None ("no restriction"); a
    None is a cancel or a stopping app. The search must not run unfiltered (#17)."""
    from web.pages import search as search_page
    queue = page({(WORD, False): ([_row('M1')], None)})
    monkeypatch.setattr(search_page, 'has_active_filters', lambda state: True)
    seen = _io_bound_returning_none_for(monkeypatch, '_compute_restrict')

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await _wait_for(lambda: seen == ['_compute_restrict'])
        await asyncio.sleep(0.5)                # time for a search to reach the queue

    run(driver)
    assert queue.payloads == [], [p['arguments'].get('restrict_sys_ids') for p in queue.payloads]
