# -*- coding: utf-8 -*-
"""The search page's Transcriptions selector, driven through the real /search page (NiceGUI User
simulation, a job queue in place of the workers; no index, no network)."""
from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'editions-web-page-test-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    _element, run, server, store, submit, wait_for_payloads,
)


def _sources_column(user):
    """The column holding the Transcriptions selector (label and select)."""
    from nicegui import ui

    from web.pages.search_editions import SOURCE_CHOICES
    select = _element(user, ui.select, lambda e: set(e.options) == set(SOURCE_CHOICES))
    return select.parent_slot.parent


@pytest.mark.usefixtures('server')
def test_the_transcriptions_selector_hides_in_lab_mode(monkeypatch):
    """A Lab search never takes the choice, so the selector is not shown while Lab Mode is on --
    switched by the reader or set in code, as a restored Lab run sets it (Codex review of
    PR #18)."""
    from nicegui import ui

    import web.editions_assets as editions_assets
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)
    seen = {}

    async def driver(a):
        await a.open('/search')
        column = _sources_column(a)
        lab = _element(a, ui.switch, lambda e: e.text == 'Enable Lab Mode algorithms')
        seen['before'] = column.visible
        with a._client:
            lab.value = True
        seen['lab'] = column.visible
        with a._client:
            lab.value = False
        seen['after'] = column.visible

    run(driver, count=1)
    assert seen == {'before': True, 'lab': False, 'after': True}, seen


def _sources_value(user):
    from nicegui import ui

    from web.pages.search_editions import SOURCE_CHOICES
    return _element(user, ui.select, lambda e: set(e.options) == set(SOURCE_CHOICES)).value


@pytest.mark.usefixtures('server')
def test_a_fresh_address_keeps_the_current_choice_restored_rows_keep_theirs(monkeypatch):
    """A new search from the address (no run of its own) takes the visitor's current choice,
    not the last run's; the rows of that run, restored, show the choice they were searched
    with (Codex reviews of PR #18)."""
    import web.editions_assets as editions_assets
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)
    last_run = {'query': 'ישן', 'mode': 'literal', 'page_mode': 'exact', 'gap': 0,
                'sources': 'editions'}
    row = {'uid': 'r1', 'raw_header': '', 'snippet': '*ישן*', 'full_text': 'ישן',
           'display': {'id': '990000000000000001', 'shelfmark': 'T-S 1.1', 'title': '',
                       'img': '1', 'source': 'V0.8'}}
    seen = {}

    def stored_run(user):
        store(user, search_sources='all', search_query='ישן', search_mode='exact',
              search_last_run=dict(last_run), search_results=[dict(row)])

    async def driver(a):
        await a.open('/search')
        stored_run(a)
        await a.open('/search?q=חדש')
        seen['fresh'] = _sources_value(a)
        stored_run(a)
        await a.open('/search')
        seen['restored'] = _sources_value(a)

    run(driver, count=1)
    assert seen == {'fresh': 'all', 'restored': 'editions'}, seen


def test_a_search_after_the_reader_lost_access_reads_the_automatic_texts(server, monkeypatch):  # noqa: F811
    """With EDITIONS_SEARCH_ENABLED=editors, an editor who signs out in another tab has lost the
    scholarly transcriptions: the page reads access again at each search, sends no source
    choice and hides the selector (Codex review of PR #18)."""
    import web.editions_assets as editions_assets
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False, sources=None):
        raise AssertionError('must run through the worker queue, not in process')

    # The stand-in engine takes the source choice, as the real one does.
    monkeypatch.setattr(FakeSearchEngine, 'execute_search', execute_search)
    served = {'now': True}
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: served['now'])
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, 'שלום')
        await wait_for_payloads(server.queue, 1)
        await asyncio.sleep(0.5)
        served['now'] = False
        submit(a, 'עליכם')
        await wait_for_payloads(server.queue, 2)
        seen['visible'] = _sources_column(a).visible

    run(driver, count=1)
    first, second = server.queue.payloads[:2]
    assert first['arguments'].get('sources') == 'all', first['arguments']
    assert 'sources' not in second['arguments'], second['arguments']
    assert seen['visible'] is False
