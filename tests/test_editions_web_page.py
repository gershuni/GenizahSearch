# -*- coding: utf-8 -*-
"""The search page's Transcriptions selector, driven through the real /search page (NiceGUI User
simulation, a job queue in place of the workers; no index, no network)."""
from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'editions-web-page-test-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    SEARCH_BOX, _element, run, server, store, submit, wait_for_payloads,
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


def _notice(user):
    """The strip over the results naming the one kind of transcription searched."""
    from nicegui import ui
    return _element(user, ui.row, lambda e: 'sources-notice' in e.classes)


def _notice_shown(user):
    return _notice(user)._style.get('display') == 'flex'


@pytest.mark.usefixtures('server')
def test_a_search_of_one_kind_says_so_and_offers_all(server, monkeypatch):  # noqa: F811
    """Scholarly or Automatic alone (owner, 2026-11-18): the selector is framed while not on All,
    the results say which kind was searched, and the notice's button runs the same search in all
    of them, after which the notice is gone."""
    from nicegui import ui

    import web.editions_assets as editions_assets
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine
    from web.pages.search_editions import (NARROWED_CLASS, SEARCH_ALL_SOURCES, SOURCE_CHOICES,
                                           SOURCES_NOTICES)

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False, sources=None):
        raise AssertionError('must run through the worker queue, not in process')

    # The stand-in engine takes the source choice, as the real one does.
    monkeypatch.setattr(FakeSearchEngine, 'execute_search', execute_search)
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)
    seen = {}

    async def driver(a):
        await a.open('/search')
        select = _element(a, ui.select, lambda e: set(e.options) == set(SOURCE_CHOICES))
        seen['all_framed'] = NARROWED_CLASS in select.classes
        seen['before_search'] = _notice_shown(a)
        with a._client:
            select.value = 'editions'
        seen['editions_framed'] = NARROWED_CLASS in select.classes
        submit(a, 'שלום')
        await wait_for_payloads(server.queue, 1)
        await asyncio.sleep(0.3)
        seen['shown'] = _notice_shown(a)
        with a._client:
            seen['text'] = [e.text for e in _notice(a).descendants() if isinstance(e, ui.label)]
            button = next(e for e in _notice(a).descendants() if isinstance(e, ui.button))
        seen['button'] = button.text
        # The box edited since: the button still runs the search the notice describes (Codex
        # on #50), in every transcription.
        box = _element(a, ui.input, lambda e: SEARCH_BOX in e.props)
        with a._client:
            box.value = 'משהו אחר'
        from tests.test_web_variant_settings_per_visitor import _fire
        _fire(a, button, 'click')
        await wait_for_payloads(server.queue, 2)
        await asyncio.sleep(0.3)
        seen['value_after'] = select.value
        seen['box_after'] = box.value
        seen['framed_after'] = NARROWED_CLASS in select.classes
        seen['shown_after'] = _notice_shown(a)

    run(driver, count=1)
    first, second = server.queue.payloads[:2]
    assert first['arguments'].get('sources') == 'editions', first['arguments']
    assert second['arguments'].get('sources') == 'all', second['arguments']
    assert second['arguments'].get('query_str') == first['arguments'].get('query_str') == 'שלום', (
        first['arguments'], second['arguments'])
    assert seen == {
        'all_framed': False, 'before_search': False, 'editions_framed': True, 'shown': True,
        'text': [SOURCES_NOTICES['editions']], 'button': SEARCH_ALL_SOURCES,
        'value_after': 'all', 'box_after': 'שלום', 'framed_after': False, 'shown_after': False,
    }, seen


@pytest.mark.usefixtures('server')
def test_search_all_reruns_the_shown_runs_level_changes_and_not_words(server, monkeypatch):  # noqa: F811
    """"Search all transcriptions" reruns the search the notice describes, whatever the
    controls hold since (Codex on #50): its mode, variant level and Num Changes as recorded
    (not the visitor's stored preference, changed meanwhile), and its NOT words (not the
    box's new ones)."""
    from nicegui import ui

    import web.editions_assets as editions_assets
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine
    from web.pages.search_editions import SOURCE_CHOICES

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False, sources=None):
        raise AssertionError('must run through the worker queue, not in process')

    monkeypatch.setattr(FakeSearchEngine, 'execute_search', execute_search)
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)

    def not_box(user):
        return _element(user, ui.input,
                        lambda e: e.props.get('placeholder') == 'Words to exclude (space separated)')

    async def driver(a):
        await a.open('/search')
        store(a, search_max_changes=1)
        await a.open('/search')
        select = _element(a, ui.select, lambda e: set(e.options) == set(SOURCE_CHOICES))
        with a._client:
            select.value = 'editions'
            not_box(a).value = 'ישן'
        submit(a, '??? שלום')
        await wait_for_payloads(server.queue, 1)
        await asyncio.sleep(0.3)
        # Everything changed since: the stored Num Changes, the mode, the NOT words.
        store(a, search_max_changes=3)
        mode = _element(a, ui.select, lambda e: 'exact' in (e.options or {}) and 'Regex' in e.options)
        with a._client:
            mode.value = 'exact'
            not_box(a).value = 'חדש'
            button = next(e for e in _notice(a).descendants() if isinstance(e, ui.button))
        from tests.test_web_variant_settings_per_visitor import _fire
        _fire(a, button, 'click')
        await wait_for_payloads(server.queue, 2)

    run(driver, count=1)
    first, second = server.queue.payloads[:2]
    assert first['arguments'].get('sources') == 'editions', first['arguments']
    assert second['arguments'].get('sources') == 'all', second['arguments']
    for key in ('query_str', 'mode', 'exclude_words'):
        assert second['arguments'].get(key) == first['arguments'].get(key), (
            key, first['arguments'], second['arguments'])
    assert first['arguments'].get('exclude_words') == ['ישן'], first['arguments']
    assert first['settings']['variant_max_changes'] == 1, first['settings']
    for key in ('variant_pairs_count', 'variant_max_changes'):
        assert second['settings'][key] == first['settings'][key], (key, first['settings'], second['settings'])


@pytest.mark.usefixtures('server')
def test_search_all_reruns_the_shown_responsa_options(server, monkeypatch):  # noqa: F811
    """A Responsa run's options go back with it, not the boxes as ticked since (Codex on #50)."""
    from nicegui import ui

    import web.editions_assets as editions_assets
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine, _fire
    from web.pages.search_editions import SOURCE_CHOICES

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False, sources=None):
        raise AssertionError('must run through the worker queue, not in process')

    monkeypatch.setattr(FakeSearchEngine, 'execute_search', execute_search)
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)

    def box(user, label):
        return _element(user, ui.checkbox, lambda e: e.text == label)

    async def driver(a):
        await a.open('/search')
        select = _element(a, ui.select, lambda e: set(e.options) == set(SOURCE_CHOICES))
        mode = _element(a, ui.select, lambda e: 'responsa' in (e.options or {}) and 'Regex' in e.options)
        with a._client:
            select.value = 'editions'
            mode.value = 'responsa'
            box(a, 'Judeo-Arabic').value = True
            box(a, 'Flex Spacing').value = False
        submit(a, 'שלום')
        await wait_for_payloads(server.queue, 1)
        await asyncio.sleep(0.3)
        with a._client:  # changed since: other boxes, another mode
            box(a, 'Judeo-Arabic').value = False
            box(a, 'Flex Spacing').value = True
            mode.value = 'exact'
            button = next(e for e in _notice(a).descendants() if isinstance(e, ui.button))
        _fire(a, button, 'click')
        await wait_for_payloads(server.queue, 2)

    run(driver, count=1)
    first, second = server.queue.payloads[:2]
    assert first['arguments'].get('sources') == 'editions', first['arguments']
    assert second['arguments'].get('sources') == 'all', second['arguments']
    shown = first['arguments'].get('responsa_options')
    assert shown and shown.get('ja') is True and shown.get('flex_spacing') is False, first['arguments']
    assert second['arguments'].get('responsa_options') == shown, (first['arguments'], second['arguments'])
