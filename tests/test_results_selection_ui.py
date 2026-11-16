# -*- coding: utf-8 -*-
"""Selecting search results: "N selected" and the two bulk actions (add to a list, copy the
text) show while a result is selected and hide again once none is, driven through the real
/search page (NiceGUI User simulation, a job queue in place of the workers; no index, no
network). They used to stay hidden: the page showed them with `.style('')`, which in NiceGUI
only adds styles and so never took back their `display: none`."""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'selection-ui-test-secret-0123456789abcdef')

from tests.test_scholarly_transcription_filter import (  # noqa: E402
    WORD, _answer_with_rows, _until_button_shows,
)
from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    _element, run, server, submit, wait_for_payloads,
)


def _by_class(user, kind, cls):
    return _element(user, kind, lambda e: cls in e.classes)


def _select_all(user):
    from nicegui import ui
    return _element(user, ui.checkbox,
                    lambda e: 'results-count-group' in e.parent_slot.parent.classes)


def _state(user):
    """(the counter's text, counter shown, bulk actions shown, the header's has-selection)."""
    from nicegui import ui
    counter = _by_class(user, ui.label, 'results-selection-count')
    bulk = _by_class(user, ui.row, 'results-bulk-actions')
    header = _by_class(user, ui.row, 'results-header')
    return (counter.text, 'tb-hidden' not in counter.classes, 'tb-hidden' not in bulk.classes,
            'has-selection' in header.classes)


def test_selecting_results_shows_the_count_and_the_bulk_actions(server, monkeypatch):  # noqa: F811
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        await asyncio.sleep(0.2)
        seen['before'] = _state(a)
        with a._client:
            _select_all(a).value = True
        await asyncio.sleep(0.2)
        seen['selected'] = _state(a)
        with a._client:
            _select_all(a).value = False
        await asyncio.sleep(0.2)
        seen['cleared'] = _state(a)

    run(driver, count=1)
    assert seen['before'] == ('', False, False, False), seen
    assert seen['selected'] == ('3 selected', True, True, True), seen
    assert seen['cleared'] == ('', False, False, False), seen


def _row_boxes(user):
    """The result rows' checkboxes (every checkbox inside a result card)."""
    from nicegui import ui
    with user._client:
        cards = [e for e in user._client.elements.values()
                 if isinstance(e, ui.card) and 'result-card' in e.classes]
        return [c for card in cards for c in card.descendants() if isinstance(c, ui.checkbox)]


def _ticked(user):
    return sum(1 for box in _row_boxes(user) if box.value)


async def _select_all_rows(a):
    with a._client:
        _select_all(a).value = True
    await asyncio.sleep(0.2)


async def _search(a, srv, payloads):
    submit(a, WORD)
    await wait_for_payloads(srv.queue, payloads)
    await _until_button_shows(a)
    await asyncio.sleep(0.3)


def test_unticking_one_row_of_a_full_selection_keeps_the_others(server, monkeypatch):  # noqa: F811
    """The select-all box follows the rows; that change is the page's, not the reader's, so it
    must not clear the rest (Codex on #26)."""
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        await _search(a, server, 1)
        await _select_all_rows(a)
        seen['all'] = (_state(a), _ticked(a))
        with a._client:
            _row_boxes(a)[0].value = False
        await asyncio.sleep(0.2)
        seen['one off'] = (_state(a), _ticked(a), _select_all(a).value)

    run(driver, count=1)
    assert seen['all'] == (('3 selected', True, True, True), 3), seen
    assert seen['one off'] == (('2 selected', True, True, True), 2, False), seen


def test_a_new_search_or_a_restored_entry_starts_with_nothing_selected(server, monkeypatch):  # noqa: F811
    """The selection is row positions, so the rows that replace a selected result set must not
    inherit it: neither the counter, the bulk actions nor the rows' boxes (Codex on #26)."""
    from tests.test_scholarly_transcription_filter import _row
    from tests.test_web_variant_settings_per_visitor import _restore_history, store
    earlier = 'ישן'
    entry = {'query': earlier, 'mode': 'exact', 'result_count': 2, 'params': {'mode': 'exact'},
             'state': {'results': [_row('b1', '990000000000000001', 1, snippet=''),
                                   _row('b2', '990000000000000004', 1, snippet='')]}}
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        await _search(a, server, 1)
        await _select_all_rows(a)
        seen['selected'] = (_state(a), _ticked(a))
        await _search(a, server, 2)
        seen['new search'] = (_state(a), _ticked(a), len(_row_boxes(a)))
        await _select_all_rows(a)
        store(a, search_history=[entry])
        _restore_history(a, earlier)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while len(_row_boxes(a)) != 2:
            assert loop.time() < deadline, len(_row_boxes(a))
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)
        seen['restored'] = (_state(a), _ticked(a))

    run(driver, count=1)
    assert seen['selected'] == (('3 selected', True, True, True), 3), seen
    assert seen['new search'] == (('', False, False, False), 0, 3), seen
    assert seen['restored'] == (('', False, False, False), 0), seen


def test_with_a_filter_on_the_selection_names_the_rows_shown(server, monkeypatch):  # noqa: F811
    """A filter hides the first result: ticking the first row shown selects that row, not the
    hidden one, for the counter, the export and the bulk copy; select-all takes the rows
    shown and keeps the filter on screen; with the filter off the hidden row shows unticked
    (Codex on #26: positions in the full list named other results)."""
    import web.pages.search as search_page
    from tests.test_scholarly_transcription_filter import ED, NONE, TRANS, _choose, _count_line, _row
    from tests.test_web_variant_settings_per_visitor import _fire, stored
    rows = [_row('r1', ED, 1, snippet='*שלום*', full_text='טקסט ראשון'),
            _row('r2', NONE, 1, snippet='*שלום*', full_text='טקסט שני'),
            _row('r3', TRANS, 1, snippet='*שלום*', full_text='טקסט שלישי')]
    _answer_with_rows(server, monkeypatch, {'r1'}, rows=rows)
    copied = []

    async def fake_copy(text, **_kw):
        copied.append(text)
    monkeypatch.setattr(search_page, 'copy_text_to_clipboard', fake_copy)
    seen = {}

    def export_uids(a):
        return (stored(a, 'export_search_payload') or {}).get('selected_uids')

    async def driver(a):
        from nicegui import ui
        await a.open('/search')
        await asyncio.sleep(0.5)
        await _search(a, server, 1)
        _choose(a, 'hide')                       # r1 has one: hidden
        await asyncio.sleep(0.4)
        seen['filtered'] = (_count_line(a), len(_row_boxes(a)))
        with a._client:
            _row_boxes(a)[0].value = True        # the first row shown: r2
        await asyncio.sleep(0.2)
        seen['first'] = (_state(a)[0], export_uids(a))
        _fire(a, _element(a, ui.button, lambda e: e.props.get('icon') == 'content_copy'
                          and 'results-bulk-actions' in e.parent_slot.parent.classes), 'click')
        await asyncio.sleep(0.4)
        seen['copied'] = list(copied)
        await _select_all_rows(a)
        seen['all'] = (_state(a)[0], export_uids(a), len(_row_boxes(a)), _count_line(a))
        _choose(a, 'all')
        await asyncio.sleep(0.4)
        seen['unfiltered'] = (_state(a)[0], _ticked(a), len(_row_boxes(a)), _select_all(a).value)

    run(driver, count=1)
    assert seen['filtered'] == ('2 of 3 Results (No scholarly transcription)', 2), seen
    assert seen['first'] == ('1 selected', ['r2']), seen
    assert len(seen['copied']) == 1 and 'טקסט שני' in seen['copied'][0], seen
    assert 'טקסט ראשון' not in seen['copied'][0], seen
    assert seen['all'] == ('2 selected', ['r2', 'r3'], 2,
                           '2 of 3 Results (No scholarly transcription)'), seen
    assert seen['unfiltered'] == ('2 selected', 2, 3, False), seen


def test_unticking_select_all_keeps_a_selected_row_a_filter_hides(server, monkeypatch):  # noqa: F811
    """Select all, then a filter hides one selected row: unticking select-all unticks the rows
    shown, and the hidden one shows ticked again once the filter is off -- the box acts on the
    rows shown both ways (Codex on #30)."""
    from tests.test_scholarly_transcription_filter import _choose
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        await _search(a, server, 1)
        await _select_all_rows(a)
        seen['all'] = _state(a)[0]
        _choose(a, 'hide')                       # r1 has one: hidden, still selected
        await asyncio.sleep(0.4)
        seen['filtered'] = (_state(a)[0], _select_all(a).value)
        with a._client:
            _select_all(a).value = False
        await asyncio.sleep(0.2)
        seen['unticked'] = (_state(a)[0], _ticked(a))
        _choose(a, 'all')
        await asyncio.sleep(0.4)
        seen['unfiltered'] = (_state(a)[0], _ticked(a), len(_row_boxes(a)))

    run(driver, count=1)
    assert seen['all'] == '3 selected', seen
    assert seen['filtered'] == ('2 selected', True), seen
    assert seen['unticked'] == ('', 0), seen
    assert seen['unfiltered'] == ('1 selected', 1, 3), seen
