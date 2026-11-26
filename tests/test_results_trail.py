# -*- coding: utf-8 -*-
"""Browse's bar to a search's other results (web/components/results_trail.py): the lists kept
for one browser, the address parameters, the list's HTML, and the search page keeping its list
once for each list it shows."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from web.components import results_trail as rt
from web.components.results_trail import TrailItem


@pytest.fixture(autouse=True)
def _fresh():
    rt.clear()
    yield
    rt.clear()


def _items(n):
    return [TrailItem(url=f'/browse?sys_id=99{i:04d}', shelfmark=f'T-S {i}', text=f'text {i}') for i in range(n)]


def test_a_list_is_kept_for_its_own_browser_only():
    token = rt.keep_trail(_items(3), owner='a' * 32)
    assert token and rt.get_trail(token, owner='a' * 32).items[2].shelfmark == 'T-S 2'
    assert rt.get_trail(token, owner='b' * 32) is None
    assert rt.get_trail(token, owner=None) is None
    assert rt.get_trail('no such list', owner='a' * 32) is None


def test_no_owner_or_no_result_keeps_nothing():
    assert rt.keep_trail(_items(3), owner=None) is None
    assert rt.keep_trail([], owner='a' * 32) is None


def test_a_long_list_is_cut_and_says_there_are_more(monkeypatch):
    monkeypatch.setattr(rt, 'MAX_ITEMS', 5)
    trail = rt.get_trail(rt.keep_trail(_items(8), owner='o'), owner='o')
    assert len(trail.items) == 5 and trail.more
    assert rt.count_label(trail, 0).endswith('5+')


def test_lists_are_bounded_in_number_and_age(monkeypatch):
    monkeypatch.setattr(rt, 'MAX_TRAILS', 2)
    first = rt.keep_trail(_items(1), owner='o')
    rt.keep_trail(_items(1), owner='o')
    third = rt.keep_trail(_items(1), owner='o')
    assert rt.get_trail(first, owner='o') is None and rt.get_trail(third, owner='o')
    clock = [1000.0]
    monkeypatch.setattr(rt.time, 'monotonic', lambda: clock[0])
    old = rt.keep_trail(_items(1), owner='o')
    clock[0] += rt.KEEP_SECONDS + 1
    assert rt.get_trail(old, owner='o') is None


@pytest.mark.parametrize('value,expected', [('0', 0), ('12', 12), (' 3 ', 3), ('-1', None), ('x', None), (None, None)])
def test_parse_index(value, expected):
    assert rt.parse_index(value) == expected


def test_short_text_drops_the_markers_and_cuts():
    assert rt.short_text('one *two* ‖ three') == 'one two three'
    long = rt.short_text('א' * (rt.TEXT_CHARS + 10))
    assert len(long) == rt.TEXT_CHARS + 1 and long.endswith('…')


def test_each_link_carries_the_list_and_its_place():
    token = rt.keep_trail(_items(3), owner='o')
    trail = rt.get_trail(token, owner='o')
    assert rt.item_url(trail, 1) == f'/browse?sys_id=990001&trail={token}&r=1'
    assert rt.trail_params(None, 4) == ''


def test_the_list_html_escapes_and_marks_the_current_result():
    items = [TrailItem(url='/browse?sys_id=1&highlight=a"b', shelfmark='<b>T-S</b>', text='x & y'),
             TrailItem(url='/browse?sys_id=2', shelfmark='T-S 2', text='z')]
    trail = rt.get_trail(rt.keep_trail(items, owner='o'), owner='o')
    out = rt.list_html(trail, 1)
    assert '<b>T-S</b>' not in out and '&lt;b&gt;T-S&lt;/b&gt;' in out and 'x &amp; y' in out
    assert 'a&quot;b' in out
    first, second = out.split('</a>')[:2]
    assert 'data-current' not in first and 'data-current' in second and '&amp;r=1' in second


def test_browse_places_nothing_for_an_unknown_list_or_place(monkeypatch):
    import web.safe_storage as ss
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: 'o')
    placed = []
    monkeypatch.setattr(rt, 'place_results_bar', lambda trail, index, on_close: placed.append(index))
    token = rt.keep_trail(_items(3), owner='o')
    for args in (None, ('nope-nope-nope', '0'), (token, '3'), (token, 'x')):
        kept = {}
        rt.browse_results_bar(args, kept=kept, on_close=lambda: None)
        assert kept == {} and placed == []
    kept = {}
    rt.browse_results_bar((token, '2'), kept=kept, on_close=lambda: None)
    assert kept == {'trail': token, 'r': 2} and placed == [2]


def test_closing_the_bar_drops_its_address_parameters(monkeypatch):
    import web.safe_storage as ss
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: 'o')
    closers = []
    monkeypatch.setattr(rt, 'place_results_bar', lambda trail, index, on_close: closers.append(on_close))
    token = rt.keep_trail(_items(2), owner='o')
    kept, calls = {}, []
    rt.browse_results_bar((token, '0'), kept=kept, on_close=lambda: calls.append(dict(kept)))
    closers[0]()
    assert kept == {} and calls == [{}]


# --- the search page -------------------------------------------------------------------------

def _result(sys_id, shelfmark, snippet='a *b* c'):
    return {'display': {'id': sys_id, 'shelfmark': shelfmark}, 'snippet': snippet}


def _search_state():
    from web.pages.search_state import SearchUIState
    return SearchUIState()


def test_the_search_keeps_its_list_once_and_a_new_list_anew(monkeypatch):
    import web.safe_storage as ss
    from web.pages import search_results as sr
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: 'o')
    monkeypatch.setattr(sr, 'state', SimpleNamespace(meta_mgr=None))
    st = _search_state()
    results = [_result('990001', 'T-S 1'), _result('990002', 'T-S 2')]
    sr.keep_results_trail(st, results)
    token = st.trail_token
    trail = rt.get_trail(token, owner='o')
    assert st.trail_size == 2 and [i.shelfmark for i in trail.items] == ['T-S 1', 'T-S 2']
    assert trail.items[0].url == '/browse?sys_id=990001&highlight=b' and trail.items[0].text == 'a b c'
    sr.keep_results_trail(st, results)
    assert st.trail_token == token            # the same list: kept once
    sr.keep_results_trail(st, results[:1])
    assert st.trail_token != token and st.trail_size == 1


def test_the_list_stops_at_a_result_browse_cannot_open(monkeypatch):
    import web.safe_storage as ss
    from web.pages import search_results as sr
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: 'o')
    monkeypatch.setattr(sr, 'state', SimpleNamespace(meta_mgr=None))
    st = _search_state()
    sr.keep_results_trail(st, [_result('990001', 'T-S 1'), _result(None, 'none'), _result('990003', 'T-S 3')])
    assert st.trail_size == 1 and rt.get_trail(st.trail_token, owner='o').more


def test_no_session_keeps_no_list(monkeypatch):
    import web.safe_storage as ss
    from web.pages import search_results as sr
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: None)
    monkeypatch.setattr(sr, 'state', SimpleNamespace(meta_mgr=None))
    st = _search_state()
    sr.keep_results_trail(st, [_result('990001', 'T-S 1')])
    assert st.trail_token is None and st.trail_size == 0


def test_the_card_link_and_browse_are_wired():
    """The wiring sites, which a unit test of the helpers would not see (call sites)."""
    import inspect
    from web.pages import browse, search_results
    card = inspect.getsource(search_results.create_result_card)
    assert 'trail_params(search_state.trail_token, index)' in card
    assert 'keep_results_trail(search_state, results)' in inspect.getsource(search_results.render_results)
    page = inspect.getsource(browse.create_browse_page)
    assert 'browse_results_bar(results_trail, kept=trail_kept, on_close=_update_browser_url)' in page
    assert 'params.update(trail_kept)' in page
