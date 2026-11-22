# -*- coding: utf-8 -*-
"""The web shelf picker's views of the shelfmarks: a list, a table and thumbnails
(``shared/shelfmark_tree_rows.py``; the private plan SHELFMARK_PICKER_VIEWS_PLAN.md, VX1-VX9), on test
pages in a NiceGUI User simulation with the fixture tree of tests/shelfmark_tree_fixture.py.

* The switch is in the shelfmarks pane's header; the list is the default; the choice is kept for the
  reader and the next picker opens in it.
* The layout rule: in the table and the thumbnails only the last node column is drawn; in the list the
  last three, as before. A phone keeps a collection's volumes beside its own shelfmarks in every view.
* The table: the search table's columns, its cells from ``row_details``, read off the loop in one batch
  for the page of rows, drawn again when they come; a failing read leaves the rows as they are.
* The thumbnails: each tile's image address at its size's width, as the Joins Lab fetches.
* Every action works in every view: a click selects (the pane shows it), the foot's button opens it, a
  double click opens it; in the multiple choice a click ticks.
"""
from __future__ import annotations

import asyncio
import threading
import time

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_rows import FIELDS, STORAGE_KEY
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

PICKED: list = []
CONFIRMED: list = []
SERVICE: dict = {}
CALLS: list = []

HERE = SYS['as2'][6]


async def _on_confirm(entries, label):
    CONFIRMED.append([e['sys_id'] for e in entries])
    return None


@ui.page('/_test/shelf-views-open')
def _open_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    open_shelf_picker(ui.column(), SERVICE['svc'], PICKED.append, start=start or None, pick_label='Open')


@ui.page('/_test/shelf-views-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    open_shelf_picker(ui.column(), SERVICE['svc'], PICKED.append, start=start or None, purpose='test_views_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open')


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


def _with_class(user, cls):
    with user.client:
        return [e for e in user.client.elements.values() if cls in e.classes and not e.is_deleted]


def _node_columns(user) -> list:
    return [e.props['data-col'] for e in _els(user, 'data-col') if e.props['data-col'].startswith('n:')]


def _leaves(user) -> dict:
    return {e.props['data-sys-id']: e for e in _els(user, 'data-leaf')}


def _view_button(user, view):
    return next(e for e in _els(user, 'data-view') if isinstance(e, ui.button) and e.props['data-view'] == view)


async def _until(predicate, timeout=10.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def _setup(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    PICKED.clear()
    CONFIRMED.clear()
    CALLS.clear()
    return svc


def _fake_rows(sys_ids, lang='en', *, thumb_of=None, **_kw):
    """A stand-in for ``row_details``: known values for each manuscript, the thread it ran on noted."""
    ids = list(sys_ids)
    CALLS.append((ids, threading.current_thread() is threading.main_thread()))
    out = {}
    for n, sid in enumerate(ids):
        row = dict.fromkeys(FIELDS)
        row.update({'title': f'Title of {sid}', 'snippet': f'snippet {sid}', 'domains': ['Letters', 'Bible'],
                    'pages': n + 1, 'printed': n == 0, 'pgp': n == 1, 'edition': n == 2,
                    'thumb': thumb_of(sid) if thumb_of else None})
        out[sid] = row
    return out


def _cells(user) -> dict:
    """Each table row's (or tile's) HTML, by its manuscript."""
    out = {}
    for row in _with_class(user, 'stp-trow') + _with_class(user, 'stp-tile'):
        kids = [k for k in row.descendants() if isinstance(k, ui.html)]
        out[row.props['data-sys-id']] = kids[0].content if kids else ''
    return out


def test_the_switch_is_in_the_pane_and_the_list_is_the_default(tmp_path):
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user) and _testid(user, 'shelf-views'))
            [group] = _testid(user, 'shelf-views')
            seen['in_pane'] = any('stp-leaves' in a.classes for a in _with_class(user, 'stp-col')
                                  if group in a.descendants())
            seen['buttons'] = [(e.props['data-view'], e.props['aria-pressed'])
                               for e in group.descendants() if isinstance(e, ui.button)]
            seen['grid'] = bool(_with_class(user, 'stp-grid'))
            seen['columns'] = _node_columns(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['in_pane']
    assert seen['buttons'] == [('list', 'true'), ('table', 'false'), ('gallery', 'false')]
    assert seen['grid'] and len(seen['columns']) == 3          # the list as before: the last three columns


def test_the_table_keeps_the_last_column_its_details_read_once_off_the_loop(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            seen['before'] = _node_columns(user)
            UserInteraction(user, {_view_button(user, 'table')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-trow') and CALLS
                                and 'Title of' in ''.join(_cells(user).values()))
            seen['after'] = _node_columns(user)
            seen['cells'] = _cells(user)
            seen['head'] = [e.content for e in _with_class(user, 'stp-thead')]
            seen['rows'] = set(_leaves(user))
            seen['calls'] = list(CALLS)
            # back to the list and to the table again: the details are kept, not read again
            UserInteraction(user, {_view_button(user, 'list')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-grid'))
            seen['list_columns'] = _node_columns(user)
            UserInteraction(user, {_view_button(user, 'table')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-trow'))
            await asyncio.sleep(0.2)
            seen['calls_after'] = len(CALLS)

    try:
        _run(go)
    finally:
        svc.close()
    assert len(seen['before']) == 3 and seen['after'] == seen['before'][-1:]   # the last column alone
    assert len(seen['list_columns']) == 3
    ids, on_loop = seen['calls'][0]
    assert len(seen['calls']) == 1 and set(ids) == seen['rows'] and not on_loop   # one batch, off the loop
    assert seen['calls_after'] == 1
    first, second, third = ids[0], ids[1], ids[2]
    html_first = seen['cells'][first]
    for part in (f'Title of {first}', f'snippet {first}', '>Letters<', '>+1<', 'data-cell="pages">1<',
                 'class="stp-printed"'):
        assert part in html_first, part
    assert 'stp-sign-pgp' in seen['cells'][second] and 'stp-sign-pgp' not in html_first
    assert 'stp-sign-edition' in seen['cells'][third]
    head = seen['head'][0]
    assert [h in head for h in ('>Shelfmark<', '>Title<', '>Snippet<', '>Domain<', '>Pages<', '>Printed<',
                                '>PGP<', '>Edition<')] == [True] * 8


def test_the_view_is_kept_for_the_reader(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            UserInteraction(user, {_view_button(user, 'gallery')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-tile'))
            from web.safe_storage import safe_user_get
            with user.client:
                seen['stored'] = safe_user_get(STORAGE_KEY)
            # the next picker opens in it
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            seen['tiles'] = bool(_with_class(user, 'stp-tile'))
            seen['pressed'] = [e.props['data-view'] for e in _els(user, 'data-view')
                               if isinstance(e, ui.button) and e.props.get('aria-pressed') == 'true']

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['stored'] == {'view': 'gallery', 'size': 'm'}
    assert seen['tiles'] and seen['pressed'] == ['gallery']


def test_the_thumbnails_take_their_sizes_width(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}?w={width}')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            UserInteraction(user, {_view_button(user, 'gallery')}, None).click()
            assert await _until(lambda: 'data-src' in _cells(user).get(HERE, ''))
            seen['medium'] = _cells(user)[HERE]
            seen['columns'] = _node_columns(user)
            [size] = _testid(user, 'shelf-thumb-size')
            with user.client:
                size.value = 'l'
            assert await _until(lambda: 'w=600' in _cells(user).get(HERE, ''))
            seen['large'] = _cells(user)[HERE]
            seen['class'] = [e.classes for e in _els(user, 'data-col') if 'stp-gallery' in e.classes]

    try:
        _run(go)
    finally:
        svc.close()
    assert f'data-src="/img/{HERE}?w=300"' in seen['medium'] and 'class="stp-lazy"' in seen['medium']
    assert f'data-src="/img/{HERE}?w=600"' in seen['large']
    assert f'data-cell="title">Title of {HERE}<' in seen['medium']      # the start of its title under it
    assert len(seen['columns']) == 1 and 'stp-size-l' in seen['class'][0]


def test_a_phone_keeps_a_collections_volumes_beside_its_shelfmarks_in_every_view(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    svc = _setup(tmp_path)
    seen = {}

    def phone_panels(user):
        out = []
        for panel in _with_class(user, 'stp-panel'):
            if 'stp-phone' not in panel.classes:
                continue
            inner = [c for c in panel.descendants() if 'data-node' in c.props or 'data-leaf' in c.props]
            out.append(sorted({('node' if 'data-node' in c.props else 'leaf') for c in inner}))
        return out

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-views-open?start=col:10')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            for view in ('list', 'table', 'gallery'):
                if view != 'list':
                    UserInteraction(user, {_view_button(user, view)}, None).click()
                    assert await _until(lambda: _with_class(user, 'stp-trow' if view == 'table' else 'stp-tile'))
                seen[view] = phone_panels(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen == {'list': [['node'], ['leaf']], 'table': [['node'], ['leaf']], 'gallery': [['node'], ['leaf']]}


def test_every_action_works_in_the_table_and_the_thumbnails(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    shown = []

    def details(service, leaf, lang='en', **_kw):
        shown.append(leaf.sys_id)
        return {'sys_id': leaf.sys_id, 'shelfmark': leaf.label, 'others': [], 'part': '', 'title': '',
                'subjects': [], 'date': '', 'pages': None, 'text': [], 'transcriptions': [], 'joins': [],
                'comments': None, 'catalogue': None, 'bibliography': None, 'links': []}
    monkeypatch.setattr(picker, 'item_details', details)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    target = SYS['as2'][2]
    seen = {}

    async def go():
        async with _app(object()) as user:
            for view, cls in (('table', 'stp-trow'), ('gallery', 'stp-tile')):
                PICKED.clear()
                await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
                await _connect(user)
                assert await _until(lambda: _leaves(user))
                UserInteraction(user, {_view_button(user, view)}, None).click()
                assert await _until(lambda: _with_class(user, cls))
                row = _leaves(user)[target]
                seen[f'{view}_role'] = (row.props['role'], cls in row.classes)
                UserInteraction(user, {row}, None).trigger('click')
                assert await _until(lambda: _leaves(user)[target].props.get('aria-selected') == 'true')
                assert await _until(lambda: shown and shown[-1] == target)
                UserInteraction(user, {_testid(user, 'shelf-picker-confirm')[0]}, None).click()
                assert await _until(lambda: PICKED)
                seen[f'{view}_open'] = [p['sys_id'] for p in PICKED]
            # a double click opens, in the thumbnails
            PICKED.clear()
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _with_class(user, 'stp-tile'))
            UserInteraction(user, {_leaves(user)[target]}, None).trigger('dblclick')
            assert await _until(lambda: PICKED)
            seen['double'] = [p['sys_id'] for p in PICKED]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['table_role'] == ('option', True) and seen['gallery_role'] == ('option', True)
    assert seen['table_open'] == [target] and seen['gallery_open'] == [target] and seen['double'] == [target]


def test_ticks_in_the_table_and_the_thumbnails(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    a, b = SYS['as2'][1], SYS['as2'][3]
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user) and _testid(user, 'shelf-picker-switch'))
            [switch] = _testid(user, 'shelf-picker-switch')
            with user.client:
                switch.value = True
            assert await _until(lambda: any(e.props.get('role') == 'checkbox' for e in _leaves(user).values()))
            UserInteraction(user, {_view_button(user, 'table')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-trow'))
            seen['multi_class'] = any('stp-multi' in e.classes for e in _els(user, 'data-col'))
            seen['head'] = [e.content for e in _with_class(user, 'stp-thead')]
            UserInteraction(user, {_leaves(user)[a]}, None).trigger('click')
            assert await _until(lambda: _leaves(user)[a].props.get('aria-checked') == 'true')
            seen['icon'] = [k.name for k in _leaves(user)[a].descendants() if isinstance(k, ui.icon)]
            UserInteraction(user, {_view_button(user, 'gallery')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-tile'))
            seen['kept'] = _leaves(user)[a].props.get('aria-checked')
            UserInteraction(user, {_leaves(user)[b]}, None).trigger('click')
            assert await _until(lambda: _leaves(user)[b].props.get('aria-checked') == 'true')
            UserInteraction(user, {_testid(user, 'shelf-picker-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['multi_class'] and seen['icon'] == ['check_box'] and seen['kept'] == 'true'
    # Codex on #94: a blank heading over the tick boxes, so "Shelfmark" stands over the shelfmarks
    assert seen['head'][0].startswith('<span class="stp-c" data-head="box"></span><span class="stp-c" data-head="shelfmark"')
    assert CONFIRMED == [[a, b]]


def test_a_failing_details_read_leaves_the_rows(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker

    def broken(sys_ids, lang='en', **_kw):
        CALLS.append(list(sys_ids))
        raise RuntimeError('down')
    monkeypatch.setattr(picker, 'row_details', broken)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            n = len(_leaves(user))
            UserInteraction(user, {_view_button(user, 'table')}, None).click()
            assert await _until(lambda: CALLS)
            await asyncio.sleep(0.2)
            seen['rows'] = (n, len(_with_class(user, 'stp-trow')))

    try:
        _run(go)
    finally:
        svc.close()
    n, drawn = seen['rows']
    assert n and drawn == n


def test_the_pane_shows_the_tiles_picture_without_asking_for_it_again(tmp_path, monkeypatch):
    """Owner, 2026-11-22: a picture the tiles have is the pane's at once (the same address: the browser
    serves it from its cache), not asked for again."""
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    asked = []

    def src(sys_id, shelfmark, width=320):
        asked.append((sys_id, width))
        return f'/img/{sys_id}?w={width}'
    monkeypatch.setattr(picker, '_thumbnail_src', src)
    svc = _setup(tmp_path)
    target = SYS['as2'][2]
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            UserInteraction(user, {_view_button(user, 'gallery')}, None).click()
            assert await _until(lambda: 'data-src' in _cells(user).get(target, ''))
            await asyncio.sleep(0.3)
            before = list(asked)
            UserInteraction(user, {_leaves(user)[target]}, None).trigger('click')
            assert await _until(lambda: any(e.props.get('src') == f'/img/{target}?w=300'
                                            for e in _els(user, 'src')))
            seen['asked'] = asked[len(before):]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['asked'] == []                                   # the pane asked for no address


def test_the_table_headings_have_grips_to_resize_the_columns(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-views-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _leaves(user))
            UserInteraction(user, {_view_button(user, 'table')}, None).click()
            assert await _until(lambda: _with_class(user, 'stp-thead'))
            seen['head'] = _with_class(user, 'stp-thead')[0].content

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['head'].count('class="stp-rsz"') == 8                 # one grip a column
    assert 'stpTableCols' in picker._RESIZE_JS and "localStorage" in picker._RESIZE_JS
