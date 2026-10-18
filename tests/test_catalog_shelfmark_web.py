# -*- coding: utf-8 -*-
"""Web pickers for shelfmarks that name several records (catalogue-shelfmark plan, gate 10).

Rows H (reading desk), I (Joins Lab) and J (Fragment Puzzle) of
docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md, driven through NiceGUI's User simulation
(no index, no network): two results open the picker; choosing a card adds or loads
that record; closing the picker does nothing; a lookup with no answer says nothing;
the Joins Lab `?shelfmark=` deep link still loads the first match (owner question 3).
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'catalogue-shelfmark-web-secret-0123456789abcdef')

from tests.test_web_io_bound_none_findings import (  # noqa: E402
    _ANCHOR_BOX, _click, _puzzle_io_bound, _puzzle_patches, _run_puzzle,
)
from tests.test_web_search_within_cutoff import _label_texts, _wait_for  # noqa: E402
from tests.test_web_variant_settings_per_visitor import _element, run, server  # noqa: E402,F401

ONE, TWO = '990000000000000011', '990000000000000012'


@pytest.fixture(autouse=True)
def _own_wait_pool(monkeypatch):
    """The app lifespan shuts the research wait pool down on exit. Without this, a pool an
    earlier test made is left closed, and every later /api test in the process answers 500
    ("cannot schedule new futures after shutdown"); this file sorts before them."""
    from web import research_jobs
    monkeypatch.setattr(research_jobs, '_wait_executor', None)


def _manuscripts():
    return [SimpleNamespace(sys_id=ONE, shelfmark='Moss. VIII,402.1', title='', library_code='Mosseri'),
            SimpleNamespace(sys_id=TWO, shelfmark='Moss. VIII,402.2', title='', library_code='Mosseri')]


def _cards(user):
    from nicegui import ui
    with user._client:
        return [e for e in user._client.elements.values()
                if isinstance(e, ui.card) and e.props.get('data-sys-id')]


def _close_picker(user):
    from nicegui import ui
    with user._client:
        for e in list(user._client.elements.values()):
            if isinstance(e, ui.button) and e.props.get('icon') == 'close' and any(
                    isinstance(p, ui.dialog) for p in _ancestors(e)):
                _click(user, e)
                return
    raise AssertionError('no picker close button')


def _ancestors(element):
    parent = element.parent_slot.parent if element.parent_slot else None
    while parent is not None:
        yield parent
        parent = parent.parent_slot.parent if parent.parent_slot else None


# ---------------------------------------------------------------- I: Joins Lab

def _joins_lab(monkeypatch):
    from web.pages import joins_lab
    monkeypatch.setattr(joins_lab, 'get_service', lambda: SimpleNamespace(
        search_by_shelfmark=lambda q, limit=20: (_manuscripts(), False)))

    class FakeViewer:
        def __init__(self, **kwargs):
            pass

        async def update_content(self, **kwargs):
            return None
    monkeypatch.setattr(joins_lab, 'AnchorViewer', FakeViewer)
    monkeypatch.setattr(joins_lab, 'write_anchor', lambda *a, **k: True)


@pytest.mark.parametrize('choice', ['second', 'close'])
def test_joins_lab_two_matches_open_the_picker(server, monkeypatch, choice):  # noqa: F811
    from nicegui import ui
    _joins_lab(monkeypatch)
    shown = {}

    async def driver(a, b):
        await a.open('/joins-lab')
        box = _element(a, ui.input, lambda e: e.props.get('placeholder') == _ANCHOR_BOX)
        with a._client:
            box.value = 'Mosseri, Jacques, Ms. VIII.402'
        _click(a, _element(a, ui.button, lambda e: e.text == 'Load Anchor'))
        await _wait_for(lambda: len(_cards(a)) == 2)
        shown['cards'] = [c.props['data-sys-id'] for c in _cards(a)]
        if choice == 'second':
            _click(a, _cards(a)[1])
            await _wait_for(lambda: TWO in _label_texts(a))
        else:
            _close_picker(a)
            await asyncio.sleep(0.3)
        shown['texts'] = _label_texts(a)

    run(driver)
    assert shown['cards'] == [ONE, TWO]
    if choice == 'second':
        assert TWO in shown['texts'] and ONE not in shown['texts']
    else:
        assert ONE not in shown['texts'] and TWO not in shown['texts']
    assert not any('Fragment not found' in t for t in shown['texts'])


def test_joins_lab_deep_link_still_loads_the_first_match(server, monkeypatch):  # noqa: F811
    _joins_lab(monkeypatch)
    shown = {}

    async def driver(a, b):
        await a.open('/joins-lab?shelfmark=Mosseri%2C%20Jacques%2C%20Ms.%20VIII.402')
        await _wait_for(lambda: ONE in _label_texts(a))
        shown['cards'] = _cards(a)

    run(driver)
    assert shown['cards'] == []          # no picker on the deep link


# ---------------------------------------------------------------- J: Fragment Puzzle

def _puzzle_meta(monkeypatch, answer):
    from web.pages import puzzle
    from web.state import state

    def resolve_system_by_shelfmark(text):
        return answer(text)
    monkeypatch.setattr(state, 'meta_mgr', SimpleNamespace(
        resolve_system_by_shelfmark=resolve_system_by_shelfmark, get_library_for_id=lambda sid: 'Mosseri',
        csv_bank={}, nli_cache={}))
    added = []

    async def fake_add(sys_id, shelfmark, *_a):
        added.append((sys_id, shelfmark))
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', fake_add)
    return added


async def _puzzle_add(user, text):
    from nicegui import ui
    await user.open('/puzzle')
    box = _element(user, ui.input, lambda e: e.props.get('placeholder') == 'Enter shelfmark...')
    with user._client:
        box.value = text
    _click(user, _element(user, ui.button, lambda e: e.props.get('icon') == 'add'))
    return box


def test_puzzle_two_matches_open_the_picker_and_add_the_chosen_one(monkeypatch):
    options = [{'sys_id': m.sys_id, 'shelfmark': m.shelfmark, 'title': ''} for m in _manuscripts()]
    added = _puzzle_meta(monkeypatch, lambda t: {'sys_id': None, 'options': options, 'selected_shelfmark': None})
    shown = {}

    async def driver(a, b):
        box = await _puzzle_add(a, 'Mosseri, Jacques, Ms. VIII.402')
        await _wait_for(lambda: len(_cards(a)) == 2)
        _click(a, _cards(a)[1])
        await _wait_for(lambda: added)
        shown['box'] = box.value
        shown['messages'] = list(a.notify.messages)

    _run_puzzle(driver, _puzzle_patches())
    assert added == [(TWO, 'Moss. VIII,402.2')]
    assert shown['box'] == ''
    assert 'Shelfmark not found' not in shown['messages']


def test_puzzle_closed_picker_adds_nothing(monkeypatch):
    options = [{'sys_id': m.sys_id, 'shelfmark': m.shelfmark, 'title': ''} for m in _manuscripts()]
    added = _puzzle_meta(monkeypatch, lambda t: {'sys_id': None, 'options': options, 'selected_shelfmark': None})
    shown = {}

    async def driver(a, b):
        box = await _puzzle_add(a, 'Mosseri, Jacques, Ms. VIII.402')
        await _wait_for(lambda: len(_cards(a)) == 2)
        _close_picker(a)
        await asyncio.sleep(0.3)
        shown['box'] = box.value

    _run_puzzle(driver, _puzzle_patches())
    assert added == [] and shown['box'] == 'Mosseri, Jacques, Ms. VIII.402'


def test_puzzle_lookup_with_no_answer_says_nothing(monkeypatch):
    added = _puzzle_meta(monkeypatch, lambda t: pytest.fail('the lookup must not run'))
    _puzzle_io_bound(monkeypatch, lambda fn: 'none' if getattr(fn, '__name__', '') == 'resolve_system_by_shelfmark'
                     else 'run')
    shown = {}

    async def driver(a, b):
        await _puzzle_add(a, 'T-S K 08.026')
        await asyncio.sleep(0.5)
        shown['messages'] = list(a.notify.messages)

    _run_puzzle(driver, _puzzle_patches())
    assert added == [] and 'Shelfmark not found' not in shown['messages']


# ---------------------------------------------------------------- H: reading desk

def test_reading_desk_add_opens_the_picker_and_adds_the_chosen_one(server):  # noqa: F811
    from web.pages.browse import add_to_reading_desk_by_shelfmark
    service = SimpleNamespace(search_by_shelfmark=lambda q, limit=20: (_manuscripts(), False))
    single = SimpleNamespace(search_by_shelfmark=lambda q, limit=20: (_manuscripts()[:1], True))
    empty = SimpleNamespace(search_by_shelfmark=lambda q, limit=20: ([], False))
    added, shown = [], {}

    async def driver(a, b):
        await a.open('/joins-lab')
        with a._client:
            shown['two'] = await add_to_reading_desk_by_shelfmark(
                service, 'Mosseri, Jacques, Ms. VIII.402', a._client.layout, lambda s, sm: added.append((s, sm)))
        assert added == []                       # nothing until the user chooses
        await _wait_for(lambda: len(_cards(a)) == 2)
        _click(a, _cards(a)[1])
        await _wait_for(lambda: added)
        with a._client:
            shown['one'] = await add_to_reading_desk_by_shelfmark(
                single, 'Moss. VIII,402.1', a._client.layout, lambda s, sm: added.append((s, sm)))
            shown['none'] = await add_to_reading_desk_by_shelfmark(
                empty, 'nothing 999', a._client.layout, lambda s, sm: added.append((s, sm)))

    run(driver)
    assert shown == {'two': True, 'one': True, 'none': False}
    assert added == [(TWO, 'Moss. VIII,402.2'), (ONE, 'Moss. VIII,402.1')]


def test_reading_desk_closed_picker_adds_nothing(server):  # noqa: F811
    from web.pages.browse import add_to_reading_desk_by_shelfmark
    service = SimpleNamespace(search_by_shelfmark=lambda q, limit=20: (_manuscripts(), False))
    added, shown = [], {}

    async def driver(a, b):
        await a.open('/joins-lab')
        with a._client:
            shown['found'] = await add_to_reading_desk_by_shelfmark(
                service, 'Mosseri, Jacques, Ms. VIII.402', a._client.layout, lambda s, sm: added.append((s, sm)))
        await _wait_for(lambda: len(_cards(a)) == 2)
        _close_picker(a)
        await asyncio.sleep(0.3)

    run(driver)
    assert shown == {'found': True} and added == []   # the toolbar clears its box only on an add


def test_reading_desk_toolbar_awaits_the_lookup():
    """The reading desk exists only inside a loaded Browse session, so its event chain
    is pinned here: Enter and Add both run rd_toolbar_add, which must AWAIT the async
    toolbar_add_by_shelfmark (a sync caller would drop the coroutine and add nothing)."""
    import web.pages.browse as browse
    tree = ast.parse(inspect.getsource(browse))
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for name, callee in (('rd_toolbar_add', 'toolbar_add_by_shelfmark'),
                         ('toolbar_add_by_shelfmark', 'add_to_reading_desk_by_shelfmark')):
        fn = funcs[name]
        assert isinstance(fn, ast.AsyncFunctionDef), name
        awaited = {c.value.func.id for c in ast.walk(fn)
                   if isinstance(c, ast.Await) and isinstance(c.value, ast.Call)
                   and isinstance(c.value.func, ast.Name)}
        assert callee in awaited, (name, callee)
