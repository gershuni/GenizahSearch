# -*- coding: utf-8 -*-
"""Pick from the shelf in Share a Discovery and in the comment's shelfmark reference (shelf tree
picker plan §3.5, §6, §10.4; PR 7, web half).

Call sites, not the picker: on test pages in a NiceGUI User simulation, each host's "Pick from the
shelf" button is clicked with the picker replaced by a stand-in that answers one selection, and the
test checks that the selection reached the SAME handler the host's list path uses and what the host
then saves: the linked document of a new and of an edited discovery, an additional shelfmark that
keeps its manuscript id (and loses it when its text is typed over), and the comment's
``[[shelfmark:…|id:…]]`` mention, without the comment being submitted. With the shelf tree not
served no host shows the button; a LOCAL ``97…`` selection and a closed picker change nothing.
The harness is the one tests/render_smoke/test_search_url_open_dispatch.py built. The test pages
open the real dialogs directly (the feed's Share button asks for a login before it opens one).
"""
from __future__ import annotations

import asyncio
import time

import pytest
from nicegui import background_tasks, ui
from nicegui.testing.user_interaction import UserInteraction

from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run

PICKED_SYS = '990000020205171'
PICK = {'sys_id': PICKED_SYS, 'shelfmark': 'T-S AS 2.2', 'placement_id': 'inv:2002'}
OPENED: list = []      # (start, title) of each picker opened
ANSWER: dict = {'selection': PICK}
SAVED: dict = {}
EDIT_ITEM = {'document_id': '990000010205171', 'shelfmark': 'T-S AS 2.1', 'page_number': 1,
             'title': 'An edited discovery', 'content_preview': 'Text'}
TESTIDS = ('discovery-shelf-pick', 'discovery-additional-shelf-pick', 'discovery-edit-shelf-pick',
           'comment-shelf-pick')
ENTRY_PLACEHOLDER = 'T-S 13J1.1'


WORDS: list = []


def _fake_picker(container, service, on_pick, start=None, title=None, pick_label=None, **_kw):
    """The shelf picker's stand-in: records where it opened and the word on its button, then
    answers in the client's context (as the real dialog's button does), or, with no answer, is
    closed without a choice."""
    OPENED.append((start, title))
    WORDS.append(pick_label)
    if ANSWER['selection'] is not None:
        selection = dict(ANSWER['selection'])

        async def answer():
            with container:
                await on_pick(selection)
        background_tasks.create(answer(), name='fake-shelf-pick')


@ui.page('/_test/discovery-create')
def _create_page():
    from web.pages.discoveries import create_new_discovery_dialog
    ui.label('host')
    create_new_discovery_dialog().open()


@ui.page('/_test/discovery-edit')
def _edit_page():
    from web.pages.discoveries import open_edit_discovery_dialog
    ui.label('host')
    open_edit_discovery_dialog('7', dict(EDIT_ITEM))


@ui.page('/_test/comment')
def _comment_page():
    from web.components.comment_dialog import create_comment_dialog
    ui.label('host')
    create_comment_dialog(document_id='990000030205171', page_number=1, shelfmark='T-S AS 2.3').open()


@pytest.fixture
def served(monkeypatch):
    """The shelf tree served, the picker replaced, a signed-in user, the writes recorded."""
    from web.components import shelf_pick as spb
    from web.pages import discoveries
    monkeypatch.setattr(spb, 'shelf_tree_service', lambda: object())
    monkeypatch.setattr(spb, 'open_shelf_picker', _fake_picker)
    monkeypatch.setattr(discoveries.GlobalAuthState, 'get_user_id', staticmethod(lambda: 'user-1'))
    monkeypatch.setattr(discoveries.GlobalAuthState, 'is_logged_in', staticmethod(lambda: True))

    def _create(**kwargs):
        SAVED['create'] = kwargs
        return {'success': True}

    def _update(discovery_id, data):
        SAVED['update'] = (discovery_id, data)
        return {'success': True}
    monkeypatch.setattr(discoveries, 'create_discovery', _create)
    monkeypatch.setattr(discoveries, 'update_discovery', _update)
    OPENED.clear()
    WORDS.clear()
    SAVED.clear()
    ANSWER['selection'] = PICK
    yield


def _all(user):
    with user.client:
        return list(user.client.elements.values())


def _by_testid(user, testid):
    return [e for e in _all(user) if e.props.get('data-testid') == testid]


def _button(user, text):
    return next(e for e in _all(user) if isinstance(e, ui.button) and getattr(e, 'text', '') == text)


def _fill_title_and_description(user):
    title, content = [e for e in _all(user) if isinstance(e, (ui.input, ui.textarea))
                      and 'hidden' not in e.classes][:2]
    title.value, content.value = 'A find', 'Pieces of one letter'


async def _until(predicate, timeout=5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def _open(user, path):
    await _get_page(user, path)
    await _connect(user)


def _click(user, element):
    UserInteraction(user, {element}, None).click()


def test_share_a_discovery_links_the_picked_document_through_select_document(served):
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _open(user, '/_test/discovery-create')
            hidden = [e for e in _all(user) if isinstance(e, ui.input) and 'hidden' in e.classes]
            _click(user, _by_testid(user, 'discovery-shelf-pick')[0])
            assert await _until(lambda: hidden[0].value == PICKED_SYS), 'the pick did not reach select_document'
            seen['hidden'] = [h.value for h in hidden]
            seen['labels'] = [e.text for e in _all(user) if isinstance(e, ui.label)]
            _fill_title_and_description(user)
            _click(user, _button(user, 'Share'))
            assert await _until(lambda: 'create' in SAVED)

    _run(go)
    assert OPENED == [(None, None)]                              # nothing linked yet: the libraries
    assert WORDS == ['Select']                                   # the picker's button (handoff DX21)
    assert seen['hidden'] == [PICKED_SYS, 'T-S AS 2.2']          # doc_id_input, shelfmark_hidden
    assert f'ID: {PICKED_SYS}' in seen['labels'] and 'T-S AS 2.2' in seen['labels']
    assert SAVED['create']['document_id'] == PICKED_SYS and SAVED['create']['shelfmark'] == 'T-S AS 2.2'


def test_an_additional_shelfmark_keeps_the_picked_id_until_its_text_is_edited(served):
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _open(user, '/_test/discovery-create')
            button = _by_testid(user, 'discovery-additional-shelf-pick')[0]

            def entry_inputs():
                return [e for e in _all(user)
                        if isinstance(e, ui.input) and e.props.get('placeholder') == ENTRY_PLACEHOLDER]

            def card_of(entry_input):
                return entry_input.parent_slot.parent.parent_slot.parent

            async def pick(selection, count):
                ANSWER['selection'] = selection
                _click(user, button)
                assert await _until(lambda: len(entry_inputs()) == count)

            _click(user, _button(user, 'Add shelfmark'))                  # A: typed, removed first
            assert await _until(lambda: len(entry_inputs()) == 1)
            await pick(PICK, 2)                                           # B: picked, kept
            await pick({'sys_id': '990000040205171', 'shelfmark': 'T-S AS 2.4', 'placement_id': 'inv:2004'}, 3)
            await pick(PICK, 4)                                           # D: the same as B, removed
            _click(user, _button(user, 'Add shelfmark'))                  # E: typed
            assert await _until(lambda: len(entry_inputs()) == 5)
            a, b, c, d, e = entry_inputs()
            seen['prefilled'] = [x.value for x in (a, b, c, d, e)]
            a.value = 'T-S 1.1'
            c.value = 'T-S AS 2.44'                                       # C: typed over
            e.value = 'T-S NS 1.1'
            for gone in (a, d):    # an earlier entry goes first; D (equal to B) must go, not B or E
                close = next(x for x in card_of(gone).descendants()
                             if isinstance(x, ui.button) and x.props.get('icon') == 'close')
                _click(user, close)
            page_b = next(x for x in card_of(b).descendants() if isinstance(x, ui.number))
            page_b.value = 3                                              # a surviving row is still its own
            _fill_title_and_description(user)
            _click(user, _button(user, 'Share'))
            assert await _until(lambda: 'create' in SAVED)

    _run(go)
    assert seen['prefilled'] == ['', 'T-S AS 2.2', 'T-S AS 2.4', 'T-S AS 2.2', '']
    assert SAVED['create']['additional_shelfmarks'] == [
        {'shelfmark': 'T-S AS 2.2', 'document_id': PICKED_SYS, 'page_number': 3},
        {'shelfmark': 'T-S AS 2.44', 'document_id': '', 'page_number': None},      # typed over: id dropped
        {'shelfmark': 'T-S NS 1.1', 'document_id': '', 'page_number': None},       # typed (was never kept)
    ]


def test_the_picked_id_is_in_what_create_discovery_inserts(monkeypatch):
    # The JSONB `shelfmarks` array the insert writes, with a fake client: the id is kept there.
    from web import supabase_client
    inserted = {}

    class _Table:
        def insert(self, data):
            inserted.update(data)
            return self

        def execute(self):
            return type('R', (), {'data': [dict(inserted, id=1)]})()

    monkeypatch.setattr(supabase_client, 'get_user_client', lambda: type('C', (), {'table': lambda self, n: _Table()})())
    result = supabase_client.create_discovery(
        user_id='user-1', title='A find', content='Text', document_id=PICKED_SYS, shelfmark='T-S AS 2.2',
        page_number=1, additional_shelfmarks=[{'shelfmark': 'T-S AS 2.4', 'document_id': '990000040205171',
                                               'page_number': None}])
    assert result.get('success')
    assert inserted['shelfmarks'] == [
        {'document_id': PICKED_SYS, 'shelfmark': 'T-S AS 2.2', 'page_number': 1},
        {'shelfmark': 'T-S AS 2.4', 'document_id': '990000040205171', 'page_number': None},
    ]


def test_editing_a_discovery_relinks_through_its_own_select_document(served):
    async def go():
        async with _app(object()) as user:
            await _open(user, '/_test/discovery-edit')
            _click(user, _by_testid(user, 'discovery-edit-shelf-pick')[0])
            assert await _until(lambda: any(isinstance(e, ui.label) and e.text == 'T-S AS 2.2' for e in _all(user)))
            _click(user, _button(user, 'Save'))
            assert await _until(lambda: 'update' in SAVED)

    _run(go)
    assert OPENED == [(EDIT_ITEM['document_id'], None)]          # opens at the linked manuscript
    assert WORDS == ['Select']
    discovery_id, data = SAVED['update']
    assert discovery_id == 7 and data['document_id'] == PICKED_SYS and data['shelfmark'] == 'T-S AS 2.2'


def test_the_comment_gets_the_mention_and_is_not_submitted(served, monkeypatch):
    from web.components import comment_dialog
    submitted = []
    monkeypatch.setattr(comment_dialog, 'create_comment', lambda **kw: submitted.append(kw) or {'id': 1})
    monkeypatch.setattr(comment_dialog.GlobalAuthState, 'is_logged_in', staticmethod(lambda: True))
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _open(user, '/_test/comment')
            text = next(e for e in _all(user) if isinstance(e, ui.textarea))
            text.value = 'See'
            seen['submit_enabled'] = 'disable' not in _button(user, 'Submit').props
            _click(user, _by_testid(user, 'comment-shelf-pick')[0])
            assert await _until(lambda: 'shelfmark:' in (text.value or ''))
            await asyncio.sleep(0.1)
            seen['text'] = text.value
            seen['open'] = [d for d in _all(user) if isinstance(d, ui.dialog) and d.value]

    _run(go)
    assert seen['submit_enabled']                                # signed in: submitting was possible
    assert OPENED == [('990000030205171', None)]                 # opens at the commented manuscript
    assert WORDS == ['Add shelfmark reference']                  # the host's own words (DX21)
    assert seen['text'] == f'See [[shelfmark:T-S AS 2.2|id:{PICKED_SYS}]] '
    assert submitted == [] and len(seen['open']) == 1            # not submitted; the dialog stays open


def test_the_list_paths_and_the_picker_insert_the_same_mention():
    from types import SimpleNamespace

    from web.components.comment_dialog import insert_shelfmark_mention
    box = SimpleNamespace(value=None)
    insert_shelfmark_mention(box, 'T-S AS 2.2', PICKED_SYS)
    insert_shelfmark_mention(box, 'Ms. 3', '990000403020517')
    assert box.value == f' [[shelfmark:T-S AS 2.2|id:{PICKED_SYS}]]  [[shelfmark:Ms. 3|id:990000403020517]] '


@pytest.mark.parametrize('path', ['/_test/discovery-create', '/_test/discovery-edit', '/_test/comment'])
def test_no_button_without_the_shelf_tree(path, monkeypatch):
    # SHELFMARK_TREE_ENABLED on, the file missing: shelf_tree_service() answers None.
    from web.components import shelf_pick as spb
    monkeypatch.setattr(spb, 'shelf_tree_service', lambda: None)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _open(user, path)
            seen['ids'] = [e.props.get('data-testid') for e in _all(user) if e.props.get('data-testid') in TESTIDS]
            seen['host'] = any(isinstance(e, ui.button) for e in _all(user))

    _run(go)
    assert seen['host'] and seen['ids'] == []


def test_a_local_id_and_a_closed_picker_change_nothing(served):
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _open(user, '/_test/discovery-create')
            hidden = [e for e in _all(user) if isinstance(e, ui.input) and 'hidden' in e.classes]
            ANSWER['selection'] = {'sys_id': '970000000000001', 'shelfmark': 'My scan', 'placement_id': 'x'}
            _click(user, _by_testid(user, 'discovery-shelf-pick')[0])
            _click(user, _by_testid(user, 'discovery-additional-shelf-pick')[0])
            ANSWER['selection'] = None                                   # closed without a choice
            _click(user, _by_testid(user, 'discovery-shelf-pick')[0])
            await asyncio.sleep(0.3)
            seen['hidden'] = [h.value for h in hidden]
            seen['entries'] = [e for e in _all(user)
                               if isinstance(e, ui.input) and e.props.get('placeholder') == ENTRY_PLACEHOLDER]

    _run(go)
    assert len(OPENED) == 3
    assert seen['hidden'] == ['', ''] and seen['entries'] == []


def test_only_a_corpus_selection_is_taken():
    from web.components.shelf_pick import corpus_selection
    assert corpus_selection(PICK) is not None
    assert corpus_selection({'sys_id': '970000000000001'}) is None
    assert corpus_selection({'sys_id': ''}) is None and corpus_selection(None) is None
    assert corpus_selection({'sys_id': f'x{PICKED_SYS}'}) is None
