# -*- coding: utf-8 -*-
"""/lists: the page column, and edits that reach the right cloud row.

W1  The two helpers: a list item that is one page shows "<shelfmark> - Page N" (unless the
    web's own "Add page to list" already put that page in the shelfmark), and its Browse
    link opens that page -- only when the page is a number, because /browse takes
    ``page: int``. Rows from before the migration have no 'page' key: nothing changes.
W2  No web write sends ``page`` (inserts leave it NULL; note and tag edits are partial
    updates), so the website can never erase a page a desktop recorded. This is a guard:
    it held before this change too.
W3  An ordinary list's items are raw list_items rows (``id``, no ``item_id``). The page
    passed ``item.get('item_id')`` -- None -- to the note/tag edit and to Remove, whose
    ``int(item_id)`` raised, so every edit reported "could not be saved". Driven through
    the real page callbacks and the real UserListsManager down to a faked
    web.supabase_client: the edit and the removal address row 41. The recent list keeps
    its ``item_id``.

The page is driven with a MagicMock ``ui`` (as tests/test_lists_page_write_callbacks.py
does): no NiceGUI client, no network, no Supabase.
"""
from __future__ import annotations

import asyncio
import inspect
import types
from unittest.mock import MagicMock

import pytest

import web.pages.lists as lists_page
import web.supabase_client as supabase_client
import web.translations as web_translations
import web.user_lists as user_lists
from web.auth_state import GlobalAuthState
from shared.genizah_translations import TRANSLATIONS


@pytest.fixture
def lang(monkeypatch):
    """Set the web interface language for one test (it is process-global)."""
    def _set(code):
        monkeypatch.setattr(web_translations, '_current_lang', code)
    _set('en')
    return _set


# ---------------------------------------------------------------------------
# W1 -- the helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('page', [None, '', '   '])
def test_an_item_without_a_page_shows_its_shelfmark_unchanged(lang, page):
    assert lists_page.list_item_page_label('T-S 12.123', page) == 'T-S 12.123'
    assert lists_page.list_item_browse_url('990001', page) == '/browse?sys_id=990001'


def test_a_page_item_shows_page_n_and_browses_to_that_page(lang):
    assert lists_page.list_item_page_label('T-S 12.123', '3') == 'T-S 12.123 - Page 3'
    assert lists_page.list_item_page_label('T-S 12.123', 3) == 'T-S 12.123 - Page 3'
    assert lists_page.list_item_page_label('T-S 12.123', ' 12 ') == 'T-S 12.123 - Page 12'
    assert lists_page.list_item_browse_url('990001', '3') == '/browse?sys_id=990001&page=3'
    assert lists_page.list_item_browse_url('990001', 3) == '/browse?sys_id=990001&page=3'


def test_the_page_label_is_in_the_interface_language(lang):
    lang('he')
    assert lists_page.list_item_page_label('T-S 12.123', '3') == f"T-S 12.123 - {TRANSLATIONS['Page']} 3"


@pytest.mark.parametrize('shelfmark', [
    'T-S 12.123 - Page 3',        # web "Add page to list" in English
    'T-S 12.123 - עמוד 3',        # ... in Hebrew (today's translation of "Page")
    'T-S 12.123 - דף 3',          # ... in Hebrew (the older translation)
    'T-S 12.123 - Page 3  ',
])
def test_a_shelfmark_that_already_names_the_page_is_not_labelled_twice(lang, shelfmark):
    assert lists_page.list_item_page_label(shelfmark, '3') == shelfmark


@pytest.mark.parametrize('shelfmark', ['T-S 12.123 - Page 13', 'T-S 12.123 - Page 3a', 'T-S 3'])
def test_a_shelfmark_naming_another_page_still_gets_the_label(lang, shelfmark):
    assert lists_page.list_item_page_label(shelfmark, '3') == f'{shelfmark.rstrip()} - Page 3'


def test_an_unknown_page_is_labelled_but_browse_opens_the_first_page(lang):
    assert lists_page.list_item_page_label('T-S 12.123', 'Unknown') == 'T-S 12.123 - Page Unknown'
    assert lists_page.list_item_browse_url('990001', 'Unknown') == '/browse?sys_id=990001'
    assert lists_page.list_item_browse_url('990001', '٣') == '/browse?sys_id=990001'  # not an ASCII digit


def test_a_missing_shelfmark_with_a_page_shows_the_page_alone(lang):
    assert lists_page.list_item_page_label(None, '3') == 'Page 3'
    assert lists_page.list_item_page_label(None, None) is None


# ---------------------------------------------------------------------------
# W2 -- no web write carries page (guard)
# ---------------------------------------------------------------------------

class _RecordingClient:
    """A supabase client that records every list_items write and answers with one row."""

    def __init__(self):
        self.writes = []

    def table(self, name):
        client = self

        class _Query:
            def __init__(self):
                self.op = None
                self.payload = None
                self.filters = []

            def insert(self, payload):
                self.op, self.payload = 'insert', payload
                return self

            def update(self, payload):
                self.op, self.payload = 'update', payload
                return self

            def delete(self):
                self.op = 'delete'
                return self

            def eq(self, column, value):
                self.filters.append((column, value))
                return self

            def execute(self):
                client.writes.append((name, self.op, self.payload, list(self.filters)))
                row = dict(self.payload or {}, id=41)
                return types.SimpleNamespace(data=[row])

        return _Query()


@pytest.fixture
def signed_in(monkeypatch):
    monkeypatch.setattr(GlobalAuthState, 'is_logged_in', classmethod(lambda cls: True))
    monkeypatch.setattr(GlobalAuthState, 'get_user_id', classmethod(lambda cls: 'user-1'))


def test_no_web_list_item_write_sends_a_page(monkeypatch, signed_in):
    client = _RecordingClient()
    monkeypatch.setattr(supabase_client, 'get_user_client', lambda: client)
    mgr = user_lists.UserListsManager(None, None)

    assert supabase_client.add_list_item(7, '990001', shelfmark='T-S 1.1', fl_id='FL1', note='n')['success']
    # The manager's add_item accepts img= (the page) and must not pass it on.
    assert asyncio.run(mgr.add_item('990001', list_id='7', note='n', fl_id='FL1', img='3'))
    assert mgr.add_item_sync('990001', list_id='7', img='3')
    assert asyncio.run(mgr.update_item_note('41', 'a new note'))
    assert asyncio.run(mgr.update_item_tags('41', ['a', 'b']))

    kinds = [(op, table) for table, op, _payload, _filters in client.writes]
    assert kinds == [('insert', 'list_items')] * 3 + [('update', 'list_items')] * 2, kinds
    for _table, op, payload, _filters in client.writes:
        assert 'page' not in payload, (op, payload)
    updates = [(payload, filters) for _t, op, payload, filters in client.writes if op == 'update']
    assert updates == [({'note': 'a new note'}, [('id', 41)]), ({'tags': ['a', 'b']}, [('id', 41)])]


# ---------------------------------------------------------------------------
# W3 -- the card's edit and Remove reach the row id (driven through the page)
# ---------------------------------------------------------------------------

ORDINARY_LIST, RECENT_LIST = '7', '9'
ROW = {
    'id': 41, 'list_id': 7, 'sys_id': '990001', 'shelfmark': 'T-S 12.123', 'title': None,
    'fl_id': None, 'note': 'old note', 'tags': ['a'], 'page': '3',
}


class _FakeSupabase:
    """The web.supabase_client functions UserListsManager calls, as seen from web.user_lists."""

    def __init__(self):
        self.updates = []
        self.deletes = []

    def get_user_lists(self, user_id):
        return [
            {'id': 7, 'name': 'Research', 'name_en': 'Research', 'is_default': False, 'is_system': False},
            {'id': 9, 'name': 'Recently Viewed', 'name_en': 'Recently Viewed', 'is_system': True},
        ]

    def get_projects(self, user_id):
        return []

    def get_list_items(self, list_id, *, client=None):
        return [dict(ROW)] if list_id == 7 else []

    def get_recent_items(self, user_id, *, client=None):
        return [{'sys_id': '990002', 'shelfmark': 'ENA 1.2', 'title': '', 'fl_id': ''}]

    def update_list_item(self, item_id, data):
        self.updates.append((item_id, data))
        return {'success': True, 'item': dict(ROW, **data)}

    def delete_list_item(self, item_id):
        self.deletes.append(item_id)
        return {'success': True}


@pytest.fixture
def page(monkeypatch, signed_in, lang):
    """create_lists_page() over a MagicMock ui, a real UserListsManager and a fake Supabase."""
    fake_ui = MagicMock(name='ui')
    monkeypatch.setattr(lists_page, 'ui', fake_ui)
    import web.components.lists_write as runner_mod  # the write runner toasts through its own ui
    monkeypatch.setattr(runner_mod, 'ui', fake_ui)
    headings = []
    monkeypatch.setattr(lists_page, 'h1', MagicMock(name='h1'))
    monkeypatch.setattr(lists_page, 'h3', lambda text, **kw: headings.append(text) or MagicMock())
    selectors = []
    monkeypatch.setattr(lists_page, 'create_project_tree',
                        lambda **kw: selectors.append(kw['on_select']))
    monkeypatch.setattr(lists_page, '_load_list_item_counts', lambda: None)

    cloud = _FakeSupabase()
    for name in ('get_user_lists', 'get_projects', 'get_list_items', 'get_recent_items',
                 'update_list_item', 'delete_list_item'):
        monkeypatch.setattr(user_lists, name, getattr(cloud, name))
    mgr = user_lists.UserListsManager(None, None)
    monkeypatch.setattr(lists_page, 'state', types.SimpleNamespace(lists_mgr=mgr, meta_mgr=None))

    lists_page.create_lists_page()
    assert selectors, 'the page did not build its list sidebar'
    return types.SimpleNamespace(ui=fake_ui, cloud=cloud, headings=headings, select=selectors[0])


def _button_clicks(fake_ui, icon):
    """on_click of every icon-only button with this icon (the card's buttons have no text;
    the header's Trash button shares the 'delete' icon but has a label)."""
    return [c.kwargs['on_click'] for c in fake_ui.button.call_args_list
            if c.kwargs.get('icon') == icon and not c.args]


def _labelled_button_click(fake_ui, text):
    return next(c.kwargs['on_click'] for c in fake_ui.button.call_args_list
                if c.args and c.args[0] == text)


def _run(result):
    return asyncio.run(result) if inspect.isawaitable(result) else result


def _toasts(fake_ui):
    return [c.kwargs.get('type') for c in fake_ui.notify.call_args_list]


def test_an_ordinary_list_item_carries_its_row_id_and_shows_its_page(page):
    page.select(ORDINARY_LIST)

    (edit,) = _button_clicks(page.ui, 'edit')
    (remove,) = _button_clicks(page.ui, 'delete')
    assert edit.__defaults__[0] == '41', 'the edit button does not carry the row id'
    assert remove.__defaults__[0] == '41', 'the remove button does not carry the row id'

    assert 'T-S 12.123 - Page 3' in page.headings
    (browse,) = _button_clicks(page.ui, 'menu_book')
    browse()
    page.ui.navigate.to.assert_called_with('/browse?sys_id=990001&page=3')


def test_the_note_and_tag_edit_reach_the_row(page):
    page.select(ORDINARY_LIST)
    (edit,) = _button_clicks(page.ui, 'edit')
    edit()  # opens the edit dialog

    shown = [c.args[0] for c in page.ui.label.call_args_list if c.args]
    assert 'Item: T-S 12.123 - Page 3' in shown, shown

    # note_input = ui.textarea(...).classes(...).props(...); tags_input = ui.input(...) likewise
    page.ui.textarea.return_value.classes.return_value.props.return_value.value = 'new note'
    page.ui.input.return_value.classes.return_value.props.return_value.value = 'a, b'
    _run(_labelled_button_click(page.ui, 'Save')())

    assert page.cloud.updates == [(41, {'note': 'new note'}), (41, {'tags': ['a', 'b']})]
    assert 'negative' not in _toasts(page.ui), 'the edit reported "could not be saved"'
    assert 'positive' in _toasts(page.ui)


def test_remove_reaches_the_row(page):
    page.select(ORDINARY_LIST)
    (remove,) = _button_clicks(page.ui, 'delete')
    _run(remove())

    assert page.cloud.deletes == [41]
    assert 'negative' not in _toasts(page.ui)


def test_the_recent_list_still_carries_its_item_id(page):
    page.select(RECENT_LIST)
    (edit,) = _button_clicks(page.ui, 'edit')
    assert edit.__defaults__[0] == '990002'
    assert _button_clicks(page.ui, 'delete') == [], 'the recent list offers no Remove'
    (browse,) = _button_clicks(page.ui, 'menu_book')
    browse()
    page.ui.navigate.to.assert_called_with('/browse?sys_id=990002')


def test_a_row_from_before_the_migration_shows_what_it_showed_before(page, monkeypatch):
    before = {k: v for k, v in ROW.items() if k != 'page'}
    monkeypatch.setattr(user_lists, 'get_list_items',
                        lambda list_id, *, client=None: [dict(before)])
    page.select(ORDINARY_LIST)

    assert 'T-S 12.123' in page.headings
    assert not any('Page' in h for h in page.headings if isinstance(h, str)), page.headings
    (browse,) = _button_clicks(page.ui, 'menu_book')
    browse()
    page.ui.navigate.to.assert_called_with('/browse?sys_id=990001')
