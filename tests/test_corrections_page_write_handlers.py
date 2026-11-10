# -*- coding: utf-8 -*-
"""/corrections page: deleting a draft, deleting a comment and editing a comment.

Before the fix, the three click handlers inside ``create_corrections_page``
wrote through the anonymous ``get_client()`` and toasted success
unconditionally, so a change RLS rejected (0 rows) looked like it worked.

The handlers now call module-level actions (``_delete_correction_action``,
``_delete_comment_action``, ``_save_comment_action``) that go through the
row-checking helpers in web/supabase_client.py. These tests drive the actions
with ``ui`` replaced by a MagicMock, and pin the page closures to them
(test the call sites, not only the helpers).
"""

import ast
import inspect
from unittest.mock import MagicMock

import pytest

corrections = pytest.importorskip('web.pages.corrections')

NO_ROWS = {'error': 'Nothing was deleted', 'no_rows': True}


@pytest.fixture
def fake_ui(monkeypatch):
    ui = MagicMock(name='ui')
    monkeypatch.setattr(corrections, 'ui', ui)
    return ui


def _notify_types(ui):
    return [c.kwargs.get('type') for c in ui.notify.call_args_list]


def _notify_messages(ui):
    return [c.args[0] if c.args else c.kwargs.get('message') for c in ui.notify.call_args_list]


def test_delete_correction_zero_rows_shows_error_toast_and_does_not_reload(fake_ui, monkeypatch):
    helper = MagicMock(return_value=NO_ROWS)
    monkeypatch.setattr(corrections, 'sb_delete_correction', helper)
    corrections._delete_correction_action(5)
    helper.assert_called_once_with(5)
    assert 'negative' in _notify_types(fake_ui)
    assert 'positive' not in _notify_types(fake_ui)
    assert not fake_ui.navigate.reload.called
    # the helper's English log string is never shown to the user
    assert 'Nothing was deleted' not in _notify_messages(fake_ui)

    fake_ui.reset_mock()
    monkeypatch.setattr(corrections, 'sb_delete_correction', MagicMock(return_value={'success': True}))
    corrections._delete_correction_action(5)
    assert _notify_types(fake_ui) == ['positive']
    assert fake_ui.navigate.reload.call_count == 1


def test_delete_comment_zero_rows_shows_error_toast(fake_ui, monkeypatch):
    helper = MagicMock(return_value=NO_ROWS)
    monkeypatch.setattr(corrections, 'sb_delete_comment', helper)
    dialog = MagicMock(name='confirm_dialog')
    corrections._delete_comment_action(7, dialog)
    helper.assert_called_once_with(7)
    assert 'negative' in _notify_types(fake_ui)
    assert 'positive' not in _notify_types(fake_ui)
    assert not fake_ui.navigate.reload.called
    assert dialog.close.called

    fake_ui.reset_mock()
    dialog = MagicMock(name='confirm_dialog')
    monkeypatch.setattr(corrections, 'sb_delete_comment', MagicMock(return_value={'success': True}))
    corrections._delete_comment_action(7, dialog)
    assert _notify_types(fake_ui) == ['positive']
    assert fake_ui.navigate.reload.call_count == 1
    assert dialog.close.called


def test_save_comment_zero_rows_shows_error_toast_and_keeps_dialog(fake_ui, monkeypatch):
    helper = MagicMock(return_value={'error': 'Update failed', 'no_rows': True})
    monkeypatch.setattr(corrections, 'sb_update_comment', helper)
    dialog = MagicMock(name='edit_dialog')
    corrections._save_comment_action(7, 'new text', dialog)
    helper.assert_called_once_with(7, 'new text')
    assert 'negative' in _notify_types(fake_ui)
    assert 'positive' not in _notify_types(fake_ui)
    assert not dialog.close.called, "the edit dialog must stay open so the user keeps the text"
    assert not fake_ui.navigate.reload.called

    fake_ui.reset_mock()
    dialog = MagicMock(name='edit_dialog')
    monkeypatch.setattr(corrections, 'sb_update_comment', MagicMock(return_value={'success': True}))
    corrections._save_comment_action(7, 'new text', dialog)
    assert _notify_types(fake_ui) == ['positive']
    assert dialog.close.called
    assert fake_ui.navigate.reload.call_count == 1


def test_zero_rows_and_other_errors_show_different_fixed_messages(fake_ui, monkeypatch):
    """0 rows means 'nothing changed / maybe no permission'; a raised error is a connection-type failure."""
    monkeypatch.setattr(corrections, 'sb_delete_correction', MagicMock(return_value=NO_ROWS))
    corrections._delete_correction_action(5)
    monkeypatch.setattr(corrections, 'sb_delete_correction', MagicMock(return_value={'error': 'boom'}))
    corrections._delete_correction_action(5)
    first, second = _notify_messages(fake_ui)
    assert first != second
    assert 'boom' not in second


ROUTED_ACTIONS = ('_delete_correction_action', '_delete_comment_action', '_save_comment_action')
_FUNC_TYPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


def _routed_action_calls(page_fn):
    """Names of the routed actions CALLED from a closure nested inside ``page_fn``.

    Parsed with ast, so a comment or a string that merely names an action does
    not count, and a call made directly in the page body (not from a click
    handler) does not count either.
    """
    parents = {}
    for node in ast.walk(page_fn):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    found = set()
    for node in ast.walk(page_fn):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in ROUTED_ACTIONS):
            continue
        cur = parents.get(node)
        while cur is not None and not isinstance(cur, _FUNC_TYPES):
            cur = parents.get(cur)
        if cur is not None and cur is not page_fn:
            found.add(node.func.id)
    return found


def _page_fn(source, name='create_corrections_page'):
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError('%s not found' % name)


def test_page_closures_route_through_the_actions():
    fn = _page_fn(inspect.getsource(corrections))
    missing = set(ROUTED_ACTIONS) - _routed_action_calls(fn)
    assert not missing, "create_corrections_page's click handlers do not call %s" % sorted(missing)
    anon = [n.lineno for n in ast.walk(fn) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name) and n.func.id == 'get_client']
    assert anon == [], "a /corrections page handler still uses the anonymous client (lines %r)" % anon


@pytest.mark.parametrize('snippet,expected', [
    ("async def create_corrections_page():\n"
     "    def on_click():\n"
     "        _delete_correction_action(1)\n"
     "        _delete_comment_action(1, d)\n"
     "        _save_comment_action(1, 't', d)\n", set(ROUTED_ACTIONS)),
    # comments and strings name the actions but call nothing
    ("async def create_corrections_page():\n"
     "    def on_click():\n"
     "        # _delete_correction_action(1)\n"
     "        s = '_delete_comment_action(1, d)'\n", set()),
    # called in the page body, not from a click handler
    ("async def create_corrections_page():\n"
     "    _save_comment_action(1, 't', d)\n", set()),
])
def test_routed_action_detector(snippet, expected):
    """Regression guard, green before and after: proves the call-site pin can fail."""
    assert _routed_action_calls(_page_fn(snippet)) == expected
