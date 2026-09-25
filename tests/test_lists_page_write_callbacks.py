# -*- coding: utf-8 -*-
"""/lists write callbacks: awaited, re-checked for sign-in, and honest about the result.

The page returns a sign-in state for anonymous visitors, but that check runs
once, at render. A signed-in session can end while the page stays open, and
UserListsManager then returns None/False for every write. Before this fix the
callbacks ignored those results and still showed "List created", "List
deleted", "Item updated" and so on. Worse, three callbacks called ASYNC manager
methods without awaiting them (the inline list rename, the item note/tag edit,
and the remove-item fallback), so for signed-in users those writes never ran at
all while the page reported success.

The contract pinned here:

* ``_run_lists_write`` re-checks sign-in, awaits the write, and toasts a
  failure when the write reports nothing done (None/False) or raises.
* Every lists-manager write in web/pages/lists.py goes through it: the method
  call is the body of a lambda handed to ``_run_lists_write``, every
  ``_run_lists_write`` call is awaited, and its result is tested (``if not x:``
  / ``if x is None:`` followed by a return) before anything reports success.
* No ``*_sync`` write fallbacks remain on the page.
* The inline rename (``create_inline_edit_label``) awaits the rename and keeps
  the old name on failure.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path
from unittest.mock import MagicMock

import pytest

LISTS_PY = Path(__file__).resolve().parent.parent / 'web' / 'pages' / 'lists.py'

WRITE_METHODS = {
    'create_list', 'update_list', 'update_list_project', 'delete_list',
    'restore_list', 'permanently_delete_list', 'empty_trash', 'add_item',
    'remove_item_from_list', 'update_item_note', 'update_item_tags',
    'create_project', 'update_project', 'delete_project', 'move_list_to_project',
}
SYNC_WRITE_METHODS = {
    'create_list_sync', 'add_item_sync', 'remove_item_from_list_sync',
    'create_project_sync',
}


@pytest.fixture
def lists_page(monkeypatch):
    mod = pytest.importorskip('web.pages.lists')
    fake_ui = MagicMock()
    monkeypatch.setattr(mod, 'ui', fake_ui)
    return mod


def _signed_in(monkeypatch, mod, value: bool):
    monkeypatch.setattr(mod.GlobalAuthState, 'is_logged_in', staticmethod(lambda: value))


def _toast_types(fake_ui):
    return [c.kwargs.get('type') for c in fake_ui.notify.call_args_list]


# ---------------------------------------------------------------------------
# _run_lists_write behaviour
# ---------------------------------------------------------------------------

def test_signed_out_write_is_refused_before_the_manager_is_called(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, False)
    write = MagicMock()
    result = asyncio.run(lists_page._run_lists_write(write))
    assert result is None
    write.assert_not_called()
    assert _toast_types(lists_page.ui) == ['warning']


@pytest.mark.parametrize('answer', [None, False, ''])
def test_a_write_that_reports_nothing_done_is_a_failure(monkeypatch, lists_page, answer):
    _signed_in(monkeypatch, lists_page, True)

    async def write():
        return answer

    assert asyncio.run(lists_page._run_lists_write(write)) is None
    assert _toast_types(lists_page.ui) == ['negative']


def test_a_write_that_raises_is_a_failure(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, True)

    async def write():
        raise RuntimeError('boom')

    assert asyncio.run(lists_page._run_lists_write(write)) is None
    assert _toast_types(lists_page.ui) == ['negative']


def test_a_successful_write_is_awaited_and_returned_without_a_toast(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, True)
    ran = []

    async def write():
        ran.append(True)
        return 'list-7'

    assert asyncio.run(lists_page._run_lists_write(write)) == 'list-7'
    assert ran == [True]
    assert _toast_types(lists_page.ui) == []


def test_a_count_of_zero_is_not_a_failure_when_falsy_results_are_allowed(monkeypatch, lists_page):
    """Empty Trash legitimately deletes 0 lists."""
    _signed_in(monkeypatch, lists_page, True)

    async def write():
        return 0

    assert asyncio.run(lists_page._run_lists_write(write, falsy_is_failure=False)) == 0
    assert _toast_types(lists_page.ui) == []


# ---------------------------------------------------------------------------
# The inline rename, driven at its real call site
# ---------------------------------------------------------------------------

class _FakeListsMgr:
    def __init__(self, answer):
        self.answer = answer
        self.renamed = []

    async def update_list(self, list_id, name=None, color=None):
        self.renamed.append((list_id, name))
        return self.answer


def _drive_inline_rename(mod, mgr, new_name='New name'):
    """Build the inline label with a fake ui, start editing, type, and blur."""
    mod.create_inline_edit_label(
        current_name='Old name', list_id='5', is_system=False,
        lists_mgr=mgr, tr_func=lambda s: s,
    )
    # The page builds them as ui.label(...).classes(...) and ui.input(...).classes(...).props(...).
    label_el = mod.ui.label.return_value.classes.return_value
    input_el = mod.ui.input.return_value.classes.return_value.props.return_value
    label_el.text = 'Old name'
    start_editing = next(c.args[1] for c in label_el.on.call_args_list if c.args[0] == 'click')
    save_on_blur = next(c.args[1] for c in input_el.on.call_args_list if c.args[0] == 'blur')
    start_editing()
    input_el.value = new_name
    out = save_on_blur()
    if inspect.isawaitable(out):
        asyncio.run(out)
    return label_el


def test_inline_rename_is_awaited_and_reported(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, True)
    mgr = _FakeListsMgr(answer=True)
    label_el = _drive_inline_rename(lists_page, mgr)
    assert mgr.renamed == [('5', 'New name')], 'the rename coroutine never ran'
    assert label_el.text == 'New name'
    assert _toast_types(lists_page.ui) == ['positive']


def test_inline_rename_failure_keeps_the_old_name(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, True)
    mgr = _FakeListsMgr(answer=False)
    label_el = _drive_inline_rename(lists_page, mgr)
    assert mgr.renamed == [('5', 'New name')]
    assert label_el.text == 'Old name'
    assert 'positive' not in _toast_types(lists_page.ui)
    assert 'negative' in _toast_types(lists_page.ui)


def test_inline_rename_after_sign_out_does_not_write(monkeypatch, lists_page):
    _signed_in(monkeypatch, lists_page, False)
    mgr = _FakeListsMgr(answer=True)
    label_el = _drive_inline_rename(lists_page, mgr)
    assert mgr.renamed == []
    assert label_el.text == 'Old name'
    assert 'positive' not in _toast_types(lists_page.ui)


# ---------------------------------------------------------------------------
# Call-site rules over web/pages/lists.py (the other callbacks are closures
# inside create_lists_page behind dialogs, so they are pinned by AST)
# ---------------------------------------------------------------------------

def _parents(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _receiver_is_lists_mgr(call: ast.Call) -> bool:
    return (isinstance(call.func, ast.Attribute)
            and ast.unparse(call.func.value).endswith('lists_mgr'))


def _is_run_lists_write(call) -> bool:
    return (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
            and call.func.id == '_run_lists_write')


def _write_call_problems(source: str) -> list[str]:
    tree = ast.parse(source)
    parents = _parents(tree)
    problems = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _receiver_is_lists_mgr(node):
            continue
        name = node.func.attr
        if name in SYNC_WRITE_METHODS:
            problems.append(f'line {node.lineno}: {name}() is a sync write fallback')
            continue
        if name not in WRITE_METHODS:
            continue
        lam = parents.get(node)
        runner = parents.get(lam)
        if not (isinstance(lam, ast.Lambda) and lam.body is node
                and _is_run_lists_write(runner) and runner.args and runner.args[0] is lam):
            problems.append(f'line {node.lineno}: {name}() is not handed to _run_lists_write')
    for node in ast.walk(tree):
        if not _is_run_lists_write(node):
            continue
        aw = parents.get(node)
        assign = parents.get(aw)
        if not isinstance(aw, ast.Await):
            problems.append(f'line {node.lineno}: _run_lists_write() is not awaited')
            continue
        if not (isinstance(assign, ast.Assign) and len(assign.targets) == 1
                and isinstance(assign.targets[0], ast.Name)):
            problems.append(f'line {node.lineno}: the _run_lists_write() result is not kept')
            continue
        target = assign.targets[0].id
        body = parents.get(assign)
        siblings = next((getattr(body, f) for f in ('body', 'orelse', 'finalbody')
                         if assign in getattr(body, f, [])), [])
        idx = siblings.index(assign)
        nxt = siblings[idx + 1] if idx + 1 < len(siblings) else None
        tests_result = (
            isinstance(nxt, ast.If)
            and target in {n.id for n in ast.walk(nxt.test) if isinstance(n, ast.Name)}
            and isinstance(nxt.body[-1], ast.Return)
        )
        if not tests_result:
            problems.append(f'line {node.lineno}: the result of _run_lists_write() is not '
                            f'checked (if not {target}: return) before reporting success')
    return problems


def test_every_lists_write_goes_through_the_checked_runner():
    problems = _write_call_problems(LISTS_PY.read_text(encoding='utf-8'))
    assert problems == [], '\n'.join(problems)


def test_inline_rename_handlers_are_async():
    tree = ast.parse(LISTS_PY.read_text(encoding='utf-8'))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == 'create_inline_edit_label')
    inner = {n.name: n for n in ast.walk(fn) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert isinstance(inner['save_edit'], ast.AsyncFunctionDef)
    assert isinstance(inner['handle_keydown'], ast.AsyncFunctionDef)
    awaited = [n for n in ast.walk(inner['handle_keydown'])
               if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
               and isinstance(n.value.func, ast.Name) and n.value.func.id == 'save_edit']
    assert awaited, 'handle_keydown must await save_edit()'


_GOOD = '''
async def cb():
    ok = await _run_lists_write(lambda: state.lists_mgr.delete_list(x))
    if not ok:
        return
    ui.notify('done')
'''


@pytest.mark.parametrize('source,expected', [
    (_GOOD, 0),
    ('def cb():\n    state.lists_mgr.update_item_note(i, n)\n', 1),
    ('async def cb():\n    await state.lists_mgr.delete_list(x)\n', 1),
    ('def cb():\n    state.lists_mgr.create_list_sync(n)\n', 1),
    ('async def cb():\n    _run_lists_write(lambda: state.lists_mgr.delete_list(x))\n', 1),
    ('async def cb():\n    ok = await _run_lists_write(lambda: state.lists_mgr.delete_list(x))\n'
     '    ui.notify("done")\n', 1),
    ('async def cb():\n    ok = await _run_lists_write(lambda: state.lists_mgr.delete_list(x))\n'
     '    # if not ok: return\n    ui.notify("done")\n', 1),
])
def test_the_write_call_rule_can_fail(source, expected):
    assert len(_write_call_problems(source)) == expected, _write_call_problems(source)


def test_empty_trash_failure_still_rebuilds_the_trash_dialog():
    """A partial Empty Trash deletes some rows and then fails: the dialog must not keep
    showing (and offering actions on) rows that are gone. On failure the callback
    closes the dialog and refreshes the page before returning."""
    tree = ast.parse(LISTS_PY.read_text(encoding='utf-8'))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == 'empty_trash')
    guard = next(n for n in ast.walk(fn) if isinstance(n, ast.If)
                 and 'count' in {x.id for x in ast.walk(n.test) if isinstance(x, ast.Name)})
    called = {ast.unparse(c.func) for c in ast.walk(ast.Module(body=guard.body, type_ignores=[]))
              if isinstance(c, ast.Call)}
    assert 'dialog.close' in called, 'the failure branch leaves the stale trash dialog open'
    assert 'async_refresh_ui' in called, 'the failure branch does not refresh the page'
    assert isinstance(guard.body[-1], ast.Return)
