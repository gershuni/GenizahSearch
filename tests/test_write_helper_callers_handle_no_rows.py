# -*- coding: utf-8 -*-
"""Callers of the row-checked write helpers handle the 'nothing changed' result.

``delete_comment``, ``delete_correction``, ``delete_fragment_join`` and
``update_comment`` in web/supabase_client.py return
``{'error': <English log string>, 'no_rows': True}`` when the write matched no
row (RLS let nothing change: not the user's row, a missing policy). Every
caller must branch on ``no_rows`` and show the fixed translated message
("Nothing was changed. You may not have permission."), not the helper's
English string and not a connection message.

The rule, per call site: the innermost function around the call -- skipping a
trivial wrapper whose whole body is ``return <the call>`` -- must mention the
constant ``'no_rows'`` in code, or hand the result to the page's shared
``_notify_write_failure``. Parsed with ast, so a comment never satisfies it.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB = REPO_ROOT / 'web'

HELPERS = {
    'delete_comment', 'delete_correction', 'delete_fragment_join', 'update_comment',
    # /corrections imports them under these aliases
    'sb_delete_comment', 'sb_delete_correction', 'sb_update_comment',
}
SHARED_HANDLERS = {'_notify_write_failure'}
_FUNC_TYPES = (ast.FunctionDef, ast.AsyncFunctionDef)

# Callers that turn the result into a bool before any UI sees it. Their own
# callers show a fixed translated message on False.
EXEMPT = {
    # returns True/False; its caller shows tr('Failed to delete join')
    ('web/components/joins_panel.py', 'delete_join'),
}


def _parents(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _is_trivial_wrapper(fn, call):
    body = [s for s in fn.body if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))]
    return len(body) == 1 and isinstance(body[0], ast.Return) and body[0].value is call


def _handles_no_rows(fn):
    for node in ast.walk(fn):
        if isinstance(node, ast.Constant) and node.value == 'no_rows':
            return True
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in SHARED_HANDLERS):
            return True
    return False


def _unhandled_calls(source, relpath):
    tree = ast.parse(source)
    parents = _parents(tree)
    hits = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in HELPERS):
            continue
        fn = parents.get(node)
        while fn is not None and not isinstance(fn, _FUNC_TYPES):
            fn = parents.get(fn)
        if fn is not None and _is_trivial_wrapper(fn, node):
            fn = parents.get(fn)
            while fn is not None and not isinstance(fn, _FUNC_TYPES):
                fn = parents.get(fn)
        if fn is None:
            hits.append((relpath, node.lineno, '<module>'))
            continue
        if fn.name in HELPERS or (relpath, fn.name) in EXEMPT:
            continue
        if not _handles_no_rows(fn):
            hits.append((relpath, node.lineno, fn.name))
    return sorted(hits)


def test_every_write_helper_caller_handles_no_rows():
    hits = []
    for path in sorted(WEB.rglob('*.py')):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel == 'web/supabase_client.py':
            continue
        hits.extend(_unhandled_calls(path.read_text(encoding='utf-8'), rel))
    assert hits == [], (
        "These callers of a row-checked write helper never look at result['no_rows'], "
        "so a write that changed nothing shows the helper's English string or a "
        "misleading connection message. Show tr('Nothing was changed. You may not "
        "have permission.') for no_rows and log result['error']: %r" % hits)


@pytest.mark.parametrize('snippet,expected', [
    # shows the English helper string: flagged
    ("def do_delete():\n"
     "    result = delete_comment(1)\n"
     "    if 'error' in result:\n"
     "        ui.notify(result['error'])\n", 1),
    # the only mention of no_rows is a comment: still flagged
    ("def do_delete():\n"
     "    result = delete_correction(1)  # no_rows handled elsewhere\n"
     "    ui.notify(result.get('error'))\n", 1),
    # a trivial wrapper does not hide the consuming function
    ("async def outer():\n"
     "    def _run():\n"
     "        return delete_fragment_join(1)\n"
     "    result = _run()\n"
     "    ui.notify('x')\n", 1),
    # handled: branches on no_rows
    ("def do_delete():\n"
     "    result = delete_comment(1)\n"
     "    if result.get('no_rows'):\n"
     "        ui.notify(tr('Nothing was changed. You may not have permission.'))\n", 0),
    # handled through the wrapper's consumer
    ("async def outer():\n"
     "    def _run():\n"
     "        return delete_fragment_join(1)\n"
     "    result = _run()\n"
     "    if result.get('no_rows'):\n"
     "        pass\n", 0),
    # handled by the shared page handler
    ("def act():\n"
     "    result = sb_update_comment(1, 't')\n"
     "    if 'error' in result:\n"
     "        _notify_write_failure(result)\n", 0),
])
def test_no_rows_detector(snippet, expected):
    """Regression guard, green before and after: proves the call-site rule can fail."""
    assert len(_unhandled_calls(snippet, 'web/_synthetic.py')) == expected
