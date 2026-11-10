# -*- coding: utf-8 -*-
"""No web event handler hands NiceGUI a detached task.

``on_click=lambda: asyncio.ensure_future(work())`` returns an asyncio.Task.
NiceGUI awaits a handler's returned awaitable inside the element's slot, but
it EXCLUDES a Task from that path (nicegui/events.py, ``handle_event``: ``not
isinstance(result, asyncio.Task)``), so the task runs with no page: ui.notify,
ui.download, ui.run_javascript and app.storage.tab raise there, and the sites
swallowed the error (the Joins Lab export, see the comment above
``_on_export_csv`` in web/pages/joins_lab.py; the 2026-10-20 page-context
fixes). Pass the coroutine function itself, or a lambda that returns the
coroutine (``lambda: work()``), and NiceGUI runs it in the page.

The scan covers lambdas given to an ``on_*=`` keyword or to an ``.on(...)`` /
``.on_*(...)`` call anywhere under web/.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB = REPO_ROOT / 'web'
SPAWNERS = {'ensure_future', 'create_task'}


def _spawns_task(node):
    """True for ``asyncio.ensure_future(...)``, ``loop.create_task(...)``, bare ``create_task(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, 'id', None)
    return name in SPAWNERS


def _handler_lambdas(tree):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg and kw.arg.startswith('on_') and isinstance(kw.value, ast.Lambda):
                yield kw.value
        func = node.func
        if isinstance(func, ast.Attribute) and (func.attr == 'on' or func.attr.startswith('on_')):
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                if isinstance(arg, ast.Lambda):
                    yield arg


def detached_handler_tasks(source):
    """Line numbers of event-handler lambdas whose body spawns a task."""
    return sorted(lam.lineno for lam in _handler_lambdas(ast.parse(source)) if _spawns_task(lam.body))


def test_no_web_event_handler_returns_a_detached_task():
    offenders = []
    for path in sorted(WEB.rglob('*.py')):
        lines = detached_handler_tasks(path.read_text(encoding='utf-8'))
        offenders += [f'{path.relative_to(REPO_ROOT).as_posix()}:{line}' for line in lines]
    assert not offenders, (
        'event handler returns asyncio.ensure_future/create_task, which NiceGUI runs outside the '
        'page (no ui.notify, ui.download, app.storage.tab); pass the coroutine instead:\n  '
        + '\n  '.join(offenders))


@pytest.mark.parametrize('snippet,lines', [
    ("ui.button('x', on_click=lambda: asyncio.ensure_future(go()))\n", [1]),
    ("b.on('click', lambda e: asyncio.create_task(go(e)))\n", [1]),
    ("b.on_click(lambda: loop.create_task(go()))\n", [1]),
    ("ui.item('x', on_click=lambda: ensure_future(go()))\n", [1]),
    ("ui.button('x', on_click=lambda: go())\n", []),
    ("ui.button('x', on_click=go)\n", []),
    ("b.on('click', lambda e: go(e))\n", []),
    ("t = asyncio.ensure_future(go())\n", []),
    ("# on_click=lambda: asyncio.ensure_future(go())\n", []),
])
def test_detector(snippet, lines):
    """Regression guard, green before and after: proves the scan can fail."""
    assert detached_handler_tasks(snippet) == lines
