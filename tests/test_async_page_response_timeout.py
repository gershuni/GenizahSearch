# -*- coding: utf-8 -*-
"""Every async ``@ui.page`` in web/main.py waits long enough for its own build.

NiceGUI gives an async page builder ``response_timeout`` (default 3.0 s) to finish
before it answers. A sync page building in another tab blocks the one event loop,
so a stall elsewhere pushes an async page past 3 s. On NiceGUI 3.8 the page is
then served with an already-deleted client and the browser reload-loops; on 3.18
the visitor gets NiceGUI's English 500 page instead -- after /auth/callback has
already signed them in. Verified in process on both versions (2026-10-20 review).
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from nicegui import ui

MAIN = Path(__file__).resolve().parents[1] / 'web' / 'main.py'
MINIMUM_SECONDS = 15.0


def _short_async_pages(source: str) -> list[str]:
    """Async page routes whose response_timeout is missing or below the minimum."""
    tree = ast.parse(source)
    constants = {
        target.id: node.value.value
        for node in tree.body if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant)
    }
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == 'page'):
                continue
            timeout = next((k.value for k in decorator.keywords if k.arg == 'response_timeout'), None)
            if isinstance(timeout, ast.Name):
                timeout = constants.get(timeout.id)
            elif isinstance(timeout, ast.Constant):
                timeout = timeout.value
            if not isinstance(timeout, (int, float)) or timeout < MINIMUM_SECONDS:
                offenders.append(node.name)
    return offenders


def test_every_async_page_has_a_long_enough_response_timeout():
    offenders = _short_async_pages(MAIN.read_text(encoding='utf-8'))
    assert offenders == [], (
        f'async @ui.page routes left on the 3 s default (or too short): {offenders}')


def test_the_check_can_fail():
    seeded = (
        "SHORT = 2.0\n"
        "@ui.page('/a')\nasync def bare():\n    pass\n"
        "@ui.page('/b', response_timeout=SHORT)\nasync def short():\n    pass\n"
        "@ui.page('/c', response_timeout=20)\nasync def fine():\n    pass\n"
        "@ui.page('/d')\ndef sync_page():\n    pass\n"
    )
    assert _short_async_pages(seeded) == ['bare', 'short']


def test_ui_page_takes_response_timeout():
    assert 'response_timeout' in inspect.signature(ui.page.__init__).parameters
