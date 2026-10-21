# -*- coding: utf-8 -*-
"""A search started from the URL (/search?q=, the homepage box) starts at once.

The page waited a fixed 0.5 s before running it, "for the UI to render first".
execute_search reads only server-side values, and everything it sends is held in
the outbox until the browser's socket connects, so the wait only delayed the
first rows (by up to 0.5 s, overlapping the page's own load).
"""

from __future__ import annotations

import ast
from pathlib import Path

SEARCH = Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'search.py'


def _delays_before(func_name: str, source: str) -> list[float]:
    return [
        node.args[0].value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_after_delay'
        and len(node.args) >= 2 and getattr(node.args[1], 'id', None) == func_name
        and isinstance(node.args[0], ast.Constant)
    ]


def test_url_started_searches_do_not_wait():
    delays = _delays_before('execute_search', SEARCH.read_text(encoding='utf-8'))
    assert delays, 'no URL-started search found; update this test'
    assert all(d == 0 for d in delays), delays


def test_the_check_can_fail():
    assert _delays_before('execute_search', '_after_delay(0.5, execute_search)\n') == [0.5]
