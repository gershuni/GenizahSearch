# -*- coding: utf-8 -*-
"""The /parallels sidebar entry carries no "New! Fast search feature" badge.

The badge (added 2026-08-25) was removed at the owner's request, 2026-10-08.
Read from the AST of web/main.py rather than by importing it: `create_layout`
has NiceGUI page side effects, and the assertion is about one nav tuple.
"""
from __future__ import annotations

import ast
import os

MAIN_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'web', 'main.py',
)


def _nav_tuple(path: str) -> ast.Tuple:
    """The 4-tuple `(path, icon, label, badge)` for `path` in create_layout.

    The LENGTH check is load-bearing: `_WHATS_NEW_SUPPRESSED_ON` also starts
    with '/parallels', and matching on the first element alone hits it.
    """
    with open(MAIN_PATH, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef)
                and node.name == 'create_layout'):
            continue
        for sub in ast.walk(node):
            if (isinstance(sub, ast.Tuple) and len(sub.elts) == 4
                    and isinstance(sub.elts[0], ast.Constant)
                    and sub.elts[0].value == path):
                return sub
        raise AssertionError(f'no 4-element nav entry for {path} in create_layout')
    raise AssertionError('create_layout not found in web/main.py')


def test_the_parallels_nav_entry_has_no_badge():
    badge = _nav_tuple('/parallels').elts[3]
    assert isinstance(badge, ast.Constant) and badge.value is None


def test_the_fast_search_badge_string_is_gone():
    with open(MAIN_PATH, encoding='utf-8') as fh:
        assert 'New! Fast search feature' not in fh.read()
