# -*- coding: utf-8 -*-
"""Web Joins Lab: a single-line Exact search is the website's whole-word Exact.

2026-10-21 (owner decision, with the public API's 'exact'): the single-line
search sends its mode through ``web.pages.search_state.engine_mode``, so Exact
reaches the engine as 'literal'. The Responsa-style branch keeps its own
'exact' / 'variants' (its structured query is the Responsa engine's), and
both branches reach the one ``execute_search`` call through ``mode_str``.
"""
from __future__ import annotations

import ast
from pathlib import Path

from web.pages.search_state import engine_mode

JOINS_LAB = Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'joins_lab.py'


def _mode_str_assignments():
    tree = ast.parse(JOINS_LAB.read_text(encoding='utf-8'))
    return [ast.unparse(node.value) for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == 'mode_str' for t in node.targets)]


def test_single_line_exact_goes_through_engine_mode_and_responsa_keeps_its_own():
    assert sorted(_mode_str_assignments()) == sorted([
        "'variants' if side.variants else 'exact'",  # Responsa-style branch: unchanged
        "engine_mode(q['mode'])",                     # single-line branch
    ])


def test_the_search_runs_with_mode_str():
    tree = ast.parse(JOINS_LAB.read_text(encoding='utf-8'))
    modes = [ast.unparse(kw.value) for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
             and node.func.attr == 'execute_search'
             for kw in node.keywords if kw.arg == 'mode']
    assert modes == ['mode_str'], modes


def test_engine_mode_maps_only_exact():
    assert engine_mode('exact') == 'literal'
    for mode in ('variants', 'variants_extended', 'fuzzy', 'Regex', 'Responsa'):
        assert engine_mode(mode) == mode
