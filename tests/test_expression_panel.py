# -*- coding: utf-8 -*-
"""web/components/expression_panel.py, shared by Friedman's and Blau's tabs, without a browser:
its class names are styled under the popover's scope and are the literals friedman_panel keeps,
its strings have their Hebrew, Blau's tab draws its expression search through it, and the lookup
event counts an expression's words for Blau as it does for Friedman (never the words)."""
from __future__ import annotations

import ast
from pathlib import Path

from shared.blau_service import BlauExpression, empty_expression, empty_lookup
from shared.genizah_translations import TRANSLATIONS
from web.components import blau_panel, expression_panel, friedman_panel

REPO = Path(__file__).resolve().parents[1]
SRC = (REPO / "web" / "components" / "expression_panel.py").read_text(encoding="utf-8")
BLAU_SRC = (REPO / "web" / "components" / "blau_panel.py").read_text(encoding="utf-8")
FRIEDMAN_SRC = (REPO / "web" / "components" / "friedman_panel.py").read_text(encoding="utf-8")
BROWSE_SRC = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")


def _translated_arguments(src: str) -> set[str]:
    tree = ast.parse(src)
    constants = {node.targets[0].id: node.value.value for node in tree.body
                 if isinstance(node, ast.Assign) and len(node.targets) == 1
                 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Constant)
                 and isinstance(node.value.value, str)}
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "tr" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                out.add(arg.value)
            elif isinstance(arg, ast.Name):
                out.add(constants[arg.id])
    return out


def test_the_shared_classes_and_strings_are_friedman_s_literals():
    for name in ("GAP_CLASS", "HIT_MARK_CLASS", "PLACE_CLASS", "COMPONENTS_CLASS",
                 "NOTHING_TOGETHER", "GAP_LABEL", "MATCHED_LABEL", "MORE_PLACES"):
        assert getattr(friedman_panel, name) == getattr(expression_panel, name), name
    for name in ("GAP_CLASS", "PLACE_CLASS", "COMPONENTS_CLASS"):
        assert f".gs-blau .{getattr(expression_panel, name)}" in CSS, name
    assert f".gs-blau mark.{expression_panel.HIT_MARK_CLASS}" in CSS
    assert f'<mark class="{{HIT_MARK_CLASS}}">{{html.escape(word)}}</mark>' in SRC, "the words are escaped; the marks are the only HTML"
    assert "html.escape" not in FRIEDMAN_SRC, "the marks are drawn in one place"


def test_every_shown_string_has_its_hebrew():
    shown = _translated_arguments(SRC)
    assert {expression_panel.GAP_LABEL, expression_panel.MATCHED_LABEL} <= shown
    for text in (*shown, expression_panel.NOTHING_TOGETHER, expression_panel.MORE_PLACES):
        assert TRANSLATIONS.get(text, "").strip(), text


def test_blau_s_tab_draws_its_expression_search_through_the_shared_parts():
    assert f".gs-blau .{blau_panel.EXPRESSION_CLASS}" in CSS
    assert "expression_panel.render_gap(result.gap, result.text, on_gap)" in BLAU_SRC
    assert "expression_panel.render_marked(place.words, place.positions, 'ja')" in BLAU_SRC
    assert "expression_panel.render_components(place)" in BLAU_SRC
    assert "tr(expression_panel.NOTHING_TOGETHER)" in BLAU_SRC and "tr(expression_panel.MORE_PLACES)" in BLAU_SRC
    assert "tr(friedman_panel.UNDER)" in BLAU_SRC and "tr(friedman_panel.IN_HEAD)" in BLAU_SRC, "one string, one translation"
    assert "_render_blau(result, credit_he, credit_en, lang, on_gap=on_gap)" in BLAU_SRC
    assert "blau_service.answer(word, gap=dict_gap['value'])" in BROWSE_SRC, "several words reach Blau's expression search"
    assert "expression_panel.render_gap(result.gap, result.text, on_gap)" in FRIEDMAN_SRC


def test_the_lookup_event_counts_an_expression_s_words_for_blau_too():
    assert blau_panel.lookup_event(False, empty_lookup("כתאב"), None) == {'how': 'word', 'blau': False, 'lane': None}
    expression = empty_expression("כתב כטה", 1, ("כתב", "כטה"), (("כתב",), ("כטה",)))
    assert isinstance(expression, BlauExpression)
    assert blau_panel.lookup_event(True, expression, None) == {'how': 'box', 'blau': False, 'lane': None, 'expression': 2}


def test_render_lookup_draws_nothing_without_a_credit_line():
    assert blau_panel.render_lookup(empty_expression("כתב כטה"), credit_he="") is False
