# -*- coding: utf-8 -*-
"""web/components/friedman_panel.py without a browser: every class it draws is styled under the
popover's scope, every string it shows has its Hebrew, and the dictionary panel gives it a tab.

Read as SOURCE, never imported (tests/test_lane_panel.py says why).
"""
from __future__ import annotations

import ast
from pathlib import Path

from shared.genizah_translations import TRANSLATIONS

REPO = Path(__file__).resolve().parents[1]
SRC = (REPO / "web" / "components" / "friedman_panel.py").read_text(encoding="utf-8")
PANEL_SRC = (REPO / "web" / "components" / "blau_panel.py").read_text(encoding="utf-8")
BROWSE_SRC = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
MAIN_SRC = (REPO / "web" / "main.py").read_text(encoding="utf-8")
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")
TREE = ast.parse(SRC)
CONSTANTS = {node.targets[0].id: node.value.value for node in TREE.body
             if isinstance(node, ast.Assign) and len(node.targets) == 1
             and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Constant)
             and isinstance(node.value.value, str)}
DICTS = {node.targets[0].id: {k.value: v.value for k, v in zip(node.value.keys, node.value.values)}
         for node in TREE.body if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
         and isinstance(node.targets[0], ast.Name)}


def _translated_arguments() -> set[str]:
    out = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "tr" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                out.add(arg.value)
            elif isinstance(arg, ast.Name):
                out.add(CONSTANTS[arg.id])
            elif isinstance(arg, ast.Call) and isinstance(arg.func, ast.Attribute) and arg.func.attr == "get":
                out.update(DICTS[arg.func.value.id].values())  # tr(KIND_LABELS.get(...))
            else:
                raise AssertionError(f"tr() with an argument this test cannot read: {ast.dump(arg)}")
    return out


def test_every_friedman_class_is_styled_under_the_popover_scope():
    for name in ("SECTION_CLASS", "EMPTY_SECTION_CLASS", "STATUS_CLASS", "MATCH_CLASS", "VIA_CLASS",
                 "QUOTE_CLASS", "CHILD_CLASS", "CREDIT_CLASS", "PLACE_CLASS", "COMPONENTS_CLASS", "GAP_CLASS"):
        assert f".gs-blau .{CONSTANTS[name]}" in CSS, name
    assert f".gs-blau mark.{CONSTANTS['HIT_MARK_CLASS']}" in CSS, "the mark on a matched word"
    assert "expression_panel.render_marked(" in SRC, "the marks are drawn by the shared expression panel (tests/test_expression_panel.py)"


def test_every_shown_string_has_its_hebrew():
    shown = _translated_arguments()
    assert {CONSTANTS["TITLE"], CONSTANTS["STATUS_NOTE"], CONSTANTS["NOTHING"], CONSTANTS["OPEN_PAGE"],
            CONSTANTS["UNDER"], CONSTANTS["MORE_CHILDREN"], CONSTANTS["MORE_QUOTATIONS"],
            CONSTANTS["MORE_MATCHES"], CONSTANTS["NOTHING_TOGETHER"], CONSTANTS["IN_HEAD"], CONSTANTS["MORE_PLACES"],
            *DICTS["KIND_LABELS"].values(), *DICTS["VIA_LABELS"].values()} <= shown
    # The gap control and the matched-word pairs are drawn by web/components/expression_panel.py,
    # shared with Blau's tab; this module's literals are the same (tests/test_expression_panel.py).
    missing = sorted(t for t in shown if not TRANSLATIONS.get(t))
    assert not missing, missing
    assert TRANSLATIONS[CONSTANTS["STATUS_NOTE"]] == "פוענח אוטומטית מתוך קובצי ה-PDF ולא נבדק. פתחו את הקישור כדי לוודא", \
        "the owner's wording, 2026-11-07"
    assert TRANSLATIONS[DICTS["VIA_LABELS"]["stem"]] == "נמצא לפי צורת הבסיס", "the owner's wording: not a stem"
    approved = {  # the expression search's strings, approved by the owner as drafted, 2026-11-08
        "NOTHING_TOGETHER": "המילים לא נמצאו יחד במילון.",
        "GAP_LABEL": "מילים מותרות בין מילה למילה",
        "MATCHED_LABEL": "המילים שנמצאו",
        "IN_HEAD": "בראש הערך",
        "MORE_PLACES": "מקומות נוספים בערך זה",
    }
    for name, hebrew in approved.items():
        assert TRANSLATIONS[CONSTANTS[name]] == hebrew, name


def test_the_panel_the_page_and_the_startup_give_friedman_its_tab():
    assert "FRIEDMAN_TAB = 'friedman'" in PANEL_SRC and "FRIEDMAN_TAB: 'Friedman'" in PANEL_SRC
    assert TRANSLATIONS["Friedman"] == "פרידמן"
    assert "friedman_panel.render_friedman(" in PANEL_SRC and "friedman_panel.render_friedman_empty(" in PANEL_SRC
    assert "friedman: Optional[FriedmanAnswer] = None, friedman_credit: str = ''" in PANEL_SRC
    assert "friedman_service = get_friedman_service()" in BROWSE_SRC
    assert "friedman_service.is_available()" in BROWSE_SRC
    assert "friedman=friedman, friedman_credit=friedman_service.credit_he" in BROWSE_SRC
    assert "return friedman_service.lookup(word)" in BROWSE_SRC
    assert "get_friedman_service" in MAIN_SRC and "Friedman's dictionary not served" in MAIN_SRC
    assert PANEL_SRC.index("tabs.append(FRIEDMAN_TAB)") < PANEL_SRC.index("tabs.append(LANE_TAB)"), \
        "Friedman's tab beside Blau's, before Lane's (owner, 2026-11-08)"
    from web.components.blau_panel import BLAU_TAB, FRIEDMAN_TAB, LANE_TAB, pick_tab
    assert pick_tab([BLAU_TAB, FRIEDMAN_TAB, LANE_TAB], {BLAU_TAB: False, FRIEDMAN_TAB: True, LANE_TAB: True}) == FRIEDMAN_TAB


def test_several_words_are_an_expression_search_with_the_gap_the_reader_chose():
    assert "if len(word.split()) > 1:" in BROWSE_SRC
    assert "return friedman_service.expression(word, gap=dict_gap['value'])" in BROWSE_SRC
    assert "dict_gap = {'value': FRIEDMAN_DEFAULT_GAP}" in BROWSE_SRC
    assert "dict_gap['value'] = gap" in BROWSE_SRC and "on_gap=_choose_gap" in BROWSE_SRC
    assert "isinstance(friedman, FriedmanExpression)" in PANEL_SRC
    assert "friedman_panel.render_friedman_expression(" in PANEL_SRC
    assert "friedman_panel.render_friedman_expression_empty(" in PANEL_SRC
    assert "event['expression'] = len(friedman.words)" in PANEL_SRC, "how many words, never which"
    assert "_render_gap(result, on_gap)" in SRC.split("def render_friedman_expression_empty")[1], \
        "the gap control stays when nothing was found, so the reader can widen it"


def test_no_text_without_the_credit_and_the_status_on_every_section():
    assert "if not credit:" in SRC and "return False" in SRC
    assert "ui.label(tr(STATUS_NOTE))" in SRC and "_render_credit(credit, lang)" in SRC
    assert "ui.link(credit, SITE_URL, new_tab=True)" in SRC


def test_a_rejected_entry_shows_the_books_braces():
    """The book lists an entry in braces to reject it ({ } = listed elsewhere, held wrong); the
    panel shows every head through _head_label, which puts the braces back from the flag."""
    assert "'braced_head' in unit.flags" in SRC
    assert "_rtl(_head_label(unit))" in SRC and "_rtl(_head_label(match.parent))" in SRC and "_rtl(_head_label(hit.parent))" in SRC
    assert "_rtl(unit.head_text)" not in SRC and ".parent.head_text)" not in SRC, "no head is shown without the braces"
    assert "expression_panel.render_marked(_braced_words(unit, marked.words), marked.positions" in SRC,         "a marked head keeps its braces and the printed order"
