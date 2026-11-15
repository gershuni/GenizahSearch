# -*- coding: utf-8 -*-
"""web/components/show_more.py: the rest of a dictionary card behind one button that draws it in place
(owner, 2026-11-11: no sense, quotation or sub-entry only on the site or the PDF page).

Pinned here: the three labels are the owner's wording in both languages ("הצג", not "הצגת"); both panels
use them, and neither links its senses, quotations or sub-entries away for anything a card carries.
The clicks themselves are in the render-smoke tests of Blau's and Friedman's tabs.
"""
from __future__ import annotations

from pathlib import Path

from shared.genizah_translations import TRANSLATIONS
from web.components import blau_panel, friedman_panel, show_more

ROOT = Path(__file__).resolve().parents[1]
CSS = (ROOT / "web" / "static" / "common.css").read_text(encoding="utf-8")


def test_the_labels_are_the_owner_s_wording_in_both_languages():
    approved = {
        show_more.MORE_SENSES: ("Show {n} more senses", "הצג {n} משמעויות נוספות"),
        show_more.MORE_QUOTATIONS: ("Show {n} more quotations", "הצג {n} מובאות נוספות"),
        show_more.MORE_CHILDREN: ("Show {n} more sub-entries and verb stems", "הצג {n} ערכי משנה ובניינים נוספים"),
    }
    for label, (english, hebrew) in approved.items():
        assert label == english and TRANSLATIONS[label] == hebrew
        assert label.format(n=3) == english.replace("{n}", "3") and "{n}" in TRANSLATIONS[label]


def test_lane_s_new_lines_are_the_owner_s_wording():
    from web.components import lane_panel
    assert show_more.MORE_ENTRIES == "Show {n} more entries" and TRANSLATIONS[show_more.MORE_ENTRIES] == "הצג {n} ערכים נוספים"
    assert lane_panel.FULL_ENTRY == "Show the full entry" and TRANSLATIONS[lane_panel.FULL_ENTRY] == "הצג את הערך המלא"
    assert lane_panel.ENTRIES_SHOWN == 3


def test_no_line_names_the_dictionary_a_borrowed_root_came_from():
    """Owner, 2026-11-12: "Root from Friedman's dictionary" and the like were redundant; neither tab
    draws them, and their strings are gone."""
    for name in ("Root from Friedman's dictionary", "Root from Lane's Lexicon", "Root from Blau's dictionary"):
        assert name not in TRANSLATIONS, name
    for panel in (blau_panel, friedman_panel):
        assert not hasattr(panel, "BORROWED_LABELS") and not hasattr(panel, "BORROWED_CLASS")


def test_the_verb_form_line_is_the_owner_s_wording_in_both_panels():
    """PR 3 (approved 2026-11-11): one line on a Blau card and a Friedman match found by verb form."""
    from shared.friedman_service import VIA_VERB_FORM
    assert blau_panel.VERB_FORM == friedman_panel.VIA_LABELS[VIA_VERB_FORM] == "found by its verb form"
    assert TRANSLATIONS[blau_panel.VERB_FORM] == "נמצא לפי צורת הפועל"


def test_both_panels_open_the_rest_in_place():
    blau = (ROOT / "web" / "components" / "blau_panel.py").read_text(encoding="utf-8")
    friedman = (ROOT / "web" / "components" / "friedman_panel.py").read_text(encoding="utf-8")
    assert "show_more.render_more(card.senses[SENSES_SHOWN:]" in blau
    assert "show_more.render_more(unit.quotations[QUOTATIONS_SHOWN:]" in friedman
    assert "show_more.render_more(child.quotations[1:]" in friedman
    assert "show_more.render_more(match.children[first:], _render_child, show_more.MORE_CHILDREN)" in friedman
    assert blau_panel.SENSES_SHOWN == 4 and friedman_panel.QUOTATIONS_SHOWN == 8 and friedman_panel.CHILDREN_SHOWN == 30


def test_the_button_is_styled_under_the_popover_scope():
    assert f".gs-blau .{show_more.BUTTON_CLASS}.q-btn" in CSS
