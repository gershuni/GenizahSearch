# -*- coding: utf-8 -*-
"""web/components/blau_panel.py without a browser: the event payload parser, the head line, and
the names the click script, the stylesheet and the renderer must agree on (a renamed class or
event on one side would leave the companion silently dead on the page)."""
from __future__ import annotations

import json
import re
from pathlib import Path

from shared.arabic_service import empty_lane
from shared.blau_service import empty_lookup
from web.components import blau_panel

REPO = Path(blau_panel.__file__).resolve().parents[2]
JS = (REPO / "web" / "static" / "js" / "blau_lookup.js").read_text(encoding="utf-8")
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")
PANEL_SRC = Path(blau_panel.__file__).read_text(encoding="utf-8")


def test_lookup_word_reads_the_script_s_payload():
    assert blau_panel.lookup_word({"detail": json.dumps({"word": " כתאב "})}) == "כתאב"
    assert blau_panel.lookup_word(json.dumps({"word": "כתאב"})) == "כתאב"
    assert blau_panel.lookup_word({"word": "כתאב"}) == "כתאב"


def test_lookup_word_is_empty_on_anything_else():
    for args in (None, "", "not json", {"detail": "{"}, {"detail": json.dumps(["כתאב"])},
                 {"detail": json.dumps({"word": 7})}, {"detail": json.dumps({})}, 7, [1]):
        assert blau_panel.lookup_word(args) == "", args


def test_head_html_loads_the_script_versioned_by_its_content(monkeypatch, tmp_path):
    line = blau_panel.head_html()
    assert line.startswith('<script src="/static/js/blau_lookup.js?v=') and ' defer>' in line
    version = re.search(r"\?v=([0-9a-f]{10})", line).group(1)
    monkeypatch.setattr(blau_panel, "_JS_PATH", tmp_path / "missing.js")
    assert blau_panel.head_html() == ""
    changed = tmp_path / "changed.js"
    changed.write_text(JS + "\n// changed", encoding="utf-8")
    monkeypatch.setattr(blau_panel, "_JS_PATH", changed)
    assert version not in blau_panel.head_html()


def test_the_script_and_the_renderer_use_the_same_names():
    assert f"'.{blau_panel.POPOVER_CLASS}'" in JS
    assert f"'[{blau_panel.PANE_ATTR}]'" in JS
    assert f"'{blau_panel.HIT_CLASS}'" in JS
    assert f"'{blau_panel.CLOSE_CLASS}'" in JS  # the close control clears the word's mark
    assert f"'{blau_panel.LOOKUP_EVENT}'" in JS and f"'{blau_panel.CLOSE_EVENT}'" in JS
    assert "JSON.stringify" in JS and "word: word.text" in JS


def test_the_script_and_the_page_agree_on_the_open_button_and_its_event():
    assert f"'{blau_panel.OPEN_CLASS}'" in JS and f"'{blau_panel.OPEN_EVENT}'" in JS
    # The open button is placed beside, like a word.
    assert "place(pop, opener.getBoundingClientRect())" in JS


def test_the_script_writes_the_position_where_the_page_cannot_wipe_it():
    """The page shows the popover with `.style('display: flex;')`, which
    replaces the popover's inline style: a position written there is lost and
    the panel opens at the fallback corner (seen in the browser, 2026-11-04).
    The script writes the custom properties on <html>; they inherit."""
    place = JS[JS.index("function place("):JS.index("function dispatch(")]
    assert "document.documentElement.style" in place
    assert "pop.style.setProperty" not in place


def test_the_script_asks_the_server_to_close_only_what_is_open():
    """With the lookup switched off nothing on the page is marked, and a click
    must not send the server a close (one message per click, for nothing)."""
    assert "if (open) dispatch(pop, 'blau-close')" in JS
    assert "shown(pop) || document.querySelector('.' + HIT) !== null" in JS


def test_the_switch_is_off_until_the_reader_turns_it_on(monkeypatch):
    store = {}
    monkeypatch.setattr(blau_panel, "safe_user_get", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(blau_panel, "safe_user_set", lambda key, value: store.__setitem__(key, value) or True)
    assert blau_panel.lookup_on() is False
    blau_panel.set_lookup_on(True)
    assert store == {blau_panel.PREF_KEY: True, blau_panel.SEEN_KEY: True} and blau_panel.lookup_on() is True
    blau_panel.set_lookup_on(0)
    assert blau_panel.lookup_on() is False
    assert store[blau_panel.SEEN_KEY] is True, 'turned off again, the switch is not new again'


def test_the_switch_is_new_until_the_reader_first_turns_it_on(monkeypatch):
    """Owner, 2026-11-04 (the feature was too hidden): highlighted, its label saying so, until
    the visitor first turns the dictionaries on; a visitor whose lookup is already on has used it."""
    store = {}
    monkeypatch.setattr(blau_panel, "safe_user_get", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(blau_panel, "safe_user_set", lambda key, value: store.__setitem__(key, value) or True)
    assert blau_panel.switch_is_new(False) is True and store == {}, "asking does not mark it seen"
    assert blau_panel.switch_is_new(False) is True
    assert blau_panel.switch_is_new(True) is False and store == {blau_panel.SEEN_KEY: True}
    assert blau_panel.switch_is_new(False) is False
    store.clear()
    blau_panel.set_lookup_on(False)
    assert blau_panel.switch_is_new(False) is True, "turning it off is not using it"
    blau_panel.set_lookup_on(True)
    assert blau_panel.switch_is_new(False) is False


def test_the_switch_s_label_is_the_owner_s_wording_with_the_maqaf():
    from shared.genizah_translations import TRANSLATIONS
    from web.translations import using_language

    with using_language("en"):
        assert blau_panel.switch_label(True) == "New! Judeo-Arabic dictionaries!"
        assert blau_panel.switch_label(False) == "Judeo-Arabic dictionaries"
    with using_language("he"):
        assert blau_panel.switch_label(True) == "חדש! מילונים לערבית־יהודית!"
        assert blau_panel.switch_label(False) == "מילונים לערבית־יהודית"
    for text in (blau_panel.NEW_LABEL, blau_panel.LABEL):
        assert "־" in TRANSLATIONS[text] and "-" not in TRANSLATIONS[text], "the maqaf, not a hyphen"


def test_the_switch_s_letters_are_drawn_not_typed():
    """עع as outlines: the same on every device, no Arabic font needed; coloured by the state."""
    svg = blau_panel.GLYPH_SVG
    assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg" viewBox=') and svg.endswith('"/></svg>')
    assert 'fill="currentColor"' in svg and 'aria-hidden="true"' in svg
    assert svg.count("<path ") == 1 and "<text" not in svg and "font" not in svg
    assert svg.count("Z") == 3, "three closed outlines: ع, and ע's two"


def test_a_lookup_is_counted_without_the_word():
    assert blau_panel.lookup_event(False, empty_lookup("כתאב"), None) == {'how': 'word', 'blau': False, 'lane': None}
    assert blau_panel.lookup_event(True, None, empty_lane("כתאב")) == {'how': 'box', 'blau': None, 'lane': False}
    assert {blau_panel.POSTHOG_SWITCH, blau_panel.POSTHOG_LOOKUP, blau_panel.POSTHOG_TAB} == {
        "dictionary_switch", "dictionary_lookup", "dictionary_tab"}


def test_reset_hints_makes_the_switch_new_again():
    main = (REPO / "web" / "main.py").read_text(encoding="utf-8")
    route = main[main.index("def reset_hints_route"):main.index("@ui.page('/settings'")]
    assert "SEEN_KEY as dictionaries_seen" in route and "'hint_tabular_seen', dictionaries_seen)" in route


def test_the_switch_says_what_pressing_it_will_do():
    from web.translations import using_language

    with using_language("en"):
        assert blau_panel.switch_tooltip(False) == "Turn on the dictionaries (Judeo-Arabic)"
        assert blau_panel.switch_tooltip(True) == "Turn off the dictionaries"
    with using_language("he"):
        assert blau_panel.switch_tooltip(False) == "הפעלת המילונים (ערבית־יהודית)"


def test_every_new_panel_string_has_its_hebrew():
    from shared.genizah_translations import TRANSLATIONS

    for text in ("Turn on the dictionaries (Judeo-Arabic)", "Turn off the dictionaries",
                 "Type a word to look up", "A word in Hebrew or Arabic letters (ד' for ד̇)", "Look up",
                 "Type a Judeo-Arabic word in Hebrew letters, or an Arabic word.",
                 "spelled without the dot"):
        assert TRANSLATIONS.get(text, "").strip(), text
        assert text in PANEL_SRC, f"{text!r} is not the renderer's string any more"
    for text in (blau_panel.TITLE, blau_panel.BLAU_TITLE, blau_panel.NOTHING, *blau_panel.TAB_LABELS.values()):
        assert TRANSLATIONS.get(text, "").strip(), text
    assert "tr(DISCLAIMER)" in PANEL_SRC
    assert "tr(TITLE if several else titles[tabs[0]])" in PANEL_SRC
    assert "label=tr(TAB_LABELS[name])" in PANEL_SRC and "ui.label(tr(NOTHING))" in PANEL_SRC


def test_the_page_s_hint_names_no_one_dictionary():
    """Owner, 2026-11-04: the panel is not Blau's alone, so the hint beside the switch
    does not say Blau."""
    from shared.genizah_translations import TRANSLATIONS

    browse = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
    hint = "Click a word to look it up in the dictionaries"
    assert f"ui.label(tr('{hint}'))" in browse
    assert TRANSLATIONS[hint] == "לחיצה על מילה מחפשת אותה במילונים"
    assert "בלאו" not in TRANSLATIONS[hint] and "בלאו" not in blau_panel.switch_tooltip(True)


def test_a_lookup_opens_on_the_reader_s_tab_while_it_has_an_answer():
    tabs = [blau_panel.BLAU_TAB, blau_panel.LANE_TAB]
    both = {blau_panel.BLAU_TAB: True, blau_panel.LANE_TAB: True}
    assert blau_panel.pick_tab(tabs, both) == "blau", "no choice yet: the first tab with an answer"
    assert blau_panel.pick_tab(tabs, both, "lane") == "lane", "the reader's choice holds"
    lane_only = {blau_panel.BLAU_TAB: False, blau_panel.LANE_TAB: True}
    assert blau_panel.pick_tab(tabs, lane_only) == "lane", "Blau has nothing: Lane"
    assert blau_panel.pick_tab(tabs, lane_only, "blau") == "lane", "the chosen tab has nothing: one that has"
    blau_only = {blau_panel.BLAU_TAB: True, blau_panel.LANE_TAB: False}
    assert blau_panel.pick_tab(tabs, blau_only, "lane") == "blau"
    none = {blau_panel.BLAU_TAB: False, blau_panel.LANE_TAB: False}
    assert blau_panel.pick_tab(tabs, none, "lane") == "lane", "nothing anywhere: the reader's tab stays"
    assert blau_panel.pick_tab(tabs, none) == "blau" and blau_panel.pick_tab(tabs, none, "other") == "blau"
    assert blau_panel.pick_tab(["lane"], both, "blau") == "lane", "a choice of a tab not shown is ignored"


def test_the_disclaimer_is_the_owner_s_hebrew():
    """Owner, 2026-11-04: a disclaimer on every panel, in these words."""
    from shared.genizah_translations import TRANSLATIONS

    assert TRANSLATIONS[blau_panel.DISCLAIMER] == (
        "מילון בלאו מתעד רק הוראות ייחודיות בערבית-יהודית. "
        "כדי לחפש פעלים ונטיות יש להקליד את השורש או את צורת הבסיס.")


def test_the_digital_edition_credits_midrash_in_both_languages():
    """Owner, 2026-11-04 (Ephraim Meiri's request): the MiDRASH project is credited
    on every panel, linked to the site's about page, which names the team."""
    from shared.genizah_translations import TRANSLATIONS

    he = TRANSLATIONS.get(blau_panel.MIDRASH_CREDIT, "")
    assert "MiDRASH" in blau_panel.MIDRASH_CREDIT and "101071829" in blau_panel.MIDRASH_CREDIT
    assert "MiDRASH" in he and "101071829" in he and "האיחוד האירופי" in he
    assert "tr(MIDRASH_CREDIT), ABOUT_URL" in PANEL_SRC
    assert blau_panel.ABOUT_URL == "https://blau-dict.netlify.app/about.html"


def test_enter_in_a_box_that_works_in_the_page_does_not_start_the_top_bar():
    """The site's top bar starts on Enter in any text box and is hidden only by a
    page load or a 15-second fallback; the dictionary box and the /search query
    work in the page, so it ran on after every query (owner, 2026-11-04)."""
    main = (REPO / "web" / "main.py").read_text(encoding="utf-8")
    enter = main[main.index("// 2. Trigger on Enter key"):main.index("// 3. KEY FIX")]
    assert "closest('[data-no-loading-bar]')) return;" in enter
    browse = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
    assert "aria-live=\"polite\" data-no-loading-bar" in browse
    search = (REPO / "web" / "pages" / "search.py").read_text(encoding="utf-8")
    assert "data-ux-enter=search_results data-no-loading-bar" in search


def test_the_script_s_word_characters_match_the_normalizer_s_word():
    """The class the script widens a click over, run as a Python regex (same
    syntax): Hebrew letters, combining marks and the quote family are word
    characters; the Hebrew punctuation between the marks is not -- a maqaf ends
    a word for `normalize_ja` too (Codex on #473)."""
    source = re.search(r"var WORD_CHAR = /(\[.*?\])/;", JS).group(1)
    word_char = re.compile(source)
    for ch in "אבת" + "ָׇּׁ̇" + "'\"׳״‘’":
        assert word_char.match(ch), hex(ord(ch))
    for ch in "־׀׃׆" + " .,;:()[]|" + "abc" + "ذل":
        assert not word_char.match(ch), hex(ord(ch))


def test_the_stylesheet_styles_what_the_renderer_and_the_script_make():
    for selector in (f".{blau_panel.POPOVER_CLASS} {{", f".{blau_panel.HIT_CLASS} {{",
                     f".{blau_panel.PANEL_CLASS} .credit {{", f".{blau_panel.PANEL_CLASS} .card {{",
                     f".{blau_panel.PANEL_CLASS} .sense {{", f".{blau_panel.PANEL_CLASS} .sense.matched {{",
                     "[data-blau-place=\"above\"]",
                     "--blau-left", "--blau-top", "--blau-bottom",
                     f".{blau_panel.SWITCH_CLASS}.active {{", f".{blau_panel.HINT_CLASS} {{",
                     f".{blau_panel.SWITCH_CLASS}.{blau_panel.NEW_CLASS} {{",
                     f".{blau_panel.SWITCH_CLASS} .{blau_panel.GLYPH_CLASS} svg {{",
                     f".{blau_panel.SWITCH_CLASS} .{blau_panel.LABEL_CLASS} {{",
                     f".{blau_panel.PANEL_CLASS} .panel-head .word-input input {{",
                     f".{blau_panel.PANEL_CLASS} .type-note {{",
                     f".{blau_panel.PANEL_CLASS} .{blau_panel.TABS_CLASS} {{",
                     f".{blau_panel.PANEL_CLASS} .{blau_panel.TAB_CLASS} {{",
                     f".{blau_panel.PANEL_CLASS} .{blau_panel.TAB_CLASS}.not-found {{",
                     f".{blau_panel.PANEL_CLASS} .{blau_panel.TAB_PANELS_CLASS} {{"):
        assert selector in CSS, selector
    # On a phone the switch is the letters alone, with a dot while new; from 640px, its label.
    phone = CSS[CSS.index("@media (max-width: 639px) {\n    .blau-switch"):]
    phone = phone[:phone.index("\n}\n")]
    assert f".{blau_panel.SWITCH_CLASS} .{blau_panel.LABEL_CLASS} {{ display: none; }}" in phone
    assert f".{blau_panel.SWITCH_CLASS}.{blau_panel.NEW_CLASS}::after {{" in phone
    # The chip's padding mirrors in Hebrew: logical sides only (the letters at the start).
    switch_rules = CSS[CSS.index(f".{blau_panel.SWITCH_CLASS}.q-btn {{"):]
    switch_rules = switch_rules[:switch_rules.index("@media (min-width: 640px)") + 300]
    assert "padding-inline: 8px 12px" in switch_rules
    paddings = re.findall(r"padding:\s*([^;]+);", switch_rules)
    assert paddings and all(len(p.split()) < 4 for p in paddings), f"a 4-value padding is not mirrored: {paddings}"
    # The script places the popover with the size the stylesheet gives it.
    js_width = int(re.search(r"var WIDTH = (\d+)", JS).group(1))
    js_height = int(re.search(r"var HEIGHT = (\d+)", JS).group(1))
    wide = CSS[CSS.index(f".{blau_panel.POPOVER_CLASS} {{"):]
    wide = wide[wide.index("@media (min-width: 1024px)"):]
    assert f"width: {js_width}px" in wide and f"max-height: {js_height}px" in wide


def test_render_lookup_draws_nothing_without_a_credit_line():
    assert blau_panel.render_lookup(empty_lookup("כתאב"), credit_he="") is False
    assert blau_panel.render_lookup(empty_lookup("כתאב"), credit_he="   ") is False
    assert blau_panel.render_lookup(None, lane=empty_lane("כתאב"), lane_credit=" ") is False
    assert blau_panel.render_lookup(None, credit_he="a credit") is False, "no dictionary served"
