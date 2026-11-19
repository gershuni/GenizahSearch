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
    # The dictionaries' key is among the keys the route pops (with the one-time "New"
    # marks' keys since 10.1.0), whatever follows it in that tuple.
    popped = re.search(r"for key in \((.*?)\):", route, re.S)
    assert "SEEN_KEY as dictionaries_seen" in route and popped
    assert re.search(r"\bdictionaries_seen\b", popped.group(1)), popped.group(1)
    assert "safe_user_pop(key, None)" in route


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

    browse = (REPO / "web" / "components" / "dictionary_lookup.py").read_text(encoding="utf-8")
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
    # The closest answer opens first, the first of equals; an action inside a tab
    # still stays on it; a tab with nothing is never chosen for its closeness.
    assert blau_panel.pick_tab(tabs, both, close={"blau": 2, "lane": 0}) == "lane"
    assert blau_panel.pick_tab(tabs, both, close={"blau": 1, "lane": 1}) == "blau"
    assert blau_panel.pick_tab(tabs, both, "blau", close={"blau": 3, "lane": 0}) == "blau"
    assert blau_panel.pick_tab(tabs, lane_only, close={"blau": 0, "lane": 4}) == "lane"
    assert blau_panel.pick_tab(tabs, both, close={"blau": None, "lane": None}) == "blau"
    assert blau_panel.pick_tab(["lane"], both, "blau") == "lane", "a choice of a tab not shown is ignored"


def test_the_disclaimer_is_the_owner_s_hebrew():
    """Owner, 2026-11-04: a disclaimer on every panel, in these words ("mainly", not
    "only": owner, 2026-11-05)."""
    from shared.genizah_translations import TRANSLATIONS

    assert TRANSLATIONS[blau_panel.DISCLAIMER] == (
        "מילון בלאו מתעד בעיקר הוראות ייחודיות בערבית-יהודית. "
        "כדי לחפש פעלים ונטיות יש להקליד את השורש או את צורת הבסיס.")


def test_the_digital_edition_credits_midrash_in_both_languages():
    """Owner, 2026-11-04: the MiDRASH project is credited
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
    browse = (REPO / "web" / "components" / "dictionary_lookup.py").read_text(encoding="utf-8")
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


def test_the_look_up_key_works_wherever_a_dictionary_is_served():
    """Owner, 2026-11-05: D or M tapped arms the next click, Alt + a click, selected words and
    the key, with the switch on or off. Held M + a touchpad failed: Windows ignores the
    touchpad while a letter key is held, so the key is tapped, not held."""
    browse = (REPO / "web" / "components" / "dictionary_lookup.py").read_text(encoding="utf-8")
    assert f"'[{blau_panel.KEY_PANE_ATTR}]'" in JS
    marked = browse[browse.index("    def mark_pane(self, container) -> bool:"):]
    marked = marked[:marked.index("    def switch(")]
    assert "lookup_on = self.on and blau_panel.lookup_on()" in marked
    assert "if self.on:\n            container.props(blau_panel.KEY_PANE_ATTR)" in marked
    assert "container.props[blau_panel.CHIP_LABEL_ATTR] = tr(blau_panel.CHIP_LABEL)" in marked
    page = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
    assert "blau_lookup_on = dictionaries.mark_pane(text_container)" in page
    # D and M by name, by the Hebrew letter, or by the physical key.
    assert ("event.code === 'KeyD' || event.code === 'KeyM' || event.key === 'd' || event.key === 'D' ||\n"
            "      event.key === 'm' || event.key === 'M' || event.key === 'מ'") in JS
    # A tap arms the next click; a key still down is held. Held or tapped is told by the key's
    # own down and up, not by auto-repeat: a click can come before the first repeat (Codex, PR
    # #484), so a click while it is down marks it held, and only a held key disarms on release.
    assert "down = true;\n    if (event.repeat) {" in JS
    assert "held = false;\n    if (lookUpSelection(pop, true)) { arm(false); return; }" in JS
    assert ("if (!isLookupKey(event) || !down) return;\n    down = false;\n"
            "    if (held) arm(false);") in JS
    assert "if (down) { down = false; arm(false); }" in JS          # blur: its keyup never comes
    assert "var repeated" not in JS and "repeated = " not in JS
    # ...used up by a click on the panel's or the switch's own controls too (Codex, PR #484):
    # the disarm comes before their early returns.
    click = JS[JS.index("document.addEventListener('click'"):]
    disarm = click.index("if (armed) { if (down) held = true; else arm(false); }")
    assert disarm < click.index("if (pop.contains(event.target)) {")
    assert disarm < click.index("if (opener) {")
    assert click.index("var viaKey = ") < disarm
    assert f"var ARMED = '{blau_panel.ARMED_CLASS}';" in JS and f"html.{blau_panel.ARMED_CLASS} [data-blau-pane]" in CSS
    # Alt + a click looks up too; a plain click only where the switch is on.
    assert "var viaAlt = event.altKey && !event.ctrlKey && !event.metaKey;" in JS
    assert "return el.closest(PANE) || (viaKey ? el.closest(KEY_PANE) : null);" in JS
    assert "var pane = target ? paneOf(target, viaKey) : null;" in JS
    # Never while typing (the panel's own box takes a D), never with Ctrl/Alt/Cmd, never under
    # an open dialog or menu (Codex, PR #484): all checked before the key arms.
    keydown = JS[JS.index("document.addEventListener('keydown'"):JS.index("document.addEventListener('keyup'")]
    for guard in ("if (event.ctrlKey || event.altKey || event.metaKey || !isLookupKey(event)) return;",
                  "if (inABox(event.target) || inABox(document.activeElement)) return;",
                  "if (overlayOpen(pop)) return;"):
        assert guard in keydown and keydown.index(guard) < keydown.index("down = true;"), guard
        assert keydown.index(guard) < keydown.index("arm(true);"), guard
    # A menu always; a dialog unless it holds the panel (Quick View): tests/js/blau_lookup_dialog_check.js.
    assert "if (document.querySelector('.q-menu') !== null) return true;" in JS
    assert "if (!pop || !dialogs[i].contains(pop)) return true;" in JS


def test_esc_closes_the_panel_from_anywhere_but_a_dialog():
    keydown = JS[JS.index("document.addEventListener('keydown'"):JS.index("document.addEventListener('keyup'")]
    esc = keydown[keydown.index("if (event.key === 'Escape') {"):keydown.index("if (event.ctrlKey")]
    assert "if (overlayOpen(pop)) return;" in esc and "escape(pop, event);" in esc
    closing = JS[JS.index("function escape(pop, event) {"):JS.index("var escUsed = false;")]
    assert "if (had) dispatch(pop, 'blau-close');" in closing and "arm(false);" in closing
    # Before the box guard: Esc closes the panel from its own box too.
    assert keydown.index("if (event.key === 'Escape') {") < keydown.index("inABox(event.target)")


def test_a_selection_is_looked_up_by_its_words():
    assert "lookUp(pop, picked, 'selection');" in JS
    assert "dispatch(pop, 'blau-lookup', { word: picked.text, how: how });" in JS
    assert "words.length < SELECTION_WORDS" not in JS and "if (part) words.push(part);" in JS
    assert "var part = cleanWord(parts[i]);" in JS
    assert "how: viaAlt ? 'alt' : viaKey ? 'key' : 'word'" in JS


def test_words_dragged_over_with_the_switch_on_are_looked_up_together():
    """With the switch on, selecting several words looked up only the word under
    the pointer, where the key looked up the selection. The drag ends in a click; a selection there
    (in the switched-on pane, a single click's) is the lookup, before the word under the pointer."""
    click = JS[JS.index("document.addEventListener('click'"):]
    drag = ("if (!viaKey && event.detail <= 1 && target && paneOf(target, false) && "
            "lookUpSelection(pop, false)) return;")
    assert drag in click
    assert click.index(drag) < click.index("var caret = pane ? caretAt(event.clientX, event.clientY) : null;")
    # Only the switched-on pane (`paneOf(..., false)`): with the switch off a selection is the key's.
    assert "return el.closest(PANE) || (viaKey ? el.closest(KEY_PANE) : null);" in JS


def test_a_phone_gets_a_look_up_chip_for_selected_words():
    """No key on a phone (owner, 2026-11-05): words selected there show a chip that looks them
    up, the switch on or off, labelled in the page's language."""
    from shared.genizah_translations import TRANSLATIONS

    assert "window.matchMedia('(hover: none)').matches" in JS
    assert "document.addEventListener('selectionchange'" in JS
    assert f"var CHIP = '{blau_panel.CHIP_CLASS}';" in JS and f".{blau_panel.CHIP_CLASS} {{" in CSS
    assert f"pane.getAttribute('{blau_panel.CHIP_LABEL_ATTR}')" in JS
    # Pressing it keeps the selection, and its click is not the page's "click elsewhere".
    assert "chip.addEventListener('pointerdown', function (e) { e.preventDefault(); });" in JS
    assert "e.stopPropagation();\n        var p = document.querySelector(POPOVER);\n        if (p && chipPick) lookUp(p, chipPick, 'chip');" in JS
    assert TRANSLATIONS[blau_panel.CHIP_LABEL].strip()
    # Not again for the words the open panel shows (it came back under the panel, 2026-11-05).
    assert "if (!picked || (picked.text === chipDone && shown(pop))) { hideChip(); return; }" in JS


def test_how_a_lookup_started_is_counted():
    assert blau_panel.lookup_how({"detail": json.dumps({"word": "x", "how": "key"})}) == "key"
    assert blau_panel.lookup_how(json.dumps({"word": "x", "how": "selection"})) == "selection"
    for how in ("alt", "chip"):
        assert blau_panel.lookup_how(json.dumps({"word": "x", "how": how})) == how
    for args in (None, "not json", {"detail": json.dumps({"word": "x"})}, {"detail": json.dumps({"how": "evil"})}):
        assert blau_panel.lookup_how(args) == "word", args
    assert blau_panel.lookup_event(False, empty_lookup("x"), None, "key")["how"] == "key"
    assert blau_panel.lookup_event(True, empty_lookup("x"), None, "key")["how"] == "box"
    assert blau_panel.lookup_event(False, empty_lookup("x"), None, "nonsense")["how"] == "word"
    browse = (REPO / "web" / "components" / "dictionary_lookup.py").read_text(encoding="utf-8")
    assert ("_blau_start(word, False, blau_panel.lookup_how(e.args), stems=False,\n"
            "                            beside=blau_panel.lookup_context(e.args))") in browse
    assert "_blau_start(text, True, tab=tab, stems=False)" in browse, "the gap control keeps an expression one"
    assert "lane_word = (root_and_stem(word)[0] if stems else '') or word" in browse, "Lane reads the whole selection"


def test_the_switch_s_tooltip_names_the_key_in_both_languages():
    from shared.genizah_translations import TRANSLATIONS

    browse = (REPO / "web" / "components" / "dictionary_lookup.py").read_text(encoding="utf-8")
    assert "ui.label(tr(blau_panel.KEY_HINT)).classes(" in browse
    assert blau_panel.KEY_HINT == ("Press D or M, then click a word (or click it with Alt held); "
                                   "or select words and press D or M")
    assert TRANSLATIONS[blau_panel.KEY_HINT].strip()
    assert f".{blau_panel.TIP_CLASS} .{blau_panel.KEY_HINT_CLASS}" in CSS


def test_the_box_and_the_stem_beside_it_make_one_query():
    """A field for the stem beside the box; the typed shortcut still works."""
    assert blau_panel.box_query("דלל", 10) == "דלל X" and blau_panel.box_query(" ד'לל ", 4) == "ד'לל IV"
    assert blau_panel.box_query("דלל", None) == "דלל" and blau_panel.box_query("דלל", 0) == "דלל"
    assert blau_panel.box_query("דלל IV", 10) == "דלל IV", "a stem typed in the box wins over the selector"
    assert blau_panel.box_query("", 10) == "" and blau_panel.box_query("   ", 4) == ""
    # Several words are an expression search: a stem left chosen for an earlier root stays out of it.
    assert blau_panel.box_query("כתב כטה", 10) == "כתב כטה" and blau_panel.box_query(" כתב  כטה ", 4) == "כתב כטה"
    assert f".{blau_panel.PANEL_CLASS} .panel-head .stem-select {{" in CSS
    assert "stems=BLAU_TAB in tabs or FRIEDMAN_TAB in tabs" in PANEL_SRC, "no stem field where only Lane is served"
    # The field's list is drawn outside the popover; a choice there closed the panel (seen in the
    # browser, 2026-11-11). The script and the field name the list alike, and the script returns
    # on it before its "click elsewhere" close.
    assert "popup-content-class={STEM_MENU_CLASS}" in PANEL_SRC
    assert f"var STEM_MENU = '{blau_panel.STEM_MENU_CLASS}';" in JS
    click = JS[JS.index("document.addEventListener('click'"):]
    menu = click.index("event.target.closest('.dict-gap-menu')")
    assert menu < click.index("if (open) dispatch(pop, 'blau-close');")


def test_a_clicked_word_comes_with_the_words_beside_it():
    """The script sends the words beside a clicked word (at most two a side, not
    across a sentence's end), read from the text nodes with a space where a line breaks and nothing of
    the line numbers' gutter, before the word is marked; the page takes words with a Hebrew letter."""
    from shared.ja_expression import MAX_CONTEXT_WORDS

    assert f"var CONTEXT_WORDS = {MAX_CONTEXT_WORDS};" in JS
    assert "var GUTTER = 'line-number-gutter';" in JS
    # Reading boundaries, gutter exclusion and inline spans are executed under Node
    # in test_blau_lookup_js.py; these assertions cover the event wiring only.
    click = JS[JS.index("document.addEventListener('click'"):]
    assert click.index("var beside = wordsBeside(word);") < click.index("markWord(word)")
    assert "before: beside.before, after: beside.after });" in click
    assert "SENTENCE_END = /[.:׃!?…]$/" in JS
    # A word broken inside by a gap ("מ].נ") ends the words beside it, on either side.
    assert "if (w === null) break;" in JS and "if (a === null) break;" in JS
    args = {"detail": json.dumps({"word": "אללה", "before": ["כתאבי", "אטאל", "x", "12"],
                                  "after": ["בקאך", "ואדאם", "סלאמתך"]})}
    assert blau_panel.lookup_context(args) == (("כתאבי", "אטאל"), ("בקאך", "ואדאם", "סלאמתך"))
    marked = {"before": ["וסלאמת[ך"], "after": ["וברכ[תך]"]}
    assert blau_panel.lookup_context(marked) == (("וסלאמת[ך",), ("וברכ[תך]",))
    long_before = {"detail": json.dumps({"before": ["אחד", "שני", "שלישי"], "after": "nope"})}
    assert blau_panel.lookup_context(long_before) == (("אחד", "שני", "שלישי"), ()), "up to four neighbouring words"
    for bad in (None, "x", {"detail": "{"}, {"detail": json.dumps({"before": ["a b", "א" * 41, 3, "מ].נ"]})}):
        assert blau_panel.lookup_context(bad) == ((), ()), bad


def test_the_panel_opens_on_the_answer_closest_to_the_word():
    """Priority to the closest match, not only to an expression with the words beside
    it. An answer's closeness: that expression; the word itself; its base form (a clitic peeled); a
    guessed root (another dictionary's, a verb form, Lane's reading); only quotations. Spelled without
    the word's dot, half a step further."""
    from dataclasses import replace

    from shared.arabic_service import LaneEntry, LaneLookup, LaneReading, LaneRoot
    from shared.blau_service import BlauCard, BlauExpression, BlauLookup
    from shared.friedman_service import FriedmanLookup, FriedmanMatch, FriedmanUnit

    card = BlauCard(root_id="e1", name="קול", arabic="", page=1, url="u", candidate="קול", matched_key="קול",
                    matched_kind="root", sub_entry="", via="", senses=(), more_senses=0)
    blau = BlauLookup(word="קול", key="קול", candidates=("קול",), cards=(card,), more_cards=0, examples=(),
                      more_examples=0)
    close = blau_panel.closeness
    assert close(blau) == blau_panel.CLOSE_WORD
    assert close(replace(blau, key="אלקול")) == blau_panel.CLOSE_BASE
    assert close(replace(blau, cards=(replace(card, dot_mismatch=True),))) == blau_panel.CLOSE_WORD + 0.5
    assert close(replace(blau, cards=(replace(card, borrowed=(("lane", "قول"),)),))) == blau_panel.CLOSE_GUESS
    assert close(replace(blau, cards=(replace(card, verb_form="קאל (קול I)"),))) == blau_panel.CLOSE_GUESS
    assert close(replace(blau, cards=(replace(card, verb_form="x"), card))) == blau_panel.CLOSE_WORD, "its closest card"
    from shared.blau_service import BlauExpressionHit
    from shared.ja_expression import ExpressionPlace

    named = BlauExpressionHit("e2", "אטאל אללה", "", 1, "u", "", (ExpressionPlace("form", 0, (), (0, 1), ()),), 1)
    expression = BlauExpression("אטאל אללה", ("אטאל", "אללה"), (), 0, (named,), False)
    assert close(replace(blau, context=(expression,))) == blau_panel.CLOSE_CONTEXT
    # A quotation co-occurrence stays available but cannot displace a lexical match.
    cited = replace(expression, hits=(replace(named, places=(ExpressionPlace("citation", 0, (), (0, 2), ()),)),))
    assert replace(blau, context=(cited,)).context_quoted and not replace(blau, context=(expression,)).context_quoted
    assert close(replace(blau, context=(cited,))) == blau_panel.CLOSE_WORD
    assert close(replace(blau, cards=(replace(card, dot_mismatch=True),), context=(cited,))) \
        == blau_panel.CLOSE_WORD + .5 < blau_panel.CLOSE_QUOTED
    assert close(replace(blau, key="אלקול", context=(cited,))) == blau_panel.CLOSE_BASE
    assert close(replace(blau, cards=(), context=(cited,))) == blau_panel.CLOSE_QUOTED
    from shared.ja_expression import ExpressionComponent
    inferred_place = replace(cited.hits[0].places[0], components=(
        ExpressionComponent('query', 'key', 'printed', '@root:1', 'inflection'),))
    inferred = replace(cited, hits=(replace(named, places=(inferred_place,)),))
    assert close(replace(blau, cards=(), context=(inferred,))) == blau_panel.CLOSE_QUOTED
    assert close(replace(blau, cards=(replace(card, analyses=(('source_suffix',),)),))) > blau_panel.CLOSE_BASE
    assert close(expression) == blau_panel.CLOSE_WORD - 0.5, "an expression in a head precedes quotation co-occurrence"
    assert close(replace(blau, cards=())) is None and close(None) is None
    unit = FriedmanUnit("f1", "entry", "ק", None, "קול", "קול", "קול", "קול", "", "", "", "1", "u", (), (), 0)
    match = FriedmanMatch(unit, "head", "קול", None, (), 0)
    friedman = FriedmanLookup("קול", "קול", ("קול",), (match,), False, "s", "c")
    assert close(friedman) == blau_panel.CLOSE_WORD
    for via, level in (("stem", blau_panel.CLOSE_BASE), ("index", blau_panel.CLOSE_BASE),
                       ("borrowed", blau_panel.CLOSE_GUESS), ("verb_form", blau_panel.CLOSE_GUESS)):
        assert close(replace(friedman, matches=(replace(match, via=via), match))) == level, "by its first match"
    entry = LaneEntry("l1", "قول", 1, "t", False, "u", matched=True)
    lane = LaneLookup("קול", "ja", (LaneReading("قول", "قول", 9),), (LaneRoot("qwl", "قول", 1, "u", (entry,), 0),))
    assert close(lane) == blau_panel.CLOSE_WORD + 0.75
    lane_guess = replace(lane, roots=(replace(lane.roots[0], entries=(replace(entry, matched=False),)),))
    assert close(lane_guess) == blau_panel.CLOSE_GUESS + 0.5
    # The word itself in Friedman's head beats Blau's root borrowed from Lane; equals: the first tab.
    tabs, found = ["blau", "friedman", "lane"], {"blau": True, "friedman": True, "lane": True}
    borrowed = replace(blau, cards=(replace(card, borrowed=(("lane", "قول"),)),))
    assert blau_panel.pick_tab(tabs, found, close={"blau": close(borrowed), "friedman": close(friedman),
                                                   "lane": close(lane)}) == "friedman"
    assert blau_panel.pick_tab(tabs, found, close={"blau": close(blau), "friedman": close(friedman),
                                                   "lane": close(lane)}) == "blau"


def test_nothing_in_the_dictionary_panel_is_italic():
    """Friedman's printed words carry Arabic vowel marks (בَקאَהُ), and an italic
    face drew them as boxes; the panel's rules never set italic."""
    import re

    rules = re.findall(r"([^{}]*\.gs-blau[^{}]*)\{([^}]*)\}", CSS)
    assert rules
    assert [sel.strip() for sel, body in rules if "italic" in body] == []
