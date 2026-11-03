# -*- coding: utf-8 -*-
"""web/components/blau_panel.py without a browser: the event payload parser, the head line, and
the names the click script, the stylesheet and the renderer must agree on (a renamed class or
event on one side would leave the companion silently dead on the page)."""
from __future__ import annotations

import json
import re
from pathlib import Path

from shared.blau_service import empty_lookup
from web.components import blau_panel

REPO = Path(blau_panel.__file__).resolve().parents[2]
JS = (REPO / "web" / "static" / "js" / "blau_lookup.js").read_text(encoding="utf-8")
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")


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


def test_the_stylesheet_styles_what_the_renderer_and_the_script_make():
    for selector in (f".{blau_panel.POPOVER_CLASS} {{", f".{blau_panel.HIT_CLASS} {{",
                     f".{blau_panel.PANEL_CLASS} .credit {{", f".{blau_panel.PANEL_CLASS} .card {{",
                     f".{blau_panel.PANEL_CLASS} .sense {{", f".{blau_panel.PANEL_CLASS} .sense.matched {{",
                     "[data-blau-place=\"above\"]",
                     "--blau-left", "--blau-top", "--blau-bottom"):
        assert selector in CSS, selector
    # The script places the popover with the size the stylesheet gives it.
    js_width = int(re.search(r"var WIDTH = (\d+)", JS).group(1))
    js_height = int(re.search(r"var HEIGHT = (\d+)", JS).group(1))
    wide = CSS[CSS.index(f".{blau_panel.POPOVER_CLASS} {{"):]
    wide = wide[wide.index("@media (min-width: 1024px)"):]
    assert f"width: {js_width}px" in wide and f"max-height: {js_height}px" in wide


def test_render_lookup_draws_nothing_without_a_credit_line():
    assert blau_panel.render_lookup(empty_lookup("כתאב"), credit_he="") is False
    assert blau_panel.render_lookup(empty_lookup("כתאב"), credit_he="   ") is False
