# -*- coding: utf-8 -*-
"""web/components/lane_panel.py without a browser: every class it draws is styled under the
popover's scope, and every string it shows has its Hebrew (a missing one would show English in
the Hebrew UI). The drawing itself is pinned by tests/render_smoke/test_blau_panel_render_smoke.py.

The module is read as SOURCE (its constants and its ``tr(...)`` literals), never imported:
importing a web component in this process changes what tests/test_findings_page.py renders when
it runs later in the same process (as tests/test_blau_panel.py does), and CI's chunks put
unrelated files together.
"""
from __future__ import annotations

import ast
from pathlib import Path

from shared.genizah_translations import TRANSLATIONS

REPO = Path(__file__).resolve().parents[1]
SRC = (REPO / "web" / "components" / "lane_panel.py").read_text(encoding="utf-8")
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")
TREE = ast.parse(SRC)
CONSTANTS = {node.targets[0].id: node.value.value for node in TREE.body
             if isinstance(node, ast.Assign) and len(node.targets) == 1
             and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Constant)
             and isinstance(node.value.value, str)}


def _translated_arguments() -> set[str]:
    """Every string ``tr(...)`` is called with: a literal, or a module constant by name."""
    out = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "tr" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                out.add(arg.value)
            elif isinstance(arg, ast.Name):
                out.add(CONSTANTS[arg.id])
            else:
                raise AssertionError(f"tr() with an argument this test cannot read: {ast.dump(arg)}")
    return out


def test_every_lane_class_is_styled_under_the_popover_scope():
    classes = [CONSTANTS[name] for name in ("SECTION_CLASS", "READINGS_CLASS", "CHIP_CLASS", "ROOT_CLASS",
                                            "ENTRY_CLASS", "NO_ROOT_CLASS", "EMPTY_SECTION_CLASS")]
    for cls in classes:
        assert f".gs-blau .{cls}" in CSS, cls


def test_every_shown_string_has_its_hebrew():
    shown = _translated_arguments()
    assert {CONSTANTS["TITLE"], CONSTANTS["READINGS_LABEL"], CONSTANTS["NO_ROOT"], CONSTANTS["SOURCE_NOTE"],
            CONSTANTS["RANKING_NOTE"], CONSTANTS["NOTHING"]} <= shown, "the constants the section shows are translated where drawn"
    missing = sorted(t for t in shown if not TRANSLATIONS.get(t))
    assert not missing, missing
