# -*- coding: utf-8 -*-
"""web/components/lane_panel.py without a browser: every class it draws is styled under the
popover's scope, and every string it shows has its Hebrew (a missing one would show English in
the Hebrew UI). The drawing itself is pinned by tests/render_smoke/test_blau_panel_render_smoke.py."""
from __future__ import annotations

import re
from pathlib import Path

from shared.genizah_translations import TRANSLATIONS
from web.components import lane_panel

REPO = Path(lane_panel.__file__).resolve().parents[2]
CSS = (REPO / "web" / "static" / "common.css").read_text(encoding="utf-8")
SRC = Path(lane_panel.__file__).read_text(encoding="utf-8")


def test_every_lane_class_is_styled_under_the_popover_scope():
    for cls in (lane_panel.SECTION_CLASS, lane_panel.READINGS_CLASS, lane_panel.CHIP_CLASS,
                lane_panel.ROOT_CLASS, lane_panel.ENTRY_CLASS, lane_panel.NO_ROOT_CLASS):
        assert f".gs-blau .{cls}" in CSS, cls


def test_every_shown_string_has_its_hebrew():
    shown = {lane_panel.TITLE, lane_panel.READINGS_LABEL, lane_panel.NO_ROOT, lane_panel.SOURCE_NOTE,
             lane_panel.RANKING_NOTE}
    shown |= set(re.findall(r"tr\('([^']+)'\)", SRC)) | set(re.findall(r'tr\("([^"]+)"\)', SRC))
    missing = sorted(t for t in shown if not TRANSLATIONS.get(t))
    assert not missing, missing
