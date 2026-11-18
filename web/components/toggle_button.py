# -*- coding: utf-8 -*-
"""A button that is on or off: the primary colour while on, the theme's secondary text colour
while off.

Off was Quasar's ``grey-7``/``grey-8``, a fixed grey that reads as a disabled button on the dark
theme (owner, 2026-11-18; the result filter chips had the same fix, ``filter-chip-off``).
Quasar's colour classes are ``!important`` and cannot be overridden from CSS, so an off button
carries no Quasar colour at all and takes ``.q-btn.btn-off`` (web/static/common.css), which
follows the theme's variables -- the outline of an outline button with it (``currentColor``).
"""
from __future__ import annotations

from typing import Any

#: The class an off button carries (web/static/common.css).
OFF_CLASS = 'btn-off'


def set_btn_on(btn: Any, on: bool) -> None:
    """Show *btn* (a ``ui.button``) as on (primary) or off (the theme's secondary text)."""
    btn.props(remove='color')
    if on:
        btn.props('color=primary')
    btn.classes(**{'remove' if on else 'add': OFF_CLASS})
