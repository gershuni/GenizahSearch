# -*- coding: utf-8 -*-
"""The switch of each Judeo-Arabic dictionary on /browse: ``BLAU_DICTIONARY_ENABLED``,
``LANE_DICTIONARY_ENABLED`` and ``FRIEDMAN_DICTIONARY_ENABLED``, read on every call.

A switch is on (``1``, ``true``, ``yes``, ``on``), off (anything else), or ``editors``: the
dictionary is served only to a signed-in editor or admin, a preview before it is released
(the editors are trusted with corrections, so with the preview too). Unset, a switch is
its service's default.

Each service keeps two questions apart: ``is_available()`` is the public one (ready and on), which
every surface that does not know who is reading asks, so ``editors`` stays hidden there by default;
``served_to(editor)`` adds the preview for a reader the page knows is an editor. The service's own
lookups answer whenever the switch is not off, since the page decided who may ask.
"""
from __future__ import annotations

import os

ON = "on"
OFF = "off"
EDITORS = "editors"
_ON_VALUES = frozenset({"1", "true", "yes", "on"})


def read_switch(name: str, default: str) -> str:
    """``ON``, ``OFF`` or ``EDITORS``: the variable ``name``'s value, or ``default`` while unset."""
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip().lower()
    if value in _ON_VALUES:
        return ON
    return EDITORS if value == EDITORS else OFF


def serves(state: str, editor: bool) -> bool:
    """Is a dictionary whose switch is ``state`` shown to this reader? On: everyone; editors: an
    editor or admin only; off: no one."""
    return state == ON or (state == EDITORS and bool(editor))
