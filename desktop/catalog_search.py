# -*- coding: utf-8 -*-
"""The catalogue text search on the desktop (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md §4.4, PR 3).

The search mode combo's rows, in one place: before this module the index -> engine mode lists were
written out in five places of genizah_app.py, and a new mode had to be added to each. The rows are
fixed (an index is persisted nowhere, but the telemetry enum and PGP Tags' index 7 are pinned), so
the catalogue is appended at index 8, shown only where its file is installed.

The installer carries ``catalog_data/catalog_text.db`` (plan D5): the desktop app offers the mode
when the file is where the service looks for it, unless CATALOG_TEXT_SEARCH_ENABLED turns it off
(unset, the search is on here; on the website it is off until set). A file the service then
refuses (a bad copy) answers no rows.
"""
from __future__ import annotations

import os
from typing import Optional

from PyQt6.QtCore import QThread, pyqtSignal

from shared.catalog_text_rows import CATALOG_NEAR_TOOLTIP, CATALOG_TRANSLATIONS_TOOLTIP  # noqa: F401 (re-exported)

#: The combo's rows: (the engine mode a search runs in, the telemetry key). Responsa runs in the
#: engine's 'exact' with its options; PGP Tags runs no engine search.
COMBO_ROWS = (
    ("literal", "keyword"),       # 0 Exact (=)
    ("variants", "variants"),     # 1 Variants (?)
    ("exact", "responsa"),        # 2 Responsa (R)
    ("fuzzy", "fuzzy"),           # 3 Fuzzy (~)
    ("Regex", "regex"),           # 4 Regex (/)
    ("Title", "title"),           # 5 Title ($)
    ("Shelfmark", "shelfmark"),   # 6 Shelfmark (#)
    (None, "pgp_tags"),           # 7 PGP Tags
    ("Catalog", "catalog"),       # 8 Catalog (%) -- only where its file is installed
)
MODE_INDEX_RESPONSA = 2
MODE_INDEX_PGP_TAGS = 7
MODE_INDEX_CATALOG = 8
#: The combo index a mode a prefix names selects (parse_query_syntax / the live shortcut).
_INDEX_OF_PREFIX_MODE = {"literal": 0, "exact": 0, "variants": 1, "responsa": 2, "fuzzy": 3,
                         "Regex": 4, "Title": 5, "Shelfmark": 6, "Catalog": MODE_INDEX_CATALOG}


def engine_mode(index: int) -> str:
    """The engine mode the combo row *index* searches in ('literal' for anything unknown)."""
    if 0 <= index < len(COMBO_ROWS) and COMBO_ROWS[index][0]:
        return COMBO_ROWS[index][0]
    return "literal"


def step_mode(index: int) -> str:
    """The mode a refinement step records for the combo row *index* (Responsa as 'responsa')."""
    return "responsa" if index == MODE_INDEX_RESPONSA else engine_mode(index)


def telemetry_mode(index: int) -> str:
    """The search_executed mode key of the combo row *index* (never the translated label)."""
    if 0 <= index < len(COMBO_ROWS):
        return COMBO_ROWS[index][1]
    return "keyword"


def combo_index(mode: str, catalog_offered: bool) -> Optional[int]:
    """The combo row a prefix's *mode* selects; None for a mode the combo has no row for
    (a variant level is the Variants row's, chosen by the caller first), or the catalogue
    where it is not offered."""
    index = _INDEX_OF_PREFIX_MODE.get(mode)
    if index == MODE_INDEX_CATALOG and not catalog_offered:
        return None
    return index


def catalog_offered() -> bool:
    """Whether the combo offers the catalogue: its file is where the service looks for it and
    CATALOG_TEXT_SEARCH_ENABLED does not turn it off (unset is on in this app)."""
    from shared.catalog_text_service import catalog_text_enabled, default_sidecar_path
    path = default_sidecar_path()
    return bool(path) and os.path.isfile(path) and catalog_text_enabled(default=True)


def catalog_options(near_spellings: bool, include_translations: bool) -> dict:
    """The switches a catalogue search runs with, as the engine takes them."""
    return {"near_spellings": bool(near_spellings), "include_translations": bool(include_translations)}


def source_text(row) -> str:
    """The Source column of a catalogue row ('' for any other row), in the app's language."""
    from genizah_core import tr
    from shared.catalog_text_rows import source_label
    return source_label(row, tr)


class CatalogCountWorker(QThread):
    """How many manuscripts' catalogue records hold *query* (with the switches *options*), off
    the UI thread, for the offer under a text search that found nothing. Emits
    ``counted(run_id, query, total)``; 0 when the catalogue cannot answer."""

    counted = pyqtSignal(int, str, int)

    def __init__(self, query: str, options: dict, run_id: int, parent=None):
        super().__init__(parent)
        self.query = query
        self.options = dict(options)
        self.run_id = run_id

    def run(self):
        total = 0
        try:
            from shared.catalog_text_service import get_catalog_text_service
            service = get_catalog_text_service()
            if service.is_ready():
                total = service.search(self.query, limit=0, **self.options).total
        except Exception:  # noqa: BLE001 -- an offer that cannot be counted is not shown
            total = 0
        self.counted.emit(self.run_id, self.query, int(total))
