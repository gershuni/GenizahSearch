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
from collections import OrderedDict
from typing import Optional

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import QComboBox

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
MODE_INDEX_TITLE = 5


class ModeCombo(QComboBox):
    """The search-mode combo, Catalog shown right under Title (owner, 2026-11-18), as on the
    website. The window and the saved search history name a mode by its index in COMBO_ROWS,
    fixed since before the catalogue came (Catalog 8, after PGP Tags 7; a history entry keeps
    it): this combo shows its rows in another order (show_after) and still answers in those
    indices -- currentIndex, setCurrentIndex, itemText, setItemText, itemData, setItemData.

    currentIndexChanged carries the row shown, a Qt signal this class does not translate: its
    slot reads currentIndex() (genizah_app.py connects it so)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._shown: list[int] = []          # _shown[row] = the mode index shown in that row

    def addItem(self, *args):
        super().addItem(*args)
        self._shown.append(len(self._shown))

    def addItems(self, texts):
        for text in texts:
            self.addItem(text)

    def show_after(self, index: int, after: int) -> None:
        """Show mode *index* in the row right under mode *after*; every index stays as it was,
        the current mode too. No change signal."""
        if index not in self._shown or after not in self._shown or index == after:
            return
        current = self.currentIndex()
        blocked = self.blockSignals(True)
        try:
            row = self._shown.index(index)
            items = self.model().takeRow(row)
            del self._shown[row]
            target = self._shown.index(after) + 1
            self.model().insertRow(target, items)
            self._shown.insert(target, index)
            QComboBox.setCurrentIndex(self, self._row(current))
        finally:
            self.blockSignals(blocked)

    def _row(self, index: int) -> int:
        return self._shown.index(index) if 0 <= index < len(self._shown) else index

    def currentIndex(self) -> int:
        row = super().currentIndex()
        return self._shown[row] if 0 <= row < len(self._shown) else row

    def setCurrentIndex(self, index: int) -> None:
        super().setCurrentIndex(self._row(index))

    def itemText(self, index: int) -> str:
        return super().itemText(self._row(index))

    def setItemText(self, index: int, text: str) -> None:
        super().setItemText(self._row(index), text)

    def itemData(self, index: int, role=Qt.ItemDataRole.UserRole):
        return super().itemData(self._row(index), role)

    def setItemData(self, index: int, value, role=Qt.ItemDataRole.UserRole) -> None:
        super().setItemData(self._row(index), value, role)


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


def show_mode_examples(combo, query_box, translate) -> None:
    """The query box follows the mode combo (shared/search_mode_examples.py, as the website's
    /search box does): its placeholder is one of the row's examples, drawn afresh on every change
    of row, and its tooltip opens with what to type in the mode, above the shortcuts it held
    when this was called (owner, 2026-11-18). PGP Tags hides the box, so its row keeps the text
    modes'."""
    from shared.search_mode_examples import mode_lead, mode_placeholder

    shortcuts = query_box.toolTip()

    def show(_row=None):
        # The mode is currentIndex(), not the signal's argument: a combo that shows its rows in
        # another order than its indices carries the row shown.
        mode = step_mode(combo.currentIndex())
        query_box.setPlaceholderText(mode_placeholder(mode, translate))
        lead = mode_lead(mode, translate)
        query_box.setToolTip(lead + "\n\n" + shortcuts if shortcuts else lead)

    combo.currentIndexChanged.connect(show)
    show()


def catalog_options(near_spellings: bool, include_translations: bool) -> dict:
    """The switches a catalogue search runs with, as the engine takes them."""
    return {"near_spellings": bool(near_spellings), "include_translations": bool(include_translations)}


def source_text(row) -> str:
    """The Source column of a catalogue row ('' for any other row), in the app's language."""
    from genizah_core import tr
    from shared.catalog_text_rows import source_label
    return source_label(row, tr)


def found_html(found: Optional[dict], translate=lambda s: s) -> str:
    """The viewer's "Found in the catalogue records" pane for ``found_in_catalogue``'s answer:
    each matching record under its source line, whole, its matched words marked as the viewer
    marks a search's words; '' when nothing was found. Every text is escaped."""
    import html

    from shared.catalog_text_rows import FOUND_MORE

    if not found or not found.get("records"):
        return ""

    def piece(text):
        # Escaped first, then its line breaks (tests/test_desktop_angle_marks.py's guard).
        return html.escape(text).replace("\n", "<br>")

    blocks = []
    for record in found["records"]:
        body = "".join(f"<b style='color:red;'>{piece(text)}</b>" if matched else piece(text)
                       for text, matched in record["segments"])
        direction = "rtl" if record.get("rtl") else "ltr"
        blocks.append(f"<p><span style='color:gray;'>{html.escape(record['label'])}</span><br>"
                      f"<span dir='{direction}'>{body}</span></p>")
    shown, count = len(found["records"]), int(found.get("count") or 0)
    if count > shown:
        more = translate(FOUND_MORE).format(shown=shown, count=count)
        blocks.append(f"<p style='color:gray;'>{html.escape(more)}</p>")
    return "".join(blocks)


#: Lookups whose answers the viewer and Browse keep (written on the GUI thread only, in the
#: worker's signal handler): a manuscript reopened draws its pane at once.
FOUND_CACHE_SIZE = 128
_FOUND_CACHE: "OrderedDict[tuple, tuple]" = OrderedDict()


def found_lookup(row) -> Optional[tuple]:
    """The pane's lookup for a catalogue search's row -- (sys_id, query, near_spellings,
    include_translations), from the search the row carries -- or None for any other row."""
    info = row.get("catalogue") if isinstance(row, dict) else None
    searched = info.get("searched") if isinstance(info, dict) else None
    sys_id = (row.get("display") or {}).get("id") if isinstance(row, dict) else None
    if not isinstance(searched, dict) or not sys_id or not (searched.get("query") or "").strip():
        return None
    return (str(sys_id), searched["query"], bool(searched.get("near_spellings")),
            bool(searched.get("include_translations", True)))


def cached_found(lookup) -> Optional[tuple]:
    """A lookup's kept answer, (matches, count), or None."""
    answer = _FOUND_CACHE.get(lookup)
    if answer is not None:
        _FOUND_CACHE.move_to_end(lookup)
    return answer


def keep_found(lookup, answer) -> None:
    _FOUND_CACHE[lookup] = answer
    _FOUND_CACHE.move_to_end(lookup)
    while len(_FOUND_CACHE) > FOUND_CACHE_SIZE:
        _FOUND_CACHE.popitem(last=False)


def found_html_for(answer, translate=lambda s: s) -> str:
    """``found_html`` of a lookup's answer, its labels in the reader's language."""
    from shared.catalog_text_rows import found_records
    return found_html(found_records(*answer, translate=translate), translate)


class CatalogFoundWorker(QThread):
    """A pane's lookup off the GUI thread: with near spellings it builds and looks up each word's
    candidates, up to a few tenths of a second a word (Codex review of PR #51). Emits
    ``answered(lookup, (matches, count))``; ``([], 0)`` when the catalogue cannot answer."""

    answered = pyqtSignal(object, object)

    def __init__(self, lookup: tuple, parent=None):
        super().__init__(parent)
        self.lookup = lookup

    def run(self):
        answer = ([], 0)
        try:
            from shared.catalog_text_rows import catalogue_matches
            sys_id, query, near, translations = self.lookup
            answer = catalogue_matches(sys_id, query, {"near_spellings": near, "include_translations": translations})
        except Exception:  # noqa: BLE001 -- a pane that cannot be filled is not shown
            answer = ([], 0)
        if not self.isInterruptionRequested():
            self.answered.emit(self.lookup, answer)


class FoundLookupQueue:
    """One owner's pane lookups (the viewer's, Browse's): at most one ``CatalogFoundWorker``
    running at a time. A request while one runs waits, and only the latest waiting one runs next
    (unless its answer is kept by then): paging through results opens no thread per result, and
    the first run fills the near spellings' cache for the next (Codex review of PR #51). Every
    answer is kept and handed to *on_answer(lookup, answer)* on the GUI thread."""

    def __init__(self, on_answer, parent=None):
        self._on_answer = on_answer
        self._parent = parent
        self._running = None
        self._waiting = None
        self._stopped = False

    def request(self, lookup) -> None:
        if self._stopped:
            return
        if self._running is not None:
            self._waiting = None if self._running.lookup == lookup else lookup
            return
        self._start(lookup)

    def _start(self, lookup) -> None:
        worker = CatalogFoundWorker(lookup, self._parent)
        worker.answered.connect(self._answered)
        worker.finished.connect(lambda w=worker: self._finished(w))
        self._running = worker
        worker.start()

    def _answered(self, lookup, answer) -> None:
        keep_found(lookup, answer)
        if not self._stopped:
            self._on_answer(lookup, answer)

    def _finished(self, worker) -> None:
        worker.deleteLater()
        if self._running is worker:
            self._running = None
        lookup, self._waiting = self._waiting, None
        if lookup is None or self._stopped:
            return
        kept = cached_found(lookup)
        if kept is not None:
            self._on_answer(lookup, kept)
        else:
            self._start(lookup)

    def stop(self, timeout_ms: int = 2000) -> None:
        """No more answers; the running lookup is interrupted and waited for (the owner closes)."""
        self._stopped = True
        self._waiting = None
        worker = self._running
        if worker is not None and worker.isRunning():
            worker.requestInterruption()
            if not worker.wait(timeout_ms):
                worker.terminate()
                worker.wait()


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
