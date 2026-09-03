"""Desktop PGP links: the clickable results-table badge and the two PGP buttons.

Three surfaces, one rule: a PGP link is offered ONLY when a pgp_url is actually
known.

  * search results table -- the green "PGP" badge becomes an underlined,
    clickable cell carrying its url; a PGP row with no url keeps the plain badge
  * ResultDialog        -- btn_rd_pgp / btn_compact_pgp, hidden until a url arrives
  * Browse tab          -- btn_b_pgp, hidden until a url arrives

pgp_url is nullable TEXT in the sidecar (scripts/export_pgp_sidecar.py), so
"has PGP info" and "has a PGP url" are different questions and the tests keep
them apart.
"""

import os
import sqlite3
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QApplication, QPushButton, QTableWidget, QTableWidgetItem,
)

import pytest

from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed
# non-GUI run segfaults after thousands of NiceGUI/asyncio tests share the
# process (2026-08-21).


_APP = QApplication.instance() or QApplication([])

PGP_URL = "https://geniza.princeton.edu/documents/12345/"
OTHER_URL = "https://geniza.princeton.edu/documents/999/"


# ---------------------------------------------------------------------------
# 1. The data layer: sys_id -> pgp_url
# ---------------------------------------------------------------------------

def _build_pgp_db(path, documents, fragments):
    """Minimal pgp.db with just the two tables the url query joins."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, pgp_url TEXT)")
    conn.execute("CREATE TABLE document_fragments (sys_id TEXT, document_id INTEGER)")
    conn.executemany("INSERT INTO documents (pgpid, pgp_url) VALUES (?, ?)", documents)
    conn.executemany(
        "INSERT INTO document_fragments (sys_id, document_id) VALUES (?, ?)", fragments
    )
    conn.commit()
    conn.close()


@pytest.fixture
def pgp_urls_fn(tmp_path):
    """The real PgpService.get_pgp_urls_for_sys_ids bound to a temp sidecar."""
    from shared.document_service import PgpService

    db = tmp_path / "pgp.db"
    _build_pgp_db(
        str(db),
        documents=[
            (700, PGP_URL),        # higher pgpid, same sys_id as 300
            (300, OTHER_URL),      # LOWER pgpid -> must win for 'shared'
            (400, ""),             # empty url -> excluded
            (500, None),           # NULL url -> excluded
            (600, PGP_URL),
        ],
        fragments=[
            ("shared", 700),
            ("shared", 300),
            ("empty", 400),
            ("null", 500),
            ("plain", 600),
        ],
    )
    conn = sqlite3.connect(str(db), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    svc = PgpService.__new__(PgpService)
    svc._conn = conn
    yield svc.get_pgp_urls_for_sys_ids
    conn.close()


def test_urls_returns_only_sys_ids_with_a_real_url(pgp_urls_fn):
    got = pgp_urls_fn(["plain", "empty", "null", "absent"])
    assert got == {"plain": PGP_URL}, (
        "an empty or NULL pgp_url must be ABSENT, not mapped to '' -- the caller "
        "uses membership to decide whether to render a link"
    )


def test_urls_picks_the_lowest_pgpid_when_a_sys_id_has_several_documents(pgp_urls_fn):
    got = pgp_urls_fn(["shared"])
    assert got["shared"] == OTHER_URL, (
        "the link a row opens must not depend on SQLite row order"
    )
    # Deterministic across repeated calls, not just once.
    assert pgp_urls_fn(["shared"]) == got


def test_urls_empty_input_short_circuits(pgp_urls_fn):
    assert pgp_urls_fn([]) == {}


def test_urls_chunks_past_the_sqlite_variable_limit(pgp_urls_fn):
    # 1200 ids > the 999 SQLite variable limit; a single un-chunked IN() would
    # raise sqlite3.OperationalError and the method would return {}.
    ids = [f"filler{i}" for i in range(1200)] + ["plain"]
    assert pgp_urls_fn(ids) == {"plain": PGP_URL}


def test_urls_no_connection_returns_empty():
    from shared.document_service import PgpService

    svc = PgpService.__new__(PgpService)
    svc._conn = None
    assert svc.get_pgp_urls_for_sys_ids(["plain"]) == {}


# ---------------------------------------------------------------------------
# 2. PGPBadgeWorker carries the urls alongside the two badge sets
# ---------------------------------------------------------------------------

def _run_badge_worker(monkeypatch, link_ids, manual_ids, urls,
                      url_error=False):
    import shared.document_service as ds
    import shared.transcription_service as ts
    from gui_threads import PGPBadgeWorker

    monkeypatch.setattr(ds, "get_sys_ids_with_transcriptions",
                        lambda ids: set(link_ids))
    monkeypatch.setattr(ts, "get_sys_ids_with_manual_transcriptions",
                        lambda ids: set(manual_ids))

    def _urls(ids):
        if url_error:
            raise RuntimeError("pgp.db unavailable")
        return dict(urls)

    monkeypatch.setattr(ds, "get_pgp_urls_for_sys_ids", _urls)

    captured = {}
    w = PGPBadgeWorker(["a", "b"])
    w.finished.connect(lambda p, m, u: captured.update(pgp=p, manual=m, urls=u))
    w.run()  # synchronous: exercise run() without starting a thread
    return captured


def test_worker_emits_the_url_map(monkeypatch):
    got = _run_badge_worker(monkeypatch, {"a"}, {"a"}, {"a": PGP_URL})
    assert got["pgp"] == {"a"}
    assert got["urls"] == {"a": PGP_URL}


def test_worker_url_failure_leaves_the_badges_intact(monkeypatch):
    """A url query failure must degrade to unlinked badges, never to no badges."""
    got = _run_badge_worker(monkeypatch, {"a", "b"}, {"a"}, {}, url_error=True)
    assert got["pgp"] == {"a", "b"}
    assert got["manual"] == {"a"}
    assert got["urls"] == {}


def test_worker_skips_the_url_query_when_no_row_has_pgp(monkeypatch):
    import shared.document_service as ds
    import shared.transcription_service as ts
    from gui_threads import PGPBadgeWorker

    calls = []
    monkeypatch.setattr(ds, "get_sys_ids_with_transcriptions", lambda ids: set())
    monkeypatch.setattr(ts, "get_sys_ids_with_manual_transcriptions",
                        lambda ids: set())
    monkeypatch.setattr(ds, "get_pgp_urls_for_sys_ids",
                        lambda ids: calls.append(ids) or {})
    captured = {}
    w = PGPBadgeWorker(["a"])
    w.finished.connect(lambda p, m, u: captured.update(urls=u))
    w.run()
    assert captured["urls"] == {}
    assert calls == [], "no PGP rows -> no reason to touch pgp.db"


def test_worker_still_feeds_a_two_argument_slot(monkeypatch):
    """The third argument was APPENDED; old-shape slots must keep working."""
    import shared.document_service as ds
    import shared.transcription_service as ts
    from gui_threads import PGPBadgeWorker

    monkeypatch.setattr(ds, "get_sys_ids_with_transcriptions", lambda ids: {"a"})
    monkeypatch.setattr(ts, "get_sys_ids_with_manual_transcriptions",
                        lambda ids: set())
    monkeypatch.setattr(ds, "get_pgp_urls_for_sys_ids", lambda ids: {"a": PGP_URL})
    seen = {}
    w = PGPBadgeWorker(["a"])
    w.finished.connect(lambda pgp, manual: seen.update(n=len(pgp)))
    w.run()
    assert seen == {"n": 1}


# ---------------------------------------------------------------------------
# 3. The results-table badge cell
# ---------------------------------------------------------------------------

class _ResultsHarness:
    """Stub GenizahGUI exposing only the results-table PGP machinery."""

    COL_SYS_ID = 2
    COL_PGP = 9
    COL_TRANSCRIPTION = 12

    def __init__(self):
        self._PGP_URL_ROLE = Qt.ItemDataRole.UserRole
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(13)
        self._pgp_transcription_sys_ids = set()
        self._manual_transcription_sys_ids = set()
        self._pgp_url_by_sys_id = {}
        self.opened = []
        self.full_text_calls = 0

        for name in (
            "_make_pgp_badge_item", "_write_pgp_badge_cell", "_pgp_url_for_cell",
            "_on_results_cell_clicked", "_on_results_double_clicked",
            "_on_pgp_badges_loaded",
        ):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    # collaborators the real methods reach for
    def show_full_text(self):
        self.full_text_calls += 1

    def _apply_results_table_filters(self):
        return None

    def add_row(self, row, sys_id):
        self.results_table.setRowCount(max(self.results_table.rowCount(), row + 1))
        self.results_table.setItem(row, self.COL_SYS_ID, QTableWidgetItem(sys_id))


@pytest.fixture
def results(monkeypatch):
    h = _ResultsHarness()
    import genizah_app

    monkeypatch.setattr(genizah_app.QDesktopServices, "openUrl",
                        lambda url: h.opened.append(url.toString()))
    return h


def test_badge_with_a_url_is_dressed_as_a_link(results):
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    item = results._make_pgp_badge_item("s1")
    assert item.text() == "PGP"
    assert item.foreground().color() == QColor("#27ae60")
    assert item.font().underline() is True
    assert item.data(Qt.ItemDataRole.UserRole) == PGP_URL
    assert PGP_URL in item.toolTip()
    assert not (item.flags() & Qt.ItemFlag.ItemIsEditable)


def test_badge_without_a_url_stays_a_plain_badge(results):
    """"has PGP info" is NOT "has a PGP url" -- no underline we cannot honour."""
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {}
    item = results._make_pgp_badge_item("s1")
    assert item.text() == "PGP"
    assert item.font().underline() is False
    assert item.data(Qt.ItemDataRole.UserRole) is None
    assert item.toolTip() == ""


def test_no_badge_at_all_for_a_manuscript_without_pgp(results):
    results._pgp_transcription_sys_ids = {"s1"}
    assert results._make_pgp_badge_item("other") is None
    assert results._make_pgp_badge_item("") is None
    assert results._make_pgp_badge_item(None) is None


def test_write_cell_clears_the_cell_for_a_non_pgp_row(results):
    results.add_row(0, "other")
    results._pgp_transcription_sys_ids = {"s1"}
    results._write_pgp_badge_cell(0, "other")
    cell = results.results_table.item(0, results.COL_PGP)
    assert cell is not None and cell.text() == ""
    assert cell.data(Qt.ItemDataRole.UserRole) is None


def test_clicking_a_linked_badge_opens_the_pgp_page(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_PGP)
    assert results.opened == [PGP_URL]


def test_clicking_an_unlinked_badge_opens_nothing(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_PGP)
    assert results.opened == []


def test_clicking_another_column_opens_nothing(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_SYS_ID)
    assert results.opened == []


def test_double_click_on_a_linked_badge_does_not_also_open_the_dialog(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    results._write_pgp_badge_cell(0, "s1")
    index = results.results_table.model().index(0, results.COL_PGP)
    results._on_results_double_clicked(index)
    assert results.full_text_calls == 0, (
        "the first click already opened the PGP page; one gesture, one window"
    )


def test_double_click_elsewhere_still_opens_the_dialog(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    results._write_pgp_badge_cell(0, "s1")
    index = results.results_table.model().index(0, results.COL_SYS_ID)
    results._on_results_double_clicked(index)
    assert results.full_text_calls == 1


def test_double_click_on_an_unlinked_pgp_cell_still_opens_the_dialog(results):
    """Suppression is keyed on the URL, not on the column."""
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._write_pgp_badge_cell(0, "s1")
    index = results.results_table.model().index(0, results.COL_PGP)
    results._on_results_double_clicked(index)
    assert results.full_text_calls == 1


def test_badges_loaded_slot_stores_the_urls_and_links_the_rows(results):
    results.add_row(0, "s1")
    results.add_row(1, "s2")
    results._on_pgp_badges_loaded({"s1", "s2"}, set(), {"s1": PGP_URL})
    assert results._pgp_url_by_sys_id == {"s1": PGP_URL}
    assert results.results_table.item(0, results.COL_PGP).font().underline() is True
    assert results.results_table.item(1, results.COL_PGP).font().underline() is False
    assert results.results_table.item(1, results.COL_PGP).text() == "PGP"


def test_badges_loaded_slot_tolerates_the_old_two_argument_call(results):
    results.add_row(0, "s1")
    results._pgp_url_by_sys_id = {"s1": PGP_URL}
    results._on_pgp_badges_loaded({"s1"}, set())
    assert results._pgp_url_by_sys_id == {}, "a stale url must not survive"
    assert results.results_table.item(0, results.COL_PGP).font().underline() is False


# ---------------------------------------------------------------------------
# 4. The Browse tab button
# ---------------------------------------------------------------------------

class _BrowseHarness:
    def __init__(self):
        self.btn_b_pgp = QPushButton()
        self.btn_b_pgp.setVisible(False)
        self._browse_pgp_url = None
        self.opened = []
        for name in ("_update_browse_pgp_button", "browse_open_pgp"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))


@pytest.fixture
def browse(monkeypatch):
    h = _BrowseHarness()
    import genizah_app

    monkeypatch.setattr(genizah_app.QDesktopServices, "openUrl",
                        lambda url: h.opened.append(url.toString()))
    return h


def test_browse_button_appears_only_with_a_url(browse):
    browse._update_browse_pgp_button({"pgp_url": PGP_URL})
    assert browse.btn_b_pgp.isVisible() is True
    assert browse._browse_pgp_url == PGP_URL


@pytest.mark.parametrize("doc", [None, {}, {"pgp_url": None}, {"pgp_url": ""},
                                 {"pgpid": 42}])
def test_browse_button_hidden_without_a_url(browse, doc):
    browse._update_browse_pgp_button({"pgp_url": PGP_URL})
    browse._update_browse_pgp_button(doc)
    assert browse.btn_b_pgp.isVisible() is False
    assert browse._browse_pgp_url is None


def test_browse_click_opens_the_url_and_is_inert_when_hidden(browse):
    browse._update_browse_pgp_button({"pgp_url": PGP_URL})
    browse.browse_open_pgp()
    browse._update_browse_pgp_button(None)
    browse.browse_open_pgp()
    assert browse.opened == [PGP_URL]


# ---------------------------------------------------------------------------
# 5. The ResultDialog buttons (normal + compact)
# ---------------------------------------------------------------------------

class _RDHarness:
    def __init__(self):
        from desktop.result_dialog import ResultDialog

        self.btn_rd_pgp = QPushButton()
        self.btn_rd_pgp.setVisible(False)
        self.btn_compact_pgp = QPushButton()
        self.btn_compact_pgp.setVisible(False)
        self._rd_pgp_url = None
        self.opened = []
        for name in ("_update_rd_pgp_button", "open_pgp_link"):
            setattr(self, name, MethodType(getattr(ResultDialog, name), self))


@pytest.fixture
def rd(monkeypatch):
    h = _RDHarness()
    import desktop.result_dialog as rd_mod

    monkeypatch.setattr(rd_mod.QDesktopServices, "openUrl",
                        lambda url: h.opened.append(url.toString()))
    return h


def test_rd_both_buttons_follow_the_url(rd):
    rd._update_rd_pgp_button({"pgp_url": PGP_URL})
    assert rd.btn_rd_pgp.isVisible() is True
    assert rd.btn_compact_pgp.isVisible() is True, (
        "compact mode must not lose the link"
    )
    rd._update_rd_pgp_button({"pgp_url": ""})
    assert rd.btn_rd_pgp.isVisible() is False
    assert rd.btn_compact_pgp.isVisible() is False


def test_rd_click_opens_the_url_and_is_inert_when_hidden(rd):
    rd._update_rd_pgp_button({"pgp_url": PGP_URL})
    rd.open_pgp_link()
    rd._update_rd_pgp_button(None)
    rd.open_pgp_link()
    assert rd.opened == [PGP_URL]


def test_rd_update_survives_a_missing_compact_button(rd):
    """The compact bar is built after info_row; a partly-built dialog must not raise."""
    del rd.btn_compact_pgp
    rd._update_rd_pgp_button({"pgp_url": PGP_URL})
    assert rd.btn_rd_pgp.isVisible() is True


# ---------------------------------------------------------------------------
# 6. Source-text guards -- the wiring the harnesses above cannot exercise
# ---------------------------------------------------------------------------

def _read(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def test_both_badge_write_sites_go_through_the_one_builder():
    """The two badge sites used to be copy-pasted; either can be the last writer."""
    src = _read("genizah_app.py")
    assert src.count("self._write_pgp_badge_cell(") == 2, (
        "expected exactly two callers of the shared writer: the initial row "
        "render and the badge-worker slot"
    )
    assert src.count('QTableWidgetItem("PGP")') == 1, (
        "the literal PGP cell must be built in ONE place -- a second hand-rolled "
        "badge would silently render without the link"
    )


def test_results_table_wires_the_click_and_double_click_handlers():
    src = _read("genizah_app.py")
    assert "self.results_table.cellClicked.connect(self._on_results_cell_clicked)" in src
    assert ("self.results_table.doubleClicked.connect(self._on_results_double_clicked)"
            in src), "the double-click must route through the PGP-aware wrapper"
    assert "self.results_table.doubleClicked.connect(self.show_full_text)" not in src


def test_browse_pgp_button_is_added_to_the_toolbar_row():
    src = _read("genizah_app.py")
    assert "ext_info_row.addWidget(self.btn_b_pgp)" in src


def test_result_dialog_adds_both_pgp_buttons_to_their_rows():
    src = _read("desktop/result_dialog.py")
    assert "info_row.addWidget(self.btn_rd_pgp)" in src
    assert "compact_layout.addWidget(self.btn_compact_pgp)" in src


def test_the_new_tooltip_string_is_translated():
    import genizah_translations as t

    he = t.TRANSLATIONS["Open on the Princeton Geniza Project website"]
    assert he and he != "Open on the Princeton Geniza Project website"
    assert any("֐" <= ch <= "׿" for ch in he)
