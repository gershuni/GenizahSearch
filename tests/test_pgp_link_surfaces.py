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
    conn.execute(
        "CREATE TABLE document_fragments "
        "(sys_id TEXT, document_id INTEGER, page_info TEXT)"
    )
    conn.executemany("INSERT INTO documents (pgpid, pgp_url) VALUES (?, ?)", documents)
    conn.executemany(
        "INSERT INTO document_fragments (sys_id, document_id, page_info) "
        "VALUES (?, ?, ?)",
        [(f + (None,))[:3] for f in fragments],
    )
    conn.commit()
    conn.close()


@pytest.fixture
def pgp_urls_fn(tmp_path):
    """The real PgpService.get_pgp_page_urls_for_sys_ids bound to a temp sidecar."""
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
    yield svc.get_pgp_page_urls_for_sys_ids
    conn.close()


def test_candidates_keep_their_null_urls_for_the_page_selection(pgp_urls_fn):
    """Codex P2, PR #334 -- filtering here hid a candidate from page selection.

    A recto document with no url and a verso document with one: drop the recto
    candidate and the recto ROW falls back to the verso document. The honest
    answer is no link, which only the selector can give -- so every linked
    document comes back and link availability is decided afterwards.
    """
    got = pgp_urls_fn(["plain", "empty", "null", "absent"])
    assert got == {
        "plain": [{"page_info": None, "pgp_url": PGP_URL}],
        "empty": [{"page_info": None, "pgp_url": ""}],
        "null": [{"page_info": None, "pgp_url": None}],
    }
    assert "absent" not in got, "a sys_id with no linked document at all is absent"


def test_a_page_whose_document_has_no_url_offers_no_link(results):
    """Not a link to the OTHER page's document."""
    results.add_row(0, "s1", {"display": {"id": "s1", "img": 1}})
    results.add_row(1, "s1", {"display": {"id": "s1", "img": 2}})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [
        {"page_info": "recto", "pgp_url": None},
        {"page_info": "verso", "pgp_url": VERSO_URL},
    ]}
    assert results._pgp_url_for_row(0, "s1") is None, (
        "the recto document has no url; linking to the verso one would open a "
        "different page than the row shows"
    )
    assert results._pgp_url_for_row(1, "s1") == VERSO_URL
    results._write_pgp_badge_cell(0, "s1")
    assert results.results_table.item(0, results.COL_PGP).font().underline() is False


def test_urls_picks_the_lowest_pgpid_when_a_sys_id_has_several_documents(pgp_urls_fn):
    got = pgp_urls_fn(["shared"])
    assert [e["pgp_url"] for e in got["shared"]] == [OTHER_URL, PGP_URL], (
        "candidates must come back in pgpid order, not SQLite row order"
    )
    # Deterministic across repeated calls, not just once.
    assert pgp_urls_fn(["shared"]) == got


def test_urls_empty_input_short_circuits(pgp_urls_fn):
    assert pgp_urls_fn([]) == {}


def test_urls_chunks_past_the_sqlite_variable_limit(pgp_urls_fn):
    # 1200 ids > the 999 SQLite variable limit; a single un-chunked IN() would
    # raise sqlite3.OperationalError and the method would return {}.
    ids = [f"filler{i}" for i in range(1200)] + ["plain"]
    assert pgp_urls_fn(ids) == {"plain": [{"page_info": None, "pgp_url": PGP_URL}]}


def test_urls_no_connection_returns_empty():
    from shared.document_service import PgpService

    svc = PgpService.__new__(PgpService)
    svc._conn = None
    assert svc.get_pgp_page_urls_for_sys_ids(["plain"]) == {}


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

    monkeypatch.setattr(ds, "get_pgp_page_urls_for_sys_ids", _urls)

    captured = {}
    w = PGPBadgeWorker(["a", "b"])
    w.finished.connect(lambda p, m, u: captured.update(pgp=p, manual=m, urls=u))
    w.run()  # synchronous: exercise run() without starting a thread
    return captured


def test_worker_emits_the_url_map(monkeypatch):
    pages = {"a": [{"page_info": None, "pgp_url": PGP_URL}]}
    got = _run_badge_worker(monkeypatch, {"a"}, {"a"}, pages)
    assert got["pgp"] == {"a"}
    assert got["urls"] == pages


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
    monkeypatch.setattr(ds, "get_pgp_page_urls_for_sys_ids",
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
    monkeypatch.setattr(ds, "get_pgp_page_urls_for_sys_ids",
                        lambda ids: {"a": [{"page_info": None, "pgp_url": PGP_URL}]})
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
        self._pgp_pages_by_sys_id = {}
        self.meta_mgr = None  # rows here carry display['img'], not a raw header
        self.opened = []
        self.full_text_calls = 0

        for name in (
            "_make_pgp_badge_item", "_write_pgp_badge_cell", "_pgp_url_for_cell",
            "_pgp_url_for_row", "_result_page_num",
            "_on_results_cell_clicked", "_on_results_double_clicked",
            "_on_pgp_badges_loaded",
        ):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    # collaborators the real methods reach for
    def show_full_text(self):
        self.full_text_calls += 1

    def _apply_results_table_filters(self):
        return None

    def add_row(self, row, sys_id, res=None):
        self.results_table.setRowCount(max(self.results_table.rowCount(), row + 1))
        item = QTableWidgetItem(sys_id)
        if res is not None:
            item.setData(Qt.ItemDataRole.UserRole, res)
        self.results_table.setItem(row, self.COL_SYS_ID, item)


@pytest.fixture
def results(monkeypatch):
    h = _ResultsHarness()
    import genizah_app

    monkeypatch.setattr(genizah_app.QDesktopServices, "openUrl",
                        lambda url: h.opened.append(url.toString()))
    return h


def test_badge_with_a_url_is_dressed_as_a_link(results):
    # Through the writer, which is what resolves a row's url -- see
    # _pgp_url_for_row (a row may name its own PGP document).
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    results._write_pgp_badge_cell(0, "s1")
    item = results.results_table.item(0, results.COL_PGP)
    assert item.text() == "PGP"
    assert item.foreground().color() == QColor("#27ae60")
    assert item.font().underline() is True
    assert item.data(Qt.ItemDataRole.UserRole) == PGP_URL
    assert PGP_URL in item.toolTip()
    assert not (item.flags() & Qt.ItemFlag.ItemIsEditable)


def test_badge_without_a_url_stays_a_plain_badge(results):
    """"has PGP info" is NOT "has a PGP url" -- no underline we cannot honour."""
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {}
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
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
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
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_SYS_ID)
    assert results.opened == []


def test_double_click_on_a_linked_badge_does_not_also_open_the_dialog(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    results._write_pgp_badge_cell(0, "s1")
    index = results.results_table.model().index(0, results.COL_PGP)
    results._on_results_double_clicked(index)
    assert results.full_text_calls == 0, (
        "the first click already opened the PGP page; one gesture, one window"
    )


def test_double_click_elsewhere_still_opens_the_dialog(results):
    results.add_row(0, "s1")
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
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
    pages = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    results._on_pgp_badges_loaded({"s1", "s2"}, set(), pages)
    assert results._pgp_pages_by_sys_id == pages
    assert results.results_table.item(0, results.COL_PGP).font().underline() is True
    assert results.results_table.item(1, results.COL_PGP).font().underline() is False
    assert results.results_table.item(1, results.COL_PGP).text() == "PGP"


def test_badges_loaded_slot_tolerates_the_old_two_argument_call(results):
    results.add_row(0, "s1")
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    results._on_pgp_badges_loaded({"s1"}, set())
    assert results._pgp_pages_by_sys_id == {}, "a stale url must not survive"
    assert results.results_table.item(0, results.COL_PGP).font().underline() is False


# ---------------------------------------------------------------------------
# 4. The Browse tab button
# ---------------------------------------------------------------------------

class _BrowseHarness:
    def __init__(self):
        self.btn_b_pgp = QPushButton()
        self.btn_b_pgp.setVisible(False)
        self._browse_pgp_url = None
        self.current_browse_sid = "sysA"
        self.opened = []
        for name in ("_update_browse_pgp_button", "browse_open_pgp",
                     "_on_browse_pgp_error"):
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
        self.current_sys_id = "sysA"
        self.opened = []
        for name in ("_update_rd_pgp_button", "open_pgp_link",
                     "_on_rd_pgp_error"):
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


def test_browse_error_from_an_abandoned_worker_leaves_the_button_alone(browse):
    """A late error for a manuscript the user has left must not hide the current one."""
    browse.current_browse_sid = "sysB"
    browse._update_browse_pgp_button({"pgp_url": PGP_URL})
    browse._on_browse_pgp_error("sysA", "boom")
    assert browse.btn_b_pgp.isVisible() is True
    assert browse._browse_pgp_url == PGP_URL


def test_browse_error_for_the_current_manuscript_hides_the_button(browse):
    browse.current_browse_sid = "sysB"
    browse._update_browse_pgp_button({"pgp_url": PGP_URL})
    browse._on_browse_pgp_error("sysB", "boom")
    assert browse.btn_b_pgp.isVisible() is False


def test_rd_error_from_an_abandoned_worker_leaves_the_button_alone(rd):
    rd.current_sys_id = "sysB"
    rd._update_rd_pgp_button({"pgp_url": PGP_URL})
    rd._on_rd_pgp_error("sysA", "boom")
    assert rd.btn_rd_pgp.isVisible() is True


def test_rd_error_for_the_current_result_hides_the_button(rd):
    rd.current_sys_id = "sysB"
    rd._update_rd_pgp_button({"pgp_url": PGP_URL})
    rd._on_rd_pgp_error("sysB", "boom")
    assert rd.btn_rd_pgp.isVisible() is False


# ---------------------------------------------------------------------------
# 7. Every navigation path that changes what is on screen must drop the link
# ---------------------------------------------------------------------------
#
# These are AST gates, not substring searches: a call has to be REACHABLE
# inside the named function body, so a mention in a comment or docstring
# cannot green them. They cover paths whose behaviour needs a fully built
# GenizahGUI (a real index, a real manuscript) to exercise directly.

def _self_calls_in(path, func_name):
    """Names of self.X(...) calls anywhere inside the named function."""
    import ast

    with open(path, "r", encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))                 and node.name == func_name:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call)                         and isinstance(sub.func, ast.Attribute)                         and isinstance(sub.func.value, ast.Name)                         and sub.func.value.id == "self":
                    found.add(sub.func.attr)
    assert found, f"{func_name} not found in {path}"
    return found


@pytest.mark.parametrize("func", [
    "_start_browse_enrichment",      # cross-manuscript load
    "_browse_refresh_pgp_for_page",  # page turn / folio combo
    "_open_local_browse_page",       # LOCAL "My Library" file
])
def test_browse_navigation_paths_drop_the_stale_link(func):
    assert "_update_browse_pgp_button" in _self_calls_in("genizah_app.py", func), (
        f"{func} changes what is displayed; leaving the PGP button up means it "
        f"keeps opening the previous manuscript"
    )


def test_in_part_folio_navigation_refreshes_pgp():
    """Moving between Oxford Part folios changes the sys_id, not just the page."""
    assert "_browse_refresh_pgp_for_page" in _self_calls_in(
        "genizah_app.py", "navigate_manuscript"
    )


def test_result_dialog_local_page_drops_the_stale_link():
    """load_page returns into load_local_page BEFORE its own reset block runs."""
    assert "_update_rd_pgp_button" in _self_calls_in(
        "desktop/result_dialog.py", "load_local_page"
    )


# ---------------------------------------------------------------------------
# 8. A row that names its OWN PGP document links to that document
# ---------------------------------------------------------------------------
#
# Codex P2 on PR #334. A PGP-tag hit stands for one specific tagged document --
# the snippet IS that document's transcription -- and 1,845 of 34,171 linked
# manuscripts have more than one PGP document (one has 104). Resolving those
# rows through the per-manuscript map sends them all to the lowest pgpid,
# which may not even carry the tag that was searched.

TAG_DOC_URL = "https://geniza.princeton.edu/documents/38608/"


def test_pgpid_urls_returns_only_real_urls(tmp_path):
    from shared.document_service import PgpService

    db = tmp_path / "pgp.db"
    _build_pgp_db(
        str(db),
        documents=[(1, PGP_URL), (2, ""), (3, None), (4, TAG_DOC_URL)],
        fragments=[],
    )
    conn = sqlite3.connect(str(db), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    svc = PgpService.__new__(PgpService)
    svc._conn = conn
    try:
        assert svc.get_pgp_urls_for_pgpids([1, 2, 3, 4, 99]) == {
            1: PGP_URL, 4: TAG_DOC_URL,
        }
        assert svc.get_pgp_urls_for_pgpids([]) == {}
        # past the 999-variable limit -- an un-chunked IN() would raise
        assert svc.get_pgp_urls_for_pgpids(list(range(100, 1400)) + [1]) == {1: PGP_URL}
    finally:
        conn.close()


def test_pgpid_urls_no_connection_returns_empty():
    from shared.document_service import PgpService

    svc = PgpService.__new__(PgpService)
    svc._conn = None
    assert svc.get_pgp_urls_for_pgpids([1]) == {}


def test_a_rows_own_url_beats_the_per_manuscript_map(results):
    """The tag row names its document; the map only knows the manuscript."""
    results.add_row(0, "s1", {"pgpid": 38608, "pgp_url": TAG_DOC_URL})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}  # the lowest-pgpid answer
    assert results._pgp_url_for_row(0, "s1") == TAG_DOC_URL
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_PGP)
    assert results.opened == [TAG_DOC_URL]


def test_two_rows_of_one_manuscript_link_to_their_own_documents(results):
    """The exact case the per-manuscript map cannot express."""
    other = "https://geniza.princeton.edu/documents/38607/"
    results.add_row(0, "s1", {"pgpid": 38608, "pgp_url": TAG_DOC_URL})
    results.add_row(1, "s1", {"pgpid": 38607, "pgp_url": other})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {}
    results._write_pgp_badge_cell(0, "s1")
    results._write_pgp_badge_cell(1, "s1")
    results._on_results_cell_clicked(0, results.COL_PGP)
    results._on_results_cell_clicked(1, results.COL_PGP)
    assert results.opened == [TAG_DOC_URL, other]


def test_an_ordinary_search_row_still_uses_the_map(results):
    """A search hit names a manuscript page, not a PGP document."""
    results.add_row(0, "s1", {"display": {"id": "s1"}, "snippet": "x"})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    assert results._pgp_url_for_row(0, "s1") == PGP_URL


def test_url_for_row_survives_a_row_with_no_sys_id_item(results):
    results.results_table.setRowCount(1)
    results._pgp_pages_by_sys_id = {"s1": [{"page_info": None, "pgp_url": PGP_URL}]}
    assert results._pgp_url_for_row(0, "s1") == PGP_URL
    assert results._pgp_url_for_row(0, "absent") is None


def test_the_tag_path_resolves_urls_by_pgpid_not_by_sys_id():
    """One source of truth for a tag row's link."""
    calls = _self_calls_in("genizah_app.py", "_on_tag_search_results")
    src = _read("genizah_app.py")
    tag_body = src[src.index("def _on_tag_search_results"):
                   src.index("def _on_tag_search_results") + 8000]
    assert "get_pgp_urls_for_pgpids" in tag_body
    assert "get_pgp_page_urls_for_sys_ids" not in tag_body, (
        "the per-manuscript map cannot tell two rows of one manuscript apart"
    )
    assert "'pgpid': r.get('pgpid')" in tag_body, (
        "each formatted row must record the document it came from"
    )
    assert calls, "sanity: the function was found"


# ---------------------------------------------------------------------------
# 9. An ordinary row resolves by its PAGE
# ---------------------------------------------------------------------------
#
# Codex P2, round 3. 144 manuscripts in the live sidecar have both a
# recto-tagged and a verso-tagged PGP document, and the lowest pgpid is not
# reliably the recto -- so "one url per manuscript" sent verso hits to the
# recto document, disagreeing with the reading dialog opened from that very row.

RECTO_URL = "https://geniza.princeton.edu/documents/5378/"
VERSO_URL = "https://geniza.princeton.edu/documents/5393/"
# ordered by pgpid: the recto document has the LOWER id, as in the real data
RV_PAGES = [
    {"page_info": "recto", "pgp_url": RECTO_URL},
    {"page_info": "verso", "pgp_url": VERSO_URL},
]


@pytest.mark.parametrize("page_num,expected", [
    (1, RECTO_URL),
    (2, VERSO_URL),
    (3, VERSO_URL),      # anything past recto is treated as verso
    (None, RECTO_URL),   # no page -> first candidate
])
def test_the_shared_rule_picks_by_page(page_num, expected):
    from shared.document_service import select_pgp_page_entry

    assert select_pgp_page_entry(RV_PAGES, page_num)["pgp_url"] == expected


def test_the_shared_rule_is_a_no_op_with_one_candidate():
    from shared.document_service import select_pgp_page_entry

    one = [{"page_info": "recto", "pgp_url": RECTO_URL}]
    assert select_pgp_page_entry(one, 2)["pgp_url"] == RECTO_URL
    assert select_pgp_page_entry([], 1) is None
    assert select_pgp_page_entry(None, 1) is None


def test_the_shared_rule_leaves_recto_and_verso_unmatched():
    """768 rows read 'recto and verso'; matching them would CHANGE web behaviour."""
    from shared.document_service import select_pgp_page_entry

    both = [
        {"page_info": "recto and verso", "pgp_url": RECTO_URL},
        {"page_info": "verso", "pgp_url": VERSO_URL},
    ]
    assert select_pgp_page_entry(both, 2)["pgp_url"] == VERSO_URL
    # page 1 finds no exact 'recto' -> first candidate, exactly as before
    assert select_pgp_page_entry(both, 1)["pgp_url"] == RECTO_URL


def test_get_document_for_fragment_uses_the_same_rule():
    """One rule, or the badge and the reading dialog drift apart again."""
    src = _read("shared/document_service.py")
    body = src[src.index("def get_document_for_fragment(self"):]
    body = body[:body.index("    def get_fragments_for_document")]
    assert "select_pgp_page_entry(frags, page_num)" in body
    assert "target_page = " not in body, "the rule must not be re-implemented here"


def test_get_document_for_fragment_resolves_in_a_defined_order(tmp_path):
    """The fallback must not depend on SQLite row order.

    Behavioural, not a source-text check: an earlier version of this test
    asserted "ORDER BY document_id" appeared in the method's source, and the
    phrase also appears in the comment explaining it -- so deleting the clause
    from the SQL left the test green.
    """
    from shared.document_service import PgpService

    db = tmp_path / "pgp.db"
    # Inserted HIGHEST pgpid first, so rowid order is the opposite of pgpid order.
    _build_pgp_db(
        str(db),
        documents=[(900, VERSO_URL), (100, RECTO_URL)],
        fragments=[("s1", 900, None), ("s1", 100, None)],
    )
    conn = sqlite3.connect(str(db), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    svc = PgpService.__new__(PgpService)
    svc._conn = conn
    try:
        doc = svc.get_document_for_fragment("s1")
        assert doc["pgpid"] == 100, (
            "with no page to go on, the lowest pgpid must win -- otherwise this "
            "disagrees with the badge's pgpid-ordered batch query for the same row"
        )
    finally:
        conn.close()


@pytest.mark.parametrize("page_img,expected", [(1, RECTO_URL), (2, VERSO_URL)])
def test_a_verso_row_opens_the_verso_document(results, page_img, expected):
    results.add_row(0, "s1", {"display": {"id": "s1", "img": page_img}})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": RV_PAGES}
    results._write_pgp_badge_cell(0, "s1")
    results._on_results_cell_clicked(0, results.COL_PGP)
    assert results.opened == [expected]
    assert expected in results.results_table.item(0, results.COL_PGP).toolTip(), (
        "the tooltip must name the url the click will actually open"
    )


def test_a_row_with_no_page_falls_back_to_the_first_candidate(results):
    results.add_row(0, "s1", {"display": {"id": "s1", "img": ""}})
    results._pgp_transcription_sys_ids = {"s1"}
    results._pgp_pages_by_sys_id = {"s1": RV_PAGES}
    assert results._pgp_url_for_row(0, "s1") == RECTO_URL


@pytest.mark.parametrize("res,expected", [
    ({"display": {"img": 2}}, 2),
    ({"display": {"img": "2"}}, 2),
    ({"display": {"img": ""}}, None),
    ({"display": {}}, None),
    ({}, None),
    (None, None),
    ("not a dict", None),
])
def test_result_page_num_reads_the_row_safely(results, res, expected):
    assert results._result_page_num(res) == expected


def test_result_page_num_prefers_the_parsed_header(results):
    class _Meta:
        def parse_full_id_components(self, header):
            return {"p_num": 2}

    results.meta_mgr = _Meta()
    assert results._result_page_num(
        {"raw_header": "hdr", "display": {"img": 1}}
    ) == 2, "the header is the canonical page for a Genizah hit"


def test_result_page_num_survives_a_meta_manager_that_raises(results):
    class _Meta:
        def parse_full_id_components(self, header):
            raise ValueError("bad header")

    results.meta_mgr = _Meta()
    assert results._result_page_num({"raw_header": "x", "display": {"img": 2}}) == 2
