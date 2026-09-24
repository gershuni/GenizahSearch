"""shared/web_links.py and the desktop's "Open on the website" / "Copy link" buttons."""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from shared.export_utils import GENIZAHSEARCH_URL
from shared.web_links import web_browse_url

SID = "990001435310205171"
LOCAL = "970000000100000001"          # 97 + 16 digits: a My Library document
SYNTH = "990000001234000000"          # 99 + inventory + 000000: metadata-only


def test_the_page_on_screen():
    assert web_browse_url(SID, 66) == f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}&page=66"
    assert web_browse_url(SID, 7, "IE156747973") == (
        f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}&page=7&volume_ie=IE156747973")


def test_no_page_number_gives_the_manuscript():
    for p in (None, 0, -1, "", "x"):
        assert web_browse_url(SID, p) == f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}"


def test_my_library_documents_are_not_on_the_website():
    assert web_browse_url(LOCAL, 3) is None
    assert web_browse_url("", 3) is None
    assert web_browse_url(None) is None


def test_a_metadata_only_record_links_to_its_record_only():
    from shared.synthetic_sys_id import is_synthetic_sys_id
    assert is_synthetic_sys_id(SYNTH)
    assert web_browse_url(SYNTH, 5, "IE1") == f"{GENIZAHSEARCH_URL}/browse?sys_id={SYNTH}"


def test_same_address_as_the_websites_own_folio_links():
    """The website's link builder for a fully known folio; the two must agree."""
    from web.components.discovery_links import browse_url
    assert web_browse_url(SID, 7, "IE156747973") == GENIZAHSEARCH_URL + browse_url(
        SID, page=7, volume_ie="IE156747973")


# --- the Manuscript Viewer's buttons ----------------------------------------------

@pytest.fixture
def viewer(monkeypatch):
    from PyQt6.QtWidgets import QApplication
    QApplication.instance() or QApplication([])
    import desktop.result_dialog as rd
    monkeypatch.setattr(rd.ResultDialog, "load_result_by_index", lambda self, i: None)
    dlg = rd.ResultDialog(MagicMock(), [{}], 0, MagicMock(), MagicMock())
    opened = []
    monkeypatch.setattr(rd.QDesktopServices, "openUrl", staticmethod(lambda u: opened.append(u.toString())))
    monkeypatch.setattr(rd.QMessageBox, "information", staticmethod(lambda *a, **k: None))
    dlg._opened = opened
    yield dlg
    dlg.deleteLater()


@pytest.mark.gui
def test_viewer_opens_and_copies_the_page_on_screen(viewer):
    from PyQt6.QtWidgets import QApplication
    viewer.current_sys_id, viewer.current_p_num, viewer.current_volume_ie = SID, 66, None
    viewer.btn_rd_open_web.click()
    assert viewer._opened == [f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}&page=66"]
    viewer.btn_rd_copy_web.click()
    assert QApplication.clipboard().text() == f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}&page=66"


@pytest.mark.gui
def test_viewer_opens_nothing_for_a_my_library_document(viewer):
    viewer.current_sys_id, viewer.current_p_num = LOCAL, 1
    viewer.btn_rd_open_web.click()
    assert viewer._opened == []


# --- the Browse tab's buttons -----------------------------------------------------

@pytest.mark.gui
def test_browse_buttons_follow_the_page_and_the_document(monkeypatch):
    from PyQt6.QtWidgets import QApplication, QPushButton
    QApplication.instance() or QApplication([])
    import genizah_app
    G = genizah_app.GenizahGUI
    host = SimpleNamespace(
        current_browse_sid=SID, current_browse_p=4, current_browse_volume_ie="IE9",
        btn_b_cite=QPushButton(), btn_b_open_web=QPushButton(), btn_b_copy_web=QPushButton(),
        statusBar=lambda: SimpleNamespace(showMessage=lambda *a, **k: None))
    for name in ("_browse_web_url", "_browse_open_on_web", "_browse_copy_web_link",
                 "_sync_browse_cite_button"):
        setattr(host, name, getattr(G, name).__get__(host))
    opened = []
    monkeypatch.setattr(genizah_app.QDesktopServices, "openUrl",
                        staticmethod(lambda u: opened.append(u.toString())))
    host._sync_browse_cite_button()
    assert host.btn_b_open_web.isEnabled() and host.btn_b_copy_web.isEnabled()
    host._browse_open_on_web()
    assert opened == [f"{GENIZAHSEARCH_URL}/browse?sys_id={SID}&page=4&volume_ie=IE9"]
    host.current_browse_p = 5                        # the reader turned the page
    host._browse_copy_web_link()
    assert QApplication.clipboard().text().endswith("&page=5&volume_ie=IE9")
    host.current_browse_sid = LOCAL
    host._sync_browse_cite_button()
    assert not host.btn_b_open_web.isEnabled() and not host.btn_b_copy_web.isEnabled()
    host.current_browse_sid = SYNTH                  # no citation, but a page on the site
    host._sync_browse_cite_button()
    assert host.btn_b_open_web.isEnabled() and not host.btn_b_cite.isEnabled()


@pytest.mark.gui
def test_viewer_link_keeps_the_volume_after_crossing_into_another_manuscript(viewer, monkeypatch):
    """Codex, #362: navigating into another manuscript resets current_volume_ie,
    but the page number still counts within one volume -- the link must name the
    page's own volume, read from its header, when the manuscript has several."""
    import genizah_core
    viewer.current_sys_id, viewer.current_p_num, viewer.current_volume_ie = SID, 7, None
    viewer.current_full_header = "hdr"
    viewer.meta_mgr.parse_full_id_components.return_value = {"sys_id": SID, "ie_id": "IE222"}
    monkeypatch.setattr(genizah_core, "get_volumes_for_sys_id", lambda sid: ["IE111", "IE222"])
    assert viewer._rd_web_url().endswith(f"sys_id={SID}&page=7&volume_ie=IE222")
    monkeypatch.setattr(genizah_core, "get_volumes_for_sys_id", lambda sid: ["IE222"])
    assert viewer._rd_web_url().endswith(f"sys_id={SID}&page=7")      # one volume: no IE
    viewer.current_volume_ie = "IE111"                                  # an explicit choice wins
    assert viewer._rd_web_url().endswith("&volume_ie=IE111")


def test_every_change_of_the_browse_document_resyncs_its_buttons():
    """Codex, #362: opening a My Library document by one of the local paths changed
    current_browse_sid without re-syncing, so the website buttons (and Cite) stayed
    enabled for a file that is not on the website. Every assignment after __init__
    is now followed by the sync."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(
        encoding="utf-8").splitlines()
    sites = [i for i, line in enumerate(src) if "self.current_browse_sid = " in line]
    assert len(sites) >= 9
    missing = [i + 1 for i in sites[1:]            # [0] is the __init__ default
               if "_sync_browse_cite_button()" not in src[i + 1]]
    assert not missing, f"genizah_app.py lines {missing} change the Browse document without re-syncing"


def test_main_window_floor_never_exceeds_the_screen():
    """Codex, #362: 1920x1080 at 300% is about 640x360 logical px."""
    from PyQt6.QtCore import QRect
    import genizah_app
    scr = SimpleNamespace(availableGeometry=lambda: QRect(0, 0, 640, 330))
    w, h = genizah_app.GenizahGUI._main_window_floor(scr)
    assert w <= 640 and h <= 330
    big = SimpleNamespace(availableGeometry=lambda: QRect(0, 0, 1920, 1040))
    assert genizah_app.GenizahGUI._main_window_floor(big) == (800, 520)
