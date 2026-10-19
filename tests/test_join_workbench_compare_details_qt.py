# -*- coding: utf-8 -*-
"""Desktop Compare window: copy button, info line, links row, anchor title (2026-10-19).

Drives the real CompareDialog pane widgets and fill methods with a stub dialog,
so no workbench, image loader or sidecar is involved.
"""
from __future__ import annotations

import types

import pytest

pytestmark = pytest.mark.gui  # real QWidgets + clipboard

from PyQt6.QtWidgets import QApplication  # noqa: E402

import desktop.join_workbench as jw  # noqa: E402
from shared.joins_lab import Candidate  # noqa: E402

SID = "990000000010205171"
CAND_SID = "990000000020205171"


@pytest.fixture(scope="module")
def app():
    return QApplication.instance() or QApplication([])


def _stub(loaded: list):
    """A CompareDialog stand-in: real _render_pane_info, recorded loaders."""
    s = types.SimpleNamespace()
    s.wb = types.SimpleNamespace(_anchor_meas={}, filtered=[], idx=0)
    s._render_pane_info = lambda pane: jw.CompareDialog._render_pane_info(s, pane)
    s._load_pane_details = lambda pane: loaded.append(("details", pane["sys_id"]))
    s._load_pane_page_text = lambda pane, h: loaded.append(("text", pane["sys_id"]))
    s._load_pane_image = lambda pane, sid, p: loaded.append(("image", sid))
    return s


def _pane(app, s):
    return jw.CompareDialog._pane(s)


def test_anchor_pane_shows_title_and_library_from_display(app):
    """The anchor result keeps library and title under 'display'; the old meta line
    read them from the top level and came out empty."""
    loaded = []
    s = _stub(loaded)
    pane = _pane(app, s)
    res = {
        "display": {"id": SID, "shelfmark": "T-S 12.1", "title": "Piyyut for Shavuot",
                    "library_code": "CUL"},
        "uid": f"{SID}|p2",
        "page": 2,
    }
    jw.CompareDialog._fill_anchor(s, pane, res)

    assert pane["shelf"].text() == "T-S 12.1"
    assert pane["meta"].text() == "Piyyut for Shavuot"
    assert pane["info_lbl"].text().startswith("CUL")
    assert ("details", SID) in loaded


def test_candidate_info_line_and_links_after_details(app):
    loaded = []
    s = _stub(loaded)
    s.wb._candidate_pane = types.SimpleNamespace(
        _enrich={(CAND_SID, 3): {"width_cm": 18.2, "height_cm": 12.4, "material": "paper"}}
    )
    pane = _pane(app, s)
    cand = Candidate(sys_id=CAND_SID, page=3, shelfmark="T-S 13.5", title="Letter",
                     library_code="CUL")
    jw.CompareDialog._fill_candidate(s, pane, cand)

    # Before the details arrive: library + size + material, and the website link.
    assert "CUL" in pane["info_lbl"].text() and "18×12 cm" in pane["info_lbl"].text()
    assert "genizahsearch.com/browse?sys_id=" + CAND_SID + "&amp;page=3" in pane["links_lbl"].text()

    pane["details"] = {
        "domains": [{"domain": "Letters", "domain_heb": "מכתבים"}],
        "dating": "1100",
        "links": [{"key": "ktiv", "label": "Ktiv", "url": "https://ktiv.example/?a=1&b=2"}],
    }
    jw.CompareDialog._render_pane_info(s, pane)
    line = pane["info_lbl"].text()
    assert "1100" in line and ("Letters" in line or "מכתבים" in line)
    html = pane["links_lbl"].text()
    assert html.index("Ktiv") < html.index("genizahsearch.com")   # website link last
    assert 'href="https://ktiv.example/?a=1&amp;b=2"' in html          # escaped
    assert pane["links_lbl"].openExternalLinks()


def test_copy_button_copies_the_plain_shelfmark(app):
    loaded = []
    s = _stub(loaded)
    pane = _pane(app, s)
    cand = Candidate(sys_id=CAND_SID, page=1, shelfmark="T-S 13.5", via_vs=True)
    jw.CompareDialog._fill_candidate(s, pane, cand)
    assert pane["shelf"].text() != "T-S 13.5"    # the label carries the 👁 badge

    from PyQt6.QtWidgets import QPushButton
    # The pane is a bare layout here; the copy button sits in the shelfmark row.
    row = pane["box"].itemAt(0).layout()
    copy_btns = [row.itemAt(i).widget() for i in range(row.count())
                 if isinstance(row.itemAt(i).widget(), QPushButton)]
    assert len(copy_btns) == 1
    copy_btns[0].click()
    assert QApplication.clipboard().text() == "T-S 13.5"


def test_stale_details_are_dropped(app, monkeypatch):
    """A details result for a fragment the pane no longer shows is ignored."""
    emitted = {}

    class _Signal:
        def __init__(self):
            self.slots = []

        def connect(self, fn):
            self.slots.append(fn)

    class FakeWorker:
        def __init__(self, wb, sid, p):
            self.sid = sid
            self.done = _Signal()
            self.finished = _Signal()
            emitted["worker"] = self

        def start(self):
            pass

    monkeypatch.setattr(jw, "_PaneDetailsWorker", FakeWorker)
    loaded = []
    s = _stub(loaded)
    s._reap_pane_text_worker = lambda wid: None
    pane = _pane(app, s)
    pane["sys_id"] = CAND_SID
    pane["info_base"] = ("CUL", None, None, "")
    jw.CompareDialog._load_pane_details(s, pane)
    worker = emitted["worker"]

    pane["sys_id"] = "990000000030205171"            # the user moved to the next candidate
    for slot in worker.done.slots:
        slot(CAND_SID, {"dating": "STALE", "links": []})
    assert pane["details"] is None and "STALE" not in pane["info_lbl"].text()

    pane["sys_id"] = CAND_SID
    for slot in worker.done.slots:
        slot(CAND_SID, {"dating": "1100", "links": []})
    assert pane["details"] == {"dating": "1100", "links": []}
    assert "1100" in pane["info_lbl"].text()
