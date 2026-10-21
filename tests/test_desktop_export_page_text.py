# -*- coding: utf-8 -*-
"""The desktop exports write the text of the page a row shows (OPEN_ISSUES P2
"Desktop Excel export: 'Full Text' holds the whole manuscript, and line breaks
are deleted", 2026-10-21).

A row found through a whole-manuscript or part doc
(search_engine._aggregate_result_row) carries that doc's whole text in
'full_text', while its 'uid' is the page it is shown as. The Excel Full Text
column wrote the whole manuscript, cut at 32,000 characters (on the owner's
`*כהנים *גדולים` export, 132 of 405 cells did not hold the match); the DOCX
and TXT exports centred their context on the term's first occurrence in the
whole manuscript, which can be on another page. The website reads the page
by uid (web/export_service.py::_resolve_result_full_text).

Driven through GenizahGUI.export_results on the stub window of
tests/test_exclusion_surfaces.py, so the call site's fetcher is what is
tested, not only the builder.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import genizah_app
import genizah_core
from tests.test_exclusion_surfaces import (A, _personal_state_in_tmp,  # noqa: F401
                                           _res, _search, slot_errors, window)
from tests.test_export_locked_file_sites import _search_export

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

openpyxl = pytest.importorskip("openpyxl")

_PAGE_UID = "IE1_P000002_FL2"
_PAGE_ONE = "עמוד ראשון כהנים אחרים"         # the term occurs here first
_PAGE_TWO = "עמוד שני כהנים גדולים"


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


def _export_whole_manuscript_row(w, monkeypatch, tmp_path, fmt):
    w, out, _shown = _search_export(w, monkeypatch, tmp_path, fmt)
    row = _res(A, 2)
    row.update(scope="system", uid=_PAGE_UID,
               full_text=_PAGE_ONE + "\n" + _PAGE_TWO,
               snippet="שני *כהנים* גדולים", raw_file_hl="שני *כהנים* גדולים")
    _search(w, [row])
    w.searcher = SimpleNamespace(get_full_text_by_id={_PAGE_UID: _PAGE_TWO}.get)
    w.export_results(fmt)
    return out


def test_excel_full_text_is_the_page(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    out = _export_whole_manuscript_row(window, monkeypatch, tmp_path, "xlsx")
    ws = openpyxl.load_workbook(out)["Search Results"]
    assert ws.cell(1, 8).value == "Full Text"
    assert ws.cell(2, 8).value == _PAGE_TWO


def test_text_export_context_is_the_page(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    out = _export_whole_manuscript_row(window, monkeypatch, tmp_path, "txt")
    text = out.read_text(encoding="utf-8-sig")
    assert "שני *כהנים* גדולים" in text
    assert "ראשון" not in text


def test_word_export_context_is_the_page(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    docx = pytest.importorskip("docx")
    out = _export_whole_manuscript_row(window, monkeypatch, tmp_path, "docx")
    text = "\n".join(p.text for p in docx.Document(str(out)).paragraphs)
    assert "שני כהנים גדולים" in text
    assert "ראשון" not in text
