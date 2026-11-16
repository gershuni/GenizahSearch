# -*- coding: utf-8 -*-
"""The desktop PGP source worker shows a 'recto and verso' edition on both pages.

It used to keep its own copy of the page rule, which dropped such an edition on
every page; it now calls the website's ``filter_sources_for_page``. Run
synchronously with the data calls replaced, so no sidecar is needed.
"""

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

HEADINGS = "Recto\nrecto body\nVerso\nverso body"
UNMARKED = "one text\nwith no side headings"
SECTIONS = [
    {"canvas_num": 1, "text": "first side line"},
    {"canvas_num": 2, "text": "second side line"},
]


def _src(content, page_info="recto and verso", sections=None, relation="Digital Edition"):
    return {"source": "pgp", "doc_relation": relation, "content": content,
            "page_info": page_info, "sections": sections, "pgpid": 1}


def _run(monkeypatch, sources, page_num):
    """The (source contents) the worker hands the viewer for one page."""
    import shared.document_service as ds
    import shared.fgp_service as fs
    from desktop.gui_threads import PGPSourceWorker

    monkeypatch.setattr(ds, "get_all_sources_for_fragment", lambda sys_id: [dict(s) for s in sources])
    monkeypatch.setattr(ds, "get_document_for_fragment", lambda sys_id, page_num=None: {})
    monkeypatch.setattr(fs, "get_fgp_sources_for_fragment", lambda sys_id: [])

    emitted, errors = [], []
    worker = PGPSourceWorker("990000000000205171", page_num)
    worker.finished_signal.connect(lambda sys_id, srcs, doc: emitted.append(srcs))
    worker.error_signal.connect(lambda sys_id, msg: errors.append(msg))
    worker.run()  # synchronously, on this thread
    assert not errors, errors
    return [s.get("content") for s in emitted[-1]]


def test_headings_split_the_text_by_page(monkeypatch):
    assert _run(monkeypatch, [_src(HEADINGS)], 1) == ["recto body"]
    assert _run(monkeypatch, [_src(HEADINGS)], 2) == ["verso body"]


def test_sections_split_the_text_by_page(monkeypatch):
    src = _src("first side line\nsecond side line", sections=SECTIONS)
    assert _run(monkeypatch, [src], 1) == ["first side line"]
    assert _run(monkeypatch, [src], 2) == ["second side line"]


def test_text_that_cannot_be_split_is_whole_on_both_pages(monkeypatch):
    assert _run(monkeypatch, [_src(UNMARKED)], 1) == [UNMARKED]
    assert _run(monkeypatch, [_src(UNMARKED)], 2) == [UNMARKED]


def test_headings_decide_a_page_its_sections_skip(monkeypatch):
    src = _src(HEADINGS, sections=[{"canvas_num": 1, "text": "recto body"}])
    assert _run(monkeypatch, [src], 2) == ["verso body"]


def test_translations_keep_the_desktop_rule(monkeypatch):
    # The desktop shows a PGP translation on every page whatever its side.
    one_sided = _src("full translation", page_info="recto", relation="Digital Translation")
    assert _run(monkeypatch, [one_sided], 2) == ["full translation"]
    unsided = _src(HEADINGS, page_info=None, relation="Digital Translation")
    assert _run(monkeypatch, [unsided], 2) == ["verso body"]


def test_one_sided_editions_unchanged(monkeypatch):
    srcs = [_src("r", page_info="recto"), _src("v", page_info="verso")]
    assert _run(monkeypatch, srcs, 1) == ["r"]
    assert _run(monkeypatch, srcs, 2) == ["v"]
