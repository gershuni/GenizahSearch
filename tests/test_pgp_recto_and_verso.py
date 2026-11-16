# -*- coding: utf-8 -*-
"""A PGP edition linked to a fragment as 'recto and verso' shows on both pages.

The version chooser's page filter (``shared/fgp_service.py::filter_sources_for_page``)
kept a PGP source only when its ``page_info`` was the shown side ('recto' on page 1,
'verso' on any other) or empty, so a 'recto and verso' edition was dropped on every
page. It now shows on every page, its text narrowed to the page as for a source
with no side; a text that cannot be split is shown whole on both pages.

The desktop worker calls the same function: tests/test_pgp_recto_and_verso_desktop.py.
"""

from shared.document_service import has_side_markers
from shared.fgp_service import (
    PGP_BOTH_SIDES,
    filter_sources_for_page,
    group_transcription_sources,
)

HEADINGS = "Recto\nrecto body\nVerso\nverso body"
UNMARKED = "one text\nwith no side headings"
SECTIONS = [
    {"canvas_num": 1, "text": "first side line"},
    {"canvas_num": 2, "text": "second side line"},
]


def _pgp(content, page_info=PGP_BOTH_SIDES, sections=None, relation="Digital Edition"):
    # A fresh dict per call: the filter narrows ``content`` in place.
    return {"source": "pgp", "doc_relation": relation, "content": content,
            "page_info": page_info, "sections": sections}


def _shown(sources, page_num, **kw):
    """The PGP editions the chooser offers on a page, by text."""
    kept = filter_sources_for_page(sources, page_num, **kw)
    return [s["content"] for s in group_transcription_sources(kept)["pgp_editions"]]


class TestEditionShownOnBothPages:
    def test_headings_split_the_text_by_page(self):
        assert _shown([_pgp(HEADINGS)], 1) == ["recto body"]
        assert _shown([_pgp(HEADINGS)], 2) == ["verso body"]

    def test_sections_split_the_text_by_page(self):
        content = "first side line\nsecond side line"
        assert _shown([_pgp(content, sections=SECTIONS)], 1) == ["first side line"]
        assert _shown([_pgp(content, sections=SECTIONS)], 2) == ["second side line"]

    def test_text_that_cannot_be_split_is_whole_on_both_pages(self):
        assert _shown([_pgp(UNMARKED)], 1) == [UNMARKED]
        assert _shown([_pgp(UNMARKED)], 2) == [UNMARKED]

    def test_a_page_its_sections_leave_empty_gets_the_whole_text(self):
        # Canvas 2 is present but empty and there are no headings: nothing tells
        # which part is page 2's, so page 2 shows the whole text.
        content = "first side line\nsecond side line"
        sections = [{"canvas_num": 1, "text": "first side line"},
                    {"canvas_num": 2, "text": ""}]
        assert _shown([_pgp(content, sections=sections)], 1) == ["first side line"]
        assert _shown([_pgp(content, sections=sections)], 2) == [content]

    def test_a_text_headed_verso_only_stays_off_page_one(self):
        # Its own heading says which side it is: the 'recto and verso' link does
        # not put it on page 1.
        content = "Verso\nverso body"
        assert _shown([_pgp(content)], 1) == []
        assert _shown([_pgp(content)], 2) == ["verso body"]

    def test_headings_decide_a_page_its_sections_skip(self):
        # Sections only for canvas 1: page 2 is read from the headings, not shown
        # whole (Codex P2 on PR #24).
        one_canvas = [{"canvas_num": 1, "text": "recto body"}]
        assert _shown([_pgp(HEADINGS, sections=one_canvas)], 1) == ["recto body"]
        assert _shown([_pgp(HEADINGS, sections=one_canvas)], 2) == ["verso body"]

    def test_a_recto_only_text_stays_off_a_page_its_sections_skip(self):
        content = "Recto\nrecto body"
        one_canvas = [{"canvas_num": 1, "text": "recto body"}]
        assert _shown([_pgp(content, sections=one_canvas)], 1) == ["recto body"]
        assert _shown([_pgp(content, sections=one_canvas)], 2) == []

    def test_later_pages_keep_it(self):
        assert _shown([_pgp(UNMARKED)], 3) == [UNMARKED]


class TestTranslations:
    def test_website_shows_the_translation_whole_on_both_pages(self):
        tr = lambda: _pgp(HEADINGS, relation="Digital Translation")  # noqa: E731
        for page_num in (1, 2):
            kept = filter_sources_for_page([tr()], page_num)
            assert [s["content"] for s in kept] == [HEADINGS]

    def test_desktop_rule_narrows_it_like_an_edition(self):
        tr = lambda: _pgp(HEADINGS, relation="Digital Translation")  # noqa: E731
        assert [s["content"] for s in filter_sources_for_page(
            [tr()], 1, translations_every_page=True)] == ["recto body"]
        assert [s["content"] for s in filter_sources_for_page(
            [tr()], 2, translations_every_page=True)] == ["verso body"]

    def test_desktop_rule_keeps_a_one_sided_translation_on_every_page(self):
        tr = lambda: _pgp("full translation", page_info="recto",  # noqa: E731
                          relation="Digital Translation")
        assert filter_sources_for_page([tr()], 2) == []
        assert [s["content"] for s in filter_sources_for_page(
            [tr()], 2, translations_every_page=True)] == ["full translation"]


class TestOtherSidesUnchanged:
    def test_one_sided_edition_shows_on_its_side_only(self):
        assert _shown([_pgp("r", page_info="recto")], 1) == ["r"]
        assert _shown([_pgp("r", page_info="recto")], 2) == []
        assert _shown([_pgp("v", page_info="verso")], 1) == []
        assert _shown([_pgp("v", page_info="verso")], 2) == ["v"]

    def test_edition_without_a_side_keeps_its_old_narrowing(self):
        # A text with no side and no headings shows on page 1 only, as before:
        # the whole-text rule is for 'recto and verso' alone.
        assert _shown([_pgp(UNMARKED, page_info=None)], 1) == [UNMARKED]
        assert _shown([_pgp(UNMARKED, page_info=None)], 2) == []
        assert _shown([_pgp(HEADINGS, page_info=None)], 2) == ["verso body"]


def test_has_side_markers():
    assert has_side_markers(HEADINGS)
    assert has_side_markers("Verso (address)\ntext")
    assert not has_side_markers(UNMARKED)
    assert not has_side_markers("recto text in a line\nmore")
    assert not has_side_markers("")
    assert not has_side_markers(None)
