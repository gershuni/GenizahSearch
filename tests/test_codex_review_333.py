"""Regressions for the three Codex findings on PR #333 (2026-09-02).

P1 `shared/fgp_service.py` — the text-match demotion validated *some* whole-doc
edition instead of the one that would be displayed, so a long unrelated row could
still win while a short related one satisfied the gate.

P2 `desktop/result_dialog.py` — the search phrase for the `must_contain` policy
was scraped from text that already carried the reader's own `*...*` annotation
markers, so a manual note could be taken for the search hit.

P2 `desktop/viewers.py` — after the Oxford->NLI auto-fallback the credit label
still read the Bodleian, under an NLI image.
"""
from __future__ import annotations

import ast
import re

from shared.fgp_service import choose_default_source


def _read(path):
    return open(path, encoding="utf-8").read()


def _ed(content, source_id, c_number=None, image_side=None):
    """A whole-document FGP edition unless a per-image key is given."""
    return {
        "id": source_id,
        "content": content,
        "doc_relation": "Digital Edition",
        "source": "fgp",          # source_provider() keys on this
        "is_fgp": True,
        "text_source": "FGP",
        "c_number": c_number,
        "image_side": image_side,
        "language": "Hebrew",
    }


# The displayed folio, and two whole-doc editions: a LONG unrelated one (wins on
# coverage) and a SHORT one that is actually about this folio.
FOLIO_HTR = (
    "תקום רבה דיניך וכהדרי אלצדקת עיניך ולבך מביט ועיניך וג במקום אחד "
    "אהובים היום נדמו כמלאכים בקומה זקופה כמעמד מל גנונים אגודים"
)
UNRELATED_LONG = " ".join([
    "ברכת מזון אברך לאל אמונה בחלק שבעה ושמונה גנונה מר אש אמנה דיצתה",
    "אנקת שיח שועי בזאת חנוכה קשבת ישעי גדעתה קרן מרשיעי דלתות היכלך",
] * 6)
RELATED_SHORT = "תקום רבה דיניך וכהדרי אלצדקת עיניך ולבך מביט ועיניך וג במקום אחד"


class TestCandidateItselfIsValidated:
    def test_long_unrelated_row_is_not_kept_because_another_row_matches(self):
        d = choose_default_source([_ed(UNRELATED_LONG, 1), _ed(RELATED_SHORT, 2)], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["source"] is not None
        assert d["source"]["id"] == 2, (
            "the displayed edition must be the one that overlaps the folio, "
            f"not the coverage winner (got id={d['source']['id']}, reason={d['reason']})"
        )
        assert d["eligible"] is True
        assert d["reason"] == "fgp_text_match"

    def test_all_rows_unrelated_still_demotes(self):
        d = choose_default_source([_ed(UNRELATED_LONG, 1), _ed(UNRELATED_LONG[:200], 2)], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["eligible"] is False
        assert d["reason"] == "demote_no_text_match"
        assert d["source"] is None

    def test_matching_candidate_is_kept_untouched(self):
        d = choose_default_source([_ed(RELATED_SHORT + " " + FOLIO_HTR, 7)], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["eligible"] is True
        assert d["source"]["id"] == 7
        assert d["reason"] != "demote_no_text_match"

    def test_foliated_row_still_exempt(self):
        # A confident per-image match is never subject to the text gate.
        d = choose_default_source([_ed(UNRELATED_LONG, 3, c_number="C123")], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["eligible"] is True
        assert d["source"]["id"] == 3


class TestSearchPhraseComesFromCleanText:
    """The phrase must be derived from `highlight_pattern` over the CLEAN page
    text, never by scanning `*...*` in text that manual annotations also mark."""

    def test_load_page_derives_the_phrase_before_manual_markers(self):
        src = _read("desktop/result_dialog.py")
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "load_page")
        body = ast.get_source_segment(src, fn)
        assert "clean_text = raw_text" in body
        assert "re.search(pattern_str, clean_text" in body
        clean_at = body.index("clean_text = raw_text")
        manual_at = body.index("_apply_manual_highlights_to_text")
        assert clean_at < manual_at, "the clean copy must be taken before manual markers"

    def test_no_star_scan_feeds_the_must_contain_phrase(self):
        src = _read("desktop/result_dialog.py")
        assert not re.search(r"_rd_search_match_text\s*=\s*_rd_match", src)
        assert r"re.search(r'\*([^*]+)\*', raw_text)" not in src


class TestDesktopCreditFollowsTheDisplayedImage:
    def test_viewer_keeps_both_credits_and_swaps_them(self):
        src = _read("desktop/viewers.py")
        assert "def _apply_attribution_for_source(self):" in src
        assert "self._attr_nli = meta.get('attribution_nli')" in src
        # called on load, on the auto-fallback, and on the manual switch
        assert src.count("self._apply_attribution_for_source()") >= 3

    def test_fallback_branch_updates_the_credit(self):
        src = _read("desktop/viewers.py")
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_on_image_load_failed")
        body = ast.get_source_segment(src, fn)
        assert "_apply_attribution_for_source()" in body

    def test_metadata_manager_preserves_the_nli_credit(self):
        src = _read("shared/metadata_manager.py")
        assert "current_meta['attribution_nli'] = nli_iiif_data.get('attribution', '')" in src
