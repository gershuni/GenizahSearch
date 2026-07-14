# -*- coding: utf-8 -*-
"""
Behavior tests for the FGP-vs-HTR default-coverage policy (SEED-030).

``shared.fgp_service.choose_default_source`` decides whether an FGP edition may be
the reading-view default for a folio, or must be demoted below the V0.8/HTR
("MiDRASH") transcription because it is a partial/selected excerpt (the Firkovich
case: median ~9% of the folio's text). Pure logic — no GUI, no sidecar DB.

The web (``version_selector``) + desktop (``_auto_select_pgp_edition``) wiring is
guarded structurally in ``tests/test_fgp_chooser_integration.py``.
"""

import os

import pytest

from shared.fgp_service import (
    _COVERAGE_MIN_HTR_LETTERS,
    _DEFAULT_MIN_COVERAGE,
    _heb_letter_count,
    choose_default_source,
    fgp_needs_full_htr,
)


def _edition(content, rel="Digital Edition", **kw):
    # Default: a WHOLE-DOCUMENT row (no per-image folio) — _fgp_match_folio → ''.
    d = {"source": "fgp", "doc_relation": rel, "content": content}
    d.update(kw)
    return d


def _foliated_edition(content, image_side="2r", **kw):
    # A per-image (foliated) row — _fgp_match_folio → the image_side.
    return _edition(content, image_side=image_side, **kw)


# ── Normalization (_heb_letter_count) ─────────────────────────────────────────


class TestHebLetterCount:
    def test_nikud_and_teamim_stripped(self):
        # שלום (4 base letters) with points/accents still counts 4.
        assert _heb_letter_count("שלום") == 4
        assert _heb_letter_count("שָׁלוֹם") == 4

    def test_lacuna_markers_stripped_but_bracketed_letters_kept(self):
        # The HTR's ][ uncertainty markers are not letters; letters inside
        # editorial brackets ARE text and must be counted.
        assert _heb_letter_count("אב][גד") == 4
        assert _heb_letter_count("[אבג] דה") == 5

    def test_whitespace_punctuation_latin_digits_ignored(self):
        assert _heb_letter_count("אב, גד. 123 abc\n\tה") == 5

    def test_empty_and_none(self):
        assert _heb_letter_count("") == 0
        assert _heb_letter_count(None) == 0


# ── choose_default_source ─────────────────────────────────────────────────────

# A comfortably-long HTR baseline (400 base letters).
_HTR = "אבגד " * 100


class TestChooseDefaultSource:
    def test_partial_fgp_is_demoted(self):
        # Firkovich-like: FGP ~9% of the HTR text → demote below V0.8.
        d = choose_default_source([_edition("אבגד " * 9)], _HTR)
        assert d["eligible"] is False
        assert d["reason"] == "demote_low_coverage"
        assert d["source"] is None
        assert d["ratio"] == pytest.approx(0.09, abs=0.01)

    def test_full_fgp_is_kept(self):
        # CUL-like: FGP as full as / fuller than the HTR → stays the default.
        ed = _edition("אבגד " * 120)  # 480 / 400 = 1.2
        d = choose_default_source([ed], _HTR)
        assert d["eligible"] is True
        assert d["reason"] == "fgp_sufficient"
        assert d["source"] is ed

    def test_threshold_boundary(self):
        # Exactly at the default threshold → kept (>= is eligible).
        n = round(_DEFAULT_MIN_COVERAGE * 100)  # letters per side comparable
        ed = _edition("אבגד " * n)  # ratio ≈ threshold
        d = choose_default_source([ed], _HTR)
        assert d["ratio"] == pytest.approx(_DEFAULT_MIN_COVERAGE, abs=0.02)
        assert d["eligible"] is (d["ratio"] >= _DEFAULT_MIN_COVERAGE)

    def test_htr_too_short_keeps_fgp(self):
        # HTR is a fullness baseline, not ground truth: a blank/tiny HTR folio
        # must NEVER demote FGP (fail toward FGP).
        assert _COVERAGE_MIN_HTR_LETTERS > 3
        ed = _edition("אבגד " * 5)
        d = choose_default_source([ed], "אבג")
        assert d["eligible"] is True
        assert d["reason"] == "htr_too_short"
        assert d["source"] is ed

    def test_translation_only_never_coverage_demoted(self):
        # A translation is a different language — no length ratio; not an edition.
        d = choose_default_source(
            [_edition("קצר", rel="Digital Translation")], _HTR
        )
        assert d["reason"] == "no_fgp_edition"
        assert d["eligible"] is False
        assert d["source"] is None

    def test_no_sources(self):
        d = choose_default_source([], _HTR)
        assert d["reason"] == "no_fgp_edition" and d["eligible"] is False
        d = choose_default_source(None, _HTR)
        assert d["reason"] == "no_fgp_edition" and d["eligible"] is False

    def test_measures_displayed_content_not_sections(self):
        # The chooser DISPLAYS the whole-row ``content`` (FGP text is never
        # narrowed to a sub-section on display, and ``sources`` is folio-aligned
        # upstream), so coverage is measured against ``content`` even when a
        # ``sections`` list is present.
        row = _edition(
            "אבגד " * 120,  # full content ≈ 1.2× HTR
            sections=[{"page_num": 1, "text": "אבגד "}],  # a tiny section is ignored
        )
        d = choose_default_source([row], _HTR)
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"

    def test_no_page_dependence(self):
        # HIGH regression guard: the decision must NOT depend on a global page
        # number (the old code measured a recto row on page ≥2 as empty and
        # wrongly demoted it). A full foliated row stays the default on any folio.
        row = _foliated_edition("אבגד " * 120, image_side="3r")
        assert choose_default_source([row], _HTR)["eligible"] is True


# ── Whole-document vs foliated baseline (SEED-030 follow-up) ───────────────────


class TestWholeDocBaseline:
    # The 990000925330205171 case: a whole-document *selective* transcription
    # (772 letters ≈ 2.6% of a 26-folio, ~29,780-letter MS) must be demoted even
    # though it is longer than any single folio's HTR.
    _FULL_MS = "אבגד " * 7445  # ~29,780 base letters

    def test_whole_doc_selective_demoted_against_full_ms(self):
        row = _edition("אבגד " * 193)  # ~772 letters
        getter = lambda: self._FULL_MS  # noqa: E731
        # Even if the displayed folio's HTR is large, the whole-doc row is judged
        # against the WHOLE MS → 2.6% → demote.
        d = choose_default_source([row], "אבגד " * 300, full_htr_getter=getter)
        assert d["eligible"] is False and d["reason"] == "demote_low_coverage"
        assert d["ratio"] == pytest.approx(0.026, abs=0.01)

    def test_whole_doc_selective_kept_on_blank_folio_without_full_ms(self):
        # Regression: on folio 1 (blank HTR) with NO full-MS getter, the old code
        # kept FGP via the htr-too-short floor. With the getter it correctly
        # demotes — the fix depends on the caller supplying the getter.
        row = _edition("אבגד " * 193)
        # No getter → whole-doc falls back to the (blank) folio baseline → unknown
        # → keep (documents the degraded fallback).
        assert choose_default_source([row], "", full_htr_getter=None)["eligible"] is True
        # With getter + blank folio → judged against whole MS → demote.
        d = choose_default_source([row], "", full_htr_getter=lambda: self._FULL_MS)
        assert d["eligible"] is False and d["reason"] == "demote_low_coverage"

    def test_whole_doc_comprehensive_kept(self):
        # A comprehensive whole-doc transcription (≈ the whole MS) stays default.
        row = _edition("אבגד " * 7000)  # ≈ full MS
        d = choose_default_source([row], "אבגד " * 300, full_htr_getter=lambda: self._FULL_MS)
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"

    def test_foliated_row_uses_folio_baseline_not_full_ms(self):
        # A foliated row is this folio's transcription → judged against the folio
        # HTR, NOT the whole MS (else every foliated row would be demoted).
        row = _foliated_edition("אבגד " * 120)  # ≈ folio HTR (_HTR = 400)
        called = []
        d = choose_default_source(
            [row], _HTR, full_htr_getter=lambda: called.append(1) or "x" * 99999
        )
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"
        assert not called, "getter must NOT be called for a foliated-only page"

    def test_needs_full_htr(self):
        assert fgp_needs_full_htr([_edition("x")]) is True            # whole-doc
        assert fgp_needs_full_htr([_foliated_edition("x")]) is False  # foliated
        assert fgp_needs_full_htr([]) is False
        # A translation is not an edition → no full-HTR needed for it.
        assert fgp_needs_full_htr([_edition("x", rel="Digital Translation")]) is False

    def test_best_of_multiple_editions_wins(self):
        # When several editions align to the folio, the highest-coverage one
        # decides (keep if any is full enough).
        partial = _edition("אבגד " * 5)
        full = _edition("אבגד " * 120)
        d = choose_default_source([partial, full], _HTR)
        assert d["eligible"] is True and d["source"] is full

    def test_env_override(self, monkeypatch):
        partial = _edition("אבגד " * 9)  # ratio 0.09
        # Default threshold 0.33 → demote.
        assert choose_default_source([partial], _HTR)["eligible"] is False
        # Lower the bar below 0.09 → kept.
        monkeypatch.setenv("FGP_DEFAULT_MIN_COVERAGE", "0.05")
        assert choose_default_source([partial], _HTR)["eligible"] is True
        # Unparseable / out-of-range falls back to the default → demote again.
        monkeypatch.setenv("FGP_DEFAULT_MIN_COVERAGE", "bogus")
        assert choose_default_source([partial], _HTR)["eligible"] is False
        monkeypatch.setenv("FGP_DEFAULT_MIN_COVERAGE", "9")
        assert choose_default_source([partial], _HTR)["eligible"] is False


# ── Wiring guards (both apps route through the shared policy) ──────────────────
# The GUI modules can't be imported headlessly, so guard the wiring by source
# (the project's static-guard pattern; see tests/test_fgp_chooser_integration.py).

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(rel):
    with open(os.path.join(_REPO, rel), encoding="utf-8") as fh:
        return fh.read()


class TestWiring:
    def test_web_version_selector_uses_policy(self):
        src = _read("web/components/version_selector.py")
        assert "choose_default_source" in src
        assert "shorter than V0.8" in src  # demotion hint

    def test_desktop_uses_policy(self):
        src = _read("genizah_app.py")
        assert "choose_default_source" in src
        assert "shorter than V0.8" in src

    def test_hint_is_translated(self):
        import genizah_translations as t

        assert t.TRANSLATIONS.get("shorter than V0.8")
