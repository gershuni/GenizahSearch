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
import sqlite3

import pytest

from shared.fgp_service import (
    _COVERAGE_MIN_HTR_LETTERS,
    _DEFAULT_MIN_COVERAGE,
    _heb_letter_count,
    _normalize_for_contains,
    choose_default_source,
    fgp_incipit,
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


def _cnum_edition(content, c_number="C62553", **kw):
    # A per-image row keyed by c_number with NO folio label — must be treated as
    # per-image (folio baseline), NOT whole-doc (5.8k such editions exist).
    return _edition(content, c_number=c_number, **kw)


def _pgp_edition(content, rel="Digital Edition", **kw):
    # A PGP source: no 'source'/'is_fgp' marker -> source_provider() == 'pgp'.
    d = {"doc_relation": rel, "content": content}
    d.update(kw)
    return d


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

    def test_cnumber_row_is_per_image_not_whole_doc(self):
        # A c-numbered row with null image_side is per-image (5.8k such editions):
        # judged against the FOLIO HTR, never the whole MS, and the full-MS getter
        # must NOT be invoked for it.
        row = _cnum_edition("אבגד " * 120)  # ≈ folio HTR (_HTR = 400)
        called = []
        d = choose_default_source(
            [row], _HTR, full_htr_getter=lambda: called.append(1) or "x" * 99999
        )
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"
        assert not called, "c-numbered per-image row must not trigger the full-MS fetch"

    def test_needs_full_htr(self):
        assert fgp_needs_full_htr([_edition("x")]) is True            # whole-doc
        assert fgp_needs_full_htr([_foliated_edition("x")]) is False  # foliated
        assert fgp_needs_full_htr([_cnum_edition("x")]) is False      # c-numbered per-image
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


# ── D: text-match demotion for whole-document FGP rows ─────────────────────────
# (debug/oxford-fgp-image-mismatch.md sub-issue D) A whole-doc row clearing the
# coverage bar (comprehensive vs. the whole MS, or an unmeasurable "unknown"
# baseline) is not necessarily ABOUT the displayed folio -- a codex-level
# catalogue excerpt can be long/comprehensive yet describe a DIFFERENT folio
# entirely. MS heb. g.2 (sys_id 990053489970205171) is the real case: its
# longest FGP row (4,281 chars, "Ox, Bold. Heb. g. 2 (2700) [example]...") is
# a catalogue sample that clears coverage but never mentions folio 27's text.

# The real folio-27 V0.8/HTR text (tantivy index unique_id
# IE168181472_P000002_FL168181475) -- contains BOTH phrases the real-data pin
# below checks for absence in every FGP row.
_FOLIO27_HTR = (
    "רוצצנו במחש כי קברים הוציאנו מעברותיוו\n"
    "מודררים ייי פוקח עוזרים . שאפנו והיו\n"
    "שאופים . ביטה בנפילת רחופים . זוקף\n"
    "כפופים. תקותינו וסברנו בך סלחנא כדרכ\n"
    "טובך י צבאות אשרי אדם בוטח כך .\n"
    "קדושי\n"
    "שי\n"
    "תקום רבה\n"
    "דיניך וכהדרי אלצדקת עיניך ולבך מביט\n"
    "ועיניך וג במקום אחד\n"
    "ארפן\n"
    "אהובים היום נדמו כמלאכים . בקומה זקופה\n"
    "כמעמד מל . גנונים אגודים במ' . דוברים\n"
    "קדוש וברוך כמ' הם לובשי לובן כמ'. ובמו\n"
    "אין אכילה ושתיה כמ' . זימון שינה מפרידים\n"
    "כמ' חוסן טהרה יש בם במ' טעם שלום"
)

_FGP_DB = os.path.join("fgp_data", "fgp_transcriptions.db")


class TestTextMatchDemotion:
    def test_whole_doc_no_overlap_is_demoted_even_if_coverage_passes(self):
        # Long enough to clear coverage against the folio baseline, but shares
        # NO vocabulary with the actual displayed folio -> demoted.
        unrelated = _edition("שונה לגמרי טקסט אחר בעליל " * 20)
        d = choose_default_source([unrelated], _FOLIO27_HTR)
        assert d["eligible"] is False
        assert d["reason"] == "demote_no_text_match"
        assert d["source"] is None

    def test_whole_doc_with_real_overlap_is_not_demoted(self):
        # A whole-doc row that DOES share the folio's vocabulary (clears the
        # similarity floor) stays eligible even though it is whole-doc.
        related = _edition(_FOLIO27_HTR * 3)
        d = choose_default_source([related], _FOLIO27_HTR)
        assert d["eligible"] is True
        assert d["reason"] == "fgp_sufficient"

    def test_foliated_row_exempt_from_text_match_demotion(self):
        # A confident per-image match (image_side) is NEVER subject to this --
        # zero regression on the ~5,400 foliated FGP editions: coverage alone
        # still governs, even with zero vocabulary overlap (e.g. a garbled OCR
        # page).
        row = _foliated_edition("שונה לגמרי טקסט אחר בעליל " * 20, image_side="27b")
        d = choose_default_source([row], _FOLIO27_HTR)
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"

    def test_cnumbered_row_exempt_from_text_match_demotion(self):
        row = _cnum_edition("שונה לגמרי טקסט אחר בעליל " * 20)
        d = choose_default_source([row], _FOLIO27_HTR)
        assert d["eligible"] is True and d["reason"] == "fgp_sufficient"

    def test_short_htr_skips_demotion_check(self):
        # Below _SIM_MIN_TOKENS -- no reliable overlap signal -- fail toward
        # FGP (never demote on an unmeasurable folio baseline); this is the
        # SAME "htr_too_short" path D must not touch.
        unrelated = _edition("שונה לגמרי אחר " * 20)
        d = choose_default_source([unrelated], "אבג")
        assert d["eligible"] is True
        assert d["reason"] == "htr_too_short"

    @pytest.mark.skipif(not os.path.exists(_FGP_DB), reason="fgp_transcriptions.db sidecar absent")
    def test_real_ms_heb_g2_folio27_case(self):
        # The exact reported bug (debug/oxford-fgp-image-mismatch.md), pinned
        # against the REAL sidecar DB + the REAL V0.8 text of the folio the
        # user's search hit ("תקום רבה דיניך").
        conn = sqlite3.connect(_FGP_DB)
        try:
            rows = conn.execute(
                "SELECT id, content, doc_relation FROM fgp_transcriptions WHERE sys_id = ?",
                ("990053489970205171",),
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 10, "MS heb. g.2/27's FGP row count changed -- re-pin the case"

        # Pin the underlying data fact this bug rests on: NONE of the 10 rows
        # contain either phrase actually on folio 27 (diacritic-stripped
        # substring check, same normalization choose_default_source uses) --
        # this is WHY defaulting to any of them is wrong.
        phrases = ["תקום רבה דיניך", "אהובים היום"]
        for rid, content, _rel in rows:
            normalized = _normalize_for_contains(content)
            for phrase in phrases:
                assert _normalize_for_contains(phrase) not in normalized, (
                    f"row {rid} unexpectedly contains {phrase!r} -- "
                    "re-pin the case, the underlying data changed"
                )

        sources = [
            {"source": "fgp", "id": rid, "doc_relation": rel, "content": content}
            for rid, content, rel in rows
        ]
        d = choose_default_source(sources, _FOLIO27_HTR)
        assert d["reason"] == "demote_no_text_match"
        assert d["eligible"] is False
        assert d["source"] is None


# ── D2: FGP combo/menu incipit label ────────────────────────────────────────────


class TestFgpIncipit:
    def test_strips_nikud_and_collapses_whitespace(self):
        assert fgp_incipit("שָׁלוֹם\n\nעוֹלָם") == "שלום עולם"

    def test_short_content_returned_whole_no_ellipsis(self):
        assert fgp_incipit("אבגד", max_chars=40) == "אבגד"

    def test_truncated_at_max_chars_with_ellipsis(self):
        content = "א" * 50
        out = fgp_incipit(content, max_chars=40)
        assert out == "א" * 40 + "…"

    def test_empty_and_none(self):
        assert fgp_incipit("") == ""
        assert fgp_incipit(None) == ""

    def test_distinguishes_two_rows_with_identical_labels(self):
        # The actual bug (MS heb. g.2): ~10 rows all render the bare "FGP
        # Transcription" label. The incipit is what makes them tellable apart.
        a = fgp_incipit("ברכת מזון אֲבָרֵך לְאֵל אֱמוּנָה")
        b = fgp_incipit("Ox, Bold. Heb. g. 2 (2700) [example]")
        assert a != b and a and b


# ── SEED-033 Option A: search-scoped "must_contain" override ───────────────────
# (planted in .planning/seeds/SEED-033-pgp-default-masks-matched-transcription.md,
# ruled into this debug session 2026-09-02) A PGP edition wins UNCONDITIONALLY
# today, even when it does not contain what the user actually searched for --
# the reported case: pgpid 37732 (a 73-char Arabic address) beats the 491-char
# V0.8 text of the SAME folio that DOES contain the searched phrase. When the
# caller supplies that phrase, prefer whichever source (PGP -> FGP -> V0.8)
# actually contains it; a miss falls through to today's exact order, unchanged.

# The REAL pgpid-37732 PGP source row (pgp_data/pgp.db, document_sources id
# 5825) -- Arabic khidma-letter address, does NOT contain the Hebrew phrase.
_PGP_37732 = _pgp_edition(
    "\nخدمة تعرض\nعلى مجلس المولا\nالفقيه الجليل\nجمال الدين مجمل\nالنعوت والاوصاف\n",
    pgpid=37732, id=5825, source_scholar="Alan Elbaum, unpublished editions (2023).",
    language="Arabic",
)
# The REAL V0.8 text of the SAME folio (tantivy unique_id
# IE61676826_P000002_FL61676829) -- DOES contain "עצים עליו למודה" (twice,
# duplicated in the source -- a separate, already-documented data defect not
# in scope here; see SEED-033's "Findings that are NOT this seed").
_V08_HEBR18_FOLIO2 = (
    "]\n]\n][\n]\nל\n]\nל\n]\n]\n][\nנ\n]\nⲙ\n][\n]\n]\n]\n[\n][\n[\n\nול\nב\nב\nן\n]\n"
    "]ה[ מכולה וגלמון\nשמ[ בו רכון זול דנה אתור\nישרא הר צין תאכנה בנות\n]ום אם פי\n"
    "אב.נה מא בח עבודה\n]ך עצים עליו למודה\nי. אל יחידו בח.. הד[\nלכוזה\nעד[ ] כבן\n"
    "ומעל ענינים שהו כאקדה\nאת בנו לחופק ↑ ) ליה\nלקח[ זילה אלסלדה\nת [\n]\n]"
)

_PGP_DB = os.path.join("pgp_data", "pgp.db")


class TestMustContainOverride:
    def test_default_order_unaffected_when_must_contain_absent(self):
        # Preserves PGP-first EXACTLY when must_contain is not supplied.
        d = choose_default_source([_PGP_37732], _V08_HEBR18_FOLIO2)
        assert d["eligible"] is True
        assert d["reason"] == "pgp_edition"
        assert d["provider"] == "pgp"
        assert d["source"] is _PGP_37732
        assert d["must_contain_matched"] is False

    def test_must_contain_in_v08_overrides_unconditional_pgp(self):
        # The exact reported bug: PGP wins unconditionally today even though
        # it does not contain the searched phrase, while V0.8 does.
        d = choose_default_source(
            [_PGP_37732], _V08_HEBR18_FOLIO2, must_contain="עצים עליו למודה",
        )
        assert d["eligible"] is False
        assert d["reason"] == "must_contain_v08"
        assert d["provider"] == "v08"
        assert d["source"] is None
        assert d["must_contain_matched"] is True

    def test_must_contain_in_pgp_keeps_pgp(self):
        d = choose_default_source(
            [_PGP_37732], _V08_HEBR18_FOLIO2, must_contain="جمال الدين",
        )
        assert d["eligible"] is True
        assert d["reason"] == "must_contain_match"
        assert d["provider"] == "pgp"
        assert d["source"] is _PGP_37732
        assert d["must_contain_matched"] is True

    def test_must_contain_miss_falls_through_to_pgp_first(self):
        # Matches NEITHER source -> NOT a demotion signal; today's order.
        d = choose_default_source(
            [_PGP_37732], _V08_HEBR18_FOLIO2, must_contain="שום דבר שלא קיים כאן",
        )
        assert d["eligible"] is True
        assert d["reason"] == "pgp_edition"
        assert d["provider"] == "pgp"
        assert d["must_contain_matched"] is False

    def test_must_contain_prefers_fgp_over_v08_when_no_pgp(self):
        fgp = _edition("טקסט אחר לגמרי שאינו קשור", id=1)
        needle_fgp = _edition("זהו הביטוי המבוקש בתוכן ה-FGP", id=2)
        d = choose_default_source(
            [fgp, needle_fgp], "טקסט שונה ב-V0.8 שלא מכיל את הביטוי",
            must_contain="הביטוי המבוקש",
        )
        assert d["eligible"] is True
        assert d["reason"] == "must_contain_match"
        assert d["provider"] == "fgp"
        assert d["source"] is needle_fgp

    def test_diacritic_and_whitespace_mismatch_still_matches(self):
        # strip_search_diacritics + nikud + whitespace normalization on BOTH
        # sides (SEED-030 rule 1 / SEED-033 Option A spec).
        pointed = _pgp_edition("שָׁלוֹם\nעוֹלָם", id=9)
        d = choose_default_source([pointed], "", must_contain="שלום   עולם")
        assert d["eligible"] is True and d["source"] is pointed

    @pytest.mark.skipif(not os.path.exists(_PGP_DB), reason="pgp.db sidecar absent")
    def test_real_pgpid_37732_pair(self):
        # RE-fetches the pinned row from the REAL sidecar (not just the
        # hardcoded literal above) so a future data refresh cannot silently
        # invalidate the case without failing this test.
        conn = sqlite3.connect(_PGP_DB)
        try:
            row = conn.execute(
                "SELECT id, pgpid, source_scholar, doc_relation, language, content "
                "FROM document_sources WHERE pgpid = ?", (37732,),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None, "pgpid 37732 not found -- re-pin the case"
        rid, pgpid, scholar, rel, lang, content = row
        assert "עצים עליו למודה" not in content
        pgp_source = {
            "id": rid, "pgpid": pgpid, "source_scholar": scholar,
            "doc_relation": rel, "language": lang, "content": content,
        }

        d_before = choose_default_source([pgp_source], _V08_HEBR18_FOLIO2)
        assert d_before["reason"] == "pgp_edition"  # unconditional PGP-first

        d_after = choose_default_source(
            [pgp_source], _V08_HEBR18_FOLIO2, must_contain="עצים עליו למודה",
        )
        assert d_after["eligible"] is False
        assert d_after["reason"] == "must_contain_v08"
        assert d_after["provider"] == "v08"


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
        assert "full_original_text" in src  # whole-MS baseline plumbed in

    def test_web_version_selector_honors_v08_must_contain_before_legacy_pgp_fallback(self):
        # 2026-09-02 live check (Heid. Hebr. 18 fol. 1v, /browse?...&highlight=עצים עליו למודה):
        # the shared policy returned provider='v08' (V0.8 contains the phrase) but the
        # renderer fell through to the legacy ``pgp_transcription`` block and re-applied
        # the Arabic PGP edition under a V0.8 badge. The v08 branch must re-render the
        # original text and RETURN before that fallback.
        src = _read("web/components/version_selector.py")
        v08 = src.index("decision.get('provider') == 'v08'")
        legacy = src.index("# Fallback to pgp_transcription for backward compatibility")
        assert v08 < legacy
        branch = src[v08:legacy]
        assert "on_version_change(original_text, {'source': 'original'" in branch
        assert "return" in branch

    def test_web_search_results_uses_policy(self):
        # The Advanced/search-result reading view is a SECOND web selector surface.
        src = _read("web/pages/search_results.py")
        assert "choose_default_source" in src
        assert "full_original_text" in src
        # SEED-033 Option A: the inline reader passes the snippet's matched phrase
        # and no longer hard-codes PGP-first ahead of the shared helper.
        # round 11: the phrase now goes through the page-scoping helper
        assert "must_contain=_hit_scope_phrase(snippet, adv_state, page)" in src
        assert "_snippet_match_phrase(" in src   # still the extractor underneath
        assert "_groups['pgp_editions'][0].get('content'" not in src

    def test_web_browse_passes_full_htr(self):
        assert "fgp_full_htr_text" in _read("web/pages/browse_enrichment.py")
        assert "fgp_needs_full_htr" in _read("web/pages/browse_enrichment.py")

    def test_desktop_uses_policy(self):
        src = _read("genizah_app.py")
        assert "choose_default_source" in src
        assert "shorter than V0.8" in src

    def test_desktop_selector_methods_are_context_parametrized(self):
        # Regression guard (Codex F3): the shared desktop selector helpers must
        # take sys_id/htr_text from the CALLER, never read self.browse_* — else
        # the ResultDialog would score against the Browse tab's manuscript.
        src = _read("genizah_app.py")
        for sig in (
            "def _auto_select_pgp_edition(self, combo, sources=None, htr_text=None, sys_id=None,",
            "def _populate_pgp_combo(self, combo, sources, pgp_doc, htr_text=None, sys_id=None)",
        ):
            assert sig in src, sig
        # SEED-033 Option A: the search-scoped override is a real parameter,
        # not bolted on via **kwargs.
        assert "must_contain=None)" in src
        # The ResultDialog threads its OWN context through.
        rd = _read("desktop/result_dialog.py")
        assert "sys_id=self.current_sys_id" in rd
        assert "must_contain=getattr(self, '_rd_search_match_text', None)" in rd

    def test_hint_is_translated(self):
        import genizah_translations as t

        assert t.TRANSLATIONS.get("shorter than V0.8")
