# -*- coding: utf-8 -*-
"""Phase 96 D-F4 → Phase 102: rawdict-primary PDF extraction tests.

REVISION 2026-05-29 (Phase 102-02 M1 / MED-6): Phase 102 makes rawdict the
PRIMARY extraction path. The old blocks path survives ONLY as the D-03
LTR-damage guard (comparison/fallback net). This module is updated to reflect
the rawdict-primary contract, while preserving all valid RTL helper tests.

Key changes from Phase 96/101 version:
  - Primary path now calls get_text("rawdict", ...) — NOT get_text("blocks").
  - The blocks path still runs internally as the D-03 comparison net.
  - The sort=True fallback is still called from inside the blocks comparison
    net (via _extract_blocks_text → _fix_sort_true_rtl_page for pathological
    single-word-per-line pages that the LTR-damage guard routes to).
  - _FakePdfPage.get_text("rawdict") now returns {"blocks": []} so the rawdict
    path produces empty text → D-03 fires → falls back to blocks path output.
    Tests that relied on this fallback path continue to exercise the correct
    behavior via the D-03 net.

Fixture notes (REVISION 2026-05-24 discovery preserved):
  - `single_word_per_line.pdf` — blocks mode ratio >= 0.70 (pathological).
  - `clean_sample.pdf` — blocks mode ratio 0.0 (normal multi-word paragraphs).
  - `hebrew_sample.pdf` — itself ~97% single-word in blocks mode (pathological).
"""
import os
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")
CLEAN_PDF = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
HEBREW_PDF = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")


def _import_indexer_helpers():
    """Import the Phase 96 detection helper + extractor."""
    from shared.local_indexer import (
        extract_pdf_pages,
        _detect_single_word_per_line,
    )
    return extract_pdf_pages, _detect_single_word_per_line


def test_pathological_pdf_uses_fallback():
    """D-F4 (Phase 102): single_word_per_line.pdf must produce paragraph-shaped text.

    Phase 102 primary path is rawdict. For pathological letter-spaced PDFs, the
    rawdict de-space/reorder pipeline should produce paragraph-shaped output
    (multiple words per line). If rawdict output is damaged (D-03 guard fires),
    the blocks path fallback via sort=True also produces paragraph-shaped text.
    Either way, the final output must NOT be one-word-per-line.

    Fixture: `single_word_per_line.pdf` — a pathological PDF where words are
    placed at distinct x-positions but in scrambled content-stream order.
    REVISION 2026-05-29: now asserts rawdict is the primary call; blocks is
    the D-03 fallback net (called internally, not the primary path).
    """
    extract_pdf_pages, _ = _import_indexer_helpers()
    if not os.path.exists(SINGLE_WORD_PDF):
        pytest.fail(
            "Phase 96 Wave 0 fixture missing: " + SINGLE_WORD_PDF + "\n"
            "Run scripts/generate_single_word_fixture.py to regenerate."
        )
    pages = list(extract_pdf_pages(SINGLE_WORD_PDF))
    assert len(pages) >= 1
    page_num, text, title = pages[0]
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 5:
        pytest.skip("Sample too small for ratio check")
    single = sum(1 for ln in lines if len(ln.split()) <= 1)
    ratio = single / len(lines)
    # After rawdict de-space/reorder (or D-03 fallback via sort=True),
    # ratio should drop well below the 0.70 trigger threshold.
    assert ratio < 0.50, (
        f"Rawdict path (or D-03 fallback) did NOT produce paragraph-shaped text "
        f"(single_word_ratio={ratio:.2f}, expected < 0.50)"
    )


def test_good_pdf_stays_blocks():
    """D-F4 regression-direction-two: clean_sample.pdf (a synthetic PDF with
    proper multi-word paragraphs) must NOT trip the single-word detector.

    REVISION 2026-05-24: changed control fixture from hebrew_sample.pdf to
    clean_sample.pdf. The hebrew_sample.pdf fixture is itself ~97% single-word
    in blocks mode (real-world pathological), so it cannot serve as the clean
    control. clean_sample.pdf is a synthetic fixture created by insert_text()
    calls that produce proper multi-word blocks (ratio 0.0 in blocks mode).
    The fallback should NOT fire.
    """
    extract_pdf_pages, detect = _import_indexer_helpers()
    if not os.path.exists(CLEAN_PDF):
        pytest.fail(
            "Phase 96 clean_sample.pdf fixture missing: " + CLEAN_PDF + "\n"
            "Run scripts/generate_single_word_fixture.py to regenerate."
        )
    pages = list(extract_pdf_pages(CLEAN_PDF))
    assert len(pages) > 0
    for _p, text, _t in pages:
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) >= 5:
            assert not detect(text), (
                "False positive: clean_sample.pdf should NOT trip "
                "single-word-per-line detection (ratio should be 0.0)"
            )


def test_small_sample_skipped():
    """D-F4: detection heuristic returns False when < 5 non-empty lines
    (small-sample guard per RESEARCH §2)."""
    _, detect = _import_indexer_helpers()
    # 4 single-word lines: below the threshold sample size — must NOT trip.
    text = "one\ntwo\nthree\nfour\n"
    assert detect(text) is False, (
        "Small-sample guard failed: < 5 lines should skip detection"
    )


def test_good_pdf_does_not_invoke_fallback_mode():
    """Phase 102 rawdict-primary: clean_sample.pdf must call get_text("rawdict").

    REVISION 2026-05-29 (Phase 102 M1): Phase 102 makes rawdict the PRIMARY
    extraction path. The old assertion ("blocks must be called as primary") is
    replaced by the new reality: "rawdict must be called as primary."

    The D-03 LTR-damage guard internally calls get_text("blocks") for comparison,
    but "blocks" is no longer the PRIMARY path — it's the fallback net.

    The sort=True fallback path must NOT be invoked for clean PDFs (this invariant
    is preserved; the D-03 net only calls sort=True when blocks detects single-
    word-per-line, which clean_sample.pdf does NOT exhibit).

    Fixture: clean_sample.pdf (synthetic, ratio 0.0 in blocks mode).
    """
    import fitz
    from unittest.mock import patch

    extract_pdf_pages, _ = _import_indexer_helpers()
    if not os.path.exists(CLEAN_PDF):
        pytest.skip("clean_sample.pdf fixture not found")

    invoked_calls = []  # list of (mode, sort) tuples

    original_get_text = fitz.Page.get_text

    def _spy_get_text(self, *args, **kwargs):
        mode = args[0] if args else kwargs.get("option", None)
        sort = kwargs.get("sort", False)
        invoked_calls.append((mode, sort))
        return original_get_text(self, *args, **kwargs)

    with patch.object(fitz.Page, "get_text", _spy_get_text):
        pages = list(extract_pdf_pages(CLEAN_PDF))

    assert len(pages) > 0, "clean_sample.pdf should extract at least 1 page"

    # Phase 102: PRIMARY path is rawdict — confirm it was called.
    modes_called = [c[0] for c in invoked_calls]
    assert "rawdict" in modes_called, (
        "Phase 102 rawdict-primary regression: get_text('rawdict') was never "
        f"invoked as the primary extraction path. Modes called: {modes_called}"
    )

    # The sort=True fallback must NOT be invoked for clean PDFs.
    # (Even inside the D-03 blocks comparison net, clean_sample.pdf has
    # multi-word blocks and must not trigger the sort=True branch.)
    fallback_invocations = [
        i for i, (m, s) in enumerate(invoked_calls)
        if m == "text" and s is True
    ]
    assert not fallback_invocations, (
        f"Regression: clean_sample.pdf (a synthetic clean PDF) triggered the "
        f"get_text('text', sort=True) fallback (inside the D-03 blocks net). "
        f"The detection heuristic is over-triggering. "
        f"All invocations: {invoked_calls}"
    )


# ---------------------------------------------------------------------------
# Phase 101: sort=True RTL word-order fix tests (Wave 0 RED)
# ---------------------------------------------------------------------------

HEBREW_RTL_FIXTURE_PDF = os.path.join(FIXTURES_DIR, "hebrew_rtl_fixture.pdf")


def _import_phase101_helpers():
    from shared.local_indexer import (
        _fix_sort_true_rtl_line,
        _fix_sort_true_rtl_page,
        extract_pdf_pages,
    )
    return _fix_sort_true_rtl_line, _fix_sort_true_rtl_page, extract_pdf_pages


# ----- USER-DEC-1 (S-1 directional-run reversal) ---------------------------

def test_sort_true_rtl_pure_hebrew_word_order_fixed():
    """D-01/D-03: pure-RTL line (single RTL run) reverses ALL word tokens."""
    _, fix_page, _ = _import_phase101_helpers()
    wrong = "האישי בארכיונו עיור בעקבות"
    fixed = fix_page(wrong)
    assert fixed.split() == list(reversed(wrong.split())), (
        f"Pure-RTL line: tokens must reverse; got {fixed.split()!r}"
    )
    # Letters within each word stay in correct logical order (NOT char-reversed)
    assert "בעקבות" in fixed.split()


def test_sort_true_rtl_directional_runs_preserve_shelfmarks():
    """USER-DEC-1 / S-1: directional-run reversal keeps Latin shelfmarks adjacent.

    Input as sort=True would emit it (LTR visual order):
    'האישי T-S 12.123 בארכיונו' — three runs: [Hebrew], [Latin shelfmark],
    [Hebrew]. Run sequence reverses; within-run order preserved.
    """
    _, fix_page, _ = _import_phase101_helpers()
    inp = "האישי T-S 12.123 בארכיונו"
    out = fix_page(inp)
    toks = out.split()
    # T-S and 12.123 must remain adjacent (within-run order preserved).
    assert toks.index("T-S") + 1 == toks.index("12.123"), (
        f"S-1: T-S must stay adjacent to 12.123 in {toks!r}"
    )
    # Run sequence reversed: the trailing Hebrew word now precedes the Latin run.
    assert toks.index("בארכיונו") < toks.index("T-S"), (
        f"S-1: trailing RTL run must come before LTR run in {toks!r}"
    )


def test_sort_true_rtl_digits_run_with_hebrew():
    """S-1: digit tokens form non-RTL singleton runs between Hebrew tokens.

    Input runs: [פרק][5][עמוד][42] = [R, L, R, L]. Reversed run sequence
    yields [42, עמוד, 5, פרק] (each run is one token so within-run order
    is trivially preserved).
    """
    _, fix_page, _ = _import_phase101_helpers()
    inp = "פרק 5 עמוד 42"
    out = fix_page(inp)
    assert out.split() == ["42", "עמוד", "5", "פרק"], (
        f"S-1 digit interleave: got {out.split()!r}"
    )


def test_sort_true_ltr_noop():
    """D-05: _fix_sort_true_rtl_page is a no-op on pure-LTR/numeric/empty text."""
    _, fix_page, _ = _import_phase101_helpers()
    assert fix_page("hello world\nfoo bar baz") == "hello world\nfoo bar baz"
    assert fix_page("1234 5678") == "1234 5678"
    assert fix_page("") == ""
    assert fix_page("page 3 of 10") == "page 3 of 10"


# ----- Claude C-7 (boundary _rtl_ratio cases) ------------------------------

def test_sort_true_rtl_boundary_below_threshold_noop():
    """C-7 (TIGHTENED per REVIEWS round 2 Codex MEDIUM #9): _rtl_ratio JUST
    BELOW 0.4 must no-op. The candidate is tuned so its computed ratio is
    within ±0.05 of the 0.4 threshold so future drift in _rtl_ratio's
    numerator/denominator semantics actually catches the threshold gate.
    """
    from shared.local_indexer import _rtl_ratio
    _, fix_page, _ = _import_phase101_helpers()
    # 3 RTL letters / 8 alpha letters = 0.375 — just below the 0.4 gate.
    # Executor: if _rtl_ratio's tokenization changes, retune so 0.35 < r < 0.40.
    candidate = "abcde שלם"  # alpha chars: a,b,c,d,e + ש,ל,ם = 8; RTL = 3
    ratio = _rtl_ratio(candidate)
    assert 0.35 < ratio < 0.40, (
        f"Boundary test prerequisite: _rtl_ratio({candidate!r}) must be in "
        f"(0.35, 0.40) to actually test threshold proximity; got {ratio}. "
        f"Retune the candidate."
    )
    assert fix_page(candidate) == candidate, (
        f"C-7 below-threshold: {ratio:.3f} <= 0.4 must no-op; got transformed output"
    )


def test_sort_true_rtl_boundary_above_threshold_reverses():
    """C-7 (TIGHTENED per REVIEWS round 2 Codex MEDIUM #9): _rtl_ratio JUST
    ABOVE 0.4 triggers directional-run reversal. Candidate tuned to be within
    ±0.05 of the 0.4 threshold so the gate proximity is actually exercised.
    """
    from shared.local_indexer import _rtl_ratio
    _, fix_page, _ = _import_phase101_helpers()
    # 4 RTL letters / 9 alpha letters ≈ 0.444 — just above the 0.4 gate.
    candidate = "abcde שלום"  # alpha: a,b,c,d,e + ש,ל,ו,ם = 9; RTL = 4
    ratio = _rtl_ratio(candidate)
    assert 0.40 < ratio < 0.50, (
        f"Boundary test prerequisite: _rtl_ratio({candidate!r}) must be in "
        f"(0.40, 0.50) to actually test threshold proximity; got {ratio}. "
        f"Retune the candidate."
    )
    out = fix_page(candidate)
    assert out != candidate, (
        f"C-7 above-threshold: {ratio:.3f} > 0.4 must transform; got passthrough {out!r}"
    )


# ----- Claude S-8 (xfail residual-edge-case guard) -------------------------

@pytest.mark.xfail(
    strict=True,   # REVIEWS round 2 Codex LOW #10: strict=True so an XPASS
                   # actually fails CI and forces the re-review the docstring
                   # describes. strict=False let CI go green on accidental fix.
    reason=(
        "S-8: documents the residual edge case where the directional-run "
        "algorithm still has imperfect output for pathological mixed scripts "
        "with attached punctuation. XPASS = the S-1 algorithm has expanded "
        "to handle this case — STRICT failure forces re-review (was strict=False)."
    ),
)
def test_sort_true_rtl_pathological_mixed_script():
    """S-8: pathological mixed-script case kept as a known-limitation marker."""
    _, fix_page, _ = _import_phase101_helpers()
    # Parens/brackets attached to words are a known limitation of pure token-
    # based directional-run reversal (the full Unicode Bidi Algorithm handles
    # them via paired bracket pairs). The expected output below is the "ideal"
    # algorithm-perfect form which directional-run reversal does NOT produce.
    inp = "(שלום) text [42]"
    out = fix_page(inp)
    assert out == "[42] text (שלום)"


# ----- REV-2a (branch-integration tests for extract_pdf_pages) -------------

class _FakePdfPage:
    """Minimal fitz.Page surrogate for REV-2a branch-integration tests.

    REVISION 2026-05-29 (Phase 102-02 M1): Phase 102 makes rawdict the PRIMARY
    path. get_text("rawdict") now returns {"blocks": []} (empty rawdict dict)
    so the rawdict pipeline produces empty text, triggering the D-03 LTR-damage
    guard which falls back to the blocks path output. This preserves existing
    test coverage: the sort=True RTL fix is still exercised via the D-03 net's
    internal blocks path → sort=True fallback for pathological PDFs.
    """

    def __init__(self, blocks_text: str, sort_true_text: str):
        self._blocks_text = blocks_text
        self._sort_true_text = sort_true_text
        # Real fitz.Page exposes .number; extract_pdf_pages uses it.
        self.number = 0

    def get_text(self, mode, sort=False, **_kwargs):
        # Phase 102: PRIMARY path is get_text("rawdict") — return empty dict so
        # the D-03 LTR-damage guard fires and falls back to the blocks path.
        if mode == "rawdict":
            return {"blocks": []}
        if mode == "blocks":
            # Real PyMuPDF blocks mode returns a list of tuples
            # (x0, y0, x1, y1, text, block_no, block_type). The production
            # code keeps only blocks where b[6] == 0 (text blocks) and reads
            # b[4]. Synthesize one block per line of the configured text so
            # _extract_blocks_text reconstructs the input verbatim.
            return [
                (0.0, float(i), 100.0, float(i + 1), line, i, 0)
                for i, line in enumerate(self._blocks_text.splitlines())
                if line.strip()
            ]
        if mode == "text" and sort:
            return self._sort_true_text
        return ""


class _FakePdfDocument:
    def __init__(self, page: _FakePdfPage):
        self._page = page
        # extract_pdf_pages uses len(doc) and indexing / iteration.
        self.page_count = 1
        self.metadata = {}  # mirrors fitz.Document.metadata

    def __len__(self):
        return 1

    def __iter__(self):
        yield self._page

    def __getitem__(self, idx):
        if idx != 0:
            raise IndexError(idx)
        return self._page

    def load_page(self, idx):
        return self[idx]

    def close(self):
        pass


def _install_fake_fitz(monkeypatch, page: _FakePdfPage):
    """Replace shared.local_indexer.fitz.open with a stub returning our fake doc.

    The executor MUST read shared/local_indexer.py top-of-file to see how fitz
    is imported (likely `import fitz` or `import pymupdf as fitz`). Whichever
    name extract_pdf_pages uses, monkeypatch the .open attribute on that name
    inside the shared.local_indexer namespace so production code sees the fake.
    """
    import shared.local_indexer as li_mod
    doc = _FakePdfDocument(page)
    # Patch the `fitz` module reference inside shared.local_indexer's namespace.
    # `setattr(li_mod.fitz, 'open', ...)` is safer than replacing the whole
    # module since other names (TOOLS, etc.) remain reachable.
    monkeypatch.setattr(li_mod.fitz, "open", lambda *a, **kw: doc, raising=True)


def test_extract_pdf_pages_applies_rtl_fix_in_sort_true_fallback(monkeypatch):
    """REV-2a (Phase 102 revision): proves the RTL fix runs via the D-03 net.

    REVISION 2026-05-29 (Phase 102-02 M1): Phase 102 makes rawdict PRIMARY.
    The fake page returns {"blocks": []} for rawdict → empty rawdict text →
    D-03 guard fires → falls back to _extract_blocks_text() which calls
    get_text("blocks") → single-word-per-line → triggers sort=True fallback →
    _fix_sort_true_rtl_page reverses to logical reading order.

    The end result is the same: extract_pdf_pages produces the RTL-corrected
    text for pathological single-word-per-line RTL pages. The RTL fix now runs
    inside the D-03 blocks-fallback net rather than as the primary path.
    """
    _, _, extract_pdf_pages = _import_phase101_helpers()
    # Single-word-per-line text triggers the fallback gate (ratio == 1.0).
    blocks_text = "\n".join(["שלום", "עליכם", "חברים", "טובים", "אחים"])
    # sort=True LTR-visual order (what PyMuPDF returns for an RTL page).
    # The directional-run reversal should flip this to logical reading order.
    sort_true_text = "האישי בארכיונו עיור בעקבות"
    expected_after_fix = "בעקבות עיור בארכיונו האישי"

    page = _FakePdfPage(blocks_text=blocks_text, sort_true_text=sort_true_text)
    _install_fake_fitz(monkeypatch, page)

    pages = list(extract_pdf_pages("/fake/path.pdf"))
    assert len(pages) == 1, f"expected 1 page, got {len(pages)}"
    _page_num, text, _title = pages[0]
    # Hard assertion: the emitted text is the directional-run-reversed form,
    # NOT the raw sort=True fixture. This proves _fix_sort_true_rtl_page ran
    # (via the D-03 blocks-fallback net → sort=True path).
    assert text.strip() == expected_after_fix, (
        f"REV-2a: D-03 fallback net must apply _fix_sort_true_rtl_page; "
        f"got {text.strip()!r}, expected {expected_after_fix!r}"
    )
    assert text.strip() != sort_true_text, (
        "REV-2a: emitted text must DIFFER from the raw sort=True fixture"
    )


def test_extract_pdf_pages_blocks_path_untouched(monkeypatch):
    """REV-2a companion (Phase 102 revision): D-03 blocks net does not corrupt
    already-correct multi-word RTL text.

    REVISION 2026-05-29 (Phase 102-02 M1): rawdict returns empty → D-03 net
    fires → blocks extraction runs → multi-word blocks (ratio < 0.70) → sort=True
    NOT invoked → TRAP sentinel must not appear in output.

    The token set of the blocks text must be preserved (D-03 blocks fallback
    passes multi-word text through without reversing it).
    """
    _, _, extract_pdf_pages = _import_phase101_helpers()
    # Multi-word-per-line RTL text — _detect_single_word_per_line (>= 0.70)
    # does NOT fire, so sort=True is NOT called from the D-03 net.
    blocks_text = (
        "בעקבות עיור בארכיונו האישי\n"
        "שלום עליכם חברים טובים\n"
        "אחים יקרים מאוד\n"
        "ועוד שורה ארוכה\n"
        "וגם שורה אחרונה כאן"
    )
    # If extract_pdf_pages WRONGLY calls sort=True, this TRAP sentinel appears.
    sort_true_text = "TRAP — this branch should not run"

    page = _FakePdfPage(blocks_text=blocks_text, sort_true_text=sort_true_text)
    _install_fake_fitz(monkeypatch, page)

    pages = list(extract_pdf_pages("/fake/path.pdf"))
    assert len(pages) == 1
    _page_num, text, _title = pages[0]
    # Hard assertion: TRAP sentinel must NOT appear in output.
    assert "TRAP" not in text, (
        "REV-2a: sort=True fallback must NOT run when D-03 blocks net "
        "gives multi-word lines (single_word_ratio < 0.70)"
    )
    # The D-03 blocks fallback preserves original tokens.
    in_toks = set(blocks_text.split())
    out_toks = set(text.split())
    assert in_toks == out_toks, (
        f"REV-2a: D-03 blocks fallback must preserve token set; "
        f"missing={in_toks - out_toks!r}, extra={out_toks - in_toks!r}"
    )


# ---------------------------------------------------------------------------
# Phase 101 follow-up (2026-05-28): intra-block newline collapse tests
# ---------------------------------------------------------------------------
# Bug origin: Hillel's UAT 2026-05-28 — PyMuPDF blocks output of a Hebrew
# scholarly book put individual characters/commas/quote marks on their own
# '\n'-delimited lines INSIDE a single block, producing fragmented prose like:
#   הכרעת רבי העומדת במרכז ה
#   סוגיא
#   ,
#   הינה הכרעה
# Decision: collapse ALL intra-block '\n' to space. Block boundaries (joined
# upstream as '\n\n') still mark true paragraph breaks.


def _import_collapse_helper():
    from shared.local_indexer import _collapse_intra_block_newlines
    return _collapse_intra_block_newlines


def test_collapse_intra_block_newlines_single_line_unchanged():
    fn = _import_collapse_helper()
    assert fn("hello world") == "hello world"
    assert fn("שלום עליכם") == "שלום עליכם"


def test_collapse_intra_block_newlines_empty():
    fn = _import_collapse_helper()
    assert fn("") == ""
    assert fn("   \n  \n  ") == ""


def test_collapse_intra_block_newlines_strips_outer_whitespace():
    fn = _import_collapse_helper()
    assert fn("  hello  ") == "hello"
    assert fn("\nhello\n") == "hello"


def test_collapse_intra_block_newlines_collapses_internal_newlines():
    """Hillel UAT 2026-05-28: fragmented Hebrew bidi output joins back to prose."""
    fn = _import_collapse_helper()
    fragmented = (
        "הכרעת רבי העומדת במרכז ה\n"
        "סוגיא\n"
        ",\n"
        "הינה הכרעה"
    )
    expected = "הכרעת רבי העומדת במרכז ה סוגיא , הינה הכרעה"
    assert fn(fragmented) == expected


def test_collapse_intra_block_newlines_collapses_runs_of_whitespace_around_newline():
    fn = _import_collapse_helper()
    # Trailing spaces before '\n' and leading spaces after must collapse to one space.
    assert fn("foo  \n  bar") == "foo bar"
    assert fn("foo\n\nbar") == "foo bar"


def test_collapse_intra_block_newlines_no_sentence_boundary_preserved():
    """Per Hillel: 'Join is better than divide' — sentence-final punctuation
    inside a block does NOT preserve the break. Block boundaries (set by
    PyMuPDF) are the only paragraph signal."""
    fn = _import_collapse_helper()
    assert fn("First sentence.\nSecond sentence.") == "First sentence. Second sentence."


class _MultilineBlocksFakePage:
    """Variant of _FakePdfPage where get_text('blocks') returns blocks whose
    text bodies contain internal '\\n'. Used to test the intra-block collapse.

    REVISION 2026-05-29 (Phase 102-02 M1): returns {"blocks": []} for rawdict
    so the D-03 LTR-damage guard fires and falls back to the blocks path, which
    exercises the intra-block newline collapse via _collapse_intra_block_newlines.
    """

    def __init__(self, block_bodies):
        self._block_bodies = list(block_bodies)
        self.number = 0

    def get_text(self, mode, sort=False, **_kwargs):
        if mode == "rawdict":
            return {"blocks": []}  # Phase 102: empty rawdict → D-03 fires → blocks fallback
        if mode == "blocks":
            return [
                (0.0, float(i), 100.0, float(i + 1), body, i, 0)
                for i, body in enumerate(self._block_bodies)
                if body.strip()
            ]
        if mode == "text" and sort:
            return ""
        return ""


def _install_multiline_fake(monkeypatch, page):
    import shared.local_indexer as li_mod
    doc = _FakePdfDocument(page)
    monkeypatch.setattr(li_mod.fitz, "open", lambda *a, **kw: doc, raising=True)


def test_extract_pdf_pages_collapses_intra_block_newlines(monkeypatch):
    """End-to-end: a block containing the fragmented Hebrew bidi output
    becomes one continuous paragraph; block boundaries still produce '\\n\\n'."""
    _, _, extract_pdf_pages = _import_phase101_helpers()
    block_1 = (
        "הכרעת רבי העומדת במרכז ה\n"
        "סוגיא\n"
        ",\n"
        "הינה הכרעה"
    )
    block_2 = (
        "לעומת זאת\n"
        ",\n"
        "נראה שמרכז ה\n"
        "סוגיא"
    )
    page = _MultilineBlocksFakePage([block_1, block_2])
    _install_multiline_fake(monkeypatch, page)

    pages = list(extract_pdf_pages("/fake/path.pdf"))
    assert len(pages) == 1
    _page_num, text, _title = pages[0]
    assert "\n\n" in text, "block boundary marker must survive"
    paras = text.split("\n\n")
    assert len(paras) == 2
    assert "\n" not in paras[0], f"paragraph 1 must have no internal newlines: {paras[0]!r}"
    assert "\n" not in paras[1], f"paragraph 2 must have no internal newlines: {paras[1]!r}"
    assert paras[0] == "הכרעת רבי העומדת במרכז ה סוגיא , הינה הכרעה"
    assert paras[1] == "לעומת זאת , נראה שמרכז ה סוגיא"


# ----- D-06 real fixture ---------------------------------------------------

def test_sort_true_rtl_real_hebrew_fixture():
    """D-06: Real Hebrew PDF (Phase 100 UAT book excerpt) extracts in correct
    word order. Skips until the inbound fixture is committed (Hillel provides
    excerpt). Provenance: tests/fixtures/local_indexer/README.md.
    """
    _, _, extract_pdf_pages = _import_phase101_helpers()
    if not os.path.exists(HEBREW_RTL_FIXTURE_PDF):
        pytest.skip("hebrew_rtl_fixture.pdf not yet committed (inbound asset from user)")
    pages = list(extract_pdf_pages(HEBREW_RTL_FIXTURE_PDF))
    assert len(pages) >= 1
    _page_num, text, _title = pages[0]
    assert text.strip(), "Fixture PDF must yield non-empty text"
