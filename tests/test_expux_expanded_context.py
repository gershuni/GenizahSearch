# -*- coding: utf-8 -*-
"""Phase 105 EXPUX-04: full matched-passage context (capped + highlighted) in
DOCX / TXT exports, with safe fallback to the ±60-char snippet.

Covers the pure helper ``shared_export_utils.build_expanded_context`` plus the
module-level TXT block builders in ``genizah_app`` (no Qt instance needed).
"""
from shared_export_utils import build_expanded_context, EXPORT_CONTEXT_CAP


# --- build_expanded_context ------------------------------------------------

def test_expands_beyond_snippet_and_keeps_markers():
    full = ("alpha beta gamma delta epsilon HITWORD zeta eta theta iota "
            "kappa lambda mu nu xi omicron")
    snippet = "delta epsilon *HITWORD* zeta eta"  # narrow ±window
    out = build_expanded_context(full, snippet)
    # Fuller context: words outside the narrow snippet now present.
    assert "alpha" in out
    assert "omicron" in out
    # Matched term re-highlighted with * markers.
    assert "*HITWORD*" in out


def test_fallback_when_no_full_text():
    snippet = "some *hit* text"
    assert build_expanded_context("", snippet) == snippet
    assert build_expanded_context(None, snippet) == snippet


def test_fallback_when_term_not_in_full_text():
    # Normalization mismatch: marked term absent from full_text -> fallback.
    snippet = "x *NOTFOUND* y"
    full = "completely different text without the marked term"
    assert build_expanded_context(full, snippet) == snippet


def test_fallback_when_no_markers():
    snippet = "no markers here"
    full = "no markers here and a lot more context after it"
    # No * markers => no recoverable term => returns snippet unchanged.
    assert build_expanded_context(full, snippet) == snippet


def test_cap_limits_window_length():
    full = "X " * 5000 + "*needle*" + " Y" * 5000  # ~20k chars
    full_plain = full.replace("*", "")
    snippet = "Z *needle* Z"
    out = build_expanded_context(full_plain, snippet, cap=2000)
    # Window (sans ellipsis/markers) must respect the cap with slack for markers.
    assert len(out) <= EXPORT_CONTEXT_CAP + 20
    assert "*needle*" in out
    assert out.startswith("… ") or out.endswith(" …")  # truncation flagged


def test_multiple_occurrences_all_highlighted():
    full = "rain in spain falls mainly rain again rain"
    snippet = "in *rain* falls"
    out = build_expanded_context(full, snippet)
    assert out.count("*rain*") >= 2


def test_strips_stray_source_asterisks():
    full = "before 2*3 math *not a marker* needle after"
    snippet = "math *needle* after"
    out = build_expanded_context(full, snippet)
    # Only the recovered term is wrapped; source '*' became spaces.
    assert "*needle*" in out
    # The stray "*not a marker*" pair from source must not survive as markers.
    assert "*not a marker*" not in out


# --- TXT block builders (genizah_app module-level helpers) ------------------

def test_genizah_txt_expands_with_full_text_keeps_header():
    from genizah_app import _format_txt_genizah_block
    r = {
        "display": {"shelfmark": "T-S 12.1", "title": "Letter", "source": ""},
        "raw_file_hl": "delta *HITWORD* zeta",
    }
    full = "alpha beta gamma delta HITWORD zeta eta theta omicron"
    block = _format_txt_genizah_block(r, full_text=full)
    assert block.startswith("=== T-S 12.1 | Letter ===")
    assert "alpha" in block          # expanded context
    assert "*HITWORD*" in block      # markers preserved for Genizah TXT
    assert "Path:" not in block


def test_genizah_txt_fallback_byte_identical_without_full_text():
    from genizah_app import _format_txt_genizah_block
    r = {
        "display": {"shelfmark": "T-S 12.1", "title": "Letter", "source": ""},
        "raw_file_hl": "gen *hit* text",
    }
    block = _format_txt_genizah_block(r)  # no full_text => pre-v7.17 output
    assert block == "=== T-S 12.1 | Letter ===\ngen *hit* text"


def test_local_txt_expands_and_strips_markers():
    from genizah_app import _format_txt_local_block
    r = {
        "display": {"shelfmark": "notes.pdf", "source": "LOCAL", "id": "97000000000000001"},
        "raw_file_hl": "beta *needle* gamma",
        "chunk_locator": "p. 3",
    }
    full = "alpha beta needle gamma delta epsilon zeta eta theta"
    block = _format_txt_local_block(r, lambda sid: r"C:\docs\notes.pdf", full_text=full)
    assert block.startswith("=== notes.pdf | docs ===")
    assert "alpha" in block        # expanded context
    assert "*" not in block        # LOCAL TXT strips highlight markers
    assert "(p. 3)" in block
