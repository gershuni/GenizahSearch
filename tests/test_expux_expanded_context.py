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


# --- XML-illegal control char safety (DOCX crash fix) ----------------------

def test_build_expanded_context_strips_control_chars():
    import re as _re
    full = "alpha \x00 beta needle \x0c gamma \x07 delta epsilon zeta eta"
    out = build_expanded_context(full, "beta *needle* gamma")
    assert not _re.search(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', out), \
        "XML-illegal control chars must be stripped from expanded context"
    assert "*needle*" in out


def test_write_docx_block_with_control_chars_saves_without_error():
    from io import BytesIO
    from docx import Document
    from shared.docx_export import write_docx_result_block
    doc = Document()
    r = {
        "display": {"shelfmark": "notes.pdf", "source": "LOCAL", "id": "97000000000000001"},
        "raw_file_hl": "beta *needle* gamma",
    }
    # full_text laced with NUL / form-feed / control chars (as real PDF page text can be).
    full = "alpha beta needle gamma \x00 delta \x0c epsilon \x07 zeta eta theta"
    write_docx_result_block(doc, r, filepath=r"C:\docs\notes.pdf", lang="en", full_text=full)
    buf = BytesIO()
    doc.save(buf)  # must NOT raise "All strings must be XML compatible …"
    assert buf.tell() > 0


def test_write_docx_block_genizah_with_control_chars_saves():
    from io import BytesIO
    from docx import Document
    from shared.docx_export import write_docx_result_block
    doc = Document()
    r = {
        "display": {"shelfmark": "T-S 12.1", "title": "Letter", "source": "", "id": "990012345678901"},
        "raw_file_hl": "delta *hit* zeta",
    }
    full = "alpha beta gamma delta hit zeta \x00 eta \x01 theta iota"
    write_docx_result_block(doc, r, filepath="", lang="he", full_text=full)
    buf = BytesIO()
    doc.save(buf)
    assert buf.tell() > 0
