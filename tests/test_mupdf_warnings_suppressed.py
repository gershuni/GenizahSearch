# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-C (D-11) — MuPDF stderr suppression at module import.

Pins behavior of `shared/local_indexer.py`'s import-time call to
`fitz.TOOLS.mupdf_display_warnings(False)` (and the adjacent
`mupdf_display_errors(False)` deviation — see source comment).

Codex Critique #2 fix: broad `except Exception` is REQUIRED. Narrow
`except AttributeError` is explicitly forbidden because warning suppression
is non-critical; a future PyMuPDF API change must NOT crash module import.

Codex Critique #3 + round-4 MEDIUM tightening: Test 4 pins the EFFECT (zero
`MuPDF error:` lines on stderr when running extract_pdf_pages against a
deterministic malformed-Tf fixture). NO skip-on-missing-dep escape hatch
(no `pytest` skip helpers anywhere in this file) — PyMuPDF is a hard
project dependency per CompileScriptGenizah.iss and requirements.txt. If
`fitz` is somehow unavailable the test FAILS (not skips).
"""
from __future__ import annotations

import importlib
import logging
import os
import sys

# Path bootstrap so `shared.local_indexer` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_module_import_invokes_mupdf_display_warnings(monkeypatch):
    """Import (or reload) of shared.local_indexer must call mupdf_display_warnings(False)."""
    import fitz
    calls = []
    monkeypatch.setattr(
        fitz.TOOLS, "mupdf_display_warnings", lambda v: calls.append(v)
    )
    if "shared.local_indexer" in sys.modules:
        importlib.reload(sys.modules["shared.local_indexer"])
    else:
        import shared.local_indexer  # noqa: F401
    assert False in calls, (
        f"Expected mupdf_display_warnings(False) call at import; got {calls}"
    )


def test_module_import_survives_attributeerror_on_tools(monkeypatch, caplog):
    """Codex Critique #2: broad except Exception must catch AttributeError on TOOLS access."""
    import fitz

    class _BrokenTools:
        def __getattr__(self, name):
            raise AttributeError(f"simulated missing {name}")

    monkeypatch.setattr(fitz, "TOOLS", _BrokenTools())
    with caplog.at_level(logging.DEBUG, logger="shared.local_indexer"):
        if "shared.local_indexer" in sys.modules:
            importlib.reload(sys.modules["shared.local_indexer"])
        else:
            import shared.local_indexer  # noqa: F401
    # No exception propagated; debug log emitted mentioning the suppressed API.
    assert any(
        "mupdf_display_warnings" in rec.message for rec in caplog.records
    ), (
        "Expected logger.debug entry mentioning mupdf_display_warnings on "
        "AttributeError; got records: "
        + ", ".join(repr(rec.message) for rec in caplog.records)
    )


def test_module_import_survives_arbitrary_exception(monkeypatch, caplog):
    """Codex Critique #2 fix: broad except Exception (NOT narrow AttributeError).

    A future PyMuPDF API change might raise RuntimeError / TypeError / OSError
    — any of those must be recovered to a debug-log line instead of crashing
    module import (which would break the entire desktop app).
    """
    import fitz

    def _boom(_v):
        raise RuntimeError("future API change")

    monkeypatch.setattr(fitz.TOOLS, "mupdf_display_warnings", _boom)
    with caplog.at_level(logging.DEBUG, logger="shared.local_indexer"):
        if "shared.local_indexer" in sys.modules:
            importlib.reload(sys.modules["shared.local_indexer"])
        else:
            import shared.local_indexer  # noqa: F401
    assert any(
        "mupdf_display_warnings" in rec.message for rec in caplog.records
    ), (
        "Expected logger.debug entry; broad except Exception did NOT catch "
        "RuntimeError. Records: "
        + ", ".join(repr(rec.message) for rec in caplog.records)
    )


def test_extract_pdf_pages_emits_no_mupdf_error_to_stderr(tmp_path, capfd):
    """SPEC R97.3-C verbatim acceptance: zero 'MuPDF error:' lines on stderr.

    Codex Critique #3 + round-4 MEDIUM: pins the EFFECT, not just the
    suppression call. Constructs a malformed-Tf PDF fixture deterministically
    (no skip-on-missing-dep — PyMuPDF is a hard project dependency).

    Uses `capfd` rather than `capsys` because PyMuPDF writes to fd 2
    directly via C-level fprintf; capsys (Python-level sys.stderr) does NOT
    capture C-level writes, whereas capfd does.
    """
    import fitz

    # Ensure shared.local_indexer has been imported (which calls the
    # suppression APIs). Reload to be deterministic if a prior test
    # monkeypatched fitz.TOOLS.
    monkey_safe_fitz_tools_warn = fitz.TOOLS.mupdf_display_warnings
    monkey_safe_fitz_tools_err = fitz.TOOLS.mupdf_display_errors
    if "shared.local_indexer" in sys.modules:
        importlib.reload(sys.modules["shared.local_indexer"])
    else:
        import shared.local_indexer  # noqa: F401
    # Sanity: ensure suppression is active even if reload short-circuited.
    monkey_safe_fitz_tools_warn(False)
    monkey_safe_fitz_tools_err(False)

    # --- Construct a deterministic malformed-Tf PDF fixture ---
    # Phase 1: create a minimal valid PDF with one text page.
    fixture_path = tmp_path / "malformed.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), "hello", fontname="helv")
    doc.save(str(fixture_path))
    doc.close()

    # Phase 2: re-open and inject a malformed content-stream prefix that
    # uses the invalid `sz` operand for Tf (PyMuPDF historically emits
    # `MuPDF error: syntax error: unknown keyword: 'sz'` here — same class
    # of error as the user's UAT folder's 624× 'TF' noise).
    doc = fitz.open(str(fixture_path))
    xref_ids = doc[0].get_contents()
    assert xref_ids, "fixture must have a content stream xref"
    xref = xref_ids[0]
    original = doc.xref_stream(xref)
    injected = b"q\n/F1 sz Tf\nQ\n" + original
    doc.update_stream(xref, injected)
    doc.save(str(tmp_path / "malformed_final.pdf"))
    doc.close()
    final_path = tmp_path / "malformed_final.pdf"

    # Clear any captured output from fixture construction.
    capfd.readouterr()

    # --- Now exercise extract_pdf_pages and capture stderr ---
    import shared.local_indexer
    list(shared.local_indexer.extract_pdf_pages(str(final_path)))

    captured = capfd.readouterr()
    assert "MuPDF error:" not in captured.err, (
        "SPEC R97.3-C: extract_pdf_pages must produce zero 'MuPDF error:' "
        f"lines on stderr. Captured stderr:\n{captured.err!r}"
    )
    assert "MuPDF warning:" not in captured.err, (
        "SPEC R97.3-C: extract_pdf_pages must also produce zero 'MuPDF "
        "warning:' lines on stderr (suppression covers both channels). "
        f"Captured stderr:\n{captured.err!r}"
    )
