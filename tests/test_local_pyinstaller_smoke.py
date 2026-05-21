# -*- coding: utf-8 -*-
"""Phase 95 D-43 — PyInstaller packaging smoke for PyMuPDF.

Gated @pytest.mark.packaging. Runs in release CI only — NOT default
``pytest tests/`` runs (the marker is not in the default ``-m`` filter).

Without GenizahSearchPro.spec's ``collect_all('pymupdf')`` call, the packaged
EXE raises ``ModuleNotFoundError: fitz._fitz`` at runtime. This test imports
fitz and runs a Hebrew PDF extraction to prove the dependency is correctly
bundled.

TWO TIERS (HIGH-5 review fix):

Tier 1 (always-on, no EXE required):
    Imports fitz in the current venv and runs the same Hebrew extraction call
    as ``test_pymupdf_hebrew_extraction_quality`` in ``test_local_indexer.py``.
    This is the development-time signal — catches requirements.txt regressions.

Tier 2 (release-gated, EXE required):
    Subprocess-invokes ``dist/GenizahSearchPro.exe --self-test-pymupdf`` and
    asserts ``returncode == 0`` AND ``b"PYMUPDF_OK"`` in stdout. This is the
    deployment-time signal — the ONLY tier that catches ``fitz._fitz``
    packaged-binary collection failure that D-43 was designed to surface.
    Gracefully SKIPs when the EXE is absent (dev environments, web CI).
"""
import pathlib
import subprocess

import pytest

pytestmark = pytest.mark.packaging


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def hebrew_pdf_fixture():
    path = (
        pathlib.Path(__file__).parent
        / "fixtures"
        / "local_indexer"
        / "hebrew_sample.pdf"
    )
    if not path.exists():
        pytest.skip(
            "D-44 Hebrew PDF fixture not available "
            "(tests/fixtures/local_indexer/hebrew_sample.pdf missing)"
        )
    return str(path)


# ---------------------------------------------------------------------------
# Tier 1 — venv-side fitz import + extraction smoke
# ---------------------------------------------------------------------------

def test_fitz_importable():
    """fitz (PyMuPDF) must be importable — confirms requirements.txt pin."""
    import fitz  # noqa: PLC0415

    assert fitz.VersionBind, "fitz imported but VersionBind missing"
    # D-43 contract: >= 1.24
    parts = fitz.VersionBind.split(".")
    major, minor = int(parts[0]), int(parts[1])
    assert major > 1 or (major == 1 and minor >= 24), (
        f"PyMuPDF version {fitz.VersionBind} is below the >=1.24 contract (D-43)"
    )


def test_packaged_exe_extracts_hebrew_pdf(hebrew_pdf_fixture):
    """Open Hebrew PDF via fitz.get_text('blocks'); assert Hebrew text returned.

    Tier 1: exercises fitz in the current venv (not the packaged EXE).
    Same extraction call as test_pymupdf_hebrew_extraction_quality in
    test_local_indexer.py; gated @pytest.mark.packaging so it runs in
    release CI to pin packaging regressions specifically.
    """
    import fitz  # noqa: PLC0415

    doc = fitz.open(hebrew_pdf_fixture)
    try:
        assert doc.page_count >= 1, "Hebrew PDF fixture has no pages"
        page = doc[0]
        blocks = page.get_text("blocks")
        text_parts = [
            b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()
        ]
        text = "\n\n".join(text_parts)
        assert text, (
            "PyMuPDF returned empty text from Hebrew PDF — "
            "packaging or extraction broken"
        )
        # Sanity: at least some Hebrew codepoints must be present.
        hebrew_chars = sum(1 for ch in text if "֐" <= ch <= "׿")
        assert hebrew_chars > 0, (
            "Extracted text has zero Hebrew characters — extraction broken"
        )
    finally:
        doc.close()


def test_spec_file_collects_pymupdf():
    """Affirmative check: GenizahSearchPro.spec calls collect_all('pymupdf').

    Tier 1: static contract — if someone removes the collect_all call the
    packaged EXE will fail with fitz._fitz import error at runtime (D-43).
    """
    spec_path = pathlib.Path(__file__).parent.parent / "GenizahSearchPro.spec"
    if not spec_path.exists():
        pytest.skip("GenizahSearchPro.spec not in this environment")
    content = spec_path.read_text(encoding="utf-8")
    assert (
        "collect_all('pymupdf')" in content
        or 'collect_all("pymupdf")' in content
    ), "GenizahSearchPro.spec missing collect_all('pymupdf') call — D-43 regression"


# ---------------------------------------------------------------------------
# Tier 2 — packaged-EXE subprocess smoke (HIGH-5 review fix)
# ---------------------------------------------------------------------------

def test_packaged_exe_self_test_pymupdf_subprocess():
    """HIGH-5 review fix — Tier 2: subprocess-invoke the packaged EXE.

    Runs ``dist/GenizahSearchPro.exe --self-test-pymupdf`` and asserts:
      (a) returncode == 0
      (b) b"PYMUPDF_OK" in stdout

    This is the ONLY test that catches ``fitz._fitz`` packaged-binary
    collection failure — the Tier 1 venv tests pass even when PyInstaller
    fails to bundle the C-extension binary (because Tier 1 uses the venv
    fitz, not the bundled one). D-43 was designed specifically to surface
    this failure mode.

    SKIPS gracefully when ``dist/GenizahSearchPro.exe`` is absent (dev
    environments and web CI). Release CI MUST have the EXE built and this
    test MUST pass before a release is tagged.
    """
    repo_root = pathlib.Path(__file__).parent.parent
    exe_path = repo_root / "dist" / "GenizahSearchPro.exe"
    if not exe_path.exists():
        pytest.skip(
            f"Packaged EXE not built at {exe_path} — "
            "Tier 2 smoke is release-CI only. "
            "Build with PyInstaller first: pyinstaller GenizahSearchPro.spec"
        )

    try:
        result = subprocess.run(
            [str(exe_path), "--self-test-pymupdf"],
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "HIGH-5: dist/GenizahSearchPro.exe --self-test-pymupdf timed out "
            "after 30s — likely the CLI flag was not honored before the Qt "
            "event loop started (check the if __name__ == '__main__' block)"
        )

    assert result.returncode == 0, (
        f"HIGH-5: packaged-EXE self-test returned {result.returncode}. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert b"PYMUPDF_OK" in result.stdout, (
        f"HIGH-5: packaged-EXE self-test did not print PYMUPDF_OK marker. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
