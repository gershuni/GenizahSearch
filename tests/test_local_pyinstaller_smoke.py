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
    Subprocess-invokes ``dist/GenizahSearchPro/GenizahSearchPro.exe --self-test-pymupdf``
    and asserts ``returncode == 0`` AND ``b"PYMUPDF_OK"`` in stdout. This is the
    deployment-time signal — the ONLY tier that catches ``fitz._fitz``
    packaged-binary collection failure that D-43 was designed to surface.

    SKIPS when the EXE is absent (dev environments, web CI) or OLDER than genizah_app.py (a
    stale build does not know the current self-test flags and would start the GUI) -- UNLESS
    ``GENIZAH_PACKAGING_SMOKE=1`` is set, in which case either is a FAILURE. Set the
    flag after ``build_app.bat`` and before a release: from 2026-04 to 2026-09-18 this test
    looked for ``dist/GenizahSearchPro.exe`` (the COLLECT build writes
    ``dist/GenizahSearchPro/GenizahSearchPro.exe``), so it skipped on every machine that had
    just built the app and nobody noticed. The fail-not-skip flag is what makes that visible.

    ``test_packaged_exe_self_test_imports`` runs the EXE with ``--self-test-imports`` and
    expects ``IMPORTS_OK`` (added 2026-09-19 with the first module move; the test skips only if
    the flag ever disappears from genizah_app.py). ``GENIZAH_PACKAGING_EXE=<path>`` points both
    Tier-2 tests at a build made with ``--distpath`` elsewhere, so a scratch build never has to
    overwrite ``dist/``.
"""
import os
import pathlib
import subprocess

import pytest

pytestmark = pytest.mark.packaging

REPO_ROOT = pathlib.Path(__file__).parent.parent
# The COLLECT build (GenizahSearchPro.spec) writes a directory, not a one-file EXE.
# GENIZAH_PACKAGING_EXE points the Tier-2 tests at a build made elsewhere (PyInstaller --distpath), so
# a baseline or trial build can be checked without overwriting the release build in dist/.
EXE_OVERRIDE_ENV = "GENIZAH_PACKAGING_EXE"
EXE_PATH = (
    pathlib.Path(os.environ[EXE_OVERRIDE_ENV])
    if os.environ.get(EXE_OVERRIDE_ENV, "").strip()
    else REPO_ROOT / "dist" / "GenizahSearchPro" / "GenizahSearchPro.exe"
)
FORCE_ENV = "GENIZAH_PACKAGING_SMOKE"


def _forced() -> bool:
    return os.environ.get(FORCE_ENV, "").strip() in ("1", "true", "yes")


def _skip_or_fail(msg: str) -> None:
    if _forced():
        pytest.fail(f"{FORCE_ENV}={os.environ[FORCE_ENV]!r} but " + msg)
    pytest.skip(msg + f" Set {FORCE_ENV}=1 to make this a failure instead of a skip.")


def _require_exe() -> pathlib.Path:
    """The packaged EXE, or skip -- or FAIL when GENIZAH_PACKAGING_SMOKE=1 says it must be there and fresh.

    "Fresh" means newer than genizah_app.py. An EXE built before the current entry-point source does
    not know the current self-test flags, and an unknown flag makes that old EXE start the full GUI
    (measured 2026-09-19: a 60 s timeout with the application window on screen, inside the bounded
    suite). So a stale EXE is never executed: it skips loudly, or fails under the flag, and both
    messages carry the rebuild command.
    """
    if not EXE_PATH.exists():
        _skip_or_fail(
            f"Packaged EXE not built at {EXE_PATH} -- run build_app.bat first "
            "(python -m PyInstaller --noconfirm --clean GenizahSearchPro.spec)."
        )
    app_source = REPO_ROOT / "genizah_app.py"
    if EXE_PATH.stat().st_mtime < app_source.stat().st_mtime:
        _skip_or_fail(
            f"Packaged EXE {EXE_PATH} is older than genizah_app.py, so it predates the current "
            "self-test flags and would start the GUI instead of answering them -- rebuild with "
            "build_app.bat, or point GENIZAH_PACKAGING_EXE at a fresh --distpath build."
        )
    return EXE_PATH


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

    Skips when the EXE is absent unless GENIZAH_PACKAGING_SMOKE=1 (then it fails). A release
    MUST run this against a fresh build with the flag set.
    """
    exe_path = _require_exe()

    try:
        result = subprocess.run(
            [str(exe_path), "--self-test-pymupdf"],
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "HIGH-5: dist/GenizahSearchPro/GenizahSearchPro.exe --self-test-pymupdf timed out "
            "after 30s — likely the CLI flag was not honored before the Qt "
            "event loop started (check the if __name__ == '__main__' block)"
        )

    assert result.returncode == 0, (
        f"HIGH-5: packaged-EXE self-test returned {result.returncode}. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}. "
        "If stderr says the fixture is missing, the EXE predates the 2026-09-18 fix that lets the "
        "self-test find tests/fixtures/ from the current directory: rebuild with build_app.bat."
    )
    assert b"PYMUPDF_OK" in result.stdout, (
        f"HIGH-5: packaged-EXE self-test did not print PYMUPDF_OK marker. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def test_packaged_exe_self_test_imports():
    """Tier 2: the frozen app can import every canonical module by its real dotted path.

    Runs ``GenizahSearchPro.exe --self-test-imports`` and expects ``IMPORTS_OK`` on stdout. The
    flag is added by Stage 2 of the repo-structure plan together with the module moves it
    verifies (an alias stub at an old path is not in the PyInstaller graph once consumers are
    rewritten, so only the frozen process can prove the new paths resolve). Until the flag exists
    in genizah_app.py this test skips with that reason; it never goes red for a missing feature.
    """
    app_source = (REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8", errors="replace")
    if "--self-test-imports" not in app_source:
        pytest.skip(
            "genizah_app.py has no --self-test-imports flag yet (Stage 2 of the repo-structure "
            "plan adds it with the module moves); nothing to run."
        )
    exe_path = _require_exe()
    try:
        result = subprocess.run([str(exe_path), "--self-test-imports"], capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        pytest.fail("GenizahSearchPro.exe --self-test-imports timed out after 60s")
    assert result.returncode == 0, (
        f"--self-test-imports returned {result.returncode}. stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert b"IMPORTS_OK" in result.stdout, (
        f"--self-test-imports did not print IMPORTS_OK. stdout={result.stdout!r} stderr={result.stderr!r}"
    )
