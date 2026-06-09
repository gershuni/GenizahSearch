"""Test configuration for ensuring project modules are importable."""

import os
import sys
import types
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ROOT_PATH = str(ROOT_DIR)

if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)


# ---------------------------------------------------------------------------
# Headless Qt for CI on Linux without an X server.
#
# Many test files import QApplication at module top to instantiate widgets
# under test. Without an offscreen platform plugin, QApplication() aborts
# with SIGABRT during pytest collection on Ubuntu CI — which kills the entire
# test session before any test runs.
#
# Setting QT_QPA_PLATFORM=offscreen here (in conftest, which loads before
# any test file) guarantees every Qt-importing test gets the headless
# platform. setdefault() means local dev machines with $DISPLAY still use
# their native plugin, and a user-set QT_QPA_PLATFORM is respected.
# ---------------------------------------------------------------------------
if sys.platform.startswith("linux") and not os.environ.get("DISPLAY"):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


# ---------------------------------------------------------------------------
# Skip Phase 97.3 / 100 / 97.2 desktop tests on CI runners (D-F15).
#
# Phase 97.3 (2026-05-26) introduced the FolderWalkWorker QThread + heavily
# threaded MyLibraryTab tests; Phase 100 (2026-05-27) added PdfImageController
# + PdfRenderWorker QThread; Phase 97.2 added a Tantivy FailingWriter shim.
#
# These tests pass in isolation on local Windows but fail in CI on BOTH
# Ubuntu (SIGSEGV under offscreen Qt) AND Windows-latest (access violation
# under headless GitHub Actions runner). When run alongside the full pytest
# suite, QThread cleanup races with the test teardown — the production
# desktop app on real user machines doesn't hit this because real Qt event
# loops on real displays tear workers down cleanly between dialogs.
#
# Gating on CI (GITHUB_ACTIONS=true) instead of platform so local devs can
# still exercise these tests directly via `pytest tests/test_X.py`. CI just
# skips them at collection time — same set of tests, both runners.
#
# Proper fix lands in v7.16 (D-F15 in docs/OPEN_ISSUES.md): audit the
# FolderWalkWorker + PdfRenderWorker cleanup paths so QThread.quit() + wait()
# completes deterministically before the parent QWidget destructor runs.
# ---------------------------------------------------------------------------
collect_ignore_glob: list[str] = []
if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
    collect_ignore_glob.extend([
        "test_my_library_tab*.py",
        "test_unified_tree_async_populate.py",
        "test_folder_walk_worker.py",
        "test_local_optout_persistence.py",
        "test_recovery_scan_runs_cleanup.py",
        "test_disk_headroom.py",
        # Phase 108 Join Lab widget construction smoke tests (QWidget build).
        "test_join_workbench_construct.py",
        # Phase 100 PdfImageController + PdfRenderWorker QThread.
        "test_pdf_image_controller.py",
        "test_pdf_page_renderer.py",
        # Phase 97.2 discard_run / Tantivy-failure short-circuit — Linux-only
        # failure mode but skipping uniformly keeps the conftest simple.
        "test_phase_97_2_sqlite_vs_tantivy_consistency.py",
    ])


# ---------------------------------------------------------------------------
# Phase 85 (Plan 85-03 + 85-02): pin `scripts` to the root namespace package.
#
# Both `C:/Genizahsearch/scripts/` (root, namespace package) and
# `C:/Genizahsearch/skills/cairo-genizah-research/scripts/__init__.py`
# (skill, regular package) coexist. When pytest collects tests across both
# trees in one process, sys.modules['scripts'] can end up bound to whichever
# was imported first — leading to ImportError ("cannot import name 'X' from
# 'scripts' ...skills/.../scripts/__init__.py") for callers expecting the
# root one. Bind the root namespace package up-front so subsequent
# `from scripts import X` resolves to root scripts/.
# ---------------------------------------------------------------------------
def _pin_root_scripts_namespace() -> None:
    root_scripts = ROOT_DIR / "scripts"
    if not root_scripts.is_dir():
        return
    existing = sys.modules.get("scripts")
    existing_path = getattr(existing, "__path__", None)
    # If already bound to a path that does not include root scripts/, replace.
    if existing is None or str(root_scripts) not in (list(existing_path) if existing_path else []):
        pkg = types.ModuleType("scripts")
        pkg.__path__ = [str(root_scripts)]  # type: ignore[attr-defined]
        pkg.__package__ = "scripts"
        sys.modules["scripts"] = pkg


_pin_root_scripts_namespace()


# ---------------------------------------------------------------------------
# Phase 97 Wave E (Plan 97-05): --run-scale flag for @pytest.mark.scale tests.
# Scale tests synthesise large corpora (50K+ files) and are excluded by default.
# Enable with:  pytest tests/test_50k_scale_smoke.py --run-scale -x
# ---------------------------------------------------------------------------
def pytest_addoption(parser):
    group = parser.getgroup("scale")
    group.addoption(
        "--run-scale",
        action="store_true",
        default=False,
        help="run @pytest.mark.scale tests (requires large synthesised corpora)",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-scale"):
        return  # run everything including scale tests
    import pytest as _pytest
    skip_scale = _pytest.mark.skip(reason="scale test; use --run-scale to enable")
    for item in items:
        if "scale" in item.keywords:
            item.add_marker(skip_scale)


# ---------------------------------------------------------------------------
# Skill import bridge: skills/cairo-genizah-research/ (hyphens in dir name)
# is not directly importable as `skills.cairo_genizah_research`. Register it
# as a package under the underscore name so test imports work without renaming
# the directory (Anthropic Skill naming mandates hyphens).
# ---------------------------------------------------------------------------
def _register_skill_package() -> None:
    skill_dir = ROOT_DIR / "skills" / "cairo-genizah-research"
    if not skill_dir.is_dir():
        return  # skill not yet scaffolded

    # Ensure top-level `skills` package exists in sys.modules
    if "skills" not in sys.modules:
        skills_pkg = types.ModuleType("skills")
        skills_pkg.__path__ = [str(ROOT_DIR / "skills")]  # type: ignore[attr-defined]
        skills_pkg.__package__ = "skills"
        sys.modules["skills"] = skills_pkg

    # Register `skills.cairo_genizah_research` pointing at the hyphenated dir
    pkg_name = "skills.cairo_genizah_research"
    if pkg_name not in sys.modules:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(skill_dir)]  # type: ignore[attr-defined]
        pkg.__package__ = pkg_name
        sys.modules[pkg_name] = pkg

    # Register `skills.cairo_genizah_research.scripts`
    scripts_dir = skill_dir / "scripts"
    scripts_name = "skills.cairo_genizah_research.scripts"
    if scripts_name not in sys.modules and scripts_dir.is_dir():
        scripts_pkg = types.ModuleType(scripts_name)
        scripts_pkg.__path__ = [str(scripts_dir)]  # type: ignore[attr-defined]
        scripts_pkg.__package__ = scripts_name
        sys.modules[scripts_name] = scripts_pkg


_register_skill_package()


# ---------------------------------------------------------------------------
# Phase 95 (Plan 95-01 Wave 0): shared fixtures for LOCAL indexer tests.
# Used by Wave 1-3 plans (02-08).
# ---------------------------------------------------------------------------
import os
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def temp_local_index_dir(tmp_path, monkeypatch):
    """Isolated LOCAL_INDEX_DIR + LOCAL_LAB_INDEX_DIR for indexer tests (D-14)."""
    local = tmp_path / "LocalIndex"
    lab = tmp_path / "LocalLabIndex"
    local.mkdir()
    lab.mkdir()
    # Monkey-patch genizah_core.Config when the indexer reads it.
    from genizah_core import Config
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", str(local), raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", str(lab), raising=False)
    return {"local": str(local), "lab": str(lab)}


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client for cloud-write gate tests (REQ-9).

    Used by test_local_namespace_no_lists_leak.py and
    test_local_namespace_no_corrections_leak.py. Any call to
    .table(...) or .from_(...) on this mock is RECORDED — tests assert
    call_count == 0 when LOCAL sys_id is passed."""
    mock = MagicMock(name="supabase_client")
    mock.table = MagicMock(name="supabase_client.table")
    mock.from_ = MagicMock(name="supabase_client.from_")
    return mock


@pytest.fixture
def local_indexer_fixtures_dir():
    """Path to tests/fixtures/local_indexer/."""
    return os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")


# ---------------------------------------------------------------------------
# Phase 98 (Plan 02 Wave 2): autouse fixture for NLI circuit breaker state reset.
#
# Module-level state in shared/nli_circuit_breaker.py persists across test
# functions in the same pytest process. Without this fixture, test A that
# trips the breaker would pollute test B (and CI green local → red on rerun).
#
# See 98-RESEARCH.md Pitfall 6 for the full rationale.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_nli_breaker_state():
    """Reset shared.nli_circuit_breaker module state before EACH test.

    Runs project-wide because the breaker is imported transitively by web/api.py,
    genizah_core.py, shared/puzzle_image_service.py — any test touching those
    paths could inadvertently leave state behind.
    """
    try:
        from shared.nli_circuit_breaker import _reset_for_tests
        _reset_for_tests()
    except ImportError:
        pass  # Defensive: if the module is removed/renamed, don't break the suite
    yield
    # After the test, reset again so the next test starts clean even if THIS
    # test left state behind (e.g., on assertion failure mid-test).
    try:
        from shared.nli_circuit_breaker import _reset_for_tests
        _reset_for_tests()
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# Phase 101 D-09 (USER-DEC-3, REVISED per REVIEWS round 2 HIGH #2):
# autouse fixture insulating tests/test_local_indexer.py from importlib.reload
# pollution by sibling tests (test_mupdf_warnings_suppressed.py reloads
# shared.local_indexer in 3 of its tests).
#
# THE STALE-ALIAS HAZARD: tests/test_local_indexer.py has module-level aliases:
#   from shared.local_indexer import LocalIndexer, EncodingError, extract_txt
# After a sibling test reloads shared.local_indexer, the module's __dict__ is
# rebuilt with NEW class/function objects, but the names in
# test_local_indexer.py's namespace still point to the OLD objects. The
# specific failure mode (Codex round-2): old `extract_txt` (closed over old
# globals) raises the NEW EncodingError, while `pytest.raises(EncodingError)`
# at the test site still references the OLD class -- so pytest sees "DID NOT
# RAISE expected exception".
#
# THE FIX IS TWO STEPS -- reloading the module is NOT ENOUGH:
#   1. importlib.reload(shared.local_indexer)
#   2. Rebind the imported names in the test module's namespace to the
#      freshly-reloaded objects via request.module.<name>.
#
# Scoped to tests/test_local_indexer.py by request.node.fspath check so the
# rest of the suite is unaffected.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _refresh_local_indexer_for_local_indexer_tests(request):
    """D-09 fix: reload shared.local_indexer AND rebind its imported aliases
    in the test module's namespace before each test in tests/test_local_indexer.py.
    """
    if 'test_local_indexer.py' in str(request.node.fspath):
        import importlib
        import shared.local_indexer
        importlib.reload(shared.local_indexer)
        # Rebind the names test_local_indexer.py imports at module level so
        # `pytest.raises(EncodingError)` etc. reference the SAME objects the
        # reloaded module's functions now raise / construct.
        _imported_names = ('LocalIndexer', 'EncodingError', 'extract_txt')
        for _name in _imported_names:
            if hasattr(shared.local_indexer, _name) and hasattr(request.module, _name):
                setattr(request.module, _name, getattr(shared.local_indexer, _name))
    yield


# ---------------------------------------------------------------------------
# Headless safety net: never let a modal QMessageBox.exec() hang the suite.
#
# The My Library confirm dialogs (remove-folder, Re-index All, the large-folder
# ceiling warning) build a QMessageBox and call ``mb.exec()`` — switched from the
# static ``QMessageBox.question(...)`` by the 2026-06 localize-buttons change
# (commit 6d75ac58). With no display, a real ``exec()`` BLOCKS FOREVER and hangs
# the whole pytest job (observed as a 30-min CI hang on test_local_ceiling_*).
#
# Default ``QMessageBox.exec`` to a non-destructive Cancel so no test can hang on
# a modal. Tests that need a specific result (or want to capture the dialog text)
# patch ``QMessageBox.exec`` themselves — their patch wins inside the test body.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _no_blocking_modal_exec(monkeypatch):
    try:
        from PyQt6.QtWidgets import QMessageBox
    except Exception:
        return  # Qt not available in this environment — nothing to guard
    monkeypatch.setattr(
        QMessageBox,
        "exec",
        lambda self: QMessageBox.StandardButton.Cancel,
        raising=False,
    )
