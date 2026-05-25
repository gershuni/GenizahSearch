"""Test configuration for ensuring project modules are importable."""

import sys
import types
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ROOT_PATH = str(ROOT_DIR)

if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)


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
