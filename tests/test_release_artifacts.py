"""Phase 83 Wave 0 — Release artifact content tests (PUBLIC-08).

These tests are RED until Plan 05 (version bump + CHANGELOG) lands.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
VERSION_PY = REPO_ROOT / "version.py"
CHANGELOG_MD = REPO_ROOT / "CHANGELOG.md"
CLAUDE_MD = REPO_ROOT / "CLAUDE.md"

_TARGET_VERSION = "7.10.0"


def test_version_is_7_10_0():
    content = VERSION_PY.read_text(encoding="utf-8")
    assert f'APP_VERSION = "{_TARGET_VERSION}"' in content


def test_changelog_has_7_10_0_section():
    content = CHANGELOG_MD.read_text(encoding="utf-8")
    assert "## [7.10.0]" in content


def test_claude_md_recently_changed_has_v7_10():
    content = CLAUDE_MD.read_text(encoding="utf-8")
    assert "v7.10" in content
