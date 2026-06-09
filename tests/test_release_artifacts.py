"""Release artifact content tests.

Bumped at each release to assert version.py, CHANGELOG, and CLAUDE.md stay in sync.
Originally a Phase 83 Wave 0 gate (PUBLIC-08) for v7.10.0; reused at each subsequent
release.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
VERSION_PY = REPO_ROOT / "version.py"
CHANGELOG_MD = REPO_ROOT / "CHANGELOG.md"
CLAUDE_MD = REPO_ROOT / "CLAUDE.md"

_TARGET_VERSION = "8.0.0"


def test_version_matches_target():
    content = VERSION_PY.read_text(encoding="utf-8")
    assert f'APP_VERSION = "{_TARGET_VERSION}"' in content


def test_changelog_has_target_section():
    content = CHANGELOG_MD.read_text(encoding="utf-8")
    assert f"## [{_TARGET_VERSION}]" in content


def test_claude_md_recently_changed_has_target():
    content = CLAUDE_MD.read_text(encoding="utf-8")
    # Match the family prefix (e.g. "v7.11") so minor patches don't need test churn.
    _family = ".".join(_TARGET_VERSION.split(".")[:2])
    assert f"v{_family}" in content
