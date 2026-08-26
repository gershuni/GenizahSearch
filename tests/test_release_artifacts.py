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

_TARGET_VERSION = "9.0.0"


def test_version_matches_target():
    content = VERSION_PY.read_text(encoding="utf-8")
    assert f'APP_VERSION = "{_TARGET_VERSION}"' in content


def test_changelog_has_target_section():
    content = CHANGELOG_MD.read_text(encoding="utf-8")
    assert f"## [{_TARGET_VERSION}]" in content


def test_changelog_has_exactly_one_section_per_version():
    """A version must never get a SECOND heading.

    9.0.0 shipped twice -- web on 2026-08-16, desktop on 2026-08-26 -- and the
    obvious move the second time is to add another `## [9.0.0]`. Two headings
    for one number split its notes in half, and a reader who finds the first
    one has no reason to keep looking for the second. The desktop half extends
    the existing section instead; this keeps it that way.
    """
    import re

    content = CHANGELOG_MD.read_text(encoding="utf-8")
    headings = re.findall(r"(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+)\]", content)
    duplicated = sorted({v for v in headings if headings.count(v) > 1})
    assert not duplicated, (
        "these versions have more than one `## [x.y.z]` heading, so their notes "
        "are split across the file: " + ", ".join(duplicated))
    assert headings.count(_TARGET_VERSION) == 1


def test_claude_md_recently_changed_has_target():
    content = CLAUDE_MD.read_text(encoding="utf-8")
    # Match the family prefix (e.g. "v7.11") so minor patches don't need test churn.
    _family = ".".join(_TARGET_VERSION.split(".")[:2])
    assert f"v{_family}" in content
