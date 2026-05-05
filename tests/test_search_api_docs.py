"""Phase 83 Wave 0 — Content-presence tests for docs reframe (PUBLIC-01/03/05/06).

These tests are RED until Plans 02, 04, and 05 land.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SEARCH_API_MD = REPO_ROOT / "docs" / "SEARCH_API.md"
README_MD = REPO_ROOT / "README.md"
SKILL_MD = REPO_ROOT / "skills" / "cairo-genizah-research" / "SKILL.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_stability_statement_present():
    content = _read(SEARCH_API_MD)
    assert "We aim to keep this contract stable" in content, (
        "Stability statement (D-02) not found in docs/SEARCH_API.md"
    )


def test_no_internal_helper_banner():
    content = _read(SEARCH_API_MD)
    assert "Internal Helper" not in content, (
        "Old 'Internal Helper - No Stability Promise' banner still present in docs/SEARCH_API.md; "
        "must be removed per D-06"
    )


def test_quick_start_section_present():
    content = _read(SEARCH_API_MD)
    assert "## Quick Start" in content


def test_attribution_section_present():
    content = _read(SEARCH_API_MD)
    assert "## Attribution" in content


def test_changelog_section_present():
    content = _read(SEARCH_API_MD)
    assert "## Changelog" in content


def test_readme_has_api_section():
    content = _read(README_MD)
    assert "## API" in content


def test_readme_api_links_to_search_api_md():
    content = _read(README_MD)
    idx = content.find("## API")
    assert idx != -1
    after = content[idx:]
    assert "docs/SEARCH_API.md" in after


def test_skill_md_references_public_docs():
    content = _read(SKILL_MD)
    assert "docs/SEARCH_API.md" in content
