"""Phase 83 Wave 0 — Content-presence tests for docs reframe (PUBLIC-01/03/05/06).

These tests are RED until Plans 02, 04, and 05 land.

2026-09-08 addition (external MCP-server incident, P1/P2/P3 fix): the tests below
this point lock in the header/changelog/capabilities-endpoint/edge-timeout doc fixes
so they cannot silently rot back to the stale-header state that caused an external
developer to conclude four real, live features did not exist.
"""
import re
from datetime import date, datetime, timedelta
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


# ---------------------------------------------------------------------------
# 2026-09-08 — P1 (stale header) fix.
# ---------------------------------------------------------------------------

# The defect this guards was a header reading 2026-05-05 over a body edited
# through August: the date going BACKWARDS, or standing still while the body
# moved. So that is what is asserted.
#
# It used to assert the literal string "Last updated: 2026-09-08", which pinned
# the fix in place and then blocked it -- the next legitimate bump broke the
# test. That happened for real: a squash merge on 2026-09-09 restamped the
# file's commit date, scripts/check_docs.py (correctly) demanded the header
# move, and moving it reddened this test on both CI platforms. A date literal
# in an assertion about freshness is a contradiction; it can only ever go
# stale, and it takes the build down when someone does the right thing.
_HEADER_FIX_DATE = date(2026, 9, 8)


def test_header_last_updated_is_current():
    content = _read(SEARCH_API_MD)
    match = re.search(r"Last updated:?\s*(\d{4}-\d\d-\d\d)", content)
    assert match, (
        "docs/SEARCH_API.md header carries no parsable 'Last updated: "
        "YYYY-MM-DD' — it is what a reader uses to decide whether the body is "
        "current, and its absence is the staleness this fix addressed (P1)."
    )
    stamped = datetime.strptime(match.group(1), "%Y-%m-%d").date()
    assert stamped >= _HEADER_FIX_DATE, (
        "docs/SEARCH_API.md header date went BACKWARDS to %s, behind the %s "
        "repair. The original defect was a header months behind its own body, "
        "which an integrator read as 'the newer sections are aspirational'."
        % (stamped.isoformat(), _HEADER_FIX_DATE.isoformat())
    )
    # A tomorrow-tolerance, not zero: CI runs in UTC and the owner does not.
    assert stamped <= date.today() + timedelta(days=1), (
        "docs/SEARCH_API.md header date %s is in the future. 'Last updated' "
        "records an edit that happened, not one that is planned."
        % stamped.isoformat()
    )


def test_header_first_line_does_not_carry_stale_version_tag():
    content = _read(SEARCH_API_MD)
    first_line = content.splitlines()[0]
    assert "(v7.10)" not in first_line, (
        f"docs/SEARCH_API.md's first line still carries the stale '(v7.10)' tag "
        f"that made the passage/multi-witness sections read as older-than-the-doc "
        f"and therefore aspirational (P1): {first_line!r}"
    )


# ---------------------------------------------------------------------------
# 2026-09-08 — P2/D3 (capabilities endpoint documented).
# ---------------------------------------------------------------------------

def test_capabilities_endpoint_section_present():
    content = _read(SEARCH_API_MD)
    assert "## Endpoint: GET /api/capabilities" in content


# ---------------------------------------------------------------------------
# 2026-09-08 — D1 (300s ceilings swept to 110s, historical mentions kept).
#
# The naive form of this assertion ("no line mentions both 300 and
# SEARCH_API_FUZZY_TIMEOUT / SEARCH_API_PARALLELS_TIMEOUT") is WRONG for this
# document: it deliberately keeps several historical "(was 300s before
# 2026-09-08)" / "lowered from 300s" mentions so the change is documented, not
# erased. A per-line ban would redden against the correct, intentional doc.
# A ban with no qualifier check at all would be too narrow to prove anything
# (it would only catch a "SEARCH_API_FUZZY_TIMEOUT: 300.0" line, not prose).
#
# The assertion actually chosen: split the file into paragraphs (markdown
# blank-line boundaries — this also makes each markdown TABLE one paragraph,
# so a qualifier anywhere in the same row/table still counts). Any paragraph
# containing a "300 second(s)" mention (`\b300(\.0)?\s*s\b` — matches "300s",
# "300 s", "300.0" + trailing "s", but NOT the unrelated "1300" in the
# date_to example) must ALSO contain a historical/negating qualifier nearby
# ("was", "before", "down from", "lowered", "previously", "not a hardcoded",
# "reduction", "no longer", "used to") — proving every remaining "300" is
# framed as history or an explicit disclaimer, never a currently-true ceiling.
# ---------------------------------------------------------------------------

_THREE_HUNDRED_SECONDS_RE = re.compile(r'\b300(?:\.0)?\s*s\b')
_HISTORICAL_QUALIFIERS = (
    'was ', 'before', 'down from', 'lowered', 'previously',
    'not a hardcoded', 'reduction', 'no longer', 'used to',
)


def test_no_300_second_ceiling_stated_as_current():
    content = _read(SEARCH_API_MD)
    paragraphs = re.split(r'\n\s*\n', content)
    violations = []
    for para in paragraphs:
        if _THREE_HUNDRED_SECONDS_RE.search(para):
            low = para.lower()
            if not any(q in low for q in _HISTORICAL_QUALIFIERS):
                violations.append(para.strip()[:200])
    assert not violations, (
        "docs/SEARCH_API.md states a 300-second ceiling with no historical/"
        f"disclaiming qualifier nearby (looks like a live-ceiling claim): {violations}"
    )


def test_fuzzy_and_parallels_timeout_defaults_are_110_in_env_table():
    content = _read(SEARCH_API_MD)
    assert "`SEARCH_API_FUZZY_TIMEOUT` | `110.0`" in content
    assert "`SEARCH_API_PARALLELS_TIMEOUT` | `110.0`" in content


# ---------------------------------------------------------------------------
# 2026-09-08 — P3 (edge proxy may return a non-JSON 5xx).
# ---------------------------------------------------------------------------

def test_warns_5xx_may_be_non_json():
    content = _read(SEARCH_API_MD)
    assert "may not be JSON" in content or "non-JSON" in content, (
        "docs/SEARCH_API.md no longer warns that a 5xx from the public "
        "deployment's edge proxy can be a non-JSON body (P3)"
    )


# ---------------------------------------------------------------------------
# 2026-09-08 — Changelog entry for the four previously-undocumented features.
# ---------------------------------------------------------------------------

def test_changelog_records_the_four_previously_undocumented_features():
    content = _read(SEARCH_API_MD)
    idx = content.find("## Changelog")
    assert idx != -1, "docs/SEARCH_API.md has no '## Changelog' section"
    changelog = content[idx:]
    for token in ('witnesses', 'method', 'fuzzy', 'sort'):
        assert token in changelog, (
            f"Changelog section does not mention {token!r} — the four features "
            f"an external client concluded (from the stale doc alone) did not "
            f"exist were search_mode='fuzzy', method='passage', witnesses[], "
            f"and sort"
        )
