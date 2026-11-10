"""Live smoke test for the cairo-genizah-research skill.

Skipped by default. To run: SKILL_SMOKE=1 pytest tests/test_skill_smoke.py -v
Hits the production deployment (or override via GENIZAH_API_BASE).
"""
import os
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SKILL_SMOKE") != "1",
    reason="Live smoke test — set SKILL_SMOKE=1 to run",
)


def test_smoke_search_endpoint_returns_envelope():
    """Hit /api/search?search_mode=exact&query=test against live deployment."""
    from skills.cairo_genizah_research.scripts.search import call_search
    result = call_search(query="ויאמר", search_mode="exact", limit=5)
    assert result.get("schema_version") == 1
    assert result.get("source") == "search"
    assert "results" in result
    assert isinstance(result["results"], list)


def test_smoke_browse_endpoint_round_trips_locator():
    """Search → browse round-trip on a single result."""
    from skills.cairo_genizah_research.scripts.search import call_search
    from skills.cairo_genizah_research.scripts.browse import call_browse
    s = call_search(query="ויאמר", search_mode="exact", limit=1)
    if not s["results"]:
        pytest.skip("Live search returned 0 results — try a different query")
    first = s["results"][0]
    b = call_browse(uid=first["uid"])
    assert b["locator"]["sys_id"] == first["locator"]["sys_id"]
    assert "text_source" in b
    assert b["text_source"] in {"pgp_transcription", "snippet", "none"}
