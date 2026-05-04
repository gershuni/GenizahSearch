"""RED tests for Phase 81B Claude Skill Consumer.

Covers SKILL-04 (honesty annotations), SKILL-05 (known-witness flag/exclude policy),
and SKILL-02 (merge-by-uid staged discovery).

These tests fail at collection time with ModuleNotFoundError until Plan 03 lands
format_output.py, normalize_shelfmark.py, and stage.py in the skill source tree.
That is the intended RED state — Plan 03's acceptance criteria flips them GREEN.

R2 mapping decision locked here: text_source='pgp_transcription' maps to "full"
(no honesty annotation needed). Phase 79 D-10 enum has no 'full' value; the skill
treats 'pgp_transcription' as the "full text available" signal.
"""
import pytest
from tests.conftest_skill import load_fixture
from skills.cairo_genizah_research.scripts.format_output import honesty_annotation, apply_known_witness_policy
from skills.cairo_genizah_research.scripts.normalize_shelfmark import normalize
from skills.cairo_genizah_research.scripts.stage import merge_results


# ---------------------------------------------------------------------------
# SKILL-04: Honesty annotation tests
# ---------------------------------------------------------------------------

def test_honesty_annotation_pgp_full_returns_empty():
    """pgp_transcription text_source = 'full' per R2 mapping — no annotation."""
    browse = load_fixture("browse_pgp_full.json")
    result = honesty_annotation(browse)
    assert result == ""


def test_honesty_annotation_snippet_appends_text_warning():
    """snippet text_source triggers (full text unavailable; based on snippet of N chars)."""
    browse = load_fixture("browse_snippet.json")
    result = honesty_annotation(browse)
    assert "(full text unavailable; based on snippet of" in result


def test_honesty_annotation_includes_char_count():
    """Char count in annotation equals len(browse['text'])."""
    browse = load_fixture("browse_snippet.json")
    result = honesty_annotation(browse)
    n = len(browse["text"])
    assert f"snippet of {n} chars" in result


def test_honesty_annotation_no_image_appends_image_warning():
    """image.url=null and sources=[] triggers (no image available)."""
    browse = load_fixture("browse_no_image.json")
    result = honesty_annotation(browse)
    assert "(no image available)" in result


def test_honesty_annotation_text_source_none_treated_as_not_full():
    """text_source='none' is not 'pgp_transcription' — must get honesty annotation."""
    browse = {"text_source": "none", "text": "", "image": {"url": "/x", "sources": []}}
    result = honesty_annotation(browse)
    assert "(full text unavailable" in result


def test_honesty_annotation_maps_pgp_transcription_as_full_per_R2():
    """Locks R2 decision: pgp_transcription → full; NO 'full text unavailable' annotation.

    Phase 79 D-10 enum is pgp_transcription|snippet|none — no 'full' value.
    Skill maps pgp_transcription → 'full' per planner decision (RESEARCH.md §4, R2).
    """
    browse = load_fixture("browse_pgp_full.json")
    assert browse["text_source"] == "pgp_transcription", "fixture must use pgp_transcription"
    result = honesty_annotation(browse)
    assert "full text unavailable" not in result


# ---------------------------------------------------------------------------
# SKILL-05: Known-witness policy tests
# ---------------------------------------------------------------------------

def test_apply_known_witness_policy_flag_marks_known():
    """policy='flag' adds known_witness=True to matching candidates; others False."""
    candidates = [
        {"uid": "U1", "score": 0.9},
        {"uid": "U2", "score": 0.8},
        {"uid": "U3", "score": 0.7},
    ]
    known_uids = {"U2"}
    result = apply_known_witness_policy(candidates, known_uids, policy="flag")
    assert len(result) == 3
    assert result[1]["known_witness"] is True
    assert result[0]["known_witness"] is False
    assert result[2]["known_witness"] is False


def test_apply_known_witness_policy_exclude_drops_known():
    """policy='exclude' removes candidates whose uid is in known_uids."""
    candidates = [
        {"uid": "U1", "score": 0.9},
        {"uid": "U2", "score": 0.8},
        {"uid": "U3", "score": 0.7},
    ]
    known_uids = {"U2"}
    result = apply_known_witness_policy(candidates, known_uids, policy="exclude")
    assert len(result) == 2
    uids = [c["uid"] for c in result]
    assert "U2" not in uids


def test_apply_known_witness_policy_unknown_raises_valueerror():
    """Unknown policy string raises ValueError."""
    with pytest.raises(ValueError):
        apply_known_witness_policy([], set(), policy="foo")


# ---------------------------------------------------------------------------
# SKILL-05: Shelfmark normalization tests
# ---------------------------------------------------------------------------

def test_normalize_shelfmark_collapses_whitespace():
    """Double spaces and single space collapse to the same normalized form."""
    assert normalize("T-S  12.123") == normalize("T-S 12.123")


def test_normalize_shelfmark_strips_ms_prefix():
    """'MS ' prefix is stripped before comparison."""
    assert normalize("MS T-S 12.123") == normalize("T-S 12.123")


def test_normalize_shelfmark_idempotent():
    """Applying normalize twice yields the same result as applying it once."""
    for s in ["T-S 12.123", "ENA-MS 1234", "MS Heb c 57"]:
        assert normalize(normalize(s)) == normalize(s)


# ---------------------------------------------------------------------------
# SKILL-02: merge-by-uid staged discovery tests
# ---------------------------------------------------------------------------

def _make_result(uid: str, score: float = 0.5) -> dict:
    return {"uid": uid, "score": score, "snippet": f"snippet for {uid}"}


def test_merge_by_uid_aggregates_phrase_count():
    """uid appearing in 2 of 3 phrase lists gets _phrase_count=2; single-list uid gets 1."""
    phrase_results = [
        [_make_result("U1", 0.9), _make_result("U2", 0.7)],
        [_make_result("U1", 0.8)],
        [_make_result("U3", 0.6)],
    ]
    merged = merge_results(phrase_results)
    by_uid = {c["uid"]: c for c in merged}
    assert by_uid["U1"]["_phrase_count"] == 2
    assert by_uid["U2"]["_phrase_count"] == 1


def test_merge_by_uid_assigns_tier_a_for_3plus_phrases():
    """3+ phrase appearances → tier A; 2 → B; 1 → C."""
    phrase_results = [
        [_make_result("U1", 0.9)],
        [_make_result("U1", 0.8)],
        [_make_result("U1", 0.7)],
        [_make_result("U2", 0.6)],
        [_make_result("U2", 0.5)],
        [_make_result("U3", 0.4)],
    ]
    merged = merge_results(phrase_results)
    by_uid = {c["uid"]: c for c in merged}
    assert by_uid["U1"]["_tier"] == "A"
    assert by_uid["U2"]["_tier"] == "B"
    assert by_uid["U3"]["_tier"] == "C"


def test_merge_by_uid_sorts_by_phrase_count_desc_then_score():
    """Results sorted by descending _phrase_count; ties broken by descending score."""
    phrase_results = [
        [_make_result("U_low", 0.9), _make_result("U_high", 0.5)],
        [_make_result("U_high", 0.6)],
    ]
    merged = merge_results(phrase_results)
    assert merged[0]["uid"] == "U_high", "U_high appeared in 2 phrases; should rank first"
    assert merged[1]["uid"] == "U_low"
