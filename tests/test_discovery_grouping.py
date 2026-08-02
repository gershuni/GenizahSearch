# -*- coding: utf-8 -*-
"""Behavior + regression-fixture test suite for `shared/discovery_grouping.py`
(Phase 136, plan 136-07, Task 2: `collapse_canonical` / `lead_attribution` /
`separate_granularity`).

Masking discipline (matches `tests/test_discovery_ids.py`): every
page_id/evidence_id value below is a synthetic fixture placeholder. The ONE
exception, per this plan's own instruction, is the "two-Rashi-titles"
regression fixture -- it deliberately reuses the REAL sys_id/work_id/
canonical_work_id/title/span values recorded in `136-GATE1-EVIDENCE.md`
because that worked example is the whole reason `separate_granularity`
exists; these are already-published, masking-safe catalogue/work metadata
(titles + authors), never restricted-corpus reference text.
"""

from __future__ import annotations

import pathlib
import random

from shared.discovery_grouping import (
    GENERIC_SHARED_TEXT,
    SAME_WORK_GRANULARITY,
    UNDECIDABLE,
    collapse_canonical,
    lead_attribution,
    normalize_title,
    separate_granularity,
    titles_share_prefix,
    works_related_by_title,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "shared" / "discovery_grouping.py"


# ---------------------------------------------------------------------------
# D-13a: collapse_canonical.
# ---------------------------------------------------------------------------

def test_collapse_same_canonical_work_id_uses_canonical_title():
    """Two claims recording the same canonical_work_id collapse to ONE
    identification, and the canonical work's own title is the one
    displayed."""
    rows = [
        {"work_id": "w000190", "canonical_work_id": "w001382", "title": "M-source title"},
        {"work_id": "w001382", "canonical_work_id": "w001382", "title": "Sefaria title (canonical)"},
    ]
    collapsed = collapse_canonical(rows)
    assert len(collapsed) == 1
    assert collapsed[0]["work_id"] == "w001382"
    assert collapsed[0]["title"] == "Sefaria title (canonical)"


def test_collapse_no_anchor_present_picks_lexicographically_smallest_work_id():
    """When no member IS the canonical anchor (a corpus corner case), the
    lexicographically smallest work_id wins, deterministically."""
    rows = [
        {"work_id": "w000500", "canonical_work_id": "w000999", "title": "B"},
        {"work_id": "w000200", "canonical_work_id": "w000999", "title": "A"},
    ]
    collapsed = collapse_canonical(rows)
    assert len(collapsed) == 1
    assert collapsed[0]["work_id"] == "w000200"
    assert collapsed[0]["title"] == "A"


def test_collapse_changes_derived_count():
    """A collapse changes every derived count: pre-collapse and
    post-collapse counts differ on a fixture built at the observed shape
    (D-13a: 921 row-pairs corpus-wide collapse from 2 rows to 1)."""
    rows = [
        {"work_id": "w000190", "canonical_work_id": "w001382", "title": "M-source title"},
        {"work_id": "w001382", "canonical_work_id": "w001382", "title": "Sefaria title (canonical)"},
        {"work_id": "w002000", "canonical_work_id": "w002000", "title": "Unrelated, no duplicate"},
    ]
    pre_collapse_count = len(rows)
    post_collapse_count = len(collapse_canonical(rows))
    assert pre_collapse_count == 3
    assert post_collapse_count == 2
    assert post_collapse_count != pre_collapse_count


# ---------------------------------------------------------------------------
# D-13b: lead_attribution.
# ---------------------------------------------------------------------------

def _evidence_row(evidence_id, evidence_source, confidence_band, **extra):
    row = {
        "evidence_id": evidence_id,
        "evidence_source": evidence_source,
        "confidence_band": confidence_band,
    }
    row.update(extra)
    return row


def test_lead_attribution_deterministic_and_ties_broken_by_evidence_id():
    """Given several claims on byte-identical offsets, lead attribution is
    deterministic, and ties beyond band rank are broken by the ratified
    secondary key -- ascending lexicographic evidence_id (D-13b: "band rank
    alone leaves 1,542 of 1,553 identical-span groups (99.3%) still
    tied")."""
    # All three rows share the SAME band (tier_a) -- a tie band rank cannot
    # break, so the evidence_id lexicographic order must decide.
    row_b = _evidence_row("bbb_evidence", "track1_direct", "tier_a")
    row_a = _evidence_row("aaa_evidence", "track1_direct", "tier_a")
    row_c = _evidence_row("ccc_evidence", "track1_direct", "tier_a")
    group = [row_b, row_a, row_c]

    lead, remainder = lead_attribution(group)
    assert lead["evidence_id"] == "aaa_evidence"
    assert [r["evidence_id"] for r in remainder] == ["bbb_evidence", "ccc_evidence"]


def test_lead_attribution_band_rank_wins_over_evidence_id():
    """A stronger band always wins the lead, regardless of evidence_id
    ordering."""
    strong = _evidence_row("zzz_evidence", "track1_direct", "expert_verified")
    weak = _evidence_row("aaa_evidence", "propagated", "weak")
    lead, remainder = lead_attribution([weak, strong])
    assert lead["evidence_id"] == "zzz_evidence"
    assert remainder == [weak]


def test_lead_attribution_is_shuffle_invariant():
    """Determinism: shuffling the input yields an IDENTICAL lead and an
    IDENTICAL remainder ordering."""
    rows = [
        _evidence_row("m_evidence", "track1_direct", "tier_a"),
        _evidence_row("a_evidence", "track1_direct", "tier_a"),
        _evidence_row("z_evidence", "track1_direct", "tier_a"),
        _evidence_row("q_evidence", "track1_direct", "tier_a"),
    ]
    baseline_lead, baseline_remainder = lead_attribution(rows)

    shuffled = list(rows)
    random.Random(42).shuffle(shuffled)
    assert shuffled != rows  # sanity: the shuffle actually reordered something

    shuffled_lead, shuffled_remainder = lead_attribution(shuffled)
    assert shuffled_lead == baseline_lead
    assert shuffled_remainder == baseline_remainder


# ---------------------------------------------------------------------------
# D-13d: separate_granularity + works_related_by_title.
# ---------------------------------------------------------------------------

def test_same_work_granularity_collapses_and_stays_identification():
    """An identical-span group whose works are the SAME work at different
    granularities (same author, shared title prefix) collapses like a
    duplicate and stays an identification."""
    work_a = {
        "canonical_work_id": "w100",
        "author": "Author X",
        "neutral_title": "Author X on the whole Torah",
    }
    work_b = {
        "canonical_work_id": "w200",
        "author": "Author X",
        "neutral_title": "Author X on Genesis",
    }
    assert separate_granularity([work_a, work_b]) == SAME_WORK_GRANULARITY


def test_generic_shared_text_leaves_identifications_bucket():
    """An identical-span group whose works are genuinely different is
    classified as generic shared text and leaves the identifications
    bucket (D-13d's original prayer-book/Tur Orach Chaim/Yalkut Shimoni
    example -- no shared author, no shared title family)."""
    work_a = {
        "canonical_work_id": "w300",
        "author": "Author A",
        "neutral_title": "Some Prayer Book",
    }
    work_b = {
        "canonical_work_id": "w400",
        "author": "Author B",
        "neutral_title": "Tur Orach Chaim",
    }
    work_c = {
        "canonical_work_id": "w500",
        "author": None,
        "neutral_title": "Yalkut Shimoni on Nevi'im",
    }
    assert separate_granularity([work_a, work_b, work_c]) == GENERIC_SHARED_TEXT


def test_undecidable_group_maps_conservatively_to_generic():
    """A group the predicate cannot decide (fewer than 2 distinct canonical
    works -- this predicate's own precondition is unmet) is classified
    UNDECIDABLE; the CALLER must map that conservatively to the SAME
    disposition as GENERIC_SHARED_TEXT, never silently promoted to a
    collapse."""
    degenerate_group = [
        {"canonical_work_id": "w600", "author": "Author Z", "neutral_title": "Only one work here"},
    ]
    result = separate_granularity(degenerate_group)
    assert result == UNDECIDABLE

    # Caller-side conservative mapping (mirrors what the panel/bake must do):
    # UNDECIDABLE is treated identically to GENERIC_SHARED_TEXT -- never
    # silently promoted to a same-work collapse.
    def _caller_bucket(granularity_result):
        if granularity_result == SAME_WORK_GRANULARITY:
            return "collapse_and_keep_as_identifications"
        return "generic_shared_text_bucket"  # GENERIC_SHARED_TEXT and UNDECIDABLE alike

    assert _caller_bucket(result) == _caller_bucket(GENERIC_SHARED_TEXT)
    assert _caller_bucket(result) != "collapse_and_keep_as_identifications"


def test_works_related_by_title_requires_non_null_matching_author():
    """works_related_by_title returns False whenever either author is
    missing or the two authors differ -- author-gating is load-bearing
    (D-13d: "it is what stops the rule over-collapsing large generic-title
    clusters such as 'Responsa of the Geonim'")."""
    shared_title_a = {"canonical_work_id": "w1", "author": None, "neutral_title": "Responsa of the Geonim"}
    shared_title_b = {"canonical_work_id": "w2", "author": None, "neutral_title": "Responsa of the Geonim"}
    assert works_related_by_title(shared_title_a, shared_title_b) is False

    different_authors_a = {"canonical_work_id": "w3", "author": "Author One", "neutral_title": "Shared Prefix Text A"}
    different_authors_b = {"canonical_work_id": "w4", "author": "Author Two", "neutral_title": "Shared Prefix Text B"}
    assert works_related_by_title(different_authors_a, different_authors_b) is False


def test_normalize_title_and_titles_share_prefix_helpers():
    assert normalize_title(None) == ""
    # Quote/geresh/gershayim marks are stripped so both spellings of the
    # same title normalize identically.
    assert normalize_title('רש"י על התורה') == normalize_title('רש׳י על התורה')
    assert titles_share_prefix("abcdxyz", "abcdqrs", min_len=4) is True
    assert titles_share_prefix("abcd", "abce", min_len=4) is False
    assert titles_share_prefix("ab", "abcd", min_len=4) is False


# ---------------------------------------------------------------------------
# The named worked-case regression fixture (136-GATE1-EVIDENCE.md, "D-13d --
# the granularity separation rule (KNOWN FLAW)"): T-S Misc. 12.31.14,
# sys_id 990051079570205171, span 0-962. Real work ids/titles/author values
# -- already-published catalogue metadata, masking-safe.
# ---------------------------------------------------------------------------

_WORKED_CASE_SYS_ID = "990051079570205171"
_WORKED_CASE_SPAN = (0, 962)
_WORKED_CASE_WORK_A = {
    "work_id": "w000171",
    "canonical_work_id": "w000171",
    "author": 'שלמה בן יצחק (רש״י)',
    "neutral_title": 'רש"י על התורה',
}
_WORKED_CASE_WORK_B = {
    "work_id": "w001281",
    "canonical_work_id": "w001281",
    "author": 'שלמה בן יצחק (רש״י)',
    "neutral_title": 'רש"י על בראשית',
}


def test_worked_two_rashi_titles_regression_fixture():
    """T-S Misc. 12.31.14 (sys_id 990051079570205171), span 0-962: w000171
    (רש"י על התורה) and w001281 (רש"י על בראשית), both authored by שלמה בן
    יצחק (רש"י) -- the same underlying commentary at two catalogued
    granularities, carrying DIFFERENT canonical_work_ids. This is the
    ⚠ KNOWN FLAW's worked example: the whole reason separate_granularity
    exists is to classify this pair as same-work-at-two-granularities
    (never generic), so this manuscript renders TWO identifications
    (Rashi on the Torah AND Rashi on Genesis) rather than being pulled
    into "also shares text with" as one indistinguishable generic group."""
    group = [_WORKED_CASE_WORK_A, _WORKED_CASE_WORK_B]

    # Sanity: this really is an identical-span group over >=2 DIFFERENT
    # canonical works, per this fixture's own citation.
    assert _WORKED_CASE_WORK_A["canonical_work_id"] != _WORKED_CASE_WORK_B["canonical_work_id"]
    assert _WORKED_CASE_SPAN == (0, 962)
    assert _WORKED_CASE_SYS_ID == "990051079570205171"

    result = separate_granularity(group)
    assert result == SAME_WORK_GRANULARITY

    # Caller-side: SAME_WORK_GRANULARITY means both works stay identifications
    # -- neither is pulled into the generic "also shares text with" bucket.
    surviving_identifications = (
        list(group) if result == SAME_WORK_GRANULARITY else []
    )
    assert len(surviving_identifications) == 2


# ---------------------------------------------------------------------------
# Acceptance criteria: no `density`, no `web/` import.
# ---------------------------------------------------------------------------

def test_module_never_reads_density():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "density" not in source


def test_module_never_imports_web():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "import web" not in source
    assert "from web" not in source
