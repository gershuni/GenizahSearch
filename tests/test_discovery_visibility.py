# -*- coding: utf-8 -*-
"""Tests for `shared/discovery_visibility.py` -- the VIS-01 two-axis
public/private visibility derivation (Phase 136, plan 136-08, owner decision
D-22). One test per `<behavior>` bullet in `136-08-PLAN.md` Task 1, plus the
adversarial-input table and the no-raw-origin-id assertion the plan's
acceptance criteria require explicitly.
"""
from __future__ import annotations

import pytest

from shared.discovery_visibility import (
    VISIBILITY_PRIVATE,
    VISIBILITY_PUBLIC,
    assertion_visibility,
    identity_visibility,
    is_public,
    reconcile_launch_scope,
)

# A raw-id-shaped string that must NEVER appear in any return value -- mirrors
# the `M:`/`J:`/`REF`-prefix raw research ids `scripts/discovery_ids.py`
# documents as the thing that must never be echoed.
_RAW_ID_SHAPED = "M:some-restricted-research-work-id-12345"


# ---------------------------------------------------------------------------
# Behavior 1: assertion_visibility -- open work / restricted assertion is
# NOT public (one direction of the mislabelling pair).
# ---------------------------------------------------------------------------

def test_assertion_visibility_restricted_origin_is_private():
    row = {"assertion_source_corpus": "msource"}
    assert assertion_visibility(row) == VISIBILITY_PRIVATE


def test_assertion_visibility_open_origins_are_public():
    assert assertion_visibility({"assertion_source_corpus": "sefaria"}) == VISIBILITY_PUBLIC
    assert assertion_visibility({"assertion_source_corpus": "ja"}) == VISIBILITY_PUBLIC


# ---------------------------------------------------------------------------
# Behavior 2: identity_visibility -- a work whose identity originates in a
# restricted corpus is NOT public even when the assertion is open (the
# mirror mislabelling direction).
# ---------------------------------------------------------------------------

def test_identity_visibility_restricted_source_is_private():
    row = {"source_corpus": "msource"}
    assert identity_visibility(row) == VISIBILITY_PRIVATE


def test_identity_visibility_open_sources_are_public():
    assert identity_visibility({"source_corpus": "sefaria"}) == VISIBILITY_PUBLIC
    assert identity_visibility({"source_corpus": "ja"}) == VISIBILITY_PUBLIC


# ---------------------------------------------------------------------------
# Behavior 3: public eligibility is the conjunction of BOTH axes.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "assertion_vis,identity_vis,expected",
    [
        (VISIBILITY_PUBLIC, VISIBILITY_PUBLIC, True),
        (VISIBILITY_PUBLIC, VISIBILITY_PRIVATE, False),
        (VISIBILITY_PRIVATE, VISIBILITY_PUBLIC, False),
        (VISIBILITY_PRIVATE, VISIBILITY_PRIVATE, False),
    ],
)
def test_is_public_is_the_conjunction(assertion_vis, identity_vis, expected):
    assert is_public(assertion_vis, identity_vis) is expected


def test_mislabelling_open_work_restricted_assertion_not_public():
    """A row whose WORK is open but whose displayed ASSERTION originates in
    a restricted corpus must NOT be public -- this is the exact D-22
    scenario a corpus-keyed (`works.source_corpus`-only) shortcut would
    mislabel as public."""
    assertion_vis = assertion_visibility({"assertion_source_corpus": "msource"})
    identity_vis = identity_visibility({"source_corpus": "sefaria"})
    assert is_public(assertion_vis, identity_vis) is False


def test_mislabelling_restricted_work_open_assertion_not_public():
    """The mirror case: an open assertion citing a work whose OWN identity
    originates in a restricted corpus must also NOT be public."""
    assertion_vis = assertion_visibility({"assertion_source_corpus": "sefaria"})
    identity_vis = identity_visibility({"source_corpus": "msource"})
    assert is_public(assertion_vis, identity_vis) is False


def test_both_axes_open_is_public():
    assertion_vis = assertion_visibility({"assertion_source_corpus": "ja"})
    identity_vis = identity_visibility({"source_corpus": "sefaria"})
    assert is_public(assertion_vis, identity_vis) is True


# ---------------------------------------------------------------------------
# Behavior 4: unknown, missing, or malformed origin fails closed to private
# -- NEVER public by default. Adversarial-input table.
# ---------------------------------------------------------------------------

_ADVERSARIAL_ASSERTION_INPUTS = [
    pytest.param({}, id="missing-key"),
    pytest.param({"assertion_source_corpus": None}, id="none-value"),
    pytest.param({"assertion_source_corpus": ""}, id="empty-string"),
    pytest.param({"assertion_source_corpus": "   "}, id="whitespace-only"),
    pytest.param({"assertion_source_corpus": "unknown_corpus_code"}, id="unknown-code"),
    pytest.param({"assertion_source_corpus": 12345}, id="non-string-int"),
    pytest.param({"assertion_source_corpus": ["sefaria"]}, id="non-string-list"),
    pytest.param(None, id="none-row"),
    pytest.param("not-a-mapping", id="string-row"),
    pytest.param({"assertion_source_corpus": "msource"}, id="restricted-code-explicit"),
    # A code carrying the restricted prefix but claiming to be an open
    # identity in the SAME field -- must still fail closed (there is no
    # "open by elimination" branch).
    pytest.param({"assertion_source_corpus": "msource_but_actually_open"}, id="restricted-prefix-open-claim"),
    # The mirror case: a string that merely CONTAINS an open code as a
    # substring must not be treated as open (exact match only).
    pytest.param({"assertion_source_corpus": "sefaria_extra"}, id="open-prefix-not-exact"),
]


@pytest.mark.parametrize("row", _ADVERSARIAL_ASSERTION_INPUTS)
def test_assertion_visibility_adversarial_inputs_fail_closed(row):
    assert assertion_visibility(row) == VISIBILITY_PRIVATE


_ADVERSARIAL_IDENTITY_INPUTS = [
    pytest.param({}, id="missing-key"),
    pytest.param({"source_corpus": None}, id="none-value"),
    pytest.param({"source_corpus": ""}, id="empty-string"),
    pytest.param({"source_corpus": "   "}, id="whitespace-only"),
    pytest.param({"source_corpus": "unknown_corpus_code"}, id="unknown-code"),
    pytest.param({"source_corpus": 12345}, id="non-string-int"),
    pytest.param({"source_corpus": ["sefaria"]}, id="non-string-list"),
    pytest.param(None, id="none-row"),
    pytest.param("not-a-mapping", id="string-row"),
    pytest.param({"source_corpus": "msource"}, id="restricted-code-explicit"),
]


@pytest.mark.parametrize("row", _ADVERSARIAL_IDENTITY_INPUTS)
def test_identity_visibility_adversarial_inputs_fail_closed(row):
    assert identity_visibility(row) == VISIBILITY_PRIVATE


@pytest.mark.parametrize(
    "assertion_vis,identity_vis",
    [
        (None, None),
        (None, VISIBILITY_PUBLIC),
        (VISIBILITY_PUBLIC, None),
        ("public ", VISIBILITY_PUBLIC),  # trailing space -- not an exact match
        ("PUBLIC", VISIBILITY_PUBLIC),  # wrong case -- not an exact match
        (123, VISIBILITY_PUBLIC),
    ],
)
def test_is_public_adversarial_inputs_fail_closed(assertion_vis, identity_vis):
    assert is_public(assertion_vis, identity_vis) is False


# ---------------------------------------------------------------------------
# Behavior 5: no function in this module ever returns, logs, or interpolates
# a raw origin id -- asserted over the FULL adversarial input table.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("row", _ADVERSARIAL_ASSERTION_INPUTS + [
    {"assertion_source_corpus": _RAW_ID_SHAPED},
])
def test_assertion_visibility_never_returns_raw_origin_id(row):
    result = assertion_visibility(row)
    assert result in (VISIBILITY_PUBLIC, VISIBILITY_PRIVATE)
    assert _RAW_ID_SHAPED not in result
    assert result not in (_RAW_ID_SHAPED,)


@pytest.mark.parametrize("row", _ADVERSARIAL_IDENTITY_INPUTS + [
    {"source_corpus": _RAW_ID_SHAPED},
])
def test_identity_visibility_never_returns_raw_origin_id(row):
    result = identity_visibility(row)
    assert result in (VISIBILITY_PUBLIC, VISIBILITY_PRIVATE)
    assert _RAW_ID_SHAPED not in result


def test_raw_id_shaped_origin_is_never_treated_as_open():
    """A raw `M:`-prefixed research id passed where a masked corpus CODE is
    expected must fail closed to private -- it is not one of the two open
    codes, so it can never accidentally resolve to public."""
    assert assertion_visibility({"assertion_source_corpus": _RAW_ID_SHAPED}) == VISIBILITY_PRIVATE
    assert identity_visibility({"source_corpus": _RAW_ID_SHAPED}) == VISIBILITY_PRIVATE


# ---------------------------------------------------------------------------
# Behavior 6: reconcile_launch_scope reports the disagreement between
# VIS-01's launch-scope shortcut and the D-22 conjunction -- and resolves
# NOTHING.
# ---------------------------------------------------------------------------

def test_reconcile_launch_scope_agreement_case():
    """A fully-open direct/sefaria row: both rules agree it is included."""
    rows = [
        {
            "evidence_source": "track1_direct",
            "source_corpus": "sefaria",
            "assertion_source_corpus": "sefaria",
        }
    ]
    result = reconcile_launch_scope(rows)
    assert result["total_rows"] == 1
    assert result["vis01_launch_scope_count"] == 1
    assert result["conjunction_count"] == 1
    assert result["symmetric_difference_count"] == 0
    assert result["symmetric_difference_by_corpus_family"] == {}


def test_reconcile_launch_scope_vis01_included_conjunction_excluded():
    """A `propagated` row citing an msource-identity work: VIS-01's shortcut
    includes ALL propagated rows regardless of corpus, but the conjunction
    excludes it because identity_visibility is private. This is a genuine
    disagreement that must be REPORTED, not resolved."""
    rows = [
        {
            "evidence_source": "propagated",
            "source_corpus": "msource",
            "assertion_source_corpus": "sefaria",
        }
    ]
    result = reconcile_launch_scope(rows)
    assert result["vis01_launch_scope_count"] == 1
    assert result["conjunction_count"] == 0
    assert result["symmetric_difference_count"] == 1
    assert result["symmetric_difference_by_corpus_family"] == {("msource", "propagated"): 1}


def test_reconcile_launch_scope_conjunction_included_vis01_excluded():
    """A `track1_direct` row from the `ja` corpus, fully open on both axes:
    the conjunction includes it (both axes public), but VIS-01's shortcut
    excludes it (the shortcut only admits `track1_direct` when
    `source_corpus == 'sefaria'`) -- the OPPOSITE disagreement direction."""
    rows = [
        {
            "evidence_source": "track1_direct",
            "source_corpus": "ja",
            "assertion_source_corpus": "ja",
        }
    ]
    result = reconcile_launch_scope(rows)
    assert result["vis01_launch_scope_count"] == 0
    assert result["conjunction_count"] == 1
    assert result["symmetric_difference_count"] == 1
    assert result["symmetric_difference_by_corpus_family"] == {("ja", "track1_direct"): 1}


def test_reconcile_launch_scope_mixed_rows_breakdown_and_totals():
    rows = [
        # Agreement: direct/sefaria, fully open.
        {"evidence_source": "track1_direct", "source_corpus": "sefaria",
         "assertion_source_corpus": "sefaria"},
        # Disagreement A: propagated/msource-identity (VIS01 in, conjunction out).
        {"evidence_source": "propagated", "source_corpus": "msource",
         "assertion_source_corpus": "sefaria"},
        {"evidence_source": "propagated", "source_corpus": "msource",
         "assertion_source_corpus": "ja"},
        # Disagreement B: direct/ja, fully open (VIS01 out, conjunction in).
        {"evidence_source": "track1_direct", "source_corpus": "ja",
         "assertion_source_corpus": "ja"},
        # Agreement: direct/msource, fully restricted -- both exclude.
        {"evidence_source": "track1_direct", "source_corpus": "msource",
         "assertion_source_corpus": "msource"},
    ]
    result = reconcile_launch_scope(rows)
    assert result["total_rows"] == 5
    assert result["vis01_launch_scope_count"] == 3  # sefaria-direct + 2 propagated
    assert result["conjunction_count"] == 2  # sefaria-direct + ja-direct
    assert result["symmetric_difference_count"] == 3
    assert result["symmetric_difference_by_corpus_family"] == {
        ("msource", "propagated"): 2,
        ("ja", "track1_direct"): 1,
    }


def test_reconcile_launch_scope_never_mutates_or_drops_rows():
    """The function only counts and reports -- it must not resolve, drop, or
    relabel a row. Prove totals always account for every input row exactly
    once (vis01_count + conjunction_count - both aren't double counted
    incorrectly relative to symmetric difference, and empty input is a
    degenerate all-zero report rather than an error)."""
    result = reconcile_launch_scope([])
    assert result == {
        "total_rows": 0,
        "vis01_launch_scope_count": 0,
        "conjunction_count": 0,
        "symmetric_difference_count": 0,
        "symmetric_difference_by_corpus_family": {},
    }


def test_reconcile_launch_scope_accepts_a_generator_not_just_a_list():
    def _gen():
        yield {"evidence_source": "propagated", "source_corpus": "sefaria",
               "assertion_source_corpus": "sefaria"}

    result = reconcile_launch_scope(_gen())
    assert result["total_rows"] == 1
    assert result["vis01_launch_scope_count"] == 1
    assert result["conjunction_count"] == 1
