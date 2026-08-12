# -*- coding: utf-8 -*-
"""matrix-v1 (Contract 1, `docs/specs/discovery-relation-matrix-v1.md` §2) — the
six rules, their PRECEDENCE, the missing-input table, and the two properties the
rest of the system leans on: the output is always in the frozen vocabulary, and
`work_quotes_page` is unreachable in v1.

The precedence tests deliberately construct rows where TWO steps would fire and
assert the earlier one wins. A matrix whose rules are individually right and
collectively mis-ordered renders wrong relations on exactly the rows that matter
most (a router-flagged row that is also a curated quoter, say), and no
single-rule test can see it."""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT), str(REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import scripts.discovery_ids as ids
from shared import discovery_relation_matrix as matrix
from shared.discovery_relation_matrix import (
    DEPLOY_1_PARAMETERIZATION,
    MatrixParameterization,
    RelationInputs,
    RelationMatrixError,
    render_relation,
)


# A row that reaches step 6 and renders `direct_witness`: shipped evidence, a
# benign routing reason, no region knowledge, unmeasured divergence, not
# curated, coverage known. Every test below perturbs exactly one field.
def _baseline(**overrides) -> RelationInputs:
    kwargs = dict(
        has_shipped_evidence=True,
        routing_reason=ids.ROUTING_REASON_NONE,
        footprint_all_non_discriminative=None,
        work_divergence=None,
        on_curated_quoter_list=False,
        coverage_known=True,
        stored_relation_kind=ids.CLAIM_TYPE_DIRECT_WITNESS,
    )
    kwargs.update(overrides)
    return RelationInputs(**kwargs)


REGION_ON = MatrixParameterization(region_active=True, quoter_threshold=None)
QUOTER_50 = MatrixParameterization(region_active=False, quoter_threshold=0.5)
BOTH_ON = MatrixParameterization(region_active=True, quoter_threshold=0.5)


# ---------------------------------------------------------------------------
# The baseline itself, and each rule in isolation.
# ---------------------------------------------------------------------------

def test_the_baseline_row_reaches_step_6_and_renders_what_it_stores():
    assert render_relation(_baseline()) == ids.RENDERED_RELATION_DIRECT_WITNESS


def test_step_1_no_shipped_evidence_renders_uncertain():
    row = _baseline(has_shipped_evidence=False)
    assert render_relation(row) == ids.RENDERED_RELATION_UNCERTAIN


@pytest.mark.parametrize("reason", [
    ids.ROUTING_REASON_LATER_SHARED_TEXT,
    ids.ROUTING_REASON_CO_CITATION,
])
def test_step_2_router_reasons_render_shared_text(reason):
    assert render_relation(_baseline(routing_reason=reason)) == (
        ids.RENDERED_RELATION_SHARED_TEXT
    )


@pytest.mark.parametrize("reason", sorted(
    ids.ROUTING_REASONS - matrix.SHARED_TEXT_ROUTING_REASONS
))
def test_step_2_fires_for_those_two_reasons_and_no_others(reason):
    """The other six routing reasons must NOT demote. `impurity` and
    `runner_up_conflict` are real router decisions, but they are not statements
    about text-sharing, and the §2 census counted 173 rows — steps 1+2 exactly,
    not every flagged row."""
    assert render_relation(_baseline(routing_reason=reason)) == (
        ids.RENDERED_RELATION_DIRECT_WITNESS
    )


def test_step_3_demotes_only_when_region_is_active():
    row = _baseline(footprint_all_non_discriminative=True)
    # Deploy 1: step 3 is NOT activated, so the same row stands.
    assert render_relation(row) == ids.RENDERED_RELATION_DIRECT_WITNESS
    assert render_relation(row, REGION_ON) == ids.RENDERED_RELATION_SHARED_TEXT


@pytest.mark.parametrize("footprint", [False, None])
def test_step_3_needs_positive_knowledge_of_the_whole_footprint(footprint):
    """`False` (a discriminative unit is present) and `None` (nobody ruled) both
    block the demotion. A demotion is also an assertion — the region map is
    partial, and an unasked unit is not a ruling."""
    row = _baseline(footprint_all_non_discriminative=footprint)
    assert render_relation(row, REGION_ON) == ids.RENDERED_RELATION_DIRECT_WITNESS


def test_step_4_curated_arm_fires_with_no_threshold_set():
    """The curated list is an owner ruling, not a measurement: it needs no T.
    This is the arm that is LIVE in deploy 1 (both Yalkut works, 2026-08-12)."""
    row = _baseline(on_curated_quoter_list=True)
    assert render_relation(row, DEPLOY_1_PARAMETERIZATION) == (
        ids.RENDERED_RELATION_QUOTES_THIS_WORK
    )


def test_step_4_divergence_arm_cannot_fire_while_the_threshold_is_unset():
    """The missing-input rule: a divergence value with no threshold is not a
    decision. This is why deploy 1 moves no row on divergence alone."""
    row = _baseline(work_divergence=0.99)
    assert render_relation(row, DEPLOY_1_PARAMETERIZATION) == (
        ids.RENDERED_RELATION_DIRECT_WITNESS
    )
    assert render_relation(row, QUOTER_50) == ids.RENDERED_RELATION_QUOTES_THIS_WORK


@pytest.mark.parametrize("divergence,expected", [
    (0.49, ids.RENDERED_RELATION_DIRECT_WITNESS),
    (0.50, ids.RENDERED_RELATION_QUOTES_THIS_WORK),   # >= T, boundary included
    (0.51, ids.RENDERED_RELATION_QUOTES_THIS_WORK),
    (None, ids.RENDERED_RELATION_DIRECT_WITNESS),     # unmeasured
])
def test_step_4_threshold_is_inclusive_at_the_boundary(divergence, expected):
    row = _baseline(work_divergence=divergence)
    assert render_relation(row, QUOTER_50) == expected


def test_step_5_unknown_coverage_renders_uncertain():
    row = _baseline(coverage_known=False)
    assert render_relation(row) == ids.RENDERED_RELATION_UNCERTAIN


@pytest.mark.parametrize("stored", sorted(ids.CLAIM_TYPES))
def test_step_6_passes_through_every_stored_relation_kind(stored):
    assert render_relation(_baseline(stored_relation_kind=stored)) == stored


@pytest.mark.parametrize("stored", [None, "", "direct", "work_quotes_page", "uncertain"])
def test_step_6_fails_closed_on_anything_outside_the_stored_vocabulary(stored):
    """Including the two rendered-only tokens: `uncertain` and
    `work_quotes_page` are matrix OUTPUTS, never stored relation kinds, so
    finding one in the column is corruption, not a pass-through."""
    row = _baseline(stored_relation_kind=stored)
    assert render_relation(row) == ids.RENDERED_RELATION_FAIL_CLOSED
    assert render_relation(row) == ids.RENDERED_RELATION_UNCERTAIN


# ---------------------------------------------------------------------------
# PRECEDENCE — the part no single-rule test can see.
# ---------------------------------------------------------------------------

def test_step_1_beats_every_later_step():
    """No shipped evidence wins even when the row is a curated quoter with a
    demotable footprint and unknown coverage: absence of evidence is the
    strongest statement the matrix can make."""
    row = _baseline(
        has_shipped_evidence=False,
        routing_reason=ids.ROUTING_REASON_CO_CITATION,
        footprint_all_non_discriminative=True,
        work_divergence=0.99,
        on_curated_quoter_list=True,
        coverage_known=False,
    )
    assert render_relation(row, BOTH_ON) == ids.RENDERED_RELATION_UNCERTAIN


def test_step_2_beats_the_quoter_step():
    """A router-flagged row that is ALSO a curated quoter renders `shared_text`.
    The router's decision is about this row; the curated list is about the work,
    and the narrower statement wins."""
    row = _baseline(
        routing_reason=ids.ROUTING_REASON_LATER_SHARED_TEXT,
        on_curated_quoter_list=True,
        work_divergence=0.99,
    )
    assert render_relation(row, BOTH_ON) == ids.RENDERED_RELATION_SHARED_TEXT


def test_step_3_beats_the_quoter_step_when_active():
    row = _baseline(
        footprint_all_non_discriminative=True,
        on_curated_quoter_list=True,
    )
    assert render_relation(row, BOTH_ON) == ids.RENDERED_RELATION_SHARED_TEXT


def test_step_4_beats_the_coverage_step():
    """A curated quoter with unknown coverage renders `quotes_this_work`, not
    `uncertain` — the ruling is knowledge, and step 5 only fires in its
    absence."""
    row = _baseline(on_curated_quoter_list=True, coverage_known=False)
    assert render_relation(row) == ids.RENDERED_RELATION_QUOTES_THIS_WORK


def test_step_5_beats_the_stored_relation():
    row = _baseline(coverage_known=False, stored_relation_kind=ids.CLAIM_TYPE_SHARED_TEXT)
    assert render_relation(row) == ids.RENDERED_RELATION_UNCERTAIN


def test_the_deploy_1_census_shape_of_a_router_row_is_shared_text_not_uncertain():
    """Regression pin on the §2 reading: step 2's 173 rows have shipped evidence
    (they are SHOWN rows whose display reason is a router reason). If step 1 were
    widened to "any evidence is review_only", these would render `uncertain` and
    the census would be wrong by 173 rows."""
    row = _baseline(
        has_shipped_evidence=True,
        routing_reason=ids.ROUTING_REASON_CO_CITATION,
    )
    assert render_relation(row) == ids.RENDERED_RELATION_SHARED_TEXT


# ---------------------------------------------------------------------------
# Whole-input-space properties.
# ---------------------------------------------------------------------------

_ALL_PARAMS = (DEPLOY_1_PARAMETERIZATION, REGION_ON, QUOTER_50, BOTH_ON)


def _exhaustive_rows():
    domains = {
        "has_shipped_evidence": (True, False),
        "routing_reason": tuple(sorted(ids.ROUTING_REASONS)) + (None,),
        "footprint_all_non_discriminative": (True, False, None),
        "work_divergence": (None, 0.0, 0.49, 0.5, 1.0),
        "on_curated_quoter_list": (True, False),
        "coverage_known": (True, False),
        "stored_relation_kind": tuple(sorted(ids.CLAIM_TYPES)) + (None, "bogus"),
    }
    keys = tuple(domains)
    for combo in itertools.product(*(domains[k] for k in keys)):
        yield RelationInputs(**dict(zip(keys, combo)))


def test_every_input_combination_renders_a_member_of_the_frozen_vocabulary():
    """~7,900 rows × 4 parameterizations. The read path calls
    `relation_chip(rendered_relation)`, which RAISES on an unknown key, so an
    out-of-vocabulary output is a blank surface on a real row."""
    seen = set()
    for row in _exhaustive_rows():
        for params in _ALL_PARAMS:
            out = render_relation(row, params)
            assert out in ids.RENDERED_RELATIONS, (row, params, out)
            seen.add(out)
    # The four reachable states are all genuinely reached by this sweep.
    assert seen == ids.RENDERED_RELATIONS - matrix.NEVER_RENDERED_IN_V1


def test_work_quotes_page_is_unreachable_in_v1():
    """Spec §1: it renders only where a validated direction signal supports it,
    and no such signal exists. Assigning its reader strings is an owner item
    deferred until one ships — so the surface must never be handed this token."""
    for row in _exhaustive_rows():
        for params in _ALL_PARAMS:
            assert render_relation(row, params) != ids.RENDERED_RELATION_WORK_QUOTES_PAGE


def test_deploy_1_is_the_owner_ruled_parameterization():
    """Steps 1, 2, 5, 6 active (unparameterized); step 3 NOT activated; step 4's
    threshold arm unset. Pinned because flipping either flag silently changes
    what every shipped asset asserts."""
    assert DEPLOY_1_PARAMETERIZATION.region_active is False
    assert DEPLOY_1_PARAMETERIZATION.quoter_threshold is None


# ---------------------------------------------------------------------------
# Step 4a's ratio recipe.
# ---------------------------------------------------------------------------

def test_work_divergence_ratio_reproduces_the_census_recipe():
    """`not_checked` leaves the denominator; both divergence shades count; the
    ratio is divergent/checked."""
    rows = [("wA", "diverges_work"), ("wA", "diverges_part"), ("wA", "confirms"),
            ("wA", "fills_gap"), ("wA", "extends"), ("wA", "not_checked")]
    ratios = matrix.work_divergence_ratios(rows)
    assert ratios == {"wA": 2 / 5}


def test_work_divergence_omits_works_below_the_denominator_floor():
    rows = [("wB", "diverges_work")] * 2 + [("wB", "confirms")] * 2  # checked = 4
    assert matrix.work_divergence_ratios(rows) == {}
    assert matrix.work_divergence_ratios(rows + [("wB", "confirms")]) == {"wB": 2 / 5}


def test_work_divergence_ignores_not_checked_rows_entirely():
    """Five `not_checked` rows are not a measured zero — the work is absent, so
    step 4a cannot fire on it."""
    rows = [("wC", "not_checked")] * 20
    assert matrix.work_divergence_ratios(rows) == {}


def test_work_divergence_treats_null_novelty_like_not_checked():
    rows = [("wD", None)] * 5 + [("wD", "confirms")] * 5
    assert matrix.work_divergence_ratios(rows) == {"wD": 0.0}


def test_omitting_a_sub_floor_work_is_equivalent_to_the_sweeps_zero():
    """The stated equivalence, exercised rather than asserted in prose: for any
    admissible T, a sub-floor work renders the same either way."""
    sub_floor = _baseline(work_divergence=None)          # omitted -> absent
    sweep_style = _baseline(work_divergence=0.0)         # sweep's stored 0.0
    for t in (0.01, 0.3, 0.5, 0.6, 1.0):
        params = MatrixParameterization(quoter_threshold=t)
        assert render_relation(sub_floor, params) == render_relation(sweep_style, params)


# ---------------------------------------------------------------------------
# The parameterization contract: validation, and the meta round-trip the
# verifier's equality gate depends on.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [0.0, -0.1, 1.01, 2, True, "0.5"])
def test_threshold_outside_the_ratio_domain_is_refused(bad):
    """T=0 would flag every novelty-checked work; a percentage-shaped 50 would
    flag none. Both are the kind of unit error that ships silently."""
    with pytest.raises(RelationMatrixError):
        MatrixParameterization(quoter_threshold=bad)


@pytest.mark.parametrize("params", _ALL_PARAMS)
def test_parameterization_survives_the_meta_round_trip(params):
    meta = dict(matrix.parameterization_meta_rows(params))
    assert matrix.parameterization_from_meta(meta) == params


def test_meta_rows_always_write_every_key():
    """A missing key and an unset value are different observations; the verifier
    distinguishes "built before the matrix shipped" from "built with no
    threshold"."""
    keys = [k for k, _ in matrix.parameterization_meta_rows(DEPLOY_1_PARAMETERIZATION)]
    assert keys == list(matrix.PARAMETERIZATION_META_KEYS)
    assert dict(matrix.parameterization_meta_rows(DEPLOY_1_PARAMETERIZATION))[
        "relation_matrix_quoter_threshold"] == ""


@pytest.mark.parametrize("drop", sorted(matrix.PARAMETERIZATION_META_KEYS))
def test_reconstruction_refuses_a_meta_missing_any_key(drop):
    meta = dict(matrix.parameterization_meta_rows(QUOTER_50))
    del meta[drop]
    with pytest.raises(RelationMatrixError):
        matrix.parameterization_from_meta(meta)


def test_reconstruction_refuses_a_foreign_matrix_version():
    """Recomputing a matrix-v2 asset with v1 rules would report equality it has
    no basis for. The gate must refuse, not adapt."""
    meta = dict(matrix.parameterization_meta_rows(QUOTER_50))
    meta["relation_matrix_version"] = "matrix-v2"
    with pytest.raises(RelationMatrixError, match="refusing to recompute"):
        matrix.parameterization_from_meta(meta)


@pytest.mark.parametrize("key,value", [
    ("relation_matrix_region_active", "true"),
    ("relation_matrix_region_active", ""),
    ("relation_matrix_region_active", "2"),
    ("relation_matrix_quoter_threshold", "half"),
    ("relation_matrix_quoter_threshold", "50"),      # percent, not ratio
])
def test_reconstruction_refuses_malformed_values(key, value):
    meta = dict(matrix.parameterization_meta_rows(QUOTER_50))
    meta[key] = value
    with pytest.raises(RelationMatrixError):
        matrix.parameterization_from_meta(meta)


def test_matrix_version_is_pinned():
    assert matrix.MATRIX_VERSION == "matrix-v1"


def test_the_shared_text_routing_reasons_are_exactly_the_two_frozen_ones():
    """Drift guard. Widening this set silently re-renders rows the A0a-2 census
    counted as direct."""
    assert matrix.SHARED_TEXT_ROUTING_REASONS == {"later_shared_text", "co_citation"}
    assert matrix.SHARED_TEXT_ROUTING_REASONS <= ids.ROUTING_REASONS


def test_the_divergence_floor_is_the_census_floor():
    assert matrix.WORK_DIVERGENCE_MIN_DENOMINATOR == 5
