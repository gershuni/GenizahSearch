# -*- coding: utf-8 -*-
"""Golden-hash + validator + total-routing + display-precedence tests for
`scripts/discovery_ids.py` (Phase 134, plan 134-01, Task 3).

Mirrors the golden-digest shape of `tests/atlas_bake/test_atlas_bake.py`
(`test_determinism` / `test_content_hash_changes`): compute the frozen id
recipes once from fixed, fabricated inputs, paste the digests as committed
constants below, and assert equality on every run -- so any future drift in
`scripts/discovery_ids.py`'s hashing recipe fails CI immediately instead of
silently reinterpreting an already-shipped `discovery.db`.

Masking discipline (`tests/test_atlas_masking_scan.py` convention): every
negative-validator input below is a FABRICATED, test-only, obviously-fake
token (e.g. `ZZZ_FAKE_SOURCE_CORPUS_ZZZ`) -- NEVER a real restricted corpus
name. All page_id/sys_id/work_id fixture values in this file are synthetic
placeholders, not real research-data identifiers.
"""

import sqlite3

import pytest

import scripts.discovery_ids as d
from scripts import build_discovery_sidecar as sidecar_build

# ---------------------------------------------------------------------------
# Committed golden digests -- computed once from the frozen fixed inputs
# below via `scripts/discovery_ids.py`'s claim_id/evidence_id/unit_id. If a
# recipe change is intentional, regenerate these via:
#   python -c "import scripts.discovery_ids as d; print(d.claim_id(...))"
# and re-review the schema doc (docs/specs/discovery-sidecar-schema-v1.md §2)
# alongside the new constant -- never edit the digest without updating the
# doc's frozen recipe description.
# ---------------------------------------------------------------------------

GOLDEN_CLAIM_ID = "7c674091880866e271190fd2a1d6be59fe9e6a190bd95c6d9600794ec5df0c53"
GOLDEN_EVIDENCE_ID = "0dc089ed54b837e2c57bae86280e764cf6a3b8c760731ee5bcb77993689e6b34"
GOLDEN_EVIDENCE_ID_PROPAGATED = "181a45199297db75b0a466e73fa901aa797cc3c8301de2220dcb6d3ced3e7fcb"
GOLDEN_UNIT_ID = "408a75a003722601d6ead1682f71f4c83f0b93ea3ae3475b1dd91ddb6563f928"

FROZEN_PAGE_ID = "page_frozen_001"
FROZEN_WORK_ID = "w000042"
FROZEN_SYS_ID = "990000000000000001"

# A multi-occurrence propagated evidence row (R4/G3) -- two distinct
# candidate-side occurrences, deliberately supplied in two different input
# orders below to prove the seed_spans digest sorts before hashing.
_SEED_SPANS_ORDER_A = [
    {
        "occ0": 30, "occ1": 80, "occ_class": "core",
        "seed_page_ids": ["p2", "p1"],
        "seed_sys_ids": ["990000000000000003", "990000000000000002"],
    },
    {
        "occ0": 5, "occ1": 20, "occ_class": "flank",
        "seed_page_ids": ["p3"],
        "seed_sys_ids": ["990000000000000004"],
    },
]
_SEED_SPANS_ORDER_B = list(reversed(_SEED_SPANS_ORDER_A))


# ---------------------------------------------------------------------------
# test_claim_id_golden
# ---------------------------------------------------------------------------

def test_claim_id_golden():
    h1 = d.claim_id(FROZEN_PAGE_ID, FROZEN_WORK_ID)
    h2 = d.claim_id(FROZEN_PAGE_ID, FROZEN_WORK_ID)
    assert len(h1) == 64
    assert h1 == h2, "claim_id must be stable across repeated calls (rebuild -> identical)"
    assert h1 == GOLDEN_CLAIM_ID, (
        "claim_id digest drifted from the committed golden value -- "
        "see docs/specs/discovery-sidecar-schema-v1.md §2 before regenerating"
    )


def test_claim_id_not_a_function_of_claim_type():
    # G5: claim_id has no claim_type parameter at all -- calling with the
    # same (page_id, work_id) always yields the identical digest regardless
    # of how the caller's claim_type later resolves/flips.
    assert d.claim_id(FROZEN_PAGE_ID, FROZEN_WORK_ID) == GOLDEN_CLAIM_ID


def test_evidence_id_golden():
    h1 = d.evidence_id(
        work_id=FROZEN_WORK_ID,
        a_page_id=FROZEN_PAGE_ID,
        sys_id=FROZEN_SYS_ID,
        evidence_kind="witness",
        evidence_source="track1_direct",
        confidence_band="tier_a",
        span_start=10,
        span_end=50,
        other_page_id=None,
        seed_spans=None,
    )
    h2 = d.evidence_id(
        work_id=FROZEN_WORK_ID,
        a_page_id=FROZEN_PAGE_ID,
        sys_id=FROZEN_SYS_ID,
        evidence_kind="witness",
        evidence_source="track1_direct",
        confidence_band="tier_a",
        span_start=10,
        span_end=50,
        other_page_id=None,
        seed_spans=None,
    )
    assert len(h1) == 64
    assert h1 == h2
    assert h1 == GOLDEN_EVIDENCE_ID


def test_evidence_id_seed_spans_order_invariant_and_golden():
    # R4/G3: the seed_spans list is sorted by (occ0, occ1) before hashing, so
    # two callers assembling the SAME distinct occurrences in different
    # input order produce the IDENTICAL evidence_id.
    h_a = d.evidence_id(
        work_id="w000099", a_page_id="page_frozen_002", sys_id="990000000000000005",
        evidence_kind="witness", evidence_source="propagated", confidence_band="corroborated",
        span_start=30, span_end=80, other_page_id=None, seed_spans=_SEED_SPANS_ORDER_A,
    )
    h_b = d.evidence_id(
        work_id="w000099", a_page_id="page_frozen_002", sys_id="990000000000000005",
        evidence_kind="witness", evidence_source="propagated", confidence_band="corroborated",
        span_start=30, span_end=80, other_page_id=None, seed_spans=_SEED_SPANS_ORDER_B,
    )
    assert h_a == h_b == GOLDEN_EVIDENCE_ID_PROPAGATED


def test_unit_id_golden_and_order_invariant():
    h_a = d.unit_id(["990000000000000002", "990000000000000001"])
    h_b = d.unit_id(["990000000000000001", "990000000000000002"])
    assert h_a == h_b == GOLDEN_UNIT_ID


# ---------------------------------------------------------------------------
# test_content_key_changes -- no accidental collision
# ---------------------------------------------------------------------------

def test_content_key_changes_claim_id():
    base = d.claim_id(FROZEN_PAGE_ID, FROZEN_WORK_ID)
    changed_page = d.claim_id(FROZEN_PAGE_ID + "_x", FROZEN_WORK_ID)
    changed_work = d.claim_id(FROZEN_PAGE_ID, FROZEN_WORK_ID + "_x")
    assert base != changed_page
    assert base != changed_work
    assert changed_page != changed_work


def test_content_key_changes_evidence_id():
    base = d.evidence_id(
        work_id=FROZEN_WORK_ID, a_page_id=FROZEN_PAGE_ID, sys_id=FROZEN_SYS_ID,
        evidence_kind="witness", evidence_source="track1_direct", confidence_band="tier_a",
        span_start=10, span_end=50, other_page_id=None, seed_spans=None,
    )
    # change one span coordinate by 1 -- must change the digest
    changed_span = d.evidence_id(
        work_id=FROZEN_WORK_ID, a_page_id=FROZEN_PAGE_ID, sys_id=FROZEN_SYS_ID,
        evidence_kind="witness", evidence_source="track1_direct", confidence_band="tier_a",
        span_start=10, span_end=51, other_page_id=None, seed_spans=None,
    )
    # change confidence_band only -- must change the digest
    changed_band = d.evidence_id(
        work_id=FROZEN_WORK_ID, a_page_id=FROZEN_PAGE_ID, sys_id=FROZEN_SYS_ID,
        evidence_kind="witness", evidence_source="track1_direct", confidence_band="screening_rb",
        span_start=10, span_end=50, other_page_id=None, seed_spans=None,
    )
    assert base != changed_span
    assert base != changed_band
    assert changed_span != changed_band


def test_content_key_changes_unit_id():
    base = d.unit_id(["990000000000000001", "990000000000000002"])
    changed = d.unit_id(["990000000000000001", "990000000000000003"])
    assert base != changed


# ---------------------------------------------------------------------------
# test_validate_source_corpus_code
# ---------------------------------------------------------------------------

def test_validate_source_corpus_code_passes_frozen_codes():
    for code in ("sefaria", "ja", "msource"):
        assert d.validate_source_corpus_code(code) == code


def test_validate_source_corpus_code_raises_on_fabricated_code():
    # Fabricated, test-only, obviously-fake token -- NEVER a real restricted
    # corpus name (masking-scan convention, tests/test_atlas_masking_scan.py).
    with pytest.raises(ValueError):
        d.validate_source_corpus_code("ZZZ_FAKE_SOURCE_CORPUS_ZZZ")


# ---------------------------------------------------------------------------
# test_corroborated_predicate
# ---------------------------------------------------------------------------

def test_corroborated_predicate_two_seed_witness_row():
    row = {"_bucket": "witness", "is_new": True, "impurity": False, "trials": 2}
    assert d.corroborated_predicate(row) is True


def test_corroborated_predicate_one_seed_row_false():
    # router-cleaned one-seed row: no 'trials' key, carries 'rung' instead.
    row = {"_bucket": "witness", "is_new": True, "impurity": False, "rung": "A"}
    assert d.corroborated_predicate(row) is False


def test_corroborated_predicate_impure_row_false():
    row = {"_bucket": "witness", "is_new": True, "impurity": True, "trials": 3}
    assert d.corroborated_predicate(row) is False


def test_corroborated_predicate_not_new_row_false():
    row = {"_bucket": "witness", "is_new": False, "impurity": False, "trials": 5}
    assert d.corroborated_predicate(row) is False


def test_corroborated_predicate_never_true_for_family_router_rows():
    # R3: corroborated_predicate REQUIRES _bucket == 'witness'; family-router
    # rows (_bucket in {tafsir_targum, with_arabic}) must NEVER pass, even
    # with a qualifying trials count -- they route to shared_text/
    # not_evaluated/review_only/co_citation instead, never a witness band.
    for bucket in ("tafsir_targum", "with_arabic"):
        row = {"_bucket": bucket, "is_new": True, "impurity": False, "trials": 5}
        assert d.corroborated_predicate(row) is False


def test_is_impure_helper_documented_definition():
    assert d.is_impure({"runner_up": 5, "support": 10}) is True   # 5 >= 0.5*10
    assert d.is_impure({"runner_up": 2, "support": 10}) is False  # 2 < 0.5*10
    assert d.is_impure({"runner_up": 5, "support": 0}) is False   # support must be > 0


# ---------------------------------------------------------------------------
# test_claim_type_routing_total
# ---------------------------------------------------------------------------

def test_claim_type_for_work_witness_dominant_span():
    assert d.claim_type_for_work_witness([40, 10], 40) == d.CLAIM_TYPE_DIRECT_WITNESS


def test_claim_type_for_work_witness_non_dominant_span():
    assert d.claim_type_for_work_witness([40, 10], 10) == d.CLAIM_TYPE_QUOTES_THIS_WORK


def test_claim_type_for_work_witness_single_claim_page():
    assert d.claim_type_for_work_witness([25], 25) == d.CLAIM_TYPE_DIRECT_WITNESS
    assert d.claim_type_for_work_witness([], 25) == d.CLAIM_TYPE_DIRECT_WITNESS


def test_resolve_claim_type_witness_present():
    rows = [{"evidence_kind": "witness", "claim_type": "direct_witness"}]
    assert d.resolve_claim_type(rows) == d.CLAIM_TYPE_DIRECT_WITNESS


def test_resolve_claim_type_shared_text_only():
    rows = [{"evidence_kind": "shared_text"}]
    assert d.resolve_claim_type(rows) == d.CLAIM_TYPE_SHARED_TEXT


def test_resolve_claim_type_mixed_witness_dominates():
    # F7: the 43,046-row collision -- a claim carrying BOTH a witness row
    # AND a shared_text row resolves via the witness rule, never shared_text.
    rows = [
        {"evidence_kind": "witness", "claim_type": "quotes_this_work"},
        {"evidence_kind": "shared_text"},
    ]
    assert d.resolve_claim_type(rows) == d.CLAIM_TYPE_QUOTES_THIS_WORK


def test_resolve_claim_type_empty_rows_total():
    # Totality: even a claim with zero rows passed in must return a defined
    # value (never raise) -- falls to shared_text (no witness evidence).
    assert d.resolve_claim_type([]) == d.CLAIM_TYPE_SHARED_TEXT


# ---------------------------------------------------------------------------
# test_display_precedence -- the full frozen (C-5/R6) lattice
# ---------------------------------------------------------------------------

def _row(evidence_id, evidence_source, confidence_band, adjudication_status):
    return {
        "evidence_id": evidence_id,
        "evidence_source": evidence_source,
        "confidence_band": confidence_band,
        "adjudication_status": adjudication_status,
    }


def test_display_precedence_human_confirmed_track1_direct_wins_reachable_case():
    # The REACHABLE R6 case: the 174 individually-adjudicated
    # e1_adjudicated_a.jsonl expert_verified rows.
    rows = [
        _row("A", "track1_direct", "expert_verified", "human_confirmed"),
        _row("B", "propagated", "corroborated", "unreviewed"),
        _row("C", "track1_direct", "screening_rb", "provisional"),
    ]
    assert d.select_display_evidence(rows) == "A"


def test_display_precedence_human_confirmed_screening_still_dominates_unreachable_totality_cell():
    # Unreachable-in-v1 totality cell: proves family-specific dominance is
    # defined across ALL track1_direct bands, not just expert_verified.
    rows = [
        _row("A", "track1_direct", "screening_canon", "human_confirmed"),
        _row("B", "propagated", "corroborated", "unreviewed"),
        _row("C", "track1_direct", "expert_verified", "unreviewed"),
    ]
    assert d.select_display_evidence(rows) == "A"


def test_display_precedence_corroborated_beats_screening_bands():
    rows = [
        _row("A", "propagated", "corroborated", "unreviewed"),
        _row("B", "track1_direct", "screening_rb", "provisional"),
        _row("C", "track1_direct", "screening_canon", "provisional"),
    ]
    assert d.select_display_evidence(rows) == "A"


def test_display_precedence_tier_a_beats_corroborated():
    rows = [
        _row("A", "track1_direct", "tier_a", "unreviewed"),
        _row("B", "propagated", "corroborated", "unreviewed"),
    ]
    assert d.select_display_evidence(rows) == "A"


def test_display_precedence_unreviewed_expert_verified_beats_corroborated():
    # R6: the 1,570-row e1_ra_confirmed.jsonl population is unreviewed, yet
    # still outranks propagated corroborated via the global band-rank (not
    # the human_confirmed dominance flag).
    rows = [
        _row("A", "track1_direct", "expert_verified", "unreviewed"),
        _row("B", "propagated", "corroborated", "unreviewed"),
    ]
    assert d.select_display_evidence(rows) == "A"


def test_display_precedence_not_evaluated_never_chosen_with_witness_present():
    rows = [
        _row("A", "propagated", "not_evaluated", "unreviewed"),
        _row("B", "propagated", "weak", "provisional"),
    ]
    assert d.select_display_evidence(rows) == "B"


def test_display_precedence_deterministic_evidence_id_tiebreak():
    # Two rows tied on every ranked field -- lexicographically smaller
    # evidence_id wins, deterministically.
    rows = [
        _row("zzz_higher", "track1_direct", "tier_a", "unreviewed"),
        _row("aaa_lower", "track1_direct", "tier_a", "unreviewed"),
    ]
    assert d.select_display_evidence(rows) == "aaa_lower"


def test_display_precedence_raises_on_empty():
    with pytest.raises(ValueError):
        d.select_display_evidence([])


# ---------------------------------------------------------------------------
# Phase 135-05: v2 vocabulary + schema lockstep (Task 1 TDD + Task 3 golden).
#
# routing_reason gains `later_shared_text`; the stored track1_direct band adds
# the v2 key `high_confidence_algorithmic` (renaming `expert_verified` in the
# v2 vocabulary while RETAINING the v1 key for read-compat -- Codex #8);
# band_precision gains the five registry columns with a CLOSED-vocab
# `measurement_status` CHECK (Codex #B3); and the masking-safe
# `discovery_routing_audit` table is created. The DDL CHECKs, the frozen
# frozensets, and the build DDL must all agree (T-135-05-01 lockstep).
#
# All fixture values below are synthetic placeholders, never real research
# identifiers (masking discipline, matches the header note).
# ---------------------------------------------------------------------------


def _fresh_schema_conn():
    """An in-memory DB with the FROZEN build DDL applied. Foreign keys are
    turned OFF afterwards so the CHECK-constraint tests below can insert a
    single row without satisfying every cross-table FK -- CHECK constraints
    are still enforced regardless of the FK pragma."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def _insert_evidence(conn, *, routing_reason):
    conn.execute(
        """
        INSERT INTO discovery_evidence (
            evidence_id, claim_id, evidence_kind, evidence_source, confidence_band,
            adjudication_status, audit_status, routing_status, routing_reason,
            is_new, a_page_id, sys_id, span_start, span_end
        ) VALUES (?, 'claim_x', 'witness', 'track1_direct', 'tier_a',
                   'unreviewed', 'n/a', 'shipped', ?, 0, 'p001', '990000000000000001', 1, 5)
        """,
        (f"ev_{routing_reason}", routing_reason),
    )
    conn.commit()


# -- routing_reason frozen enum + discovery_evidence DDL CHECK --------------

def test_routing_reason_later_shared_text_constant_and_membership():
    assert d.ROUTING_REASON_LATER_SHARED_TEXT == "later_shared_text"
    assert "later_shared_text" in d.ROUTING_REASONS
    assert len(d.ROUTING_REASONS) == 5


def test_discovery_evidence_routing_reason_check_accepts_later_shared_text():
    conn = _fresh_schema_conn()
    try:
        _insert_evidence(conn, routing_reason="later_shared_text")  # must insert cleanly
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_evidence WHERE routing_reason = 'later_shared_text'"
        ).fetchone()
        assert n == 1
    finally:
        conn.close()


def test_discovery_evidence_routing_reason_check_rejects_bogus():
    conn = _fresh_schema_conn()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_evidence(conn, routing_reason="ZZZ_FAKE_ROUTING_REASON_ZZZ")
    finally:
        conn.close()


# -- band rename with v1-read-compat (Codex #8) -----------------------------

def test_confidence_band_high_confidence_algorithmic_constant_and_membership():
    assert d.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC == "high_confidence_algorithmic"
    track1 = d.CONFIDENCE_BANDS_BY_SOURCE[d.EVIDENCE_SOURCE_TRACK1_DIRECT]
    assert "high_confidence_algorithmic" in track1
    # v1-read-compat: the v1 stored key is RETAINED until the v2 manifest is
    # live (the live v1 asset + the v1 fixture tests still read it).
    assert "expert_verified" in track1


# -- band_precision registry columns + closed-vocab measurement_status CHECK -

def test_band_precision_has_all_registry_columns():
    conn = _fresh_schema_conn()
    try:
        cols = {r[1] for r in conn.execute('PRAGMA table_info("band_precision")')}
    finally:
        conn.close()
    assert {
        "measurement_status", "measurement_date", "grader", "audit_status", "report_id"
    } <= cols


def test_band_precision_measurement_status_accepts_closed_vocab():
    conn = _fresh_schema_conn()
    try:
        for status in (
            "not_measured", "measured_pass", "measured_fail", "insufficient_evidence", None,
        ):
            conn.execute(
                "INSERT INTO band_precision (scope, collection_id, measurement_status) "
                "VALUES ('band', 'c1', ?)",
                (status,),
            )
        conn.commit()
        (n,) = conn.execute("SELECT COUNT(*) FROM band_precision").fetchone()
        assert n == 5
    finally:
        conn.close()


def test_band_precision_measurement_status_rejects_bogus():
    conn = _fresh_schema_conn()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO band_precision (scope, collection_id, measurement_status) "
                "VALUES ('band', 'c1', 'ZZZ_FAKE_STATUS_ZZZ')"
            )
            conn.commit()
    finally:
        conn.close()


# -- masking-safe discovery_routing_audit table -----------------------------

def test_discovery_routing_audit_accepts_valid_row():
    conn = _fresh_schema_conn()
    try:
        conn.execute(
            """
            INSERT INTO discovery_routing_audit
                (page_id, kept_work_id, demoted_work_id, kept_year, demoted_year,
                 delta_years, decision, routing_reason)
            VALUES ('p001', 'w000001', 'w000002', 900, 1100, 200, 'demoted', 'later_shared_text')
            """
        )
        conn.commit()
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_routing_audit WHERE decision = 'demoted'"
        ).fetchone()
        assert n == 1
    finally:
        conn.close()


def test_discovery_routing_audit_rejects_bogus_decision():
    conn = _fresh_schema_conn()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO discovery_routing_audit (page_id, decision) "
                "VALUES ('p001', 'ZZZ_FAKE_DECISION_ZZZ')"
            )
            conn.commit()
    finally:
        conn.close()


# -- Task 3 golden pin: the committed amended vocabulary. If a change here is
#    intentional, regenerate + re-review docs/specs/discovery-sidecar-schema-v1.md
#    (the 2026-07-24 amendment) + discovery-band-labels-v1.md §5 alongside it.

GOLDEN_ROUTING_REASONS = [
    "co_citation", "impurity", "later_shared_text", "none", "runner_up_conflict",
]
GOLDEN_TRACK1_CONFIDENCE_BANDS = [
    "expert_verified", "high_confidence_algorithmic", "screening_canon",
    "screening_rb", "tier_a",
]


def test_routing_reasons_golden_membership():
    assert sorted(d.ROUTING_REASONS) == GOLDEN_ROUTING_REASONS


def test_track1_confidence_bands_golden_membership():
    # The v2 key is present AND the v1 key is retained (v1-read-compat, Codex #8).
    assert sorted(d.CONFIDENCE_BANDS_BY_SOURCE[d.EVIDENCE_SOURCE_TRACK1_DIRECT]) == (
        GOLDEN_TRACK1_CONFIDENCE_BANDS
    )
    assert d.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC in (
        d.CONFIDENCE_BANDS_BY_SOURCE[d.EVIDENCE_SOURCE_TRACK1_DIRECT]
    )
