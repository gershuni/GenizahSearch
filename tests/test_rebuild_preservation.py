# -*- coding: utf-8 -*-
"""Positive controls for the rebuild-preservation gate (Phase 136, plan 136-05, Task 3).

Every fixture here is FABRICATED test data (a small, self-contained pair of
temp SQLite sidecars) -- NEVER real research content. Proves the gate PASSES
on an allowed-changes-only fixture pair (the baseline -- without it, every
failure control below is meaningless), then proves it FAILS seven different
ways, one per failure class the gate exists to catch (D-02b).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts import discovery_ids as ids
from scripts import verify_rebuild_preservation as vrp

REPO_ROOT = Path(__file__).resolve().parent.parent
PHASE_DIR = REPO_ROOT / ".planning" / "phases" / "136-read-surfaces-connections-panel-work-witnesses"

# ---------------------------------------------------------------------------
# Fixture geometry (fabricated, masking-safe -- synthetic ids/titles only)
# ---------------------------------------------------------------------------

PAGE1, SYS1 = "p001", "990000000000000001"
PAGE2, SYS2 = "p002", "990000000000000002"
PAGE3 = "p003"

WORK1 = "w000001"  # sefaria
WORK2 = "w000002"  # ja

C1_ID = ids.claim_id(PAGE1, WORK1)
C2_ID = ids.claim_id(PAGE2, WORK2)

EA_ID = ids.evidence_id(WORK1, PAGE1, SYS1, ids.EVIDENCE_KIND_WITNESS,
                        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A, 10, 60)
EB_ID = ids.evidence_id(WORK1, PAGE1, SYS1, ids.EVIDENCE_KIND_WITNESS,
                        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A, 5, 40)
EC_ID = ids.evidence_id(WORK2, PAGE2, SYS2, ids.EVIDENCE_KIND_WITNESS,
                        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A, 0, 30)
ED_ID = ids.evidence_id(WORK2, PAGE2, SYS2, ids.EVIDENCE_KIND_WITNESS,
                        ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_CORROBORATED, 0, 20)

# The 2026-08-02 amendment columns. When this fixture was written none of them
# existed in `create_schema`, so the "new" side had to ALTER them in to simulate
# an additive rebuild. Plans 136-11/136-12 then added them to `create_schema`
# itself, at which point every ALTER here became a `duplicate column name` error
# and the whole module errored out on both CI platforms.
#
# Kept rather than deleted so the fixture still works against a build whose
# schema PREDATES the amendment, but applied idempotently: a column that
# `create_schema` already provides is skipped. Asserting the end state (the
# column exists) is what this fixture actually needs; how it got there is not
# the property under test.
_AMENDMENT_COLUMNS = (
    ('works', 'identity_visibility', 'TEXT'),
    ('discovery_evidence', 'coverage_ppm', 'INTEGER'),
    ('discovery_evidence', 'coverage_status', 'TEXT'),
    ('discovery_evidence', 'band_rank', 'INTEGER'),
    ('discovery_evidence', 'novelty_status', 'TEXT'),
    ('discovery_evidence', 'novelty_source_label', 'TEXT'),
    ('discovery_evidence', 'assertion_visibility', 'TEXT'),
)


def _apply_amendment_columns(conn: sqlite3.Connection) -> None:
    for table, column, decl in _AMENDMENT_COLUMNS:
        existing = {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
        if column not in existing:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {decl}')
        assert column in {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}


def _insert_row(conn: sqlite3.Connection, table: str, values: dict) -> None:
    """Insert `values` into `table`, silently dropping any key that is not an
    ACTUAL column of `table` on this connection -- lets ONE shared row-values
    dict serve both the pre-amendment ("old") and post-amendment ("new")
    schema: a brand-new amendment column is simply absent from `old`'s column
    list and therefore ignored there, with zero special-casing per column."""
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    provided = {c: values.get(c) for c in cols}
    col_csv = ", ".join(provided.keys())
    ph_csv = ", ".join(f":{c}" for c in provided.keys())
    conn.execute(f"INSERT INTO {table} ({col_csv}) VALUES ({ph_csv})", provided)  # noqa: S608


def _populate_base_rows(conn: sqlite3.Connection, *, is_new: bool) -> None:
    """The ONE shared row layout for both fixture halves. `is_new` toggles
    ONLY the handful of values the Phase-136 rebuild is authorized to change
    on an already-existing column (works.genre, the kept_tie row's
    demoted_work_id, and the tier_a band_precision row's
    measurement_status/ci_low) -- every brand-new amendment column (present
    only on the `new` connection's schema) is supplied unconditionally and
    is a no-op on `old` via `_insert_row`'s column-set filtering."""
    _insert_row(conn, "works", dict(
        work_id=WORK1, canonical_work_id=WORK1, neutral_title="Synthetic Neutral Title Alpha",
        author="Synthetic Author A", genre=("Synthetic Domain X" if is_new else None),
        source_corpus="sefaria", identity_visibility="public",
    ))
    _insert_row(conn, "works", dict(
        work_id=WORK2, canonical_work_id=WORK2, neutral_title="Synthetic Neutral Title Beta",
        author=None, genre=("Synthetic Domain Y" if is_new else None),
        source_corpus="ja", identity_visibility="public",
    ))

    _insert_row(conn, "discovery_claim", dict(
        page_id=PAGE1, work_id=WORK1, claim_id=C1_ID, claim_type=ids.CLAIM_TYPE_DIRECT_WITNESS,
        display_evidence_id=EA_ID, source_corpus="sefaria", sidecar_version="fixture-v1",
    ))
    _insert_row(conn, "discovery_claim", dict(
        page_id=PAGE2, work_id=WORK2, claim_id=C2_ID, claim_type=ids.CLAIM_TYPE_DIRECT_WITNESS,
        display_evidence_id=EC_ID, source_corpus="ja", sidecar_version="fixture-v1",
    ))

    _new_evidence_cols = dict(
        coverage_ppm=520000, coverage_status="measured", band_rank=2,
        novelty_status="not_checked", novelty_source_label=None, assertion_visibility="public",
    )
    _insert_row(conn, "discovery_evidence", dict(
        evidence_id=EA_ID, claim_id=C1_ID, evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT, confidence_band=ids.CONFIDENCE_BAND_TIER_A,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED, audit_status=ids.AUDIT_STATUS_NA,
        routing_status=ids.ROUTING_STATUS_SHIPPED, routing_reason=ids.ROUTING_REASON_NONE, is_new=0,
        a_page_id=PAGE1, sys_id=SYS1, span_start=10, span_end=60, matched_letters=50,
        text_layer="htr", snapshot_hash="snap-a1", **_new_evidence_cols,
    ))
    _insert_row(conn, "discovery_evidence", dict(
        evidence_id=EB_ID, claim_id=C1_ID, evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT, confidence_band=ids.CONFIDENCE_BAND_TIER_A,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED, audit_status=ids.AUDIT_STATUS_NA,
        routing_status=ids.ROUTING_STATUS_SHIPPED, routing_reason=ids.ROUTING_REASON_NONE, is_new=0,
        a_page_id=PAGE1, sys_id=SYS1, span_start=5, span_end=40, matched_letters=35,
        text_layer="htr", snapshot_hash="snap-a2", **_new_evidence_cols,
    ))
    _insert_row(conn, "discovery_evidence", dict(
        evidence_id=EC_ID, claim_id=C2_ID, evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT, confidence_band=ids.CONFIDENCE_BAND_TIER_A,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED, audit_status=ids.AUDIT_STATUS_NA,
        routing_status=ids.ROUTING_STATUS_SHIPPED, routing_reason=ids.ROUTING_REASON_NONE, is_new=0,
        a_page_id=PAGE2, sys_id=SYS2, span_start=0, span_end=30, matched_letters=25,
        text_layer="htr", snapshot_hash="snap-c1", **_new_evidence_cols,
    ))
    _insert_row(conn, "discovery_evidence", dict(
        evidence_id=ED_ID, claim_id=C2_ID, evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED, confidence_band=ids.CONFIDENCE_BAND_CORROBORATED,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED, audit_status=ids.AUDIT_STATUS_AUDIT_PENDING,
        routing_status=ids.ROUTING_STATUS_SHIPPED, routing_reason=ids.ROUTING_REASON_NONE, is_new=1,
        a_page_id=PAGE2, sys_id=SYS2, span_start=0, span_end=20, trials=2,
        text_layer="htr", snapshot_hash="snap-d1", **_new_evidence_cols,
    ))

    _insert_row(conn, "witness_units", dict(unit_id="u001"))
    _insert_row(conn, "witness_units", dict(unit_id="u002"))
    _insert_row(conn, "witness_unit_members", dict(
        unit_id="u001", sys_id=SYS1, merge_basis=ids.MERGE_BASIS_OXFORD_PART))
    _insert_row(conn, "witness_unit_members", dict(
        unit_id="u002", sys_id=SYS2, merge_basis=ids.MERGE_BASIS_OXFORD_PART))

    _insert_row(conn, "discovery_routing_audit", dict(
        page_id=PAGE1, kept_work_id=WORK1, demoted_work_id=WORK2,
        kept_year=1200, demoted_year=1400, delta_years=200, decision="demoted",
        routing_reason=ids.ROUTING_REASON_LATER_SHARED_TEXT,
    ))
    _insert_row(conn, "discovery_routing_audit", dict(
        page_id=PAGE2, kept_work_id=WORK1, demoted_work_id=(WORK2 if is_new else None),
        kept_year=1200, demoted_year=1200, delta_years=0, decision="kept_tie", routing_reason=None,
    ))

    _insert_row(conn, "band_precision", dict(
        scope="collection", collection_id="propagated_witness_collection_v1",
        evidence_source=None, confidence_band=None, numerator=180, denominator=194,
        precision=0.926, ci_low=0.875, ci_high=0.968, method="held-out-draw",
    ))
    _insert_row(conn, "band_precision", dict(
        scope="band", collection_id="propagated_witness_collection_v1",
        evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED, confidence_band=ids.CONFIDENCE_BAND_CORROBORATED,
        numerator=None, denominator=None, precision=None, ci_low=None, ci_high=None, method=None,
    ))
    _insert_row(conn, "band_precision", dict(
        scope="band", collection_id="e1_certification_registry_v1",
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT, confidence_band=ids.CONFIDENCE_BAND_TIER_A,
        numerator=None, denominator=None, precision=None,
        ci_low=(0.9084 if is_new else None), ci_high=None, method=None,
        measurement_status=("measured_pass" if is_new else None),
    ))


def build_fixture_pair(tmp_path: Path):
    """Build a small "old" sidecar and a "new" one that differs ONLY via
    ALLOWED changes (per docs/specs/discovery-sidecar-schema-v1.md's
    Amendment 2026-08-02)."""
    old_path = tmp_path / "old.db"
    new_path = tmp_path / "new.db"

    old_conn = sqlite3.connect(str(old_path))
    old_conn.execute("PRAGMA foreign_keys = ON")
    sidecar_build.create_schema(old_conn)
    _populate_base_rows(old_conn, is_new=False)
    old_conn.commit()
    old_conn.close()

    new_conn = sqlite3.connect(str(new_path))
    new_conn.execute("PRAGMA foreign_keys = ON")
    sidecar_build.create_schema(new_conn)
    _apply_amendment_columns(new_conn)
    _populate_base_rows(new_conn, is_new=True)
    new_conn.commit()
    new_conn.close()

    return old_path, new_path


@pytest.fixture
def fixture_pair(tmp_path):
    old_path, new_path = build_fixture_pair(tmp_path)
    expected_path = tmp_path / "expected.json"
    rc = vrp.generate_expectation(str(old_path), str(expected_path), allow_overwrite=True)
    assert rc == 0
    return old_path, new_path, expected_path


# ---------------------------------------------------------------------------
# Baseline: the gate PASSES on an allowed-changes-only fixture pair.
# ---------------------------------------------------------------------------

def test_baseline_allowed_changes_only_passes(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[baseline] violations={len(result.violations)}")
    assert result.violations == [], result.violations
    assert result.exit_code == 0
    # Every one of the six core tables reported PASS (auditable, not silent).
    pass_lines = [s for s in result.summary_lines if s.startswith(tuple(vrp.CORE_TABLES)) and "PASS" in s]
    assert len(pass_lines) == len(vrp.CORE_TABLES)


# ---------------------------------------------------------------------------
# Control 1: a single matched_letters cell changed inside a stratum -- the
# case the frame hash provably cannot see, and the specific reason this gate
# exists.
# ---------------------------------------------------------------------------

def test_control_1_matched_letters_drift_invisible_to_frame_hash(fixture_pair):
    old_path, new_path, expected_path = fixture_pair

    conn = sqlite3.connect(str(new_path))
    frame_before = sidecar_build.compute_frame_content_hash(conn)
    conn.execute("UPDATE discovery_evidence SET matched_letters = 999999 WHERE evidence_id = ?", (EA_ID,))
    conn.commit()
    frame_after = sidecar_build.compute_frame_content_hash(conn)
    conn.close()

    # Half 1: the frame hash cannot see the drift.
    assert frame_before == frame_after, (
        "compute_frame_content_hash keys on (page_id, work_id, claim_type, display_evidence_id, "
        "evidence_id, evidence_kind, evidence_source, confidence_band) only -- matched_letters must "
        "be invisible to it"
    )

    # Half 2: THIS gate sees it.
    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 1] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("discovery_evidence" in v and "matched_letters" in v for v in result.violations)
    for v in result.violations:
        assert "999999" not in v


# ---------------------------------------------------------------------------
# Control 2: a row deleted from discovery_evidence.
# ---------------------------------------------------------------------------

def test_control_2_row_deleted_from_evidence(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    conn = sqlite3.connect(str(new_path))
    conn.execute("DELETE FROM discovery_evidence WHERE evidence_id = ?", (ED_ID,))
    conn.commit()
    conn.close()

    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 2] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("discovery_evidence" in v and "row deleted" in v for v in result.violations)


# ---------------------------------------------------------------------------
# Control 3: a row added to discovery_claim.
# ---------------------------------------------------------------------------

def test_control_3_row_added_to_claim(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    conn = sqlite3.connect(str(new_path))
    conn.execute(
        "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, display_evidence_id, "
        "source_corpus, sidecar_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
        # EB_ID, not EA_ID: phase 136 added the UNIQUE index
        # `ux_discovery_claim_display_evidence_id` (a real uniqueness invariant,
        # not a performance hint), so reusing EA_ID — already the display pointer
        # of claim C1 — now fails on INSERT and the control could never reach the
        # assertion it exists to make. EB_ID is a real evidence row in the base
        # fixture that nothing points at yet, so it satisfies both the FK and the
        # new uniqueness constraint.
        (PAGE3, WORK1, ids.claim_id(PAGE3, WORK1), ids.CLAIM_TYPE_DIRECT_WITNESS, EB_ID,
         "sefaria", "fixture-v1"),
    )
    conn.commit()
    conn.close()

    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 3] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("discovery_claim" in v and "row added" in v for v in result.violations)


# ---------------------------------------------------------------------------
# Control 4: a `works` title changed -- also proves no raw cell value is
# echoed even for a genuinely sensitive text field.
# ---------------------------------------------------------------------------

def test_control_4_works_title_changed_no_value_echoed(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    secret_title = "A Totally Different Fabricated Secret Title Nobody Should See In A Violation Message"
    conn = sqlite3.connect(str(new_path))
    conn.execute("UPDATE works SET neutral_title = ? WHERE work_id = ?", (secret_title, WORK1))
    conn.commit()
    conn.close()

    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 4] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("works" in v and "neutral_title" in v for v in result.violations)
    for v in result.violations:
        assert secret_title not in v


# ---------------------------------------------------------------------------
# Control 5: a band_precision change beyond the one authorized tier_a row.
# ---------------------------------------------------------------------------

def test_control_5_band_precision_unauthorized_change(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    conn = sqlite3.connect(str(new_path))
    conn.execute(
        "UPDATE band_precision SET precision = 0.5 WHERE scope='band' AND evidence_source=? "
        "AND confidence_band=?",
        (ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_CORROBORATED),
    )
    conn.commit()
    conn.close()

    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 5] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("band_precision" in v and "precision" in v for v in result.violations)


def test_control_5b_tier_a_precision_may_never_go_non_null(fixture_pair):
    """Even the ONE authorized tier_a row may not smuggle a non-NULL
    precision through the D-02a exception."""
    old_path, new_path, expected_path = fixture_pair
    conn = sqlite3.connect(str(new_path))
    conn.execute(
        "UPDATE band_precision SET precision = 0.9382 WHERE scope='band' AND evidence_source=? "
        "AND confidence_band=?",
        (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A),
    )
    conn.commit()
    conn.close()

    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path))
    print(f"[control 5b] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("band_precision" in v and "tier_a" in v for v in result.violations)


# ---------------------------------------------------------------------------
# Control 6: a graded card's display_evidence_id repointed while all counts
# stay identical.
# ---------------------------------------------------------------------------

def test_control_6_graded_card_display_evidence_repointed(fixture_pair):
    old_path, new_path, expected_path = fixture_pair
    conn = sqlite3.connect(str(new_path))
    conn.execute("UPDATE discovery_claim SET display_evidence_id = ? WHERE claim_id = ?", (EB_ID, C1_ID))
    conn.commit()
    conn.close()

    cards = [{"uid": "card1", "page_id": PAGE1, "canonical_work_id": WORK1}]
    violations, _summary = vrp.check_card_binding(str(old_path), str(new_path), cards)
    print(f"[control 6] card-binding violations={len(violations)}")
    assert violations, "the dedicated card-binding check must independently fire on a repoint"
    assert any("display_evidence_id" in v for v in violations)

    # The full gate (which also runs the generic six-table diff) fails too --
    # a repointed display_evidence_id is not allowlisted on discovery_claim.
    result = vrp.run_verification(str(old_path), str(new_path), str(expected_path), cards=cards)
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Control 7: an expectation file taken from the candidate rather than the
# pinned artifact -- the gate must still fail (closes F-04 by test, not only
# by convention).
# ---------------------------------------------------------------------------

def test_control_7_expectation_sourced_from_candidate_still_fails(fixture_pair):
    old_path, new_path, _expected_path = fixture_pair
    candidate_expected_path = old_path.parent / "candidate_expected.json"
    rc = vrp.generate_expectation(str(new_path), str(candidate_expected_path), allow_overwrite=True)
    assert rc == 0

    result = vrp.run_verification(str(old_path), str(new_path), str(candidate_expected_path))
    print(f"[control 7] violations={len(result.violations)}")
    assert result.exit_code != 0
    assert any("db_content_hash" in v for v in result.violations)


# ---------------------------------------------------------------------------
# Generator discipline: refuse to overwrite without the explicit flag.
# ---------------------------------------------------------------------------

def test_generate_refuses_to_overwrite_without_flag(tmp_path):
    old_path, _new_path = build_fixture_pair(tmp_path)
    out_path = tmp_path / "expected.json"

    rc1 = vrp.generate_expectation(str(old_path), str(out_path))
    assert rc1 == 0

    rc2 = vrp.generate_expectation(str(old_path), str(out_path))
    assert rc2 != 0, "must refuse to overwrite an existing expectation file without the explicit flag"

    rc3 = vrp.generate_expectation(str(old_path), str(out_path), allow_overwrite=True)
    assert rc3 == 0, "must succeed once --regenerate-i-know-what-this-means is given"


# ---------------------------------------------------------------------------
# The committed expectation artifact's provenance (Task 2 acceptance): its
# db_content_hash/frame_content_hash/population_hash/cluster_map_hash equal
# the CERT-01 pre-registration's pinned values for the SAME live asset.
# ---------------------------------------------------------------------------

def test_committed_expectation_matches_cert01_prereg_pinned_values():
    expected_path = PHASE_DIR / "136-REBUILD-PRESERVATION-EXPECTED.json"
    prereg_path = (
        REPO_ROOT / ".planning" / "phases" / "135-precision-certificate-confidence-bands"
        / "cert01_prereg.json"
    )
    if not expected_path.exists() or not prereg_path.exists():
        pytest.skip("committed artifacts not present in this checkout")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    prereg = json.loads(prereg_path.read_text(encoding="utf-8"))
    assert expected["db_content_hash"] == prereg["db_content_hash"]
    assert expected["frame_content_hash"] == prereg["frame_content_hash"]
    assert expected["population_hash"] == prereg["population_hash"]
    assert expected["cluster_map_hash"] == prereg["cluster_map_hash"]
    assert len(expected["table_hashes"]) >= 6


def test_verify_cert01_grading_unmodified():
    """D-02c: scripts/verify_cert01_grading.py's check 10 must stay
    IMMUTABLE -- this plan never touches it."""
    import subprocess
    result = subprocess.run(
        ["git", "diff", "--stat", "scripts/verify_cert01_grading.py"],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    assert result.stdout.strip() == "", f"scripts/verify_cert01_grading.py must be unmodified: {result.stdout}"


def test_allowlist_provenance_clean():
    violations = vrp.check_allowlist_provenance()
    assert violations == [], violations


# ---------------------------------------------------------------------------
# The amendment section must be BOUNDED.
#
# `check_allowlist_provenance` claims to confirm that every allowlisted column
# is cited in ONE dated amendment section. The schema doc carries several
# amendments after that one (2026-08-07 (E), 2026-08-07 (F)+(G), 2026-08-03),
# so a reader from the outside cannot see whether the check is scoped or not --
# both a bounded and an unbounded parser return "clean" against the real doc.
#
# These are the tests that tell the two apart. The mutation is: put a name ONLY
# in a LATER amendment. A bounded parser rejects it; the read-to-EOF parser
# accepts it, and the check silently certifies a provenance it never had.
# ---------------------------------------------------------------------------

_SENTINEL_COLUMN = "sentinel_only_in_a_later_amendment"

_TWO_AMENDMENT_DOC = f"""# Discovery Sidecar Schema v1

## 1. Two-Table Claim Model

Body text that belongs to no amendment.

{vrp.SCHEMA_AMENDMENT_HEADER}

### (A) additions

This section cites `locus_he` and nothing else.

## Amendment 2026-08-07 (F)+(G) -- a LATER, DIFFERENT dated section

This section cites `{_SENTINEL_COLUMN}`, which the earlier section never
mentions. A check scoped to the earlier section must not see it.
"""


def _write_two_amendment_doc(tmp_path) -> str:
    path = tmp_path / "schema-two-amendments.md"
    path.write_text(_TWO_AMENDMENT_DOC, encoding="utf-8")
    return str(path)


def test_amendment_section_stops_at_the_next_amendment(tmp_path):
    """A name in a later dated section is NOT in the requested section's text."""
    doc = _write_two_amendment_doc(tmp_path)
    section = vrp._read_amendment_section_text(doc)
    assert "locus_he" in section, "the requested section's own content must survive"
    assert _SENTINEL_COLUMN not in section, (
        "the section runs to the NEXT heading of the same level, not to EOF -- "
        "otherwise every later amendment is silently folded into this one"
    )


def test_a_column_cited_only_later_fails_the_provenance_check(tmp_path, monkeypatch):
    """The gate, end to end: allowlisting a column documented only in a later
    amendment must be reported as a violation."""
    doc = _write_two_amendment_doc(tmp_path)
    monkeypatch.setattr(
        vrp, "ALLOWED_DIFFERING_COLUMNS",
        {"discovery_evidence": frozenset({_SENTINEL_COLUMN})},
    )
    violations = vrp.check_allowlist_provenance(doc)
    assert len(violations) == 1, violations
    assert _SENTINEL_COLUMN in violations[0]


def test_a_column_cited_in_the_requested_section_still_passes(tmp_path, monkeypatch):
    """The bound must not be so tight that it rejects the real thing -- a
    section parser that returned nothing would pass the two tests above while
    being useless."""
    doc = _write_two_amendment_doc(tmp_path)
    monkeypatch.setattr(
        vrp, "ALLOWED_DIFFERING_COLUMNS",
        {"discovery_evidence": frozenset({"locus_he"})},
    )
    assert vrp.check_allowlist_provenance(doc) == []


def test_read_to_eof_would_have_accepted_the_later_citation(tmp_path):
    """The defect, proven able to fail -- against a LOCAL re-implementation of
    the old behaviour, so the sensitivity of the two tests above is demonstrated
    rather than asserted."""
    doc = _write_two_amendment_doc(tmp_path)
    text = Path(doc).read_text(encoding="utf-8")
    read_to_eof = text[text.find(vrp.SCHEMA_AMENDMENT_HEADER):]      # <- the old parser

    assert _SENTINEL_COLUMN in read_to_eof, "the old parser swallows the later amendment"
    assert _SENTINEL_COLUMN not in vrp._read_amendment_section_text(doc)


def test_a_missing_amendment_header_is_still_an_explicit_failure(tmp_path):
    path = tmp_path / "no-amendment.md"
    path.write_text("# Schema\n\nNo amendment section here.\n", encoding="utf-8")
    with pytest.raises(RuntimeError):
        vrp._read_amendment_section_text(str(path))


def test_the_real_schema_doc_section_is_actually_bounded():
    """Against the committed doc: the section must end before the amendments
    that follow it, and must not be empty."""
    section = vrp._read_amendment_section_text()
    assert section.startswith(vrp.SCHEMA_AMENDMENT_HEADER)
    assert len(section) > 200, "a section parser that returns nothing passes vacuously"
    for later in (
        "## Amendment 2026-08-07 (E)",
        "## Amendment 2026-08-07 (F)+(G)",
        "## Amendment 2026-08-03",
    ):
        assert later not in section, f"{later} must not be folded into the cited section"
