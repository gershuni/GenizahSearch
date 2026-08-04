# -*- coding: utf-8 -*-
"""Tests for `scripts/project_discovery_public.py` -- the VIS-01 closed-graph
public projection (Phase 136, plan 136-08, Tasks 2 and 3).

Every fixture in this file is FABRICATED synthetic test data (small
in-memory/temp SQLite databases built by hand) -- never real corpus content.
The leak-control tests (2/2b) source their seeded "restricted" marker
DYNAMICALLY from the local, gitignored `MASKING_SCAN_PATTERNS_FILE` at test
run time (never hardcoded into this committed file) and never print it.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

import scripts.check_atlas_masking as masking
import scripts.project_discovery_public as proj
import scripts.verify_discovery_sidecar as verify

# A throwaway, disposable pattern for tests that only exercise the masking
# gate's WIRING (argv shape, pass/fail plumbing) and do not need to prove
# detection against a REAL restricted-corpus-shaped string. Mirrors the
# existing convention in tests/test_discovery_build.py.
_DISPOSABLE_PATTERN = "TOTALLY-UNMATCHED-MARKER-XYZ-999"


# ---------------------------------------------------------------------------
# Synthetic private-sidecar schema + fixture builders.
# ---------------------------------------------------------------------------

_SCHEMA_DDL = [
    """CREATE TABLE works (
        work_id TEXT PRIMARY KEY,
        canonical_work_id TEXT NOT NULL,
        neutral_title TEXT NOT NULL,
        author TEXT,
        genre TEXT,
        source_corpus TEXT NOT NULL,
        identity_visibility TEXT NOT NULL
    )""",
    """CREATE TABLE discovery_claim (
        page_id TEXT NOT NULL,
        work_id TEXT NOT NULL,
        claim_id TEXT NOT NULL UNIQUE,
        claim_type TEXT NOT NULL,
        display_evidence_id TEXT NOT NULL,
        source_corpus TEXT NOT NULL,
        sidecar_version TEXT NOT NULL,
        PRIMARY KEY (page_id, work_id)
    )""",
    """CREATE TABLE discovery_evidence (
        evidence_id TEXT PRIMARY KEY,
        claim_id TEXT NOT NULL,
        evidence_kind TEXT NOT NULL,
        evidence_source TEXT NOT NULL,
        confidence_band TEXT NOT NULL,
        adjudication_status TEXT NOT NULL,
        routing_status TEXT NOT NULL,
        routing_reason TEXT,
        is_new INTEGER NOT NULL DEFAULT 0,
        a_page_id TEXT NOT NULL,
        sys_id TEXT NOT NULL,
        span_start INTEGER NOT NULL,
        span_end INTEGER NOT NULL,
        -- 2026-08-03 (136-13): `matched_letters` is read by the production
        -- identification materializer, which the projection now calls instead of
        -- reimplementing the grain. Its absence here is why these fixtures could
        -- not detect that the projection was materializing over a different
        -- evidence population than the private builder.
        matched_letters INTEGER,
        density REAL,
        coverage_ppm INTEGER,
        coverage_status TEXT,
        band_rank INTEGER,
        novelty_status TEXT,
        novelty_source_label TEXT,
        divergence_correctness TEXT,
        assertion_visibility TEXT NOT NULL
    )""",
    "CREATE TABLE witness_units (unit_id TEXT PRIMARY KEY)",
    """CREATE TABLE witness_unit_members (
        unit_id TEXT NOT NULL,
        sys_id TEXT NOT NULL,
        merge_basis TEXT NOT NULL
    )""",
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)",
    """CREATE TABLE band_precision (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        collection_id TEXT NOT NULL,
        evidence_source TEXT,
        confidence_band TEXT,
        numerator INTEGER,
        denominator INTEGER,
        precision REAL,
        ci_low REAL,
        ci_high REAL,
        -- 2026-08-03 (136-13): read by the production materializer's
        -- band-measurement index (the D-02a tier_a authorization pair).
        measurement_status TEXT,
        method TEXT,
        notes TEXT
    )""",
    """CREATE TABLE discovery_routing_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_id TEXT,
        kept_work_id TEXT,
        demoted_work_id TEXT,
        kept_year INTEGER,
        demoted_year INTEGER,
        delta_years INTEGER,
        decision TEXT,
        routing_reason TEXT
    )""",
    """CREATE TABLE discovery_identification (
        identification_id TEXT PRIMARY KEY,
        sys_id TEXT NOT NULL,
        canonical_work_id TEXT NOT NULL,
        display_work_id TEXT NOT NULL,
        main_pool INTEGER NOT NULL,
        main_pool_reason TEXT NOT NULL,
        best_band_rank INTEGER NOT NULL,
        page_count INTEGER NOT NULL,
        max_coverage_ppm INTEGER,
        relation_kind TEXT NOT NULL,
        -- 2026-08-03 (136-13): written by the production materializer; records
        -- WHICH of the two D-13g rules admitted the row.
        eligibility_basis TEXT,
        novelty_status TEXT NOT NULL,
        divergence_correctness TEXT,
        assertion_visibility TEXT NOT NULL,
        identity_visibility TEXT NOT NULL
    )""",
    """CREATE TABLE manuscript_display (
        sys_id TEXT PRIMARY KEY,
        library_code TEXT NOT NULL,
        library_sort_key TEXT NOT NULL,
        shelfmark_display TEXT NOT NULL,
        shelfmark_sort_key TEXT NOT NULL
    )""",
]


def _create_private_schema(conn: sqlite3.Connection) -> None:
    for stmt in _SCHEMA_DDL:
        conn.execute(stmt)


def _insert(conn: sqlite3.Connection, table: str, **row) -> None:
    cols = list(row.keys())
    conn.execute(
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
        [row[c] for c in cols],
    )


def _new_private_conn(tmp_path: Path, name: str = "private.db"):
    path = tmp_path / name
    conn = sqlite3.connect(str(path))
    _create_private_schema(conn)
    return path, conn


def _add_work(conn, work_id, *, canonical_work_id=None, source_corpus="sefaria",
              identity_visibility="public", title=None):
    _insert(
        conn, "works",
        work_id=work_id,
        canonical_work_id=canonical_work_id or work_id,
        neutral_title=title or f"Title of {work_id}",
        author=None, genre=None,
        source_corpus=source_corpus,
        identity_visibility=identity_visibility,
    )


def _add_claim_with_evidence(
    conn, *, claim_id, page_id, work_id, evidence_id, sys_id,
    a_page_id=None, assertion_visibility="public",
    evidence_source="track1_direct", confidence_band="tier_a",
    band_rank=2, coverage_ppm=500000, adjudication_status="unreviewed",
    routing_status="shipped", claim_type="direct_witness",
    novelty_status="not_checked", divergence_correctness=None,
    claim_source_corpus="sefaria",
):
    a_page_id = a_page_id or page_id
    _insert(
        conn, "discovery_claim",
        page_id=page_id, work_id=work_id, claim_id=claim_id,
        claim_type=claim_type, display_evidence_id=evidence_id,
        source_corpus=claim_source_corpus, sidecar_version="v1-test",
    )
    _insert(
        conn, "discovery_evidence",
        evidence_id=evidence_id, claim_id=claim_id,
        evidence_kind="witness", evidence_source=evidence_source,
        confidence_band=confidence_band, adjudication_status=adjudication_status,
        routing_status=routing_status, routing_reason="none", is_new=0,
        a_page_id=a_page_id, sys_id=sys_id, span_start=0, span_end=100,
        coverage_ppm=coverage_ppm, coverage_status="measured", band_rank=band_rank,
        novelty_status=novelty_status, novelty_source_label=None,
        divergence_correctness=divergence_correctness,
        assertion_visibility=assertion_visibility,
    )


def _add_meta(conn, **kv):
    for k, v in kv.items():
        _insert(conn, "meta", key=k, value=(str(v) if v is not None else None))


def _finalize(conn) -> None:
    """Commit, and materialize the PRIVATE fixture's `discovery_identification`
    with the same production rule the projection now uses.

    2026-08-03 (136-13): these fixtures previously left the private
    identification table empty (or hand-written), so nothing here could notice
    that the projection was deriving its own table over a DIFFERENT evidence
    population than the private builder -- the defect that shipped a public
    artifact with 95,149 identification rows against a 64,522-row private
    superset. Building both sides with one rule makes the fixture internally
    consistent, which is what lets `check_identification_key_subset` mean
    anything here."""
    conn.commit()
    import build_discovery_sidecar as builder

    try:
        builder.populate_discovery_identification(conn)
    except Exception:
        # A control fixture may be deliberately malformed (an orphan work, a
        # broken FK). Those tests assert on the projection's own failure, not on
        # the identification grain, so a materializer refusal here is not the
        # property under test.
        conn.rollback()
    conn.commit()
    conn.close()


def _run_projection(tmp_path, private_path, name="public.db", masking_patterns=None):
    out_path = tmp_path / name
    patterns = masking_patterns if masking_patterns is not None else [_DISPOSABLE_PATTERN]
    report = proj.project(str(private_path), str(out_path), masking_patterns=patterns)
    return out_path, report


def _open_ro(path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# A pytest fixture that sources a REAL restricted pattern from the local,
# gitignored MASKING_SCAN_PATTERNS_FILE at test-run time -- never hardcoded,
# never printed. Skips (gracefully) when the file is unavailable in this
# environment, per the plan's own portability discipline.
# ---------------------------------------------------------------------------

# A synthetic marker that is ALWAYS available. The two leak controls below
# exercise identical code paths with it, so their logic runs in every
# environment including CI -- see the note on `real_masking_pattern`.
PORTABLE_MASKING_PATTERN = "SYNTHETIC-LEAK-MARKER-3ZP77"


@pytest.fixture
def portable_masking_pattern():
    return PORTABLE_MASKING_PATTERN


@pytest.fixture
def real_masking_pattern():
    """A REAL restricted pattern, sourced from the configured, gitignored
    MASKING_SCAN_PATTERNS_FILE at run time -- never hardcoded, never printed.

    Honours whatever the environment configures. It previously OVERRODE the env
    with a hardcoded workstation path (`C:/Genizahsearch/.masking_patterns`),
    which meant that on any other machine -- CI included -- both leak controls
    skipped and never executed at all (Codex code review 2026-08-03, finding 9).
    A security control that silently does not run is the failure mode this whole
    phase keeps producing.

    Skipping here is now safe rather than silent, because each control has a
    `_portable` twin that runs unconditionally on PORTABLE_MASKING_PATTERN and
    covers the same code path. This fixture adds the extra assurance that the
    machinery also works against a genuine restricted string, on machines that
    have one."""
    patterns = masking.load_patterns()
    if not patterns:
        pytest.skip(
            "no MASKING_SCAN_PATTERNS_FILE configured in this environment -- the "
            "same code path is covered unconditionally by the *_portable twin"
        )
    return patterns[0]


# ---------------------------------------------------------------------------
# --help documents both positional paths (Task 2 acceptance).
# ---------------------------------------------------------------------------

def test_help_documents_both_positional_paths(capsys):
    parser = proj.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--help"])
    out = capsys.readouterr().out
    assert "private_db" in out
    assert "public_db_out" in out


# ---------------------------------------------------------------------------
# Baseline: the four measured shapes -- projection keeps EXACTLY the fully
# open rows.
# ---------------------------------------------------------------------------

def test_baseline_keeps_exactly_the_fully_open_rows(tmp_path):
    path, conn = _new_private_conn(tmp_path)

    _add_work(conn, "W_OPEN", source_corpus="sefaria", identity_visibility="public")
    _add_work(conn, "W_PRIVATE", source_corpus="msource", identity_visibility="private")

    # Shape 1: open work, open assertion -- SURVIVES.
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1",
        sys_id="SYS1", assertion_visibility="public", claim_source_corpus="sefaria",
    )
    # Shape 2: open work, restricted assertion -- EXCLUDED.
    _add_claim_with_evidence(
        conn, claim_id="C2", page_id="P2", work_id="W_OPEN", evidence_id="E2",
        sys_id="SYS2", assertion_visibility="private", claim_source_corpus="sefaria",
    )
    # Shape 3: restricted work, open assertion -- EXCLUDED.
    _add_claim_with_evidence(
        conn, claim_id="C3", page_id="P3", work_id="W_PRIVATE", evidence_id="E3",
        sys_id="SYS3", assertion_visibility="public", claim_source_corpus="msource",
    )
    # Shape 4: restricted work, restricted assertion -- EXCLUDED.
    _add_claim_with_evidence(
        conn, claim_id="C4", page_id="P4", work_id="W_PRIVATE", evidence_id="E4",
        sys_id="SYS4", assertion_visibility="private", claim_source_corpus="msource",
    )
    _add_meta(conn, audience="private", schema_version="test-v1")
    _finalize(conn)

    out_path, report = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)

    claim_ids = {r["claim_id"] for r in out_conn.execute("SELECT claim_id FROM discovery_claim")}
    evidence_ids = {r["evidence_id"] for r in out_conn.execute("SELECT evidence_id FROM discovery_evidence")}
    work_ids = {r["work_id"] for r in out_conn.execute("SELECT work_id FROM works")}

    assert claim_ids == {"C1"}
    assert evidence_ids == {"E1"}
    assert work_ids == {"W_OPEN"}


# ---------------------------------------------------------------------------
# Control 1: structural absence -- the private row's PK does not exist at
# all in the public database (not merely flagged).
# ---------------------------------------------------------------------------

def test_control1_structural_absence_of_private_rows(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_work(conn, "W_PRIVATE", source_corpus="msource", identity_visibility="private")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_claim_with_evidence(
        conn, claim_id="C2", page_id="P2", work_id="W_PRIVATE", evidence_id="E2", sys_id="SYS2",
        assertion_visibility="public", claim_source_corpus="msource",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)

    # NOT "is there a private flag set" -- the row's PRIMARY KEY must not
    # exist in the table AT ALL.
    assert out_conn.execute("SELECT 1 FROM works WHERE work_id='W_PRIVATE'").fetchone() is None
    assert out_conn.execute("SELECT 1 FROM discovery_claim WHERE claim_id='C2'").fetchone() is None
    assert out_conn.execute("SELECT 1 FROM discovery_evidence WHERE evidence_id='E2'").fetchone() is None
    # The public row IS present.
    assert out_conn.execute("SELECT 1 FROM works WHERE work_id='W_OPEN'").fetchone() is not None


# ---------------------------------------------------------------------------
# Control 2: leak control, CELL level -- a restricted-corpus marker seeded
# into a projected (surviving) work's title fails the masking gate.
# ---------------------------------------------------------------------------

def _assert_cell_level_leak_fails_the_gate(tmp_path, marker):
    path, conn = _new_private_conn(tmp_path)
    # The marker is embedded in a work that WILL survive projection.
    _add_work(conn, "W_OPEN", identity_visibility="public",
              title=f"Some title containing {marker} inline")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path = tmp_path / "public.db"
    with pytest.raises(proj.ProjectionError) as excinfo:
        proj.project(str(path), str(out_path), masking_patterns=[marker])

    assert not out_path.exists(), "a masking-dirty artifact must be REMOVED, never left on disk"
    match = re.search(r"issue_count=(\d+)", str(excinfo.value))
    assert match is not None, "the failure must record how many findings the gate produced"
    issue_count = int(match.group(1))
    assert issue_count >= 1


def test_control2_cell_level_leak_fails_the_gate_portable(tmp_path, portable_masking_pattern):
    """Runs everywhere, CI included."""
    _assert_cell_level_leak_fails_the_gate(tmp_path, portable_masking_pattern)


def test_control2_cell_level_leak_fails_the_gate(tmp_path, real_masking_pattern):
    """Same path, against a genuine restricted string where one is configured."""
    _assert_cell_level_leak_fails_the_gate(tmp_path, real_masking_pattern)


# ---------------------------------------------------------------------------
# Control 2b: leak control, SCHEMA level -- a marker seeded into a COLUMN
# NAME is caught by --scan-sqlite, demonstrating schema-level coverage
# distinct from cell-level content scanning.
# ---------------------------------------------------------------------------

def _assert_schema_level_leak_caught(tmp_path, marker):
    db_path = tmp_path / "schema_leak.db"
    conn = sqlite3.connect(str(db_path))
    quoted_marker = marker.replace('"', '""')
    conn.execute(f'CREATE TABLE t (id INTEGER, "{quoted_marker}" TEXT)')
    conn.execute("INSERT INTO t (id) VALUES (1)")
    conn.commit()
    conn.close()

    issues = masking.scan_sqlite(str(db_path), [marker])
    assert len(issues) >= 1
    assert any("::schema" in issue.path for issue in issues), (
        "a marker embedded ONLY in a column name must be caught via the "
        "schema (sqlite_master.sql) surface"
    )


def test_control2b_schema_level_leak_caught_by_scan_sqlite_portable(
        tmp_path, portable_masking_pattern):
    """Runs everywhere, CI included."""
    _assert_schema_level_leak_caught(tmp_path, portable_masking_pattern)


def test_control2b_schema_level_leak_caught_by_scan_sqlite(tmp_path, real_masking_pattern):
    """Same path, against a genuine restricted string where one is configured."""
    _assert_schema_level_leak_caught(tmp_path, real_masking_pattern)


def test_masking_gate_argv_always_includes_both_scan_flags():
    argv = proj._masking_gate_argv("some/path.db")
    assert "--strict" in argv
    assert "--scan-repo" in argv
    assert "--scan-asset" in argv
    assert "--scan-sqlite" in argv


# ---------------------------------------------------------------------------
# Control 3: fail-closed -- an unset MASKING_SCAN_PATTERNS_FILE causes a
# nonzero exit, never a silent pass. This is the intended fail-SAFE, and
# this test says so explicitly.
# ---------------------------------------------------------------------------

def test_control3_fail_closed_when_patterns_file_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("MASKING_SCAN_PATTERNS_FILE", raising=False)
    db_path = tmp_path / "dummy.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (x TEXT)")
    conn.commit()
    conn.close()

    # Intended fail-SAFE, not a bug: with no patterns file configured, the
    # scan must refuse to run (a zero-pattern scan is the canonical
    # false-green) rather than silently reporting "clean".
    result = masking.main(["--scan-asset", str(db_path)])
    assert result != 0


# ---------------------------------------------------------------------------
# Control 4: orphan control -- removing a work referenced by a surviving
# claim makes the FK-closure check fail.
# ---------------------------------------------------------------------------

def test_control4_orphan_work_fails_fk_closure(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)

    # A GOOD artifact has zero FK-closure violations.
    out_conn = _open_ro(out_path)
    assert proj.check_fk_closure(out_conn) == []
    out_conn.close()

    # Now corrupt it: remove the work a surviving claim references.
    broken_conn = sqlite3.connect(str(out_path))
    broken_conn.execute("DELETE FROM works WHERE work_id='W_OPEN'")
    broken_conn.commit()

    violations = proj.check_fk_closure(broken_conn)
    assert violations, "removing a referenced work must be reported as a dangling FK"
    broken_conn.close()


# ---------------------------------------------------------------------------
# Control 5: copied-total control -- an un-recomputed stored total is
# reported.
# ---------------------------------------------------------------------------

def test_control5_copied_total_is_reported(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)

    out_conn = _open_ro(out_path)
    assert proj.check_meta_counts(out_conn) == []
    out_conn.close()

    # Simulate a build that forgot to recompute a stored total -- leave it
    # at a value that does NOT match the actual projected row count.
    broken_conn = sqlite3.connect(str(out_path))
    broken_conn.execute(
        "UPDATE meta SET value = '999' WHERE key = 'expected_rows_claims'"
    )
    broken_conn.commit()
    violations = proj.check_meta_counts(broken_conn)
    assert violations, "a stale (un-recomputed) stored total must be reported"
    assert any("expected_rows_claims" in v for v in violations)
    broken_conn.close()


# ---------------------------------------------------------------------------
# Control 6: unprojected-table control -- a table with no projection rule
# is a build error.
# ---------------------------------------------------------------------------

def test_control6_unprojected_table_is_a_build_error(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_meta(conn, audience="private")
    conn.execute("CREATE TABLE mystery_table_no_rule (id INTEGER)")
    _finalize(conn)

    out_path = tmp_path / "public.db"
    with pytest.raises(proj.ProjectionError) as excinfo:
        proj.project(str(path), str(out_path), masking_patterns=[_DISPOSABLE_PATTERN])
    assert "mystery_table_no_rule" in str(excinfo.value)
    assert not out_path.exists()


# ---------------------------------------------------------------------------
# Task 2 acceptance: meta counts recomputed (not copied), discovery_identification
# recomputed from surviving public claims only, display_work_id recomputed,
# the reconciliation report, and meta.audience.
# ---------------------------------------------------------------------------

def test_meta_counts_are_recomputed_over_projected_rows_not_copied(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_work(conn, "W_PRIVATE", source_corpus="msource", identity_visibility="private")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_claim_with_evidence(
        conn, claim_id="C2", page_id="P2", work_id="W_PRIVATE", evidence_id="E2", sys_id="SYS2",
        assertion_visibility="public", claim_source_corpus="msource",
    )
    # A private-side stored total reflecting BOTH claims -- the output must
    # NOT copy this; it must recompute over the ONE surviving claim.
    _add_meta(conn, audience="private", expected_rows_claims=2, expected_rows_evidence=2,
              expected_rows_works=2)
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)
    meta = {r["key"]: r["value"] for r in out_conn.execute("SELECT key, value FROM meta")}
    assert meta["expected_rows_claims"] == "1"
    assert meta["expected_rows_evidence"] == "1"
    assert meta["expected_rows_works"] == "1"
    actual_claim_count = out_conn.execute("SELECT COUNT(*) FROM discovery_claim").fetchone()[0]
    assert actual_claim_count == 1
    assert int(meta["expected_rows_claims"]) == actual_claim_count


def test_meta_audience_public_on_output_private_on_input(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    # The PRIVATE input carries 'private'.
    private_conn = sqlite3.connect(str(path))
    private_meta = {r[0]: r[1] for r in private_conn.execute("SELECT key, value FROM meta")}
    assert private_meta["audience"] == "private"
    private_conn.close()

    out_path, _ = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)
    out_meta = {r["key"]: r["value"] for r in out_conn.execute("SELECT key, value FROM meta")}
    assert out_meta["audience"] == "public"


def test_discovery_identification_recomputed_from_public_claims_only(tmp_path):
    """A fixture where a PRIVATE claim would change the bucket/reason/page
    count if it were (wrongly) included -- the public identification row
    must reflect ONLY the surviving public claim."""
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", canonical_work_id="W_OPEN", identity_visibility="public")
    _add_work(conn, "W_PRIVATE_SAME_CANON", canonical_work_id="W_OPEN",
              source_corpus="msource", identity_visibility="private")

    # A single public claim on ONE page -- alone this is NOT multi-folio.
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public", claim_source_corpus="sefaria",
    )
    # A PRIVATE claim on a SECOND page of the SAME manuscript/canonical work
    # -- if wrongly included, page_count would become 2 (multi-folio, a
    # DIFFERENT bucket/reason) and best_band_rank/max_coverage_ppm would
    # blend in a private contribution.
    _add_claim_with_evidence(
        conn, claim_id="C2", page_id="P2", work_id="W_OPEN", evidence_id="E2", sys_id="SYS1",
        assertion_visibility="private", claim_source_corpus="sefaria",
        band_rank=1, coverage_ppm=999999,
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)
    rows = out_conn.execute(
        "SELECT * FROM discovery_identification WHERE sys_id='SYS1' AND canonical_work_id='W_OPEN'"
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    # page_count must reflect ONLY the surviving public claim's page (1),
    # never the private claim's second page.
    assert row["page_count"] == 1
    assert row["main_pool_reason"] != "main_multifolio"
    # best_band_rank/max_coverage_ppm must come from the SURVIVING evidence
    # (band_rank=2, coverage_ppm=500000), never the private row's stronger
    # values (band_rank=1, coverage_ppm=999999).
    assert row["best_band_rank"] == 2
    assert row["max_coverage_ppm"] == 500000


def test_display_work_id_resolves_to_public_representative_when_anchor_private(tmp_path):
    """The canonical ANCHOR (work_id == canonical_work_id) is itself
    private, but another member of the same canonical group is public and
    has a surviving claim -- display_work_id must resolve to the PUBLIC
    member, never dangle, and never point at the private anchor."""
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "ANCHOR_PRIVATE", canonical_work_id="ANCHOR_PRIVATE",
              source_corpus="msource", identity_visibility="private")
    _add_work(conn, "MEMBER_PUBLIC", canonical_work_id="ANCHOR_PRIVATE",
              source_corpus="sefaria", identity_visibility="public")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="MEMBER_PUBLIC", evidence_id="E1",
        sys_id="SYS1", assertion_visibility="public", claim_source_corpus="sefaria",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, _ = _run_projection(tmp_path, path)
    out_conn = _open_ro(out_path)

    ident_rows = out_conn.execute("SELECT * FROM discovery_identification").fetchall()
    assert len(ident_rows) == 1
    assert ident_rows[0]["display_work_id"] == "MEMBER_PUBLIC"

    # And it must never dangle: display_work_id must exist in the public
    # works table.
    work_ids = {r["work_id"] for r in out_conn.execute("SELECT work_id FROM works")}
    assert ident_rows[0]["display_work_id"] in work_ids
    assert "ANCHOR_PRIVATE" not in work_ids


def test_reconciliation_report_lists_per_table_and_launch_scope(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_work(conn, "W_PRIVATE", source_corpus="msource", identity_visibility="private")
    _add_claim_with_evidence(
        conn, claim_id="C1", page_id="P1", work_id="W_OPEN", evidence_id="E1", sys_id="SYS1",
        assertion_visibility="public", evidence_source="propagated", claim_source_corpus="sefaria",
    )
    _add_claim_with_evidence(
        conn, claim_id="C2", page_id="P2", work_id="W_PRIVATE", evidence_id="E2", sys_id="SYS2",
        assertion_visibility="public", evidence_source="propagated", claim_source_corpus="msource",
    )
    _add_meta(conn, audience="private")
    _finalize(conn)

    out_path, report = _run_projection(tmp_path, path)

    assert "per_table" in report
    assert "works" in report["per_table"]
    assert report["per_table"]["works"]["private_count"] == 2
    assert report["per_table"]["works"]["public_count"] == 1
    assert report["per_table"]["works"]["delta"] == 1

    lsr = report["launch_scope_reconciliation"]
    assert lsr["total_rows"] == 2
    # Both rows are `propagated` -- VIS-01's shortcut includes BOTH
    # regardless of corpus, but the conjunction excludes the msource one.
    assert lsr["vis01_launch_scope_count"] == 2
    assert lsr["conjunction_count"] == 1
    assert lsr["symmetric_difference_count"] == 1

    # The report is also written to disk.
    report_path = Path(str(out_path) + ".reconciliation.json")
    assert report_path.exists()


# ---------------------------------------------------------------------------
# Output-path safety.
# ---------------------------------------------------------------------------

def test_output_path_refused_inside_web_static(tmp_path):
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_OPEN", identity_visibility="public")
    _add_meta(conn, audience="private")
    _finalize(conn)

    bad_out = tmp_path / "web" / "static" / "discovery_public.db"
    with pytest.raises(proj.ProjectionError):
        proj.project(str(path), str(bad_out), masking_patterns=[_DISPOSABLE_PATTERN])
    assert not bad_out.exists()


# ---------------------------------------------------------------------------
# 136-13 gate-5 regressions. Both defects below shipped because no fixture in
# this suite ever built the shape that exposes them: the suite covered the four
# VIS-01 visibility combinations it was scoped to prove, and these two failures
# live in the INTERACTION between VIS-01 and rules owned by other plans
# (136-11's D-13g eligibility rule; Phase-135's later_shared_text invariant).
# ---------------------------------------------------------------------------

def test_review_only_evidence_does_not_create_a_public_identification(tmp_path):
    """D-13g: identification eligibility is `shipped` OR `human_confirmed`.

    A review_only, unreviewed evidence row can be perfectly VIS-01-public and
    still must not mint an identification. The projection used to derive the
    grain over every surviving row, producing more public identifications than
    the private asset it came from."""
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_PUB")
    _add_claim_with_evidence(
        conn, claim_id="C_SHIP", page_id="P1", work_id="W_PUB",
        evidence_id="E_SHIP", sys_id="SYS_A", routing_status="shipped",
    )
    _add_claim_with_evidence(
        conn, claim_id="C_REVIEW", page_id="P2", work_id="W_PUB",
        evidence_id="E_REVIEW", sys_id="SYS_B",
        routing_status="review_only", adjudication_status="unreviewed",
    )
    _add_meta(conn, audience="private", schema_version="discovery-v1")
    _finalize(conn)
    conn.close()

    out = tmp_path / "public.db"
    proj.project(str(path), str(out), masking_patterns=[_DISPOSABLE_PATTERN])
    out_conn = sqlite3.connect(str(out))
    try:
        sys_ids = {r[0] for r in out_conn.execute(
            "SELECT sys_id FROM discovery_identification")}
        # the review_only row's manuscript survives as EVIDENCE ...
        assert out_conn.execute(
            "SELECT COUNT(*) FROM discovery_evidence WHERE sys_id='SYS_B'"
        ).fetchone()[0] == 1
        # ... but must NOT appear as an identification
        assert "SYS_A" in sys_ids
        assert "SYS_B" not in sys_ids, (
            "a review_only/unreviewed row minted a public identification -- the "
            "D-13g eligibility rule was not applied"
        )
    finally:
        out_conn.close()


def test_later_shared_text_evidence_is_pruned_when_its_demotion_cannot_be_published(tmp_path):
    """Owner ruling 2026-08-03: an evidence row whose `later_shared_text` reason
    is backed by an audit row naming a NON-PUBLIC work must not be published.

    The audit row is correctly withheld (publishing it would disclose the
    restricted work's identity); the evidence row that cites it must go too,
    or the artifact asserts a routing decision it cannot substantiate."""
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_PUB")
    _add_work(conn, "W_HIDDEN", identity_visibility="private", source_corpus="msource")
    _add_claim_with_evidence(
        conn, claim_id="C_KEEP", page_id="P_OK", work_id="W_PUB",
        evidence_id="E_KEEP", sys_id="SYS_OK",
    )
    _add_claim_with_evidence(
        conn, claim_id="C_LST", page_id="P_LST", work_id="W_PUB",
        evidence_id="E_LST", sys_id="SYS_LST",
    )
    conn.execute(
        "UPDATE discovery_evidence SET routing_reason='later_shared_text' "
        "WHERE evidence_id='E_LST'"
    )
    # the demotion that explains it names a work that cannot be published
    _insert(
        conn, "discovery_routing_audit",
        page_id="P_LST", kept_work_id="W_PUB", demoted_work_id="W_HIDDEN",
        kept_year=900, demoted_year=1400, delta_years=500, decision="demoted",
    )
    _add_meta(conn, audience="private", schema_version="discovery-v1")
    _finalize(conn)
    conn.close()

    out = tmp_path / "public.db"
    report = proj.project(str(path), str(out), masking_patterns=[_DISPOSABLE_PATTERN])
    out_conn = sqlite3.connect(str(out))
    try:
        surviving = {r[0] for r in out_conn.execute(
            "SELECT evidence_id FROM discovery_evidence")}
        assert "E_KEEP" in surviving
        assert "E_LST" not in surviving, (
            "a later_shared_text row survived without a publishable demotion -- "
            "the public artifact cannot substantiate its own routing reason"
        )
        # and the hidden work never appears
        assert not out_conn.execute(
            "SELECT 1 FROM works WHERE work_id='W_HIDDEN'").fetchone()
        assert not out_conn.execute(
            "SELECT 1 FROM discovery_routing_audit WHERE demoted_work_id='W_HIDDEN'"
        ).fetchone()
    finally:
        out_conn.close()
    assert report["pruned_unreplayable_evidence"] == 1, (
        "the prune must be reported in the OFFLINE reconciliation report"
    )


def test_page_with_two_demotions_does_not_vouch_for_the_wrong_work(tmp_path):
    """The false-green the page-only closure key allowed (Codex code review
    2026-08-03, finding 2).

    Setup: ONE page carrying TWO demotions.
      * (P_SHARED, demoted=W_A) -- kept_work_id names a PRIVATE work, so the
        audit row cannot be published. E_A cites this decision.
      * (P_SHARED, demoted=W_C) -- both sides public, so this row IS published.

    Keyed by page alone, the second row makes P_SHARED "replayable" and E_A
    survives -- asserting a routing decision the artifact does not contain.
    Keyed by (page, demoted canonical work), E_A is correctly pruned while the
    legitimately-backed evidence on the same page is untouched.

    The pre-existing prune test cannot express this: it puts exactly one
    demotion on its page, so the two keys agree there by construction.
    """
    path, conn = _new_private_conn(tmp_path)
    _add_work(conn, "W_A")
    _add_work(conn, "W_B")
    _add_work(conn, "W_C")
    _add_work(conn, "W_HIDDEN", identity_visibility="private", source_corpus="msource")

    # E_A: later_shared_text, backed ONLY by the unpublishable demotion.
    _add_claim_with_evidence(
        conn, claim_id="C_A", page_id="P_SHARED", work_id="W_A",
        evidence_id="E_A", sys_id="SYS_A",
    )
    conn.execute(
        "UPDATE discovery_evidence SET routing_reason='later_shared_text' "
        "WHERE evidence_id='E_A'"
    )
    # W_B and W_C need surviving public claims to count as public works.
    _add_claim_with_evidence(
        conn, claim_id="C_B", page_id="P_OTHER", work_id="W_B",
        evidence_id="E_B", sys_id="SYS_B",
    )
    _add_claim_with_evidence(
        conn, claim_id="C_C", page_id="P_OTHER2", work_id="W_C",
        evidence_id="E_C", sys_id="SYS_C",
    )

    # Row 1 -- explains E_A, but names a private work as kept => withheld.
    _insert(
        conn, "discovery_routing_audit",
        page_id="P_SHARED", kept_work_id="W_HIDDEN", demoted_work_id="W_A",
        kept_year=900, demoted_year=1400, delta_years=500, decision="demoted",
    )
    # Row 2 -- same page, unrelated to E_A, fully publishable.
    _insert(
        conn, "discovery_routing_audit",
        page_id="P_SHARED", kept_work_id="W_B", demoted_work_id="W_C",
        kept_year=800, demoted_year=1500, delta_years=700, decision="demoted",
    )
    _add_meta(conn, audience="private", schema_version="discovery-v1")
    _finalize(conn)
    conn.close()

    out = tmp_path / "public.db"
    report = proj.project(str(path), str(out), masking_patterns=[_DISPOSABLE_PATTERN])
    out_conn = sqlite3.connect(str(out))
    try:
        surviving = {r[0] for r in out_conn.execute(
            "SELECT evidence_id FROM discovery_evidence")}
        assert "E_A" not in surviving, (
            "a publishable demotion on the SAME PAGE vouched for evidence whose own "
            "backing demotion was withheld -- the closure is keyed by page instead "
            "of by (page, demoted work)"
        )
        assert {"E_B", "E_C"} <= surviving, (
            "the tightened key over-pruned: evidence with no later_shared_text "
            "reason at all was dropped"
        )
        # the publishable demotion is still there; the withheld one is not
        kept_audit = {
            (r[0], r[1]) for r in out_conn.execute(
                "SELECT page_id, demoted_work_id FROM discovery_routing_audit")
        }
        assert ("P_SHARED", "W_C") in kept_audit
        assert ("P_SHARED", "W_A") not in kept_audit
        assert not out_conn.execute(
            "SELECT 1 FROM works WHERE work_id='W_HIDDEN'").fetchone()
    finally:
        out_conn.close()
    assert report["pruned_unreplayable_evidence"] == 1


def test_verifier_closure_keys_on_the_claim_page_and_the_canonical_work(tmp_path):
    """The VERIFIER half of the same closure, which the sibling test above does
    not reach (Codex code review 2A, finding 2).

    That test exercises `project()` only, and builds its rows with equal page
    fields and self-canonical work IDs -- so it passes whether the verifier keys
    on `dc.page_id` or `de.a_page_id`, and whether it resolves canonical identity
    or not. It could not detect a revert.

    This one separates all three:
      * `de.a_page_id` deliberately DIFFERS from `dc.page_id`, so reading the
        evidence's page finds no audit row and the check fires spuriously;
      * `work_id != canonical_work_id`, so skipping the canonical resolution
        finds no audit row either;
      * the audit row is keyed on the CLAIM page + the CANONICAL work, which is
        what the projection uses.

    A correct verifier reports nothing here. Either half of the bug reports a
    violation on a well-formed artifact.
    """
    path, conn = _new_private_conn(tmp_path)
    # W_ALIAS collapses into W_CANON -- the 15 such works in the live artifact
    # are why this mapping is load-bearing rather than decorative.
    _add_work(conn, "W_CANON")
    _add_work(conn, "W_ALIAS", canonical_work_id="W_CANON")
    _add_claim_with_evidence(
        conn, claim_id="C_X", page_id="P_CLAIM", work_id="W_ALIAS",
        evidence_id="E_X", sys_id="SYS_X",
    )
    conn.execute(
        "UPDATE discovery_evidence SET routing_reason='later_shared_text', "
        "a_page_id='P_EVIDENCE_DIFFERENT' WHERE evidence_id='E_X'"
    )
    _insert(
        conn, "discovery_routing_audit",
        page_id="P_CLAIM", kept_work_id="W_CANON", demoted_work_id="W_CANON",
        kept_year=900, demoted_year=1400, delta_years=500, decision="demoted",
    )
    _add_meta(conn, audience="private", schema_version="discovery-v1")
    _finalize(conn)  # commits, materializes identifications, and CLOSES

    check_conn = sqlite3.connect(str(path))
    try:
        violations = verify.check_unknown_date_never_demoted(check_conn)
    finally:
        check_conn.close()

    assert violations == [], (
        "the verifier disagreed with the projection on a well-formed artifact. "
        "It must key on the CLAIM's page (dc.page_id) and the work's CANONICAL "
        "id -- reading de.a_page_id, or skipping the canonical resolution, makes "
        f"the two sides non-equivalent. Violations:\n{violations}"
    )
