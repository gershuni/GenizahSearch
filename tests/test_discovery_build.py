# -*- coding: utf-8 -*-
"""Real-mode distillation tests (Phase 134, plan 134-04).

Every fixture in this file is FABRICATED test data (small in-memory/temp
sqlite tables + hand-written JSONL-shaped dicts) -- NEVER real research
content. Raw `work_id` test values use an obviously-synthetic `raw:` prefix
(never a real `M:`/`J:`/`REF` token); the single genre-taxonomy category
label used below (`ספרות יפה` = "belles-lettres") is a standard Hebrew
bibliographic classification term, not a corpus name or siglum (see
`scripts/build_discovery_sidecar.py::_GENRE_CLASS_LITERARY_KEEP`). CI never
touches the gitignored research tree -- everything here is self-contained.
"""
import csv
import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts import discovery_ids as ids
from scripts import verify_discovery_sidecar as verify_mod
from shared.discovery_band_labels import is_default_eligible

# `build_discovery_sidecar.py` imports `check_atlas_masking` FLAT (via its own
# sys.path insertion, not as `scripts.check_atlas_masking`) so its
# `MaskingGateFailure`/`ScanError` handling shares ONE module identity with
# the script. Import it the SAME way here so `pytest.raises(cam.ScanError)`
# matches the exact exception class `finalize_build` raises internally
# (two different import paths for the same file would otherwise create two
# distinct-identity `ScanError` classes).
_SCRIPTS_DIR = str(Path(__file__).resolve().parent.parent / "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
import check_atlas_masking as cam  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _mk_track1_row(page_id, sys_id, work_id, cat, *, genre=None, author=None, title=None,
                    matched_letters=10, best_density=0.5, n_spans=1,
                    spans_json="[[0, 10, 0.5]]", shadowed_by=None):
    return (page_id, sys_id, work_id, cat, genre, author, title, None,
            matched_letters, best_density, n_spans, spans_json, shadowed_by)


def _build_track1_db(tmp_path, rows, name="research.db"):
    """Small sqlite db with a `track1_matches` (+ empty `pages`) table shaped
    exactly like the real research DB's schema (verified column set)."""
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE track1_matches (
          page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT, author TEXT,
          title TEXT, mesirah TEXT, matched_letters INT, best_density REAL, n_spans INT,
          spans_json TEXT, shadowed_by TEXT
        );
        CREATE TABLE pages (
          page_id TEXT PRIMARY KEY, sys_id TEXT, buckets TEXT, n_chars INTEGER,
          text TEXT, provenance TEXT, fgp_id INTEGER, fgp_score REAL, htr_n_chars INTEGER
        );
        """
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    conn.commit()
    conn.close()
    return db_path


def _pages_conn(pages):
    """`pages`: list of (page_id, provenance, text) triples."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE pages (page_id TEXT PRIMARY KEY, provenance TEXT, text TEXT)")
    conn.executemany("INSERT INTO pages VALUES (?, ?, ?)", pages)
    return conn


def _write_approved_csv(path, rows):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sidecar_build.APPROVED_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def _build_fjms_db(tmp_path, *, domains=(), catalog=(), titles=(), persons=(), name="fjms.db"):
    """Tiny in-repo-shape fjms_enrichment.db slice for FJMS-enrichment tests.

    Every value here is FABRICATED (synthetic domain/author labels) -- never
    real FJMS content. Schema mirrors ONLY the columns the enrichment queries
    touch:
      domains(AlmaId, Domain, DomainHeb, ParentDomain)
      catalog(AlmaId, GenizahTitleId, Author, CopyName)  -- CopyName present to
        prove the composition-author query never reads the scribe column
      genizah_titles(GenizahTitleId, AuthorId)
      genizah_persons(GenizahPersonId, EngDesc, HebDesc)
    """
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE domains (AlmaId TEXT, Domain TEXT, DomainHeb TEXT, ParentDomain TEXT);
        CREATE TABLE catalog (AlmaId TEXT, GenizahTitleId INTEGER, Author INTEGER, CopyName TEXT);
        CREATE TABLE genizah_titles (GenizahTitleId INTEGER, AuthorId INTEGER);
        CREATE TABLE genizah_persons (GenizahPersonId INTEGER, EngDesc TEXT, HebDesc TEXT);
        """
    )
    conn.executemany("INSERT INTO domains VALUES (?, ?, ?, ?)", domains)
    conn.executemany("INSERT INTO catalog VALUES (?, ?, ?, ?)", catalog)
    conn.executemany("INSERT INTO genizah_titles VALUES (?, ?)", titles)
    conn.executemany("INSERT INTO genizah_persons VALUES (?, ?, ?)", persons)
    conn.commit()
    conn.close()
    return db_path


# ===========================================================================
# Task 1: shown-work selection + opaque work_id + review artifact + approved reader
# ===========================================================================

def test_map_cat_to_source_corpus():
    assert sidecar_build._map_cat_to_source_corpus("Sefaria") == ids.SOURCE_CORPUS_SEFARIA
    assert sidecar_build._map_cat_to_source_corpus("Bible") == ids.SOURCE_CORPUS_SEFARIA
    assert sidecar_build._map_cat_to_source_corpus("Bavli") == ids.SOURCE_CORPUS_SEFARIA
    assert sidecar_build._map_cat_to_source_corpus("JA") == ids.SOURCE_CORPUS_JA
    # Any OTHER non-empty cat is reached purely by elimination (Landmine 2 --
    # the masked corpus's real name is never compared against).
    assert sidecar_build._map_cat_to_source_corpus("AnyOtherResearchLabel") == ids.SOURCE_CORPUS_MSOURCE
    assert sidecar_build._map_cat_to_source_corpus(None) is None
    assert sidecar_build._map_cat_to_source_corpus("") is None


def test_is_literary_genre_keep_and_exclude():
    assert sidecar_build._is_literary_genre("ספרות יפה") is True  # belles-lettres
    assert sidecar_build._is_literary_genre("פיוט ותפילה") is False  # piyyut and prayer
    assert sidecar_build._is_literary_genre(None) is False
    assert sidecar_build._is_literary_genre("some-unrecognized-genre") is False


def test_select_shown_works_open_corpus_all_and_msource_genre_policy(tmp_path):
    rows = [
        _mk_track1_row("p1", "s1", "raw:sef1", "Sefaria", title="Open Title One"),
        _mk_track1_row("p2", "s2", "raw:ja1", "JA", title="JA Title"),
        _mk_track1_row("p3", "s3", "raw:msource-lit", "MaskedCorpus", genre="ספרות יפה", title="lit title"),
        _mk_track1_row("p4", "s4", "raw:msource-piyyut", "MaskedCorpus", genre="פיוט ותפילה", title="piyyut title"),
        _mk_track1_row("p5", "s5", "raw:shadowed", "Sefaria", shadowed_by="p1"),
    ]
    db_path = _build_track1_db(tmp_path, rows)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        candidates = sidecar_build.select_shown_works(conn)
    finally:
        conn.close()

    raw_ids = {c["raw_work_id"] for c in candidates}
    assert raw_ids == {"raw:sef1", "raw:ja1", "raw:msource-lit"}
    by_raw = {c["raw_work_id"]: c for c in candidates}
    assert by_raw["raw:sef1"]["source_corpus"] == ids.SOURCE_CORPUS_SEFARIA
    assert by_raw["raw:ja1"]["source_corpus"] == ids.SOURCE_CORPUS_JA
    assert by_raw["raw:msource-lit"]["source_corpus"] == ids.SOURCE_CORPUS_MSOURCE


def test_assign_opaque_work_ids_stable_across_two_builds(tmp_path):
    crosswalk_path = tmp_path / "crosswalk.json"
    candidates1 = [{"raw_work_id": "raw:a"}, {"raw_work_id": "raw:b"}]
    sidecar_build.assign_opaque_work_ids(candidates1, crosswalk_path, create_if_missing=True)
    ids_first = {c["raw_work_id"]: c["work_id"] for c in candidates1}

    candidates2 = [{"raw_work_id": "raw:b"}, {"raw_work_id": "raw:a"}, {"raw_work_id": "raw:c"}]
    sidecar_build.assign_opaque_work_ids(candidates2, crosswalk_path, create_if_missing=False)
    ids_second = {c["raw_work_id"]: c["work_id"] for c in candidates2}

    assert ids_second["raw:a"] == ids_first["raw:a"]
    assert ids_second["raw:b"] == ids_first["raw:b"]
    assert ids_second["raw:c"] not in (ids_first["raw:a"], ids_first["raw:b"])
    for opaque in ids_second.values():
        assert opaque.startswith("w") and opaque[1:].isdigit()
        assert not opaque.startswith(("M:", "J:", "REF"))


def test_assign_opaque_work_ids_absent_crosswalk_aborts(tmp_path):
    missing_path = tmp_path / "does-not-exist.json"
    with pytest.raises(sidecar_build.CrosswalkAbortError):
        sidecar_build.assign_opaque_work_ids([{"raw_work_id": "raw:x"}], missing_path)


def test_assign_opaque_work_ids_malformed_persisted_value_aborts(tmp_path):
    """M1: a persisted crosswalk value NOT matching the frozen opaque
    work_id format (e.g. a raw-shaped identifier or filename stem) must
    abort BEFORE any candidate/work_id is assigned -- never silently
    echoed through to a candidate's work_id."""
    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(
        json.dumps({"raw:a": "M:some-raw-identifier"}), encoding="utf-8",
    )
    with pytest.raises(sidecar_build.CrosswalkValidationError):
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}], crosswalk_path, create_if_missing=False,
        )


def test_assign_opaque_work_ids_duplicate_opaque_value_aborts(tmp_path):
    """M1: two DIFFERENT raw work_ids sharing the SAME persisted opaque
    work_id (a non-1:1 crosswalk) must abort, never silently pick one."""
    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(
        json.dumps({"raw:a": "w000001", "raw:b": "w000001"}), encoding="utf-8",
    )
    with pytest.raises(sidecar_build.CrosswalkValidationError):
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}, {"raw_work_id": "raw:b"}],
            crosswalk_path, create_if_missing=False,
        )


def test_assign_opaque_work_ids_valid_persisted_crosswalk_passes(tmp_path):
    """Positive case: a well-formed, 1:1 persisted crosswalk still round-trips
    exactly as before (M1 must not reject legitimate crosswalks)."""
    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(
        json.dumps({"raw:a": "w000001", "raw:b": "w000002"}), encoding="utf-8",
    )
    candidates = sidecar_build.assign_opaque_work_ids(
        [{"raw_work_id": "raw:a"}, {"raw_work_id": "raw:b"}, {"raw_work_id": "raw:c"}],
        crosswalk_path, create_if_missing=False,
    )
    by_raw = {c["raw_work_id"]: c["work_id"] for c in candidates}
    assert by_raw["raw:a"] == "w000001"
    assert by_raw["raw:b"] == "w000002"
    assert by_raw["raw:c"] == "w000003"


# ---------------------------------------------------------------------------
# Codex R2 masking finding: _validate_crosswalk must NEVER echo a raw
# crosswalk key/value into its exception message (only counts/positions),
# and the opaque-id regex must accept ASCII digits ONLY (mirrors
# discovery_ids.mint_work_id's actual `format(..., "06d")` output alphabet).
# ---------------------------------------------------------------------------

def test_validate_crosswalk_malformed_value_message_never_echoes_raw_value(tmp_path):
    """MASKING: a malformed persisted crosswalk value's exception message
    must NEVER include the raw crosswalk key or value -- only counts and
    positions. A restricted M-source raw identifier could otherwise leak
    via a CLI invocation or an uncaught traceback."""
    crosswalk_path = tmp_path / "crosswalk.json"
    raw_secret_value = "M:SECRET-RAW-RESEARCH-IDENTIFIER-XYZ"
    crosswalk_path.write_text(
        json.dumps({"raw:a": raw_secret_value}), encoding="utf-8",
    )
    with pytest.raises(sidecar_build.CrosswalkValidationError) as exc_info:
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}], crosswalk_path, create_if_missing=False,
        )
    message = str(exc_info.value)
    assert raw_secret_value not in message
    assert "raw:a" not in message
    assert "position" in message.lower()


def test_validate_crosswalk_duplicate_value_message_never_echoes_raw_value(tmp_path):
    """MASKING: same guarantee for the duplicate-value (non-1:1) branch."""
    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(
        json.dumps({"raw:a": "w000001", "raw:b": "w000001"}), encoding="utf-8",
    )
    with pytest.raises(sidecar_build.CrosswalkValidationError) as exc_info:
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}, {"raw_work_id": "raw:b"}],
            crosswalk_path, create_if_missing=False,
        )
    message = str(exc_info.value)
    assert "raw:a" not in message
    assert "raw:b" not in message
    assert "w000001" not in message
    assert "position" in message.lower()


def test_validate_crosswalk_rejects_non_ascii_digit_opaque_value(tmp_path):
    """HIGH/masking: the frozen `mint_work_id` output alphabet is ASCII
    digits ONLY (`format(int, "06d")` never emits a non-ASCII decimal
    digit) -- a value using non-ASCII Unicode decimal digits (which the
    OLD bare `\\d` pattern, under Python's default UNICODE regex flag,
    would have wrongly accepted) must be rejected by the fixed
    `[0-9]`-only pattern."""
    crosswalk_path = tmp_path / "crosswalk.json"
    non_ascii_digit_value = "w" + "０" * 5 + "１"  # fullwidth "000001"
    crosswalk_path.write_text(
        json.dumps({"raw:a": non_ascii_digit_value}, ensure_ascii=False), encoding="utf-8",
    )
    assert sidecar_build._OPAQUE_WORK_ID_PATTERN.match(non_ascii_digit_value) is None
    with pytest.raises(sidecar_build.CrosswalkValidationError) as exc_info:
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}], crosswalk_path, create_if_missing=False,
        )
    assert non_ascii_digit_value not in str(exc_info.value)


def test_validate_crosswalk_accepts_genuine_mint_work_id_output():
    """Positive case: every real `mint_work_id(...)` output for a range of
    counters must match the (now ASCII-only, whole-string) frozen pattern."""
    for counter in (1, 42, 999999):
        opaque = ids.mint_work_id(counter)
        assert sidecar_build._OPAQUE_WORK_ID_PATTERN.fullmatch(opaque) is not None


def test_validate_crosswalk_rejects_terminal_newline_opaque_value(tmp_path):
    """Codex R3 MED: Python's `$` matches just before a TERMINAL newline, so
    the old `^w[0-9]{6}$`.match() accepted "w000001\\n" -- which
    `format(int, "06d")` can never emit. The fix (`re.fullmatch(r"w[0-9]{6}")`)
    requires the WHOLE string to be consumed, rejecting the trailing newline so
    a non-frozen opaque id can never reach the crosswalk/review artifact."""
    trailing_newline_value = "w000001\n"
    # Contrast: the OLD `$`-anchored `.match` WOULD have wrongly accepted it ...
    assert re.compile(r"^w[0-9]{6}$").match(trailing_newline_value) is not None
    # ... but the fixed whole-string `fullmatch` rejects it.
    assert sidecar_build._OPAQUE_WORK_ID_PATTERN.fullmatch(trailing_newline_value) is None
    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(json.dumps({"raw:a": trailing_newline_value}), encoding="utf-8")
    with pytest.raises(sidecar_build.CrosswalkValidationError) as exc_info:
        sidecar_build.assign_opaque_work_ids(
            [{"raw_work_id": "raw:a"}], crosswalk_path, create_if_missing=False,
        )
    assert trailing_newline_value not in str(exc_info.value)


def test_candidate_and_approved_headers_are_frozen():
    assert sidecar_build.CANDIDATE_HEADER == [
        "work_id", "candidate_title", "author", "genre", "source_label",
        "confidence_basis", "tier_a_witnesses", "claim_count",
        "owner_title", "owner_verdict", "owner_note",
    ]
    # The APPROVED csv IS the CANDIDATE csv shape -- the owner edits the
    # SAME 11-column file in place and returns it (134-07 Task A/B).
    assert sidecar_build.APPROVED_HEADER == sidecar_build.CANDIDATE_HEADER


def _candidate_row(*, work_id, candidate_title="", author="", genre="",
                    source_label=ids.SOURCE_CORPUS_SEFARIA,
                    confidence_basis=sidecar_build.CONFIDENCE_BASIS_OPEN_CORPUS_TITLE,
                    tier_a_witnesses=0, claim_count=1,
                    owner_title="", owner_verdict="", owner_note=""):
    """Build one enriched review-csv row dict (CANDIDATE_HEADER/APPROVED_HEADER
    shape) with sensible defaults, for `--from-approved` reader tests."""
    return {
        "work_id": work_id, "candidate_title": candidate_title, "author": author,
        "genre": genre, "source_label": source_label, "confidence_basis": confidence_basis,
        "tier_a_witnesses": tier_a_witnesses, "claim_count": claim_count,
        "owner_title": owner_title, "owner_verdict": owner_verdict, "owner_note": owner_note,
    }


def test_emit_review_artifact_and_load_approved_roundtrip(tmp_path):
    candidates = [
        {"raw_work_id": "raw:sef1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "Open Corpus Title", "author": "Open Author", "genre": "canon"},
        {"raw_work_id": "raw:msource-lit", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_MSOURCE,
         "title": "raw research title", "author": "raw author", "genre": "ספרות יפה"},
    ]
    impact_counts = {
        "w000001": {"claim_count": 3, "tier_a_witnesses": 2},
        "w000002": {"claim_count": 1, "tier_a_witnesses": 0},
    }
    candidate_csv = tmp_path / "candidates.csv"
    rows = sidecar_build.emit_review_artifact(candidates, candidate_csv, impact_counts=impact_counts)

    open_row = next(r for r in rows if r["work_id"] == "w000001")
    assert open_row["candidate_title"] == "Open Corpus Title"
    assert open_row["confidence_basis"] == sidecar_build.CONFIDENCE_BASIS_OPEN_CORPUS_TITLE
    assert open_row["owner_title"] == "" and open_row["owner_verdict"] == "" and open_row["owner_note"] == ""
    msource_row = next(r for r in rows if r["work_id"] == "w000002")
    assert msource_row["candidate_title"] == ""
    assert msource_row["confidence_basis"] == sidecar_build.CONFIDENCE_BASIS_NONE_OWNER_SUPPLIES
    # source provenance is masked in every row -- only the code, never a name.
    assert {r["source_label"] for r in rows} <= ids.SOURCE_CORPUS_CODES

    with open(candidate_csv, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == sidecar_build.CANDIDATE_HEADER

    # Simulate the owner's edit pass: approve w000001 as-is, EDIT w000002's title.
    approved_csv = tmp_path / "approved.csv"
    approved_rows = []
    for r in rows:
        r = dict(r)
        if r["work_id"] == "w000001":
            r["owner_verdict"] = "approve"
        else:
            r["owner_title"] = "Owner Chosen Neutral Title"
            r["owner_verdict"] = "edit"
        approved_rows.append(r)
    _write_approved_csv(approved_csv, approved_rows)

    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids={"w000001", "w000002"})
    by_id = {a["work_id"]: a for a in approved}
    assert set(by_id) == {"w000001", "w000002"}
    assert by_id["w000001"]["neutral_title"] == "Open Corpus Title"  # candidate_title, no owner override
    assert by_id["w000002"]["neutral_title"] == "Owner Chosen Neutral Title"  # owner_title wins
    assert by_id["w000001"]["source_corpus"] == ids.SOURCE_CORPUS_SEFARIA


def test_emit_review_artifact_columns_present_and_ordered(tmp_path):
    candidates = [
        {"raw_work_id": "raw:a", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "T", "author": "A", "genre": "G"},
    ]
    impact_counts = {"w000001": {"claim_count": 1, "tier_a_witnesses": 0}}
    out_csv = tmp_path / "candidates.csv"
    rows = sidecar_build.emit_review_artifact(candidates, out_csv, impact_counts=impact_counts)
    assert list(rows[0].keys()) == sidecar_build.CANDIDATE_HEADER
    with open(out_csv, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == sidecar_build.CANDIDATE_HEADER


def test_emit_review_artifact_sorted_by_impact_then_work_id(tmp_path):
    candidates = [
        {"raw_work_id": f"raw:{wid}", "work_id": wid, "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "title": "T"}
        for wid in ["w000003", "w000001", "w000002", "w000004"]
    ]
    impact_counts = {
        "w000001": {"claim_count": 5, "tier_a_witnesses": 1},
        "w000002": {"claim_count": 5, "tier_a_witnesses": 1},  # ties w000001 -> work_id ASC tiebreak
        "w000003": {"claim_count": 10, "tier_a_witnesses": 0},  # highest claim_count, but tier_a 0
        "w000004": {"claim_count": 2, "tier_a_witnesses": 2},  # highest tier_a_witnesses -> sorts first
    }
    out_csv = tmp_path / "candidates.csv"
    rows = sidecar_build.emit_review_artifact(candidates, out_csv, impact_counts=impact_counts)
    assert [r["work_id"] for r in rows] == ["w000004", "w000001", "w000002", "w000003"]


def test_emit_review_artifact_excludes_zero_claim_works(tmp_path):
    """SCOPE (134-07 Task A): the PRIORITIZED FULL set is every work with
    >=1 claim in the assembled distillation -- a work absent from
    `impact_counts` (zero claims) must never surface in the review csv."""
    candidates = [
        {"raw_work_id": "raw:a", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "title": "T1"},
        {"raw_work_id": "raw:b", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "title": "T2"},
    ]
    impact_counts = {
        "w000001": {"claim_count": 1, "tier_a_witnesses": 0},
        # w000002 absent entirely -- zero claims, never surfaces in the real distillation.
    }
    out_csv = tmp_path / "candidates.csv"
    rows = sidecar_build.emit_review_artifact(candidates, out_csv, impact_counts=impact_counts)
    assert [r["work_id"] for r in rows] == ["w000001"]


def test_emit_review_artifact_masked_metadata_flag_gates_raw_fallback(tmp_path):
    """--include-masked-metadata gates the RAW M-source genre/author FALLBACK
    (owner decision 2026-07-22). With NO fjms_meta supplied, a masked row
    falls through to the raw path: DEFAULT OFF keeps author/genre blank
    (fail-closed); the flag ON surfaces the raw research author/genre.
    candidate_title stays blank for the masked row regardless -- the owner
    supplies the neutral title. The open-corpus row is unaffected either
    way (its raw fallback is public, always allowed)."""
    candidates = [
        {"raw_work_id": "raw:open", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "Open Title", "author": "Open Author", "genre": "canon"},
        {"raw_work_id": "raw:msource", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_MSOURCE,
         "title": "raw research title", "author": "raw research author", "genre": "ספרות יפה"},
    ]
    impact_counts = {
        "w000001": {"claim_count": 1, "tier_a_witnesses": 0},
        "w000002": {"claim_count": 1, "tier_a_witnesses": 0},
    }

    # DEFAULT OFF, no FJMS -- masked row raw fallback withheld (fail-closed).
    off_csv = tmp_path / "off.csv"
    off_rows = sidecar_build.emit_review_artifact(candidates, off_csv, impact_counts=impact_counts)
    off_by_id = {r["work_id"]: r for r in off_rows}
    assert off_by_id["w000002"]["author"] == ""
    assert off_by_id["w000002"]["genre"] == ""
    assert off_by_id["w000002"]["candidate_title"] == ""
    assert off_by_id["w000002"]["confidence_basis"] == sidecar_build.CONFIDENCE_BASIS_NONE_OWNER_SUPPLIES

    # OPT-IN ON, no FJMS -- masked row raw fallback surfaces; candidate_title
    # STILL blank (owner supplies the neutral title).
    on_csv = tmp_path / "on.csv"
    on_rows = sidecar_build.emit_review_artifact(
        candidates, on_csv, impact_counts=impact_counts, include_masked_metadata=True,
    )
    on_by_id = {r["work_id"]: r for r in on_rows}
    assert on_by_id["w000002"]["author"] == "raw research author"
    assert on_by_id["w000002"]["genre"] == "ספרות יפה"
    assert on_by_id["w000002"]["candidate_title"] == ""  # never auto-derived for masked rows
    assert on_by_id["w000002"]["confidence_basis"] == sidecar_build.CONFIDENCE_BASIS_NONE_OWNER_SUPPLIES

    # The open-corpus row is identical under both modes (flag only affects masked rows).
    assert off_by_id["w000001"]["candidate_title"] == "Open Title"
    assert on_by_id["w000001"] == off_by_id["w000001"]


def test_emit_review_artifact_fjms_first_over_raw(tmp_path):
    """FJMS PUBLIC vocabulary wins over the raw-research value whenever
    present -- for BOTH open-corpus and M-source rows -- and needs NO flag
    (FJMS domain/composition-author are public Genizah catalog data)."""
    candidates = [
        {"raw_work_id": "raw:open", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "Open Title", "author": "Raw Open Author", "genre": "raw open genre"},
        {"raw_work_id": "raw:msource", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_MSOURCE,
         "title": "raw research title", "author": "raw msource author", "genre": "raw msource genre"},
    ]
    impact_counts = {
        "w000001": {"claim_count": 1, "tier_a_witnesses": 0},
        "w000002": {"claim_count": 1, "tier_a_witnesses": 0},
    }
    fjms_meta = {
        "w000001": {"genre": "FJMS Domain A", "author": "FJMS Author A"},
        "w000002": {"genre": "FJMS Domain B", "author": "FJMS Author B"},
    }
    out_csv = tmp_path / "candidates.csv"
    # NOTE: include_masked_metadata is OFF -- the FJMS value must STILL populate
    # the masked row (only the RAW fallback is gated, never the FJMS value).
    rows = sidecar_build.emit_review_artifact(
        candidates, out_csv, impact_counts=impact_counts, fjms_meta=fjms_meta,
        include_masked_metadata=False,
    )
    by_id = {r["work_id"]: r for r in rows}
    assert by_id["w000001"]["genre"] == "FJMS Domain A"
    assert by_id["w000001"]["author"] == "FJMS Author A"
    assert by_id["w000002"]["genre"] == "FJMS Domain B"  # masked row, FJMS value, no flag needed
    assert by_id["w000002"]["author"] == "FJMS Author B"
    assert by_id["w000002"]["candidate_title"] == ""  # still owner-supplied


def test_emit_review_artifact_raw_fallback_only_when_fjms_empty(tmp_path):
    """When FJMS has nothing for a work, fall back to raw -- open-corpus raw
    ALWAYS allowed (public); M-source raw gated by the flag (default OFF =>
    blank even though the raw value exists)."""
    candidates = [
        {"raw_work_id": "raw:open", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "Open Title", "author": "Raw Open Author", "genre": "raw open genre"},
        {"raw_work_id": "raw:msource", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_MSOURCE,
         "title": "raw research title", "author": "raw msource author", "genre": "raw msource genre"},
    ]
    impact_counts = {
        "w000001": {"claim_count": 1, "tier_a_witnesses": 0},
        "w000002": {"claim_count": 1, "tier_a_witnesses": 0},
    }
    fjms_meta = {}  # FJMS empty for every work

    off = {r["work_id"]: r for r in sidecar_build.emit_review_artifact(
        candidates, tmp_path / "off.csv", impact_counts=impact_counts, fjms_meta=fjms_meta,
        include_masked_metadata=False,
    )}
    # open-corpus raw fallback ALWAYS allowed
    assert off["w000001"]["genre"] == "raw open genre"
    assert off["w000001"]["author"] == "Raw Open Author"
    # M-source raw fallback withheld without the flag
    assert off["w000002"]["genre"] == ""
    assert off["w000002"]["author"] == ""

    on = {r["work_id"]: r for r in sidecar_build.emit_review_artifact(
        candidates, tmp_path / "on.csv", impact_counts=impact_counts, fjms_meta=fjms_meta,
        include_masked_metadata=True,
    )}
    assert on["w000002"]["genre"] == "raw msource genre"  # now allowed
    assert on["w000002"]["author"] == "raw msource author"


def test_compute_fjms_enrichment_modal_domain_count_distinct_almaid(tmp_path):
    """genre = MODAL FJMS domain by COUNT(DISTINCT AlmaId) across a work's
    witness sys_ids; exact-duplicate (AlmaId, Domain) rows dedup by AlmaId;
    tie-break domain name ASC."""
    fjms_db = _build_fjms_db(tmp_path, domains=[
        # work-1 witnesses A1/A2/A3: D1 on all three (3), D2 on A1 (1), D3 on A3 (1)
        ("A1", "D1", "", None), ("A1", "D2", "", None),
        ("A1", "D1", "", None),  # exact-duplicate (A1, D1) -- must dedup by AlmaId
        ("A2", "D1", "", None),
        ("A3", "D1", "", None), ("A3", "D3", "", None),
        # work-2 witnesses B1/B2: tie AAA(1) vs BBB(1) -> domain name ASC -> AAA
        ("B1", "BBB", "", None), ("B2", "AAA", "", None),
    ])
    conn = sqlite3.connect(f"file:{fjms_db}?mode=ro", uri=True)
    try:
        enrichment = sidecar_build.compute_fjms_enrichment(conn, {
            "w000001": {"A1", "A2", "A3"},
            "w000002": {"B1", "B2"},
        })
    finally:
        conn.close()
    assert enrichment["w000001"]["genre"] == "D1"  # modal, carried by 3 distinct AlmaIds
    assert enrichment["w000002"]["genre"] == "AAA"  # tie -> domain name ASC


def test_compute_fjms_enrichment_author_two_path_union_not_scribe(tmp_path):
    """author = MODAL FJMS COMPOSITION-author via the two-path union
    (catalog->genizah_titles->genizah_persons UNION catalog.Author->
    genizah_persons); modal by COUNT(DISTINCT AlmaId), tie-break person_id
    ASC; EngDesc as the value. The scribe column (CopyName) is NEVER read."""
    fjms_db = _build_fjms_db(
        tmp_path,
        catalog=[
            # work-1: A1 via title-path -> P1; A2 direct -> P1; A3 direct -> P2
            ("A1", 10, None, "SCRIBE-SHOULD-NEVER-APPEAR"),
            ("A2", None, 1, "SCRIBE-SHOULD-NEVER-APPEAR"),
            ("A3", None, 2, None),
            # work-2: C1 direct -> P5, C2 direct -> P3 (tie 1-1 -> person_id ASC -> P3)
            ("C1", None, 5, None),
            ("C2", None, 3, None),
        ],
        titles=[(10, 1)],  # GenizahTitleId 10 -> AuthorId 1 (P1)
        persons=[
            (1, "Author One", "he1"),
            (2, "Author Two", "he2"),
            (3, "Person Three", "he3"),
            (5, "Person Five", "he5"),
        ],
    )
    conn = sqlite3.connect(f"file:{fjms_db}?mode=ro", uri=True)
    try:
        enrichment = sidecar_build.compute_fjms_enrichment(conn, {
            "w000001": {"A1", "A2", "A3"},
            "w000002": {"C1", "C2"},
        })
    finally:
        conn.close()
    # P1 carried by 2 distinct AlmaIds (A1 via title, A2 direct) vs P2 by 1 -> P1
    assert enrichment["w000001"]["author"] == "Author One"
    # scribe/CopyName never leaks into the author cell
    assert "SCRIBE" not in enrichment["w000001"]["author"]
    # tie 1-1 between P5 and P3 -> person_id ASC -> P3
    assert enrichment["w000002"]["author"] == "Person Three"


def test_compute_fjms_enrichment_empty_when_no_fjms_match(tmp_path):
    """A work whose witness sys_ids have no FJMS domain/author rows resolves
    to empty strings (author is SPARSE ~10-23% coverage -- expected, not a
    bug)."""
    fjms_db = _build_fjms_db(tmp_path, domains=[("OTHER", "D1", "", None)])
    conn = sqlite3.connect(f"file:{fjms_db}?mode=ro", uri=True)
    try:
        enrichment = sidecar_build.compute_fjms_enrichment(conn, {"w000001": {"A1", "A2"}})
    finally:
        conn.close()
    assert enrichment["w000001"] == {"genre": "", "author": ""}


def test_collect_work_witness_sys_ids_ignores_shared_text(tmp_path):
    """collect_work_witness_sys_ids maps work_id -> its WITNESS sys_ids only;
    shared_text evidence rows contribute no sys_id (the genre/author signal is
    a property of the physical witness manuscripts)."""
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([
        ("p1", "htr", "x" * 100), ("seedp1", "htr", "y" * 40),
    ]))
    e1_ra = [{"page_id": "p1", "sys_id": "SYS-WITNESS-1", "work_id": "raw:w1",
              "o0": 0, "o1": 50, "ml": 50, "dens": 0.9, "n_spans": 1}]
    q2_shared = [{
        "cpage": "p1", "csys": "SYS-SHARED-TEXT-ONLY", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T2", "aligned_len": 120, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 60, "occ1": 90, "seed_page": "seedp1", "cross_language": False, "is_new": False,
    }]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx, e1_ra_confirmed=e1_ra, q2_shared_text=q2_shared,
    )
    work_sys_ids = sidecar_build.collect_work_witness_sys_ids(
        result["claim_rows"], result["evidence_rows"]
    )
    assert work_sys_ids == {"w000001": {"SYS-WITNESS-1"}}


def test_resolve_fjms_db_path_prefers_explicit_and_skips_empty(tmp_path, monkeypatch):
    """resolve_fjms_db_path returns an explicit path verbatim; with no
    explicit path and no non-empty candidate, returns None (enrichment
    silently skipped). A 0-byte file (the repo-root placeholder shape) is
    never returned."""
    # explicit wins
    assert sidecar_build.resolve_fjms_db_path("some/explicit/path.db") == "some/explicit/path.db"
    # no candidates -> None (point the resolver's repo-root + LOCALAPPDATA at
    # empty temp dirs so the real dev-box fist_data/ DB can't be picked up)
    monkeypatch.setattr(sidecar_build, "_REPO_ROOT", str(tmp_path / "norepo"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "noappdata"))
    assert sidecar_build.resolve_fjms_db_path(None) is None
    # a 0-byte placeholder under the (patched) repo root is skipped
    placeholder_dir = tmp_path / "norepo" / "fist_data"
    placeholder_dir.mkdir(parents=True)
    (placeholder_dir / "fjms_enrichment.db").touch()  # 0 bytes
    assert sidecar_build.resolve_fjms_db_path(None) is None


def test_compute_work_impact_counts_matches_assembled_data(tmp_path):
    """`tier_a_witnesses`/`claim_count` must be derived from the SAME
    build_claims_and_evidence/assemble_claims_and_evidence assembly used by
    the real build -- never a hand-rolled divergent counter."""
    works = [
        {"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA},
        {"raw_work_id": "raw:w2", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_SEFARIA},
    ]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([
        ("p1", "htr", "x" * 100), ("p2", "htr", "x" * 100), ("p3", "htr", "x" * 100),
    ]))
    track1_rows = [
        _mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 50, 0.9]]"),
        _mk_track1_row("p2", "s2", "raw:w1", "Sefaria", spans_json="[[0, 50, 0.9]]"),
        _mk_track1_row("p3", "s3", "raw:w2", "Sefaria", spans_json="[[0, 50, 0.9]]"),
    ]
    db_path = _build_track1_db(tmp_path, track1_rows)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # p1 ALSO carries a non-overlapping shared_text row -- same claim
    # (page_id=p1, work_id=w000001), a SECOND evidence row, NOT a second claim.
    q2_shared = [{
        "cpage": "p1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T2", "aligned_len": 120, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 60, "occ1": 90, "seed_page": "sp1", "cross_language": False, "is_new": False,
    }]
    try:
        result = sidecar_build.build_claims_and_evidence(
            conn=conn, works=works, page_index=page_idx, q2_shared_text=q2_shared,
        )
    finally:
        conn.close()
    counts = sidecar_build.compute_work_impact_counts(result["claim_rows"], result["evidence_rows"])
    assert counts["w000001"] == {"claim_count": 2, "tier_a_witnesses": 2}
    assert counts["w000002"] == {"claim_count": 1, "tier_a_witnesses": 1}


def test_load_approved_works_ships_approve_with_candidate_title(tmp_path):
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id="w000001", candidate_title="Candidate Title A", owner_verdict="approve"),
    ])
    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids={"w000001"})
    assert len(approved) == 1
    assert approved[0]["neutral_title"] == "Candidate Title A"


def test_load_approved_works_ships_edit_with_owner_title_wins(tmp_path):
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id="w000001", candidate_title="Candidate Title A",
                       owner_title="Owner Corrected Title", owner_verdict="edit"),
    ])
    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids={"w000001"})
    assert approved[0]["neutral_title"] == "Owner Corrected Title"


def test_load_approved_works_rejection_rules(tmp_path):
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id="w000001", candidate_title="Good Title", owner_verdict="approve"),  # kept
        _candidate_row(work_id="w000002", candidate_title="Rejected", owner_verdict="reject"),  # excluded
        _candidate_row(work_id="w000003", candidate_title="Suppressed", owner_verdict="suppress"),  # excluded
        _candidate_row(work_id="w000004", candidate_title="Unverdicted", owner_verdict=""),  # excluded: blank verdict
        _candidate_row(work_id="w000005", candidate_title="", owner_title="",
                       owner_verdict="approve"),  # excluded: empty resolved title
        _candidate_row(work_id="w000999", candidate_title="Not In Crosswalk", owner_verdict="approve"),  # unknown id
        _candidate_row(work_id="w000006", candidate_title="Bad Corpus", source_label="not-a-real-code",
                       owner_verdict="approve"),  # excluded: bad source_label code
    ])
    valid_work_ids = {"w000001", "w000002", "w000003", "w000004", "w000005", "w000006"}
    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids=valid_work_ids)
    assert [a["work_id"] for a in approved] == ["w000001"]


def test_load_approved_works_excludes_reject_suppress_blank_and_empty_title(tmp_path):
    """Explicit ship/exclude-rule regression (134-07 Task B): reject,
    suppress, blank owner_verdict, and an empty resolved title ALL exclude
    -- fail-closed, no research-title fallback."""
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id="w000001", candidate_title="T1", owner_verdict="reject"),
        _candidate_row(work_id="w000002", candidate_title="T2", owner_verdict="suppress"),
        _candidate_row(work_id="w000003", candidate_title="T3", owner_verdict=""),
        _candidate_row(work_id="w000004", candidate_title="", owner_title="", owner_verdict="approve"),
    ])
    approved = sidecar_build.load_approved_works(
        approved_csv, valid_work_ids={"w000001", "w000002", "w000003", "w000004"},
    )
    assert approved == []


def test_load_approved_works_header_mismatch_raises(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    with open(bad_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["work_id", "title"])
        writer.writeheader()
    with pytest.raises(ValueError):
        sidecar_build.load_approved_works(bad_csv)


def test_load_approved_works_header_mismatch_never_echoes_cell_masking_sentinel(tmp_path):
    """MASKING: a malformed-header csv raises with COLUMN NAMES only -- even
    when the file's data ROWS carry a masking-sentinel-shaped value in some
    cell, that cell value must never be echoed into the raised message."""
    bad_csv = tmp_path / "bad.csv"
    sentinel = "RESTRICTED-MSOURCE-SENTINEL-VALUE"
    with open(bad_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["work_id", "title"])
        writer.writeheader()
        writer.writerow({"work_id": sentinel, "title": sentinel})
    with pytest.raises(ValueError) as exc_info:
        sidecar_build.load_approved_works(bad_csv)
    assert sentinel not in str(exc_info.value)


# ===========================================================================
# Task 2: unified witness family + shared_text family + routing/status matrices
# ===========================================================================

def test_ingest_e1_rows_expert_verified_split_and_offsets():
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "hello world")]))
    rows = [{"page_id": "p1", "sys_id": "s1", "work_id": "raw:w1",
              "o0": 0, "o1": 5, "ml": 5, "dens": 0.9, "n_spans": 1}]

    unreviewed = sidecar_build._ingest_e1_rows(
        rows, work_index=work_index, page_index=page_idx,
        confidence_band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED,
        audit_status=ids.AUDIT_STATUS_AUDIT_PENDING,
    )
    assert len(unreviewed) == 1
    assert unreviewed[0]["confidence_band"] == ids.CONFIDENCE_BAND_EXPERT_VERIFIED
    assert unreviewed[0]["adjudication_status"] == ids.ADJUDICATION_STATUS_UNREVIEWED
    assert unreviewed[0]["work_id"] == "w000001"
    assert unreviewed[0]["span_start"] == 0 and unreviewed[0]["span_end"] == 5
    assert unreviewed[0]["text_layer"] == "htr"
    assert unreviewed[0]["routing_status"] == ids.ROUTING_STATUS_SHIPPED
    assert unreviewed[0]["routing_reason"] == ids.ROUTING_REASON_NONE

    human_confirmed = sidecar_build._ingest_e1_rows(
        rows, work_index=work_index, page_index=page_idx,
        confidence_band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED,
        adjudication_status=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
        audit_status=ids.AUDIT_STATUS_AUDIT_PENDING,
    )
    assert human_confirmed[0]["adjudication_status"] == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED


@pytest.mark.parametrize("band,adjudication_status,audit_status", [
    (ids.CONFIDENCE_BAND_SCREENING_RB, ids.ADJUDICATION_STATUS_PROVISIONAL, ids.AUDIT_STATUS_NA),
    (ids.CONFIDENCE_BAND_SCREENING_CANON, ids.ADJUDICATION_STATUS_PROVISIONAL, ids.AUDIT_STATUS_NA),
])
def test_ingest_e1_rows_screening_status_matrix(band, adjudication_status, audit_status):
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "hello")]))
    rows = [{"page_id": "p1", "sys_id": "s1", "work_id": "raw:w1",
              "o0": 0, "o1": 5, "ml": 5, "dens": 0.9, "n_spans": 1}]
    out = sidecar_build._ingest_e1_rows(
        rows, work_index=work_index, page_index=page_idx,
        confidence_band=band, adjudication_status=adjudication_status, audit_status=audit_status,
    )
    assert out[0]["adjudication_status"] == adjudication_status
    assert out[0]["audit_status"] == audit_status
    assert out[0]["routing_status"] == ids.ROUTING_STATUS_SHIPPED
    assert out[0]["routing_reason"] == ids.ROUTING_REASON_NONE


def test_ingest_tier_a_shadowed_filter_and_largest_span(tmp_path):
    rows = [
        _mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 10, 0.9], [20, 50, 0.2]]"),
        _mk_track1_row("p1", "s1", "raw:w1-shadowed-out", "Sefaria", shadowed_by="p1"),
    ]
    db_path = _build_track1_db(tmp_path, rows)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "x" * 60)]))
    work_index = {"raw:w1": {"work_id": "w000001"}}
    try:
        out = sidecar_build._ingest_tier_a(conn, work_index, page_idx)
    finally:
        conn.close()
    assert len(out) == 1
    assert out[0]["confidence_band"] == ids.CONFIDENCE_BAND_TIER_A
    assert (out[0]["span_start"], out[0]["span_end"]) == (20, 50)  # (50-20)=30 > (10-0)=10


def test_ingest_tier_a_unknown_work_id_excluded():
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "x" * 20)]))
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        "CREATE TABLE track1_matches (page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, "
        "genre TEXT, author TEXT, title TEXT, mesirah TEXT, matched_letters INT, "
        "best_density REAL, n_spans INT, spans_json TEXT, shadowed_by TEXT);"
    )
    conn.execute(
        "INSERT INTO track1_matches VALUES ('p1','s1','raw:unknown','Sefaria',NULL,NULL,NULL,"
        "NULL,5,0.5,1,'[[0,10,0.5]]',NULL)"
    )
    out = sidecar_build._ingest_tier_a(conn, {"raw:w1": {"work_id": "w000001"}}, page_idx)
    conn.close()
    assert out == []


def test_propagated_corroborated_vs_weak_predicate():
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 60)]))
    corrob_row = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
        "is_new": True, "impurity": False, "trials": 3,
        "seeds": [{"occ0": 0, "occ1": 30, "occ_class": "core", "seed_page": "sp1", "seed_sys": "ss1"}],
    }
    weak_row = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
        "is_new": True, "impurity": False, "rung": "A",
        "seeds": [{"occ0": 0, "occ1": 20, "occ_class": "core", "seed_page": "sp2", "seed_sys": "ss2"}],
    }
    out = sidecar_build._ingest_propagated_witness([corrob_row, weak_row], work_index, page_idx)
    bands = {o["confidence_band"] for o in out}
    assert bands == {ids.CONFIDENCE_BAND_CORROBORATED, ids.CONFIDENCE_BAND_WEAK}
    by_band = {o["confidence_band"]: o for o in out}
    assert by_band[ids.CONFIDENCE_BAND_CORROBORATED]["adjudication_status"] == ids.ADJUDICATION_STATUS_UNREVIEWED
    assert by_band[ids.CONFIDENCE_BAND_CORROBORATED]["audit_status"] == ids.AUDIT_STATUS_AUDIT_PENDING
    assert by_band[ids.CONFIDENCE_BAND_WEAK]["adjudication_status"] == ids.ADJUDICATION_STATUS_PROVISIONAL
    assert by_band[ids.CONFIDENCE_BAND_WEAK]["audit_status"] == ids.AUDIT_STATUS_NA


def test_propagated_multi_occurrence_seed_spans_r4():
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 100)]))
    row = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
        "is_new": True, "impurity": False, "trials": 4,
        "seeds": [
            {"occ0": 0, "occ1": 20, "occ_class": "a", "seed_page": "sp1", "seed_sys": "ss1"},
            {"occ0": 0, "occ1": 20, "occ_class": "a", "seed_page": "sp2", "seed_sys": "ss2"},
            {"occ0": 50, "occ1": 90, "occ_class": "b", "seed_page": "sp3", "seed_sys": "ss3"},
        ],
    }
    out = sidecar_build._ingest_propagated_witness([row], work_index, page_idx)
    assert len(out) == 1
    ev = out[0]
    assert (ev["span_start"], ev["span_end"]) == (50, 90)  # the larger distinct occurrence wins
    assert len(ev["seed_spans"]) == 2
    span0 = next(s for s in ev["seed_spans"] if s["occ0"] == 0)
    assert span0["seed_page_ids"] == ["sp1", "sp2"]
    assert set(ev["seed_ms_ids"]) >= {"sp1", "sp2", "sp3", "ss1", "ss2", "ss3"}


def test_family_router_ingestion_non_witness_review_only_co_citation():
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 80), ("seedp1", "htr", "y" * 40)]))
    router_rows = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "tafsir_targum",
        "is_new": True, "impurity": False, "trials": 5,  # >=2 trials MUST NOT band as witness (R3)
        "seeds": [{"occ0": 0, "occ1": 30, "occ_class": "core", "seed_page": "seedp1", "seed_sys": "ss1"}],
    }]
    out = sidecar_build._ingest_family_router(
        router_rows, work_index, page_idx, router_bucket="tafsir_targum"
    )
    assert len(out) == 1
    ev = out[0]
    assert ev["evidence_kind"] == ids.EVIDENCE_KIND_SHARED_TEXT
    assert ev["confidence_band"] == ids.CONFIDENCE_BAND_NOT_EVALUATED
    assert ev["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY
    assert ev["routing_reason"] == ids.ROUTING_REASON_CO_CITATION
    assert ev["router_bucket"] == "tafsir_targum"
    assert ev["other_page_id"] == "seedp1"
    assert ev["snapshot_hash_b"] is not None
    assert ev["b_start"] is None and ev["b_end"] is None


def test_family_router_never_calls_corroborated_predicate_shape():
    """corroborated_predicate requires _bucket=='witness'; router rows carry
    _bucket in {tafsir_targum, with_arabic}, so even if it WERE (mis)called,
    it would return False -- but the ingestion function never calls it at
    all (asserted structurally: every router row bands not_evaluated)."""
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 80), ("seedp1", "htr", "y")]))
    row = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "with_arabic",
        "is_new": True, "impurity": False, "trials": 10,
        "seeds": [{"occ0": 0, "occ1": 10, "occ_class": "core", "seed_page": "seedp1", "seed_sys": "ss1"}],
    }
    assert ids.corroborated_predicate(row) is False  # sanity: _bucket != 'witness'
    out = sidecar_build._ingest_family_router(
        [row], work_index, page_idx, router_bucket="with_arabic"
    )
    assert out[0]["confidence_band"] not in ids.CONFIDENCE_BANDS_BY_SOURCE[ids.EVIDENCE_SOURCE_PROPAGATED] - {
        ids.CONFIDENCE_BAND_NOT_EVALUATED
    }


def test_shared_text_ingestion_actual_attrs_only():
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 60), ("seedp1", "htr", "y" * 40)]))
    rows = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T2", "aligned_len": 120, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 5, "occ1": 45, "seed_page": "seedp1", "cross_language": False, "is_new": True,
    }]
    out = sidecar_build._ingest_shared_text(rows, work_index, page_idx)
    assert len(out) == 1
    ev = out[0]
    assert ev["evidence_kind"] == ids.EVIDENCE_KIND_SHARED_TEXT
    assert ev["evidence_source"] == ids.EVIDENCE_SOURCE_PROPAGATED
    assert ev["confidence_band"] == ids.CONFIDENCE_BAND_NOT_EVALUATED
    assert ev["tier"] == "T2" and ev["aligned_len"] == 120 and ev["n_seed_ms"] == 2
    assert ev["other_page_id"] == "seedp1"
    assert ev["b_start"] is None and ev["b_end"] is None
    # NEVER assert propagated-witness-only attrs on a shared_text row.
    assert ev.get("router_bucket") is None
    assert ev.get("rung") is None
    assert ev.get("ge3") is None


def test_shared_text_collision_onto_witness_claim_f7():
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 100), ("seedp1", "htr", "y" * 40)]))
    q2_witness = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
        "is_new": True, "impurity": False, "trials": 2,
        "seeds": [{"occ0": 0, "occ1": 30, "occ_class": "core", "seed_page": "sp1", "seed_sys": "ss1"}],
    }]
    q2_shared = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T2", "aligned_len": 120, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 0, "occ1": 30, "seed_page": "seedp1", "cross_language": False, "is_new": False,
    }]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx,
        q2_witness_collection=q2_witness, q2_shared_text=q2_shared,
    )
    assert len(result["claim_rows"]) == 1
    claim_row = result["claim_rows"][0]
    claim_id = claim_row[2]
    evidence_for_claim = [e for e in result["evidence_rows"] if e[1] == claim_id]
    assert len(evidence_for_claim) == 2
    kinds = {e[2] for e in evidence_for_claim}
    assert kinds == {ids.EVIDENCE_KIND_WITNESS, ids.EVIDENCE_KIND_SHARED_TEXT}
    # parent claim_type stays the WITNESS rule (F7) -- never shared_text
    # when a witness evidence row is also present.
    assert claim_row[3] in (ids.CLAIM_TYPE_DIRECT_WITNESS, ids.CLAIM_TYPE_QUOTES_THIS_WORK)


def test_evidence_id_collision_shared_text_vs_family_router_prefers_shipped():
    """Real-data-observed edge case (see deferred-items.md): a plain
    q2_shared_text row and a family-router row can independently resolve to
    the IDENTICAL (work_id, a_page_id, sys_id, evidence_kind, evidence_source,
    confidence_band, span, other_page_id) tuple -- the FROZEN evidence_id
    recipe has no "which collection" discriminator by design. The build must
    NEVER crash (UNIQUE(claim_id, evidence_id) would otherwise reject the
    second insert) and must deterministically prefer the SHIPPED row over a
    review_only one."""
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 100), ("seedp1", "htr", "y" * 40)]))
    q2_shared = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T1", "aligned_len": 400, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 0, "occ1": 30, "seed_page": "seedp1", "cross_language": False, "is_new": True,
    }]
    router_rows = [{
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "_bucket": "tafsir_targum",
        "is_new": True, "impurity": False, "trials": 4,
        "seeds": [{"occ0": 0, "occ1": 30, "occ_class": "core", "seed_page": "seedp1", "seed_sys": "ss1"}],
    }]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx,
        q2_shared_text=q2_shared, q2_collection_tafsir_targum=router_rows,
    )
    assert result["evidence_id_collisions"] == 1
    assert len(result["claim_rows"]) == 1
    assert len(result["evidence_rows"]) == 1  # deduped -- only ONE row persists
    winning = result["evidence_rows"][0]
    assert winning[7] == ids.ROUTING_STATUS_SHIPPED  # shipped kept over review_only
    assert winning[8] == ids.ROUTING_REASON_NONE


def test_evidence_id_collision_equal_priority_identical_content_deduped_without_raising():
    """L2: the exact SAME logical row appearing twice (e.g. a duplicate
    JSONL line) collides on evidence_id at EQUAL routing priority (both
    shipped) -- this is a harmless true duplicate and must dedupe
    deterministically WITHOUT raising."""
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 100), ("seedp1", "htr", "y" * 40)]))
    row = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T2", "aligned_len": 120, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 0, "occ1": 30, "seed_page": "seedp1", "cross_language": False, "is_new": True,
    }
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx,
        q2_shared_text=[row, dict(row)],
    )
    assert result["evidence_id_collisions"] == 1
    assert len(result["evidence_rows"]) == 1


def test_evidence_id_collision_equal_priority_different_content_raises():
    """L2: two rows sharing every evidence_id-KEY field (work_id, a_page_id,
    sys_id, evidence_kind, evidence_source, confidence_band, span, other_page_id)
    but carrying DIFFERENT non-key attributes (tier/aligned_len) collide at
    EQUAL routing priority with no deterministic winner -- must raise
    fail-closed rather than silently pick one based on ingestion order."""
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("cp1", "htr", "x" * 100), ("seedp1", "htr", "y" * 40)]))
    row_a = {
        "cpage": "cp1", "csys": "s1", "work_id": "raw:w1", "cat": "Sefaria",
        "tier": "T1", "aligned_len": 300, "occ_class": "core", "n_seed_ms": 2,
        "occ0": 0, "occ1": 30, "seed_page": "seedp1", "cross_language": False, "is_new": True,
    }
    row_b = {**row_a, "tier": "T3", "aligned_len": 45}
    with pytest.raises(sidecar_build.EvidenceIdCollisionError):
        sidecar_build.build_claims_and_evidence(
            conn=None, works=works, page_index=page_idx,
            q2_shared_text=[row_a, row_b],
        )


def test_claim_type_dominance_across_works_on_same_page():
    works = [
        {"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA},
        {"raw_work_id": "raw:w2", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_SEFARIA},
    ]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "x" * 600)]))
    e1_ra = [
        {"page_id": "p1", "sys_id": "s1", "work_id": "raw:w1", "o0": 0, "o1": 500, "ml": 490, "dens": 0.9, "n_spans": 1},
        {"page_id": "p1", "sys_id": "s1", "work_id": "raw:w2", "o0": 510, "o1": 550, "ml": 40, "dens": 0.7, "n_spans": 1},
    ]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx, e1_ra_confirmed=e1_ra,
    )
    by_work = {row[1]: row for row in result["claim_rows"]}
    assert by_work["w000001"][3] == ids.CLAIM_TYPE_DIRECT_WITNESS
    assert by_work["w000002"][3] == ids.CLAIM_TYPE_QUOTES_THIS_WORK


def test_no_physical_ms_collapse_two_pages_same_sys_stay_separate_claims():
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("pA", "htr", "x" * 40), ("pB", "htr", "x" * 40)]))
    e1_ra = [
        {"page_id": "pA", "sys_id": "s1", "work_id": "raw:w1", "o0": 0, "o1": 30, "ml": 30, "dens": 0.8, "n_spans": 1},
        {"page_id": "pB", "sys_id": "s1", "work_id": "raw:w1", "o0": 0, "o1": 30, "ml": 30, "dens": 0.8, "n_spans": 1},
    ]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx, e1_ra_confirmed=e1_ra,
    )
    page_ids = {row[0] for row in result["claim_rows"]}
    assert page_ids == {"pA", "pB"}
    assert len(result["claim_rows"]) == 2
    # claim_id is stable per (page_id, work_id) -- not collapsed by shared sys_id.
    claim_ids = {row[2] for row in result["claim_rows"]}
    assert len(claim_ids) == 2


def test_display_evidence_precedence_tier_a_beats_corroborated(tmp_path):
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "x" * 100)]))
    track1_rows = [_mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 50, 0.9]]")]
    db_path = _build_track1_db(tmp_path, track1_rows)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    q2_witness = [{
        "cpage": "p1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
        "is_new": True, "impurity": False, "trials": 2,
        "seeds": [{"occ0": 0, "occ1": 40, "occ_class": "core", "seed_page": "sp1", "seed_sys": "ss1"}],
    }]
    try:
        result = sidecar_build.build_claims_and_evidence(
            conn=conn, works=works, page_index=page_idx, q2_witness_collection=q2_witness,
        )
    finally:
        conn.close()
    claim_row = result["claim_rows"][0]
    display_id = claim_row[4]
    winning = next(e for e in result["evidence_rows"] if e[0] == display_id)
    assert winning[4] == ids.CONFIDENCE_BAND_TIER_A


def test_unknown_work_id_rows_are_excluded_from_claims():
    works = [{"raw_work_id": "raw:w1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA}]
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", "x" * 40)]))
    e1_ra = [{"page_id": "p1", "sys_id": "s1", "work_id": "raw:unknown",
               "o0": 0, "o1": 30, "ml": 30, "dens": 0.8, "n_spans": 1}]
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_idx, e1_ra_confirmed=e1_ra,
    )
    assert result["claim_rows"] == []
    assert result["evidence_rows"] == []


# ===========================================================================
# Task 3: witness units (DATA-10) + build orchestration + masking gate
# ===========================================================================

def test_build_witness_units_oxford_part_and_physical_join():
    oxford_parts = [("s1", "PART-A"), ("s2", "PART-A"), ("s3", "")]
    physical_joins = [("s4", 1, "Physical Join"), ("s5", 1, "Physical Join")]
    units = sidecar_build.build_witness_units(oxford_parts, physical_joins)
    assert len(units) == 2
    oxford_unit = next(u for u in units if u["merge_basis"] == ids.MERGE_BASIS_OXFORD_PART)
    assert oxford_unit["members"] == ["s1", "s2"]
    join_unit = next(u for u in units if u["merge_basis"] == ids.MERGE_BASIS_PHYSICAL_JOIN)
    assert join_unit["members"] == ["s4", "s5"]


def test_build_witness_units_excludes_scribe_join():
    physical_joins = [("s1", 1, "Scribe join"), ("s2", 1, "Scribe join")]
    units = sidecar_build.build_witness_units([], physical_joins)
    assert units == []


def test_build_witness_units_ambiguous_basis_not_merged():
    physical_joins = [
        ("s1", 1, None), ("s2", 1, ""), ("s3", 1, "Insufficient information"),
        ("s4", 1, "Partially Physical and not Join"),
    ]
    units = sidecar_build.build_witness_units([], physical_joins)
    assert units == []


def test_build_witness_units_each_sys_id_at_most_one_unit():
    oxford_parts = [("s1", "PART-A"), ("s2", "PART-A")]
    physical_joins = [("s1", 5, "Physical Join"), ("s2", 5, "Physical Join"), ("s6", 5, "Physical Join")]
    units = sidecar_build.build_witness_units(oxford_parts, physical_joins)
    all_members = [m for u in units for m in u["members"]]
    assert len(all_members) == len(set(all_members))
    assert any(
        u["merge_basis"] == ids.MERGE_BASIS_OXFORD_PART and set(u["members"]) == {"s1", "s2"}
        for u in units
    )
    # s6 alone (s1/s2 already claimed by the Oxford-part unit) -- not enough
    # members left in the join group to form a physical_join unit.
    assert not any(u["merge_basis"] == ids.MERGE_BASIS_PHYSICAL_JOIN for u in units)
    unit_id_1 = ids.unit_id(["s1", "s2"])
    unit_id_2 = ids.unit_id(["s2", "s1"])
    assert unit_id_1 == unit_id_2  # unit_id is order-invariant (deterministic)


# ---------------------------------------------------------------------------
# finalize_build end-to-end orchestration
# ---------------------------------------------------------------------------

def _build_minimal_finalize_fixture(tmp_path, *, neutral_title="Clean Neutral Title"):
    research_rows = [_mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 40, 0.9]]")]
    research_db = _build_track1_db(tmp_path, research_rows, name="research.db")
    conn = sqlite3.connect(str(research_db))
    conn.execute(
        "INSERT INTO pages VALUES ('p1', 's1', 'witness', 40, 'sample htr text', 'htr', NULL, NULL, 40)"
    )
    conn.commit()
    conn.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    conn = sqlite3.connect(f"file:{research_db}?mode=ro", uri=True)
    candidates = sidecar_build.select_shown_works(conn)
    conn.close()
    candidates = sidecar_build.assign_opaque_work_ids(candidates, crosswalk_path, create_if_missing=True)
    work_id = candidates[0]["work_id"]

    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id=work_id, candidate_title=neutral_title, owner_verdict="approve"),
    ])

    return {
        "research_db": research_db,
        "crosswalk_path": crosswalk_path,
        "approved_csv": approved_csv,
        "out_db_path": tmp_path / "discovery_data" / "discovery-v1.db",
    }


def test_finalize_build_end_to_end_success(tmp_path):
    fx = _build_minimal_finalize_fixture(tmp_path)
    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    assert Path(stats["db_path"]).exists()
    assert stats["row_counts"]["works"] == 1
    assert stats["row_counts"]["discovery_claim"] == 1
    assert stats["band_precision_rows"] > 0

    rc = verify_mod.verify(stats["db_path"], expected_frame_hash=stats["frame_content_hash"])
    assert rc == 0

    # F13: band_precision rows are ALREADY inside the file whose bytes were hashed
    # (re-hashing the final file must reproduce the recorded content_hash).
    assert sidecar_build._hash_file(Path(stats["db_path"])) == stats["content_hash"]

    manifest = json.loads((Path(fx["out_db_path"]).parent / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["content_hash"] == stats["content_hash"]
    assert manifest["frame_content_hash"] == stats["frame_content_hash"]


def test_finalize_build_masking_gate_blocks_and_removes_db(tmp_path):
    fx = _build_minimal_finalize_fixture(tmp_path, neutral_title="LEAK-MARKER-ABC neutral title")
    with pytest.raises(sidecar_build.MaskingGateFailure):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            masking_patterns=["LEAK-MARKER-ABC"],
        )
    assert not fx["out_db_path"].exists()


def test_finalize_build_masking_scan_exception_still_removes_db(tmp_path, monkeypatch):
    """M2: ANY exception raised while loading patterns or scanning (a
    ScanError from a broken pattern file, an unreadable db, etc.) -- NOT
    just an actual masking HIT -- must still delete the just-written
    output .db before propagating. Previously only the HIT branch cleaned
    up; a scan-time exception left a half-finalized, unproven-clean
    artifact on disk."""
    fx = _build_minimal_finalize_fixture(tmp_path)

    def _boom(*_args, **_kwargs):
        raise cam.ScanError("simulated scan-time failure (M2 regression)")

    monkeypatch.setattr(sidecar_build._cam, "scan_sqlite", _boom)

    with pytest.raises(cam.ScanError):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
        )
    assert not fx["out_db_path"].exists()


def test_finalize_build_requires_nonempty_masking_patterns(tmp_path):
    fx = _build_minimal_finalize_fixture(tmp_path)
    with pytest.raises(cam.ScanError):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            masking_patterns=[],
        )
    # M2: an empty pattern set is a _require_patterns ScanError, NOT an
    # actual masking hit -- it must ALSO trigger cleanup of the
    # just-written .db (previously only a HIT deleted the file, leaving
    # an unscanned, unproven-clean artifact on disk on this path).
    assert not fx["out_db_path"].exists()


def test_finalize_build_artifact_scan_is_nonblocking(tmp_path):
    """The non-blocking artifact scan targets the CANDIDATE csv, never the
    shipped .db. Under the enriched schema an M-source (masked) candidate's
    author/genre are ALWAYS blanked (fail-closed, Task A) -- so the sentinel
    must instead ride an auto-adopted OPEN-CORPUS candidate's verbatim
    `candidate_title` to still exercise "leaked into the artifact, never
    shipped because left unapproved"."""
    research_rows = [
        _mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 40, 0.9]]"),
        _mk_track1_row("p2", "s2", "raw:w2-open", "Sefaria", title="SECRET-RAW-TITLE-XYZ"),
    ]
    research_db = _build_track1_db(tmp_path, research_rows, name="research.db")
    conn = sqlite3.connect(str(research_db))
    conn.execute("INSERT INTO pages VALUES ('p1','s1','witness',40,'sample htr text','htr',NULL,NULL,40)")
    conn.execute("INSERT INTO pages VALUES ('p2','s2','witness',40,'other htr text','htr',NULL,NULL,40)")
    conn.commit()
    conn.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    conn = sqlite3.connect(f"file:{research_db}?mode=ro", uri=True)
    candidates = sidecar_build.select_shown_works(conn)
    conn.close()
    candidates = sidecar_build.assign_opaque_work_ids(candidates, crosswalk_path, create_if_missing=True)
    by_raw = {c["raw_work_id"]: c for c in candidates}
    work_id_1 = by_raw["raw:w1"]["work_id"]
    # raw:w2-open is deliberately left UNAPPROVED -- candidate-only, never shipped.

    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        _candidate_row(work_id=work_id_1, candidate_title="Clean Neutral Title", owner_verdict="approve"),
    ])

    out_db_path = tmp_path / "discovery_data" / "discovery-v1.db"
    review_artifact_path = tmp_path / "candidates.csv"
    stats = sidecar_build.finalize_build(
        source_db_path=str(research_db),
        from_approved_path=str(approved_csv),
        crosswalk_path=str(crosswalk_path),
        out_db_path=str(out_db_path),
        review_artifact_path=str(review_artifact_path),
        masking_patterns=["SECRET-RAW-TITLE-XYZ"],
    )
    assert stats["artifact_masking_issues"] > 0
    assert Path(stats["db_path"]).exists()
    # the unapproved work never reached the shipped works table
    assert stats["row_counts"]["works"] == 1


# ===========================================================================
# H2: release-mode input-completeness gate
# ===========================================================================

def _router_rows(bucket, total, two_seed):
    """Build `total` router rows for `bucket`, exactly `two_seed` of which carry
    trials>=2 (the rest trials=1), matching the frozen 106/18 & 108/57 shape."""
    return [
        {"_bucket": bucket, "trials": 2 if i < two_seed else 1}
        for i in range(total)
    ]


def _h2_complete_kwargs(**overrides):
    """A fully-conforming set of _assert_release_inputs_complete kwargs
    (every collection at its EXACT frozen expected count, and each router
    collection at its frozen two-seed subset + bucket identity) -- individual
    tests override just the field(s) they want to break."""
    kwargs = dict(
        release=True, allow_partial_sources=False,
        e1_ra_confirmed=[{}] * sidecar_build._EXPECTED_E1_RA_CONFIRMED_ROWS,
        e1_adjudicated_a=[{}] * sidecar_build._EXPECTED_E1_ADJUDICATED_A_ROWS,
        e1_rb_screening=[{}] * sidecar_build._EXPECTED_E1_RB_SCREENING_ROWS,
        e1_r3_frame=[{}] * sidecar_build._EXPECTED_E1_R3_FRAME_ROWS,
        q2_witness_collection=[{}] * sidecar_build._EXPECTED_Q2_WITNESS_COLLECTION_ROWS,
        q2_shared_text=[{}] * sidecar_build._EXPECTED_Q2_SHARED_TEXT_ROWS,
        q2_collection_tafsir_targum=_router_rows(
            "tafsir_targum", sidecar_build._EXPECTED_TAFSIR_TARGUM_ROWS,
            sidecar_build._EXPECTED_TAFSIR_TARGUM_TWO_SEED,
        ),
        q2_collection_with_arabic=_router_rows(
            "with_arabic", sidecar_build._EXPECTED_WITH_ARABIC_ROWS,
            sidecar_build._EXPECTED_WITH_ARABIC_TWO_SEED,
        ),
        tier_a_row_count=sidecar_build._EXPECTED_TIER_A_ROWS,
    )
    kwargs.update(overrides)
    return kwargs


def test_assert_release_inputs_complete_passes_when_every_count_matches():
    # Must NOT raise.
    sidecar_build._assert_release_inputs_complete(**_h2_complete_kwargs())


def test_assert_release_inputs_complete_non_release_is_a_noop():
    # release=False -- the gate never fires, regardless of how incomplete
    # the inputs are (unit tests / build_claims_and_evidence-level tests
    # rely on this).
    sidecar_build._assert_release_inputs_complete(**_h2_complete_kwargs(
        release=False, e1_ra_confirmed=[], q2_witness_collection=[], tier_a_row_count=0,
    ))


def test_assert_release_inputs_complete_missing_collection_raises():
    """H2: a MISSING collection (empty list, as if the path were never
    supplied) must abort a release build -- never silently ingest as
    empty and produce a tier-A-only sidecar that still passes every
    other gate."""
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError, match="q2_witness_collection"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(q2_witness_collection=[])
        )


def test_assert_release_inputs_complete_short_collection_raises():
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError, match="e1_r3_frame"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(e1_r3_frame=[{}] * 100)
        )


def test_assert_release_inputs_complete_tier_a_count_mismatch_raises():
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError, match="tier_a"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(tier_a_row_count=1)
        )


def test_assert_release_inputs_complete_router_two_seed_subset_mismatch_raises():
    """Codex R5 MED: a router collection with the correct TOTAL but a drifted
    two-seed (trials>=2) subset count must be rejected -- the frozen contract
    pins 106/18 & 108/57, not just the totals."""
    # correct total (106) but only 5 two-seed rows instead of the frozen 18
    bad = _router_rows("tafsir_targum", sidecar_build._EXPECTED_TAFSIR_TARGUM_ROWS, 5)
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError, match="two-seed"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(q2_collection_tafsir_targum=bad)
        )


def test_assert_release_inputs_complete_router_wrong_bucket_identity_raises():
    """Codex R5 MED: every router row's _bucket must match the expected bucket
    identity (a wrong file / mixed collection must not pass H2)."""
    bad = _router_rows("with_arabic", sidecar_build._EXPECTED_WITH_ARABIC_ROWS,
                       sidecar_build._EXPECTED_WITH_ARABIC_TWO_SEED)
    bad[0] = {**bad[0], "_bucket": "tafsir_targum"}  # one row on the wrong bucket
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError, match="_bucket"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(q2_collection_with_arabic=bad)
        )


def test_assert_release_inputs_complete_allow_partial_sources_with_release_raises():
    """H2: --allow-partial-sources may NEVER be combined with release=True --
    a release build must never silently accept a partial source set."""
    with pytest.raises(ValueError, match="allow-partial-sources"):
        sidecar_build._assert_release_inputs_complete(
            **_h2_complete_kwargs(allow_partial_sources=True)
        )


def test_finalize_build_release_true_with_no_collections_raises_incomplete(tmp_path):
    """End-to-end (H2): finalize_build(release=True) with NO Q2/E1
    collection paths supplied (every collection loads as an empty list,
    and the fixture's tiny research DB has only 1 track1_matches row) must
    raise ReleaseInputsIncompleteError -- confirms the gate is actually
    wired into finalize_build, not just unit-tested in isolation."""
    fx = _build_minimal_finalize_fixture(tmp_path)
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
            release=True,
            frozen_precision_defaults=True,
        )
    assert not fx["out_db_path"].exists()


# ===========================================================================
# Codex R2 MED (gate-ordering fix): the H2/H3 gates must run BEFORE any
# output/crosswalk/review-artifact mutation -- a failed release build must
# leave every prior artifact completely untouched.
# ===========================================================================

def test_finalize_build_h2_gate_failure_leaves_prior_artifacts_untouched(tmp_path):
    """A release=True call that fails the H2 completeness gate must NOT
    have deleted the prior output .db, persisted a crosswalk update, or
    overwritten the review artifact -- all three mutations must run AFTER
    the gate, never before."""
    fx = _build_minimal_finalize_fixture(tmp_path)
    review_artifact_path = fx["out_db_path"].parent / "candidates.csv"

    # First, a successful non-release build to create the "prior" artifacts.
    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        review_artifact_path=str(review_artifact_path),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    assert Path(stats["db_path"]).exists()
    prior_db_bytes = Path(stats["db_path"]).read_bytes()
    prior_crosswalk_bytes = fx["crosswalk_path"].read_bytes()
    prior_review_bytes = review_artifact_path.read_bytes()

    # A release=True call with NO Q2/E1 collections must fail the H2 gate
    # WITHOUT touching any of the three prior artifacts.
    with pytest.raises(sidecar_build.ReleaseInputsIncompleteError):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            review_artifact_path=str(review_artifact_path),
            masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
            release=True,
            frozen_precision_defaults=True,
        )

    assert Path(stats["db_path"]).read_bytes() == prior_db_bytes, (
        "H2 gate failure must never have deleted the prior output .db"
    )
    assert fx["crosswalk_path"].read_bytes() == prior_crosswalk_bytes, (
        "H2 gate failure must never have persisted a crosswalk update"
    )
    assert review_artifact_path.read_bytes() == prior_review_bytes, (
        "H2 gate failure must never have overwritten the review artifact"
    )


def test_finalize_build_h3_gate_failure_leaves_prior_artifacts_untouched(tmp_path):
    """A release=True call with NEITHER --precision-spec NOR
    --frozen-precision-defaults must fail the H3 gate WITHOUT touching any
    prior artifact -- H3 is a pure argument-validation gate that must run
    before ANY file mutation, regardless of collection completeness."""
    fx = _build_minimal_finalize_fixture(tmp_path)
    review_artifact_path = fx["out_db_path"].parent / "candidates.csv"

    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        review_artifact_path=str(review_artifact_path),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    prior_db_bytes = Path(stats["db_path"]).read_bytes()
    prior_crosswalk_bytes = fx["crosswalk_path"].read_bytes()
    prior_review_bytes = review_artifact_path.read_bytes()

    with pytest.raises(ValueError, match="precision-spec"):
        sidecar_build.finalize_build(
            source_db_path=str(fx["research_db"]),
            from_approved_path=str(fx["approved_csv"]),
            crosswalk_path=str(fx["crosswalk_path"]),
            out_db_path=str(fx["out_db_path"]),
            review_artifact_path=str(review_artifact_path),
            masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
            release=True,
            # neither precision_spec nor frozen_precision_defaults supplied
        )

    assert Path(stats["db_path"]).read_bytes() == prior_db_bytes, (
        "H3 gate failure must never have deleted the prior output .db"
    )
    assert fx["crosswalk_path"].read_bytes() == prior_crosswalk_bytes, (
        "H3 gate failure must never have persisted a crosswalk update"
    )
    assert review_artifact_path.read_bytes() == prior_review_bytes, (
        "H3 gate failure must never have overwritten the review artifact"
    )


# ===========================================================================
# H3: real/release band_precision -- never a fabricated tier_a number
# ===========================================================================

def test_resolve_band_precision_spec_explicit_spec_wins():
    """H3: an explicit --precision-spec wins verbatim (identity) -- but
    (Codex R2 HIGH) only once it is validated as a frozen-conforming spec;
    a fabricated/incomplete spec like the old
    `[{"scope": "collection", "collection_id": "x"}]` must now be REJECTED
    (see test_resolve_band_precision_spec_explicit_spec_* rejection tests
    below), so this positive case uses a legitimately-shaped owner override
    (a copy of the frozen rows with a non-validated metadata field tweaked,
    modeling a 134-07 owner annotation) to prove verbatim pass-through
    still holds for a VALID spec."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    custom[0] = {**custom[0], "notes": "owner override at 134-07"}
    result = sidecar_build._resolve_band_precision_spec(
        precision_spec=custom, frozen_precision_defaults=False, release=True,
    )
    assert result is custom
    assert result[0]["notes"] == "owner override at 134-07"


# ---------------------------------------------------------------------------
# Codex R2 HIGH: _resolve_band_precision_spec / _validate_precision_spec --
# an explicit --precision-spec must be validated against the EXACT frozen
# release row-set BEFORE any output/artifact write, never trusted verbatim.
# ---------------------------------------------------------------------------

def test_resolve_band_precision_spec_rejects_fabricated_tier_a():
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    for row in custom:
        if row["scope"] == "band" and row["confidence_band"] == "tier_a":
            row["precision"] = 0.90  # fabricated -- must never be accepted
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="tier_a"):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_rejects_missing_frozen_row():
    custom = [
        r for r in sidecar_build._frozen_real_band_precision_rows()
        if not (r["scope"] == "band" and r["confidence_band"] == "screening_canon")
    ]
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="screening_canon"):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_rejects_extra_band_row():
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    custom.append({
        "scope": "band", "collection_id": "e1_certification_registry_v1",
        "evidence_source": "track1_direct", "confidence_band": "bogus_extra_band",
        "numerator": None, "denominator": None, "precision": None,
        "ci_low": None, "ci_high": None, "method": None,
        "sampling_frame": None, "ins_policy": None, "weighting": None, "notes": None,
    })
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="unexpected"):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_rejects_extra_collection_row():
    """Codex R3 HIGH: an EXTRA scope='collection' row with a DIFFERENT
    collection_id was previously ignored (the value check only located the ONE
    frozen collection_id and never counted total collection rows). The exact
    frozen key multiset must reject it before any output/artifact write."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    custom.append({
        "scope": "collection", "collection_id": "some_other_collection_v1",
        "evidence_source": None, "confidence_band": None,
        "numerator": 1, "denominator": 1, "precision": 0.5,
        "ci_low": 0.4, "ci_high": 0.6, "method": "bogus",
        "sampling_frame": "x", "ins_policy": "x", "weighting": "x", "notes": None,
    })
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_rejects_band_on_wrong_collection_id():
    """Codex R3 HIGH: a band with a valid (evidence_source, confidence_band)
    pair but the WRONG collection_id previously passed (the band value check
    keyed on (source, band) ONLY, ignoring collection_id). The full
    (scope, collection_id, evidence_source, confidence_band) key multiset must
    reject it."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    for row in custom:
        if row["scope"] == "band" and row["confidence_band"] == "expert_verified":
            # move a real track1 measured band onto the propagated collection id
            row["collection_id"] = "propagated_witness_collection_v1"
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_validate_precision_spec_message_never_echoes_supplied_key_values():
    """Codex R4 MED (masking): a supplied --precision-spec is potentially
    hand-/owner-authored, so a malformed key field could embed restricted
    text. The structural diagnostic must report unexpected/duplicate rows by
    POSITION only -- never rendering a supplied scope/collection_id/
    evidence_source/confidence_band value into the raised message."""
    sentinel = "RESTRICTED_SENTINEL_DO_NOT_LEAK"
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    custom.append({
        "scope": "band", "collection_id": sentinel,
        "evidence_source": sentinel, "confidence_band": sentinel,
        "numerator": None, "denominator": None, "precision": None,
        "ci_low": None, "ci_high": None, "method": None,
        "sampling_frame": None, "ins_policy": None, "weighting": None, "notes": None,
    })
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError) as exc_info:
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )
    assert sentinel not in str(exc_info.value)


def test_validate_precision_spec_value_message_never_echoes_supplied_precision():
    """Codex R5 MED (masking): a supplied precision VALUE (not just key fields)
    could embed restricted text if given as a string. Neither the collection-
    nor the band-precision mismatch message may render the supplied value."""
    sentinel = "RESTRICTED_PRECISION_SENTINEL"
    coll_bad = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    for r in coll_bad:
        if r["scope"] == "collection":
            r["precision"] = sentinel
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError) as ei1:
        sidecar_build._resolve_band_precision_spec(
            precision_spec=coll_bad, frozen_precision_defaults=False, release=True,
        )
    assert sentinel not in str(ei1.value)
    band_bad = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    for r in band_bad:
        if r["scope"] == "band" and r["confidence_band"] == "expert_verified":
            r["precision"] = sentinel
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError) as ei2:
        sidecar_build._resolve_band_precision_spec(
            precision_spec=band_bad, frozen_precision_defaults=False, release=True,
        )
    assert sentinel not in str(ei2.value)


def test_resolve_band_precision_spec_rejects_the_old_minimal_fabricated_spec():
    """The EXACT scenario the HIGH finding cites: a spec that is basically
    just `[{"scope": "collection", "collection_id": "x"}]` (missing every
    frozen row, wrong collection_id, no measured bands at all) must be
    rejected outright rather than reaching a real/release build verbatim."""
    custom = [{"scope": "collection", "collection_id": "x"}]
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=custom, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_accepts_frozen_rows_unchanged():
    """Positive case: the exact frozen rows (unmodified) must always pass."""
    frozen = sidecar_build._frozen_real_band_precision_rows()
    result = sidecar_build._resolve_band_precision_spec(
        precision_spec=frozen, frozen_precision_defaults=False, release=True,
    )
    assert result is frozen


# ---------------------------------------------------------------------------
# D-02a (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment
# 2026-08-02): the tier_a CERT-01 AUTHORIZATION lockstep -- fixtures proving
# BOTH the pass and the fail branch (136-CONTEXT.md D-02a).
# ---------------------------------------------------------------------------

def _tier_a_row(rows):
    return next(
        r for r in rows
        if r["scope"] == "band" and r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
        and r["confidence_band"] == ids.CONFIDENCE_BAND_TIER_A
    )


def test_d02a_pass_branch_authorized_pair_validates_and_flips_default_eligible():
    """PASS branch: the frozen (unmodified) row-set carries the authorized
    tier_a pair and `_validate_precision_spec` raises nothing. The SAME
    authorized values, fed through `is_default_eligible`, flip the D-18 gate
    True -- and the test documents the delta by also asserting the
    PRE-AMENDMENT all-NULL shape still reads False."""
    frozen = sidecar_build._frozen_real_band_precision_rows()
    sidecar_build._validate_precision_spec(frozen)  # must not raise

    tier_a = _tier_a_row(frozen)
    assert tier_a["precision"] is None
    assert tier_a["ci_low"] == 0.9084
    assert tier_a["measurement_status"] == "measured_pass"

    assert is_default_eligible(
        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A,
        "unreviewed", "shipped", "measured_pass", ci_low=0.9084,
    ) is True
    # Documents exactly what changed: the PRE-amendment shape (no stored
    # measurement_status/ci_low at all) never qualified tier_a for default
    # visibility.
    assert is_default_eligible(
        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A,
        "unreviewed", "shipped", None, ci_low=None,
    ) is False


def test_d02a_fail_a_ci_low_below_strict_floor_rejected():
    """FAIL (a): a tier_a ci_low below STRICT_FLOOR (0.85) can never match
    the frozen 0.9084 -- _validate_precision_spec rejects it."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    _tier_a_row(custom)["ci_low"] = 0.70
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="tier_a"):
        sidecar_build._validate_precision_spec(custom)


def test_d02a_fail_b_measurement_status_outside_closed_vocabulary_rejected():
    """FAIL (b): a measurement_status outside MEASUREMENT_STATUSES is a
    build error, independent of whether it happens to also mismatch the
    frozen row's expected value."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    _tier_a_row(custom)["measurement_status"] = "bogus_status_outside_vocab"
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="tier_a"):
        sidecar_build._validate_precision_spec(custom)


def test_d02a_fail_c_non_null_precision_on_tier_a_rejected():
    """FAIL (c): the pre-existing rule -- any non-NULL tier_a precision is
    rejected. Proves this rule survived the D-02a widening unrelaxed."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    _tier_a_row(custom)["precision"] = 0.90  # any fabricated non-NULL number must be rejected
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match="tier_a"):
        sidecar_build._validate_precision_spec(custom)


def test_d02a_fail_d_measured_pass_on_unauthorized_band_rejected():
    """FAIL (d): `measured_pass` asserted on a band OTHER than tier_a (not
    part of the frozen authorized set) is rejected -- otherwise an
    arbitrary band could smuggle itself into default visibility through
    this exact slot."""
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    for r in custom:
        if r["scope"] == "band" and r["confidence_band"] == ids.CONFIDENCE_BAND_SCREENING_RB:
            r["measurement_status"] = "measured_pass"
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError, match=ids.CONFIDENCE_BAND_SCREENING_RB):
        sidecar_build._validate_precision_spec(custom)


def test_d02a_mechanism_dict_override_carries_measurement_status_into_insert_params():
    """Mechanism test: `{"measurement_status": None, **r}` (the exact
    expression at the band_precision INSERT site) lets a row's OWN
    `measurement_status` key win over the `None` default -- non-obvious from
    reading the INSERT alone, so it is pinned here directly."""
    frozen = sidecar_build._frozen_real_band_precision_rows()
    tier_a = _tier_a_row(frozen)
    merged = {"measurement_status": None, **tier_a}
    assert merged["measurement_status"] == "measured_pass"


def test_d02a_masking_never_echoes_supplied_ci_low_sentinel():
    """Masking-discipline test: a recognisable sentinel ci_low value must
    never appear anywhere in the raised violation text -- only the frozen
    expected value may be named (same discipline as the existing R4/R5
    masking tests above)."""
    sentinel = "RESTRICTED_D02A_CI_LOW_SENTINEL_DO_NOT_LEAK"
    custom = [dict(r) for r in sidecar_build._frozen_real_band_precision_rows()]
    _tier_a_row(custom)["ci_low"] = sentinel
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError) as exc_info:
        sidecar_build._validate_precision_spec(custom)
    assert sentinel not in str(exc_info.value)


def test_resolve_band_precision_spec_frozen_defaults_tier_a_is_null():
    result = sidecar_build._resolve_band_precision_spec(
        precision_spec=None, frozen_precision_defaults=True, release=True,
    )
    tier_a_row = next(
        r for r in result
        if r["scope"] == "band" and r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
        and r["confidence_band"] == ids.CONFIDENCE_BAND_TIER_A
    )
    assert tier_a_row["precision"] is None
    # the three MEASURED track1_direct bands are still present at their
    # frozen values.
    by_band = {
        r["confidence_band"]: r["precision"] for r in result
        if r["scope"] == "band" and r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
    }
    assert by_band[ids.CONFIDENCE_BAND_EXPERT_VERIFIED] == 0.889
    assert by_band[ids.CONFIDENCE_BAND_SCREENING_RB] == 0.859
    assert by_band[ids.CONFIDENCE_BAND_SCREENING_CANON] == 0.647


def test_resolve_band_precision_spec_release_without_explicit_choice_raises():
    """H3: a --release build with NEITHER --precision-spec NOR
    --frozen-precision-defaults must raise -- never silently default."""
    with pytest.raises(ValueError, match="precision-spec"):
        sidecar_build._resolve_band_precision_spec(
            precision_spec=None, frozen_precision_defaults=False, release=True,
        )


def test_resolve_band_precision_spec_non_release_defaults_to_frozen_rows_tier_a_null():
    """Non-release calls (unit tests, --allow-partial-sources smoke builds)
    default to the frozen-contract rows -- tier_a NULL, never the
    SYNTHETIC-fixture-only 0.90 placeholder from _band_precision_rows."""
    result = sidecar_build._resolve_band_precision_spec(
        precision_spec=None, frozen_precision_defaults=False, release=False,
    )
    tier_a_row = next(
        r for r in result
        if r["scope"] == "band" and r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
        and r["confidence_band"] == ids.CONFIDENCE_BAND_TIER_A
    )
    assert tier_a_row["precision"] is None


def test_finalize_build_default_never_writes_fabricated_tier_a_precision(tmp_path):
    """End-to-end (H3): a finalize_build call with NO release/precision
    flags (the existing test-suite calling convention) must still never
    write the synthetic-mode 0.90 tier_a placeholder into a real build."""
    fx = _build_minimal_finalize_fixture(tmp_path)
    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    conn = sqlite3.connect(f"file:{stats['db_path']}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT precision FROM band_precision WHERE scope='band' "
            "AND evidence_source='track1_direct' AND confidence_band='tier_a'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] is None


# ===========================================================================
# 136-11 Task 1: persisted coverage (D-08a) + materialized band_rank (D-10a)
# + the authorized index set.
# ===========================================================================

# The Hebrew page text below is FABRICATED (a repeated alef-bet run), never
# real research content -- it exists only so `norm_stream_letter_count` has a
# nonzero denominator to divide by.
_HEB_PAGE_TEXT = "אבגדהוזחטיכלמנסעפצקרשת" * 5   # 110 Hebrew base letters


def _ingest_direct_and_propagated_specs(*, page_text=_HEB_PAGE_TEXT, matched_letters=88):
    """One track1_direct spec (through `_attach_coverage`) and one propagated
    spec (which never gets a coverage denominator at all), over the same page."""
    work_index = {"raw:w1": {"work_id": "w000001"}}
    page_idx = sidecar_build.PageTextIndex(_pages_conn([("p1", "htr", page_text)]))
    direct = sidecar_build._ingest_e1_rows(
        [{"page_id": "p1", "sys_id": "s1", "work_id": "raw:w1",
          "o0": 0, "o1": 90, "ml": matched_letters, "dens": 0.2, "n_spans": 1}],
        work_index=work_index, page_index=page_idx,
        confidence_band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED,
        adjudication_status=ids.ADJUDICATION_STATUS_UNREVIEWED,
        audit_status=ids.AUDIT_STATUS_AUDIT_PENDING,
    )
    propagated = sidecar_build._ingest_propagated_witness(
        [{"cpage": "p1", "csys": "s1", "work_id": "raw:w1", "_bucket": "witness",
          "is_new": False, "impurity": False, "trials": 3,
          "seeds": [{"occ0": 0, "occ1": 30, "occ_class": "core",
                     "seed_page": "sp1", "seed_sys": "ss1"}]}],
        work_index, page_idx,
    )
    return direct + propagated


def _assemble_into_schema(evidence_specs, *, corpus=ids.SOURCE_CORPUS_SEFARIA):
    """Assemble specs and INSERT them through the real column lists, so these
    tests exercise the persistence path rather than the in-memory dicts."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    cur = conn.cursor()
    work_ids = sorted({e["work_id"] for e in evidence_specs})
    sidecar_build._insert_works_real(
        cur,
        [{"work_id": w, "neutral_title": f"Synthetic Title {w}", "author": None,
          "genre": None, "source_corpus": corpus} for w in work_ids],
    )
    result = sidecar_build.assemble_claims_and_evidence(
        evidence_specs, {w: corpus for w in work_ids}
    )
    sidecar_build._insert_claims_and_evidence_real(
        cur, result["claim_rows"], result["evidence_rows"]
    )
    conn.commit()
    return conn


def test_coverage_ppm_direct_family_only_propagated_rows_carry_none():
    """D-08a: the percentage is stored for the DIRECT family only. A propagated
    row gets no `coverage_ppm` at all -- because it has no page-length
    denominator, not because the number was omitted for display reasons."""
    conn = _assemble_into_schema(_ingest_direct_and_propagated_specs())
    try:
        rows = dict(conn.execute(
            "SELECT evidence_source, coverage_ppm FROM discovery_evidence"
        ).fetchall())
        statuses = dict(conn.execute(
            "SELECT evidence_source, coverage_status FROM discovery_evidence"
        ).fetchall())
    finally:
        conn.close()

    # 88 matched letters / 110 normalized page letters = 0.8 -> 800000 ppm.
    assert rows[ids.EVIDENCE_SOURCE_TRACK1_DIRECT] == 800000
    assert statuses[ids.EVIDENCE_SOURCE_TRACK1_DIRECT] == sidecar_build.COVERAGE_STATUS_MEASURED
    assert rows[ids.EVIDENCE_SOURCE_PROPAGATED] is None
    assert statuses[ids.EVIDENCE_SOURCE_PROPAGATED] == sidecar_build.COVERAGE_STATUS_NOT_APPLICABLE


def test_coverage_status_three_reachable_values_and_no_denominator_is_not_zero():
    """T-136-11-04: a MISSING denominator must never be stored as a genuine
    coverage of zero. `compute_page_coverage` returns 0.0 in that case (a
    routing sentinel), so the persistence layer has to tell the two apart --
    otherwise a surface reads "we could not measure" as "we measured almost
    nothing"."""
    measured = _ingest_direct_and_propagated_specs()
    # A page whose text carries NO Hebrew base letters at all -> the Lever-1
    # denominator is 0, `compute_page_coverage` returns its 0.0 sentinel.
    no_denominator = _ingest_direct_and_propagated_specs(page_text="latin only text")

    conn = _assemble_into_schema(measured)
    try:
        measured_rows = conn.execute(
            "SELECT coverage_status, coverage_ppm FROM discovery_evidence "
            "WHERE evidence_source='track1_direct'"
        ).fetchall()
    finally:
        conn.close()

    conn = _assemble_into_schema(no_denominator)
    try:
        nd_rows = conn.execute(
            "SELECT coverage_status, coverage_ppm FROM discovery_evidence "
            "WHERE evidence_source='track1_direct'"
        ).fetchall()
        na_rows = conn.execute(
            "SELECT coverage_status, coverage_ppm FROM discovery_evidence "
            "WHERE evidence_source='propagated'"
        ).fetchall()
    finally:
        conn.close()

    reachable = (
        {r[0] for r in measured_rows} | {r[0] for r in nd_rows} | {r[0] for r in na_rows}
    )
    assert reachable == {
        sidecar_build.COVERAGE_STATUS_MEASURED,
        sidecar_build.COVERAGE_STATUS_NO_DENOMINATOR,
        sidecar_build.COVERAGE_STATUS_NOT_APPLICABLE,
    }
    assert reachable == set(sidecar_build.COVERAGE_STATUSES)

    # The load-bearing assertion: no_denominator stores NULL, never 0.
    assert nd_rows[0][0] == sidecar_build.COVERAGE_STATUS_NO_DENOMINATOR
    assert nd_rows[0][1] is None
    # ...while the routing input itself really did see the 0.0 sentinel, so the
    # two facts genuinely were indistinguishable before this column existed.
    assert sidecar_build.compute_page_coverage(88, 0) == 0.0


def test_coverage_ppm_helper_maps_every_case():
    fn = sidecar_build.coverage_ppm_and_status
    assert fn(ids.EVIDENCE_SOURCE_PROPAGATED, None, None) == (
        None, sidecar_build.COVERAGE_STATUS_NOT_APPLICABLE)
    # A propagated row stays not_applicable even if a coverage value somehow
    # reached it -- D-08a is a family rule, not a "value present?" rule.
    assert fn(ids.EVIDENCE_SOURCE_PROPAGATED, 0.9, 100) == (
        None, sidecar_build.COVERAGE_STATUS_NOT_APPLICABLE)
    assert fn(ids.EVIDENCE_SOURCE_TRACK1_DIRECT, None, 100) == (
        None, sidecar_build.COVERAGE_STATUS_NO_DENOMINATOR)
    assert fn(ids.EVIDENCE_SOURCE_TRACK1_DIRECT, 0.0, 0) == (
        None, sidecar_build.COVERAGE_STATUS_NO_DENOMINATOR)
    assert fn(ids.EVIDENCE_SOURCE_TRACK1_DIRECT, 1.0, 100) == (
        1000000, sidecar_build.COVERAGE_STATUS_MEASURED)
    # A genuine near-zero measurement is stored AS a measurement.
    assert fn(ids.EVIDENCE_SOURCE_TRACK1_DIRECT, 0.000001, 1000000) == (
        1, sidecar_build.COVERAGE_STATUS_MEASURED)


def test_coverage_computation_functions_are_unchanged():
    """The metric itself was always correct -- only its persistence was
    missing. Pin the two computation functions behaviourally so a future edit
    to the PERSISTENCE path can never quietly change the NUMBER."""
    assert sidecar_build.norm_stream_letter_count(_HEB_PAGE_TEXT) == 110
    assert sidecar_build.norm_stream_letter_count("latin only text") == 0
    assert sidecar_build.norm_stream_letter_count(None) == 0
    assert sidecar_build.compute_page_coverage(None, 100) is None
    assert sidecar_build.compute_page_coverage(88, 110) == pytest.approx(0.8)
    assert sidecar_build.compute_page_coverage(500, 100) == 1.0   # clamped
    assert sidecar_build.compute_page_coverage(88, None) == 0.0   # the sentinel


def test_band_rank_equals_the_runtime_lattice_for_every_pair():
    """T-136-11-02: the STORED sort key and the lattice the runtime service
    sorts by must be the same ordering -- asserted over the FULL mapping, not
    a sample, and against `shared.discovery_service` itself rather than a
    second literal copy of the table."""
    from shared import discovery_service as runtime

    for evidence_source, confidence_band in runtime._BAND_RANK_ORDER:
        assert sidecar_build.evidence_band_rank(evidence_source, confidence_band) == \
            runtime._band_rank(evidence_source, confidence_band)
    # An unknown pair ranks last at build time exactly as it does at runtime.
    assert sidecar_build.evidence_band_rank("track1_direct", "no_such_band") == \
        runtime._band_rank("track1_direct", "no_such_band")
    assert sidecar_build.BAND_RANK_LATTICE_SIZE == len(runtime._BAND_RANK_ORDER)


def test_band_rank_materialized_matches_runtime_for_every_pair_in_a_build():
    """The same equality, asserted over every (evidence_source,
    confidence_band) pair a real fixture build actually produces."""
    from shared import discovery_service as runtime

    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="band-rank-fixture")
        pairs = conn.execute(
            "SELECT DISTINCT evidence_source, confidence_band, band_rank FROM discovery_evidence"
        ).fetchall()
    finally:
        conn.close()
    assert pairs, "fixture produced no evidence rows"
    for evidence_source, confidence_band, stored in pairs:
        assert stored == runtime._band_rank(evidence_source, confidence_band), (
            f"stored band_rank {stored} != runtime lattice for "
            f"({evidence_source!r}, {confidence_band!r})"
        )
    # Every band the fixture emits is a RANKED one (never the unranked sentinel).
    assert all(stored < len(runtime._BAND_RANK_ORDER) for _, _, stored in pairs)


def test_builder_does_not_redefine_the_band_rank_ordering():
    """The ordering is IMPORTED, never re-declared -- a second literal list in
    the builder is exactly the drift this test exists to prevent."""
    source = Path(sidecar_build.__file__).read_text(encoding="utf-8")
    assert "from shared.discovery_service import" in source
    assert "_BAND_RANK_ORDER" in source
    # No second assignment of the lattice anywhere in the builder.
    assert not re.search(r"^_BAND_RANK_ORDER\s*[:=]", source, re.MULTILINE)


_AUTHORIZED_INDEXES = {
    "discovery_evidence": {
        "ix_discovery_evidence_coverage_ppm",
        "ix_discovery_evidence_band_rank",
        "ix_discovery_evidence_novelty_status",
    },
    "discovery_claim": {"ux_discovery_claim_display_evidence_id"},
    "discovery_identification": {
        "ix_discovery_identification_order",
        "ix_discovery_identification_canonical_work_id",
        "ix_discovery_identification_sys_id",
    },
    "manuscript_display": {"ix_manuscript_display_sort"},
}


def test_d10a_authorized_index_set_is_present_in_a_fixture_build():
    """Schema Amendment 2026-08-02 (D): the authorized index set, and the
    `discovery_claim(display_evidence_id)` index is UNIQUE (a real invariant,
    not merely a lookup hint)."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="index-fixture")
        for table, expected in _AUTHORIZED_INDEXES.items():
            present = {r[1] for r in conn.execute(f"PRAGMA index_list('{table}')")}
            assert expected <= present, f"{table}: missing {sorted(expected - present)}"
        unique_flags = {
            r[1]: r[2] for r in conn.execute("PRAGMA index_list('discovery_claim')")
        }
        assert unique_flags["ux_discovery_claim_display_evidence_id"] == 1
        # The novelty index targets the STATUS column, never the legacy boolean.
        (novelty_sql,) = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='ix_discovery_evidence_novelty_status'"
        ).fetchone()
        assert "novelty_status" in novelty_sql and "is_new" not in novelty_sql
    finally:
        conn.close()


def test_display_evidence_id_uniqueness_is_enforced_not_merely_indexed():
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        conn.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
            "VALUES ('w000001','w000001','T',NULL,NULL,'sefaria')"
        )
        conn.execute(
            "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
            "display_evidence_id, source_corpus, sidecar_version) "
            "VALUES ('p1','w000001','c1','direct_witness','SHARED','sefaria','v')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
                "display_evidence_id, source_corpus, sidecar_version) "
                "VALUES ('p2','w000001','c2','direct_witness','SHARED','sefaria','v')"
            )
    finally:
        conn.close()


def test_novelty_status_defaults_fail_closed_and_rejects_out_of_vocab():
    """The COLUMN + its D-10a index land here; the VALUES are 136-12's job.
    Until then every row must read `not_checked` -- never "novel by default"."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="novelty-default-fixture")
        statuses = {r[0] for r in conn.execute(
            "SELECT DISTINCT novelty_status FROM discovery_evidence")}
        assert statuses == {"not_checked"}
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE discovery_evidence SET novelty_status='not_in_finding_aids'"
            )
    finally:
        conn.close()


# ===========================================================================
# 136-11 Task 2: discovery_identification (the grain) + manuscript_display
# ===========================================================================

def _ident_fixture_conn(*, works_rows, evidence_specs, routing_audit=()):
    """A schema'd in-memory DB carrying `works_rows` (tuples of
    `(work_id, canonical_work_id, source_corpus)`), the assembled claims and
    evidence for `evidence_specs`, the frozen band_precision registry, and any
    routing-audit rows -- i.e. everything `populate_discovery_identification`
    reads."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
        "VALUES (?, ?, ?, NULL, NULL, ?)",
        [(w, c, f"Synthetic Title {w}", corpus) for w, c, corpus in works_rows],
    )
    corpus_by_work = {w: corpus for w, _c, corpus in works_rows}
    result = sidecar_build.assemble_claims_and_evidence(evidence_specs, corpus_by_work)
    sidecar_build._insert_claims_and_evidence_real(
        cur, result["claim_rows"], result["evidence_rows"]
    )
    cur.executemany(
        """
        INSERT INTO band_precision (
            scope, collection_id, evidence_source, confidence_band, numerator, denominator,
            precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes,
            measurement_status
        ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
                   :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
                   :ins_policy, :weighting, :notes, :measurement_status)
        """,
        [{"measurement_status": None, **r}
         for r in sidecar_build._frozen_real_band_precision_rows()],
    )
    cur.executemany(
        "INSERT INTO discovery_routing_audit (page_id, kept_work_id, demoted_work_id, "
        "kept_year, demoted_year, delta_years, decision, routing_reason) "
        "VALUES (?, NULL, NULL, NULL, NULL, NULL, ?, NULL)",
        list(routing_audit),
    )
    conn.commit()
    return conn


def _direct_spec(page_id, sys_id, work_id, *, span=(0, 100), coverage=0.95,
                 page_norm_letters=100, matched_letters=95,
                 band=None, adjudication=None, routing=None):
    return sidecar_build._mk_evidence(
        page_id=page_id, work_id=work_id, sys_id=sys_id,
        evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        confidence_band=band or ids.CONFIDENCE_BAND_EXPERT_VERIFIED,
        adjudication_status=adjudication or ids.ADJUDICATION_STATUS_UNREVIEWED,
        audit_status=ids.AUDIT_STATUS_AUDIT_PENDING,
        routing_status=routing or ids.ROUTING_STATUS_SHIPPED,
        routing_reason=ids.ROUTING_REASON_NONE,
        span_start=span[0], span_end=span[1],
        matched_letters=matched_letters, density=0.2, n_spans=1,
        coverage=coverage, page_norm_letters=page_norm_letters,
    )


def test_identification_grain_is_one_row_per_sys_id_x_canonical_work():
    """Two pages of the same manuscript matching the same canonical work are
    ONE identification -- and two `works` rows sharing a canonical_work_id
    collapse structurally (D-13a), so every count derived from this table is
    already deduplicated."""
    conn = _ident_fixture_conn(
        works_rows=[
            ("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA),
            # A D-13a duplicate: a second works row recording the SAME canonical id.
            ("w000002", "w000001", ids.SOURCE_CORPUS_MSOURCE),
        ],
        evidence_specs=[
            _direct_spec("p1", "s1", "w000001"),
            _direct_spec("p2", "s1", "w000001"),
            _direct_spec("p3", "s1", "w000002"),
        ],
    )
    try:
        stats = sidecar_build.populate_discovery_identification(conn)
        rows = conn.execute(
            "SELECT sys_id, canonical_work_id, page_count FROM discovery_identification"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("s1", "w000001", 3)]
    assert stats["identifications"] == 1
    assert stats["duplicate_canonical_groups"] == 1


def test_identification_row_count_equals_distinct_pairs_and_records_shipped_only():
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        stats = sidecar_build.populate_synthetic(conn, source_db_hash="grain-fixture")
        ident = stats["identification"]
        (pairs,) = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT de.sys_id, w.canonical_work_id
                FROM discovery_evidence de
                JOIN discovery_claim dc ON dc.claim_id = de.claim_id
                JOIN works w            ON w.work_id  = dc.work_id
                WHERE de.routing_status = 'shipped'
                   OR de.adjudication_status = 'human_confirmed'
            )
            """
        ).fetchone()
        (rows,) = conn.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
    finally:
        conn.close()
    assert rows == pairs == ident["identifications"]
    # The shipped-only figure is recorded alongside, so the D-13g delta is
    # visible rather than absorbed.
    assert "identifications_shipped_only" in ident
    assert ident["identifications_shipped_only"] <= ident["identifications"]
    assert (ident["identifications"] - ident["identifications_shipped_only"]) >= 0


def test_grain_assertion_fires_when_the_row_count_is_tampered_with():
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[_direct_spec("p1", "s1", "w000001")],
    )
    try:
        sidecar_build.populate_discovery_identification(conn)
        sidecar_build.assert_identification_grain_consistent(conn)  # clean
        conn.execute("DELETE FROM discovery_identification")
        with pytest.raises(sidecar_build.IdentificationGrainError):
            sidecar_build.assert_identification_grain_consistent(conn)
    finally:
        conn.close()


def test_review_only_human_confirmed_identification_is_materialized_with_its_basis():
    """D-13g, second half: the service restores review-only human-confirmed rows
    to the page query. If this table held only SHIPPED identifications, an inner
    join would drop them a second time and a left join would leave them with no
    bucket and no reason -- either way the fix is undone one layer down."""
    conn = _ident_fixture_conn(
        works_rows=[
            ("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA),
            ("w000002", "w000002", ids.SOURCE_CORPUS_SEFARIA),
        ],
        evidence_specs=[
            _direct_spec("p1", "s1", "w000001"),
            # Routing DEMOTED this one, but a human confirmed it.
            _direct_spec("p2", "s1", "w000002",
                         adjudication=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
                         routing=ids.ROUTING_STATUS_REVIEW_ONLY),
        ],
    )
    try:
        stats = sidecar_build.populate_discovery_identification(conn)
        rows = dict(conn.execute(
            "SELECT canonical_work_id, eligibility_basis FROM discovery_identification"
        ).fetchall())
        buckets = dict(conn.execute(
            "SELECT canonical_work_id, main_pool_reason FROM discovery_identification"
        ).fetchall())
    finally:
        conn.close()

    assert rows == {"w000001": "shipped", "w000002": "human_confirmed"}
    # The human-confirmed override is main pool ahead of every gate.
    assert buckets["w000002"] == "main_human_confirmed"
    assert stats["identifications_restored_by_human_confirmed"] == 1
    assert stats["identifications_shipped_only"] == 1
    assert stats["identifications"] == 2


def test_display_work_id_rule_is_ordered_and_total():
    select = sidecar_build.select_display_work_id
    # 1. the canonical anchor, when it is a member of its own group
    assert select("w000005", [("w000009", "sefaria"), ("w000005", "msource")]) == "w000005"
    # 2. else lowest source_corpus in the fixed order sefaria < ja < msource
    assert select("w000005", [("w000009", "msource"), ("w000007", "sefaria")]) == "w000007"
    assert select("w000005", [("w000009", "msource"), ("w000007", "ja")]) == "w000007"
    # 3. else the lexicographically smallest work_id
    assert select("w000005", [("w000009", "ja"), ("w000007", "ja")]) == "w000007"
    with pytest.raises(sidecar_build.IdentificationGrainError):
        select("w000005", [])


def test_display_work_id_never_null_and_works_join_is_exactly_one_to_one():
    """A duplicated canonical_work_id group is the 65,587-row failure. Prove
    (a) the SS(B1) rule keeps the identity join 1:1, and (b) the assertion FIRES
    when the rule is bypassed by joining on canonical_work_id instead."""
    conn = _ident_fixture_conn(
        works_rows=[
            ("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA),
            ("w000002", "w000001", ids.SOURCE_CORPUS_MSOURCE),
            ("w000003", "w000001", ids.SOURCE_CORPUS_JA),
        ],
        evidence_specs=[_direct_spec("p1", "s1", "w000002")],
    )
    try:
        sidecar_build.populate_discovery_identification(conn)
        counts = sidecar_build.assert_identification_grain_consistent(conn)
        assert counts["identification_works_join_rows"] == counts["identifications"] == 1
        (null_display,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification WHERE display_work_id IS NULL"
        ).fetchone()
        assert null_display == 0
        # The rule picked the canonical anchor, which is itself a group member.
        (display_work_id,) = conn.execute(
            "SELECT display_work_id FROM discovery_identification"
        ).fetchone()
        assert display_work_id == "w000001"

        # BYPASS the rule -- join the grain to `works` on canonical_work_id, the
        # exact mistake SS(B1) exists to prevent -- and watch it fan out 1 -> 3.
        (fanned,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification di "
            "JOIN works w ON w.canonical_work_id = di.canonical_work_id"
        ).fetchone()
        assert fanned == 3
    finally:
        conn.close()


def test_grain_assertion_fires_when_display_work_id_does_not_resolve():
    """The SAME bypass expressed as STORED data. `canonical_work_id` is not a
    `work_id` in a group with no anchor, so writing it into `display_work_id` is
    exactly the SS(B1) mistake -- and it must be refused TWICE over: the FK
    rejects it outright, and (with FK enforcement off, as it is by default on a
    bare sqlite3 connection) assertion 2's identity-join count catches it."""
    conn = _ident_fixture_conn(
        works_rows=[
            # No member equals the canonical id -- there is no anchor.
            ("w000002", "w000001", ids.SOURCE_CORPUS_MSOURCE),
            ("w000003", "w000001", ids.SOURCE_CORPUS_JA),
        ],
        evidence_specs=[
            _direct_spec("p1", "s1", "w000002"),
            _direct_spec("p2", "s2", "w000003"),
        ],
    )
    try:
        sidecar_build.populate_discovery_identification(conn)
        assert sidecar_build.assert_identification_grain_consistent(conn)["identifications"] == 2
        # The SS(B1) rule picked the lowest source_corpus member (ja < msource).
        assert {r[0] for r in conn.execute(
            "SELECT DISTINCT display_work_id FROM discovery_identification")} == {"w000003"}

        # Layer 1: the foreign key refuses the bypass outright.
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE discovery_identification SET display_work_id = canonical_work_id")

        # Layer 2: with FK enforcement off, the assertion still catches it.
        # (`PRAGMA foreign_keys` is a no-op inside a transaction, hence the commit.)
        conn.commit()
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("UPDATE discovery_identification SET display_work_id = canonical_work_id")
        with pytest.raises(sidecar_build.IdentificationGrainError):
            sidecar_build.assert_identification_grain_consistent(conn)
    finally:
        conn.close()


def test_every_identification_carries_a_reason_from_the_closed_vocabulary():
    from shared.discovery_main_pool import MAIN_POOL_REASONS

    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="reason-fixture")
        reasons = [r[0] for r in conn.execute(
            "SELECT main_pool_reason FROM discovery_identification")]
        (nulls,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification WHERE main_pool_reason IS NULL"
        ).fetchone()
    finally:
        conn.close()
    assert reasons and nulls == 0
    assert set(reasons) <= MAIN_POOL_REASONS


def test_builder_has_no_local_main_pool_implementation():
    """T-136-11-01: the bucket rule has ONE implementation. A second one in the
    builder is exactly the drift that made sketch 003's `confOf()` disagree with
    the predicate the codebase already had."""
    source = Path(sidecar_build.__file__).read_text(encoding="utf-8")
    assert len(re.findall(r"^\s*def main_pool", source, re.MULTILINE)) == 0
    assert "from shared.discovery_main_pool import" in source
    assert "main_pool_decision(" in source


def test_main_pool_bucket_routes_through_the_shared_rule(monkeypatch):
    """Prove the builder CALLS the shared rule rather than reproducing its
    outcome: stub the shared predicate and watch the stored bucket follow it."""
    calls = []

    def _fake_decision(identification):
        calls.append(identification)
        return False, "overlapping_tie"

    monkeypatch.setattr(sidecar_build, "main_pool_decision", _fake_decision)
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[_direct_spec("p1", "s1", "w000001")],
    )
    try:
        sidecar_build.populate_discovery_identification(conn)
        row = conn.execute(
            "SELECT main_pool, main_pool_reason FROM discovery_identification"
        ).fetchone()
    finally:
        conn.close()
    assert calls, "the builder never called shared.discovery_main_pool"
    assert row == (0, "overlapping_tie")


def test_identification_columns_and_defaults_left_for_136_12():
    """136-11 writes the STRUCTURE; 136-12 computes novelty + the visibility
    axes. Until then both must fail CLOSED -- novelty `not_checked` (never
    "novel by default") and BOTH visibility axes `private` (public eligibility
    requires both to be public, D-22)."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="defaults-fixture")
        rows = conn.execute(
            "SELECT DISTINCT novelty_status, divergence_correctness, "
            "assertion_visibility, identity_visibility FROM discovery_identification"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("not_checked", None, "private", "private")]


def test_identification_id_is_deterministic_across_two_builds():
    def _ids():
        conn = sqlite3.connect(":memory:")
        sidecar_build.create_schema(conn)
        try:
            sidecar_build.populate_synthetic(conn, source_db_hash="determinism-fixture")
            return [r[0] for r in conn.execute(
                "SELECT identification_id FROM discovery_identification "
                "ORDER BY sys_id, canonical_work_id")]
        finally:
            conn.close()

    first, second = _ids(), _ids()
    assert first and first == second
    assert len(set(first)) == len(first)


def test_identification_id_recipe_matches_the_public_projection():
    """The recipe is frozen in the schema doc and (until a later plan
    centralizes it into scripts/discovery_ids.py) implemented in TWO places.
    Pin them to each other so a drift is a red suite, not a silent divergence."""
    from scripts import project_discovery_public as projection

    class _Ctx:
        canonical_groups = {"w000001": [{"work_id": "w000001", "source_corpus": "sefaria"}]}
        public_work_ids = {"w000001"}
        claims_by_id = {"c1": {"claim_type": "direct_witness", "work_id": "w000001"}}

    projected = projection._recompute_identification_row(
        "s1", "w000001",
        [{"evidence_id": "e1", "claim_id": "c1", "a_page_id": "p1",
          "band_rank": 1, "coverage_ppm": 900000,
          "evidence_source": "track1_direct", "confidence_band": "expert_verified",
          "adjudication_status": "unreviewed", "routing_status": "shipped"}],
        _Ctx(),
    )
    assert projected["identification_id"] == sidecar_build.identification_id("s1", "w000001")


# --- manuscript_display ----------------------------------------------------

def _write_libraries_csv(path, rows):
    """`rows`: (system_number, oxford_part_id, call_numbers, library_code)."""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["system_number", "oxford_part_id", "call_numbers",
                          "library_code", "c4", "c5", "c6", "titles_non_placeholder"])
        for sys_id, part, calls, lib in rows:
            writer.writerow([sys_id, part, calls, lib, "", "", "", "SYNTHETIC TITLE MUST NOT LEAK"])


def test_manuscript_display_carries_only_libraries_csv_catalogue_fields(tmp_path):
    """T-136-11-03: no work title, no reference text, no locus -- the column set
    is exactly the five catalogue fields the schema authorizes, and the title
    column of libraries.csv is never read."""
    csv_path = tmp_path / "libraries.csv"
    _write_libraries_csv(csv_path, [
        ("990000000000000001", "", "T-S 12.123|T-S 12.123 (a longer variant)", "CUL"),
        ("990000000000000002", "", "MS Heb c 57", "Oxford"),
        ("990000000000000003", "", "UNRELATED 1", "JTS"),
    ])
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[
            _direct_spec("p1", "990000000000000001", "w000001"),
            _direct_spec("p2", "990000000000000002", "w000001"),
        ],
    )
    try:
        stats = sidecar_build.populate_manuscript_display(conn, str(csv_path))
        columns = [r[1] for r in conn.execute("PRAGMA table_info('manuscript_display')")]
        rows = conn.execute(
            "SELECT sys_id, library_code, shelfmark_display FROM manuscript_display "
            "ORDER BY sys_id"
        ).fetchall()
    finally:
        conn.close()

    assert columns == ["sys_id", "library_code", "library_sort_key",
                       "shelfmark_display", "shelfmark_sort_key"]
    forbidden = {"title", "neutral_title", "text", "reference_text", "locus",
                 "span_start", "span_end", "work_id"}
    assert not (set(columns) & forbidden)
    # Only manuscripts carrying an eligible claim -- never the whole catalogue.
    assert rows == [
        ("990000000000000001", "CUL", "T-S 12.123"),      # SHORTEST variant wins
        ("990000000000000002", "Oxford", "MS Heb c 57"),
    ]
    assert stats["manuscript_display"] == 2
    assert stats["manuscript_display_missing_from_libraries_csv"] == 0


def test_manuscript_display_reports_manuscripts_absent_from_libraries_csv(tmp_path):
    csv_path = tmp_path / "libraries.csv"
    _write_libraries_csv(csv_path, [("990000000000000001", "", "T-S 1.1", "CUL")])
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[
            _direct_spec("p1", "990000000000000001", "w000001"),
            _direct_spec("p2", "990000000000000099", "w000001"),
        ],
    )
    try:
        stats = sidecar_build.populate_manuscript_display(conn, str(csv_path))
    finally:
        conn.close()
    assert stats["manuscript_display"] == 1
    assert stats["manuscript_display_sys_ids_with_claims"] == 2
    # The gap is REPORTED, never silently absorbed -- and never back-filled from
    # some other source, because libraries.csv is the only sanctioned one.
    assert stats["manuscript_display_missing_from_libraries_csv"] == 1


def test_manuscript_display_is_empty_without_a_libraries_csv():
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[_direct_spec("p1", "s1", "w000001")],
    )
    try:
        stats = sidecar_build.populate_manuscript_display(conn, None)
        (n,) = conn.execute("SELECT COUNT(*) FROM manuscript_display").fetchone()
    finally:
        conn.close()
    assert n == 0 and stats["manuscript_display"] == 0


def test_shelfmark_sort_key_orders_numerically_not_lexically():
    """Raw lexical order puts "T-S 12.123" before "T-S 12.9". The stored sort
    key must not."""
    key = sidecar_build.normalize_sort_key
    assert "T-S 12.123" < "T-S 12.9"                    # the bug, in plain text
    assert key("T-S 12.9") < key("T-S 12.123")          # ...fixed by the key
    assert key("t-s  12.9") == key("T-S 12.9")          # case + whitespace folded
    assert key(None) == "" and key("") == ""


def test_manuscript_display_sort_index_exists_and_orders_by_library_then_shelfmark(tmp_path):
    csv_path = tmp_path / "libraries.csv"
    _write_libraries_csv(csv_path, [
        ("990000000000000001", "", "T-S 12.123", "CUL"),
        ("990000000000000002", "", "T-S 12.9", "CUL"),
        ("990000000000000003", "", "MS Heb c 57", "Oxford"),
    ])
    conn = _ident_fixture_conn(
        works_rows=[("w000001", "w000001", ids.SOURCE_CORPUS_SEFARIA)],
        evidence_specs=[
            _direct_spec(f"p{n}", f"99000000000000000{n}", "w000001")
            for n in (1, 2, 3)
        ],
    )
    try:
        sidecar_build.populate_manuscript_display(conn, str(csv_path))
        ordered = [r[0] for r in conn.execute(
            "SELECT shelfmark_display FROM manuscript_display "
            "ORDER BY library_sort_key, shelfmark_sort_key")]
        indexes = {r[1] for r in conn.execute("PRAGMA index_list('manuscript_display')")}
    finally:
        conn.close()
    assert ordered == ["T-S 12.9", "T-S 12.123", "MS Heb c 57"]
    assert "ix_manuscript_display_sort" in indexes


def test_finalize_build_materializes_both_tables_end_to_end(tmp_path):
    """The real-mode path, through `finalize_build`: both tables populated, the
    grain assertions run, and the release-contract count meta keys written."""
    fx = _build_minimal_finalize_fixture(tmp_path)
    csv_path = tmp_path / "libraries.csv"
    _write_libraries_csv(csv_path, [("s1", "", "T-S 1.1", "CUL")])

    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        libraries_csv_path=str(csv_path),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    assert stats["row_counts"]["discovery_identification"] == 1
    assert stats["row_counts"]["manuscript_display"] == 1
    assert stats["identification"]["identifications_shipped_only"] == 1

    conn = sqlite3.connect(f"file:{stats['db_path']}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        ident = conn.execute(
            "SELECT sys_id, display_work_id, main_pool_reason, page_count, "
            "relation_kind, eligibility_basis FROM discovery_identification"
        ).fetchone()
        display = conn.execute(
            "SELECT sys_id, library_code, shelfmark_display FROM manuscript_display"
        ).fetchone()
    finally:
        conn.close()

    assert meta["expected_rows_discovery_identification"] == "1"
    assert meta["expected_rows_manuscript_display"] == "1"
    assert ident[0] == "s1" and ident[1].startswith("w")
    assert ident[3] == 1 and ident[4] == ids.CLAIM_TYPE_DIRECT_WITNESS
    assert ident[5] == "shipped"
    assert display == ("s1", "CUL", "T-S 1.1")

    rc = verify_mod.verify(stats["db_path"], expected_frame_hash=stats["frame_content_hash"])
    assert rc == 0


# ===========================================================================
# 136-11 Task 3: bench_findings_page() -- the corpus-wide findings probe
# ===========================================================================

def _bench_fixture_db(tmp_path, name="bench-fixture.db"):
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    sidecar_build.create_schema(conn)
    try:
        sidecar_build.populate_synthetic(conn, source_db_hash="bench-fixture")
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_bench_findings_page_skips_cleanly_against_a_pre_rebuild_asset(tmp_path):
    """Against a PRE-rebuild asset the materialized tables do not exist. The
    probe must say WHICH shapes it skipped and why -- not crash with a bare
    exception, and not silently report zero shapes as a pass."""
    from scripts import bench_discovery

    db_path = tmp_path / "pre-rebuild.db"
    conn = sqlite3.connect(str(db_path))
    try:
        # The pre-136-11 shape: claims + evidence, no identification grain.
        conn.executescript(
            "CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT);"
            "CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY);"
        )
        conn.commit()
    finally:
        conn.close()

    readiness = bench_discovery.findings_probe_readiness(str(db_path))
    assert readiness["ready"] is False
    assert set(readiness["missing_tables"]) == {"discovery_identification", "manuscript_display"}

    result = bench_discovery.bench_findings_page(str(db_path))
    assert result["skipped"] is True
    assert result["shapes"] == [] and result["failures"] == []
    assert "discovery_identification" in result["reason"]
    assert "PRE-REBUILD" in result["reason"]
    # Every shape is named as skipped, so a reader can never mistake "not
    # measured" for "measured and fine".
    assert len(result["skipped_shapes"]) == 6
    # ...and the reporter renders it without raising.
    bench_discovery.report_findings_page(result)


def test_bench_findings_page_measures_six_named_shapes(tmp_path):
    from scripts import bench_discovery

    db_path = _bench_fixture_db(tmp_path)
    result = bench_discovery.bench_findings_page(
        str(db_path), page_size=2, repeats=3, deep_page=2
    )
    assert result["skipped"] is False

    measured = {r["label"] for r in result["shapes"]}
    skipped = {s["label"] for s in result["skipped_shapes"]}
    assert len(measured | skipped) == 6
    for expected in ("findings_default_ordering", "findings_novelty_filter",
                     "findings_relation_filter", "findings_domain_filter",
                     "findings_visible_total", "findings_deep_page_2"):
        assert expected in (measured | skipped), expected

    for r in result["shapes"]:
        assert r["rows"] > 0, f"{r['label']} recorded a timing on an EMPTY result"
        for key in ("p50_ms", "p95_ms", "max_ms"):
            assert key in r
        assert r["p50_ms"] <= r["max_ms"] and r["p95_ms"] <= r["max_ms"]

    # The visible TOTAL count carries its OWN, tighter cap -- §5 gives the count
    # and the row fetch separate budgets.
    by_label = {r["label"]: r for r in result["shapes"]}
    assert by_label["findings_visible_total"]["cap_ms"] == bench_discovery.FINDINGS_COUNT_CAP_MS
    assert by_label["findings_default_ordering"]["cap_ms"] == bench_discovery.FINDINGS_ORDERING_CAP_MS
    assert bench_discovery.FINDINGS_COUNT_CAP_MS < bench_discovery.FINDINGS_ORDERING_CAP_MS


def test_bench_findings_page_never_records_a_timing_on_an_empty_result(tmp_path, monkeypatch):
    """F14's discipline, on the new shapes: a filter value that matches nothing
    must RAISE, never be recorded as a fast query."""
    from scripts import bench_discovery

    db_path = _bench_fixture_db(tmp_path)
    monkeypatch.setattr(
        bench_discovery, "pick_findings_filters",
        lambda conn: {"novelty_status": "NO-SUCH-STATUS",
                      "relation_kind": "direct_witness", "domain": None},
    )
    with pytest.raises(AssertionError, match="ZERO rows"):
        bench_discovery.bench_findings_page(str(db_path), page_size=2, repeats=1, deep_page=2)


def test_bench_findings_page_reports_a_failing_shape_with_its_query_plan(tmp_path, monkeypatch):
    """T-136-11-06: a shape over its cap must FAIL with its SQLite query plan --
    the cap is never relaxed to make it pass."""
    from scripts import bench_discovery

    db_path = _bench_fixture_db(tmp_path)
    # An impossible cap, so every measured shape trips -- the point under test is
    # the failure PATH, not the machine this runs on.
    monkeypatch.setattr(bench_discovery, "FINDINGS_ORDERING_CAP_MS", -1.0)
    monkeypatch.setattr(bench_discovery, "FINDINGS_COUNT_CAP_MS", -1.0)
    result = bench_discovery.bench_findings_page(
        str(db_path), page_size=2, repeats=1, deep_page=2
    )
    assert result["failures"], "a shape over its cap must be reported as a failure"
    for r in result["failures"]:
        assert r["query_plan"], f"{r['label']} failed without a query plan"
    bench_discovery.report_findings_page(result)   # must not raise


def test_findings_regression_message_names_the_prior_measurement():
    """A performance assertion that says what the number USED to be is worth
    several that do not."""
    from scripts import bench_discovery

    assert "3.41-3.55 s" in bench_discovery._PRIOR_ORDERING_MEASUREMENT
    assert "1.5 s cap" in bench_discovery._PRIOR_ORDERING_MEASUREMENT
    assert "16 s" in bench_discovery._PRIOR_COUNT_MEASUREMENT
    assert "COUNT" in bench_discovery._PRIOR_COUNT_MEASUREMENT


def _fake_findings_result():
    return {
        "skipped": False, "reason": "", "missing_tables": [],
        "identifications": 64522, "page_size": 50, "deep_page": 20,
        "filters": {"novelty_status": "fills_gap", "relation_kind": "direct_witness",
                    "domain": "Synthetic Domain"},
        "shapes": [
            {"label": "findings_default_ordering", "kind": "ordering",
             "cap_ms": 1500.0, "rows": 50, "p50_ms": 41.2, "p95_ms": 58.9, "max_ms": 61.0},
            {"label": "findings_visible_total", "kind": "count",
             "cap_ms": 500.0, "rows": 1, "p50_ms": 3.1, "p95_ms": 4.4, "max_ms": 4.9},
        ],
        "skipped_shapes": [{"label": "findings_domain_filter", "reason": "works.genre is NULL"}],
        "failures": [],
    }


def test_write_budgets_records_findings_actuals_and_never_edits_a_cap():
    """`--write-budgets` must add a findings-page actuals table with p50/p95/max
    per shape, record the COUNT query separately from the ordering, and leave
    every CAP section byte-identical -- a benchmark that can silently rewrite
    the number it is measured against is not a gate (T-136-11-06)."""
    from scripts import bench_discovery

    budgets_path = (Path(bench_discovery.__file__).resolve().parent.parent
                    / "docs" / "specs" / "discovery-budgets.md")
    original = budgets_path.read_text(encoding="utf-8")

    block = bench_discovery._findings_actuals_block(_fake_findings_result())
    updated = bench_discovery._upsert_findings_block(original, block)

    assert "| Shape | Cap | p50 | p95 | max | Rows |" in updated
    assert "`findings_default_ordering`" in updated and "58.90 ms" in updated
    # The count query is recorded SEPARATELY, with its own tighter cap.
    assert "`findings_visible_total`" in updated and "p95 ≤ 500 ms" in updated
    assert "p95 ≤ 1500 ms" in updated
    # A skipped shape is NAMED with its reason, never silently dropped.
    assert "`findings_domain_filter` — works.genre is NULL" in updated

    # Every CAP section survives byte-identical.
    def _section(text, header):
        start = text.index(header)
        rest = text[start + len(header):]
        nxt = re.search(r"^## ", rest, re.MULTILINE)
        return rest[: nxt.start()] if nxt else rest

    for header in ("## 1. Initial Numeric Caps", "## 2. DATA-06 Discretion Defaults",
                   "## 5. Amendment 2026-08-02"):
        assert _section(original, header) == _section(updated, header), header
    # The recorded prod-box actuals of 2026-07-28 are human-measured and must
    # never be overwritten by a dev-box run.
    assert "### 4.2 MEASURED ACTUALS (prod-box)" in updated
    assert "**0.49 ms** ✓" in updated

    # Idempotent: a second write replaces §4.4 rather than appending a second one.
    twice = bench_discovery._upsert_findings_block(updated, block)
    assert twice.count("### 4.4 Corpus-wide findings page") == 1


def test_write_budgets_pending_block_is_honest_about_not_measuring():
    from scripts import bench_discovery

    block = bench_discovery._findings_actuals_block(
        {"skipped": True, "reason": "the tables are absent (PRE-REBUILD asset)"}
    )
    assert "PENDING" in block
    assert "the tables are absent (PRE-REBUILD asset)" in block
    # No invented numbers.
    assert "| Shape | Cap |" not in block
    assert "3.41-3.55 s" in block and "16 s" in block
