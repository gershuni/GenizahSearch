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
import sqlite3
import sys
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts import discovery_ids as ids
from scripts import verify_discovery_sidecar as verify_mod

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


def test_candidate_and_approved_headers_are_frozen():
    assert sidecar_build.CANDIDATE_HEADER == [
        "work_id", "candidate_neutral_title", "author", "genre",
        "source_corpus", "review_status", "review_notes",
    ]
    assert sidecar_build.APPROVED_HEADER == [
        "work_id", "neutral_title", "author", "genre", "source_corpus", "review_status",
    ]


def test_emit_review_artifact_and_load_approved_roundtrip(tmp_path):
    candidates = [
        {"raw_work_id": "raw:sef1", "work_id": "w000001", "source_corpus": ids.SOURCE_CORPUS_SEFARIA,
         "title": "Open Corpus Title", "author": "Open Author", "genre": "canon"},
        {"raw_work_id": "raw:msource-lit", "work_id": "w000002", "source_corpus": ids.SOURCE_CORPUS_MSOURCE,
         "title": "raw research title", "author": "raw author", "genre": "ספרות יפה"},
    ]
    candidate_csv = tmp_path / "candidates.csv"
    rows = sidecar_build.emit_review_artifact(candidates, candidate_csv)

    open_row = next(r for r in rows if r["work_id"] == "w000001")
    assert open_row["review_status"] == "approved"
    assert open_row["candidate_neutral_title"] == "Open Corpus Title"
    msource_row = next(r for r in rows if r["work_id"] == "w000002")
    assert msource_row["review_status"] == ""
    assert msource_row["candidate_neutral_title"] == ""
    # source provenance is masked in every row -- only the code, never a name.
    assert {r["source_corpus"] for r in rows} <= ids.SOURCE_CORPUS_CODES

    with open(candidate_csv, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == sidecar_build.CANDIDATE_HEADER

    # Simulate the owner's edit pass: approve BOTH rows on the APPROVED schema.
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        {"work_id": "w000001", "neutral_title": "Open Corpus Title", "author": "Open Author",
         "genre": "canon", "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "review_status": "approved"},
        {"work_id": "w000002", "neutral_title": "Owner Chosen Neutral Title", "author": "Owner Author",
         "genre": "literary", "source_corpus": ids.SOURCE_CORPUS_MSOURCE, "review_status": "approved"},
    ])

    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids={"w000001", "w000002"})
    by_id = {a["work_id"]: a for a in approved}
    assert set(by_id) == {"w000001", "w000002"}
    assert by_id["w000002"]["neutral_title"] == "Owner Chosen Neutral Title"
    assert by_id["w000001"]["source_corpus"] == ids.SOURCE_CORPUS_SEFARIA


def test_load_approved_works_rejection_rules(tmp_path):
    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [
        {"work_id": "w000001", "neutral_title": "Good Title", "author": "", "genre": "",
         "source_corpus": "sefaria", "review_status": "approved"},  # kept
        {"work_id": "w000002", "neutral_title": "Unapproved", "author": "", "genre": "",
         "source_corpus": "sefaria", "review_status": ""},  # rejected: not approved
        {"work_id": "w000003", "neutral_title": "", "author": "", "genre": "",
         "source_corpus": "sefaria", "review_status": "approved"},  # rejected: empty title
        {"work_id": "w000999", "neutral_title": "Not In Crosswalk", "author": "", "genre": "",
         "source_corpus": "sefaria", "review_status": "approved"},  # rejected: unknown work_id
        {"work_id": "w000004", "neutral_title": "Bad Corpus", "author": "", "genre": "",
         "source_corpus": "not-a-real-code", "review_status": "approved"},  # rejected: bad code
    ])
    valid_work_ids = {"w000001", "w000002", "w000003", "w000004"}
    approved = sidecar_build.load_approved_works(approved_csv, valid_work_ids=valid_work_ids)
    assert [a["work_id"] for a in approved] == ["w000001"]


def test_load_approved_works_header_mismatch_raises(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    with open(bad_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["work_id", "title"])
        writer.writeheader()
    with pytest.raises(ValueError):
        sidecar_build.load_approved_works(bad_csv)


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
    _write_approved_csv(approved_csv, [{
        "work_id": work_id, "neutral_title": neutral_title, "author": "",
        "genre": "", "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "review_status": "approved",
    }])

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


def test_finalize_build_artifact_scan_is_nonblocking(tmp_path):
    research_rows = [
        _mk_track1_row("p1", "s1", "raw:w1", "Sefaria", spans_json="[[0, 40, 0.9]]"),
        _mk_track1_row("p2", "s2", "raw:w2-lit", "MaskedCorpus", genre="ספרות יפה",
                        author="SECRET-RAW-TITLE-XYZ", title="unused"),
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
    # raw:w2-lit is deliberately left UNAPPROVED -- candidate-only, never shipped.

    approved_csv = tmp_path / "approved.csv"
    _write_approved_csv(approved_csv, [{
        "work_id": work_id_1, "neutral_title": "Clean Neutral Title", "author": "",
        "genre": "", "source_corpus": ids.SOURCE_CORPUS_SEFARIA, "review_status": "approved",
    }])

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
