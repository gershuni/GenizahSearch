# -*- coding: utf-8 -*-
"""Discovery v2 re-distill build LOGIC + verifier invariants (Phase 135, plan
135-06).

EVERY fixture here is FABRICATED, masking-clean test data -- never real
research content. Opaque work ids are neutral `w000xxx` strings; raw source
ids use the obviously-synthetic `raw:` prefix; titles are neutral synthetic
strings; every date is a numeric year only. Designator vocabularies for the
composition-date normalizer are FABRICATED ASCII placeholders (never the
real owner-held M-source vocabulary). CI never touches the gitignored
research tree.

This plan delivers LOGIC + its unit tests only -- there is NO production
bake here (that is 135-07).
"""
import csv
import json
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts import discovery_ids as ids
from scripts import verify_discovery_sidecar as verify_mod

# Import check_atlas_masking FLAT (same one-identity trick the build script and
# tests/test_discovery_build.py use) so exception classes share ONE identity.
_SCRIPTS_DIR = str(Path(__file__).resolve().parent.parent / "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
import check_atlas_masking as cam  # noqa: E402,F401

_REPO_ROOT = Path(__file__).resolve().parent.parent

# The REAL hash-pinned SLIM census build.json (orchestrator-verified present +
# hash-matched + masking-clean). This is the ONLY real artifact this test
# suite reads; every other input is fabricated below.
_SLIM_CENSUS_PATH = _REPO_ROOT / "discovery_data" / "v2_canonical_merges.build.json"
_SLIM_CENSUS_SHA256 = "cc054d111b9b4a76dd69912923ba50cd2b63f7820cb632617f645c12c207429a"

# The REAL delivered production composition-dates artifact (gitignored,
# masking-sensitive; orchestrator-verified present + hash-matched). It is a FLAT
# pre-normalized {raw_id: int-CE-year} map -- the production chrono pipeline
# already did the (range-aware) anchoring. Read for a smoke-parse only; its
# SHA-256 is a RUNTIME --composition-dates-sha256 pin, deliberately NOT hardcoded
# as a test gate here.
_COMPOSITION_DATES_PATH = _REPO_ROOT / "discovery_data" / "composition_dates.json"


# ---------------------------------------------------------------------------
# Shared fabricated-fixture helpers
# ---------------------------------------------------------------------------

def _write_json(path, obj):
    Path(path).write_text(json.dumps(obj), encoding="utf-8")
    return str(path)


def _merges_doc(merges, dropped=()):
    return {"merges": list(merges), "dropped_by_135": list(dropped)}


def _merge(members, canonical, verdict="approve"):
    return {"members_w": list(members), "canonical_w": canonical, "owner_verdict": verdict}


# ===========================================================================
# Task 1: hash-pinned --canonical-merges -> cross_corpus_map + drop-list
# ===========================================================================

def test_canonical_merges_builds_cross_corpus_map(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([
        _merge(["w000190", "w001382"], "w001382"),
        _merge(["w000192", "w001269"], "w001269"),
    ]))
    out = sidecar_build.load_canonical_merges(path)
    m = out["cross_corpus_map"]
    # BOTH members of a merge resolve to the ONE canonical rep.
    assert m["w000190"] == "w001382"
    assert m["w001382"] == "w001382"
    assert m["w000192"] == "w001269"
    assert out["approve_count"] == 2


def test_canonical_merges_d14_flip_canonical_is_msource_when_sefaria_dropped(tmp_path):
    # merge #7: the D-14 flip -- canonical is w000452 (M-source rep), NOT the
    # Sefaria member w001239 (which is dropped).
    path = _write_json(tmp_path / "m.json", _merges_doc(
        [_merge(["w000452", "w001239"], "w000452")],
        dropped=["w001239"],
    ))
    out = sidecar_build.load_canonical_merges(path)
    assert out["cross_corpus_map"]["w001239"] == "w000452"
    assert out["cross_corpus_map"]["w000452"] == "w000452"
    assert out["dropped"] == {"w001239"}


def test_canonical_merges_only_approve_rows_load(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([
        _merge(["w000190", "w001382"], "w001382", verdict="approve"),
        _merge(["w000451", "w001239"], "w001239", verdict="contested_drop_w001239"),
    ]))
    out = sidecar_build.load_canonical_merges(path)
    assert out["approve_count"] == 1
    assert "w000451" not in out["cross_corpus_map"]  # non-approve row skipped
    assert out["cross_corpus_map"]["w000190"] == "w001382"


def test_canonical_merges_sha256_mismatch_halts(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([_merge(["w000190", "w001382"], "w001382")]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path, sha256="deadbeef" * 8)


def test_canonical_merges_unexpected_top_level_key_rejected(tmp_path):
    doc = _merges_doc([_merge(["w000190", "w001382"], "w001382")])
    doc["totally_unknown_key"] = 1
    path = _write_json(tmp_path / "m.json", doc)
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_extra_field_in_merge_entry_rejected(tmp_path):
    bad = _merge(["w000190", "w001382"], "w001382")
    bad["extra_field"] = "x"
    path = _write_json(tmp_path / "m.json", _merges_doc([bad]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_canonical_not_a_member_rejected(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([_merge(["w000190", "w001382"], "w009999")]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_single_member_group_rejected(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([_merge(["w000190"], "w000190")]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_non_w_shaped_id_rejected(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([_merge(["w000190", "M:leak"], "w000190")]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_transitivity_guard_rejects_shared_id(tmp_path):
    path = _write_json(tmp_path / "m.json", _merges_doc([
        _merge(["w000190", "w001382"], "w001382"),
        _merge(["w000190", "w001269"], "w001269"),  # w000190 in two groups
    ]))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path)


def test_canonical_merges_duplicate_json_key_rejected(tmp_path):
    # Two "merges" keys at the top level -- strict object_pairs_hook rejects.
    raw = '{"merges": [], "merges": [], "dropped_by_135": []}'
    path = tmp_path / "m.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(str(path))


def test_canonical_merges_real_slim_file_smoke_parse():
    """Smoke-parse the REAL hash-pinned SLIM build.json: 16 approve merges,
    dropped_by_135 == {w001239}, D-14 flip resolves to w000452, and the
    verified SHA-256 matches the orchestrator-provided pin."""
    assert _SLIM_CENSUS_PATH.exists()
    out = sidecar_build.load_canonical_merges(
        str(_SLIM_CENSUS_PATH),
        sha256=_SLIM_CENSUS_SHA256,
        require_release_semantics=True,
    )
    assert out["approve_count"] == 16
    assert out["dropped"] == {"w001239"}
    assert out["sha256"] == _SLIM_CENSUS_SHA256
    # D-14: w001239 dropped, canonical rep of its group is w000452.
    assert out["cross_corpus_map"]["w001239"] == "w000452"
    assert out["cross_corpus_map"]["w000452"] == "w000452"


def test_canonical_merges_release_semantics_reject_wrong_drop_set(tmp_path):
    # Structurally valid but semantically wrong (drop set != {w001239}).
    path = _write_json(tmp_path / "m.json", _merges_doc(
        [_merge(["w000190", "w001382"], "w001382")],
        dropped=["w000190"],
    ))
    with pytest.raises(sidecar_build.CanonicalMergesError):
        sidecar_build.load_canonical_merges(path, require_release_semantics=True)


def test_insert_works_real_threads_cross_corpus_map(tmp_path):
    """The cross_corpus_map is threaded into the real-mode works insert:
    both merged members carry the SAME canonical_work_id."""
    out_db = tmp_path / "works.db"
    conn = sqlite3.connect(str(out_db))
    sidecar_build.create_schema(conn)
    cur = conn.cursor()
    works = [
        {"work_id": "w000190", "neutral_title": "Synthetic Alpha", "author": None,
         "genre": None, "source_corpus": "sefaria"},
        {"work_id": "w001382", "neutral_title": "Synthetic Beta", "author": None,
         "genre": None, "source_corpus": "sefaria"},
    ]
    cross_corpus_map = {"w000190": "w001382", "w001382": "w001382"}
    sidecar_build._insert_works_real(cur, works, cross_corpus_map=cross_corpus_map)
    conn.commit()
    rows = dict(cur.execute("SELECT work_id, canonical_work_id FROM works").fetchall())
    conn.close()
    assert rows["w000190"] == "w001382"
    assert rows["w001382"] == "w001382"


def test_finalize_build_drops_work_and_records_merge_sha(tmp_path):
    """A canonical-merges input with a drop-list excludes the dropped work
    from ALL output (zero claims/evidence) and records the merge SHA in meta."""
    fx = _build_v2_finalize_fixture(tmp_path)
    merges_path = _write_json(tmp_path / "merges.json", _merges_doc(
        merges=[], dropped=["w000002"],  # drop the second shown work
    ))
    merge_sha = sidecar_build._hash_file(Path(merges_path))
    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        canonical_merges_path=merges_path,
        canonical_merges_sha256=merge_sha,
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    conn = sqlite3.connect(str(stats["db_path"]))
    try:
        (n_dropped_works,) = conn.execute(
            "SELECT COUNT(*) FROM works WHERE work_id='w000002'").fetchone()
        (n_dropped_claims,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_claim WHERE work_id='w000002'").fetchone()
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    finally:
        conn.close()
    assert n_dropped_works == 0
    assert n_dropped_claims == 0
    assert meta.get("canonical_merges_sha256") == merge_sha


def test_build_script_threads_cross_corpus_map_and_records_sha_statically():
    """Static acceptance heuristic (135-06 Task 1): cross_corpus_map is
    threaded (>=2 occurrences), the CLI arg exists, and the merge SHA is
    provenance-recorded."""
    t = (_REPO_ROOT / "scripts" / "build_discovery_sidecar.py").read_text(encoding="utf-8")
    assert t.count("cross_corpus_map") >= 2
    assert "canonical-merges" in t
    assert "canonical_merges_sha256" in t


# ---------------------------------------------------------------------------
# Fabricated finalize_build fixture (two shown works, minimal track1_matches)
# ---------------------------------------------------------------------------

def _build_v2_finalize_fixture(tmp_path, *, neutral_titles=None):
    import csv

    research_db = tmp_path / "research.db"
    conn = sqlite3.connect(str(research_db))
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
        "INSERT INTO track1_matches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("pg1", "s1", "raw:w1", "Sefaria", None, None, None, None,
             300, 0.9, 1, "[[0, 300, 0.9]]", None),
            ("pg2", "s2", "raw:w2", "Sefaria", None, None, None, None,
             300, 0.9, 1, "[[0, 300, 0.9]]", None),
        ],
    )
    conn.executemany(
        "INSERT INTO pages (page_id, sys_id, provenance, text) VALUES (?, ?, ?, ?)",
        [("pg1", "s1", "htr", "x" * 400), ("pg2", "s2", "htr", "x" * 400)],
    )
    conn.commit()
    conn.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    _write_json(crosswalk_path, {"raw:w1": "w000001", "raw:w2": "w000002"})

    titles = neutral_titles or {"w000001": "Synthetic Neutral One", "w000002": "Synthetic Neutral Two"}
    approved_csv = tmp_path / "approved.csv"
    with open(approved_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sidecar_build.APPROVED_HEADER)
        writer.writeheader()
        for wid, title in titles.items():
            row = {h: "" for h in sidecar_build.APPROVED_HEADER}
            row["work_id"] = wid
            row["owner_verdict"] = "approve"
            row["candidate_title"] = title
            row["source_label"] = ids.SOURCE_CORPUS_SEFARIA
            writer.writerow(row)

    return {
        "research_db": research_db,
        "crosswalk_path": crosswalk_path,
        "approved_csv": approved_csv,
        "out_db_path": tmp_path / "out" / "discovery-v2.db",
    }


# ===========================================================================
# Task 2: hash-pinned date inputs + coverage gate + Lever-1 + D-17 + reband
# ===========================================================================

# FABRICATED, masking-clean designator vocabularies (never the real owner-held
# M-source vocabulary) -- neutral ASCII placeholders.
_CENTURY_DESIGNATORS = ["cent"]
_RANGE_DESIGNATORS = ["between"]
_ERA_QUALIFIERS = ["approx"]


def _witness_spec(page, work, sys, *, band, ml, span, routing=None, density=None,
                  adjudication=None, source=None):
    return sidecar_build._mk_evidence(
        page_id=page, work_id=work, sys_id=sys,
        evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=source or ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        confidence_band=band,
        adjudication_status=adjudication or ids.ADJUDICATION_STATUS_UNREVIEWED,
        audit_status=ids.AUDIT_STATUS_NA,
        routing_status=routing or ids.ROUTING_STATUS_SHIPPED,
        routing_reason=ids.ROUTING_REASON_NONE,
        span_start=span[0], span_end=span[1],
        matched_letters=ml, density=density,
    )


# --- composition-date normalizer (frozen 3-category contract) --------------

def _norm(value):
    return sidecar_build.normalize_composition_date(
        value, century_designators=_CENTURY_DESIGNATORS,
        range_designators=_RANGE_DESIGNATORS, era_qualifiers=_ERA_QUALIFIERS)


def test_composition_date_explicit_year():
    assert _norm("950") == 950


def test_composition_date_century_midpoint():
    # 10th century -> 100*(10-1)+50 = 950
    assert _norm("cent 10") == 950


def test_composition_date_range_midpoint():
    # between 900-1000 -> floor((900+1000)/2) = 950
    assert _norm("between 900-1000") == 950


def test_composition_date_near_miss_unparseable_extra_token_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("950 apocrypha")  # unrecognized trailing word -> unparseable


def test_composition_date_out_of_range_low_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("499")


def test_composition_date_out_of_range_high_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("1601")


def test_composition_date_ambiguous_dual_designator_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("cent between 10")


def test_composition_date_range_with_comma_separator_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("between 900,1000")  # comma is not a pinned dash separator


def test_composition_date_century_ordinal_out_of_bounds_rejected():
    with pytest.raises(sidecar_build.CompositionDatesError):
        _norm("cent 20")


def test_parse_composition_dates_file_roundtrip(tmp_path):
    path = _write_json(tmp_path / "comp.json", {
        "century_designators": _CENTURY_DESIGNATORS,
        "range_designators": _RANGE_DESIGNATORS,
        "era_qualifiers": _ERA_QUALIFIERS,
        "dates": {"raw:w1": "950", "raw:w2": "cent 11", "raw:w3": "between 1000-1100"},
    })
    out = sidecar_build.parse_composition_dates(path)
    assert out == {"raw:w1": 950, "raw:w2": 1050, "raw:w3": 1050}


def test_parse_composition_dates_missing_key_rejected(tmp_path):
    path = _write_json(tmp_path / "comp.json", {
        "century_designators": _CENTURY_DESIGNATORS,
        "range_designators": _RANGE_DESIGNATORS,
        "dates": {},  # missing era_qualifiers
    })
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_sha_mismatch_halts(tmp_path):
    path = _write_json(tmp_path / "comp.json", {
        "century_designators": _CENTURY_DESIGNATORS, "range_designators": _RANGE_DESIGNATORS,
        "era_qualifiers": [], "dates": {"raw:w1": "950"},
    })
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path, sha256="beef" * 16)


# --- composition-dates FLAT pre-normalized {raw_id: int-CE-year} form -------
# The production chrono pipeline hands over explicit anchored integer years
# (no descriptive strings enter the input). parse_composition_dates accepts
# this as a SECOND schema branch alongside the frozen designator+string form.

def test_parse_composition_dates_flat_int_form(tmp_path):
    """A non-empty {raw_id: int} map parses to the SAME {raw_id: year} shape the
    designator+string path returns (so the downstream crosswalk join is
    unchanged). Boundary years 500 and 1600 are inclusive."""
    path = _write_json(tmp_path / "comp_flat.json", {
        "M:w1": 950, "M:w2": 1186, "M:w3": 500, "M:w4": 1600,
    })
    out = sidecar_build.parse_composition_dates(path)
    assert out == {"M:w1": 950, "M:w2": 1186, "M:w3": 500, "M:w4": 1600}


def test_parse_composition_dates_flat_out_of_range_high_halts(tmp_path):
    path = _write_json(tmp_path / "c.json", {"M:w1": 950, "M:w2": 1700})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_flat_out_of_range_low_halts(tmp_path):
    path = _write_json(tmp_path / "c.json", {"M:w1": 400})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_flat_bool_value_halts(tmp_path):
    # bool is an int subclass -- a JSON true/false is NEVER a year -> HALT.
    path = _write_json(tmp_path / "c.json", {"M:w1": True})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_flat_string_value_halts(tmp_path):
    path = _write_json(tmp_path / "c.json", {"M:w1": "950"})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_neither_form_halts(tmp_path):
    # A mixed doc (some int, some non-int) is neither the 4-key designator form
    # nor an all-int flat map -> ambiguous/malformed -> HALT (never a silent skip).
    path = _write_json(tmp_path / "c.json", {"M:w1": 950, "M:w2": "text"})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_empty_object_halts(tmp_path):
    path = _write_json(tmp_path / "c.json", {})
    with pytest.raises(sidecar_build.CompositionDatesError):
        sidecar_build.parse_composition_dates(path)


def test_parse_composition_dates_real_flat_file_smoke_parse():
    """Smoke-parse the REAL delivered production artifact (a flat pre-normalized
    {raw_id: int-CE-year} map): exactly 7,277 entries, every value an int in
    [500, 1587]. No SHA arg -> no pin check (the SHA is a runtime
    --composition-dates-sha256 pin, never a test gate)."""
    assert _COMPOSITION_DATES_PATH.exists()
    out = sidecar_build.parse_composition_dates(str(_COMPOSITION_DATES_PATH))
    assert len(out) == 7277
    assert all(type(v) is int for v in out.values())
    assert min(out.values()) == 500
    assert max(out.values()) == 1587


# --- seftja dates (frozen {year:int, basis:str}) ---------------------------

def test_parse_seftja_dates_roundtrip_discards_basis(tmp_path):
    path = _write_json(tmp_path / "s.json", {
        "raw:w1": {"year": 1100, "basis": "colophon"},
        "raw:w2": {"year": 900, "basis": ""},
    })
    assert sidecar_build.parse_seftja_dates(path) == {"raw:w1": 1100, "raw:w2": 900}


def test_parse_seftja_dates_missing_year_rejected(tmp_path):
    path = _write_json(tmp_path / "s.json", {"raw:w1": {"basis": "x"}})
    with pytest.raises(sidecar_build.SeftjaDatesError):
        sidecar_build.parse_seftja_dates(path)


def test_parse_seftja_dates_non_integer_year_rejected(tmp_path):
    path = _write_json(tmp_path / "s.json", {"raw:w1": {"year": "1100", "basis": "x"}})
    with pytest.raises(sidecar_build.SeftjaDatesError):
        sidecar_build.parse_seftja_dates(path)


def test_parse_seftja_dates_extra_key_rejected(tmp_path):
    path = _write_json(tmp_path / "s.json", {"raw:w1": {"year": 1100, "basis": "x", "extra": 1}})
    with pytest.raises(sidecar_build.SeftjaDatesError):
        sidecar_build.parse_seftja_dates(path)


def test_parse_seftja_dates_sha_mismatch_halts(tmp_path):
    path = _write_json(tmp_path / "s.json", {"raw:w1": {"year": 1100, "basis": "x"}})
    with pytest.raises(sidecar_build.SeftjaDatesError):
        sidecar_build.parse_seftja_dates(path, sha256="beef" * 16)


# --- Lever-1 coverage routing ----------------------------------------------

def test_lever1_routes_low_coverage_to_review_only():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300,
                      span=(0, 300), density=0.30),  # below 0.45 -> review_only
        _witness_spec("p2", "w000002", "s2", band=ids.CONFIDENCE_BAND_TIER_A, ml=300,
                      span=(0, 300), density=0.90),  # ships
    ]
    n = sidecar_build.apply_lever1_coverage(specs)
    assert n == 1
    assert specs[0]["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY
    assert specs[0]["routing_reason"] != ids.ROUTING_REASON_LATER_SHARED_TEXT  # coverage, not D-17
    assert specs[1]["routing_status"] == ids.ROUTING_STATUS_SHIPPED


# --- D-17 chronological demotion -------------------------------------------

def test_d17_demotes_only_later_shipped_coclaimant():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900, "w000002": 1100})
    # w000001 (earlier) stays shipped; w000002 (later, delta 200) demoted.
    early = next(s for s in specs if s["work_id"] == "w000001")
    late = next(s for s in specs if s["work_id"] == "w000002")
    assert early["routing_status"] == ids.ROUTING_STATUS_SHIPPED
    assert late["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY
    assert late["routing_reason"] == ids.ROUTING_REASON_LATER_SHARED_TEXT
    demoted = [a for a in audit if a["decision"] == "demoted"]
    assert len(demoted) == 1
    assert demoted[0]["kept_work_id"] == "w000001"
    assert demoted[0]["demoted_work_id"] == "w000002"
    assert demoted[0]["kept_year"] == 900 and demoted[0]["demoted_year"] == 1100
    assert demoted[0]["delta_years"] == 200


def test_d17_merged_twin_pair_produces_no_demotion():
    # Two source copies of the SAME work (merged twin) -> ONE canonical -> never
    # a co-claim pair, never a self-demotion (Codex #4).
    ccm = {"w000190": "w001382", "w001382": "w001382"}
    specs = [
        _witness_spec("p1", "w000190", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w001382", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map=ccm, year_by_canonical={"w001382": 950})
    assert audit == []
    assert all(s["routing_status"] == ids.ROUTING_STATUS_SHIPPED for s in specs)


def test_d17_unknown_date_never_demoted_fail_safe():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900})  # w000002 undated
    assert all(s["routing_status"] == ids.ROUTING_STATUS_SHIPPED for s in specs)
    assert len(audit) == 1
    assert audit[0]["decision"] == "fail_safe_unknown_date"
    assert audit[0]["demoted_work_id"] is None


def test_d17_within_delta_tie_demotes_neither():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 950, "w000002": 990})  # delta 40 < 100
    assert all(s["routing_status"] == ids.ROUTING_STATUS_SHIPPED for s in specs)
    assert len(audit) == 1 and audit[0]["decision"] == "kept_tie"
    assert audit[0]["demoted_work_id"] is None


def test_d17_distinctive_non_overlapping_span_keeps_shipped():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(500, 800)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900, "w000002": 1200})
    # disjoint spans -> never a candidate pair -> no demotion.
    assert audit == []
    assert all(s["routing_status"] == ids.ROUTING_STATUS_SHIPPED for s in specs)


def test_d17_earliest_low_coverage_later_shipped_not_orphaned():
    """Codex #6: earliest is Lever-1 review_only (cov<0.45); the later
    claimant is shipped -> the later is NOT orphaned/promoted, and the Lever-1
    review_only row keeps its coverage routing_reason (never later_shared_text)."""
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300,
                      span=(0, 300), density=0.20),  # earliest, low coverage
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300,
                      span=(0, 300), density=0.90),  # later, ships
    ]
    sidecar_build.apply_lever1_coverage(specs)
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900, "w000002": 1100})
    early = next(s for s in specs if s["work_id"] == "w000001")
    late = next(s for s in specs if s["work_id"] == "w000002")
    assert early["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY
    assert early["routing_reason"] != ids.ROUTING_REASON_LATER_SHARED_TEXT  # coverage provenance preserved
    assert late["routing_status"] == ids.ROUTING_STATUS_SHIPPED  # never orphaned/promoted
    # earliest excluded from the shipped population -> no pair formed.
    assert audit == []


def test_d17_below_ml_floor_excluded():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=50, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=50, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900, "w000002": 1200})
    assert audit == []  # both below MIN_ML=200


def test_d17_audit_rows_are_numeric_year_only_masking_safe():
    specs = [
        _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
        _witness_spec("p1", "w000002", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300)),
    ]
    audit = sidecar_build.apply_d17_demotion(
        specs, cross_corpus_map={}, year_by_canonical={"w000001": 900, "w000002": 1100})
    for a in audit:
        for k in ("kept_year", "demoted_year", "delta_years"):
            assert a[k] is None or isinstance(a[k], int)  # numeric years only
        for k in ("kept_work_id", "demoted_work_id"):
            assert a[k] is None or (isinstance(a[k], str) and a[k].startswith("w"))


# --- coverage gate ---------------------------------------------------------

def test_coverage_gate_zero_candidate_hard_fails():
    with pytest.raises(sidecar_build.DateCoverageError):
        sidecar_build.compute_pair_coverage([])


def test_coverage_gate_halts_below_floor():
    audit = (
        [{"decision": "demoted"}]
        + [{"decision": "fail_safe_unknown_date"} for _ in range(10)]
    )  # coverage 1/11 ~ 0.09
    with pytest.raises(sidecar_build.DateCoverageError):
        sidecar_build.assert_pair_coverage_floor(audit, floor=0.99)


def test_coverage_gate_passes_above_floor():
    audit = [{"decision": "demoted"} for _ in range(100)] + [{"decision": "kept_tie"}]
    cov = sidecar_build.assert_pair_coverage_floor(audit, floor=0.99)
    assert cov == pytest.approx(1.0)


# --- CERT-01 FAIL-branch reband --------------------------------------------

def _measured_fail_spec():
    return [{
        "scope": "band", "collection_id": "e1_certification_registry_v1",
        "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        "confidence_band": ids.CONFIDENCE_BAND_TIER_A,
        "measurement_status": "measured_fail",
        "precision": 0.70, "ci_low": 0.62, "ci_high": 0.78, "numerator": 70, "denominator": 100,
    }]


def test_reband_decision_triggers_on_measured_fail():
    d = sidecar_build.resolve_reband_decision(_measured_fail_spec())
    assert d is not None
    assert d["trigger"]["ci_low"] == 0.62


def test_reband_decision_none_for_insufficient_evidence():
    spec = [{"scope": "band", "collection_id": "e1_certification_registry_v1",
             "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
             "confidence_band": ids.CONFIDENCE_BAND_TIER_A,
             "measurement_status": "insufficient_evidence"}]
    assert sidecar_build.resolve_reband_decision(spec) is None


def test_reband_decision_rejects_inconsistent_measured_fail():
    bad = _measured_fail_spec()
    bad[0]["ci_low"] = 0.90  # >= 0.85 contradicts measured_fail
    with pytest.raises(sidecar_build.InvalidPrecisionSpecError):
        sidecar_build.resolve_reband_decision(bad)


def test_reband_band_precision_invalidation_and_meta():
    frozen = sidecar_build._frozen_real_band_precision_rows()
    decision = sidecar_build.resolve_reband_decision(_measured_fail_spec())
    new_rows, meta_extra = sidecar_build.invalidate_reband_band_precision(frozen, decision)
    by_band = {(r.get("evidence_source"), r.get("confidence_band")): r for r in new_rows}
    for band in (ids.CONFIDENCE_BAND_TIER_A, ids.CONFIDENCE_BAND_SCREENING_RB):
        row = by_band[(ids.EVIDENCE_SOURCE_TRACK1_DIRECT, band)]
        assert row["measurement_status"] == "not_measured"
        assert row["precision"] is None and row["ci_low"] is None
    assert meta_extra["tier_a_reband_target"] == ids.CONFIDENCE_BAND_SCREENING_RB
    assert meta_extra["tier_a_reband_trigger_ci_low"] == "0.62"


def test_reband_is_rebuild_input_regenerates_id_and_moves_display_to_shipped_sibling():
    """Codex-R4 new-HIGH: the reband is consumed BEFORE assemble, so each
    rebanded row's evidence_id regenerates over screening_rb AND the display
    pointer moves to the surviving shipped sibling (never the demoted row)."""
    tier_a = _witness_spec("p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_TIER_A, ml=300, span=(0, 300))
    sibling = _witness_spec(
        "p1", "w000001", "s1", band=ids.CONFIDENCE_BAND_CORROBORATED, ml=None, span=(0, 300),
        source=ids.EVIDENCE_SOURCE_PROPAGATED)
    specs = [tier_a, sibling]
    n = sidecar_build.apply_reband(specs)
    assert n == 1
    assert tier_a["confidence_band"] == ids.CONFIDENCE_BAND_SCREENING_RB
    assert tier_a["routing_status"] == ids.ROUTING_STATUS_REVIEW_ONLY

    result = sidecar_build.assemble_claims_and_evidence(
        specs, {"w000001": ids.SOURCE_CORPUS_SEFARIA})
    ev_by_id = {e[0]: e for e in result["evidence_rows"]}
    # rebanded row's stored evidence_id regenerated over the NEW band.
    rebanded_id = ids.evidence_id(
        work_id="w000001", a_page_id="p1", sys_id="s1",
        evidence_kind=ids.EVIDENCE_KIND_WITNESS,
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        confidence_band=ids.CONFIDENCE_BAND_SCREENING_RB,
        span_start=0, span_end=300, other_page_id=None, seed_spans=None)
    assert rebanded_id in ev_by_id
    # display moves to the surviving shipped corroborated sibling.
    display_id = result["claim_rows"][0][4]
    assert display_id != rebanded_id
    assert ev_by_id[display_id][4] == ids.CONFIDENCE_BAND_CORROBORATED
    assert ev_by_id[display_id][7] == ids.ROUTING_STATUS_SHIPPED


# --- v2 real-mode band flip (expert_verified -> high_confidence_algorithmic) ---
# 135-06 amendment (2026-07-24): the E1 track1_direct top tier is an ALGORITHMIC
# top-score, not human approval, so a v2 build bands it
# `high_confidence_algorithmic`; a v1 build keeps the legacy `expert_verified`.

def _e1_band_inputs():
    """One shown work, one RA-confirmed E1 row (pg1) + one adjudicated-A E1 row
    (pg2). conn=None so `_ingest_tier_a` is skipped -> the ONLY evidence rows
    are the two E1 top-tier rows under test."""
    works = [
        {"raw_work_id": "raw:w1", "work_id": "w000001",
         "neutral_title": "Synthetic Neutral One", "source_corpus": ids.SOURCE_CORPUS_SEFARIA},
    ]
    page_index = {"pg1": ("htr", "h1"), "pg2": ("htr", "h2")}
    e1_ra = [{"page_id": "pg1", "sys_id": "s1", "work_id": "raw:w1",
              "o0": 0, "o1": 300, "ml": 300, "dens": 0.9, "n_spans": 1}]
    e1_adj = [{"page_id": "pg2", "sys_id": "s2", "work_id": "raw:w1",
               "o0": 0, "o1": 300, "ml": 300, "dens": 0.9, "n_spans": 1}]
    return works, page_index, e1_ra, e1_adj


def test_v2_bands_flip_e1_top_tier_to_high_confidence_algorithmic():
    works, page_index, e1_ra, e1_adj = _e1_band_inputs()
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_index,
        e1_ra_confirmed=e1_ra, e1_adjudicated_a=e1_adj, v2_bands=True)
    bands = [e[4] for e in result["evidence_rows"]]  # index 4 == confidence_band
    assert ids.CONFIDENCE_BAND_EXPERT_VERIFIED not in bands
    assert bands.count(ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC) == 2  # RA + adjudicated-A


def test_v1_bands_default_keeps_expert_verified():
    """Regression: the DEFAULT (v2_bands=False) v1 build still bands both E1
    top-tier rows `expert_verified` -- guards the v1 golden/byte-identical path."""
    works, page_index, e1_ra, e1_adj = _e1_band_inputs()
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_index,
        e1_ra_confirmed=e1_ra, e1_adjudicated_a=e1_adj)  # v2_bands defaults False
    bands = [e[4] for e in result["evidence_rows"]]
    assert ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC not in bands
    assert bands.count(ids.CONFIDENCE_BAND_EXPERT_VERIFIED) == 2


def test_v2_assembled_asset_has_no_expert_verified_literal(tmp_path):
    """Mirrors 135-07's acceptance: a v2-mode-assembled asset has the v1 band
    literal `expert_verified` grep-ABSENT and the v2 key
    `high_confidence_algorithmic` present in the shipped DB bytes. (band_precision
    is not populated on this path -- the frozen expert_verified band_precision
    default rides an owner --precision-spec at the 135-07 real bake, out of this
    evidence-band amendment's scope.)"""
    works, page_index, e1_ra, e1_adj = _e1_band_inputs()
    result = sidecar_build.build_claims_and_evidence(
        conn=None, works=works, page_index=page_index,
        e1_ra_confirmed=e1_ra, e1_adjudicated_a=e1_adj, v2_bands=True)
    db_path = tmp_path / "v2_asset.db"
    conn = sqlite3.connect(str(db_path))
    sidecar_build.create_schema(conn)
    cur = conn.cursor()
    sidecar_build._insert_works_real(cur, works)
    sidecar_build._insert_claims_and_evidence_real(
        cur, result["claim_rows"], result["evidence_rows"])
    conn.commit()
    conn.close()
    raw = db_path.read_bytes()
    assert b"expert_verified" not in raw
    assert b"high_confidence_algorithmic" in raw


def test_finalize_build_d17_end_to_end_writes_audit_and_date_shas(tmp_path):
    """End-to-end: two co-claiming works on one page, dated 200y apart, with
    the date inputs supplied -> D-17 demotes the later, writes a
    discovery_routing_audit row, and records the date-input SHAs in meta."""
    fx = _build_v2_finalize_fixture_shared_page(tmp_path)
    comp_path = _write_json(tmp_path / "comp.json", {
        "century_designators": _CENTURY_DESIGNATORS, "range_designators": _RANGE_DESIGNATORS,
        "era_qualifiers": [], "dates": {"raw:w1": "900"},
    })
    seftja_path = _write_json(tmp_path / "s.json", {"raw:w2": {"year": 1100, "basis": "x"}})
    stats = sidecar_build.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved_csv"]),
        crosswalk_path=str(fx["crosswalk_path"]),
        out_db_path=str(fx["out_db_path"]),
        composition_dates_path=comp_path,
        seftja_dates_path=seftja_path,
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    conn = sqlite3.connect(str(stats["db_path"]))
    try:
        audit = conn.execute(
            "SELECT decision, kept_work_id, demoted_work_id, kept_year, demoted_year, delta_years "
            "FROM discovery_routing_audit").fetchall()
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        late = conn.execute(
            "SELECT routing_status, routing_reason FROM discovery_evidence de "
            "JOIN discovery_claim dc ON dc.claim_id=de.claim_id WHERE dc.work_id='w000002'").fetchone()
    finally:
        conn.close()
    demoted = [a for a in audit if a[0] == "demoted"]
    assert len(demoted) == 1
    assert demoted[0][1] == "w000001" and demoted[0][2] == "w000002"
    assert demoted[0][5] == 200  # delta_years
    assert late == (ids.ROUTING_STATUS_REVIEW_ONLY, ids.ROUTING_REASON_LATER_SHARED_TEXT)
    assert "composition_dates_sha256" in meta and "seftja_dates_sha256" in meta


def _build_v2_finalize_fixture_shared_page(tmp_path):
    """Two works co-claiming the SAME page pg1 with overlapping spans."""
    import csv
    research_db = tmp_path / "research.db"
    conn = sqlite3.connect(str(research_db))
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
        "INSERT INTO track1_matches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("pg1", "s1", "raw:w1", "Sefaria", None, None, None, None,
             300, 0.9, 1, "[[0, 300, 0.9]]", None),
            ("pg1", "s1", "raw:w2", "Sefaria", None, None, None, None,
             300, 0.9, 1, "[[0, 300, 0.9]]", None),
        ],
    )
    conn.execute("INSERT INTO pages (page_id, sys_id, provenance, text) VALUES ('pg1','s1','htr',?)",
                 ("x" * 400,))
    conn.commit()
    conn.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    _write_json(crosswalk_path, {"raw:w1": "w000001", "raw:w2": "w000002"})

    approved_csv = tmp_path / "approved.csv"
    with open(approved_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sidecar_build.APPROVED_HEADER)
        writer.writeheader()
        for wid, title in (("w000001", "Synthetic One"), ("w000002", "Synthetic Two")):
            row = {h: "" for h in sidecar_build.APPROVED_HEADER}
            row["work_id"] = wid
            row["owner_verdict"] = "approve"
            row["candidate_title"] = title
            row["source_label"] = ids.SOURCE_CORPUS_SEFARIA
            writer.writerow(row)

    return {
        "research_db": research_db, "crosswalk_path": crosswalk_path,
        "approved_csv": approved_csv, "out_db_path": tmp_path / "out" / "discovery-v2.db",
    }


# ===========================================================================
# Task 3: new verifier invariants (hand-built minimal discovery.db assets)
# ===========================================================================

_W = ids.EVIDENCE_KIND_WITNESS
_ST = ids.EVIDENCE_KIND_SHARED_TEXT
_T1 = ids.EVIDENCE_SOURCE_TRACK1_DIRECT
_PROP = ids.EVIDENCE_SOURCE_PROPAGATED
_SHIP = ids.ROUTING_STATUS_SHIPPED
_REV = ids.ROUTING_STATUS_REVIEW_ONLY


def _new_db(tmp_path, name="v2.db"):
    path = tmp_path / name
    conn = sqlite3.connect(str(path))
    sidecar_build.create_schema(conn)
    return conn


def _ins_evidence(conn, claim_id, *, work_id, page_id, sys_id, kind=_W, source=_T1,
                  band=ids.CONFIDENCE_BAND_TIER_A, adjudication=ids.ADJUDICATION_STATUS_UNREVIEWED,
                  audit=ids.AUDIT_STATUS_NA, routing=_SHIP, reason=ids.ROUTING_REASON_NONE,
                  span=(0, 300), other_page_id=None, seed_spans=None, evidence_id=None):
    """Insert ONE evidence row with a GENUINE ids.evidence_id (unless an
    explicit stale evidence_id is passed, to simulate a bad asset)."""
    eid = evidence_id or ids.evidence_id(
        work_id=work_id, a_page_id=page_id, sys_id=sys_id, evidence_kind=kind,
        evidence_source=source, confidence_band=band, span_start=span[0], span_end=span[1],
        other_page_id=other_page_id, seed_spans=seed_spans)
    conn.execute(
        """INSERT INTO discovery_evidence
           (evidence_id, claim_id, evidence_kind, evidence_source, confidence_band,
            adjudication_status, audit_status, routing_status, routing_reason, is_new,
            a_page_id, sys_id, span_start, span_end, text_layer, snapshot_hash,
            seed_spans, other_page_id)
           VALUES (?,?,?,?,?,?,?,?,?,0,?,?,?,?,'htr','h', ?, ?)""",
        (eid, claim_id, kind, source, band, adjudication, audit, routing, reason,
         page_id, sys_id, span[0], span[1],
         json.dumps(seed_spans) if seed_spans else None, other_page_id),
    )
    return eid


def _ins_claim(conn, *, page_id, work_id, display_evidence_id, claim_type=None,
               source_corpus=ids.SOURCE_CORPUS_SEFARIA):
    claim_id = ids.claim_id(page_id, work_id)
    conn.execute(
        "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
        "display_evidence_id, source_corpus, sidecar_version) VALUES (?,?,?,?,?,?,?)",
        (page_id, work_id, claim_id, claim_type or ids.CLAIM_TYPE_DIRECT_WITNESS,
         display_evidence_id, source_corpus, sidecar_build.REAL_SIDECAR_VERSION),
    )
    return claim_id


def _ins_work(conn, work_id, *, canonical=None, corpus=ids.SOURCE_CORPUS_SEFARIA):
    conn.execute(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, source_corpus) VALUES (?,?,?,?)",
        (work_id, canonical or work_id, "Synthetic Neutral", corpus),
    )


def _display_for(conn, claim_id):
    rows = [
        {"evidence_id": r[0], "evidence_source": r[1], "confidence_band": r[2],
         "adjudication_status": r[3], "routing_status": r[4]}
        for r in conn.execute(
            "SELECT evidence_id, evidence_source, confidence_band, adjudication_status, "
            "routing_status FROM discovery_evidence WHERE claim_id=?", (claim_id,))
    ]
    return ids.select_display_evidence(rows)


# --- no-mixed-enum-state ---------------------------------------------------

def test_no_mixed_enum_state_flags_both_v1_and_v2_keys(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    _ins_work(conn, "w000002")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    e1 = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                       band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (e1, c1))
    c2 = _ins_claim(conn, page_id="p2", work_id="w000002", display_evidence_id="x")
    e2 = _ins_evidence(conn, c2, work_id="w000002", page_id="p2", sys_id="s2",
                       band=ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (e2, c2))
    conn.commit()
    assert verify_mod.check_no_mixed_enum_state(conn)  # non-empty -> violation
    conn.close()


def test_no_mixed_enum_state_pure_v1_asset_passes(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    e1 = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                       band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (e1, c1))
    conn.commit()
    assert verify_mod.check_no_mixed_enum_state(conn) == []
    conn.close()


# --- never-orphan-shipped --------------------------------------------------

def test_never_orphan_shipped_flags_review_only_only_witness_page(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    e1 = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                       band=ids.CONFIDENCE_BAND_SCREENING_RB, routing=_REV,
                       reason=ids.ROUTING_REASON_LATER_SHARED_TEXT)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (e1, c1))
    conn.commit()
    assert verify_mod.check_never_orphan_shipped(conn)  # shadow-orphan HARD FAIL
    conn.close()


def test_never_orphan_shipped_display_points_at_review_only_with_shipped_sibling(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    rev = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                        band=ids.CONFIDENCE_BAND_SCREENING_RB, routing=_REV,
                        reason=ids.ROUTING_REASON_LATER_SHARED_TEXT)
    _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                  band=ids.CONFIDENCE_BAND_CORROBORATED, source=_PROP, routing=_SHIP)
    # DELIBERATELY point display at the review_only row despite a shipped sibling.
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (rev, c1))
    conn.commit()
    assert verify_mod.check_never_orphan_shipped(conn)
    conn.close()


def test_never_orphan_shipped_clean_asset_passes(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    e1 = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                       band=ids.CONFIDENCE_BAND_TIER_A, routing=_SHIP)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (e1, c1))
    conn.commit()
    assert verify_mod.check_never_orphan_shipped(conn) == []
    conn.close()


# --- routing-audit replayability + unknown-date-never-demoted --------------

def test_routing_audit_replayability_flags_sub_delta_demotion(tmp_path):
    conn = _new_db(tmp_path)
    conn.execute(
        "INSERT INTO discovery_routing_audit (page_id, kept_work_id, demoted_work_id, "
        "kept_year, demoted_year, delta_years, decision, routing_reason) VALUES "
        "('p1','w000001','w000002',900,950,50,'demoted','later_shared_text')")  # delta 50 < 100
    conn.commit()
    assert verify_mod.check_routing_audit_replayability(conn)
    conn.close()


def test_routing_audit_replayability_flags_demoted_with_null_year(tmp_path):
    conn = _new_db(tmp_path)
    conn.execute(
        "INSERT INTO discovery_routing_audit (page_id, kept_work_id, demoted_work_id, "
        "kept_year, demoted_year, delta_years, decision, routing_reason) VALUES "
        "('p1','w000001','w000002',900,NULL,NULL,'demoted','later_shared_text')")
    conn.commit()
    assert verify_mod.check_routing_audit_replayability(conn)
    conn.close()


def test_routing_audit_replayability_clean_passes(tmp_path):
    conn = _new_db(tmp_path)
    conn.execute(
        "INSERT INTO discovery_routing_audit (page_id, kept_work_id, demoted_work_id, "
        "kept_year, demoted_year, delta_years, decision, routing_reason) VALUES "
        "('p1','w000001','w000002',900,1100,200,'demoted','later_shared_text')")
    conn.commit()
    assert verify_mod.check_routing_audit_replayability(conn) == []
    conn.close()


def test_unknown_date_never_demoted_flags_orphan_later_shared_text(tmp_path):
    # a later_shared_text evidence row with NO corresponding demoted audit row.
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000002")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000002", display_evidence_id="x")
    _ins_evidence(conn, c1, work_id="w000002", page_id="p1", sys_id="s1",
                  band=ids.CONFIDENCE_BAND_TIER_A, routing=_REV,
                  reason=ids.ROUTING_REASON_LATER_SHARED_TEXT)
    conn.commit()
    assert verify_mod.check_unknown_date_never_demoted(conn)
    conn.close()


# --- measurement_status <-> ci_low consistency ----------------------------

def _ins_band_precision(conn, *, band, status, precision=None, ci_low=None, ci_high=None,
                        numerator=None, denominator=None, source=_T1,
                        collection_id="e1_certification_registry_v1"):
    conn.execute(
        "INSERT INTO band_precision (scope, collection_id, evidence_source, confidence_band, "
        "numerator, denominator, precision, ci_low, ci_high, measurement_status) "
        "VALUES ('band',?,?,?,?,?,?,?,?,?)",
        (collection_id, source, band, numerator, denominator, precision, ci_low, ci_high, status),
    )


def test_measurement_status_ci_consistency_flags_pass_below_floor(tmp_path):
    conn = _new_db(tmp_path)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_TIER_A, status="measured_pass",
                        precision=0.80, ci_low=0.70, ci_high=0.90, numerator=80, denominator=100)
    conn.commit()
    assert verify_mod.check_measurement_status_ci_consistency(conn)
    conn.close()


def test_measurement_status_ci_consistency_flags_fail_above_floor(tmp_path):
    conn = _new_db(tmp_path)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_TIER_A, status="measured_fail",
                        precision=0.90, ci_low=0.88, ci_high=0.95, numerator=90, denominator=100)
    conn.commit()
    assert verify_mod.check_measurement_status_ci_consistency(conn)
    conn.close()


def test_measurement_status_ci_consistency_clean_passes(tmp_path):
    conn = _new_db(tmp_path)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_EXPERT_VERIFIED, status="measured_pass",
                        precision=0.90, ci_low=0.87, ci_high=0.95, numerator=90, denominator=100)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_TIER_A, status="not_measured")
    conn.commit()
    assert verify_mod.check_measurement_status_ci_consistency(conn) == []
    conn.close()


# --- reband-precision-invalidation (gate-13 iff) ---------------------------

def test_reband_precision_invalidation_flags_retained_precision(tmp_path):
    conn = _new_db(tmp_path)
    conn.execute("INSERT INTO meta (key, value) VALUES ('tier_a_reband_target', 'screening_rb')")
    # screening_rb precision RETAINED (measured) despite the reband -> HARD FAIL.
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_SCREENING_RB, status="measured_pass",
                        precision=0.859, ci_low=0.86, ci_high=0.90, numerator=86, denominator=100)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_TIER_A, status="not_measured")
    conn.commit()
    assert verify_mod.check_reband_precision_invalidation(conn, {"tier_a_reband_target": "screening_rb"})
    conn.close()


def test_reband_precision_invalidation_clean_when_invalidated(tmp_path):
    conn = _new_db(tmp_path)
    conn.execute("INSERT INTO meta (key, value) VALUES ('tier_a_reband_target', 'screening_rb')")
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_SCREENING_RB, status="not_measured")
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_TIER_A, status="not_measured")
    conn.commit()
    meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    assert verify_mod.check_reband_precision_invalidation(conn, meta) == []
    conn.close()


def test_reband_precision_invalidation_iff_marker_absent(tmp_path):
    # No reband marker -> check is a no-op even if screening_rb carries a number.
    conn = _new_db(tmp_path)
    _ins_band_precision(conn, band=ids.CONFIDENCE_BAND_SCREENING_RB, status="measured_pass",
                        precision=0.859, ci_low=0.86, ci_high=0.90, numerator=86, denominator=100)
    conn.commit()
    assert verify_mod.check_reband_precision_invalidation(conn, {}) == []
    conn.close()


# --- evidence_id-content-consistency ---------------------------------------

def test_evidence_id_consistency_flags_stale_id(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    # a STALE evidence_id (simulating a bare in-place confidence_band UPDATE).
    stale = "deadbeef" * 8
    _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                  band=ids.CONFIDENCE_BAND_SCREENING_RB, evidence_id=stale)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (stale, c1))
    conn.commit()
    assert verify_mod.check_evidence_id_content_consistency(conn)
    conn.close()


def test_evidence_id_consistency_flags_stale_display_pointer(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    rev = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                        band=ids.CONFIDENCE_BAND_SCREENING_RB, routing=_REV,
                        reason=ids.ROUTING_REASON_LATER_SHARED_TEXT)
    _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                  band=ids.CONFIDENCE_BAND_CORROBORATED, source=_PROP, routing=_SHIP)
    # stale display pointer at the review_only row (select would pick the shipped sibling).
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?", (rev, c1))
    conn.commit()
    assert verify_mod.check_evidence_id_content_consistency(conn)
    conn.close()


def test_evidence_id_consistency_clean_rebuilt_asset_passes(tmp_path):
    conn = _new_db(tmp_path)
    _ins_work(conn, "w000001")
    c1 = _ins_claim(conn, page_id="p1", work_id="w000001", display_evidence_id="x")
    rev = _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                        band=ids.CONFIDENCE_BAND_SCREENING_RB, routing=_REV,
                        reason=ids.ROUTING_REASON_LATER_SHARED_TEXT)
    _ins_evidence(conn, c1, work_id="w000001", page_id="p1", sys_id="s1",
                  band=ids.CONFIDENCE_BAND_CORROBORATED, source=_PROP, routing=_SHIP)
    conn.execute("UPDATE discovery_claim SET display_evidence_id=? WHERE claim_id=?",
                 (_display_for(conn, c1), c1))
    conn.commit()
    assert rev  # the review_only row exists but is NOT the display pointer
    assert verify_mod.check_evidence_id_content_consistency(conn) == []
    conn.close()


# ===========================================================================
# 135-07: hermetic release-contract MUTATION suite (the systemic false-NEGATIVE
# guard). A valid v2 fixture build verifies rc==0 through the FULL verifier
# (incl no-mixed-enum + release-strict band_precision + --require-v2); then each
# INDEPENDENT mutation must return NONZERO. A deliberately-v1 asset stays green
# on the SAME code (byte-identity guard) but FAILS under --require-v2.
#
# Every input below is FABRICATED, masking-clean (opaque w000xxx ids, `raw:`
# source ids, neutral ASCII titles, no real research content). No production
# bake is run here.
# ===========================================================================

def _finalize_asset(tmp_path, *, v2: bool):
    """Build a complete, release-strict-valid discovery.db via `finalize_build`
    over fabricated inputs. `v2=True` supplies a (fabricated, empty) hash-pinned
    `--canonical-merges` census -> the E1 top tier + band_precision top tier key
    on `high_confidence_algorithmic` and meta records band_vocab_version='v2';
    `v2=False` is the byte-identical v1 path (top tier `expert_verified`, marker
    'v1'). One shown work (w000001) with a tier_a track1 row on pg1 + one
    RA-confirmed E1 top-tier row on pg2, so the asset carries BOTH the top-tier
    band (E1) and a tier_a band."""
    research_db = tmp_path / "research.db"
    conn = sqlite3.connect(str(research_db))
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
    conn.execute(
        "INSERT INTO track1_matches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("pg1", "s1", "raw:w1", "Sefaria", None, None, None, None,
         300, 0.9, 1, "[[0, 300, 0.9]]", None),
    )
    conn.executemany(
        "INSERT INTO pages (page_id, sys_id, provenance, text) VALUES (?,?,?,?)",
        [("pg1", "s1", "htr", "x" * 400), ("pg2", "s1", "htr", "y" * 400)],
    )
    conn.commit()
    conn.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    _write_json(crosswalk_path, {"raw:w1": "w000001"})

    approved_csv = tmp_path / "approved.csv"
    with open(approved_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sidecar_build.APPROVED_HEADER)
        writer.writeheader()
        row = {h: "" for h in sidecar_build.APPROVED_HEADER}
        row["work_id"] = "w000001"
        row["owner_verdict"] = "approve"
        row["candidate_title"] = "Synthetic Neutral One"
        row["source_label"] = ids.SOURCE_CORPUS_SEFARIA
        writer.writerow(row)

    e1_ra_path = tmp_path / "e1_ra.jsonl"
    e1_ra_path.write_text(
        json.dumps({"page_id": "pg2", "sys_id": "s1", "work_id": "raw:w1",
                    "o0": 0, "o1": 300, "ml": 300, "dens": 0.9, "n_spans": 1}) + "\n",
        encoding="utf-8",
    )

    kwargs = dict(
        source_db_path=str(research_db),
        from_approved_path=str(approved_csv),
        crosswalk_path=str(crosswalk_path),
        out_db_path=str(tmp_path / "out" / "discovery.db"),
        e1_ra_confirmed_path=str(e1_ra_path),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
    )
    if v2:
        merges_path = _write_json(tmp_path / "merges.json", _merges_doc(merges=[], dropped=[]))
        kwargs["canonical_merges_path"] = merges_path
        kwargs["canonical_merges_sha256"] = sidecar_build._hash_file(Path(merges_path))
    return sidecar_build.finalize_build(**kwargs)


def _rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def test_v2_release_valid_asset_full_verify_passes_require_v2(tmp_path):
    """A valid v2 fixture build verifies rc==0 through the FULL verifier,
    including --require-v2 (band_vocab_version='v2', no-mixed-enum,
    release-strict band_precision keyed on high_confidence_algorithmic)."""
    stats = _finalize_asset(tmp_path, v2=True)
    conn = sqlite3.connect(str(stats["db_path"]))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        ev_bands = {r[0] for r in conn.execute("SELECT confidence_band FROM discovery_evidence")}
        bp_bands = {r[0] for r in conn.execute(
            "SELECT confidence_band FROM band_precision WHERE scope='band'")}
    finally:
        conn.close()
    assert meta["band_vocab_version"] == "v2"
    assert ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC in ev_bands
    assert ids.CONFIDENCE_BAND_EXPERT_VERIFIED not in ev_bands
    assert ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC in bp_bands
    assert ids.CONFIDENCE_BAND_EXPERT_VERIFIED not in bp_bands
    assert verify_mod.verify(
        str(stats["db_path"]), stats["frame_content_hash"],
        expected_band_vocabulary="v2") == 0


def test_mutation_a_remove_v2_marker_fails_require_v2(tmp_path):
    """(a) Remove the v2 marker from meta -> under --require-v2 the asset no
    longer proves v2 intent -> NONZERO."""
    stats = _finalize_asset(tmp_path, v2=True)
    corrupt = tmp_path / "mut_a.db"
    shutil.copyfile(stats["db_path"], corrupt)
    conn = _rw(corrupt)
    conn.execute("DELETE FROM meta WHERE key='band_vocab_version'")
    conn.commit()
    conn.close()
    assert verify_mod.verify(
        str(corrupt), stats["frame_content_hash"], expected_band_vocabulary="v2") != 0


def test_mutation_b_evidence_band_reverted_to_v1_fails(tmp_path):
    """(b) Change ONLY the evidence-side band key back to the v1 literal ->
    mixed-enum + stale evidence_id -> NONZERO (even under --require-v2)."""
    stats = _finalize_asset(tmp_path, v2=True)
    corrupt = tmp_path / "mut_b.db"
    shutil.copyfile(stats["db_path"], corrupt)
    conn = _rw(corrupt)
    conn.execute(
        "UPDATE discovery_evidence SET confidence_band=? WHERE confidence_band=?",
        (ids.CONFIDENCE_BAND_EXPERT_VERIFIED, ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC),
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(
        str(corrupt), stats["frame_content_hash"], expected_band_vocabulary="v2") != 0


def test_mutation_c_band_precision_band_reverted_to_v1_fails(tmp_path):
    """(c) Change ONLY the band_precision key back to the v1 literal ->
    mixed-enum + release-strict keyset mismatch -> NONZERO."""
    stats = _finalize_asset(tmp_path, v2=True)
    corrupt = tmp_path / "mut_c.db"
    shutil.copyfile(stats["db_path"], corrupt)
    conn = _rw(corrupt)
    conn.execute(
        "UPDATE band_precision SET confidence_band=? WHERE confidence_band=?",
        (ids.CONFIDENCE_BAND_EXPERT_VERIFIED, ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC),
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(
        str(corrupt), stats["frame_content_hash"], expected_band_vocabulary="v2") != 0


def test_mutation_d_pure_v1_asset_green_but_fails_require_v2(tmp_path):
    """(d) A deliberately-v1 asset uses the v1 key end to end and stays GREEN on
    the SAME verifier code (byte-identity guard) -- but FAILS under --require-v2
    (the false-green closer: a valid v1 asset is not an intended-v2 bake)."""
    stats = _finalize_asset(tmp_path, v2=False)
    conn = sqlite3.connect(str(stats["db_path"]))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        ev_bands = {r[0] for r in conn.execute("SELECT confidence_band FROM discovery_evidence")}
    finally:
        conn.close()
    assert meta["band_vocab_version"] == "v1"
    assert ids.CONFIDENCE_BAND_EXPERT_VERIFIED in ev_bands
    assert ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC not in ev_bands
    # v1 asset is internally valid (green) with NO operator-intent flag ...
    assert verify_mod.verify(str(stats["db_path"]), stats["frame_content_hash"]) == 0
    # ... but must FAIL when the operator declared a v2 bake.
    assert verify_mod.verify(
        str(stats["db_path"]), stats["frame_content_hash"],
        expected_band_vocabulary="v2") != 0


def test_v2_top_tier_competing_sibling_wins_display_selection():
    """Guards D (auditor catch #9): a v2 top-tier (high_confidence_algorithmic)
    evidence row wins `select_display_evidence` over a COMPETING lower-band
    sibling. Without the _BAND_RANK dual-key fix the v2 key falls to
    _UNRANKED_BAND and the corroborated sibling would wrongly win."""
    hca = {"evidence_id": "e_hca", "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
           "confidence_band": ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
           "adjudication_status": ids.ADJUDICATION_STATUS_UNREVIEWED,
           "routing_status": ids.ROUTING_STATUS_SHIPPED}
    corroborated_sibling = {
        "evidence_id": "e_corr", "evidence_source": ids.EVIDENCE_SOURCE_PROPAGATED,
        "confidence_band": ids.CONFIDENCE_BAND_CORROBORATED,
        "adjudication_status": ids.ADJUDICATION_STATUS_UNREVIEWED,
        "routing_status": ids.ROUTING_STATUS_SHIPPED}
    # Order the sibling FIRST so a naive min() over an unranked v2 key would pick it.
    assert ids.select_display_evidence([corroborated_sibling, hca]) == "e_hca"
    # Both top-tier keys share rank 0 (dual-key), and neither is _UNRANKED_BAND.
    assert ids._BAND_RANK_INDEX[(ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
                                 ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC)] == 0
    assert ids._BAND_RANK_INDEX[(ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
                                 ids.CONFIDENCE_BAND_EXPERT_VERIFIED)] == 0
