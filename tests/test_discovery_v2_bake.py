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
import json
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
