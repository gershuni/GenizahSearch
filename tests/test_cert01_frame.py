# -*- coding: utf-8 -*-
"""Unit tests for `scripts/cert01_frame.py` (Phase 135, plan 135-09, Task 1).

Every fixture below is FABRICATED test data (small in-memory sqlite DBs +
hand-written page text) -- NEVER real research content. `work_id` values use
the real product-shaped `w000xxx` form (opaque, never restricted) since that
is the frozen product vocabulary; page/sys ids are obviously-synthetic
(`page:1`, `sys:1`, ...), never a real shelfmark-shaped identifier.
"""
import json
import sqlite3

import pytest

from scripts import cert01_frame as cf

# ---------------------------------------------------------------------------
# Fabricated fixture builders
# ---------------------------------------------------------------------------


def _make_sidecar_db(rows, works=None, unit_members=None):
    """rows: list of dicts with page_id/work_id/canonical_work_id/source_corpus/
    confidence_band/evidence_source/routing_status/adjudication_status/
    matched_letters/sys_id/a_page_id. Builds a minimal in-memory sidecar."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT,
                             source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
                                       claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
                                          evidence_kind TEXT,
                                          evidence_source TEXT, confidence_band TEXT,
                                          adjudication_status TEXT, routing_status TEXT,
                                          a_page_id TEXT, sys_id TEXT, matched_letters INTEGER);
        CREATE TABLE witness_unit_members (unit_id TEXT, sys_id TEXT, merge_basis TEXT);
        """
    )
    work_rows = works or {}
    seen_works = {}
    for r in rows:
        seen_works.setdefault(r["work_id"], {
            "canonical_work_id": r.get("canonical_work_id", r["work_id"]),
            "source_corpus": r["source_corpus"],
        })
    seen_works.update(work_rows)
    for wid, w in seen_works.items():
        conn.execute("INSERT INTO works VALUES (?,?,?)",
                    (wid, w["canonical_work_id"], w["source_corpus"]))
    for i, r in enumerate(rows):
        claim_id = f"claim{i}"
        evidence_id = r.get("evidence_id", f"ev{i}")
        conn.execute("INSERT INTO discovery_claim VALUES (?,?,?,?,?)",
                    (r["page_id"], r["work_id"], claim_id, "direct_witness", evidence_id))
        conn.execute(
            "INSERT INTO discovery_evidence VALUES (?,?,?,?,?,?,?,?,?,?)",
            (evidence_id, claim_id, "witness", r["evidence_source"], r["confidence_band"],
             r.get("adjudication_status", "unreviewed"), r["routing_status"],
             r.get("a_page_id", r["page_id"]), r["sys_id"], r.get("matched_letters")),
        )
    for unit_id, sys_id in (unit_members or []):
        conn.execute("INSERT INTO witness_unit_members VALUES (?,?,?)",
                    (unit_id, sys_id, "oxford_part"))
    conn.commit()
    return conn


def _make_research_db(pages):
    """pages: {page_id: text}."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE pages (page_id TEXT PRIMARY KEY, text TEXT)")
    for pid, text in pages.items():
        conn.execute("INSERT INTO pages VALUES (?,?)", (pid, text))
    conn.commit()
    return conn


def _write_conn_to_tempfile(conn, path):
    dest = sqlite3.connect(str(path))
    conn.backup(dest)
    dest.close()


# ---------------------------------------------------------------------------
# Hash primitives
# ---------------------------------------------------------------------------


def test_population_hash_order_invariant():
    rows_a = [
        {"page_id": "p2", "canonical_work_id": "w2", "stratum": "sefaria:high"},
        {"page_id": "p1", "canonical_work_id": "w1", "stratum": "ja:medium"},
    ]
    rows_b = list(reversed(rows_a))
    assert cf.population_hash(rows_a) == cf.population_hash(rows_b)


def test_population_hash_changes_on_stratum_change():
    rows = [{"page_id": "p1", "canonical_work_id": "w1", "stratum": "sefaria:high"}]
    h1 = cf.population_hash(rows)
    rows[0]["stratum"] = "sefaria:medium"
    h2 = cf.population_hash(rows)
    assert h1 != h2


def test_cluster_map_hash_order_invariant_and_sensitive_to_unit_key():
    rows_a = [
        {"page_id": "p1", "canonical_work_id": "w1", "unit_key": "unitA"},
        {"page_id": "p2", "canonical_work_id": "w2", "unit_key": "sys:990001"},
    ]
    rows_b = [dict(r) for r in reversed(rows_a)]  # deep-enough copy: independent dicts
    assert cf.cluster_map_hash(rows_a) == cf.cluster_map_hash(rows_b)
    rows_a[0]["unit_key"] = "unitB"
    assert cf.cluster_map_hash(rows_a) != cf.cluster_map_hash(rows_b)


def test_stratum_counts_and_cluster_sizes():
    rows = [
        {"stratum": "sefaria:high", "unit_key": "u1"},
        {"stratum": "sefaria:high", "unit_key": "u1"},
        {"stratum": "ja:medium", "unit_key": "u2"},
    ]
    assert cf.stratum_counts(rows) == {"ja:medium": 1, "sefaria:high": 2}
    assert cf.cluster_sizes(rows) == [2, 1]


# ---------------------------------------------------------------------------
# report_id construction (protocol §5.2) -- self-referential, finite
# ---------------------------------------------------------------------------


def test_report_id_omits_its_own_field_and_is_stable():
    payload = {"a": 1, "b": [1, 2, 3], "seed": {"x": 7}}
    rid1 = cf.compute_report_id(payload)
    payload_with_stale_id = dict(payload, report_id="stale-value-should-be-ignored")
    rid2 = cf.compute_report_id(payload_with_stale_id)
    assert rid1 == rid2, "report_id must be computed over the payload MINUS its own field"


def test_report_id_changes_when_payload_changes():
    payload = {"a": 1}
    rid1 = cf.compute_report_id(payload)
    payload["a"] = 2
    rid2 = cf.compute_report_id(payload)
    assert rid1 != rid2


def test_canonical_json_minus_report_id_excludes_key():
    payload = {"z": 1, "a": 2, "report_id": "should-not-appear"}
    ser = cf.canonical_json_minus_report_id(payload)
    assert "report_id" not in ser
    assert "should-not-appear" not in ser
    # sorted keys, compact separators
    assert ser == '{"a":2,"z":1}'


# ---------------------------------------------------------------------------
# Stratum tie-break (protocol §1.4) + unit_key (§1.3)
# ---------------------------------------------------------------------------


def test_resolve_stratum_corpus_map_single_corpus():
    rows = [
        {"page_id": "p1", "work_id": "w000001", "source_corpus": "msource",
         "sys_id": "s1", "confidence_band": "tier_a", "evidence_source": "track1_direct",
         "routing_status": "shipped", "matched_letters": 500},
    ]
    conn = _make_sidecar_db(rows)
    m = cf.resolve_stratum_corpus_map(conn)
    assert m == {"w000001": "msource"}


def test_resolve_stratum_corpus_map_tie_break_prefers_sefaria_over_msource():
    # Two raw claims under the SAME canonical_work_id (a merge group), one
    # from sefaria and one from msource -- the tie-break must pick sefaria
    # (lower corpus_rank) as the canonical work's stratum corpus.
    rows = [
        {"page_id": "pA", "work_id": "w_sef", "canonical_work_id": "w_sef",
         "source_corpus": "sefaria", "sys_id": "sA", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 500},
        {"page_id": "pB", "work_id": "w_msrc", "canonical_work_id": "w_sef",
         "source_corpus": "msource", "sys_id": "sB", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 500},
    ]
    conn = _make_sidecar_db(rows)
    m = cf.resolve_stratum_corpus_map(conn)
    assert m["w_sef"] == "sefaria"


def test_unit_key_of_uses_unit_map_or_falls_back_to_sys_prefix():
    unit_map = {"sys1": "unitABC"}
    assert cf.unit_key_of("sys1", unit_map) == "unitABC"
    assert cf.unit_key_of("sys2", unit_map) == "sys:sys2"


# ---------------------------------------------------------------------------
# compute_estimand_rows -- the full ranked/dedup SQL against fabricated DBs
# ---------------------------------------------------------------------------


def test_compute_estimand_rows_filters_to_shipped_tier_a_only(tmp_path):
    rows = [
        # shipped tier_a -- IN the estimand
        {"page_id": "p1", "work_id": "w000001", "source_corpus": "sefaria",
         "sys_id": "sys1", "a_page_id": "p1", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 40},
        # review_only tier_a -- excluded (not shipped)
        {"page_id": "p2", "work_id": "w000002", "source_corpus": "sefaria",
         "sys_id": "sys2", "a_page_id": "p2", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "review_only",
         "matched_letters": 5},
        # shipped screening_rb -- excluded (not tier_a)
        {"page_id": "p3", "work_id": "w000003", "source_corpus": "ja",
         "sys_id": "sys3", "a_page_id": "p3", "confidence_band": "screening_rb",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 40},
    ]
    conn = _make_sidecar_db(rows)
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)

    # Hebrew base-letter page text: 100 alef characters -> page_norm_letters=100.
    research_conn = _make_research_db({"p1": "א" * 100})
    research_path = tmp_path / "research.db"
    _write_conn_to_tempfile(research_conn, research_path)

    estimand = cf.compute_estimand_rows(str(db_path), str(research_path))
    assert len(estimand) == 1
    row = estimand[0]
    assert row["page_id"] == "p1"
    assert row["canonical_work_id"] == "w000001"
    # coverage = 40/100 = 0.40 -> below the 0.60 high floor -> medium band
    assert row["coverage"] == pytest.approx(0.40)
    assert row["coverage_band"] == "medium"
    assert row["stratum"] == "sefaria:medium"


def test_compute_estimand_rows_dedups_collision_via_precedence_lattice(tmp_path):
    # Two raw claims on the SAME page, merged to the SAME canonical_work_id:
    # one propagated/not_evaluated (weakest band-rank), one track1_direct/
    # tier_a (rank 2). The ranked SQL must keep the tier_a row.
    rows = [
        {"page_id": "p1", "work_id": "w_child", "canonical_work_id": "w_parent",
         "source_corpus": "msource", "sys_id": "sys1", "a_page_id": "p1",
         "confidence_band": "tier_a", "evidence_source": "track1_direct",
         "routing_status": "shipped", "matched_letters": 90,
         "evidence_id": "ev_tier_a"},
        {"page_id": "p1", "work_id": "w_parent", "canonical_work_id": "w_parent",
         "source_corpus": "sefaria", "sys_id": "sys1", "a_page_id": "p1",
         "confidence_band": "not_evaluated", "evidence_source": "propagated",
         "routing_status": "shipped", "matched_letters": None,
         "evidence_id": "ev_shared_text"},
    ]
    conn = _make_sidecar_db(rows)
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)
    research_conn = _make_research_db({"p1": "א" * 100})
    research_path = tmp_path / "research.db"
    _write_conn_to_tempfile(research_conn, research_path)

    estimand = cf.compute_estimand_rows(str(db_path), str(research_path))
    assert len(estimand) == 1
    assert estimand[0]["display_evidence_id"] == "ev_tier_a"
    assert estimand[0]["work_id"] == "w_child"


def test_compute_estimand_rows_excludes_dropped_work_ids(tmp_path):
    rows = [
        {"page_id": "p1", "work_id": "w001239", "source_corpus": "sefaria",
         "sys_id": "sys1", "a_page_id": "p1", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 90},
    ]
    conn = _make_sidecar_db(rows)
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)
    research_conn = _make_research_db({"p1": "א" * 100})
    research_path = tmp_path / "research.db"
    _write_conn_to_tempfile(research_conn, research_path)

    estimand = cf.compute_estimand_rows(str(db_path), str(research_path))
    assert estimand == []


# ---------------------------------------------------------------------------
# allocate_stratum_cards
# ---------------------------------------------------------------------------


def test_allocate_stratum_cards_sums_to_total_and_respects_floor():
    counts = {"a": 1000, "b": 20, "c": 5000}
    alloc = cf.allocate_stratum_cards(counts, total=220, min_per_stratum=15)
    assert sum(alloc.values()) == 220
    for s in counts:
        assert alloc[s] >= min(15, counts[s])
        assert alloc[s] <= counts[s]


def test_allocate_stratum_cards_never_exceeds_available_rows():
    counts = {"tiny": 3, "big": 10000}
    alloc = cf.allocate_stratum_cards(counts, total=220, min_per_stratum=15)
    assert alloc["tiny"] == 3
    assert sum(alloc.values()) <= 3 + 10000


def test_allocate_stratum_cards_proportional_ordering():
    counts = {"small": 1000, "large": 9000}
    alloc = cf.allocate_stratum_cards(counts, total=100, min_per_stratum=5)
    assert alloc["large"] > alloc["small"]


# ---------------------------------------------------------------------------
# read_input_hashes -- recompute-and-compare fail-loud behavior
# ---------------------------------------------------------------------------


def test_read_input_hashes_raises_on_frame_content_hash_mismatch(tmp_path, monkeypatch):
    rows = [
        {"page_id": "p1", "work_id": "w000001", "source_corpus": "sefaria",
         "sys_id": "sys1", "a_page_id": "p1", "confidence_band": "tier_a",
         "evidence_source": "track1_direct", "routing_status": "shipped",
         "matched_letters": 90},
    ]
    conn = _make_sidecar_db(rows)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO meta VALUES ('frame_content_hash', 'WRONG_HASH')")
    conn.execute("INSERT INTO meta VALUES ('canonical_merges_sha256', 'x')")
    conn.execute("INSERT INTO meta VALUES ('composition_dates_sha256', 'y')")
    conn.execute("INSERT INTO meta VALUES ('seftja_dates_sha256', 'z')")
    conn.execute("INSERT INTO meta VALUES ('crosswalk_sha256', 'w')")
    conn.commit()
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"content_hash": cf.hash_file(db_path)}), encoding="utf-8")

    with pytest.raises(ValueError, match="frame_content_hash recompute mismatch"):
        cf.read_input_hashes(str(db_path), str(manifest_path))


def test_read_input_hashes_raises_on_db_content_hash_mismatch(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT,
            routing_status TEXT, a_page_id TEXT, sys_id TEXT, matched_letters INTEGER);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.commit()
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)

    import scripts.build_discovery_sidecar as sidecar_build
    fresh = sqlite3.connect(str(db_path))
    expected_frame_hash = sidecar_build.compute_frame_content_hash(fresh)
    fresh.close()
    conn.execute("INSERT INTO meta VALUES ('frame_content_hash', ?)", (expected_frame_hash,))
    conn.execute("INSERT INTO meta VALUES ('canonical_merges_sha256', 'x')")
    conn.execute("INSERT INTO meta VALUES ('composition_dates_sha256', 'y')")
    conn.execute("INSERT INTO meta VALUES ('seftja_dates_sha256', 'z')")
    conn.execute("INSERT INTO meta VALUES ('crosswalk_sha256', 'w')")
    conn.commit()
    _write_conn_to_tempfile(conn, db_path)

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"content_hash": "WRONG_DB_HASH"}), encoding="utf-8")

    with pytest.raises(ValueError, match="db_content_hash recompute mismatch"):
        cf.read_input_hashes(str(db_path), str(manifest_path))


def test_read_input_hashes_verifies_optional_paths(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT,
            routing_status TEXT, a_page_id TEXT, sys_id TEXT, matched_letters INTEGER);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.commit()
    db_path = tmp_path / "sidecar.db"
    _write_conn_to_tempfile(conn, db_path)

    import scripts.build_discovery_sidecar as sidecar_build
    fresh = sqlite3.connect(str(db_path))
    expected_frame_hash = sidecar_build.compute_frame_content_hash(fresh)
    fresh.close()

    merges_path = tmp_path / "merges.json"
    merges_path.write_text("{}", encoding="utf-8")
    real_merges_hash = cf.hash_file(merges_path)

    conn2 = sqlite3.connect(str(db_path))
    conn2.execute("INSERT INTO meta VALUES ('frame_content_hash', ?)", (expected_frame_hash,))
    conn2.execute("INSERT INTO meta VALUES ('canonical_merges_sha256', ?)", (real_merges_hash,))
    conn2.execute("INSERT INTO meta VALUES ('composition_dates_sha256', 'y')")
    conn2.execute("INSERT INTO meta VALUES ('seftja_dates_sha256', 'z')")
    conn2.execute("INSERT INTO meta VALUES ('crosswalk_sha256', 'w')")
    conn2.commit()
    conn2.close()

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"content_hash": cf.hash_file(db_path)}), encoding="utf-8")

    # correct path -> no raise
    hashes = cf.read_input_hashes(str(db_path), str(manifest_path), canonical_merges_path=str(merges_path))
    assert hashes["canonical_merges_sha256"] == real_merges_hash

    # tampered file on disk -> raises
    merges_path.write_text('{"tampered": true}', encoding="utf-8")
    with pytest.raises(ValueError, match="canonical_merges_sha256 recompute mismatch"):
        cf.read_input_hashes(str(db_path), str(manifest_path), canonical_merges_path=str(merges_path))
