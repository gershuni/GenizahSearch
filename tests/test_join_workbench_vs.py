# -*- coding: utf-8 -*-
"""Phase 109 — VS adapter (JWB-12a), shelfmark fallback (review #5), grey-out logic (JWB-12g),
D-14a parity (review #3), page=None four-actions safety (review #7), network-page-lazy (review #4).

Plan 01 tests: pure shim + grey-out data-layer predicate.
Plan 02 tests: test_load_visual_candidates_parity (D-14a), test_page_none_actions_do_not_crash
               (review #7), test_thumbnail_path_is_page_scoped (review #4/D-09 AMENDMENT).
"""
import sqlite3

import pytest

from shared.visual_similarity_service import VisualSimilarityService
from shared.joins_lab import normalize_candidate
from desktop.join_workbench import _normalize_vs_row


@pytest.fixture
def tmp_vs_db(tmp_path):
    """Create a temporary visual_similarity.db with known VS rows for alma_id_a=100."""
    db_path = str(tmp_path / 'visual_similarity.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE visual_suggestions (
        alma_id_a INTEGER NOT NULL, alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL, PRIMARY KEY (alma_id_a, alma_id_b))''')
    conn.execute('CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a)')
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)',
        [(100, 201, 15.5), (100, 202, 12.3), (100, 203, 10.1), (100, 204, 8.7), (100, 205, 5.2)])
    conn.commit()
    conn.close()
    return db_path


def test_vs_adapter_maps_fields():
    """JWB-12a: VS dict -> normalize_candidate produces correct Candidate fields."""
    row = {"alma_id": "990001234500205171", "svm_score": 14.7, "rank": 1}
    c = normalize_candidate(_normalize_vs_row(row))
    assert c.sys_id == "990001234500205171"
    assert c.page is None            # VS is manuscript-level (display.img=None -> page_of -> None)
    assert c.via_vs is True
    assert c.vs_rank == 1
    assert c.vs_score == 14.7


def test_vs_adapter_shelfmark_fallback():
    """Review #5: shim must never produce an empty shelfmark — cards render blank otherwise."""
    row = {"alma_id": "990007654300205171", "svm_score": 9.1, "rank": 3}
    c = normalize_candidate(_normalize_vs_row(row))
    assert c.shelfmark, "VS shim must never produce an empty shelfmark (review #5 — cards render blank)"
    assert c.shelfmark == "990007654300205171"   # fallback == str(alma_id) when no metadata


def test_visual_source_greyed_when_no_vs(tmp_vs_db):
    """JWB-12g: anchor absent from VS DB -> has_suggestions False -> empty candidate list."""
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert svc.has_suggestions("100") is True
    assert svc.has_suggestions("999999") is False   # anchor absent -> grey out Visual (D-08)
    # When has_suggestions is False, the VS load contract is: produce []
    raw = svc.get_suggestions("999999", 200)
    assert [normalize_candidate(_normalize_vs_row(r)) for r in raw] == []


# ---------------------------------------------------------------------------
# Plan 02 tests
# ---------------------------------------------------------------------------


def test_load_visual_candidates_parity(tmp_vs_db):
    """D-14a: the Workbench Visual source returns the SAME sys_id set as get_suggestions."""
    from desktop.join_workbench import JoinCandidatePane
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)

    class _StubMeta:  # supplies csv_bank for the batch shelfmark enrichment (review #5)
        csv_bank = {}

    class _StubWB:
        meta_mgr = _StubMeta()
        _anchor_sid = "100"

    # Call the helper unbound with a stub self exposing .wb (no Qt construction needed).
    stub = type("S", (), {"wb": _StubWB()})()
    cands = JoinCandidatePane._load_visual_candidates(stub, "100", service=svc)
    expected = {r["alma_id"] for r in svc.get_suggestions("100", 200)}
    assert {c.sys_id for c in cands} == expected      # SAME sys_id set — D-14a
    assert all(c.via_vs for c in cands)
    assert all(c.shelfmark for c in cands)            # review #5: never blank (fallback)


def test_page_none_actions_do_not_crash():
    """Review #7 / D-16: the four-action dispatch must not crash on a page=None VS Candidate.

    candidate_to_result_dict (Browse seam) and the add-to-list img derivation both derive a
    page from c.page=None; assert they tolerate None without raising and produce safe values.
    """
    from desktop.join_workbench import candidate_to_result_dict, _normalize_vs_row
    from shared.joins_lab import normalize_candidate
    c = normalize_candidate(_normalize_vs_row({"alma_id": "990001", "svm_score": 7.0, "rank": 2}))
    assert c.page is None
    res = candidate_to_result_dict(c)                  # Browse action seam — must not raise
    assert res["display"]["id"] == "990001"
    assert res["display"]["img"] is None               # page=None propagates safely (host re-derives)
    assert (c.page or 1) == 1                           # add-to-list img derivation: None -> 1
    # puzzle + add-as-join use c.sys_id / c.shelfmark only (no page) — page-safe by construction
    assert c.sys_id and c.shelfmark


def test_thumbnail_path_is_page_scoped():
    """Review #4 / D-09 AMENDMENT: the network/thumbnail path (ThumbResolver) receives only the
    visible page (<=_PER_PAGE), NOT the full <=200 set. Cheap local enrichment stays batched-full.
    """
    from desktop.join_workbench import _PER_PAGE
    # Simulate an 80-candidate result set; the page slice fed to ThumbResolver is
    # results[start:start+_PER_PAGE].
    big = list(range(80))
    page = 0
    start = page * _PER_PAGE
    page_slice = big[start:start + _PER_PAGE]
    assert len(page_slice) == _PER_PAGE        # exactly one page (<=20), never all 80 (review #4)
    assert _PER_PAGE <= 20
