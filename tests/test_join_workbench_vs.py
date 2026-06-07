# -*- coding: utf-8 -*-
"""Phase 109 — VS adapter (JWB-12a), shelfmark fallback (review #5), grey-out logic (JWB-12g).

The TRUE D-14a parity invariant — Workbench Visual source sys_id set == get_suggestions —
is tested in tests/test_join_workbench_vs.py::test_load_visual_candidates_parity, which is
ADDED IN PLAN 02 alongside the _load_visual_candidates helper it drives (review concern #3).
This file's Plan-01 tests cover only the pure shim + the grey-out data-layer predicate.
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
