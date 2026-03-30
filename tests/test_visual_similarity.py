# -*- coding: utf-8 -*-
"""Tests for visual similarity service and import script."""

import pytest
import sqlite3
import os
import tempfile

from shared.visual_similarity_service import VisualSimilarityService


@pytest.fixture
def tmp_vs_db(tmp_path):
    """Create a temporary visual_similarity.db with known test data."""
    db_path = str(tmp_path / 'visual_similarity.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE visual_suggestions (
        alma_id_a INTEGER NOT NULL,
        alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL,
        PRIMARY KEY (alma_id_a, alma_id_b)
    )''')
    conn.execute('CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a)')
    conn.execute('''CREATE TABLE vs_metadata (key TEXT PRIMARY KEY, value TEXT)''')
    conn.execute("INSERT INTO vs_metadata VALUES ('version', '1.0.0')")
    conn.execute("INSERT INTO vs_metadata VALUES ('import_date', '2026-03-29')")
    conn.execute("INSERT INTO vs_metadata VALUES ('pair_count', '8')")
    conn.execute("INSERT INTO vs_metadata VALUES ('manuscript_count', '2')")
    # Insert 5 test pairs for alma_id_a=100
    test_pairs = [
        (100, 201, 15.5), (100, 202, 12.3), (100, 203, 10.1),
        (100, 204, 8.7), (100, 205, 5.2),
    ]
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)', test_pairs)
    # Insert pairs for alma_id_a=200
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)', [
        (200, 301, 11.0), (200, 205, 9.5), (200, 302, 7.0),
    ])
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def tmp_fist_db(tmp_path):
    """Create a minimal FIST.db fixture with the 4 required tables."""
    db_path = str(tmp_path / 'FIST.db')
    conn = sqlite3.connect(db_path)

    # Image_ImageDocument: DocumentId -> FGPImageNumberIdRecto
    conn.execute('''CREATE TABLE Image_ImageDocument (
        DocumentId INTEGER PRIMARY KEY,
        FGPImageNumberIdRecto INTEGER,
        FGPImageNumberIdVerso INTEGER
    )''')
    # dbo_ImgDigitalImage: FGPImageNumberId -> InventoryId
    conn.execute('''CREATE TABLE dbo_ImgDigitalImage (
        FGPImageNumberId INTEGER PRIMARY KEY,
        InventoryId INTEGER
    )''')
    # dbo_InventoryAlma: InventoryId -> AlmaId
    conn.execute('''CREATE TABLE dbo_InventoryAlma (
        InventoryId INTEGER PRIMARY KEY,
        AlmaId INTEGER,
        SiteId INTEGER
    )''')
    # Image_BestMarkForJoin: SVM similarity pairs
    conn.execute('''CREATE TABLE Image_BestMarkForJoin (
        BestMarkID INTEGER PRIMARY KEY,
        DocumentID_A INTEGER,
        DocumentID_B INTEGER,
        SVMMark REAL,
        MarkCode INTEGER
    )''')

    # Setup chain: Doc 1 -> FGP 10 -> Inv 100 -> Alma 1000
    #              Doc 2 -> FGP 20 -> Inv 200 -> Alma 2000
    #              Doc 3 -> FGP 30 -> Inv 300 -> Alma 3000
    #              Doc 4 -> FGP 40 -> Inv 400 -> Alma 1000 (same Alma as Doc 1 -- dedup test)
    conn.executemany('INSERT INTO Image_ImageDocument VALUES (?,?,?)', [
        (1, 10, 11), (2, 20, 21), (3, 30, 31), (4, 40, 41),
    ])
    conn.executemany('INSERT INTO dbo_ImgDigitalImage VALUES (?,?)', [
        (10, 100), (20, 200), (30, 300), (40, 100),  # FGP 40 -> Inv 100 (same as FGP 10)
    ])
    conn.executemany('INSERT INTO dbo_InventoryAlma VALUES (?,?,?)', [
        (100, 1000, 1), (200, 2000, 1), (300, 3000, 1),
    ])

    # BestMarkForJoin pairs:
    # 1. Normal pair: Doc1->Doc2, score 15.5, MarkCode=NULL
    # 2. Normal pair: Doc1->Doc3, score 10.0, MarkCode=NULL
    # 3. Dedup test: Doc4->Doc2, score 12.0, MarkCode=NULL (Doc4 maps to same Alma as Doc1)
    # 4. Self-pair: Doc1->Doc4, score 8.0, MarkCode=NULL (Alma 1000->1000, should be excluded)
    # 5. MarkCode 32318 (score=0): should be skipped
    # 6. MarkCode 33318 (score=0): should be skipped
    conn.executemany('INSERT INTO Image_BestMarkForJoin VALUES (?,?,?,?,?)', [
        (1, 1, 2, 15.5, None),    # Alma 1000 -> 2000, score 15.5
        (2, 1, 3, 10.0, None),    # Alma 1000 -> 3000, score 10.0
        (3, 4, 2, 12.0, None),    # Alma 1000 -> 2000, score 12.0 (dedup: MAX(15.5, 12.0) = 15.5)
        (4, 1, 4, 8.0, None),     # Alma 1000 -> 1000, self-pair (excluded)
        (5, 2, 3, 5.0, 32318),    # MarkCode 32318, score=0 pattern -- should be skipped
        (6, 3, 2, 3.0, 33318),    # MarkCode 33318, score=0 pattern -- should be skipped
        (7, 2, 1, 9.0, None),     # Alma 2000 -> 1000
        (8, 3, 1, 7.0, None),     # Alma 3000 -> 1000
    ])

    conn.commit()
    conn.close()
    return db_path


def test_get_suggestions_returns_ranked_list(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    results = svc.get_suggestions("100")
    assert len(results) == 5
    # Should be ordered by svm_score descending
    assert results[0]['svm_score'] == 15.5
    assert results[0]['rank'] == 1
    assert results[0]['alma_id'] == '201'
    assert results[4]['svm_score'] == 5.2
    assert results[4]['rank'] == 5
    assert results[4]['alma_id'] == '205'


def test_get_suggestions_empty(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    results = svc.get_suggestions("999999")
    assert results == []


def test_has_suggestions_true(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert svc.has_suggestions("100") is True


def test_has_suggestions_false(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert svc.has_suggestions("999999") is False


def test_get_suggestion_partners_union(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    # A=100 has partners: 201, 202, 203, 204, 205
    # A=200 has partners: 301, 205, 302
    # Union = {201, 202, 203, 204, 205, 301, 302}
    partners = svc.get_suggestion_partners(["100", "200"], mode='union')
    assert partners == {'201', '202', '203', '204', '205', '301', '302'}


def test_get_suggestion_partners_intersection(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    # A=100 partners: {201, 202, 203, 204, 205}
    # A=200 partners: {301, 205, 302}
    # Intersection = {205}
    partners = svc.get_suggestion_partners(["100", "200"], mode='intersection')
    assert partners == {'205'}


def test_get_suggestion_partners_empty_input(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    partners = svc.get_suggestion_partners([])
    assert partners == set()


def test_get_suggestion_count(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert svc.get_suggestion_count("100") == 5
    assert svc.get_suggestion_count("200") == 3
    assert svc.get_suggestion_count("999999") == 0


def test_batch_has_suggestions(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    result = svc.batch_has_suggestions(["100", "200", "999"])
    assert result == {"100": True, "200": True, "999": False}


def test_get_db_version(tmp_vs_db):
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    version = svc.get_db_version()
    assert version['version'] == '1.0.0'
    assert version['import_date'] == '2026-03-29'
    assert version['pair_count'] == '8'
    assert version['manuscript_count'] == '2'


def test_import_creates_sidecar(tmp_fist_db, tmp_path):
    from scripts.import_visual_similarity import import_visual_similarity
    output_path = str(tmp_path / 'output_vs.db')
    pair_count, ms_count = import_visual_similarity(tmp_fist_db, output_path)

    assert os.path.isfile(output_path)
    assert pair_count > 0

    # Verify we can read from the output
    conn = sqlite3.connect(output_path)
    rows = conn.execute('SELECT COUNT(*) FROM visual_suggestions').fetchone()[0]
    assert rows == pair_count
    conn.close()


def test_import_deduplicates(tmp_fist_db, tmp_path):
    """Same (alma_a, alma_b) from different DocumentIDs -> single row with MAX(score)."""
    from scripts.import_visual_similarity import import_visual_similarity
    output_path = str(tmp_path / 'dedup_vs.db')
    import_visual_similarity(tmp_fist_db, output_path)

    conn = sqlite3.connect(output_path)
    # Doc1 (Alma 1000) -> Doc2 (Alma 2000) at score 15.5
    # Doc4 (Alma 1000) -> Doc2 (Alma 2000) at score 12.0
    # Should keep MAX = 15.5
    row = conn.execute(
        'SELECT svm_score FROM visual_suggestions WHERE alma_id_a = 1000 AND alma_id_b = 2000'
    ).fetchone()
    assert row is not None
    assert row[0] == 15.5  # MAX of 15.5 and 12.0
    conn.close()


def test_import_excludes_self_pairs(tmp_fist_db, tmp_path):
    """Pairs where alma_a == alma_b are excluded."""
    from scripts.import_visual_similarity import import_visual_similarity
    output_path = str(tmp_path / 'selfpair_vs.db')
    import_visual_similarity(tmp_fist_db, output_path)

    conn = sqlite3.connect(output_path)
    # Doc1 (Alma 1000) -> Doc4 (Alma 1000) should be excluded
    row = conn.execute(
        'SELECT COUNT(*) FROM visual_suggestions WHERE alma_id_a = alma_id_b'
    ).fetchone()
    assert row[0] == 0
    conn.close()


def test_import_skips_zero_score_markcodes(tmp_fist_db, tmp_path):
    """MarkCode=32318 and 33318 rows (score=0) are skipped."""
    from scripts.import_visual_similarity import import_visual_similarity
    output_path = str(tmp_path / 'markcode_vs.db')
    import_visual_similarity(tmp_fist_db, output_path)

    conn = sqlite3.connect(output_path)
    # Pairs with MarkCode 32318/33318 should not appear
    # In our fixture: Doc2->Doc3 (Alma 2000->3000) with MarkCode=32318
    # and Doc3->Doc2 (Alma 3000->2000) with MarkCode=33318
    # These should NOT be in the output (only MarkCode IS NULL passes)
    row = conn.execute(
        'SELECT COUNT(*) FROM visual_suggestions WHERE alma_id_a = 2000 AND alma_id_b = 3000'
    ).fetchone()
    assert row[0] == 0, "MarkCode 32318 pair should be excluded"

    row2 = conn.execute(
        'SELECT COUNT(*) FROM visual_suggestions WHERE alma_id_a = 3000 AND alma_id_b = 2000'
    ).fetchone()
    assert row2[0] == 0, "MarkCode 33318 pair should be excluded"
    conn.close()
