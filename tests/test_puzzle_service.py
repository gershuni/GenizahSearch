# -*- coding: utf-8 -*-
"""Tests for PuzzleService (joins.db sidecar CRUD, concurrency, fragment index)."""

import threading
from shared.puzzle_model import PuzzleDocument, PuzzleFragment
from shared.puzzle_service import PuzzleService, get_puzzle_service, reset_puzzle_service


def _make_doc(title="Test", fragments=None):
    """Helper to create a PuzzleDocument with optional fragments."""
    return PuzzleDocument(
        title=title,
        fragments=fragments or []
    )


def _make_frag(sys_id="S1", folio_label="1r", fl_id="FL100"):
    """Helper to create a PuzzleFragment."""
    return PuzzleFragment(sys_id=sys_id, folio_label=folio_label, fl_id=fl_id)


class TestSchemaCreation:

    def test_schema_creation(self, tmp_path):
        """PuzzleService creates join_documents, join_document_fragments, and meta tables."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        assert svc.is_available()

        # Check tables exist
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "joins.db"))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()

        assert 'join_documents' in tables
        assert 'join_document_fragments' in tables
        assert 'meta' in tables

        # Check schema version
        import sqlite3 as s
        c = s.connect(str(tmp_path / "joins.db"))
        ver = c.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        c.close()
        assert ver[0] == '2'


class TestCRUD:

    def test_create_and_load_document(self, tmp_path):
        """Save a PuzzleDocument with 2 fragments, load it back, all fields match."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        frag1 = PuzzleFragment(sys_id="S1", folio_label="1r", fl_id="FL100",
                               x=10.0, y=20.0, rotation=90.0)
        frag2 = PuzzleFragment(sys_id="S2", folio_label="2v", fl_id="FL200",
                               scale=0.5, flip_h=True, bg_removal_threshold=45.0)
        doc = PuzzleDocument(
            title="My Join", notes="Test notes", join_type="physical",
            fragments=[frag1, frag2]
        )
        doc_id = svc.save_document(doc)
        assert doc_id == doc.id

        loaded = svc.load_document(doc.id)
        assert loaded is not None
        assert loaded.id == doc.id
        assert loaded.title == "My Join"
        assert loaded.notes == "Test notes"
        assert loaded.join_type == "physical"
        assert len(loaded.fragments) == 2
        assert loaded.fragments[0].sys_id == "S1"
        assert loaded.fragments[0].x == 10.0
        assert loaded.fragments[1].flip_h is True
        assert loaded.fragments[1].bg_removal_threshold == 45.0

    def test_list_documents(self, tmp_path):
        """Save 3 documents, list_documents() returns all 3 sorted by updated_at DESC."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        import time
        for i in range(3):
            doc = _make_doc(title=f"Doc {i}")
            svc.save_document(doc)
            time.sleep(0.05)  # ensure different updated_at

        docs = svc.list_documents()
        assert len(docs) == 3
        # Most recently saved should be first
        assert docs[0]['title'] == "Doc 2"
        assert docs[2]['title'] == "Doc 0"

    def test_update_document(self, tmp_path):
        """Save document, modify title, save again, load -- title is updated."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        doc = _make_doc(title="Original")
        svc.save_document(doc)

        doc.title = "Updated"
        doc.updated_at = "2099-01-01T00:00:00"
        svc.save_document(doc)

        loaded = svc.load_document(doc.id)
        assert loaded.title == "Updated"
        assert loaded.updated_at == "2099-01-01T00:00:00"

    def test_delete_document(self, tmp_path):
        """Save document, delete it, load returns None. Fragment index also deleted."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        doc = _make_doc(fragments=[_make_frag()])
        svc.save_document(doc)
        assert svc.load_document(doc.id) is not None

        result = svc.delete_document(doc.id)
        assert result is True
        assert svc.load_document(doc.id) is None

        # Fragment index entries also gone
        frags = svc.list_documents_for_fragment(fl_id="FL100")
        assert doc.id not in frags


class TestGracefulDegradation:

    def test_graceful_degradation(self, tmp_path):
        """PuzzleService with invalid path degrades gracefully."""
        svc = PuzzleService(db_path="Z:/nonexistent/path/to/joins.db")
        assert svc.is_available() is False
        assert svc.save_document(_make_doc()) is None
        assert svc.load_document("fake-id") is None
        assert svc.list_documents() == []
        assert svc.delete_document("fake-id") is False
        assert svc.list_documents_for_fragment(fl_id="FL1") == []


class TestSingleton:

    def test_singleton_pattern(self, tmp_path, monkeypatch):
        """get_puzzle_service() returns same instance; reset clears it."""
        reset_puzzle_service()
        # Monkeypatch to use tmp_path
        monkeypatch.setattr('shared.puzzle_service._find_project_root',
                           lambda: tmp_path)
        (tmp_path / "libraries.csv").touch()
        (tmp_path / "joins_data").mkdir(exist_ok=True)

        svc1 = get_puzzle_service()
        svc2 = get_puzzle_service()
        assert svc1 is svc2

        reset_puzzle_service()
        svc3 = get_puzzle_service()
        assert svc3 is not svc1

        reset_puzzle_service()  # cleanup


class TestConcurrency:

    def test_concurrent_writes(self, tmp_path):
        """3 threads save different documents simultaneously, all succeed."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"), thread_safe=True)
        docs = [_make_doc(title=f"Thread-{i}", fragments=[_make_frag(sys_id=f"S{i}")])
                for i in range(3)]
        errors = []

        def save(doc):
            try:
                svc.save_document(doc)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=save, args=(d,)) for d in docs]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Concurrent write errors: {errors}"
        for doc in docs:
            loaded = svc.load_document(doc.id)
            assert loaded is not None, f"Document {doc.id} not found after concurrent write"


class TestFragmentIndex:

    def test_fragment_index_populated(self, tmp_path):
        """Save document with 2 fragments, fragment index has 2 rows."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        doc = _make_doc(fragments=[
            _make_frag(sys_id="S1", fl_id="FL100"),
            _make_frag(sys_id="S2", fl_id="FL200"),
        ])
        svc.save_document(doc)

        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "joins.db"))
        rows = conn.execute(
            "SELECT doc_id, fl_id, sys_id FROM join_document_fragments ORDER BY fl_id"
        ).fetchall()
        conn.close()

        assert len(rows) == 2
        assert rows[0] == (doc.id, "FL100", "S1")
        assert rows[1] == (doc.id, "FL200", "S2")

    def test_list_documents_for_fragment(self, tmp_path):
        """Save 2 documents both containing fl_id FL123, lookup returns both."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        doc1 = _make_doc(title="D1", fragments=[_make_frag(fl_id="FL123")])
        doc2 = _make_doc(title="D2", fragments=[_make_frag(fl_id="FL123", sys_id="S2")])
        svc.save_document(doc1)
        svc.save_document(doc2)

        result = svc.list_documents_for_fragment(fl_id="FL123")
        assert set(result) == {doc1.id, doc2.id}

    def test_list_documents_for_fragment_by_sys_id(self, tmp_path):
        """Save document with sys_id S1, lookup by sys_id returns that doc."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        doc = _make_doc(fragments=[_make_frag(sys_id="S1")])
        svc.save_document(doc)

        result = svc.list_documents_for_fragment(sys_id="S1")
        assert doc.id in result


class TestExternalFragmentPersistence:
    """Tests for saving/loading external library fragments."""

    def test_save_load_external_fragment(self, tmp_path):
        """External fragment (image_url, no fl_id) survives save/load roundtrip."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        frag = PuzzleFragment(
            sys_id="M123", folio_label="A", fl_id="",
            shelfmark="Rylands Genizah A 123",
            image_url="https://luna.manchester.ac.uk/luna/servlet/iiif/UoMimg~1~1~12345",
            external_provider="manchester",
            page_index=0
        )
        doc = PuzzleDocument(title="External Test", fragments=[frag])
        doc_id = svc.save_document(doc)
        assert doc_id is not None

        loaded = svc.load_document(doc_id)
        assert loaded is not None
        assert len(loaded.fragments) == 1
        lf = loaded.fragments[0]
        assert lf.fl_id == ""
        assert lf.image_url == "https://luna.manchester.ac.uk/luna/servlet/iiif/UoMimg~1~1~12345"
        assert lf.external_provider == "manchester"
        assert lf.page_index == 0

    def test_save_load_mixed_nli_external(self, tmp_path):
        """Mixed NLI + external puzzle document survives save/load roundtrip."""
        svc = PuzzleService(db_path=str(tmp_path / "joins.db"))
        nli_frag = PuzzleFragment(sys_id="N1", folio_label="1r", fl_id="FL999",
                                   shelfmark="T-S 12.1")
        ext_frag = PuzzleFragment(sys_id="M1", folio_label="B", fl_id="",
                                   shelfmark="Rylands 456",
                                   image_url="https://luna.manchester.ac.uk/iiif/test",
                                   external_provider="manchester", page_index=1)
        doc = PuzzleDocument(title="Mixed", fragments=[nli_frag, ext_frag])
        doc_id = svc.save_document(doc)
        loaded = svc.load_document(doc_id)
        assert len(loaded.fragments) == 2
        assert loaded.fragments[0].fl_id == "FL999"
        assert loaded.fragments[0].image_url == ""
        assert loaded.fragments[1].fl_id == ""
        assert loaded.fragments[1].image_url == "https://luna.manchester.ac.uk/iiif/test"

    def test_load_old_doc_without_external_fields(self, tmp_path):
        """Old documents without external fields load with correct defaults."""
        import sqlite3, json
        db_path = str(tmp_path / "joins.db")
        svc = PuzzleService(db_path=db_path)

        # Insert old-format row directly into DB (no image_url/external_provider/page_index)
        old_fragments = json.dumps([
            {"sys_id": "S1", "folio_label": "1r", "fl_id": "FL100",
             "shelfmark": "T-S 12.1", "x": 0, "y": 0, "rotation": 0, "scale": 1,
             "flip_h": False, "flip_v": False, "bg_removal_threshold": 30.0,
             "crop_top": 0, "crop_bottom": 0, "crop_left": 0, "crop_right": 0,
             "processed": True}
        ])
        import uuid
        doc_id = str(uuid.uuid4())
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO join_documents (id, title, notes, join_type, fragments_json, created_at, updated_at) "
            "VALUES (?, 'Old', '', 'physical', ?, datetime('now'), datetime('now'))",
            (doc_id, old_fragments)
        )
        conn.commit()
        conn.close()

        loaded = svc.load_document(doc_id)
        assert loaded is not None
        frag = loaded.fragments[0]
        assert frag.fl_id == "FL100"
        assert frag.image_url == ''
        assert frag.external_provider == ''
        assert frag.page_index == -1
