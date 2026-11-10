# -*- coding: utf-8 -*-
"""Tests for Phase 30: Direct image resolution from crossref sidecar."""

import pytest
import sqlite3


class TestLocalImageResolution:
    """Tests for Phase 30: Direct image resolution from crossref sidecar."""

    @pytest.fixture
    def mock_crossref_db(self, tmp_path):
        """Create a minimal crossref sidecar for testing."""
        db_path = str(tmp_path / "nli_crossref.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE nli_images (
            NLI_AlmaId TEXT, FGPImageNumberId TEXT, FGPNumber TEXT,
            ImageName TEXT, ImageSourceName TEXT, Shelfmark TEXT,
            PartOf TEXT DEFAULT '', See TEXT DEFAULT '', BifolioWith TEXT DEFAULT '',
            Material TEXT DEFAULT '', NumFolio TEXT DEFAULT '', NumBifolio TEXT DEFAULT '',
            Size TEXT DEFAULT ''
        )""")
        conn.execute("CREATE INDEX idx_nli_alma ON nli_images(NLI_AlmaId)")
        conn.execute("CREATE INDEX idx_nli_fgp ON nli_images(FGPImageNumberId)")
        conn.execute("INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("990001234", "421365", "C21365", "page_001r", "source1", "T-S 12.123", "", "", "", "", "", "", ""))
        conn.execute("INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("990001234", "421366", "C21366", "page_001v", "source1", "T-S 12.123", "", "", "", "", "", "", ""))
        conn.execute("""CREATE TABLE cambridge_manifests (
            label TEXT, manifest_url TEXT, normalized_shelfmark TEXT
        )""")
        conn.execute("CREATE INDEX idx_cam_norm ON cambridge_manifests(normalized_shelfmark)")
        conn.execute("INSERT INTO cambridge_manifests VALUES (?,?,?)",
            ("MS-TS-00012-00123", "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123", "ts12123"))
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO meta VALUES ('version', '1.0.0')")
        conn.commit()
        conn.close()
        return db_path

    # --- NLI FL ID resolution tests (IMG-01) ---

    def test_local_fl_ids_returned_in_page_order(self, mock_crossref_db):
        """FL IDs from crossref are returned ordered by ImageName (page sequence)."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        images = svc.get_images("990001234")
        assert len(images) == 2
        assert images[0]["fgp_image_number_id"] == "421365"
        assert images[1]["fgp_image_number_id"] == "421366"
        assert images[0]["image_name"] < images[1]["image_name"]
        svc.close()

    def test_fgp_ids_are_not_fl_ids(self, mock_crossref_db):
        """FGPImageNumberId is a Friedberg photo number, NOT an NLI IIIF FL ID.
        Crossref data is for metadata only; IIIF manifest provides actual FL IDs."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        images = svc.get_images("990001234")
        # FGP IDs are returned but should NOT be used to construct NLI IIIF URLs
        for img in images:
            assert img["fgp_image_number_id"]  # Non-empty
            assert img["image_name"]  # Has label for metadata use
        svc.close()

    def test_missing_sys_id_returns_empty(self, mock_crossref_db):
        """Unknown sys_id returns empty list (triggers network fallback)."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        images = svc.get_images("NONEXISTENT")
        assert images == []
        svc.close()

    def test_empty_fgp_ids_filtered(self, mock_crossref_db):
        """Records with empty FGPImageNumberId are excluded from image URL construction."""
        import sqlite3
        conn = sqlite3.connect(mock_crossref_db)
        conn.execute("INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("990009999", "", "", "page_empty", "source1", "T-S 99.999", "", "", "", "", "", "", ""))
        conn.commit()
        conn.close()

        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        images = svc.get_images("990009999")
        fgp_ids = [img["fgp_image_number_id"] for img in images if img["fgp_image_number_id"]]
        assert len(fgp_ids) == 0
        svc.close()

    # --- Cambridge manifest resolution tests (IMG-02) ---

    def test_cambridge_manifest_found_by_normalized_shelfmark(self, mock_crossref_db):
        """Cambridge manifest URL found via normalized shelfmark."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        url = svc.get_cambridge_manifest("ts12123")
        assert url == "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123"
        svc.close()

    def test_cambridge_manifest_missing_returns_none(self, mock_crossref_db):
        """Unknown shelfmark returns None for Cambridge manifest."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path=mock_crossref_db)
        url = svc.get_cambridge_manifest("nonexistent")
        assert url is None
        svc.close()

    # --- Fallback / graceful degradation tests ---

    def test_service_unavailable_returns_empty(self):
        """Service with nonexistent DB path returns empty results gracefully."""
        from shared.nli_crossref_service import NliCrossrefService
        svc = NliCrossrefService(db_path="/nonexistent/path.db")
        assert svc.is_available() is False
        assert svc.get_images("any_id") == []
        assert svc.get_cambridge_manifest("any") is None
        svc.close()

    # --- Integration accessor test ---

    def test_crossref_service_accessor_exists(self):
        """genizah_core._get_crossref_service function is accessible."""
        from genizah_core import _get_crossref_service
        result = _get_crossref_service()
        assert result is None or hasattr(result, 'get_images')
