# -*- coding: utf-8 -*-
"""
Unit tests for NliCrossrefService.

Tests use temporary SQLite databases to avoid dependency on the real
nli_crossref.db sidecar. Covers all methods, thread-safe mode,
graceful degradation, and edge cases.
"""

import sqlite3
import pytest

from shared.nli_crossref_service import NliCrossrefService, get_nli_crossref_service, parse_folio_label


@pytest.fixture
def test_db(tmp_path):
    """Create a minimal nli_crossref.db for testing."""
    db_path = str(tmp_path / "test_nli_crossref.db")
    conn = sqlite3.connect(db_path)

    # Create meta table
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", "1.0.0"),
            ("created", "2026-02-15T00:00:00Z"),
            ("source_nli", "nli_crossreference.csv"),
            ("source_cambridge", "cambridge_genizah.json"),
        ],
    )

    # Create nli_images table (all 25 columns as TEXT, matching import script)
    conn.execute("""
        CREATE TABLE nli_images (
            LibraryNameEng TEXT, LibraryAbbrev TEXT, LibraryCity TEXT,
            LibraryNameHeb TEXT, CollectionName TEXT, Shelfmark TEXT,
            InventoryId TEXT, OBBox TEXT, OBVolume TEXT, OBFolio TEXT,
            NLI_AlmaId TEXT, CatalogAbbrev TEXT, CatalogEntry TEXT,
            FGPImageNumberId TEXT, FGPNumber TEXT, ImageName TEXT,
            ImageSourceName TEXT, PartOf TEXT, See TEXT, BifolioWith TEXT,
            NumFolio TEXT, NumBifolio TEXT, Material TEXT, Size TEXT,
            IsNotGenizah TEXT
        )
    """)

    # Insert test NLI image data - multiple AlmaIds with varying data
    # ImageName values use NLI pattern: {prefix}__L{leaf}F{folio}B{bifolio}S{side}
    nli_rows = [
        # AlmaId "A001" - has FGP images, PartOf, BifolioWith, physical metadata
        ("Cambridge UL", "CUL", "Cambridge", "ספריית קיימברידג'", "Taylor-Schechter",
         "T-S 12.123", "INV001", "", "", "", "A001", "CAT1", "1",
         "FGP001", "1234", "T_S_12_123__L1F0B0S1", "NLI", "T-S NS 321.5", "",
         "T-S 12.124 leaf 2", "4", "2", "Paper", "15x20", ""),
        ("Cambridge UL", "CUL", "Cambridge", "ספריית קיימברידג'", "Taylor-Schechter",
         "T-S 12.123", "INV001", "", "", "", "A001", "CAT1", "1",
         "FGP002", "1235", "T_S_12_123__L1F0B0S2", "NLI", "", "",
         "", "4", "2", "Paper", "15x20", ""),
        ("Cambridge UL", "CUL", "Cambridge", "ספריית קיימברידג'", "Taylor-Schechter",
         "T-S 12.123", "INV001", "", "", "", "A001", "CAT1", "1",
         "FGP003", "1236", "T_S_12_123__L2F0B0S1", "NLI", "T-S NS 321.5", "",
         "T-S 12.125 leaf 1", "4", "2", "Paper", "15x20", ""),

        # AlmaId "A002" - has FGP images, no relationships
        ("JTS", "JTS", "New York", "בית המדרש לרבנים", "ENA",
         "ENA 2345.1", "INV002", "", "", "", "A002", "CAT2", "5",
         "FGP010", "5678", "ENA_2345__L1F0B0S1", "NLI", "", "", "",
         "2", "", "Vellum", "10x12", ""),

        # AlmaId "A003" - NO FGP images (empty FGPImageNumberId), has metadata
        ("BL", "BL", "London", "הספריה הבריטית", "Or.",
         "Or. 5557B", "INV003", "", "", "", "A003", "CAT3", "10",
         "", "", "Or_5557B_missing_pattern", "BL", "", "", "",
         "1", "", "Parchment", "25x30", ""),

        # AlmaId "A004" - all metadata fields empty (should be skipped by get_physical_metadata)
        ("RNL", "RNL", "St Petersburg", "הספריה הלאומית של רוסיה", "Firk.",
         "Firk. I 100", "INV004", "", "", "", "A004", "CAT4", "20",
         "FGP020", "9999", "Firk_I_100__L1F0B0S1", "NLI", "", "", "",
         "", "", "", "", ""),
    ]
    conn.executemany(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        nli_rows,
    )

    # Create cambridge_manifests table
    conn.execute("""
        CREATE TABLE cambridge_manifests (
            label TEXT NOT NULL,
            manifest_url TEXT NOT NULL,
            normalized_shelfmark TEXT
        )
    """)

    # Insert test Cambridge manifest data
    cam_rows = [
        ("MS-TS-00012-00123", "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123", "ts12123"),
        ("MS-TS-00016-00114", "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00016-00114", "ts16114"),
        ("MS-ADD-00863-00002", "https://cudl.lib.cam.ac.uk/iiif/MS-ADD-00863-00002", "add8632"),
        ("MS-MOSSERI-II-00292-00002", "https://cudl.lib.cam.ac.uk/iiif/MS-MOSSERI-II-00292-00002", "mosseriii2922"),
    ]
    conn.executemany(
        "INSERT INTO cambridge_manifests VALUES (?, ?, ?)",
        cam_rows,
    )

    # Create indexes (matching import script)
    conn.execute("CREATE INDEX idx_nli_alma ON nli_images(NLI_AlmaId)")
    conn.execute("CREATE INDEX idx_nli_fgp ON nli_images(FGPImageNumberId)")
    conn.execute("CREATE INDEX idx_nli_shelfmark ON nli_images(Shelfmark)")
    conn.execute("CREATE INDEX idx_cam_shelfmark ON cambridge_manifests(normalized_shelfmark)")
    conn.execute("CREATE INDEX idx_cam_label ON cambridge_manifests(label)")

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(test_db):
    """Create an NliCrossrefService instance with the test database."""
    svc = NliCrossrefService(db_path=test_db)
    yield svc
    svc.close()


# ── Connection tests ────────────────────────────────────────────────


def test_missing_db_is_not_available():
    """Service with nonexistent path returns is_available=False."""
    svc = NliCrossrefService(db_path="nonexistent_path_that_does_not_exist.db")
    assert svc.is_available() is False
    svc.close()


def test_valid_db_is_available(service):
    """Service with valid temp db returns is_available=True."""
    assert service.is_available() is True


def test_get_version(service):
    """Returns version from meta table."""
    assert service.get_version() == "1.0.0"


def test_thread_safe_mode(test_db):
    """Passes check_same_thread=False when thread_safe=True."""
    svc = NliCrossrefService(db_path=test_db, thread_safe=True)
    assert svc.is_available() is True
    # Verify queries work in thread-safe mode
    assert svc.get_version() == "1.0.0"
    svc.close()


# ── Image lookup tests ─────────────────────────────────────────────


def test_get_images_found(service):
    """Returns ordered image records for known AlmaId."""
    images = service.get_images("A001")
    assert len(images) == 3
    # Check ordering by ImageName
    names = [img["image_name"] for img in images]
    assert names == sorted(names)
    # Check keys
    for img in images:
        assert "fgp_image_number_id" in img
        assert "fgp_number" in img
        assert "image_name" in img
        assert "image_source_name" in img
        assert "shelfmark" in img
    # Verify first image data
    assert images[0]["fgp_image_number_id"] == "FGP001"
    assert images[0]["shelfmark"] == "T-S 12.123"


def test_get_images_not_found(service):
    """Returns empty list for unknown AlmaId."""
    assert service.get_images("UNKNOWN_ID") == []


def test_get_images_batch(service):
    """Returns dict mapping sys_ids to image lists."""
    result = service.get_images_batch(["A001", "A002", "UNKNOWN"])
    assert "A001" in result
    assert len(result["A001"]) == 3
    assert "A002" in result
    assert len(result["A002"]) == 1
    # Unknown should not be in results
    assert "UNKNOWN" not in result


def test_get_images_batch_empty(service):
    """Returns empty dict for empty input."""
    assert service.get_images_batch([]) == {}


# ── Cambridge tests ────────────────────────────────────────────────


def test_get_cambridge_manifest_found(service):
    """Returns manifest URL for known normalized shelfmark."""
    url = service.get_cambridge_manifest("ts12123")
    assert url == "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123"


def test_get_cambridge_manifest_not_found(service):
    """Returns None for unknown shelfmark."""
    assert service.get_cambridge_manifest("nonexistent") is None


def test_get_cambridge_manifest_by_label(service):
    """Returns manifest URL by CUDL label."""
    url = service.get_cambridge_manifest_by_label("MS-TS-00016-00114")
    assert url == "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00016-00114"


# ── Metadata tests ─────────────────────────────────────────────────


def test_get_physical_metadata_found(service):
    """Returns material, folio counts for known AlmaId."""
    meta = service.get_physical_metadata("A001")
    assert meta is not None
    assert meta["material"] == "Paper"
    assert meta["num_folio"] == "4"
    assert meta["num_bifolio"] == "2"
    assert meta["size"] == "15x20"


def test_get_physical_metadata_not_found(service):
    """Returns None for unknown AlmaId."""
    assert service.get_physical_metadata("UNKNOWN_ID") is None


def test_get_physical_metadata_empty_fields(service):
    """Skips rows where all metadata fields are empty."""
    # A004 has all metadata fields empty
    assert service.get_physical_metadata("A004") is None


# ── Relationship tests ─────────────────────────────────────────────


def test_get_part_of(service):
    """Returns PartOf shelfmarks."""
    parts = service.get_part_of("A001")
    assert len(parts) >= 1
    assert "T-S NS 321.5" in parts


def test_get_see_references_empty(service):
    """Returns empty list (no See data in test fixture)."""
    refs = service.get_see_references("A001")
    assert refs == []


def test_get_bifolio_partners(service):
    """Returns BifolioWith entries."""
    partners = service.get_bifolio_partners("A001")
    assert len(partners) >= 1
    bifolio_values = [p["bifolio_with"] for p in partners]
    assert "T-S 12.124 leaf 2" in bifolio_values
    # Check keys
    for p in partners:
        assert "bifolio_with" in p
        assert "image_name" in p


# ── Image sources tests ────────────────────────────────────────────


def test_get_image_sources_with_fgp(service):
    """Returns nli_fgp=True when FGP images exist."""
    sources = service.get_image_sources("A001")
    assert sources["nli_fgp"] is True
    assert sources["image_count"] == 3


def test_get_image_sources_with_cambridge(service):
    """Returns cambridge=True when manifest exists."""
    sources = service.get_image_sources("A001", normalized_shelfmark="ts12123")
    assert sources["cambridge"] is True
    assert sources["nli_fgp"] is True


def test_get_image_sources_no_fgp(service):
    """Returns nli_fgp=False when no FGP images exist."""
    # A003 has empty FGPImageNumberId
    sources = service.get_image_sources("A003")
    assert sources["nli_fgp"] is False
    assert sources["image_count"] == 0


def test_get_image_sources_no_data(service):
    """Returns all False for unknown sys_id."""
    sources = service.get_image_sources("UNKNOWN_ID")
    assert sources["nli_fgp"] is False
    assert sources["cambridge"] is False
    assert sources["image_count"] == 0


# ── Folio label parsing tests ──────────────────────────────────────


def test_parse_folio_label_standard():
    """Standard recto pattern: L1F0B0S1 -> 1r."""
    assert parse_folio_label("T_S_12_1__L1F0B0S1") == "1r"
    assert parse_folio_label("I_C_71__L3F0B0S1") == "3r"


def test_parse_folio_label_verso():
    """Verso pattern: S2 -> v."""
    assert parse_folio_label("T_S_12_1__L1F0B0S2") == "1v"
    assert parse_folio_label("577_7_6__L1F0B1S2") == "1v"


def test_parse_folio_label_missing():
    """Fallback on unrecognized patterns returns empty string."""
    assert parse_folio_label("Missing1") == ""
    assert parse_folio_label("") == ""
    assert parse_folio_label("no_match_here") == ""


def test_parse_folio_label_high_leaf():
    """High leaf numbers (L10+) parse correctly."""
    assert parse_folio_label("Yevr_III_B_1093__L7F0B0S1") == "7r"
    assert parse_folio_label("Test__L10F0B0S2") == "10v"
    assert parse_folio_label("Test__L25F0B0S1") == "25r"


def test_get_folio_images(service):
    """get_folio_images returns enriched dicts with folio_label key."""
    images = service.get_folio_images("A001")
    assert len(images) == 3
    # All images should have folio_label key
    for img in images:
        assert "folio_label" in img
    # Check specific labels based on test data ImageNames
    assert images[0]["folio_label"] == "1r"  # L1...S1
    assert images[1]["folio_label"] == "1v"  # L1...S2
    assert images[2]["folio_label"] == "2r"  # L2...S1


def test_get_folio_images_fallback_label(service):
    """Images with unrecognized ImageName patterns get sequential fallback labels."""
    images = service.get_folio_images("A003")
    assert len(images) == 1
    # A003 has "Or_5557B_missing_pattern" -- no L/S pattern
    assert images[0]["folio_label"] == "1"  # sequential fallback


def test_get_folio_images_empty(service):
    """Unknown sys_id returns empty list."""
    assert service.get_folio_images("UNKNOWN") == []


# ── Graceful degradation tests ─────────────────────────────────────


def test_all_methods_return_empty_when_unavailable():
    """Every method returns empty/None when conn is None."""
    svc = NliCrossrefService(db_path="nonexistent_file.db")
    assert svc.is_available() is False
    assert svc.get_version() is None
    assert svc.get_images("x") == []
    assert svc.get_folio_images("x") == []
    assert svc.get_images_batch(["x"]) == {}
    assert svc.get_cambridge_manifest("x") is None
    assert svc.get_cambridge_manifest_by_label("x") is None
    assert svc.get_physical_metadata("x") is None
    assert svc.get_part_of("x") == []
    assert svc.get_see_references("x") == []
    assert svc.get_bifolio_partners("x") == []
    sources = svc.get_image_sources("x", "y")
    assert sources == {"nli_fgp": False, "cambridge": False, "image_count": 0}
    # close() should not raise
    svc.close()


# ── Close tests ────────────────────────────────────────────────────


def test_close_sets_conn_to_none(test_db):
    """close() sets the connection to None."""
    svc = NliCrossrefService(db_path=test_db)
    assert svc.is_available() is True
    svc.close()
    assert svc.is_available() is False


def test_close_idempotent(test_db):
    """Calling close() multiple times does not raise."""
    svc = NliCrossrefService(db_path=test_db)
    svc.close()
    svc.close()  # Should not raise


# ── Web shim import test ──────────────────────────────────────────


def test_web_shim_import():
    """web.nli_crossref_service shim re-exports NliCrossrefService and get_nli_crossref_service."""
    from web.nli_crossref_service import NliCrossrefService as WebNliService
    from web.nli_crossref_service import get_nli_crossref_service as web_get_service
    assert WebNliService is NliCrossrefService
    assert web_get_service is get_nli_crossref_service
