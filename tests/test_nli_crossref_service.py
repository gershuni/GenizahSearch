# -*- coding: utf-8 -*-
"""
Unit tests for NliCrossrefService.

Tests use temporary SQLite databases to avoid dependency on the real
nli_crossref.db sidecar. Covers all methods, thread-safe mode,
graceful degradation, and edge cases.
"""

import sqlite3
from pathlib import Path

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

    # Create manchester_luna table (Phase 34)
    conn.execute("""
        CREATE TABLE manchester_luna (
            image_source_name TEXT PRIMARY KEY,
            luna_id TEXT NOT NULL,
            jrl_filename TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO manchester_luna (image_source_name, luna_id, jrl_filename) VALUES (?, ?, ?)",
        [
            ("rylands_jrl1379735", "UoMUoML~95~2~95~268773~95~268800", "rylands_jrl1379735"),
            ("rylands_jrl1400001", "UoMUoML~95~2~95~300000~95~300001", "rylands_jrl1400001"),
        ],
    )

    # Create jts_dpul table (Phase 34)
    conn.execute("""
        CREATE TABLE jts_dpul (
            shelfmark TEXT PRIMARY KEY,
            ark_suffix TEXT,
            manifest_url TEXT,
            dpul_url TEXT,
            thumbnail_url TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO jts_dpul (shelfmark, ark_suffix, manifest_url, dpul_url, thumbnail_url) VALUES (?, ?, ?, ?, ?)",
        [
            ("ENA 2345.1", "abc123", "https://figgy.princeton.edu/concern/scanned_resources/abc123/manifest",
             "https://dpul.princeton.edu/cairo_geniza/catalog/abc123", "https://iiif-cloud.princeton.edu/iiif/2/abc123/thumb"),
            ("ENA 9999", "def456", "https://figgy.princeton.edu/concern/scanned_resources/def456/manifest",
             "https://dpul.princeton.edu/cairo_geniza/catalog/def456", "https://iiif-cloud.princeton.edu/iiif/2/def456/thumb"),
        ],
    )

    # Additional test data for Phase 33 metadata enrichment tests
    # A005: IsNotGenizah='True', has CatalogEntry and CollectionName/OBBox/OBVolume/OBFolio
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Oxford", "Oxford", "Oxford", "אוקספורד", "Bodleian",
         "MS Heb c 57.1", "INV005", "Box 12", "Vol 3", "Fol 45",
         "A005", "NC", "Neubauer - Cowley 2603.1",
         "FGP050", "5050", "MS_Heb_c_57__L1F0B0S1", "NLI", "", "", "",
         "2", "", "Parchment", "20x25", "True"),
    )
    # A006: IsNotGenizah='False' (not flagged), has CatalogEntry but empty collection storage
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Oxford", "Oxford", "Oxford", "אוקספורד", "Bodleian",
         "MS Heb d 20.5", "INV006", "", "", "",
         "A006", "NC", "Neubauer - Cowley 1500",
         "FGP060", "6060", "MS_Heb_d_20__L1F0B0S1", "NLI", "", "", "",
         "1", "", "Paper", "15x20", "False"),
    )

    # Create indexes (matching import script)
    conn.execute("CREATE INDEX idx_nli_alma ON nli_images(NLI_AlmaId)")
    conn.execute("CREATE INDEX idx_nli_fgp ON nli_images(FGPImageNumberId)")
    conn.execute("CREATE INDEX idx_nli_shelfmark ON nli_images(Shelfmark)")
    conn.execute("CREATE INDEX idx_cam_shelfmark ON cambridge_manifests(normalized_shelfmark)")
    conn.execute("CREATE INDEX idx_cam_label ON cambridge_manifests(label)")
    conn.execute("CREATE INDEX idx_man_luna ON manchester_luna(image_source_name)")
    conn.execute("CREATE INDEX idx_jts_shelfmark ON jts_dpul(shelfmark)")

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


class TestParseFolioLabelPairedLeaf:
    """
    Paired-leaf / bifolio ImageName notation: L{first}_{second}F{...}B{...}S{...}.

    Historically the _FOLIO_PATTERN regex required ``F`` to immediately follow
    the leaf number, so paired-leaf names like ``T_S_NS_158_112__L1_12F0B0S1``
    produced an empty folio label. This broke folio-picker display and
    collapsed every paired-leaf image into the (999999, 0) sort fallback.

    Regression test fixtures are bug 260419-nwv (T-S NS 158.112 / CUL).
    Decision: primary leaf is the first number in the pair (leaf 1 in
    ``L1_12``), matching conservation page-turning order.
    """

    def test_paired_leaf_recto_primary_is_first_number(self):
        assert parse_folio_label("T_S_NS_158_112__L1_12F0B0S1") == "1r"

    def test_paired_leaf_verso_primary_is_first_number(self):
        assert parse_folio_label("T_S_NS_158_112__L1_12F0B0S2") == "1v"

    def test_paired_leaf_middle_pair(self):
        # L2_11 in a bifolio of leaves 2 and 11 -> primary leaf 2.
        assert parse_folio_label("T_S_NS_158_112__L2_11F0B0S1") == "2r"

    def test_non_paired_still_works_regression_guard(self):
        # L5F... (no paired suffix) must still parse the same way.
        assert parse_folio_label("T_S_NS_158_112__L5F0B0S1") == "5r"

    def test_existing_single_leaf_case_unchanged(self):
        # Regression guard for the original test_parse_folio_label_standard case.
        assert parse_folio_label("T_S_12_1__L1F0B0S2") == "1v"

    def test_existing_I_C_case_unchanged(self):
        assert parse_folio_label("I_C_71__L3F0B0S1") == "3r"

    def test_empty_input_returns_empty(self):
        assert parse_folio_label("") == ""

    def test_no_pattern_returns_empty(self):
        assert parse_folio_label("no_folio_here") == ""


def test_get_folio_images_sorts_paired_leaf_by_leaf_number(tmp_path):
    """
    Paired-leaf ImageNames must sort by primary (first) leaf number, then
    side -- not collapse into the (999999, 0) alphabetical fallback.

    Regression fixture for bug 260419-nwv: prior to the _FOLIO_PATTERN fix,
    all three rows had sort key (999999, 0) and stayed in alphabetical
    ImageName order, placing L2_11F0B0S1 *before* L1_12F0B0S2.
    """
    db_path = str(tmp_path / "paired_leaf.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
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
        """
    )
    # Insert in intentionally-wrong alphabetical order to prove the sort is
    # numeric-by-leaf, not alphabetical by ImageName.
    rows = [
        ("CUL", "CUL", "", "", "", "T-S NS 158.112", "", "", "", "",
         "SYS001", "", "", "", "", "T_S_NS_158_112__L2_11F0B0S1", "NLI",
         "", "", "", "", "", "", "", ""),
        ("CUL", "CUL", "", "", "", "T-S NS 158.112", "", "", "", "",
         "SYS001", "", "", "", "", "T_S_NS_158_112__L1_12F0B0S2", "NLI",
         "", "", "", "", "", "", "", ""),
        ("CUL", "CUL", "", "", "", "T-S NS 158.112", "", "", "", "",
         "SYS001", "", "", "", "", "T_S_NS_158_112__L1_12F0B0S1", "NLI",
         "", "", "", "", "", "", "", ""),
    ]
    # Normalize to 25 columns (table has 25)
    rows = [r[:25] + ("",) * (25 - len(r)) for r in rows]
    conn.executemany(
        "INSERT INTO nli_images VALUES (" + ",".join(["?"] * 25) + ")",
        rows,
    )
    # Minimal cambridge_manifests + meta tables so service initializes cleanly.
    conn.execute(
        "CREATE TABLE cambridge_manifests (normalized_shelfmark TEXT PRIMARY KEY, manifest_url TEXT)"
    )
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=db_path)
    images = svc.get_folio_images("SYS001")
    assert len(images) == 3
    image_names = [img["image_name"] for img in images]
    assert image_names == [
        "T_S_NS_158_112__L1_12F0B0S1",  # leaf 1, side 1 (recto)
        "T_S_NS_158_112__L1_12F0B0S2",  # leaf 1, side 2 (verso)
        "T_S_NS_158_112__L2_11F0B0S1",  # leaf 2, side 1 (recto)
    ]
    assert images[0]["folio_label"] == "1r"
    assert images[1]["folio_label"] == "1v"
    assert images[2]["folio_label"] == "2r"


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


# ── Library viewer URL tests ──────────────────────────────────────


def test_get_library_viewer_url_cul(service):
    """CUL library returns Cambridge Digital Library search URL."""
    result = service.get_library_viewer_url("A001")
    assert result is not None
    assert "cudl.lib.cam.ac.uk" in result["url"]
    assert result["library_abbrev"] == "CUL"
    assert result["label"] == "Cambridge Digital Library"
    assert result["library_name_eng"] == "Cambridge UL"


def test_get_library_viewer_url_manchester_with_luna(test_db):
    """Manchester library returns LUNA detail page URL when luna_id found in sidecar."""
    # Insert a Manchester row whose ImageSourceName matches manchester_luna
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
         "Gaster 756", "INV_M", "", "", "", "M001", "CAT_M", "1",
         "FGP_M", "1111", "Gaster_756__L1F0B0S1", "rylands_jrl1379735", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()
    svc = NliCrossrefService(db_path=test_db)
    result = svc.get_library_viewer_url("M001")
    assert result is not None
    assert "luna.manchester.ac.uk/luna/servlet/detail/" in result["url"]
    assert "UoMUoML" in result["url"]
    assert result["library_abbrev"] == "Manchester"
    assert result["label"] == "Manchester LUNA"
    svc.close()


def test_get_library_viewer_url_manchester_fallback(test_db):
    """Manchester library falls back to search URL when luna_id not in sidecar."""
    # Insert a Manchester row whose ImageSourceName does NOT match manchester_luna
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
         "Gaster 999", "INV_M2", "", "", "", "M002", "CAT_M2", "1",
         "FGP_M2", "2222", "Gaster_999__L1F0B0S1", "unknown_source", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()
    svc = NliCrossrefService(db_path=test_db)
    result = svc.get_library_viewer_url("M002")
    assert result is not None
    assert "luna.manchester.ac.uk/luna/servlet/view/search" in result["url"]
    assert "q=Gaster" in result["url"]
    assert result["library_abbrev"] == "Manchester"
    assert result["label"] == "Manchester LUNA"
    svc.close()


def test_get_library_viewer_url_jts_with_dpul(service):
    """JTS library returns DPUL catalog page URL when shelfmark found in sidecar."""
    # A002 has JTS abbrev with shelfmark "ENA 2345.1" which is in jts_dpul
    result = service.get_library_viewer_url("A002")
    assert result is not None
    assert "dpul.princeton.edu/cairo_geniza/catalog/abc123" in result["url"]
    assert result["library_abbrev"] == "JTS"
    assert result["label"] == "Princeton Digital Library"


def test_get_library_viewer_url_jts_fallback(test_db):
    """JTS library falls back to search URL when shelfmark not in jts_dpul."""
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("JTS", "JTS", "New York", "בית המדרש לרבנים", "ENA",
         "ENA 0001.99", "INV_J2", "", "", "", "J002", "CAT_J2", "1",
         "FGP_J2", "3333", "ENA_0001__L1F0B0S1", "NLI", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()
    svc = NliCrossrefService(db_path=test_db)
    result = svc.get_library_viewer_url("J002")
    assert result is not None
    assert "dpul.princeton.edu/cairo_geniza/catalog?" in result["url"]
    assert "q=ENA" in result["url"]
    assert result["library_abbrev"] == "JTS"
    assert result["label"] == "Princeton Digital Library"
    svc.close()


def test_get_library_viewer_url_bl(test_db):
    """BL library returns searcharchives.bl.uk URL."""
    # A003 has BL library in fixture with shelfmark "Or. 5557B" (no leaf suffix)
    svc = NliCrossrefService(db_path=test_db)
    result = svc.get_library_viewer_url("A003")
    assert result is not None
    assert "searcharchives.bl.uk" in result["url"]
    assert "q=Or.+5557B" in result["url"] or "q=Or.%205557B" in result["url"]
    assert result["library_abbrev"] == "BL"
    assert result["label"] == "British Library"
    svc.close()


def test_get_library_viewer_url_bl_strips_leaf(test_db):
    """BL library strips leaf suffix (.N) from shelfmark for search."""
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("BL", "BL", "London", "British Library", "Or.",
         "OR 10110.1", "INV_BL2", "", "", "", "BL002", "CAT_BL", "1",
         "FGP_BL2", "2222", "OR_10110__L1F0B0S1", "NLI", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()
    svc = NliCrossrefService(db_path=test_db)
    result = svc.get_library_viewer_url("BL002")
    assert result is not None
    assert "searcharchives.bl.uk" in result["url"]
    # Leaf suffix ".1" should be stripped
    assert "OR+10110" in result["url"] or "OR%2010110" in result["url"]
    assert ".1" not in result["url"]
    # Spaces must NOT be converted to underscores (searcharchives requires URL-encoded spaces)
    assert "OR_10110" not in result["url"]
    svc.close()


def test_get_library_viewer_url_unknown(service):
    """Unknown library abbreviation returns None."""
    # A004 has RNL (not in the URL map)
    result = service.get_library_viewer_url("A004")
    assert result is None


def test_get_library_viewer_url_missing(service):
    """Missing sys_id returns None."""
    result = service.get_library_viewer_url("NONEXISTENT")
    assert result is None


# ── Manchester LUNA tests ─────────────────────────────────────────


def test_get_manchester_luna_id_found(service):
    """Returns luna_id for known image source name."""
    luna_id = service.get_manchester_luna_id("rylands_jrl1379735")
    assert luna_id == "UoMUoML~95~2~95~268773~95~268800"


def test_get_manchester_luna_id_not_found(service):
    """Returns None for unknown image source name."""
    assert service.get_manchester_luna_id("unknown_source") is None


def test_get_manchester_manifest_url_found(service):
    """Returns IIIF manifest URL for known image source name."""
    url = service.get_manchester_manifest_url("rylands_jrl1379735")
    assert url == "https://luna.manchester.ac.uk/luna/servlet/iiif/m/UoMUoML~95~2~95~268773~95~268800/manifest"


def test_get_manchester_manifest_url_not_found(service):
    """Returns None for unknown image source name."""
    assert service.get_manchester_manifest_url("unknown_source") is None


# ── Manchester canvases tests ─────────────────────────────────────


def test_get_manchester_canvases_two_images_recto_verso(test_db):
    """sys_id with 2 crossref images (S1 recto, S2 verso) that both have luna_ids
    returns 2 canvas entries with distinct IIIF URLs and folio labels '1r' and '1v'."""
    conn = sqlite3.connect(test_db)
    conn.row_factory = sqlite3.Row
    # Insert Manchester nli_images rows with ImageSourceNames matching manchester_luna fixture
    conn.executemany(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 756", "INV_M1", "", "", "", "M001", "", "",
             "FGP_M1", "1111", "Gaster_756__L1F0B0S1", "rylands_jrl1379735", "", "", "",
             "1", "", "Paper", "10x15", ""),
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 756", "INV_M1", "", "", "", "M001", "", "",
             "FGP_M2", "1112", "Gaster_756__L1F0B0S2", "rylands_jrl1400001", "", "", "",
             "1", "", "Paper", "10x15", ""),
        ],
    )
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=test_db)
    canvases = svc.get_manchester_canvases("M001")
    svc.close()

    assert len(canvases) == 2
    # First canvas = recto (S1)
    assert canvases[0]['label'] == '1r'
    assert canvases[0]['url'] == 'https://luna.manchester.ac.uk/luna/servlet/iiif/UoMUoML~95~2~95~268773~95~268800'
    assert canvases[0]['folio_num'] == 1
    # Second canvas = verso (S2)
    assert canvases[1]['label'] == '1v'
    assert canvases[1]['url'] == 'https://luna.manchester.ac.uk/luna/servlet/iiif/UoMUoML~95~2~95~300000~95~300001'
    assert canvases[1]['folio_num'] == 1
    # URLs must be distinct
    assert canvases[0]['url'] != canvases[1]['url']


def test_get_manchester_canvases_multi_leaf(test_db):
    """sys_id with 3 crossref images (multi-leaf) returns 3 canvases in ImageName sort order."""
    conn = sqlite3.connect(test_db)
    conn.row_factory = sqlite3.Row
    # Add a third luna_id for the third image
    conn.execute(
        "INSERT INTO manchester_luna (image_source_name, luna_id, jrl_filename) VALUES (?, ?, ?)",
        ("rylands_jrl1500001", "UoMUoML~95~2~95~400000~95~400001", "rylands_jrl1500001"),
    )
    conn.executemany(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 800", "INV_M2", "", "", "", "M002", "", "",
             "FGP_M3", "2001", "Gaster_800__L1F0B0S1", "rylands_jrl1379735", "", "", "",
             "2", "", "Paper", "10x15", ""),
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 800", "INV_M2", "", "", "", "M002", "", "",
             "FGP_M4", "2002", "Gaster_800__L1F0B0S2", "rylands_jrl1400001", "", "", "",
             "2", "", "Paper", "10x15", ""),
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 800", "INV_M2", "", "", "", "M002", "", "",
             "FGP_M5", "2003", "Gaster_800__L2F0B0S1", "rylands_jrl1500001", "", "", "",
             "2", "", "Paper", "10x15", ""),
        ],
    )
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=test_db)
    canvases = svc.get_manchester_canvases("M002")
    svc.close()

    assert len(canvases) == 3
    # Verify sorted by ImageName order: L1S1, L1S2, L2S1
    assert canvases[0]['label'] == '1r'
    assert canvases[1]['label'] == '1v'
    assert canvases[2]['label'] == '2r'
    assert canvases[2]['folio_num'] == 2


def test_get_manchester_canvases_no_luna_id(test_db):
    """sys_id with 1 image whose luna_id is NOT in manchester_luna returns empty list."""
    conn = sqlite3.connect(test_db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
         "Gaster 900", "INV_M3", "", "", "", "M003", "", "",
         "FGP_M6", "3001", "Gaster_900__L1F0B0S1", "rylands_unknown_source", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=test_db)
    canvases = svc.get_manchester_canvases("M003")
    svc.close()

    assert canvases == []


def test_get_manchester_canvases_no_images(service):
    """sys_id with no crossref images returns empty list."""
    canvases = service.get_manchester_canvases("NONEXISTENT_ID")
    assert canvases == []


def test_get_manchester_canvases_url_format(test_db):
    """Canvas URL format is correct IIIF image base (no /manifest suffix)."""
    conn = sqlite3.connect(test_db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
         "Gaster 950", "INV_M5", "", "", "", "M005", "", "",
         "FGP_M10", "5001", "Gaster_950__L1F0B0S1", "rylands_jrl1379735", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=test_db)
    canvases = svc.get_manchester_canvases("M005")
    svc.close()

    assert len(canvases) == 1
    url = canvases[0]['url']
    assert url.startswith('https://luna.manchester.ac.uk/luna/servlet/iiif/')
    assert not url.endswith('/manifest')
    assert 'UoMUoML~95~2~95~268773~95~268800' in url


def test_get_manchester_canvases_folio_num(test_db):
    """folio_num is correctly derived from the label (1 for '1r', 1 for '1v', 2 for '2r')."""
    conn = sqlite3.connect(test_db)
    conn.row_factory = sqlite3.Row
    # Add third luna_id if not already present
    conn.execute(
        "INSERT OR IGNORE INTO manchester_luna (image_source_name, luna_id, jrl_filename) VALUES (?, ?, ?)",
        ("rylands_jrl1500001", "UoMUoML~95~2~95~400000~95~400001", "rylands_jrl1500001"),
    )
    conn.executemany(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 960", "INV_M6", "", "", "", "M006", "", "",
             "FGP_M11", "6001", "Gaster_960__L1F0B0S1", "rylands_jrl1379735", "", "", "",
             "2", "", "Paper", "10x15", ""),
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 960", "INV_M6", "", "", "", "M006", "", "",
             "FGP_M12", "6002", "Gaster_960__L1F0B0S2", "rylands_jrl1400001", "", "", "",
             "2", "", "Paper", "10x15", ""),
            ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
             "Gaster 960", "INV_M6", "", "", "", "M006", "", "",
             "FGP_M13", "6003", "Gaster_960__L2F0B0S1", "rylands_jrl1500001", "", "", "",
             "2", "", "Paper", "10x15", ""),
        ],
    )
    conn.commit()
    conn.close()

    svc = NliCrossrefService(db_path=test_db)
    canvases = svc.get_manchester_canvases("M006")
    svc.close()

    assert canvases[0]['folio_num'] == 1  # '1r' -> 1
    assert canvases[1]['folio_num'] == 1  # '1v' -> 1
    assert canvases[2]['folio_num'] == 2  # '2r' -> 2


# ── JTS/Princeton DPUL tests ─────────────────────────────────────


def test_get_jts_manifest_url_found(service):
    """Returns Figgy manifest URL for known shelfmark."""
    url = service.get_jts_manifest_url("ENA 2345.1")
    assert url == "https://figgy.princeton.edu/concern/scanned_resources/abc123/manifest"


def test_get_jts_manifest_url_base_fallback(service):
    """Falls back to base shelfmark when leaf suffix not found directly."""
    # "ENA 9999.5" is not in jts_dpul, but "ENA 9999" is
    url = service.get_jts_manifest_url("ENA 9999.5")
    assert url == "https://figgy.princeton.edu/concern/scanned_resources/def456/manifest"


def test_get_jts_manifest_url_not_found(service):
    """Returns None for unknown shelfmark."""
    assert service.get_jts_manifest_url("UNKNOWN 000") is None


def test_get_jts_dpul_url_found(service):
    """Returns DPUL catalog URL for known shelfmark."""
    url = service.get_jts_dpul_url("ENA 2345.1")
    assert url == "https://dpul.princeton.edu/cairo_geniza/catalog/abc123"


def test_get_jts_dpul_url_base_fallback(service):
    """Falls back to base shelfmark when leaf suffix not found directly."""
    url = service.get_jts_dpul_url("ENA 9999.5")
    assert url == "https://dpul.princeton.edu/cairo_geniza/catalog/def456"


def test_get_jts_dpul_url_not_found(service):
    """Returns None for unknown shelfmark."""
    assert service.get_jts_dpul_url("UNKNOWN 000") is None


# ── Image sources (Manchester + JTS) tests ────────────────────────


def test_get_image_sources_with_manchester(test_db):
    """Returns manchester=True when Manchester LUNA data exists in sidecar."""
    # Insert a Manchester row with ImageSourceName matching manchester_luna
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Manchester", "Manchester", "Manchester", "מנצ'סטר", "Rylands",
         "Gaster 756", "INV_MS", "", "", "", "MS001", "CAT_MS", "1",
         "FGP_MS", "4444", "Gaster_756__L1F0B0S1", "rylands_jrl1379735", "", "", "",
         "1", "", "Paper", "10x15", ""),
    )
    conn.commit()
    conn.close()
    svc = NliCrossrefService(db_path=test_db)
    sources = svc.get_image_sources("MS001", normalized_shelfmark="gaster756")
    assert sources["manchester"] is True
    svc.close()


def test_get_image_sources_manchester_false(service):
    """Returns manchester=False when no Manchester LUNA data for sys_id."""
    sources = service.get_image_sources("A001", normalized_shelfmark="ts12123")
    assert sources["manchester"] is False


def test_get_image_sources_with_jts(service):
    """Returns jts=True when JTS DPUL data exists for the shelfmark."""
    # A002 has JTS library with shelfmark "ENA 2345.1" which is in jts_dpul
    sources = service.get_image_sources("A002", normalized_shelfmark="ena23451")
    assert sources["jts"] is True


def test_get_image_sources_jts_false(service):
    """Returns jts=False when no JTS DPUL data for the shelfmark."""
    sources = service.get_image_sources("A001", normalized_shelfmark="ts12123")
    assert sources["jts"] is False


# ── Catalog entry tests ──────────────────────────────────────────


def test_get_catalog_entry(service):
    """Returns CatalogEntry string for manuscript with Neubauer-Cowley reference."""
    entry = service.get_catalog_entry("A005")
    assert entry == "Neubauer - Cowley 2603.1"


def test_get_catalog_entry_empty(service):
    """Returns None for non-existent sys_id."""
    assert service.get_catalog_entry("NONEXISTENT") is None


def test_get_catalog_entry_numeric_only(service):
    """Returns numeric CatalogEntry values as-is (they are stored as TEXT)."""
    # A001 has CatalogEntry="1"
    entry = service.get_catalog_entry("A001")
    assert entry == "1"


# ── Collection storage tests ─────────────────────────────────────


def test_get_collection_storage(service):
    """Returns dict with collection/storage info for manuscript with data."""
    storage = service.get_collection_storage("A005")
    assert storage is not None
    assert storage["collection_name"] == "Bodleian"
    assert storage["ob_box"] == "Box 12"
    assert storage["ob_volume"] == "Vol 3"
    assert storage["ob_folio"] == "Fol 45"


def test_get_collection_storage_empty(service):
    """Returns None when no rows match the storage criteria."""
    # No rows for NONEXISTENT -- same behavior as truly empty
    storage = service.get_collection_storage("NONEXISTENT")
    assert storage is None


def test_get_collection_storage_no_data(service):
    """Returns None for non-existent sys_id."""
    assert service.get_collection_storage("NONEXISTENT") is None


def test_get_collection_storage_partial(service):
    """Returns dict even when only CollectionName is populated."""
    # A001 has CollectionName="Taylor-Schechter" but OBBox/OBVolume/OBFolio all empty
    storage = service.get_collection_storage("A001")
    assert storage is not None
    assert storage["collection_name"] == "Taylor-Schechter"


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
    assert svc.get_library_viewer_url("x") is None
    assert svc.get_part_of("x") == []
    assert svc.get_see_references("x") == []
    assert svc.get_bifolio_partners("x") == []
    # Phase 33 metadata enrichment methods
    assert svc.get_catalog_entry("x") is None
    assert svc.get_collection_storage("x") is None
    # Manchester and JTS methods
    assert svc.get_manchester_luna_id("x") is None
    assert svc.get_manchester_manifest_url("x") is None
    assert svc.get_jts_manifest_url("x") is None
    assert svc.get_jts_dpul_url("x") is None
    sources = svc.get_image_sources("x", "y")
    assert sources == {
        "nli_fgp": False, "cambridge": False,
        "manchester": False, "jts": False,
        "image_count": 0,
    }
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


# ── _parse_cudl_label (genizah_core, 260419-cfx) ──────────────────


def test_parse_cudl_label_bare_numeric_is_recto():
    """Bare numeric label ('1') is treated as recto by convention."""
    from genizah_core import _parse_cudl_label
    assert _parse_cudl_label('1') == (1, 'r')


def test_parse_cudl_label_verso():
    """'1v' parses to (1, 'v')."""
    from genizah_core import _parse_cudl_label
    assert _parse_cudl_label('1v') == (1, 'v')


def test_parse_cudl_label_binding():
    """Non-folio label 'Binding' returns (None, None)."""
    from genizah_core import _parse_cudl_label
    assert _parse_cudl_label('Binding') == (None, None)


def test_parse_cudl_label_with_f_prefix():
    """'f.2v' and 'f. 3r' (space variant) parse correctly."""
    from genizah_core import _parse_cudl_label
    assert _parse_cudl_label('f.2v') == (2, 'v')
    assert _parse_cudl_label('f. 3r') == (3, 'r')


# ── resolve_cambridge_canvas_for_page (260419-cfx) ────────────────


class TestFolioSideResolver:
    """T-S NS 158.112 fixture: 14 nli_images rows (incl. paired-leaf
    bifolio ImageNames), 12 CUDL canvases 1r..6v.

    Validates resolve_cambridge_canvas_for_page for all 14 pages:
        pages 0..11 → exact canvas match (1r, 1v, ..., 6v)
        pages 12..13 → None (folios 8r/8v; no CUDL canvas)
    """

    TS_NS_158_112_IMAGE_NAMES = [
        'T_S_NS_158_112__L1_12F0B0S1',  # 1r
        'T_S_NS_158_112__L1_12F0B0S2',  # 1v
        'T_S_NS_158_112__L2_11F0B0S1',  # 2r
        'T_S_NS_158_112__L2_11F0B0S2',  # 2v
        'T_S_NS_158_112__L3_10F0B0S1',  # 3r
        'T_S_NS_158_112__L3_10F0B0S2',  # 3v
        'T_S_NS_158_112__L4_9F0B0S1',   # 4r
        'T_S_NS_158_112__L4_9F0B0S2',   # 4v
        'T_S_NS_158_112__L5F0B0S1',     # 5r
        'T_S_NS_158_112__L5F0B0S2',     # 5v
        'T_S_NS_158_112__L6_7F0B0S1',   # 6r
        'T_S_NS_158_112__L6_7F0B0S2',   # 6v
        'T_S_NS_158_112__L8F0B0S1',     # 8r — NO CUDL canvas
        'T_S_NS_158_112__L8F0B0S2',     # 8v — NO CUDL canvas
    ]

    TS_NS_158_112_CUDL_CANVASES = [
        {'label': '1r', 'url': 'https://x/1r', 'folio_num': 1, 'folio_side': 'r'},
        {'label': '1v', 'url': 'https://x/1v', 'folio_num': 1, 'folio_side': 'v'},
        {'label': '2r', 'url': 'https://x/2r', 'folio_num': 2, 'folio_side': 'r'},
        {'label': '2v', 'url': 'https://x/2v', 'folio_num': 2, 'folio_side': 'v'},
        {'label': '3r', 'url': 'https://x/3r', 'folio_num': 3, 'folio_side': 'r'},
        {'label': '3v', 'url': 'https://x/3v', 'folio_num': 3, 'folio_side': 'v'},
        {'label': '4r', 'url': 'https://x/4r', 'folio_num': 4, 'folio_side': 'r'},
        {'label': '4v', 'url': 'https://x/4v', 'folio_num': 4, 'folio_side': 'v'},
        {'label': '5r', 'url': 'https://x/5r', 'folio_num': 5, 'folio_side': 'r'},
        {'label': '5v', 'url': 'https://x/5v', 'folio_num': 5, 'folio_side': 'v'},
        {'label': '6r', 'url': 'https://x/6r', 'folio_num': 6, 'folio_side': 'r'},
        {'label': '6v', 'url': 'https://x/6v', 'folio_num': 6, 'folio_side': 'v'},
    ]

    @pytest.fixture
    def ts_ns_158_112_svc(self, tmp_path):
        """Bare-schema SQLite fixture: 14 nli_images rows for T-S NS 158.112.

        Plan Task 1 Step 0 verified: NliCrossrefService.__init__ only
        opens the file; is_available() returns True for a bare schema
        with empty `meta` and a populated `nli_images` table. If that
        changes in future, seed ('version','0.0.0-test') into meta here.
        """
        db_path = tmp_path / "nli_crossref.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, "
            "FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT, "
            "Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT "
            "DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '', "
            "LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '', "
            "CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '', "
            "OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT "
            "DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '', "
            "BifolioWith TEXT DEFAULT '')"
        )
        for idx, img_name in enumerate(self.TS_NS_158_112_IMAGE_NAMES):
            conn.execute(
                "INSERT INTO nli_images (NLI_AlmaId, FGPImageNumberId, "
                "FGPNumber, ImageName, ImageSourceName, Shelfmark) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("990051537270205171", f"FGP{idx}", str(idx), img_name, "",
                 "T-S NS 158.112"),
            )
        conn.commit()
        conn.close()
        svc = NliCrossrefService(db_path=str(db_path))
        # Smoke-check before any resolver test — if this fails, every
        # resolver test below would silently return {'degraded': True}
        # and assertions would all pass wrong.
        assert svc.is_available(), (
            "Fixture sqlite is_available() must be True. If False, seed "
            "a meta.version row or inspect __init__."
        )
        assert len(svc.get_folio_images("990051537270205171")) == 14
        yield svc
        svc.close()

    @pytest.mark.parametrize("page,expected_canvas_idx,expected_folio,expected_side", [
        (0, 0, 1, 'r'),
        (1, 1, 1, 'v'),
        (2, 2, 2, 'r'),
        (3, 3, 2, 'v'),
        (4, 4, 3, 'r'),
        (5, 5, 3, 'v'),
        (6, 6, 4, 'r'),
        (7, 7, 4, 'v'),
        (8, 8, 5, 'r'),
        (9, 9, 5, 'v'),
        (10, 10, 6, 'r'),
        (11, 11, 6, 'v'),
    ])
    def test_resolves_exact_canvas_for_pages_0_through_11(
        self, ts_ns_158_112_svc, page, expected_canvas_idx,
        expected_folio, expected_side,
    ):
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        out = resolve_cambridge_canvas_for_page(
            "990051537270205171", page,
            self.TS_NS_158_112_CUDL_CANVASES,
            svc=ts_ns_158_112_svc,
        )
        assert out == {
            'canvas_index': expected_canvas_idx,
            'folio_num': expected_folio,
            'side': expected_side,
        }

    def test_returns_none_for_page_12_folio_8r_no_canvas(self, ts_ns_158_112_svc):
        """Page 12 → folio 8r; no CUDL canvas → None (NLI fallback)."""
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        out = resolve_cambridge_canvas_for_page(
            "990051537270205171", 12,
            self.TS_NS_158_112_CUDL_CANVASES,
            svc=ts_ns_158_112_svc,
        )
        assert out is None

    def test_returns_none_for_page_13_folio_8v_no_canvas(self, ts_ns_158_112_svc):
        """Page 13 → folio 8v; no CUDL canvas → None (NLI fallback)."""
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        out = resolve_cambridge_canvas_for_page(
            "990051537270205171", 13,
            self.TS_NS_158_112_CUDL_CANVASES,
            svc=ts_ns_158_112_svc,
        )
        assert out is None

    def test_degraded_when_sys_id_unknown(self, ts_ns_158_112_svc):
        """Unknown sys_id returns {'degraded': True} (caller uses legacy positional)."""
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        out = resolve_cambridge_canvas_for_page(
            "UNKNOWN_SYS_ID", 0,
            self.TS_NS_158_112_CUDL_CANVASES,
            svc=ts_ns_158_112_svc,
        )
        assert out == {'degraded': True}

    def test_bare_numeric_label_matches_recto_only(self, ts_ns_158_112_svc):
        """A canvas with folio_num=1 and folio_side=None (bare '1' label)
        matches target (1, 'r') but NOT (1, 'v')."""
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        side_less_canvases = [
            {'label': '1', 'url': 'https://x/1', 'folio_num': 1, 'folio_side': None},
        ]
        # Page 0 = 1r → should match side-less canvas (bare-numeric = recto)
        out_recto = resolve_cambridge_canvas_for_page(
            "990051537270205171", 0, side_less_canvases, svc=ts_ns_158_112_svc,
        )
        # Page 1 = 1v → verso target does NOT match side-less canvas → None
        out_verso = resolve_cambridge_canvas_for_page(
            "990051537270205171", 1, side_less_canvases, svc=ts_ns_158_112_svc,
        )
        assert out_recto == {'canvas_index': 0, 'folio_num': 1, 'side': 'r'}
        assert out_verso is None


# ── classify_cambridge_alignment (260421-aln) ─────────────────────


class TestClassifyCambridgeAlignment:
    """Per-position alignment verdict for CUL Cambridge manuscripts.

    Covers the three real-world verdicts and edge cases surfaced by
    Codex review 2026-04-21:

    - ``aligned / ok``          — CUDL positions match NLI folio order
    - ``misaligned / count_mismatch``       — e.g. T-S NS 158.112 (14 NLI vs 12 CUDL)
    - ``misaligned / position_mismatch``    — e.g. Or.2245 (42 vs 42 but position 2 diverges)
    - ``unknown / no_ext``      — no CUDL list at all
    - ``unknown / no_sidecar``  — sidecar unavailable or sys_id has no NLI rows
    - unparseable positions (binding canvas, garbled NLI label) are skipped
      rather than forced into a mismatch
    """

    @pytest.fixture
    def aligned_svc(self, tmp_path):
        """Six NLI rows 1r..3v for sys_id ALIGN001."""
        db_path = tmp_path / "nli_crossref.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, "
            "FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT, "
            "Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT "
            "DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '', "
            "LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '', "
            "CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '', "
            "OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT "
            "DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '', "
            "BifolioWith TEXT DEFAULT '')"
        )
        for idx, name in enumerate([
            'ALIGN_001__L1F0B0S1', 'ALIGN_001__L1F0B0S2',
            'ALIGN_001__L2F0B0S1', 'ALIGN_001__L2F0B0S2',
            'ALIGN_001__L3F0B0S1', 'ALIGN_001__L3F0B0S2',
        ]):
            conn.execute(
                "INSERT INTO nli_images (NLI_AlmaId, FGPImageNumberId, "
                "FGPNumber, ImageName, ImageSourceName, Shelfmark) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("ALIGN001", f"FGP{idx}", str(idx), name, "", "Dummy-S 1.1"),
            )
        conn.commit()
        conn.close()
        svc = NliCrossrefService(db_path=str(db_path))
        assert svc.is_available()
        yield svc
        svc.close()

    def test_aligned_when_positions_match(self, aligned_svc):
        from shared.nli_crossref_service import classify_cambridge_alignment
        cudl = [
            {'folio_num': 1, 'folio_side': 'r'},
            {'folio_num': 1, 'folio_side': 'v'},
            {'folio_num': 2, 'folio_side': 'r'},
            {'folio_num': 2, 'folio_side': 'v'},
            {'folio_num': 3, 'folio_side': 'r'},
            {'folio_num': 3, 'folio_side': 'v'},
        ]
        out = classify_cambridge_alignment("ALIGN001", cudl, svc=aligned_svc)
        assert out == {
            'verdict': 'aligned', 'reason': 'ok',
            'ext_count': 6, 'nli_count': 6,
            'first_mismatch_index': None,
        }

    def test_misaligned_count_mismatch_ts_ns_158_112(self):
        """T-S NS 158.112 fixture: 14 NLI rows vs 12 CUDL canvases."""
        from shared.nli_crossref_service import classify_cambridge_alignment
        # Reuse the TestFolioSideResolver fixtures via a fresh svc
        import tempfile
        tmp = tempfile.TemporaryDirectory()
        try:
            db_path = Path(tmp.name) / "nli_crossref.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
            conn.execute(
                "CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, "
                "FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT, "
                "Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT "
                "DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '', "
                "LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '', "
                "CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '', "
                "OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT "
                "DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '', "
                "BifolioWith TEXT DEFAULT '')"
            )
            for idx, name in enumerate(TestFolioSideResolver.TS_NS_158_112_IMAGE_NAMES):
                conn.execute(
                    "INSERT INTO nli_images (NLI_AlmaId, FGPImageNumberId, "
                    "FGPNumber, ImageName, ImageSourceName, Shelfmark) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("990051537270205171", f"FGP{idx}", str(idx), name, "",
                     "T-S NS 158.112"),
                )
            conn.commit()
            conn.close()
            svc = NliCrossrefService(db_path=str(db_path))
            out = classify_cambridge_alignment(
                "990051537270205171",
                TestFolioSideResolver.TS_NS_158_112_CUDL_CANVASES,
                svc=svc,
            )
            svc.close()
            assert out['verdict'] == 'misaligned'
            assert out['reason'] == 'count_mismatch'
            assert out['ext_count'] == 12
            assert out['nli_count'] == 14
            assert out['first_mismatch_index'] is None
        finally:
            tmp.cleanup()

    def test_misaligned_position_mismatch_or_2245(self, aligned_svc):
        """Or.2245 pattern: same count but position 2 diverges.

        Codex's bounded live sample found Or.2245 (sys_id 990001332980205171)
        has 42 NLI rows vs 42 CUDL canvases but position 2 is NLI=2r /
        CUDL=1v. Reduced to 6 rows for the fixture: CUDL has a leading
        binding duplicate.
        """
        from shared.nli_crossref_service import classify_cambridge_alignment
        # NLI sequence from aligned_svc: 1r, 1v, 2r, 2v, 3r, 3v
        # CUDL: 1r, 1v, 1v (duplicate), 2r, 2v, 3r — same length, position 2 diverges
        cudl = [
            {'folio_num': 1, 'folio_side': 'r'},
            {'folio_num': 1, 'folio_side': 'v'},
            {'folio_num': 1, 'folio_side': 'v'},  # expected 2r — diverges here
            {'folio_num': 2, 'folio_side': 'r'},
            {'folio_num': 2, 'folio_side': 'v'},
            {'folio_num': 3, 'folio_side': 'r'},
        ]
        out = classify_cambridge_alignment("ALIGN001", cudl, svc=aligned_svc)
        assert out['verdict'] == 'misaligned'
        assert out['reason'] == 'position_mismatch'
        assert out['first_mismatch_index'] == 2
        assert out['ext_count'] == 6
        assert out['nli_count'] == 6

    def test_unparseable_cudl_position_skipped(self, aligned_svc):
        """A binding/cover canvas at an otherwise aligned position must not
        force the whole manuscript to NLI. folio_num=None is skipped."""
        from shared.nli_crossref_service import classify_cambridge_alignment
        # Same length (6) as NLI, but canvas[0] is a binding (unparseable)
        # which happens to coincide with NLI[0]=1r. Skipping it lets the
        # remaining 5 positions disagree... so to truly test "skip not
        # force mismatch", keep the rest aligned by shifting labels:
        # NLI:  1r 1v 2r 2v 3r 3v
        # CUDL: ?  1v 2r 2v 3r 3v  (CUDL[0] unparseable, rest aligned with NLI[1..])
        # Per helper semantics the unparseable [0] is skipped (no mismatch);
        # positions [1..5] compared: (1v vs 1v)(2r vs 2r)... no mismatch.
        # BUT that ignores [0] alignment — this is the documented trade-off
        # (binding/cover skipping). So result is 'aligned / ok'.
        cudl = [
            {'folio_num': None, 'folio_side': None},  # binding
            {'folio_num': 1, 'folio_side': 'v'},
            {'folio_num': 2, 'folio_side': 'r'},
            {'folio_num': 2, 'folio_side': 'v'},
            {'folio_num': 3, 'folio_side': 'r'},
            {'folio_num': 3, 'folio_side': 'v'},
        ]
        out = classify_cambridge_alignment("ALIGN001", cudl, svc=aligned_svc)
        # Position 1 now compares CUDL(1v) with NLI[1]=1v → match.
        # Position 2 compares CUDL(2r) with NLI[2]=2r → match. Etc.
        # So the verdict is aligned, as documented.
        assert out['verdict'] == 'aligned'
        assert out['reason'] == 'ok'

    def test_unparseable_cudl_does_not_mask_real_mismatch(self, aligned_svc):
        """An unparseable canvas is skipped, but a real (folio, side)
        disagreement later in the list must still be reported."""
        from shared.nli_crossref_service import classify_cambridge_alignment
        cudl = [
            {'folio_num': None, 'folio_side': None},  # binding (skipped)
            {'folio_num': 1, 'folio_side': 'v'},
            {'folio_num': 2, 'folio_side': 'r'},
            {'folio_num': 9, 'folio_side': 'v'},      # NLI[3]=2v → mismatch
            {'folio_num': 3, 'folio_side': 'r'},
            {'folio_num': 3, 'folio_side': 'v'},
        ]
        out = classify_cambridge_alignment("ALIGN001", cudl, svc=aligned_svc)
        assert out['verdict'] == 'misaligned'
        assert out['reason'] == 'position_mismatch'
        assert out['first_mismatch_index'] == 3

    def test_unknown_no_ext_when_cudl_empty(self, aligned_svc):
        from shared.nli_crossref_service import classify_cambridge_alignment
        out = classify_cambridge_alignment("ALIGN001", [], svc=aligned_svc)
        assert out['verdict'] == 'unknown'
        assert out['reason'] == 'no_ext'
        assert out['ext_count'] == 0
        assert out['nli_count'] == 0

    def test_unknown_no_sidecar_when_svc_degraded(self, tmp_path):
        """If the sidecar has no rows for this sys_id, caller must keep
        legacy default behavior — not force NLI."""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, "
            "FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT, "
            "Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT "
            "DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '', "
            "LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '', "
            "CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '', "
            "OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT "
            "DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '', "
            "BifolioWith TEXT DEFAULT '')"
        )
        conn.commit()
        conn.close()
        svc = NliCrossrefService(db_path=str(db_path))
        from shared.nli_crossref_service import classify_cambridge_alignment
        cudl = [{'folio_num': 1, 'folio_side': 'r'}]
        out = classify_cambridge_alignment("MISSING_SYS", cudl, svc=svc)
        svc.close()
        assert out['verdict'] == 'unknown'
        assert out['reason'] == 'no_sidecar'
        assert out['ext_count'] == 1
        assert out['nli_count'] == 0
