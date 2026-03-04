# -*- coding: utf-8 -*-
"""
Unit tests for FjmsService.

Tests use temporary SQLite databases to avoid dependency on the real
fjms_enrichment.db sidecar. Covers all methods, thread-safe mode,
graceful degradation, and edge cases.
"""

import sqlite3
import pytest

from shared.fjms_service import FjmsService, get_fjms_service, get_team_display_name, TEAM_DISPLAY_NAMES


@pytest.fixture
def test_db(tmp_path):
    """Create a minimal fjms_enrichment.db for testing."""
    db_path = str(tmp_path / "test_fjms.db")
    conn = sqlite3.connect(db_path)

    # Create schema (matches export script)
    conn.execute("""
        CREATE TABLE domains (
            AlmaId TEXT NOT NULL,
            Domain TEXT NOT NULL,
            DomainHeb TEXT,
            ParentDomain TEXT,
            ParentDomainHeb TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE joins (
            AlmaId TEXT NOT NULL,
            JoinGroupId INTEGER NOT NULL,
            ScholarName TEXT,
            Comment TEXT,
            JoinType TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE catalog (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            Title TEXT,
            TitleHeb TEXT,
            AuthorText TEXT,
            CopyDate TEXT,
            CopyPlace TEXT,
            TextualFrameHeb TEXT,
            TextualFrameEng TEXT,
            SourceName TEXT,
            SourceNameHeb TEXT,
            NumFolio REAL,
            NumBifolio REAL,
            NumColumn TEXT,
            NumRow TEXT,
            GenizahTitleOrgTitle TEXT,
            GenizahTitleEngTitle TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Insert test domain data
    conn.executemany(
        "INSERT INTO domains VALUES (?, ?, ?, ?, ?)",
        [
            ("990001", "Piyyut", "פיוט", None, None),
            ("990001", "Liturgy", "ליטורגיה", "Piyyut", "פיוט"),
            ("990002", "Letters", "מכתבים", "Documentary", "תעודות"),
            ("990003", "Piyyut", "פיוט", None, None),
        ],
    )

    # Insert test joins data (two manuscripts in same group)
    conn.executemany(
        "INSERT INTO joins VALUES (?, ?, ?, ?, ?)",
        [
            ("990001", 100, "Goitein", "Fragment A", "Physical"),
            ("990004", 100, "Goitein", "Fragment B", "Physical"),
            ("990005", 100, "Goitein", "Fragment C", "Physical"),
            ("990006", 200, "Gil", "Separate group", "Content"),
            # Group 200: overlapping members with group 100 but different metadata
            ("990004", 200, "Gil", "Also in group 200", "Codex Join"),
            ("990001", 200, "Gil", "Also in group 200", None),
            ("990006", 200, "Gil", None, "Scribe Join"),
        ],
    )

    # Insert test catalog data (v3.1.0 schema — includes NumBifolio)
    conn.executemany(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "990001", 100,
                "A Legal Document", "שטר משפטי",
                "Unknown", "1150", "Fustat",
                "טקסט עברי", "Hebrew text",
                "PGPID", "",
                14.0, 7.0, "2", "20",
                "שטר", "Legal Document",
            ),
            (
                "990001", 101,
                "A Legal Document", "שטר משפטי",
                "Unknown", "1150", "Fustat",
                "טקסט עברי נוסף", "Additional Hebrew text",
                "FGP", "",
                14.0, None, "2", "20",
                None, None,
            ),
            (
                "990002", 200,
                "Letter to a Merchant", "מכתב לסוחר",
                "Nahray b. Nissim", "1050", "Alexandria",
                None, None,
                "Inventory", "",
                None, None, None, None,
                None, None,
            ),
        ],
    )

    # Insert meta data
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", "2.0.0"),
            ("created", "2026-02-12T00:00:00Z"),
            ("source", "FIST.db"),
        ],
    )

    # Create bibliography table (Phase 33 export)
    conn.execute("""
        CREATE TABLE bibliography (
            AlmaId TEXT NOT NULL,
            RunningTitle TEXT,
            TitleYear TEXT,
            TitleAcronym TEXT,
            MentionPage TEXT,
            FromPage TEXT,
            ToPage TEXT,
            Volume TEXT,
            MentionType TEXT,
            TranscriptionType TEXT,
            TranslationType TEXT,
            ArticleName TEXT,
            ArticleAuthorEng TEXT,
            ArticleAuthorHeb TEXT,
            CatalogAcronym TEXT
        )
    """)

    # Insert test bibliography data -- Discussion should sort first
    conn.executemany(
        "INSERT INTO bibliography VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("990001", "Goitein Med Soc", "1967", "GMS", "123", "120", "130", "I",
             "Discussion", "", "", "The India Trade", "S.D. Goitein", "גויטיין", ""),
            ("990001", "Gil Palestine", "1983", "GP", "456", "450", "460", "II",
             "Mentioned", "", "", "", "Moshe Gil", "משה גיל", ""),
            ("990001", "Ashtor Prix", "1969", "AP", "78", "", "", "",
             "Transcription", "Full", "", "Price History", "E. Ashtor", "אשתור", ""),
        ],
    )

    # Create catalog_refs table (Phase 33 export)
    conn.execute("""
        CREATE TABLE catalog_refs (
            AlmaId TEXT NOT NULL,
            CatAcronym TEXT,
            CatalogAuthor TEXT,
            CatalogTitle TEXT,
            CatalogEntry TEXT,
            IsSource TEXT
        )
    """)

    # Insert test catalog_refs data
    conn.executemany(
        "INSERT INTO catalog_refs VALUES (?,?,?,?,?,?)",
        [
            ("990001", "GMS", "S.D. Goitein", "A Mediterranean Society", "I, 123", "1"),
            ("990001", "GP", "Moshe Gil", "Palestine During the First Muslim Period", "456", "0"),
        ],
    )

    # Insert additional catalog row for 990001 with a generic source name
    conn.execute(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("990001", 102, "Title 2", "", "", "", "", "", "", "Institution", "", None, None, None, None, None, None),
    )

    # Additional catalog rows for browse tests (Phase 41)
    conn.executemany(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("990007", 700, "Sefer HaMitzvot", "\u05e1\u05e4\u05e8 \u05d4\u05de\u05e6\u05d5\u05d5\u05ea",
             "Maimonides", None, None, "Frame1", None, None, None, None, None, None, None, None, None),
            ("990008", 800, "Mishneh Torah", "\u05de\u05e9\u05e0\u05d4 \u05ea\u05d5\u05e8\u05d4",
             "Maimonides", "1180", None, "Frame2", None, None, None, None, None, None, None, None, None),
            ("990009", 900, "Commentary on Psalms", "\u05e4\u05d9\u05e8\u05d5\u05e9 \u05e2\u05dc \u05ea\u05d4\u05dc\u05d9\u05dd",
             "Saadia Gaon", "930", None, None, "Commentary", None, None, None, None, None, None, None, None),
            # 990010: catalog record but NO domain (used for unclassified test)
            ("990010", 1000, "Unknown Fragment", "\u05e7\u05d8\u05e2 \u05dc\u05d0 \u05de\u05d6\u05d5\u05d4\u05d4",
             None, None, None, None, None, None, None, None, None, None, None, None, None),
        ],
    )

    # Additional domain entries for browse tests
    conn.executemany(
        "INSERT INTO domains VALUES (?, ?, ?, ?, ?)",
        [
            ("990007", "Piyyut", "\u05e4\u05d9\u05d5\u05d8", None, None),
            ("990008", "Letters", "\u05de\u05db\u05ea\u05d1\u05d9\u05dd", "Documentary", "\u05ea\u05e2\u05d5\u05d3\u05d5\u05ea"),
            ("990009", "Bible", "\u05de\u05e7\u05e8\u05d0", None, None),
        ],
    )

    # Create catalog child tables (v3.0.0)

    # catalog_running_titles
    conn.execute("""
        CREATE TABLE catalog_running_titles (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            RunningTitle TEXT, Comment TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_running_titles VALUES (?, ?, ?, ?)", [
        ("990001", 100, "Midrash Lamentations Rabbati", "Petihah 2-7"),
        ("990001", 101, "Lamentations Commentary", None),
    ])

    # catalog_sizes
    conn.execute("""
        CREATE TABLE catalog_sizes (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            SizeX REAL, SizeY REAL, InnerSizeX REAL, InnerSizeY REAL
        )
    """)
    conn.executemany("INSERT INTO catalog_sizes VALUES (?, ?, ?, ?, ?, ?)", [
        ("990001", 100, 165.0, 210.0, None, None),
    ])

    # catalog_fields
    conn.execute("""
        CREATE TABLE catalog_fields (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            FieldCategory TEXT NOT NULL, FieldValue TEXT, FieldValueHeb TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", [
        ("990001", 100, "FragmentMaterial", "Vellum", "קלף"),
        ("990001", 100, "FragmentStatus", "Torn", "קרוע"),
        ("990001", 100, "GenizahLanguages", "Hebrew", "עברית"),
        ("990001", 101, "FragmentMaterial", "Vellum", "קלף"),
    ])

    # catalog_free_desc
    conn.execute("""
        CREATE TABLE catalog_free_desc (
            AlmaId TEXT NOT NULL, SignatureId INTEGER NOT NULL, FreeDesc TEXT,
            SourceName TEXT, SourceNameHeb TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_free_desc VALUES (?, ?, ?, ?, ?)", [
        ("990001", 500, "Parchment fragment, left and right margins visible.", "T-S Cataloging", "\u05e7\u05d8\u05dc\u05d5\u05d2 \u05d8-\u05e9"),
    ])

    # catalog_full_texts (v4.0.0)
    conn.execute("""
        CREATE TABLE catalog_full_texts (
            AlmaId TEXT NOT NULL, SignatureId INTEGER NOT NULL, FullText TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_full_texts VALUES (?, ?, ?)", [
        ("990001", 500, "This is a detailed scholarly description of the fragment, discussing its provenance and significance."),
        ("990001", 501, "Additional catalog description from a different signature version."),
    ])

    # catalog_textual_frames (v4.0.0)
    conn.execute("""
        CREATE TABLE catalog_textual_frames (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            TextualFrameHeb TEXT, TextualFrameEng TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_textual_frames VALUES (?, ?, ?, ?)", [
        ("990001", 100, "במדבר יא:יד-כד", "[Bible]: Numbers 11:14-24"),
        ("990001", 100, "במדבר יב:א-יד", "[Bible]: Numbers 12:1-14"),
        ("990001", 101, "איכה רבה פתיחתא ב", "[Midrash]: Lamentations Rabbah Petihta 2"),
    ])

    # catalog_mentions (v4.0.0)
    conn.execute("""
        CREATE TABLE catalog_mentions (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            MentionType TEXT, Mention TEXT, MentionDesc TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_mentions VALUES (?, ?, ?, ?, ?)", [
        ("990001", 100, "Personalities", "Moses", "Biblical figure"),
        ("990001", 100, "Personalities", "Aaron", "High priest"),
        ("990001", 100, "Places", "Sinai", "Mountain"),
        ("990001", 101, "Dates", "70 CE", "Destruction of the Temple"),
    ])

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(test_db):
    """Create an FjmsService instance with the test database."""
    svc = FjmsService(db_path=test_db)
    yield svc
    svc.close()


# ── Availability tests ────────────────────────────────────────────


def test_service_available_with_valid_db(service):
    """FjmsService reports available with a valid database."""
    assert service.is_available() is True


def test_service_unavailable_with_missing_db():
    """FjmsService reports unavailable when database does not exist."""
    svc = FjmsService(db_path="nonexistent_path_that_does_not_exist.db")
    assert svc.is_available() is False
    svc.close()


# ── Version tests ─────────────────────────────────────────────────


def test_get_version(service):
    """get_version returns the stored version string."""
    assert service.get_version() == "2.0.0"


def test_get_version_unavailable():
    """get_version returns None when service is unavailable."""
    svc = FjmsService(db_path="nonexistent.db")
    assert svc.get_version() is None
    svc.close()


# ── Domain tests ──────────────────────────────────────────────────


def test_get_domains_returns_list(service):
    """get_domains returns correct domain dicts for a known AlmaId."""
    domains = service.get_domains("990001")
    assert len(domains) == 2
    domain_names = {d["domain"] for d in domains}
    assert "Piyyut" in domain_names
    assert "Liturgy" in domain_names


def test_get_domains_has_correct_keys(service):
    """Each domain dict has the expected keys."""
    domains = service.get_domains("990001")
    assert len(domains) > 0
    for d in domains:
        assert "domain" in d
        assert "domain_heb" in d
        assert "parent_domain" in d
        assert "parent_domain_heb" in d


def test_get_domains_not_found(service):
    """get_domains returns empty list for unknown AlmaId."""
    assert service.get_domains("999999") == []


def test_get_domains_hebrew_values(service):
    """get_domains includes Hebrew domain names."""
    domains = service.get_domains("990001")
    piyyut = next(d for d in domains if d["domain"] == "Piyyut")
    assert piyyut["domain_heb"] == "פיוט"


# ── Manuscripts by domain tests ──────────────────────────────────


def test_get_manuscripts_by_domain(service):
    """get_manuscripts_by_domain returns correct set of AlmaIds."""
    manuscripts = service.get_manuscripts_by_domain("Letters")
    assert "990002" in manuscripts


def test_get_manuscripts_by_parent_domain(service):
    """get_manuscripts_by_domain also matches parent domain references."""
    # Piyyut appears as both direct Domain and as ParentDomain
    manuscripts = service.get_manuscripts_by_domain("Piyyut")
    assert "990001" in manuscripts  # Direct domain = Piyyut
    assert "990003" in manuscripts  # Direct domain = Piyyut
    # 990001 also has Liturgy with ParentDomain = Piyyut (already included)


def test_get_manuscripts_by_domain_returns_set(service):
    """get_manuscripts_by_domain returns a set type."""
    result = service.get_manuscripts_by_domain("Piyyut")
    assert isinstance(result, set)


def test_get_manuscripts_by_domain_not_found(service):
    """get_manuscripts_by_domain returns empty set for unknown domain."""
    assert service.get_manuscripts_by_domain("NonexistentDomain") == set()


# ── All domains tests ────────────────────────────────────────────


def test_get_all_domains(service):
    """get_all_domains returns list of domain dicts with counts."""
    all_domains = service.get_all_domains()
    assert len(all_domains) > 0
    # Check structure
    for d in all_domains:
        assert "domain" in d
        assert "domain_heb" in d
        assert "count" in d
        assert isinstance(d["count"], int)


def test_get_all_domains_sorted_by_count(service):
    """get_all_domains returns domains sorted by count descending."""
    all_domains = service.get_all_domains()
    counts = [d["count"] for d in all_domains]
    assert counts == sorted(counts, reverse=True)


def test_get_all_domains_piyyut_count(service):
    """Piyyut domain has correct count of distinct manuscripts."""
    all_domains = service.get_all_domains()
    piyyut = next(d for d in all_domains if d["domain"] == "Piyyut")
    # 990001, 990003, and 990007 all have domain=Piyyut
    assert piyyut["count"] == 3


# ── Join group tests ─────────────────────────────────────────────


def test_get_join_group(service):
    """get_join_group returns other members of the join group."""
    joins = service.get_join_group("990001")
    # Groups 100 and 200 have 990001 -- should return unique partners
    alma_ids = {j["alma_id"] for j in joins}
    assert "990004" in alma_ids
    assert "990005" in alma_ids
    # Should NOT include the queried manuscript
    assert "990001" not in alma_ids


def test_get_join_group_has_correct_keys(service):
    """Each join group member dict has expected aggregated keys."""
    joins = service.get_join_group("990001")
    assert len(joins) > 0
    for j in joins:
        assert "alma_id" in j
        assert "join_group_ids" in j
        assert "scholar_names" in j
        assert "comment" in j
        assert "join_types" in j
        # New format: lists not scalars
        assert isinstance(j["join_group_ids"], list)
        assert isinstance(j["scholar_names"], list)
        assert isinstance(j["join_types"], list)


def test_get_join_group_not_found(service):
    """get_join_group returns empty list for AlmaId with no joins."""
    assert service.get_join_group("999999") == []


def test_get_join_group_excludes_self(service):
    """get_join_group never includes the queried AlmaId in results."""
    joins = service.get_join_group("990004")
    alma_ids = {j["alma_id"] for j in joins}
    assert "990004" not in alma_ids
    # But should include other group members
    assert "990001" in alma_ids
    assert "990005" in alma_ids


# ── Multi-group deduplication tests ──────────────────────────────


def test_get_join_group_deduplicates_across_groups(service):
    """Partners appearing in multiple groups are returned exactly once."""
    # 990006 is in group 200. 990004 and 990001 are also in group 200.
    joins = service.get_join_group("990006")
    alma_ids = [j["alma_id"] for j in joins]
    # Each partner should appear exactly once
    assert len(alma_ids) == len(set(alma_ids)), f"Duplicate alma_ids found: {alma_ids}"
    assert "990004" in alma_ids
    assert "990001" in alma_ids
    assert len(joins) == 2


def test_get_join_group_aggregates_scholars(service):
    """Partners in multiple groups with different scholars aggregate all scholar names."""
    # 990004 is in groups 100 (Goitein) and 200 (Gil).
    # Query for 990004: 990001 appears in both groups with different scholars.
    joins = service.get_join_group("990004")
    join_990001 = next(j for j in joins if j["alma_id"] == "990001")
    # 990001 in group 100 has scholar "Goitein", in group 200 has scholar "Gil"
    assert "Goitein" in join_990001["scholar_names"]
    assert "Gil" in join_990001["scholar_names"]


def test_get_join_group_aggregates_join_types(service):
    """Partners in multiple groups with different join types aggregate all types."""
    # 990001 is in groups 100 and 200.
    # Query for 990001: 990004 is in group 100 (JoinType="Physical") and group 200 (JoinType="Codex Join")
    joins = service.get_join_group("990001")
    join_990004 = next(j for j in joins if j["alma_id"] == "990004")
    assert "Physical" in join_990004["join_types"]
    assert "Codex Join" in join_990004["join_types"]


def test_get_join_group_filters_null_from_aggregation(service):
    """NULL values are not included in aggregated lists."""
    # 990001 is in groups 100 and 200.
    # In group 200, 990001 has JoinType=None.
    # Query for 990004: 990001 in group 100 has JoinType="Physical", in group 200 has JoinType=None
    joins = service.get_join_group("990004")
    join_990001 = next(j for j in joins if j["alma_id"] == "990001")
    # join_types should contain "Physical" but NOT None or empty string
    assert all(jt for jt in join_990001["join_types"]), "NULL/empty values found in join_types"
    assert "Physical" in join_990001["join_types"]


# ── Catalog tests ────────────────────────────────────────────────


def test_get_catalog(service):
    """get_catalog returns dict with expected keys for a known AlmaId."""
    catalog = service.get_catalog("990001")
    assert catalog is not None
    assert catalog["title"] == "A Legal Document"
    assert catalog["title_heb"] == "שטר משפטי"
    assert catalog["author_text"] == "Unknown"
    assert catalog["copy_date"] == "1150"
    assert catalog["copy_place"] == "Fustat"
    assert catalog["textual_frame_heb"] == "טקסט עברי"
    assert catalog["textual_frame_eng"] == "Hebrew text"
    assert catalog["unit_catalog_rec_id"] == 100
    assert catalog["num_folio"] == 14.0
    assert catalog["num_column"] == "2"
    assert catalog["num_row"] == "20"
    assert catalog["genizah_title_org"] == "שטר"
    assert catalog["genizah_title_eng"] == "Legal Document"


def test_get_catalog_not_found(service):
    """get_catalog returns None for unknown AlmaId."""
    assert service.get_catalog("999999") is None


def test_get_catalog_has_all_keys(service):
    """get_catalog result has all expected keys."""
    catalog = service.get_catalog("990001")
    expected_keys = {
        "title", "title_heb", "author_text", "copy_date", "copy_place",
        "textual_frame_heb", "textual_frame_eng",
        "unit_catalog_rec_id", "num_folio", "num_column", "num_row",
        "genizah_title_org", "genizah_title_eng",
    }
    assert set(catalog.keys()) == expected_keys


# ── Thread-safe mode tests ───────────────────────────────────────


def test_thread_safe_mode(test_db):
    """FjmsService with thread_safe=True opens without error."""
    svc = FjmsService(db_path=test_db, thread_safe=True)
    assert svc.is_available() is True
    # Verify queries work in thread-safe mode
    version = svc.get_version()
    assert version == "2.0.0"
    svc.close()


# ── Graceful degradation tests ───────────────────────────────────


def test_graceful_degradation_all_methods():
    """All methods return empty/None when connection is None."""
    svc = FjmsService(db_path="nonexistent_file.db")
    assert svc.is_available() is False
    assert svc.get_domains("123") == []
    assert svc.get_manuscripts_by_domain("test") == set()
    assert svc.get_all_domains() == []
    assert svc.get_join_group("123") == []
    assert svc.get_catalog("123") is None
    assert svc.get_version() is None
    # close() should not raise
    svc.close()


# ── Close tests ──────────────────────────────────────────────────


def test_close_sets_conn_to_none(test_db):
    """close() sets the connection to None."""
    svc = FjmsService(db_path=test_db)
    assert svc.is_available() is True
    svc.close()
    assert svc.is_available() is False


def test_close_idempotent(test_db):
    """Calling close() multiple times does not raise."""
    svc = FjmsService(db_path=test_db)
    svc.close()
    svc.close()  # Should not raise


# ── Bibliography tests ───────────────────────────────────────────


def test_get_bibliography_returns_list(service):
    """get_bibliography returns correctly shaped dicts with Discussion first."""
    bib = service.get_bibliography("990001")
    assert len(bib) == 3
    # Discussion should sort first
    assert bib[0]["mention_type"] == "Discussion"
    assert bib[1]["mention_type"] == "Mentioned"
    assert bib[2]["mention_type"] == "Transcription"
    # Check dict keys
    expected_keys = {
        "running_title", "title_year", "title_acronym", "mention_page",
        "from_page", "to_page", "volume", "mention_type",
        "transcription_type", "translation_type", "article_name",
        "article_author_eng", "article_author_heb", "catalog_acronym",
    }
    for entry in bib:
        assert set(entry.keys()) == expected_keys
    # Verify specific values
    assert bib[0]["running_title"] == "Goitein Med Soc"
    assert bib[0]["title_year"] == "1967"
    assert bib[0]["article_author_eng"] == "S.D. Goitein"


def test_get_bibliography_empty(service):
    """get_bibliography returns [] for non-existent AlmaId."""
    assert service.get_bibliography("999999") == []


def test_get_bibliography_missing_table(tmp_path):
    """get_bibliography returns [] when bibliography table doesn't exist (old sidecar)."""
    db_path = str(tmp_path / "old_sidecar.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('version', '1.0.0')")
    conn.commit()
    conn.close()
    svc = FjmsService(db_path=db_path)
    assert svc.get_bibliography("990001") == []
    svc.close()


# ── Catalog refs tests ──────────────────────────────────────────


def test_get_catalog_refs_returns_list(service):
    """get_catalog_refs returns correctly shaped dicts for a known AlmaId."""
    refs = service.get_catalog_refs("990001")
    assert len(refs) == 2
    expected_keys = {"cat_acronym", "catalog_author", "catalog_title", "catalog_entry", "is_source"}
    for ref in refs:
        assert set(ref.keys()) == expected_keys
    # Check ordering by CatAcronym
    assert refs[0]["cat_acronym"] == "GMS"
    assert refs[1]["cat_acronym"] == "GP"
    # Verify specific values
    assert refs[0]["catalog_author"] == "S.D. Goitein"
    assert refs[0]["catalog_entry"] == "I, 123"
    assert refs[0]["is_source"] == "1"


def test_get_catalog_refs_empty(service):
    """get_catalog_refs returns [] for non-existent AlmaId."""
    assert service.get_catalog_refs("999999") == []


def test_get_catalog_refs_missing_table(tmp_path):
    """get_catalog_refs returns [] when catalog_refs table doesn't exist."""
    db_path = str(tmp_path / "old_sidecar2.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('version', '1.0.0')")
    conn.commit()
    conn.close()
    svc = FjmsService(db_path=db_path)
    assert svc.get_catalog_refs("990001") == []
    svc.close()


# ── Source names tests ──────────────────────────────────────────


def test_get_source_names_filters_generic(service):
    """get_source_names returns only scholarly sources, filtering out generic labels."""
    names = service.get_source_names("990001")
    # 990001 has SourceName='PGPID', 'Institution' (generic), and 'FGP'
    assert "PGPID" in names
    assert "FGP" in names
    assert "Institution" not in names
    assert "Catalogs" not in names


def test_get_source_names_all_generic(service):
    """get_source_names returns [] when only generic source names exist."""
    # 990002 has SourceName='Inventory' which is generic
    names = service.get_source_names("990002")
    assert names == []


def test_get_source_names_empty(service):
    """get_source_names returns [] for non-existent AlmaId."""
    assert service.get_source_names("999999") == []


# ── Catalog source counts tests ───────────────────────────────────


def test_get_catalog_source_counts(service):
    """get_catalog_source_counts returns batch counts excluding generic sources."""
    counts = service.get_catalog_source_counts(["990001", "990002", "990099"])
    # 990001 has SourceName='PGPID', 'FGP', 'Institution' -- Institution is generic
    assert counts["990001"] == 2  # PGPID and FGP only
    # 990002 has only 'Inventory' (generic) -- should not appear
    assert "990002" not in counts
    # 990099 doesn't exist -- should not appear
    assert "990099" not in counts


def test_get_catalog_source_counts_empty(service):
    """get_catalog_source_counts returns {} for empty input list."""
    assert service.get_catalog_source_counts([]) == {}


def test_get_catalog_source_counts_unavailable():
    """get_catalog_source_counts returns {} when service is unavailable."""
    svc = FjmsService(db_path="nonexistent.db")
    assert svc.get_catalog_source_counts(["990001"]) == {}
    svc.close()


# ── Catalog detail tests ─────────────────────────────────────────


def test_get_catalog_detail(service):
    """get_catalog_detail returns structured dict with all child table data."""
    detail = service.get_catalog_detail("990001")

    # Records
    assert len(detail["records"]) > 0
    # Check first record has new v3.0.0 keys
    rec = detail["records"][0]
    assert "unit_catalog_rec_id" in rec
    assert "num_folio" in rec

    # Running titles grouped by UnitCatalogRecId
    rt = detail["running_titles"]
    assert len(rt) > 0
    assert 100 in rt
    assert rt[100][0]["running_title"] == "Midrash Lamentations Rabbati"
    assert rt[100][0]["comment"] == "Petihah 2-7"
    assert 101 in rt
    assert rt[101][0]["running_title"] == "Lamentations Commentary"
    assert rt[101][0]["comment"] is None

    # Sizes
    sizes = detail["sizes"]
    assert 100 in sizes
    assert sizes[100][0]["size_x"] == 165.0
    assert sizes[100][0]["size_y"] == 210.0
    assert sizes[100][0]["inner_size_x"] is None

    # Fields grouped by UnitCatalogRecId then FieldCategory
    fields = detail["fields"]
    assert 100 in fields
    assert "FragmentMaterial" in fields[100]
    assert fields[100]["FragmentMaterial"][0]["value"] == "Vellum"
    assert fields[100]["FragmentMaterial"][0]["value_heb"] == "קלף"
    assert "FragmentStatus" in fields[100]
    assert "GenizahLanguages" in fields[100]
    # rec_id 101 also has fields
    assert 101 in fields
    assert "FragmentMaterial" in fields[101]

    # Free descriptions
    fd = detail["free_descriptions"]
    assert len(fd) == 1
    assert fd[0]["text"] == "Parchment fragment, left and right margins visible."
    assert fd[0]["signature_id"] == 500


def test_catalog_detail_free_desc_has_source(service):
    """get_catalog_detail returns source_name in free_descriptions entries."""
    detail = service.get_catalog_detail("990001")
    fds = detail["free_descriptions"]
    assert len(fds) >= 1
    assert fds[0]["source_name"] == "T-S Cataloging"
    assert fds[0]["source_name_heb"] == "\u05e7\u05d8\u05dc\u05d5\u05d2 \u05d8-\u05e9"


def test_get_catalog_detail_no_data(service):
    """get_catalog_detail returns dict with empty values for non-existent sys_id."""
    detail = service.get_catalog_detail("990099")
    assert detail["records"] == []
    assert detail["running_titles"] == {}
    assert detail["sizes"] == {}
    assert detail["fields"] == {}
    assert detail["free_descriptions"] == []
    assert detail["full_texts"] == []
    assert detail["textual_frames"] == {}
    assert detail["mentions"] == {}


def test_get_catalog_detail_unavailable():
    """get_catalog_detail returns dict with empty values when service is unavailable."""
    svc = FjmsService(db_path="nonexistent.db")
    detail = svc.get_catalog_detail("990001")
    assert detail["records"] == []
    assert detail["running_titles"] == {}
    assert detail["sizes"] == {}
    assert detail["fields"] == {}
    assert detail["free_descriptions"] == []
    assert detail["full_texts"] == []
    assert detail["textual_frames"] == {}
    assert detail["mentions"] == {}
    svc.close()


# ── Full texts tests ──────────────────────────────────────────────


def test_get_catalog_detail_full_texts(service):
    """get_catalog_detail returns full_texts list with expected entries."""
    detail = service.get_catalog_detail("990001")
    ft = detail["full_texts"]
    assert len(ft) == 2
    assert ft[0]["signature_id"] == 500
    assert "scholarly description" in ft[0]["text"].lower()
    assert ft[1]["signature_id"] == 501


def test_get_catalog_detail_full_texts_empty(service):
    """full_texts is empty list for sys_id with no full text data."""
    detail = service.get_catalog_detail("990002")
    assert detail["full_texts"] == []


# ── Textual frames tests ────────────────────────────────────────


def test_get_catalog_detail_textual_frames(service):
    """get_catalog_detail returns textual_frames grouped by UnitCatalogRecId."""
    detail = service.get_catalog_detail("990001")
    tf = detail["textual_frames"]
    assert len(tf) > 0
    # rec_id 100 has 2 entries
    assert 100 in tf
    assert len(tf[100]) == 2
    assert tf[100][0]["eng"] == "[Bible]: Numbers 11:14-24"
    assert tf[100][1]["heb"] == "במדבר יב:א-יד"
    # rec_id 101 has 1 entry
    assert 101 in tf
    assert len(tf[101]) == 1
    assert "Lamentations" in tf[101][0]["eng"]


def test_get_catalog_detail_textual_frames_empty(service):
    """textual_frames is empty dict for sys_id with no frame data."""
    detail = service.get_catalog_detail("990002")
    assert detail["textual_frames"] == {}


# ── Mentions tests ───────────────────────────────────────────────


def test_get_catalog_detail_mentions(service):
    """get_catalog_detail returns mentions grouped by UnitCatalogRecId."""
    detail = service.get_catalog_detail("990001")
    mn = detail["mentions"]
    assert len(mn) > 0
    # rec_id 100 has 3 mentions (2 Personalities + 1 Places)
    assert 100 in mn
    assert len(mn[100]) == 3
    types = {m["mention_type"] for m in mn[100]}
    assert "Personalities" in types
    assert "Places" in types
    names = {m["mention"] for m in mn[100]}
    assert "Moses" in names
    assert "Sinai" in names
    # rec_id 101 has 1 mention (Dates)
    assert 101 in mn
    assert mn[101][0]["mention_type"] == "Dates"
    assert mn[101][0]["mention"] == "70 CE"


def test_get_catalog_detail_mentions_empty(service):
    """mentions is empty dict for sys_id with no mention data."""
    detail = service.get_catalog_detail("990002")
    assert detail["mentions"] == {}


# ── Backward compat with old sidecar tests ──────────────────────


def test_get_catalog_detail_old_sidecar_no_crash(tmp_path):
    """get_catalog_detail works gracefully when new tables don't exist (old sidecar)."""
    db_path = str(tmp_path / "old_v3.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('version', '3.1.0')")
    # Minimal catalog table so records query doesn't crash
    conn.execute("""
        CREATE TABLE catalog (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            Title TEXT, TitleHeb TEXT, AuthorText TEXT, CopyDate TEXT,
            CopyPlace TEXT, TextualFrameHeb TEXT, TextualFrameEng TEXT,
            SourceName TEXT, SourceNameHeb TEXT, NumFolio REAL,
            NumBifolio REAL, NumColumn TEXT, NumRow TEXT,
            GenizahTitleOrgTitle TEXT, GenizahTitleEngTitle TEXT
        )
    """)
    conn.commit()
    conn.close()

    svc = FjmsService(db_path=db_path)
    detail = svc.get_catalog_detail("990001")
    # New keys should exist with empty defaults
    assert detail["full_texts"] == []
    assert detail["textual_frames"] == {}
    assert detail["mentions"] == {}
    # Old keys also empty
    assert detail["records"] == []
    assert detail["running_titles"] == {}
    svc.close()


# ── Team display name tests ─────────────────────────────────────


def test_get_team_display_name_known():
    """get_team_display_name returns enriched name for known teams."""
    assert "Aharon Maman" in get_team_display_name("Linguistics")
    assert "FGP" in get_team_display_name("Linguistics")
    assert "(head)" in get_team_display_name("Linguistics")


def test_get_team_display_name_unknown():
    """get_team_display_name falls back to original name for unknown teams."""
    assert get_team_display_name("Some Unknown Team") == "Some Unknown Team"


def test_get_team_display_name_empty():
    """get_team_display_name handles empty/None input."""
    assert get_team_display_name("") == ""
    assert get_team_display_name(None) == ""


def test_team_display_names_has_key_teams():
    """TEAM_DISPLAY_NAMES has entries for major research teams."""
    assert "Talmudic Literature" in TEAM_DISPLAY_NAMES
    assert "Magic" in TEAM_DISPLAY_NAMES
    assert "Responsa" in TEAM_DISPLAY_NAMES
    assert len(TEAM_DISPLAY_NAMES) >= 20


# ── Hierarchy cache tests ────────────────────────────────────────


def test_hierarchy_cache_returns_same_object(service):
    """Second call to get_domain_hierarchy returns cached result (same object)."""
    result1 = service.get_domain_hierarchy()
    assert result1  # Non-empty with test data
    result2 = service.get_domain_hierarchy()
    # Must be the exact same object (cached), not just equal
    assert result1 is result2
    # Cache should be populated
    assert service._hierarchy_cache is not None


def test_hierarchy_cache_not_set_when_no_connection():
    """get_domain_hierarchy with no connection returns {} and does not cache."""
    svc = FjmsService(db_path="nonexistent_file_for_cache_test.db")
    result = svc.get_domain_hierarchy()
    assert result == {}
    assert svc._hierarchy_cache is None  # Should NOT cache empty result
    svc.close()


# ── Browse service tests (Phase 41) ─────────────────────────────


def test_get_browse_authors(test_db):
    """get_browse_authors returns all unique authors with counts, sorted by count desc."""
    svc = FjmsService(db_path=test_db)
    authors = svc.get_browse_authors()

    # Should have: "Unknown" (990001), "Nahray b. Nissim" (990002),
    # "Maimonides" (990007, 990008), "Saadia Gaon" (990009)
    # Note: 990010 has no AuthorText, should be excluded
    author_names = {a["eng_desc"] for a in authors}
    assert "Maimonides" in author_names
    assert "Saadia Gaon" in author_names
    assert "Unknown" in author_names
    assert "Nahray b. Nissim" in author_names
    assert len(authors) == 4

    # Maimonides has 2 manuscripts (990007, 990008), should have count=2
    maim = next(a for a in authors if a["eng_desc"] == "Maimonides")
    assert maim["count"] == 2

    # Sorted by count descending
    counts = [a["count"] for a in authors]
    assert counts == sorted(counts, reverse=True)

    svc.close()


def test_get_browse_authors_filtered_by_domain(test_db):
    """get_browse_authors with domain filter only returns authors in that domain."""
    svc = FjmsService(db_path=test_db)

    # Piyyut domain: 990001 (Unknown), 990007 (Maimonides)
    piyyut_authors = svc.get_browse_authors(domain="Piyyut")
    piyyut_names = {a["eng_desc"] for a in piyyut_authors}
    assert "Unknown" in piyyut_names
    assert "Maimonides" in piyyut_names
    assert "Nahray b. Nissim" not in piyyut_names  # in Documentary, not Piyyut

    # Documentary domain: 990002 (Nahray), 990008 (Maimonides via ParentDomain)
    doc_authors = svc.get_browse_authors(domain="Documentary")
    doc_names = {a["eng_desc"] for a in doc_authors}
    assert "Nahray b. Nissim" in doc_names
    assert "Maimonides" in doc_names  # 990008 has ParentDomain=Documentary

    svc.close()


def test_get_browse_works(test_db):
    """get_browse_works returns all unique title pairs with counts, sorted by count desc."""
    svc = FjmsService(db_path=test_db)
    works = svc.get_browse_works()

    # Should include titles from catalog records (excluding totally empty ones)
    titles = {w["org_title"] for w in works}
    assert "Sefer HaMitzvot" in titles
    assert "Mishneh Torah" in titles
    assert "Commentary on Psalms" in titles
    assert "A Legal Document" in titles
    assert "Letter to a Merchant" in titles

    # Title 2 from 990001 generic record (has Title but empty TitleHeb) -- should be included
    assert "Title 2" in titles

    # Verify empty titles excluded (990010 has None Title and None TitleHeb)
    # The "Unknown Fragment" record has a title so it IS included
    assert "Unknown Fragment" in titles

    # Sorted by count descending
    counts = [w["count"] for w in works]
    assert counts == sorted(counts, reverse=True)

    svc.close()


def test_get_browse_works_filtered(test_db):
    """get_browse_works with author or domain filter narrows results."""
    svc = FjmsService(db_path=test_db)

    # Filter by author: Maimonides
    maim_works = svc.get_browse_works(author="Maimonides")
    maim_titles = {w["org_title"] for w in maim_works}
    assert "Sefer HaMitzvot" in maim_titles
    assert "Mishneh Torah" in maim_titles
    assert "A Legal Document" not in maim_titles  # by "Unknown"
    assert len(maim_works) == 2

    # Filter by domain: Piyyut (990001 and 990007)
    piyyut_works = svc.get_browse_works(domain="Piyyut")
    piyyut_titles = {w["org_title"] for w in piyyut_works}
    assert "A Legal Document" in piyyut_titles  # 990001 in Piyyut
    assert "Sefer HaMitzvot" in piyyut_titles  # 990007 in Piyyut
    assert "Letter to a Merchant" not in piyyut_titles  # 990002 in Documentary

    svc.close()


def test_get_browse_results_no_filter(test_db):
    """get_browse_results with no filters returns all manuscripts with catalog data."""
    svc = FjmsService(db_path=test_db)
    res = svc.get_browse_results()

    assert res["total"] > 0
    # Should return all AlmaIds that have catalog entries
    sys_ids = {r["sys_id"] for r in res["results"]}
    assert "990001" in sys_ids
    assert "990002" in sys_ids
    assert "990007" in sys_ids
    assert "990008" in sys_ids
    assert "990009" in sys_ids
    assert "990010" in sys_ids

    # Check result structure
    first = res["results"][0]
    assert "sys_id" in first
    assert "title" in first
    assert "title_heb" in first
    assert "author" in first
    assert "copy_date" in first
    assert "textual_frame_heb" in first
    assert "textual_frame_eng" in first
    assert "domains" in first
    assert "domains_heb" in first
    assert isinstance(first["domains"], list)

    svc.close()


def test_get_browse_results_domain_filter(test_db):
    """get_browse_results with domain filter returns only matching manuscripts."""
    svc = FjmsService(db_path=test_db)

    # Piyyut: 990001, 990003 (domain only), 990007
    # But 990003 has no catalog -> still returned because catalog JOIN finds entries
    res = svc.get_browse_results(domain="Piyyut")
    sys_ids = {r["sys_id"] for r in res["results"]}
    assert "990001" in sys_ids
    assert "990007" in sys_ids
    assert "990002" not in sys_ids  # in Documentary, not Piyyut

    svc.close()


def test_get_browse_results_combined_filter(test_db):
    """get_browse_results with combined domain+author filters returns intersection."""
    svc = FjmsService(db_path=test_db)

    # Piyyut + Maimonides: only 990007 (Maimonides in Piyyut)
    res = svc.get_browse_results(domain="Piyyut", author="Maimonides")
    assert res["total"] == 1
    assert res["results"][0]["sys_id"] == "990007"
    assert res["results"][0]["author"] == "Maimonides"

    svc.close()


def test_get_browse_results_pagination(test_db):
    """get_browse_results pagination returns correct slices."""
    svc = FjmsService(db_path=test_db)

    # Get all results
    all_res = svc.get_browse_results(limit=100)
    total = all_res["total"]
    assert total > 1  # Need at least 2 to test pagination

    # First page
    page1 = svc.get_browse_results(limit=1, offset=0)
    assert len(page1["results"]) == 1
    assert page1["total"] == total

    # Second page
    page2 = svc.get_browse_results(limit=1, offset=1)
    assert len(page2["results"]) == 1
    assert page2["total"] == total

    # Different results on different pages
    assert page1["results"][0]["sys_id"] != page2["results"][0]["sys_id"]

    svc.close()


def test_get_unclassified_count(test_db):
    """get_unclassified_count returns count of catalog AlmaIds not in domains table."""
    svc = FjmsService(db_path=test_db)
    count = svc.get_unclassified_count()

    # 990010 has catalog but no domain entry -> unclassified
    # 990001, 990002, 990007, 990008, 990009 all have domains
    assert count >= 1
    # 990010 definitely unclassified
    assert count > 0

    svc.close()


def test_browse_authors_cache(test_db):
    """get_browse_authors caches unfiltered result and returns same object on second call."""
    svc = FjmsService(db_path=test_db)

    result1 = svc.get_browse_authors()
    assert result1  # Non-empty
    assert svc._authors_cache is not None

    result2 = svc.get_browse_authors()
    # Same object (cached), not just equal
    assert result1 is result2

    svc.close()


def test_browse_works_cache(test_db):
    """get_browse_works caches unfiltered result and returns same object on second call."""
    svc = FjmsService(db_path=test_db)

    result1 = svc.get_browse_works()
    assert result1  # Non-empty
    assert svc._works_cache is not None

    result2 = svc.get_browse_works()
    assert result1 is result2

    svc.close()


def test_browse_graceful_degradation():
    """All browse methods return empty results when connection is None."""
    svc = FjmsService(db_path="nonexistent_browse_test.db")
    assert svc.get_browse_authors() == []
    assert svc.get_browse_works() == []
    assert svc.get_browse_results() == {"results": [], "total": 0}
    assert svc.get_unclassified_count() == 0
    svc.close()


# ── Filter sys_ids tests ─────────────────────────────────────────


@pytest.fixture
def filter_db(tmp_path):
    """Create a test database with v5+ schema for filter_sys_ids tests.

    Schema includes genizah_persons, genizah_titles, catalog with Author/GenizahTitleId
    columns, catalog_fields with FragmentMaterial entries, and domains.
    """
    db_path = str(tmp_path / "filter_test.db")
    conn = sqlite3.connect(db_path)

    # --- meta ---
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('version', '5.0.0')")

    # --- genizah_persons ---
    conn.execute("""
        CREATE TABLE genizah_persons (
            GenizahPersonId INTEGER PRIMARY KEY,
            OrgName TEXT, EngName TEXT, HebName TEXT
        )
    """)
    conn.executemany("INSERT INTO genizah_persons VALUES (?, ?, ?, ?)", [
        (10, "Maimonides", "Maimonides", "רמב\"ם"),
        (20, "Saadia Gaon", "Saadia Gaon", "רב סעדיה גאון"),
    ])

    # --- genizah_titles ---
    conn.execute("""
        CREATE TABLE genizah_titles (
            GenizahTitleId INTEGER PRIMARY KEY,
            OrgTitle TEXT, EngTitle TEXT, HebTitle TEXT,
            AuthorId INTEGER
        )
    """)
    conn.executemany("INSERT INTO genizah_titles VALUES (?, ?, ?, ?, ?)", [
        (100, "Mishneh Torah", "Mishneh Torah", "משנה תורה", 10),
        (200, "Tafsir", "Tafsir Psalms", "תפסיר תהלים", 20),
        (300, "Guide", "Guide for the Perplexed", "מורה נבוכים", 10),
    ])

    # --- catalog (v5+ with Author and GenizahTitleId columns) ---
    conn.execute("""
        CREATE TABLE catalog (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            Title TEXT, TitleHeb TEXT, AuthorText TEXT,
            CopyDate TEXT, CopyPlace TEXT,
            TextualFrameHeb TEXT, TextualFrameEng TEXT,
            SourceName TEXT, SourceNameHeb TEXT,
            NumFolio REAL, NumBifolio REAL,
            NumColumn TEXT, NumRow TEXT,
            GenizahTitleOrgTitle TEXT, GenizahTitleEngTitle TEXT,
            Author INTEGER, GenizahTitleId INTEGER
        )
    """)
    # 8 manuscripts:
    # SYS001: Halakha domain, author=Maimonides (via title FK 100), date 1200, Vellum
    # SYS002: Halakha domain, author=Maimonides (via direct Author FK 10), date 1250, Printed
    # SYS003: Bible domain, author=Saadia (via title FK 200), date 950, Paper
    # SYS004: Liturgy domain, no author, no date, Vellum
    # SYS005: Halakha domain, author=Maimonides (via title FK 300), date 1300, Printed
    # SYS006: No domain, no author, date 1100, Paper
    # SYS007: Bible domain, no author, date 1050, Vellum
    # SYS008: Halakha+Bible domain (dual), no author, date 1150, Vellum
    cols = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    conn.executemany(f"INSERT INTO catalog VALUES {cols}", [
        ("SYS001", 1, "Mishneh Torah", "משנה תורה", "Maimonides",
         "1200", "Cairo", None, None, None, None, None, None, None, None,
         None, None, 10, 100),
        ("SYS002", 2, "Mishneh Torah Copy", "העתק משנה תורה", "Maimonides",
         "1250", "Fustat", None, None, None, None, None, None, None, None,
         None, None, 10, None),
        ("SYS003", 3, "Tafsir Psalms", "תפסיר תהלים", "Saadia Gaon",
         "950", "Baghdad", None, None, None, None, None, None, None, None,
         None, None, 20, 200),
        ("SYS004", 4, "Piyyut Fragment", "קטע פיוט", None,
         None, None, None, None, None, None, None, None, None, None,
         None, None, None, None),
        ("SYS005", 5, "Guide Perplexed", "מורה נבוכים", "Maimonides",
         "1300", "Spain", None, None, None, None, None, None, None, None,
         None, None, 10, 300),
        ("SYS006", 6, "Unknown Fragment", "קטע", None,
         "1100", None, None, None, None, None, None, None, None, None,
         None, None, None, None),
        ("SYS007", 7, "Psalms Commentary", "פירוש תהלים", None,
         "1050", None, None, None, None, None, None, None, None, None,
         None, None, None, None),
        ("SYS008", 8, "Halakhic Bible", "הלכה ומקרא", None,
         "1150", None, None, None, None, None, None, None, None, None,
         None, None, None, None),
    ])

    # --- domains ---
    conn.execute("""
        CREATE TABLE domains (
            AlmaId TEXT NOT NULL, Domain TEXT NOT NULL,
            DomainHeb TEXT, ParentDomain TEXT, ParentDomainHeb TEXT
        )
    """)
    conn.executemany("INSERT INTO domains VALUES (?, ?, ?, ?, ?)", [
        ("SYS001", "Halakha", "הלכה", "Rabbinic", "רבני"),
        ("SYS002", "Halakha", "הלכה", "Rabbinic", "רבני"),
        ("SYS003", "Bible", "מקרא", None, None),
        ("SYS004", "Liturgy", "ליטורגיה", "Piyyut", "פיוט"),
        ("SYS005", "Halakha", "הלכה", "Rabbinic", "רבני"),
        ("SYS007", "Bible", "מקרא", None, None),
        # SYS008 has TWO domains
        ("SYS008", "Halakha", "הלכה", "Rabbinic", "רבני"),
        ("SYS008", "Bible", "מקרא", None, None),
    ])

    # --- catalog_fields (for material filters) ---
    conn.execute("""
        CREATE TABLE catalog_fields (
            AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,
            FieldCategory TEXT NOT NULL, FieldValue TEXT, FieldValueHeb TEXT
        )
    """)
    conn.executemany("INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", [
        ("SYS001", 1, "FragmentMaterial", "Vellum", "קלף"),
        ("SYS002", 2, "FragmentMaterial", "Printed", "דפוס"),
        ("SYS003", 3, "FragmentMaterial", "Paper", "נייר"),
        ("SYS004", 4, "FragmentMaterial", "Vellum", "קלף"),
        ("SYS005", 5, "FragmentMaterial", "Printed", "דפוס"),
        ("SYS007", 7, "FragmentMaterial", "Vellum", "קלף"),
        ("SYS008", 8, "FragmentMaterial", "Vellum", "קלף"),
    ])

    # --- catalog_fts (FTS5 virtual table for text filters) ---
    conn.execute("""
        CREATE VIRTUAL TABLE catalog_fts USING fts5(
            Title, TitleHeb, TextualFrameHeb, TextualFrameEng,
            RunningTitle, FreeDescription, FullText, DetailedFrames,
            content='catalog', content_rowid='rowid'
        )
    """)
    # Populate FTS5 from catalog
    conn.execute("""
        INSERT INTO catalog_fts(rowid, Title, TitleHeb, TextualFrameHeb, TextualFrameEng,
            RunningTitle, FreeDescription, FullText, DetailedFrames)
        SELECT rowid, Title, TitleHeb, TextualFrameHeb, TextualFrameEng,
            COALESCE(Title, ''), COALESCE(TitleHeb, ''), '', ''
        FROM catalog
    """)

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def filter_svc(filter_db):
    """Create FjmsService for filter tests."""
    svc = FjmsService(db_path=filter_db)
    yield svc
    svc.close()


def test_get_filter_sys_ids_no_filters_returns_none(filter_svc):
    """With no filters active, returns None (meaning 'no restriction')."""
    result = filter_svc.get_filter_sys_ids()
    assert result is None


def test_get_filter_sys_ids_domain_filter(filter_svc):
    """domain='Halakha' returns only sys_ids classified under Halakha."""
    result = filter_svc.get_filter_sys_ids(domain="Halakha")
    assert isinstance(result, set)
    # SYS001, SYS002, SYS005 have Halakha domain; SYS008 has Halakha+Bible
    assert result == {"SYS001", "SYS002", "SYS005", "SYS008"}


def test_get_filter_sys_ids_domain_filter_parent(filter_svc):
    """domain='Rabbinic' (parent domain) matches children classified under it."""
    result = filter_svc.get_filter_sys_ids(domain="Rabbinic")
    assert isinstance(result, set)
    # Halakha has ParentDomain='Rabbinic' -> SYS001, SYS002, SYS005, SYS008
    assert result == {"SYS001", "SYS002", "SYS005", "SYS008"}


def test_get_filter_sys_ids_author_filter_person_id(filter_svc):
    """author=person_id returns sys_ids where catalog Author FK or title Author FK matches."""
    result = filter_svc.get_filter_sys_ids(author="10")  # Maimonides person_id
    assert isinstance(result, set)
    # SYS001 (title FK 100, AuthorId=10), SYS002 (direct Author=10), SYS005 (title FK 300, AuthorId=10)
    assert result == {"SYS001", "SYS002", "SYS005"}


def test_get_filter_sys_ids_author_filter_legacy_string(filter_svc):
    """author=string (legacy) matches AuthorText."""
    result = filter_svc.get_filter_sys_ids(author="Saadia Gaon")
    assert isinstance(result, set)
    assert result == {"SYS003"}


def test_get_filter_sys_ids_work_filter(filter_svc):
    """work=title_id returns sys_ids where GenizahTitleId matches."""
    result = filter_svc.get_filter_sys_ids(work="100")  # Mishneh Torah
    assert isinstance(result, set)
    assert result == {"SYS001"}


def test_get_filter_sys_ids_date_range(filter_svc):
    """date_from/date_to returns sys_ids with CopyDate in range."""
    result = filter_svc.get_filter_sys_ids(date_from=1100, date_to=1200)
    assert isinstance(result, set)
    # SYS001 (1200), SYS006 (1100), SYS008 (1150) are in range
    assert result == {"SYS001", "SYS006", "SYS008"}


def test_get_filter_sys_ids_date_from_only(filter_svc):
    """date_from only returns sys_ids with CopyDate >= value."""
    result = filter_svc.get_filter_sys_ids(date_from=1200)
    assert isinstance(result, set)
    # SYS001 (1200), SYS002 (1250), SYS005 (1300)
    assert result == {"SYS001", "SYS002", "SYS005"}


def test_get_filter_sys_ids_date_to_only(filter_svc):
    """date_to only returns sys_ids with CopyDate <= value."""
    result = filter_svc.get_filter_sys_ids(date_to=1050)
    assert isinstance(result, set)
    # SYS003 (950), SYS007 (1050)
    assert result == {"SYS003", "SYS007"}


def test_get_filter_sys_ids_date_include_undated(filter_svc):
    """include_undated=True also includes records with no date."""
    result = filter_svc.get_filter_sys_ids(date_from=1200, date_to=1300, include_undated=True)
    assert isinstance(result, set)
    # SYS001 (1200), SYS002 (1250), SYS005 (1300) + SYS004 (no date)
    assert result == {"SYS001", "SYS002", "SYS004", "SYS005"}


def test_get_filter_sys_ids_material_include(filter_svc):
    """material_include=['Printed'] returns only sys_ids with matching material."""
    result = filter_svc.get_filter_sys_ids(material_include=["Printed"])
    assert isinstance(result, set)
    assert result == {"SYS002", "SYS005"}


def test_get_filter_sys_ids_material_exclude(filter_svc):
    """material_exclude=['Printed'] returns sys_ids without Printed material."""
    result = filter_svc.get_filter_sys_ids(material_exclude=["Printed"])
    assert isinstance(result, set)
    # All catalog entries minus SYS002 and SYS005 (printed)
    # SYS006 has no material entry but has a catalog record -> included (NOT excluded)
    assert "SYS002" not in result
    assert "SYS005" not in result
    assert "SYS001" in result
    assert "SYS003" in result


def test_get_filter_sys_ids_combined_filters(filter_svc):
    """Combining filters uses intersection (AND logic)."""
    # Halakha domain + Maimonides author + date >= 1200
    result = filter_svc.get_filter_sys_ids(domain="Halakha", author="10", date_from=1200)
    assert isinstance(result, set)
    # SYS001 (Halakha, Maimonides, 1200), SYS002 (Halakha, Maimonides, 1250),
    # SYS005 (Halakha, Maimonides, 1300)
    assert result == {"SYS001", "SYS002", "SYS005"}


def test_get_filter_sys_ids_combined_domain_material_exclude(filter_svc):
    """Halakha domain + exclude Printed = only non-printed Halakha manuscripts."""
    result = filter_svc.get_filter_sys_ids(domain="Halakha", material_exclude=["Printed"])
    assert isinstance(result, set)
    # SYS001 (Halakha, Vellum), SYS008 (Halakha+Bible, Vellum)
    # SYS002 and SYS005 are Halakha but Printed -> excluded
    assert result == {"SYS001", "SYS008"}


def test_get_filter_sys_ids_no_match_returns_empty_set(filter_svc):
    """When filters are active but match nothing, returns empty set (not None)."""
    result = filter_svc.get_filter_sys_ids(domain="NonexistentDomain")
    assert isinstance(result, set)
    assert result == set()


def test_get_filter_sys_ids_returns_set_type(filter_svc):
    """Result is a set for O(1) membership testing."""
    result = filter_svc.get_filter_sys_ids(domain="Bible")
    assert isinstance(result, set)


def test_get_filter_sys_ids_graceful_degradation():
    """Returns None when connection is unavailable (no crash)."""
    svc = FjmsService(db_path="nonexistent_filter_test.db")
    result = svc.get_filter_sys_ids(domain="Halakha")
    assert result is None
    svc.close()


# ── Multi-select filter tests ──────────────────────────────────


def test_get_filter_sys_ids_multi_domain_include(filter_svc):
    """Multiple domains include returns union of matching sys_ids."""
    result = filter_svc.get_filter_sys_ids(domains=["Halakha", "Bible"])
    assert result is not None
    # Halakha: SYS001, SYS002, SYS005, SYS008; Bible: SYS003, SYS007, SYS008
    assert {"SYS001", "SYS002", "SYS003", "SYS005", "SYS007", "SYS008"} == result


def test_get_filter_sys_ids_multi_domain_exclude(filter_svc):
    """Domains exclude removes matching manuscripts."""
    result = filter_svc.get_filter_sys_ids(domains_exclude=["Halakha"])
    assert result is not None
    # Exclude Halakha (SYS001, SYS002, SYS005, SYS008) -> SYS003, SYS004, SYS006, SYS007
    assert "SYS001" not in result
    assert "SYS002" not in result
    assert "SYS003" in result
    assert "SYS006" in result


def test_get_filter_sys_ids_multi_author_include(filter_svc):
    """Multiple authors include returns union."""
    result = filter_svc.get_filter_sys_ids(authors=["10", "20"])
    assert result is not None
    # Maimonides (10): SYS001, SYS002, SYS005; Saadia (20): SYS003
    assert {"SYS001", "SYS002", "SYS003", "SYS005"} == result


def test_get_filter_sys_ids_multi_author_exclude(filter_svc):
    """Authors exclude removes matching manuscripts."""
    result = filter_svc.get_filter_sys_ids(authors_exclude=["10"])
    assert result is not None
    # Exclude Maimonides -> should not have SYS001, SYS002, SYS005
    assert "SYS001" not in result
    assert "SYS002" not in result
    assert "SYS005" not in result
    assert "SYS003" in result  # Saadia


def test_get_filter_sys_ids_multi_work_include(filter_svc):
    """Multiple works include returns union."""
    result = filter_svc.get_filter_sys_ids(works=["100", "200"])
    assert result is not None
    # Mishneh Torah (100): SYS001; Tafsir (200): SYS003
    assert {"SYS001", "SYS003"} == result


def test_get_filter_sys_ids_multi_work_exclude(filter_svc):
    """Works exclude removes matching manuscripts."""
    result = filter_svc.get_filter_sys_ids(works_exclude=["100"])
    assert result is not None
    assert "SYS001" not in result
    assert "SYS003" in result


def test_get_filter_sys_ids_legacy_domain_compat(filter_svc):
    """Legacy single domain param auto-converts to domains list."""
    single = filter_svc.get_filter_sys_ids(domain="Halakha")
    multi = filter_svc.get_filter_sys_ids(domains=["Halakha"])
    assert single == multi


def test_get_filter_sys_ids_domain_include_exclude_combined(filter_svc):
    """Include + exclude domains together intersect correctly."""
    # Include Rabbinic parent (gets SYS001, SYS002, SYS005, SYS008)
    # Exclude Bible (removes SYS003, SYS007, SYS008)
    result = filter_svc.get_filter_sys_ids(
        domains=["Rabbinic"], domains_exclude=["Bible"]
    )
    assert result is not None
    assert "SYS001" in result
    assert "SYS008" not in result  # Has both Halakha and Bible domains


def test_get_filter_sys_ids_text_all(filter_svc):
    """text_all requires ALL terms to match."""
    result = filter_svc.get_filter_sys_ids(text_all=["Mishneh", "Torah"])
    assert result is not None
    assert "SYS001" in result  # "Mishneh Torah"
    assert "SYS003" not in result  # "Tafsir Psalms"


def test_get_filter_sys_ids_text_any(filter_svc):
    """text_any requires ANY term to match."""
    result = filter_svc.get_filter_sys_ids(text_any=["Psalms", "Guide"])
    assert result is not None
    assert "SYS003" in result  # "Tafsir Psalms"
    assert "SYS005" in result  # "Guide Perplexed"
    assert "SYS007" in result  # "Psalms Commentary"


def test_get_filter_sys_ids_text_not(filter_svc):
    """text_not excludes manuscripts matching term."""
    all_halakha = filter_svc.get_filter_sys_ids(domains=["Halakha"])
    result = filter_svc.get_filter_sys_ids(domains=["Halakha"], text_not=["Guide"])
    assert result is not None
    assert "SYS005" not in result  # "Guide Perplexed" excluded
    assert "SYS001" in result


def test_get_filter_sys_ids_combined_multi_and_date(filter_svc):
    """Multi-domain + date range combined filter."""
    result = filter_svc.get_filter_sys_ids(
        domains=["Halakha", "Bible"], date_from=1100, date_to=1250
    )
    assert result is not None
    # Halakha+Bible in 1100-1250: SYS001 (1200), SYS008 (1150)
    assert "SYS001" in result
    assert "SYS008" in result
    assert "SYS005" not in result  # 1300
    assert "SYS003" not in result  # 950


# ── Web shim import test ─────────────────────────────────────────


def test_web_shim_import():
    """web.fjms_service shim re-exports FjmsService and get_fjms_service."""
    from web.fjms_service import FjmsService as WebFjmsService
    from web.fjms_service import get_fjms_service as web_get_fjms_service
    assert WebFjmsService is FjmsService
    assert web_get_fjms_service is get_fjms_service
