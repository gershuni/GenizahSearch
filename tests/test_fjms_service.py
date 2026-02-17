# -*- coding: utf-8 -*-
"""
Unit tests for FjmsService.

Tests use temporary SQLite databases to avoid dependency on the real
fjms_enrichment.db sidecar. Covers all methods, thread-safe mode,
graceful degradation, and edge cases.
"""

import sqlite3
import pytest

from shared.fjms_service import FjmsService, get_fjms_service


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
            Title TEXT,
            TitleHeb TEXT,
            AuthorText TEXT,
            CopyDate TEXT,
            CopyPlace TEXT,
            DescriptionEng TEXT,
            DescriptionHeb TEXT,
            TextualFrameHeb TEXT,
            TextualFrameEng TEXT
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

    # Insert test catalog data
    conn.executemany(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "990001",
                "A Legal Document",
                "שטר משפטי",
                "Unknown",
                "1150",
                "Fustat",
                "A legal document from the Genizah",
                "שטר משפטי מן הגניזה",
                "טקסט עברי",
                "Hebrew text",
            ),
            (
                "990002",
                "Letter to a Merchant",
                "מכתב לסוחר",
                "Nahray b. Nissim",
                "1050",
                "Alexandria",
                "A merchant letter",
                "מכתב סוחר",
                None,
                None,
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

    # Add SourceName columns to catalog table for get_source_names test
    conn.execute("ALTER TABLE catalog ADD COLUMN SourceName TEXT")
    conn.execute("ALTER TABLE catalog ADD COLUMN SourceNameHeb TEXT")
    # Update existing rows with source names
    conn.execute("UPDATE catalog SET SourceName = 'PGPID', SourceNameHeb = '' WHERE AlmaId = '990001'")
    conn.execute("UPDATE catalog SET SourceName = 'Catalogs', SourceNameHeb = '' WHERE AlmaId = '990002'")
    # Insert additional row for 990001 with a generic source name
    conn.execute(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("990001", "Title 2", "", "", "", "", "", "", "", "", "Institution", ""),
    )
    # Insert additional row for 990001 with another scholarly source
    conn.execute(
        "INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("990001", "Title 3", "", "", "", "", "", "", "", "", "FGP", ""),
    )

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
    # 990001 and 990003 both have domain=Piyyut
    assert piyyut["count"] == 2


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
    assert catalog["description_eng"] == "A legal document from the Genizah"
    assert catalog["description_heb"] == "שטר משפטי מן הגניזה"
    assert catalog["textual_frame_heb"] == "טקסט עברי"
    assert catalog["textual_frame_eng"] == "Hebrew text"


def test_get_catalog_not_found(service):
    """get_catalog returns None for unknown AlmaId."""
    assert service.get_catalog("999999") is None


def test_get_catalog_has_all_keys(service):
    """get_catalog result has all expected keys."""
    catalog = service.get_catalog("990001")
    expected_keys = {
        "title", "title_heb", "author_text", "copy_date", "copy_place",
        "description_eng", "description_heb", "textual_frame_heb", "textual_frame_eng",
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


# ── Catalog record counts tests ────────────────────────────────


def test_get_catalog_record_counts(service):
    """get_catalog_record_counts returns correct counts for known sys_ids."""
    counts = service.get_catalog_record_counts(["990001", "990002", "999999"])
    assert "990001" in counts
    assert counts["990001"] >= 1
    assert "999999" not in counts  # No data for this sys_id


def test_get_catalog_record_counts_empty(service):
    """get_catalog_record_counts returns empty dict for empty input."""
    assert service.get_catalog_record_counts([]) == {}


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
    # 990002 has SourceName='Catalogs' which is generic
    names = service.get_source_names("990002")
    assert names == []


def test_get_source_names_empty(service):
    """get_source_names returns [] for non-existent AlmaId."""
    assert service.get_source_names("999999") == []


# ── Web shim import test ─────────────────────────────────────────


def test_web_shim_import():
    """web.fjms_service shim re-exports FjmsService and get_fjms_service."""
    from web.fjms_service import FjmsService as WebFjmsService
    from web.fjms_service import get_fjms_service as web_get_fjms_service
    assert WebFjmsService is FjmsService
    assert web_get_fjms_service is get_fjms_service
