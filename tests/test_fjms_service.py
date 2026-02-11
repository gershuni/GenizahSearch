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
            ("version", "1.0.0"),
            ("created", "2026-02-12T00:00:00Z"),
            ("source", "FIST.db"),
        ],
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
    assert service.get_version() == "1.0.0"


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
    # Group 100 has 990001, 990004, 990005 -- should return 990004 and 990005
    alma_ids = {j["alma_id"] for j in joins}
    assert "990004" in alma_ids
    assert "990005" in alma_ids
    # Should NOT include the queried manuscript
    assert "990001" not in alma_ids


def test_get_join_group_has_correct_keys(service):
    """Each join group member dict has expected keys."""
    joins = service.get_join_group("990001")
    assert len(joins) > 0
    for j in joins:
        assert "alma_id" in j
        assert "join_group_id" in j
        assert "scholar_name" in j
        assert "comment" in j
        assert "join_type" in j


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
    assert version == "1.0.0"
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


# ── Web shim import test ─────────────────────────────────────────


def test_web_shim_import():
    """web.fjms_service shim re-exports FjmsService and get_fjms_service."""
    from web.fjms_service import FjmsService as WebFjmsService
    from web.fjms_service import get_fjms_service as web_get_fjms_service
    assert WebFjmsService is FjmsService
    assert web_get_fjms_service is get_fjms_service
