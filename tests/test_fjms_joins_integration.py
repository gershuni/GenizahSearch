# -*- coding: utf-8 -*-
"""
Integration tests for FJMS joins in both web and desktop apps.

Tests verify that FJMS scholarly joins are correctly merged into the
Related Fragments pipeline, with proper deduplication, scholar attribution,
and graceful degradation when the FjmsService is unavailable.
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from shared.fjms_service import FjmsService


@pytest.fixture
def fjms_test_db(tmp_path):
    """Create a minimal fjms_enrichment.db for testing joins integration."""
    db_path = str(tmp_path / "test_fjms_joins.db")
    conn = sqlite3.connect(db_path)

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

    # Insert test joins data: group 100 has 3 members
    conn.executemany(
        "INSERT INTO joins VALUES (?, ?, ?, ?, ?)",
        [
            ("SYS001", 100, "Goitein", "Fragment A", "Physical Join"),
            ("SYS002", 100, "Goitein", "Fragment B", "Physical Join"),
            ("SYS003", 100, "Gil", "Fragment C", "Codex Join"),
            # Group 200: overlapping members for multi-group tests
            ("SYS001", 200, "Ben-Sasson", "Another group", "Codex Join"),
            ("SYS002", 200, "Ben-Sasson", "Also in group 200", None),
        ],
    )
    conn.execute("INSERT INTO meta (key, value) VALUES ('version', '1.0.0')")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def fjms_service(fjms_test_db):
    """Create an FjmsService instance with the test database."""
    svc = FjmsService(db_path=fjms_test_db, thread_safe=True)
    yield svc
    svc.close()


@pytest.fixture
def mock_meta_mgr():
    """Create a mock metadata manager that resolves sys_ids to shelfmarks."""
    meta = MagicMock()

    def get_meta_for_id(sys_id):
        mapping = {
            "SYS001": ("T-S 12.100", "Title A"),
            "SYS002": ("T-S 12.200", "Title B"),
            "SYS003": ("T-S 12.300", "Title C"),
        }
        return mapping.get(sys_id, ("Unknown", ""))

    meta.get_meta_for_id = get_meta_for_id
    return meta


# ── Web app integration tests ─────────────────────────────────────


class TestFjmsJoinsMergeIntoFetchConnectedFragments:
    """Test FJMS joins merge into web app's fetch_connected_fragments."""

    @patch("web.components.joins_panel.get_fragment_joins")
    @patch("web.components.joins_panel.state")
    @patch("web.document_service.get_document_for_fragment", return_value=None)
    @patch("web.fjms_service.get_fjms_service")
    def test_fjms_joins_merge(self, mock_get_fjms, mock_doc_for_frag, mock_state, mock_get_joins, fjms_service, mock_meta_mgr):
        """FJMS joins appear in fetch_connected_fragments output with correct structure."""
        mock_get_joins.return_value = []  # No user joins
        mock_state.meta_mgr = mock_meta_mgr
        mock_get_fjms.return_value = fjms_service

        from web.components.joins_panel import fetch_connected_fragments

        result = fetch_connected_fragments(
            shelfmark="T-S 12.100",
            document_id="SYS001",
            force_refresh=True,
        )

        # Should have FJMS joins (SYS002, SYS003 are in groups with SYS001)
        fjms_joins = [j for j in result.get("joins", []) if j.get("source") == "FJMS"]
        assert len(fjms_joins) == 2

        # Check structure
        for j in fjms_joins:
            assert j["id"] is None
            assert j["source"] == "FJMS"
            assert "scholar_name" in j
            assert "join_group_id" in j
            assert j["fragment_a"] == "T-S 12.100"

        # Check scholar names (aggregated: SYS002 has Goitein+Ben-Sasson, SYS003 has Gil)
        all_scholars = ' '.join(j["scholar_name"] for j in fjms_joins)
        assert "Goitein" in all_scholars
        assert "Gil" in all_scholars


class TestFjmsJoinsDeduplication:
    """Test deduplication between FJMS and PGP/user joins."""

    @patch("web.components.joins_panel.get_fragment_joins")
    @patch("web.components.joins_panel.state")
    @patch("web.document_service.get_document_for_fragment", return_value=None)
    @patch("web.fjms_service.get_fjms_service")
    def test_fjms_deduplication_with_user_joins(self, mock_get_fjms, mock_doc_for_frag, mock_state, mock_get_joins, fjms_service, mock_meta_mgr):
        """When a fragment appears in both user and FJMS joins, it is not duplicated."""
        # Simulate user join already containing T-S 12.200
        mock_get_joins.return_value = [
            {
                "fragment_a_shelfmark": "T-S 12.100",
                "fragment_b_shelfmark": "T-S 12.200",
                "fragment_a_sys_id": "SYS001",
                "fragment_b_sys_id": "SYS002",
                "id": 1,
                "join_type": "physical_join",
                "notes": "",
            }
        ]
        mock_state.meta_mgr = mock_meta_mgr
        mock_get_fjms.return_value = fjms_service

        from web.components.joins_panel import fetch_connected_fragments

        result = fetch_connected_fragments(
            shelfmark="T-S 12.100",
            document_id="SYS001",
            force_refresh=True,
        )

        # T-S 12.200 should appear only once (from user join, not duplicated by FJMS)
        shelfmarks = result.get("fragments", [])
        count_200 = sum(1 for s in shelfmarks if "12.200" in s)
        assert count_200 == 1

        # But T-S 12.300 should appear (new from FJMS)
        assert any("12.300" in s for s in shelfmarks)


class TestFjmsJoinsScholarAttribution:
    """Test scholar name preservation through the pipeline."""

    @patch("web.components.joins_panel.get_fragment_joins")
    @patch("web.components.joins_panel.state")
    @patch("web.document_service.get_document_for_fragment", return_value=None)
    @patch("web.fjms_service.get_fjms_service")
    def test_scholar_name_preserved(self, mock_get_fjms, mock_doc_for_frag, mock_state, mock_get_joins, fjms_service, mock_meta_mgr):
        """Scholar name is preserved in FJMS join data."""
        mock_get_joins.return_value = []
        mock_state.meta_mgr = mock_meta_mgr
        mock_get_fjms.return_value = fjms_service

        from web.components.joins_panel import fetch_connected_fragments

        result = fetch_connected_fragments(
            shelfmark="T-S 12.100",
            document_id="SYS001",
            force_refresh=True,
        )

        fjms_joins = [j for j in result.get("joins", []) if j.get("source") == "FJMS"]
        # SYS002 join -- has scholars from both groups (Goitein, Ben-Sasson)
        sys002_joins = [j for j in fjms_joins if j["fragment_b"] == "T-S 12.200"]
        assert len(sys002_joins) == 1
        assert "Goitein" in sys002_joins[0]["scholar_name"]

        # SYS003 join -- only in group 100 (Gil)
        sys003_joins = [j for j in fjms_joins if j["fragment_b"] == "T-S 12.300"]
        assert len(sys003_joins) == 1
        assert "Gil" in sys003_joins[0]["scholar_name"]


class TestFjmsJoinsGracefulDegradation:
    """Test graceful degradation when FjmsService is unavailable."""

    @patch("web.components.joins_panel.get_fragment_joins")
    @patch("web.components.joins_panel.state")
    @patch("web.document_service.get_document_for_fragment", return_value=None)
    @patch("web.fjms_service.get_fjms_service")
    def test_no_fjms_when_unavailable(self, mock_get_fjms, mock_doc_for_frag, mock_state, mock_get_joins):
        """No FJMS joins added when FjmsService is unavailable."""
        mock_get_joins.return_value = []
        mock_state.meta_mgr = MagicMock()

        # Service that reports unavailable
        unavailable_svc = MagicMock()
        unavailable_svc.is_available.return_value = False
        mock_get_fjms.return_value = unavailable_svc

        from web.components.joins_panel import fetch_connected_fragments

        result = fetch_connected_fragments(
            shelfmark="T-S 12.100",
            document_id="SYS001",
            force_refresh=True,
        )

        fjms_joins = [j for j in result.get("joins", []) if j.get("source") == "FJMS"]
        assert len(fjms_joins) == 0


# ── Desktop app integration tests ─────────────────────────────────


class TestFjmsJoinsDesktopDialog:
    """Test FJMS joins in desktop JoinsDialog _get_fjms_joins.

    Since JoinsDialog extends QDialog (C++ class), we test _get_fjms_joins
    by binding the unbound method to a plain object with the right attributes.
    """

    def _make_dialog_stub(self, document_id, shelfmark, meta_mgr):
        """Create a lightweight stub with the attributes _get_fjms_joins needs."""
        from corrections_ui import JoinsDialog

        stub = MagicMock()
        stub.document_id = document_id
        stub.shelfmark = shelfmark
        stub.meta_mgr = meta_mgr
        # Bind the real method to the stub
        stub._get_fjms_joins = JoinsDialog._get_fjms_joins.__get__(stub, type(stub))
        return stub

    def test_get_fjms_joins_returns_proper_structure(self, fjms_service, mock_meta_mgr):
        """_get_fjms_joins returns shelfmarks, joins, and details."""
        dialog = self._make_dialog_stub("SYS001", "T-S 12.100", mock_meta_mgr)

        with patch("shared.fjms_service.get_fjms_service", return_value=fjms_service):
            fjms_frags, fjms_joins, fjms_details = dialog._get_fjms_joins()

        # Should return non-self members
        assert len(fjms_joins) == 2

        # Check join structure
        for j in fjms_joins:
            assert j["source"] == "FJMS"
            assert j["id"] is None
            assert "created_by_username" in j  # scholar_name stored here
            assert j["fragment_a"] == "T-S 12.100"

        # Check scholar names in joins (aggregated as comma-separated strings)
        all_scholars = ' '.join(j["created_by_username"] for j in fjms_joins)
        assert "Goitein" in all_scholars
        assert "Gil" in all_scholars

    def test_get_fjms_joins_graceful_degradation(self):
        """_get_fjms_joins returns empty tuples when service unavailable."""
        dialog = self._make_dialog_stub("SYS001", "T-S 12.100", MagicMock())

        unavailable_svc = MagicMock()
        unavailable_svc.is_available.return_value = False

        with patch("shared.fjms_service.get_fjms_service", return_value=unavailable_svc):
            fjms_frags, fjms_joins, fjms_details = dialog._get_fjms_joins()

        assert fjms_frags == []
        assert fjms_joins == []
        assert fjms_details == []

    def test_get_fjms_joins_skips_self(self, fjms_service, mock_meta_mgr):
        """_get_fjms_joins does not include the current document in joins."""
        dialog = self._make_dialog_stub("SYS001", "T-S 12.100", mock_meta_mgr)

        with patch("shared.fjms_service.get_fjms_service", return_value=fjms_service):
            fjms_frags, fjms_joins, fjms_details = dialog._get_fjms_joins()

        # None of the joins should have fragment_b == current shelfmark
        for j in fjms_joins:
            assert j["fragment_b"].upper() != "T-S 12.100"

    def test_get_fjms_joins_multi_group_aggregated(self, fjms_service, mock_meta_mgr):
        """Multi-group partners appear once with all scholars aggregated in desktop."""
        dialog = self._make_dialog_stub("SYS001", "T-S 12.100", mock_meta_mgr)

        with patch("shared.fjms_service.get_fjms_service", return_value=fjms_service):
            fjms_frags, fjms_joins, fjms_details = dialog._get_fjms_joins()

        # Each fragment_b should appear at most once
        fragment_bs = [j["fragment_b"] for j in fjms_joins]
        assert len(fragment_bs) == len(set(fragment_bs)), f"Duplicate fragment_b: {fragment_bs}"

        # SYS002 is in groups 100 (Goitein) and 200 (Ben-Sasson)
        sys002_join = next(j for j in fjms_joins if j["fragment_b"] == "T-S 12.200")
        assert "Goitein" in sys002_join["created_by_username"]
        assert "Ben-Sasson" in sys002_join["created_by_username"]


# ── Web multi-group integration test ────────────────────────────────


class TestFjmsMultiGroupWeb:
    """Test multi-group deduplication in web app fetch_connected_fragments."""

    @patch("web.components.joins_panel.get_fragment_joins")
    @patch("web.components.joins_panel.state")
    @patch("web.document_service.get_document_for_fragment", return_value=None)
    @patch("web.fjms_service.get_fjms_service")
    def test_fjms_multi_group_shows_all_scholars(self, mock_get_fjms, mock_doc_for_frag, mock_state, mock_get_joins, fjms_service, mock_meta_mgr):
        """Multi-group partners show all contributing scholars in web app."""
        mock_get_joins.return_value = []  # No user joins
        mock_state.meta_mgr = mock_meta_mgr
        mock_get_fjms.return_value = fjms_service

        from web.components.joins_panel import fetch_connected_fragments

        result = fetch_connected_fragments(
            shelfmark="T-S 12.100",
            document_id="SYS001",
            force_refresh=True,
        )

        fjms_joins = [j for j in result.get("joins", []) if j.get("source") == "FJMS"]

        # SYS002 should appear exactly once
        sys002_joins = [j for j in fjms_joins if j["fragment_b"] == "T-S 12.200"]
        assert len(sys002_joins) == 1

        # Scholar names should contain both scholars (comma-separated)
        scholar_str = sys002_joins[0]["scholar_name"]
        assert "Goitein" in scholar_str
        assert "Ben-Sasson" in scholar_str

        # Relationship type should contain the non-NULL join type
        rel_type = sys002_joins[0]["relationship_type"]
        assert "Physical Join" in rel_type
