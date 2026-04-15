# -*- coding: utf-8 -*-
"""
Verification tests for desktop pending corrections display.

Phase 24 / CORR-06: Confirms the existing desktop app implementation in
genizah_app.py correctly handles pending corrections in both Browse tab
and Reading Desk version selectors.

These tests read genizah_app.py source code to verify patterns, and use
dataclass introspection for the Correction model. This follows the same
approach as tests/test_version_selector_pending.py (Phase 23).

Success criteria verified:
  1. Pending corrections appear in version selector (tests 1, 6)
  2. Visually distinct labels with emoji + status text (tests 2, 7)
  3. Selecting a pending correction displays corrected_text (tests 4, 8)
  4. Permission filtering for non-logged-in users (test 5)
  5. Correction dataclass has required fields (test 9)
"""

import os
import re
import dataclasses
import pytest


# ---------------------------------------------------------------------------
# Helpers: read source file and extract method sections
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def genizah_app_source():
    """Return the full source code of genizah_app.py as a string."""
    src_path = os.path.join(os.path.dirname(__file__), '..', 'genizah_app.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        return f.read()


@pytest.fixture(scope="module")
def desktop_rd_source():
    """Return the full source code of desktop/result_dialog.py as a string."""
    src_path = os.path.join(os.path.dirname(__file__), '..', 'desktop', 'result_dialog.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        return f.read()


def _extract_method(source, method_name):
    """Extract a method's source from the full file by finding its def and
    reading until the next def at the same or lower indentation level."""
    pattern = re.compile(r'^( +)def ' + re.escape(method_name) + r'\(', re.MULTILINE)
    match = pattern.search(source)
    if not match:
        return ""
    indent = len(match.group(1))
    start = match.start()
    # Find end: next def at same or lower indent, or end of file
    rest = source[match.end():]
    end_pattern = re.compile(r'^\s{0,' + str(indent) + r'}(?:def |class )', re.MULTILINE)
    end_match = end_pattern.search(rest)
    if end_match:
        return source[start:match.end() + end_match.start()]
    return source[start:]


# ---------------------------------------------------------------------------
# Browse tab tests (tests 1-5)
# ---------------------------------------------------------------------------

class TestBrowseTabPendingCorrections:
    """Verify Browse tab version selector handles pending corrections."""

    def test_browse_tab_fetches_corrections_with_drafts(self, genizah_app_source):
        """Browse tab calls get_corrections_for_document with include_drafts=True."""
        # The browse version loading section must fetch corrections including drafts
        assert "get_corrections_for_document" in genizah_app_source, \
            "Source should call get_corrections_for_document"
        assert "include_drafts=True" in genizah_app_source, \
            "Source should pass include_drafts=True to include pending/draft corrections"

    def test_browse_tab_shows_pending_labels(self, genizah_app_source):
        """Browse tab shows emoji-labeled status indicators for pending corrections."""
        # Pencil emoji for drafts, hourglass for pending
        assert "\U0001f4dd" in genizah_app_source, \
            "Source should contain pencil emoji for Draft label"
        assert "Draft" in genizah_app_source, \
            "Source should contain 'Draft' status text"
        assert "\u231b" in genizah_app_source or "\u23f3" in genizah_app_source, \
            "Source should contain hourglass emoji for Pending label"
        assert "Pending" in genizah_app_source, \
            "Source should contain 'Pending' status text"

    def test_browse_tab_stores_correction_data(self, genizah_app_source):
        """Browse tab stores correction data with source, corrected_text, and status."""
        # The combo item data dict must include these keys for selection handling
        assert '"source": "correction"' in genizah_app_source, \
            "Source should store 'correction' as the source type"
        assert '"corrected_text"' in genizah_app_source, \
            "Source should store corrected_text in combo item data"
        assert '"status"' in genizah_app_source, \
            "Source should store status in combo item data"

    def test_browse_load_version_handles_corrections(self, genizah_app_source):
        """_browse_load_version handles source=='correction' and reads corrected_text."""
        method_source = _extract_method(genizah_app_source, '_browse_load_version')
        assert method_source, "_browse_load_version method should exist in genizah_app.py"

        assert 'source == "correction"' in method_source or "source == 'correction'" in method_source, \
            "_browse_load_version should check for source == 'correction'"
        assert "corrected_text" in method_source, \
            "_browse_load_version should read corrected_text from version_data"

    def test_browse_tab_filters_by_permissions(self, genizah_app_source):
        """Browse tab filters pending corrections based on user permissions."""
        # Must check ownership and role for draft/pending corrections
        assert "is_own_correction" in genizah_app_source, \
            "Source should have is_own_correction variable for permission checks"
        assert "is_reviewer_or_admin" in genizah_app_source, \
            "Source should check is_reviewer_or_admin role"
        # Draft and pending status filtering
        assert "'draft'" in genizah_app_source or '"draft"' in genizah_app_source, \
            "Source should filter on 'draft' status"
        assert "'pending'" in genizah_app_source or '"pending"' in genizah_app_source, \
            "Source should filter on 'pending' status"


# ---------------------------------------------------------------------------
# Reading Desk tests (tests 6-8)
# ---------------------------------------------------------------------------

class TestReadingDeskPendingCorrections:
    """Verify Reading Desk version selector handles pending corrections."""

    def test_reading_desk_fetches_corrections_with_drafts(self, desktop_rd_source):
        """Reading Desk calls get_corrections_for_document with include_drafts=True."""
        method_source = _extract_method(desktop_rd_source, '_rd_refresh_versions')
        assert method_source, "_rd_refresh_versions method should exist in desktop/result_dialog.py"

        assert "get_corrections_for_document" in method_source, \
            "_rd_refresh_versions should call get_corrections_for_document"
        assert "include_drafts=True" in method_source, \
            "_rd_refresh_versions should pass include_drafts=True"

    def test_reading_desk_shows_pending_labels(self, desktop_rd_source):
        """Reading Desk shows emoji status labels for pending corrections."""
        method_source = _extract_method(desktop_rd_source, '_rd_refresh_versions')
        assert method_source, "_rd_refresh_versions method should exist in desktop/result_dialog.py"

        # Same emoji labels as Browse tab
        assert "\U0001f4dd" in method_source, \
            "RD should contain pencil emoji for Draft label"
        assert "\u231b" in method_source or "\u23f3" in method_source, \
            "RD should contain hourglass emoji for Pending label"
        assert "Pending" in method_source, \
            "RD should contain 'Pending' status text"

    def test_reading_desk_handles_correction_selection(self, desktop_rd_source):
        """_rd_load_version_content handles source=='correction' and reads corrected_text."""
        method_source = _extract_method(desktop_rd_source, '_rd_load_version_content')
        assert method_source, "_rd_load_version_content method should exist in desktop/result_dialog.py"

        assert 'source == "correction"' in method_source or "source == 'correction'" in method_source, \
            "_rd_load_version_content should check for source == 'correction'"
        assert "corrected_text" in method_source, \
            "_rd_load_version_content should read corrected_text from version_data"


# ---------------------------------------------------------------------------
# Data structure test (test 9)
# ---------------------------------------------------------------------------

class TestCorrectionDataclass:
    """Verify the Correction dataclass has required fields."""

    def test_correction_dataclass_has_required_fields(self):
        """Correction dataclass has all fields needed for pending corrections display."""
        from supabase_corrections_client import Correction

        field_names = {f.name for f in dataclasses.fields(Correction)}

        required_fields = {
            'corrected_text',  # Displayed when selected
            'status',          # Used for label and filtering
            'page_number',     # Used for page-level filtering
            'author_username', # Shown in label
            'created_at',      # Shown in date
            'id',              # Stored in combo item data
        }

        for field_name in required_fields:
            assert field_name in field_names, \
                f"Correction dataclass missing required field: {field_name}"
