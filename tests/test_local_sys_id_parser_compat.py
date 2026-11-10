# -*- coding: utf-8 -*-
"""Phase 95 D-13: parse_header_smart + parse_full_id_components must accept
97-prefix LOCAL sys_ids (Codex P0 fix).

Uses MetadataManager.__new__ to bypass __init__ (no Tantivy index needed for
parser methods — they are pure regex operations on the full_header string).

D-34 LOCAL full_header format:
  unique_id: "LOCAL_{sys_id}_P{page_num}"
  full_header: "{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"
  Example: "970012345601234567_LOCAL_P3_F0042"
"""
from __future__ import annotations

import pytest

from genizah_core import MetadataManager


@pytest.fixture
def mgr():
    """Minimal MetadataManager instance — bypasses __init__ for parser-only tests."""
    return MetadataManager.__new__(MetadataManager)


class TestParseHeaderSmartLocal:
    def test_parse_header_smart_recognizes_local(self, mgr):
        """D-13 P0: parse_header_smart correctly extracts 97-prefix sys_id and page number."""
        sys_id, p_num = mgr.parse_header_smart("970012345601234567_LOCAL_P3_F0042")
        assert sys_id == "970012345601234567", f"Expected LOCAL sys_id, got: {sys_id!r}"
        assert p_num == "3", f"Expected p_num='3', got: {p_num!r}"

    def test_parse_header_smart_still_recognizes_synthetic(self, mgr):
        """REGRESSION: 99-prefix synthetic sys_ids still extracted correctly."""
        sys_id, p_num = mgr.parse_header_smart("990012345600000000_IE1_P5_FL2")
        assert sys_id == "990012345600000000", f"Expected synthetic sys_id, got: {sys_id!r}"
        assert p_num == "5", f"Expected p_num='5', got: {p_num!r}"

    def test_parse_header_smart_still_recognizes_real_alma(self, mgr):
        """REGRESSION: real Alma sys_ids (99-prefix, no 000000 suffix) still extracted."""
        sys_id, p_num = mgr.parse_header_smart("990025143260205171_IE1_P5_FL2")
        assert sys_id == "990025143260205171", f"Expected real Alma sys_id, got: {sys_id!r}"
        assert p_num == "5", f"Expected p_num='5', got: {p_num!r}"


class TestParseFullIdComponentsLocal:
    def test_parse_full_id_components_local(self, mgr):
        """D-13 P0 + D-34: full dict extraction from LOCAL full_header."""
        result = mgr.parse_full_id_components("970012345601234567_LOCAL_P3_F0042")
        assert result["sys_id"] == "970012345601234567", (
            f"Expected LOCAL sys_id, got: {result['sys_id']!r}"
        )
        assert result["ie_id"] == "F0042", (
            f"Expected ie_id='F0042' (D-34 synthetic file_id), got: {result['ie_id']!r}"
        )
        assert result["p_num"] == "3", f"Expected p_num='3', got: {result['p_num']!r}"
        assert result["fl_id"] is None, f"Expected fl_id=None, got: {result['fl_id']!r}"

    def test_parse_full_id_components_synthetic_unchanged(self, mgr):
        """REGRESSION: synthetic headers still extract IE and FL components correctly."""
        result = mgr.parse_full_id_components("990012345600000000_IE1_P5_FL2")
        assert result["sys_id"] == "990012345600000000"
        assert result["ie_id"] == "IE1", (
            f"Expected ie_id='IE1' for synthetic, got: {result['ie_id']!r}"
        )
        assert result["p_num"] == "5"
        assert result["fl_id"] == "2", (
            f"Expected fl_id='2' for synthetic, got: {result['fl_id']!r}"
        )

    def test_parse_full_id_components_local_leading_zero_page(self, mgr):
        """D-13: Page number zero-stripped (P03 -> '3')."""
        result = mgr.parse_full_id_components("970012345601234567_LOCAL_P03_F0001")
        assert result["p_num"] == "3", f"Expected p_num='3' (stripped), got: {result['p_num']!r}"

    def test_parse_full_id_components_local_multidigit_file_id(self, mgr):
        """D-34: Large file_id still produces valid ie_id."""
        result = mgr.parse_full_id_components("970012345601234567_LOCAL_P12_F12345")
        assert result["ie_id"] == "F12345", (
            f"Expected ie_id='F12345', got: {result['ie_id']!r}"
        )
        assert result["p_num"] == "12"
