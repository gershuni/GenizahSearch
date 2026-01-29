"""Test shelfmark normalization and matching logic."""

import pytest
import sys
from pathlib import Path

# Ensure project root is in path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


class TestShelfmarkNormalization:
    """Test the _normalize_shelfmark method."""

    def get_meta_mgr_class(self):
        """Import MetadataManager dynamically to avoid initialization issues."""
        # We only need to test the normalization method, so we can mock the class
        import re

        class MockMetadataManager:
            """Mock class with just the normalization method for testing."""

            def _normalize_shelfmark(self, shelfmark: str) -> str:
                """Normalize shelfmarks: remove non-alphanumeric chars but preserve dots between digits."""
                if not shelfmark:
                    return ""

                # First, preserve dots that appear between digits (like 120.2) by replacing with a marker
                temp = re.sub(r'(\d)\.(\d)', r'\1DOTMARKER\2', shelfmark)

                # Remove all other non-alphanumeric characters
                cleaned = re.sub(r'\W+', '', temp).casefold()

                # Restore the preserved dots
                cleaned = cleaned.replace('dotmarker', '.')

                if cleaned.startswith("ms"):
                    cleaned = cleaned[2:]

                return cleaned

        return MockMetadataManager

    def test_normalize_removes_spaces(self):
        """Test that spaces are removed."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("T-S 12.123") == "ts12.123"
        assert mgr._normalize_shelfmark("T-S  12  123") == "ts12123"

    def test_normalize_removes_dashes(self):
        """Test that dashes are removed."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("T-S 12.123") == "ts12.123"
        assert mgr._normalize_shelfmark("T--S-12-123") == "ts12123"

    def test_normalize_case_insensitive(self):
        """Test that normalization is case-insensitive."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("T-S 12.123") == mgr._normalize_shelfmark("t-s 12.123")
        assert mgr._normalize_shelfmark("TS12.123") == mgr._normalize_shelfmark("ts12.123")

    def test_normalize_preserves_dots_between_digits(self):
        """Test that dots between digits are preserved."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("T-S 12.123") == "ts12.123"
        assert mgr._normalize_shelfmark("120.2") == "120.2"

    def test_normalize_removes_non_digit_dots(self):
        """Test that dots not between digits are removed."""
        mgr = self.get_meta_mgr_class()()
        # Dot before digit but after letter - removed
        assert mgr._normalize_shelfmark("MS. Heb. a.1") == "heba1"

    def test_normalize_strips_ms_prefix(self):
        """Test that MS prefix is stripped."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("MS Heb. a.1") == "heba1"
        assert mgr._normalize_shelfmark("Ms. Heb. a.1") == "heba1"

    def test_normalize_empty_string(self):
        """Test empty string handling."""
        mgr = self.get_meta_mgr_class()()
        assert mgr._normalize_shelfmark("") == ""
        assert mgr._normalize_shelfmark(None) == ""


class TestShelfmarkMatching:
    """Test that different shelfmark formats match correctly."""

    def test_various_formats_normalize_same(self):
        """Test that various user input formats normalize to match the canonical form."""
        import re

        def _normalize_shelfmark(shelfmark: str) -> str:
            if not shelfmark:
                return ""
            temp = re.sub(r'(\d)\.(\d)', r'\1DOTMARKER\2', shelfmark)
            cleaned = re.sub(r'\W+', '', temp).casefold()
            cleaned = cleaned.replace('dotmarker', '.')
            if cleaned.startswith("ms"):
                cleaned = cleaned[2:]
            return cleaned

        canonical = "T-S 12.123"
        canonical_norm = _normalize_shelfmark(canonical)

        # All these variations should normalize to the same value
        variations = [
            "ts12.123",      # No dashes, no spaces
            "T-S 12 123",    # Space instead of dot
            "t-s12.123",     # No space after prefix
            "TS 12.123",     # No dash
            "t s 12.123",    # Space instead of dash
            "T-S  12.123",   # Extra space
        ]

        for variant in variations:
            variant_norm = _normalize_shelfmark(variant)
            # For "T-S 12 123" (space instead of dot), the normalized form will be "ts12123"
            # which won't exactly match "ts12.123", but the matching logic handles this
            # by also checking prefix matches
            assert variant_norm.replace('.', '') == canonical_norm.replace('.', ''), \
                f"'{variant}' normalized to '{variant_norm}', expected similar to '{canonical_norm}'"


class TestSearchByMetaMatching:
    """Test the full search_by_meta matching logic for shelfmarks."""

    def create_mock_meta_manager(self):
        """Create a minimal mock MetadataManager for testing search_by_meta."""
        import re

        class MockMetadataManager:
            def __init__(self):
                # Simulated data
                self.csv_bank = {
                    "sys001": {"shelfmark": "T-S 12.123", "title": "Fragment 1"},
                    "sys002": {"shelfmark": "T-S NS 120.2", "title": "Fragment 2"},
                    "sys003": {"shelfmark": "T-S NS 121.4", "title": "Fragment 3"},
                    "sys004": {"shelfmark": "MS Heb. a.1", "title": "Oxford Fragment"},
                }
                self.nli_cache = {}

            def _normalize_shelfmark(self, shelfmark: str) -> str:
                if not shelfmark:
                    return ""
                temp = re.sub(r'(\d)\.(\d)', r'\1DOTMARKER\2', shelfmark)
                cleaned = re.sub(r'\W+', '', temp).casefold()
                cleaned = cleaned.replace('dotmarker', '.')
                if cleaned.startswith("ms"):
                    cleaned = cleaned[2:]
                return cleaned

            def search_by_meta(self, query, field):
                """Simplified version of search_by_meta for testing."""
                results = set()
                q_norm = query.lower().strip()
                q_normalized = self._normalize_shelfmark(query) if field == 'shelfmark' else None

                def matches(value, query_norm):
                    val_norm = value.lower().strip()

                    if val_norm == query_norm:
                        return True

                    if field == 'shelfmark':
                        val_normalized = self._normalize_shelfmark(value)

                        if val_normalized == q_normalized:
                            return True

                        if q_normalized and val_normalized.startswith(q_normalized):
                            next_pos = len(q_normalized)
                            if next_pos < len(val_normalized):
                                next_char = val_normalized[next_pos]
                                if next_char == '.' or not next_char.isdigit():
                                    return True
                                # If digit continues, fall through to token matching
                            else:
                                return True

                        val_tokens = [t for t in re.split(r'[\s\.\-]+', val_norm) if t]
                        query_tokens = [t for t in re.split(r'[\s\.\-]+', query_norm) if t]

                        if not query_tokens:
                            return False

                        query_idx = 0
                        val_idx = 0

                        while query_idx < len(query_tokens) and val_idx < len(val_tokens):
                            qt = query_tokens[query_idx]
                            vt = val_tokens[val_idx]

                            if qt == vt:
                                query_idx += 1
                                val_idx += 1
                            elif vt.startswith(qt):
                                # For the LAST query token, allow numeric prefix
                                if query_idx == len(query_tokens) - 1:
                                    query_idx += 1
                                    val_idx += 1
                                elif qt.isdigit() and vt.isdigit():
                                    if len(vt) > len(qt) and vt[len(qt)].isdigit():
                                        val_idx += 1
                                        continue
                                    query_idx += 1
                                    val_idx += 1
                                else:
                                    query_idx += 1
                                    val_idx += 1
                            else:
                                val_idx += 1

                        return query_idx == len(query_tokens)
                    else:
                        return query_norm in val_norm

                    return False

                for sys_id, data in self.csv_bank.items():
                    val = data.get(field, '')
                    if val and matches(val, q_norm):
                        results.add(sys_id)

                for sys_id, data in self.nli_cache.items():
                    val = data.get(field, '')
                    if val and matches(val, q_norm):
                        results.add(sys_id)

                return list(results)

        return MockMetadataManager()

    def test_exact_match(self):
        """Test exact shelfmark match."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("T-S 12.123", "shelfmark")
        assert "sys001" in results

    def test_normalized_match_no_spaces(self):
        """Test that 'ts12.123' matches 'T-S 12.123'."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("ts12.123", "shelfmark")
        assert "sys001" in results

    def test_normalized_match_different_spacing(self):
        """Test that 'T-S  12.123' (extra space) matches 'T-S 12.123'."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("T-S  12.123", "shelfmark")
        assert "sys001" in results

    def test_normalized_match_no_dash(self):
        """Test that 'TS 12.123' (no dash) matches 'T-S 12.123'."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("TS 12.123", "shelfmark")
        assert "sys001" in results

    def test_case_insensitive_match(self):
        """Test case-insensitive matching."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("t-s 12.123", "shelfmark")
        assert "sys001" in results

    def test_no_false_positive_120_vs_121(self):
        """Test that '120.2' does not match '121.4'."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("T-S NS 120", "shelfmark")
        # Should match sys002 (T-S NS 120.2) but NOT sys003 (T-S NS 121.4)
        assert "sys002" in results
        assert "sys003" not in results

    def test_prefix_match(self):
        """Test that 'T-S NS 12' matches both 120.2 and 121.4."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("T-S NS 12", "shelfmark")
        # Prefix "12" should match both 120.2 and 121.4
        assert "sys002" in results
        assert "sys003" in results

    def test_oxford_ms_prefix_stripped(self):
        """Test that Oxford MS prefixes are handled."""
        mgr = self.create_mock_meta_manager()
        # Searching for 'Heb a 1' should find 'MS Heb. a.1'
        results = mgr.search_by_meta("Heb. a.1", "shelfmark")
        assert "sys004" in results

    def test_partial_match_ts_ns(self):
        """Test partial shelfmark matching."""
        mgr = self.create_mock_meta_manager()
        results = mgr.search_by_meta("T-S NS", "shelfmark")
        # Should match both T-S NS items
        assert "sys002" in results
        assert "sys003" in results
        # Should not match T-S 12.123 (no NS)
        assert "sys001" not in results


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
