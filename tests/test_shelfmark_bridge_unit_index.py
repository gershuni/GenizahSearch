"""Phase 84 Plan 05: deterministic unit tests for the alias-index/lookup path.

Codex MEDIUM #7: avoid integration-only coverage. These tests build small synthetic
csv_banks in memory and exercise the bridge logic directly. Run in any environment.
"""
import pytest
from shared.shelfmark_bridge import (
    cudl_normalize, _collapse_numeric_runs, shelfmark_to_cudl_label,
    build_alias_index, lookup_cudl,
)


class TestCudlNormalize:
    def test_dot_after_letter_dropped(self):
        assert cudl_normalize("T-S Ar. 48.211") == "tsar48.211"

    def test_slash_to_dot(self):
        assert cudl_normalize("T-S F 8/002") == "tsf8.2"

    def test_comma_to_dot(self):
        assert cudl_normalize("Add. 863, 2") == "add863.2"

    def test_leading_zero_strip(self):
        assert cudl_normalize("T-S NS 329/0014") == "tsns329.14"

    def test_empty_input(self):
        assert cudl_normalize("") == ""
        assert cudl_normalize(None) == ""


class TestNumericRunCollapse:
    def test_three_run(self):
        assert _collapse_numeric_runs("or1080.1.1") == "or1080.11"

    def test_two_run_unchanged(self):
        assert _collapse_numeric_runs("tsar48.211") == "tsar48.211"


class TestShelfmarkToCudlLabel:
    def test_mosseri(self):
        assert shelfmark_to_cudl_label("Moss. III,27O") == "mosseriiii27o"

    def test_or_letter_suffix(self):
        assert shelfmark_to_cudl_label("Or. 1080 J 15") == "or1080j15"

    def test_or_numeric_collapse(self):
        # Round 3 Codex MEDIUM regression check
        assert shelfmark_to_cudl_label("Or. 1080.1.1") == "or1080.11"

    def test_ts_ar(self):
        assert shelfmark_to_cudl_label("T-S Ar. 48.211") == "tsar48.211"

    def test_add(self):
        assert shelfmark_to_cudl_label("Add. 863, 2") == "add863.2"

    def test_uncertain_returns_none_halper(self):
        assert shelfmark_to_cudl_label("Halper 331") is None

    def test_uncertain_returns_none_yevr(self):
        assert shelfmark_to_cudl_label("Yevr. III B 1093") is None

    def test_uncertain_returns_none_ena(self):
        assert shelfmark_to_cudl_label("ENA-MS 2956") is None

    def test_empty(self):
        assert shelfmark_to_cudl_label("") is None
        assert shelfmark_to_cudl_label(None) is None


class TestAliasIndexInMemory:
    def test_mosseri_lookup(self, tmp_path):
        build_alias_index({
            "X1": {"shelfmark": "Moss. III,27O", "library_code": "Mosseri", "call_numbers_raw": ["Moss. III,27O"]},
        }, report_path=tmp_path / "col.csv")
        r = lookup_cudl("mosseriiii27o")
        assert r and r["sys_id"] == "X1"

    def test_or_numeric_collapse_lookup(self, tmp_path):
        build_alias_index({
            "X2": {"shelfmark": "Or. 1080.1.1", "library_code": "CUL", "call_numbers_raw": ["Or. 1080.1.1"]},
        }, report_path=tmp_path / "col.csv")
        r = lookup_cudl("or1080.11")
        assert r and r["sys_id"] == "X2"

    def test_or_expanded_form_resolves_via_tier3(self, tmp_path):
        # Round 3 Codex HIGH #2 regression — lookup_cudl tier-3 collapse retry
        build_alias_index({
            "X2b": {"shelfmark": "Or. 1080.1.1", "library_code": "CUL", "call_numbers_raw": ["Or. 1080.1.1"]},
        }, report_path=tmp_path / "col.csv")
        r = lookup_cudl("or1080.1.1")
        assert r and r["sys_id"] == "X2b"

    def test_forward_label_form_still_resolves(self, tmp_path):
        # Round 3 Codex HIGH #2 regression — Plan 02 tier-2 forward-label fallback
        build_alias_index({
            "X2c": {"shelfmark": "Moss. III,27O", "library_code": "Mosseri", "call_numbers_raw": ["Moss. III,27O"]},
        }, report_path=tmp_path / "col.csv")
        r = lookup_cudl("MS-MOSSERI-III-00027-O")
        assert r and r["sys_id"] == "X2c"

    def test_non_or_numeric_run_not_collapsed(self, tmp_path):
        build_alias_index({
            "X3": {"shelfmark": "X-Z 1.2.3", "library_code": "CUL", "call_numbers_raw": ["X-Z 1.2.3"]},
        }, report_path=tmp_path / "col.csv")
        from shared.shelfmark_bridge import _CUDL_ALIAS_INDEX
        assert "xz1.23" not in _CUDL_ALIAS_INDEX
