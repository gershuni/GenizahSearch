"""Phase 84 Plan 02: ambiguity-policy unit tests for shared.shelfmark_bridge.

Codex HIGH #2: build_alias_index must EXCLUDE keys whose normalized form maps to
>1 distinct sys_id, and write them to a CSV report.

Round 3 Codex MEDIUM: tests use the report_path parameter (added to build_alias_index
in Plan 02 Round 3) so they do NOT mutate reports/cudl_alias_collisions.csv. This
keeps the working tree clean and preserves the real diagnostic artifact.
"""
import csv
from pathlib import Path

import pytest

from shared.shelfmark_bridge import build_alias_index, lookup_cudl


def _read_collisions_keys(path: Path):
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8", newline="") as f:
        return {row["key"] for row in csv.DictReader(f) if row.get("key")}


class TestAmbiguityExclusion:
    def test_two_distinct_sys_ids_same_key_are_excluded(self, tmp_path):
        report = tmp_path / "collisions.csv"
        build_alias_index({
            "9A": {"shelfmark": "T-S Y 1", "library_code": "CUL", "call_numbers_raw": ["T-S Y 1"]},
            "9B": {"shelfmark": "T-S Y 1", "library_code": "CUL", "call_numbers_raw": ["T-S Y 1"]},
        }, report_path=report)
        assert lookup_cudl("tsy1") is None
        assert "tsy1" in _read_collisions_keys(report)

    def test_same_sys_id_multiple_paths_is_not_ambiguous(self, tmp_path):
        report = tmp_path / "collisions.csv"
        build_alias_index({
            "9C": {
                "shelfmark": "Moss. III,27O",
                "library_code": "Mosseri",
                "call_numbers_raw": ["Moss. III,27O", "Moss III 27 O"],
            },
        }, report_path=report)
        r = lookup_cudl("mosseriiii27o")
        assert r is not None and r["sys_id"] == "9C"

    def test_collision_report_header(self, tmp_path):
        report = tmp_path / "collisions.csv"
        build_alias_index({
            "9D": {"shelfmark": "T-S Z 1", "library_code": "CUL", "call_numbers_raw": ["T-S Z 1"]},
            "9E": {"shelfmark": "T-S Z 1", "library_code": "CUL", "call_numbers_raw": ["T-S Z 1"]},
        }, report_path=report)
        assert report.exists()
        first_line = report.read_text(encoding="utf-8").splitlines()[0]
        assert first_line == "key,sys_ids,shelfmarks"

    def test_three_way_ambiguity_excluded(self, tmp_path):
        report = tmp_path / "collisions.csv"
        build_alias_index({
            "9F": {"shelfmark": "T-S Q 1", "library_code": "CUL", "call_numbers_raw": ["T-S Q 1"]},
            "9G": {"shelfmark": "T-S Q 1", "library_code": "CUL", "call_numbers_raw": ["T-S Q 1"]},
            "9H": {"shelfmark": "T-S Q 1", "library_code": "CUL", "call_numbers_raw": ["T-S Q 1"]},
        }, report_path=report)
        assert lookup_cudl("tsq1") is None
        assert "tsq1" in _read_collisions_keys(report)

    def test_real_reports_dir_not_mutated(self, tmp_path):
        """Round 3 Codex MEDIUM regression check — using report_path keeps
        reports/cudl_alias_collisions.csv unchanged across this test run."""
        real = Path(__file__).resolve().parent.parent / "reports" / "cudl_alias_collisions.csv"
        before = real.read_bytes() if real.exists() else None
        report = tmp_path / "collisions.csv"
        build_alias_index({
            "9X": {"shelfmark": "T-S A 1", "library_code": "CUL", "call_numbers_raw": ["T-S A 1"]},
            "9Y": {"shelfmark": "T-S A 1", "library_code": "CUL", "call_numbers_raw": ["T-S A 1"]},
        }, report_path=report)
        after = real.read_bytes() if real.exists() else None
        assert before == after, "real reports/cudl_alias_collisions.csv was mutated by a unit test"
