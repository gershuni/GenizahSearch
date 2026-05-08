"""Phase 85 SYNTH-02 + SYNTH-03 regeneration script tests.

Covers:
  - csv_bank marker-block tolerance (TestLoaderMarkerTolerance)
  - generate_synthetic_rows.py idempotency, collision-detection, ambiguity-residue
    (XFAIL placeholders below until Task 2 ships the script)

Per Phase 84 lesson (Round 3 Codex MEDIUM): NEVER mutate real libraries.csv,
real reports/, or real fist_data/ from tests. Use tmp_path fixture for all writes.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# csv_bank marker-block tolerance (genizah_core.py:_load_csv_bank guard)
# ---------------------------------------------------------------------------


def _write_libraries_csv(path: Path, rows: list[list[str]]) -> None:
    """Write a libraries.csv-shaped CSV (8 columns, header) to `path`."""
    header = [
        "system_number",
        "oxford_part_id",
        "call_numbers",
        "library_code",
        "",
        "",
        "",
        "titles_non_placeholder",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


class TestLoaderMarkerTolerance:
    def test_loader_skips_marker_lines(self, tmp_path, monkeypatch):
        """A '# BEGIN SYNTHETIC' line in column 0 must not appear in csv_bank."""
        from genizah_core import Config, MetadataManager

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["990025143260205171", "", "T-S 1.1", "CUL", "", "", "", "Test title"],
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "Synthetic Title"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        assert "990025143260205171" in mm.csv_bank
        assert "990001234560000000" in mm.csv_bank
        # Marker lines must not produce a key
        assert "" not in mm.csv_bank
        assert all(not k.startswith("#") for k in mm.csv_bank)

    def test_loader_preserves_real_rows(self, tmp_path, monkeypatch):
        """3 real + 2 markers + 2 synthetic = csv_bank with exactly 5 entries."""
        from genizah_core import Config, MetadataManager

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["990025143260205171", "", "T-S 1.1", "CUL", "", "", "", "Real 1"],
            ["990025143270205171", "", "T-S 1.2", "CUL", "", "", "", "Real 2"],
            ["990025143280205171", "", "T-S 1.3", "CUL", "", "", "", "Real 3"],
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "Synth 1"],
            ["990001234570000000", "", "T-S NS 329.97", "CUL", "", "", "", "Synth 2"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        assert len(mm.csv_bank) == 5

    def test_loader_synthetic_rows_have_shelfmark(self, tmp_path, monkeypatch):
        """SYNTH-03: csv_bank entries for synthetic IDs have populated shelfmark + title (Pitfall 5)."""
        from genizah_core import Config, MetadataManager
        from shared.synthetic_sys_id import is_synthetic_sys_id

        csv_path = tmp_path / "libraries.csv"
        rows = [
            ["# BEGIN SYNTHETIC", "", "", "", "", "", "", ""],
            ["990001234560000000", "", "T-S NS 329.96", "CUL", "", "", "", "מילון תלמודי"],
            ["# END SYNTHETIC", "", "", "", "", "", "", ""],
        ]
        _write_libraries_csv(csv_path, rows)

        monkeypatch.setattr(Config, "LIBRARIES_CSV", str(csv_path))
        mm = MetadataManager()
        mm._load_csv_bank()

        entry = mm.csv_bank["990001234560000000"]
        assert is_synthetic_sys_id("990001234560000000")
        assert entry["shelfmark"] == "T-S NS 329.96"
        assert entry["title"] == "מילון תלמודי"
        assert entry["library_code"] == "CUL"


# ---------------------------------------------------------------------------
# Placeholder XFAILs for script tests (Task 2 implements)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(reason="Task 2: generate_synthetic_rows.py not yet implemented")
def test_idempotent_regeneration_xfail():
    raise NotImplementedError("Task 2 ships scripts/generate_synthetic_rows.py + flips this to a real test")


@pytest.mark.xfail(reason="Task 2: collision-check pending")
def test_no_collision_with_real_alma_xfail():
    raise NotImplementedError("Task 2")


@pytest.mark.xfail(reason="Task 2: ambiguity-residue (multi-inventory + multi-signature) pending")
def test_ambiguity_residue_logged_xfail():
    raise NotImplementedError("Task 2")


@pytest.mark.xfail(reason="Task 2: marker-block round-trip pending")
def test_marker_block_round_trip_xfail():
    raise NotImplementedError("Task 2")


@pytest.mark.xfail(reason="Task 2: csv-injection fail-loud pending (Codex MEDIUM)")
def test_csv_injection_fail_loud_xfail():
    raise NotImplementedError("Task 2")


@pytest.mark.xfail(reason="Task 2: manifest authority for Plan 03 pending")
def test_manifest_is_authoritative_xfail():
    raise NotImplementedError("Task 2")
