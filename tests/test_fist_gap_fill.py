"""Tests for Phase 53 FIST gap fill: CSV format, normalization, library codes, search guard.

Tests cover:
1. fist_gap_rows.csv structural integrity (column count, dedup, library codes)
2. Manifest/CSV alignment
3. Shelfmark normalization for Yevr/Halper aliases
4. LIBRARY_CODES completeness for gap set
5. Metadata-only search results (guard fix)
6. Text results retain metadata_only=False
"""

import csv
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


# Ensure project root is importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from genizah_core import normalize_shelfmark, LIBRARY_CODES, SearchEngine


# ── Paths ──

CSV_PATH = ROOT / "fist_gap_rows.csv"
MANIFEST_PATH = ROOT / "fist_gap_manifest.txt"


# ── Test 1: CSV format validation ──

def test_csv_format():
    """fist_gap_rows.csv has 8 columns per row, no duplicate AlmaIds, valid library codes."""
    assert CSV_PATH.exists(), f"Missing {CSV_PATH}"

    seen_ids = set()
    duplicates = []
    bad_cols = []
    bad_lib_codes = []

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert len(header) == 8, f"Header has {len(header)} columns, expected 8"

        for row_num, row in enumerate(reader, start=2):
            if len(row) != 8:
                bad_cols.append((row_num, len(row)))
            alma_id = row[0]
            if alma_id in seen_ids:
                duplicates.append(alma_id)
            seen_ids.add(alma_id)

            lib_code = row[3]
            if lib_code and lib_code not in LIBRARY_CODES:
                bad_lib_codes.append((row_num, lib_code))

    assert not bad_cols, f"Rows with wrong column count: {bad_cols[:5]}"
    assert not duplicates, f"Duplicate AlmaIds: {duplicates[:5]}"
    assert not bad_lib_codes, f"Unknown library codes: {bad_lib_codes[:5]}"
    assert len(seen_ids) == 38673, f"Expected 38,673 data rows, got {len(seen_ids)}"


# ── Test 2: Manifest matches CSV ──

def test_manifest_matches_csv():
    """fist_gap_manifest.txt AlmaIds match fist_gap_rows.csv AlmaIds exactly."""
    assert MANIFEST_PATH.exists(), f"Missing {MANIFEST_PATH}"
    assert CSV_PATH.exists(), f"Missing {CSV_PATH}"

    manifest_ids = set()
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                manifest_ids.add(line)

    csv_ids = set()
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            csv_ids.add(row[0])

    assert manifest_ids == csv_ids, (
        f"Manifest/CSV mismatch: "
        f"{len(manifest_ids - csv_ids)} in manifest only, "
        f"{len(csv_ids - manifest_ids)} in CSV only"
    )


# ── Test 3: normalize_shelfmark Yevr aliases ──

def test_normalize_yevr():
    """Yevr. shelfmarks normalize to EVR prefix (real normalize_shelfmark, not mocked)."""
    assert normalize_shelfmark("Yevr. II B 1563") == "evriib1563"
    assert normalize_shelfmark("MS Yevr. II B") == "evriib"
    assert normalize_shelfmark("Yevr.-Arab. 123") == "evrarab123"


# ── Test 4: normalize_shelfmark Halper vs Halpern ──

def test_normalize_halper_vs_halpern():
    """Halper normalizes to genizah prefix, but Halpern stays as-is."""
    assert normalize_shelfmark("Halper 100") == "genizah100"
    assert normalize_shelfmark("Halpern 5") == "halpern5"
    assert normalize_shelfmark("Ms. Halper 100") == "genizah100"


# ── Test 5: LIBRARY_CODES completeness for gap set ──

def test_library_codes_complete():
    """All library codes used in fist_gap_rows.csv exist in LIBRARY_CODES."""
    assert CSV_PATH.exists(), f"Missing {CSV_PATH}"

    gap_codes = set()
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) > 3 and row[3]:
                gap_codes.add(row[3])

    missing = gap_codes - set(LIBRARY_CODES.keys())
    assert not missing, f"Library codes in gap CSV but not in LIBRARY_CODES: {missing}"


# ── Test 6: Metadata-only result structure ──

def test_metadata_only_result_structure():
    """Metadata search for a record with no Tantivy text returns metadata_only=True result."""
    # Create a mock meta_mgr -- get_meta_for_id returns (shelfmark, title) tuple
    meta_mgr = MagicMock()
    meta_mgr.search_by_meta.return_value = ["999999999"]
    meta_mgr.get_meta_for_id.return_value = ('T-S Test 1.1', 'Test Title')
    meta_mgr.get_library_for_id.return_value = 'CUL'

    # Create SearchEngine with mocked dependencies
    with patch.object(SearchEngine, '__init__', lambda self, *a, **kw: None):
        engine = SearchEngine.__new__(SearchEngine)
        engine.meta_mgr = meta_mgr
        engine.searcher = None  # No Tantivy index
        engine.index = None

        results = engine._execute_metadata_search("test", "Title")

    assert len(results) == 1
    r = results[0]
    assert r['metadata_only'] is True
    assert r['display']['shelfmark'] == 'T-S Test 1.1'
    assert r['display']['title'] == 'Test Title'
    assert r['display']['library_code'] == 'CUL'
    assert r['display']['id'] == '999999999'
    assert r['snippet'] == ''
    assert r['full_text'] == ''
    assert r['uid'] == ''


# ── Test 7: Text result has metadata_only=False ──

def test_text_result_has_metadata_only_false():
    """Metadata search for a record WITH Tantivy text returns metadata_only=False."""
    meta_mgr = MagicMock()
    meta_mgr.search_by_meta.return_value = ["123456"]
    meta_mgr.get_display_data.return_value = {
        'shelfmark': 'T-S 12.345',
        'title': 'Some Title',
        'img': '1',
        'source': 'V0.8',
        'id': '123456',
        'library_code': 'CUL',
    }

    with patch.object(SearchEngine, '__init__', lambda self, *a, **kw: None):
        engine = SearchEngine.__new__(SearchEngine)
        engine.meta_mgr = meta_mgr
        engine.searcher = MagicMock()  # Tantivy available
        engine.index = MagicMock()

        # Mock _get_best_text_for_id to return text
        engine._get_best_text_for_id = MagicMock(return_value=(
            "Sample transcription text content here",
            "full_header_123456_1",
            "V0.8",
            "uid_12345"
        ))

        results = engine._execute_metadata_search("test", "Title")

    assert len(results) == 1
    r = results[0]
    assert r['metadata_only'] is False
    assert r['full_text'] == "Sample transcription text content here"
    assert r['uid'] == "uid_12345"
    assert r['snippet'] == "Sample transcription text content here"
