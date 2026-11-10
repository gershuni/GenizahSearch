"""v7.9.4 regression test (AUDIT-03 D-10): canonical 461 NLI-flipped rows
must stay library_code='NLI' (Pass 2 MEDIUM-4 — canonical fixture from v7.9.4
fix-commit replay).
"""
from __future__ import annotations
import csv
import re
from pathlib import Path

import pytest

CSV_PATH = Path(__file__).resolve().parent.parent / "libraries.csv"
NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)
GOLDEN_FIXTURE = Path(__file__).parent / "fixtures" / "v7_9_4_nli_flipped_sys_ids.txt"


@pytest.fixture(scope="module")
def libraries_csv_data():
    rows_by_sysid = {}
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or (row[0] or "").startswith("#"):
                continue
            rows_by_sysid[row[0]] = row
    return rows_by_sysid


@pytest.fixture(scope="module")
def golden_sysids():
    if not GOLDEN_FIXTURE.exists():
        pytest.skip(f"Fixture file missing: {GOLDEN_FIXTURE}")
    return [
        s.strip()
        for s in GOLDEN_FIXTURE.read_text(encoding="utf-8").splitlines()
        if s.strip()
    ]


def test_golden_fixture_size(golden_sysids):
    """Pass 2 MEDIUM-4: canonical fixture has exactly 461 sys_ids."""
    assert len(golden_sysids) == 461, (
        f"Expected exactly 461 canonical sys_ids, got {len(golden_sysids)}"
    )


def test_nli_flipped_rows_unchanged(libraries_csv_data, golden_sysids):
    """Each of the canonical 461 sys_ids must remain library_code='NLI'."""
    missing = []
    regressions = []
    for sys_id in golden_sysids:
        row = libraries_csv_data.get(sys_id)
        if row is None:
            missing.append(sys_id)
            continue
        if row[3] != "NLI":
            regressions.append((sys_id, row[3]))
    assert not missing, (
        f"sys_ids missing from libraries.csv: {missing[:5]} (total {len(missing)})"
    )
    assert not regressions, (
        f"v7.9.4 regression: {len(regressions)} rows wrong library_code; "
        f"sample: {regressions[:5]}"
    )


def test_no_new_oxford_with_nli_text(libraries_csv_data):
    """Catch-all: no Oxford-coded row should match the v7.9.4 NLI regex."""
    regressions = [
        sys_id
        for sys_id, row in libraries_csv_data.items()
        if row[3] == "Oxford" and NLI_RE.search(row[2] or "")
    ]
    assert not regressions, (
        f"v7.9.4 regression: {len(regressions)} Oxford rows match NLI regex; "
        f"sample: {regressions[:5]}"
    )
