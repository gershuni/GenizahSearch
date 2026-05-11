"""Audit v7.9.4 NLI Oxford mislabel fix (461 rows flipped 2026-05-04 in libraries.csv).

Run: python scripts/audit_nli_attribution.py
Asserts no row in libraries.csv has library_code='Oxford' AND a call_numbers
field matching the v7.9.4 NLI regex. Returns nonzero exit code on regression.
Read-only scan; does NOT rewrite libraries.csv.
"""
from __future__ import annotations
import csv
import re
import sys
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parent.parent / "libraries.csv"
# v7.9.4 regex from scripts/fix_nli_oxford_mislabel.py (canonical source).
NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)


def main() -> int:
    regressions = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or (row[0] or "").startswith("#"):
                continue
            if row[3] == "Oxford" and NLI_RE.search(row[2] or ""):
                regressions.append((row[0], row[2][:80]))
    if regressions:
        print(
            f"REGRESSION: {len(regressions)} Oxford rows match NLI regex",
            file=sys.stderr,
        )
        for sys_id, calls in regressions[:5]:
            print(f"  {sys_id}  {calls}", file=sys.stderr)
        return 1
    print("OK: no Oxford rows match NLI regex (v7.9.4 fix intact)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
