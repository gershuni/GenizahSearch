"""Fix mislabeled NLI rows in libraries.csv (library_code='Oxford' but call_numbers
contains 'The National Library of Israel' / 'JER NLI Heb').

Usage:
    python scripts/fix_nli_oxford_mislabel.py --dry-run
    python scripts/fix_nli_oxford_mislabel.py --apply
"""
from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parent.parent / "libraries.csv"
NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    flipped: list[tuple[str, str]] = []
    out_rows: list[list[str]] = []

    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 4 and row[3] == "Oxford" and NLI_RE.search(row[2] or ""):
                flipped.append((row[0], row[2]))
                row[3] = "NLI"
            out_rows.append(row)

    print(f"Rows to flip Oxford -> NLI: {len(flipped)}")
    for sys_id, call in flipped[:5]:
        print(f"  {sys_id}  {call[:90]}")
    if len(flipped) > 5:
        print(f"  ... ({len(flipped) - 5} more)")

    if args.dry_run:
        return 0

    backup = CSV_PATH.with_suffix(".csv.bak")
    shutil.copy2(CSV_PATH, backup)
    print(f"Backup: {backup}")

    # Detect dominant line ending in original file to preserve it.
    with CSV_PATH.open("rb") as f:
        sample = f.read(8192)
    line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"

    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator=line_terminator)
        writer.writerows(out_rows)
    print(f"Wrote {CSV_PATH} ({len(out_rows)} rows, {len(flipped)} flipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
