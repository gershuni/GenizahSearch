#!/usr/bin/env python3
"""Find saved joins/puzzles whose fragments' Cambridge (CUDL) item changed.

The 2026-10-29 Cambridge decision (shared/cudl_identity.py) moves some records to
another CUDL item or drops their CUDL link. A saved puzzle that stored the old item's
images, or re-resolves its pages, may now show a different image under existing crops.
This lists the saved work that references any such record; it changes nothing.

Usage (from the repo root):

    python scripts/audit_cudl_saved_work.py                       # the local joins.db
    python scripts/audit_cudl_saved_work.py --joins-db PATH       # another joins.db
    python scripts/audit_cudl_saved_work.py --include-added       # also newly linked records

The change list is reports/cudl_resolution_changes_2026-10-29.csv: every record whose
linked CUDL item differs before/after (change = added / corrected / removed). By default
only 'corrected' and 'removed' are audited: an 'added' CUL record keeps its NLI images in
the puzzle (CUL's NLI images are real), and an 'added' Mosseri record had no image before.

Published joins live in Supabase (published_join_fragments.sys_id); the script prints
the query to run there, since it holds no service credentials.
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHANGES = ROOT / "reports" / "cudl_resolution_changes_2026-10-29.csv"


def load_changes(include_added: bool) -> dict:
    with CHANGES.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    keep = {"corrected", "removed"} | ({"added"} if include_added else set())
    return {r["sys_id"]: r for r in rows if r["change"] in keep}


def audit_joins_db(path: Path, changes: dict) -> list:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        hits = []
        marks = ",".join("?" * len(changes))
        for doc_id, title, sys_id in conn.execute(
            "SELECT d.id, d.title, f.sys_id FROM join_document_fragments f "
            "JOIN join_documents d ON d.id = f.doc_id "
            f"WHERE f.sys_id IN ({marks}) ORDER BY d.id", list(changes),
        ):
            c = changes[sys_id]
            hits.append((doc_id, title, sys_id, c["main_name"], c["change"], c["before"], c["after"]))
        return hits
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--joins-db", type=Path, default=ROOT / "joins_data" / "joins.db")
    ap.add_argument("--include-added", action="store_true")
    args = ap.parse_args()

    changes = load_changes(args.include_added)
    print(f"{len(changes)} records audited ({CHANGES.name})")
    if args.joins_db.exists():
        hits = audit_joins_db(args.joins_db, changes)
        print(f"\n{args.joins_db}: {len(hits)} saved fragment(s) on a changed record")
        for h in hits:
            print("  doc={} title={!r} sys_id={} name={!r} {}: {} -> {}".format(*h))
    else:
        print(f"\n{args.joins_db}: not found (no local saved joins to audit)")

    ids = ", ".join(f"'{s}'" for s in sorted(changes))
    print("\nPublished joins -- run in the Supabase SQL editor:")
    print("  SELECT j.id, j.title, f.sys_id, f.shelfmark FROM published_join_fragments f\n"
          "  JOIN published_joins j ON j.id = f.join_id\n"
          f"  WHERE f.sys_id IN ({ids}) ORDER BY j.id;")
    return 0


if __name__ == "__main__":
    sys.exit(main())
