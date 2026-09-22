#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Restore ``documents.transcription`` values a bulk upsert erased.

WHY THIS EXISTS
---------------
``scripts/import_pgp_full.py::prepare_document_records`` omits ``transcription`` and
``transcription_source`` for documents with no edition content in the current CSV, so that
an upsert leaves whatever is already stored alone. That contract did not survive batching:
PostgREST sends one request per batch whose column list is the **union** of the payload's
keys, so a record that omitted a key had NULL written for it as soon as another record in
the same batch carried that key.

The 2026-09-22 refresh lost both fields on eight documents this way::

    897, 4093, 7336, 11073, 11265, 11266, 26420, 38240

``scripts/import_pgp_full.py`` now batches by column set, so it cannot recur. But
**re-running the import does not repair the existing losses** -- omitting the key now
faithfully preserves the NULL. Hence this script.

It restores from a baseline sidecar (by default the pinned pre-refresh snapshot), and only
for documents that (a) had a value in the baseline and (b) have none now. It never
overwrites a populated field, so it cannot undo a legitimate refresh.

USAGE
-----
    python scripts/repair_lost_transcriptions.py --dry-run
    python scripts/repair_lost_transcriptions.py --execute

Exit codes: 0 done (or dry run, or nothing to do), 1 failure, 2 bad input.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

from dotenv import load_dotenv

load_dotenv()

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase not installed. Run: pip install supabase", file=sys.stderr)
    sys.exit(1)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BASELINE = os.path.join(
    PROJECT_ROOT, "pgp_data", "pgp_APRIL_SNAPSHOT_pre_2026-09-22_import.db"
)


def _ro(path: str) -> sqlite3.Connection:
    """Read-only. A bare connect() on a missing path creates a 0-byte stub that later
    reads as 'no such table'."""
    return sqlite3.connect("file:%s?mode=ro" % path.replace("\\", "/"), uri=True)


def find_losses(baseline_path: str, client) -> list:
    """Documents that had a transcription in the baseline and have none now."""
    baseline = _ro(baseline_path)
    try:
        had = {
            row[0]: (row[1], row[2])
            for row in baseline.execute(
                "SELECT pgpid, transcription, transcription_source FROM documents "
                "WHERE transcription IS NOT NULL AND transcription != ''"
            )
        }
    finally:
        baseline.close()

    losses = []
    ids = sorted(had)
    # PostgREST caps the length of an in() list, so ask in chunks.
    for start in range(0, len(ids), 200):
        chunk = ids[start:start + 200]
        rows = (
            client.table("documents")
            .select("pgpid,transcription,transcription_source")
            .in_("pgpid", chunk)
            .execute()
            .data
        )
        for row in rows:
            if row.get("transcription"):
                continue  # still populated -- nothing to restore
            text, scholar = had[row["pgpid"]]
            losses.append({
                "pgpid": row["pgpid"],
                "transcription": text,
                "transcription_source": scholar,
            })
    return losses


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="report what would be restored and change nothing (default)")
    group.add_argument("--execute", action="store_true",
                       help="actually restore the values")
    parser.add_argument("--baseline", default=DEFAULT_BASELINE,
                        help="sidecar to restore FROM (default: %(default)s)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    dry_run = not args.execute

    if not os.path.exists(args.baseline):
        print("ERROR: baseline not found: %s" % args.baseline, file=sys.stderr)
        return 2

    url = os.environ.get("SUPABASE_URL", "https://ylcpglwxompwjcufdemz.supabase.co")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        print("ERROR: SUPABASE_SERVICE_KEY is not set (required to write).", file=sys.stderr)
        return 2
    client = create_client(url, key)

    print("Baseline: %s" % args.baseline)
    print("Target:   %s" % url)
    print()

    losses = find_losses(args.baseline, client)
    if not losses:
        print("Nothing to repair: every baseline transcription is still present.")
        return 0

    print("%d document(s) lost their transcription and need restoring:" % len(losses))
    for item in losses:
        print("  pgpid %-8d %-44.44s %d chars"
              % (item["pgpid"], item["transcription_source"] or "(no source)",
                 len(item["transcription"] or "")))
    print()

    if dry_run:
        print("Dry run: nothing written. Re-run with --execute to restore.")
        return 0

    for item in losses:
        client.table("documents").update({
            "transcription": item["transcription"],
            "transcription_source": item["transcription_source"],
        }).eq("pgpid", item["pgpid"]).execute()

    # Read back rather than trusting the writes.
    remaining = find_losses(args.baseline, client)
    if remaining:
        print("ERROR: %d document(s) still missing their transcription:" % len(remaining),
              file=sys.stderr)
        for item in remaining:
            print("  pgpid %d" % item["pgpid"], file=sys.stderr)
        return 1

    print("Restored %d document(s), verified by reading them back." % len(losses))
    print("Rebuild the sidecar (scripts/export_pgp_sidecar.py) so it carries the repair.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
