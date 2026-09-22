#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Put a withheld ``pgp_translations`` table back into ``pgp_data/pgp.db``.

WHY THIS EXISTS
---------------
On 2026-09-21 the regenerated Hebrew translations were lifted out of the shipping sidecar by
owner decision: a 180-row audit graded **27.8% of them materially wrong** (95% CI 21.2-34.3%),
and ``GenizahSearchPro.spec`` bundles ``pgp_data\\pgp.db`` into every desktop installer, so
leaving the table in place would have shipped them to users even though the website never got
them. The full run was preserved rather than deleted -- 35,111 rows, 15.1 hours of API time --
because it is the measurement baseline any remediation has to beat. The finding and the four
remediation options are in ``docs/plans/PGP_TRANSLATION_QUALITY.md``.

So: this script is the other half of that decision. It exists so the withheld data is a
documented, reversible state rather than an 18 MB file nobody remembers the meaning of.

**Restoring the 2026-09-21 sidecar puts known-bad translations back in front of users.** Do it to
measure against, or once a better corpus has replaced its contents -- not to ship as it stands.

USAGE
-----
    python scripts/restore_pgp_translations.py --dry-run
    python scripts/restore_pgp_translations.py
    python scripts/restore_pgp_translations.py --source <other.db> --pgp-db <other pgp.db>

Exit codes: 0 done (or dry run), 1 refused, 2 bad input.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PGP_DB = os.path.join(PROJECT_ROOT, "pgp_data", "pgp.db")
DEFAULT_SOURCE = os.path.join(
    PROJECT_ROOT, "pgp_data", "pgp_translations_withheld_2026-09-21.db"
)

COLUMNS = ("pgpid", "description_he", "document_type_he", "translated_at", "model_version")

SCHEMA = """
CREATE TABLE IF NOT EXISTS pgp_translations (
    pgpid INTEGER PRIMARY KEY,
    description_he TEXT,
    document_type_he TEXT,
    translated_at TEXT,
    model_version TEXT DEFAULT 'dictalm2.0'
)
"""


def _ro(path: str) -> sqlite3.Connection:
    """Read-only connection. A bare connect() on a missing path creates a 0-byte stub that later
    reads as 'no such table' -- a trap this repo has hit before."""
    return sqlite3.connect("file:%s?mode=ro" % path.replace("\\", "/"), uri=True)


def fingerprint(conn: sqlite3.Connection) -> tuple:
    """(row count, sha256 over every column, pgpid order) -- so 'verified' means content."""
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute(
        "SELECT %s FROM pgp_translations ORDER BY pgpid" % ", ".join(COLUMNS)
    ):
        digest.update(repr(row).encode("utf-8"))
        count += 1
    return count, digest.hexdigest()


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE,
                        help="the withheld sidecar to restore from (default: %(default)s)")
    parser.add_argument("--pgp-db", default=DEFAULT_PGP_DB,
                        help="the sidecar to restore INTO (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would happen and change nothing")
    parser.add_argument("--force", action="store_true",
                        help="replace an existing pgp_translations table instead of refusing")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    for path, label in ((args.source, "source"), (args.pgp_db, "target")):
        if not os.path.exists(path):
            print("ERROR: %s does not exist: %s" % (label, path), file=sys.stderr)
            return 2

    src = _ro(args.source)
    try:
        if not table_exists(src, "pgp_translations"):
            print("ERROR: %s has no pgp_translations table" % args.source, file=sys.stderr)
            return 2
        n_src, hash_src = fingerprint(src)
        note = None
        if table_exists(src, "withheld_note"):
            row = src.execute("SELECT note FROM withheld_note").fetchone()
            note = row[0] if row else None
        rows = src.execute(
            "SELECT %s FROM pgp_translations ORDER BY pgpid" % ", ".join(COLUMNS)
        ).fetchall()
    finally:
        src.close()

    print("source: %s" % args.source)
    print("        %d rows, sha256 %s" % (n_src, hash_src[:16]))
    if note:
        print("        note: %s" % note)

    tgt_ro = _ro(args.pgp_db)
    try:
        existing = None
        if table_exists(tgt_ro, "pgp_translations"):
            existing = fingerprint(tgt_ro)
    finally:
        tgt_ro.close()

    print("target: %s" % args.pgp_db)
    if existing is None:
        print("        no pgp_translations table (this is the withheld state)")
    else:
        print("        already has %d rows, sha256 %s" % (existing[0], existing[1][:16]))
        if existing == (n_src, hash_src):
            print("Nothing to do: the target already holds exactly this data.")
            return 0
        if not args.force:
            print(
                "\nREFUSING: the target already has a pgp_translations table with different "
                "content.\nPass --force to replace it, after deciding which corpus you want.",
                file=sys.stderr,
            )
            return 1

    if args.dry_run:
        print("\nDry run: would write %d rows. Nothing changed." % n_src)
        return 0

    # Stage into a side table and swap at the end. The previous version dropped the
    # existing table FIRST and then inserted, so an interrupted or failing insert left
    # the old corpus destroyed -- in the one script whose entire job is to make that
    # corpus recoverable. DDL in SQLite is transactional, so the swap is all-or-nothing.
    conn = sqlite3.connect(args.pgp_db)
    try:
        conn.execute("DROP TABLE IF EXISTS pgp_translations_restore_tmp")
        conn.execute(SCHEMA.replace("pgp_translations", "pgp_translations_restore_tmp"))
        conn.executemany(
            "INSERT INTO pgp_translations_restore_tmp (%s) VALUES (%s)"
            % (", ".join(COLUMNS), ", ".join("?" * len(COLUMNS))),
            rows,
        )

        staged = conn.execute(
            "SELECT COUNT(*) FROM pgp_translations_restore_tmp"
        ).fetchone()[0]
        if staged != n_src:
            conn.rollback()
            conn.execute("DROP TABLE IF EXISTS pgp_translations_restore_tmp")
            conn.commit()
            print("ERROR: staged %d of %d rows; target left untouched"
                  % (staged, n_src), file=sys.stderr)
            return 1

        with conn:
            if existing is not None:
                conn.execute("DROP TABLE pgp_translations")
            conn.execute(
                "ALTER TABLE pgp_translations_restore_tmp RENAME TO pgp_translations"
            )
        n_new, hash_new = fingerprint(conn)
    except Exception:
        try:
            conn.execute("DROP TABLE IF EXISTS pgp_translations_restore_tmp")
            conn.commit()
        except sqlite3.Error:
            pass
        conn.close()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if (n_new, hash_new) != (n_src, hash_src):
        print(
            "ERROR: restored table does not match the source (%d/%s vs %d/%s)"
            % (n_new, hash_new[:16], n_src, hash_src[:16]),
            file=sys.stderr,
        )
        return 1

    print("\nRestored %d rows, verified row-for-row against the source." % n_new)
    print(
        "REMINDER: the 2026-09-21 corpus is ~28% materially wrong "
        "(docs/plans/PGP_TRANSLATION_QUALITY.md). Do not ship it as it stands."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
