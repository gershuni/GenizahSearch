"""Checkpoint WAL journals into main .db files before PyInstaller build.

PyInstaller copies only the .db file, not -wal/-shm journals.
If a sidecar is in WAL mode, data in the journal is lost and the
installed copy may have empty tables.
"""
import sqlite3
import os

SIDECARS = [
    "fist_data/fjms_enrichment.db",
    "fist_data/visual_similarity.db",
    "pgp_data/pgp.db",
    "nli_data/nli_crossref.db",
]

for path in SIDECARS:
    if not os.path.exists(path):
        continue
    try:
        conn = sqlite3.connect(path, timeout=5.0)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.close()
        print(f"  {path}: ok")
    except sqlite3.Error as e:
        # One locked/busy sidecar must not abort the whole pre-build checkpoint
        # (it would silently skip the remaining DBs). Close any running app that
        # holds this DB before building. An empty -wal means the .db is already
        # complete, so a skipped checkpoint is data-safe.
        print(f"  {path}: SKIPPED ({e}) — close any running GenizahSearch app holding this DB")
