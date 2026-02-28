"""Checkpoint WAL journals into main .db files before PyInstaller build.

PyInstaller copies only the .db file, not -wal/-shm journals.
If a sidecar is in WAL mode, data in the journal is lost and the
installed copy may have empty tables.
"""
import sqlite3
import os

SIDECARS = [
    "fist_data/fjms_enrichment.db",
    "pgp_data/pgp.db",
    "nli_data/nli_crossref.db",
]

for path in SIDECARS:
    if not os.path.exists(path):
        continue
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    print(f"  {path}: ok")
