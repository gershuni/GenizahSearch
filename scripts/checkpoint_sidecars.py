"""Checkpoint WAL journals into main .db files before PyInstaller build.

PyInstaller copies only the .db file, not -wal/-shm journals.
If a sidecar is in WAL mode, data in the journal is lost and the
installed copy may have empty tables.
"""
import os
import sqlite3
import sys

SIDECARS = [
    "fist_data/fjms_enrichment.db",
    "fist_data/visual_similarity.db",
    "pgp_data/pgp.db",
    "nli_data/nli_crossref.db",
    "fgp_data/fgp_transcriptions.db",
]


def _wal_size(db_path: str) -> int:
    wal_path = db_path + "-wal"
    return os.path.getsize(wal_path) if os.path.exists(wal_path) else 0


def checkpoint_one(path: str) -> bool:
    """Checkpoint one sidecar. Returns True unless a NON-EMPTY WAL survives it.

    PRAGMA wal_checkpoint(TRUNCATE) can report `busy` in its own result row
    without ever raising -- a caller that only watches for sqlite3.Error would
    call that success. So failure is judged from the row's busy flag AND the
    post-checkpoint WAL size, not from exceptions alone. A sidecar with no WAL
    (or an already-empty one) has nothing to lose and always passes.
    """
    wal_before = _wal_size(path)
    busy = False
    exc = None
    conn = None
    try:
        conn = sqlite3.connect(path, timeout=5.0)
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        busy = bool(row and row[0])
    except sqlite3.Error as e:
        exc = e
    if conn is not None:
        # Best-effort from here down. Dropping WAL mode needs EXCLUSIVE
        # access -- any other open handle on this .db (this process's own
        # idle connections included) makes this specific pragma raise even
        # though the checkpoint above already fully drained the WAL, so it
        # must not be judged as a checkpoint failure the way `exc` above is.
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.Error:
            pass
        conn.close()

    if wal_before == 0:
        # Nothing was at risk; a connect/pragma error here is harmless.
        print(f"  {path}: ok" if exc is None else
              f"  {path}: ok (WAL already empty; {exc})")
        return True

    wal_after = _wal_size(path)
    if exc is not None or busy or wal_after > 0:
        reason = f"error: {exc}" if exc is not None else (
            f"busy={busy}, {wal_after} bytes remain in WAL")
        print(f"  {path}: FAILED ({reason}) -- close any running "
              f"GenizahSearch app holding this DB")
        return False

    print(f"  {path}: ok")
    return True


def main(sidecars=SIDECARS) -> int:
    ok = True
    for path in sidecars:
        if not os.path.exists(path):
            continue
        if not checkpoint_one(path):
            ok = False
    if not ok:
        print("checkpoint FAILED for one or more sidecars with a non-empty "
              "WAL -- aborting so the build does not package stale data")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
