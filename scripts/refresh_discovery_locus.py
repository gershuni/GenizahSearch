#!/usr/bin/env python3
"""Replace only the locus layer of an already-built private discovery asset.

This is the fast path for a division-table correction whose reference corpus,
claims, routing, domains, and frame are unchanged.  It copies the input asset,
empties the three imported locus tables, re-runs the production importer, and
re-materializes both claim- and identification-grain labels plus filter pieces.
The public projection must still be rerun afterwards; it recomputes those
grains again after masking.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_discovery_sidecar import (
    LOCUS_SCHEMA_VERSION,
    ingest_locus_divisions,
    locus_display_meta_rows,
    materialize_locus_labels,
)


def refresh_locus(
    source_db: Path,
    output_db: Path,
    locus_db: Path,
    crosswalk_path: Path,
    reference_corpus_sha256: str,
    locus_sha256: str,
) -> dict:
    if source_db.resolve() == output_db.resolve():
        raise ValueError("locus refresh requires a distinct output path")
    for path in (source_db, locus_db, crosswalk_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    output_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_db, output_db)

    with crosswalk_path.open(encoding="utf-8") as stream:
        document = json.load(stream)
    crosswalk = document.get("crosswalk", document)
    if not isinstance(crosswalk, dict):
        raise ValueError("crosswalk must be a JSON mapping")

    conn = sqlite3.connect(output_db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM discovery_locus_piece")
        for table in ("locus_edition", "locus_unit", "locus_work"):
            conn.execute(f"DELETE FROM {table}")
        conn.execute(
            "DELETE FROM meta WHERE key LIKE 'locus_%' "
            "OR key LIKE 'expected_locus_%' "
            "OR key IN ('expected_rows_locus_work', 'expected_rows_locus_unit', "
            "'expected_rows_locus_edition', 'expected_rows_discovery_locus_piece')"
        )
        input_meta = ingest_locus_divisions(
            conn,
            str(locus_db),
            crosswalk,
            reference_corpus_sha256,
            expected_sha256=locus_sha256,
        )
        status_counts = materialize_locus_labels(conn)
        rows = [("locus_schema_version", LOCUS_SCHEMA_VERSION), *input_meta]
        for table in ("locus_work", "locus_unit", "locus_edition"):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            rows.append((f"expected_rows_{table}", str(count)))
        rows.extend(locus_display_meta_rows(conn))
        conn.executemany(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", rows
        )
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != "ok" or foreign_keys:
            raise ValueError(
                f"refreshed asset failed SQLite checks: integrity={integrity!r}, "
                f"foreign_keys={len(foreign_keys)}"
            )
        conn.commit()
        return {
            "locus_work": conn.execute("SELECT COUNT(*) FROM locus_work").fetchone()[0],
            "locus_unit": conn.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0],
            "locus_edition": conn.execute(
                "SELECT COUNT(*) FROM locus_edition"
            ).fetchone()[0],
            **status_counts,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_db")
    parser.add_argument("--out", required=True)
    parser.add_argument("--locus-divisions", required=True)
    parser.add_argument("--locus-sha256", required=True)
    parser.add_argument("--crosswalk", required=True)
    parser.add_argument("--reference-corpus-sha256", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = refresh_locus(
        Path(args.source_db),
        Path(args.out),
        Path(args.locus_divisions),
        Path(args.crosswalk),
        args.reference_corpus_sha256,
        args.locus_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
