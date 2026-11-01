# -*- coding: utf-8 -*-
"""Upgrade an FJMS sidecar from 5.0.x to 5.1.0: the catalogue criteria data.

Step 4 of the build sequence in docs/guides/DEPLOYMENT_TECHNICAL.md
("Catalogue sidecar 5.1.0"); docs/plans/CATALOGUE_CRITERIA_PLAN.md §6.5.
Reads only the file's own tables (no FIST.db), and adds:

  - criteria_numbers(AlmaId, UnitCatalogRecId, key, lo, hi): the catalogue's
    line and column counts (catalog.NumRow / NumColumn, free text) parsed by
    shared/catalog_numbers.py::parse_number_text (ruling D7), one row per
    interval, key 'lines' or 'columns'; and each record's copy-date span,
    key 'period' (copy_span, ruling D18); index (key, lo, hi, AlmaId);
  - criteria_values(AlmaId, UnitCatalogRecId, key, value): the coded catalog
    columns as values, key 'vocal_extent' (PartVocalCode -> code_values
    label) and 'attributes' (shared/catalog_criteria.py::ATTRIBUTES); index
    (key, value, AlmaId). Without these two tables each such lookup scans
    the whole catalog table (2-14 s);
  - index catalog_fields(FieldCategory, FieldValue, AlmaId) for the list
    criteria;
  - manuscript_measurements.avg_line_height_mm, via
    scripts/import_measurements.py::migrate_add_line_height (the 5.0.0 file
    lacks it, so today's Line Height filter matches nothing);
  - meta.version = 5.1.0, meta.criteria_built_at.

Refuses (exit 2) a file whose version is not 5.0.x or 5.1.0, and (exit 3) a
file that still has 'Instatution' source rows: run
scripts/fix_instatution_sources.py first. On a complete 5.1.0 file it does
nothing (exit 0); an incomplete 5.1.0 is refused (exit 2) -- rebuild it from
its 5.0.x base.

Usage
-----
    python scripts/migrate_fjms_criteria.py --db COPY.db            # dry run
    python scripts/migrate_fjms_criteria.py --db COPY.db --apply    # write
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from shared.catalog_criteria import ATTRIBUTES, stored_code  # noqa: E402
from shared.catalog_numbers import copy_span, parse_number_text  # noqa: E402

DEFAULT_DB = Path("fist_data/fjms_enrichment.db")
TARGET_VERSION = "5.1.0"
NUMBER_COLUMNS = (("lines", "NumRow"), ("columns", "NumColumn"))
NUMBERS_INDEX = "idx_critnum_key_range"
VALUES_INDEX = "idx_critval_key_value"
FIELDS_INDEX = "idx_catfld_cat_val"
VOCAL_EXTENT_TABLE = 36  # code_values table of catalog.PartVocalCode

EXIT_OK, EXIT_REFUSED, EXIT_INSTATUTION = 0, 2, 3


def log(msg: str = "") -> None:
    print(msg, flush=True)


def meta_version(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM meta WHERE key = 'version'").fetchone()
    return row[0] if row else ""


def has_table(conn, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def has_index(conn, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?", (name,)
    ).fetchone() is not None


def has_column(conn, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def missing_parts(conn) -> list[str]:
    """What a 5.1.0 file must hold that this one does not (empty = complete)."""
    missing = []
    if not has_table(conn, "criteria_numbers"):
        missing.append("table criteria_numbers")
    elif not conn.execute("SELECT 1 FROM criteria_numbers LIMIT 1").fetchone():
        missing.append("rows in criteria_numbers")
    if not has_index(conn, NUMBERS_INDEX):
        missing.append(f"index {NUMBERS_INDEX}")
    if not has_table(conn, "criteria_values"):
        missing.append("table criteria_values")
    if not has_index(conn, VALUES_INDEX):
        missing.append(f"index {VALUES_INDEX}")
    if not has_index(conn, FIELDS_INDEX):
        missing.append(f"index {FIELDS_INDEX}")
    if has_table(conn, "manuscript_measurements") and not has_column(
            conn, "manuscript_measurements", "avg_line_height_mm"):
        missing.append("column manuscript_measurements.avg_line_height_mm")
    return missing


def instatution_rows(conn) -> int:
    total = 0
    for table in ("catalog", "catalog_free_desc"):
        if has_table(conn, table) and has_column(conn, table, "SourceName"):
            total += conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE SourceName = 'Instatution'"
            ).fetchone()[0]
    return total


def number_rows(conn) -> list[tuple]:
    """(AlmaId, UnitCatalogRecId, key, lo, hi) for every parsed interval and
    every dated record."""
    rows = []
    for key, column in NUMBER_COLUMNS:
        for alma, ucr, text in conn.execute(
                f"SELECT AlmaId, UnitCatalogRecId, {column} FROM catalog "
                f"WHERE {column} IS NOT NULL AND TRIM({column}) <> ''"):
            for lo, hi in parse_number_text(text):
                rows.append((alma, ucr, key, lo, hi))
    for alma, ucr, start, end in conn.execute(
            "SELECT AlmaId, UnitCatalogRecId, CopyDate, CopyToDate FROM catalog "
            "WHERE CopyDate IS NOT NULL AND TRIM(CopyDate) <> ''"):
        span = copy_span(start, end)
        if span:
            rows.append((alma, ucr, "period", *span))
    return rows


def value_rows(conn) -> list[tuple]:
    """(AlmaId, UnitCatalogRecId, key, value) for the coded catalog columns."""
    labels = {stored_code(code, VOCAL_EXTENT_TABLE): eng for code, eng in conn.execute(
        "SELECT Code, EngDesc FROM code_values WHERE FCDTableId = ?", [VOCAL_EXTENT_TABLE])}
    rows = [(alma, ucr, "vocal_extent", labels[stored])
            for alma, ucr, stored in conn.execute(
                "SELECT AlmaId, UnitCatalogRecId, PartVocalCode FROM catalog "
                "WHERE PartVocalCode IS NOT NULL")
            if stored in labels]
    for value, (_he, _en, condition) in ATTRIBUTES.items():
        rows += [(alma, ucr, "attributes", value) for alma, ucr in conn.execute(
            f"SELECT AlmaId, UnitCatalogRecId FROM catalog WHERE {condition}")]
    return rows


def summarize(rows: list[tuple], what: str) -> None:
    for key in sorted({r[2] for r in rows}):
        keyed = [r for r in rows if r[2] == key]
        log(f"  {key:13} {len(keyed):>9,} {what}, "
            f"{len({r[0] for r in keyed}):>9,} manuscripts")


def apply(conn, rows: list[tuple], values: list[tuple]) -> None:
    from scripts.import_measurements import migrate_add_line_height

    conn.execute("BEGIN")
    conn.execute("DROP TABLE IF EXISTS criteria_numbers")
    conn.execute(
        "CREATE TABLE criteria_numbers ("
        " AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER,"
        " key TEXT NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL)"
    )
    conn.executemany("INSERT INTO criteria_numbers VALUES (?, ?, ?, ?, ?)", rows)
    conn.execute(f"CREATE INDEX {NUMBERS_INDEX} ON criteria_numbers(key, lo, hi, AlmaId)")
    conn.execute("DROP TABLE IF EXISTS criteria_values")
    conn.execute(
        "CREATE TABLE criteria_values ("
        " AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER,"
        " key TEXT NOT NULL, value TEXT NOT NULL)"
    )
    conn.executemany("INSERT INTO criteria_values VALUES (?, ?, ?, ?)", values)
    conn.execute(f"CREATE INDEX {VALUES_INDEX} ON criteria_values(key, value, AlmaId)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS {FIELDS_INDEX} "
                 "ON catalog_fields(FieldCategory, FieldValue, AlmaId)")
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
                 (TARGET_VERSION,))
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('criteria_built_at', ?)",
                 (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
    conn.execute("COMMIT")
    if has_table(conn, "manuscript_measurements"):
        migrate_add_line_height(conn)  # idempotent; commits itself


def run(db: Path, do_apply: bool) -> int:
    if not db.is_file():
        log(f"FATAL: {db} not found")
        return EXIT_REFUSED
    mode = "rw" if do_apply else "ro"
    conn = sqlite3.connect(f"file:{db.resolve().as_posix()}?mode={mode}", uri=True,
                           isolation_level=None)
    try:
        version = meta_version(conn)
        log(f"{db}: meta version {version or '(none)'}; "
            f"mode {'APPLY' if do_apply else 'DRY RUN (no writes)'}")
        if version == TARGET_VERSION:
            missing = missing_parts(conn)
            if not missing:
                log("Already a complete 5.1.0 file. Nothing to do.")
                return EXIT_OK
            log(f"REFUSED: version 5.1.0 but missing {', '.join(missing)}. "
                "Rebuild it from its 5.0.x base.")
            return EXIT_REFUSED
        if not version.startswith("5.0."):
            log(f"REFUSED: expected a 5.0.x file, found '{version}'.")
            return EXIT_REFUSED
        stale = instatution_rows(conn)
        if stale:
            log(f"REFUSED: {stale:,} 'Instatution' source rows. Run "
                "scripts/fix_instatution_sources.py on this file first.")
            return EXIT_INSTATUTION

        rows = number_rows(conn)
        values = value_rows(conn)
        log("criteria_numbers:")
        summarize(rows, "intervals")
        log("criteria_values:")
        summarize(values, "values")
        if not do_apply:
            log("Dry run: no writes. Re-run with --apply.")
            return EXIT_OK
        apply(conn, rows, values)
        missing = missing_parts(conn)
        if missing:
            log(f"FAILED: still missing {', '.join(missing)}")
            return EXIT_REFUSED
        log(f"Done: {db} is now {meta_version(conn)}.")
        return EXIT_OK
    finally:
        conn.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB,
                    help=f"sidecar to upgrade in place (default {DEFAULT_DB})")
    ap.add_argument("--apply", action="store_true", help="write the changes")
    args = ap.parse_args(argv)
    return run(args.db, args.apply)


if __name__ == "__main__":
    sys.exit(main())
