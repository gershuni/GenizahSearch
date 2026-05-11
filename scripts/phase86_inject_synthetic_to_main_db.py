#!/usr/bin/env python3
"""Phase 86 surgical synthetic-AlmaId injection into existing fjms_enrichment.db.

Why this exists
---------------
`scripts/export_fist_enrichment.py` regenerates `fjms_enrichment.db` from
scratch (~24 base tables). The deployed/local DB has been augmented post-export
by separate import scripts with 6+ supplemental tables (translations,
measurements, blank_images, extra_info, import_meta). A full re-export would
LOSE those supplemental tables.

Phase 86 Plan 04 produced a worktree DB containing the synthetic delta for the
12 base AlmaId-keyed tables. This one-off script extracts the delta and
INSERTs it into the target DB, preserving the supplemental tables.

Codex review fold-in (2026-05-11):
  - Backup via SQLite .backup() API + integrity_check + chunked gzip CRC
  - BEGIN IMMEDIATE transaction; plain INSERT (no OR IGNORE); rollback on dry-run
  - Catalog-parent integrity check: every synthetic AlmaId in child tables
    MUST exist in injected catalog
  - SKIP catalog_sizes: schema-incompatible (v7.3.0 measurements migration owns it)
  - Rebuild catalog_fts for synthetic AlmaIds (contentless FTS5; append-aligned)
  - Idempotency: pre-flight refuses to run if synthetic AlmaIds already present

Usage
-----
  python scripts/phase86_inject_synthetic_to_main_db.py --dry-run
  python scripts/phase86_inject_synthetic_to_main_db.py --apply

  --source PATH  Override source DB (default: worktree DB from Phase 86 Plan 04)
  --target PATH  Override target DB (default: fist_data/fjms_enrichment.db)
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Allow `from shared.synthetic_sys_id import ...` when invoked as a script
sys.path.insert(0, str(REPO_ROOT))

from shared.synthetic_sys_id import is_synthetic_sys_id  # noqa: E402

DEFAULT_TARGET = REPO_ROOT / "fist_data" / "fjms_enrichment.db"
DEFAULT_SOURCE = (
    REPO_ROOT
    / ".claude"
    / "worktrees"
    / "agent-ab0ad3493be564d3d"
    / "fist_data"
    / "fjms_enrichment.db"
)
BACKUP_DIR = REPO_ROOT / "_tmp" / "phase86_backups"

# Synthetic AlmaId pattern: 18 digits, starts with 99, ends with 000000
SYNTHETIC_GLOB = "99??????????000000"

# 11 base tables to inject (catalog_sizes excluded — schema drift, owned by
# scripts/import_measurements.py per Codex)
BASE_TABLES = [
    "catalog",  # parent — must be injected first for parent-integrity check
    "domains",
    "joins",
    "catalog_running_titles",
    "catalog_fields",
    "catalog_free_desc",
    "catalog_full_texts",
    "catalog_textual_frames",
    "catalog_mentions",
    "bibliography",
    "catalog_refs",
]
SKIPPED_TABLES = ["catalog_sizes"]  # explicit skip with rationale in module docstring

# Supplemental tables that MUST remain unchanged (Codex check #4)
SUPPLEMENTAL_TABLES = [
    "fjms_translations",
    "extra_info",
    "manuscript_measurements",
    "computed_measurements",
    "blank_images",
    "import_meta",
]

# Child tables that reference catalog.AlmaId (for parent-integrity check)
CHILD_TABLES = [t for t in BASE_TABLES if t != "catalog"]


def log(msg: str) -> None:
    # Strip any non-cp1255 chars defensively (Windows console encoding)
    safe = msg.encode("ascii", "replace").decode("ascii")
    print(f"[inject] {safe}", flush=True)


def err(msg: str) -> None:
    safe = msg.encode("ascii", "replace").decode("ascii")
    print(f"[inject][ERROR] {safe}", file=sys.stderr, flush=True)


def fail(msg: str, code: int = 1) -> None:
    err(msg)
    sys.exit(code)


def table_columns(conn: sqlite3.Connection, table: str) -> list[tuple]:
    """Return [(name, type, notnull, pk)] for table columns."""
    return [(r[1], r[2], r[3], r[5]) for r in conn.execute(f'PRAGMA table_info("{table}")')]


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def synthetic_row_count(conn: sqlite3.Connection, table: str) -> int:
    """Count rows in `table` whose AlmaId matches the synthetic GLOB."""
    return conn.execute(
        f"SELECT COUNT(*) FROM \"{table}\" WHERE CAST(AlmaId AS TEXT) GLOB ?",
        (SYNTHETIC_GLOB,),
    ).fetchone()[0]


def all_row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def verify_schema_match(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    """Codex check: schema must match for the 11 base tables before insert."""
    log("Verifying schema match (11 base tables)...")
    mismatches = []
    for tbl in BASE_TABLES:
        if not table_exists(source, tbl):
            mismatches.append(f"{tbl}: missing in source")
            continue
        if not table_exists(target, tbl):
            mismatches.append(f"{tbl}: missing in target")
            continue
        sc = table_columns(source, tbl)
        tc = table_columns(target, tbl)
        if sc != tc:
            mismatches.append(
                f"{tbl}: source has {len(sc)} cols, target has {len(tc)} cols — schema drift"
            )
    if mismatches:
        for m in mismatches:
            err(f"  {m}")
        fail("Schema mismatch — aborting (re-check the migration plan)", code=10)
    log("  Schema match OK for 11 base tables")


def verify_target_clean(target: sqlite3.Connection) -> None:
    """Codex check: refuse to run if synthetic AlmaIds already present in target.

    Idempotency / re-run protection.
    """
    log("Verifying target has zero synthetic AlmaIds (idempotency check)...")
    for tbl in BASE_TABLES:
        n = synthetic_row_count(target, tbl)
        if n != 0:
            fail(
                f"Target table '{tbl}' already has {n} synthetic AlmaId rows. "
                f"Re-running this script would create duplicates. Restore from "
                f"backup or investigate before retrying.",
                code=11,
            )
    log("  Target is clean (zero synthetic rows in all 11 base tables)")


def verify_synthetic_ids_strict(source: sqlite3.Connection) -> set[str]:
    """Cross-check Codex constraint: every AlmaId matched by GLOB must also
    pass `is_synthetic_sys_id()`. Returns the set of synthetic AlmaIds present
    in source's catalog table.
    """
    log("Cross-checking synthetic AlmaIds via is_synthetic_sys_id()...")
    catalog_alma_ids = {
        str(r[0])
        for r in source.execute(
            "SELECT DISTINCT AlmaId FROM catalog WHERE CAST(AlmaId AS TEXT) GLOB ?",
            (SYNTHETIC_GLOB,),
        )
    }
    bad = [a for a in catalog_alma_ids if not is_synthetic_sys_id(a)]
    if bad:
        fail(
            f"GLOB matched {len(bad)} AlmaIds that fail is_synthetic_sys_id() — "
            f"helper-contract drift. Sample: {bad[:3]}",
            code=12,
        )
    log(f"  {len(catalog_alma_ids)} synthetic AlmaIds verified (catalog table)")
    return catalog_alma_ids


def backup_target(target_path: Path) -> Path:
    """SQLite .backup() API + integrity_check + gzip with full-stream CRC verify.

    Codex check #1: backup must validate complete gzip stream, not just magic bytes.
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%dT%H%M%S")
    uncompressed = BACKUP_DIR / f"fjms_enrichment.db.pre-inject.{ts}.bak"
    compressed = BACKUP_DIR / f"fjms_enrichment.db.pre-inject.{ts}.bak.gz"

    log(f"Backing up {target_path} via SQLite .backup() API...")
    log(f"  → uncompressed: {uncompressed}")
    with closing(sqlite3.connect(str(target_path))) as src, closing(
        sqlite3.connect(str(uncompressed))
    ) as dst:
        src.backup(dst)
    log(f"  uncompressed backup size: {uncompressed.stat().st_size:,} bytes")

    log("Running PRAGMA integrity_check on backup...")
    with closing(sqlite3.connect(str(uncompressed))) as bak:
        result = bak.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        fail(f"Backup integrity_check FAILED: {result}", code=13)
    log("  integrity_check: ok")

    log(f"Compressing backup → {compressed}")
    with open(uncompressed, "rb") as f_in, gzip.open(compressed, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out, length=8 * 1024 * 1024)
    log(f"  compressed size: {compressed.stat().st_size:,} bytes")

    # Codex check #1: validate the FULL gzip stream by reading it back end-to-end
    # (forces CRC32 + ISIZE footer validation; raises if truncated/corrupt).
    log("Validating gzip stream (full read, CRC + ISIZE footer)...")
    bytes_read = 0
    with gzip.open(compressed, "rb") as f:
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)
    if bytes_read != uncompressed.stat().st_size:
        fail(
            f"Gzip stream byte-count mismatch: read {bytes_read:,}, "
            f"expected {uncompressed.stat().st_size:,}",
            code=14,
        )
    log(f"  gzip stream valid ({bytes_read:,} bytes round-trip)")

    # Remove the uncompressed intermediate (keep only the .gz)
    uncompressed.unlink()
    log("  uncompressed intermediate removed (kept .gz)")
    return compressed


def snapshot_supplemental_counts(target: sqlite3.Connection) -> dict[str, int]:
    """Codex check #4: snapshot row counts of supplemental tables. Verify
    unchanged post-COMMIT.
    """
    snap = {}
    for tbl in SUPPLEMENTAL_TABLES:
        if table_exists(target, tbl):
            snap[tbl] = all_row_count(target, tbl)
    return snap


def inject_base_tables(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
) -> dict[str, int]:
    """For each of 11 base tables, SELECT synthetic rows from source and
    INSERT into target. Plain INSERT (no OR IGNORE) — collisions fail loudly.
    Returns per-table inserted-row counts.
    """
    inserted = {}
    for tbl in BASE_TABLES:
        cols = [c[0] for c in table_columns(source, tbl)]
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        rows = list(
            source.execute(
                f'SELECT {col_list} FROM "{tbl}" WHERE CAST(AlmaId AS TEXT) GLOB ?',
                (SYNTHETIC_GLOB,),
            )
        )
        if rows:
            target.executemany(
                f'INSERT INTO "{tbl}" ({col_list}) VALUES ({placeholders})',
                rows,
            )
        inserted[tbl] = len(rows)
        log(f"  {tbl:<28} +{len(rows):,} rows")
    return inserted


def verify_catalog_parent_integrity(
    target: sqlite3.Connection,
    catalog_alma_ids: set[str],
) -> None:
    """Codex check #5: every synthetic AlmaId in STRICT child tables must
    exist in the injected catalog. `bibliography` is exempt — FJMS data
    legitimately contains bib citations without a UnitCatalogRec parent
    (AIU-prefixed inventories surface as bib-only).
    """
    log("Verifying catalog-parent integrity for synthetic AlmaIds...")
    strict_children = [t for t in CHILD_TABLES if t != "bibliography"]
    orphans_total = 0
    for tbl in strict_children:
        child_ids = {
            str(r[0])
            for r in target.execute(
                f'SELECT DISTINCT AlmaId FROM "{tbl}" WHERE CAST(AlmaId AS TEXT) GLOB ?',
                (SYNTHETIC_GLOB,),
            )
        }
        orphans = child_ids - catalog_alma_ids
        if orphans:
            err(
                f"  {tbl}: {len(orphans)} synthetic AlmaId(s) absent from catalog — "
                f"orphan refs. Sample: {sorted(orphans)[:3]}"
            )
            orphans_total += len(orphans)
        else:
            log(f"  {tbl:<28} OK ({len(child_ids)} synthetic AlmaIds, all parented)")
    # bibliography: warn-only (genuine FJMS bib-only citation pattern)
    bib_ids = {
        str(r[0])
        for r in target.execute(
            "SELECT DISTINCT AlmaId FROM bibliography WHERE CAST(AlmaId AS TEXT) GLOB ?",
            (SYNTHETIC_GLOB,),
        )
    }
    bib_orphans = bib_ids - catalog_alma_ids
    if bib_orphans:
        log(
            f"  bibliography                 {len(bib_orphans)} synthetic AlmaId(s) "
            f"with bib-only coverage (no catalog parent — expected FJMS pattern)"
        )
        for a in sorted(bib_orphans):
            log(f"    bib-only: {a}")
    else:
        log(f"  bibliography                 OK ({len(bib_ids)} synthetic AlmaIds, all parented)")
    if orphans_total:
        fail(
            f"Parent-integrity violation: {orphans_total} orphan synthetic-AlmaId "
            f"reference(s) across STRICT child tables (bibliography exempt).",
            code=15,
        )


def rebuild_synthetic_fts_entries(target: sqlite3.Connection) -> int:
    """Append aggregated FTS5 rows for synthetic AlmaIds.

    Codex check #2: catalog_fts is contentless FTS5 with one row per DISTINCT
    AlmaId aggregating text from catalog + catalog_running_titles +
    catalog_free_desc + catalog_full_texts + catalog_textual_frames. Append
    target-rowid-aligned FTS rows for the newly injected synthetic AlmaIds.

    Mirrors the aggregation logic from scripts/export_fist_enrichment.py:1535-1614.
    """
    log("Appending synthetic AlmaIds to catalog_fts...")
    synthetic_alma_ids = [
        r[0]
        for r in target.execute(
            "SELECT DISTINCT AlmaId FROM catalog WHERE CAST(AlmaId AS TEXT) GLOB ?",
            (SYNTHETIC_GLOB,),
        )
    ]

    has_full_texts = table_exists(target, "catalog_full_texts")
    has_textual_frames = table_exists(target, "catalog_textual_frames")

    batch = []
    for alma_id in synthetic_alma_ids:
        cat_row = target.execute(
            "SELECT Title, TitleHeb, TextualFrameHeb, TextualFrameEng "
            "FROM catalog WHERE AlmaId = ? LIMIT 1",
            (alma_id,),
        ).fetchone()

        rt = target.execute(
            "SELECT GROUP_CONCAT(RunningTitle, '; ') FROM catalog_running_titles WHERE AlmaId = ?",
            (alma_id,),
        ).fetchone()
        running_titles = rt[0] if rt and rt[0] else ""

        fd = target.execute(
            "SELECT GROUP_CONCAT(FreeDesc, '; ') FROM catalog_free_desc WHERE AlmaId = ?",
            (alma_id,),
        ).fetchone()
        free_descs = fd[0] if fd and fd[0] else ""

        full_texts = ""
        if has_full_texts:
            ft = target.execute(
                "SELECT GROUP_CONCAT(FullText, '; ') FROM catalog_full_texts WHERE AlmaId = ?",
                (alma_id,),
            ).fetchone()
            full_texts = ft[0] if ft and ft[0] else ""

        detailed_frames = ""
        if has_textual_frames:
            tf = target.execute(
                "SELECT GROUP_CONCAT(COALESCE(TextualFrameEng, '') || ' ' || COALESCE(TextualFrameHeb, ''), '; ') "
                "FROM catalog_textual_frames WHERE AlmaId = ?",
                (alma_id,),
            ).fetchone()
            detailed_frames = tf[0] if tf and tf[0] else ""

        batch.append(
            (
                alma_id,
                cat_row[0] if cat_row else "",
                cat_row[1] if cat_row else "",
                cat_row[2] if cat_row else "",
                cat_row[3] if cat_row else "",
                running_titles,
                free_descs,
                full_texts,
                detailed_frames,
            )
        )

    if batch:
        target.executemany(
            "INSERT INTO catalog_fts(AlmaId, Title, TitleHeb, TextualFrameHeb, "
            "TextualFrameEng, RunningTitle, FreeDescription, FullText, DetailedFrames) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
    log(f"  appended {len(batch)} synthetic FTS5 documents")
    return len(batch)


def verify_supplemental_unchanged(
    target: sqlite3.Connection, snapshot: dict[str, int]
) -> None:
    """Codex check #4: verify the 6 supplemental tables are untouched."""
    log("Verifying supplemental tables unchanged...")
    drift = []
    for tbl, before in snapshot.items():
        after = all_row_count(target, tbl)
        if after != before:
            drift.append((tbl, before, after))
            err(f"  {tbl}: {before:,} → {after:,} (DRIFT)")
        else:
            log(f"  {tbl:<30} {before:>10,} (unchanged)")
    if drift:
        fail(f"Supplemental table drift: {drift}", code=16)


def verify_post_integrity(target: sqlite3.Connection) -> None:
    log("Running PRAGMA integrity_check on target...")
    result = target.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        fail(f"Post-insert integrity_check FAILED: {result}", code=17)
    log("  integrity_check: ok")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Simulate; ROLLBACK at end")
    mode.add_argument("--apply", action="store_true", help="Execute; COMMIT at end")
    p.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Source DB (worktree)")
    p.add_argument("--target", type=Path, default=DEFAULT_TARGET, help="Target DB (main)")
    p.add_argument(
        "--skip-backup",
        action="store_true",
        help="Skip backup step (DANGEROUS — only for re-runs immediately after a successful backup)",
    )
    args = p.parse_args(argv)

    if not args.source.exists():
        fail(f"Source DB not found: {args.source}", code=2)
    if not args.target.exists():
        fail(f"Target DB not found: {args.target}", code=2)

    log(f"Source: {args.source}")
    log(f"Target: {args.target}")
    log(f"Mode:   {'APPLY (COMMIT)' if args.apply else 'DRY-RUN (ROLLBACK)'}")

    if args.apply and not args.skip_backup:
        compressed = backup_target(args.target)
        log(f"Backup ready: {compressed}")
    elif args.apply:
        log("WARNING: --skip-backup set; proceeding without backup")
    else:
        log("DRY-RUN: skipping backup step (no mutation will be committed)")

    with closing(sqlite3.connect(str(args.source))) as source, closing(
        sqlite3.connect(str(args.target))
    ) as target:
        # Pre-flight checks
        verify_schema_match(source, target)
        verify_target_clean(target)
        synthetic_alma_ids = verify_synthetic_ids_strict(source)
        supplemental_snapshot = snapshot_supplemental_counts(target)
        log(f"Supplemental snapshot: {supplemental_snapshot}")

        # Begin transaction
        target.execute("BEGIN IMMEDIATE")
        try:
            inserted = inject_base_tables(source, target)
            verify_catalog_parent_integrity(target, synthetic_alma_ids)
            fts_count = rebuild_synthetic_fts_entries(target)
            verify_supplemental_unchanged(target, supplemental_snapshot)

            log("")
            log(f"Insert summary: {sum(inserted.values()):,} rows across {len(inserted)} tables")
            log(f"  + {fts_count} catalog_fts entries")
            log(f"Skipped tables (by design): {SKIPPED_TABLES}")

            if args.dry_run:
                target.rollback()
                log("")
                log("DRY-RUN ROLLBACK complete — no changes committed")
                # Post-rollback verify nothing leaked
                for tbl in BASE_TABLES:
                    n = synthetic_row_count(target, tbl)
                    if n != 0:
                        fail(f"Rollback failed — {tbl} still has {n} synthetic rows", code=18)
                log("Post-rollback verify: target unchanged OK")
            else:
                target.commit()
                log("")
                log("COMMIT complete — changes persisted")
                verify_post_integrity(target)
                log("")
                log("Phase 86 synthetic-AlmaId injection: SUCCESS")
        except Exception:
            target.rollback()
            err("Transaction ROLLED BACK due to exception")
            raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
