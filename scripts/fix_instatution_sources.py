# -*- coding: utf-8 -*-
"""
Fix FJMS catalog rows whose SourceName was imported as the typo
'Instatution' (Institution), leaving them filtered out by
GENERIC_SOURCE_NAMES. This produces empty scholarly-source button
counts (get_source_names / get_catalog_source_counts) and empty
catalog dialogs for ~30.6K manuscripts.

Rewrites SourceName + SourceNameHeb on both tables that carry these
labels:
  - fjms_enrichment.db::catalog (267,104 Instatution rows)
  - fjms_enrichment.db::catalog_free_desc (47,800 Instatution rows)

Resolution path (local, authoritative — not the API bridge checkpoint):

  enrichment.catalog(UnitCatalogRecId, SourceName='Instatution')
    ↓ via FIST.db::dbo_UnitCatalogRec.UnitCatalogRecId == .SignatureId
  dbo_Signature(SourceId=400).SubId
    ↓ == CODE_Institution.InstitutionId
  name = COALESCE(NULLIF(EngDescAc,''), EngDesc, SourceName)
  name_heb = COALESCE(NULLIF(HebDescAc,''), HebDesc, SourceNameHeb)

For `catalog_free_desc` the join is via the row's SignatureId directly
(the column already IS a SignatureId, not a UnitCatalogRecId).

Idempotent: only UPDATEs rows where SourceName='Instatution' — safe to
rerun; never overwrites already-correct names.

Usage
-----
    python scripts/fix_instatution_sources.py              # dry-run
    python scripts/fix_instatution_sources.py --apply      # commit changes
    python scripts/fix_instatution_sources.py --sample ID  # dry-run single sys_id

Exit codes
----------
    0  migration successful (or dry-run complete)
    1  FIST.db missing / unresolvable SubIds too many
    2  backup failed
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

FIST_DB = Path("fist_data/FIST.db")
ENRICHMENT_DB = Path("fist_data/fjms_enrichment.db")
TARGET_SOURCE_ID = 400  # Panel 7: Institutions

TARGET_LABEL = "Instatution"  # the typo we are rewriting


def log(msg: str) -> None:
    print(msg, flush=True)


def build_institution_map(fist_conn: sqlite3.Connection) -> dict[int, tuple[str, str]]:
    """Return {InstitutionId: (eng_name, heb_name)} from CODE_Institution."""
    cur = fist_conn.execute(
        """SELECT InstitutionId,
                  COALESCE(NULLIF(EngDescAc,''), EngDesc) AS eng,
                  COALESCE(NULLIF(HebDescAc,''), HebDesc) AS heb
             FROM CODE_Institution"""
    )
    return {row[0]: (row[1] or "", row[2] or "") for row in cur.fetchall()}


def build_ucr_to_subid(fist_conn: sqlite3.Connection, ucr_ids: list[int]) -> dict[int, int]:
    """Return {UnitCatalogRecId: SubId} for rows where SourceId=400."""
    out: dict[int, int] = {}
    if not ucr_ids:
        return out
    for i in range(0, len(ucr_ids), 500):
        batch = ucr_ids[i : i + 500]
        placeholders = ",".join("?" * len(batch))
        cur = fist_conn.execute(
            f"""SELECT u.UnitCatalogRecId, s.SubId
                  FROM dbo_UnitCatalogRec u
                  JOIN dbo_Signature s ON s.SignatureId = u.SignatureId
                 WHERE u.UnitCatalogRecId IN ({placeholders})
                   AND s.SourceId = {TARGET_SOURCE_ID}""",
            batch,
        )
        for ucr, sub in cur.fetchall():
            out[ucr] = sub
    return out


def build_sigid_to_subid(fist_conn: sqlite3.Connection, sig_ids: list[int]) -> dict[int, int]:
    """Return {SignatureId: SubId} for rows where SourceId=400.

    catalog_free_desc.SignatureId is a dbo_Signature.SignatureId directly
    (not a UnitCatalogRec id), so we join one hop less than for catalog.
    """
    out: dict[int, int] = {}
    if not sig_ids:
        return out
    for i in range(0, len(sig_ids), 500):
        batch = sig_ids[i : i + 500]
        placeholders = ",".join("?" * len(batch))
        cur = fist_conn.execute(
            f"""SELECT SignatureId, SubId
                  FROM dbo_Signature
                 WHERE SignatureId IN ({placeholders})
                   AND SourceId = {TARGET_SOURCE_ID}""",
            batch,
        )
        for sig, sub in cur.fetchall():
            out[sig] = sub
    return out


def plan_updates_catalog(
    enrich_conn: sqlite3.Connection,
    fist_conn: sqlite3.Connection,
    inst_map: dict[int, tuple[str, str]],
    only_alma: str | None = None,
) -> tuple[list[tuple], dict]:
    """Plan catalog UPDATEs. Returns (updates, stats).

    updates: list of (new_eng, new_heb, alma_id, ucr_id) for executemany.
    stats: {'total_rows', 'resolved', 'unresolved_no_subid',
            'unresolved_no_inst_name', 'by_subid'}.
    """
    where_extra = f" AND AlmaId = {only_alma!r}" if only_alma else ""
    rows = enrich_conn.execute(
        f"SELECT AlmaId, UnitCatalogRecId FROM catalog "
        f"WHERE SourceName = ?{where_extra}",
        (TARGET_LABEL,),
    ).fetchall()

    ucr_ids = sorted({r[1] for r in rows})
    ucr_to_sub = build_ucr_to_subid(fist_conn, ucr_ids)

    updates: list[tuple] = []
    by_subid: dict[int, int] = {}
    unresolved_no_subid = 0
    unresolved_no_inst_name = 0

    for alma, ucr in rows:
        sub = ucr_to_sub.get(ucr)
        if sub is None:
            unresolved_no_subid += 1
            continue
        names = inst_map.get(sub)
        if names is None or not names[0]:
            unresolved_no_inst_name += 1
            continue
        eng, heb = names
        by_subid[sub] = by_subid.get(sub, 0) + 1
        updates.append((eng, heb, alma, ucr))

    stats = {
        "total_rows": len(rows),
        "resolved": len(updates),
        "unresolved_no_subid": unresolved_no_subid,
        "unresolved_no_inst_name": unresolved_no_inst_name,
        "by_subid": by_subid,
    }
    return updates, stats


def plan_updates_free_desc(
    enrich_conn: sqlite3.Connection,
    fist_conn: sqlite3.Connection,
    inst_map: dict[int, tuple[str, str]],
    only_alma: str | None = None,
) -> tuple[list[tuple], dict]:
    """Plan catalog_free_desc UPDATEs. Same shape as plan_updates_catalog
    but join is via SignatureId directly (no UnitCatalogRec hop).
    """
    where_extra = f" AND AlmaId = {only_alma!r}" if only_alma else ""
    rows = enrich_conn.execute(
        f"SELECT AlmaId, SignatureId FROM catalog_free_desc "
        f"WHERE SourceName = ?{where_extra}",
        (TARGET_LABEL,),
    ).fetchall()

    sig_ids = sorted({r[1] for r in rows})
    sig_to_sub = build_sigid_to_subid(fist_conn, sig_ids)

    updates: list[tuple] = []
    by_subid: dict[int, int] = {}
    unresolved_no_subid = 0
    unresolved_no_inst_name = 0

    for alma, sig in rows:
        sub = sig_to_sub.get(sig)
        if sub is None:
            unresolved_no_subid += 1
            continue
        names = inst_map.get(sub)
        if names is None or not names[0]:
            unresolved_no_inst_name += 1
            continue
        eng, heb = names
        by_subid[sub] = by_subid.get(sub, 0) + 1
        updates.append((eng, heb, alma, sig))

    stats = {
        "total_rows": len(rows),
        "resolved": len(updates),
        "unresolved_no_subid": unresolved_no_subid,
        "unresolved_no_inst_name": unresolved_no_inst_name,
        "by_subid": by_subid,
    }
    return updates, stats


def pretty_stats(label: str, stats: dict, inst_map: dict) -> None:
    log(f"  [{label}]")
    log(f"    total 'Instatution' rows: {stats['total_rows']:,}")
    log(f"    resolvable to named institution: {stats['resolved']:,}")
    log(f"    unresolved — no SubId (SourceId != 400): {stats['unresolved_no_subid']:,}")
    log(f"    unresolved — SubId has no CODE_Institution row: {stats['unresolved_no_inst_name']:,}")
    if stats["by_subid"]:
        log(f"    resolved distribution by SubId (top 10):")
        sorted_subs = sorted(stats["by_subid"].items(), key=lambda kv: -kv[1])
        for sub, cnt in sorted_subs[:10]:
            eng = inst_map.get(sub, ("?", ""))[0]
            log(f"      SubId={sub:<4} {cnt:>8,}  {eng}")
        if len(sorted_subs) > 10:
            log(f"      ... +{len(sorted_subs) - 10} more SubIds")


def backup_enrichment() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = ENRICHMENT_DB.with_name(
        f"fjms_enrichment_pre_instatution_{ts}.db"
    )
    shutil.copy2(ENRICHMENT_DB, dst)
    return dst


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Commit changes to enrichment DB")
    ap.add_argument("--sample", metavar="ALMA_ID", help="Dry-run single sys_id (e.g. 990051720350205171)")
    args = ap.parse_args()

    if not FIST_DB.exists():
        log(f"FATAL: {FIST_DB} not found")
        return 1
    if not ENRICHMENT_DB.exists():
        log(f"FATAL: {ENRICHMENT_DB} not found")
        return 1

    log("=" * 70)
    log("Fix 'Instatution' SourceName -> local CODE_Institution mapping")
    log("=" * 70)
    log(f"  FIST.db:       {FIST_DB}")
    log(f"  enrichment:    {ENRICHMENT_DB}")
    log(f"  mode:          {'APPLY (will commit)' if args.apply else 'DRY RUN (no writes)'}")
    if args.sample:
        log(f"  sample filter: AlmaId = {args.sample}")
    log("")

    fist_conn = sqlite3.connect(FIST_DB, timeout=10)
    enrich_conn = sqlite3.connect(ENRICHMENT_DB, timeout=30)

    try:
        inst_map = build_institution_map(fist_conn)
        log(f"CODE_Institution entries loaded: {len(inst_map):,}")
        log("")

        cat_updates, cat_stats = plan_updates_catalog(
            enrich_conn, fist_conn, inst_map, only_alma=args.sample,
        )
        fd_updates, fd_stats = plan_updates_free_desc(
            enrich_conn, fist_conn, inst_map, only_alma=args.sample,
        )

        log("Planned updates:")
        pretty_stats("catalog", cat_stats, inst_map)
        log("")
        pretty_stats("catalog_free_desc", fd_stats, inst_map)
        log("")

        total_updates = len(cat_updates) + len(fd_updates)
        if total_updates == 0:
            log("Nothing to update. Done.")
            return 0

        # Sanity counts: if resolution rate is very low, surface loudly.
        resolvable_rate_cat = (
            cat_stats["resolved"] / cat_stats["total_rows"]
            if cat_stats["total_rows"] else 1.0
        )
        if cat_stats["total_rows"] and resolvable_rate_cat < 0.5:
            log(f"WARNING: catalog resolution rate is only "
                f"{resolvable_rate_cat*100:.1f}% — inspect before --apply")

        if not args.apply:
            log("Dry run — no writes performed.")
            log("Re-run with --apply to commit.")
            return 0

        # APPLY mode
        backup = backup_enrichment()
        log(f"Backup written: {backup}")

        # Show before-counts for observability
        before_cat = enrich_conn.execute(
            "SELECT COUNT(*) FROM catalog WHERE SourceName=?", (TARGET_LABEL,)
        ).fetchone()[0]
        before_fd = enrich_conn.execute(
            "SELECT COUNT(*) FROM catalog_free_desc WHERE SourceName=?", (TARGET_LABEL,)
        ).fetchone()[0]
        log(f"Before: catalog={before_cat:,}  catalog_free_desc={before_fd:,} (SourceName='Instatution')")

        try:
            enrich_conn.execute("BEGIN")
            enrich_conn.executemany(
                "UPDATE catalog SET SourceName = ?, SourceNameHeb = ? "
                "WHERE AlmaId = ? AND UnitCatalogRecId = ? AND SourceName = ?",
                [(eng, heb, alma, ucr, TARGET_LABEL) for (eng, heb, alma, ucr) in cat_updates],
            )
            enrich_conn.executemany(
                "UPDATE catalog_free_desc SET SourceName = ?, SourceNameHeb = ? "
                "WHERE AlmaId = ? AND SignatureId = ? AND SourceName = ?",
                [(eng, heb, alma, sig, TARGET_LABEL) for (eng, heb, alma, sig) in fd_updates],
            )
            enrich_conn.execute("COMMIT")
        except Exception:
            enrich_conn.execute("ROLLBACK")
            log("Transaction rolled back; restore from backup if needed.")
            raise

        after_cat = enrich_conn.execute(
            "SELECT COUNT(*) FROM catalog WHERE SourceName=?", (TARGET_LABEL,)
        ).fetchone()[0]
        after_fd = enrich_conn.execute(
            "SELECT COUNT(*) FROM catalog_free_desc WHERE SourceName=?", (TARGET_LABEL,)
        ).fetchone()[0]
        log(f"After:  catalog={after_cat:,}  catalog_free_desc={after_fd:,} (SourceName='Instatution')")
        log(f"Updated: catalog={before_cat - after_cat:,}  "
            f"catalog_free_desc={before_fd - after_fd:,}")
        log("")
        log("Done.")
        return 0

    finally:
        fist_conn.close()
        enrich_conn.close()


if __name__ == "__main__":
    sys.exit(main())
