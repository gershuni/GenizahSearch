# -*- coding: utf-8 -*-
"""Close the R-source row-metadata gaps the outside audit found (2026-08-30).

Four fixes, one transaction:

1. **shelfmark / library_code** were NULL on all 226,679 R-source rows -- the
   base render reads them from the v4.2 artifact's `manuscript_display`, which
   the G-R adapter does not carry. Backfilled from `manuscript_display` where
   the manuscript is known there, else from `libraries.csv` (first call-number
   variant + library code) -- 2,764 manuscripts have R-source-only
   identifications and exist nowhere else. Searching a real shelfmark
   ("EVR II A 313/14") returned 0 rows before this.

2. **work_author** was '' (empty string) instead of NULL on every R-source
   row, so the labeled "no author recorded" filter silently excluded exactly
   the corpus with no recorded authors. Normalized to NULL.

3. **main_pool_reason** carried the hand-written sentence "shipped evidence on
   this page" on main-pool rows -- one prose value amid snake_case codes.
   Normalized to `main_shipped_evidence` (a code in the same style; the
   VOCABULARY difference from the base corpora is real and stays: R-source's
   demotion reasons were never computed, and those rows honestly stay NULL).

4. **owner_ruling / owner_ruling_date / owner_ruling_note / compilation_risk /
   title_provenance** -- the quickstart documents them, the adapter's `works`
   table carries them, but the review renderer never copied them onto rows.
   Added as columns and filled per work (all corpora: base rows get NULL --
   these fields exist only for the R-source half's works).

Run (review server STOPPED):
    python -X utf8 scripts/fix_v5_rsource_row_metadata.py
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NEW_COLS = ("owner_ruling", "owner_ruling_date", "owner_ruling_note",
            "compilation_risk", "title_provenance")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--review-db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--adapter", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "gr-adapter.db"))
    ap.add_argument("--v42", default=os.path.join(
        REPO_ROOT, "discovery_builds", "discovery_v4_2", "build",
        "discovery-v42lit.db"))
    ap.add_argument("--libraries-csv", default=os.path.join(
        REPO_ROOT, "libraries.csv"))
    args = ap.parse_args(argv)
    for p in (args.review_db, args.adapter, args.v42, args.libraries_csv):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)

    # ---- gather, on separate read-only connections ------------------------
    con = sqlite3.connect(args.review_db)
    rs_sys = {r[0] for r in con.execute(
        "SELECT DISTINCT sys_id FROM review_row WHERE source_corpus='rsource' "
        "AND shelfmark IS NULL")}

    v42 = sqlite3.connect("file:%s?mode=ro" % args.v42, uri=True)
    disp = {r[0]: (r[1], r[2]) for r in v42.execute(
        "SELECT sys_id, shelfmark_display, library_code "
        "FROM manuscript_display") if r[0] in rs_sys}
    v42.close()

    missing = rs_sys - set(disp)
    csv_hits = 0
    if missing:
        with open(args.libraries_csv, encoding="utf-8-sig", newline="") as fh:
            for row in csv.reader(fh):
                if row and row[0] in missing:
                    shelf = (row[2].split("|")[0].strip()
                             if len(row) > 2 and row[2] else None)
                    lib = row[3].strip() if len(row) > 3 and row[3] else None
                    if shelf or lib:
                        disp[row[0]] = (shelf, lib)
                        csv_hits += 1

    ad = sqlite3.connect("file:%s?mode=ro" % args.adapter, uri=True)
    works = ad.execute(
        "SELECT work_id, owner_ruling, owner_ruling_date, owner_ruling_note, "
        "compilation_risk, title_provenance FROM works "
        "WHERE source_corpus='rsource'").fetchall()
    ad.close()

    # ---- apply -------------------------------------------------------------
    have = {r[1] for r in con.execute("PRAGMA table_info(review_row)")}
    for c in NEW_COLS:
        if c not in have:
            con.execute("ALTER TABLE review_row ADD COLUMN %s TEXT" % c)
    con.execute("BEGIN")
    con.executemany(
        "UPDATE review_row SET shelfmark=?, library_code=? "
        "WHERE sys_id=? AND source_corpus='rsource' AND shelfmark IS NULL",
        [(v[0], v[1], k) for k, v in disp.items()])
    n_shelf = con.execute(
        "SELECT COUNT(*) FROM review_row WHERE source_corpus='rsource' "
        "AND shelfmark IS NOT NULL").fetchone()[0]
    con.execute("UPDATE review_row SET work_author=NULL "
                "WHERE source_corpus='rsource' AND work_author=''")
    con.execute("UPDATE review_row SET main_pool_reason='main_shipped_evidence' "
                "WHERE source_corpus='rsource' "
                "AND main_pool_reason='shipped evidence on this page'")
    con.executemany(
        "UPDATE review_row SET owner_ruling=?, owner_ruling_date=?, "
        "owner_ruling_note=?, compilation_risk=?, title_provenance=? "
        "WHERE work_id=?",
        [(w[1], w[2], w[3], w[4], w[5], w[0]) for w in works])
    ruled = con.execute(
        "SELECT COUNT(*) FROM review_row WHERE owner_ruling IS NOT NULL "
        "AND owner_ruling != ''").fetchone()[0]
    for k, v in (("rsource_rowmeta.shelfmarks",
                  "%d rows now carry one (%d manuscripts via "
                  "manuscript_display, %d via libraries.csv)"
                  % (n_shelf, len(disp) - csv_hits, csv_hits)),
                 ("rsource_rowmeta.owner_ruled_rows", str(ruled)),
                 ("rsource_rowmeta.at", time.strftime("%Y-%m-%d %H:%M:%S"))):
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    # doc text correction: routing_status is NOT shipped<=>same_work for the
    # R-source half, and the whole corpus is not on the live site at all.
    doc = con.execute("SELECT value FROM meta "
                      "WHERE key='doc.routing_status'").fetchone()
    if doc and "R-source" not in doc[0]:
        con.execute(
            "UPDATE meta SET value = value || ? WHERE key='doc.routing_status'",
            (" EXCEPTION: on R-source rows 'shipped' describes the matching "
             "run's internal tiering, not the website -- the R-source corpus "
             "is not on the live site at all, and every one of its rows is "
             "review-only in that sense.",))
    con.execute("DROP TABLE IF EXISTS facet_row")  # shelfmark etc. are facets
    con.execute("COMMIT")
    con.close()
    print("shelfmarks : %d rsource rows filled (%d manuscripts, %d via csv)"
          % (n_shelf, len(disp), csv_hits))
    print("ruled rows : %d carry an owner_ruling" % ruled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
