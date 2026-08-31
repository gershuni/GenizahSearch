# -*- coding: utf-8 -*-
"""Backfill `coverage_ppm` on the review artifact's R-source rows.

THE GAP. The adapter left `coverage_ppm` NULL on every R-source evidence row,
and the viewer rendered that NULL as "covers 0.0% of this page's letters" -- a
display artifact reading as a claim. The router in fact computed page coverage
for every routed (page, work) group (`coverage_route.page_coverage` in the
run db); this script carries it over:

    review_row.page_id + works.canonical_work_id  ->  coverage_route

Rows whose group the router never saw keep NULL -- and the viewer now says
"coverage not recorded" for NULL instead of inventing a number.

Run (review server STOPPED):
    python -X utf8 scripts/backfill_rsource_coverage.py
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--review-db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--adapter", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "gr-adapter.db"))
    ap.add_argument("--run-db", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data", "g_r.db"))
    args = ap.parse_args(argv)
    for p in (args.review_db, args.adapter, args.run_db):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)

    con = sqlite3.connect(args.review_db)
    try:
        # ATTACH before any transaction -- SQLite refuses it inside one.
        con.execute("ATTACH DATABASE ? AS ad", (args.adapter,))
        con.execute("ATTACH DATABASE ? AS rr", (args.run_db,))
        base_before = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE source_corpus != 'rsource' "
            "AND coverage_ppm IS NOT NULL").fetchone()[0]
        con.execute("BEGIN")
        con.execute("""
            UPDATE review_row SET coverage_ppm = (
              SELECT CAST(ROUND(cr.page_coverage * 1000000) AS INTEGER)
              FROM ad.works w
              JOIN rr.coverage_route cr
                ON cr.canonical_work_id = w.canonical_work_id
               AND cr.page_id = review_row.page_id
               AND cr.run_id = 'g_r'
              WHERE w.work_id = review_row.work_id)
            WHERE source_corpus = 'rsource'""")
        got = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE source_corpus='rsource' "
            "AND coverage_ppm IS NOT NULL").fetchone()[0]
        still_null = 226679 - got
        base_after = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE source_corpus != 'rsource' "
            "AND coverage_ppm IS NOT NULL").fetchone()[0]
        if base_after != base_before:
            raise RuntimeError("base-corpora coverage changed (%d -> %d)"
                               % (base_before, base_after))
        if got == 0:
            raise RuntimeError("nothing joined -- the canonical/page key is "
                               "wrong, refusing to publish")
        for k, v in (("rsource_coverage.backfilled", str(got)),
                     ("rsource_coverage.still_null", str(still_null)),
                     ("rsource_coverage.source",
                      "g_r coverage_route.page_coverage via adapter works"),
                     ("rsource_coverage.at",
                      time.strftime("%Y-%m-%d %H:%M:%S"))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        # coverage_ppm is a facet column -> the projection must rebuild
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
        dist = con.execute("""
            SELECT CASE WHEN coverage_ppm IS NULL THEN 'not recorded'
                        WHEN coverage_ppm < 10000 THEN 'under 1%'
                        WHEN coverage_ppm < 100000 THEN '1-10%'
                        ELSE 'over 10%' END, COUNT(*)
            FROM review_row WHERE source_corpus='rsource' GROUP BY 1""").fetchall()
    finally:
        con.close()
    print("backfilled: %d rsource rows; %d remain NULL (never routed)"
          % (got, still_null))
    for b, n in dist:
        print("  %7d  %s" % (n, b))
    return 0


if __name__ == "__main__":
    sys.exit(main())
