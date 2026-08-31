# -*- coding: utf-8 -*-
"""Give the review artifact the live app's ATOMIC locus list per work.

THE COMPLAINT THIS FIXES. The Part-of-work From/To dropdowns were fed from the
rows' own `locus_label` values -- but a v4.2 claim spanning units carries a
RANGE label ("פרק ב–ג", "עמ' 8–הדרשנות, עמ' 12"), so the dropdowns offered
composite addresses the live app never shows. The live app's control reads the
sidecar's `locus_unit` table: one row per ATOMIC citation unit, in reading
order, with its stream offset. This script embeds that table:

  * base corpora -- copied verbatim from the pinned v4.2 artifact
    (`locus_unit`: work_id, unit_ord, start_offset, label_he);
  * R-source -- synthesized at the same shape from the rows themselves
    (their loci are single headers, already atomic; ordinal = stream order).

The viewer then serves dropdown options from this table and resolves a
From/To choice to stream bounds: From = the first unit with that label,
To = up to (not including) the unit AFTER the last unit with that label.

Run (review server STOPPED):
    python -X utf8 scripts/attach_locus_units.py
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DDL = """CREATE TABLE locus_unit(
  work_id      TEXT NOT NULL,
  unit_ord     INTEGER NOT NULL,   -- reading order within the work
  start_offset INTEGER NOT NULL,   -- letter-stream offset (same space as w_start)
  label_he     TEXT NOT NULL,
  PRIMARY KEY (work_id, unit_ord)
)"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--review-db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--v42", default=os.path.join(
        REPO_ROOT, "discovery_builds", "discovery_v4_2", "build",
        "discovery-v42lit.db"))
    args = ap.parse_args(argv)
    for p in (args.review_db, args.v42):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)

    # The v4.2 units are read on their OWN connection and inserted with
    # executemany. An ATTACH would be tidier, but creating a table named
    # `locus_unit` in this main db makes the attached `v42.locus_unit`
    # unresolvable on this specific pairing (a schema-cache quirk: the same
    # statements succeed with a :memory: main or a different table name), so
    # the copy goes through Python.
    src = sqlite3.connect("file:%s?mode=ro" % args.v42, uri=True)
    have = src.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='locus_unit'").fetchone()[0]
    if not have:
        raise SystemExit("%s carries no locus_unit table" % args.v42)

    con = sqlite3.connect(args.review_db)
    try:
        base_works = {r[0] for r in con.execute(
            "SELECT DISTINCT work_id FROM review_row "
            "WHERE source_corpus != 'rsource'")}
        units = [r for r in src.execute(
            "SELECT work_id, unit_ord, start_offset, label_he "
            "FROM locus_unit") if r[0] in base_works]
        src.close()
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS locus_unit")
        con.execute(DDL)
        # base corpora: the live app's own units, only for works this
        # artifact actually shows
        con.executemany("INSERT INTO locus_unit VALUES (?,?,?,?)", units)
        n_base = con.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0]
        # R-source: loci are single headers (atomic by construction); order
        # and offset come from the matches' own stream positions
        con.execute("""
            INSERT INTO locus_unit
            SELECT work_id,
                   ROW_NUMBER() OVER (PARTITION BY work_id
                                      ORDER BY MIN(w_start)) - 1,
                   MIN(w_start), locus_label
            FROM review_row
            WHERE source_corpus='rsource' AND locus_status='resolved'
              AND w_start IS NOT NULL AND locus_label IS NOT NULL
            GROUP BY work_id, locus_label""")
        n_all = con.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0]
        works = con.execute(
            "SELECT COUNT(DISTINCT work_id) FROM locus_unit").fetchone()[0]
        con.execute("CREATE INDEX ix_lu_work ON locus_unit(work_id, unit_ord)")
        for k, v in (("locus_units.base_rows", str(n_base)),
                     ("locus_units.rsource_rows", str(n_all - n_base)),
                     ("locus_units.works", str(works)),
                     ("locus_units.at", time.strftime("%Y-%m-%d %H:%M:%S"))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    print("locus_unit: %d base + %d rsource units over %d works"
          % (n_base, n_all - n_base, works))
    return 0


if __name__ == "__main__":
    sys.exit(main())
