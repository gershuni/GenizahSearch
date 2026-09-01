# -*- coding: utf-8 -*-
"""R-source author corrections (owner rulings, 2026-08-31).

The from-title derivation credited X wherever a title names a RELATION to X
("תלמיד הרמב\"ן" -> author רמב"ן). Owner: plain wrong. Rulings applied here:

  - relation-to-X titles (תלמיד/תלמידי X, בן X where the son is the author):
    the student works get NO author (the title already says everything we
    know); the Avraham-ben-haRambam works get their actual author, still
    verbatim from the title.
  - מכלול gains its well-known author (רד"ק) by explicit owner ruling.
  - "לרש\"י" attributions stay רש"י by explicit owner ruling ("I would stick
    with רש\"י even though it's complicated").

Run (review server STOPPED):
    python -X utf8 scripts/fix_rsource_author_overrides.py
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# work_id -> (author or None, author_provenance or None)
OVERRIDES = {
    # תלמיד הרמב"ן: the author is an anonymous student, never רמב"ן
    "rs039d56a5ad4f": (None, None),
    # תלמידי הרשב"א: same pattern
    "rsd91ddad50fc1": (None, None),
    # רבי אברהם בן הרמב"ם is the AUTHOR named by these titles, not רמב"ם
    "rsf75c30530030": ("רבי אברהם בן הרמב״ם", "from_title"),
    "rs86b18b09c3c0": ("רבי אברהם בן הרמב״ם", "from_title"),
    "rsb73a28f86fbf": ("רבי אברהם בן הרמב״ם", "from_title"),
    # מכלול: owner ruling 2026-08-31
    "rs01ec1e799568": ("רד״ק", "owner_ruling"),
    # איסור והיתר לרש"י stays רש"י (owner: "I would stick with רש\"י even
    # though it's complicated") -- owner_ruling provenance also shields it
    # from the alias-cluster author harmonization
    "rs030885fa348c": ("רש״י", "owner_ruling"),
}


def apply(db_path, say=print):
    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        total = 0
        for wid, (author, prov) in OVERRIDES.items():
            cur = con.execute(
                "UPDATE review_row SET work_author = ?, author_provenance = ? "
                "WHERE work_id = ?", (author, prov, wid))
            say("%s -> %r (%s rows)" % (wid, author, cur.rowcount))
            total += cur.rowcount
        if total == 0:
            raise SystemExit("no rows matched any override work_id -- wrong db?")
        con.execute("INSERT OR REPLACE INTO meta VALUES "
                    "('rsource_author_overrides.at', datetime('now'))")
        con.execute("DROP TABLE IF EXISTS facet_row")   # work_author is faceted
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    say("updated %d rows across %d works" % (total, len(OVERRIDES)))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        raise SystemExit("missing: %s" % args.db)
    apply(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
