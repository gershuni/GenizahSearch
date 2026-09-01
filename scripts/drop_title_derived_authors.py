# -*- coding: utf-8 -*-
"""Clear authors that were derived from the work's own title (owner ruling,
2026-09-01).

The R-source ruling pass wrote an author whenever one known person-token
appeared verbatim in a work's title, and marked it
`author_provenance='from_title'`. The viewer then had to annotate it
("מן הכותרת") to say the attribution was not a new claim. Measured over this
artifact: all 37 works so annotated carry an author string already contained in
their own title, and NOT ONE adds anything a reader could not read off the
title. So the value went, and the annotation with it.

Two things this script refuses to do:

  * delete an author that is NOT contained in its title -- that would be a real
    attribution, and it stops the run for an owner ruling instead;
  * delete an author an owner actually ruled on. `fix_rsource_author_overrides`
    names three works whose author IS an owner decision (correcting רמב״ם to
    רבי אברהם בן הרמב״ם) but recorded it with the mechanical `from_title`
    label. Those keep their author and are RELABELLED `owner_ruling`, which is
    what put it there.

Run (review server STOPPED), then rebuild the registry and the cards -- both
read `work_author`:
    python -X utf8 scripts/drop_title_derived_authors.py
"""
import argparse
import os
import re
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")

# authors that ARE an owner ruling, recorded with the mechanical label
# (scripts/fix_rsource_author_overrides.py)
OWNER_RULED = ("rsf75c30530030", "rs86b18b09c3c0", "rsb73a28f86fbf")

# gershayim/geresh variants, quotes, brackets, diacritics and spacing all
# differ between a title and the name inside it; containment is tested on the
# letters alone
_STRIP = re.compile(r"[֑-ׇ׳״\"'׳״()\[\]\s.,־-]")


def bare(s):
    return _STRIP.sub("", s or "")


class GateError(SystemExit):
    pass


def apply(db_path, say=print):
    con = sqlite3.connect(db_path)
    try:
        works = list(con.execute(
            "SELECT work_id, MIN(work_title), MIN(work_author), COUNT(*) "
            "FROM review_row WHERE author_provenance='from_title' "
            "GROUP BY work_id"))
        if not works:
            say("no title-derived authors remain")
            return 0, 0
        drop, keep, additive = [], [], []
        for wid, title, author, n in works:
            if wid in OWNER_RULED:
                keep.append((wid, title, author, n))
            elif bare(author) and bare(author) in bare(title):
                drop.append((wid, title, author, n))
            else:
                additive.append((wid, title, author, n))
        if additive:
            for wid, title, author, n in additive:
                say(f"ADDITIVE {wid}: author {author!r} is not spelled in "
                    f"title {title!r} ({n} rows)")
            raise GateError(
                f"{len(additive)} title-derived author(s) say something the "
                "title does not; refusing to delete an attribution without an "
                "owner ruling")
        con.execute("BEGIN")
        rows = 0
        for wid, title, author, n in drop:
            cur = con.execute(
                "UPDATE review_row SET work_author=NULL, "
                "author_provenance=NULL WHERE work_id=?", (wid,))
            rows += cur.rowcount
        for wid, title, author, n in keep:
            con.execute(
                "UPDATE review_row SET author_provenance='owner_ruling' "
                "WHERE work_id=?", (wid,))
        left = con.execute("SELECT COUNT(*) FROM review_row WHERE "
                           "author_provenance='from_title'").fetchone()[0]
        if left:
            con.execute("ROLLBACK")
            raise GateError(f"{left} rows still marked from_title after the "
                            "pass -- refusing to leave the label behind")
        con.execute(
            "INSERT OR REPLACE INTO meta VALUES "
            "('rsource_authors.title_derived_cleared', ?)",
            (f"{len(drop)} works / {rows} rows cleared; {len(keep)} "
             "owner-ruled works relabelled owner_ruling (owner 2026-09-01)",))
        # title and author are facet columns: the slim projection must be
        # rebuilt, and the viewer rebuilds it on next start
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass
    say(f"cleared {len(drop)} works ({rows} rows); relabelled {len(keep)} "
        "owner-ruled works; facet_row dropped for rebuild")
    say("now rebuild: build_work_registry.py, then attach_review_cards.py")
    return len(drop), len(keep)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args(argv)
    apply(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
