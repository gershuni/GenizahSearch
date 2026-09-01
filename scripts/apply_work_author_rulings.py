# -*- coding: utf-8 -*-
"""Apply owner rulings about work authors and titles to the review db.

Two ways in, one set of gates:

  * `RULINGS` below -- rulings given in conversation, recorded in code so they
    survive without a spreadsheet;
  * `--csv work_authors.csv` -- the file `scripts/export_work_authors.py`
    writes, with the owner's DROP_AUTHOR / NEW_AUTHOR / NEW_TITLE / NOTE
    columns filled in.

Every applied ruling is recorded in `work_author_ruling` inside the artifact:
what the value was, what it became, and why. That makes the pass idempotent
(a second run finds nothing to do), gives the audit trail a home that travels
with the db, and means a later re-derivation cannot quietly undo an owner
decision without contradicting a stored record.

STALENESS IS FATAL, not a warning: if the CSV's `work_author` no longer matches
the db, the file was exported before some other pass changed that work, and the
owner's correction was made against a value they can no longer see. The run
refuses rather than applying it.

    python -X utf8 scripts/apply_work_author_rulings.py            # RULINGS
    python -X utf8 scripts/apply_work_author_rulings.py --csv work_authors.csv

Then rebuild what reads authors: build_work_registry.py, attach_review_cards.py.
"""
import argparse
import csv
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")

TRUTHY = {"x", "X", "1", "yes", "YES", "y", "Y", "true", "TRUE", "v", "V"}

# work_id -> dict(drop_author=True | author="..." | title="...", note="...")
#
# Owner, 2026-09-01: "פרקי דרבי אליעזר - drop the author". The string was
# "מיוחס לר' אליעזר בן הורקנוס (נתחבר במאות ה-8-9)" -- an attribution note plus
# a dating, not an author. It sat on five works: both title spellings
# (פרקי דרבי אליעזר / פרקי רבי אליעזר) across all three corpora, which are the
# same work, so the ruling covers all five rather than leaving the identical
# gloss on the other spelling.
_PIRKEI = ("rs706e270d3581", "w000807", "rs24b9e0fe9290", "w001507", "w001447")
RULINGS = {w: dict(drop_author=True,
                   note="owner 2026-09-01: attribution note + dating, not an "
                        "author")
           for w in _PIRKEI}

DDL = """CREATE TABLE IF NOT EXISTS work_author_ruling(
  work_id TEXT NOT NULL,
  field TEXT NOT NULL CHECK (field IN ('author','title')),
  old_value TEXT,
  new_value TEXT,
  note TEXT,
  source TEXT NOT NULL CHECK (source IN ('code','csv')),
  ruled_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (work_id, field)
)"""


class GateError(SystemExit):
    pass


def _read_csv(path):
    out = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for i, r in enumerate(csv.DictReader(f), start=2):
            wid = (r.get("work_id") or "").strip()
            if not wid:
                continue
            drop = (r.get("DROP_AUTHOR") or "").strip() in TRUTHY
            author = (r.get("NEW_AUTHOR") or "").strip()
            title = (r.get("NEW_TITLE") or "").strip()
            if not (drop or author or title):
                continue
            if drop and author:
                raise GateError(f"line {i} ({wid}): DROP_AUTHOR and NEW_AUTHOR "
                                "both set -- which is it?")
            out[wid] = dict(drop_author=drop, author=author or None,
                            title=title or None,
                            note=(r.get("NOTE") or "").strip() or None,
                            seen_author=(r.get("work_author") or "").strip())
    return out


def apply(db_path, rulings=None, csv_path=None, say=print):
    source = "csv" if csv_path else "code"
    rulings = _read_csv(csv_path) if csv_path else dict(rulings or RULINGS)
    if not rulings:
        say("nothing to apply")
        return 0, 0
    con = sqlite3.connect(db_path)
    try:
        live = {w: (t, a) for w, t, a in con.execute(
            "SELECT work_id, MIN(work_title), MIN(work_author) FROM review_row "
            "GROUP BY work_id")}
        missing = sorted(w for w in rulings if w not in live)
        if missing:
            raise GateError(f"work_id(s) not in this db: {missing[:10]}")
        if csv_path:
            # the correction was made against what the file showed
            stale = [(w, r["seen_author"], live[w][1] or "")
                     for w, r in rulings.items()
                     if r["seen_author"] != (live[w][1] or "")]
            if stale:
                for w, seen, now in stale[:10]:
                    say(f"STALE {w}: file shows {seen!r}, db has {now!r}")
                raise GateError(
                    f"{len(stale)} row(s) corrected against an author the db no "
                    "longer carries -- re-export and redo those rows")
        con.execute("BEGIN")
        con.execute(DDL)
        n_auth = n_title = 0
        for wid, r in sorted(rulings.items()):
            title_now, author_now = live[wid]
            if r.get("drop_author"):
                if author_now is not None:
                    con.execute("UPDATE review_row SET work_author=NULL, "
                                "author_provenance=NULL WHERE work_id=?", (wid,))
                    n_auth += 1
                con.execute(
                    "INSERT OR REPLACE INTO work_author_ruling"
                    "(work_id, field, old_value, new_value, note, source) "
                    "VALUES (?,'author',?,NULL,?,?)",
                    (wid, author_now, r.get("note"), source))
            elif r.get("author"):
                if r["author"] != author_now:
                    con.execute("UPDATE review_row SET work_author=?, "
                                "author_provenance='owner_ruling' "
                                "WHERE work_id=?", (r["author"], wid))
                    n_auth += 1
                con.execute(
                    "INSERT OR REPLACE INTO work_author_ruling"
                    "(work_id, field, old_value, new_value, note, source) "
                    "VALUES (?,'author',?,?,?,?)",
                    (wid, author_now, r["author"], r.get("note"), source))
            if r.get("title"):
                if r["title"] != title_now:
                    con.execute("UPDATE review_row SET work_title=?, "
                                "title_provenance='owner_ruling' "
                                "WHERE work_id=?", (r["title"], wid))
                    n_title += 1
                con.execute(
                    "INSERT OR REPLACE INTO work_author_ruling"
                    "(work_id, field, old_value, new_value, note, source) "
                    "VALUES (?,'title',?,?,?,?)",
                    (wid, title_now, r["title"], r.get("note"), source))
        # title and author are facet columns; the viewer rebuilds the slim
        # projection on its next start
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass
    say(f"{len(rulings)} ruling(s) from {source}: {n_auth} author change(s), "
        f"{n_title} title change(s); facet_row dropped for rebuild")
    if n_auth or n_title:
        say("now rebuild: build_work_registry.py, then attach_review_cards.py")
    return n_auth, n_title


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--csv", default=None,
                    help="an edited work_authors.csv (default: the RULINGS "
                         "recorded in this file)")
    args = ap.parse_args(argv)
    apply(args.db, csv_path=args.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
