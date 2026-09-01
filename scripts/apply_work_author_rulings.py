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
import re
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


_GERSHAYIM = re.compile(r"(?<=[֐-ת])\"(?=[֐-ת])")
_GERESH = re.compile(r"(?<=[֐-ת])'")


def normalize_hebrew_quotes(s):
    """ASCII quotes typed on a keyboard -> the corpus's gershayim/geresh.

    The canonical author form here is "full name + acronym" with HEBREW
    gershayim -- רמב״ם, not רמב"ם -- and `attach_author_authority.VARIANTS`
    already exists to map the ASCII spellings onto the Hebrew ones. So an
    edited cell typed as ראב"ש is the same name as ראב״ש, and storing it
    verbatim would put two spellings of one person in a db whose whole author
    authority is "one string per person". Only quotes BETWEEN Hebrew letters
    (or a geresh after one) are touched; nothing else in the string is.
    """
    if not s:
        return s
    return _GERESH.sub("׳", _GERSHAYIM.sub("״", s))


def _read_csv(path):
    """Read the edited export.

    The owner edits `work_author` / `work_title` IN PLACE, so an edit is the
    diff against the `ORIG_*` baseline the exporter wrote: different text is a
    change, an emptied cell is a drop, an untouched cell is nothing. The older
    DROP_AUTHOR / NEW_AUTHOR / NEW_TITLE columns are still honoured so a file
    exported before this change still applies -- but a row that uses BOTH ways
    is refused rather than resolved.
    """
    out = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        cols = set(rdr.fieldnames or ())
        if "work_id" not in cols:
            raise GateError(f"{path} has no work_id column -- is it the file "
                            "scripts/export_work_authors.py writes?")
        in_place = "ORIG_AUTHOR" in cols or "ORIG_TITLE" in cols
        for i, r in enumerate(rdr, start=2):
            wid = (r.get("work_id") or "").strip()
            if not wid:
                continue
            drop = (r.get("DROP_AUTHOR") or "").strip() in TRUTHY
            author = normalize_hebrew_quotes((r.get("NEW_AUTHOR") or "").strip())
            title = normalize_hebrew_quotes((r.get("NEW_TITLE") or "").strip())
            old_cols = bool(drop or author or title)
            edit_author = edit_title = None
            if in_place:
                cell_a = normalize_hebrew_quotes(
                    (r.get("work_author") or "").strip())
                cell_t = normalize_hebrew_quotes(
                    (r.get("work_title") or "").strip())
                # BOTH sides normalized, or a stored value that itself uses
                # ASCII quotes would read as an edit on every cycle and the
                # export/apply loop would invent changes nobody made
                base_a = normalize_hebrew_quotes(
                    (r.get("ORIG_AUTHOR") or "").strip())
                base_t = normalize_hebrew_quotes(
                    (r.get("ORIG_TITLE") or "").strip())
                if cell_a != base_a:
                    edit_author = cell_a          # "" means: clear the author
                if cell_t != base_t:
                    if not cell_t:
                        raise GateError(
                            f"line {i} ({wid}): work_title was emptied. A work "
                            "must keep a title -- correct it instead of "
                            "clearing it.")
                    edit_title = cell_t
            if old_cols and (edit_author is not None or edit_title is not None):
                raise GateError(
                    f"line {i} ({wid}): edited in place AND filled the "
                    "DROP_AUTHOR/NEW_AUTHOR/NEW_TITLE columns -- pick one way")
            if drop and author:
                raise GateError(f"line {i} ({wid}): DROP_AUTHOR and NEW_AUTHOR "
                                "both set -- which is it?")
            if edit_author is not None:
                drop, author = (edit_author == ""), (edit_author or "")
            if edit_title is not None:
                title = edit_title
            if not (drop or author or title):
                continue
            out[wid] = dict(drop_author=drop, author=author or None,
                            title=title or None,
                            note=(r.get("NOTE") or "").strip() or None,
                            # the baseline is what the correction was made
                            # against; with the old columns, the cell itself is
                            seen_author=((r.get("ORIG_AUTHOR") or "").strip()
                                         if in_place
                                         else (r.get("work_author") or "").strip()),
                            kw_author=(r.get("kw_author") or "").strip(),
                            line=i)
    return out


def apply(db_path, rulings=None, csv_path=None, say=print, dry_run=False):
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
            # An edit to kw_author cannot work: the identity's author is
            # DERIVED from its witnesses by the registry. Refuse rather than
            # ignore -- silence would read as "applied".
            kw_live = {}
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                           "name='known_work_member'").fetchone():
                kw_live = {w: (a or "") for w, a in con.execute(
                    "SELECT m.work_id, k.author FROM known_work_member m "
                    "JOIN known_work k ON k.kw_id = m.kw_id")}
            edited_kw = [(w, r["kw_author"], kw_live.get(w, ""))
                         for w, r in rulings.items()
                         if r.get("kw_author") and w in kw_live
                         and r["kw_author"] != kw_live[w]]
            if edited_kw:
                for w, got, now in edited_kw[:10]:
                    say(f"KW EDIT {w}: file has kw_author {got!r}, db has "
                        f"{now!r}")
                raise GateError(
                    f"{len(edited_kw)} row(s) edited kw_author. The known "
                    "work's author is re-derived from its witnesses -- correct "
                    "work_author instead, on every witness of that work.")
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
        # what this file would do, in the reader's own terms, BEFORE any write:
        # an author cleared, corrected, or supplied where there was none are
        # three different acts and are counted as three
        kinds = {"author cleared": [], "author changed": [],
                 "author added": [], "title changed": []}
        for wid, r in sorted(rulings.items()):
            title_now, author_now = live[wid]
            if r.get("drop_author") and author_now is not None:
                kinds["author cleared"].append((wid, author_now, None))
            elif r.get("author") and r["author"] != author_now:
                kinds["author added" if not author_now
                      else "author changed"].append(
                          (wid, author_now, r["author"]))
            if r.get("title") and r["title"] != title_now:
                kinds["title changed"].append((wid, title_now, r["title"]))
        for k in ("author cleared", "author changed", "author added",
                  "title changed"):
            if not kinds[k]:
                continue
            say(f"{k}: {len(kinds[k])}")
            for wid, old, new in kinds[k][:8]:
                say(f"    {wid}: {old!r} -> {new!r}")
            if len(kinds[k]) > 8:
                say(f"    ... and {len(kinds[k]) - 8} more")
        if dry_run:
            say("DRY RUN -- nothing written")
            return (len(kinds["author cleared"]) + len(kinds["author changed"])
                    + len(kinds["author added"]), len(kinds["title changed"]))
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
    ap.add_argument("--dry-run", action="store_true",
                    help="report what the file would change and write nothing")
    args = ap.parse_args(argv)
    apply(args.db, csv_path=args.csv, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
