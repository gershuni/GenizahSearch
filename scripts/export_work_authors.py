# -*- coding: utf-8 -*-
"""Export every work with its title and author, for the owner to correct.

One row per `work_id` in the review db. **Edit `work_author` and `work_title`
in place**: change the text to correct it, or CLEAR the cell to say the work has
none. `NOTE` is free text carried into the ruling record. Everything else is
reference material.

`ORIG_AUTHOR` / `ORIG_TITLE` at the end are the untouched baseline. That is what
makes in-place editing safe: `scripts/apply_work_author_rulings.py` reads an
edit as the DIFF between your cell and the baseline, and refuses the file if the
baseline no longer matches the db -- which would mean you corrected a value some
other pass has since changed. Do not edit those two columns, and do not edit the
`kw_*` columns (the identity's author is re-derived from the witnesses; editing
it there would do nothing, so the applier refuses instead of ignoring you).

Deleting a whole ROW is not a deletion of anything: the applier acts only on the
rows present, so a removed row simply goes uncorrected.

The `FLAG` column is a hint, never a decision -- it names the pattern that made
a row worth a look:

    author_in_title      the author is already spelled inside the title, so it
                         adds nothing (this is what the retired from-title
                         derivation produced)
    anonymous            the author field says "anonymous", which is not an
                         author but the absence of one
    attribution_gloss    "attributed to X", a dating, or a century -- an
                         editorial note about the attribution
    title_in_author      a parenthetical that looks like an alternate TITLE
                         sitting in the author field
    relation_to_person   "student of X", "sons of X" -- names a relation, not
                         the author

Written UTF-8 with a BOM so Excel opens the Hebrew correctly.

    python -X utf8 scripts/export_work_authors.py
"""
import argparse
import csv
import os
import re
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
DEFAULT_OUT = os.path.join(REPO_ROOT, "work_authors.csv")

# The two EDITABLE columns come first, right after what identifies the row; the
# baseline and the identity's own strings sit at the end, out of the way.
EDITABLE = ("work_title", "work_author")
COLUMNS = ("work_id", "source_corpus", "evidence_rows") + EDITABLE + (
    "FLAG", "NOTE", "author_provenance", "title_provenance", "kw_id",
    "kw_title", "kw_author", "kw_author_basis", "ORIG_TITLE", "ORIG_AUTHOR")

_STRIP = re.compile(r"[֑-ׇ׳״\"'()\[\]\s.,־-]")
# a dating or an attribution note, not a person
_GLOSS = re.compile(r"מיוחס|נתחבר|נכתב|המאה|במאות|סביב|לפני|אחרי|\d")
_RELATION = re.compile(r"^\s*(תלמיד|תלמידי|בני|בן|חוג|בית מדרש)\b")
# A parenthetical is NOT suspicious by itself: "full name + acronym" -- e.g.
# משה בן מימון (רמב״ם) -- is the owner's ruled canonical form, and flagging the
# 249 works that use it would bury the handful whose parenthetical is really a
# TITLE sitting in the author field. So the flag fires only when the
# parenthetical names a WORK.
_TITLE_WORD = re.compile("מדרש|ספר|שו״?ת|פירוש|מסכת|הלכות|סדר|מגילת|תרגום|"
                         "פרקי|ברייתא|תוספתא")
_PAREN = re.compile(r"\(([^)]*)\)")


def bare(s):
    return _STRIP.sub("", s or "")


def flag_of(title, author):
    if not author:
        return ""
    if "אנונימי" in author or "לא ידוע" in author:
        return "anonymous"
    if _RELATION.search(author):
        return "relation_to_person"
    if _GLOSS.search(author):
        return "attribution_gloss"
    if ";" in author:
        # author + translator, e.g. "X; תרגום Y". Sanctioned (the compound
        # legitimately differs from the bare author) -- named so it is visible,
        # not because it is wrong.
        return "compound_credit"
    inner = " ".join(_PAREN.findall(author))
    if inner and _TITLE_WORD.search(inner):
        return "title_in_author"
    if bare(author) and bare(author) in bare(title):
        return "author_in_title"
    return ""


def export(db_path, out_path, say=print):
    con = sqlite3.connect("file:%s?mode=ro" % db_path.replace("\\", "/"),
                          uri=True, timeout=60)
    con.row_factory = sqlite3.Row
    kw = {}
    have_kw = bool(con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='known_work_member'").fetchone())
    if have_kw:
        for r in con.execute(
                "SELECT m.work_id AS w, k.kw_id AS kid, k.title AS t, "
                "k.author AS a, k.author_basis AS ab FROM known_work_member m "
                "JOIN known_work k ON k.kw_id = m.kw_id"):
            kw.setdefault(r["w"], r)
    rows = []
    for r in con.execute(
            "SELECT work_id, MIN(work_title) AS t, MIN(work_author) AS a, "
            "MIN(author_provenance) AS ap, MIN(title_provenance) AS tp, "
            "MIN(source_corpus) AS c, COUNT(*) AS n FROM review_row "
            "GROUP BY work_id"):
        k = kw.get(r["work_id"])
        rows.append({
            "work_id": r["work_id"], "source_corpus": r["c"],
            "evidence_rows": r["n"], "work_title": r["t"] or "",
            "work_author": r["a"] or "", "author_provenance": r["ap"] or "",
            "title_provenance": r["tp"] or "",
            "kw_id": (k["kid"] if k else ""), "kw_title": (k["t"] if k else ""),
            "kw_author": (k["a"] if k else "") or "",
            "kw_author_basis": (k["ab"] if k else ""),
            "FLAG": flag_of(r["t"], r["a"]),
            # the baseline an edit is measured against
            "ORIG_TITLE": r["t"] or "", "ORIG_AUTHOR": r["a"] or "",
            "NOTE": ""})
    con.close()
    # flagged first, then works that have an author at all, then by weight:
    # the rows worth an owner's minutes are at the top of the file
    rows.sort(key=lambda d: (not d["FLAG"], not d["work_author"],
                             -d["evidence_rows"], d["work_id"]))
    try:
        f = open(out_path, "w", encoding="utf-8-sig", newline="")
    except PermissionError:
        # Excel holds an exclusive lock on an open workbook. Overwriting is not
        # the failure to worry about -- silently clobbering edits in progress
        # is -- so say which file and stop.
        raise SystemExit(
            f"cannot write {out_path}: it is open in another program (Excel "
            "locks it). Close it, or pass --out with a different filename so "
            "your edits in progress are left alone.")
    with f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    n_auth = sum(1 for d in rows if d["work_author"])
    flags = {}
    for d in rows:
        if d["FLAG"]:
            flags[d["FLAG"]] = flags.get(d["FLAG"], 0) + 1
    say(f"{len(rows)} works -> {out_path}")
    say(f"  with an author: {n_auth}; without: {len(rows) - n_auth}")
    for k in sorted(flags):
        say(f"  flagged {k}: {flags[k]}")
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    export(args.db, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
