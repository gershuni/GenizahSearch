# -*- coding: utf-8 -*-
"""Report what the author surface still needs -- and ADD NOTHING.

Owner, 2026-09-01: "it seems there are more to be added (but carefully, do not
add without me approving)". So this script only ever REPORTS. It never writes an
author, never guesses one, and never touches the db: filling a cell stays a
human act, made in the export file and applied by
`scripts/apply_work_author_rulings.py`.

Three kinds, in one CSV, each row naming the work_id to find in
work_authors_edit.csv:

  missing_author    the work has no author at all, heaviest first (evidence
                    rows), so the gaps that cost the most reading come first
  duplicate_person  two author strings that look like the SAME person -- the
                    same acronym in parentheses, or one name contained in the
                    other. The whole author authority is "one canonical string
                    per person db-wide", so these are the rows that break it.
  ascii_quotes      a stored author still spelled with an ASCII quote instead of
                    gershayim. Edits are normalized on the way in, so these are
                    older values nothing has corrected yet.

    python -X utf8 scripts/report_author_issues.py
"""
import argparse
import csv
import os
import re
import sqlite3
import sys
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
DEFAULT_OUT = os.path.join(REPO_ROOT, "work_authors_issues.csv")

COLUMNS = ("kind", "detail", "work_id", "work_title", "work_author",
           "source_corpus", "evidence_rows", "kw_id", "kw_title")

_BARE = re.compile(r"[֑-ׇ׳״\"'()\[\]\s.,־-]")
_ACRONYM = re.compile(r"\(([^)]*)\)")


def bare(s):
    return _BARE.sub("", s or "")


def acronym_of(author):
    """The parenthetical acronym, if the string uses the canonical
    "full name (acronym)" form."""
    m = _ACRONYM.findall(author or "")
    return bare(m[-1]) if m else ""


def duplicate_groups(authors):
    """Author strings that plausibly name one person.

    Two signals, both conservative: the same parenthetical acronym, or one
    stripped name wholly contained in the other (a shortened form -- e.g.
    אברהם בן דוד (ראב״ד) beside אברהם בן דוד מפושקיירא (ראב״ד)). Names are
    NEVER merged here; the pair is reported for a human to rule on.
    """
    groups = defaultdict(set)
    for a in authors:
        ac = acronym_of(a)
        if ac:
            groups["acronym:" + ac].add(a)
    out = {k: v for k, v in groups.items() if len(v) > 1}
    ordered = sorted(authors, key=lambda s: (len(bare(s)), s))
    for i, short in enumerate(ordered):
        if not bare(short) or ";" in short:
            continue
        for long in ordered[i + 1:]:
            if bare(short) != bare(long) and bare(short) in bare(long):
                out.setdefault("contains:" + short, set()).update({short, long})
    return out


def report(db_path, out_path, say=print):
    con = sqlite3.connect("file:%s?mode=ro" % db_path.replace("\\", "/"),
                          uri=True, timeout=60)
    con.row_factory = sqlite3.Row
    kw = {}
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                   "name='known_work_member'").fetchone():
        for r in con.execute(
                "SELECT m.work_id AS w, k.kw_id AS kid, k.title AS t "
                "FROM known_work_member m JOIN known_work k "
                "ON k.kw_id = m.kw_id"):
            kw.setdefault(r["w"], (r["kid"], r["t"]))
    works = [dict(work_id=r["work_id"], title=r["t"] or "", author=r["a"],
                  corpus=r["c"], rows=r["n"])
             for r in con.execute(
                 "SELECT work_id, MIN(work_title) AS t, MIN(work_author) AS a, "
                 "MIN(source_corpus) AS c, COUNT(*) AS n FROM review_row "
                 "GROUP BY work_id")]
    con.close()

    def row(kind, detail, w):
        k = kw.get(w["work_id"], ("", ""))
        return dict(kind=kind, detail=detail, work_id=w["work_id"],
                    work_title=w["title"], work_author=w["author"] or "",
                    source_corpus=w["corpus"], evidence_rows=w["rows"],
                    kw_id=k[0], kw_title=k[1])

    out = []
    missing = sorted((w for w in works if not w["author"]),
                     key=lambda w: (-w["rows"], w["work_id"]))
    out += [row("missing_author", "", w) for w in missing]

    authored = [w for w in works if w["author"]]
    by_author = defaultdict(list)
    for w in authored:
        by_author[w["author"]].append(w)
    for key, names in sorted(duplicate_groups(set(by_author)).items()):
        detail = " | ".join(sorted(names))
        for a in sorted(names):
            for w in by_author[a]:
                out.append(row("duplicate_person", detail, w))

    for w in authored:
        if '"' in w["author"] or "'" in w["author"]:
            out.append(row("ascii_quotes", "spell with gershayim ״ / geresh ׳",
                           w))

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=COLUMNS)
        wr.writeheader()
        wr.writerows(out)
    kinds = defaultdict(int)
    for r in out:
        kinds[r["kind"]] += 1
    say(f"{len(out)} rows -> {out_path}")
    for k in sorted(kinds):
        say(f"  {k}: {kinds[k]}")
    say(f"  (works with an author: {len(authored)} of {len(works)})")
    say("nothing was written to the db and no author was filled in")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    report(args.db, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
