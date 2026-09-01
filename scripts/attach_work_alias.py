# -*- coding: utf-8 -*-
"""Attach `work_alias_fact`: cross-corpus same-work links (owner, 2026-08-31).

The same work often appears twice -- an R-source whole-work file and the base
corpus's per-book works. Links were built in three steps: span-agreement
candidates (612 pairs) -> an excerpt-comparison LLM gate (same wording vs
parallel versions vs shared material, $0.69) -> owner rulings (green+yellow
approved; two red-band recoveries; the Targum/responsa false links rejected).
A link NEVER merges identities -- offsets keep their raw file -- it renders as
a chip and harmonizes author strings within a cluster.

Run (review server STOPPED):
    python -X utf8 scripts/attach_work_alias.py
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUCKETS = os.path.join(REPO_ROOT, "_tmp", "work_alias", "final_buckets.json")

# owner recoveries from the red band (2026-08-31): (rs_title, base_title
# substring). "תשובות הרמב\"ם ד" was approved with an explicit "I think".
OWNER_RECOVERIES = (
    ("מדרש רבה", "שיר השירים רבה"),
    ("שו״ת הרמב״ם", "תשובות הרמב"),
)

# owner rulings 2026-09-01: these gate links are WRONG -- shared wording came
# from quotation, not identity ("two different works"). They are removed from
# work_alias_fact (an identity table); the dedup registry may later record them
# as shares-material edges, which is a different assertion.
#   - פירוש הר"ן על נדרים vs ר"ן על הרי"ף: two different works by the same author
#   - הלכות שמחות (מהר"ם מרוטנבורג, quotes ראב"ד/רי"ץ גיאת) vs הלכות גדולות
OWNER_REJECTED_LINKS = (
    ("פירוש הר״ן על נדרים", 'ר"ן על הרי"ף'),
    ("הלכות שמחות", "הלכות גדולות"),
)

# link-scan batch (12-agent sweep + verification), owner rulings 2026-09-01.
# Keyed by (rs_work, base_work) ids -- titles are ambiguous here (two files
# share the title מדרש רבה).
OWNER_REJECTED_LINK_IDS = (
    # the MEGILLOT מדרש רבה file has no ויקרא section; the 9 shared pages are
    # the איכה רבה / ויקרא רבה shared-petichta material
    ("rs8d952b934ee0", "w001463"),
    # תוספות הרא"ש vs the generic printed תוספות on three tractates, and
    # תוספות רי"ד vs תוספות על חולין: different works commenting on the same
    # sugyot (4-7 shared pages; genuine same-work links run 73-1206)
    ("rs68924ece1f0e", "w001360"),
    ("rs68924ece1f0e", "w001379"),
    ("rs68924ece1f0e", "w001380"),
    ("rs18ae0a685c68", "w001357"),
    # רב ניסים גאון vs the JA חמשה ספרים: 3 shared pages, quotation-grade
    # evidence (both sides quote the same Mishnah)
    ("rs899c1a52f054", "w000071"),
    # registry step-0 owner rulings 2026-09-01 (shares-material, not identity):
    # Tanchuma is TWO known works (Warsaw printed / Buber recensions); the
    # Buber rs file keeps only its Buber partners
    ("rs685e0a5435ce", "w000926"),
    ("rs685e0a5435ce", "w001479"),
    # מגדל דוד is a reworking of ספר המצוות, not the same composition
    ("rs7b1f1a344792", "w000025"),
    # geonic responsa collections: a responsum shared between collections is
    # shared material; each collection is its own work (הרכבי keeps only its
    # own-edition partner w001517)
    ("rs01fd4809e0ff", "w000572"),
    ("rs01fd4809e0ff", "w000660"),
    ("rs01fd4809e0ff", "w000662"),
    ("rs01fd4809e0ff", "w000675"),
    ("rs292b4363df45", "w000590"),
)

# kind corrections: the rs מסכתות קטנות file is much broader than the base
# שבע מסכתות קטנות (it includes all of אבות דרבי נתן) -- containment, not
# identity. Owner also classified the family as an ANTHOLOGY for the registry.
OWNER_KIND_FIXES = (
    ("rs1041ff83b2c9", "w000756", "same_work_contains"),
)

DDL = """CREATE TABLE work_alias_fact(
  rs_work      TEXT NOT NULL,
  base_work    TEXT NOT NULL,
  kind         TEXT NOT NULL CHECK (kind IN ('same_work','same_work_contains')),
  source       TEXT NOT NULL CHECK (source IN ('gate','owner_ruling')),
  shared_pages INTEGER,
  PRIMARY KEY (rs_work, base_work)
)"""

DOC = ("Cross-corpus same-work links: the R-source work and the base-corpus "
       "work are one literary work listed twice (same_work), or one contains "
       "the other (same_work_contains -- a whole-work file vs per-book works). "
       "Built from span-agreement candidates, an excerpt-comparison model "
       "gate, and owner rulings; parallel versions (different Targumim, "
       "commentaries sharing a base text) and responsa collections sharing "
       "responsa are NOT linked unless the owner ruled them so. A link never "
       "merges identities or moves rows between pools -- it renders as a "
       "'same work under another corpus' chip, and author strings are "
       "harmonized inside a linked cluster.")


def load_links():
    b = json.load(open(BUCKETS, encoding="utf-8"))
    links = []
    for r in b["link"]:
        links.append((r["rs_work"], r["base_work"], r["verdict"], "gate",
                      r["shared_pages"]))
    for r in b["owner_queue"]:
        links.append((r["rs_work"], r["base_work"],
                      r["verdict"] if r["verdict"] in
                      ("same_work", "same_work_contains") else "same_work",
                      "owner_ruling", r["shared_pages"]))
    norm = lambda s: (s or "").replace('"', "").replace("״", "")
    for r in b["reject"]:
        for rst, bt in OWNER_RECOVERIES:
            if norm(r["rs_title"]) == norm(rst) and norm(bt) in norm(r["base_title"]):
                links.append((r["rs_work"], r["base_work"], "same_work_contains",
                              "owner_ruling", r["shared_pages"]))
    # dedup on the PK, first entry wins
    seen, out = set(), []
    for l in links:
        if (l[0], l[1]) not in seen:
            seen.add((l[0], l[1]))
            out.append(l)
    return out


def drop_rejected_links(con, say=print):
    """Remove owner-rejected identity links by TITLE pair (ruling 2026-09-01)."""
    n = 0
    for rst, bt in OWNER_REJECTED_LINKS:
        cur = con.execute(
            "DELETE FROM work_alias_fact WHERE rowid IN ("
            " SELECT w.rowid FROM work_alias_fact w"
            " JOIN (SELECT DISTINCT work_id, work_title FROM review_row) tr"
            "   ON tr.work_id = w.rs_work"
            " JOIN (SELECT DISTINCT work_id, work_title FROM review_row) tb"
            "   ON tb.work_id = w.base_work"
            " WHERE tr.work_title = ? AND tb.work_title = ?)", (rst, bt))
        say("  rejected link %r -> %r: removed %d" % (rst, bt, cur.rowcount))
        n += cur.rowcount
    for rs, bw in OWNER_REJECTED_LINK_IDS:
        cur = con.execute(
            "DELETE FROM work_alias_fact WHERE rs_work = ? AND base_work = ?",
            (rs, bw))
        say("  rejected link %s -> %s: removed %d" % (rs, bw, cur.rowcount))
        n += cur.rowcount
    for rs, bw, kind in OWNER_KIND_FIXES:
        cur = con.execute(
            "UPDATE work_alias_fact SET kind = ? WHERE rs_work = ? AND base_work = ?",
            (kind, rs, bw))
        say("  kind fix %s -> %s: %s (%d)" % (rs, bw, kind, cur.rowcount))
    return n


def harmonize_authors(con, say):
    """Inside each linked cluster, give the rs work the base corpus's curated
    author string -- but never overwrite an owner-ruled author."""
    n_updates = 0
    rs_works = [r[0] for r in con.execute(
        "SELECT DISTINCT rs_work FROM work_alias_fact")]
    for rs in rs_works:
        cur = con.execute(
            "SELECT DISTINCT work_author, author_provenance FROM review_row "
            "WHERE work_id = ?", (rs,)).fetchall()
        if not cur:
            continue
        author, prov = cur[0]
        if prov == "owner_ruling":
            continue
        base_authors = Counter(a for (a,) in con.execute(
            "SELECT r.work_author FROM work_alias_fact w "
            "JOIN review_row r ON r.work_id = w.base_work "
            "WHERE w.rs_work = ? AND r.work_author IS NOT NULL "
            "AND r.work_author != ''", (rs,)))
        if not base_authors:
            continue
        best = base_authors.most_common(1)[0][0]
        if best and best != author:
            c = con.execute(
                "UPDATE review_row SET work_author = ?, "
                "author_provenance = 'alias_harmonized' WHERE work_id = ?",
                (best, rs))
            say("  author: %s: %r -> %r (%d rows)" % (rs, author, best, c.rowcount))
            n_updates += c.rowcount
    return n_updates


def attach(db_path, say=print):
    links = load_links()
    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS work_alias_fact")
        con.execute(DDL)
        con.executemany("INSERT INTO work_alias_fact VALUES (?,?,?,?,?)", links)
        # the twin-chip attacher queries by page; review_row never had this index
        con.execute("CREATE INDEX IF NOT EXISTS ix_rr_page ON review_row(page_id)")
        drop_rejected_links(con, say)
        n_auth = harmonize_authors(con, say)
        n_live = con.execute("SELECT COUNT(*) FROM work_alias_fact").fetchone()[0]
        for k, v in (("work_alias.version", "1"),
                     ("work_alias.count", str(n_live)),
                     ("doc.work_alias", DOC)):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
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
    say("work_alias_fact: %d links live (%d before owner rejections); "
        "%d author rows harmonized" % (n_live, len(links), n_auth))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        raise SystemExit("missing: %s" % args.db)
    attach(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
