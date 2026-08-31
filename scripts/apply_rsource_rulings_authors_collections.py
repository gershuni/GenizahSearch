# -*- coding: utf-8 -*-
"""Apply the 2026-08-30 owner rulings + the two approved metadata passes.

Owner delegated: "עשה לפי המלצתך" (do as recommended), so each change below
records that provenance.

1. RULINGS on the 12 medium-risk works: EXCLUDE מנורת המאור (an aggadic
   anthology -- the Yalkut precedent) and קטעי מדרשים (not one work at all: a
   modern collection of fragments); KEEP the other ten, with a caution note on
   כללי המצוות (mostly parallel matches). Applied to the adapter's works table
   (the track's source of record) AND to the review rows.

2. COLLECTION FILES: seven source files hold more than one work under a
   file-level title naming only one member. Retitled as collections (Hebrew
   only, hand-written from the files' own member names);
   `title_provenance='collection_retitle'`; the viewer chips it and the locus
   keeps naming the actual sub-work.

3. AUTHORS, derived verbatim from titles: a curated list of PERSON tokens; a
   work whose title contains exactly ONE of them gets that token as its
   author, marked `author_provenance='from_title'`. No expansion, no new
   attribution claims -- the title already asserts it. Ambiguous or
   token-free titles stay NULL.

Run (review server STOPPED):
    python -X utf8 scripts/apply_rsource_rulings_authors_collections.py
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TODAY = "2026-08-30"
NOTE = "owner adopted the recommendation (2026-08-30 review round)"

EXCLUDE = {
    "rsff31c5930082": "anthology of aggadot -- the Yalkut precedent: a match "
                      "usually witnesses the source it compiled",
    "rsda6783a0036a": "not one work: a modern collection of midrash fragments",
}
KEEP = {
    "rs075a7d47f2c0": "", "rs4ae678d10b0b": "", "rs3d8f8b83c965": "",
    "rs67be89fcf116": "", "rs3787f747b78c": "", "rseb524c17d3d4": "",
    "rsadc96920286f": "", "rs6f38d40a1a87": "", "rs67ba1d681071": "",
    "rse4e3d3d61817": "kept with caution: matches are mostly parallel "
                      "(26 witness / 266 parallel)",
}

# raw_id -> collection title (Hebrew only; written from the file's own members)
COLLECTIONS = {
    "RS:10.0.1": "אגרות רמב״ם ורמב״ן (אסופה)",
    "RS:10.0.17": "יסוד מורא והיראה (אסופה)",
    "RS:8.0.18": "דרשות לפסח ולראש השנה — רוקח ורמב״ן (אסופה)",
    "RS:8.0.20": "דרשות מהר״ח אור זרוע והגדות ריטב״א ורשב״ץ (אסופה)",
    "RS:8.0.23": "הלכות ברכות לריטב״א והלכות לולב לראב״ד (אסופה)",
    "RS:8.0.47": "המנהגות ומנהגי הרב זלמן יענט (אסופה)",
    "RS:8.0.57": "מצוות זמניות ומשפט החרם (אסופה)",
}

# person tokens, exactly as they appear inside titles (gershayim U+05F4)
PERSONS = (
    "רש״י", "רמב״ם", "רמב״ן", "רשב״א", "ריטב״א", "רא״ש", "ר״ן", "רי״ד",
    "רי״ף", "ראב״ד", "רד״ק", "רשב״ם", "רשב״ץ", "ריב״ש", "רוקח", "מאירי",
    "מהר״ם מרוטנבורג", "מהרי״ל", "מהר״ח אור זרוע", "רבינו יונה",
    "רבינו חננאל", "רבינו גרשום", "אבן עזרא", "אברבנאל", "רלב״ג", "ספורנו",
    "רב נטרונאי גאון", "רב שרירא גאון", "רב עמרם גאון", "רב סעדיה גאון",
    "רס״ג", "בעל המאור", "רבינו בחיי", "ר׳ יוסף בן עקנין",
)


def derive_author(title):
    hits = [p for p in PERSONS if p in (title or "")]
    # longest-token containment can double count (מהר״ם מרוטנבורג contains
    # nothing else here, but keep the rule strict): exactly one distinct hit
    hits = [h for h in hits if not any(h != o and h in o for o in hits)]
    return hits[0] if len(hits) == 1 else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--review-db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--adapter", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "gr-adapter.db"))
    args = ap.parse_args(argv)

    # ---- map raw_id -> work_id, and collect titles ------------------------
    con = sqlite3.connect(args.review_db)
    con.row_factory = sqlite3.Row
    rid2wid = dict(con.execute(
        "SELECT rw.raw_id, r.work_id FROM review_row r "
        "JOIN reference_witness rw ON rw.witness_id = r.witness_id "
        "WHERE r.source_corpus='rsource' GROUP BY 1"))
    titles = dict(con.execute(
        "SELECT work_id, work_title FROM review_row "
        "WHERE source_corpus='rsource' GROUP BY 1"))
    missing = [r for r in COLLECTIONS if r not in rid2wid]
    if missing:
        raise SystemExit("collection raw ids not in artifact: %s" % missing)

    retitle = {rid2wid[r]: t for r, t in COLLECTIONS.items()}
    authors = {}
    for wid, title in titles.items():
        if wid in retitle:
            continue                      # a collection has no single author
        a = derive_author(title)
        if a:
            authors[wid] = a

    # ---- review db ---------------------------------------------------------
    have = {r[1] for r in con.execute("PRAGMA table_info(review_row)")}
    if "author_provenance" not in have:
        con.execute("ALTER TABLE review_row ADD COLUMN author_provenance TEXT")
    con.execute("BEGIN")
    for wid, why in EXCLUDE.items():
        con.execute("UPDATE review_row SET owner_ruling="
                    "'excluded_from_public_identities', owner_ruling_date=?, "
                    "owner_ruling_note=? WHERE work_id=?",
                    (TODAY, "%s -- %s" % (why, NOTE), wid))
    for wid, extra in KEEP.items():
        con.execute("UPDATE review_row SET owner_ruling='kept_by_owner_ruling',"
                    " owner_ruling_date=?, owner_ruling_note=? WHERE work_id=?",
                    (TODAY, (extra + " -- " if extra else "") + NOTE, wid))
    for wid, t in retitle.items():
        con.execute("UPDATE review_row SET work_title=?, "
                    "title_provenance='collection_retitle' WHERE work_id=?",
                    (t, wid))
    for wid, a in authors.items():
        con.execute("UPDATE review_row SET work_author=?, "
                    "author_provenance='from_title' "
                    "WHERE work_id=? AND work_author IS NULL", (a, wid))
    n_auth = con.execute("SELECT COUNT(DISTINCT work_id) FROM review_row "
                         "WHERE author_provenance='from_title'").fetchone()[0]
    for k, v in (("rsource_rulings.applied",
                  "exclude %d / keep %d works, %s" % (len(EXCLUDE), len(KEEP),
                                                      TODAY)),
                 ("rsource_collections.retitled", str(len(retitle))),
                 ("rsource_authors.derived_works", str(n_auth)),
                 ("rsource_authors.method",
                  "single person-token contained verbatim in the title; "
                  "marked author_provenance='from_title'"),
                 ("rsource_rulings.at", time.strftime("%Y-%m-%d %H:%M:%S"))):
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    con.execute("DROP TABLE IF EXISTS facet_row")   # title/author are facets
    con.execute("COMMIT")
    con.close()

    # ---- adapter (the track's source of record) ----------------------------
    ad = sqlite3.connect(args.adapter)
    for wid, why in EXCLUDE.items():
        ad.execute("UPDATE works SET owner_ruling="
                   "'excluded_from_public_identities', owner_ruling_date=?, "
                   "owner_ruling_note=? WHERE work_id=?",
                   (TODAY, "%s -- %s" % (why, NOTE), wid))
    for wid, extra in KEEP.items():
        ad.execute("UPDATE works SET owner_ruling='kept_by_owner_ruling', "
                   "owner_ruling_date=?, owner_ruling_note=? WHERE work_id=?",
                   (TODAY, (extra + " -- " if extra else "") + NOTE, wid))
    for wid, t in retitle.items():
        ad.execute("UPDATE works SET neutral_title=?, "
                   "title_provenance='collection_retitle' WHERE work_id=?",
                   (t, wid))
    for wid, a in authors.items():
        ad.execute("UPDATE works SET author=? WHERE work_id=? "
                   "AND (author IS NULL OR author='')", (a, wid))
    ad.execute("INSERT OR REPLACE INTO meta VALUES "
               "('owner_rulings_2026_08_30', 'exclude Menorat haMaor + "
               "Kitei Midrashim; keep 10; 7 collection retitles; authors "
               "from titles')")
    ad.commit()
    ad.close()

    print("rulings   : %d excluded, %d kept" % (len(EXCLUDE), len(KEEP)))
    print("collections: %d retitled" % len(retitle))
    print("authors   : %d works derived from their titles" % n_auth)
    return 0


if __name__ == "__main__":
    sys.exit(main())
