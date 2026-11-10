# -*- coding: utf-8 -*-
"""Attach `formula_fact`: the liturgy/formulary detector (owner + Codex, 2026-08-30).

Three kinds, each measured before it was believed:

  embedded_section   -- a liturgy or formulary SECTION inside a non-liturgy
                        work (Mishneh Torah's prayer appendix, Seder Rav
                        Amram's orders, Machzor Vitry's Haggadah, Sefer
                        ha-Shetarot's deeds), detected from the row's own
                        resolved section header. The carrier text is a fixed
                        formula every siddur/notary shares -- it does not
                        identify the page as a witness of THIS work.
                        Measured on the owner's grades: 5/5 embedded formulas
                        caught, 0/233 witnesses hit.  -> shared_quotes pool.

  standalone_unit    -- the claimed work IS a standalone liturgy unit (an
                        Amidah, a Haggadah). Coverage measures quantity, not
                        distinctiveness: a generic prayer excerpt cannot
                        identify a page (Codex ruling; the owner's own grades
                        split both ways under two framings, so the default is
                        honest ambiguity).  -> barred from main, lands in
                        unclear; the catalogue may ORDER that queue but never
                        decides identification.

  documentary_page   -- the manuscript page is catalogued as a legal document
                        ONLY (a deed, a get, a ketubah) and the claimed work
                        is not canonical scripture: the match is most likely
                        the document's own formula quoted by the work.
                        Multi-tag reuse pages (a deed on one side, Bible on
                        the other -- "מקרא [טקסט];תעודות") are spared.
                        CONTEXT ONLY (owner, 2026-08-30): this kind is
                        catalogue-derived, and the catalogue never judges an
                        identification -- it renders as a chip and moves
                        nothing between pools.

Run (review server STOPPED):
    python -X utf8 scripts/attach_formula_flags.py
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# the WHOLE liturgy parent: Common Prayers, Brakhot, Passover Haggadah,
# Karaite Prayers -- a match to any standalone liturgy unit is ambiguous by
# nature, whichever leaf it sits in
LITURGY_DOMAIN_PREFIX = "Liturgy and Brakhot / "

# liturgy/formulary section headers (the locus). NEG: a section whose head is
# 'הלכות X' is halakha ABOUT prayer, not prayer -- unless it also names a rite
# order or a formulary explicitly.
KW = re.compile("סדר תפלות|סדר תפילות|ברכות השחר|סדור|סידור|מחזור|קדיש|פיוט|"
                "נוסח הברכות|נוסח התפלה|נוסח התפילה|נוסח הגט|נוסח הכתובה|"
                "נוסח ההגדה|סדר ההגדה|"
                "שטר|כתובה|סדר רב עמרם|סדר ראש השנה|סדר יום הכפורים|"
                "סדר תעניות|הגדה של פסח|עמידה|שמונה עשרה|ברכת המזון")
NEG_HEAD = re.compile(r"^הלכות ")
NEG_OVERRIDE = re.compile("סדר תפלות|סדר תפילות|נוסח")

# a page tag is DOCUMENTARY-ONLY when it names legal-document classes and no
# literary class at all
DOC_TAG = re.compile("שטר|תעודות|תעודה|מסמכ|כתובה|גט |גיטין")
LITERARY_TAG = re.compile("מקרא|תנ\"ך|פירוש|פרשנות|תפלה|תפילה|פיוט|הלכ|מדרש|"
                          "תלמוד|משנה|תוספתא|קבלה|דקדוק|מסורה|הגדה|ברכות|"
                          "סידור|סדור|שו\"ת|תשובה|ספרות|שיר|אגד|תרגום|מוסר|"
                          "פילוסופ|רפוא|לקסיק|מילון")
CANONICAL_DOMAINS_SQL = (
    "(r.domain LIKE 'Bible:%' OR r.domain LIKE 'Mishnah:%' "
    "OR r.domain LIKE 'Talmud Bavli:%' OR r.domain LIKE 'Massorah%' "
    "OR r.domain = 'Rabbinic Literature / Tosefta' "
    "OR r.domain = 'Rabbinic Literature / Talmud Yerushalmi')")


def section_is_formulary(locus):
    if not locus:
        return False
    if NEG_HEAD.search(locus) and not NEG_OVERRIDE.search(locus):
        return False
    return bool(KW.search(locus))


def page_is_documentary_only(catalogue_title):
    t = catalogue_title or ""
    return bool(DOC_TAG.search(t)) and not LITERARY_TAG.search(t)


def classify(domain, locus, catalogue_title, work_is_canonical):
    """First matching kind wins; None = no flag."""
    if (domain or "").startswith(LITURGY_DOMAIN_PREFIX):
        return "standalone_unit"
    if section_is_formulary(locus):
        return "embedded_section"
    if not work_is_canonical and page_is_documentary_only(catalogue_title):
        return "documentary_page"
    return None


DDL = """CREATE TABLE formula_fact(
  evidence_id TEXT PRIMARY KEY REFERENCES review_row(evidence_id),
  kind        TEXT NOT NULL CHECK (kind IN
              ('embedded_section','standalone_unit','documentary_page'))
)"""


def attach(db_path, say=print):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = []
        counts = {}
        cur = con.execute(
            "SELECT evidence_id, domain, locus_label, catalogue_title, "
            "CASE WHEN %s THEN 1 ELSE 0 END AS canon "
            "FROM review_row r" % CANONICAL_DOMAINS_SQL)
        for r in cur:
            k = classify(r["domain"], r["locus_label"],
                         r["catalogue_title"], bool(r["canon"]))
            if k:
                rows.append((r["evidence_id"], k))
                counts[k] = counts.get(k, 0) + 1
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS formula_fact")
        con.execute(DDL)
        con.executemany("INSERT INTO formula_fact VALUES (?,?)", rows)
        for k, v in (("formula_fact.version", "1"),
                     ("formula_fact.counts", json.dumps(counts)),
                     ("formula_fact.at", time.strftime("%Y-%m-%d %H:%M:%S")),
                     ("doc.formula_kind",
                      "A liturgy/formulary label, never a relation verdict. "
                      "embedded_section: the matched section inside a "
                      "non-liturgy work is a fixed prayer or notarial formula "
                      "(named by the work's own section header) -- carrier "
                      "text every siddur or deed shares, so the row sits with "
                      "'only shared quotations'. standalone_unit: the claimed "
                      "work IS a standalone liturgy unit; a generic prayer "
                      "excerpt cannot identify a page, so the row stays out "
                      "of the main pool and lands in 'unclear'. "
                      "documentary_page: the page is catalogued as a legal "
                      "document only, and the match to a literary work may "
                      "be the document's own formula quoted by the work -- "
                      "shown as CONTEXT ONLY, because the catalogue is a "
                      "yardstick and never judges an identification: this "
                      "kind moves nothing between pools. Nothing is hidden: "
                      "every flagged row stays visible with this label.")):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        con.execute("DROP TABLE IF EXISTS facet_row")   # triage must rebuild
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    say("formula_fact: %s" % counts)
    return counts


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
