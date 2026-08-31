# -*- coding: utf-8 -*-
"""Move R-source rows out of the placeholder domain into the real tree.

The adapter stamped every R-source row `domain='R-source'`, which made the
corpus a domain of its own instead of spreading its works across the tree the
way every other corpus does. The source files carry their own taxonomy: the
first ` -- `-separated segment of each `###` header is the source tree's
category for that work (`מדרשי אגדה`, `ראשונים ופוסקים על הבבלי`, ...) --
constant per file for the grouped categories, and equal to the work's own name
for standalone works.

MAP is a hand-written, closed mapping from every category that occurs to an
EXISTING domain string of this artifact (parent / leaf, the exact strings the
base corpora already use -- verified against the db before writing). A
category not in MAP fails the run: a future producer addition must be mapped
deliberately, never defaulted.

The Hebrew category names never enter the db -- only the English domain
strings the artifact already displays.

Run (review server STOPPED -- this writes into the artifact):
    python -X utf8 scripts/attach_rsource_domains.py \
        --db discovery_data/discovery-v5-REVIEW.db \
        --sourcekeys %USERPROFILE%\\.genizah-private\\sourcekeys.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HEADER_RE = re.compile(r"^###(.*)$", re.M)

# Existing domain strings, shortened here only for the table's readability.
HAL_GAON = "Halakhic / Halakhic- Gaonim"
HAL_RISH = "Halakhic / Halakhic- Rishonim and Aharonim"
HAL_RIF = "Halakhic / Halakhot ha-Rif and its Commentaries"
HAL_MT = "Halakhic / Mishneh Torah and its Commentaries"
TC = "Halakhic Literature and Talmudic Commentaries"
TC_BAVLI = TC + " / Talmud Bavli Commentaries"
TC_MISHNAH = TC + " / Mishnaic Commentaries"
TC_INTRO = TC + " / Talmud – Introductions and Rules"
TC_MITZVOT = TC + " / Sifrei Mitzvot (Rabbinical)"
RESP_GAON = "Responsa and Halakhic Decisions / Responsa- Gaonim"
RESP_RISH = "Responsa and Halakhic Decisions / Responsa- Rishonim and Aharonim"
HIST = ("Historiography and geographical descriptions / "
        "Historiography and geographical descriptions")

MAP = {
    # grouped categories (many works each)
    "ספרי הלכה ומנהג - ראשונים": HAL_RISH,
    "ראשונים ופוסקים על הבבלי": TC_BAVLI,
    "ספרי שאלות ותשובות - ראשונים": RESP_RISH,
    "ספרי מחשבה ומוסר - ראשונים":
        "Philosophy, Theology, Ethical literature / Ethical Literature",
    'מפרשי תנ"ך': "Biblical Exegesis / Biblical Exegesis- Rabbanite",
    "מדרשי אגדה": "Midrash / Aggadic Midrashim",
    "רי\"ף ונושאי כליו": HAL_RIF,
    "מפרשי המשנה": TC_MISHNAH,
    "מדרשי הלכה": "Midrash / Halakhic Midrashim",
    "מפרשים על הרמב\"ם": HAL_MT,
    "מפרשים על הרמב\"ם על הדף": HAL_MT,
    "פירושים על מסכת אבות": TC_MISHNAH,
    # Rashi on TANAKH plus its supercommentaries -- the loci run by book /
    # chapter / verse (בראשית פרק א פסוק א), NOT by tractate. First mapped to
    # Bavli commentaries by the category's sound; the loci corrected it.
    "רש\"י ומפרשיו": "Biblical Exegesis / Biblical Exegesis- Rabbanite",
    "מפרשי ספרא וספרי": "Midrash / Halakhic Midrashim",
    "רא\"ש ונושאי כליו": HAL_RISH,
    "ספר המצוות לרמב\"ם ומפרשיו": TC_MITZVOT,
    # standalone works (the header's first segment is the work itself)
    "תוספתא (ליברמן; צוקרמנדל)": "Rabbinic Literature / Tosefta",
    "מסכתות קטנות": "Rabbinic Literature / Minor Tractates",
    "משנה תורה לרמב\"ם (עם ראב\"ד)": HAL_MT,
    "טור": HAL_RISH,
    "מפרשי הטור": HAL_RISH,
    "אגרת רב שרירא גאון": HIST,
    "ספר הכריתות": TC_INTRO,
    "מבוא התלמוד לר' יוסף בן עקנין": TC_INTRO,
    "סדר עולם זוטא": HIST,
    "סדר תנאים ואמוראים": TC_INTRO,
    "הליכות עולם": TC_INTRO,
    "ספר יוחסין": HIST,
    "מכלול": "Philology / Grammar",
    "זוהר": "Kabbalah / Other",
    "משפטי שבועות": HAL_GAON,
    "סדר רב עמרם גאון": HAL_GAON,
    "שאילתות דרב אחאי ומפרשיו": HAL_GAON,
    "השטרות (לרה\"ג)": HAL_GAON,
    "תשובות הגאונים - גאוני מזרח ומערב": RESP_GAON,
    "תשובות הגאונים - גאונים קדמונים": RESP_GAON,
    "תשובות הגאונים החדשות - עמנואל (אופק)": RESP_GAON,
    "תשובות הגאונים - הרכבי": RESP_GAON,
    "תשובות הגאונים - מוסאפיה (ליק)": RESP_GAON,
    "תשובות הגאונים - שערי צדק": RESP_GAON,
    "תשובות רב נטרונאי גאון - ברודי (אופק)": RESP_GAON,
    "הלכות גדולות": HAL_GAON,
    "הלכות קצובות": HAL_GAON,
    "החילוקים": HAL_GAON,
    "המקח והממכר": HAL_GAON,
    "משפטי הלואות": HAL_GAON,
    "משפטי התנאים": HAL_GAON,
    "יראים": HAL_RISH,
    "סמ\"ג": TC_MITZVOT,
    "סמ\"ק": TC_MITZVOT,
    "מגדל דוד ספר מצוה": TC_MITZVOT,
    "כללי המצוות": TC_MITZVOT,
}


def category_of(nfc_text):
    """The source tree's category: first ` -- ` segment of the first header."""
    m = HEADER_RE.search(nfc_text)
    if not m:
        return None
    return m.group(1).split(" -- ")[0].strip("# ").strip()


def compute(db_path, keys, say=print):
    """-> {ref_id: domain}; fails loud on any unmapped category."""
    con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    rids = [r[0] for r in con.execute(
        "SELECT DISTINCT sf.ref_id FROM review_row r "
        "JOIN reference_witness rw ON rw.witness_id = r.witness_id "
        "JOIN source_file sf ON sf.id = rw.source_file_id "
        "WHERE r.source_corpus='rsource'")]
    existing = {r[0] for r in con.execute(
        "SELECT DISTINCT domain FROM review_row "
        "WHERE source_corpus != 'rsource' AND domain IS NOT NULL")}
    con.close()

    unmapped, not_in_db, out = [], set(), {}
    for rid in rids:
        if rid not in keys:
            raise SystemExit("key file has no path for %s" % rid)
        with open(keys[rid], encoding="utf-8", errors="strict") as f:
            cat = category_of(unicodedata.normalize("NFC", f.read()))
        dom = MAP.get(cat)
        if dom is None:
            unmapped.append((rid, cat))
            continue
        if dom not in existing:
            not_in_db.add(dom)
        out[rid] = dom
    if unmapped:
        raise SystemExit("UNMAPPED categories -- map them deliberately:\n" +
                         "\n".join("  %s: %r" % u for u in unmapped[:20]))
    if not_in_db:
        raise SystemExit("mapping targets not present in this artifact's own "
                         "domain vocabulary: %s" % sorted(not_in_db))
    say("mapped    : %d works -> %d distinct domains"
        % (len(out), len(set(out.values()))))
    return out


def attach(db_path, sourcekeys_path, say=print):
    keys = json.load(open(sourcekeys_path, encoding="utf-8"))
    dom_by_rid = compute(db_path, keys, say)
    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        for rid, dom in sorted(dom_by_rid.items()):
            con.execute(
                "UPDATE review_row SET domain=? WHERE evidence_id IN ("
                "  SELECT r.evidence_id FROM review_row r"
                "  JOIN reference_witness rw ON rw.witness_id = r.witness_id"
                "  JOIN source_file sf ON sf.id = rw.source_file_id"
                "  WHERE sf.ref_id=? AND r.source_corpus='rsource')",
                (dom, rid))
        left = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE domain='R-source'"
        ).fetchone()[0]
        if left:
            raise RuntimeError("%d rows still carry the placeholder domain"
                               % left)
        counts = con.execute(
            "SELECT domain, COUNT(*) FROM review_row "
            "WHERE source_corpus='rsource' GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        for k, v in (("rsource_domains.version", "1"),
                     ("rsource_domains.method",
                      "source-tree category (first header segment) -> "
                      "existing domain, closed hand map"),
                     ("rsource_domains.counts",
                      json.dumps(dict(counts), ensure_ascii=False)),
                     ("rsource_domains.built_at",
                      time.strftime("%Y-%m-%d %H:%M:%S"))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        # domain is a facet column; the projection must rebuild.
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    for dom, n in counts:
        say("  %6d  %s" % (n, dom))
    return dict(counts)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--sourcekeys", default=os.path.join(
        os.path.expanduser("~"), ".genizah-private", "sourcekeys.json"))
    args = ap.parse_args(argv)
    for p in (args.db, args.sourcekeys):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)
    attach(args.db, args.sourcekeys)
    return 0


if __name__ == "__main__":
    sys.exit(main())
