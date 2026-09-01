# -*- coding: utf-8 -*-
"""Author authority: one string per person across the whole review db.

Owner ruling 2026-09-01: the same person must carry the SAME author string
everywhere -- inside a linked cluster and between unrelated works -- and the
canonical form is FULL NAME + acronym ("שלמה בן יצחק (רש״י)", not "רש״י").

Measured before this fix: 153 distinct author strings, 11 same-person variant
groups (corpus house styles: R-source used bare acronyms, the base corpus full
names; plus quote-char and spelling drift like מימון/מיימון, קיירא/קיארא), and
20 alias links whose two sides disagreed.

Every canonical string below is a form that ALREADY EXISTS in the corpus
(quote-normalized to gershayim) -- nothing is invented. Rows whose
author_provenance is 'owner_ruling' get the string mapped (same person, new
style) but KEEP their provenance. FILLS extend a cluster's author onto linked
works that had none; the איסור והיתר fill extends the existing owner ruling to
the same_work twin.

Deliberately NOT touched (flagged for owner rulings instead):
  - collection containers vs single-author parts (תשובות הגאונים vs האיי גאון)
    -- legitimately different;
  - the empty-author Tosafot bases linked to תוספות הרא״ש/רי״ד -- filling רא״ש
    onto generic תוספות would assert an attribution the link alone can't carry;
  - the known w000022-class misattribution (חובות הלבבות is בחיי אבן פקודה, not
    בחיי בן אשר) and translator-as-author rows -- attribution corrections, not
    dedup.

Run (review server STOPPED):
    python -X utf8 scripts/attach_author_authority.py
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# variant string (exact match) -> canonical string
VARIANTS = {
    # רש"י
    "רש״י": "שלמה בן יצחק (רש״י)",
    # רמב"ם
    "רמב״ם": "משה בן מימון (רמב״ם)",
    "משה בן מיימון (רמב״ם)": "משה בן מימון (רמב״ם)",
    'משה בן מימון (רמב"ם)': "משה בן מימון (רמב״ם)",
    "משה בן מימון; תרגום משה אבן תיבון":
        "משה בן מימון (רמב״ם); תרגום משה אבן תיבון",
    # רמב"ן
    "רמב״ן": "משה בן נחמן (רמב״ן)",
    'משה בן נחמן (רמב"ן)': "משה בן נחמן (רמב״ן)",
    # ראב"ד
    "ראב״ד": "אברהם בן דוד מפושקיירא (ראב״ד)",
    'אברהם בן דוד מפושקיירא (ראב"ד)': "אברהם בן דוד מפושקיירא (ראב״ד)",
    # רד"ק
    "רד״ק": "דוד קמחי (רד״ק)",
    'דוד קמחי (רד"ק)': "דוד קמחי (רד״ק)",
    # רי"ף
    "רי״ף": "יצחק אלפסי (רי״ף)",
    "יצחק אלפסי": "יצחק אלפסי (רי״ף)",
    'יצחק אלפסי (רי"ף)': "יצחק אלפסי (רי״ף)",
    # רשב"ם
    "רשב״ם": "שמואל בן מאיר (רשב״ם)",
    'שמואל בן מאיר (רשב"ם)': "שמואל בן מאיר (רשב״ם)",
    # ר"ן
    "ר״ן": "ניסים בן ראובן גירונדי (הר״ן)",
    'ניסים בן ראובן גירונדי (הר"ן)': "ניסים בן ראובן גירונדי (הר״ן)",
    # רס"ג
    "סעדיה גאון": "סעדיה גאון (רס״ג)",
    'סעדיה גאון (רס"ג)': "סעדיה גאון (רס״ג)",
    "סעדיה גאון; תרגום יהודה אבן תיבון":
        "סעדיה גאון (רס״ג); תרגום יהודה אבן תיבון",
    # spelling drift
    "שמעון קיארא": "שמעון קיירא",
    # one-string-per-person merges where a fuller corpus form exists
    "רבינו יונה": "יונה בן אברהם (יונה גירונדי)",
    "אבן עזרא": "אברהם אבן עזרא",
    # owner 2026-09-01 restyled this to name + acronym; the variant must point
    # at the CURRENT canonical form or a re-run would revert the ruling
    'אברהם בן הרמב"ם': "אברהם בן הרמב״ם (ראב״ם)",
    "רבי אברהם בן הרמב״ם": "אברהם בן הרמב״ם (ראב״ם)",
    "רבינו גרשום": "רבנו גרשום בן יהודה מאור הגולה",
    # his Torah commentary; NOT the חובות הלבבות Bahya (בחיי אבן פקודה)
    "רבינו בחיי": "בחיי בן אשר",
    # owner 2026-09-01: "תוספות הרי\"ד is ישעיה דיטראני (רי\"ד)" -- person-level,
    # so all three רי"ד-authored works (תוספות/פסקי/שו"ת הרי"ד) restyle together.
    # רא"ש stays "רא״ש" by the same message; plain תוספות works stay empty.
    "רי״ד": "ישעיה דיטראני (רי״ד)",
}

# work_id -> (author, provenance): fill empty authors inside approved clusters
FILLS = {
    # פרקי רבי אליעזר twins of the R-source פרקי דרבי אליעזר (same_work)
    "w000807": ("מיוחס לר' אליעזר בן הורקנוס (נתחבר במאות ה-8-9)", "alias_harmonized"),
    "w001447": ("מיוחס לר' אליעזר בן הורקנוס (נתחבר במאות ה-8-9)", "alias_harmonized"),
    # הלכות גדולות introduction parts (container author, single-author work)
    "w000734": ("שמעון קיירא", "alias_harmonized"),
    "w000735": ("שמעון קיירא", "alias_harmonized"),
    # איסור והיתר לרש"י base twin: owner ruled the author is רש"י for this work
    # (2026-08-31, "I would stick with רש\"י"); the same_work link carries the
    # ruling to both sides, in the canonical full-name form.
    "w001486": ("שלמה בן יצחק (רש״י)", "owner_ruling"),
}

# work_id -> author or None. Owner rulings 2026-09-01, applied UNCONDITIONALLY
# (they overwrite whatever the row carries):
#   - "הרכבי is no author" -- Harkavy is the editor of a geonic collection, and
#     "גאונים" is not an author string; both Harkavy-edition works lose it.
#   - "חובות הלבבות is בחיי אבן פקודה and not בן אשר nor אבן תיבון" -- the ja
#     original carried the known w000022 misattribution, the translations
#     credited only the translator.
OWNER_AUTHOR_RULINGS = {
    "rs01fd4809e0ff": None,               # תשובות הגאונים - הרכבי
    "w001517": None,                      # תשובות הגאונים (מהדורת הרכבי)
    "w000022": "בחיי אבן פקודה",          # תורת חובות הלבבות (ja original)
    "rs80d7433cbb4c": "בחיי אבן פקודה",   # חובות הלבבות
    "w000195": "בחיי אבן פקודה",          # חובות הלבבות (תרגום אבן תיבון)
    "w001476": "בחיי אבן פקודה",          # חובות הלבבות (תרגום אבן תיבון)
    # הלכות שמחות (rs224666342134): simanim to קמח quoting ראב"ד and רי"ץ גיאת
    # by name -- centuries after שמעון קיירא, whose author string reached it only
    # through a WRONG containment link to הלכות גדולות (owner: two different
    # works). Owner confirmed 2026-09-01: מהר"ם מרוטנבורג.
    "rs224666342134": "מהר״ם מרוטנבורג",
    # -- link-scan batch, owner rulings 2026-09-01 --
    # פתרון תורה: anonymous per scholarship (Urbach); the Hai Gaon attribution
    # was corpus data on the base copy, harmonized onto the rs twin.
    "rs73d0dfb47ca3": None,
    "w000993": None,
    # שערי צדק: a multi-gaon collection; 'שרירא גאון' was an alias-harmonization
    # smear through its link to תשובות שרירא גאון.
    "rs292b4363df45": None,
    # אלמסאיל: the title names יהושע הנגיד; the 'Moses b. Maimon, Rambam'
    # attribution (the db's only English author string) was wrong.
    "w001151": "יהושע הנגיד",
    # translator-as-author class, the חובות הלבבות pattern: author = composer.
    "w000194": "יהודה הלוי",           # ספר הכוזרי, תרגום
    "w001488": "יהודה הלוי",           # ספר הכוזרי, תרגום (sefaria copy)
    "w000196": "יונה אבן ג'נאח",       # ספר הרקמה, תרגום
    "w000827": "ישועה בן יהודה",       # ספר הישר, תרגום
    "w000830": "יוסף הרואה בן אברהם",  # ספר נעימות, תרגום
}

# work_id -> title. Owner ruling 2026-09-01 (the נחשון גאון class): the minted
# titles took "יצחק" from the AUTHOR segment's patronymic ("נחשון גאון בר'
# (יצחק) צדוק") -- the father's alternate name, not the author. A full sweep of
# all 647 msource works against their source-record segments found exactly
# these six; no other work has the bug.
OWNER_TITLE_RULINGS = {
    "w000445": "נחשון גאון על בבא בתרא וסנהדרין",
    "w000554": "תשובות נחשון גאון",
    "w000556": "תשובות נחשון גאון",
    "w000557": "תשובות נחשון גאון",
    "w000559": "תשובות נחשון גאון",
    "w000561": "תשובות נחשון גאון",
}

DOC = ("Author authority (owner ruling 2026-09-01): one canonical string per "
       "person db-wide, full name + acronym style. Variant strings (bare "
       "acronyms, quote-char and spelling drift) were mapped onto existing "
       "corpus forms; owner-ruled rows keep provenance 'owner_ruling' with the "
       "string restyled. Collection containers may still differ from their "
       "single-author parts by design.")


def apply(db_path, say=print):
    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        total = 0
        for var, canon in VARIANTS.items():
            cur = con.execute(
                "UPDATE review_row SET work_author = ?, author_provenance = "
                "CASE WHEN author_provenance = 'owner_ruling' THEN 'owner_ruling' "
                "ELSE 'author_authority' END "
                "WHERE work_author = ?", (canon, var))
            if cur.rowcount:
                say("  %r -> %r (%d rows)" % (var, canon, cur.rowcount))
            total += cur.rowcount
        n_fill = 0
        for wid, (author, prov) in FILLS.items():
            cur = con.execute(
                "UPDATE review_row SET work_author = ?, author_provenance = ? "
                "WHERE work_id = ? AND (work_author IS NULL OR work_author = '' "
                "OR ? = 'owner_ruling')", (author, prov, wid, prov))
            say("  fill %s -> %r (%d rows)" % (wid, author, cur.rowcount))
            n_fill += cur.rowcount
        n_title = 0
        for wid, new_title in OWNER_TITLE_RULINGS.items():
            cur = con.execute(
                "UPDATE review_row SET work_title = ?, title_provenance = "
                "'owner_ruling' WHERE work_id = ?", (new_title, wid))
            say("  title %s -> %r (%d rows)" % (wid, new_title, cur.rowcount))
            n_title += cur.rowcount
        n_ruled = 0
        for wid, author in OWNER_AUTHOR_RULINGS.items():
            prov = "owner_ruling" if author is not None else None
            cur = con.execute(
                "UPDATE review_row SET work_author = ?, author_provenance = ? "
                "WHERE work_id = ?", (author, prov, wid))
            say("  ruling %s -> %r (%d rows)" % (wid, author, cur.rowcount))
            n_ruled += cur.rowcount
        if total == 0 and n_fill == 0 and n_ruled == 0:
            raise SystemExit("nothing matched -- wrong db, or already applied?")
        con.execute("INSERT OR REPLACE INTO meta VALUES "
                    "('author_authority.at', datetime('now'))")
        con.execute("INSERT OR REPLACE INTO meta VALUES "
                    "('doc.author_authority', ?)", (DOC,))
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
    say("merged %d rows, filled %d rows, ruled %d rows" % (total, n_fill, n_ruled))


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
