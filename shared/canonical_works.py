# -*- coding: utf-8 -*-
"""Recognise a manuscript that IS a canonical work, from its catalogue title.

Why by title and not by text. Widening the passage matcher surfaces a lot of
scriptural and talmudic quotation: graded on 44 reference-source cards, 28 of
them were `canonical`. The obvious fix -- filter against canonical TEXT, which
this project already supports for the incumbent via `filter_text` -- was
measured and does not work here:

  exact-substring of the matched span against 15.1M letters of canonical text
      removed 0 of 28 canonical, and 1 of 12 genuine
  any short sub-window of the span present in the canon
      caught 22 of 28 canonical, but also 7 of 12 genuine
  coverage of the span by canonical windows (the discovery guard's shape)
      did not separate at all -- median 0.49 canonical vs 0.53 same_text

Real parallels quote scripture too, so no test on the shared TEXT can tell
"this manuscript is a Bible codex I found via a quoted verse" from "this
manuscript genuinely carries my composition, which quotes a verse".

The catalogue titles separate them immediately. Cards graded `canonical` are
manuscripts that ARE canonical works -- Talmud, Mishnah, Torah, Bible, the
major codes. Cards graded `same_text` are liturgy, piyyut, Haggadah, Arukh,
derashot. So the signal is the identity of the RETURNED MANUSCRIPT, and it is
already in `libraries.csv` (resolved for 42/42 of the graded sys_ids).

Measured on those cards: demotes 18 of 26 canonical while losing 2 of 14
genuine. The two losses are a Prophets manuscript and a Bavli manuscript that
really did carry the searched text -- irreducible for a title rule. Hence the
contract: this DEMOTES, never deletes, and the caller must keep it optional.

What it does NOT do, stated because the owner named it: it does not address
SHARED QUOTATIONS -- two non-canonical manuscripts matching each other only
because both quote the same verse. That is a different problem and this
module makes no claim on it.
"""
from __future__ import annotations

import re

# Titles that name a canonical work. Hebrew catalogue titles, matched as
# substrings because the catalogue appends tractate/section detail
# ("תלמוד בבלי [טקסט]. ; Talmud Bavli: Gittin").
#
# Deliberately NOT included, because the graded cards show they carry genuine
# parallels rather than canonical copies: פיוט, תפילה/תפילות, הגדה, דרשות,
# ערוך, סידור, קטעי גניזה documentary material.
_CANONICAL_TITLE = re.compile(
    'תלמוד בבלי|תלמוד ירושלמי|תוספתא|'
    'משנה תורה|פרוש המשנה|משנה סדר|משנה \\[|^משנה|;משנה|'
    'תורה \\(|^תורה$|^תורה\\.|מקרא|נביאים|כתובים|'
    'הלכות גדולות|הלכות הרי|הלכות רב אלפס|'
    'ספרא|ספרי'
)

# The English half of the same catalogue strings, for rows whose Hebrew side
# is absent (the field concatenates both when it has both).
_CANONICAL_TITLE_EN = re.compile(
    r'Talmud Bavli|Talmud Yerushalmi|Mishnah|Tosefta|Bible|Torah|'
    r'Pentateuch|Prophets|Hagiographa',
    re.IGNORECASE)


def is_canonical_title(title: str) -> bool:
    """True when a catalogue title names a canonical work.

    Empty or missing titles are NOT canonical: 8 of the 26 graded canonical
    cards are untitled or idiosyncratically titled fragments, and guessing on
    an empty string would demote every unidentified manuscript in the corpus
    -- a far worse error than missing those eight.
    """
    if not title:
        return False
    t = str(title).strip()
    if not t:
        return False
    return bool(_CANONICAL_TITLE.search(t) or _CANONICAL_TITLE_EN.search(t))


def is_canonical_sys_id(sys_id: str, meta_mgr) -> bool:
    """As above, resolving the title through the shared MetadataManager.

    `meta_mgr.get_meta_for_id` returns (shelfmark, title) from the CSV bank
    that is already loaded in both apps, so this adds no data dependency and
    no I/O. A lookup failure is not canonical -- fail toward SHOWING the row,
    because this feature hides results and a false positive costs the user a
    find they will never know was there.
    """
    if not sys_id or meta_mgr is None:
        return False
    try:
        _shelf, title = meta_mgr.get_meta_for_id(sys_id)
    except Exception:
        return False
    return is_canonical_title(title)


def partition_rows(rows: list, meta_mgr, *, sys_id_of=None) -> tuple:
    """-> (kept, demoted). Never deletes; the caller decides where to put them.

    `sys_id_of` extracts a row's manuscript id; the default reads the
    `raw_header` / `uid` shape the parallels row contract uses.
    """
    def _default(row):
        h = row.get('raw_header') or row.get('uid') or ''
        return str(h).split('_', 1)[0]

    pick = sys_id_of or _default
    kept, demoted = [], []
    for row in rows:
        (demoted if is_canonical_sys_id(pick(row), meta_mgr)
         else kept).append(row)
    return kept, demoted
