# -*- coding: utf-8 -*-
"""Stage-0 corpus hygiene. Contract: passage-matching-algorithm.md section 9.

Not optional, and not a tidiness pass. The false-positive classes here are
mechanical and they are the ones that look like discoveries: of physically
joined fragment pairs that appeared to share text, 36 of 36 turned out to be
duplicate photography; of short-span pairs, 6 of 8 were microfilm title
sheets. An index built without these filters produces confident nonsense.

Ported from the research tree's `stage0.py`. Two halves, used at different
times:

  page_filter(text)      pre-index, per record. Returns a drop REASON or None.
                         Every drop is counted and recorded -- the set of
                         surviving records is the "eligible record manifest"
                         that any method comparison must share, so it cannot
                         be an untracked side effect of the builder.

  line_agreement(a, b)   post-verify, per candidate pair. Detects the same
                         physical page photographed twice. Quadratic in
                         returned records, so a query path must bound it (see
                         section 9.3); this module only provides the measure.
"""
from __future__ import annotations

import re

from rapidfuzz.distance import Levenshtein

from shared.passage_normalize import norm_stream_fast

# Cheap Hebrew-letter count: equivalent to normalized stream length for the
# length gates, without paying full normalization on a 948K-record pass.
_HEB_RE = re.compile(r'[א-ת]')

# Microfilm catalog-card template words.
TARGET_SHEET_WORDS = ('סימן', 'תוכן', 'מחבר', 'שנה', 'הערות')

# Library ownership-stamp / ruler-card pages photographed alongside
# manuscripts. Found late, and worth the specificity: one apparent "unit" of
# 2,618 manuscripts turned out to share nothing but this stamp. Transcription
# mangles it variously, hence the alternation.
STAMP_RE = re.compile(r'בית הספרים|אוניברסיט|האוני\s?ברסיט|הספרים הלאומ')

MIN_RECORD_LETTERS = 80
SHORT_RECORD_LETTERS = 400

# Duplicate-photography detector.
DUP_LINE_MIN_LETTERS = 10
DUP_MIN_LINES = 4
DUP_LINE_MAX_DENSITY = 0.30
DUP_AGREEMENT_THRESHOLD = 0.60
DUP_LOOKAHEAD = 3

DROP_REASONS = ('short', 'target_sheet', 'library_stamp')


def hebrew_letter_count(text: str) -> int:
    return len(_HEB_RE.findall(text))


def page_filter(text: str):
    """None if the record is eligible, else one of DROP_REASONS."""
    n_heb = hebrew_letter_count(text)
    if n_heb < MIN_RECORD_LETTERS:
        return 'short'
    hits = sum(1 for w in TARGET_SHEET_WORDS if w in text)
    if hits >= 4 or (hits >= 3 and n_heb < SHORT_RECORD_LETTERS):
        return 'target_sheet'
    if n_heb < SHORT_RECORD_LETTERS and STAMP_RE.search(text):
        return 'library_stamp'
    return None


def line_agreement(text_a: str, text_b: str) -> float:
    """Fraction of transcription LINES matching near-identically, in order.

    The tell is that line breaks are a property of the physical page: two
    genuine textual witnesses never agree on them, two photographs of one page
    must. Measured precision 100% (31 of 31), recall 74%.
    """
    la = [norm_stream_fast(x) for x in text_a.split('\n')]
    lb = [norm_stream_fast(x) for x in text_b.split('\n')]
    la = [x for x in la if len(x) >= DUP_LINE_MIN_LETTERS]
    lb = [x for x in lb if len(x) >= DUP_LINE_MIN_LETTERS]
    if min(len(la), len(lb)) < DUP_MIN_LINES:
        return 0.0
    j = matched = 0
    for a in la:
        for jj in range(j, min(j + DUP_LOOKAHEAD, len(lb))):
            if Levenshtein.normalized_distance(a, lb[jj]) <= DUP_LINE_MAX_DENSITY:
                matched += 1
                j = jj + 1
                break
    return round(matched / max(len(la), len(lb)), 3)


def is_duplicate_photography(text_a: str, text_b: str) -> bool:
    return line_agreement(text_a, text_b) >= DUP_AGREEMENT_THRESHOLD
