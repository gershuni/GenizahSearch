# -*- coding: utf-8 -*-
"""Stage-0 corpus hygiene (METHOD.md §7) — importable module.

Filters/dedup tiers:
(a) same-FL-image dedup   — fl_of(page_id); collapse identical photographs
    (catches the 997…-prefix NLI catalog duplicates: same IE/P/FL).
(b) same-shelfmark dedup  — load_shelf_variants(); different sys_ids resolving
    to one physical shelfmark (flag pairs, post-verify).
(c) line-break-agreement  — line_agreement(); re-photographed pages: line
    breaks are physical-page properties, genuine witnesses never agree on
    them (precision 100%, recall 74% vs human grades). Post-verify.
(d) microfilm target-sheet / catalog-card filter — page-level, pre-index.
(e) empty/short pages     — pre-index.
"""
import csv
import re

from rapidfuzz.distance import Levenshtein

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"

HEB_RE = re.compile(r'[א-ת]')

# FGP microfilm card template words (probe finding F7; graded junk class)
TARGET_SHEET_WORDS = ('סימן', 'תוכן', 'מחבר', 'שנה', 'הערות')

# NLI ownership stamp / ruler-card pages photographed with RNL (Firkovich)
# manuscripts — discovered 2026-07-08 by the passage-units view (unit of
# 2,618 "MSS" sharing the stamp text). HTR mangles the stamp variously:
# 'בית הספרים הלאומי והאוני ברסיטא' etc.
STAMP_RE = re.compile(r'בית הספרים|אוניברסיט|האוני\s?ברסיט|הספרים הלאומ')

MIN_STREAM_LETTERS = 80


def fl_of(page_id: str) -> str:
    """FL image id from '{sys}_{IE…}_{P######}_{FL…}' (last FL part)."""
    for part in reversed(page_id.split('_')):
        if part.startswith('FL'):
            return part
    return page_id  # no FL part -> unique key, never collapses


def page_filter(text: str) -> str | None:
    """Return None if the page passes, else a drop-reason string.

    Uses a cheap Hebrew-letter count (equivalent to stream length for the
    length gate) so the 948K-record streaming pass stays fast.
    """
    n_heb = len(HEB_RE.findall(text))
    if n_heb < MIN_STREAM_LETTERS:
        return 'short'
    hits = sum(1 for w in TARGET_SHEET_WORDS if w in text)
    if hits >= 4 or (hits >= 3 and n_heb < 400):
        return 'target_sheet'
    if n_heb < 400 and STAMP_RE.search(text):
        return 'library_stamp'
    return None


def _norm_shelf(s):
    t = re.sub(r'(\d)\.(\d)', r'\1DOT\2', s.replace('/', '.'))
    t = re.sub(r'\W+', '', t).casefold().replace('dot', '.')
    return t[2:] if t.startswith('ms') else t


def load_shelf_variants():
    """sys_id -> set of normalized call-number variants (libraries.csv)."""
    out = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                out[row[0]] = {_norm_shelf(v) for v in variants if v}
    return out


def same_shelf(sys_a, sys_b, shelf_variants) -> bool:
    va = shelf_variants.get(sys_a)
    vb = shelf_variants.get(sys_b)
    return bool(va and vb and (va & vb))


def line_agreement(text_a: str, text_b: str):
    """Same-page-photographed-twice detector over full page texts.

    Fraction of HTR LINES (>=10 letters; >=4 such lines both sides) matching
    near-identically IN ORDER. >=0.60 -> duplicate photography.
    """
    la = [norm_stream(x)[0] for x in text_a.split('\n')]
    lb = [norm_stream(x)[0] for x in text_b.split('\n')]
    la = [x for x in la if len(x) >= 10]
    lb = [x for x in lb if len(x) >= 10]
    if min(len(la), len(lb)) < 4:
        return 0.0
    j = matched = 0
    for a in la:
        for jj in range(j, min(j + 3, len(lb))):
            if Levenshtein.normalized_distance(a, lb[jj]) <= 0.30:
                matched += 1
                j = jj + 1
                break
    return round(matched / max(len(la), len(lb)), 3)
