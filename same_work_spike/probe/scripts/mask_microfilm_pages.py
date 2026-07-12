# -*- coding: utf-8 -*-
"""MAPV2-10 — detect NLI microfilm title-card / copyright-stamp pages.

These are start-of-film library cards ("בית הספרים הלאומי והאוניברסיטאי
ירושלים ... כל הזכויות שמורות") photographed at the head of microfilms and
read by the HTR in wildly different garblings. In the wide tier they match
EACH OTHER and pollute the small-fragment pair map (Hillel 2026-07-11:
"should not be mapped at all").

Detection: rapidfuzz partial_ratio of normalized boilerplate needles against
the page's norm_stream. A page is flagged when any needle clears its cutoff.
Writes data/microfilm_title_pages.json:
  {meta: {...}, pages: {page_id: {score, needle, len}}}

Consumers: build_track2_wide_deck.py (pair exclude), track2_wide_run.py
(pool exclude on future runs), mapv2_deck.py (page exclude).

Usage: python -X utf8 -u mask_microfilm_pages.py [--report-only]
"""
import argparse
import json
import os
import sqlite3
import time

from rapidfuzz import fuzz

from normalize import norm_stream

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
CORPUS = os.path.join(PROBE, 'data', 'fullcorpus_v2.db')
OUT = os.path.join(PROBE, 'data', 'microfilm_title_pages.json')

# Normalized needles (letters-only, finals folded — norm_stream space).
# Cutoffs tuned per needle length: long distinctive phrases tolerate more
# garbling; the short rights-stamp needs a tighter match.
NEEDLES = [
    # בית הספרים הלאומי והאוניברסיטאי
    ('bet_hasfarim', norm_stream('בית הספרים הלאומי והאוניברסיטאי')[0], 78),
    # כל הזכויות שמורות (modern copyright stamp — never medieval content)
    ('rights', norm_stream('כל הזכויות שמורות')[0], 84),
    # המכון לתצלומי כתבי היד העבריים
    ('makhon_tatslumim',
     norm_stream('המכון לתצלומי כתבי היד העבריים')[0], 80),
]


def detect(stream):
    """Best (needle_name, score) clearing its cutoff, else None."""
    best = None
    for name, needle, cutoff in NEEDLES:
        sc = fuzz.partial_ratio(needle, stream, score_cutoff=cutoff)
        if sc and (best is None or sc > best[1]):
            best = (name, sc)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--report-only', action='store_true')
    args = ap.parse_args()

    t0 = time.time()
    con = sqlite3.connect(f"file:{CORPUS}?mode=ro", uri=True)
    flagged = {}
    n = 0
    for pid, txt in con.execute("SELECT page_id, text FROM pages"):
        n += 1
        if n % 100000 == 0:
            print(f"  {n:,} pages, {len(flagged):,} flagged "
                  f"({time.time() - t0:.0f}s)", flush=True)
        if not txt:
            continue
        stream = norm_stream(txt)[0]
        if len(stream) < 10:
            continue
        hit = detect(stream)
        if hit:
            flagged[pid] = {'needle': hit[0], 'score': round(hit[1], 1),
                            'len': len(stream)}
    con.close()

    by_needle = {}
    lens = sorted(v['len'] for v in flagged.values())
    for v in flagged.values():
        by_needle[v['needle']] = by_needle.get(v['needle'], 0) + 1
    q = (lambda f: lens[int(f * (len(lens) - 1))]) if lens else (lambda f: 0)
    print(f"\nscanned {n:,} pages in {time.time() - t0:.0f}s")
    print(f"flagged {len(flagged):,} title-card/stamp pages; "
          f"by needle: {by_needle}")
    if lens:
        print(f"stream-length of flagged pages: min {lens[0]}, "
              f"p25 {q(.25)}, median {q(.5)}, p75 {q(.75)}, "
              f"p95 {q(.95)}, max {lens[-1]}")

    if args.report_only:
        return
    meta = {'built': time.strftime('%Y-%m-%d %H:%M'),
            'corpus': CORPUS, 'n_scanned': n, 'n_flagged': len(flagged),
            'needles': [(nm, nd, ct) for nm, nd, ct in NEEDLES],
            'by_needle': by_needle}
    json.dump({'meta': meta, 'pages': flagged},
              open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"wrote {OUT}")


if __name__ == '__main__':
    main()
