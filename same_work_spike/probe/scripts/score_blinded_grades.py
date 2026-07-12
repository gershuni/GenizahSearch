# -*- coding: utf-8 -*-
"""Score Hillel's blind grading of the 55-card stratified deck against the
hidden key (P + margin band per card). Card N in his file = B-{N:03d}
(display order). Verdict classes distilled from his free-text grades.

Usage: python -X utf8 -u score_blinded_grades.py
Out:   results/blinded_deck_scoring.md
"""
import json
from collections import defaultdict

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
KEY = PROBE + r"\review\full_deck\mapv2_blinded_key.json"
OUT = PROBE + r"\results\blinded_deck_scoring.md"

# Hillel's verdicts (BlindedDeckGrading.txt), distilled:
#  W = witness / same work (עד נוסח, NLI-same-work, titles agree)
#  P = genuine literary parallel / needs study (valuable, not a witness)
#  L = citation/verse/formula leak
#  N = no relation at all
#  U = unclear
VERDICT = {
    1: 'L', 2: 'L', 3: 'P', 4: 'W', 5: 'P', 6: 'L', 7: 'L', 8: 'P',
    9: 'L', 10: 'L', 11: 'L', 12: 'L', 13: 'L', 14: 'P', 15: 'W',
    16: 'W', 17: 'P', 18: 'P', 19: 'P', 20: 'P', 21: 'L', 22: 'P',
    23: 'P', 24: 'L', 25: 'L', 26: 'P', 27: 'L', 28: 'U', 29: 'W',
    30: 'W', 31: 'W', 32: 'L', 33: 'L', 34: 'L', 35: 'W', 36: 'L',
    37: 'L', 38: 'L', 39: 'W', 40: 'L', 41: 'L', 42: 'N', 43: 'N',
    44: 'L', 45: 'L', 46: 'L', 47: 'L', 48: 'L', 49: 'P', 50: 'W',
    51: 'L', 52: 'L', 53: 'L', 54: 'L', 55: 'L',
}
NOTE = {
    11: 'citation formula ואמרו במדרש (missing from CITE_MARKERS)',
    23: 'REVERSED direction: Maagarim edition quotes the Genizah source; '
        'JA citation term לקו׳ (=לקולה) — JA marker family absent',
    17: 'Karaite↔Saadia shared verse-pair — adversaries sharing sequence',
    23 + 1000: '',
}


def main():
    key = json.load(open(KEY, encoding='utf-8'))
    rows = []
    for n, v in VERDICT.items():
        k = key.get(f'B-{n:03d}')
        if k:
            rows.append((n, v, k['p'], k['band'], k['alen'], k['dens'],
                         k['work'], k['sys']))
    # per P quintile
    buckets = defaultdict(lambda: defaultdict(int))
    for n, v, p, band, alen, dens, w, s in rows:
        q = min(4, int(p * 5))
        buckets[q][v] += 1
        buckets[q]['n'] += 1
    bands = defaultdict(lambda: defaultdict(int))
    for n, v, p, band, alen, dens, w, s in rows:
        bands[band][v] += 1
        bands[band]['n'] += 1

    L = ["# Blind-grading scoring — Hillel's 55 verdicts vs the hidden key\n"]
    L.append("Classes: W = witness/same work · P = genuine parallel (valuable"
             ", not a witness) · L = citation/verse/formula leak · N = no "
             "relation · U = unclear\n")
    L.append("## By hidden P quintile\n")
    L.append("| P range | n | W | P(parallel) | L | N | U | W-rate | W+P rate |")
    L.append("|---|--|--|--|--|--|--|--|--|")
    for q in sorted(buckets, reverse=True):
        b = buckets[q]
        n = b['n']
        wr = b['W'] / n
        wpr = (b['W'] + b['P']) / n
        L.append(f"| {q/5:.1f}–{(q+1)/5:.1f} | {n} | {b['W']} | {b['P']} | "
                 f"{b['L']} | {b['N']} | {b['U']} | {wr:.0%} | {wpr:.0%} |")
    L.append("\n## By hidden margin band\n")
    L.append("| band | n | W | P | L | N | U | W-rate |")
    L.append("|---|--|--|--|--|--|--|--|")
    for band in sorted(bands):
        b = bands[band]
        L.append(f"| {band} | {b['n']} | {b['W']} | {b['P']} | {b['L']} | "
                 f"{b['N']} | {b['U']} | {b['W']/b['n']:.0%} |")
    L.append("\n## Card-level detail (sorted by hidden P desc)\n")
    L.append("| # | verdict | P | band | alen | dens | work |")
    L.append("|---|---|---|---|---|---|---|")
    for n, v, p, band, alen, dens, w, s in sorted(rows, key=lambda r: -r[2]):
        L.append(f"| {n} | {v} | {p:.2f} | {band} | {alen} | {dens:.2f} "
                 f"| {w} |")
    L.append("\n## Method notes Hillel handed us\n")
    L.append("- #11: the page introduces the quote with **ואמרו במדרש** — "
             "this exact formula is missing from the deck's CITE_MARKERS.")
    L.append("- #23: **reversed quotation direction** — the Maagarim edition "
             "quotes the source the Genizah page carries (citation term "
             "**לקו׳** = לקולה): a Judeo-Arabic citation-marker family is "
             "entirely absent from the lexicon. Reversed cases are potential "
             "FINDS (the page may witness the older quoted source).")
    L.append("- #17/#18: Karaite↔Rabbanite shared verse-sequences — "
             "adversarial literatures sharing quotation chains; a distinct "
             "cross-community parallel class worth its own product.")
    open(OUT, 'w', encoding='utf-8').write("\n".join(L) + "\n")
    print(f"wrote {OUT} ({len(rows)} cards scored)")


if __name__ == '__main__':
    main()
