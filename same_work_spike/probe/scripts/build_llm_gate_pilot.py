# -*- coding: utf-8 -*-
"""LLM title-understanding gate — PILOT candidate builder.

The mechanical identified-gate/TitleGate cannot connect a matched work to the
manuscript's catalog identification when they differ by LANGUAGE (כתאב אללמע =
ריב"ג ספר הרקמה), ABBREVIATION/ALT-TITLE (כתאב אלמרשד = חובות הלבבות), or a
compound/section name the token-bag drops (משנה תורה ספר שופטים; שופטים is a
weak Bible-book token). This assembles the alias-suspect slice for an LLM to
classify:
  same_work  catalog names the SAME work (alias/translation/abbrev/section) -> known
  container  catalog names a CONTAINER (siddur/anthology/piyyut booklet) that
             legitimately HOLDS this specific unit -> keep as discovery/witness
  different  catalog names a genuinely different work -> other
  uncertain  -> keep + flag

Scope: discovery-pile rows (bucket2=='discovery') whose NLI title is SPECIFIC
(non-generic); one record per distinct (sys_id, work_id), ranked by best score.

Out: data/llm_gate_pilot_candidates.json
Usage: python -X utf8 -u build_llm_gate_pilot.py [--top N]
"""
import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from title_gate import TitleGate, is_generic_title

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
LIB = r"C:\Genizahsearch\libraries.csv"
FLANK = PROBE + r"\data\discovery_scored_flank.jsonl"
OUT = PROBE + r"\data\llm_gate_pilot_candidates.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=200)
    a = ap.parse_args()

    nli = {}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for row in rd:
            if len(row) >= 8 and row[0]:
                nli[row[0]] = (row[7] or '').strip()
    tg = TitleGate(nli)

    rows = [json.loads(l) for l in open(FLANK, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']

    # group by distinct (sys_id, work_id); keep best score + count
    best = {}
    for r in disc:
        sid = str(r['sys_id'])
        t = nli.get(sid, '')
        if not t or is_generic_title(t):
            continue                       # only rows with a SPECIFIC catalog id
        key = (sid, r['work_id'])
        sc = r.get('disc_score2_flank', r.get('disc_score2', 0)) or 0
        cur = best.get(key)
        if cur is None or sc > cur['best_score']:
            best[key] = {
                'uid': f"{sid}|{r['work_id']}",
                'sys_id': sid,
                'work_id': r['work_id'],
                'work_title': r.get('title') or '',
                'author': r.get('author') or '',
                'cat': r.get('cat') or '',
                'genre': r.get('genre') or '',
                'nli_title': t,
                'catalog_titles': tg.titles_of(sid),
                'best_score': round(sc, 4),
                'n_rows': 0,
            }
    for r in disc:
        key = (str(r['sys_id']), r['work_id'])
        if key in best:
            best[key]['n_rows'] += 1

    cand = sorted(best.values(), key=lambda c: -c['best_score'])[:a.top]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(cand, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"distinct (sys,work) with specific catalog title: {len(best)}; "
          f"pilot top-{a.top} written")
    # confirm the named cases are present
    named = ['ספר הרקמה', 'חובות הלבבות', 'משנה תורה, ספר שופטים',
             'העיונים והדיונים']
    for nm in named:
        hits = [c for c in cand if nm in c['work_title']]
        print(f"  '{nm}': {len(hits)} in pilot"
              + (f" (top score {max(h['best_score'] for h in hits):.2f})" if hits else ""))
    print(f"wrote {OUT}")


if __name__ == '__main__':
    main()
