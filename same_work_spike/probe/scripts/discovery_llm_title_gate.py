# -*- coding: utf-8 -*-
"""MAPV2-15o — apply the LLM title-understanding gate to the scored pile.

Runs AFTER discovery_identified_gate.py, BEFORE discovery_flank.py. For each
surviving discovery row whose manuscript has a SPECIFIC catalogue title, looks
up the cached LLM verdict (title_gate_llm.get_verdict: alias-seed -> cache).
Refined 5-way taxonomy (validated 99% vs Hillel's 103 grades) — the test is
"could a cataloguer PREDICT this content from the title?":

  known + high    -> bucket2='known', disc_score2=0   (catalogue explicitly names it)
  known + med/low -> KEEP discovery + llm_flag='likely_catalogued' (he confirms)
  witness         -> KEEP; llm_verdict drives Road 1 to suggest עד נוסח
                     (catalogue = a specific predictive rite/ceremony/Bible-section)
  discovery/different/uncertain -> KEEP (generic catalogue = a real find)

Every touched discovery row is annotated with llm_verdict/llm_conf/llm_reason so
Road 1 can show WHY. Idempotent: only acts on rows still bucket2=='discovery'.

In/Out: data/discovery_scored_gated.jsonl (updated in place)
        + results/discovery_llm_title_gate_report.md
Usage: python -X utf8 -u discovery_llm_title_gate.py
"""
import csv
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from title_gate_llm import get_verdict, load_cache

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
GATED = PROBE + r"\data\discovery_scored_gated.jsonl"
LIB = r"C:\Genizahsearch\libraries.csv"
MD = PROBE + r"\results\discovery_llm_title_gate_report.md"


def _load_nli():
    out = {}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for row in rd:
            if len(row) >= 8 and row[0]:
                out[row[0]] = (row[7] or '').strip()
    return out


def main():
    nli = _load_nli()
    cache = load_cache()
    rows = [json.loads(l) for l in open(GATED, encoding='utf-8')]

    n_disc = n_titled = 0
    demoted = 0
    flagged = 0
    vcount = Counter()
    examples = {'known': [], 'witness': [], 'discovery': [], 'different': []}
    for r in rows:
        r.setdefault('llm_verdict', None)
        r.setdefault('llm_conf', None)
        r.setdefault('llm_reason', None)
        r.setdefault('llm_flag', None)
        if r.get('bucket2') != 'discovery':
            continue
        n_disc += 1
        t = nli.get(str(r['sys_id']), '')
        if not t:
            continue
        n_titled += 1
        v = get_verdict(r.get('title') or '', [t], cache)
        if not v:
            continue
        r['llm_verdict'] = v['verdict']
        r['llm_conf'] = v['confidence']
        r['llm_reason'] = v['reason']
        vcount[v['verdict']] += 1
        if v['verdict'] == 'known':
            if v['confidence'] == 'high':
                r['bucket2'] = 'known'
                r['disc_score2'] = 0.0
                demoted += 1
                if len(examples['known']) < 25:
                    examples['known'].append((r.get('title') or '', t, v['reason']))
            else:
                r['llm_flag'] = 'likely_catalogued'
                flagged += 1
        elif v['verdict'] in examples and len(examples[v['verdict']]) < 8:
            examples[v['verdict']].append((r.get('title') or '', t, v['reason']))

    with open(GATED, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    L = ["# LLM title-understanding gate (MAPV2-15o, refined 5-way)", "",
         f"- discovery rows entering: {n_disc}",
         f"- with a catalogue title (gate scope): {n_titled}",
         f"- verdicts applied: {dict(vcount)}",
         f"- **demoted discovery -> known (known, high): {demoted}**",
         f"- flagged 'likely_catalogued' (known, med/low — kept): {flagged}",
         "", "## sample known demotions (work <= catalogue | reason)", ""]
    for w, t, why in examples['known']:
        L.append(f"- {w[:40]}  ⇐  {t[:44]}  | {why[:40]}")
    L += ["", "## sample witness (KEPT — catalogue predicts the unit)", ""]
    for w, t, why in examples['witness']:
        L.append(f"- {w[:40]}  ⇐  {t[:44]}  | {why[:40]}")
    L += ["", "## sample discovery (KEPT — generic catalogue)", ""]
    for w, t, why in examples['discovery']:
        L.append(f"- {w[:40]}  ⇐  {t[:44]}  | {why[:40]}")
    open(MD, 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    print('\n'.join(L[:12]))
    print(f"\nwrote {GATED} + {MD}")


if __name__ == '__main__':
    main()
