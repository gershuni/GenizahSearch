# -*- coding: utf-8 -*-
"""Q3: identification framing - for each BH page, is its top partner
(by aligned_len, full-pilot verified pairs) another known BH witness?"""
import json
import sqlite3
from collections import defaultdict

ROOT = r"C:\Genizahsearch"
bh = json.load(open(ROOT + r"\same_work_spike\probe\data\bh_witnesses.json",
                    encoding='utf-8'))
sys_to_sigla = defaultdict(set)
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            sys_to_sigla[s].add(sig)

vpairs = json.load(open(ROOT + r"\same_work_spike\probe\results\verified_pairs.json",
                        encoding='utf-8'))
con = sqlite3.connect(ROOT + r"\same_work_spike\probe\data\probe.db")
bh_pages = {r[0] for r in con.execute(
    "SELECT page_id FROM pages WHERE buckets LIKE '%bh%'")}

best = {}
for v in vpairs:
    for me, other in ((v['a'], v['b']), (v['b'], v['a'])):
        if me in bh_pages:
            cur = best.get(me)
            if cur is None or v['aligned_len'] > cur[0]:
                best[me] = (v['aligned_len'], other)

n = len(best)
hit = sum(1 for ln, o in best.values() if sys_to_sigla.get(o.split('_')[0]))
out = (f"BH pages with a partner (full pilot, density<=0.30): {n}/{len(bh_pages)}\n"
       f"top partner is another known BH witness: {hit} ({100 * hit / max(1, n):.0f}%)")
print(out)
open(ROOT + r"\same_work_spike\probe\results\bh_q3.txt", 'w',
     encoding='utf-8').write(out)
