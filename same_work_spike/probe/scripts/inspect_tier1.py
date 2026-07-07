# -*- coding: utf-8 -*-
"""Dump sample Tier-1 verified pairs with projected Hebrew spans for manual QA."""
import json
import sqlite3
import sys
import unicodedata

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream, project_span  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
OUT = ROOT + r"\same_work_spike\probe\results\tier1_samples.txt"

tier1 = json.load(open(TIER1, encoding='utf-8'))
con = sqlite3.connect(PROBE_DB)


def get_page(pid):
    row = con.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
    return unicodedata.normalize('NFC', row[0]) if row else None


out = []
for fam in ('titles', 'bh', 'joins'):
    pairs = sorted(tier1.get(fam, []), key=lambda p: -p['aligned_len'])
    # top pair, a mid pair, and a borderline short one
    picks = []
    if pairs:
        picks.append(pairs[0])
        if len(pairs) > 4:
            picks.append(pairs[len(pairs) // 2])
        if len(pairs) > 8:
            picks.append(pairs[-1])
    for p in picks:
        ta, tb = get_page(p['a']), get_page(p['b'])
        if not ta or not tb:
            continue
        sa, oa = norm_stream(ta)
        sb, ob = norm_stream(tb)
        out.append(
            f"=== [{fam}] {p['a']} <-> {p['b']} group={p.get('group','')}\n"
            f"    len={p['aligned_len']} density={p['density']} "
            f"anchors={p['n_anchors']} cov={p['coverage_shorter']}\n"
            f"A: {project_span(oa, p['a0'], p['a1'], ta, pad=3)}\n"
            f"B: {project_span(ob, p['b0'], p['b1'], tb, pad=3)}\n")

open(OUT, 'w', encoding='utf-8').write("\n".join(out))
print(f"wrote {OUT} ({len(out)} samples)")
