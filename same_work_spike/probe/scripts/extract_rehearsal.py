# -*- coding: utf-8 -*-
"""100K-page scale-rehearsal corpus: Transcriptions.txt -> rehearsal.db.

One streaming pass. Stage-0 page filters (short / target-sheet) + FL-image
dedup applied INLINE (pre-index, per METHOD.md §7). Sample:
- tracers, force-included: all pages of BH-witness sys_ids ('bh') and the
  Tier-1 titles pages ('tier1t') — recall instruments at rehearsal scale;
- background: 100,000-page uniform reservoir over all passing pages (seed 44).
"""
import json
import random
import re
import sqlite3
import time
from collections import Counter

from stage0 import fl_of, page_filter

ROOT = r"C:\Genizahsearch"
SRC = ROOT + r"\Transcriptions.txt"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
DB = ROOT + r"\same_work_spike\probe\data\rehearsal.db"
REPORT = ROOT + r"\same_work_spike\probe\data\rehearsal_extract_report.txt"

random.seed(44)
N_BG = 100_000
HEADER_RE = re.compile(r'^==> (\S+) <==')

# ---- tracer targets ----
bh = json.load(open(BH, encoding='utf-8'))
bh_sys = set()
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        bh_sys.update(sm.get('sys_ids', []))

tier1 = json.load(open(TIER1, encoding='utf-8'))
tier1_pages = set()
for p in tier1['titles']:
    tier1_pages.add(p['a'])
    tier1_pages.add(p['b'])
print(f"tracers: bh sys_ids={len(bh_sys)}, tier1-title pages={len(tier1_pages)}")

# ---- stream ----
drops = Counter()
seen_fl = set()
reservoir = []       # (page_id, sys_id, text)
seen_bg = 0
tracer_pages = []    # (page_id, sys_id, tag, text)
n_records = 0
t0 = time.time()
cur_id = None
buf = []


def flush(cur_id, buf):
    global seen_bg, n_records
    if cur_id is None:
        return
    n_records += 1
    text = "\n".join(buf).strip()
    reason = page_filter(text)
    if reason:
        drops[reason] += 1
        return
    fl = fl_of(cur_id)
    if fl in seen_fl:
        drops['dup_fl'] += 1
        return
    seen_fl.add(fl)
    sys_id = cur_id.split('_')[0]
    tags = []
    if sys_id in bh_sys:
        tags.append('bh')
    if cur_id in tier1_pages:
        tags.append('tier1t')
    if tags:
        tracer_pages.append((cur_id, sys_id, ','.join(tags), text))
    else:
        seen_bg += 1
        if len(reservoir) < N_BG:
            reservoir.append((cur_id, sys_id, text))
        else:
            j = random.randrange(seen_bg)
            if j < N_BG:
                reservoir[j] = (cur_id, sys_id, text)


with open(SRC, encoding='utf-8', errors='replace') as f:
    for line in f:
        m = HEADER_RE.match(line)
        if m:
            flush(cur_id, buf)
            cur_id = m.group(1)
            buf = []
        else:
            buf.append(line.rstrip('\n'))
    flush(cur_id, buf)

elapsed = time.time() - t0

# ---- write DB ----
con = sqlite3.connect(DB)
con.execute("DROP TABLE IF EXISTS pages")
con.execute("""
    CREATE TABLE pages (
        page_id TEXT PRIMARY KEY,
        sys_id TEXT NOT NULL,
        buckets TEXT NOT NULL,
        n_chars INTEGER,
        text TEXT
    )
""")
rows = [(pid, sid, tag, len(tx), tx) for pid, sid, tag, tx in tracer_pages]
rows += [(pid, sid, 'background', len(tx), tx) for pid, sid, tx in reservoir]
con.executemany("INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?)", rows)
con.execute("CREATE INDEX idx_sys ON pages(sys_id)")
con.commit()
con.close()

lines = [
    f"records streamed: {n_records} in {elapsed:.0f}s",
    f"stage-0 drops: {dict(drops)}",
    f"tracer pages kept: {len(tracer_pages)} "
    f"({Counter(t for _, _, t, _ in tracer_pages)})",
    f"background pool (passing): {seen_bg}, sampled: {len(reservoir)}",
    f"TOTAL pages in rehearsal.db: {len(rows)}",
]
open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
