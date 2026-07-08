# -*- coding: utf-8 -*-
"""Full-corpus extraction: Transcriptions.txt -> fullcorpus.db.

Same streaming pass as extract_rehearsal.py, but keeps ALL pages passing
stage-0 (short / target-sheet / FL-image dedup) — no reservoir sampling.
Tracer tags ('bh', 'tier1t') kept for recall instrumentation; everything
else tagged 'all'.
"""
import json
import re
import sqlite3
import time
from collections import Counter

from stage0 import fl_of, page_filter

ROOT = r"C:\Genizahsearch"
SRC = ROOT + r"\Transcriptions.txt"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
REPORT = ROOT + r"\same_work_spike\probe\data\fullcorpus_extract_report.txt"

HEADER_RE = re.compile(r'^==> (\S+) <==')
BATCH = 5000

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

# ---- DB ----
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

# ---- stream ----
drops = Counter()
seen_fl = set()
tag_counts = Counter()
n_records = n_kept = 0
pending = []
t0 = time.time()
cur_id = None
buf = []


def flush(cur_id, buf):
    global n_records, n_kept
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
    tag = ','.join(tags) if tags else 'all'
    tag_counts[tag] += 1
    n_kept += 1
    pending.append((cur_id, sys_id, tag, len(text), text))
    if len(pending) >= BATCH:
        con.executemany("INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?)",
                        pending)
        pending.clear()


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

if pending:
    con.executemany("INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?)", pending)
con.execute("CREATE INDEX idx_sys ON pages(sys_id)")
con.commit()
n_db = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
con.close()

elapsed = time.time() - t0
lines = [
    f"records streamed: {n_records} in {elapsed:.0f}s",
    f"stage-0 drops: {dict(drops)}",
    f"kept: {n_kept} (tags: {dict(tag_counts)})",
    f"TOTAL pages in fullcorpus.db: {n_db}",
]
open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
