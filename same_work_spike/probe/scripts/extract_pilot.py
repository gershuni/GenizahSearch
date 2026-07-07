# -*- coding: utf-8 -*-
"""Single streaming pass over Transcriptions.txt -> probe.db (pilot corpus).

Buckets: joins / titles / fgp / bh (target sys_ids) + 10K reservoir background.
"""
import json
import random
import re
import sqlite3
import time

ROOT = r"C:\Genizahsearch"
SRC = ROOT + r"\Transcriptions.txt"
BUCKETS = ROOT + r"\same_work_spike\probe\data\buckets.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
DB = ROOT + r"\same_work_spike\probe\data\probe.db"
REPORT = ROOT + r"\same_work_spike\probe\data\extract_report.txt"

random.seed(43)
HEADER_RE = re.compile(r'^==> (\S+) <==')
HEB_RE = re.compile(r'[א-ת]')

# ---- build target map: sys_id -> set(bucket tags) ----
targets = {}
b = json.load(open(BUCKETS, encoding='utf-8'))
for gid, members in b['joins'].items():
    for s in members:
        targets.setdefault(s, set()).add('joins')
for tid, t in b['titles'].items():
    for s in t['sys_ids']:
        targets.setdefault(s, set()).add('titles')
for s in b['fgp']:
    targets.setdefault(s, set()).add('fgp')

bh = json.load(open(BH, encoding='utf-8'))
bh_sys = set()
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            bh_sys.add(s)
            targets.setdefault(s, set()).add('bh')

print(f"target sys_ids: {len(targets)} (bh={len(bh_sys)})")

# ---- stream ----
N_BG = 10_000
reservoir = []  # (page_id, sys_id, text)
seen_bg = 0
target_pages = []  # (page_id, sys_id, buckets, text)

t0 = time.time()
cur_id = None
buf = []
n_records = 0


def flush(cur_id, buf):
    global seen_bg, n_records
    if cur_id is None:
        return
    n_records += 1
    sys_id = cur_id.split('_')[0]
    text = "\n".join(buf).strip()
    if sys_id in targets:
        target_pages.append((cur_id, sys_id, ','.join(sorted(targets[sys_id])), text))
    else:
        # background candidates: require some real Hebrew content
        heb = len(HEB_RE.findall(text))
        if heb >= 80:
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
rows = [(pid, sid, bk, len(tx), tx) for pid, sid, bk, tx in target_pages]
rows += [(pid, sid, 'background', len(tx), tx) for pid, sid, tx in reservoir]
con.executemany("INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?)", rows)
con.execute("CREATE INDEX idx_sys ON pages(sys_id)")
con.execute("CREATE INDEX idx_buckets ON pages(buckets)")
con.commit()

# ---- report ----
lines = [
    f"records streamed: {n_records} in {elapsed:.0f}s",
    f"target pages: {len(target_pages)}",
    f"background pool (>=80 heb chars): {seen_bg}, sampled: {len(reservoir)}",
]
from collections import Counter
per_bucket_pages = Counter()
per_bucket_sys = {}
found_sys = set()
for pid, sid, bk, tx in target_pages:
    found_sys.add(sid)
    for tag in bk.split(','):
        per_bucket_pages[tag] += 1
        per_bucket_sys.setdefault(tag, set()).add(sid)
for tag in sorted(per_bucket_pages):
    lines.append(f"  {tag}: {per_bucket_pages[tag]} pages, "
                 f"{len(per_bucket_sys[tag])} sys_ids")
missing = {s: tags for s, tags in targets.items() if s not in found_sys}
mc = Counter(t for tags in missing.values() for t in tags)
lines.append(f"target sys_ids with NO pages in corpus: {len(missing)} "
             f"(by bucket: {dict(mc)})")
open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
con.close()
