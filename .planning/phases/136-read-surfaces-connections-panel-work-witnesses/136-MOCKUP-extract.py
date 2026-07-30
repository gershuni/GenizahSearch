# -*- coding: utf-8 -*-
"""Extract REAL data from the frozen discovery asset + Transcriptions.txt for the
Phase 136 mockup. Read-only. Writes mockup_data.json to the scratchpad."""
import json, os, re, sqlite3, sys, csv

DB = r'C:\Genizahsearch\discovery_data\discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db'
TRANS = r'C:\Genizahsearch\Transcriptions.txt'
LIBS = r'C:\Genizahsearch\libraries.csv'
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mockup_data.json')

# The anchor manuscript (real): 16 shipped claims / 9 works / 3 pages, one page with 8
ANCHOR_SYS = '990001441730205171'
ANCHOR_PAGE = '990001441730205171_IE49002253_P000006_FL49002304'
# A typical single-claim manuscript for the common case, chosen below.

conn = sqlite3.connect('file:' + DB + '?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

DISP = """
SELECT dc.page_id, dc.work_id, dc.claim_id, dc.source_corpus, dc.claim_type,
       e.evidence_id, e.evidence_kind, e.evidence_source, e.confidence_band,
       e.adjudication_status, e.routing_status, e.is_new, e.sys_id, e.a_page_id,
       e.matched_letters, e.density, e.n_spans, e.span_start, e.span_end,
       e.text_layer, e.snapshot_hash, e.other_page_id,
       w.neutral_title, w.author, w.source_corpus AS work_corpus, w.canonical_work_id
FROM discovery_claim dc
JOIN discovery_evidence e ON e.evidence_id = dc.display_evidence_id
JOIN works w ON w.work_id = dc.work_id
WHERE e.sys_id = ? AND e.routing_status = 'shipped'
ORDER BY dc.page_id, e.matched_letters DESC
"""

data = {'anchor': {'sys_id': ANCHOR_SYS, 'page_id': ANCHOR_PAGE}}
rows = [dict(r) for r in conn.execute(DISP, (ANCHOR_SYS,))]
data['anchor']['claims'] = rows
print('anchor shipped claims:', len(rows), 'pages:', len({r['page_id'] for r in rows}))

# review_only rows on the anchor (what sits behind the toggle)
rows_ro = [dict(r) for r in conn.execute(DISP.replace("= 'shipped'", "= 'review_only'"), (ANCHOR_SYS,))]
data['anchor']['review_only'] = rows_ro
print('anchor review_only claims:', len(rows_ro))

# shared_text (related pages) touching the anchor page
st = [dict(r) for r in conn.execute("""
    SELECT evidence_id, a_page_id, other_page_id, sys_id, confidence_band, evidence_source,
           adjudication_status, routing_status, matched_letters, span_start, span_end, is_new
    FROM discovery_evidence
    WHERE evidence_kind='shared_text' AND (a_page_id=? OR other_page_id=?)
    ORDER BY routing_status, matched_letters DESC
""", (ANCHOR_PAGE, ANCHOR_PAGE))]
data['anchor']['shared_text'] = st
print('shared_text rows on anchor page:', len(st))

# If the anchor page has no shared_text, find a page that does (for the related-pages demo)
if not st:
    r = conn.execute("""
        SELECT a_page_id, COUNT(*) n FROM discovery_evidence
        WHERE evidence_kind='shared_text' AND routing_status='shipped'
        GROUP BY 1 ORDER BY n DESC LIMIT 1
    """).fetchone()
    alt = r['a_page_id']
    data['shared_text_demo_page'] = alt
    data['shared_text_demo'] = [dict(x) for x in conn.execute("""
        SELECT evidence_id, a_page_id, other_page_id, sys_id, confidence_band, evidence_source,
               adjudication_status, routing_status, matched_letters, span_start, span_end
        FROM discovery_evidence
        WHERE evidence_kind='shared_text' AND a_page_id=? AND routing_status='shipped'
        ORDER BY matched_letters DESC LIMIT 8
    """, (alt,))]
    print('shared_text demo page:', alt, len(data['shared_text_demo']))

# ---- the work page: pick a work with a mid-size witness list + one giant one
work_sizes = [dict(r) for r in conn.execute("""
    SELECT dc.work_id, w.neutral_title, w.author, w.source_corpus, COUNT(*) n,
           COUNT(DISTINCT e.sys_id) nsys
    FROM discovery_claim dc JOIN discovery_evidence e ON e.evidence_id=dc.display_evidence_id
    JOIN works w ON w.work_id=dc.work_id
    WHERE e.routing_status='shipped'
    GROUP BY 1 ORDER BY n DESC
""")]
data['work_sizes_top'] = work_sizes[:8]
# a work carried by the anchor manuscript, mid-size, sefaria if possible
anchor_works = {r['work_id'] for r in rows}
cand = [w for w in work_sizes if w['work_id'] in anchor_works and w['source_corpus'] == 'sefaria' and 20 <= w['n'] <= 400]
if not cand:
    cand = [w for w in work_sizes if w['work_id'] in anchor_works and 20 <= w['n'] <= 400]
if not cand:
    cand = [w for w in work_sizes if w['work_id'] in anchor_works]
WORK = cand[0]
data['work'] = dict(WORK)
print('work page demo:', WORK['work_id'], WORK['neutral_title'], WORK['n'])

# unit x work projection for that work (mirrors get_work_witnesses ordering intent)
BAND_RANK = {('track1_direct','high_confidence_algorithmic'):0, ('track1_direct','expert_verified'):0,
             ('track1_direct','tier_a'):1, ('propagated','corroborated'):2, ('propagated','weak'):3,
             ('track1_direct','screening_rb'):4, ('track1_direct','screening_canon'):5,
             ('propagated','not_evaluated'):6}
wrows = [dict(r) for r in conn.execute("""
    SELECT dc.page_id, dc.work_id, dc.claim_id, e.sys_id, e.confidence_band, e.evidence_source,
           e.adjudication_status, e.matched_letters, e.is_new,
           (SELECT unit_id FROM witness_unit_members m WHERE m.sys_id=e.sys_id) AS unit_id
    FROM discovery_claim dc JOIN discovery_evidence e ON e.evidence_id=dc.display_evidence_id
    WHERE dc.work_id=? AND e.routing_status='shipped'
""", (WORK['work_id'],))]
# group by unit (fall back to sys_id when a sys has no unit)
units = {}
for r in wrows:
    key = r['unit_id'] or ('sys:' + r['sys_id'])
    cur = units.get(key)
    rank = BAND_RANK.get((r['evidence_source'], r['confidence_band']), 9)
    if cur is None or rank < cur['_rank']:
        units[key] = {**r, '_rank': rank, 'unit_key': key, 'members': {r['sys_id']}, 'npages': 1}
    else:
        cur['members'].add(r['sys_id'])
        cur['npages'] += 1
for u in units.values():
    u['members'] = sorted(u['members'])
data['work_units'] = sorted(units.values(), key=lambda u: (u['_rank'], u['sys_id']))
print('work units:', len(data['work_units']))

# ---- shelfmarks / libraries for every sys_id we will render
need_sys = {ANCHOR_SYS}
need_sys |= {r['sys_id'] for r in rows} | {r['sys_id'] for r in rows_ro}
need_sys |= {u['sys_id'] for u in data['work_units']}
need_sys |= {m for u in data['work_units'] for m in u['members']}
for k in ('shared_text', 'shared_text_demo'):
    for r in (data.get('anchor', {}).get(k) or data.get(k) or []):
        if r.get('sys_id'):
            need_sys.add(r['sys_id'])
        for pid in (r.get('other_page_id'), r.get('a_page_id')):
            if pid:
                need_sys.add(pid.split('_')[0])
libs = {}
with open(LIBS, encoding='utf-8-sig', newline='') as f:
    for row in csv.reader(f):
        if not row:
            continue
        sid = row[0].strip()
        if sid in need_sys:
            libs[sid] = {'call_numbers': (row[2] if len(row) > 2 else ''),
                         'library_code': (row[3] if len(row) > 3 else ''),
                         'title': (row[7] if len(row) > 7 else '')}
data['libraries'] = libs
print('shelfmarks resolved:', len(libs), 'of', len(need_sys))

# ---- HTR page text for the pages we render
need_pages = {ANCHOR_PAGE} | {r['page_id'] for r in rows} | {r['a_page_id'] for r in rows}
for r in (data['anchor']['shared_text'] or data.get('shared_text_demo') or []):
    if r.get('a_page_id'):
        need_pages.add(r['a_page_id'])
    if r.get('other_page_id'):
        need_pages.add(r['other_page_id'])
if data.get('shared_text_demo_page'):
    need_pages.add(data['shared_text_demo_page'])
print('pages needed from Transcriptions.txt:', len(need_pages))

hdr = re.compile(r'^==> (.+?) <==\s*$')
texts = {}
cur = None
buf = []
with open(TRANS, encoding='utf-8') as f:
    for line in f:
        m = hdr.match(line)
        if m:
            if cur in need_pages:
                texts[cur] = ''.join(buf)
                if len(texts) == len(need_pages):
                    cur = None
                    buf = []
                    break
            cur = m.group(1)
            buf = []
        elif cur in need_pages:
            buf.append(line)
    if cur in need_pages and cur not in texts:
        texts[cur] = ''.join(buf)
data['page_texts'] = {k: v for k, v in texts.items()}
print('page texts found:', len(texts), 'of', len(need_pages))
for p in sorted(need_pages - set(texts)):
    print('  MISSING TEXT:', p)

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=1, default=str)
print('wrote', OUT, os.path.getsize(OUT), 'bytes')
