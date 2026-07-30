# -*- coding: utf-8 -*-
"""Extract REAL data for the multi-manuscript Phase 136 mockup (7 anchors)."""
import csv, json, os, re, sqlite3

DB = r'C:\Genizahsearch\discovery_data\discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db'
TRANS = r'C:\Genizahsearch\Transcriptions.txt'
LIBS = r'C:\Genizahsearch\libraries.csv'
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multi_data.json')

ANCHORS = [
    ('clean',        '990051830420205171', None),
    ('commentary',   '990051079570205171', '990051079570205171_IE158601508_P000002_FL158601518'),
    ('judeo-arabic', '990051266160205171', '990051266160205171_IE166214342_P000001_FL166214344'),
    ('reviewed',     '990053836730205171', '990053836730205171_IE138575966_P000023_FL138575992'),
    ('siddur',       '990001441730205171', '990001441730205171_IE49002253_P000006_FL49002304'),
    ('shared-text',  '990025289620205171', '990025289620205171_IE169347673_P000001_FL169347675'),
    ('high-count',   '990000571720205171', None),
]

conn = sqlite3.connect('file:' + DB + '?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

CLAIMS = """
SELECT dc.page_id, dc.work_id, dc.claim_id, dc.source_corpus, dc.claim_type,
       e.evidence_id, e.evidence_kind, e.evidence_source, e.confidence_band,
       e.adjudication_status, e.routing_status, e.is_new, e.sys_id, e.a_page_id,
       e.matched_letters, e.density, e.n_spans, e.span_start, e.span_end, e.snapshot_hash,
       w.neutral_title, w.author, w.source_corpus AS work_corpus, w.canonical_work_id,
       cw.neutral_title AS canon_title
FROM discovery_claim dc
JOIN discovery_evidence e ON e.evidence_id = dc.display_evidence_id
JOIN works w ON w.work_id = dc.work_id
LEFT JOIN works cw ON cw.work_id = w.canonical_work_id
WHERE e.sys_id = ?
ORDER BY dc.page_id, e.matched_letters DESC
"""

out = {'manuscripts': []}
need_pages, need_sys = set(), set()

for kind, sys_id, anchor_page in ANCHORS:
    rows = [dict(r) for r in conn.execute(CLAIMS, (sys_id,))]
    shipped = [r for r in rows if r['routing_status'] == 'shipped']
    if anchor_page is None:
        # pick the page with the most shipped claims
        from collections import Counter
        cnt = Counter(r['page_id'] for r in shipped)
        anchor_page = cnt.most_common(1)[0][0] if cnt else None
    st = [dict(r) for r in conn.execute("""
        SELECT evidence_id, a_page_id, other_page_id, sys_id, confidence_band, evidence_source,
               adjudication_status, routing_status, matched_letters, span_start, span_end
        FROM discovery_evidence
        WHERE evidence_kind='shared_text' AND (a_page_id=? OR other_page_id=?)
        ORDER BY routing_status, matched_letters DESC
    """, (anchor_page, anchor_page))]
    m = {'kind': kind, 'sys_id': sys_id, 'anchor_page': anchor_page,
         'claims': rows, 'shared_text': st,
         'n_shipped': len(shipped),
         'n_pages': len({r['page_id'] for r in shipped}),
         'n_works': len({r['work_id'] for r in shipped})}
    out['manuscripts'].append(m)
    need_sys.add(sys_id)
    if anchor_page:
        need_pages.add(anchor_page)
    for r in rows:
        need_sys.add(r['sys_id'])
    for r in st:
        if r.get('sys_id'):
            need_sys.add(r['sys_id'])
        for pid in (r.get('a_page_id'), r.get('other_page_id')):
            if pid:
                need_sys.add(pid.split('_')[0])
    print(kind, sys_id, 'shipped', len(shipped), 'pages', m['n_pages'], 'works', m['n_works'],
          'shared_text', len(st), 'anchor', (anchor_page or '')[-20:])

# witness counts per work (for the nested "other manuscripts" line)
works_needed = {r['work_id'] for m in out['manuscripts'] for r in m['claims']}
wc = {}
for w in works_needed:
    row = conn.execute("""
        SELECT COUNT(*) n, COUNT(DISTINCT e.sys_id) nsys
        FROM discovery_claim dc JOIN discovery_evidence e ON e.evidence_id=dc.display_evidence_id
        WHERE dc.work_id=? AND e.routing_status='shipped'
    """, (w,)).fetchone()
    wc[w] = {'n': row['n'], 'nsys': row['nsys']}
out['work_counts'] = wc

libs = {}
with open(LIBS, encoding='utf-8-sig', newline='') as f:
    for row in csv.reader(f):
        if row and row[0].strip() in need_sys:
            libs[row[0].strip()] = {
                'call_numbers': (row[2] if len(row) > 2 else ''),
                'library_code': (row[3] if len(row) > 3 else ''),
                'title': (row[7] if len(row) > 7 else ''),
            }
out['libraries'] = libs
print('shelfmarks', len(libs), 'of', len(need_sys))

hdr = re.compile(r'^==> (.+?) <==\s*$')
texts, cur, buf = {}, None, []
with open(TRANS, encoding='utf-8') as f:
    for line in f:
        mm = hdr.match(line)
        if mm:
            if cur in need_pages:
                texts[cur] = ''.join(buf)
                if len(texts) == len(need_pages):
                    cur, buf = None, []
                    break
            cur, buf = mm.group(1), []
        elif cur in need_pages:
            buf.append(line)
    if cur in need_pages and cur not in texts:
        texts[cur] = ''.join(buf)
out['page_texts'] = texts
print('page texts', len(texts), 'of', len(need_pages))

json.dump(out, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, default=str)
print('wrote', OUT, os.path.getsize(OUT))
