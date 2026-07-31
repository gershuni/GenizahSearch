# -*- coding: utf-8 -*-
"""Extract REAL corpus-wide data for sketch 003 — the discovery findings page.
Three candidate row units, real facet counts, real sample rows."""
import csv, json, sqlite3, time

DB   = r'C:\Genizahsearch\discovery_data\discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db'
LIBS = r'C:\Genizahsearch\libraries.csv'
OUT  = r'C:\Genizahsearch\.planning\sketches\003-discovery-findings-page\data.js'

conn = sqlite3.connect('file:' + DB + '?mode=ro', uri=True)
conn.row_factory = sqlite3.Row
t0 = time.time()

BASE = """FROM discovery_claim dc
JOIN discovery_evidence e ON e.evidence_id = dc.display_evidence_id
WHERE e.routing_status='shipped'"""

out = {}

# ---- totals for the three candidate units -------------------------------
out['totals'] = {
  'claims':          conn.execute(f"SELECT COUNT(*) n {BASE}").fetchone()['n'],
  'identifications': conn.execute(f"SELECT COUNT(*) n FROM (SELECT e.sys_id, dc.work_id {BASE} GROUP BY 1,2)").fetchone()['n'],
  'manuscripts':     conn.execute(f"SELECT COUNT(DISTINCT e.sys_id) n {BASE}").fetchone()['n'],
  'works':           conn.execute(f"SELECT COUNT(DISTINCT dc.work_id) n {BASE}").fetchone()['n'],
  'msMultiWork':     conn.execute(f"SELECT COUNT(*) n FROM (SELECT e.sys_id {BASE} GROUP BY 1 HAVING COUNT(DISTINCT dc.work_id)>1)").fetchone()['n'],
}
print('totals', dict(out['totals']), f'{time.time()-t0:.0f}s')

# ---- tier facet counts (the ONE filter axis that exists today) ----------
out['tierFacets'] = [dict(r) for r in conn.execute(f"""
  SELECT e.evidence_source AS src, e.confidence_band AS band, COUNT(*) AS n {BASE}
  GROUP BY 1,2 ORDER BY 3 DESC""")]

# ---- novelty availability (the honesty problem) -------------------------
out['noveltyState'] = [dict(r) for r in conn.execute(f"""
  SELECT e.evidence_source AS src, e.is_new AS isNew, COUNT(*) AS n {BASE}
  GROUP BY 1,2 ORDER BY 1,2""")]

# ---- UNIT A: one line per identification (manuscript x work) -----------
rows_a = [dict(r) for r in conn.execute(f"""
  SELECT e.sys_id AS sysId, dc.work_id AS wid,
         MIN(CASE e.confidence_band WHEN 'tier_a' THEN 0
                  WHEN 'high_confidence_algorithmic' THEN 1
                  WHEN 'corroborated' THEN 2 WHEN 'weak' THEN 3
                  WHEN 'screening_canon' THEN 4 WHEN 'screening_rb' THEN 5 ELSE 6 END) AS rank,
         COUNT(DISTINCT dc.page_id) AS pages,
         SUM(COALESCE(e.matched_letters,0)) AS letters,
         MAX(COALESCE(e.matched_letters,0)) AS maxLetters,
         MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                  WHEN 'quotes_this_work' THEN 1 ELSE 2 END) AS ctrank,
         MAX(e.evidence_source) AS src,
         MAX(e.is_new) AS isNew
  {BASE} GROUP BY 1,2
  ORDER BY rank ASC, letters DESC LIMIT 60""")]
CTYPE = {0:'direct_witness',1:'quotes_this_work',2:'shared_text'}
def stamp(rows):
    for r in rows:
        r['ctype'] = CTYPE.get(r.get('ctrank'), 'shared_text')
        r.pop('ctrank', None)
    return rows

RANKBAND = {0:'tier_a',1:'high_confidence_algorithmic',2:'corroborated',3:'weak',
            4:'screening_canon',5:'screening_rb',6:'not_evaluated'}
for r in rows_a:
    r['band'] = RANKBAND[r['rank']]

# a few screening rows so the tier filter has something to hide
rows_a += [dict(r, band=RANKBAND[r['rank']]) for r in conn.execute(f"""
  SELECT e.sys_id AS sysId, dc.work_id AS wid, 4 AS rank,
         COUNT(DISTINCT dc.page_id) AS pages, SUM(COALESCE(e.matched_letters,0)) AS letters,
         MAX(COALESCE(e.matched_letters,0)) AS maxLetters,
         MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                  WHEN 'quotes_this_work' THEN 1 ELSE 2 END) AS ctrank,
         MAX(e.evidence_source) AS src, MAX(e.is_new) AS isNew
  {BASE} AND e.confidence_band IN ('screening_canon','screening_rb')
  GROUP BY 1,2 ORDER BY letters DESC LIMIT 12""")]
# and propagated rows, which are where novelty actually exists today
rows_a += [dict(r, band='not_evaluated') for r in conn.execute(f"""
  SELECT e.sys_id AS sysId, dc.work_id AS wid, 6 AS rank,
         COUNT(DISTINCT dc.page_id) AS pages, SUM(COALESCE(e.matched_letters,0)) AS letters,
         MAX(COALESCE(e.matched_letters,0)) AS maxLetters,
         MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                  WHEN 'quotes_this_work' THEN 1 ELSE 2 END) AS ctrank,
         MAX(e.evidence_source) AS src, MAX(e.is_new) AS isNew
  {BASE} AND e.evidence_source='propagated' AND e.is_new=1
  GROUP BY 1,2 ORDER BY pages DESC LIMIT 14""")]
print('unit A rows', len(rows_a), f'{time.time()-t0:.0f}s')

# ---- UNIT B: one line per manuscript ------------------------------------
rows_b = [dict(r) for r in conn.execute(f"""
  SELECT e.sys_id AS sysId, COUNT(DISTINCT dc.work_id) AS works,
         COUNT(DISTINCT dc.page_id) AS pages, COUNT(*) AS claims,
         MIN(CASE e.confidence_band WHEN 'tier_a' THEN 0
                  WHEN 'high_confidence_algorithmic' THEN 1 WHEN 'corroborated' THEN 2
                  WHEN 'weak' THEN 3 WHEN 'screening_canon' THEN 4
                  WHEN 'screening_rb' THEN 5 ELSE 6 END) AS rank,
         MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                  WHEN 'quotes_this_work' THEN 1 ELSE 2 END) AS ctrank,
         MAX(COALESCE(e.matched_letters,0)) AS maxLetters,
         MAX(e.is_new) AS isNew
  {BASE} GROUP BY 1 ORDER BY works DESC, claims DESC LIMIT 45""")]
for r in rows_b:
    r['band'] = RANKBAND[r['rank']]
print('unit B rows', len(rows_b), f'{time.time()-t0:.0f}s')

# ---- UNIT C: one line per work ------------------------------------------
rows_c = [dict(r) for r in conn.execute(f"""
  SELECT dc.work_id AS wid, COUNT(DISTINCT e.sys_id) AS mss,
         COUNT(DISTINCT dc.page_id) AS pages, COUNT(*) AS claims,
         MIN(CASE e.confidence_band WHEN 'tier_a' THEN 0
                  WHEN 'high_confidence_algorithmic' THEN 1 WHEN 'corroborated' THEN 2
                  WHEN 'weak' THEN 3 WHEN 'screening_canon' THEN 4
                  WHEN 'screening_rb' THEN 5 ELSE 6 END) AS rank,
         MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                  WHEN 'quotes_this_work' THEN 1 ELSE 2 END) AS ctrank
  {BASE} GROUP BY 1 ORDER BY mss DESC LIMIT 45""")]
for r in rows_c:
    r['band'] = RANKBAND[r['rank']]
print('unit C rows', len(rows_c), f'{time.time()-t0:.0f}s')

# ---- work titles + shelfmarks -------------------------------------------
wids = {r['wid'] for r in rows_a} | {r['wid'] for r in rows_c}
works = {}
for w in wids:
    r = conn.execute("""SELECT w.work_id, COALESCE(cw.neutral_title, w.neutral_title) AS title,
                               w.author, w.genre FROM works w
                        LEFT JOIN works cw ON cw.work_id=w.canonical_work_id
                        WHERE w.work_id=?""", (w,)).fetchone()
    if r:
        works[w] = {'title': r['title'], 'author': r['author'] or '', 'genre': r['genre'] or ''}
out['works'] = works

need = {r['sysId'] for r in rows_a} | {r['sysId'] for r in rows_b}
libs = {}
with open(LIBS, encoding='utf-8-sig', newline='') as f:
    for row in csv.reader(f):
        if row and row[0].strip() in need:
            libs[row[0].strip()] = {
                'sm': (row[2] if len(row) > 2 else '').split('|')[0].strip(),
                'lib': (row[3] if len(row) > 3 else ''),
                'cat': (row[7] if len(row) > 7 else ''),
            }
out['libs'] = libs
print('works', len(works), 'shelfmarks', len(libs), 'of', len(need))

# manuscript -> its work titles, for the manuscript-unit rows
msWorks = {}
for sid in {r['sysId'] for r in rows_b}:
    ts = [x['title'] for x in conn.execute("""
      SELECT DISTINCT COALESCE(cw.neutral_title, w.neutral_title) AS title
      FROM discovery_claim dc JOIN discovery_evidence e ON e.evidence_id=dc.display_evidence_id
      JOIN works w ON w.work_id=dc.work_id LEFT JOIN works cw ON cw.work_id=w.canonical_work_id
      WHERE e.sys_id=? AND e.routing_status='shipped' LIMIT 6""", (sid,))]
    msWorks[sid] = ts
out['msWorks'] = msWorks

out['unitA'], out['unitB'], out['unitC'] = stamp(rows_a), stamp(rows_b), stamp(rows_c)
out['schema'] = {
    'coveragePpmExists': 'coverage_ppm' in [r[1] for r in conn.execute('PRAGMA table_info(discovery_evidence)')],
    'bandRankExists':    'band_rank'    in [r[1] for r in conn.execute('PRAGMA table_info(discovery_evidence)')],
}

import os
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as f:
    f.write('/* REAL corpus-wide data from the deployed discovery-v1-33499c5b asset.\n'
            '   Generated by extract_findings.py -- do not hand-edit. */\n')
    f.write('const FINDINGS = ')
    json.dump(out, f, ensure_ascii=False, indent=1, default=str)
    f.write(';\n')
print('wrote', OUT, os.path.getsize(OUT), 'bytes', f'{time.time()-t0:.0f}s total')
