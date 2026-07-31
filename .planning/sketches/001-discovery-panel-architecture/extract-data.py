# -*- coding: utf-8 -*-
"""Extract REAL panel data for sketch 001 round 2.

Wider manuscript set, all tiers, all three claim_types, REAL 'elsewhere' work
names (D-13h), and the manuscript-wide D-13g population.

Disposition model (what the panel would actually show):
  default-visible  = shipped AND band not screening AND matched_letters >= 150
                     OR adjudication_status == 'human_confirmed'   <- D-13g fix
  toggle-gated      = everything else (screening bands, review_only, short passages)
"""
import csv, json, os, re, sqlite3, unicodedata
from collections import defaultdict

DB    = r'C:\Genizahsearch\discovery_data\discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db'
TRANS = r'C:\Genizahsearch\Transcriptions.txt'
LIBS  = r'C:\Genizahsearch\libraries.csv'
OUT   = r'C:\Genizahsearch\.planning\sketches\001-discovery-panel-architecture\data.js'
TCACHE = r'C:\Users\gersh\AppData\Local\Temp\claude\C--Genizahsearch\c7dc22b6-829f-4105-8778-248720ba53ce\scratchpad\ptexts.json'

SHORT_LETTERS = 150
SCREENING_BANDS = {'screening_canon', 'screening_rb'}

ANCHORS = [
    ('clean',        '990051830420205171', None),
    ('commentary',   '990051079570205171', '990051079570205171_IE158601508_P000002_FL158601518'),
    ('judeo-arabic', '990051266160205171', '990051266160205171_IE166214342_P000001_FL166214344'),
    ('reviewed',     '990053836730205171', '990053836730205171_IE138575966_P000023_FL138575992'),
    ('siddur',       '990001441730205171', '990001441730205171_IE49002253_P000006_FL49002304'),
    ('shared-text',  '990025289620205171', '990025289620205171_IE169347673_P000001_FL169347675'),
    ('high-count',   '990000571720205171', None),
    ('variety-a',    '990051762180205171', None),
    ('variety-b',    '990000989500205171', None),
    ('variety-c',    '990000852430205171', None),
    ('variety-d',    '997012160770405171', None),
    ('variety-e',    '990001233460205171', None),
    ('variety-f',    '990051381880205171', None),
]

conn = sqlite3.connect('file:' + DB + '?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

CLAIMS = """
SELECT dc.page_id, dc.work_id, dc.claim_type,
       e.evidence_source, e.confidence_band, e.adjudication_status, e.routing_status,
       e.routing_reason, e.is_new, e.sys_id, e.matched_letters, e.n_spans,
       e.span_start, e.span_end,
       w.neutral_title, w.author, w.canonical_work_id, cw.neutral_title AS canon_title
FROM discovery_claim dc
JOIN discovery_evidence e ON e.evidence_id = dc.display_evidence_id
JOIN works w  ON w.work_id = dc.work_id
LEFT JOIN works cw ON cw.work_id = w.canonical_work_id
WHERE e.sys_id = ?
"""

HEB = re.compile(r'[\u0590-\u05FF]')
def heb_letters(s):
    return sum(1 for ch in s if HEB.match(ch) and unicodedata.category(ch) != 'Mn')

_strip = re.compile(r'[\s\u05be\u05f3\u05f4"\'()\[\],.;:\-]+')
def norm_title(s):
    return _strip.sub('', (s or ''))

def same_work_different_granularity(a, b):
    """D-13d display-time heuristic (research rec.): normalized title containment."""
    na, nb = norm_title(a), norm_title(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na

def title_of(r):
    return r['canon_title'] or r['neutral_title']

def visible_by_default(r):
    if r['adjudication_status'] == 'human_confirmed':
        return True                                   # D-13g
    if r['routing_status'] != 'shipped':
        return False
    if r['confidence_band'] in SCREENING_BANDS:
        return False
    ml = r['matched_letters']
    if ml is not None and ml < SHORT_LETTERS:
        return False                                  # D-13c
    return True

wcount = {}
def witnesses(wid):
    if wid not in wcount:
        wcount[wid] = conn.execute(
            """SELECT COUNT(DISTINCT e.sys_id) n FROM discovery_claim dc
               JOIN discovery_evidence e ON e.evidence_id=dc.display_evidence_id
               WHERE dc.work_id=? AND e.routing_status='shipped'""", (wid,)).fetchone()['n']
    return wcount[wid]


def build_page(rows_on_page, page_letters):
    """Return (identifications, generic_span_groups, hidden_short, hidden_gated)."""
    # D-13a: collapse duplicates by canonical_work_id, canonical title wins
    seen, collapsed = {}, []
    for r in sorted(rows_on_page, key=lambda x: -(x['matched_letters'] or 0)):
        ck = r['canonical_work_id'] or r['work_id']
        if ck in seen:
            continue
        seen[ck] = r
        collapsed.append(r)

    vis = [r for r in collapsed if visible_by_default(r)]
    hidden = [r for r in collapsed if not visible_by_default(r)]
    h_short = sum(1 for r in hidden
                  if r['matched_letters'] is not None and r['matched_letters'] < SHORT_LETTERS)
    h_gated = len(hidden) - h_short

    # D-13d: identical-span groups among the visible set
    groups = defaultdict(list)
    for r in vis:
        if r['span_start'] is not None and r['span_end'] is not None:
            groups[(r['span_start'], r['span_end'])].append(r)

    generic, alias, drop = [], {}, set()
    for (s, e), grp in groups.items():
        if len(grp) < 2:
            continue
        titles = [title_of(g) for g in grp]
        if all(same_work_different_granularity(titles[0], t) for t in titles[1:]):
            lead = grp[0]                              # granularity aliases -> collapse
            alias[id(lead)] = [{'title': title_of(g), 'wid': g['work_id']} for g in grp[1:]]
            drop.update(id(g) for g in grp[1:])
        else:
            generic.append({'offs': f'{s}\u2013{e}',
                            'letters': max((g['matched_letters'] or 0) for g in grp),
                            'works': titles})
            drop.update(id(g) for g in grp)

    ids = []
    for r in vis:
        if id(r) in drop:
            continue
        ml = r['matched_letters']
        cov = None
        if r['evidence_source'] == 'track1_direct' and ml and page_letters:
            cov = min(100, round(100 * ml / page_letters))
        ids.append({
            'work': title_of(r), 'wid': r['work_id'], 'author': r['author'] or '',
            'family': 'direct' if r['evidence_source'] == 'track1_direct' else 'propagated',
            'ctype': r['claim_type'], 'band': r['confidence_band'],
            'humanConfirmed': r['adjudication_status'] == 'human_confirmed',
            'lowCoverage': r['routing_status'] != 'shipped'
                           and r['routing_reason'] == 'low_coverage',
            'cov': cov, 'letters': ml, 'nspans': r['n_spans'],
            'offs': f"{r['span_start']}\u2013{r['span_end']}" if r['span_start'] is not None else None,
            'others': witnesses(r['work_id']),
            'alias': alias.get(id(r)),
        })
    ids.sort(key=lambda x: (x['band'] != 'tier_a', -(x['letters'] or 0)))
    return ids, generic, h_short, h_gated


# ---- pass 1: load rows, choose anchors by RENDERED outcome ----
raw, need_pages, need_sys = {}, set(), set()
for kind, sys_id, fixed in ANCHORS:
    rows = [dict(r) for r in conn.execute(CLAIMS, (sys_id,))]
    by_page = defaultdict(list)
    for r in rows:
        by_page[r['page_id']].append(r)
    if fixed:
        anchor = fixed
    else:
        # score without page_letters (coverage does not affect selection)
        best, anchor = None, None
        for pg, prs in by_page.items():
            ids, gen, hs, hg = build_page(prs, 0)
            score = (len(ids), len({i['ctype'] for i in ids}), len({i['band'] for i in ids}), len(prs))
            if best is None or score > best:
                best, anchor = score, pg
    raw[sys_id] = (kind, anchor, rows, by_page)
    need_sys.add(sys_id)
    need_pages.add(anchor)

libs = {}
with open(LIBS, encoding='utf-8-sig', newline='') as f:
    for row in csv.reader(f):
        if row and row[0].strip() in need_sys:
            libs[row[0].strip()] = {
                'sm': (row[2] if len(row) > 2 else '').split('|')[0].strip(),
                'lib': (row[3] if len(row) > 3 else ''),
                'cat': (row[7] if len(row) > 7 else ''),
            }
print('shelfmarks', len(libs), 'of', len(need_sys))

# ---- page texts, cached ----
texts = {}
if os.path.exists(TCACHE):
    texts = {k: v for k, v in json.load(open(TCACHE, encoding='utf-8')).items()}
missing = need_pages - set(texts)
if missing:
    hdr = re.compile(r'^==> (.+?) <==\s*$')
    cur, buf = None, []
    with open(TRANS, encoding='utf-8') as f:
        for line in f:
            m = hdr.match(line)
            if m:
                if cur in missing:
                    texts[cur] = ''.join(buf)
                    if not (missing - set(texts)):
                        break
                cur, buf = m.group(1), []
            elif cur in missing:
                buf.append(line)
        if cur in missing and cur not in texts:
            texts[cur] = ''.join(buf)
    json.dump(texts, open(TCACHE, 'w', encoding='utf-8'), ensure_ascii=False)
print('page texts', len(need_pages & set(texts)), 'of', len(need_pages))

# ---- pass 2: build ----
out = {'manuscripts': []}
for sys_id, (kind, anchor, rows, by_page) in raw.items():
    L = libs.get(sys_id, {})
    page_letters = heb_letters(texts.get(anchor, ''))
    ids, generic, h_short, h_gated = build_page(by_page.get(anchor, []), page_letters)

    # D-13h: elsewhere, REAL names, split default-visible vs toggle-gated
    ew = {}
    for r in rows:
        if r['page_id'] == anchor:
            continue
        ck = r['canonical_work_id'] or r['work_id']
        d = ew.setdefault(ck, {'title': title_of(r), 'pages': set(), 'gatedPages': set(),
                               'lowcov': False, 'humanConfirmed': False,
                               'ctypes': set(), 'bands': set()})
        d['ctypes'].add(r['claim_type'])
        d['bands'].add(f"{r['evidence_source']}|{r['confidence_band']}")
        if visible_by_default(r):
            d['pages'].add(r['page_id'])
            if r['adjudication_status'] == 'human_confirmed':
                d['humanConfirmed'] = True
            if r['routing_status'] != 'shipped' and r['routing_reason'] == 'low_coverage':
                d['lowcov'] = True
        else:
            d['gatedPages'].add(r['page_id'])
    elsewhere = []
    for d in ew.values():
        elsewhere.append({'title': d['title'], 'pages': len(d['pages']),
                          'gatedPages': len(d['gatedPages']),
                          'gated': len(d['pages']) == 0,
                          'lowcov': d['lowcov'], 'humanConfirmed': d['humanConfirmed'],
                          'ctypes': sorted(d['ctypes']), 'bands': sorted(d['bands'])})
    elsewhere.sort(key=lambda x: (x['gated'], -(x['pages'] + x['gatedPages'])))

    d13g = [{'work': title_of(r), 'page': r['page_id'].split('_')[2],
             'letters': r['matched_letters'], 'reason': r['routing_reason']}
            for r in rows
            if r['adjudication_status'] == 'human_confirmed' and r['routing_status'] != 'shipped']

    rel = conn.execute(
        """SELECT COUNT(DISTINCT CASE WHEN a_page_id=? THEN other_page_id ELSE a_page_id END) n
           FROM discovery_evidence
           WHERE evidence_kind='shared_text' AND (a_page_id=? OR other_page_id=?)
             AND routing_status='shipped'""", (anchor, anchor, anchor)).fetchone()['n']

    ctypes = sorted({r['claim_type'] for r in rows})
    out['manuscripts'].append({
        'key': kind, 'sysId': sys_id,
        'sm': L.get('sm') or sys_id, 'lib': L.get('lib') or '', 'cat': L.get('cat') or '',
        'anchor': anchor, 'pageNum': anchor.split('_')[2].lstrip('P').lstrip('0') or '1',
        'pageLetters': page_letters,
        'onPage': ids, 'shares': generic, 'related': rel,
        'hidden': {'short': h_short, 'gated': h_gated},
        'elsewhere': {'claims': sum(1 for r in rows if r['page_id'] != anchor
                                    and visible_by_default(r)),
                      'pages': len({r['page_id'] for r in rows if r['page_id'] != anchor
                                    and visible_by_default(r)}),
                      'gatedClaims': sum(1 for r in rows if r['page_id'] != anchor
                                         and not visible_by_default(r)),
                      'works': elsewhere},
        'd13g': d13g, 'ctypes': ctypes,
        'totalClaims': len(rows),
        'totalShipped': sum(1 for r in rows if r['routing_status'] == 'shipped'),
        'totalWorks': len({r['work_id'] for r in rows}),
    })
    print(f"{kind:13} {L.get('lib',''):9} ids={len(ids)} ctypes={len({i['ctype'] for i in ids})} "
          f"bands={len({i['band'] for i in ids})} hid={h_short}+{h_gated} gen={len(generic)} "
          f"rel={rel:3} elseW={len(elsewhere)} d13g={len(d13g)} p{page_letters}")

with open(OUT, 'w', encoding='utf-8') as f:
    f.write('/* REAL data from discovery-v1-33499c5b (the deployed asset).\n'
            '   Generated by extract_sk002.py -- do not hand-edit.\n'
            '   Coverage %% is computed from real HTR page text (the asset does not store it). */\n')
    f.write('const PANEL_DATA = ')
    json.dump(out, f, ensure_ascii=False, indent=1, default=str)
    f.write(';\n')
print('wrote', OUT, os.path.getsize(OUT), 'bytes')
