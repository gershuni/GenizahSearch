# -*- coding: utf-8 -*-
"""MAPV2-15n — Road 1: the browsable, faceted discovery report.

Reads data/discovery_scored_flank.jsonl (the corpus discovery candidates with
disc_score2_flank), ranks discovery-on-top, enriches each with shelfmark +
catalog title + a readable page snippet, and emits a self-contained dark-mode
RTL HTML with a sortable / filterable / group-by-work table. This is the
mineable DB Hillel reviews; the SAME rows back the public web/desktop UI (Road 2).

Page/ref text is licensed (Maagarim/Genizah) -> the HTML is LOCAL only; only a
slim id+score manifest is committed.

Out: review/discovery_report.html (LOCAL) + data/discovery_report_manifest.json
Usage: python -X utf8 -u build_discovery_report.py [--top N]
"""
import argparse
import csv
import html
import json
import os
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from discovery_flank import load_target_spans
from build_smoke_preview2 import snippet

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
LIB = r"C:\Genizahsearch\libraries.csv"
IN = PROBE + r"\data\discovery_scored_flank.jsonl"
HTML = PROBE + r"\review\discovery_report.html"
MAN = PROBE + r"\data\discovery_report_manifest.json"

VERD_LABEL = {'target_continuation_strong': 'witness✓✓',
              'target_continuation_weak': 'witness✓', 'mixed_multiwork': 'mixed',
              'abstain': '—', 'likely_citation_weak': 'citation?',
              'likely_citation_strong': 'citation'}


def _ro():
    return sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro', uri=True)


def load_lib():
    """sys_id -> (shelfmark, nli_title) from libraries.csv."""
    out = {}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 8 and row[0]:
                shelf = (row[2] or '').split('|')[0].strip()
                out[row[0]] = (shelf, (row[7] or '').strip())
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=2500)
    a = ap.parse_args()

    rows = [json.loads(l) for l in open(IN, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']
    disc.sort(key=lambda r: -r.get('disc_score2_flank', r.get('disc_score2', 0)))
    total = len(disc)
    top = disc[:a.top]
    print(f"discovery rows {total}; embedding top {len(top)}", flush=True)

    lib = load_lib()
    pages = {r['page_id'] for r in top}
    con = _ro()
    ptext = {}
    pl = list(pages)
    for i in range(0, len(pl), 400):
        b = pl[i:i + 400]
        qm = ','.join('?' * len(b))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({qm})", b):
            ptext[pid] = tx or ''
    con.close()
    tgt = load_target_spans(pages)

    cards = []
    for i, r in enumerate(top):
        sid = str(r['sys_id'])
        shelf, nli = lib.get(sid, ('', ''))
        spans = tgt.get((r['page_id'], r['work_id']), [])
        snip = ''
        if spans and ptext.get(r['page_id']):
            try:
                snip = snippet(ptext[r['page_id']], spans, pad=60)[0]
            except Exception:
                snip = ''
        fl = r.get('flank', {})
        cards.append({
            'rank': i + 1,
            'score': round(r.get('disc_score2_flank', 0), 3),
            'base': round(r.get('disc_score2', 0), 3),
            'work': r.get('title') or r.get('work_id'),
            'genre': r.get('genre') or r.get('cat') or '?',
            'cat': r.get('cat') or '?',
            'letters': r.get('matched_letters', 0),
            'wit': r.get('work_nms', 0),
            'verdict': VERD_LABEL.get(fl.get('verdict', ''), '—'),
            'why': fl.get('why', ''),
            'shelf': shelf, 'nli': nli, 'sys_id': sid,
            'snip': snip[:600],
        })

    # slim committable manifest (ids + scores, NO text)
    json.dump({'total': total, 'embedded': len(cards),
               'cards': [{'rank': c['rank'], 'sys_id': c['sys_id'],
                          'work': c['work'], 'score': c['score'],
                          'verdict': c['verdict'], 'genre': c['genre']}
                         for c in cards]},
              open(MAN, 'w', encoding='utf-8'), ensure_ascii=False)

    genres = Counter(c['genre'] for c in cards)
    verds = Counter(c['verdict'] for c in cards)
    data_json = json.dumps(cards, ensure_ascii=False)
    doc = _render(cards, total, genres, verds, data_json)
    open(HTML, 'w', encoding='utf-8').write(doc)
    print(f"genres: {dict(genres.most_common(8))}")
    print(f"verdicts: {dict(verds)}")
    print(f"wrote {HTML} + {MAN}")


def _render(cards, total, genres, verds, data_json):
    opts = ''.join(f'<option value="{html.escape(g)}">{html.escape(g)} '
                   f'({genres[g]})</option>' for g, _ in genres.most_common())
    return f"""<!doctype html><html lang="he" dir="rtl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>מאגר תגליות — Discovery DB</title><style>
:root{{color-scheme:dark}}
body{{background:#0f1115;color:#e6e6e6;font-family:'Segoe UI',Arial,sans-serif;margin:0;padding:12px}}
h1{{font-size:18px;margin:4px 0}}
.bar{{position:sticky;top:0;background:#0f1115;padding:8px 0;border-bottom:1px solid #333;z-index:5}}
.bar input,.bar select{{background:#1a1d24;color:#e6e6e6;border:1px solid #3a3f4b;border-radius:6px;padding:6px 8px;margin-left:6px;font-size:13px}}
.meta{{color:#8b93a7;font-size:12px;margin:6px 0}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{border-bottom:1px solid #232733;padding:6px 8px;text-align:right;vertical-align:top}}
th{{cursor:pointer;position:sticky;top:52px;background:#161922;color:#cbd3e1;user-select:none}}
th:hover{{color:#fff}}
tr:hover td{{background:#161922}}
.sc{{font-variant-numeric:tabular-nums;font-weight:600}}
.v-witness✓✓,.v-witness✓{{color:#57d98a}} .v-citation,.v-citation\\?{{color:#e0704f}}
.v-mixed{{color:#d9b657}} .snip{{color:#c7cdd9;max-width:520px;line-height:1.5}}
mark{{background:#3b5bdb;color:#fff;padding:0 1px;border-radius:2px}}
.pill{{display:inline-block;background:#1a1d24;border:1px solid #3a3f4b;border-radius:10px;padding:1px 7px;font-size:11px}}
a{{color:#6ea8fe}}
</style></head><body>
<h1>מאגר תגליות · Discovery DB <span class="meta">({total:,} candidates · top {len(cards):,} shown · ranked by flank-adjusted score)</span></h1>
<div class="bar">
 חיפוש<input id="q" placeholder="work / shelfmark / text…" oninput="render()">
 סוגה<select id="g" onchange="render()"><option value="">all genres</option>{opts}</select>
 verdict<select id="v" onchange="render()"><option value="">all</option>
  <option>witness✓✓</option><option>witness✓</option><option>mixed</option>
  <option>—</option><option>citation?</option><option>citation</option></select>
 <label><input type="checkbox" id="grp" onchange="render()"> group by work</label>
 <span class="meta" id="count"></span>
</div>
<div id="host"></div>
<script>
const DATA={data_json};
let sortKey='score',sortDir=-1;
const COLS=[['rank','#'],['score','score'],['verdict','verdict'],['work','work'],
 ['genre','סוגה'],['letters','letters'],['wit','witnesses'],['shelf','shelfmark'],['snip','text']];
function sortBy(k){{sortDir=(sortKey===k)?-sortDir:-1;sortKey=k;render();}}
function esc(s){{return (s==null?'':''+s);}}
function rowHtml(c){{
 return '<tr>'+
  '<td>'+c.rank+'</td>'+
  '<td class="sc">'+c.score.toFixed(2)+'<br><span class="meta">'+c.base.toFixed(2)+'</span></td>'+
  '<td class="v-'+esc(c.verdict)+'">'+esc(c.verdict)+'</td>'+
  '<td><b>'+esc(c.work)+'</b>'+(c.nli?'<br><span class="meta">cat: '+esc(c.nli)+'</span>':'')+
     (c.why?'<br><span class="meta">'+esc(c.why)+'</span>':'')+'</td>'+
  '<td><span class="pill">'+esc(c.genre)+'</span></td>'+
  '<td>'+c.letters+'</td><td>'+c.wit+'</td>'+
  '<td><a href="https://www.nli.org.il/he/search?q='+encodeURIComponent(c.shelf||c.sys_id)+'" target="_blank">'+esc(c.shelf||c.sys_id)+'</a></td>'+
  '<td class="snip">'+esc(c.snip)+'</td></tr>';
}}
function render(){{
 const q=document.getElementById('q').value.trim().toLowerCase();
 const g=document.getElementById('g').value, v=document.getElementById('v').value;
 const grp=document.getElementById('grp').checked;
 let rows=DATA.filter(c=>(!g||c.genre===g)&&(!v||c.verdict===v)&&
   (!q||(c.work+' '+c.shelf+' '+c.snip+' '+c.nli).toLowerCase().includes(q)));
 rows.sort((a,b)=>{{let x=a[sortKey],y=b[sortKey];
   if(typeof x==='string')return sortDir*x.localeCompare(y,'he');return sortDir*(x-y);}});
 document.getElementById('count').textContent=rows.length+' shown';
 let h='';
 if(grp){{
   const by={{}};rows.forEach(c=>{{(by[c.work]=by[c.work]||[]).push(c);}});
   const works=Object.keys(by).sort((a,b)=>by[b].length-by[a].length||
     Math.max(...by[b].map(c=>c.score))-Math.max(...by[a].map(c=>c.score)));
   h='<table><thead><tr>'+COLS.map(c=>'<th onclick="sortBy(\\''+c[0]+'\\')">'+c[1]+'</th>').join('')+'</tr></thead><tbody>';
   for(const w of works){{h+='<tr><td colspan="9" style="background:#12151c;color:#9ecbff;font-weight:600">'+
     esc(w)+' · '+by[w].length+' fragments</td></tr>';by[w].forEach(c=>h+=rowHtml(c));}}
   h+='</tbody></table>';
 }} else {{
   h='<table><thead><tr>'+COLS.map(c=>'<th onclick="sortBy(\\''+c[0]+'\\')">'+c[1]+'</th>').join('')+'</tr></thead><tbody>'+
     rows.map(rowHtml).join('')+'</tbody></table>';
 }}
 document.getElementById('host').innerHTML=h;
}}
render();
</script></body></html>"""


if __name__ == '__main__':
    main()
