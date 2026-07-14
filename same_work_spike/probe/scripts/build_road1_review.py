# -*- coding: utf-8 -*-
"""MAPV2-15n (v2) — Road 1 as a RICH grading page (like the prior review decks).

Top-N discovery candidates (data/discovery_scored_flank.jsonl, ranked by
disc_score2_flank) rendered as cards with: the 4-BUCKET SUGGESTION up front
(Known / Discovery=תגלית / Textual-witness=עד נוסח / Other), the flank verdict +
score as supporting evidence, two text panes (the Genizah page vs the reference
edition), external info (NLI catalog title + Friedberg bib + Maagarim מסירה),
an open-in-Genizah link, and grading buttons for the four buckets (+ צ"ע).
Grades persist to localStorage and export to JSON.

Not blind (this is a mining/review tool, not the held-out test) — the machine
suggestion is shown; the human confirms/overrides.

Out: review/discovery_road1_review.html (LOCAL, licensed text) +
     data/discovery_road1_manifest.json (slim: ids + suggestion + score)
Usage: python -X utf8 -u build_road1_review.py [--top N]
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
from bib_gate import BibGate
from build_smoke_preview2 import RefText, snippet
from discovery_flank import LITURGY_TITLE

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
LIB = r"C:\Genizahsearch\libraries.csv"
IN = PROBE + r"\data\discovery_scored_flank.jsonl"
OUT_HTML = PROBE + r"\review\discovery_road1_review.html"
OUT_MAN = PROBE + r"\data\discovery_road1_manifest.json"

# statutory liturgical unit heads -> a same-work witness of these is a TEXTUAL
# WITNESS (עד נוסח, catalog predicts the unit by genre) rather than a תגלית.
STAT_HEADS = ('ברכת', 'ברכה', 'תפילה', 'תפיל', 'קדוש', 'הבדלה', 'וידוי',
              'הרחבה', 'פתיחה', 'מעין', 'הושענא', 'תחנון')


def suggest_bucket(r):
    """4-bucket SUGGESTION from the pipeline signals. Every row is already
    catalog-silent (bucket2=='discovery', identified-gate ran) so 'known' won't
    normally appear; the flank verdict splits witness vs citation."""
    v = r.get('flank', {}).get('verdict', '')
    if v.startswith('likely_citation'):
        return ('other', 'אחר (ציטוט/מקור משותף)')
    if v == 'mixed_multiwork':
        return ('other', 'אחר (טקסט משולב)')
    title = r.get('title') or ''
    genre = r.get('genre') or ''
    liturgical = genre in ('פיוט ותפילה', 'שירת ספרד') or \
        any(k in title for k in LITURGY_TITLE)
    if liturgical and any(title.startswith(h) or (' ' + h) in title
                          for h in STAT_HEADS):
        return ('witness', 'עד נוסח')       # standard statutory unit
    return ('discovery', 'תגלית')            # catalog-silent specific content


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=500)
    a = ap.parse_args()

    rows = [json.loads(l) for l in open(IN, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']
    disc.sort(key=lambda r: -r.get('disc_score2_flank', r.get('disc_score2', 0)))
    top = disc[:a.top]
    total = len(disc)
    print(f"discovery {total}; rich cards for top {len(top)}", flush=True)

    nli = {}
    want = {r['sys_id'] for r in top}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for row in rd:
            if len(row) >= 8 and row[0] in want:
                nli[row[0]] = ((row[2] or '').split('|')[0].strip(),
                               (row[7] or '').strip())
    bg = BibGate()
    rt = RefText()
    wit = {}
    try:
        for r in json.load(open(PROBE + r"\data\mesirot_nosafot.json", encoding='utf-8')):
            w = r.get('work_id')
            if w:
                d = {str(m['sys_id']): 'מסירה' for m in (r.get('msirot_matched') or [])}
                for m in (r.get('matched') or []):
                    d.setdefault(str(m['sys_id']), 'מסירה נוספת')
                wit[w] = d
    except FileNotFoundError:
        pass

    con = sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro', uri=True)
    cards = []
    for i, r in enumerate(top, 1):
        pid, wid, sid = r['page_id'], r['work_id'], str(r['sys_id'])
        pr = con.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
        if not pr or not pr[0]:
            continue
        sp = con.execute("SELECT spans_json FROM track1_matches WHERE page_id=? "
                         "AND work_id=?", (pid, wid)).fetchone() or \
            con.execute("SELECT spans_json FROM track1_candidates WHERE page_id=? "
                        "AND work_id=?", (pid, wid)).fetchone()
        spans = json.loads(sp[0]) if sp else []
        page_htm, page_slice = snippet(pr[0], spans) if spans \
            else (html.escape(pr[0][:1200]), None)
        ref_htm = rt.passage(wid, page_slice) if page_slice else ''
        shelf, nli_t = nli.get(sid, ('', ''))
        bib = bg.display(sid)
        bib = '\n'.join(str(x) for x in bib) if isinstance(bib, (list, tuple)) else (bib or '')
        bucket, bhe = suggest_bucket(r)
        fl = r.get('flank', {})
        cards.append({
            'no': i, 'sys_id': sid, 'work': r.get('title') or wid,
            'genre': r.get('genre') or r.get('cat') or '', 'nli': nli_t,
            'shelf': shelf, 'bib': bib, 'mesirah': wit.get(wid, {}).get(sid),
            'page_html': page_htm, 'ref_html': ref_htm,
            'sugg': bucket, 'sugg_he': bhe,
            'verdict': fl.get('verdict', ''), 'flank_why': fl.get('why', ''),
            'score': round(r.get('disc_score2_flank', 0), 3),
            'letters': r.get('matched_letters', 0), 'wit': r.get('work_nms', 0),
            'url': f"https://genizahsearch.com/browse?sys_id={sid}",
        })
    con.close()

    json.dump({'total': total, 'n': len(cards),
               'cards': [{'no': c['no'], 'sys_id': c['sys_id'], 'work': c['work'],
                          'sugg': c['sugg'], 'score': c['score'],
                          'verdict': c['verdict']} for c in cards]},
              open(OUT_MAN, 'w', encoding='utf-8'), ensure_ascii=False)
    _write_html(cards, total)
    print(f"suggestions: {dict(Counter(c['sugg'] for c in cards))}")
    print(f"wrote {OUT_HTML} + {OUT_MAN} ({len(cards)} cards)")


def _write_html(cards, total):
    data = json.dumps(cards, ensure_ascii=False)
    grades = [('discovery', 'תגלית', '1'), ('witness', 'עד נוסח', '2'),
              ('known', 'ידוע', '3'), ('other', 'אחר (ציטוט/משותף)', '4'),
              ('tsarich', 'צ"ע', '5')]
    btns = ''.join(f"<button class='g' data-g='{g}' onclick=\"grade('{g}')\">{he} "
                   f"<kbd>{k}</kbd></button>" for g, he, k in grades)
    doc = (_HTML.replace('__DATA__', data).replace('__BTNS__', btns)
           .replace('__N__', str(len(cards))).replace('__TOTAL__', f"{total:,}"))
    open(OUT_HTML, 'w', encoding='utf-8', newline='\n').write(doc)


_HTML = r"""<!doctype html><html lang=he dir=rtl><head><meta charset=utf-8>
<title>מאגר תגליות — סקירה ודירוג</title><style>
*{box-sizing:border-box}body{margin:0;background:#17181c;color:#e8e8ea;
font-family:'Segoe UI',Arial,sans-serif;line-height:1.7}
#bar{position:sticky;top:0;background:#1f2127;border-bottom:1px solid #333;
padding:10px 16px;display:flex;gap:14px;align-items:center;flex-wrap:wrap;z-index:9}
#bar b{color:#9ecbff}.sp{flex:1}
button{background:#2a2d36;color:#e8e8ea;border:1px solid #3a3d47;border-radius:7px;
padding:7px 12px;cursor:pointer;font-size:15px}button:hover{background:#333743}
button.g{border-color:#4a4d57}kbd{background:#111;border-radius:4px;padding:1px 6px;
font-size:12px;color:#aaa}#wrap{max-width:1050px;margin:0 auto;padding:18px}
.card{background:#1c1e24;border:1px solid #2c2f38;border-radius:12px;padding:18px}
.work{font-size:20px;color:#ffd479;margin:0 0 6px}.meta{color:#9aa;font-size:14px;margin-bottom:4px}
.nli{color:#8fce8f}.bib{color:#c9a227;font-size:13px;white-space:pre-wrap;margin:6px 0}
.panes{display:flex;gap:14px;flex-wrap:wrap;margin-top:12px}
.pane{flex:1 1 440px;background:#15171c;border:1px solid #2a2d36;border-radius:9px;padding:12px}
.lbl{color:#89a;font-size:13px;margin-bottom:6px}.ev{font-size:17px;white-space:pre-wrap}
mark{background:#5a4a00;color:#ffe9a6;padding:0 2px;border-radius:3px}
textarea{width:100%;background:#15171c;color:#e8e8ea;border:1px solid #2a2d36;
border-radius:7px;padding:8px;margin-top:10px;font-family:inherit;font-size:14px}
#done{color:#7bd88f}#gr{color:#ffd479}a{color:#9ecbff}
.sugg{display:inline-block;border-radius:8px;padding:3px 11px;font-size:15px;font-weight:600;margin-inline-end:8px}
.s-discovery{background:#173a2a;color:#7bd88f;border:1px solid #2f6b4d}
.s-witness{background:#33310f;color:#ffd479;border:1px solid #6b6321}
.s-other{background:#3a1f1f;color:#e08a6f;border:1px solid #6b3a2f}
.chip{display:inline-block;background:#2a2d36;border-radius:6px;padding:1px 8px;font-size:12px;color:#bbb;margin-inline-start:6px}
.v-strong{color:#57d98a}.v-cit{color:#e0704f}</style></head><body>
<div id=bar>
 <b>מאגר תגליות — סקירה</b>
 <span>כרטיס <span id=cur>1</span>/__N__ <span class=chip>מתוך __TOTAL__ מועמדים</span></span>
 <span>· דורגו <span id=done>0</span></span>
 <span>· נוכחי: <span id=gr>—</span></span>
 <span class=sp></span>
 <select id=fsug onchange="applyFilter()"><option value="">כל ההצעות</option>
  <option value=discovery>תגלית</option><option value=witness>עד נוסח</option>
  <option value=other>אחר</option></select>
 <button onclick=prev()>הקודם</button><button onclick=next_()>הבא ›</button>
 <button onclick=nextUn()>הבא ללא דירוג</button>
 <button onclick=exportG()>ייצוא JSON</button>
</div>
<div id=wrap><div class=card id=card></div>
 <div style="margin-top:12px">__BTNS__</div>
 <textarea id=note rows=2 placeholder="הערה חופשית (נשמר אוטומטית)"></textarea>
</div>
<script>
window.onerror=function(m,s,l){var d=document.getElementById('card');
 if(d)d.innerHTML="<pre style='color:#f88;white-space:pre-wrap'>JS error: "+m+" (line "+l+")</pre>";return false;};
const ALL=__DATA__;const KEY='seed029_discovery_road1_v1';
let store=JSON.parse(localStorage.getItem(KEY)||'{}');let CARDS=ALL.slice();let i=0;
const GK={'1':'discovery','2':'witness','3':'known','4':'other','5':'tsarich'};
const HE={discovery:'תגלית',witness:'עד נוסח',known:'ידוע',other:'אחר',tsarich:'צ"ע'};
const VHE={target_continuation_strong:'עד־נוסח חזק (רצף)',target_continuation_weak:'רצף לחיבור',
 mixed_multiwork:'טקסט משולב',abstain:'לא הוכרע (רצף קצר)',
 likely_citation_weak:'אולי ציטוט',likely_citation_strong:'ציטוט (דחוי)'};
function applyFilter(){const f=document.getElementById('fsug').value;
 CARDS=f?ALL.filter(c=>c.sugg===f):ALL.slice();i=0;render();}
function render(){if(!CARDS.length){document.getElementById('card').innerHTML='(אין כרטיסים)';return;}
 const c=CARDS[i];const rec=store[c.no]||{};
 document.getElementById('cur').textContent=c.no;
 document.getElementById('done').textContent=Object.keys(store).filter(k=>store[k].grade).length;
 document.getElementById('gr').textContent=rec.grade?HE[rec.grade]:'—';
 let bib=c.bib?`<div class=bib>${c.bib.replace(/</g,'&lt;')}</div>`:'';
 let mes=c.mesirah?`<div style="color:#7bd88f;font-weight:600;margin:4px 0">🔖 מסירת מאגרים ידועה: ${c.mesirah} (→ לרוב ״ידוע״)</div>`:'';
 let vcl=c.verdict.indexOf('citation')>=0?'v-cit':(c.verdict.indexOf('continuation')>=0?'v-strong':'');
 document.getElementById('card').innerHTML=
  `<div><span class="sugg s-${c.sugg}">הצעה: ${c.sugg_he}</span>`+
   `<span class=chip>ציון ${c.score.toFixed(2)}</span>`+
   `<span class="chip ${vcl}">flank: ${VHE[c.verdict]||c.verdict}</span>`+
   `<span class=chip>${c.letters} אותיות · ${c.wit} עדים</span>`+
   (c.flank_why?`<span class=chip>${c.flank_why.replace(/</g,'&lt;')}</span>`:'')+`</div>`+
  `<div class=work>${c.work||'—'}</div>`+mes+
  `<div class=meta>${c.genre||''} `+(c.nli?`<span class=nli>· קטלוג NLI: ${c.nli}</span>`:'')+
   ` · <a href="${c.url}" target=_blank>פתח בגניזה↗</a>`+
   (c.shelf?` <span class=chip>${c.shelf}</span>`:'')+`</div>`+bib+
  `<div class=panes><div class=pane><div class=lbl>הדף (גניזה):</div><div class=ev>${c.page_html||''}</div></div>`+
  `<div class=pane><div class=lbl>המקבילה במהדורה (${c.work}):</div><div class=ev>${c.ref_html||'(לא אותרה לתצוגה)'}</div></div></div>`;
 document.getElementById('note').value=rec.note||'';}
function grade(g){const c=CARDS[i];store[c.no]=Object.assign(store[c.no]||{},{grade:g,work:c.work,sugg:c.sugg});save();render();setTimeout(next_,120);}
function noteSave(){const c=CARDS[i];store[c.no]=Object.assign(store[c.no]||{},{note:document.getElementById('note').value});save();}
function save(){localStorage.setItem(KEY,JSON.stringify(store));}
function next_(){if(i<CARDS.length-1){i++;render();}}
function prev(){if(i>0){i--;render();}}
function nextUn(){for(let j=1;j<=CARDS.length;j++){let k=(i+j)%CARDS.length;if(!(store[CARDS[k].no]||{}).grade){i=k;render();return;}}}
function exportG(){const out=ALL.map(c=>({no:c.no,sys_id:c.sys_id,work:c.work,sugg:c.sugg,
 grade:(store[c.no]||{}).grade||null,note:(store[c.no]||{}).note||null}));
 const b=new Blob([JSON.stringify(out,null,1)],{type:'application/json'});
 const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='mapv2_discovery_road1_human.json';a.click();}
document.getElementById('note').addEventListener('blur',noteSave);
document.addEventListener('keydown',e=>{if(['TEXTAREA','INPUT','SELECT'].includes(e.target.tagName))return;
 if(GK[e.key]){grade(GK[e.key]);e.preventDefault();}
 else if(e.key==='ArrowLeft'){next_();}else if(e.key==='ArrowRight'){prev();}});
try{render();}catch(err){document.getElementById('card').innerHTML=
 "<pre style='color:#f88;white-space:pre-wrap'>render error: "+err.message+"</pre>";}
</script></body></html>"""


if __name__ == '__main__':
    main()
