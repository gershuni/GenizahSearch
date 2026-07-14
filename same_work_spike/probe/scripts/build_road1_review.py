# -*- coding: utf-8 -*-
"""MAPV2-15n (v3) — Road 1 rich grading page + Hillel's grading-feedback fixes.

Top-N discovery candidates (data/discovery_scored_flank.jsonl) as rich cards:
4-bucket SUGGESTION (תגלית/עד נוסח/אחר) + flank verdict + two text panes (page
vs reference edition) + external info + link + grading buttons (localStorage +
export).

Grading-feedback fixes (Hillel, 2026-07-14):
  F1 — מסירה shelfmark cross-check: if the fragment's SHELFMARK appears in the
       matched work's `mesirah` (##המסירה:## edition-source header), the
       fragment IS the edition's base ms -> KNOWN, excluded from the discovery
       pile. Catches works absent from the 738-work resolved mesirot json (card
       27: פירושי גאונים, Evr. II A 313/20). ~142 corpus-wide.
  F2 — scripture-catalog companion: if the fragment's NLI catalog title is a
       Bible section (מקרא/כתובים/תורה/נביאים/חומש/מגילות/…) AND the match is a
       Targum/Tafsir/commentary, the catalog PREDICTS the companion -> suggest
       עד נוסח, not תגלית (a Targum in a Ketuvim codex is not surprising).
       ~3,005 corpus-wide (14%). Kept visible + ranked below true discoveries.

Cards are keyed by manuscript (sys_id|work), so grades survive regeneration;
Hillel's first 27 grades are embedded as a seed. Discoveries rank first.

Out: review/discovery_road1_review.html (LOCAL) + data/discovery_road1_manifest.json
Usage: python -X utf8 -u build_road1_review.py [--top N]
"""
import argparse
import csv
import html
import json
import os
import pickle
import re
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bib_gate import BibGate
from build_smoke_preview2 import RefText, snippet

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
REF_PKL = PROBE + r"\data\ref_corpus_v2.pkl"
LIB = r"C:\Genizahsearch\libraries.csv"
IN = PROBE + r"\data\discovery_scored_flank.jsonl"
OUT_HTML = PROBE + r"\review\discovery_road1_review.html"
OUT_MAN = PROBE + r"\data\discovery_road1_manifest.json"

SUGG_RANK = {'discovery': 0, 'witness': 1, 'other': 2}

# Hillel's first 27 grades (2026-07-14), keyed by sys_id|work so they survive
# regeneration and re-ranking. F1 excludes #27 (known); F2 relabels the Targum
# suggestions to עד נוסח but his 'known' grades stand.
PRIOR_GRADES = {
    "990001438230205171|נפש אשר כבד מאוד חולייה (טקסט בלבד)": "discovery",
    "990001834580205171|פזמונים לשבועות": "discovery",
    "990001966850205171|פיוט הקפות לשמחת תורה": "witness",
    "990001232960205171|מפיוט לקרובת ויושע": "discovery",
    "990001966850205171|מעריב לא׳ דפסח": "witness",
    "990001445620205171|רהיט מקדושתא לראש השנה": "discovery",
    "990053953020205171|קחה מן ההדס מנחת ענפיו (טקסט בלבד)": "discovery",
    "990001232910205171|פיוט לגשם": "discovery",
    "990001443400205171|האל העירה וראה צר על עם שפל יגאה": "discovery",
    "997008595470105171|מעריבים": "discovery",
    "990001966850205171|זמירות ושונות": "witness",
    "990051505420205171|פיוטים לווידוי מקדושת מנחה ליום הכיפורים": "discovery",
    "990001665890205171|תרגום רות": "known",
    "990051213840205171|מלחמות אדוני": "discovery",
    "990053790550205171|השיר צועק על מפלתו כי פשט בגדי תפארתו": "discovery",
    "997008595470105171|שבעתא לשבת": "discovery",
    "990001232940205171|קדושתא לשבועות": "discovery",
    "990001232890205171|קינה על גזירות בסיליאוס": "discovery",
    "990001665890205171|תרגום קהלת": "known",
    "990001438750205171|בעוברי על פני רימון מפחד (טקסט בלבד)": "discovery",
    "990051370270205171|יושב תהילות שמעה צדק והקשב נאקות חרד": "discovery",
    "990051518790205171|לבורא כול וכול יכול ועושה עש כסיל וכימה": "discovery",
    "990001460790205171|אשר הבר שעיפיו ואזניו לאל גלה": "discovery",
    "990001437150205171|פירושי גאונים וקדמונים לתלמוד, יבמות": "known",
}


def norm_shelf(s):
    s = (s or '').lower()
    for j in ('ms.', 'st. petersburg', 'russian national library',
              'the national library of russia', ' rnl ', 'national library',
              'cambridge university library', 'bodleian library', 'bodleian'):
        s = s.replace(j, ' ')
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9]+', ' ', s)).strip()


def suggest_bucket(r, nli_title):
    # The LLM title gate (refined 5-way, validated 99% vs Hillel's grades) is
    # authoritative — the test is "could the catalogue title predict this
    # content?". witness = catalogue names a specific predictive rite/ceremony/
    # Bible-section; discovery = generic catalogue can't predict it (a specific ID
    # beyond a generic title is a real find). 'known'(high) rows were demoted
    # upstream; 'known'(medium, kept) get a "likely catalogued" witness label.
    # The old mechanical F2/statutory fallback is RETIRED — it wrongly called
    # generic-catalogue rows witness (Hillel's 2nd-round grading); every titled
    # row now carries an LLM verdict, and untitled rows default to discovery.
    v = r.get('flank', {}).get('verdict', '')
    lv = r.get('llm_verdict')
    if lv == 'witness':
        return ('witness', 'עד נוסח (הכותרת צופה את התוכן)')
    if lv == 'known':
        return ('witness', 'עד נוסח (כנראה בקטלוג)')
    if v.startswith('likely_citation'):
        return ('other', 'אחר (ציטוט/מקור משותף)')
    if v == 'mixed_multiwork':
        return ('other', 'אחר (טקסט משולב)')
    return ('discovery', 'תגלית')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=500)
    a = ap.parse_args()

    rows = [json.loads(l) for l in open(IN, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']

    mesirah = {w['id']: (w.get('mesirah') or '')
               for w in pickle.load(open(REF_PKL, 'rb'))}
    nli = {}
    with open(LIB, encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for row in rd:
            if len(row) >= 8 and row[0]:
                nli[row[0]] = ((row[2] or '').split('|')[0].strip(),
                               (row[7] or '').strip())

    # resolved (work_id, sys_id) witness pairs from the EXPANDED מסירה registry
    # (maagarim_nosafot_harvest over all discovery works; 738 -> 1,641 works).
    wit_pairs = set()
    try:
        for e in json.load(open(PROBE + r"\data\mesirot_nosafot.json", encoding='utf-8')):
            w = e.get('work_id')
            for m in (e.get('msirot_matched') or []) + (e.get('matched') or []):
                if m.get('sys_id'):
                    wit_pairs.add((w, str(m['sys_id'])))
    except FileNotFoundError:
        pass

    # F1: drop fragments that are KNOWN witnesses — either a resolved witness in
    # the מסירה registry (sys_id matched to the work) OR the fragment shelfmark
    # appears in the work's ##המסירה:## edition-source header.
    kept, dropped_mes, dropped_hdr = [], 0, 0
    for r in disc:
        if (r['work_id'], str(r['sys_id'])) in wit_pairs:
            dropped_mes += 1
            continue
        shelf = nli.get(str(r['sys_id']), ('', ''))[0]
        ns, nm = norm_shelf(shelf), norm_shelf(mesirah.get(r['work_id'], ''))
        if ns and len(ns) > 4 and nm and ns in nm:
            dropped_hdr += 1
            continue
        kept.append(r)
    dropped_f1 = dropped_mes + dropped_hdr
    print(f"F1 drops: מסירה-registry witness={dropped_mes}, "
          f"edition-source-header={dropped_hdr}", flush=True)
    kept.sort(key=lambda r: -r.get('disc_score2_flank', r.get('disc_score2', 0)))
    total = len(kept)
    print(f"discovery {len(disc)}; dropped F1(מסירה-source)={dropped_f1}; "
          f"kept {total}", flush=True)

    top = kept[:a.top]
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
    for r in top:
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
        bucket, bhe = suggest_bucket(r, nli_t)
        fl = r.get('flank', {})
        work = r.get('title') or wid
        cards.append({
            'uid': f"{sid}|{work}", 'sys_id': sid, 'work': work,
            'genre': r.get('genre') or r.get('cat') or '', 'nli': nli_t,
            'shelf': shelf, 'bib': bib, 'mesirah': wit.get(wid, {}).get(sid),
            'page_html': page_htm, 'ref_html': ref_htm,
            'sugg': bucket, 'sugg_he': bhe,
            'verdict': fl.get('verdict', ''), 'flank_why': fl.get('why', ''),
            'llm_flag': r.get('llm_flag'), 'llm_verdict': r.get('llm_verdict') or '',
            'llm_reason': r.get('llm_reason') or '',
            'score': round(r.get('disc_score2_flank', 0), 3),
            'letters': r.get('matched_letters', 0), 'wit': r.get('work_nms', 0),
            'url': f"https://genizahsearch.com/browse?sys_id={sid}",
        })
    con.close()
    # discoveries first, then witnesses, then other; by score within
    cards.sort(key=lambda c: (SUGG_RANK.get(c['sugg'], 3), -c['score']))
    for i, c in enumerate(cards, 1):
        c['no'] = i

    json.dump({'total': total, 'n': len(cards), 'dropped_mesirah_source': dropped_f1,
               'cards': [{'no': c['no'], 'uid': c['uid'], 'work': c['work'],
                          'sugg': c['sugg'], 'score': c['score'],
                          'verdict': c['verdict']} for c in cards]},
              open(OUT_MAN, 'w', encoding='utf-8'), ensure_ascii=False)
    _write_html(cards, total, dropped_f1)
    print(f"suggestions: {dict(Counter(c['sugg'] for c in cards))}")
    print(f"wrote {OUT_HTML} + {OUT_MAN} ({len(cards)} cards)")


def _write_html(cards, total, dropped_f1):
    data = json.dumps(cards, ensure_ascii=False)
    prior = json.dumps(PRIOR_GRADES, ensure_ascii=False)
    grades = [('discovery', 'תגלית', '1'), ('witness', 'עד נוסח', '2'),
              ('known', 'ידוע', '3'), ('other', 'אחר (ציטוט/משותף)', '4'),
              ('tsarich', 'צ"ע', '5')]
    btns = ''.join(f"<button class='g' data-g='{g}' onclick=\"grade('{g}')\">{he} "
                   f"<kbd>{k}</kbd></button>" for g, he, k in grades)
    doc = (_HTML.replace('__DATA__', data).replace('__PRIOR__', prior)
           .replace('__BTNS__', btns).replace('__N__', str(len(cards)))
           .replace('__TOTAL__', f"{total:,}").replace('__DF1__', str(dropped_f1)))
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
.v-strong{color:#57d98a}.v-cit{color:#e0704f}.seed{color:#8b93a7;font-size:12px}</style></head><body>
<div id=bar>
 <b>מאגר תגליות — סקירה</b>
 <span>כרטיס <span id=cur>1</span>/__N__ <span class=chip>מתוך __TOTAL__ מועמדים · הוסרו __DF1__ (מקור מהדורה)</span></span>
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
const ALL=__DATA__;const PRIOR=__PRIOR__;const KEY='seed029_discovery_road1_v2';
let store=JSON.parse(localStorage.getItem(KEY)||'{}');
// seed Hillel's prior grades (keyed by manuscript uid) if not already graded
for(const u in PRIOR){if(!(store[u]&&store[u].grade))store[u]=Object.assign(store[u]||{},{grade:PRIOR[u],seeded:true});}
localStorage.setItem(KEY,JSON.stringify(store));
let CARDS=ALL.slice();let i=0;
const GK={'1':'discovery','2':'witness','3':'known','4':'other','5':'tsarich'};
const HE={discovery:'תגלית',witness:'עד נוסח',known:'ידוע',other:'אחר',tsarich:'צ"ע'};
const VHE={target_continuation_strong:'עד־נוסח חזק (רצף)',target_continuation_weak:'רצף לחיבור',
 mixed_multiwork:'טקסט משולב',abstain:'לא הוכרע (רצף קצר)',
 likely_citation_weak:'אולי ציטוט',likely_citation_strong:'ציטוט (דחוי)'};
function applyFilter(){const f=document.getElementById('fsug').value;
 CARDS=f?ALL.filter(c=>c.sugg===f):ALL.slice();i=0;render();}
function render(){if(!CARDS.length){document.getElementById('card').innerHTML='(אין כרטיסים)';return;}
 const c=CARDS[i];const rec=store[c.uid]||{};
 document.getElementById('cur').textContent=c.no;
 document.getElementById('done').textContent=Object.keys(store).filter(k=>store[k].grade).length;
 document.getElementById('gr').textContent=rec.grade?HE[rec.grade]+(rec.seeded?' (מוזרע)':''):'—';
 let bib=c.bib?`<div class=bib>${c.bib.replace(/</g,'&lt;')}</div>`:'';
 let mes=c.mesirah?`<div style="color:#7bd88f;font-weight:600;margin:4px 0">🔖 מסירת מאגרים ידועה: ${c.mesirah} (→ לרוב ״ידוע״)</div>`:'';
 let vcl=c.verdict.indexOf('citation')>=0?'v-cit':(c.verdict.indexOf('continuation')>=0?'v-strong':'');
 document.getElementById('card').innerHTML=
  `<div><span class="sugg s-${c.sugg}">הצעה: ${c.sugg_he}</span>`+
   `<span class=chip>ציון ${c.score.toFixed(2)}</span>`+
   `<span class="chip ${vcl}">flank: ${VHE[c.verdict]||c.verdict}</span>`+
   `<span class=chip>${c.letters} אותיות · ${c.wit} עדים</span>`+
   (c.flank_why?`<span class=chip>${c.flank_why.replace(/</g,'&lt;')}</span>`:'')+
   (c.llm_flag?`<span class=chip style="background:#3a2f10;color:#ffd479;border:1px solid #6b6321">⚠ ייתכן שכבר בקטלוג: ${(c.llm_reason||'').replace(/</g,'&lt;')}</span>`:'')+`</div>`+
  `<div class=work>${c.work||'—'}</div>`+mes+
  `<div class=meta>${c.genre||''} `+(c.nli?`<span class=nli>· קטלוג NLI: ${c.nli}</span>`:'')+
   ` · <a href="${c.url}" target=_blank>פתח בגניזה↗</a>`+
   (c.shelf?` <span class=chip>${c.shelf}</span>`:'')+`</div>`+bib+
  `<div class=panes><div class=pane><div class=lbl>הדף (גניזה):</div><div class=ev>${c.page_html||''}</div></div>`+
  `<div class=pane><div class=lbl>המקבילה במהדורה (${c.work}):</div><div class=ev>${c.ref_html||'(לא אותרה לתצוגה)'}</div></div></div>`;
 document.getElementById('note').value=rec.note||'';}
function grade(g){const c=CARDS[i];store[c.uid]=Object.assign(store[c.uid]||{},{grade:g,work:c.work,sugg:c.sugg,seeded:false});save();render();setTimeout(next_,120);}
function noteSave(){const c=CARDS[i];store[c.uid]=Object.assign(store[c.uid]||{},{note:document.getElementById('note').value});save();}
function save(){localStorage.setItem(KEY,JSON.stringify(store));}
function next_(){if(i<CARDS.length-1){i++;render();}}
function prev(){if(i>0){i--;render();}}
function nextUn(){for(let j=1;j<=CARDS.length;j++){let k=(i+j)%CARDS.length;if(!(store[CARDS[k].uid]||{}).grade){i=k;render();return;}}}
function exportG(){const out=ALL.map(c=>({no:c.no,sys_id:c.sys_id,work:c.work,sugg:c.sugg,
 grade:(store[c.uid]||{}).grade||null,note:(store[c.uid]||{}).note||null}));
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
