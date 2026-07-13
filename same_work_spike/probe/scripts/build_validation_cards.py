# -*- coding: utf-8 -*-
"""MAPV2-15g — held-out validation: draw 100 from the FROZEN 467 audit sample,
build a BLIND dark-mode grading page (text-vs-text + catalog/bib, NO machine
prediction shown, so there is zero anchoring), and stash the grader's own
prediction separately for later scoring.

This is the honest test Codex asked for: the grader's threshold + AI layer
have NOT seen these 100 as a graded set; Hillel grades them cold, then we
score the grader against his labels and post-stratify to a corpus estimate.

Draw: deterministic (sha1 rank), proportional to the 467's genre x size x
stitch cells so the 100 is a representative sub-sample; frame cell sizes carry
through for weighting. Grade vocabulary + keys mirror the earlier review page.

Out: review/full_deck/mapv2_validation_100.html  (blind grading page)
     data/validation_100.json  (frozen: the 100 + strata + weights + the
        grader's hidden prediction, for scoring after Hillel exports)
Usage: python -X utf8 -u build_validation_cards.py [--n 100]
"""
import argparse
import hashlib
import html
import json
import os
import sqlite3
from collections import Counter, defaultdict

from bib_gate import BibGate
from build_smoke_preview2 import RefText, snippet
from canon_rarity import SHARED_TH, CanonRarity
from grader import rule_grade
from metadata_scope import ScopeGate
from normalize import norm_stream
from title_gate import TitleGate

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
AUDIT = PROBE + r"\data\audit_sample_v1.json"
OUT_HTML = PROBE + r"\review\full_deck\mapv2_validation_100.html"
OUT_JSON = PROBE + r"\data\validation_100.json"

_ap = argparse.ArgumentParser()
_ap.add_argument('--n', type=int, default=100)
_args = _ap.parse_args()


def rank(pid, wid):
    return int(hashlib.sha1(f"val|{pid}|{wid}".encode()).hexdigest(), 16)


def draw(items, n):
    """Proportional-to-cell deterministic sub-sample of the 467."""
    cells = defaultdict(list)
    for it in items:
        cells[(it['genre_bucket'], it['letters_band'], it['stitch_status'])].append(it)
    total = len(items)
    picked = []
    for c, lst in cells.items():
        lst.sort(key=lambda it: rank(it['page_id'], it['work_id']))
        k = max(1, round(n * len(lst) / total))
        picked.extend(lst[:min(k, len(lst))])
    picked.sort(key=lambda it: rank(it['page_id'], it['work_id']))
    return picked[:n]


def main():
    audit = json.load(open(AUDIT, encoding='utf-8'))
    items = audit['items']
    frame_cells = audit['manifest']['frame_cells']
    picked = draw(items, _args.n)
    print(f"drawn {len(picked)} of {len(items)}", flush=True)

    con = sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro', uri=True)
    rt = RefText()
    tg = TitleGate({})           # nli titles loaded lazily below via libraries
    # nli titles for the picked sys_ids
    import csv
    nli = {}
    with open(r"C:\Genizahsearch\libraries.csv", encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        want = {it['sys_id'] for it in picked}
        for r in rd:
            if len(r) >= 8 and r[0] in want and (r[7] or '').strip():
                nli[r[0]] = r[7].strip()
    tg.nli = nli
    bg = BibGate()
    cr = CanonRarity()
    sg = ScopeGate(nli_titles=nli)
    # Maagarim witness map: work_id -> {sys_id: kind} (מסירה used-for-edition
    # or מסירה נוספת additional witness). If THIS fragment is a listed witness
    # of the matched work, that is decisive 'known' evidence.
    wit = {}
    try:
        for r in json.load(open(PROBE + r"\data\mesirot_nosafot.json", encoding='utf-8')):
            wid_ = r.get('work_id')
            if not wid_:
                continue
            d = {str(m['sys_id']): 'מסירה'
                 for m in (r.get('msirot_matched') or [])}
            for m in (r.get('matched') or []):
                d.setdefault(str(m['sys_id']), 'מסירה נוספת')
            wit[wid_] = d
    except FileNotFoundError:
        pass

    cards = []
    for i, it in enumerate(picked, 1):
        pid, wid, sid = it['page_id'], it['work_id'], it['sys_id']
        row = con.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
        if not row or not row[0]:
            continue
        ptxt = row[0]
        sp = con.execute("SELECT spans_json FROM track1_candidates WHERE "
                         "page_id=? AND work_id=?", (pid, wid)).fetchone()
        if not sp:
            sp = con.execute("SELECT spans_json FROM track1_matches WHERE "
                             "page_id=? AND work_id=?", (pid, wid)).fetchone()
        spans = json.loads(sp[0]) if sp else []
        page_htm, page_slice = snippet(ptxt, spans) if spans else (html.escape(ptxt[:1200]), None)
        ref_htm = rt.passage(wid, page_slice) if page_slice else ''
        name = f"{it.get('author') or ''} — {it.get('title') or ''}".strip(' —')
        nli_t = nli.get(sid, '')
        bib_lines = bg.display(sid)
        if isinstance(bib_lines, (list, tuple)):
            bib_lines = '\n'.join(str(x) for x in bib_lines)
        bib_lines = bib_lines or ''
        # hidden grader prediction (NOT shown in the HTML)
        st = norm_stream(ptxt)[0]
        cmass = 0.0
        if spans:
            iv = sorted(((int(s[0]), int(s[1])) for s in spans))
            merged = []
            for a, b in iv:
                if merged and a <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], b)
                else:
                    merged.append([a, b])
            cmass = cr.mass_per_len(''.join(st[a:b] for a, b in merged))
        mes = wit.get(wid, {}).get(sid)   # 'מסירה' / 'מסירה נוספת' / None
        feat = {'title_class': it['title_class'], 'bib_class': it['bib_class'],
                'work_name': name, 'title': it.get('title'),
                'author': it.get('author'), 'genre': it.get('genre'),
                'scope_regime': it['scope_regime'], 'canon_mass': cmass,
                'maagarim_witness': mes}
        rg, why = rule_grade(feat)
        cards.append({
            'no': i, 'page_id': pid, 'work_id': wid, 'sys_id': sid,
            'work': name, 'nli': nli_t, 'bib': bib_lines, 'mesirah': mes,
            'genre': it.get('genre'), 'page_html': page_htm, 'ref_html': ref_htm,
            'url': f"https://genizahsearch.com/browse?sys_id={sid}",
            # hidden fields for scoring only:
            '_rule_pred': rg, '_rule_why': why, '_canon_mass': round(cmass, 3),
            '_cell': f"{it['genre_bucket']}|{it['letters_band']}|{it['stitch_status']}",
            '_resolution': it['resolution'], '_title_class': it['title_class'],
            '_bib_class': it['bib_class'],
        })
    con.close()

    # frozen record for scoring
    cell_counts = Counter(c['_cell'] for c in cards)
    json.dump({'meta': {'n': len(cards), 'drawn_from': 'audit_sample_v1',
                        'blind': True, 'frame_cells': frame_cells,
                        'sample_cell_counts': dict(cell_counts),
                        'shared_th': SHARED_TH},
               'cards': cards},
              open(OUT_JSON, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    _write_html(cards)
    print(f"wrote {OUT_HTML} + {OUT_JSON} ({len(cards)} cards)")
    print("rule-pred distribution (HIDDEN from grader):",
          dict(Counter(c['_rule_pred'] for c in cards)))


def _write_html(cards):
    # blind cards for the browser: NO prediction fields
    pub = [{'no': c['no'], 'work': c['work'], 'nli': c['nli'], 'bib': c['bib'],
            'genre': c['genre'], 'page_html': c['page_html'],
            'ref_html': c['ref_html'], 'url': c['url'],
            'mesirah': c.get('mesirah')} for c in cards]
    data = json.dumps(pub, ensure_ascii=False)
    grades = [('discovery', 'תגלית', '1'), ('witness', 'עד נוסח', '2'),
              ('citation', 'ציטוט', '3'), ('shared', 'מקור משותף', '4'),
              ('known', 'ידוע', '5'), ('formula', 'פורמולה בלבד', '6'),
              ('norel', 'לא קשור', '7'), ('tsarich', 'צ"ע', '8')]
    btns = ''.join(
        f"<button class='g' data-g='{g}' onclick=\"grade('{g}')\">{he} "
        f"<kbd>{k}</kbd></button>" for g, he, k in grades)
    tmpl = _HTML.replace('__DATA__', data).replace('__BTNS__', btns)
    tmpl = tmpl.replace('__N__', str(len(cards)))
    open(OUT_HTML, 'w', encoding='utf-8', newline='\n').write(tmpl)


_HTML = r"""<!doctype html><html lang=he dir=rtl><head><meta charset=utf-8>
<title>MAPV2 — מדגם ביקורת עיוור (100)</title><style>
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
.work{font-size:20px;color:#ffd479;margin:0 0 4px}.meta{color:#9aa;font-size:14px;margin-bottom:4px}
.nli{color:#8fce8f}.bib{color:#c9a227;font-size:13px;white-space:pre-wrap;margin:6px 0}
.panes{display:flex;gap:14px;flex-wrap:wrap;margin-top:12px}
.pane{flex:1 1 440px;background:#15171c;border:1px solid #2a2d36;border-radius:9px;padding:12px}
.lbl{color:#89a;font-size:13px;margin-bottom:6px}.ev{font-size:17px;white-space:pre-wrap}
mark{background:#5a4a00;color:#ffe9a6;padding:0 2px;border-radius:3px}
textarea{width:100%;background:#15171c;color:#e8e8ea;border:1px solid #2a2d36;
border-radius:7px;padding:8px;margin-top:10px;font-family:inherit;font-size:14px}
#done{color:#7bd88f}#gr{color:#ffd479}a{color:#9ecbff}
.tag{display:inline-block;background:#2a2d36;border-radius:6px;padding:1px 8px;
font-size:12px;color:#bbb;margin-inline-start:6px}</style></head><body>
<div id=bar>
 <b>מדגם ביקורת עיוור</b>
 <span>כרטיס <span id=cur>1</span>/__N__</span>
 <span>· דורגו <span id=done>0</span></span>
 <span>· נוכחי: <span id=gr>—</span></span>
 <span class=sp></span>
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
const CARDS=__DATA__;const KEY='seed029_mapv2_validation100_v1';
let store=JSON.parse(localStorage.getItem(KEY)||'{}');let i=0;
const GK={'1':'discovery','2':'witness','3':'citation','4':'shared','5':'known','6':'formula','7':'norel','8':'tsarich'};
const HE={discovery:'תגלית',witness:'עד נוסח',citation:'ציטוט',shared:'מקור משותף',known:'ידוע',formula:'פורמולה בלבד',norel:'לא קשור',tsarich:'צ"ע'};
function render(){const c=CARDS[i];const rec=store[c.no]||{};
 document.getElementById('cur').textContent=c.no;
 document.getElementById('done').textContent=Object.keys(store).filter(k=>store[k].grade).length;
 document.getElementById('gr').textContent=rec.grade?HE[rec.grade]:'—';
 let bib=c.bib?`<div class=bib>${c.bib.replace(/</g,'&lt;')}</div>`:'';
 let mes=c.mesirah?`<div style="color:#7bd88f;font-weight:600;margin:4px 0">🔖 מסירת מאגרים ידועה לחיבור זה: ${c.mesirah} (עד ידוע — לרוב ״ידוע״)</div>`:'';
 document.getElementById('card').innerHTML=
  `<div class=work>${c.work||'—'}</div>`+mes+
  `<div class=meta>${c.genre||''} `+(c.nli?`<span class=nli>· קטלוג NLI: ${c.nli}</span>`:'')+
  ` <a href="${c.url}" target=_blank>פתח בגניזה↗</a></div>`+bib+
  `<div class=panes><div class=pane><div class=lbl>הדף (גניזה):</div><div class=ev>${c.page_html||''}</div></div>`+
  `<div class=pane><div class=lbl>המקבילה במהדורה:</div><div class=ev>${c.ref_html||'(לא אותרה לתצוגה)'}</div></div></div>`;
 document.getElementById('note').value=rec.note||'';}
function grade(g){const c=CARDS[i];store[c.no]=Object.assign(store[c.no]||{},{grade:g});save();render();setTimeout(next_,120);}
function noteSave(){const c=CARDS[i];store[c.no]=Object.assign(store[c.no]||{},{note:document.getElementById('note').value});save();}
function save(){localStorage.setItem(KEY,JSON.stringify(store));}
function next_(){if(i<CARDS.length-1){i++;render();}}
function prev(){if(i>0){i--;render();}}
function nextUn(){for(let j=1;j<=CARDS.length;j++){let k=(i+j)%CARDS.length;if(!(store[CARDS[k].no]||{}).grade){i=k;render();return;}}}
function exportG(){const out=CARDS.map(c=>({no:c.no,work:c.work,grade:(store[c.no]||{}).grade||null,note:(store[c.no]||{}).note||null}));
 const b=new Blob([JSON.stringify(out,null,1)],{type:'application/json'});
 const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='mapv2_validation_100_human.json';a.click();}
document.getElementById('note').addEventListener('blur',noteSave);
document.addEventListener('keydown',e=>{if(['TEXTAREA','INPUT'].includes(e.target.tagName))return;
 if(GK[e.key]){grade(GK[e.key]);e.preventDefault();}
 else if(e.key==='ArrowLeft'){next_();}else if(e.key==='ArrowRight'){prev();}});
try{render();}catch(err){document.getElementById('card').innerHTML=
 "<pre style='color:#f88;white-space:pre-wrap'>render error: "+err.message+"</pre>";}
</script></body></html>"""


if __name__ == '__main__':
    main()
