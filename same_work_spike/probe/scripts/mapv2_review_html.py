# -*- coding: utf-8 -*-
"""MAPV2-13 — dark-mode interactive human-review page for deck v13.

One card at a time (track1_fullv2_id_review.html conventions): sticky header
with progress / section filter / next-ungraded / export, text-vs-text RTL
panes with the matched span highlighted, full card info (NLI title, FJMS
identifications, Friedberg bibliography, Opus verdict + reasoning), grading
buttons in Hillel's vocabulary (keys 1-8 grade & advance) and a free-text
note per card. Grades + notes persist in localStorage and export as JSON.

Inputs: review/full_deck/mapv2_deck_cards_enriched.json (card data incl.
snippets) + results/deck_annotation_v13/merged_annotations.json (Opus).
Output: review/full_deck/mapv2_v13_human_review.html

Usage: python -X utf8 -u mapv2_review_html.py [--tag _v13]
"""
import argparse
import json
import os

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
_ap = argparse.ArgumentParser()
_ap.add_argument('--tag', default='_v13')
_args = _ap.parse_args()
CARDS = os.path.join(PROBE, 'review', 'full_deck',
                     'mapv2_deck_cards_enriched.json')
MERGED = os.path.join(PROBE, 'results', 'deck_annotation' + _args.tag,
                      'merged_annotations.json')
CRITIC = os.path.join(PROBE, 'review', 'full_deck',
                      f'mapv2{_args.tag}_critic_grades.json')
OUT = os.path.join(PROBE, 'review', 'full_deck',
                   f'mapv2{_args.tag}_human_review.html')

VHE = {'DISCOVERY': 'תגלית', 'WITNESS': 'עד נוסח', 'CITATION': 'ציטוט',
       'PARALLEL': 'מקבילה', 'KNOWN-SAME': 'ידוע (קטלוג/ביב\')',
       'KNOWN-DEPENDENCE': 'תלות ידועה', 'SHARED-SOURCE': 'מקור משותף',
       'NO-RELATION': 'אין קשר'}
VCOL = {'DISCOVERY': '#2c7d32', 'WITNESS': '#1565c0', 'CITATION': '#ef6c00',
        'PARALLEL': '#00838f', 'KNOWN-SAME': '#616161',
        'KNOWN-DEPENDENCE': '#6d4c41', 'SHARED-SOURCE': '#c62828',
        'NO-RELATION': '#455a64'}


def main():
    cards = json.load(open(CARDS, encoding='utf-8'))
    ann = {}
    if os.path.exists(MERGED):
        ann = {c['card_no']: c.get('annotation') for c in
               json.load(open(MERGED, encoding='utf-8'))}
    critic = {}
    if os.path.exists(CRITIC):
        critic = {c['card_no']: c for c in
                  json.load(open(CRITIC, encoding='utf-8'))}
    data = []
    for c in cards:
        a = ann.get(c['card_no']) or {}
        cr = critic.get(c['card_no']) or {}
        data.append({
            'no': c['card_no'],
            'section': c['section'].split(' — ')[0],
            'shelf': c['shelfmark'], 'lib': c['library'],
            'url': c['url'], 'nli': c.get('nli_title') or '',
            'work': c['work_name'], 'cat': c['cat'],
            'p': c['p_shown'], 'band': c['band'],
            'alen': c['alen'], 'dens': c['dens'],
            'nwit': c['n_work_witnesses_tierA'],
            'npgs': c['n_pages_this_ms'],
            'flank': c['flank_class'], 'prov': c['text_provenance'],
            'page_txt': c.get('page_snippet') or '',
            'ref_txt': c.get('ref_snippet') or '',
            'fjms': [f"{i.get('work') or i.get('unit_title') or ''}"
                     f"{' · ' + i['author'] if i.get('author') else ''}"
                     f" [{i.get('identified_by', '?')}]"
                     for i in c.get('fjms_catalog_identifications', [])],
            'bib': c.get('friedberg_bibliography', []),
            'ov': a.get('verdict') or '',
            'oconf': a.get('confidence') or '',
            'onov': bool(a.get('novelty')),
            'oreason': a.get('reasoning_he') or '',
            'otr': a.get('title_relation') or '',
            'oeq': a.get('name_equation') or '',
            'cg': cr.get('grade') or '',
            'cnote': cr.get('note') or '',
            'cesc': bool(cr.get('escalate')),
        })
    n_ann = sum(1 for d in data if d['ov'])
    n_cr = sum(1 for d in data if d['cg'])
    if n_cr:
        print(f"critic layer: {n_cr} cards")
    sections = []
    for d in data:
        if d['section'] not in sections:
            sections.append(d['section'])

    doc = HTML.replace('__DATA__', json.dumps(data, ensure_ascii=False)) \
              .replace('__VHE__', json.dumps(VHE, ensure_ascii=False)) \
              .replace('__VCOL__', json.dumps(VCOL, ensure_ascii=False)) \
              .replace('__TAG__', _args.tag)
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT} ({len(data)} cards, {n_ann} with Opus annotation)")


HTML = """<!DOCTYPE html>
<html lang="he">
<head>
<meta charset="utf-8">
<title>MAPV2 v13 — ביקורת אנושית</title>
<style>
 body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#17181c;color:#d6d6d6}
 header{position:sticky;top:0;background:#20262b;color:#eee;padding:8px 16px;
        display:flex;gap:14px;align-items:center;flex-wrap:wrap;z-index:5;
        border-bottom:1px solid #33393f}
 header .prog{font-weight:600}
 header select,header button{padding:4px 8px;border-radius:6px;border:1px solid #444;
        background:#2e3138;color:#ddd;cursor:pointer}
 .card{max-width:1250px;margin:14px auto;background:#23252c;border-radius:10px;
       box-shadow:0 1px 6px rgba(0,0,0,.5);padding:14px 18px}
 .meta{display:flex;gap:12px;flex-wrap:wrap;font-size:14px;color:#aaa;
       margin-bottom:8px;align-items:center;direction:rtl}
 .meta b{color:#fff}
 .badge{background:#3a3d46;color:#ddd;border-radius:10px;padding:1px 9px;font-size:12px}
 .opus{border-radius:8px;padding:6px 12px;margin:8px 0;font-size:14px;
       direction:rtl;text-align:right;background:#1d1f25;border:1.5px solid #555}
 .opus b.v{font-size:15px}
 .cols{display:flex;gap:14px;flex-wrap:wrap}
 .pane{flex:1 1 460px;border:1px solid #3a3d46;border-radius:8px;padding:10px;
       background:#1d1f25;min-width:0}
 .pane h4{margin:0 0 6px;font-size:13px;color:#9aa4ac;direction:rtl;text-align:right;
       font-weight:normal}
 .pane h4 b{color:#e8e6df}
 .txt{direction:rtl;text-align:right;font-size:17px;line-height:1.75;
      white-space:pre-wrap;word-break:break-word;max-height:340px;overflow-y:auto;
      color:#e8e6df}
 .txt mark{background:#6b5407;color:#ffe082;padding:0 1px}
 .grades{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;direction:rtl}
 .grades button{padding:8px 12px;border-radius:8px;border:1px solid #4a4d55;
      background:#2e3138;color:#ddd;cursor:pointer;font-size:14px}
 .grades button.sel{background:#2c7d32;color:#fff;border-color:#2c7d32}
 textarea{width:100%;box-sizing:border-box;margin-top:10px;background:#1d1f25;
      color:#e8e6df;border:1px solid #3a3d46;border-radius:8px;padding:8px;
      font-family:inherit;font-size:14px;direction:rtl;min-height:60px}
 .nav{display:flex;gap:10px;margin-top:10px}
 .nav button{padding:6px 14px;border-radius:8px;border:1px solid #555;
      cursor:pointer;background:#2e3138;color:#ddd}
 a{color:#6fb3e8}
 .hint{font-size:12px;color:#8a8a8a;margin-top:6px;direction:rtl;text-align:right}
 details.bib{margin-top:10px;font-size:13px;color:#b8b8b8;direction:rtl;
      text-align:right;background:#1d1f25;border:1px solid #3a3d46;
      border-radius:8px;padding:6px 10px}
 details.bib summary{cursor:pointer;color:#9ec7e8}
 details.bib ul{margin:6px 18px 2px 0;padding:0}
 .fjt{font-size:12px;color:#9aa4ac;direction:rtl;text-align:right;margin-top:8px}
</style>
</head>
<body>
<header>
 <span class="prog" id="prog"></span>
 <select id="filter"></select>
 <button onclick="jumpUngraded()">הבא שלא נבדק</button>
 <button onclick="exportGrades()">⬇ ייצוא הביקורת</button>
 <span id="counts" style="font-size:12px"></span>
</header>
<div id="app"></div>
<script>
const DATA = __DATA__;
const VHE = __VHE__;
const VCOL = __VCOL__;
const GRADES = [
 ["discovery","1 · תגלית"],
 ["witness","2 · עד נוסח"],
 ["citation","3 · ציטוט"],
 ["shared","4 · מקור משותף"],
 ["known","5 · ידוע (קטלוג/ביב')"],
 ["formula","6 · פורמולה בלבד"],
 ["norel","7 · לא קשור"],
 ["tsarich","8 · צ\\"ע"]];
const LS_KEY = "seed029_mapv2__TAG___human_review_v1";
let store = JSON.parse(localStorage.getItem(LS_KEY) || "{}");
let idx = 0, filt = "all";

function items(){ return filt==="all" ? DATA : DATA.filter(d=>d.section===filt); }
function save(){ localStorage.setItem(LS_KEY, JSON.stringify(store)); }
function rec(no){ return store[no] || (store[no] = {}); }
function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function hl(s){ return esc(s).replace(/【/g,"<mark>").replace(/】/g,"</mark>"); }
function render(){
 const list = items();
 if(!list.length){ document.getElementById("app").innerHTML =
   "<div class='card'>אין כרטיסים במסנן הזה.</div>"; return; }
 idx = Math.max(0, Math.min(idx, list.length-1));
 const d = list[idx];
 const g = store[d.no] || {};
 const nGraded = DATA.filter(x=>(store[x.no]||{}).grade).length;
 document.getElementById("prog").textContent =
   `${idx+1} / ${list.length}  (נבדקו ${nGraded} מתוך ${DATA.length})`;
 const btns = GRADES.map(([k,lab]) =>
   `<button class="${g.grade===k?'sel':''}" onclick="grade('${k}')">${lab}</button>`).join("");
 const oc = VCOL[d.ov] || "#555";
 const opus = d.ov ? `<div class="opus" style="border-color:${oc}">
    <b class="v" style="color:${oc}">Opus: ${VHE[d.ov]||d.ov}</b>
    <span style="color:#999">(${d.oconf}${d.onov?" · חדש":""}${d.otr?" · "+d.otr:""})</span>
    ${d.oeq?`<bdi> · ${esc(d.oeq)}</bdi>`:""}
    <div style="color:#b8b8b8;margin-top:3px">${esc(d.oreason)}</div></div>` : "";
 const GHE = {discovery:"תגלית",witness:"עד נוסח",citation:"ציטוט",
   shared:"מקור משותף",known:"ידוע",formula:"פורמולה בלבד",
   norel:"לא קשור",tsarich:"צ\\"ע"};
 const critic = d.cg ? `<div class="opus" style="border-color:#c9a227">
    <b class="v" style="color:#e6c34a">מבקר Fable: ${GHE[d.cg]||d.cg}</b>
    ${d.cesc?`<span style="color:#ff8a65;font-weight:bold"> · להכרעתך</span>`:""}
    <div style="color:#b8b8b8;margin-top:3px">${esc(d.cnote)}</div></div>` : "";
 document.getElementById("app").innerHTML = `
  <div class="card">
   <div class="meta">
    <span class="badge">#${d.no}</span>
    <span class="badge">${esc(d.section)}</span>
    <span>P <b>${d.p.toFixed(2)}</b></span>
    <span class="badge">${d.band}</span>
    <span>התאמה <b>${d.alen}</b> אות</span>
    <span>מרחק <b>${d.dens.toFixed(2)}</b></span>
    <span>אגפים: <b>${d.flank}</b></span>
    <span>עדים לחיבור <b>${d.nwit}</b></span>
    ${d.npgs>1?`<span>עוד <b>${d.npgs-1}</b> עמודים בכ"י</span>`:""}
    ${d.prov!=="htr"?`<span class="badge" style="background:#4a2c6b">תעתיק ${esc(d.prov)}</span>`:""}
   </div>
   ${opus}
   ${critic}
   <div class="cols">
    <div class="pane"><h4>כתב־יד: <b>${esc(d.shelf)}</b> · ${esc(d.lib)}
      · <a href="${d.url}" target="_blank">פתח ↗</a><br>
      כותרת NLI: <b>${esc(d.nli)||"—"}</b></h4>
     <div class="txt">${hl(d.page_txt)}</div></div>
    <div class="pane"><h4>המהדורה: [${esc(d.cat)}] <b>${esc(d.work)}</b></h4>
     <div class="txt">${d.ref_txt?hl(d.ref_txt):"<i style='color:#888'>(המקבילה לא אותרה לתצוגה)</i>"}</div></div>
   </div>
   ${d.fjms.length?`<div class="fjt">זיהויי FJMS: ${d.fjms.map(esc).join(" · ")}</div>`:""}
   ${d.bib.length?`<details class="bib" open><summary>ביבליוגרפיה (פרידברג)</summary>
     <ul>${d.bib.map(x=>`<li>${esc(x)}</li>`).join("")}</ul></details>`:""}
   <div class="grades">${btns}</div>
   <textarea id="note" placeholder="הערה חופשית (נשמרת אוטומטית)..."
     onblur="noteSave(this.value)">${esc(g.note||"")}</textarea>
   <div class="nav">
    <button onclick="move(-1)">→ הקודם</button>
    <button onclick="move(1)">הבא ←</button>
   </div>
   <div class="hint">מקשים: 1–8 = ציון ומעבר · ←/→ ניווט (כשלא בתיבת הטקסט).
     ימין = קטע הדף בגניזה; שמאל = המקבילה במהדורה; ההדגשה = ההתאמה.
     פסיקת ה-Opus מוצגת לנוחות — פסוק באופן בלתי־תלוי.</div>
  </div>`;
}
function grade(k){ const d=items()[idx]; rec(d.no).grade=k; save(); move(1); }
function noteSave(v){ const d=items()[idx]; rec(d.no).note=v; save(); }
function move(dd){ idx+=dd; render(); }
function jumpUngraded(){
 const list=items();
 for(let i=0;i<list.length;i++){
   const j=(idx+1+i)%list.length;
   if(!(store[list[j].no]||{}).grade){ idx=j; render(); return; } }
 alert("הכול נבדק במסנן הזה!");
}
function exportGrades(){
 const out = DATA.filter(d=>store[d.no] && (store[d.no].grade||store[d.no].note))
  .map(d=>({card_no:d.no, section:d.section, shelf:d.shelf, work:d.work,
            opus_verdict:d.ov, grade:(store[d.no]||{}).grade||null,
            note:(store[d.no]||{}).note||null}));
 const blob = new Blob([JSON.stringify(out,null,1)],{type:"application/json"});
 const a = document.createElement("a");
 a.href = URL.createObjectURL(blob);
 a.download = "mapv2__TAG___human_grades.json"; a.click();
}
document.addEventListener("keydown", e=>{
 if(e.target.tagName==="TEXTAREA"||e.target.tagName==="INPUT") return;
 if(e.key>="1" && e.key<="8"){ grade(GRADES[+e.key-1][0]); }
 else if(e.key==="ArrowLeft"){ move(1); }   /* RTL: left = forward */
 else if(e.key==="ArrowRight"){ move(-1); }
});
const sections = ["all",...new Set(DATA.map(d=>d.section))];
document.getElementById("filter").innerHTML =
 sections.map(s=>`<option value="${s}">${s==="all"?"הכול":s}
   (${s==="all"?DATA.length:DATA.filter(d=>d.section===s).length})</option>`).join("");
document.getElementById("filter").onchange = e=>{ filt=e.target.value; idx=0; render(); };
render();
</script>
</body>
</html>
"""

if __name__ == '__main__':
    main()
