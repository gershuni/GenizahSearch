# -*- coding: utf-8 -*-
"""Deck of the owner-vs-gate disagreements from the divergence-adjudication
gold eval: every case where the gate's verdict contradicts the owner's grade.

Open the HTML, rule each card (does the gate have a point, or does the grade
stand?), Export JSON. Not blind -- the owner asked to SEE the disagreement,
so both sides are shown openly.

Run:
    python -X utf8 scripts/build_divergence_disagreement_deck.py
"""
from __future__ import annotations

import html
import json
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.divergence_adjudication_gate import (  # noqa: E402
    AidsIndex, GENUINE_VERDICTS, build_case, pair_stats,
    DEFAULT_REVIEW_DB,
)
from scripts.discovery_gate1_evidence import (  # noqa: E402
    DEFAULT_FJMS_DB, DEFAULT_LIBRARIES_CSV, DEFAULT_PGP_DB,
)

EVAL_PATH = os.path.join(REPO_ROOT, "_tmp", "divergence_gate",
                         "eval_gemini-3_7-flash.jsonl")
OUT_PATH = os.path.join(REPO_ROOT, "_tmp", "divergence_gate",
                        "disagreement_deck.html")

VERDICT_HE = {
    "catalogue_right_match_is_quotation": "הקטלוג צודק — ההתאמה היא ציטוט/טקסט משותף",
    "catalogue_right_claim_mistaken": "הקטלוג צודק — הזיהוי המחושב שגוי",
    "both_right_multiple_works": "שניהם צודקים — הדף נושא כמה חיבורים",
    "catalogue_too_general": "הקטלוג כללי מדי — הזיהוי המחושב ספציפי ותואם",
    "computed_right_catalogue_mismatch": "הזיהוי המחושב צודק — הקטלוג אינו תואם את הדף",
}

CSS = """
body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f2f0ea;color:#222}
.top{position:sticky;top:0;background:#28323e;color:#fff;padding:10px 16px;z-index:5;
     display:flex;gap:16px;align-items:center;flex-wrap:wrap}
.top button{padding:6px 14px;border:0;border-radius:6px;cursor:pointer;font-weight:600}
.card{max-width:1180px;margin:18px auto;background:#fff;border-radius:10px;
      box-shadow:0 1px 4px rgba(0,0,0,.12);padding:16px 20px}
.hdr{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:baseline}
.shelf{font-size:1.05em;font-weight:700}
.chip{display:inline-block;padding:2px 10px;border-radius:12px;font-size:.85em;margin:2px}
.gold{background:#e8f0d8;border:1px solid #9ab86a}
.gate{background:#dde8f5;border:1px solid #7aa0cc}
.danger{background:#f7dede;border:1px solid #cc7a7a}
.meta{color:#555;font-size:.9em;margin:6px 0}
.cat{background:#faf7ef;border:1px solid #e0d8c2;border-radius:8px;padding:8px 12px;
     margin:8px 0;direction:rtl;text-align:right}
.cat .tags{font-weight:600}
details{margin:4px 0}summary{cursor:pointer;color:#666;font-size:.88em}
.claim{background:#eef3fa;border:1px solid #c8d6ea;border-radius:8px;padding:8px 12px;
       margin:8px 0;direction:rtl;text-align:right}
.panes{display:flex;gap:12px;margin-top:10px}
.pane{flex:1;border:1px solid #ddd;border-radius:8px;padding:8px 12px;direction:rtl;
      text-align:right;font-size:1.02em;line-height:1.7;background:#fffdf8;min-width:0}
.pane h4{margin:0 0 6px;font-size:.85em;color:#777;direction:ltr;text-align:left}
.pane .m{background:#fff3b8;padding:0 2px;border-radius:3px}
.pane .f{color:#999}
.reason{margin:10px 0;padding:8px 12px;background:#f0edf7;border:1px solid #cdc4e4;
        border-radius:8px;font-size:.93em}
.rule{margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.rule button{padding:7px 14px;border:1px solid #bbb;border-radius:8px;background:#fafafa;
             cursor:pointer;font-size:.92em}
.rule button.sel{background:#28323e;color:#fff;border-color:#28323e}
.rule input{flex:1;min-width:220px;padding:7px 10px;border:1px solid #ccc;border-radius:8px}
.count{font-weight:700}
@media(max-width:800px){.panes{flex-direction:column}}
"""

JS = """
const KEY='divergence_disagreement_rulings_v1';
let R={};
try{R=JSON.parse(localStorage.getItem(KEY)||'{}')}catch(e){R={}}
function paint(){
  document.querySelectorAll('.card').forEach(c=>{
    const id=c.dataset.id, r=R[id]||{};
    c.querySelectorAll('.rule button[data-v]').forEach(b=>
      b.classList.toggle('sel', b.dataset.v===r.ruling));
    const inp=c.querySelector('.rule input'); if(inp && r.comment!==undefined && inp.value!==r.comment) inp.value=r.comment;
  });
  const done=Object.values(R).filter(x=>x.ruling).length;
  document.getElementById('done').textContent=done;
}
function setR(id,v){R[id]=R[id]||{};R[id].ruling=v;save()}
function setC(id,v){R[id]=R[id]||{};R[id].comment=v;save()}
function save(){try{localStorage.setItem(KEY,JSON.stringify(R))}catch(e){};paint()}
function exportJSON(){
  const blob=new Blob([JSON.stringify({deck:'divergence_disagreements_v1',rulings:R},null,1)],
    {type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='divergence_disagreement_rulings.json';a.click();
}
function importJSON(ev){
  const f=ev.target.files[0]; if(!f)return;
  f.text().then(t=>{const d=JSON.parse(t);R=d.rulings||d;save()});
}
document.addEventListener('DOMContentLoaded',paint);
"""


def esc(s):
    return html.escape(s or "", quote=False)


def pane(title, ex):
    return (f'<div class="pane"><h4>{esc(title)}</h4>'
            f'<span class="f">{esc(ex["before"])}</span> '
            f'<span class="m">{esc(ex["match"])}</span> '
            f'<span class="f">{esc(ex["after"])}</span></div>')


def main() -> int:
    recs = [json.loads(l) for l in open(EVAL_PATH, encoding="utf-8") if l.strip()]
    dis = []
    for r in recs:
        endorsed = r["verdict"] in GENUINE_VERDICTS
        if r["verdict"] == "not_checked":
            continue
        if (r["gold"] == "genuine") != endorsed:
            r["danger"] = (r["gold"] == "not_witness"
                           and r["verdict"] == "computed_right_catalogue_mismatch")
            dis.append(r)
    # dangerous first, then other false endorsements, then missed witnesses
    dis.sort(key=lambda r: (not r["danger"], r["gold"] == "genuine"))
    print(f"{len(dis)} disagreements")

    con = sqlite3.connect(f"file:{DEFAULT_REVIEW_DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    aids = AidsIndex(DEFAULT_LIBRARIES_CSV, DEFAULT_FJMS_DB, DEFAULT_PGP_DB)

    cards = []
    for i, r in enumerate(dis, 1):
        case = build_case(con, aids, r["page_id"], r["work_id"],
                          pair_stats(con, r["page_id"], r["work_id"]))
        if case is None:
            continue
        ms, cat, ci = case["manuscript"], case["catalogue"], case["computed_identification"]
        cid = f'{r["page_id"]}|{r["work_id"]}'
        gate_cls = "danger" if r["danger"] else "gate"
        bib = "".join(f"<li>{esc(b)}</li>" for b in cat["bibliography"])
        bib_html = (f'<details><summary>ביבליוגרפיה ({len(cat["bibliography"])})</summary>'
                    f'<ul>{bib}</ul></details>') if bib else ""
        pgp_html = (f'<details><summary>PGP</summary><div>{esc(cat["pgp_description"])}'
                    f'</div></details>') if cat["pgp_description"] else ""
        panes = "".join(
            f'<div class="panes">{pane("Manuscript page (transcription)", s["manuscript_side"])}'
            f'{pane("Computed work (edition text)", s["work_side"])}</div>'
            for s in case["text_samples"])
        cards.append(f"""
<div class="card" data-id="{esc(cid)}">
 <div class="hdr">
  <span class="shelf">#{i} — {esc(ms['shelfmark'])} <span class="meta">({esc(ms['library'])})</span></span>
  <span>
   <span class="chip gold">הציון שלי: {esc(', '.join(r['grades']))}</span>
   <span class="chip {gate_cls}">Gemini: {esc(VERDICT_HE.get(r['verdict'], r['verdict']))}</span>
  </span>
 </div>
 <div class="cat"><div class="tags">קטלוג: {esc(cat['structured_tags'])}</div>
  <details><summary>טקסט קטלוגי מלא</summary><div>{esc(cat['catalogue_text'])}</div></details>
  {bib_html}{pgp_html}</div>
 <div class="claim">זיהוי מחושב: <b>{esc(ci['work_title'])}</b>
  {('— ' + esc(ci['work_author'])) if ci['work_author'] else ''}
  <span class="meta">({esc(ci['domain'])})</span><br>
  מקטעים: {esc(', '.join(ci['matched_sections']))} ·
  כיסוי עמוד: {ci['page_coverage_percent']}% ·
  אותיות תואמות: {ci['matched_letters']} ·
  דפים תואמים בכה"י: {ci['n_matched_pages_in_manuscript']}</div>
 {panes}
 <div class="reason"><b>נימוק Gemini:</b> {esc(r['reason'] or '')}</div>
 <div class="rule">
  <button data-v="gemini_right" onclick="setR('{esc(cid)}','gemini_right')">Gemini צודק</button>
  <button data-v="my_grade_stands" onclick="setR('{esc(cid)}','my_grade_stands')">הציון שלי עומד</button>
  <button data-v="both_defensible" onclick="setR('{esc(cid)}','both_defensible')">שניהם סבירים</button>
  <button data-v="unsure" onclick="setR('{esc(cid)}','unsure')">לא בטוח</button>
  <input placeholder="הערה (רשות)" oninput="setC('{esc(cid)}',this.value)" dir="rtl">
 </div>
</div>""")

    doc = f"""<!doctype html><html lang="he"><head><meta charset="utf-8">
<title>Divergence gate — disagreements ({len(cards)})</title>
<style>{CSS}</style><script>{JS}</script></head><body>
<div class="top">
 <b>אי-הסכמות: הציונים שלי מול Gemini</b>
 <span>נשפטו: <span id="done" class="count">0</span>/{len(cards)}</span>
 <button onclick="exportJSON()">Export JSON</button>
 <label style="cursor:pointer"><u>Import</u><input type="file" style="display:none"
   onchange="importJSON(event)"></label>
 <span style="font-size:.85em;opacity:.8">4 הראשונים: Gemini טוען שהקטלוג טועה — המסוכנים ביותר</span>
</div>
{''.join(cards)}
</body></html>"""

    # masking scan BEFORE the write (house rule for generated review artifacts)
    from scripts.check_atlas_masking import build_matcher, load_patterns
    matcher = build_matcher(load_patterns())
    issues = matcher.scan(doc.encode("utf-8"), "disagreement_deck.html")
    if issues:
        raise SystemExit(f"MASKING VIOLATION: {len(issues)} hit(s); nothing written")

    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        fh.write(doc)
    print(f"wrote {OUT_PATH} ({len(cards)} cards)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
