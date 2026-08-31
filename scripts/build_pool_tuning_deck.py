# -*- coding: utf-8 -*-
"""Blind owner-grading deck for tuning the v5 review MAIN POOL bar.

WHAT IS BEING TUNED. The pools card sorts witness-verdict rows into the main
pool at coverage >= 60%. Against the owner's 1,398 E1-L gold pairs the
coverage-only rule measures: 29.8% line -> 78.9% precision, 60% -> 92.2%,
70% -> 94.8% -- but that gold set is gen-1-era M/J pairs. This deck samples
the CURRENT artifact's own boundary region so the bar is tuned on what the
pool actually holds (R-source included).

SAMPLE. Witness-verdict, unflagged rows, stratified by coverage band
(29.8-40 / 40-50 / 50-60 / 60-70 / 70-85 / 85-100, CARDS_PER_BAND each),
corpus-proportional inside each band, seeded RNG -- rebuildable bit-for-bit.

BLINDNESS (the E1-L deck's rule): the card shows NO system decision -- no
coverage number, no pool, no router verdict, no scripture flag. Only the two
texts (matched span highlighted), the claimed work's title, the locus, the
catalogue line, and the match's letter count. Band/coverage per card live in a
SEPARATE manifest the deck never embeds.

VOCAB: the owner's 10-way directional set (2026-07-24), unchanged for
comparability: correct / cowitness / partial / quote_ab / quote_ba /
quote_shared / formula / wrong / unsure / junk, plus a comment. Keyboard
1-9,0; arrows navigate; localStorage; Export grades JSON.

Run (server may keep running -- read-only):
    python -X utf8 scripts/build_pool_tuning_deck.py
Outputs:
    _tmp/pool_tuning_deck.html          (the deck -- open in a browser)
    _tmp/pool_tuning_manifest.json      (evidence_id -> band/coverage/corpus;
                                         NEVER send with the deck)
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import random
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SEED = 20260830
CARDS_PER_BAND = 40
BANDS = ((298000, 400000, "29.8-40"), (400000, 500000, "40-50"),
         (500000, 600000, "50-60"), (600000, 700000, "60-70"),
         (700000, 850000, "70-85"), (850000, 1000001, "85-100"))

GRADES = (("1", "correct"), ("2", "cowitness"), ("3", "partial"),
          ("4", "quote_ab"), ("5", "quote_ba"), ("6", "quote_shared"),
          ("7", "formula"), ("8", "wrong"), ("9", "unsure"), ("0", "junk"))


def pick(con, rng):
    cards, manifest = [], {}
    for lo, hi, label in BANDS:
        rows = con.execute(
            """SELECT r.evidence_id, r.work_title, r.locus_label,
                      r.catalogue_title, r.matched_letters, r.coverage_ppm,
                      r.source_corpus, r.ms_before, r.ms_match, r.ms_after,
                      r.ref_before, r.ref_match, r.ref_after
               FROM review_row r
               LEFT JOIN scripture_fact sf ON sf.evidence_id = r.evidence_id
               WHERE r.router_verdict='same_work'
                 AND COALESCE(sf.flagged, 0) = 0
                 AND r.coverage_ppm >= ? AND r.coverage_ppm < ?""",
            (lo, hi)).fetchall()
        if not rows:
            continue
        take = rng.sample(rows, min(CARDS_PER_BAND, len(rows)))
        for r in take:
            cards.append(r)
            manifest[r["evidence_id"]] = {
                "band": label, "coverage_ppm": r["coverage_ppm"],
                "corpus": r["source_corpus"]}
    # order: content-stable, decision-blind (the E1-L rule)
    cards.sort(key=lambda r: hashlib.sha1(
        r["evidence_id"].encode()).hexdigest())
    return cards, manifest


def render(cards):
    e = html.escape
    body = []
    for i, r in enumerate(cards):
        ms = ('<span class="ctx">%s</span><mark>%s</mark><span class="ctx">%s</span>'
              % (e(r["ms_before"] or ""), e(r["ms_match"] or ""),
                 e(r["ms_after"] or "")))
        rf = ('<span class="ctx">%s</span><mark>%s</mark><span class="ctx">%s</span>'
              % (e(r["ref_before"] or ""), e(r["ref_match"] or ""),
                 e(r["ref_after"] or "")))
        body.append(f"""
<div class="card" id="c{i}" data-ev="{e(r['evidence_id'])}">
  <div class="head">
    <span class="n">{i + 1}</span>
    <span class="title" dir="rtl">{e(r['work_title'] or '')}</span>
    <span class="locus" dir="rtl">{e(r['locus_label'] or '')}</span>
    <span class="meta">{r['matched_letters']} letters</span>
  </div>
  <div class="cat" dir="rtl">{e(r['catalogue_title'] or '')}</div>
  <div class="panes">
    <div class="pane"><h4>A — the manuscript page</h4>
      <div class="txt" dir="rtl">{ms}</div></div>
    <div class="pane"><h4>B — the claimed work</h4>
      <div class="txt" dir="rtl">{rf}</div></div>
  </div>
  <div class="grades"></div>
  <input class="comment" placeholder="comment (optional, saved as you type)">
</div>""")
    grades_js = json.dumps(GRADES, ensure_ascii=False)
    n = len(cards)
    return """<!doctype html><html><head><meta charset="utf-8">
<title>Main-pool tuning deck — does page A witness work B?</title>
<style>
 body{background:#14181f;color:#dde3ec;font-family:Segoe UI,sans-serif;margin:0;padding:16px}
 .bar{position:sticky;top:0;background:#14181f;padding:8px 0;border-bottom:1px solid #333;z-index:5;
      display:flex;gap:12px;align-items:center}
 .card{border:1px solid #2c333e;border-radius:10px;margin:14px 0;padding:12px;max-width:1200px}
 .card.done{border-color:#3f6f4f}
 .head{display:flex;gap:12px;align-items:baseline;flex-wrap:wrap}
 .n{color:#8b94a3}.title{font-weight:700;font-size:1.1em}.locus{color:#9fb4d0}
 .meta{color:#8b94a3;font-size:.85em}.cat{color:#8b94a3;font-size:.9em;margin:2px 0 8px}
 .panes{display:grid;grid-template-columns:1fr 1fr;gap:10px}
 .pane h4{margin:4px 0;color:#9fb4d0;font-size:.85em}
 .txt{background:#1b2129;border-radius:8px;padding:10px;line-height:1.8;max-height:340px;overflow:auto}
 .ctx{color:#7f8896} mark{background:#7a5b2333;color:#ffd9a0;padding:0 1px}
 .grades{margin:10px 0 4px;display:flex;gap:6px;flex-wrap:wrap}
 .grades button{background:#232a35;color:#dde3ec;border:1px solid #3a4250;border-radius:14px;
      padding:4px 10px;cursor:pointer}
 .grades button.on{background:#2f6f4f;border-color:#4f9f6f}
 .comment{width:100%%;background:#1b2129;color:#dde3ec;border:1px solid #2c333e;border-radius:8px;
      padding:6px;box-sizing:border-box}
 .prog{color:#9fb4d0}
 button.export{background:#2b4a6f;color:#fff;border:0;border-radius:8px;padding:6px 14px;cursor:pointer}
</style></head><body>
<div class="bar"><strong>Does page A witness the claimed work B?</strong>
 <span class="prog" id="prog"></span>
 <button class="export" onclick="exportGrades()">Export grades JSON</button>
 <button class="export" onclick="document.getElementById('imp').click()">Import</button>
 <input type="file" id="imp" accept=".json" style="display:none">
 <span style="color:#8b94a3;font-size:.85em">keys 1–9,0 grade · ↑/↓ move · EXPORT WHEN PAUSING —
   a file:// page can lose its local save</span>
</div>
%s
<script>
const GRADES = %s;
const KEY = "pool_tuning_deck_v1";
let store = {};
try { store = JSON.parse(localStorage.getItem(KEY) || "{}"); } catch(e) {}
const cards = [...document.querySelectorAll(".card")];
let cur = 0;
function save(){ try { localStorage.setItem(KEY, JSON.stringify(store)); } catch(e){}
  const done = cards.filter(c => (store[c.dataset.ev]||{}).grade).length;
  document.getElementById("prog").textContent = done + " / %d graded"; }
cards.forEach(c => {
  const ev = c.dataset.ev, g = c.querySelector(".grades");
  GRADES.forEach(([k, name]) => {
    const b = document.createElement("button");
    b.textContent = k + " " + name;
    b.onclick = () => { store[ev] = Object.assign(store[ev]||{}, {grade: name});
      [...g.children].forEach(x => x.classList.remove("on")); b.classList.add("on");
      c.classList.add("done"); save(); };
    g.appendChild(b);
  });
  const cm = c.querySelector(".comment");
  cm.oninput = () => { store[ev] = Object.assign(store[ev]||{}, {comment: cm.value}); save(); };
  const st = store[ev] || {};
  if (st.grade) { c.classList.add("done");
    [...g.children].forEach(x => { if (x.textContent.slice(2) === st.grade) x.classList.add("on"); }); }
  if (st.comment) cm.value = st.comment;
});
document.addEventListener("keydown", ev => {
  if (ev.target.tagName === "INPUT") return;
  if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
    cur = Math.max(0, Math.min(cards.length - 1, cur + (ev.key === "ArrowDown" ? 1 : -1)));
    cards[cur].scrollIntoView({block: "center"}); ev.preventDefault(); return;
  }
  const hit = GRADES.find(([k]) => k === ev.key);
  if (hit) { const btns = cards[cur].querySelectorAll(".grades button");
    [...btns].find(b => b.textContent.slice(2) === hit[1]).click(); }
});
document.getElementById("imp").onchange = ev => {
  const f = ev.target.files[0]; if (!f) return;
  f.text().then(t => {
    JSON.parse(t).forEach(r => { if (r.grade || r.comment)
      store[r.id] = {grade: r.grade || undefined, comment: r.comment || ""}; });
    save(); location.reload();
  });
};
function exportGrades(){
  const out = cards.map(c => ({id: c.dataset.ev,
    grade: (store[c.dataset.ev]||{}).grade || null,
    comment: (store[c.dataset.ev]||{}).comment || ""}));
  const blob = new Blob([JSON.stringify(out, null, 1)], {type: "application/json"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob); a.download = "pool_tuning_grades.json"; a.click();
}
save();
</script></body></html>""" % ("\n".join(body), grades_js, n)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--out", default=os.path.join(
        REPO_ROOT, "_tmp", "pool_tuning_deck.html"))
    ap.add_argument("--manifest", default=os.path.join(
        REPO_ROOT, "_tmp", "pool_tuning_manifest.json"))
    ap.add_argument("--stratum", default=None,
                    help="build a SINGLE-stratum deck instead of the banded "
                         "sample: 'corpus,min_ppm,max_ppm,n' (e.g. "
                         "'rsource,750000,1000001,40'). Ruled-out works "
                         "(dropped/excluded owner rulings) are skipped. "
                         "Writes its own manifest (pass --manifest).")
    ap.add_argument("--bands", default=None,
                    help="comma-separated band labels (e.g. '70-85,85-100') "
                         "to emit a SUB-deck of the same sample: identical "
                         "card ids and the same local save, so grades entered "
                         "in either deck count for both. The manifest is not "
                         "rewritten for a sub-deck.")
    args = ap.parse_args(argv)

    con = sqlite3.connect("file:%s?mode=ro" % args.db, uri=True)
    con.row_factory = sqlite3.Row
    rng = random.Random(SEED)
    if args.stratum:
        corpus, lo, hi, n_take = args.stratum.split(",")
        rows = con.execute(
            """SELECT r.evidence_id, r.work_title, r.locus_label,
                      r.catalogue_title, r.matched_letters, r.coverage_ppm,
                      r.source_corpus, r.ms_before, r.ms_match, r.ms_after,
                      r.ref_before, r.ref_match, r.ref_after
               FROM review_row r
               LEFT JOIN scripture_fact sf ON sf.evidence_id = r.evidence_id
               WHERE r.router_verdict='same_work'
                 AND COALESCE(sf.flagged, 0) = 0
                 AND r.source_corpus = ?
                 AND r.coverage_ppm >= ? AND r.coverage_ppm < ?
                 AND COALESCE(r.owner_ruling,'') NOT IN
                     ('dropped_as_identification_reference',
                      'excluded_from_public_identities')""",
            (corpus, int(lo), int(hi))).fetchall()
        cards = rng.sample(rows, min(int(n_take), len(rows)))
        cards.sort(key=lambda r: hashlib.sha1(
            r["evidence_id"].encode()).hexdigest())
        manifest = {r["evidence_id"]: {
            "band": "%s-%s" % (lo, hi), "coverage_ppm": r["coverage_ppm"],
            "corpus": r["source_corpus"]} for r in cards}
        con.close()
        html_text = render(cards)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(html_text)
        with open(args.manifest, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=1)
        print("deck      : %d cards -> %s" % (len(cards), args.out))
        print("manifest  : %s (do NOT send with the deck)" % args.manifest)
        return 0
    cards, manifest = pick(con, rng)
    con.close()
    if args.bands:
        want = {b.strip() for b in args.bands.split(",")}
        bad = want - {b[2] for b in BANDS}
        if bad:
            raise SystemExit("unknown bands: %s" % sorted(bad))
        keep = {ev for ev, m in manifest.items() if m["band"] in want}
        cards = [c for c in cards if c["evidence_id"] in keep]
        manifest = {ev: m for ev, m in manifest.items() if ev in keep}
    html_text = render(cards)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_text)
    if not args.bands:            # a sub-deck never rewrites the manifest
        with open(args.manifest, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=1)
    per_band = {}
    for v in manifest.values():
        per_band[v["band"]] = per_band.get(v["band"], 0) + 1
    print("deck      : %d cards -> %s" % (len(cards), args.out))
    print("manifest  : %s (do NOT send with the deck)" % args.manifest)
    print("per band  : %s" % per_band)
    return 0


if __name__ == "__main__":
    sys.exit(main())
