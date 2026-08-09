"""Local review server over the full v3 quote-identification DB.

WHY A SERVER AND NOT A PAGE. The review set is ~194,000 rows carrying both
sides of every match; as one HTML file that is over a gigabyte and no browser
opens it. So the DB stays a DB and this serves slices of it. Stdlib only
(`http.server` + `sqlite3`) -- a teammate needs the DB, this file, and Python.

A file:// page was also rejected for a second reason: browsers discard its
localStorage without warning, and grading work would evaporate on restart. Here
every grade is written to disk immediately.

WHAT IT GRADES, and why that is the point. `divergence_correctness`
(`catalogue_correct` / `claim_correct` / `unclear`) is HUMAN-ONLY by owner
ruling L, 2026-08-03: the model scored 8/28 on it -- at or below chance for a
three-way choice -- on questions the owner answered 31/32. So the column is
empty by design in every artifact, and the only way it is ever filled is a human
looking at both sides. That is exactly what this tool is for.

GRADES LIVE IN THEIR OWN FILE (`<db>.grades.db`, ATTACHed), never in the review
DB itself, so re-baking the review projection cannot destroy grading work.

Run:
    python scripts/serve_v3_review.py --db discovery_data/discovery-v3-REVIEW.db
    # then open http://127.0.0.1:8777
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIVERGENCE_VALUES = ("catalogue_correct", "claim_correct", "unclear")
PAGE_SIZE = 25

PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>v3 quote-identification review</title>
<style>
 :root{--bg:#14161a;--fg:#e8e8ea;--dim:#9aa0a8;--card:#1c1f25;--line:#2b2f36;
       --hit:#ffd54f;--acc:#4da3ff}
 *{box-sizing:border-box}
 body{margin:0;background:var(--bg);color:var(--fg);
      font:14px/1.55 system-ui,"Segoe UI",Arial,sans-serif}
 header{position:sticky;top:0;background:#0f1114;border-bottom:1px solid var(--line);
        padding:8px 12px;z-index:5}
 .filters{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
 select,input,button{background:#22262d;color:var(--fg);border:1px solid var(--line);
        border-radius:6px;padding:6px 8px;font:inherit}
 button{cursor:pointer} button:hover{border-color:var(--acc)}
 .count{color:var(--dim);margin-inline-start:auto}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;
       margin:12px;padding:12px}
 .meta{display:flex;flex-wrap:wrap;gap:8px;align-items:center;color:var(--dim);
       font-size:12px;margin-bottom:8px}
 .badge{background:#2a2f38;border-radius:999px;padding:2px 9px;color:var(--fg)}
 .badge.nov{background:#26405c}
 .cols{display:grid;grid-template-columns:1fr 1fr;gap:12px}
 @media(max-width:900px){.cols{grid-template-columns:1fr}}
 .pane h4{margin:0 0 6px;font-size:13px;color:var(--dim);font-weight:600}
 .txt{direction:rtl;text-align:right;background:#0f1114;border:1px solid var(--line);
      border-radius:8px;padding:10px;max-height:320px;overflow:auto;
      white-space:pre-wrap;font-size:15px;line-height:1.9}
 .ctx{color:#7d838b}
 mark{background:var(--hit);color:#000;padding:0 2px;border-radius:3px}
 .grade{margin-top:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
 .grade button.sel{background:var(--acc);color:#00121f;border-color:var(--acc)}
 .stream{color:#c9a227;font-size:11px}
 .pager{display:flex;gap:8px;justify-content:center;padding:16px}
</style></head><body>
<header><div class="filters">
 <select id="domain"><option value="">domain: all</option></select>
 <select id="author"><option value="">author: all</option></select>
 <select id="work"><option value="">work: all</option></select>
 <select id="novelty"><option value="">novelty: all</option></select>
 <select id="graded"><option value="">graded: any</option>
   <option value="no">ungraded only</option><option value="yes">graded only</option></select>
 <input id="q" placeholder="shelfmark contains…" size="16">
 <button onclick="load(0)">Apply</button>
 <button onclick="exportGrades()">Export grades</button>
 <span class="count" id="count"></span>
</div></header>
<div id="app"></div>
<div class="pager">
 <button onclick="prev()">&larr; prev</button>
 <button onclick="next()">next &rarr;</button>
</div>
<script>
const DIV = ["catalogue_correct","claim_correct","unclear"];
let off = 0, total = 0;
const $ = id => document.getElementById(id);
const esc = s => (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;");

function params(extra){
  const p = new URLSearchParams();
  for (const k of ["domain","author","work","novelty","graded","q"])
    if ($(k).value) p.set(k, $(k).value);
  Object.entries(extra||{}).forEach(([k,v]) => p.set(k,v));
  return p;
}
async function facets(){
  const r = await fetch("/api/facets?" + params());
  const f = await r.json();
  for (const [id,key] of [["domain","domains"],["author","authors"],
                          ["work","works"],["novelty","novelty"]]) {
    const cur = $(id).value;
    $(id).innerHTML = `<option value="">${id}: all</option>` +
      f[key].map(([v,n]) => `<option value="${esc(v)}"${v===cur?" selected":""}>`
        + `${esc(v||"(none)")} (${n.toLocaleString()})</option>`).join("");
  }
}
function pane(title, b, m, a, isStream){
  return `<div class="pane"><h4>${title}${isStream?' <span class="stream">[unspaced letter stream]</span>':''}</h4>
    <div class="txt"><span class="ctx">${esc(b)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(a)}</span></div></div>`;
}
async function load(newOff){
  if (newOff !== undefined) off = newOff;
  const r = await fetch("/api/rows?" + params({offset: off}));
  const d = await r.json();
  total = d.total;
  $("count").textContent = `${d.total.toLocaleString()} rows · showing ${off+1}-${Math.min(off+d.rows.length,total)}`;
  $("app").innerHTML = d.rows.map(x => `
    <div class="card">
      <div class="meta">
        <span class="badge nov">${esc(x.novelty_status)}</span>
        <span class="badge">${esc(x.domain||"—")}</span>
        <span>${esc(x.library_code||"")} <b>${esc(x.shelfmark||x.sys_id)}</b></span>
        <span>work: <b>${esc(x.work_title||x.work_id)}</b>${x.work_author?" · "+esc(x.work_author):""}</span>
        <span>${x.matched_letters} letters${x.n_spans>1?` · ${x.n_spans} spans`:""}</span>
        <span class="badge">${esc(x.source_corpus||"")}</span>
      </div>
      <div class="cols">
        ${pane("Manuscript", x.ms_before, x.ms_match, x.ms_after, false)}
        ${pane("Reference edition", x.ref_before, x.ref_match, x.ref_after, x.ref_is_stream)}
      </div>
      <div class="grade">
        <span style="color:var(--dim)">Which is right?</span>
        ${DIV.map(v => `<button class="${x.grade===v?'sel':''}"
           onclick="grade('${x.evidence_id}','${v}',this)">${v.replace("_"," ")}</button>`).join("")}
        <button onclick="grade('${x.evidence_id}','',this)">clear</button>
      </div>
    </div>`).join("");
  facets();
}
async function grade(id, val, btn){
  await fetch("/api/grade", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({evidence_id:id, divergence_correctness:val})});
  const row = btn.parentElement;
  [...row.querySelectorAll("button")].forEach(b => b.classList.remove("sel"));
  if (val) btn.classList.add("sel");
}
function next(){ if (off + 25 < total) load(off + 25); }
function prev(){ if (off > 0) load(Math.max(0, off - 25)); }
function exportGrades(){ window.location = "/api/export"; }
load(0);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    db_path = None

    def log_message(self, *a):
        pass

    def _conn(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        con.execute("ATTACH DATABASE ? AS g", (self.db_path + ".grades.db",))
        con.execute("""CREATE TABLE IF NOT EXISTS g.human_grade (
                         evidence_id TEXT PRIMARY KEY,
                         divergence_correctness TEXT,
                         note TEXT,
                         graded_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        return con

    def _send(self, obj, ctype="application/json", raw=None):
        body = raw if raw is not None else json.dumps(obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _where(self, q):
        cl, pr = [], {}
        for key, col in (("domain", "domain"), ("author", "work_author"),
                         ("work", "work_id"), ("novelty", "novelty_status")):
            v = (q.get(key) or [""])[0]
            if v:
                cl.append("r.%s = :%s" % (col, key))
                pr[key] = v
        s = (q.get("q") or [""])[0]
        if s:
            cl.append("(r.shelfmark LIKE :q OR r.sys_id LIKE :q)")
            pr["q"] = "%" + s + "%"
        g = (q.get("graded") or [""])[0]
        if g == "yes":
            cl.append("hg.divergence_correctness IS NOT NULL AND hg.divergence_correctness <> ''")
        elif g == "no":
            cl.append("(hg.divergence_correctness IS NULL OR hg.divergence_correctness = '')")
        return ("WHERE " + " AND ".join(cl) if cl else ""), pr

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/":
            return self._send(None, "text/html; charset=utf-8", PAGE.encode("utf-8"))
        con = self._conn()
        join = "LEFT JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id"
        where, pr = self._where(q)

        if u.path == "/api/facets":
            out = {}
            for key, col in (("domains", "domain"), ("authors", "work_author"),
                             ("works", "work_title"), ("novelty", "novelty_status")):
                # NO SILENT CAP. The first version stopped at 400 and the corpus
                # has 1,269 works, so a third of them were simply absent from the
                # "select a work" control with nothing saying so -- a filter that
                # cannot reach a third of its own domain. Facet lists are small
                # (tens of domains/authors), so they are returned whole.
                rows = con.execute(
                    "SELECT r.%s AS v, COUNT(*) n FROM review_row r %s %s "
                    "GROUP BY 1 ORDER BY n DESC" % (col, join, where), pr).fetchall()
                out[key] = [[x["v"], x["n"]] for x in rows]
            return self._send(out)

        if u.path == "/api/rows":
            off = int((q.get("offset") or ["0"])[0])
            total = con.execute("SELECT COUNT(*) c FROM review_row r %s %s"
                                % (join, where), pr).fetchone()["c"]
            pr2 = dict(pr, lim=PAGE_SIZE, off=off)
            rows = con.execute(
                "SELECT r.*, hg.divergence_correctness AS grade FROM review_row r %s %s "
                "ORDER BY r.work_id, r.sys_id LIMIT :lim OFFSET :off" % (join, where), pr2).fetchall()
            return self._send({"total": total, "rows": [dict(x) for x in rows]})

        if u.path == "/api/export":
            rows = con.execute(
                "SELECT r.evidence_id, r.sys_id, r.shelfmark, r.work_id, r.work_title, "
                "r.novelty_status, hg.divergence_correctness, hg.graded_at "
                "FROM review_row r JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id "
                "WHERE hg.divergence_correctness IS NOT NULL AND hg.divergence_correctness <> ''"
            ).fetchall()
            body = json.dumps([dict(x) for x in rows], ensure_ascii=False, indent=1).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=v3-human-grades.json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return self.wfile.write(body)

        self.send_error(404)

    def do_POST(self):
        if urllib.parse.urlparse(self.path).path != "/api/grade":
            return self.send_error(404)
        n = int(self.headers.get("Content-Length", 0))
        d = json.loads(self.rfile.read(n) or b"{}")
        val = d.get("divergence_correctness") or ""
        if val and val not in DIVERGENCE_VALUES:
            return self.send_error(400, "value outside the closed vocabulary")
        con = self._conn()
        if val:
            con.execute("INSERT INTO g.human_grade(evidence_id, divergence_correctness) "
                        "VALUES(?,?) ON CONFLICT(evidence_id) DO UPDATE SET "
                        "divergence_correctness=excluded.divergence_correctness, "
                        "graded_at=CURRENT_TIMESTAMP", (d.get("evidence_id"), val))
        else:
            con.execute("DELETE FROM g.human_grade WHERE evidence_id=?", (d.get("evidence_id"),))
        con.commit()
        self._send({"ok": True})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "discovery_data", "discovery-v3-REVIEW.db"))
    ap.add_argument("--port", type=int, default=8777)
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        raise SystemExit("review DB not found: %s" % args.db)
    Handler.db_path = args.db
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print("review DB : %s (%.0f MB)" % (args.db, os.path.getsize(args.db) / 1e6))
    print("grades    : %s.grades.db" % args.db)
    print("open      : http://127.0.0.1:%d" % args.port)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
