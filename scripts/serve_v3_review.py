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
# Stands in for SQL NULL on the wire, so a genuinely-absent value stays
# selectable and keeps a name of its own instead of collapsing into a neighbour.
NULL_TOKEN = "__null__"
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
 .badge.pool{background:#1e4034}
 .badge.more{background:#3d3520}
 .badge.cite{background:#4a2c46}
 .badge.ro{background:#4a2222;color:#ffd9d9}
 .badge.none{background:#2f2f36;color:#b9bcc4}
 .cols{display:grid;grid-template-columns:1fr 1fr;gap:12px}
 @media(max-width:900px){.cols{grid-template-columns:1fr}}
 .cat{color:var(--fg);background:#231f16;border:1px solid #3a3222;border-radius:6px;
      padding:4px 9px;font-size:12px}
 .cat b{color:#e0c07a;font-weight:600}
 .prev{margin-top:10px;border:1px solid var(--line);border-radius:8px;overflow:hidden;
       background:#0f1114;display:none}
 .prev.on{display:block}
 .prev iframe{width:100%;height:62vh;border:0;background:#fff}
 .prev .bar{display:flex;gap:10px;align-items:center;padding:6px 9px;
            border-bottom:1px solid var(--line);color:var(--dim);font-size:12px}
 .pane h4{margin:0 0 6px;font-size:13px;color:var(--dim);font-weight:600}
 .txt{direction:rtl;text-align:right;background:#0f1114;border:1px solid var(--line);
      border-radius:8px;padding:10px;max-height:320px;overflow:auto;
      white-space:pre-wrap;font-size:15px;line-height:1.9}
 .ctx{color:#7d838b}
 mark{background:var(--hit);color:#000;padding:0 2px;border-radius:3px}
 .grade{margin-top:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
 .grade button.sel{background:var(--acc);color:#00121f;border-color:var(--acc)}
 .stream{color:#c9a227;font-size:11px}
 .combo{position:relative}
 .combo input{min-width:9rem}
 .pager{display:flex;gap:8px;justify-content:center;padding:16px}
</style></head><body>
<header><div class="filters">
 <span class="combo"><input id="domain_t" list="domain_l" placeholder="domain: all"
        oninput="picked('domain')" size="18"><datalist id="domain_l"></datalist></span>
 <span class="combo"><input id="author_t" list="author_l" placeholder="author: all"
        oninput="picked('author')" size="16"><datalist id="author_l"></datalist></span>
 <span class="combo"><input id="work_t" list="work_l" placeholder="work: all"
        oninput="picked('work')" size="22"><datalist id="work_l"></datalist></span>
 <select id="novelty" onchange="load(0)"><option value="">novelty: all</option></select>
 <select id="pool" onchange="load(0)"><option value="">pool: all</option></select>
 <select id="claim" onchange="load(0)"><option value="">relation: all</option></select>
 <select id="routing" onchange="load(0)"><option value="">shown+review: all</option></select>
 <select id="graded" onchange="load(0)"><option value="">graded: any</option>
   <option value="no">ungraded only</option><option value="yes">graded only</option></select>
 <input id="q" placeholder="shelfmark contains…" size="15"
        oninput="typed()" onkeydown="if(event.key==='Enter')load(0)">
 <button onclick="reset()">Reset</button>
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
let off = 0, total = 0, LAST = [];
const $ = id => document.getElementById(id);
// QUOTES MUST BE ESCAPED, not just `&` and `<`. This value is interpolated into
// HTML ATTRIBUTES (`<option value="...">`), and Hebrew titles carry gershayim as
// a plain double quote -- `תנ"ך, תהלים` closed the attribute early and the option
// rendered as `תנ`, silently losing the rest of every such title. Escaping only
// the text-content characters is the wrong rule for an attribute.
const esc = s => (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
                        .replace(/>/g,"&gt;").replace(/"/g,"&quot;")
                        .replace(/'/g,"&#39;");

// TYPEABLE FILTERS. `datalist` gives native type-ahead over hundreds of options
// (1,080 works make a plain <select> unusable), but its value is the LABEL, so
// each combo keeps its own label->value map. A label the reader has not matched
// yet is simply not a filter -- never a silent no-match.
const COMBOS = ["domain","author","work"];
const MAP = {domain:{}, author:{}, work:{}};
function comboValue(k){
  const t = ($(k + "_t").value || "").trim();
  if (!t) return "";
  return (t in MAP[k]) ? MAP[k][t] : "";   // typed but not yet a real pick
}
function picked(k){
  const t = ($(k + "_t").value || "").trim();
  if (!t || (t in MAP[k])) load(0);                    // only query on a real pick
}
function params(extra){
  const p = new URLSearchParams();
  for (const k of COMBOS) { const v = comboValue(k); if (v) p.set(k, v); }
  for (const k of ["novelty","pool","claim","routing","graded","q"])
    if ($(k).value) p.set(k, $(k).value);
  Object.entries(extra||{}).forEach(([k,v]) => p.set(k,v));
  return p;
}
let facetSeq = 0;
async function facets(){
  // A failed or SUPERSEDED facets fetch must not blank the controls. Previously
  // any error here threw out of facets(), every <select> kept only its default
  // "all" option, and nothing said why. The sequence guard also stops a slow
  // early response from overwriting a newer one.
  const mine = ++facetSeq;
  let f;
  try {
    const r = await fetch("/api/facets?" + params());
    if (!r.ok) throw new Error("HTTP " + r.status);
    f = await r.json();
  } catch (e) {
    $("count").textContent += "  · filter lists unavailable (" + e.message + ")";
    return;
  }
  if (mine !== facetSeq) return;
  for (const k of COMBOS) {
    const key = {domain:"domains", author:"authors", work:"works"}[k];
    MAP[k] = {};
    const seen = {};
    $(k + "_l").innerHTML = f[key].map(([v,lab,n]) => {
      let label = String(lab || v || "(none)");
      if (seen[label]) label += "  · " + v;      // keep duplicates distinct
      seen[label] = 1;
      MAP[k][label] = v;
      return `<option value="${esc(label)}">${n.toLocaleString()}</option>`;
    }).join("");
  }
  for (const [id,key] of [["novelty","novelty"],["pool","pool"],["claim","claim"],
                          ["routing","routing"]]) {
    const cur = $(id).value;
    const name = {novelty:"novelty", pool:"pool", claim:"relation",
                  routing:"shown+review"}[id];
    let opts = f[key].map(([v,lab,n]) =>
      `<option value="${esc(v)}"${String(v)===cur?" selected":""}>`
      + `${esc(poolLabel(id,v))} (${n.toLocaleString()})</option>`);
    if (cur && !f[key].some(([v]) => String(v) === cur))
      opts.unshift(`<option value="${esc(cur)}" selected>${esc(cur)} (0 here)</option>`);
    $(id).innerHTML = `<option value="">${name}: all</option>` + opts.join("");
  }
}
// The reader-facing bucket names are the site's own ("main pool" / "more
// matches"), never "more findings". The second bucket means the evidence did not
// meet the rule -- it is NOT a statement that the identification is wrong.
const NULL_TOKEN = "__null__";
function poolLabel(id, v){
  if (id === "pool") {
    if (v === NULL_TOKEN || v === null) return "no identification record";
    return String(v) === "1" ? "main pool" : "more matches";
  }
  if (id === "claim") return {direct_witness:"alleged direct",
    quotes_this_work:"alleged citation", shared_text:"shared wording"}[v] || String(v);
  if (id === "routing") return {shipped:"shown on site",
    review_only:"review only"}[v] || String(v);
  return String(v);
}
function pane(title, b, m, a, isStream){
  return `<div class="pane"><h4>${title}${isStream?' <span class="stream">[unspaced letter stream]</span>':''}</h4>
    <div class="txt"><span class="ctx">${esc(b)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(a)}</span></div></div>`;
}
async function load(newOff){
  if (newOff !== undefined) off = newOff;
  const r = await fetch("/api/rows?" + params({offset: off}));
  const d = await r.json();
  total = d.total; LAST = d.rows;
  $("count").textContent = `${d.total.toLocaleString()} rows · showing ${off+1}-${Math.min(off+d.rows.length,total)}`;
  $("app").innerHTML = d.rows.map(x => `
    <div class="card">
      <div class="meta">
        <span class="badge nov">${esc(x.novelty_status)}</span>
        <span class="badge">${esc(x.domain||"—")}</span>
        <span>${esc(x.library_code||"")} <b>${esc(x.shelfmark||x.sys_id)}</b></span>
        <span>identified as: <b>${esc(x.work_title||x.work_id)}</b>${x.work_author?" · "+esc(x.work_author):""}</span>
        <span>${x.matched_letters} letters${x.n_spans>1?` · ${x.n_spans} spans`:""}</span>
        <span class="badge">${esc(x.source_corpus||"")}</span>
        <span class="badge ${x.main_pool===null?"none":(x.main_pool==1?"pool":"more")}">${
          x.main_pool===null ? "no identification record"
                             : (x.main_pool==1 ? "main pool" : "more matches")}</span>
        <span class="badge ${x.claim_type==="quotes_this_work"?"cite":""}">${poolLabel("claim",x.claim_type)}</span>
        ${x.routing_status!=="shipped" ? `<span class="badge ro">review only — not shown on the site</span>` : ``}
        ${browseUrl(x) ? `<button onclick="preview('${x.evidence_id}',this)">◱ preview folio</button>` : ``}
      </div>
      ${x.catalogue_title ? `<div class="meta"><span class="cat">catalogued as:
         <b dir="auto">${esc(x.catalogue_title)}</b></span></div>` : ``}
      <div class="cols">
        ${pane("Manuscript", x.ms_before, x.ms_match, x.ms_after, false)}
        ${pane("Reference edition", x.ref_before, x.ref_match, x.ref_after, x.ref_is_stream)}
      </div>
      <div class="prev" id="prev-${x.evidence_id}"></div>
      <div class="grade">
        <span style="color:var(--dim)">Which is right?</span>
        ${DIV.map(v => `<button class="${x.grade===v?'sel':''}"
           onclick="grade('${x.evidence_id}','${v}',this)">${v.replace("_"," ")}</button>`).join("")}
        <button onclick="grade('${x.evidence_id}','',this)">clear</button>
      </div>
    </div>`).join("");
  facets();
}
// The LIVE site's bare viewer. `embed=1` is the route built for exactly this --
// no site chrome, and it does NOT persist/restore browse state, so previewing
// here cannot overwrite wherever the reader left /browse in their own tab.
// page and volume_ie travel TOGETHER or not at all: a folio number without its
// volume is a DIFFERENT folio in each volume of a multi-volume manuscript, so a
// half address looks targeted and lands somewhere else.
const SITE = "https://genizahsearch.com";
function browseUrl(x, embed){
  if (!x.sys_id) return null;
  let u = SITE + "/browse?sys_id=" + encodeURIComponent(x.sys_id);
  if (embed) u += "&embed=1";
  if (x.page_num && x.volume_ie)
    u += "&page=" + x.page_num + "&volume_ie=" + encodeURIComponent(x.volume_ie);
  return u;
}
// Lazy BY DESIGN: 25 iframes to the live site per page turn would hammer it and
// make every page load wait on the network. The frame is built on click.
function preview(id, btn){
  const box = document.getElementById("prev-" + id);
  if (box.classList.contains("on")) {
    box.classList.remove("on"); box.innerHTML = ""; btn.textContent = "◱ preview folio"; return;
  }
  const x = LAST.find(r => r.evidence_id === id);
  const full = browseUrl(x, false), emb = browseUrl(x, true);
  box.innerHTML = `<div class="bar"><span>live genizahsearch.com — folio ${x.page_num||"?"}</span>
      <a href="${full}" target="_blank" rel="noopener">open in a tab ↗</a></div>
    <iframe src="${emb}" loading="lazy" referrerpolicy="no-referrer"></iframe>`;
  box.classList.add("on");
  btn.textContent = "◱ hide folio";
}
async function grade(id, val, btn){
  await fetch("/api/grade", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({evidence_id:id, divergence_correctness:val})});
  const row = btn.parentElement;
  [...row.querySelectorAll("button")].forEach(b => b.classList.remove("sel"));
  if (val) btn.classList.add("sel");
}

// Choosing a filter APPLIES it. An Apply button made every change a two-step
// action and, worse, let the controls show a state the rows below did not match.
// The text box debounces so a shelfmark search does not fire a query per keystroke.
let typeTimer = null;
function typed(){ clearTimeout(typeTimer); typeTimer = setTimeout(() => load(0), 300); }
function reset(){
  for (const k of COMBOS) $(k + "_t").value = "";
  for (const k of ["novelty","pool","claim","routing","graded","q"]) $(k).value = "";
  load(0);
}
function next(){ if (off + 25 < total) load(off + 25); }
function prev(){ if (off > 0) load(Math.max(0, off - 25)); }
function exportGrades(){ window.location = "/api/export"; }
load(0);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    db_path = None
    # Facets are pure functions of the filter state, and the reader re-issues the
    # same state constantly (every page turn calls facets()). Bounded so a long
    # session cannot grow it without limit.
    _facet_cache = {}
    _facet_lock = __import__("threading").Lock()

    def log_message(self, *a):
        pass

    def handle_one_request(self):
        # A reader who navigates or re-filters mid-response aborts the socket.
        # That is normal client behaviour, not a fault, and printing a traceback
        # for it buries real errors in noise.
        try:
            super().handle_one_request()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            self.close_connection = True

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

    # filter key -> column. One table, so every filter is a plain equality.
    FILTERS = (("domain", "domain"), ("author", "work_author"),
               ("work", "work_id"), ("novelty", "novelty_status"),
               ("pool", "main_pool"), ("claim", "claim_type"),
               ("routing", "routing_status"))

    def _where(self, q, exclude=None):
        """`exclude` drops ONE filter from the clause.

        That is what makes a facet list usable: a facet computed WITH its own
        selection applied contains exactly one option -- the thing already
        chosen -- so switching from one work to another meant first setting the
        control back to "all". Every other filter still applies, so the counts
        stay honest about the rest of the query.
        """
        cl, pr = [], {}
        for key, col in self.FILTERS:
            if key == exclude:
                continue
            v = (q.get(key) or [""])[0]
            if v == NULL_TOKEN:
                # A NULL cannot be selected with `=`. 64,406 review-only rows
                # carry no identification record at all, so `main_pool` is NULL
                # for them -- and that is a THIRD state, not a synonym for "not
                # in the main pool". Without this they were unselectable and,
                # worse, rendered under the wrong bucket name.
                cl.append("r.%s IS NULL" % col)
            elif v:
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
        # JOIN ONLY WHEN NEEDED. Carrying this LEFT JOIN on every facet query cost
        # seconds over 254,612 rows for a column most queries never read -- and a
        # facets response slow enough to be cancelled leaves every dropdown empty,
        # because the client's facets() throws on the aborted fetch.
        graded_active = bool((q.get("graded") or [""])[0])
        join = ("LEFT JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id"
                if graded_active else "")
        where, pr = self._where(q)

        if u.path == "/api/facets":
            ckey = tuple(sorted((k, v[0]) for k, v in q.items() if v and v[0]))
            with self._facet_lock:
                hit = self._facet_cache.get(ckey)
            if hit is not None:
                return self._send(hit)
            out = {}
            # (key, VALUE column, LABEL column). The value is what the filter
            # compares; the label is what the reader picks. They differ for works
            # and MUST NOT be conflated -- the first version listed work TITLES
            # while the filter compared work_id, so choosing any work returned
            # zero rows. Nothing failed; the two were simply never the same string.
            for key, valcol, labcol, own in (
                    ("domains", "domain", "domain", "domain"),
                    ("authors", "work_author", "work_author", "author"),
                    ("works", "work_id", "work_title", "work"),
                    ("novelty", "novelty_status", "novelty_status", "novelty"),
                    ("pool", "main_pool", "main_pool", "pool"),
                    ("claim", "claim_type", "claim_type", "claim"),
                    ("routing", "routing_status", "routing_status", "routing")):
                # Each facet is computed with its OWN filter excluded (see
                # `_where`), so its list stays switchable instead of collapsing to
                # the single value already chosen.
                fw, fp = self._where(q, exclude=own)
                # NO SILENT CAP. The first version stopped at 400 and the corpus
                # has 1,269 works, so a third of them were simply absent from the
                # "select a work" control with nothing saying so -- a filter that
                # cannot reach a third of its own domain. Facet lists are small
                # (tens of domains/authors), so they are returned whole.
                rows = con.execute(
                    "SELECT r.%s AS v, MAX(r.%s) AS lab, COUNT(*) n FROM facet_row r %s %s "
                    "GROUP BY 1 ORDER BY n DESC" % (valcol, labcol, join, fw), fp).fetchall()
                out[key] = [[NULL_TOKEN if x["v"] is None else x["v"],
                             x["lab"], x["n"]] for x in rows]
            with self._facet_lock:
                if len(self._facet_cache) > 200:
                    self._facet_cache.clear()
                self._facet_cache[ckey] = out
            return self._send(out)

        if u.path == "/api/rows":
            off = int((q.get("offset") or ["0"])[0])
            rjoin = "LEFT JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id"
            total = con.execute("SELECT COUNT(*) c FROM review_row r %s %s"
                                % (join, where), pr).fetchone()["c"]
            pr2 = dict(pr, lim=PAGE_SIZE, off=off)
            rows = con.execute(
                "SELECT r.*, hg.divergence_correctness AS grade FROM review_row r %s %s "
                "ORDER BY r.work_id, r.sys_id LIMIT :lim OFFSET :off" % (rjoin, where), pr2).fetchall()
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


class ReviewServer(ThreadingHTTPServer):
    # MUST be False on Windows. `HTTPServer` defaults it to 1, and Windows honours
    # SO_REUSEADDR by letting a SECOND process bind a port another process already
    # holds -- so this server would start, print its URL, and quietly lose every
    # request to whatever was already listening. That is not hypothetical: a stale
    # `http.server` on 8777 served its own directory listing to a reader who had
    # just started this one, and nothing anywhere reported a conflict.
    allow_reuse_address = False


def _port_is_taken(port: int) -> bool:
    """Someone already listening on loopback? Ask by connecting, not by binding:
    on Windows a bind can succeed against an in-use port, which is the whole bug."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.35)
        return s.connect_ex(("127.0.0.1", port)) == 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "discovery_data", "discovery-v3-REVIEW.db"))
    ap.add_argument("--port", type=int, default=8777)
    ap.add_argument("--strict-port", action="store_true",
                    help="fail if --port is busy instead of moving to a free one")
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        raise SystemExit("review DB not found: %s" % args.db)
    Handler.db_path = args.db
    # ENSURE INDEXES. `routing_status` had none, and its GROUP BY cost ~1s per
    # facet over 254,612 rows -- seven facets made the response slow enough for
    # the browser to cancel it, which is what left every dropdown empty. Created
    # here rather than only in the builder so an existing 1.4 GB artifact does
    # not have to be rebuilt for an index.
    _ix = sqlite3.connect(args.db)
    for name, col in (("ix_rr_routing", "routing_status"),
                      ("ix_rr_band", "confidence_band")):
        try:
            _ix.execute("CREATE INDEX IF NOT EXISTS %s ON review_row(%s)" % (name, col))
        except sqlite3.OperationalError:
            pass          # older artifact without the column -- not fatal

    # A SLIM TABLE FOR FACETS. Each review_row carries ~6 KB of both-sides text,
    # so ANY facet scan drags that payload through memory for columns it never
    # reads -- which is why a filtered facets call still took seconds even with
    # the right indexes. This projection is the filterable columns only (~40 MB
    # against 1.4 GB), so a facet scan touches a fraction of the data.
    # Derived here rather than only in the builder so an existing artifact gains
    # it without a rebuild; it is a cache, and dropping it costs only speed.
    have = _ix.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                       "AND name='facet_row'").fetchone()[0]
    if have:
        same = (_ix.execute("SELECT COUNT(*) FROM facet_row").fetchone()[0] ==
                _ix.execute("SELECT COUNT(*) FROM review_row").fetchone()[0])
        if not same:
            _ix.execute("DROP TABLE facet_row")     # stale against a rebuild
            have = 0
    if not have:
        print("building the facet index table (one time)...", flush=True)
        _ix.execute("""CREATE TABLE facet_row AS SELECT
                         evidence_id, sys_id, shelfmark, domain, work_id,
                         work_title, work_author, novelty_status, main_pool,
                         claim_type, routing_status FROM review_row""")
        for col in ("domain", "work_id", "work_author", "novelty_status",
                    "main_pool", "claim_type", "routing_status", "evidence_id"):
            _ix.execute("CREATE INDEX ix_fr_%s ON facet_row(%s)" % (col, col))
    _ix.commit()
    _ix.close()

    port = args.port
    if _port_is_taken(port):
        if args.strict_port:
            raise SystemExit(
                "port %d is already serving something else (a stale http.server?). "
                "Stop it, or re-run without --strict-port to use the next free port."
                % port)
        print("! port %d is already in use by another server -- moving on" % port)
        for cand in range(port + 1, port + 40):
            if not _port_is_taken(cand):
                port = cand
                break
        else:
            raise SystemExit("no free port in %d-%d" % (port + 1, port + 39))

    try:
        srv = ReviewServer(("127.0.0.1", port), Handler)
    except OSError as e:
        raise SystemExit("could not bind 127.0.0.1:%d -- %s" % (port, e))

    print("review DB : %s (%.0f MB)" % (args.db, os.path.getsize(args.db) / 1e6))
    print("grades    : %s.grades.db" % args.db)
    print("")
    print("   OPEN:   http://127.0.0.1:%d" % port)
    print("")
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
