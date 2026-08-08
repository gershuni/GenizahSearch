"""A PRIVATE, local review page for the UNROUTED E1 witness population.

The v3 release build halts on 19,238 `track1_direct` witness rows that arrived
from the E1 round (2026-07-19) and were never scored by gen-2's coverage
router -- the router ran over the Q2 batch, E1 came in on a parallel lane, and
the two were never joined. They therefore carry NO same_work/parallel verdict,
and `assert_emitted_parity` refuses to let them keep the ingest default.

This page exists so the owner can decide what happens to them by LOOKING at
them rather than at a coverage histogram. For each sampled row it shows the
manuscript's own transcription with the matched span highlighted, beside the
row's own metrics and the band its source collection asserts.

Two things this page is careful about, because both are easy to get wrong:

  * **Offsets.** `o0`/`o1` index the page's SPACE-FREE normalized letter stream
    (`same_work_spike/probe/scripts/normalize.py::norm_stream`), NOT the raw
    transcription. Highlighting raw text with them puts the marks on the wrong
    words. We project through the offset map `norm_stream` returns, which is
    exactly what it is kept for.
  * **What the span is.** `o0`/`o1` is ONE span -- the largest -- while `ml`
    counts matched letters across all `n_spans` of them. Verified: of 15,996
    rows with `n_spans == 1`, zero have `ml > (o1 - o0)`; every row that
    exceeds it has `n_spans >= 2`. So the highlight is labelled as one span of
    N, never as "the match".

Coverage is `ml / pages.n_chars` -- the RAW character denominator, which is the
unit gen-2's threshold was calibrated in. The `coverage` field the builder
computes for Lever-1 uses a NORMALIZED denominator and is a different number;
mixing them silently over-promotes.

MASKING (D-25): renders our own manuscript text, Hebrew work titles and opaque
ids only. Reference-corpus text is never rendered. The source corpus is shown
as a MASKED code (`sefaria` / `ja` / `msource`) via
`build_discovery_sidecar._map_cat_to_source_corpus`; the raw `cat` value never
reaches the page. Output goes to gitignored `discovery_data/` and is named
`.PRIVATE.html` -- same posture as the v2 and novelty review pages.

Usage:
    python scripts/e1_review_html.py
    python scripts/e1_review_html.py --per-cell 12 --seed 7
"""

from __future__ import annotations

import argparse
import html
import json
import os
import random
import sqlite3
import sys
from typing import Dict, List, Optional, Sequence, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
_PROBE_SCRIPTS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "scripts")
if _PROBE_SCRIPTS not in sys.path:
    sys.path.insert(0, _PROBE_SCRIPTS)

from normalize import norm_stream  # noqa: E402
from scripts.build_discovery_sidecar import _map_cat_to_source_corpus  # noqa: E402

DEFAULT_RESEARCH_DB = os.path.join(REPO_ROOT, "_tmp", "v3_research_slim.db")
DEFAULT_DATA_DIR = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data")
DEFAULT_OUT = os.path.join(REPO_ROOT, "discovery_data", "E1-UNROUTED-REVIEW.PRIVATE.html")

# The four DISJOINT E1 source collections, in the order the spine assigns their
# bands (134-CONTEXT R6): band != adjudication -- only `e1_adjudicated_a` rows
# carry a per-item human verdict; `e1_ra_confirmed` is band-evaluated and marked
# `unreviewed`.
COLLECTIONS: List[Tuple[str, str, str]] = [
    ("e1_adjudicated_a", "adjudicated-A — a human confirmed each one individually",
     "human_confirmed"),
    ("e1_ra_confirmed", "R-A confirmed band — judged as a batch, NOT item by item",
     "unreviewed"),
    ("e1_rb_screening", "R-B screening surface — flagged for a look, never got one",
     "unreviewed"),
    ("e1_r3_frame", "R-3 frame — the later, wider sweep", "unreviewed"),
]


def load_threshold(router_db: Optional[str]) -> Tuple[float, str]:
    """Read gen-2's fitted cut from the router artifact. Never hardcoded -- a
    truncated literal moves ~90 real rows across the line."""
    if router_db and os.path.isfile(router_db):
        try:
            con = sqlite3.connect(f"file:{router_db}?mode=ro", uri=True)
            try:
                # `coverage_route_meta` is ONE wide row, not a key/value table:
                # `threshold` is a column alongside the objective and calibration
                # provenance. Read it as such rather than guessing a shape.
                row = con.execute(
                    "SELECT threshold, run_id FROM coverage_route_meta LIMIT 1"
                ).fetchone()
            finally:
                con.close()
            if row is not None and row[0] is not None:
                return float(row[0]), f"{os.path.basename(router_db)} ({row[1]})"
        except sqlite3.Error:
            pass
    raise SystemExit(
        "refusing to render a page whose above/below split is drawn at a guessed "
        "threshold -- pass --router-db pointing at the gen-2 router artifact"
    )


def load_rows(data_dir: str) -> List[Dict]:
    out: List[Dict] = []
    for name, label, adjudication in COLLECTIONS:
        path = os.path.join(data_dir, f"{name}.jsonl")
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                row = json.loads(line)
                row["_collection"] = name
                row["_collection_label"] = label
                row["_adjudication"] = adjudication
                out.append(row)
    return out


def project_span(text: str, o0: Optional[int], o1: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    """Map a span in the page's normalized letter stream back onto the raw
    transcription. Returns (None, None) when the offsets do not fit the stream,
    so a bad span shows as "no highlight" rather than as a confident mark on
    the wrong words."""
    if not text or o0 is None or o1 is None:
        return None, None
    stream, offs = norm_stream(text)
    if not (0 <= o0 < o1 <= len(stream)):
        return None, None
    return offs[o0], offs[o1 - 1] + 1


def highlight(text: str, start: Optional[int], end: Optional[int],
              cap: int = 2400, ctx: int = 500) -> str:
    if not text:
        return '<span class="muted">(no transcription on file for this page)</span>'
    if start is None or end is None:
        body = text[:cap] + (" …" if len(text) > cap else "")
        return (f'<span class="muted">[span could not be projected onto this text]</span>\n'
                + html.escape(body))
    pre, mid, post = text[:start], text[start:end], text[end:]
    if len(mid) > cap:
        mid = mid[:cap] + " …"
    return (f'<span class="muted">…{html.escape(pre[-ctx:])}</span>'
            f'<mark class="hl">{html.escape(mid)}</mark>'
            f'<span class="muted">{html.escape(post[:ctx])}…</span>')


def stratified_sample(rows: List[Dict], threshold: float, per_cell: int,
                      seed: int) -> Dict[Tuple[str, bool], List[Dict]]:
    """Sample every (collection x above/below the cut) cell, so the page shows
    what the rule would DO, not just what the population looks like."""
    rng = random.Random(seed)
    cells: Dict[Tuple[str, bool], List[Dict]] = {}
    for row in rows:
        cov = row.get("_coverage")
        if cov is None:
            continue
        cells.setdefault((row["_collection"], cov >= threshold), []).append(row)
    out: Dict[Tuple[str, bool], List[Dict]] = {}
    for key, bucket in cells.items():
        rng.shuffle(bucket)
        out[key] = sorted(bucket[:per_cell], key=lambda r: -r["_coverage"])
    return out


CSS = """
:root{--bg:#0f1115;--panel:#171a21;--panel2:#1e222b;--line:#2a2f3a;--fg:#e7e9ee;--mut:#9aa3b2;
      --A:#22c55e;--H:#38bdf8;--R:#f59e0b;}
@media(prefers-color-scheme:light){:root{--bg:#f7f8fa;--panel:#fff;--panel2:#f0f2f6;--line:#dfe3ea;--fg:#1a1d24;--mut:#5b6472}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.6 -apple-system,Segoe UI,Roboto,Arial,"Noto Sans Hebrew",sans-serif}
header{padding:14px 20px;border-bottom:1px solid var(--line);background:var(--panel);position:sticky;top:0;z-index:5}
h1{font-size:17px;margin:0 0 3px}.sub{color:var(--mut);font-size:12px}
.warn{background:#7c2d12;color:#fed7aa;padding:7px 20px;font-size:12.5px;border-bottom:1px solid var(--line)}
@media(prefers-color-scheme:light){.warn{background:#fff7ed;color:#9a3412}}
nav{display:flex;gap:6px;padding:10px 20px;flex-wrap:wrap;border-bottom:1px solid var(--line);background:var(--panel)}
nav button{background:var(--panel2);color:var(--fg);border:1px solid var(--line);border-radius:7px;padding:6px 12px;cursor:pointer;font-size:13px}
nav button.active{background:var(--H);color:#04121c;border-color:var(--H);font-weight:600}
main{padding:18px 20px;max-width:1200px;margin:0 auto}
section{display:none}section.active{display:block}
.cards{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px;min-width:118px}
.card .n{font-size:20px;font-weight:700}.card .l{color:var(--mut);font-size:11.5px}
.intro{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 16px;margin-bottom:14px}
.case{background:var(--panel);border:1px solid var(--line);border-radius:11px;margin:14px 0;overflow:hidden}
.case>.head{padding:10px 14px;border-bottom:1px solid var(--line);background:var(--panel2)}
.case .ms{font-weight:600}.case .claim{color:var(--H)}
.body{padding:12px 14px}
.txt{white-space:pre-wrap;word-break:break-word;font-size:13px;max-height:330px;overflow:auto;
     background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:10px}
.metrics{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:9px;font-size:12px;color:var(--mut)}
.metrics b{color:var(--fg);font-weight:600}
mark.hl{background:#facc15;color:#000;border-radius:3px;padding:0 2px}
.pill{display:inline-block;padding:1px 7px;border-radius:6px;border:1px solid var(--line);font-size:11px;color:var(--mut);margin-inline-start:6px}
.above{background:var(--A);color:#04121c;border:none;font-weight:600}
.below{background:var(--R);color:#04121c;border:none;font-weight:600}
.muted{color:var(--mut)}
"""


def render(sample, stats, meta) -> str:
    P: List[str] = []
    A = P.append
    A('<!doctype html>\n<html lang="he" dir="rtl">\n<head>\n<meta charset="utf-8">')
    A('<meta name="robots" content="noindex,nofollow">')
    A('<meta name="viewport" content="width=device-width,initial-scale=1">')
    A("<title>E1 unrouted witnesses — local review (PRIVATE)</title>\n<style>")
    A(CSS)
    A("</style></head><body>")
    A('<header><h1>עדים ללא ניתוב — סקירה מקומית / E1 unrouted witnesses — local review</h1>')
    A(f'<div class="sub">{meta["total"]:,} rows with no copy/quote verdict · '
      f'threshold {meta["threshold"]:.4f} read from {html.escape(meta["threshold_src"])} · '
      f'{meta["sampled"]} cases sampled</div></header>')
    A('<div class="warn">PRIVATE — local review only. Renders our own transcription, Hebrew work '
      'titles and opaque ids. No reference-corpus text; source corpora appear as masked codes.</div>')

    A('<nav>')
    for i, (name, label, _adj) in enumerate(COLLECTIONS):
        cls = " class=\"active\"" if i == 0 else ""
        A(f'<button{cls} data-t="{name}">{html.escape(name)}</button>')
    A('</nav><main>')

    A('<div class="intro"><b>What you are looking at.</b> These rows carry a real match but no '
      '<i>copy vs quote</i> verdict — the labeller ran over a different batch and never saw them. '
      'Each case shows the manuscript page with the largest matched span highlighted. '
      '<b>Above</b> / <b>below</b> marks which side of gen-2\'s fitted cut the row would fall on '
      'if we applied the same rule used for the other 275,894 rows. '
      'Coverage here is matched letters ÷ raw page characters — the unit the cut was fitted in.</div>')

    for i, (name, label, adjudication) in enumerate(COLLECTIONS):
        cls = " active" if i == 0 else ""
        A(f'<section class="{cls.strip()}" id="sec-{name}">')
        st = stats.get(name, {})
        A('<div class="cards">')
        A(f'<div class="card"><div class="n">{st.get("total", 0):,}</div><div class="l">rows</div></div>')
        A(f'<div class="card"><div class="n">{st.get("above", 0):,}</div><div class="l">above the cut</div></div>')
        A(f'<div class="card"><div class="n">{st.get("below", 0):,}</div><div class="l">below the cut</div></div>')
        A(f'<div class="card"><div class="n">{st.get("median", 0):.3f}</div><div class="l">median coverage</div></div>')
        A('</div>')
        A(f'<div class="intro">{html.escape(label)} · adjudication status '
          f'<b>{html.escape(adjudication)}</b></div>')

        for above in (True, False):
            rows = sample.get((name, above), [])
            if not rows:
                continue
            A(f'<h3>{"Above" if above else "Below"} the cut — {len(rows)} sampled</h3>')
            for r in rows:
                pill = "above" if above else "below"
                A('<div class="case"><div class="head">')
                A(f'<span class="ms">{html.escape(str(r.get("cshelf") or r.get("sys_id")))}</span> '
                  f'<span class="muted">→</span> '
                  f'<span class="claim">{html.escape(str(r.get("work_title") or r.get("work_id")))}</span>'
                  f'<span class="pill {pill}">{r["_coverage"]:.3f} '
                  f'{"≥" if above else "<"} {meta["threshold"]:.3f}</span>'
                  f'<span class="pill">{html.escape(r["_corpus"])}</span>')
                A('</div><div class="body">')
                A('<div class="metrics">'
                  f'<span>matched letters <b>{r.get("ml"):,}</b></span>'
                  f'<span>page chars <b>{r.get("_n_chars"):,}</b></span>'
                  f'<span>spans <b>{r.get("n_spans")}</b></span>'
                  f'<span>band <b>{html.escape(str(r.get("band") or r.get("band2") or "—"))}</b></span>'
                  f'<span>score <b>{r.get("s")}</b></span>'
                  '</div>')
                if (r.get("n_spans") or 1) > 1:
                    A(f'<div class="metrics"><span class="muted">highlight shows the largest of '
                      f'{r.get("n_spans")} matched spans — the other letters counted above lie '
                      f'outside it</span></div>')
                if r.get("e1_status"):
                    A(f'<div class="metrics"><span>{html.escape(str(r["e1_status"]))}</span></div>')
                A(f'<div class="txt">{highlight(r["_text"], r["_raw0"], r["_raw1"])}</div>')
                A('</div></div>')
        A('</section>')

    A('</main><script>')
    A("document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{"
      "document.querySelectorAll('nav button').forEach(x=>x.classList.remove('active'));"
      "document.querySelectorAll('section').forEach(x=>x.classList.remove('active'));"
      "b.classList.add('active');"
      "document.getElementById('sec-'+b.dataset.t).classList.add('active');});")
    A('</script></body></html>')
    return "\n".join(P)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--research-db", default=DEFAULT_RESEARCH_DB)
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    ap.add_argument("--router-db", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data", "g_launch3.db"))
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--per-cell", type=int, default=10)
    ap.add_argument("--seed", type=int, default=1729)
    args = ap.parse_args(argv)

    threshold, threshold_src = load_threshold(args.router_db)
    rows = load_rows(args.data_dir)
    if not rows:
        raise SystemExit(f"no E1 collections found under {args.data_dir}")

    con = sqlite3.connect(f"file:{args.research_db}?mode=ro", uri=True)
    try:
        pages = {p: (n, t) for p, n, t in con.execute(
            "SELECT page_id, n_chars, text FROM pages")}
    finally:
        con.close()

    for r in rows:
        n_chars, text = pages.get(r["page_id"], (None, None))
        r["_n_chars"] = n_chars
        r["_text"] = text
        ml = r.get("ml")
        r["_coverage"] = (min(1.0, ml / n_chars) if n_chars and ml is not None else None)
        r["_corpus"] = _map_cat_to_source_corpus(r.get("cat")) or "unknown"

    stats: Dict[str, Dict] = {}
    for name, _label, _adj in COLLECTIONS:
        covs = sorted(r["_coverage"] for r in rows
                      if r["_collection"] == name and r["_coverage"] is not None)
        if not covs:
            continue
        stats[name] = {
            "total": len(covs),
            "above": sum(1 for c in covs if c >= threshold),
            "below": sum(1 for c in covs if c < threshold),
            "median": covs[len(covs) // 2],
        }

    sample = stratified_sample(rows, threshold, args.per_cell, args.seed)
    # Project spans only for the sampled rows -- norm_stream over every page
    # would be a needless pass over the whole corpus.
    sampled = 0
    for bucket in sample.values():
        for r in bucket:
            r["_raw0"], r["_raw1"] = project_span(r["_text"], r.get("o0"), r.get("o1"))
            sampled += 1

    meta = {
        "total": sum(s["total"] for s in stats.values()),
        "sampled": sampled,
        "threshold": threshold,
        "threshold_src": threshold_src,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(render(sample, stats, meta))
    print(f"wrote {args.out}  ({sampled} cases, {meta['total']:,} rows)")
    for name, s in stats.items():
        print(f"  {name:20s} {s['total']:6,d} rows  above={s['above']:6,d}  "
              f"below={s['below']:6,d}  median={s['median']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
