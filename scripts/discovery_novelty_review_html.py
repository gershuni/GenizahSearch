"""Phase 136 — a PRIVATE, local review page for the computed novelty axis.

Renders a sample of every novelty shade so the owner can judge the outcome by
looking at the actual evidence, not at counts. For each sampled row it shows,
side by side:

  * the MANUSCRIPT's own transcription (from ``Transcriptions.txt``, keyed by
    the same ``page_id`` the evidence carries), with the matched span
    highlighted — so the identification itself can be sanity-checked; and
  * what EACH checked finding aid actually says (catalogue / bibliography /
    PGP / FGP) — the evidence the novelty verdict was actually made on.

House style follows ``discovery-v2-REVIEW.PRIVATE.html``: RTL Hebrew, dark
theme with a light-mode media query, tabbed sections, sticky header,
``noindex,nofollow``.

MASKING: this page renders OUR manuscript text and the four checked aids only.
It deliberately does NOT render any reference-corpus text, so no restricted
("M-source") material can reach it by construction. The output still goes to
gitignored ``discovery_data/`` and is named ``.PRIVATE.html`` — same posture as
the v2 review page. Scan it before sharing it anywhere.

Usage:
    python scripts/discovery_novelty_review_html.py
    python scripts/discovery_novelty_review_html.py --per-shade 10 --fills-gap 25
"""

from __future__ import annotations

import argparse
import collections
import html
import json
import os
import random
import re
import sqlite3
import sys
from typing import Dict, List, Optional, Sequence, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.discovery_novelty_funnel import assemble_evidence_bundle  # noqa: E402
from scripts.discovery_novelty_probe import (  # noqa: E402
    DEFAULT_ASSET,
    DEFAULT_FGP_DB,
    DEFAULT_FJMS_DB,
    DEFAULT_LIBRARIES_CSV,
    DEFAULT_PGP_DB,
    build_all_candidates,
)

DEFAULT_VERDICTS = os.path.join(REPO_ROOT, "discovery_data", "novelty_production_verdicts.json")
DEFAULT_TRANSCRIPTIONS = os.path.join(REPO_ROOT, "Transcriptions.txt")
DEFAULT_OUT = os.path.join(REPO_ROOT, "discovery_data", "novelty-REVIEW.PRIVATE.html")

# Ordered so the page reads from "we agree with the aids" through to the
# categories that actually make claims about the world.
SHADE_ORDER = [
    "fills_gap", "diverges_work", "diverges_part", "container_predicts",
    "refines_granularity", "aid_more_specific", "alias_merge", "confirms", "not_checked",
]
SHADE_LABEL = {
    "fills_gap": "fills_gap — no aid identifies it (SHIPS as a candidate find)",
    "diverges_work": "diverges_work — an aid names a DIFFERENT work (hidden by default)",
    "diverges_part": "diverges_part — a different PART of the same work (hidden by default)",
    "container_predicts": "container_predicts — an aid names a container whose content predicts this",
    "refines_granularity": "refines_granularity — we are FINER than the aid",
    "aid_more_specific": "aid_more_specific — the aid is finer than us (we add nothing)",
    "alias_merge": "alias_merge — same work under two ids",
    "confirms": "confirms — an aid already names this work",
    "not_checked": "not_checked — abstained or unmapped",
}
SHIPS_AS_CANDIDATE = "fills_gap"
_SRC_LABEL = [("catalogue", "קטלוג / Catalogue"), ("bibliography", "ביבליוגרפיה / Bibliography"),
              ("pgp", "PGP"), ("fgp", "FGP")]


def load_verdicts(path: str) -> Dict[str, str]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    return {k: v["novelty_status"] for k, v in raw.items() if v.get("novelty_status")}


def sample_by_shade(
    verdicts: Dict[str, str], per_shade: int, fills_gap: int, seed: int
) -> Dict[str, List[str]]:
    by = collections.defaultdict(list)
    for key, shade in verdicts.items():
        by[shade].append(key)
    out: Dict[str, List[str]] = {}
    rng = random.Random(seed)
    for shade, keys in by.items():
        keys.sort()
        n = fills_gap if shade == SHIPS_AS_CANDIDATE else per_shade
        out[shade] = rng.sample(keys, n) if len(keys) > n else keys
    return out


def page_and_metrics(cur, sys_id: str, work_id: str) -> Tuple[Optional[str], Optional[int], Optional[float], Optional[int], Optional[int]]:
    """Best (largest-match) evidence row for this (sys_id, work) — its page,
    matched letters, coverage density and matched span."""
    cur.execute(
        """SELECT e.a_page_id, e.matched_letters, e.density, e.span_start, e.span_end
           FROM discovery_claim c JOIN discovery_evidence e ON e.claim_id = c.claim_id
           WHERE c.work_id = ? AND c.page_id LIKE ?
           ORDER BY COALESCE(e.matched_letters, 0) DESC LIMIT 1""",
        (work_id, f"{sys_id}_%"),
    )
    row = cur.fetchone()
    return row if row else (None, None, None, None, None)


def load_page_texts(path: str, wanted: set) -> Dict[str, str]:
    """One streaming pass over the (very large) transcription file, keeping
    only the pages actually sampled."""
    out: Dict[str, str] = {}
    if not os.path.isfile(path):
        return out
    marker = re.compile(r"^==> (\S+) <==\s*$")
    cur_key: Optional[str] = None
    buf: List[str] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = marker.match(line)
            if m:
                if cur_key in wanted and buf:
                    out[cur_key] = "".join(buf).strip()
                cur_key = m.group(1)
                buf = []
                if len(out) == len(wanted):
                    break
                continue
            if cur_key in wanted:
                buf.append(line)
    if cur_key in wanted and buf and cur_key not in out:
        out[cur_key] = "".join(buf).strip()
    return out


def highlight(text: str, start: Optional[int], end: Optional[int], cap: int = 2600) -> str:
    """Escape, then mark the matched span. Offsets are into the stored text
    layer; if they fall outside this rendering they are ignored rather than
    silently shifting the highlight onto the wrong words."""
    if not text:
        return '<span class="muted">(no transcription on file for this page)</span>'
    if start is not None and end is not None and 0 <= start < end <= len(text):
        pre, mid, post = text[:start], text[start:end], text[end:]
        if len(mid) > cap:
            mid = mid[:cap] + " …"
        pre = pre[-400:]
        post = post[:400]
        return (f'<span class="muted">…{html.escape(pre)}</span>'
                f'<mark class="hl">{html.escape(mid)}</mark>'
                f'<span class="muted">{html.escape(post)}…</span>')
    body = text[:cap] + (" …" if len(text) > cap else "")
    return html.escape(body)


def render(rows_by_shade, counts, meta) -> str:
    P: List[str] = []
    A = P.append
    A('<!doctype html>\n<html lang="he" dir="rtl">\n<head>\n<meta charset="utf-8">')
    A('<meta name="robots" content="noindex,nofollow">')
    A('<meta name="viewport" content="width=device-width,initial-scale=1">')
    A("<title>Novelty axis — local review (PRIVATE)</title>\n<style>")
    A("""
:root{--bg:#0f1115;--panel:#171a21;--panel2:#1e222b;--line:#2a2f3a;--fg:#e7e9ee;--mut:#9aa3b2;
      --A:#22c55e;--H:#38bdf8;--C:#a78bfa;--R:#f59e0b;--W:#f472b6;--N:#64748b;}
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
main{padding:18px 20px;max-width:1400px;margin:0 auto}
section{display:none}section.active{display:block}
.cards{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px;min-width:120px}
.card .n{font-size:20px;font-weight:700}.card .l{color:var(--mut);font-size:11.5px}
.intro{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 16px;margin-bottom:14px}
.case{background:var(--panel);border:1px solid var(--line);border-radius:11px;margin:14px 0;overflow:hidden}
.case>.head{padding:10px 14px;border-bottom:1px solid var(--line);background:var(--panel2)}
.case .ms{font-weight:600}.case .claim{color:var(--H)}
.panes{display:grid;grid-template-columns:1fr 1fr;gap:0}
@media(max-width:900px){.panes{grid-template-columns:1fr}}
.pane{padding:12px 14px;min-width:0}
.pane+.pane{border-right:1px solid var(--line)}
@media(max-width:900px){.pane+.pane{border-right:none;border-top:1px solid var(--line)}}
.pane h4{margin:0 0 8px;font-size:12px;color:var(--mut);font-weight:600;letter-spacing:.02em}
.txt{white-space:pre-wrap;word-break:break-word;font-size:13px;max-height:340px;overflow:auto;
     background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:10px}
.src{margin-bottom:9px}
.src .lab{font-size:11.5px;color:var(--mut);margin-bottom:2px}
.src .val{background:var(--panel2);border:1px solid var(--line);border-radius:7px;padding:7px 9px;font-size:13px;
          white-space:pre-wrap;word-break:break-word;max-height:150px;overflow:auto}
.none{color:var(--mut);font-style:italic}
mark.hl{background:#facc15;color:#000;border-radius:3px;padding:0 2px}
.pill{display:inline-block;padding:1px 7px;border-radius:6px;border:1px solid var(--line);font-size:11px;color:var(--mut);margin-inline-start:6px}
.ships{background:var(--A);color:#04121c;border:none;font-weight:600}
.hidden-def{background:var(--R);color:#04121c;border:none;font-weight:600}
.muted{color:var(--mut)}
""")
    A("</style></head><body>")
    A('<header><h1>ציר החידוש — סקירה מקומית / Novelty axis — local review</h1>')
    A(f'<div class="sub">{meta["total"]:,} verdicts · sampled {meta["sampled"]} cases · '
      f'model {html.escape(meta["model"])} · real spend ${meta["cost"]}</div></header>')
    A('<div class="warn">PRIVATE — local review only. Renders our own transcription and the four checked '
      'finding aids; no reference-corpus text is included. Do not publish or share.</div>')
    A("<nav>")
    for i, sh in enumerate(SHADE_ORDER):
        if sh not in rows_by_shade:
            continue
        cls = " class='active'" if i == 0 else ""
        n = f"{counts.get(sh, 0):,}"
        A(f"<button onclick=\"go('{sh}')\" id=\"b-{sh}\"{cls}>"
          f'{sh} <span class="muted">({n})</span></button>')
    A("</nav><main>")

    first = True
    for sh in SHADE_ORDER:
        rows = rows_by_shade.get(sh)
        if not rows:
            continue
        scls = " class='active'" if first else ""
        A(f'<section id="s-{sh}"{scls}>')
        first = False
        badge = ('<span class="pill ships">ships as a candidate find</span>' if sh == SHIPS_AS_CANDIDATE
                 else '<span class="pill hidden-def">hidden by default</span>'
                 if sh in ("diverges_work", "diverges_part") else "")
        A(f'<div class="intro"><b>{html.escape(SHADE_LABEL[sh])}</b> {badge}'
          f'<div class="sub">{counts.get(sh,0):,} rows in the corpus · showing {len(rows)}</div></div>')
        for r in rows:
            A('<div class="case"><div class="head">')
            A(f'<div class="ms">{html.escape(r["manuscript"])} '
              f'<span class="muted">({html.escape(r["sys_id"])})</span></div>')
            A(f'<div>טענה / our claim: <span class="claim">{html.escape(r["claim"])}</span>')
            if r["matched_letters"]:
                A(f'<span class="pill">{r["matched_letters"]:,} matched letters</span>')
            if r["density"] is not None:
                A(f'<span class="pill">{r["density"]*100:.0f}% of page</span>')
            A("</div></div>")
            A('<div class="panes">')
            A('<div class="pane"><h4>כתב היד — התעתיק שלנו / The manuscript, our transcription</h4>')
            A(f'<div class="txt">{r["page_html"]}</div></div>')
            A('<div class="pane"><h4>מה אומרים כלי העזר / What the checked finding aids say</h4>')
            for key, lab in _SRC_LABEL:
                val = r["sources"].get(key)
                A('<div class="src">')
                A(f'<div class="lab">{html.escape(lab)}</div>')
                A(f'<div class="val">{html.escape(val)}</div>' if val
                  else '<div class="val none">— none on file —</div>')
                A("</div>")
            A("</div></div></div>")
        A("</section>")
    A("</main><script>")
    A("function go(s){document.querySelectorAll('section').forEach(e=>e.classList.remove('active'));"
      "document.querySelectorAll('nav button').forEach(e=>e.classList.remove('active'));"
      "document.getElementById('s-'+s).classList.add('active');"
      "document.getElementById('b-'+s).classList.add('active');window.scrollTo(0,0);}")
    A("</script></body></html>")
    return "\n".join(P)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--verdicts", default=DEFAULT_VERDICTS)
    p.add_argument("--asset", default=DEFAULT_ASSET)
    p.add_argument("--transcriptions", default=DEFAULT_TRANSCRIPTIONS)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--per-shade", type=int, default=8)
    p.add_argument("--fills-gap", type=int, default=25)
    p.add_argument("--seed", type=int, default=20260803)
    p.add_argument("--cost", default="40.12")
    args = p.parse_args(argv)

    print("loading verdicts…")
    verdicts = load_verdicts(args.verdicts)
    counts = collections.Counter(verdicts.values())
    picks = sample_by_shade(verdicts, args.per_shade, args.fills_gap, args.seed)
    n_sampled = sum(len(v) for v in picks.values())
    print(f"{len(verdicts):,} verdicts; sampling {n_sampled} cases across {len(picks)} shades")

    print("rebuilding candidate evidence…")
    candidates, works, libraries = build_all_candidates(
        args.asset, DEFAULT_LIBRARIES_CSV, DEFAULT_FJMS_DB, DEFAULT_PGP_DB, DEFAULT_FGP_DB
    )
    by_key = {f"{c.sys_id}::{c.ref_work_id}": c for c in candidates}

    con = sqlite3.connect(f"file:{args.asset}?mode=ro", uri=True)
    cur = con.cursor()

    staged: Dict[str, List[dict]] = {}
    wanted_pages = set()
    for shade, keys in picks.items():
        out = []
        for key in keys:
            c = by_key.get(key)
            if c is None:
                continue
            page, ml, dens, s0, s1 = page_and_metrics(cur, c.sys_id, c.ref_work_id)
            if page:
                wanted_pages.add(page)
            bundle = assemble_evidence_bundle(c)
            out.append({
                "sys_id": c.sys_id,
                "manuscript": libraries.get(c.sys_id, {}).get("shelfmark") or f"sys_id {c.sys_id}",
                "claim": c.claimed_title + (f" ({c.claimed_author})" if c.claimed_author else ""),
                "page": page, "matched_letters": ml, "density": dens, "span": (s0, s1),
                "sources": {k: " ||| ".join(t for t in bundle.get(k, ()) if t) for k, _ in _SRC_LABEL},
            })
        staged[shade] = out

    print(f"streaming transcriptions for {len(wanted_pages):,} pages…")
    texts = load_page_texts(args.transcriptions, wanted_pages)
    print(f"  found {len(texts):,}")

    for shade, rows in staged.items():
        for r in rows:
            r["page_html"] = highlight(texts.get(r["page"], ""), *r["span"])

    meta = {"total": len(verdicts), "sampled": n_sampled,
            "model": "gemini-3.6-flash (effort=low, batch 10)", "cost": args.cost}
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(render(staged, counts, meta))
    print(f"wrote {args.out} ({os.path.getsize(args.out)/1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
