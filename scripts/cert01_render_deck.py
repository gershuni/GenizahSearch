"""CERT-01 deck rendering (Phase 135, plan 135-09, Task 2 correction).

Renders the FROZEN 280-card discovery deck
(`same_work_spike/probe/data/cert01_deck_key.json`) to a gradable,
catalogue-blind HTML page, reusing `e1_deck.render_deck`/`adapt_template`
VERBATIM (never re-derives the excerpt/reference-alignment logic). Does NOT
redraw or resample -- every uid, in the FROZEN order bound by
`cert01_deck_manifest.json`, is rendered exactly as drawn; this script only
enriches each frozen card with the render-time inputs `render_deck` needs
(matched span, neutral title, the raw research work_id) and NEVER modifies
`cert01_deck_key.json`, `cert01_deck_manifest.json`, or `cert01_prereg.json`.

Two data-provenance wrinkles this script resolves:

1. `render_deck` looks up reference text and the `ref_corpus_v2.pkl` work
   metadata by the RAW research work_id (`M:.../J:.../REF2:...` form), but
   the CERT-01 estimand's `canonical_work_id` is the OPAQUE product id
   (`w000xxx`). Candidate/diagnostic_retained/diagnostic_demoted cards are
   resolved back to their raw id via the REVERSE of
   `discovery_data/crosswalk.json` (the same crosswalk the sidecar build
   used to mint the opaque id in the first place -- never re-derived).
   Gold cards (drawn from `e1_adjudicated_a.jsonl`, a population that
   predates the discovery sidecar) already carry the raw id directly under
   the deck card's `canonical_work_id` field (a naming artifact of Task 2's
   card shaping, not a masking issue) and need no crosswalk lookup.
2. `matched_letters`-adjacent `span_start`/`span_end` for candidate/
   diagnostic_retained cards come from the discovery sidecar's
   `discovery_evidence` row (via `cert01_frame.compute_estimand_rows`,
   which now carries them as rendering-only fields -- NEVER part of
   `population_hash`/`cluster_map_hash`, see that module); diagnostic_demoted
   cards are re-queried the same way, scoped to `routing_reason=
   'later_shared_text'`; gold cards already carry `o0`/`o1` directly.

Usage:
    python -X utf8 scripts/cert01_render_deck.py --write

Writes (gitignored, dev-box only, never tracked):
    same_work_spike/probe/review/cert01_deck.html
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import cert01_frame as cf  # noqa: E402

_PROBE_SCRIPTS = REPO_ROOT / "same_work_spike" / "probe" / "scripts"


def load_e1_render_deps():
    """Lazily import the E1 render harness + reference-text resolver from
    the gitignored dev-box research tree (never at module import time)."""
    if not _PROBE_SCRIPTS.exists():
        raise RuntimeError(
            f"e1_deck.py not found at {_PROBE_SCRIPTS} -- this script must run "
            "on the dev box carrying the same_work_spike/probe research tree."
        )
    if str(_PROBE_SCRIPTS) not in sys.path:
        sys.path.insert(0, str(_PROBE_SCRIPTS))
    import e1_deck as e1
    from build_smoke_preview2 import RefText
    return e1, RefText


PHASE_DIR = REPO_ROOT / ".planning" / "phases" / "135-precision-certificate-confidence-bands"
DECK_MANIFEST_PATH = PHASE_DIR / "cert01_deck_manifest.json"

SIDECAR_DB = REPO_ROOT / "discovery_data" / "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db"
RESEARCH_DB = REPO_ROOT / "same_work_spike" / "probe" / "data" / "fullcorpus_v2.db"
CROSSWALK_PATH = REPO_ROOT / "discovery_data" / "crosswalk.json"
GOLD_POOL_PATH = REPO_ROOT / "same_work_spike" / "probe" / "data" / "e1_adjudicated_a.jsonl"
REF_CORPUS_PKL = REPO_ROOT / "same_work_spike" / "probe" / "data" / "ref_corpus_v2.pkl"

DECK_KEY_PATH = REPO_ROOT / "same_work_spike" / "probe" / "data" / "cert01_deck_key.json"
HTML_OUT = REPO_ROOT / "same_work_spike" / "probe" / "review" / "cert01_deck.html"

DECK_NAME = "cert01"
DECK_LABEL = "CERT-01 tier_a"


def load_frozen_deck() -> Tuple[List[dict], dict]:
    data = json.loads(DECK_KEY_PATH.read_text(encoding="utf-8"))
    return data["cards"], data["meta"]


def load_gold_pool() -> Dict[Tuple[str, str], dict]:
    out = {}
    if not GOLD_POOL_PATH.exists():
        return out
    with open(GOLD_POOL_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            out[(r["page_id"], r["work_id"])] = r
    return out


def load_demoted_spans(sidecar_db_path) -> Dict[Tuple[str, str], dict]:
    """(a_page_id, canonical_work_id) -> {span_start, span_end, sys_id} for
    every routing_reason='later_shared_text' row -- the diagnostic_demoted
    card population (protocol §8)."""
    conn = cf._connect_ro(sidecar_db_path)
    try:
        rows = conn.execute(
            """
            SELECT de.a_page_id, w.canonical_work_id, de.sys_id, de.span_start, de.span_end
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w ON w.work_id = dc.work_id
            WHERE de.routing_reason = 'later_shared_text'
            """
        ).fetchall()
    finally:
        conn.close()
    out = {}
    for a_page_id, canonical_work_id, sys_id, span_start, span_end in rows:
        out.setdefault((a_page_id, canonical_work_id), {
            "sys_id": sys_id, "span_start": span_start, "span_end": span_end,
        })
    return out


def load_neutral_titles(sidecar_db_path, canonical_work_ids) -> Dict[str, str]:
    conn = cf._connect_ro(sidecar_db_path)
    try:
        placeholders = ",".join("?" for _ in canonical_work_ids)
        rows = conn.execute(
            f"SELECT canonical_work_id, neutral_title FROM works WHERE canonical_work_id IN ({placeholders})",
            list(canonical_work_ids),
        ).fetchall() if canonical_work_ids else []
    finally:
        conn.close()
    out = {}
    for cw, title in rows:
        out.setdefault(cw, title)
    return out


def build_reverse_crosswalk(crosswalk_path) -> Dict[str, str]:
    crosswalk = json.loads(Path(crosswalk_path).read_text(encoding="utf-8"))
    return {opaque: raw for raw, opaque in crosswalk.items()}


def build_render_items(deck_cards: List[dict], *, sidecar_db_path, research_db_path,
                       crosswalk_path) -> List[dict]:
    """Builds the `items` list `e1_deck.render_deck` expects, in the FROZEN
    deck order, without redrawing or resampling anything."""
    estimand_rows = cf.compute_estimand_rows(str(sidecar_db_path), str(research_db_path))
    estimand_by_key = {(r["page_id"], r["canonical_work_id"]): r for r in estimand_rows}

    gold_pool = load_gold_pool()
    demoted_spans = load_demoted_spans(sidecar_db_path)
    reverse_crosswalk = build_reverse_crosswalk(crosswalk_path)

    candidate_like_work_ids = {
        c["canonical_work_id"] for c in deck_cards if c["role"] != "gold"
    }
    neutral_titles = load_neutral_titles(sidecar_db_path, candidate_like_work_ids)

    items = []
    for idx, card in enumerate(deck_cards, 1):
        uid = card["uid"]
        page_id = card["page_id"]
        cw = card["canonical_work_id"]
        sys_id = card["sys_id"]

        if card["role"] == "gold":
            gold_row = gold_pool.get((page_id, cw))
            if gold_row is None:
                raise ValueError(f"gold card uid={uid!r} not found in e1_adjudicated_a.jsonl "
                                  "(the gold pool must be unchanged since the Task 2 draw)")
            raw_work_id = cw  # e1_adjudicated_a.jsonl work_id is already the raw research id
            o0, o1 = gold_row["o0"], gold_row["o1"]
            work_title = gold_row.get("work_title") or ""
        elif card["role"] == "diagnostic_demoted":
            span_row = demoted_spans.get((page_id, cw))
            if span_row is None:
                raise ValueError(f"diagnostic_demoted card uid={uid!r} has no matching "
                                  "later_shared_text evidence row on the deployed sidecar")
            raw_work_id = reverse_crosswalk.get(cw)
            o0, o1 = span_row["span_start"], span_row["span_end"]
            work_title = neutral_titles.get(cw, "")
        else:  # candidate / diagnostic_retained -- both drawn from the tier_a estimand
            est_row = estimand_by_key.get((page_id, cw))
            if est_row is None:
                raise ValueError(f"{card['role']} card uid={uid!r} not found in the "
                                  "recomputed tier_a estimand (sidecar drift?)")
            raw_work_id = reverse_crosswalk.get(cw)
            o0, o1 = est_row["span_start"], est_row["span_end"]
            work_title = neutral_titles.get(cw, "")

        if raw_work_id is None:
            raise ValueError(f"card uid={uid!r} (canonical_work_id={cw!r}) has no reverse-crosswalk "
                              "entry -- cannot resolve to a reference-corpus raw work_id")

        items.append({
            "no": idx,
            "uid": uid,
            "role": "candidate",  # placeholder only -- render_deck never emits it (verified by inspection)
            "band": None,
            "row": {
                "page_id": page_id,
                "sys_id": sys_id,
                "work_id": raw_work_id,
                "o0": o0,
                "o1": o1,
                "work_title": work_title,
                "cat": "",  # NEVER source_corpus (D-03a: masked, internal-only, never displayed)
            },
        })
    return items


# ---------------------------------------------------------------------------
# Post-render HTML patch: add a "Grader" field + rewrite exportV() to emit
# the ledger LIST shape {uid, verdict, grader, ...} the validator reads,
# instead of e1_deck's native dict-of-dicts shape. Does NOT touch vote()/
# reveal()/revealOpen() -- the reveal-lock + blind-pane discipline is
# reused UNMODIFIED.
# ---------------------------------------------------------------------------

def patch_export_for_ledger_shape(html: str, deck_no: str) -> str:
    old_toolbar = " <button onclick=exportV()>Export verdicts</button>\n</div>"
    new_toolbar = (
        ' <label style="font-size:12px;color:#89a">Grader: '
        '<input id=graderName size=14 placeholder="your name"></label>\n'
        " <button onclick=exportV()>Export verdicts</button>\n</div>"
    )
    if old_toolbar not in html:
        raise ValueError("toolbar anchor text not found -- render_deck's template may have changed")
    html = html.replace(old_toolbar, new_toolbar, 1)

    old_export_fn = (
        "function exportV(){\n"
        " const out={};for(const it of ITEMS){if(store[it.uid])out[it.uid]=store[it.uid];}\n"
        " const b=new Blob([JSON.stringify(out,null,1)],{type:'application/json'});\n"
        " const a=document.createElement('a');\n"
        f" a.href=URL.createObjectURL(b);a.download='e1_deck{deck_no}_verdicts.json';a.click();}}"
    )
    new_export_fn = (
        "function exportV(){\n"
        " const grader=((document.getElementById('graderName')||{}).value||'').trim();\n"
        " if(!grader){alert('הזן שם שופט/ת לפני ייצוא / enter a grader name before exporting');return;}\n"
        " const out=[];\n"
        " for(const it of ITEMS){const rec=store[it.uid];if(rec&&rec.verdict){\n"
        "  out.push({uid:it.uid,verdict:rec.verdict,grader:grader,notes:rec.notes||'',"
        "revealed:!!rec.revealed,post_verdict:rec.post_verdict||null});}}\n"
        " const b=new Blob([JSON.stringify(out,null,1)],{type:'application/json'});\n"
        " const a=document.createElement('a');\n"
        " a.href=URL.createObjectURL(b);a.download='cert01_deck_verdicts.json';a.click();}"
    )
    if old_export_fn not in html:
        raise ValueError("exportV() anchor text not found -- render_deck's template may have changed")
    html = html.replace(old_export_fn, new_export_fn, 1)

    old_init_anchor = (
        "bi.onchange=()=>{BASE=bi.value.trim()||BASE;localStorage.setItem('q2adj_base',BASE);lastSrc='';\n"
        " if(CARDS[i])setFrame(CARDS[i]);};\n"
        "mkChecks('f_cat',counts(c=>c.cat));"
    )
    new_init = (
        "bi.onchange=()=>{BASE=bi.value.trim()||BASE;localStorage.setItem('q2adj_base',BASE);lastSrc='';\n"
        " if(CARDS[i])setFrame(CARDS[i]);};\n"
        "const gi=document.getElementById('graderName');\n"
        "if(gi){gi.value=localStorage.getItem('cert01_grader')||'';\n"
        " gi.onchange=()=>localStorage.setItem('cert01_grader',gi.value);}\n"
        "mkChecks('f_cat',counts(c=>c.cat));"
    )
    if old_init_anchor not in html:
        raise ValueError("init anchor text not found -- render_deck's template may have changed")
    html = html.replace(old_init_anchor, new_init, 1)

    return html


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sidecar-db", default=str(SIDECAR_DB))
    ap.add_argument("--research-db", default=str(RESEARCH_DB))
    ap.add_argument("--crosswalk", default=str(CROSSWALK_PATH))
    args = ap.parse_args(argv)

    e1, RefText = load_e1_render_deps()

    deck_cards, deck_meta = load_frozen_deck()
    print(f"loaded frozen deck: {len(deck_cards)} cards (prereg report_id="
          f"{deck_meta.get('prereg_report_id')})", flush=True)

    dm = json.loads(DECK_MANIFEST_PATH.read_text(encoding="utf-8"))
    if dm.get("prereg_report_id") != deck_meta.get("prereg_report_id"):
        raise ValueError("cert01_deck_key.json's bound report_id does not match "
                          "cert01_deck_manifest.json -- refusing to render a mismatched deck")

    items = build_render_items(
        deck_cards, sidecar_db_path=args.sidecar_db, research_db_path=args.research_db,
        crosswalk_path=args.crosswalk,
    )
    assert [it["uid"] for it in items] == [c["uid"] for c in deck_cards], \
        "rendered item order/uids must exactly match the frozen deck -- no redraw, no resample"
    print(f"resolved render inputs (span + raw work_id + neutral title) for all {len(items)} cards",
          flush=True)

    print("loading reference corpus + research DB connection ...", flush=True)
    works = pickle.load(open(REF_CORPUS_PKL, "rb"))
    works_by_id = {w["id"]: w for w in works}
    reftext = RefText()
    research_conn = cf._connect_ro(args.research_db)

    print("rendering (reusing e1_deck.render_deck / adapt_template verbatim) ...", flush=True)
    try:
        html = e1.render_deck(DECK_NAME, items, research_conn, works_by_id, reftext, DECK_LABEL)
    finally:
        research_conn.close()

    html = patch_export_for_ledger_shape(html, DECK_NAME)

    # Catalogue-blind re-assertion on the rendered payload itself (not just
    # the source cards): the embedded __DATA__ JSON must carry no demotion
    # field. render_deck's own per-card dict never includes one (verified by
    # direct code inspection); this is a defense-in-depth re-check.
    for forbidden in ("later_shared_text", "routing_status"):
        if f'"{forbidden}"' in html:
            raise ValueError(f"rendered deck HTML unexpectedly contains {forbidden!r} -- ABORT")

    print(f"rendered HTML: {len(html):,} bytes", flush=True)

    if args.write:
        HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
        HTML_OUT.write_text(html, encoding="utf-8")
        print(f"wrote {HTML_OUT} (gitignored)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
