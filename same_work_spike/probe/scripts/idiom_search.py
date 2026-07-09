# -*- coding: utf-8 -*-
"""Idiom search (SEED-029 fragment-ID, scholar-in-the-loop).

Hillel's method (2026-07-09): a short whole fragment can FAIL page-level
identification (too little text overall) yet contain a DISTINCTIVE idiom that
pins it exactly. And "no page-level reference match" != "not in the corpus" --
the work may be present but the fragment too short/HTR-noisy for the page
engine. So: take a CORRECTED (clean) distinctive phrase and search it, with
HTR-noise tolerance, across
  (a) the reference corpora (ref_corpus.pkl: Maagarim + JA [+ Sefaria once
      integrated]) -> is this idiom in a KNOWN work? (identification)
  (b) every Genizah page (fullcorpus.db) -> in-Genizah PARALLELS / other
      witnesses, even when no reference names the work (discovery).

The query is clean (human-corrected); reference targets are clean; Genizah
targets are HTR-noisy -> tolerance (max_edits) matters mainly on side (b).
Matches are reported with context for HUMAN judgement -- nothing auto-accepted.

Usage:
  python -X utf8 -u idiom_search.py --phrase "לא תנאף ותטמא" \
      [--shelf "T-S NS 274.68"] [--max-edits 2] [--context 30] [--limit 60]
Out: prints ranked hits; writes results/idiom_<slug>.md
"""
import argparse
import csv
import re
import sqlite3
import sys
import time
import pickle

from rapidfuzz.distance import Levenshtein

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
LIBS = ROOT + r"\libraries.csv"
K = 5


def norm_shelf(s):
    return re.sub(r'[\s,]', '', s.lower())


def load_shelf_map():
    """sys_id -> first human-readable call number (for display)."""
    out = {}
    with open(LIBS, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 3 and row[0] and row[2]:
                out[row[0]] = row[2].split('|')[0].strip()
    return out


def best_window_edits(stream, phrase, anchors, slack):
    """Min Levenshtein distance between `phrase` and any ~len(phrase) window of
    `stream` near an anchor position. anchors = positions where a shared gram
    starts. Returns (best_dist, best_window_text, best_pos) or (None,None,None)."""
    L = len(phrase)
    best = (None, None, None)
    seen = set()
    for a in anchors:
        # slide the window start across a neighborhood of the anchor
        for start in range(max(0, a - slack), min(len(stream) - 1, a + slack) + 1):
            if start in seen:
                continue
            seen.add(start)
            for wl in (L, L + 1, L - 1, L + 2, L - 2):
                if wl < 3 or start + wl > len(stream):
                    continue
                w = stream[start:start + wl]
                d = Levenshtein.distance(phrase, w)
                if best[0] is None or d < best[0]:
                    best = (d, w, start)
                    if d == 0:
                        return best
    return best


def gram_anchors(stream, grams):
    pos = []
    for g in grams:
        i = stream.find(g)
        while i != -1:
            pos.append(i)
            i = stream.find(g, i + 1)
    return sorted(set(pos))


def search_targets(iter_targets, phrase, grams, max_edits, slack, context,
                    label_fn):
    """iter_targets yields (key, stream). Returns ranked list of hit dicts."""
    hits = []
    gset = grams
    for key, stream in iter_targets:
        if not stream:
            continue
        # cheap prefilter: must contain at least one exact phrase gram
        if not any(g in stream for g in gset):
            continue
        anchors = gram_anchors(stream, gset)
        d, w, p = best_window_edits(stream, phrase, anchors, slack)
        if d is None or d > max_edits:
            continue
        ctx = stream[max(0, p - context):p + len(w) + context]
        hits.append({'key': key, 'dist': d, 'window': w, 'pos': p,
                     'context': ctx, 'label': label_fn(key)})
    hits.sort(key=lambda h: (h['dist'], h['key']))
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--phrase', required=True)
    ap.add_argument('--shelf', default=None,
                     help='source shelfmark, to flag/exclude the origin fragment')
    ap.add_argument('--max-edits', type=int, default=None,
                     help='max Levenshtein distance to accept (default '
                          'round(0.2*len), min 1)')
    ap.add_argument('--context', type=int, default=30)
    ap.add_argument('--slack', type=int, default=6)
    ap.add_argument('--limit', type=int, default=60)
    args = ap.parse_args()

    t0 = time.time()
    phrase, _ = norm_stream(args.phrase)
    if len(phrase) < K:
        raise SystemExit(f"phrase too short after normalization: {phrase!r}")
    grams = list({phrase[i:i + K] for i in range(len(phrase) - K + 1)})
    max_edits = args.max_edits if args.max_edits is not None \
        else max(1, round(0.2 * len(phrase)))
    print(f"phrase='{args.phrase}' -> norm='{phrase}' (len {len(phrase)}); "
          f"{len(grams)} distinct {K}-grams; max_edits={max_edits}", flush=True)

    shelf_map = load_shelf_map()
    inv_shelf = {}
    if args.shelf:
        tgt = norm_shelf(args.shelf)
        for sid, call in shelf_map.items():
            if tgt in norm_shelf(call):
                inv_shelf[sid] = call
        print(f"source shelfmark '{args.shelf}' -> sys_ids {list(inv_shelf)}",
              flush=True)

    # ---- (a) reference corpora ----
    works = pickle.load(open(REF, 'rb'))
    ref_hits = search_targets(
        ((w['id'], w['stream']) for w in works),
        phrase, grams, max_edits, args.slack, args.context,
        label_fn=lambda k: next((f"{w.get('author','')} — {w.get('title','')}"
                                 for w in works if w['id'] == k), k))
    print(f"[refs] {len(ref_hits)} work(s) contain the idiom "
          f"(<= {max_edits} edits) ({time.time() - t0:.0f}s)", flush=True)

    # ---- (b) Genizah pages ----
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")

    def gen_pages():
        for pid, sid, txt in con.execute(
                "SELECT page_id, sys_id, text FROM pages"):
            s, _ = norm_stream(txt)
            yield ((pid, sid), s)

    gen_hits = search_targets(
        gen_pages(), phrase, grams, max_edits, args.slack, args.context,
        label_fn=lambda k: shelf_map.get(k[1], k[1]))
    con.close()
    # split origin vs parallels
    origin = [h for h in gen_hits if h['key'][1] in inv_shelf]
    parallels = [h for h in gen_hits if h['key'][1] not in inv_shelf]
    print(f"[genizah] {len(gen_hits)} page hit(s): {len(origin)} on the source "
          f"shelfmark, {len(parallels)} OTHER pages (parallels) "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- report ----
    slug = re.sub(r'[^a-z0-9]+', '-', (args.shelf or phrase[:12]).lower()).strip('-')
    out = PROBE + rf"\results\idiom_{slug}.md"
    lines = [f"# Idiom search — `{args.phrase}`", "",
             f"normalized: `{phrase}` (len {len(phrase)}); max_edits={max_edits}; "
             f"source shelfmark: {args.shelf or '(none given)'}", ""]
    if origin:
        lines += ["## Source fragment (origin)"]
        for h in origin:
            lines.append(f"- `{h['key'][0]}` [{h['label']}] dist={h['dist']} "
                         f"· …{h['context']}…")
        lines.append("")
    lines += [f"## (a) Reference works containing the idiom — {len(ref_hits)}",
              "(a hit here = the idiom IS in a known/edited work; the page "
              "engine may have missed the short fragment)"]
    if ref_hits:
        for h in ref_hits[:args.limit]:
            lines.append(f"- **{h['label']}** (`{h['key']}`) dist={h['dist']} "
                         f"· matched `{h['window']}` · …{h['context']}…")
    else:
        lines.append("- (none — the idiom is not in the reference corpora "
                     "within tolerance)")
    lines += ["", f"## (b) In-Genizah parallels (other pages) — {len(parallels)}",
              "(other Genizah pages carrying the same idiom = witnesses / "
              "parallels, even when no reference names the work)"]
    if parallels:
        for h in parallels[:args.limit]:
            pid, sid = h['key']
            lines.append(f"- **{h['label']}** (sys {sid}, `{pid}`) "
                         f"dist={h['dist']} · matched `{h['window']}` · "
                         f"…{h['context']}…")
    else:
        lines.append("- (none within tolerance)")
    lines += ["", f"_runtime {time.time() - t0:.0f}s_"]
    open(out, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:40]))
    print(f"\nwrote {out}", flush=True)


if __name__ == '__main__':
    main()
