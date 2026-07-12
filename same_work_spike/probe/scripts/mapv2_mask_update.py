# -*- coding: utf-8 -*-
"""Incremental ref-canon mask update for the rebuilt reference corpus (v2).

The existing data/ref_canon_masks.json was a 16h full pass over the OLD
(prefix-buggy) ref_corpus.pkl. The v2 rebuild left all but 2 v1 works
byte-identical (their masks stay valid) and added 58 REF2:* works. This
script re-masks ONLY the changed/new works against the v2 canonical index
and merges with the existing masks — no full re-run.

Steps: diff v2 vs the v1 backup -> assert canonical works unchanged ->
regression-check 3 unchanged works (fresh intervals must EQUAL the
existing json) -> mask the ~60 targets -> merge -> write v2 masks + delta
report. data/ref_canon_masks.json itself is NOT modified.

NOTE: REF2 Liturgy works are EXPECTED to be heavily masked against Bible
(Hallel is ~0.97 contained in Psalms) — correct behavior, not a bug.

Usage: python -X utf8 -u mapv2_mask_update.py
Out: data/ref_canon_masks_v2.json, results/ref_canon_masks_v2_delta.md
"""
import json
import pickle
import sys
import time

from mask_ref_canon import CANON_CATS, mask_edited_works, mask_one_work
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
V2 = ROOT + r"\same_work_spike\probe\data\ref_corpus_v2.pkl"
BAK = ROOT + r"\same_work_spike\probe\data\ref_corpus_v1_prefix_buggy.pkl.bak"
OLD_MASKS = ROOT + r"\same_work_spike\probe\data\ref_canon_masks.json"
OUT = ROOT + r"\same_work_spike\probe\data\ref_canon_masks_v2.json"
REPORT = ROOT + r"\same_work_spike\probe\results\ref_canon_masks_v2_delta.md"
N_REGRESSION = 3          # unchanged works re-masked to prove parity


def masked_letters(iv):
    return sum(b - a for a, b in iv)


def main():
    t0 = time.time()
    v2 = pickle.load(open(V2, 'rb'))
    bak = pickle.load(open(BAK, 'rb'))
    old_masks = json.load(open(OLD_MASKS, encoding='utf-8'))
    print(f"v2: {len(v2)} works; backup: {len(bak)} works; "
          f"existing masks: {len(old_masks)} works", flush=True)

    bak_stream = {w['id']: w['stream'] for w in bak}
    v2_ids = {w['id'] for w in v2}

    # -- canonical works must be byte-identical (they ARE the index;
    #    if they changed, ALL existing masks would be suspect)
    canon_v2 = [w for w in v2 if w['cat'] in CANON_CATS]
    canon_bak_ids = {w['id'] for w in bak if w['cat'] in CANON_CATS}
    canon_changed = [w['id'] for w in canon_v2
                     if bak_stream.get(w['id']) != w['stream']]
    if canon_changed or {w['id'] for w in canon_v2} != canon_bak_ids:
        # Codex code-gate BLOCKER 1: the canonical works ARE the index every
        # existing mask was computed against — if they changed, EVERY
        # carried-over mask is suspect, not just the target set. Incremental
        # update is then invalid; a full mask rebuild (mask_ref_canon.py,
        # ~16h) is required. Abort rather than merge stale masks.
        print(f"ABORT: canonical works differ between backup and v2: "
              f"changed={canon_changed} "
              f"set_delta={ {w['id'] for w in canon_v2} ^ canon_bak_ids }"
              f" — incremental update invalid; run the FULL mask rebuild",
              flush=True)
        sys.exit(1)
    print(f"canonical works identical: {len(canon_v2)} works", flush=True)

    # -- targets: non-canonical v2 works that are new or stream-changed
    targets = [w for w in v2 if w['cat'] not in CANON_CATS
               and bak_stream.get(w['id']) != w['stream']]
    tgt_ids = {w['id'] for w in targets}
    n_new = sum(1 for w in targets if w['id'] not in bak_stream)
    print(f"targets: {len(targets)} works ({n_new} new, "
          f"{len(targets) - n_new} stream-changed):", flush=True)
    for w in targets:
        kind = 'NEW' if w['id'] not in bak_stream else \
            f"changed {len(w['stream']) - len(bak_stream[w['id']]):+,}"
        print(f"  [{kind}] {w['id']} ({w['cat']}, "
              f"{len(w['stream']):,} let) {w['title'][:50]}", flush=True)
    unexpected = [i for i in tgt_ids if not i.startswith('REF2:')
                  and i not in ('M:Ytext280002', 'M:Ytext721003')]
    if unexpected:
        print(f"WARNING: unexpected targets beyond the known set: "
              f"{unexpected}", flush=True)
    gone = [i for i in bak_stream if i not in v2_ids]
    if gone:
        print(f"WARNING: {len(gone)} backup works absent from v2 "
              f"(mask entries kept, harmless): {gone[:10]}", flush=True)

    # -- canonical index from v2 (exactly as mask_ref_canon.main)
    print(f"building canonical index: {len(canon_v2)} works "
          f"({sum(len(w['stream']) for w in canon_v2):,} letters)...",
          flush=True)
    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped) = build_ref_index(canon_v2)
    print(f"index: {len(seg_streams):,} segments, {len(codes_f):,} "
          f"postings ({time.time() - t0:.0f}s)", flush=True)

    # -- regression: 3 largest unchanged masked works must reproduce
    #    their existing intervals exactly on the v2 index
    reg_rows = sorted(((masked_letters(iv), wid)
                       for wid, iv in old_masks.items()
                       if wid not in tgt_ids and wid in v2_ids),
                      reverse=True)[:N_REGRESSION]
    v2_by_id = {w['id']: w for w in v2}
    reg_fail = 0
    for m, wid in reg_rows:
        stats_r = {'hits': 0, 'cand': 0, 'accepted': 0}
        tr = time.time()
        fresh = mask_one_work(v2_by_id[wid]['stream'], seg_streams,
                              codes_f, seg_f, pos_f, stats_r)
        same = ([list(p) for p in (fresh or [])]
                == [list(p) for p in old_masks[wid]])
        print(f"regression {wid}: {'IDENTICAL' if same else 'MISMATCH'} "
              f"({len(old_masks[wid])} intervals, {m:,} letters, "
              f"{time.time() - tr:.1f}s)", flush=True)
        if not same:
            reg_fail += 1
            print(f"  old: {old_masks[wid][:5]} ...", flush=True)
            print(f"  new: {(fresh or [])[:5]} ...", flush=True)
    if reg_fail:
        print(f"REGRESSION FAILED ({reg_fail}/{len(reg_rows)}) — "
              f"aborting, investigate before proceeding", flush=True)
        sys.exit(1)
    print(f"regression: {len(reg_rows)}/{len(reg_rows)} identical — "
          f"refactor + v2 index preserve behavior", flush=True)

    # -- mask the targets
    stats = {'hits': 0, 'cand': 0, 'accepted': 0}
    tm = time.time()
    last = [tm]

    def progress(wi, w, masks):
        now = time.time()
        iv = masks.get(w['id'])
        m = masked_letters(iv) if iv else 0
        print(f"  {wi + 1}/{len(targets)} {w['id']}: "
              f"{m:,}/{len(w['stream']):,} masked "
              f"({now - last[0]:.1f}s)", flush=True)
        last[0] = now

    fresh_masks = mask_edited_works(targets, seg_streams, seg_work,
                                    seg_off, codes_f, seg_f, pos_f,
                                    stats, progress_cb=progress)
    print(f"masked {len(fresh_masks)}/{len(targets)} targets, "
          f"stats={stats} ({time.time() - tm:.0f}s)", flush=True)

    # -- merge: existing masks minus stale target entries plus fresh
    merged = dict(old_masks)
    stale = [i for i in tgt_ids if i in merged]
    for i in stale:
        del merged[i]
    merged.update(fresh_masks)
    json.dump(merged, open(OUT, 'w', encoding='utf-8'))
    print(f"wrote {OUT}: {len(merged)} works "
          f"({len(old_masks)} old - {len(stale)} stale + "
          f"{len(fresh_masks)} fresh)", flush=True)

    # -- delta report
    lines = [
        "# ref_canon_masks_v2 delta report", "",
        f"Incremental update for ref_corpus_v2.pkl "
        f"({len(v2)} works). Base: ref_canon_masks.json "
        f"({len(old_masks)} works, 16h full pass over the old corpus).",
        f"Targets re-masked: {len(targets)} "
        f"({n_new} new REF2/other, {len(targets) - n_new} "
        f"stream-changed). All other works byte-identical -> masks "
        f"carried over. Canonical index works verified identical.",
        f"Regression check: {len(reg_rows)}/{len(reg_rows)} unchanged "
        f"works reproduce their existing intervals exactly on the v2 "
        f"index.",
        f"Merged output: {len(merged)} works with masks.", "",
        "NOTE: REF2 Liturgy works are EXPECTED to be heavily masked "
        "against Bible — Hallel is ~0.97 contained in Psalms, Pesukei "
        "deZimra is Psalms 145-150, etc. That is correct behavior: "
        "these works should only be identifiable through their own "
        "non-biblical formulations.", "",
        "## Per-target results (sorted by masked fraction)", "",
        "| work | cat | stream | masked | frac | intervals | old |",
        "|---|---|---|---|---|---|---|",
    ]
    rows = []
    for w in targets:
        iv = fresh_masks.get(w['id'])
        m = masked_letters(iv) if iv else 0
        frac = m / max(1, len(w['stream']))
        old_iv = old_masks.get(w['id'])
        old_txt = (f"{masked_letters(old_iv):,} (stale coords)"
                   if old_iv else "-")
        name = (f"{w['author']} — {w['title']}" if w.get('author')
                else w['title'])
        rows.append((frac, m, w, iv, old_txt, name))
    rows.sort(key=lambda r: (-r[0], r[2]['id']))
    for frac, m, w, iv, old_txt, name in rows:
        lines.append(f"| {w['id']} {name[:45]} | {w['cat']} | "
                     f"{len(w['stream']):,} | {m:,} | {100 * frac:.1f}% "
                     f"| {len(iv) if iv else 0} | {old_txt} |")
    lines += ["", "## Stats",
              f"- gram hits: {stats['hits']:,}; candidate hulls: "
              f"{stats['cand']:,}; accepted spans: {stats['accepted']:,}",
              f"- df-dropped codes in canonical index: {df_dropped:,}",
              f"- total runtime: {time.time() - t0:.0f}s"]
    open(REPORT, 'w', encoding='utf-8').write('\n'.join(lines))
    print(f"wrote {REPORT} ({time.time() - t0:.0f}s)", flush=True)


if __name__ == '__main__':
    main()
