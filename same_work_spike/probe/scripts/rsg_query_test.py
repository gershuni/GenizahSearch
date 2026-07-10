# -*- coding: utf-8 -*-
"""RSG postmortem — step 3: query cluster pages vs reference.

Tests the 4 hypotheses with numbers:
  - Build FULL reference index (production config, canon masks) and an
    RSG-ONLY index (JA works titled רס"ג/סעדיה, NO masks -> DF-cap +
    cross-work competition removed).
  - Query member pages of the RSG-tagged residue clusters against both.
  - Positive control: pages Track-1 DID label as RSG, queried vs RSG-only,
    must verify at low density (proves ref + harness work).

Read-only. Reuses frag1_truncation.query_batch + track1_match.build_ref_index
verbatim; does NOT modify pipeline scripts / DB.
"""
import json
import pickle
import random
import sqlite3
import time
from collections import defaultdict

import numpy as np

from normalize import norm_stream
from track1_match import build_ref_index, accept_density
from frag1_truncation import query_batch

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
MASKS = PROBE + r"\data\ref_canon_masks.json"
MEM = "passage_unit_members_accepted_pairs_canonmask"

CLUSTERS = {1430332: "glossary/RSG-tagged", 303006: "Deut-commentary",
            1157648: "Psalms-commentary", 1038702: "Leviticus"}
N_PER_CLUSTER = 30
RNG = random.Random(20260710)


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def is_rsg(w):
    return w.get('cat') == 'JA' and ('רס"ג' in (w.get('title') or '')
                                     or 'סעדיה' in (w.get('title') or ''))


def build_indexes(works):
    canon_masks = json.load(open(MASKS, encoding='utf-8'))
    # count how many RSG works are masked
    rsg_ids = {w['id'] for w in works if is_rsg(w)}
    masked_rsg = [i for i in rsg_ids if i in canon_masks]
    log(f"RSG works: {len(rsg_ids)}; of these masked in ref_canon_masks: "
        f"{len(masked_rsg)}")

    t = time.time()
    full = build_ref_index(works, canon_masks)
    log(f"FULL index: {len(full[0]):,} segs, {len(full[3]):,} postings, "
        f"df-dropped codes={full[6]:,} ({time.time()-t:.0f}s)")

    rsg_works = [w for w in works if is_rsg(w)]
    t = time.time()
    rsg = build_ref_index(rsg_works, None)   # no masks, isolate recension
    log(f"RSG-ONLY index: {len(rsg_works)} works, {len(rsg[0]):,} segs, "
        f"{len(rsg[3]):,} postings, df-dropped={rsg[6]:,} "
        f"({time.time()-t:.0f}s)")
    # strip df_dropped element -> query_batch expects 6-tuple
    return works, full[:6], rsg_works, rsg[:6]


def get_stream(con, pid, cache):
    if pid in cache:
        return cache[pid]
    row = con.execute("SELECT text FROM pages WHERE page_id=?",
                      (pid,)).fetchone()
    s = norm_stream(row[0] or '')[0] if row else ''
    cache[pid] = s
    return s


def summarize(streams, ref_tuple, works, rsg_idx_set, tag):
    """Query streams; return per-page best-overall + best-RSG summary."""
    results, _ = query_batch(streams, ref_tuple, want_diag=False)
    out = []
    for s, cands in zip(streams, results):
        # cands = [(work_idx, alen, dens), ...] (dens<=0.55)
        best_overall = min(cands, key=lambda c: c[2]) if cands else None
        rsg_c = [c for c in cands if c[0] in rsg_idx_set]
        best_rsg = min(rsg_c, key=lambda c: c[2]) if rsg_c else None
        # would production accept the best-overall?
        acc = (best_overall is not None
               and best_overall[2] <= accept_density(best_overall[1]))
        out.append({
            'n_cand': len(cands),
            'best_overall': best_overall, 'best_rsg': best_rsg,
            'accepts': acc,
        })
    return out


def wtitle(works, wi):
    w = works[wi]
    return f"{(w.get('title') or '')[:45]} [{w.get('cat')}]"


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")
    works = pickle.load(open(REF, 'rb'))

    works, full_tuple, rsg_works, rsg_tuple = build_indexes(works)
    # work_idx sets
    full_rsg_idx = {i for i, w in enumerate(works) if is_rsg(w)}
    rsg_only_idx = set(range(len(rsg_works)))   # all works in RSG index
    cache = {}

    report = []
    A = report.append
    A("# RSG postmortem — query test (cluster pages vs reference)")
    A(f"\nGenerated {time.strftime('%Y-%m-%d %H:%M')}. "
      f"FULL ref = {len(works):,} works (prod canon-masks). "
      f"RSG-ONLY ref = {len(rsg_works)} JA works titled רס\"ג/סעדיה "
      f"(no masks; DF-cap + cross-work competition removed).")
    A(f"\nRSG reference works ({len(rsg_works)}):")
    for w in rsg_works:
        A(f"- {w['id']} · {w['title']} · {len(w['stream']):,} letters")

    # ---------- positive control ----------
    A("\n## Positive control — pages Track-1 labeled RSG, queried vs "
      "RSG-only index")
    rsg_work_ids = tuple(w['id'] for w in rsg_works)
    ph = ','.join('?' * len(rsg_work_ids))
    ctrl = con.execute(
        f"SELECT page_id, work_id, matched_letters, best_density FROM "
        f"track1_matches WHERE shadowed_by IS NULL AND work_id IN ({ph}) "
        f"AND matched_letters>=300 AND best_density<=0.15", rsg_work_ids
    ).fetchall()
    log(f"positive-control candidate rows: {len(ctrl)}")
    RNG.shuffle(ctrl)
    ctrl = ctrl[:15]
    cstreams = [get_stream(con, r[0], cache) for r in ctrl]
    csumm = summarize(cstreams, rsg_tuple, rsg_works, rsg_only_idx, "ctrl")
    A(f"\n{len(ctrl)} pages (Track-1 best_density<=0.15 vs their RSG work). "
      "Re-query vs RSG-only index:")
    A("\n| page | T1 work | T1 dens | requery best-RSG dens | accepts? |")
    A("|---|---|---|---|---|")
    n_ctrl_ok = 0
    for r, sm in zip(ctrl, csumm):
        br = sm['best_rsg']
        d = f"{br[2]:.3f}" if br else "—"
        if sm['accepts']:
            n_ctrl_ok += 1
        A(f"| {r[0][:34]} | {r[1][:22]} | {r[3]:.3f} | {d} | "
          f"{'YES' if sm['accepts'] else 'no'} |")
    A(f"\n**Positive control: {n_ctrl_ok}/{len(ctrl)} re-verify vs RSG-only "
      "index** (confirms RSG reference + query harness work).")

    # ---------- cluster pages ----------
    for u, desc in CLUSTERS.items():
        rows = con.execute(
            f"SELECT DISTINCT page_id, sys_id FROM {MEM} WHERE unit=?",
            (u,)).fetchall()
        RNG.shuffle(rows)
        rows = rows[:N_PER_CLUSTER]
        streams = [get_stream(con, r[0], cache) for r in rows]
        full_s = summarize(streams, full_tuple, works, full_rsg_idx, "full")
        rsg_s = summarize(streams, rsg_tuple, rsg_works, rsg_only_idx, "rsg")

        A(f"\n## unit {u} — {desc} ({len(rows)} sampled pages)")
        # aggregate stats
        full_acc = sum(1 for s in full_s if s['accepts'])
        rsg_verified = [s['best_rsg'][2] for s in rsg_s if s['best_rsg']]
        full_rsg_verified = [s['best_rsg'][2] for s in full_s if s['best_rsg']]
        A(f"\n- pages whose best FULL-ref match would production-ACCEPT: "
          f"**{full_acc}/{len(rows)}**")
        A(f"- pages with ANY RSG candidate (dens<=0.55) vs RSG-only index: "
          f"**{len(rsg_verified)}/{len(rows)}**"
          + (f"; best-RSG-dens min={min(rsg_verified):.3f} "
             f"med={sorted(rsg_verified)[len(rsg_verified)//2]:.3f} "
             f"max={max(rsg_verified):.3f}" if rsg_verified else ""))
        A(f"- pages with ANY RSG candidate vs FULL index: "
          f"**{len(full_rsg_verified)}/{len(rows)}**")
        # what DOES win in the full index?
        winners = defaultdict(int)
        for s in full_s:
            if s['best_overall']:
                winners[wtitle(works, s['best_overall'][0])] += 1
        A(f"- best-overall FULL-ref match distribution (all candidates, "
          f"pre-accept): {dict(sorted(winners.items(), key=lambda x:-x[1]))}")
        # per-page table (first 12)
        A("\n| page | full best-overall (dens) | accept? | full best-RSG "
          "(dens) | RSG-only best (dens) |")
        A("|---|---|---|---|---|")
        for r, fs, rs in list(zip(rows, full_s, rsg_s))[:12]:
            bo = fs['best_overall']
            bo_s = f"{wtitle(works, bo[0])} ({bo[2]:.3f})" if bo else "—"
            frsg = f"{fs['best_rsg'][2]:.3f}" if fs['best_rsg'] else "—"
            rrsg = f"{rs['best_rsg'][2]:.3f}" if rs['best_rsg'] else "—"
            A(f"| {r[0][:30]} | {bo_s} | "
              f"{'Y' if fs['accepts'] else 'n'} | {frsg} | {rrsg} |")

    con.close()
    out = PROBE + r"\results\rsg_query_test.md"
    open(out, 'w', encoding='utf-8').write('\n'.join(report))
    log(f"wrote {out}  (total {time.time()-t0:.0f}s)")
    # also echo the aggregate lines
    for line in report:
        if line.startswith('- pages') or line.startswith('**Positive') \
                or line.startswith('## ') or line.startswith('- best-overall'):
            print(line)


if __name__ == '__main__':
    main()
