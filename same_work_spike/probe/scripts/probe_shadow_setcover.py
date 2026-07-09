# -*- coding: utf-8 -*-
"""A4 probe — global (exact MWIS) competitive-span assignment vs the greedy
heuristic in track1_shadow.py.

READ-ONLY: opens the DB in mode=ro. Never touches track1_matches. Writes
probe output only to:
  ../results/a4_shadow_assignment_probe.json
  ../results/a4_shadow_assignment_probe.md

--- Matching track1_shadow.py's actual semantics (read there first) ---
Assignment UNIT = the whole track1_matches ROW.
Per row: "the span" = the row's LONGEST individual span from spans_json
(max by length, ties arbitrary — same as track1_shadow.py's best_span()),
and "the density" = THAT span's own density value (spans_json[i][2]), NOT
the row-level best_density column (which can be the MIN across spans and
therefore differ from the longest span's density on multi-span rows).
density is a DISTANCE: lower is better.

Two rows CONFLICT (greedy: worse one gets shadowed by a live better one) iff,
letting `lo`=lower-density(better) and `hi`=higher-density(worse) of the pair:
  overlap(lo.span, hi.span) >= OVERLAP_FRAC * len(hi.span)   [>=60% of the
      WORSE row's span, exactly as track1_shadow.py computes it]
  AND hi.density - lo.density >= MIN_DENS_GAP                [>=0.03 gap]
This predicate is symmetric / state-independent (unlike the greedy loop,
which only tests a row against currently-*live* rows in ascending-density
processing order and `break`s on the first qualifying winner).

--- Alternative: global assignment (exact Maximum-Weight Independent Set) ---
Per page, build the conflict graph over its rows using the predicate above
(same OVERLAP_FRAC / MIN_DENS_GAP constants as track1_shadow.py). Solve
exact MWIS (branch-on-vertex + memoized recursion over "available" bitmask;
per-page item counts are <=20 in this corpus so this is exact and fast).

Objective (this is the "global assignment" formulation requested by the
brief): maximize sum over included rows of
    value_i = matched_letters_i * (DENS_CAP - density_i)
which is a monotone reparametrization of "minimize total covered-letters-
weighted density" that also rewards total coverage (a pure
minimize-sum(letters*density) objective is degenerate: the empty set scores
0 and would always "win", so it cannot by itself express "prefer more
coverage AND lower density" — the brief's stated equivalence needs the
per-letter constant DENS_CAP to make inclusion valuable). DENS_CAP must be
>= the corpus max density (0.35 here) so every value_i > 0 (never a reason
to leave out a non-conflicting row). We run TWO settings to bracket
sensitivity:
  DENS_CAP=1.00  (primary)  -- coverage-friendly: a much-longer worse row
                                 can outweigh a short better row.
  DENS_CAP=0.36  (secondary, "quality-picky") -- minimal headroom over the
                                 max density; closest in spirit to greedy's
                                 "quality almost always trumps size".
A shadowed-under-global row's reported "winner" = its live neighbor with
the lowest density (there must be >=1 such neighbor at the optimum, else
adding the excluded row for free would strictly improve the objective,
since every value_i is positive — this is asserted/counted as a sanity
check, expected count 0).

Usage: python -X utf8 -u probe_shadow_setcover.py [db]
"""
import json
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict, Counter

from normalize import norm_stream, project_span

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\a4_shadow_assignment_probe.json"
OUT_MD = ROOT + r"\same_work_spike\probe\results\a4_shadow_assignment_probe.md"

OVERLAP_FRAC = 0.6      # same constant as track1_shadow.py
MIN_DENS_GAP = 0.03     # same constant as track1_shadow.py
CENSUS_MIN_LETTERS = 200  # matches track1_shadow.py's per-work census filter

PRIMARY_DENS_CAP = 1.00
SECONDARY_DENS_CAP = 0.36

TEMPLE_SCROLL_WID = 'M:Ytext86000'
IBN_TIBBON_RIKMA_WID = 'M:Ytext280002'
JA_RIKMA_WID = 'J:42-ריבג-ספר-הרקמה'


def best_span(spans):
    return max(spans, key=lambda s: s[1] - s[0])


def load_rows(con):
    rows = con.execute("""
        SELECT rowid, page_id, sys_id, work_id, cat, author, title,
               matched_letters, spans_json, shadowed_by
        FROM track1_matches""").fetchall()
    by_page = defaultdict(list)
    for rowid, pid, sid, wid, cat, author, title, letters, sj, sh in rows:
        spans = [(int(s[0]), int(s[1]), float(s[2])) for s in json.loads(sj)]
        b0, b1, bd = best_span(spans)
        by_page[pid].append(dict(
            rowid=rowid, page_id=pid, sys_id=sid, wid=wid, cat=cat,
            author=author, title=title, letters=letters,
            b0=b0, b1=b1, bd=bd, shadowed_by=sh,
        ))
    return by_page, len(rows)


def overlap_and_frac(a, b):
    """Return (overlap_len, frac-of-worse-span, lo, hi) where lo=better
    (lower density), hi=worse (higher density)."""
    if a['bd'] <= b['bd']:
        lo, hi = a, b
    else:
        lo, hi = b, a
    ov = min(hi['b1'], lo['b1']) - max(hi['b0'], lo['b0'])
    worse_len = hi['b1'] - hi['b0']
    frac = (ov / worse_len) if worse_len > 0 else 0.0
    return ov, frac, lo, hi


def conflicts(a, b):
    ov, frac, lo, hi = overlap_and_frac(a, b)
    if ov <= 0:
        return False
    return frac >= OVERLAP_FRAC and (hi['bd'] - lo['bd']) >= MIN_DENS_GAP


# ---------------------------------------------------------------- greedy replica
def greedy_replica_shadow(items):
    """Faithful re-implementation of track1_shadow.py's per-page loop, but
    over an ALREADY-LOADED items list (same tuple fields), for self-check
    only. Returns dict: idx -> winner_idx (shadowed rows), live idx set."""
    order = sorted(range(len(items)), key=lambda i: items[i]['bd'])
    live = []  # list of idx (in acceptance order)
    winner_of = {}
    for i in order:
        it = items[i]
        winner = None
        for lidx in live:
            lit = items[lidx]
            ov = min(it['b1'], lit['b1']) - max(it['b0'], lit['b0'])
            if ov >= OVERLAP_FRAC * (it['b1'] - it['b0']) and \
                    it['bd'] - lit['bd'] >= MIN_DENS_GAP:
                winner = lidx
                break
        if winner is not None:
            winner_of[i] = winner
        else:
            live.append(i)
    live_set = set(live)
    return live_set, winner_of


# ---------------------------------------------------------------- global MWIS
def solve_page_mwis(items, dens_cap):
    n = len(items)
    if n == 1:
        return {0}, {}
    adj = [0] * n
    for i in range(n):
        for j in range(i + 1, n):
            if conflicts(items[i], items[j]):
                adj[i] |= (1 << j)
                adj[j] |= (1 << i)
    values = [it['letters'] * (dens_cap - it['bd']) for it in items]
    memo = {}

    def solve(avail):
        if avail == 0:
            return 0.0, 0
        cached = memo.get(avail)
        if cached is not None:
            return cached
        i = (avail & -avail).bit_length() - 1
        rest = avail & ~(1 << i)
        val_ex, mask_ex = solve(rest)
        avail_inc = rest & ~adj[i]
        val_in_sub, mask_in_sub = solve(avail_inc)
        val_in = values[i] + val_in_sub
        mask_in = (1 << i) | mask_in_sub
        if val_in > val_ex + 1e-9:
            res = (val_in, mask_in)
        else:
            res = (val_ex, mask_ex)
        memo[avail] = res
        return res

    _, mask = solve((1 << n) - 1)
    live = {i for i in range(n) if mask & (1 << i)}
    winner_of = {}
    orphan = 0
    for i in range(n):
        if i in live:
            continue
        cand = [j for j in live if adj[i] & (1 << j)]
        if cand:
            winner_of[i] = min(cand, key=lambda j: items[j]['bd'])
        else:
            orphan += 1  # should never happen (see module docstring)
    return live, winner_of, orphan


def name_of(it):
    return f"{it['author']} — {it['title']}" if it['author'] else it['title']


def main():
    t0 = time.time()
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    by_page, n_rows = load_rows(con)
    con.close()
    print(f"loaded {n_rows:,} rows over {len(by_page):,} pages "
          f"({time.time() - t0:.1f}s)", flush=True)

    # ---------------- self-check: replica greedy vs stored shadowed_by ----
    replica_shadowed = 0
    replica_mismatch_rows = 0
    stored_shadowed = 0
    for pid, items in by_page.items():
        stored_shadowed += sum(1 for it in items if it['shadowed_by'])
        if len(items) < 2:
            continue
        live_set, winner_of = greedy_replica_shadow(items)
        for i, it in enumerate(items):
            is_shadowed_stored = bool(it['shadowed_by'])
            is_shadowed_replica = i not in live_set
            if is_shadowed_stored != is_shadowed_replica:
                replica_mismatch_rows += 1
            if is_shadowed_replica:
                replica_shadowed += 1
    print(f"self-check: stored shadowed={stored_shadowed:,} "
          f"replica shadowed={replica_shadowed:,} "
          f"mismatched-verdict rows={replica_mismatch_rows:,} "
          f"({time.time() - t0:.1f}s)", flush=True)

    # ---------------- global MWIS, two DENS_CAP settings -------------------
    def run_global(dens_cap):
        global_live_rowid = set()
        winner_wid_of_rowid = {}   # rowid -> winning work_id (global-shadowed rows)
        best_work_page = {}       # page_id -> wid of the top-live row (global)
        pages_solved = 0
        total_orphan = 0
        for pid, items in by_page.items():
            if len(items) == 1:
                global_live_rowid.add(items[0]['rowid'])
                best_work_page[pid] = items[0]['wid']
                continue
            live, winner_of, orphan = solve_page_mwis(items, dens_cap)
            total_orphan += orphan
            pages_solved += 1
            for i, it in enumerate(items):
                if i in live:
                    global_live_rowid.add(it['rowid'])
                else:
                    w = winner_of.get(i)
                    if w is not None:
                        winner_wid_of_rowid[it['rowid']] = items[w]['wid']
            top = min((items[i] for i in live),
                      key=lambda it: (it['bd'], -it['letters']))
            best_work_page[pid] = top['wid']
        return global_live_rowid, winner_wid_of_rowid, best_work_page, total_orphan

    results = {}
    for label, cap in [('primary', PRIMARY_DENS_CAP),
                        ('secondary', SECONDARY_DENS_CAP)]:
        gl, wof, bwp, orphan = run_global(cap)
        results[label] = dict(dens_cap=cap, global_live=gl, winner_of=wof,
                               best_work_page=bwp, orphan=orphan)
        print(f"[{label} DENS_CAP={cap}] global-live rows={len(gl):,} "
              f"orphan-excluded={orphan} ({time.time() - t0:.1f}s)",
              flush=True)

    # ---------------- comparisons (use PRIMARY as the headline number) -----
    def compare(global_live_rowid, winner_wid_of_rowid, best_work_page):
        greedy_live_rowid = {it['rowid'] for items in by_page.values()
                              for it in items if not it['shadowed_by']}
        all_rowids = {it['rowid'] for items in by_page.values() for it in items}
        live_to_shadowed = greedy_live_rowid - global_live_rowid
        shadow_to_live = global_live_rowid - greedy_live_rowid
        flips = live_to_shadowed | shadow_to_live
        # best-work-changed pages
        best_work_changed = 0
        best_work_pages = []
        for pid, items in by_page.items():
            if len(items) < 2:
                continue
            greedy_live_items = [it for it in items if not it['shadowed_by']]
            if not greedy_live_items:
                continue
            top_greedy = min(greedy_live_items,
                              key=lambda it: (it['bd'], -it['letters']))
            top_global_wid = best_work_page.get(pid)
            if top_greedy['wid'] != top_global_wid:
                best_work_changed += 1
                best_work_pages.append(pid)
        return dict(
            total_rows=len(all_rowids),
            greedy_live=len(greedy_live_rowid),
            global_live=len(global_live_rowid),
            live_to_shadowed=sorted(live_to_shadowed),
            shadow_to_live=sorted(shadow_to_live),
            n_live_to_shadowed=len(live_to_shadowed),
            n_shadow_to_live=len(shadow_to_live),
            n_flips=len(flips),
            flip_rate=len(flips) / len(all_rowids),
            best_work_changed_pages=best_work_pages,
            n_best_work_changed=best_work_changed,
        )

    cmp_primary = compare(results['primary']['global_live'],
                          results['primary']['winner_of'],
                          results['primary']['best_work_page'])
    cmp_secondary = compare(results['secondary']['global_live'],
                            results['secondary']['winner_of'],
                            results['secondary']['best_work_page'])
    print(f"PRIMARY flips: live->shadowed={cmp_primary['n_live_to_shadowed']:,} "
          f"shadow->live={cmp_primary['n_shadow_to_live']:,} "
          f"total={cmp_primary['n_flips']:,} "
          f"({100 * cmp_primary['flip_rate']:.3f}%) "
          f"best-work-changed pages={cmp_primary['n_best_work_changed']:,}",
          flush=True)
    print(f"SECONDARY flips: live->shadowed={cmp_secondary['n_live_to_shadowed']:,} "
          f"shadow->live={cmp_secondary['n_shadow_to_live']:,} "
          f"total={cmp_secondary['n_flips']:,} "
          f"({100 * cmp_secondary['flip_rate']:.3f}%) "
          f"best-work-changed pages={cmp_secondary['n_best_work_changed']:,}",
          flush=True)

    # ---------------- per-work witness deltas (census-style, >=200 letters) -
    rowid_to_item = {}
    for items in by_page.values():
        for it in items:
            rowid_to_item[it['rowid']] = it
    name = {}
    all_ms = defaultdict(set)
    greedy_live_ms = defaultdict(set)
    global_live_ms_primary = defaultdict(set)
    global_live_ms_secondary = defaultdict(set)
    gl_primary = results['primary']['global_live']
    gl_secondary = results['secondary']['global_live']
    for it in rowid_to_item.values():
        if it['letters'] < CENSUS_MIN_LETTERS:
            continue
        wid = it['wid']
        name[wid] = name_of(it)
        all_ms[wid].add(it['sys_id'])
        if not it['shadowed_by']:
            greedy_live_ms[wid].add(it['sys_id'])
        if it['rowid'] in gl_primary:
            global_live_ms_primary[wid].add(it['sys_id'])
        if it['rowid'] in gl_secondary:
            global_live_ms_secondary[wid].add(it['sys_id'])

    per_work_rows = []
    for wid in all_ms:
        per_work_rows.append(dict(
            wid=wid, name=name[wid],
            all=len(all_ms[wid]),
            greedy_live=len(greedy_live_ms[wid]),
            global_live_primary=len(global_live_ms_primary[wid]),
            global_live_secondary=len(global_live_ms_secondary[wid]),
        ))
    per_work_rows.sort(key=lambda r: -abs(r['greedy_live'] - r['global_live_primary']))

    # known-case spot checks
    def spot(wid):
        return dict(
            wid=wid, name=name.get(wid, '(no >=200-letter rows)'),
            all=len(all_ms.get(wid, set())),
            greedy_live=len(greedy_live_ms.get(wid, set())),
            global_live_primary=len(global_live_ms_primary.get(wid, set())),
            global_live_secondary=len(global_live_ms_secondary.get(wid, set())),
        )
    known_cases = dict(
        temple_scroll=spot(TEMPLE_SCROLL_WID),
        ibn_tibbon_rikma=spot(IBN_TIBBON_RIKMA_WID),
        ja_rikma_original=spot(JA_RIKMA_WID),
    )
    print("known cases:", json.dumps(known_cases, ensure_ascii=False, indent=1),
          flush=True)

    # ---------------- disagreement stratification (primary) ----------------
    def decisive_edge_for_shadow_to_live(rowid):
        """Row was greedy-shadowed, now global-live. Reconstruct the row
        that greedy recorded as its winner (same page, wid==shadowed_by,
        live under greedy) and compute gap/overlap against it."""
        it = rowid_to_item[rowid]
        items = by_page[it['page_id']]
        cands = [x for x in items if x['wid'] == it['shadowed_by']
                 and not x['shadowed_by']]
        if not cands:
            return None
        winner = min(cands, key=lambda x: x['bd'])
        ov, frac, lo, hi = overlap_and_frac(it, winner)
        return dict(winner_rowid=winner['rowid'], winner_wid=winner['wid'],
                    winner_name=name_of(winner), overlap_frac=frac,
                    density_gap=hi['bd'] - lo['bd'])

    def decisive_edge_for_live_to_shadowed(rowid):
        """Row was greedy-live, now global-shadowed. winner_of (primary)
        gives the index of its global winner on the same page."""
        it = rowid_to_item[rowid]
        winner_wid = results['primary']['winner_of'].get(rowid)
        if winner_wid is None:
            return None
        items = by_page[it['page_id']]
        cands = [x for x in items if x['wid'] == winner_wid]
        if not cands:
            return None
        winner = min(cands, key=lambda x: x['bd'])
        ov, frac, lo, hi = overlap_and_frac(it, winner)
        return dict(winner_rowid=winner['rowid'], winner_wid=winner['wid'],
                    winner_name=name_of(winner), overlap_frac=frac,
                    density_gap=hi['bd'] - lo['bd'])

    def bucket_gap(g):
        if g < 0.05:
            return '0.03-0.05'
        if g < 0.10:
            return '0.05-0.10'
        if g < 0.20:
            return '0.10-0.20'
        return '0.20+'

    def bucket_overlap(f):
        if f < 0.7:
            return '0.6-0.7'
        if f < 0.8:
            return '0.7-0.8'
        if f < 0.9:
            return '0.8-0.9'
        return '0.9-1.0'

    strat_gap = Counter()
    strat_overlap = Counter()
    flip_details = []
    for rowid in cmp_primary['shadow_to_live']:
        edge = decisive_edge_for_shadow_to_live(rowid)
        it = rowid_to_item[rowid]
        rec = dict(direction='shadow_to_live', rowid=rowid, page_id=it['page_id'],
                   wid=it['wid'], name=name_of(it), density=it['bd'],
                   letters=it['letters'], edge=edge)
        flip_details.append(rec)
        if edge:
            strat_gap[bucket_gap(edge['density_gap'])] += 1
            strat_overlap[bucket_overlap(edge['overlap_frac'])] += 1
    for rowid in cmp_primary['live_to_shadowed']:
        edge = decisive_edge_for_live_to_shadowed(rowid)
        it = rowid_to_item[rowid]
        rec = dict(direction='live_to_shadowed', rowid=rowid, page_id=it['page_id'],
                   wid=it['wid'], name=name_of(it), density=it['bd'],
                   letters=it['letters'], edge=edge)
        flip_details.append(rec)
        if edge:
            strat_gap[bucket_gap(edge['density_gap'])] += 1
            strat_overlap[bucket_overlap(edge['overlap_frac'])] += 1

    print(f"stratify by density gap: {dict(strat_gap)}", flush=True)
    print(f"stratify by overlap frac: {dict(strat_overlap)}", flush=True)

    # ---------------- 10 example disagreement cards -------------------------
    con2 = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)

    def page_text(pid):
        r = con2.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
        return r[0] if r else ""

    def span_snippet(pid, b0, b1, pad=15):
        text = page_text(pid)
        if not text:
            return ""
        nfc = unicodedata.normalize('NFC', text)
        _, offs = norm_stream(text)
        return project_span(offs, b0, b1, nfc, pad=pad).replace('\n', ' ')

    # pick a diverse sample: 5 shadow->live, 5 live->shadowed, spread across
    # gap/overlap buckets, sorted by letters desc within each direction
    def pick_examples(direction, k=5):
        pool = [r for r in flip_details if r['direction'] == direction and r['edge']]
        pool.sort(key=lambda r: -r['letters'])
        seen_wid = set()
        picked = []
        for r in pool:
            if r['wid'] in seen_wid and len(picked) < k:
                continue
            picked.append(r)
            seen_wid.add(r['wid'])
            if len(picked) >= k:
                break
        if len(picked) < k:
            for r in pool:
                if r not in picked:
                    picked.append(r)
                if len(picked) >= k:
                    break
        return picked[:k]

    example_recs = pick_examples('shadow_to_live', 5) + pick_examples('live_to_shadowed', 5)
    cards = []
    for r in example_recs:
        it = rowid_to_item[r['rowid']]
        edge = r['edge']
        winner_it = rowid_to_item[edge['winner_rowid']]
        card = dict(
            direction=r['direction'], page_id=it['page_id'],
            row=dict(rowid=it['rowid'], wid=it['wid'], name=name_of(it),
                     density=it['bd'], letters=it['letters'],
                     span=[it['b0'], it['b1']],
                     snippet=span_snippet(it['page_id'], it['b0'], it['b1'])),
            competitor=dict(rowid=winner_it['rowid'], wid=winner_it['wid'],
                            name=name_of(winner_it), density=winner_it['bd'],
                            letters=winner_it['letters'],
                            span=[winner_it['b0'], winner_it['b1']],
                            snippet=span_snippet(winner_it['page_id'],
                                                 winner_it['b0'], winner_it['b1'])),
            overlap_frac=edge['overlap_frac'], density_gap=edge['density_gap'],
        )
        cards.append(card)
    con2.close()

    # ---------------- write JSON artifact ------------------------------------
    out = dict(
        db=DB, n_rows=n_rows, n_pages=len(by_page),
        overlap_frac=OVERLAP_FRAC, min_dens_gap=MIN_DENS_GAP,
        self_check=dict(stored_shadowed=stored_shadowed,
                        replica_shadowed=replica_shadowed,
                        mismatch_rows=replica_mismatch_rows),
        primary=dict(dens_cap=PRIMARY_DENS_CAP,
                    orphan_excluded=results['primary']['orphan'],
                    **{k: v for k, v in cmp_primary.items()
                       if k not in ('live_to_shadowed', 'shadow_to_live')}),
        secondary=dict(dens_cap=SECONDARY_DENS_CAP,
                      orphan_excluded=results['secondary']['orphan'],
                      **{k: v for k, v in cmp_secondary.items()
                         if k not in ('live_to_shadowed', 'shadow_to_live')}),
        strat_gap=dict(strat_gap), strat_overlap=dict(strat_overlap),
        known_cases=known_cases,
        per_work_top25=per_work_rows[:25],
        flip_rowids_primary=dict(
            live_to_shadowed=cmp_primary['live_to_shadowed'][:2000],
            shadow_to_live=cmp_primary['shadow_to_live'][:2000],
            truncated=(len(cmp_primary['live_to_shadowed']) > 2000
                      or len(cmp_primary['shadow_to_live']) > 2000)),
        example_cards=cards,
    )
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"wrote {OUT_JSON} ({time.time() - t0:.1f}s)", flush=True)

    return out, cards


if __name__ == '__main__':
    main()
