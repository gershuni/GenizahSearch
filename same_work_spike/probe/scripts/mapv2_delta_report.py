# -*- coding: utf-8 -*-
"""Tier-A v1 -> v2 delta report + invariant re-tests (MAPV2-5 prep).

Compares the v1 census (data/fullcorpus.db::track1_matches) against the v2
census (data/fullcorpus_v2.db::track1_matches), attributes every gained/lost
(page, work) identification to a change channel, and runs consistency checks.

Channels (first match wins):
  text          page's v2 text was substituted (provenance != 'htr')
  ref-new-work  work absent from the v1 reference (REF-2 / nosafot additions)
  ref-letters   work's reference stream length changed > 2%
  ref-mesirah   work's mesirah metadata presence changed (header-regex fix)
  masking       work's canonical-mask fraction changed > 5pp
  engine/other  residual (tier gate, DF-cap interactions, ordering)

Run AFTER chain step 3-shadow (needs shadowed_by in v2). Read-only on both DBs.
Usage: python -X utf8 -u mapv2_delta_report.py
Out:   results/mapv2_tierA_delta.md
"""
import gc
import json
import pickle
import sqlite3
import sys
from collections import Counter, defaultdict

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
V1_DB = PROBE + r"\data\fullcorpus.db"
V2_DB = PROBE + r"\data\fullcorpus_v2.db"
REF_V1_BUGGY = PROBE + r"\data\ref_corpus_v1_prefix_buggy.pkl.bak"  # what v1 census ran on
REF_V2 = PROBE + r"\data\ref_corpus_v2.pkl"
MASKS_V1 = PROBE + r"\data\ref_canon_masks.json"
MASKS_V2 = PROBE + r"\data\ref_canon_masks_v2.json"
OUT = PROBE + r"\results\mapv2_tierA_delta.md"

LETTERS_DELTA = 0.02
MASK_DELTA_PP = 0.05


def ref_summary(path):
    """{work_id: (letters, has_mesirah, title)} — loads one pkl, then frees."""
    works = pickle.load(open(path, 'rb'))
    out = {w['id']: (len(w['stream']), bool(w.get('mesirah')), w.get('title', ''))
           for w in works}
    del works
    gc.collect()
    return out


def mask_frac(masks, letters):
    out = {}
    for wid, spans in masks.items():
        tot = sum(max(0, int(b) - int(a)) for a, b in spans)
        n = letters.get(wid, (0,))[0]
        out[wid] = tot / n if n else 0.0
    return out


def load_rows(db, checks, tag):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(track1_matches)")}
    if 'shadowed_by' not in cols:
        print(f"WARN: {tag} has no shadowed_by column (shadow step not run?) — "
              f"using all rows")
        where = ""
    else:
        where = " WHERE shadowed_by IS NULL"
    rows = {}
    dup = 0
    for pid, sid, wid, letters in con.execute(
            "SELECT page_id, sys_id, work_id, matched_letters "
            "FROM track1_matches" + where):
        if (pid, wid) in rows:
            dup += 1
        rows[(pid, wid)] = (sid, letters)
    checks.append((f"{tag}: no duplicate (page,work) rows", dup == 0,
                   f"{dup} duplicates"))
    con.close()
    return rows


def main():
    checks = []  # (name, passed, detail)

    print("loading v1 census ...")
    v1 = load_rows(V1_DB, checks, "v1")
    print(f"  v1 live rows: {len(v1):,}")
    print("loading v2 census ...")
    v2 = load_rows(V2_DB, checks, "v2")
    print(f"  v2 live rows: {len(v2):,}")

    con2 = sqlite3.connect(f"file:{V2_DB}?mode=ro", uri=True)
    prov = dict(con2.execute("SELECT page_id, provenance FROM pages"))
    flags = con2.execute(
        "SELECT COUNT(*), SUM(merge_flag), SUM(weak_two_work_flag) "
        "FROM mapv2_page_flags").fetchone()
    # tier-B sanity over the same DB
    b_stats = con2.execute(
        "SELECT COUNT(*), SUM(p_same_work < 0 OR p_same_work > 1), "
        "COUNT(DISTINCT margin_band) FROM track1_candidates").fetchone()
    bad_band = con2.execute(
        "SELECT COUNT(*) FROM track1_candidates WHERE margin_band NOT IN "
        "('singleton','m_ge_010','m_003_010','m_0_003','not_best')").fetchone()[0]
    ab_overlap = con2.execute("""
        SELECT COUNT(*) FROM track1_candidates c
        WHERE EXISTS (SELECT 1 FROM track1_matches m
                      WHERE m.page_id=c.page_id AND m.work_id=c.work_id)""").fetchone()[0]
    checks.append(("v2 tier-B p_same_work all within [0,1]",
                   not b_stats[1], f"{b_stats[1]} out-of-range of {b_stats[0]:,}"))
    checks.append(("v2 tier-B margin_band values all valid",
                   bad_band == 0, f"{bad_band} invalid"))
    checks.append(("no (page,work) in BOTH tier A and tier B",
                   ab_overlap == 0, f"{ab_overlap} overlaps"))
    n_meta = dict(con2.execute("SELECT key, value FROM mapv2_meta"))
    con2.close()

    print("loading reference summaries ...")
    ref1 = ref_summary(REF_V1_BUGGY)
    ref2 = ref_summary(REF_V2)
    m1 = mask_frac(json.load(open(MASKS_V1, encoding='utf-8')), ref1)
    m2 = mask_frac(json.load(open(MASKS_V2, encoding='utf-8')), ref2)

    def channel(pid, wid):
        if prov.get(pid, 'htr') != 'htr':
            return 'text'
        if wid not in ref1:
            return 'ref-new-work'
        l1, mes1, _ = ref1[wid]
        l2, mes2, _ = ref2.get(wid, (l1, mes1, ''))
        if l1 and abs(l2 - l1) / max(l1, l2) > LETTERS_DELTA:
            return 'ref-letters'
        if mes1 != mes2:
            return 'ref-mesirah'
        if abs(m2.get(wid, 0.0) - m1.get(wid, 0.0)) > MASK_DELTA_PP:
            return 'masking'
        return 'engine/other'

    gained = [k for k in v2 if k not in v1]
    lost = [k for k in v1 if k not in v2]
    gain_ch = Counter(channel(p, w) for p, w in gained)
    lost_ch = Counter(channel(p, w) for p, w in lost)

    # stability re-test: rows whose page text AND work were UNCHANGED
    stable_pool = [k for k in v1
                   if prov.get(k[0], 'htr') == 'htr'
                   and channel(k[0], k[1]) == 'engine/other']
    kept = sum(1 for k in stable_pool if k in v2)
    stab = kept / len(stable_pool) if stable_pool else 1.0
    checks.append(("stability: unchanged-input v1 rows persist in v2 >= 90%",
                   stab >= 0.90, f"{stab:.1%} ({kept:,}/{len(stable_pool):,})"))

    # per-work witness deltas
    w1 = defaultdict(set)
    w2 = defaultdict(set)
    for (pid, wid), (sid, _) in v1.items():
        w1[wid].add(sid)
    for (pid, wid), (sid, _) in v2.items():
        w2[wid].add(sid)
    deltas = []
    for wid in set(w1) | set(w2):
        d = len(w2.get(wid, ())) - len(w1.get(wid, ()))
        if d:
            deltas.append((d, wid))
    deltas.sort()
    title_of = {w: (ref2.get(w) or ref1.get(w) or (0, 0, '?'))[2]
                for _, w in deltas}
    wiped = [(w, len(w1[w])) for w in w1
             if w2.get(w) is None or not w2[w]]
    wiped.sort(key=lambda x: -x[1])

    # ---- report ----
    L = ["# Tier-A v1 → v2 delta report (MAPV2)\n"]
    L.append(f"- v1 live identifications: **{len(v1):,}** "
             f"({len({p for p, _ in v1}):,} pages, {len(w1):,} works)")
    L.append(f"- v2 live identifications: **{len(v2):,}** "
             f"({len({p for p, _ in v2}):,} pages, {len(w2):,} works)")
    L.append(f"- gained: **{len(gained):,}**  ·  lost: **{len(lost):,}**")
    L.append(f"- v2 page flags: {flags[0]:,} rows, merge={flags[1]}, "
             f"weak-two-work={flags[2]}")
    L.append(f"- v2 meta: {n_meta}")
    L.append("\n## Attribution of GAINED identifications\n")
    for ch, n in gain_ch.most_common():
        L.append(f"- {ch}: {n:,}")
    L.append("\n## Attribution of LOST identifications\n")
    for ch, n in lost_ch.most_common():
        L.append(f"- {ch}: {n:,}")
    L.append("\n## Invariant checks\n")
    n_fail = 0
    for name, ok, detail in checks:
        n_fail += (not ok)
        L.append(f"- {'PASS' if ok else '**FAIL**'} — {name} ({detail})")
    L.append("\n## Works gaining most witnesses (manuscripts)\n")
    L.append("| Δ | work | title |")
    L.append("|---|------|-------|")
    for d, wid in list(reversed(deltas))[:30]:
        L.append(f"| +{d} | {wid} | {title_of.get(wid, '?')[:60]} |")
    L.append("\n## Works losing most witnesses\n")
    L.append("| Δ | work | title |")
    L.append("|---|------|-------|")
    for d, wid in deltas[:30]:
        L.append(f"| {d} | {wid} | {title_of.get(wid, '?')[:60]} |")
    L.append(f"\n## Works wiped out entirely ({len(wiped)}; potential "
             f"regressions unless renamed/merged in the ref rebuild)\n")
    L.append("| v1 witnesses | work |")
    L.append("|---|------|")
    for wid, n in wiped[:50]:
        L.append(f"| {n} | {wid} |")
    open(OUT, 'w', encoding='utf-8').write("\n".join(L) + "\n")
    print(f"wrote {OUT}; {len(checks) - n_fail}/{len(checks)} checks passed")
    return 1 if n_fail else 0


if __name__ == '__main__':
    sys.exit(main())
