# -*- coding: utf-8 -*-
"""REF-2: ingest staged reference acquisitions into ref_corpus_v2.pkl.

Sources (..\\refs_staging\\, 58 txt + manifest.json):
  targum_*  (42) -> cat='Targum'   (Onkelos + Jonathan/Ketuvim targums, Sefaria)
  liturgy_* (13) -> cat='Liturgy'  (statutory liturgy units, modern-rite mask refs)
  b2_*       (3) -> cat='Sefaria'  (gap works: Keter Malkhut, Radak Isaiah, Rif Shabbat)

v1 (ref_corpus.pkl) is NEVER modified. Output:
  ..\\data\\ref_corpus_v2.pkl   = v1 works (vgroup=None default, otherwise untouched)
                                  + new works appended
  ..\\data\\ref2_manifest.json  = per-work ingest record + vgroup membership

Dedup / version-group logic (char-5-gram distinct-set containment,
containment = |shared| / |smaller set|):
  >= 0.98 vs a v1 work          -> true duplicate, new work DROPPED (logged)
  >= 0.85 vs a v1 work          -> version-twin: same vgroup id, BOTH kept
  >= 0.85 pairwise among new    -> union-find version groups
Version groups are an ASSET (multi-rite recall), not noise.

SAME-KIND gate: both rules apply only when the v1 side is NOT canonical
Bible text (cat='Bible').  A liturgy unit (Hallel = Ps 113-118) or verse
anthology contained in a Bible book is canonical QUOTATION, not a version
relationship -- twinning would bind wrong same-work semantics, dropping
would delete a wanted mask reference.  Such pairs are logged as anomalies
and the new work is kept, ungrouped.  All non-Bible v1 cats (Maagarim, JA,
Bavli, Mishnah, ...) pass the gate: e.g. Rif<->Bavli abridgment containment
IS a legitimate version-twin for identification-ambiguity purposes.

Run: python -X utf8 -u ref2_build.py   (cwd = probe\\scripts)
"""
import json
import os
import pickle
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from normalize import norm_stream  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.dirname(HERE)
STAGING = os.path.join(PROBE, 'refs_staging')
V1_PKL = os.path.join(PROBE, 'data', 'ref_corpus.pkl')
V2_PKL = os.path.join(PROBE, 'data', 'ref_corpus_v2.pkl')
MANIFEST_IN = os.path.join(STAGING, 'manifest.json')
MANIFEST_OUT = os.path.join(PROBE, 'data', 'ref2_manifest.json')

K = 5
MIN_LETTERS = 200
TWIN_T = 0.85
DUP_T = 0.98

AUTHORS = {
    'b2_keter_malkhut': 'שלמה אבן גבירול',
    'b2_radak_isaiah': 'דוד קמחי (רד"ק)',
    'b2_rif_hilchot_shabbat': 'יצחק אלפסי (רי"ף)',
}


def cat_for(key: str) -> str:
    if key.startswith('targum_'):
        return 'Targum'
    if key.startswith('liturgy_'):
        return 'Liturgy'
    if key.startswith('b2_'):
        return 'Sefaria'
    raise ValueError(f'unrecognized staging key prefix: {key}')


def gram_set(stream: str) -> set:
    return {stream[i:i + K] for i in range(len(stream) - K + 1)}


# ---------------------------------------------------------------- union-find
class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        p = self.p.setdefault(x, x)
        if p != x:
            self.p[x] = p = self.find(p)
        return p

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def main():
    t0 = time.time()
    man = json.load(open(MANIFEST_IN, encoding='utf-8'))
    entries = man['entries']
    print(f'manifest: {len(entries)} entries', flush=True)

    # ---- 1b. ingest staged files ------------------------------------------
    new_works = []      # kept (pre-dedup) work dicts
    skipped = []        # (key, n_letters)
    for e in entries:
        key = e['key']
        if e.get('guard_only'):
            # citation-guard material (statutory formulas) — must NEVER
            # enter the census reference; the deck loads these itself
            print(f'  SKIP {key}: guard_only', flush=True)
            continue
        path = os.path.join(STAGING, e['body_file'])
        raw = open(path, encoding='utf-8', errors='replace').read()
        stream, _ = norm_stream(raw)
        if len(stream) < MIN_LETTERS:
            skipped.append((key, len(stream)))
            print(f'  SKIP {key}: stream {len(stream)} < {MIN_LETTERS} letters',
                  flush=True)
            continue
        new_works.append({
            'id': f'REF2:{key}',
            'cat': cat_for(key),
            'author': AUTHORS.get(key, ''),
            'title': e.get('title_he') or e.get('title_en') or key,
            'date': '',
            'genre': '',
            'mesirah': '',
            'stream': stream,
            # REF2 extra fields (additive; consumers use w['id'/'stream'/'cat'])
            'title_en': e.get('title_en', ''),
            'provenance': 'sefaria',
            'source_url': e.get('source_url'),
            'license': e.get('license'),
            'ref_kind': e.get('ref_kind'),
            'vgroup': None,
        })
    print(f'staged: {len(new_works)} ingest candidates, {len(skipped)} skipped '
          f'({time.time() - t0:.0f}s)', flush=True)

    grams = [gram_set(w['stream']) for w in new_works]
    n_new = len(new_works)

    # union inverted index: gram -> bitmask over new-work indices
    union_index = {}
    for j, gs in enumerate(grams):
        bit = 1 << j
        for g in gs:
            union_index[g] = union_index.get(g, 0) | bit
    print(f'union gram index: {len(union_index):,} distinct 5-grams '
          f'({time.time() - t0:.0f}s)', flush=True)

    # ---- 1c. containment vs v1 --------------------------------------------
    v1_works = pickle.load(open(V1_PKL, 'rb'))
    v1_ids = {w['id'] for w in v1_works}
    print(f'v1: {len(v1_works)} works loaded ({time.time() - t0:.0f}s)',
          flush=True)

    v1_twin_pairs = []   # (v1_id, new_idx, containment, v1_cat, v1_title, nv1)
    bible_quote_pairs = []  # >= TWIN_T vs cat='Bible': logged, never acted on
    for wi, w in enumerate(v1_works):
        s = w['stream']
        seen = set()
        counts = [0] * n_new
        get = union_index.get
        for i in range(len(s) - K + 1):
            g = s[i:i + K]
            bm = get(g)
            if bm is not None and g not in seen:
                seen.add(g)
                while bm:
                    j = (bm & -bm).bit_length() - 1
                    counts[j] += 1
                    bm &= bm - 1
        if seen:
            n_seen = len(seen)
            n_v1 = None
            for j in range(n_new):
                c = counts[j]
                if not c:
                    continue
                # candidate iff containment vs the smaller set could reach TWIN_T:
                #   vs new set: c/|new_j| ; vs v1 set: c/|v1| with |v1| >= n_seen
                if c >= TWIN_T * len(grams[j]) or c >= TWIN_T * n_seen:
                    if n_v1 is None:
                        n_v1 = len(gram_set(s))
                    cont = c / min(len(grams[j]), n_v1)
                    if cont >= TWIN_T:
                        rec = (w['id'], j, cont, w.get('cat', ''),
                               w.get('title', ''), n_v1)
                        if w.get('cat') == 'Bible':
                            bible_quote_pairs.append(rec)  # same-kind gate
                        else:
                            v1_twin_pairs.append(rec)
        if (wi + 1) % 1000 == 0:
            print(f'  v1 scan {wi + 1}/{len(v1_works)} '
                  f'({time.time() - t0:.0f}s)', flush=True)
    print(f'v1 containment pass done: {len(v1_twin_pairs)} same-kind pairs + '
          f'{len(bible_quote_pairs)} Bible-quotation pairs >= {TWIN_T} '
          f'({time.time() - t0:.0f}s)', flush=True)
    for vid, j, cont, vcat, vtitle, _ in sorted(v1_twin_pairs,
                                                key=lambda p: -p[2]):
        print(f'  {cont:.3f}  {new_works[j]["id"]}  ~  {vid} '
              f'[{vcat}] {vtitle}', flush=True)
    for vid, j, cont, vcat, vtitle, _ in sorted(bible_quote_pairs,
                                                key=lambda p: -p[2]):
        print(f'  BIBLE-QUOTE (no action) {cont:.3f}  {new_works[j]["id"]}'
              f'  ~  {vid} [{vcat}] {vtitle}', flush=True)

    # ---- drop true duplicates (>= DUP_T vs v1) ----------------------------
    dropped = {}         # new_idx -> (v1_id, containment)
    for vid, j, cont, _, _, _ in v1_twin_pairs:
        if cont >= DUP_T and (j not in dropped or cont > dropped[j][1]):
            dropped[j] = (vid, cont)
    for j, (vid, cont) in sorted(dropped.items()):
        print(f'  DROP true-dup {new_works[j]["id"]} contained {cont:.3f} '
              f'in v1 {vid}', flush=True)

    kept = [j for j in range(n_new) if j not in dropped]

    # ---- 1d. version groups among new works -------------------------------
    new_new_pairs = []   # (i, j, containment)
    anomalies = []
    for a in range(n_new):
        if a in dropped:
            continue
        for b in range(a + 1, n_new):
            if b in dropped:
                continue
            inter = len(grams[a] & grams[b])
            if not inter:
                continue
            cont = inter / min(len(grams[a]), len(grams[b]))
            if cont >= TWIN_T:
                new_new_pairs.append((a, b, cont))
                if cont >= DUP_T:
                    anomalies.append(
                        f'new-new containment {cont:.3f} >= {DUP_T} between '
                        f'{new_works[a]["id"]} and {new_works[b]["id"]} '
                        f'(both KEPT; spec drops only vs v1)')
    print(f'new-new pairs >= {TWIN_T}: {len(new_new_pairs)}', flush=True)
    for a, b, cont in sorted(new_new_pairs, key=lambda p: -p[2]):
        print(f'  {cont:.3f}  {new_works[a]["id"]}  ~  {new_works[b]["id"]}',
              flush=True)

    # ---- union-find over kept new works + v1 twins ------------------------
    uf = UF()
    for a, b, _ in new_new_pairs:
        uf.union(new_works[a]['id'], new_works[b]['id'])
    for vid, j, cont, _, _, _ in v1_twin_pairs:
        if j not in dropped and cont < DUP_T:
            uf.union(new_works[j]['id'], vid)
        elif j not in dropped and cont >= DUP_T:
            # can't happen: j would be in dropped; guard anyway
            pass
    # collect components with >= 2 members
    comp = {}
    members_of = {}
    node_ids = ({new_works[j]['id'] for j in kept} |
                {vid for vid, j, cont, _, _, _ in v1_twin_pairs
                 if j not in dropped})
    for nid in sorted(node_ids):
        comp.setdefault(uf.find(nid), []).append(nid)
    vgroup_of = {}
    gid = 0
    vgroups = {}
    for root in sorted(comp):
        mem = comp[root]
        if len(mem) >= 2:
            gid += 1
            vgroups[gid] = sorted(mem)
            for m in mem:
                vgroup_of[m] = gid
    print(f'version groups: {gid}', flush=True)
    for g, mem in vgroups.items():
        print(f'  vgroup {g}: {mem}', flush=True)

    # ---- 1e. write v2 ------------------------------------------------------
    for w in v1_works:
        w['vgroup'] = vgroup_of.get(w['id'])
    final_new = []
    for j in kept:
        w = new_works[j]
        w['vgroup'] = vgroup_of.get(w['id'])
        final_new.append(w)
    v2 = v1_works + final_new
    ids = [w['id'] for w in v2]
    assert len(ids) == len(set(ids)), 'duplicate work ids in v2!'
    with open(V2_PKL, 'wb') as f:
        pickle.dump(v2, f, protocol=4)
    print(f'wrote {V2_PKL} ({os.path.getsize(V2_PKL) // 1048576} MB): '
          f'{len(v1_works)} v1 + {len(final_new)} new = {len(v2)} works',
          flush=True)

    # ---- ref2_manifest.json ------------------------------------------------
    twin_info = {}
    for vid, j, cont, vcat, vtitle, _ in v1_twin_pairs:
        twin_info.setdefault(j, []).append(
            {'v1_id': vid, 'containment': round(cont, 4),
             'v1_cat': vcat, 'v1_title': vtitle})
    by_key = {e['key']: e for e in entries}
    recs = []
    for j, w in enumerate(new_works):
        key = w['id'][5:]
        e = by_key[key]
        if j in dropped:
            outcome = f'dropped_dup:{dropped[j][0]}'
        else:
            outcome = 'ingested'
        recs.append({
            'id': w['id'], 'cat': w['cat'], 'title': w['title'],
            'title_en': w['title_en'], 'provenance': 'sefaria',
            'license': e.get('license'), 'source_url': e.get('source_url'),
            'ref_kind': e.get('ref_kind'),
            'vgroup': vgroup_of.get(w['id']),
            'n_letters': len(w['stream']),
            'dedup': outcome,
            'v1_twins': twin_info.get(j, []),
        })
    for key, n in skipped:
        e = by_key[key]
        recs.append({
            'id': f'REF2:{key}', 'cat': cat_for(key),
            'title': e.get('title_he'), 'title_en': e.get('title_en'),
            'provenance': 'sefaria', 'license': e.get('license'),
            'source_url': e.get('source_url'), 'ref_kind': e.get('ref_kind'),
            'vgroup': None, 'n_letters': n, 'dedup': 'skipped_short',
            'v1_twins': [],
        })
    out = {
        'created': time.strftime('%Y-%m-%d %H:%M:%S'),
        'v1_pkl': V1_PKL, 'v2_pkl': V2_PKL,
        'v1_works': len(v1_works),
        'staged': len(entries),
        'ingested': len(final_new),
        'dropped_dups': len(dropped),
        'skipped_short': len(skipped),
        'version_groups': {str(g): mem for g, mem in vgroups.items()},
        'new_new_pairs': [
            [new_works[a]['id'], new_works[b]['id'], round(c, 4)]
            for a, b, c in sorted(new_new_pairs, key=lambda p: -p[2])],
        'bible_quotation_pairs': [
            {'new_id': new_works[j]['id'], 'v1_id': vid,
             'containment': round(cont, 4), 'v1_title': vtitle}
            for vid, j, cont, _, vtitle, _ in
            sorted(bible_quote_pairs, key=lambda p: -p[2])],
        'anomalies': anomalies,
        'works': recs,
    }
    with open(MANIFEST_OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f'wrote {MANIFEST_OUT}', flush=True)
    print(f'TOTAL {time.time() - t0:.0f}s | v1={len(v1_works)} '
          f'ingested={len(final_new)} dropped={len(dropped)} '
          f'skipped={len(skipped)} vgroups={gid}', flush=True)
    for a in anomalies:
        print(f'ANOMALY: {a}', flush=True)


if __name__ == '__main__':
    main()
