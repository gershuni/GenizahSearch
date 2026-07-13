# -*- coding: utf-8 -*-
"""MAPV2-15b — the LOCKED audit sample (the "fair test batch").

Codex final-gate conditions (e)+(f): draw a frozen, stratified sample from
the RAW match frame (track1_matches, BEFORE any stitching/routing) so that
Stage-0 (stitch / scope) errors are measurable against their own
denominator. Strata cover stitch status (chained / merge-eligible / weak-two-
work / singleton), genre, match size, and — recorded per row — scope regime,
title_class, bib_class. Frame cell sizes are stored so corpus-wide rates can
be recovered by post-stratified weighting even though rare strata are
oversampled. The output is FROZEN and must never be tuned against.

Deterministic (no RNG): each row gets a stable sha1 rank; within a cell we
take the lowest-rank rows, so re-running reproduces the identical sample.

Usage: python -X utf8 -u build_audit_sample.py [--target 420]
Writes: data/audit_sample_v1.json  +  results/audit_sample_v1.md
"""
import argparse
import hashlib
import json
import os
import sqlite3
from collections import Counter, defaultdict

from bib_gate import BibGate
from metadata_scope import ScopeGate

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
OUT_JSON = PROBE + r"\data\audit_sample_v1.json"
OUT_MD = PROBE + r"\results\audit_sample_v1.md"
VERSION = 1

_ap = argparse.ArgumentParser()
_ap.add_argument('--target', type=int, default=420)
_args = _ap.parse_args()

# collapse the corpus genre labels into audit buckets; oversample the
# discovery-bearing genres, down-weight Bible (mostly known verse matches).
GENRE_BUCKET = {
    '-מקרא': 'bible', 'פיוט ותפילה': 'piyyut', 'תלמוד ומדרש': 'talmud_midrash',
    'ספרות רבנית': 'rabbinic', 'ערבית יהודית': 'judeo_arabic',
    'ספרות הגאונים': 'geonic', 'שירת ספרד': 'sefarad',
    'ספרות הקראים': 'karaite', 'איגרות': 'letters',
    'ספרות התרגומים מערבית': 'targum',
}
# per-genre-bucket audit target (Bible deliberately small vs its frame share)
BUCKET_TARGET = {
    'bible': 45, 'piyyut': 70, 'talmud_midrash': 60, 'rabbinic': 45,
    'judeo_arabic': 45, 'geonic': 30, 'sefarad': 30, 'karaite': 25,
    'letters': 15, 'targum': 15, 'other': 30,
}


def rank(page_id, work_id):
    return int(hashlib.sha1(f"{page_id}|{work_id}".encode()).hexdigest(), 16)


def letters_band(n):
    if n < 100:
        return 'xs'
    if n < 300:
        return 's'
    if n < 800:
        return 'm'
    return 'l'


def main():
    con = sqlite3.connect(DB)

    # stitch signals
    flags = {}
    for pid, mf, wtw in con.execute(
            "SELECT page_id, merge_flag, weak_two_work_flag FROM mapv2_page_flags"):
        flags[pid] = (mf, wtw)
    chained = {}
    # ORDER BY for a stable chain-id assignment (else component_key depends on
    # DB iteration order and the split is not reproducible).
    for i, (a, b) in enumerate(con.execute(
            "SELECT pages_a, pages_b FROM page_chains_accepted_pairs_canonmask "
            "ORDER BY sys_a, sys_b, pages_a")):
        for p in set((a or '').split(',')) | set((b or '').split(',')):
            if p:
                chained.setdefault(p, i)   # page -> chain id

    def stitch_status(pid):
        if pid in chained:
            return 'chained'
        mf, wtw = flags.get(pid, (0, 0))
        if mf:
            return 'merge_eligible'
        if wtw:
            return 'weak_two_work'
        return 'singleton'

    # raw frame (non-shadowed, substantive)
    rows = con.execute(
        "SELECT page_id, sys_id, work_id, cat, genre, author, title, "
        "matched_letters, best_density, n_spans "
        "FROM track1_matches WHERE shadowed_by IS NULL AND matched_letters>=40"
    ).fetchall()
    print(f"raw frame: {len(rows):,} matches", flush=True)

    # frame cell sizes for post-stratification weights
    frame_cells = Counter()
    n_matched_works = defaultdict(set)
    enriched = []
    for r in rows:
        pid, sid, wid, cat, genre, au, ti, ml, dens, nsp = r
        gb = GENRE_BUCKET.get(genre, 'other')
        cell = (gb, letters_band(ml), stitch_status(pid))
        frame_cells[cell] += 1
        n_matched_works[sid].add(wid)
        enriched.append((r, gb, cell))
    nmw = {s: len(w) for s, w in n_matched_works.items()}

    # deterministic stratified draw: per genre bucket, spread over
    # (letters_band, stitch_status) cells, lowest-rank first, with a floor
    # that guarantees the rare stitch strata are represented.
    by_bucket = defaultdict(list)
    for (r, gb, cell) in enriched:
        by_bucket[gb].append((r, cell))

    # honor --target by scaling the per-bucket targets to hit that total
    _scale = _args.target / max(1, sum(BUCKET_TARGET.values()))
    picked = []
    for gb, items in by_bucket.items():
        target = max(3, round(BUCKET_TARGET.get(gb, 20) * _scale))
        cells = defaultdict(list)
        for (r, cell) in items:
            cells[cell].append(r)
        for c in cells:
            cells[c].sort(key=lambda r: rank(r[0], r[2]))
        # floor: ensure each present stitch-status within the bucket gets >=3
        want = {}
        stitch_present = {c[2] for c in cells}
        base = max(1, target // max(1, len(cells)))
        for c in cells:
            want[c] = base
        for st in stitch_present:
            for c in cells:
                if c[2] == st:
                    want[c] = max(want[c], 3)
        # take
        taken = 0
        for c in sorted(cells, key=lambda c: -len(cells[c])):
            k = min(want[c], len(cells[c]))
            for r in cells[c][:k]:
                picked.append((r, gb, c))
                taken += 1
        # top up toward target from remaining lowest-rank rows in the bucket
        if taken < target:
            seen = {(r[0], r[2]) for (r, _, _) in picked}
            pool = sorted((r for (r, _) in items
                           if (r[0], r[2]) not in seen),
                          key=lambda r: rank(r[0], r[2]))
            for r in pool[:target - taken]:
                picked.append((r, gb, (gb, letters_band(r[7]),
                                       stitch_status(r[0]))))

    print(f"drawn: {len(picked):,}", flush=True)

    # enrich sampled rows with scope / title / bib / resolution + snippet
    sg = ScopeGate(n_matched_works=nmw)
    bg = BibGate()
    # n_pages per sampled sys_id
    samp_sids = {r[1] for (r, _, _) in picked}
    npages = {}
    ph = list(samp_sids)
    for i in range(0, len(ph), 400):
        batch = ph[i:i + 400]
        qmarks = ','.join('?' * len(batch))
        for sid, c in con.execute(
                f"SELECT sys_id, COUNT(*) FROM pages WHERE sys_id IN ({qmarks}) "
                f"GROUP BY sys_id", batch):
            npages[sid] = c
    sg.n_pages = npages

    # page text (snippet + dedup hash) for sampled pages
    samp_pids = {r[0] for (r, _, _) in picked}
    ptext = {}
    ph = list(samp_pids)
    for i in range(0, len(ph), 400):
        batch = ph[i:i + 400]
        qmarks = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({qmarks})",
                batch):
            ptext[pid] = tx or ''

    items = []
    for (r, gb, cell) in picked:
        pid, sid, wid, cat, genre, au, ti, ml, dens, nsp = r
        name = f"{au} — {ti}" if au else (ti or '')
        tcls, tev = sg.tg.classify(sid, name)
        bcls, _ = bg.classify(sid, name, author=au, title=ti)
        resn = sg.resolution(sid, name)
        sc = sg.scope(sid)
        txt = ptext.get(pid, '')
        thash = hashlib.sha1(''.join(txt.split()).encode()).hexdigest()[:16]
        items.append({
            'page_id': pid, 'sys_id': sid, 'work_id': wid,
            'cat': cat, 'genre': genre, 'author': au, 'title': ti,
            'matched_letters': ml, 'best_density': round(dens or 0, 4),
            'n_spans': nsp,
            # strata
            'genre_bucket': gb, 'letters_band': letters_band(ml),
            'stitch_status': cell[2],
            'chain_id': chained.get(pid),
            'merge_flag': flags.get(pid, (0, 0))[0],
            'weak_two_work_flag': flags.get(pid, (0, 0))[1],
            'scope_regime': sc['regime'], 'scope_conf': sc['confidence'],
            'title_class': tcls, 'bib_class': bcls, 'resolution': resn,
            'n_pages_this_ms': npages.get(sid, 0),
            # leakage-safe split keys + dedup
            'page_text_hash': thash,
            'component_key': f"sys:{sid}" if pid not in chained
            else f"chain:{chained[pid]}",
            'page_snippet': txt[:280],
        })
    # dedup flag within the sample (identical page text under != sys_id)
    byhash = defaultdict(set)
    for it in items:
        byhash[it['page_text_hash']].add(it['sys_id'])
    for it in items:
        it['dedup_dup'] = len(byhash[it['page_text_hash']]) > 1

    frame_hash = hashlib.sha1(
        json.dumps(sorted(f"{it['page_id']}|{it['work_id']}" for it in items),
                   ensure_ascii=False).encode()).hexdigest()[:16]
    manifest = {
        'version': VERSION,
        'frozen': True,
        'note': 'LOCKED audit sample — never tune against it; keep active-'
                'learning labels out. Post-stratify by frame_cells for '
                'corpus-wide rates. Split leakage-safe by component_key '
                '(+ page_text_hash for dedup).',
        'frame_total': len(rows),
        'n_sampled': len(items),
        'frame_cells': {f"{k[0]}|{k[1]}|{k[2]}": v
                        for k, v in sorted(frame_cells.items())},
        'sample_hash': frame_hash,
        'rank_method': 'sha1(page_id|work_id), lowest-first per cell',
    }
    json.dump({'manifest': manifest, 'items': items},
              open(OUT_JSON, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)

    # manifest report
    def dist(key):
        c = Counter(it[key] for it in items)
        return ', '.join(f"{k}:{v}" for k, v in c.most_common())
    L = [f"# MAPV2-15b — locked audit sample v{VERSION}  (FROZEN)", "",
         f"- raw frame (non-shadowed, matched_letters≥40): **{len(rows):,}**",
         f"- sampled (frozen): **{len(items)}**   hash `{frame_hash}`",
         "- **Do not tune against this sample.** Post-stratify by the frame "
         "cell sizes in the JSON manifest for corpus-wide rates; split "
         "leakage-safe by `component_key` (+ `page_text_hash`).", "",
         "## sample composition", "",
         f"- genre bucket: {dist('genre_bucket')}",
         f"- match size: {dist('letters_band')}",
         f"- stitch status: {dist('stitch_status')}",
         f"- scope regime: {dist('scope_regime')}",
         f"- title_class: {dist('title_class')}",
         f"- bib_class: {dist('bib_class')}",
         f"- resolution: {dist('resolution')}",
         f"- dedup duplicates flagged: {sum(1 for it in items if it['dedup_dup'])}",
         "", "## stitch strata (Stage-0 error denominator)", "",
         f"- chained pages in frame: {len([1 for c,n in frame_cells.items() if c[2]=='chained' for _ in range(n)])}",
         ]
    open(OUT_MD, 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    print(f"wrote {OUT_JSON} + {OUT_MD}")
    print("stitch:", dist('stitch_status'))
    print("scope:", dist('scope_regime'))
    print("resolution:", dist('resolution'))
    con.close()


if __name__ == '__main__':
    main()
