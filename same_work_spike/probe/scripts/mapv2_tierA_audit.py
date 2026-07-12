# -*- coding: utf-8 -*-
"""MAPV2 tier-A verse-chain / canonical-rendering audit (files only, read-only).

The strict tier-A census (data/fullcorpus_v2.db :: track1_matches, filter
shadowed_by IS NULL) claims manuscript pages as witnesses of works. A verified
failure class from the graded deck reviews: pages claimed as witnesses of
POST-CLASSICAL works (cat Maagarim / JA / Sefaria — a midrash anthology, a
rishon's commentary, a Judeo-Arabic tract) purely through chains of Bible
verses or rabbinic quotations the work SHARES with the canon. Tier A itself
carries this class (a Bavli-quotation page reached tier A for רשב"ח מבוא
התלמוד).

This script runs the SAME two guards the tier-B discovery deck runs
(scripts/mapv2_deck.py -> build_smoke_preview2.query_batch_trimmed +
whole-slice Bible partial_ratio), but over the LIVE tier-A rows of the
non-guard-cat works, so a census consumer can distinguish a page that is a
real witness of a post-classical work from a page credited to it only through
canon it quotes. It NEVER modifies track1_matches or any DB — it emits an
audit report + a suspects json for future census consumers to filter on.

Guards (identical constants/mechanics to the deck):
  1. canonical-rendering guard — the row's best-span slice is batch-queried
     against the guard reference (Bible + Targum + statutory Liturgy + tafsir +
     Mishnah/Bavli/Yerushalmi/Tosefta + attestation-based well-attested
     rabbinic-genre works + guard_only statutory units from refs_staging).
     Per-guard-work UNION coverage of the slice >= GUARD_COVER_MIN (0.45) =>
     canonical-rendering SUSPECT (the evidence is the work rendering canon).
  2. whole-slice Bible alignment — partial_ratio(slice, bible_stream) >=
     BIBLE_ALIGN_MIN (60) => verse-chain SUSPECT.
A row is a suspect if EITHER guard fires.

Page-coverage class (matched span-union / page stream length) splits suspects:
  citation-grade   < 0.15  (a quote embedded in other material)
  partial          0.15-0.45
  testimony-grade  >= 0.45 (a whole page) — a verse-chain SUSPECT here is the
     dangerous case: a whole page of canon credited to a post-classical work.

Only rows whose cat is NOT canonical AND whose work_id is NOT in the guard set
(mirrors mapv2_deck.py guard_ids: base guard cats + JA tafsir + attestation
rabbinic-genre + guard_only units) are audited — the guard-cat claims have
their own citation semantics in the census and are out of scope here.

Chunked (4,000 slices) with an NDJSON checkpoint + fingerprint header exactly
like mapv2_deck.py (results/tierA_audit_ckpt.ndjson): a resume reuses prior
verdicts; a stale DB/ref/guard-param combination discards them.

Usage: python -X utf8 -u mapv2_tierA_audit.py [--db PATH] [--limit N] [--fresh]
Out:   results/mapv2_tierA_verse_audit.md
       data/tierA_verse_suspects.json
"""
import argparse
import ctypes
import json
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict

from rapidfuzz.fuzz import partial_ratio_alignment

from build_smoke_preview2 import (
    BIBLE_ALIGN_MIN, GUARD_COVER_MIN, merge_iv, query_batch_trimmed)
from normalize import norm_stream
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
REF_PKL = PROBE + r"\data\ref_corpus_v2.pkl"

# the census's canonical categories — guard-cat claims are out of scope
CANON_CATS = ('Bible', 'Targum', 'Liturgy', 'Mishnah', 'Bavli',
              'Yerushalmi', 'Tosefta')
# rarity gate for the attestation rule (mirror mapv2_deck DEFAULT_PARAMS)
RARITY_QUANTILE = 0.95
RARITY_BOUNDS = (30, 400)
GUARD_CHUNK = 4000
# page-coverage class thresholds
COV_CITATION = 0.15
COV_TESTIMONY = 0.45


def quantile(sorted_vals, q):
    if not sorted_vals:
        return 0
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def cov_class(c):
    if c < COV_CITATION:
        return 'citation'
    if c < COV_TESTIMONY:
        return 'partial'
    return 'testimony'


def build_guard(ref_all, a_ms, rarity_max):
    """Mirror mapv2_deck.py's guard construction. Returns
    (guard_works, guard_ids, guard_meta, bible_stream, n_midrash, n_grd).

    guard_meta[work_idx] = (id, title, cat) parallel to guard_works.
    """
    guard_works = [w for w in ref_all
                   if w['cat'] in ('Bible', 'Targum', 'Liturgy', 'Mishnah',
                                   'Bavli', 'Yerushalmi', 'Tosefta')
                   or (w['cat'] == 'JA' and 'תפסיר' in (w['title'] or ''))]
    guard_ids = {w['id'] for w in guard_works}
    # attestation rule: well-attested rabbinic-genre works (witnesses above
    # the rarity cutoff) are the shared quotation sources (Tanhuma, Rabbot...)
    n_midrash = 0
    for w in ref_all:
        if w['id'] not in guard_ids \
                and (w.get('genre') or '') == 'תלמוד ומדרש' \
                and len(a_ms.get(w['id'], ())) > rarity_max:
            guard_works.append(w)
            guard_ids.add(w['id'])
            n_midrash += 1
    # guard-only statutory units (kaddish, kol chamira, hamapil, sheva
    # berachot...): never in the census reference; GRD: ids never equal a
    # claimed work id, so the audit-set exclusion is unaffected
    n_grd = 0
    man_p = os.path.join(PROBE, 'refs_staging', 'manifest.json')
    if os.path.exists(man_p):
        for e in json.load(open(man_p, encoding='utf-8'))['entries']:
            if not e.get('guard_only'):
                continue
            bp = os.path.join(PROBE, 'refs_staging', e['body_file'])
            if not os.path.exists(bp):
                continue
            stream = norm_stream(
                open(bp, encoding='utf-8', errors='replace').read())[0]
            if len(stream) < 20:
                continue
            guard_works.append({'id': 'GRD:' + e['key'], 'stream': stream,
                                'cat': 'Liturgy',
                                'title': e.get('title', e['key'])})
            n_grd += 1
    guard_meta = [(w['id'], (w.get('title') or w['id']), w['cat'])
                  for w in guard_works]
    bible_stream = ''.join(w['stream'] for w in ref_all if w['cat'] == 'Bible')
    return guard_works, guard_ids, guard_meta, bible_stream, n_midrash, n_grd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=PROBE + r"\data\fullcorpus_v2.db")
    ap.add_argument('--limit', type=int, default=None,
                    help='debug: audit only the first N (page-id-ordered) '
                         'non-guard tier-A rows')
    ap.add_argument('--fresh', action='store_true',
                    help='ignore existing guard checkpoint')
    args = ap.parse_args()
    # BelowNormal self-priority (a deck build may own the machine)
    try:
        ctypes.windll.kernel32.SetPriorityClass(
            ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)
    except Exception:
        pass
    t0 = time.time()

    os.makedirs(PROBE + r"\results", exist_ok=True)
    os.makedirs(PROBE + r"\data", exist_ok=True)
    ckpt_path = PROBE + r"\results\tierA_audit_ckpt.ndjson"
    out_report = PROBE + r"\results\mapv2_tierA_verse_audit.md"
    out_json = PROBE + r"\data\tierA_verse_suspects.json"
    if args.fresh and os.path.exists(ckpt_path):
        os.remove(ckpt_path)

    con = sqlite3.connect('file:' + args.db.replace('\\', '/') + '?mode=ro',
                          uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(track1_matches)")}
    if 'shadowed_by' not in cols:
        sys.exit("ABORT: track1_matches has no shadowed_by column — "
                 "run track1_shadow first (chain step 3).")

    # ---- live tier-A witness counts per work + rarity gate ----
    a_ms = defaultdict(set)
    for wid, sid, sh in con.execute(
            "SELECT work_id, sys_id, shadowed_by FROM track1_matches"):
        if sh is None:
            a_ms[wid].add(sid)
    counts = sorted(len(v) for v in a_ms.values())
    lo_b, hi_b = RARITY_BOUNDS
    rarity_max = max(lo_b, min(hi_b, quantile(counts, RARITY_QUANTILE)))
    print(f"tier A: {sum(counts):,} live (ms,work) links, {len(a_ms):,} works; "
          f"witness q{int(RARITY_QUANTILE*100)}="
          f"{quantile(counts, RARITY_QUANTILE)} -> rarity_max={rarity_max}",
          flush=True)

    # ---- guard reference (same construction as the deck) ----
    import pickle
    ref_all = pickle.load(open(REF_PKL, 'rb'))
    (guard_works, guard_ids, guard_meta, bible_stream,
     n_midrash, n_grd) = build_guard(ref_all, a_ms, rarity_max)
    del ref_all
    guard_ref = build_ref_index(guard_works)[:6]
    print(f"guard reference: {len(guard_works)} works "
          f"(+{n_midrash} attestation rabbinic, +{n_grd} guard-only units); "
          f"guard_ids={len(guard_ids)}; bible stream {len(bible_stream):,} "
          f"({time.time()-t0:.0f}s)", flush=True)

    # ---- audit rows: live, non-canon-cat, work not in guard set ----
    rows = []   # (pid, sid, wid, cat, author, title, spans)
    for pid, sid, wid, cat, author, title, sj in con.execute(
            "SELECT page_id, sys_id, work_id, cat, author, title, spans_json "
            "FROM track1_matches WHERE shadowed_by IS NULL ORDER BY page_id"):
        if cat in CANON_CATS or wid in guard_ids:
            continue
        spans = [(int(s[0]), int(s[1])) for s in json.loads(sj)]
        rows.append((pid, sid, wid, cat, author or '', title or '', spans))
    if args.limit is not None:
        rows = rows[:args.limit]
    bycat_all = Counter(r[3] for r in rows)
    print(f"audit set: {len(rows):,} live tier-A rows "
          f"(non-canon, non-guard); by cat {dict(bycat_all)}"
          + (f" [--limit {args.limit}]" if args.limit is not None else ""),
          flush=True)

    # ---- checkpoint fingerprint (stale verdicts must never bypass) ----
    fp = {'hdr': 1, 'db': args.db,
          'db_mtime': int(os.path.getmtime(args.db)),
          'ref_mtime': int(os.path.getmtime(REF_PKL)),
          'guard': [GUARD_COVER_MIN, BIBLE_ALIGN_MIN, rarity_max,
                    n_midrash, n_grd]}
    done = {}
    stale = False
    if os.path.exists(ckpt_path):
        with open(ckpt_path, encoding='utf-8') as f:
            first = f.readline()
            try:
                ok = json.loads(first) == fp
            except Exception:
                ok = False
            if ok:
                for line in f:
                    try:
                        r = json.loads(line)
                        done[(r['pid'], r['wid'])] = r
                    except Exception:
                        pass
                print(f"checkpoint: {len(done):,} verdicts loaded", flush=True)
            else:
                stale = True
        if stale:
            print("checkpoint fingerprint MISMATCH — discarding old verdicts",
                  flush=True)
            os.remove(ckpt_path)
    if not os.path.exists(ckpt_path):
        with open(ckpt_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(fp) + "\n")
    ck = open(ckpt_path, 'a', encoding='utf-8')

    # ---- guard loop (chunked, checkpointed, page-locality stream cache) ----
    pcache = {}

    def pstream(pid_):
        if pid_ not in pcache:
            if len(pcache) > 8000:
                pcache.clear()
            t = con.execute("SELECT text FROM pages WHERE page_id=?",
                            (pid_,)).fetchone()
            pcache[pid_] = norm_stream(t[0] or '')[0] if t else ''
        return pcache[pid_]

    todo = [(i, r) for i, r in enumerate(rows) if (r[0], r[2]) not in done]
    print(f"guard todo: {len(todo):,} of {len(rows):,} "
          f"({time.time()-t0:.0f}s)", flush=True)
    CH = GUARD_CHUNK
    for c0 in range(0, len(todo), CH):
        chunk = todo[c0:c0 + CH]
        slices, meta = [], []
        for i, r in chunk:
            pid, sid, wid, cat, author, title, spans = r
            ps = pstream(pid)
            if spans:
                b0, b1 = max(spans, key=lambda s: s[1] - s[0])
            else:
                b0, b1 = 0, 0
            sl = ps[b0:b1]
            iv = merge_iv([(a, b) for a, b in spans])
            row_union = sum(b - a for a, b in iv)
            pc = row_union / max(1, len(ps))
            slices.append(sl)
            meta.append((r, sl, pc))
        results = query_batch_trimmed(slices, guard_ref)
        for k, (r, sl, pc) in enumerate(meta):
            slen = max(1, len(sl))
            # guard 1: per-guard-work UNION coverage of the slice
            per_work = defaultdict(list)
            for wi_g, hp0, hp1 in results[k]:
                per_work[wi_g].append((hp0, hp1))
            best_cov, best_gw = 0.0, None
            for wi_g, ivs in per_work.items():
                cov = sum(b - a for a, b in merge_iv(ivs)) / slen
                if cov > best_cov:
                    best_cov, best_gw = cov, wi_g
            gs = 1 if best_cov >= GUARD_COVER_MIN else 0
            gw_id = guard_meta[best_gw][0] if best_gw is not None else None
            # guard 2: whole-slice Bible alignment
            bs, bsc = 0, 0.0
            if len(sl) >= 30:
                res = partial_ratio_alignment(sl, bible_stream,
                                              score_cutoff=BIBLE_ALIGN_MIN)
                if res is not None:
                    bsc = res.score
                    if res.score >= BIBLE_ALIGN_MIN:
                        bs = 1
            rec = {'pid': r[0], 'wid': r[2], 'gs': gs,
                   'gc': round(best_cov, 3), 'gw': gw_id,
                   'bs': bs, 'bsc': round(bsc, 1), 'pc': round(pc, 4)}
            done[(r[0], r[2])] = rec
            ck.write(json.dumps(rec, ensure_ascii=False) + "\n")
        ck.flush()
        print(f"  guard chunk {c0//CH + 1}/{(len(todo)+CH-1)//CH} "
              f"({time.time()-t0:.0f}s)", flush=True)
    ck.close()

    # ---- aggregate ----
    guard_title = {gid: ttl for gid, ttl, _c in guard_meta}
    cls_all = Counter()          # coverage class over all audited rows
    cls_suspect = Counter()      # coverage class over suspects
    suspect_cat = Counter()      # suspects by cat
    reason = Counter()           # guard_only / bible_only / both
    work_agg = {}                # wid -> aggregation dict
    suspects = []                # full evidence records
    n_missing = 0
    for pid, sid, wid, cat, author, title, spans in rows:
        rec = done.get((pid, wid))
        if rec is None:
            n_missing += 1
            continue
        suspect = bool(rec['gs'] or rec['bs'])
        cls = cov_class(rec['pc'])
        cls_all[cls] += 1
        wa = work_agg.get(wid)
        if wa is None:
            wa = work_agg[wid] = {
                'cat': cat, 'title': title, 'author': author,
                'sys_all': set(), 'sys_nonsuspect': set(),
                'n_rows': 0, 'n_suspect': 0}
        wa['sys_all'].add(sid)
        wa['n_rows'] += 1
        if suspect:
            wa['n_suspect'] += 1
            cls_suspect[cls] += 1
            suspect_cat[cat] += 1
            if rec['gs'] and rec['bs']:
                reason['both'] += 1
            elif rec['gs']:
                reason['guard_only'] += 1
            else:
                reason['bible_only'] += 1
            name = f"{author} — {title}" if author else title
            gw_ttl = guard_title.get(rec['gw'], rec['gw']) if rec['gw'] else ''
            suspects.append({
                'page_id': pid, 'sys_id': sid, 'work_id': wid, 'cat': cat,
                'title': name, 'page_coverage': rec['pc'],
                'coverage_class': cls,
                'guard_suspect': bool(rec['gs']), 'guard_cover': rec['gc'],
                'guard_work_id': rec['gw'], 'guard_work_title': gw_ttl,
                'bible_suspect': bool(rec['bs']), 'bible_score': rec['bsc']})
        else:
            wa['sys_nonsuspect'].add(sid)

    n_aud = len(rows) - n_missing
    n_sus = len(suspects)
    print(f"aggregated: audited {n_aud:,}, suspects {n_sus:,} "
          f"({100*n_sus/max(1,n_aud):.1f}%); missing verdicts {n_missing:,}",
          flush=True)

    # ---- suspects json (all pairs + evidence) ----
    suspects.sort(key=lambda s: (-s['page_coverage'], s['work_id']))
    json.dump({
        'meta': {'db': args.db, 'audited_rows': n_aud, 'suspect_rows': n_sus,
                 'guard_cover_min': GUARD_COVER_MIN,
                 'bible_align_min': BIBLE_ALIGN_MIN,
                 'rarity_max': rarity_max,
                 'coverage_classes': {'citation': f'<{COV_CITATION}',
                                      'partial': f'{COV_CITATION}-{COV_TESTIMONY}',
                                      'testimony': f'>={COV_TESTIMONY}'},
                 'limit': args.limit},
        'suspects': suspects,
    }, open(out_json, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"wrote {out_json}", flush=True)

    # ---- report ----
    L = ["# MAPV2 tier-A verse-chain / canonical-rendering audit\n"]
    L.append(f"- DB: `{args.db}`")
    if args.limit is not None:
        L.append(f"- **DEBUG RUN — --limit {args.limit}** (partial; not the "
                 f"full census audit)")
    L.append(f"- audited: live tier-A rows (`shadowed_by IS NULL`) of "
             f"NON-canonical cats whose work is not in the guard set")
    L.append(f"- guards (same as the tier-B deck): canonical-rendering "
             f"per-work UNION coverage >= {GUARD_COVER_MIN}; whole-slice "
             f"Bible partial_ratio >= {BIBLE_ALIGN_MIN}")
    L.append(f"- guard reference: {len(guard_works)} works "
             f"(+{n_midrash} attestation rabbinic-genre, +{n_grd} guard-only "
             f"statutory units); rarity_max={rarity_max}")
    L.append(f"- coverage classes: citation `<{COV_CITATION}`, partial "
             f"`{COV_CITATION}-{COV_TESTIMONY}`, testimony `>={COV_TESTIMONY}` "
             f"(of page stream)")
    L.append("")
    L.append(f"## Totals\n")
    L.append(f"- audited rows: **{n_aud:,}**")
    L.append(f"- suspect rows: **{n_sus:,}** "
             f"({100*n_sus/max(1,n_aud):.2f}%)")
    L.append(f"- suspect reason: guard-only {reason['guard_only']:,}, "
             f"Bible-align-only {reason['bible_only']:,}, both "
             f"{reason['both']:,}")
    L.append("")
    L.append("## By reference category\n")
    L.append("| cat | audited | suspects | rate |")
    L.append("|---|--:|--:|--:|")
    for cat in sorted(bycat_all, key=lambda c: -bycat_all[c]):
        aud = bycat_all[cat]
        sus = suspect_cat[cat]
        L.append(f"| {cat} | {aud:,} | {sus:,} | "
                 f"{100*sus/max(1,aud):.1f}% |")
    L.append("")
    L.append("## By page-coverage class\n")
    L.append("| class | audited | suspects | suspect rate |")
    L.append("|---|--:|--:|--:|")
    for cls in ('citation', 'partial', 'testimony'):
        aud = cls_all[cls]
        sus = cls_suspect[cls]
        L.append(f"| {cls} | {aud:,} | {sus:,} | "
                 f"{100*sus/max(1,aud):.1f}% |")
    L.append("")
    L.append(f"**Dangerous class** (verse/canon SUSPECT at testimony-grade "
             f"coverage — a whole page of canon credited to a post-classical "
             f"work): **{cls_suspect['testimony']:,}** rows.")
    L.append("")

    # per-work suspect table (top 50 by suspect count)
    work_rows = []
    for wid, wa in work_agg.items():
        if wa['n_suspect'] == 0:
            continue
        before = len(a_ms.get(wid, wa['sys_all']))
        removed = len(wa['sys_all'] - wa['sys_nonsuspect'])
        after = before - removed
        work_rows.append((wa['n_suspect'], wid, wa['cat'],
                          (wa['author'] + ' — ' + wa['title']) if wa['author']
                          else wa['title'], wa['n_rows'], before, after))
    work_rows.sort(key=lambda t: (-t[0], -t[4]))
    L.append(f"## Suspect rate per work — top 50 of "
             f"{len(work_rows):,} works with >=1 suspect\n")
    L.append("| work_id | cat | title | rows | suspects | witnesses before | "
             "witnesses after removal |")
    L.append("|---|---|---|--:|--:|--:|--:|")
    for n_sus_w, wid, cat, name, n_rows_w, before, after in work_rows[:50]:
        nm = name.replace('|', '/')[:55]
        L.append(f"| `{wid}` | {cat} | {nm} | {n_rows_w:,} | {n_sus_w:,} | "
                 f"{before:,} | {after:,} |")
    L.append("")

    # 30 sample suspect rows (dangerous first — sorted by coverage desc)
    L.append("## 30 sample suspect rows (highest page-coverage first)\n")
    L.append("| page_id | sys_id | cat | work | coverage | class | covered by |")
    L.append("|---|---|---|---|--:|---|---|")
    for s in suspects[:30]:
        cov_by = (s['guard_work_title'] if s['guard_suspect']
                  else f"Bible-align {s['bible_score']:.0f}")
        nm = s['title'].replace('|', '/')[:40]
        cb = str(cov_by).replace('|', '/')[:40]
        L.append(f"| `{s['page_id']}` | {s['sys_id']} | {s['cat']} | {nm} | "
                 f"{s['page_coverage']:.2f} | {s['coverage_class']} | {cb} |")
    L.append("")
    L.append(f"- total time: {time.time()-t0:.0f}s")
    open(out_report, 'w', encoding='utf-8').write("\n".join(L) + "\n")
    print(f"wrote {out_report}; TOTAL {time.time()-t0:.0f}s", flush=True)
    con.close()


if __name__ == '__main__':
    main()
