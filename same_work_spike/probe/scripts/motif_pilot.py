# -*- coding: utf-8 -*-
"""Sequence-aware decomposition pilot — motifs from the liturgy continuum.

The continuum (18,676 MSS / 91,731 pages) chains through shared prayer
SEQUENCES; flat closure over span occurrences cannot split it. This
pilot decomposes it like synteny blocks:

1. BREAKPOINTS: on each page, cluster the endpoints of all its aligned
   spans (gap-merge tolerance) -> elementary segments that never span an
   observed alignment boundary.
2. LINKS: within each accepted pair, map A-segments to B-segments by
   monotone (linear) position mapping inside the aligned span.
3. MOTIFS: connected components over segment links. A motif = one
   passage (prayer / piyyut / formula); a page = a sequence of motifs.

Validation (acceptance test): the Birkat Hamazon witness index
(data/bh_witnesses.json, 597 sys_ids resolved by a human scholar) —
BH-anchored motifs should recover the known witnesses at high recall,
and their NON-index members are candidate UNKNOWN BH witnesses
(deliverable: review/bh_new_witnesses.html).

Usage: python motif_pilot.py [db] [pairs_table] [tag]
Out: results/motifs_<tag>.md, review/bh_new_witnesses_<tag>.html
     (tag 'pilot' keeps the original names), <db>::motifs_pilot /
     motif_members_pilot
"""
import html
import json
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
TABLE = sys.argv[2] if len(sys.argv) > 2 else "accepted_pairs_canonmask"
TAG = sys.argv[3] if len(sys.argv) > 3 else "pilot"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
MD_OUT = ROOT + rf"\same_work_spike\probe\results\motifs_{TAG}.md"
HTML_OUT = (ROOT + r"\same_work_spike\probe\review\bh_new_witnesses.html"
            if TAG == 'pilot' else
            ROOT + rf"\same_work_spike\probe\review\bh_new_witnesses_{TAG}.html")

BP_GAP = 25          # endpoint cluster gap (HTR jitter absorption)
MIN_SEG = 40         # elementary segments shorter than this are ignored
# Link strictness (argv[4], default 'loose'): measured trade-off
# (2026-07-08): loose maximizes witness recall on SPARSE pair data
# (canonmask: BH 119, candidates 71, max motif 691); strict prevents
# mega-motif re-chaining on DENSE data (liturgy pass: loose max motif
# 7,572 swallowing BH; strict 5,913/BH 109; on canonmask strict gives
# max 202 but BH 101). Proper fix for dense data = community detection
# on the segment graph (motif v2).
STRICT = len(sys.argv) > 4 and sys.argv[4] == 'strict'
MIN_LINK_COVER = 0.75 if STRICT else 0.5
LEN_RATIO_MAX = 1.6 if STRICT else 2.5
MOTIF_MIN_SYS = 2

# BH text anchors — passed through norm_stream at startup (the stream
# folds final letters: הזן -> הזנ; matching raw final-form phrases was a
# silent no-op in the first pilot run). Phrases must be LONG: the stream
# has no word boundaries, so short phrases hit across words (round-3
# debug: 'הזן את' matched 'סואה זנאתך' in a JA Ezekiel tafsir).
_BH_PHRASES_RAW = {
    'hazan_olam': 'הזן את העולם',
    'hazan_kol': 'הזן את הכל',
    'matnat': 'מתנת בשר ודם',
    'node': 'נודה לך יי אלהינו',
    'al_haaretz': 'על הארץ ועל המזון',
    'rahem_amkha': 'על ישראל עמך',
    'boneh': 'בונה ירושלים',
    'eretz_hemda': 'ארץ חמדה טובה ורחבה',
    'zan_umfarnes': 'זן ומפרנס',
}
BH_PHRASES = {k: norm_stream(v)[0] for k, v in _BH_PHRASES_RAW.items()}


class DSU:
    __slots__ = ('p',)

    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        p = self.p
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def load_lib_meta():
    import csv
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?', title)
    return meta


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)

    # ---- continuum membership (largest passage unit) ----
    uid = con.execute(
        f"SELECT unit FROM passage_units_{TABLE} "
        f"ORDER BY n_ms DESC LIMIT 1").fetchone()[0]
    cont_pages = {r[0] for r in con.execute(
        f"SELECT page_id FROM passage_unit_members_{TABLE} WHERE unit=?",
        (uid,))}
    print(f"continuum unit {uid}: {len(cont_pages):,} pages", flush=True)

    # ---- pairs inside the continuum ----
    pairs = []
    page_sys = {}
    for (pa, pb, sa, sb, a0, a1, b0, b1) in con.execute(f"""
            SELECT page_a, page_b, sys_a, sys_b, a0, a1, b0, b1
            FROM {TABLE} WHERE dup_shelf = 0 AND dup_lines < 0.6"""):
        if pa in cont_pages and pb in cont_pages:
            pairs.append((pa, pb, a0, a1, b0, b1))
            page_sys[pa] = sa
            page_sys[pb] = sb
    print(f"continuum pairs: {len(pairs):,} ({time.time() - t0:.0f}s)",
          flush=True)

    # ---- breakpoints per page ----
    endpoints = defaultdict(list)
    for pa, pb, a0, a1, b0, b1 in pairs:
        endpoints[pa].extend((a0, a1))
        endpoints[pb].extend((b0, b1))
    seg_bounds = {}          # page -> sorted list of segment starts+end
    seg_index = {}           # page -> global id of first segment
    n_seg = 0
    for pid, eps in endpoints.items():
        eps.sort()
        bps = [eps[0]]
        for e in eps[1:]:
            if e - bps[-1] > BP_GAP:
                bps.append(e)
        # merge to elementary segments, dropping dust
        segs = []
        for i in range(len(bps) - 1):
            if bps[i + 1] - bps[i] >= MIN_SEG:
                segs.append((bps[i], bps[i + 1]))
        if not segs:
            segs = [(bps[0], bps[-1] + MIN_SEG)]
        seg_bounds[pid] = segs
        seg_index[pid] = n_seg
        n_seg += len(segs)
    print(f"elementary segments: {n_seg:,} on {len(seg_bounds):,} pages "
          f"({time.time() - t0:.0f}s)", flush=True)

    def segs_in(pid, s, e):
        """Local indices of segments substantially inside [s, e]."""
        out = []
        for li, (gs, ge) in enumerate(seg_bounds[pid]):
            ov = min(e, ge) - max(s, gs)
            if ov >= MIN_LINK_COVER * (ge - gs):
                out.append(li)
        return out

    def seg_at(pid, pos):
        for li, (gs, ge) in enumerate(seg_bounds[pid]):
            if gs <= pos < ge:
                return li
        return None

    # ---- link segments across pairs (monotone mapping) ----
    dsu = DSU(n_seg)
    n_links = 0
    for pa, pb, a0, a1, b0, b1 in pairs:
        scale = (b1 - b0) / max(1, a1 - a0)
        for li in segs_in(pa, a0, a1):
            gs, ge = seg_bounds[pa][li]
            mid = (max(gs, a0) + min(ge, a1)) / 2
            mapped = b0 + (mid - a0) * scale
            lj = seg_at(pb, mapped)
            if lj is None:
                continue
            hs, he = seg_bounds[pb][lj]
            la, lb_ = ge - gs, he - hs
            if max(la, lb_) / max(1, min(la, lb_)) > LEN_RATIO_MAX:
                continue
            dsu.union(seg_index[pa] + li, seg_index[pb] + lj)
            n_links += 1
    print(f"links: {n_links:,} ({time.time() - t0:.0f}s)", flush=True)

    # ---- motifs = components ----
    members = defaultdict(list)   # root -> [(page, ls, le)]
    for pid, segs in seg_bounds.items():
        base = seg_index[pid]
        for li, (gs, ge) in enumerate(segs):
            members[dsu.find(base + li)].append((pid, gs, ge))
    motifs = {r: m for r, m in members.items()
              if len({page_sys[p] for p, _, _ in m}) >= MOTIF_MIN_SYS
              and len(m) >= 2}
    sizes = Counter(len({page_sys[p] for p, _, _ in m})
                    for m in motifs.values())
    print(f"motifs (>= {MOTIF_MIN_SYS} MSS): {len(motifs):,}; "
          f"size dist head {dict(sorted(sizes.items())[:8])}; "
          f"max {max(sizes) if sizes else 0}", flush=True)

    # ---- BH anchoring ----
    bh = json.load(open(BH, encoding='utf-8'))
    bh_sys = set()
    for sig, w in bh['witnesses'].items():
        for sm in w['shelfmarks']:
            bh_sys.update(sm.get('sys_ids', []))

    # streams for phrase anchoring + snippets (lazy per needed page)
    _page_cache = {}

    def page_text(pid):
        if pid not in _page_cache:
            row = con.execute("SELECT text FROM pages WHERE page_id=?",
                              (pid,)).fetchone()
            t = row[0] if row else ''
            _page_cache[pid] = (t,) + norm_stream(t)
        return _page_cache[pid]

    motif_rows = []
    for root, m in motifs.items():
        syss = {page_sys[p] for p, _, _ in m}
        n_bh = len(syss & bh_sys)
        motif_rows.append({
            'root': root, 'n_ms': len(syss), 'n_seg': len(m),
            'n_bh_sys': n_bh, 'bh_share': round(n_bh / len(syss), 3),
            'med_len': sorted(le - ls for _, ls, le in m)[len(m) // 2],
        })
    motif_rows.sort(key=lambda r: -r['n_bh_sys'])

    # ceiling: BH witnesses reachable at all from the input pairs
    bh_ceiling = {s for s in {page_sys[p] for p in seg_bounds}
                  if s in bh_sys}

    # anchor motifs: phrase scan over ALL motifs (longest segment's text)
    # + purity fallback for known-BH-dense motifs
    bh_motifs = []
    t_scan = time.time()
    for i, r in enumerate(motif_rows):
        m = motifs[r['root']]
        best = max(m, key=lambda x: x[2] - x[1])
        text, stream, offs = page_text(best[0])
        seg_stream = stream[best[1]:best[2]]
        tags = [k for k, ph in BH_PHRASES.items() if ph in seg_stream]
        r['phrases'] = tags
        r['snippet'] = ''
        if len(offs):
            e2 = min(best[2], len(offs))
            if e2 > best[1]:
                a = offs[max(0, min(best[1], len(offs) - 1))]
                z = offs[e2 - 1] + 1
                r['snippet'] = text[a:z][:260]
        if tags or (r['n_bh_sys'] >= 2 and r['bh_share'] >= 0.2):
            bh_motifs.append(r)
        if (i + 1) % 10000 == 0:
            print(f"  phrase scan {i + 1}/{len(motif_rows)} "
                  f"({time.time() - t_scan:.0f}s)", flush=True)
    covered = set()
    covered_phrase = set()
    for r in bh_motifs:
        mss = {page_sys[p] for p, _, _ in motifs[r['root']]} & bh_sys
        covered |= mss
        if r['phrases']:
            covered_phrase |= mss
    bh_motifs.sort(key=lambda r: -r['n_bh_sys'])
    print(f"BH-anchored motifs: {len(bh_motifs)} "
          f"({sum(1 for r in bh_motifs if r['phrases'])} phrase-anchored); "
          f"known BH covered: {len(covered)} (phrase-only "
          f"{len(covered_phrase)}) / ceiling {len(bh_ceiling)} "
          f"reachable of {len(bh_sys)} in index", flush=True)

    # ---- unknown-witness candidates ----
    # precision-first: the candidate's OWN segment must carry a BH phrase
    # (motif membership alone admits chained heterogeneous segments —
    # round-2 debug: top 'candidates' were JA Ezekiel exegesis + Psalms)
    cand = {}     # sys -> best (motif, page, ls, le, phrases)
    for r in bh_motifs:
        if not r['phrases']:
            continue          # only text-anchored motifs nominate
        for p, ls, le in motifs[r['root']]:
            s = page_sys[p]
            if s in bh_sys:
                continue
            _, stream, _ = page_text(p)
            own = [k for k, ph in BH_PHRASES.items()
                   if ph in stream[ls:le]]
            if not own:
                continue
            cur = cand.get(s)
            if cur is None or le - ls > cur[3] - cur[2]:
                cand[s] = (r['root'], p, ls, le, own)
    print(f"unknown-witness candidate MSS (own-segment anchored): "
          f"{len(cand)}", flush=True)

    # ---- persist ----
    con.execute("DROP TABLE IF EXISTS motifs_pilot")
    con.execute("""CREATE TABLE motifs_pilot (
        motif INT, n_ms INT, n_seg INT, n_bh_sys INT, med_len INT)""")
    con.executemany("INSERT INTO motifs_pilot VALUES (?,?,?,?,?)",
                    [(r['root'], r['n_ms'], r['n_seg'], r['n_bh_sys'],
                      r['med_len']) for r in motif_rows])
    con.execute("DROP TABLE IF EXISTS motif_members_pilot")
    con.execute("""CREATE TABLE motif_members_pilot (
        motif INT, page_id TEXT, sys_id TEXT, start INT, end INT)""")
    con.executemany(
        "INSERT INTO motif_members_pilot VALUES (?,?,?,?,?)",
        [(root, p, page_sys[p], ls, le)
         for root, m in motifs.items() for p, ls, le in m])
    con.commit()

    # ---- report ----
    meta = load_lib_meta()
    n_sing = sum(1 for c in sizes.values() if False)  # placeholder calc
    lines = [
        "# Motif pilot — sequence-aware decomposition of the continuum", "",
        f"- continuum: {len(cont_pages):,} pages, {len(pairs):,} internal "
        f"pairs -> {n_seg:,} elementary segments -> "
        f"**{len(motifs):,} motifs** (>= {MOTIF_MIN_SYS} MSS)",
        f"- motif MS-size distribution: " + " ".join(
            f"{k}:{sizes[k]}" for k in sorted(sizes)[:10]) +
        f" … max {max(sizes) if sizes else 0}", "",
        "## BH validation (acceptance test)",
        f"- known BH witness sys_ids (index): {len(bh_sys)}; reachable "
        f"in the continuum's pair data (ceiling): **{len(bh_ceiling)}**",
        f"- BH-anchored motifs: {len(bh_motifs)} "
        f"({sum(1 for r in bh_motifs if r['phrases'])} text-anchored)",
        f"- **known BH witnesses recovered: {len(covered)}** "
        f"(text-anchored motifs alone: {len(covered_phrase)}; "
        f"pairwise canonmask map connected 166)", "",
        "## BH-anchored motifs (top by known-BH members)",
    ]
    for r in bh_motifs[:25]:
        lines.append(
            f"- motif {r['root']}: {r['n_ms']} MSS ({r['n_bh_sys']} known "
            f"BH = {r['bh_share']:.0%}), med {r['med_len']} letters, "
            f"phrases {r['phrases']}  \n  «{r['snippet'][:120]}»")
    lines += ["", f"## Unknown BH witness candidates: {len(cand)} MSS "
              "(members of text-anchored BH motifs, not in the index)"]
    for s, (root, p, ls, le, tags) in sorted(
            cand.items(), key=lambda kv: -(kv[1][3] - kv[1][2]))[:30]:
        sm, lb, ti = meta.get(s, (s, '?', ''))
        lines.append(f"- {lb} **{sm}** ({le - ls} letters, motif {root}, "
                     f"{tags}) — {ti[:60] or 'no title'}")
    open(MD_OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:16]))

    # ---- new-witness review HTML ----
    cards = []
    for s, (root, p, ls, le, tags) in sorted(
            cand.items(), key=lambda kv: -(kv[1][3] - kv[1][2]))[:60]:
        sm, lb, ti = meta.get(s, (s, '?', ''))
        text, stream, offs = page_text(p)
        snippet = ''
        e2 = min(le, len(offs))
        if len(offs) and e2 > ls:
            a = offs[max(0, min(ls, len(offs) - 1))]
            z = offs[e2 - 1] + 1
            snippet = (f"<span class='ctx'>"
                       f"{html.escape(text[max(0, a - 80):a])}</span>"
                       f"<mark>{html.escape(text[a:z][:800])}</mark>"
                       f"<span class='ctx'>{html.escape(text[z:z + 80])}"
                       f"</span>")
        url = (f"https://genizahsearch.com/browse?sys_id={s}"
               f"&page={pnum(p)}")
        cards.append(
            f"<div class='card'><div class='meta'>"
            f"<a href='{url}' target='_blank'><b>{html.escape(sm)}</b></a>"
            f" · {lb} · עמ' {pnum(p)} · {le - ls} letters · motif {root} "
            f"· {'/'.join(tags)}<br>"
            f"<span style='color:#9aa4ac'>{html.escape(ti[:90]) or '—'}"
            f"</span></div><div class='txt'>{snippet}</div></div>")
    doc = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>BH — unknown witness candidates</title><style>
 body{{font-family:Segoe UI,Arial;max-width:980px;margin:20px auto;
 padding:0 12px;background:#17181c;color:#d6d6d6}}
 .card{{background:#23252c;border:1px solid #3a3d46;border-radius:8px;
 margin:10px 0;padding:10px 14px}}
 .meta{{font-size:14px;margin-bottom:6px}}
 .txt{{direction:rtl;text-align:right;font-size:16px;line-height:1.7;
 white-space:pre-wrap;color:#e8e6df}}
 .txt mark{{background:#6b5407;color:#ffe082}}
 .txt .ctx{{color:#6d6d6d}}
 a{{color:#6fb3e8}}
</style></head><body>
<h1>ברכת המזון — עדים חדשים מועמדים</h1>
<p>Members of text-anchored BH motifs whose sys_id is NOT in the
{len(bh_sys)}-witness index — {len(cand)} candidate MSS, top 60 by
evidence length. Highlighted = the motif segment on the page.</p>
{''.join(cards)}</body></html>"""
    open(HTML_OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {MD_OUT}\n      {HTML_OUT} ({time.time() - t0:.0f}s)")
    con.close()


if __name__ == '__main__':
    main()
