# -*- coding: utf-8 -*-
"""A3 spike: interleaved Bible+Targum/Tafsir class — evidence dump.

Reuses growth_unidentified.py's exact unidentified-gainer enumeration
(members/pages_of/t1_pages/unident) and copies (does not import, to
avoid re-running that script's top-level side effects) the small
page/lib-meta helpers from build_growth_review.py.

For each sampled (motif, page) pair: pulls the ORIGINAL page text from
`pages`, ALL track1_matches rows for that page (NO filter on tier or
shadowed_by -- every row, every span), and the motif's own
motif_query_hits span; builds a single sorted timeline of spans on the
page (stream coordinates -> projected back onto the ORIGINAL text via
norm_stream's offset map) and prints the quoted text for every SPAN
and every GAP between spans, plus a lightweight (hint-only, NOT the
final call) cue count for JA/Aramaic/Hebrew markers on each GAP.

This script only READS fullcorpus.db (no writes to track1_matches /
motif tables / pages). The actual HIGH/MED/LOW language classification
and interleaved/medley/other verdict is made by a human(-equivalent)
reader of this dump when writing the a3 report -- the cue counts here
are surfaced only to speed that reading, per the brief's explicit rule
that particle hits alone are not sufficient for a HIGH call.

Out:
  ../results/a3_interleaved_probe_dump.json  (full evidence, all cards)
  stdout                                     (compact console view)
"""
import csv
import json
import re
import sqlite3
from collections import defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\a3_interleaved_probe_dump.json"

TOP_MOTIFS = 10          # "top ~10 unidentified gainer motifs" per the brief
SAMPLES_PER_MOTIF = 2    # 2-3 member pages per motif (~20-25 pages total)
EXTRA_SAMPLES = 1        # for motifs added only to cover the named +40 case

P_RE = re.compile(r'_P(\d+)_')

# hint-only cue lists (NOT the classification -- see module docstring)
JA_MARKERS = ['אלדי', "אלד'י", "ד'לך", 'דלך', 'כאן ', 'ליס ', 'יעני',
              'אעני', 'קאל ', 'פאן', 'לאכן', 'ענד', 'חתי', "ג'מיע",
              'אד\'א', 'תע\'', 'אלנא', 'ואחד', 'אלא ', 'מן ', 'פי ',
              'עלי ', 'ולד']
ARAM_MARKERS = ['ית ', 'הוו', 'ארי ', 'בגין', 'למא ', 'הדין', 'הדא',
                'מטול', 'קדם ', 'בכן', 'דא ', 'ותב']


def cue_hits(text):
    ja = [w for w in JA_MARKERS if w in text]
    ar = [w for w in ARAM_MARKERS if w in text]
    return ja, ar


def load_lib_meta():
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


def page_no(page_id):
    m = P_RE.search(page_id)
    return int(m.group(1)) if m else 1


con = sqlite3.connect(DB)
meta = load_lib_meta()

# ---- reproduce growth_unidentified.py's enumeration exactly ----
members = defaultdict(set)
for m, sid in con.execute("SELECT motif, sys_id FROM motif_members_pilot"):
    members[m].add(sid)

pages_of = defaultdict(list)
for m, pid, s, e in con.execute(
        "SELECT motif, page_id, start, end FROM motif_members_pilot"):
    pages_of[m].append((pid, s, e))

hits = defaultdict(list)
for m, pid, sid, letters, d, sj in con.execute(
        "SELECT motif, page_id, sys_id, matched_letters, best_density, "
        "spans_json FROM motif_query_hits"):
    hits[m].append((pid, sid, letters, d, sj))

new_ms = {m: {h[1] for h in v} - members[m] for m, v in hits.items()}
new_ms = {m: v for m, v in new_ms.items() if v}

t1_pages = {r[0] for r in con.execute(
    "SELECT DISTINCT page_id FROM track1_matches "
    "WHERE shadowed_by IS NULL AND matched_letters >= 150")}

unident = []
for m, new in new_ms.items():
    if not any(pid in t1_pages for pid, _, _ in pages_of[m]):
        unident.append((len(new), m))
unident.sort(reverse=True)

print(f"unidentified grown motifs: {len(unident):,} "
      f"(+{sum(n for n, _ in unident):,} memberships)")

# ---- bucket totals (exact, over the FULL unident list -- for the
#      class-level harvest estimate in the report, not just the sample) ----
bucket_edges = [(50, 10**9), (10, 49), (3, 9), (1, 2)]
bucket_sum = {}
bucket_cnt = {}
for lo, hi in bucket_edges:
    members_in = [(n, m) for n, m in unident if lo <= n <= hi]
    bucket_sum[(lo, hi)] = sum(n for n, _ in members_in)
    bucket_cnt[(lo, hi)] = len(members_in)
print("bucket totals (motifs, new-membership sum):")
for lo, hi in bucket_edges:
    tag = f"{lo}+" if hi > 1000 else f"{lo}-{hi}"
    print(f"  +{tag}: {bucket_cnt[(lo, hi)]} motifs, "
          f"{bucket_sum[(lo, hi)]:,} new memberships")
print()

target = [m for _, m in unident[:TOP_MOTIFS]]
named_40 = [m for n, m in unident if n == 40 and m not in target]
target_extra = [m for m in named_40 if m not in target]
all_target = target + target_extra

n_of = {m: n for n, m in unident}
print(f"target motifs ({len(all_target)}): "
      f"{[(m, n_of[m]) for m in all_target]}")
print()

# ---- per-page evidence dump ----
_page_cache = {}


def page_view(pid):
    if pid not in _page_cache:
        row = con.execute("SELECT text FROM pages WHERE page_id=?",
                          (pid,)).fetchone()
        tx = row[0] if row else ''
        stream, offs = norm_stream(tx)
        _page_cache[pid] = (tx, stream, offs)
    return _page_cache[pid]


def orig_range(offs, s, e):
    """stream [s,e) -> original char [a,b) bounds, clipped. None if OOB."""
    if not len(offs) or s >= len(offs):
        return None
    e = min(e, len(offs))
    a = offs[s]
    b = offs[e - 1] + 1
    return a, b


def quote(tx, a, b, cap=500):
    frag = tx[a:b]
    if len(frag) > cap:
        frag = frag[:cap] + '…'
    return frag


cards = []
for m in all_target:
    old_n = len(members[m])
    new_n = old_n + len(new_ms[m])
    rows = sorted((h for h in hits[m] if h[1] in new_ms[m]),
                  key=lambda h: -h[2])  # desc by matched_letters
    n_samp = EXTRA_SAMPLES if m in target_extra else SAMPLES_PER_MOTIF
    idxs = [0]
    if n_samp > 1 and len(rows) > 1:
        idxs.append(len(rows) // 2)
    idxs = sorted(set(i for i in idxs if i < len(rows)))[:n_samp]
    sampled_rows = [rows[i] for i in idxs]

    for pid, sid, letters, dens, sj in sampled_rows:
        tx, stream, offs = page_view(pid)
        motif_spans = json.loads(sj)  # [[s,e,d], ...] stream coords

        # ALL track1_matches rows for this page -- NO filtering
        t1rows = con.execute(
            "SELECT work_id, cat, genre, author, title, mesirah, "
            "matched_letters, best_density, n_spans, spans_json, "
            "shadowed_by FROM track1_matches WHERE page_id=?",
            (pid,)).fetchall()

        timeline = []  # (start, end, label, extra)
        for (wid, cat, genre, author, title, mesirah, mletters, mdens,
             nspans, t1sj, shadow) in t1rows:
            for s, e, d in json.loads(t1sj):
                lab = f"[T1:{cat}] {(author or '').strip()} {title}".strip()
                if shadow:
                    lab += " (SHADOWED)"
                timeline.append((int(s), int(e), lab, {
                    'work_id': wid, 'cat': cat, 'genre': genre,
                    'span_density': d, 'row_letters': mletters,
                    'row_density': mdens, 'shadowed_by': shadow,
                }))
        for s, e, d in motif_spans:
            timeline.append((int(s), int(e), f"[MOTIF-QUERY {m}]",
                             {'density': d}))
        timeline.sort(key=lambda x: x[0])

        pieces = []
        cursor = 0
        for s, e, lab, extra in timeline:
            if s > cursor:
                rng = orig_range(offs, cursor, s)
                if rng:
                    gtx = quote(tx, *rng)
                    ja, ar = cue_hits(gtx)
                    pieces.append({'kind': 'GAP', 'label': None,
                                   'text': gtx, 'cue_ja': ja, 'cue_aram': ar})
            rng = orig_range(offs, s, e)
            if rng:
                pieces.append({'kind': 'SPAN', 'label': lab,
                               'extra': extra, 'text': quote(tx, *rng)})
            cursor = max(cursor, e)
        if cursor < len(offs):
            rng = orig_range(offs, cursor, len(offs))
            if rng:
                gtx = quote(tx, *rng)
                ja, ar = cue_hits(gtx)
                pieces.append({'kind': 'GAP', 'label': None,
                               'text': gtx, 'cue_ja': ja, 'cue_aram': ar})

        smk, lib, cat_title = meta.get(sid, (sid, '?', ''))
        card = {
            'motif': m, 'old_n': old_n, 'new_n': new_n,
            'page_id': pid, 'sys_id': sid, 'shelfmark': smk, 'library': lib,
            'catalog_title': cat_title,
            'url': f"https://genizahsearch.com/browse?sys_id={sid}"
                   f"&page={page_no(pid)}",
            'hit_letters': letters, 'hit_density': dens,
            'n_track1_rows': len(t1rows),
            'pieces': pieces,
        }
        cards.append(card)

        print(f"=== motif {m} ({old_n}->{new_n}, +{new_n - old_n}) "
              f"page={pid} sid={sid}")
        print(f"    [{smk} | {lib} | {cat_title[:50]}]  hit_letters="
              f"{letters} dens={dens:.3f} t1_rows={len(t1rows)}")
        print(f"    {card['url']}")
        for p in pieces:
            if p['kind'] == 'SPAN':
                print(f"  SPAN {p['label'][:70]}")
                print(f"       {p['text'][:200]}")
            else:
                cue = ''
                if p['cue_ja']:
                    cue += f" JA-cues={p['cue_ja']}"
                if p['cue_aram']:
                    cue += f" ARAM-cues={p['cue_aram']}"
                print(f"  GAP{cue}")
                print(f"       {p['text'][:300]}")
        print()

json.dump({
    'bucket_totals': {f"{lo}-{hi}": {'motifs': bucket_cnt[(lo, hi)],
                                      'new_memberships': bucket_sum[(lo, hi)]}
                       for lo, hi in bucket_edges},
    'target_motifs': [(m, n_of[m]) for m in all_target],
    'cards': cards,
}, open(OUT_JSON, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print(f"wrote {OUT_JSON} ({len(cards)} cards)")
