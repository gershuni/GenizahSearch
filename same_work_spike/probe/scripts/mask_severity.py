# -*- coding: utf-8 -*-
"""How hard does canonical masking hit each page? (Hillel 2026-07-08:
'the bible-mask may be too strict — and it's a tell for other works').

For every page with >=1 canonical (Bible/Mishnah/Tosefta/Bavli/Yerushalmi)
Track-1 span, compute masked-coverage = canonical matched letters / stream
letters. Segment by page bucket (bh tracer vs rest) and by the page's
Track-1 testimony class. The question: how many NON-testimony pages
(liturgy quoting verses, halakha quoting Bavli) lose most of their text —
i.e. get disconnected from the works census although the page itself is
NOT a canonical copy.

Usage: python mask_severity.py [db] [tag]
Out: results/mask_severity_<tag>.md
"""
import json
import sqlite3
import sys
from collections import Counter, defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "full"
OUT = ROOT + rf"\same_work_spike\probe\results\mask_severity_{TAG}.md"
CANON_CATS = ('Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi')


def merge_len(intervals):
    intervals.sort()
    total = 0
    cur0, cur1 = None, None
    for a, b in intervals:
        if cur1 is None or a > cur1:
            if cur1 is not None:
                total += cur1 - cur0
            cur0, cur1 = a, b
        else:
            cur1 = max(cur1, b)
    if cur1 is not None:
        total += cur1 - cur0
    return total


def main():
    con = sqlite3.connect(DB)
    # canonical spans per page
    spans = defaultdict(list)
    for pid, spans_json in con.execute(
            "SELECT page_id, spans_json FROM track1_matches WHERE cat IN "
            + "(" + ",".join(f"'{c}'" for c in CANON_CATS) + ")"):
        spans[pid].extend((int(s[0]), int(s[1]))
                          for s in json.loads(spans_json))
    print(f"pages with canonical spans: {len(spans):,}")

    # stream lengths + buckets for those pages
    rows = []
    ids = list(spans)
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        ph = ','.join('?' * len(batch))
        for pid, bk, tx in con.execute(
                f"SELECT page_id, buckets, text FROM pages "
                f"WHERE page_id IN ({ph})", batch):
            slen = len(norm_stream(tx)[0])
            masked = merge_len(spans[pid])
            rows.append((pid, bk, slen, masked))
    con.close()

    def hist(items):
        h = Counter()
        for _, _, slen, masked in items:
            f = masked / max(1, slen)
            h[min(int(f * 10), 10)] += 1
        return h

    def fmt_hist(h):
        n = sum(h.values())
        return " ".join(f"{b / 10:.1f}:{h[b]}({100 * h[b] / n:.0f}%)"
                        for b in sorted(h))

    bh = [r for r in rows if 'bh' in r[1]]
    rest = [r for r in rows if 'bh' not in r[1]]
    heavy = [r for r in rows if r[3] / max(1, r[2]) >= 0.5]
    dead = [r for r in rows if r[2] - r[3] < 80]

    lines = [
        f"# Mask severity — '{TAG}' (canonical spans only)", "",
        f"- pages with >=1 canonical span: **{len(rows):,}** "
        f"(of 667,411 in corpus)",
        f"- pages losing >=50% of their text to the mask: "
        f"**{len(heavy):,}**",
        f"- pages left with <80 unmasked letters (effectively removed): "
        f"**{len(dead):,}**", "",
        "## Masked-fraction histogram (fraction of page stream masked)",
        f"- ALL pages w/ canonical spans: {fmt_hist(hist(rows))}",
        f"- BH-witness pages ({len(bh):,}): {fmt_hist(hist(bh))}",
        f"- non-BH pages: {fmt_hist(hist(rest))}", "",
    ]
    # BH detail: how many BH pages effectively removed?
    bh_dead = [r for r in bh if r[2] - r[3] < 80]
    bh_heavy = [r for r in bh if r[3] / max(1, r[2]) >= 0.5]
    lines += [
        "## BH tracer impact",
        f"- BH pages with canonical spans: {len(bh):,}",
        f"- BH pages >=50% masked: {len(bh_heavy):,}",
        f"- BH pages effectively removed (<80 letters left): "
        f"{len(bh_dead):,}",
    ]
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
