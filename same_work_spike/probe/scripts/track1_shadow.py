# -*- coding: utf-8 -*-
"""Competitive span assignment — shadow worse overlapping identifications.

The Temple-Scroll class (paraphrase-of-canon reference works) matches
real Bible pages at 0.20-0.30 while the SAME pages match מקרא itself at
0.10-0.15. Reference-side masking cannot fully separate them (Qumran
orthography keeps 11QT's Deuteronomy at 0.32-0.45 clean-vs-clean, and
loosening masks legitimate halakhic paraphrase). The principled fix is
competition: when two works' spans overlap substantially ON THE PAGE,
the better-density identification wins; the worse is SHADOWED. This
also collapses multi-edition double counting (several Maagarim editions
of the same prayer competing for one page).

Adds column track1_matches.shadowed_by (NULL = live). The census/webs
consume only live rows.

Usage: python track1_shadow.py [db]
Out: results/track1_shadow_full.md
"""
import json
import sqlite3
import sys
from collections import Counter, defaultdict

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT = ROOT + r"\same_work_spike\probe\results\track1_shadow_full.md"

OVERLAP_FRAC = 0.6    # of the WORSE row's span
MIN_DENS_GAP = 0.03   # better must be meaningfully better


def main():
    con = sqlite3.connect(DB)
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    if 'shadowed_by' not in cols:
        con.execute("ALTER TABLE track1_matches ADD COLUMN shadowed_by TEXT")

    rows = con.execute("""
        SELECT rowid, page_id, work_id, cat, author, title,
               best_density, spans_json FROM track1_matches""").fetchall()
    by_page = defaultdict(list)
    for r in rows:
        spans = [(int(s[0]), int(s[1]), float(s[2]))
                 for s in json.loads(r[7])]
        by_page[r[1]].append((r[0], r[2], r[3], r[5], r[6], spans))

    def best_span(spans):
        return max(spans, key=lambda s: s[1] - s[0])

    shadows = []          # (shadowed_rowid, winner_work)
    shadow_works = Counter()
    winner_works = Counter()
    for pid, items in by_page.items():
        if len(items) < 2:
            continue
        # sort by density of the best span (ascending = better first)
        items = sorted(items, key=lambda it: best_span(it[5])[2])
        live = []
        for it in items:
            rowid, wid, cat, title, dens, spans = it
            b0, b1, bd = best_span(spans)
            winner = None
            for (lw, l0, l1, ld) in live:
                ov = min(b1, l1) - max(b0, l0)
                if ov >= OVERLAP_FRAC * (b1 - b0) and \
                        bd - ld >= MIN_DENS_GAP:
                    winner = lw
                    break
            if winner:
                shadows.append((rowid, winner))
                shadow_works[wid] += 1
                winner_works[winner] += 1
            else:
                live.append((wid, b0, b1, bd))
    con.executemany(
        "UPDATE track1_matches SET shadowed_by=? WHERE rowid=?",
        [(w, rid) for rid, w in shadows])
    con.commit()

    # per-work live witness counts for the previously-suspect works
    name = {}
    live_ms = defaultdict(set)
    all_ms = defaultdict(set)
    for wid, sid, author, title, letters, sh in con.execute(
            "SELECT work_id, sys_id, author, title, matched_letters, "
            "shadowed_by FROM track1_matches WHERE matched_letters >= 200"):
        name[wid] = f"{author} — {title}" if author else title
        all_ms[wid].add(sid)
        if not sh:
            live_ms[wid].add(sid)
    lines = [
        "# Competitive span assignment (shadowing)", "",
        f"- rows shadowed: {len(shadows):,} of {len(rows):,}",
        "", "## Biggest witness-count collapses (>= 200-letter rows)",
    ]
    drops = sorted(all_ms, key=lambda w: -(len(all_ms[w])
                                           - len(live_ms[w])))
    for w in drops[:25]:
        lines.append(f"- {name[w][:70]}: {len(all_ms[w])} -> "
                     f"**{len(live_ms[w])}** MSS")
    lines += ["", "## Biggest shadow winners (rows absorbed)"]
    for w, n in winner_works.most_common(10):
        lines.append(f"- {name.get(w, w)[:70]}: absorbed {n:,} rows")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines))
    con.close()


if __name__ == '__main__':
    main()
