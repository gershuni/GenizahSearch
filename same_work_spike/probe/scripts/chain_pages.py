# -*- coding: utf-8 -*-
"""Page-chain extension (METHOD.md §8 'edge' class follow-up).

An accepted span that runs to the page boundary on both manuscripts is
usually a same-work overlap larger than one page — the REHEARSAL-RESULTS
'edge' class (97,834 of 337K page pairs at 100K). This post-processor
joins such page pairs into MULTI-PAGE CHAINS: for a manuscript pair
(A, B), links (A_p, B_q) -> (A_{p+1}, B_{q+1}) chain when consecutive
P-numbers on BOTH sides carry accepted spans and the adjoining spans
reach the shared page boundaries (end-of-page / start-of-page within
BOUNDARY_SLOP stream letters).

A chain of N>=2 continuously-parallel pages is the strongest same-work
evidence the pipeline produces — stronger than any single-page span.

Usage: python chain_pages.py [db_path] [tag] [pairs_table]
Writes: <db>::page_chains_<pairs_table>, results/chains_<tag>.md
"""
import re
import sqlite3
import sys
from collections import defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
TABLE = sys.argv[3] if len(sys.argv) > 3 else "accepted_pairs"
OUT = ROOT + rf"\same_work_spike\probe\results\chains_{TAG}.md"

BOUNDARY_SLOP = 100    # span end/start within this of the stream edge
MIN_LINK_LEN = 200     # a chain link must be a substantive span

P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else -1


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
    con = sqlite3.connect(DB)
    rows = con.execute(f"""
        SELECT page_a, page_b, sys_a, sys_b, a0, a1, b0, b1,
               aligned_len, density
        FROM {TABLE}
        WHERE dup_shelf = 0 AND dup_lines < 0.6
          AND aligned_len >= {MIN_LINK_LEN}""").fetchall()
    print(f"clean substantive pairs: {len(rows):,}")

    # best row per (page_a, page_b)
    best = {}
    for r in rows:
        k = (r[0], r[1])
        if k not in best or r[8] > best[k][8]:
            best[k] = r

    # group by MS pair; index by (pnum_a, pnum_b)
    by_ms = defaultdict(dict)
    for (pa, pb), r in best.items():
        key = (r[2], r[3]) if r[2] < r[3] else (r[3], r[2])
        flip = r[2] > r[3]
        na, nb = pnum(pa), pnum(pb)
        if na < 0 or nb < 0:
            continue
        if flip:
            # store as (sysA, sysB) canonical: swap sides
            by_ms[key][(nb, na)] = (r[1], r[0], r[6], r[7], r[4], r[5],
                                    r[8], r[9])
        else:
            by_ms[key][(na, nb)] = (r[0], r[1], r[4], r[5], r[6], r[7],
                                    r[8], r[9])

    # stream lengths only for pages participating in potential chains
    need_pages = set()
    for key, d in by_ms.items():
        for (na, nb) in d:
            if (na + 1, nb + 1) in d or (na - 1, nb - 1) in d:
                r = d[(na, nb)]
                need_pages.update((r[0], r[1]))
    slen = {}
    if need_pages:
        ph_pages = list(need_pages)
        for i in range(0, len(ph_pages), 500):
            batch = ph_pages[i:i + 500]
            ph = ','.join('?' * len(batch))
            for pid, tx in con.execute(
                    f"SELECT page_id, text FROM pages "
                    f"WHERE page_id IN ({ph})", batch):
                slen[pid] = len(norm_stream(tx)[0])
    print(f"chain-candidate pages: {len(need_pages):,}")

    def link_ok(r_prev, r_next):
        """prev span reaches both page ends; next span starts at both."""
        pa, pb, a0, a1, b0, b1, alen, dens = r_prev
        la, lb = slen.get(pa, 0), slen.get(pb, 0)
        if la - a1 > BOUNDARY_SLOP or lb - b1 > BOUNDARY_SLOP:
            return False
        na_, nb_, c0, c1, d0, d1, alen2, dens2 = r_next
        return c0 <= BOUNDARY_SLOP and d0 <= BOUNDARY_SLOP

    chains = []
    for key, d in by_ms.items():
        used = set()
        for (na, nb) in sorted(d):
            if (na, nb) in used:
                continue
            if (na - 1, nb - 1) in d:      # not a chain head
                continue
            run = [(na, nb)]
            while (run[-1][0] + 1, run[-1][1] + 1) in d and \
                    link_ok(d[run[-1]], d[(run[-1][0] + 1, run[-1][1] + 1)]):
                run.append((run[-1][0] + 1, run[-1][1] + 1))
            used.update(run)
            if len(run) >= 2:
                recs = [d[x] for x in run]
                chains.append({
                    'sys_a': key[0], 'sys_b': key[1],
                    'n_pages': len(run),
                    'pages_a': [r[0] for r in recs],
                    'pages_b': [r[1] for r in recs],
                    'total_aligned': sum(r[6] for r in recs),
                    'mean_density': round(
                        sum(r[7] for r in recs) / len(recs), 4),
                })
    chains.sort(key=lambda c: (-c['n_pages'], -c['total_aligned']))
    print(f"chains (>=2 consecutive parallel pages): {len(chains):,}")

    tname = f"page_chains_{TABLE}"
    con.execute(f"DROP TABLE IF EXISTS {tname}")
    con.execute(f"""
        CREATE TABLE {tname} (
            sys_a TEXT, sys_b TEXT, n_pages INT,
            pages_a TEXT, pages_b TEXT,
            total_aligned INT, mean_density REAL)""")
    con.executemany(
        f"INSERT INTO {tname} VALUES (?,?,?,?,?,?,?)",
        [(c['sys_a'], c['sys_b'], c['n_pages'],
          ','.join(c['pages_a']), ','.join(c['pages_b']),
          c['total_aligned'], c['mean_density']) for c in chains])
    con.commit()

    meta = load_lib_meta()
    lines = [f"# Multi-page continuous parallels — '{TAG}' ({TABLE})", "",
             f"- chain links require: clean pair, aligned span >= "
             f"{MIN_LINK_LEN}, span reaching the shared page boundary "
             f"(slop {BOUNDARY_SLOP}), consecutive P-numbers BOTH sides",
             f"- chains found: **{len(chains):,}** "
             f"({sum(1 for c in chains if c['n_pages'] >= 3):,} with >=3 "
             f"pages, {sum(1 for c in chains if c['n_pages'] >= 5):,} "
             f"with >=5)", "",
             "## Longest chains (top 40)"]
    for c in chains[:40]:
        sa, la, ta = meta.get(c['sys_a'], (c['sys_a'], '?', ''))
        sb, lb, tb = meta.get(c['sys_b'], (c['sys_b'], '?', ''))
        lines.append(
            f"- **{c['n_pages']} pages** · {la} {sa} ↔ {lb} {sb} · "
            f"{c['total_aligned']:,} aligned letters · "
            f"d={c['mean_density']:.2f}  \n"
            f"  {ta[:70] or '—'} ↔ {tb[:70] or '—'}")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print(f"wrote {OUT}")
    con.close()


if __name__ == '__main__':
    main()
