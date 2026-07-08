# -*- coding: utf-8 -*-
"""Shared-passage UNITS — the many-to-many text-reuse view (Hillel
2026-07-08: 'we may want potential text reuse for more than 1<->1 — some
will be possible testimony, others possible citations').

A unit = one passage attested on N pages. Built by union-find over span
occurrences: each accepted pair contributes two occurrences (its span on
each page); occurrences overlapping substantially ON THE SAME PAGE
(>= 50% of the shorter) are the same occurrence; pair links union across
pages. Per member, role by occurrence coverage of its page:
  witness  (occ >= 0.45 of the page — the page essentially IS the passage)
  partial  (0.15–0.45)
  embed    (< 0.15 — quotation/citation inside other text)

Track-1 label propagation ('the tell'): if members' occurrences overlap a
Track-1 identified span (>= 50% of the occurrence), the unit inherits the
work label — labeling ALL members, including pages Track-1 never matched
directly. Unlabeled high-witness units = the ranked 'frequent but
unedited' discovery list (the continuation giant decomposed).

Usage: python passage_units.py [db] [tag] [pairs_table]
Out: <db>::passage_units/passage_unit_members,
     results/units_<tag>.md, review/units_<tag>.html
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
TAG = sys.argv[2] if len(sys.argv) > 2 else "full"
TABLE = sys.argv[3] if len(sys.argv) > 3 else "accepted_pairs_canonmask"
MD_OUT = ROOT + rf"\same_work_spike\probe\results\units_{TAG}.md"
HTML_OUT = ROOT + rf"\same_work_spike\probe\review\units_{TAG}.html"

MIN_OCC_LEN = 60          # ignore tiny spans
SAME_OCC_OVERLAP = 0.5    # same-page occurrences merge at >= 50% of shorter
T1_LABEL_OVERLAP = 0.5    # occurrence inherits a Track-1 span label
W_TESTIMONY, W_PARTIAL = 0.45, 0.15
CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
HTML_TOP_UNITS = 400
HTML_MEMBER_CAP = 80


class DSU:
    __slots__ = ('p', 'r')

    def __init__(self, n):
        self.p = list(range(n))
        self.r = [0] * n

    def find(self, x):
        p = self.p
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            ra, rb = rb, ra
        self.p[rb] = ra
        if self.r[ra] == self.r[rb]:
            self.r[ra] += 1


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


P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)
    pairs = con.execute(f"""
        SELECT page_a, page_b, sys_a, sys_b, a0, a1, b0, b1
        FROM {TABLE} WHERE dup_shelf = 0 AND dup_lines < 0.6""").fetchall()
    print(f"clean pairs: {len(pairs):,}", flush=True)

    # ---- occurrences + pair links ----
    occs = []                    # (page_id, start, end)
    by_page = defaultdict(list)  # page_id -> [occ_idx]
    links = []
    page_sys = {}
    for pa, pb, sa, sb, a0, a1, b0, b1 in pairs:
        if min(a1 - a0, b1 - b0) < MIN_OCC_LEN:
            continue
        ia, ib = len(occs), len(occs) + 1
        occs.append((pa, a0, a1))
        occs.append((pb, b0, b1))
        by_page[pa].append(ia)
        by_page[pb].append(ib)
        links.append((ia, ib))
        page_sys[pa] = sa
        page_sys[pb] = sb
    print(f"occurrences: {len(occs):,} on {len(by_page):,} pages "
          f"({time.time() - t0:.0f}s)", flush=True)

    dsu = DSU(len(occs))
    # same-page merge: sweep sorted by start
    for pid, idxs in by_page.items():
        idxs.sort(key=lambda i: occs[i][1])
        active = []              # (end, idx) not yet closed
        for i in idxs:
            s, e = occs[i][1], occs[i][2]
            keep = []
            for (ae, ai) in active:
                if ae <= s:
                    continue
                keep.append((ae, ai))
                ov = min(ae, e) - s
                if ov >= SAME_OCC_OVERLAP * min(e - s, occs[ai][2]
                                                - occs[ai][1]):
                    dsu.union(i, ai)
            keep.append((e, i))
            active = keep
    for ia, ib in links:
        dsu.union(ia, ib)
    print(f"union-find done ({time.time() - t0:.0f}s)", flush=True)

    # ---- collect units ----
    unit_occs = defaultdict(list)
    for i in range(len(occs)):
        unit_occs[dsu.find(i)].append(i)
    # merge per (unit, page): keep hull of occurrence intervals
    units = {}                   # root -> {page_id: [min_s, max_e]}
    for root, idxs in unit_occs.items():
        pagemap = {}
        for i in idxs:
            pid, s, e = occs[i]
            cur = pagemap.get(pid)
            if cur is None:
                pagemap[pid] = [s, e]
            else:
                cur[0] = min(cur[0], s)
                cur[1] = max(cur[1], e)
        if len({page_sys[p] for p in pagemap}) >= 2:
            units[root] = pagemap
    print(f"units (>=2 MSS): {len(units):,} ({time.time() - t0:.0f}s)",
          flush=True)

    # ---- page stream lengths (members only) + library-stamp junk pages ----
    from stage0 import STAMP_RE
    need = {p for pm in units.values() for p in pm}
    plen = {}
    stamp_pages = set()
    ids = list(need)
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        ph = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                batch):
            slen = len(norm_stream(tx)[0])
            plen[pid] = slen
            if slen < 400 and STAMP_RE.search(tx):
                stamp_pages.add(pid)
    # drop NLI-ownership-stamp pages (junk class found BY this view,
    # 2026-07-08: 2,618 RNL 'MSS' sharing the stamp text)
    if stamp_pages:
        for root in list(units):
            pm = units[root]
            for pid in list(pm):
                if pid in stamp_pages:
                    del pm[pid]
            if len({page_sys[p] for p in pm}) < 2:
                del units[root]
    print(f"page lengths: {len(plen):,}; stamp-junk pages dropped: "
          f"{len(stamp_pages):,}; units after: {len(units):,} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- Track-1 spans per member page (label propagation) ----
    t1 = defaultdict(list)       # page -> [(p0, p1, label, cat)]
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        ph = ','.join('?' * len(batch))
        for pid, cat, author, title, spans_json in con.execute(
                f"SELECT page_id, cat, author, title, spans_json "
                f"FROM track1_matches WHERE page_id IN ({ph})", batch):
            label = (f"{author} — {title}" if author else title)[:80]
            for s in json.loads(spans_json):
                t1[pid].append((int(s[0]), int(s[1]), label, cat))
    print(f"track1 label pages: {len(t1):,} ({time.time() - t0:.0f}s)",
          flush=True)

    # ---- aggregate per unit ----
    meta = load_lib_meta()
    unit_rows = []
    member_rows = []
    for root, pagemap in units.items():
        mss = set()
        libs = Counter()
        roles = Counter()
        labels = Counter()
        canon_labels = Counter()
        lens = []
        members = []
        for pid, (s, e) in pagemap.items():
            sysid = page_sys[pid]
            mss.add(sysid)
            libs[meta.get(sysid, ('?', '?', ''))[1]] += 1
            olen = e - s
            lens.append(olen)
            cov = olen / max(1, plen.get(pid, 1))
            role = ('witness' if cov >= W_TESTIMONY else
                    'partial' if cov >= W_PARTIAL else 'embed')
            roles[role] += 1
            lab = ''
            for (q0, q1, label, cat) in t1.get(pid, ()):
                ov = min(e, q1) - max(s, q0)
                if ov >= T1_LABEL_OVERLAP * olen:
                    if cat in CANON_CATS:
                        canon_labels[label] += 1
                    else:
                        labels[label] += 1
                    lab = label
                    break
            members.append((pid, sysid, s, e, round(cov, 3), role, lab))
        lens.sort()
        t1_label, t1_n = ('', 0)
        if labels:
            t1_label, t1_n = labels.most_common(1)[0]
        elif canon_labels:
            t1_label, t1_n = canon_labels.most_common(1)[0]
        # label CONFIDENCE gate: continuum-scale units chain MANY texts,
        # so one work's label cannot cover them (the 18,676-MS liturgy
        # continuum got 'Sefer Ahavah' from 1.2% direct evidence). For
        # single-passage units >=2 agreeing direct labels is strong
        # (Track-1 graded precision ~100%).
        n_pg = len(pagemap)
        conf = ''
        if t1_label:
            conf = 'high' if len(mss) <= 2000 and t1_n >= 2 else 'low'
        unit_rows.append({
            'unit': root, 'n_pages': n_pg, 'n_ms': len(mss),
            'med_len': lens[len(lens) // 2],
            'roles': dict(roles), 'libs': dict(libs.most_common(4)),
            't1_label': t1_label, 't1_n': t1_n, 'conf': conf,
            'labeled': conf == 'high',
        })
        member_rows.extend((root,) + m for m in members)
    unit_rows.sort(key=lambda u: -u['n_ms'])

    # ---- persist ----
    con.execute(f"DROP TABLE IF EXISTS passage_units_{TABLE}")
    con.execute(f"""CREATE TABLE passage_units_{TABLE} (
        unit INT, n_pages INT, n_ms INT, med_len INT,
        roles TEXT, libs TEXT, t1_label TEXT, t1_n INT, conf TEXT,
        labeled INT)""")
    con.executemany(
        f"INSERT INTO passage_units_{TABLE} VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(u['unit'], u['n_pages'], u['n_ms'], u['med_len'],
          json.dumps(u['roles'], ensure_ascii=False),
          json.dumps(u['libs'], ensure_ascii=False),
          u['t1_label'], u['t1_n'], u['conf'], int(u['labeled']))
         for u in unit_rows])
    con.execute(f"DROP TABLE IF EXISTS passage_unit_members_{TABLE}")
    con.execute(f"""CREATE TABLE passage_unit_members_{TABLE} (
        unit INT, page_id TEXT, sys_id TEXT, start INT, end INT,
        cov REAL, role TEXT, t1_label TEXT)""")
    con.executemany(
        f"INSERT INTO passage_unit_members_{TABLE} VALUES "
        f"(?,?,?,?,?,?,?,?)", member_rows)
    con.commit()
    print(f"persisted ({time.time() - t0:.0f}s)", flush=True)

    # ---- report ----
    CONTINUUM_MS = 2000
    sizes = Counter(u['n_ms'] for u in unit_rows)
    contin = [u for u in unit_rows if u['n_ms'] > CONTINUUM_MS]
    lab = [u for u in unit_rows
           if u['labeled'] and u['n_ms'] <= CONTINUUM_MS]
    unlab = [u for u in unit_rows
             if not u['labeled'] and u['n_ms'] <= CONTINUUM_MS]
    lines = [
        f"# Shared-passage units — '{TAG}' ({TABLE})", "",
        f"- units (passage attested in >=2 MSS): **{len(unit_rows):,}**",
        f"- MS-size distribution: " + " ".join(
            f"{k}:{sizes[k]}" for k in sorted(sizes)[:12]) +
        f" … max {max(sizes) if sizes else 0}",
        f"- confidently labeled: {len(lab):,} · unlabeled/low-conf: "
        f"**{len(unlab):,}** (the discovery/residue census) · "
        f"continuum-scale (> {CONTINUUM_MS} MSS): {len(contin)}",
        f"- label propagation (high-conf units): " + str(sum(
            max(0, u['n_pages'] - u['t1_n']) for u in lab)) +
        " member pages inherit a label their page never matched directly",
        "",
        "## Continuum-scale units (chained sequences, NOT single passages)",
    ]
    for u in contin:
        lines.append(
            f"- unit {u['unit']}: **{u['n_ms']:,} MSS** "
            f"({u['n_pages']:,} pages) — top label candidate "
            f"'{u['t1_label']}' ({u['t1_n']} direct = "
            f"{100 * u['t1_n'] / max(1, u['n_pages']):.1f}% — "
            f"{u['conf']} confidence); needs sequence-aware decomposition")
    lines += ["", "## Top UNLABELED units by witness count "
              "(frequent but unedited)"]
    for u in unlab[:30]:
        lines.append(f"- unit {u['unit']}: **{u['n_ms']} MSS** "
                     f"({u['n_pages']} pages, med {u['med_len']} letters) "
                     f"roles {u['roles']} libs {u['libs']}"
                     + (f" · label? {u['t1_label']} ({u['conf']})"
                        if u['t1_label'] else ''))
    lines += ["", "## Top LABELED units (known works, member counts)"]
    for u in lab[:30]:
        lines.append(f"- {u['t1_label'] or '?'} — {u['n_ms']} MSS "
                     f"({u['n_pages']} pages, med {u['med_len']}); "
                     f"direct Track-1 {u['t1_n']}, inherited "
                     f"{u['n_pages'] - u['t1_n']}")
    open(MD_OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:24]))

    # ---- HTML browser (top units, evidence snippet per unit) ----
    cards = []
    for u in unit_rows[:HTML_TOP_UNITS]:
        pagemap = units[u['unit']]
        # snippet: longest member occurrence
        best_pid, (bs, be) = max(pagemap.items(),
                                 key=lambda kv: kv[1][1] - kv[1][0])
        row = con.execute("SELECT text FROM pages WHERE page_id=?",
                          (best_pid,)).fetchone()
        snippet = ''
        if row:
            text = row[0]
            stream, offs = norm_stream(text)
            e2 = min(be, len(offs))
            if len(offs) and e2 > 0:
                a = offs[max(0, min(bs, len(offs) - 1))]
                z = offs[e2 - 1] + 1
                snippet = html.escape(text[a:z][:600])
        mem_rows = []
        msorted = sorted(pagemap.items(),
                         key=lambda kv: -(kv[1][1] - kv[1][0]))
        for pid, (s, e) in msorted[:HTML_MEMBER_CAP]:
            sysid = page_sys[pid]
            sm, lb, ti = meta.get(sysid, (sysid, '?', ''))
            cov = (e - s) / max(1, plen.get(pid, 1))
            role = ('witness' if cov >= W_TESTIMONY else
                    'partial' if cov >= W_PARTIAL else 'embed')
            url = (f"https://genizahsearch.com/browse?sys_id={sysid}"
                   f"&page={pnum(pid)}")
            mem_rows.append(
                f"<tr class='{role}'><td><a href='{url}' target='_blank'>"
                f"{html.escape(sm)}</a></td><td>{lb}</td>"
                f"<td>{html.escape((ti or '')[:55]) or '—'}</td>"
                f"<td>{e - s}</td><td>{cov:.2f}</td><td>{role}</td></tr>")
        head = (f"unit {u['unit']} — {u['n_ms']} MSS · med "
                f"{u['med_len']} letters")
        if u['n_ms'] > 2000:
            label = (" · <b style='color:#ef6c00'>רצף־ענק (continuum) — "
                     f"מועמד: {html.escape(u['t1_label'])}?</b>")
        elif u['labeled']:
            label = (f" · <b style='color:#2c7d32'>"
                     f"{html.escape(u['t1_label'])}</b>")
        elif u['t1_label']:
            label = (f" · <b style='color:#ef6c00'>"
                     f"{html.escape(u['t1_label'])}?</b>")
        else:
            label = " · <b style='color:#c62828'>לא מזוהה</b>"
        cards.append(
            f"<details><summary>{head}{label} · roles {u['roles']}"
            f"</summary>"
            f"<div class='snip'>{snippet}</div>"
            f"<table><tr><th>shelfmark</th><th>lib</th><th>title</th>"
            f"<th>len</th><th>cov</th><th>role</th></tr>"
            f"{''.join(mem_rows)}</table>"
            f"{'<p>… ' + str(len(pagemap) - HTML_MEMBER_CAP) + ' more</p>' if len(pagemap) > HTML_MEMBER_CAP else ''}"
            f"</details>")
    doc = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>Shared-passage units — {TAG}</title><style>
 body{{font-family:Segoe UI,Arial;max-width:1150px;margin:20px auto;
 padding:0 12px;background:#17181c;color:#d6d6d6}}
 details{{background:#23252c;border:1px solid #3a3d46;border-radius:8px;
 margin:8px 0;padding:6px 12px}}
 summary{{cursor:pointer}}
 table{{border-collapse:collapse;font-size:13px;margin:6px 0}}
 td,th{{border:1px solid #3a3d46;padding:3px 8px}}
 tr.witness td{{background:#1e3320}} tr.partial td{{background:#332e1a}}
 tr.embed td{{background:#23252c}}
 .snip{{direction:rtl;text-align:right;font-size:15px;line-height:1.6;
 background:#1d1f25;border:1px solid #3a3d46;border-radius:6px;
 padding:8px 10px;margin:6px 0;color:#e8e6df}}
 a{{color:#6fb3e8}}
</style></head><body>
<h1>Shared-passage units — the many-to-many reuse view</h1>
<p>{len(unit_rows):,} units (passage attested in ≥2 MSS), showing top
{HTML_TOP_UNITS} by witness count. Per member: role by page coverage
(witness ≥ {W_TESTIMONY} / partial / embed &lt; {W_PARTIAL}). Green work
label = inherited from Track-1 via any member; לא מזוהה = high-witness
passage NOT in the reference corpora (discovery queue). Snippet = the
longest member's occurrence text.</p>
{''.join(cards)}</body></html>"""
    open(HTML_OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {MD_OUT}\n      {HTML_OUT} ({time.time() - t0:.0f}s)")
    con.close()


if __name__ == '__main__':
    main()
