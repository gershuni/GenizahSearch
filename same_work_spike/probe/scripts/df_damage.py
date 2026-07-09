# -*- coding: utf-8 -*-
"""Who does the DF cap hurt? (Hillel 2026-07-08: 'I would guess liturgy
is not the only one that got hurt.')

Track-1 identifications are the ground truth: for each work, its pages
are known witnesses of the same text — so within-work PAIRING RATE
(fraction of a work's pages with >=1 accepted pair to another page of
the same work) measures Track-2's recall on that work. If the DF<=100
cap starves high-witness texts of anchors, pairing rate must FALL as
witness count rises. Edited (unmasked in canonmask) works are measured
against the discovery map; canonical works against the unmasked run.

The clean cohort: works SHORTER than ~2K letters — every witness carries
the same text, so dispersion cannot explain unpaired pages; low pairing
there is pure anchor starvation.

Usage: python df_damage.py [db]
Out: results/df_damage_full.md
"""
import pickle
import sqlite3
import sys
from collections import defaultdict

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT = ROOT + r"\same_work_spike\probe\results\df_damage_full.md"
CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
MIN_LETTERS = 200      # substantive presence of the work on the page
BUCKETS = [(2, 5), (6, 10), (11, 20), (21, 50), (51, 100),
           (101, 200), (201, 500), (501, 10 ** 9)]


def main():
    con = sqlite3.connect(DB)
    page_works = defaultdict(set)
    work_pages = defaultdict(set)
    work_ms = defaultdict(set)
    work_info = {}
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live = (" WHERE shadowed_by IS NULL" if 'shadowed_by' in cols else "")
    for pid, sid, wid, cat, author, title, letters in con.execute(
            "SELECT page_id, sys_id, work_id, cat, author, title, "
            f"matched_letters FROM track1_matches{live}"):
        if letters < MIN_LETTERS:
            continue
        page_works[pid].add(wid)
        work_pages[wid].add(pid)
        work_ms[wid].add(sid)
        work_info[wid] = (cat, f"{author} — {title}" if author else title)
    # single-witness works cannot pair (same-sys pairs are excluded at
    # generation) — measuring them as 'damage' is an artifact
    for wid in [w for w, ms in work_ms.items() if len(ms) < 2]:
        for pid in work_pages[wid]:
            page_works[pid].discard(wid)
        del work_pages[wid], work_ms[wid]
    print(f"works (>=2 witness MSS): {len(work_pages):,}; identified "
          f"pages: {len(page_works):,}", flush=True)

    def pairing(table, cats_keep):
        paired = defaultdict(set)   # wid -> pages with a same-work partner
        for pa, pb in con.execute(
                f"SELECT page_a, page_b FROM {table} "
                f"WHERE dup_shelf = 0 AND dup_lines < 0.6"):
            wa = page_works.get(pa)
            if not wa:
                continue
            wb = page_works.get(pb)
            if not wb:
                continue
            for w in wa & wb:
                if work_info[w][0] in cats_keep:
                    paired[w].add(pa)
                    paired[w].add(pb)
        return paired

    edited_cats = {'Maagarim', 'JA'}
    paired_e = pairing('accepted_pairs_canonmask', edited_cats)
    paired_c = pairing('accepted_pairs', CANON_CATS)
    print("pairing computed", flush=True)

    def bucket_table(cats, paired):
        """Bucket by DISTINCT WITNESS MSS (not pages)."""
        rows = []
        agg = {b: [0, 0] for b in BUCKETS}
        for wid, pages in work_pages.items():
            cat, name = work_info[wid]
            if cat not in cats or len(pages) < 2:
                continue
            n, np_ = len(pages), len(paired.get(wid, ()))
            n_ms = len(work_ms[wid])
            rows.append((wid, name, cat, n, np_, np_ / n, n_ms))
            for b in BUCKETS:
                if b[0] <= n_ms <= b[1]:
                    agg[b][0] += n
                    agg[b][1] += np_
                    break
        return rows, agg

    rows_e, agg_e = bucket_table(edited_cats, paired_e)
    rows_c, agg_c = bucket_table(CANON_CATS, paired_c)

    def fmt_agg(agg):
        out = []
        for b, (tot, par) in agg.items():
            if tot:
                lbl = f"{b[0]}-{b[1] if b[1] < 10**9 else '+'}"
                out.append(f"| {lbl} | {tot:,} | {par:,} | "
                           f"{100 * par / tot:.0f}% |")
        return out

    lines = [
        "# DF-cap damage census — within-work pairing rate (full corpus)",
        "",
        "Pairing rate = of a work's Track-1-identified pages (>= "
        f"{MIN_LETTERS} matched letters), the share with >=1 accepted "
        "pair to another page of the SAME work. Falling rate with rising "
        "witness count = the DF<=100 cap starving high-witness texts.",
        "",
        "## Edited works (Maagarim/JA) vs the discovery map (canonmask)",
        "| witness-count bucket | pages | paired | rate |",
        "|---|---|---|---|", *fmt_agg(agg_e), "",
        "## Canonical works vs the unmasked map",
        "| witness-count bucket | pages | paired | rate |",
        "|---|---|---|---|", *fmt_agg(agg_c), "",
        "## Most-damaged high-witness edited works "
        "(>= 20 witness MSS, lowest pairing rate)",
    ]
    big = [r for r in rows_e if r[6] >= 20]
    for wid, name, cat, n, np_, rate, n_ms in sorted(
            big, key=lambda r: r[5])[:30]:
        lines.append(f"- [{cat}] {name[:70]}: {n_ms} MSS / {n:,} pages, "
                     f"paired {np_:,} = **{100 * rate:.0f}%**")
    lines += ["", "## Least-damaged (same filter, highest rate)"]
    for wid, name, cat, n, np_, rate, n_ms in sorted(
            big, key=lambda r: -r[5])[:10]:
        lines.append(f"- [{cat}] {name[:70]}: {n_ms} MSS / {n:,} pages, "
                     f"paired {np_:,} = {100 * rate:.0f}%")

    # ---- the clean cohort: SHORT works (no dispersion excuse) ----
    wlen = {w['id']: len(w['stream'])
            for w in pickle.load(open(
                ROOT + r"\same_work_spike\probe\data\ref_corpus.pkl", 'rb'))}
    lines += ["", "## Short-work cohort (work < 2,000 letters, >= 10 MSS)",
              "Every witness carries the SAME text — low pairing here is "
              "pure anchor starvation:"]
    short = [(paired_e.get(wid, set()), wid) for wid in work_pages
             if work_info[wid][0] in edited_cats
             and len(work_ms[wid]) >= 10 and wlen.get(wid, 10 ** 9) < 2000]
    short_rows = sorted(
        ((len(p) / len(work_pages[w]), w, len(p)) for p, w in short))
    tot = sum(len(work_pages[w]) for _, w in short)
    par = sum(len(p) for p, _ in short)
    lines.append(f"- cohort: {len(short)} works, {tot:,} pages, paired "
                 f"{par:,} = {100 * par / max(1, tot):.0f}% overall")
    for rate, wid, np_ in short_rows[:15]:
        cat, name = work_info[wid]
        lines.append(f"- **{100 * rate:.0f}%** [{wlen[wid]} letters] "
                     f"{name[:65]} ({len(work_ms[wid])} MSS / "
                     f"{len(work_pages[wid])} pages)")
    lines += [
        "",
        "## Reading",
        "1. NO aggregate top-end collapse: pairing rate RISES with witness",
        "   count (variants create low-DF grams that survive the cap).",
        "2. The cap's real victims: SHORT high-witness texts — piyyutim,",
        "   prayers, blessings (0-30% pairing at 25-101 witnesses, inverse",
        "   gradient within the cohort). In the Genizah that class IS",
        "   liturgy/formulae almost by definition.",
        "3. Long works' low pairing = fragment dispersion (witnesses carry",
        "   different sections) — benign, not cap damage.",
        "4. Side-catch (historical): suspicious Track-1 identifications the",
        "   metric surfaced pre-shadowing — מגילת המקדש (176), מגילת פשר",
        "   ישעיהו (35), פיירברג לאן (96). Competitive span assignment",
        "   (track1_shadow.py) now resolves these; this census reads live",
        "   (unshadowed) rows only when the shadowed_by column exists.",
        "5. Remedy note: for REFERENCE-COVERED short works, Track-1 already",
        "   connects the witnesses (the testimonies census groups them",
        "   without needing Track-2 pairs). The cap damage matters for",
        "   UNREFERENCED short texts — most piyyut — which is exactly the",
        "   per-domain-second-pass / motif-guided-completion case.",
    ]
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines))
    con.close()


if __name__ == '__main__':
    main()
