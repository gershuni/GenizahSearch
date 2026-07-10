# -*- coding: utf-8 -*-
"""RSG postmortem — step 1: explore reference corpus + DB schema.
Read-only diagnostic. Does NOT modify pipeline scripts / DB."""
import pickle
import sqlite3
import re

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF1 = PROBE + r"\data\ref_corpus.pkl"
REF2 = PROBE + r"\data\ref_corpus_v2.pkl"

RSG_PAT = re.compile(r'רס"ג|רסג|סעדי|saadi|tafsir|תפסיר|al-?fayyumi|fayyumi',
                     re.IGNORECASE)


def scan_ref(path, label):
    works = pickle.load(open(path, 'rb'))
    print(f"\n===== {label}: {len(works):,} works =====")
    # show keys of first work
    print("keys:", sorted(works[0].keys()))
    # cat distribution
    from collections import Counter
    cats = Counter(w.get('cat') for w in works)
    print("cats:", dict(cats))
    # find RSG-related works
    hits = []
    for i, w in enumerate(works):
        blob = ' '.join(str(w.get(k, '')) for k in
                        ('title', 'author', 'genre', 'mesirah', 'id'))
        if RSG_PAT.search(blob):
            hits.append((i, w))
    print(f"\nRSG-matching works: {len(hits)}")
    for i, w in hits:
        print(f"  [{i}] id={w.get('id')!r} cat={w.get('cat')!r} "
              f"author={w.get('author')!r} title={w.get('title')!r} "
              f"genre={w.get('genre')!r} streamlen={len(w.get('stream',''))}")
    return works, hits


def main():
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")

    # DB schema
    print("===== track1_matches columns =====")
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    print(cols)
    print("row count:", con.execute(
        "SELECT COUNT(*) FROM track1_matches").fetchone()[0])
    if 'shadowed_by' in cols:
        live = con.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE shadowed_by IS NULL"
        ).fetchone()[0]
        print("live (shadowed_by IS NULL):", live)

    print("\n===== all tables =====")
    for (name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        print(" ", name)

    # unit 1430332 metadata + a couple other RSG clusters
    for tbl in ("passage_units_accepted_pairs_canonmask",):
        ucols = [r[1] for r in con.execute(f"PRAGMA table_info({tbl})")]
        print(f"\n{tbl} cols:", ucols)
    for u in (1430332, 303006, 1157648, 1038702):
        row = con.execute(
            "SELECT unit,n_pages,n_ms,med_len,t1_label,t1_n,conf,labeled "
            "FROM passage_units_accepted_pairs_canonmask WHERE unit=?",
            (u,)).fetchone()
        print(f"\nunit {u}: {row}")

    con.close()

    scan_ref(REF1, "ref_corpus.pkl (v1)")
    scan_ref(REF2, "ref_corpus_v2.pkl (v2)")


if __name__ == '__main__':
    main()
