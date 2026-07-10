# -*- coding: utf-8 -*-
"""RSG postmortem — step 4: pin the failure mechanism for unit 1430332.
Read-only. Uses frag1_truncation.query_batch want_diag + classify_failure."""
import json
import pickle
import sqlite3
from collections import Counter

from normalize import norm_stream
from track1_match import build_ref_index
from frag1_truncation import query_batch, classify_failure

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
MASKS = PROBE + r"\data\ref_canon_masks.json"
MEM = "passage_unit_members_accepted_pairs_canonmask"


def main():
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")

    # (1) how many of unit 1430332's pages are in LIVE track1_matches?
    for u in (1430332, 303006, 1157648, 1038702):
        pids = [r[0] for r in con.execute(
            f"SELECT DISTINCT page_id FROM {MEM} WHERE unit=?", (u,))]
        live = 0
        for i in range(0, len(pids), 400):
            b = pids[i:i+400]
            ph = ','.join('?'*len(b))
            live += con.execute(
                f"SELECT COUNT(DISTINCT page_id) FROM track1_matches "
                f"WHERE shadowed_by IS NULL AND page_id IN ({ph})", b
            ).fetchone()[0]
        print(f"unit {u}: {len(pids)} member pages, "
              f"{live} in LIVE track1_matches ({100*live/len(pids):.0f}%)")

    # (2) failure-stage classification for unit 1430332 pages vs FULL ref
    works = pickle.load(open(REF, 'rb'))
    canon_masks = json.load(open(MASKS, encoding='utf-8'))
    full = build_ref_index(works, canon_masks)[:6]

    pids = [r[0] for r in con.execute(
        f"SELECT DISTINCT page_id FROM {MEM} WHERE unit=1430332")][:80]
    streams = []
    for pid in pids:
        tx = con.execute("SELECT text FROM pages WHERE page_id=?",
                         (pid,)).fetchone()[0]
        streams.append(norm_stream(tx or '')[0])
    results, diag = query_batch(streams, full, want_diag=True)
    hist = Counter()
    samples = []
    for s, cand, dg in zip(streams, results, diag):
        cls, bd = classify_failure(dg, cand)
        hist[cls] += 1
        if len(samples) < 8:
            samples.append((len(s), dg['grams'], dg['hits'],
                            dg['best_cluster'], len(cand), cls))
    print(f"\nunit 1430332 failure-stage histogram (n={len(streams)} pages, "
          f"vs FULL ref): {dict(hist)}")
    print("mean page len:", sum(len(s) for s in streams)//len(streams))
    print("\nsample pages (len, grams, ref-hits, best_diagonal_cluster, "
          "n_verified_cand, stage):")
    for row in samples:
        print("  ", row)
    con.close()


if __name__ == '__main__':
    main()
