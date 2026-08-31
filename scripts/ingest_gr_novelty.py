# -*- coding: utf-8 -*-
"""B7 phase 2: ingest the G-R novelty verdicts, adapter first, then review db.

Path: `discovery_novelty_production_run.py` wrote a verdict cache keyed
`"{sys_id}::{ref_work_id}"`. This script

  1. loads it through the production loader (`load_novelty_verdicts`,
     whole-file SHA-256 pinned against the value recorded here at ingest time),
  2. collapses it to the centralized grain (`build_novelty_grain_index`),
  3. applies it to the adapter with the ONE shared implementation
     (`apply_novelty_verdicts` -- both of its build-time assertions run:
     masked source labels, one novelty result per claim), and
  4. propagates `novelty_status` onto the review artifact's R-source rows by
     preserved evidence_id, gated on exact per-shade count agreement.

`main_pool` / `main_pool_reason` are NOT touched: `main_pool_decision` reads
human confirmation, claim types, bands, ties and folio counts -- never
novelty -- so identifications are unaffected by this ingest.

FINGERPRINT NOTE. `expected_fingerprints=None` here, documented: that gate
exists to stop a STALE cache being reused against changed inputs. This cache
was produced in this same session from this exact adapter file, each entry
carries the fingerprint the runner itself computed, and the whole file is
SHA-pinned; there is no older cache this could be confused with.

Run (review server STOPPED -- step 4 writes into the artifact):
    python -X utf8 scripts/ingest_gr_novelty.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.build_discovery_sidecar import (  # noqa: E402
    apply_novelty_verdicts,
    build_novelty_grain_index,
    load_novelty_verdicts,
)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verdicts", default=os.path.join(
        REPO_ROOT, "discovery_data", "novelty_gr_verdicts.json"))
    ap.add_argument("--adapter", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "gr-adapter.db"))
    ap.add_argument("--review-db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    args = ap.parse_args(argv)
    for p in (args.verdicts, args.adapter, args.review_db):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)

    sha = hashlib.sha256(open(args.verdicts, "rb").read()).hexdigest()
    entries, load_stats = load_novelty_verdicts(args.verdicts, sha256=sha)
    grain, grain_stats = build_novelty_grain_index(entries)
    print("loaded    : %d entries -> %d grain keys  %s"
          % (len(entries), len(grain), grain_stats))

    # ---- adapter ----------------------------------------------------------
    con = sqlite3.connect(args.adapter)
    cols = {r[1] for r in con.execute("PRAGMA table_info(discovery_evidence)")}
    if "novelty_source_label" not in cols:
        # the production DDL has it; the adapter predates the ingest
        con.execute("ALTER TABLE discovery_evidence "
                    "ADD COLUMN novelty_source_label TEXT")
    stats = apply_novelty_verdicts(con, grain)
    con.commit()
    shades_adapter = dict(con.execute(
        "SELECT novelty_status, COUNT(*) FROM discovery_evidence GROUP BY 1"))
    for k, v in (("novelty_ingest.verdicts_sha256", sha),
                 ("novelty_ingest.stats", json.dumps(stats)),
                 ("novelty_ingest.at", time.strftime("%Y-%m-%d %H:%M:%S"))):
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    con.commit()
    con.close()
    print("adapter   : %s" % stats)
    print("  shades  : %s" % shades_adapter)

    # ---- review artifact --------------------------------------------------
    con = sqlite3.connect(args.review_db)
    try:
        con.execute("ATTACH DATABASE ? AS ad", (args.adapter,))
        con.execute("BEGIN")
        con.execute(
            "UPDATE review_row SET novelty_status = ("
            "  SELECT de.novelty_status FROM ad.discovery_evidence de"
            "  WHERE de.evidence_id = review_row.evidence_id)"
            " WHERE source_corpus='rsource'")
        n = con.execute("SELECT changes()").fetchone()[0]
        if n != 226679:
            raise RuntimeError("updated %d rsource rows, expected 226679" % n)
        nulls = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE source_corpus='rsource' "
            "AND novelty_status IS NULL").fetchone()[0]
        if nulls:
            raise RuntimeError("%d rsource rows lost their novelty_status -- "
                               "an evidence_id did not join" % nulls)
        shades_review = dict(con.execute(
            "SELECT novelty_status, COUNT(*) FROM review_row "
            "WHERE source_corpus='rsource' GROUP BY 1"))
        if shades_review != shades_adapter:
            raise RuntimeError("shade counts diverge: review %s vs adapter %s"
                               % (shades_review, shades_adapter))
        for k, v in (("rsource_novelty.verdicts_sha256", sha),
                     ("rsource_novelty.shades",
                      json.dumps(shades_review)),
                     ("rsource_novelty.at",
                      time.strftime("%Y-%m-%d %H:%M:%S"))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        # novelty_status is a facet column -> the projection must rebuild
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    print("review    : 226,679 rsource rows updated; shades match the adapter")
    for k in sorted(shades_review, key=lambda x: -shades_review[x]):
        print("  %7d  %s" % (shades_review[k], k))
    return 0


if __name__ == "__main__":
    sys.exit(main())
