# -*- coding: utf-8 -*-
"""Attach `gate_verdict_fact`: the LLM adjudication verdicts (owner, 2026-08-31).

Two pinned questions, both run 2026-08-30/31 over the main+unclear pools on
gemini-3.7-flash (scripts/divergence_adjudication_gate.py), judged at PAIR
grain -- one verdict per (sys_id, work_id), on the pair's strongest page:

  divergence -- when the computed identification and the catalogue point at
                different works, who is right? Owner-validated on 117 graded
                cases (81.2% raw; 97.4% after the owner adjudicated the
                disagreements -- 16 of 22 went to the model). The verdict
                class `computed_right_catalogue_mismatch` asserts scholars
                erred and went 0-for-4 when the owner contested it: HUMAN
                REVIEW, never an automatic signal.

  new_finds  -- for pairs no finding aid records, is the proposed NEW
                identification credible? Owner-validated on 84 graded cases
                (88.1%; `credible_new_identification` precision 58/60).

A verdict is a LABEL for review ordering and display -- it never moves a row
between pools by itself. Every reason/doubt string is masking-scanned before
the write (fail closed).

Run (review server STOPPED):
    python -X utf8 scripts/attach_gate_verdicts.py
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_RUNS = (
    ("divergence", os.path.join(REPO_ROOT, "_tmp", "divergence_gate",
                                "run_divergence_gemini-3_7-flash.jsonl")),
    ("new_finds", os.path.join(REPO_ROOT, "_tmp", "divergence_gate",
                               "run_new_finds_gemini-3_7-flash.jsonl")),
)

VERDICTS = ("catalogue_right_match_is_quotation", "catalogue_right_claim_mistaken",
            "both_right_multiple_works", "catalogue_too_general",
            "overlapping_works", "computed_right_catalogue_mismatch",
            "credible_new_identification", "plausible_needs_expert_check",
            "weak_match_generic_text", "actually_recorded", "wrong_identification",
            "not_checked")

DDL = """CREATE TABLE gate_verdict_fact(
  sys_id         TEXT NOT NULL,
  work_id        TEXT NOT NULL,
  task           TEXT NOT NULL CHECK (task IN ('divergence','new_finds')),
  verdict        TEXT NOT NULL CHECK (verdict IN (%s)),
  doubt          TEXT,
  reason         TEXT,
  judged_page_id TEXT,
  model          TEXT,
  prompt_sha     TEXT,
  PRIMARY KEY (sys_id, work_id, task)
)""" % ",".join("'%s'" % v for v in VERDICTS)

DOC_DIVERGENCE = (
    "An LLM adjudication of catalogue-divergent identifications: the model "
    "read the catalogue's own prose, the bibliography, PGP, the aligned "
    "excerpts and the computed signals, and said what the disagreement means. "
    "The catalogue is the favoured prior (scholars who studied the "
    "manuscript); two named situations discount it -- a too-general catalogue "
    "term, and a page carrying several works. Owner-validated on 117 graded "
    "cases: 'catalogue right' verdicts were 98.8% owner-confirmed; "
    "computed_right_catalogue_mismatch asserts the scholars erred and went "
    "0-for-4 when contested -- read it as 'needs human review', never as a "
    "finding. A verdict is a LABEL: it moves nothing between pools.")

DOC_NEW_FINDS = (
    "An LLM check of candidate NEW identifications (pairs no finding aid "
    "records): does the page read as continuous text of the claimed work, "
    "and is it really unrecorded? Owner-validated on 84 graded cases: "
    "credible_new_identification precision 58/60. Each verdict carries a "
    "'doubt' -- the one thing an expert should verify. actually_recorded "
    "means the model found the identification already in an aid's prose "
    "(a novelty-gate miss). A verdict is a LABEL: it moves nothing between "
    "pools.")


def load_runs(runs, say=print):
    """(sys_id, work_id, task) -> record; transport failures dropped, last
    verdict per pair wins (a resumed run appends)."""
    out = {}
    for task, path in runs:
        n = 0
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("verdict") in (None, "transport_failed"):
                    continue
                out[(rec["sys_id"], rec["work_id"], task)] = rec
                n += 1
        say("%s: %d verdict lines from %s" % (task, n, os.path.basename(path)))
    return out


def attach(db_path, runs=DEFAULT_RUNS, say=print):
    from scripts.check_atlas_masking import build_matcher, load_patterns
    matcher = build_matcher(load_patterns())   # raises if no patterns: fail closed

    recs = load_runs(runs, say)
    rows, counts = [], {}
    blob = []
    for (sys_id, work_id, task), r in sorted(recs.items()):
        rows.append((sys_id, work_id, task, r["verdict"], r.get("doubt"),
                     r.get("reason"), r.get("page_id"), r.get("model"),
                     r.get("prompt_sha")))
        counts.setdefault(task, {}).setdefault(r["verdict"], 0)
        counts[task][r["verdict"]] += 1
        blob.append("%s %s" % (r.get("doubt") or "", r.get("reason") or ""))
    issues = matcher.scan("\n".join(blob).encode("utf-8"), "gate_verdict_fact")
    if issues:
        raise SystemExit("MASKING VIOLATION in %d verdict string(s); "
                         "nothing written" % len(issues))

    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS gate_verdict_fact")
        con.execute(DDL)
        con.executemany(
            "INSERT INTO gate_verdict_fact VALUES (?,?,?,?,?,?,?,?,?)", rows)
        for k, v in (("gate_verdict.version", "1"),
                     ("gate_verdict.counts", json.dumps(counts)),
                     ("gate_verdict.at", time.strftime("%Y-%m-%d %H:%M:%S")),
                     ("doc.gate_divergence", DOC_DIVERGENCE),
                     ("doc.gate_new_finds", DOC_NEW_FINDS)):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        con.execute("DROP TABLE IF EXISTS facet_row")   # gains two columns
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    say("gate_verdict_fact: %d rows  %s" % (len(rows), json.dumps(counts)))
    return counts


def main(argv=None) -> int:
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        raise SystemExit("missing: %s" % args.db)
    for _task, path in DEFAULT_RUNS:
        if not os.path.exists(path):
            raise SystemExit("missing run checkpoint: %s" % path)
    attach(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
