#!/usr/bin/env python3
"""
Attach the novelty gate's OWN evidence to the private review DB.

WHY THIS EXISTS
---------------
The review viewer's row shows "Catalogued as:" -- `libraries.csv` column 7 and
nothing else. The novelty gate judged on far more than that: a combined
catalogue text (libraries.csv column 7 PLUS the FJMS catalog record), the
bibliography, PGP, FGP, and a per-work M-source witness count. On 83% of
`confirms` rows the thin displayed title differs from the text actually read,
so a CORRECT label looks absurd to a grader. This copies the gate's own inputs
onto the review DB so every row can show what the software read.

WHAT IT DOES (additive only)
----------------------------
Copies `gate_fact` out of the build-side facts DB into the review DB as its own
table with an index on `(sys_id, ref_work_id)`, plus meta rows documenting what
the table is and where it came from. It NEVER touches `review_row`, `facet_row`
or the existing `meta` rows, never rebuilds anything, and never opens the
grades sidecar (`<db>.grades.db`) at all -- grading work is untouchable here.

THE VERIFY GATE IS NOT OPTIONAL
-------------------------------
`gate_fact` was built from `_tmp/v3_out/discovery-v3.db`; the review DB was
projected from `_tmp/v3_out2`. Two builds. Shipping a facts table that
describes a DIFFERENT build would put wrong evidence under a correct label --
worse than the bug it fixes. So the copy runs only after `novelty_status`
agrees on every joined (sys_id, work_id) pair, within `--max-disagree-pct`
(default 0.5%). Rows the review DB has and the facts DB does not are NOT a
disagreement (they are `not_checked` and simply carry no bundle); they are
reported separately.

Idempotent: re-running drops and rewrites the table and re-stamps the meta
rows. Stdlib only.

    python scripts/attach_gate_facts.py --source <path to gate_facts.db>
    python scripts/attach_gate_facts.py --source <...> --verify-only
"""

from __future__ import annotations

import argparse
import datetime
import os
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_REVIEW_DB = os.path.join(
    REPO_ROOT, "discovery_data", "discovery-v3-REVIEW.db")

# The tables this script is FORBIDDEN to write. Asserted, not merely intended:
# a typo that dropped `review_row` would destroy the projection the viewer runs
# on, and the row counts are checked before and after the copy.
PROTECTED = ("review_row", "facet_row")

TABLE = "gate_fact"
INDEX = "ix_gate_fact_pair"

DDL = """
CREATE TABLE gate_fact (
  sys_id          TEXT,
  ref_work_id     TEXT,
  claimed_title   TEXT,   -- the identification this row asserts
  displayed_title TEXT,   -- libraries.csv col 7 alone = the thin "Catalogued as:"
  fjms_cat_text   TEXT,   -- the FJMS catalog record's own text
  gate_catalogue  TEXT,   -- the COMBINED catalogue text the gate actually read
  bib_text        TEXT,
  pgp_text        TEXT,
  fgp_text        TEXT,
  msrc_text       TEXT,   -- PER-WORK, count-only; see the meta note below
  witness_conf    TEXT,
  heuristic_reason TEXT,
  novelty_status  TEXT,
  PRIMARY KEY (sys_id, ref_work_id)
)
"""

COLUMNS = ("sys_id, ref_work_id, claimed_title, displayed_title, fjms_cat_text, "
           "gate_catalogue, bib_text, pgp_text, fgp_text, msrc_text, "
           "witness_conf, heuristic_reason, novelty_status")


def _table_names(con, schema="main"):
    return {r[0] for r in con.execute(
        "SELECT name FROM %s.sqlite_master WHERE type='table'" % schema)}


def _counts(con, tables):
    return {t: con.execute("SELECT COUNT(*) FROM main.%s" % t).fetchone()[0]
            for t in tables}


def verify(con, say=print):
    """Compare `novelty_status` on every joined pair. Returns a stats dict.

    `IS NOT` rather than `<>` so a NULL on either side counts as a comparison
    and not as a row that silently drops out of the check.
    """
    joined, agree = con.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN r.novelty_status IS g.novelty_status THEN 1 ELSE 0 END)
          FROM (SELECT DISTINCT sys_id, work_id, novelty_status FROM main.review_row) r
          JOIN src.gate_fact g
            ON g.sys_id = r.sys_id AND g.ref_work_id = r.work_id
    """).fetchone()
    agree = agree or 0
    # A pair carrying TWO different novelty values would inflate the join and
    # make "agreement" meaningless; assert it is one value per pair.
    split = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT sys_id, work_id FROM main.review_row
           GROUP BY sys_id, work_id
          HAVING COUNT(DISTINCT IFNULL(novelty_status, char(1))) > 1)
    """).fetchone()[0]
    # Uncovered pairs are expected and benign IF they are all `not_checked`.
    uncovered = con.execute("""
        SELECT IFNULL(r.novelty_status, '<null>'), COUNT(*)
          FROM (SELECT DISTINCT sys_id, work_id, novelty_status FROM main.review_row) r
          LEFT JOIN src.gate_fact g
            ON g.sys_id = r.sys_id AND g.ref_work_id = r.work_id
         WHERE g.sys_id IS NULL
         GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    stats = {
        "joined_pairs": joined,
        "agree": agree,
        "disagree": joined - agree,
        "disagree_pct": (100.0 * (joined - agree) / joined) if joined else 0.0,
        "pairs_with_split_novelty": split,
        "uncovered_by_status": [tuple(r) for r in uncovered],
        "source_rows": con.execute("SELECT COUNT(*) FROM src.gate_fact").fetchone()[0],
    }
    say("  source gate_fact rows      %d" % stats["source_rows"])
    say("  joined (sys_id, work_id)   %d" % stats["joined_pairs"])
    say("  novelty agrees             %d" % stats["agree"])
    say("  novelty DISAGREES          %d  (%.4f%%)"
        % (stats["disagree"], stats["disagree_pct"]))
    say("  pairs w/ split novelty     %d" % stats["pairs_with_split_novelty"])
    for status, n in stats["uncovered_by_status"]:
        say("  uncovered review pair      %-14s %d" % (status, n))
    return stats


META = [
    ("gate_fact.what",
     "The novelty gate's OWN evidence, one row per (sys_id, ref_work_id) "
     "identification. The viewer's 'Catalogued as:' line is libraries.csv "
     "column 7 alone; the gate read far more, and on most `confirms` rows the "
     "two differ. `gate_catalogue` is the COMBINED catalogue text the gate "
     "actually judged on -- prefer it over `displayed_title` when reading a "
     "label."),
    ("gate_fact.source",
     "Copied by scripts/attach_gate_facts.py from a facts DB extracted from "
     "the _tmp/v3_out build of discovery-v3.db. The review projection came "
     "from _tmp/v3_out2, so the copy runs only after novelty_status is proven "
     "to agree on every joined pair."),
    ("gate_fact.msrc_text",
     "PER-WORK and COUNT-ONLY: how many OTHER witnesses the M-source corpus "
     "records for this work. It is not a statement about THIS manuscript. "
     "When witness_conf IS NULL, nothing tied this manuscript to that corpus "
     "at all -- the viewer must say so on the row, because identifications "
     "resting on nothing else are being graded."),
    ("gate_fact.no_bundle",
     "A pair absent from gate_fact was never put to the gate (novelty "
     "not_checked). The viewer says so plainly rather than rendering an empty "
     "block."),
]


def attach(con, say=print):
    before = _counts(con, PROTECTED)
    con.execute("DROP INDEX IF EXISTS %s" % INDEX)
    con.execute("DROP TABLE IF EXISTS main.%s" % TABLE)
    con.execute(DDL)
    con.execute("INSERT INTO main.gate_fact (%s) SELECT %s FROM src.gate_fact"
                % (COLUMNS, COLUMNS))
    con.execute("CREATE INDEX %s ON gate_fact (sys_id, ref_work_id)" % INDEX)
    stamp = datetime.datetime.now().replace(microsecond=0).isoformat(" ")
    for key, value in META:
        con.execute("INSERT INTO meta (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value))
    con.execute("INSERT INTO meta (key, value) VALUES ('gate_fact.attached_at', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (stamp,))
    n = con.execute("SELECT COUNT(*) FROM main.gate_fact").fetchone()[0]
    con.execute("INSERT INTO meta (key, value) VALUES ('gate_fact.rows', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (str(n),))
    after = _counts(con, PROTECTED)
    if before != after:
        raise SystemExit("ABORT: a protected table changed row count: %r -> %r"
                         % (before, after))
    say("  copied                     %d rows into main.gate_fact" % n)
    say("  protected tables intact    %s"
        % ", ".join("%s=%d" % kv for kv in sorted(after.items())))
    return n


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--source", required=True,
                    help="path to the gate_facts.db carrying table gate_fact")
    ap.add_argument("--review-db", default=DEFAULT_REVIEW_DB)
    ap.add_argument("--max-disagree-pct", type=float, default=0.5)
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args(argv)

    for path in (args.source, args.review_db):
        if not os.path.exists(path):
            raise SystemExit("missing: %s" % path)

    # `uri=True` is what makes the read-only ATTACH below possible at all: a
    # bare path ATTACH would open the source WRITABLE, and the source is the
    # only copy of the gate's evidence.
    con = sqlite3.connect(args.review_db, uri=True)
    try:
        con.execute("ATTACH DATABASE ? AS src", ("file:%s?mode=ro"
                                                 % args.source.replace("\\", "/"),))
        if "gate_fact" not in _table_names(con, "src"):
            raise SystemExit("source has no table gate_fact: %s" % args.source)
        missing = [t for t in PROTECTED if t not in _table_names(con)]
        if missing:
            raise SystemExit("review DB is not the review projection (no %s)"
                             % ", ".join(missing))

        print("verify (step 1) -- gate_fact vs review_row novelty_status")
        stats = verify(con)
        if stats["pairs_with_split_novelty"]:
            raise SystemExit(
                "ABORT: %d (sys_id, work_id) pairs carry more than one "
                "novelty_status; the agreement figure would be meaningless."
                % stats["pairs_with_split_novelty"])
        if stats["disagree_pct"] > args.max_disagree_pct:
            raise SystemExit(
                "ABORT: %.4f%% disagreement exceeds the %.2f%% ceiling. The "
                "facts table describes a different build; do not ship it."
                % (stats["disagree_pct"], args.max_disagree_pct))
        if args.verify_only:
            print("verify-only: nothing written.")
            return 0

        print("attach (step 2)")
        attach(con)
        con.commit()
        print("done: %s" % args.review_db)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
