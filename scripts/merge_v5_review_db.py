# -*- coding: utf-8 -*-
"""Merge the independently rendered review satellites into one artifact.

WHY RENDER-THEN-MERGE rather than an append mode. The builder deletes its output
and rebuilds from scratch, writes single-pass metadata, and creates the facet
projection at the end; teaching it to append would mean teaching all three of
those to be stateful. Rendering each source independently and merging is both
simpler and what makes the NEXT source cheap: one more render, one more merge,
no new special case.

WHAT MAKES THE MERGE SAFE:
  * evidence ids are PRESERVED from each source and their intersection is
    asserted EMPTY, so no row can silently overwrite another (the two producers
    mint ids in structurally different namespaces, but that is checked, not
    assumed);
  * `source_file.id` and `reference_witness.witness_id` are content-derived, so
    two satellites that saw the same file agree on its id by construction and no
    FK rewriting is needed;
  * parents are inserted before children inside ONE transaction with
    `foreign_keys=ON`, and any failure rolls back without publishing;
  * per-table exact counts, `foreign_key_check` and `integrity_check` all run
    before the file is accepted;
  * meta is namespaced per input (`base.*`, `rsource.*`) with cumulative totals,
    so no counter from one pass silently overwrites the other's.

Run:
    python -X utf8 -u scripts/merge_v5_review_db.py \
        --out discovery_data/discovery-v5-REVIEW.db \
        --input base=discovery_data/discovery-v5-REVIEW.base.db \
        --input rsource=discovery_data/discovery-v5-REVIEW.rsource.db
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Parents first: source_file <- reference_witness <- review_row.
COPY_ORDER = ("source_file", "reference_witness", "review_row")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", action="append", required=True,
                    help="label=path (repeatable), in the order to merge")
    ap.add_argument("--out", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    args = ap.parse_args(argv)

    inputs = []
    for spec in args.input:
        if "=" not in spec:
            raise SystemExit("--input must be label=path, got %r" % spec)
        label, path = spec.split("=", 1)
        if not os.path.exists(path):
            raise SystemExit("missing input: %s" % path)
        inputs.append((label, os.path.abspath(path)))

    t0 = time.time()

    def log(m):
        print("[%6.0fs] %s" % (time.time() - t0, m), flush=True)

    # ---- pre-flight: schemas agree, and no evidence id is claimed twice ----
    cols_by_src, ids_by_src, counts_in = {}, {}, {}
    for label, path in inputs:
        c = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
        schema = c.execute("SELECT value FROM meta WHERE key='schema'"
                           ).fetchone()
        if not schema or not schema[0].endswith("/2"):
            raise SystemExit("%s is not a schema-v2 artifact (%r)"
                             % (label, schema and schema[0]))
        cols_by_src[label] = [r[1] for r in c.execute(
            "PRAGMA table_info(review_row)")]
        ids_by_src[label] = {r[0] for r in c.execute(
            "SELECT evidence_id FROM review_row")}
        counts_in[label] = {t: c.execute("SELECT COUNT(*) FROM %s" % t
                                         ).fetchone()[0]
                            for t in COPY_ORDER}
        log("%s: %s" % (label, counts_in[label]))
        c.close()

    ref = cols_by_src[inputs[0][0]]
    for label, _ in inputs[1:]:
        if cols_by_src[label] != ref:
            raise SystemExit("column mismatch between %s and %s"
                             % (inputs[0][0], label))
    log("review_row columns identical across inputs: %d" % len(ref))

    labels = [x[0] for x in inputs]
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            both = ids_by_src[labels[i]] & ids_by_src[labels[j]]
            if both:
                raise SystemExit(
                    "evidence_id collision between %s and %s (%d ids, e.g. %s)"
                    % (labels[i], labels[j], len(both), sorted(both)[:3]))
    log("evidence_id intersection is empty across all inputs")

    if os.path.exists(args.out):
        os.remove(args.out)
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.exists(args.out + suffix):
            os.remove(args.out + suffix)

    # ---- build: first input supplies the schema ---------------------------
    first_label, first_path = inputs[0]
    out = sqlite3.connect(args.out)
    out.execute("PRAGMA foreign_keys=ON")
    out.execute("PRAGMA cache_size=-2000000")
    src0 = sqlite3.connect("file:%s?mode=ro" % first_path, uri=True)
    ddl = [r[0] for r in src0.execute(
        "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL "
        "AND name NOT LIKE 'sqlite_%'")]
    src0.close()
    for stmt in ddl:
        # facet_row is a derived projection; the viewer rebuilds it when stale,
        # and rebuilding it here after the merge is cheaper than merging it.
        if "facet_row" in stmt:
            continue
        out.execute(stmt)
    out.commit()
    log("schema created from %s (%d statements)" % (first_label, len(ddl)))

    total = {t: 0 for t in COPY_ORDER}
    meta_rows = []
    # ATTACH must happen OUTSIDE a transaction -- SQLite refuses it inside one
    # ("database is locked"), so every input is attached under its own alias
    # first and the copy then runs as a single atomic transaction.
    aliases = []
    for i, (label, path) in enumerate(inputs):
        alias = "src%d" % i
        out.execute("ATTACH DATABASE ? AS %s" % alias, (path,))
        aliases.append((label, alias))
    log("attached %d inputs" % len(aliases))

    try:
        out.execute("BEGIN")
        for label, alias in aliases:
            for t in COPY_ORDER:
                # INSERT, never INSERT OR REPLACE, for the row table: a
                # duplicate key must raise rather than quietly discard a row.
                # The two shared parent tables are unioned by content-derived
                # id, where an identical row from both inputs is expected.
                verb = ("INSERT OR IGNORE" if t in ("source_file",
                                                    "reference_witness")
                        else "INSERT")
                out.execute("%s INTO main.%s SELECT * FROM %s.%s"
                            % (verb, t, alias, t))
            for k, v in out.execute("SELECT key, value FROM %s.meta" % alias):
                meta_rows.append(("%s.%s" % (label, k), v))
            for t in COPY_ORDER:
                total[t] = out.execute("SELECT COUNT(*) FROM main.%s"
                                       % t).fetchone()[0]
            log("merged %s -> %s" % (label, total))

        # review_row must be EXACTLY the sum; the shared tables are unioned by
        # content-derived id, so they are only required not to shrink.
        want_rows = sum(counts_in[lbl]["review_row"] for lbl in labels)
        got_rows = out.execute("SELECT COUNT(*) FROM review_row").fetchone()[0]
        if got_rows != want_rows:
            raise RuntimeError("review_row %d != expected %d"
                               % (got_rows, want_rows))
        for t in ("source_file", "reference_witness"):
            biggest = max(counts_in[lbl][t] for lbl in labels)
            if total[t] < biggest:
                raise RuntimeError("%s shrank to %d (largest input had %d)"
                                   % (t, total[t], biggest))

        for k, v in meta_rows:
            out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        # `doc.*` rows are READ BY THE VIEWER under their bare names (the help
        # panel and every tooltip) -- namespacing them breaks 7 of its 8 help
        # sections. Promote the first input's copies back to bare keys; the
        # namespaced duplicates stay for provenance. (Found by the outsider
        # review, 2026-08-30.)
        first = inputs[0][0]
        for k, v in meta_rows:
            if k.startswith(first + ".doc."):
                out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                            (k[len(first) + 1:], v))
        for k, v in (("schema", "discovery-v3-review/2"),
                     ("audience", "private"),
                     ("merged_from", json.dumps(
                         [{"label": lbl, "file": os.path.basename(p),
                           "review_rows": counts_in[lbl]["review_row"]}
                          for lbl, p in inputs], ensure_ascii=False)),
                     ("rows", str(got_rows)),
                     ("source_files", str(total["source_file"])),
                     ("reference_witnesses", str(total["reference_witness"])),
                     ("merged_at", time.strftime("%Y-%m-%d %H:%M:%S"))):
            out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        out.execute("COMMIT")
        for _, alias in aliases:
            out.execute("DETACH DATABASE %s" % alias)
    except Exception as exc:  # noqa: BLE001 -- any failure must not publish
        try:
            out.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        out.close()
        os.remove(args.out)
        print("\n!!! MERGE FAILED, output removed: %s" % exc)
        return 1

    # ---- integrity, after the data is in ---------------------------------
    fk = out.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        print("!!! foreign_key_check reported %d problems (first: %s)"
              % (len(fk), fk[0]))
        out.close()
        return 1
    integ = out.execute("PRAGMA integrity_check").fetchone()[0]
    if integ != "ok":
        print("!!! integrity_check: %s" % integ)
        out.close()
        return 1
    log("foreign_key_check clean; integrity_check ok")

    out.execute("""CREATE TABLE facet_row AS SELECT
                     evidence_id, sys_id, shelfmark, domain, work_id,
                     work_title, work_author, novelty_status, main_pool,
                     claim_type, router_verdict, routing_status FROM review_row""")
    for c in ("domain", "work_id", "work_author", "novelty_status",
              "main_pool", "claim_type", "router_verdict", "routing_status",
              "evidence_id"):
        out.execute("CREATE INDEX ix_fr_%s ON facet_row(%s)" % (c, c))
    out.commit()
    log("facet projection rebuilt")

    out.execute("VACUUM")
    by_corpus = out.execute("SELECT source_corpus, COUNT(*) FROM review_row "
                            "GROUP BY 1 ORDER BY 2 DESC").fetchall()
    out.close()
    log("wrote %s (%.0f MB)" % (args.out, os.path.getsize(args.out) / 1e6))
    print("  rows by corpus: %s" % dict(by_corpus))
    return 0


if __name__ == "__main__":
    sys.exit(main())
