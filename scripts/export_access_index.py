# -*- coding: utf-8 -*-
"""Export the v5 review artifact as a Microsoft Access INDEX database.

WHY INDEX-ONLY. Access hard-caps a database at 2 GB; the review db is 3 GB
almost entirely because every row carries both text sides. The reviewer is
getting the TEXT FILES themselves (owner, 2026-08-30), so this export drops
the six text columns and keeps everything else: identifiers, pools, verdicts,
loci, and the file+character offsets that point into the files he holds.

FILENAMES FOR MASKED CORPORA. The review db stores only masked ids (RS:1.2,
M:Ytext1000_00) by standing rule. The reviewer ALREADY POSSESSES the R-source
and M-source files, and the owner explicitly authorized giving him the
mapping (2026-08-30), so `source_file.filename` carries the real BASENAME
(never a local path) resolved from the local key file. Every basename is
masking-scanned before the export is written; a hit stops the run.

Tables:
  identification_row  -- one per review row (no text), incl. pool (triage)
  source_file         -- masked id -> kind + filename
  reference_witness   -- witness -> work + source file
  meta                -- provenance + the column documentation (doc.*)

Run (needs the 'Microsoft Access Driver (*.mdb, *.accdb)' ODBC driver):
    python -X utf8 scripts/export_access_index.py
Output: discovery_data/discovery-v5-INDEX.accdb
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEXT_COLS = {"ms_before", "ms_match", "ms_after",
             "ref_before", "ref_match", "ref_after"}
# columns that can exceed Access's 255-char TEXT limit
LONG_COLS = {"locus_label", "catalogue_title", "work_title",
             "owner_ruling_note", "main_pool_reason", "unit_source_ref"}


def access_type(name, sqlite_type):
    t = (sqlite_type or "").upper()
    if name in LONG_COLS:
        return "LONGTEXT"
    if "INT" in t:
        return "LONG"          # 32-bit; every count/offset here fits
    if t in ("REAL", "FLOAT", "DOUBLE"):
        return "DOUBLE"
    return "TEXT(255)"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--sourcekeys", default=os.path.join(
        os.path.expanduser("~"), ".genizah-private", "sourcekeys.json"))
    ap.add_argument("--out", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-INDEX.accdb"))
    args = ap.parse_args(argv)

    import msaccessdb
    import pyodbc

    keys = json.load(open(args.sourcekeys, encoding="utf-8"))
    src = sqlite3.connect("file:%s?mode=ro" % args.db, uri=True)
    src.row_factory = sqlite3.Row

    # ---- masking gate on the ONLY new strings: the masked-file basenames ---
    basenames = {rid: os.path.basename(p) for rid, p in keys.items()}
    fd, tmp = tempfile.mkstemp(suffix=".txt", prefix="accdb_names_")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(set(basenames.values()))))
    r = subprocess.run(
        [sys.executable, "-X", "utf8",
         os.path.join(REPO_ROOT, "scripts", "check_atlas_masking.py"),
         "--scan-asset", tmp], capture_output=True, text=True)
    os.unlink(tmp)
    if r.returncode != 0:
        raise SystemExit(
            "masking scan of the %d masked-file basenames FAILED -- a real "
            "corpus name pattern appears in a filename. Nothing was written; "
            "this needs an owner decision, not a suppression.\n%s"
            % (len(set(basenames.values())), (r.stdout or "") + (r.stderr or "")))
    print("masking   : %d basenames scanned clean" % len(set(basenames.values())))

    if os.path.exists(args.out):
        os.remove(args.out)
    msaccessdb.create(args.out)
    acc = pyodbc.connect(
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s" % args.out,
        autocommit=False)
    cur = acc.cursor()

    # ---- identification_row (review_row minus text, plus the pool) ---------
    cols = [(r2[1], r2[2]) for r2 in src.execute("PRAGMA table_info(review_row)")
            if r2[1] not in TEXT_COLS]
    names = [c for c, _ in cols]
    ddl = ", ".join("[%s] %s" % (c, access_type(c, t)) for c, t in cols)
    cur.execute("CREATE TABLE identification_row (%s, [pool] TEXT(32))" % ddl)
    sel = ("SELECT %s, fr.triage FROM review_row r "
           "LEFT JOIN facet_row fr ON fr.evidence_id = r.evidence_id"
           % ", ".join("r.%s" % c for c in names))
    ins = ("INSERT INTO identification_row (%s, [pool]) VALUES (%s)"
           % (", ".join("[%s]" % c for c in names),
              ", ".join(["?"] * (len(names) + 1))))
    t0 = time.time()
    batch, n = [], 0
    for row in src.execute(sel):
        vals = [row[i] for i in range(len(names) + 1)]
        # Access TEXT(255) truncation guard: anything over 255 that is not a
        # LONGTEXT column is clipped explicitly, never silently by the driver
        for i, c in enumerate(names):
            v = vals[i]
            if isinstance(v, str) and len(v) > 255 and c not in LONG_COLS:
                vals[i] = v[:255]
        batch.append(vals)
        if len(batch) >= 2000:
            cur.executemany(ins, batch)
            n += len(batch)
            batch = []
            if n % 50000 == 0:
                acc.commit()
                print("  %d rows (%.0fs)" % (n, time.time() - t0), flush=True)
    if batch:
        cur.executemany(ins, batch)
        n += len(batch)
    acc.commit()
    print("rows      : %d inserted (%.0fs)" % (n, time.time() - t0))
    want = src.execute("SELECT COUNT(*) FROM review_row").fetchone()[0]
    got = cur.execute("SELECT COUNT(*) FROM identification_row").fetchone()[0]
    if got != want:
        raise SystemExit("row count mismatch: accdb %d != sqlite %d" % (got, want))

    cur.execute("CREATE INDEX ix_ir_ev ON identification_row (evidence_id)")
    cur.execute("CREATE INDEX ix_ir_sys ON identification_row (sys_id)")
    cur.execute("CREATE INDEX ix_ir_work ON identification_row (work_id)")
    cur.execute("CREATE INDEX ix_ir_pool ON identification_row ([pool])")

    # ---- source_file with REAL basenames for the masked corpora ------------
    cur.execute("CREATE TABLE source_file ([id] TEXT(32), [kind] TEXT(16), "
                "[masked] LONG, [ref_id] TEXT(255), [filename] LONGTEXT)")
    n_named = 0
    for r2 in src.execute("SELECT id, kind, masked, ref_id, display_ref "
                          "FROM source_file"):
        fn = r2["display_ref"]
        if not fn and r2["ref_id"] in basenames:
            fn = basenames[r2["ref_id"]]
            n_named += 1
        cur.execute("INSERT INTO source_file VALUES (?,?,?,?,?)",
                    (r2["id"], r2["kind"], r2["masked"], r2["ref_id"], fn))
    cur.execute("CREATE TABLE reference_witness ([witness_id] TEXT(32), "
                "[work_id] TEXT(32), [raw_id] TEXT(255), "
                "[source_file_id] TEXT(32))")
    for r2 in src.execute("SELECT witness_id, work_id, raw_id, source_file_id "
                          "FROM reference_witness"):
        cur.execute("INSERT INTO reference_witness VALUES (?,?,?,?)", tuple(r2))

    cur.execute("CREATE TABLE meta ([meta_key] TEXT(255), [meta_value] LONGTEXT)")
    for k, v in src.execute("SELECT key, value FROM meta WHERE key LIKE 'doc.%' "
                            "OR key LIKE 'rsource_%' OR key IN "
                            "('schema','rows','merged_at')"):
        cur.execute("INSERT INTO meta VALUES (?,?)", (k, str(v)))
    cur.execute("INSERT INTO meta VALUES (?,?)", (
        "export.note",
        "Index-only export of discovery-v5-REVIEW.db (no text columns -- the "
        "text files travel separately). Offsets are CHARACTER positions in "
        "the NFC-normalized text of the named file, 0-based, end exclusive. "
        "source_file.filename for masked corpora was included by owner "
        "authorization of 2026-08-30 for a reviewer who already holds the "
        "files."))
    acc.commit()
    acc.close()
    src.close()
    print("source_file: %d masked entries got real basenames" % n_named)
    print("wrote     : %s (%.0f MB)" % (args.out, os.path.getsize(args.out) / 1e6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
