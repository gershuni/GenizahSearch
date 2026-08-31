# -*- coding: utf-8 -*-
"""Give every R-source review row a citable locus, from its own offsets.

WHY THIS IS POSSIBLE NOW. The v5 artifact stores `ref_char_start` -- the
match's character position in the reference work's own source file (NFC text)
-- and the RS raw files carry dense `###` section headers (work -- tractate --
chapter grade; all 344 files have at least one, 219 have 100+). The locus of a
match is simply the innermost header preceding its position. The production
locus machinery (`shared/discovery_locus.py`) was never built for this corpus;
this replaces nothing of it -- it reads positions this artifact already
carries and was already independently verified.

STATUS VOCABULARY (matches the base corpora's): `resolved` where a header
precedes the match; `whole_work` (label = the row's verified work title) where
the match precedes every header; rows without file offsets
(`ref_provenance_status != 'ok'`, the 278 stream-fallback rows) stay
`not_computed` -- an honest absence, never a guess.

MASKING. The raw file PATHS come from the local key file (outside the repo,
never shipped) and are never written anywhere. Header TEXT becomes the label;
it names works and editions, which this private artifact already displays
verbatim -- but every distinct label is still run through the masking scan
BEFORE anything is written, fail closed, unless --no-masking-scan is passed
explicitly.

Run (review server STOPPED -- this writes into the artifact):
    set MASKING_SCAN_PATTERNS_FILE=...  (the scan refuses to run without it)
    python -X utf8 scripts/attach_rsource_locus.py \
        --db discovery_data/discovery-v5-REVIEW.db \
        --sourcekeys %USERPROFILE%\\.genizah-private\\sourcekeys.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import unicodedata
from bisect import bisect_right

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HEADER_RE = re.compile(r"^###(.*)$", re.M)
LABEL_MAX = 200


def clean_label(header_tail):
    """Header text -> a CITABLE label.

    The raw header is a source-tree breadcrumb: `category -- work -- book --
    chapter -- verse`, `###`-terminated. A citation should not repeat the
    category (the row's domain says it) nor carry the tree's ` -- ` plumbing,
    so: closing marker stripped, the category segment dropped when at least
    work+address remain after it, segments joined with a comma. The WORK
    segment is always kept -- several source files are collections whose
    file-level title names only one member, and the locus is where the true
    sub-work stays visible. Hard cap (rarely hit at 200) so one pathological
    header cannot flood a row."""
    s = " ".join((header_tail or "").split())
    s = s.strip("# ").strip()
    parts = [p.strip() for p in s.split(" -- ") if p.strip()]
    if len(parts) >= 3:
        parts = parts[1:]          # drop the category; keep work + address
    s = ", ".join(parts)
    return s[:LABEL_MAX].rstrip() + ("…" if len(s) > LABEL_MAX else "")


def index_headers(nfc_text):
    """[(char_pos_of_line_start, label), ...] ascending, over the NFC text --
    the SAME coordinate space `ref_char_start` was stored (and verified) in."""
    return [(m.start(), clean_label(m.group(1))) for m in
            HEADER_RE.finditer(nfc_text)]


def locus_for(positions, labels, char_start):
    """Label of the innermost header at or before char_start, else None."""
    i = bisect_right(positions, char_start) - 1
    return labels[i] if i >= 0 else None


def load_file(path):
    # errors='strict' -- the same decode policy the RS offset chain used;
    # a file that no longer round-trips must fail loud, not shift silently.
    with open(path, encoding="utf-8", errors="strict") as f:
        return unicodedata.normalize("NFC", f.read())


def masking_scan_labels(labels, say=print):
    """Every distinct label through the masking scan, fail closed."""
    fd, tmp = tempfile.mkstemp(suffix=".txt", prefix="locus_labels_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(labels)))
        r = subprocess.run(
            [sys.executable, "-X", "utf8",
             os.path.join(REPO_ROOT, "scripts", "check_atlas_masking.py"),
             "--scan-asset", tmp],
            capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(
                "masking scan of %d locus labels FAILED (exit %d) -- nothing "
                "was written.\n%s" % (len(labels), r.returncode,
                                      (r.stdout or "") + (r.stderr or "")))
        say("masking   : %d distinct labels scanned clean" % len(labels))
    finally:
        os.unlink(tmp)


def compute(db_path, keys, say=print):
    """-> (updates, counters): updates = [(label, status, evidence_id)]."""
    con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    con.row_factory = sqlite3.Row
    per_file = {}   # ref_id -> (positions, labels)
    t0 = time.time()
    n = 0
    updates, counts = [], {"resolved": 0, "whole_work": 0, "not_computed": 0}
    cur = con.execute(
        "SELECT r.evidence_id, r.ref_char_start, r.ref_provenance_status, "
        "r.work_title, sf.ref_id "
        "FROM review_row r "
        "LEFT JOIN reference_witness rw ON rw.witness_id = r.witness_id "
        "LEFT JOIN source_file sf ON sf.id = rw.source_file_id "
        "WHERE r.source_corpus='rsource'")
    for r in cur:
        n += 1
        if n % 100000 == 0:
            say("  %d rows placed (%.0fs)" % (n, time.time() - t0))
        if r["ref_provenance_status"] != "ok" or r["ref_char_start"] is None \
                or not r["ref_id"]:
            counts["not_computed"] += 1
            continue                      # stays exactly as it is
        rid = r["ref_id"]
        if rid not in per_file:
            if rid not in keys:
                raise SystemExit("key file has no path for %s -- refusing to "
                                 "guess" % rid)
            hs = index_headers(load_file(keys[rid]))
            per_file[rid] = ([p for p, _ in hs], [l for _, l in hs])
        lab = locus_for(per_file[rid][0], per_file[rid][1],
                        r["ref_char_start"])
        if lab:
            updates.append((lab, "resolved", r["evidence_id"]))
            counts["resolved"] += 1
        else:
            # before the first header: the base corpora's whole_work shape --
            # the label is the work's (verified) title, never a guess of place.
            updates.append((r["work_title"] or "", "whole_work",
                            r["evidence_id"]))
            counts["whole_work"] += 1
    con.close()
    say("placed    : %s over %d files" % (counts, len(per_file)))
    return updates, counts


def attach(db_path, sourcekeys_path, masking=True, say=print):
    keys = json.load(open(sourcekeys_path, encoding="utf-8"))
    updates, counts = compute(db_path, keys, say)
    if masking:
        masking_scan_labels({u[0] for u in updates}, say)
    else:
        say("masking   : SKIPPED by explicit flag")
    con = sqlite3.connect(db_path)
    try:
        con.execute("BEGIN")
        con.executemany(
            "UPDATE review_row SET locus_label=?, locus_status=? "
            "WHERE evidence_id=?", updates)
        got = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE source_corpus='rsource' "
            "AND locus_status='resolved'").fetchone()[0]
        if got != counts["resolved"]:
            raise RuntimeError("wrote %d resolved rows but computed %d"
                               % (got, counts["resolved"]))
        for k, v in (("rsource_locus.version", "1"),
                     ("rsource_locus.method",
                      "innermost preceding ### header by ref_char_start (NFC)"),
                     ("rsource_locus.counts", json.dumps(counts)),
                     ("rsource_locus.built_at",
                      time.strftime("%Y-%m-%d %H:%M:%S"))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    say("locus     : %d resolved, %d whole_work, %d not_computed"
        % (counts["resolved"], counts["whole_work"], counts["not_computed"]))
    return counts


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--sourcekeys", default=os.path.join(
        os.path.expanduser("~"), ".genizah-private", "sourcekeys.json"))
    ap.add_argument("--no-masking-scan", dest="masking", action="store_false",
                    help="skip the pre-write masking scan of the labels "
                         "(the scan otherwise REQUIRES "
                         "MASKING_SCAN_PATTERNS_FILE and fails closed)")
    args = ap.parse_args(argv)
    for p in (args.db, args.sourcekeys):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)
    attach(args.db, args.sourcekeys, masking=args.masking)
    return 0


if __name__ == "__main__":
    sys.exit(main())
