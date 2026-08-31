# -*- coding: utf-8 -*-
"""Index every page's byte-... no: CHARACTER span inside `Transcriptions.txt`.

WHY. The review DB shows a matched passage as text. A reader who wants to go
back to the source needs to be told WHERE it is, and for the manuscript side the
source is one 1.47 GB flat file of `==> {page_id} <==` records. This pass records
each page's character span in that file once, so every review row can carry
`file_char_start/end` instead of "somewhere in Transcriptions.txt".

THE COORDINATE CONTRACT, which must never drift:

    open(SRC, encoding='utf-8', errors='replace')      # Python TEXT mode,
                                                       # universal newlines ON

Offsets are CHARACTER indices into the stream that decode yields -- not bytes.
Bytes would be wrong, provably: `errors='replace'` turns an invalid 1-4 byte run
into ONE U+FFFD that re-encodes to 3 bytes, so any byte offset derived from the
decoded text drifts after the first replacement. Under universal newlines every
line break in the decoded stream is exactly one '\n', which is what makes the
record body reconstructible (see below).

RECONSTRUCTION INVARIANT. `extract_full.py` builds a page's text as
`"\n".join(line.rstrip('\n') for line in body_lines).strip()`. Since `rstrip`
removes exactly the one delimiter `"\n".join` puts back, the joined body equals
the file's own `[body_start, body_end)` slice character for character -- so the
reconstruction IS the comparison, and this pass never has to re-read the file to
verify itself.

WHAT IS VERIFIED, and what a failure means. Every page in the corpus DB whose
`provenance` is 'htr' MUST reproduce exactly; those pages came from this file.
Pages marked 'fgp' or 'pgp' (18,982 of 667,411 in fullcorpus_v2.db) have text
from a DIFFERENT source, so they are expected not to match and are recorded with
no file span at all rather than a guessed one. A mismatching 'htr' page, or an
'htr' page this pass never saw, is a hard failure: exit non-zero, name the
page_ids. Never a silent `continue` -- a bare one is how 30% of an earlier
append vanished at exit 0.

Run:
    python -X utf8 -u scripts/index_transcriptions_offsets.py \
        --corpus same_work_spike/probe/data/fullcorpus_v2.db \
        --out same_work_spike/probe/data/transcriptions_index.db
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC = os.path.join(REPO_ROOT, "Transcriptions.txt")
DEFAULT_CORPUS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                              "fullcorpus_v2.db")
DEFAULT_OUT = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                           "transcriptions_index.db")

HEADER_RE = re.compile(r"^==> (\S+) <==")

# The decode contract, stored verbatim in the output so a future reimplementation
# trips over an explicit string instead of silently choosing other options.
DECODE_CONTRACT = ("open(path, encoding='utf-8', errors='replace') -- Python "
                   "text mode, universal newlines (newline=None). Offsets are "
                   "CHARACTER indices into that decoded stream.")

SCHEMA = """
CREATE TABLE page_offsets (
  page_id         TEXT PRIMARY KEY,
  file_char_start INTEGER NOT NULL,   -- inclusive, into the decoded stream
  file_char_end   INTEGER NOT NULL,   -- exclusive
  n_chars         INTEGER NOT NULL    -- == end - start, == len(pages.text)
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", default=DEFAULT_SRC)
    ap.add_argument("--corpus", default=DEFAULT_CORPUS)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--report-max", type=int, default=20,
                    help="how many offending page_ids to print per failure class")
    args = ap.parse_args(argv)

    t0 = time.time()

    def log(m):
        print("[%6.0fs] %s" % (time.time() - t0, m), flush=True)

    # ---- what the corpus DB expects of us ---------------------------------
    corpus = sqlite3.connect("file:%s?mode=ro" % args.corpus, uri=True)
    cols = {c[1] for c in corpus.execute("PRAGMA table_info(pages)")}
    has_prov = "provenance" in cols
    if has_prov:
        want = {pid: (txt, prov) for pid, txt, prov in corpus.execute(
            "SELECT page_id, text, provenance FROM pages")}
    else:
        want = {pid: (txt, "htr") for pid, txt in corpus.execute(
            "SELECT page_id, text FROM pages")}
    corpus.close()
    n_htr = sum(1 for _, p in want.values() if p == "htr")
    log("corpus pages: %d (provenance column: %s; htr %d, other %d)"
        % (len(want), has_prov, n_htr, len(want) - n_htr))

    # ---- one streaming pass ----------------------------------------------
    if os.path.exists(args.out):
        os.remove(args.out)
    out = sqlite3.connect(args.out)
    out.executescript(SCHEMA)

    seen_htr_ok = set()
    mismatched = []          # htr page, text differs from the file slice
    other_prov_seen = 0      # fgp/pgp pages that DO appear in the file
    n_records = 0
    rows = []
    pos = 0                  # characters consumed so far
    cur_id = None
    body_start = 0
    buf = []

    def flush():
        nonlocal n_records
        if cur_id is None:
            return
        n_records += 1
        joined = "\n".join(buf)
        lead = len(joined) - len(joined.lstrip())
        stripped = joined.strip()
        start = body_start + lead
        end = start + len(stripped)
        rec = want.get(cur_id)
        if rec is None:
            return                      # dropped by stage-0; not our business
        text, prov = rec
        if prov != "htr":
            nonlocal other_prov_seen
            other_prov_seen += 1
            return                      # text came from elsewhere: no span
        if stripped != text:
            if len(mismatched) < 10_000:
                mismatched.append(cur_id)
            return
        seen_htr_ok.add(cur_id)
        rows.append((cur_id, start, end, len(stripped)))

    with open(args.src, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = HEADER_RE.match(line)
            if m:
                flush()
                if len(rows) >= 20_000:
                    out.executemany("INSERT OR REPLACE INTO page_offsets "
                                    "VALUES (?,?,?,?)", rows)
                    out.commit()
                    rows = []
                    log("  indexed %d pages (%d records streamed)"
                        % (len(seen_htr_ok), n_records))
                cur_id = m.group(1)
                pos += len(line)
                body_start = pos
                buf = []
            else:
                buf.append(line.rstrip("\n"))
                pos += len(line)
        flush()
    if rows:
        out.executemany("INSERT OR REPLACE INTO page_offsets VALUES (?,?,?,?)",
                        rows)
    out.commit()

    missing = [pid for pid, (_, prov) in want.items()
               if prov == "htr" and pid not in seen_htr_ok]
    log("records streamed      : %d" % n_records)
    log("htr pages indexed     : %d of %d" % (len(seen_htr_ok), n_htr))
    log("non-htr pages skipped : %d (no file span by design)" % other_prov_seen)

    for k, v in (
        ("schema", "transcriptions-index/1"),
        ("decode_contract", DECODE_CONTRACT),
        ("src", os.path.basename(args.src)),
        ("src_size_bytes", str(os.path.getsize(args.src))),
        ("src_mtime", str(int(os.path.getmtime(args.src)))),
        ("corpus_db", os.path.basename(args.corpus)),
        ("corpus_pages", str(len(want))),
        ("corpus_htr_pages", str(n_htr)),
        ("records_streamed", str(n_records)),
        ("pages_indexed", str(len(seen_htr_ok))),
        ("pages_skipped_other_provenance", str(other_prov_seen)),
        ("pages_mismatched", str(len(mismatched))),
        ("pages_missing", str(len(missing))),
        ("built_at", time.strftime("%Y-%m-%d %H:%M:%S")),
    ):
        out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    out.commit()
    n_idx = out.execute("SELECT COUNT(*) FROM page_offsets").fetchone()[0]
    out.close()

    # ---- FAIL LOUD -------------------------------------------------------
    rc = 0
    if mismatched:
        rc = 1
        print("\n!!! %d 'htr' pages do NOT match their file slice. The "
              "reconstruction contract is broken -- offsets would be wrong. "
              "First %d:" % (len(mismatched), min(args.report_max,
                                                  len(mismatched))))
        for pid in mismatched[:args.report_max]:
            print("    %s" % pid)
    if missing:
        rc = 1
        print("\n!!! %d 'htr' pages were never seen in %s. Either the file "
              "changed or the header grammar drifted. First %d:"
              % (len(missing), os.path.basename(args.src),
                 min(args.report_max, len(missing))))
        for pid in missing[:args.report_max]:
            print("    %s" % pid)
    if n_idx != len(seen_htr_ok):
        rc = 1
        print("\n!!! wrote %d rows but verified %d pages" % (n_idx,
                                                             len(seen_htr_ok)))

    log("wrote %s (%d rows, %.1f MB) rc=%d"
        % (args.out, n_idx, os.path.getsize(args.out) / 1e6, rc))
    if rc == 0:
        h = hashlib.sha256()
        with open(args.out, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        print("index sha256: %s" % h.hexdigest())
    return rc


if __name__ == "__main__":
    sys.exit(main())
