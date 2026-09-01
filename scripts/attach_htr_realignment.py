# -*- coding: utf-8 -*-
"""Give the FGP/PGP-substituted review rows an address in Transcriptions.txt,
by alignment, and store the HTR text of every substituted page.

WHY. 18,982 of the corpus's 667,411 pages were searched as a HUMAN
transcription (FGP or PGP) that a gate judged fuller and more accurate than the
HTR (same_work_spike/probe/scripts/mapv2_stage0.py, owner ruling 2026-07-10).
The HTR text of every one of those pages still stands in Transcriptions.txt; it
was replaced as search text, never deleted. The review rows on those pages
(26,180 of 519,382 at the time of writing) therefore carry page offsets into a
text the reader does not hold, and `file_char_*` NULL
(`ms_provenance_status='offsets_missing'`). This pass locates each matched span
in the HTR text of the SAME page and records where it landed, with the
alignment score and a status that says how far to trust it. The FGP-based
offsets already on the row are left exactly as they were: two addresses, each
into the text it belongs to, never one overwriting the other.

FIVE STATUSES, never a bare number (`htr_align_status`):
  exact              the matched letters occur verbatim in the HTR page, once
  realigned_htr      best alignment score >= REALIGNED_MIN (90): trust it
  realign_uncertain  below that -- the best window, shown with its score; on
                     inspection the HTR is noisy here, not a different text
  ambiguous          the letters occur more than once in the page: no offset,
                     because picking the first would be a guess
  unalignable        the matched span is too short to locate (< 10 letters)

`htr_file_char_*` are NULL on a page whose NFC form differs from its raw form
(`htr_page.nfc_ok = 0`): the page offsets are right in NFC space, but adding
them to a raw-file base would mix two coordinate systems -- the same rule the
builder applies to its own `file_char_*`.

WHAT IS REFUSED (exit non-zero, nothing written):
  * a Transcriptions.txt whose size, mtime or record count differs from the
    offsets index that every other file address in the db was computed from;
  * a substituted page missing from the file, or whose captured HTR text
    length differs from the corpus's own `htr_n_chars`;
  * a sampled HTR page whose recomputed span disagrees with `page_offsets`;
  * an `offsets_missing` row on a page the corpus does not call substituted;
  * an alignment window whose letters do not reproduce the aligner's own score
    (a content check, independent of the offset arithmetic), or a stored
    address that does not slice back to those letters;
  * a status that contradicts the stored score (decided on the same rounded
    number the reader sees);
  * count drift after the write: rows stamped != offsets_missing rows, pages
    stored != substituted pages, a stamp on a row that is not offsets_missing.

Run (review server STOPPED -- it holds the db):
    python -X utf8 -u scripts/attach_htr_realignment.py
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPIKE_SCRIPTS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "scripts")
for _p in (REPO_ROOT, SPIKE_SCRIPTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# The matcher's own normalizer (spike module, gitignored tree) -- the same import
# scripts/build_v3_review_db.py makes. Using anything else would put these
# offsets in a different letter space from every other offset in the db.
from normalize import norm_stream  # noqa: E402
from rapidfuzz.fuzz import partial_ratio_alignment, ratio  # noqa: E402

DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")
DEFAULT_SRC = os.path.join(REPO_ROOT, "Transcriptions.txt")
DEFAULT_OFFSETS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                               "transcriptions_index.db")
DEFAULT_CORPUS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                              "fullcorpus_v2.db")

REALIGNED_MIN = 90.0        # partial_ratio score at or above which the address is trusted
MIN_QUERY_LETTERS = 10      # below this a span cannot be located honestly
SELFCHECK_PAGES = 500       # HTR pages re-derived and compared with page_offsets
SCORE_TOLERANCE = 0.5       # recomputed window score must match the aligner's

STATUSES = ("exact", "realigned_htr", "realign_uncertain", "ambiguous",
            "unalignable")

NEW_COLS = (("htr_page_char_start", "INTEGER"),
            ("htr_page_char_end", "INTEGER"),
            ("htr_file_char_start", "INTEGER"),
            ("htr_file_char_end", "INTEGER"),
            ("htr_align_score", "REAL"),
            ("htr_align_status", "TEXT"))

HTR_PAGE_DDL = """CREATE TABLE htr_page(
  page_id TEXT PRIMARY KEY,
  sys_id TEXT,
  search_text_source TEXT NOT NULL,   -- fgp | pgp: what the matcher searched instead
  substitution_score REAL,            -- the gate's alignment score, human text vs HTR
  search_text_n_chars INTEGER,        -- length of the text the matcher searched
  htr_text TEXT NOT NULL,             -- the HTR page as it stands in Transcriptions.txt
  htr_n_chars INTEGER NOT NULL,
  htr_file_char_start INTEGER,        -- NULL only when nfc_ok = 0
  htr_file_char_end INTEGER,
  nfc_ok INTEGER NOT NULL,            -- 1 = NFC form identical to the raw form
  in_review_set INTEGER NOT NULL      -- 1 = at least one review_row sits on this page
)"""

DOC = ("HTR re-alignment: on pages whose search text was a human transcription "
       "(FGP/PGP), htr_page_char_*/htr_file_char_* locate the matched span in "
       "the HTR text of the SAME page in Transcriptions.txt, by alignment. "
       "htr_align_status says how: exact (verbatim, once), realigned_htr "
       "(score >= %g, trust it), realign_uncertain (best window, score shown), "
       "ambiguous (occurs twice, no offset), unalignable (too short). The "
       "FGP-based page_char_* on the same row are untouched. htr_page holds "
       "the HTR text and file address of every substituted page; "
       "htr_file_char_* are NULL where nfc_ok = 0. A re-run never searched "
       "the HTR text: these are the FGP-found spans re-addressed, not new "
       "matches." % REALIGNED_MIN)

HEADER_RE = re.compile(r"^==> (\S+) <==")


class GateError(SystemExit):
    pass


def log(msg):
    print(msg, flush=True)


def _ro(path):
    return sqlite3.connect("file:%s?mode=ro" % path, uri=True)


def stream_pages(src, want):
    """page_id -> (text, file_char_start, file_char_end) for every wanted page,
    plus the record count, under the offsets index's decode contract: Python
    text mode, utf-8, errors=replace, universal newlines; offsets in CHARACTERS
    of that stream. Reconstruction is `"\\n".join(stripped lines).strip()`, so
    the text IS the file slice and the two never have to be compared again."""
    pages = {}
    state = {"cur": None, "body": [], "start": 0, "n": 0}

    def flush():
        cur = state["cur"]
        if cur is None:
            return
        state["n"] += 1
        if cur in want:
            joined = "\n".join(l.rstrip("\n") for l in state["body"])
            lead = len(joined) - len(joined.lstrip())
            s = joined.strip()
            a = state["start"] + lead
            pages[cur] = (s, a, a + len(s))
        state["cur"] = None
        state["body"] = []

    pos = 0
    with open(src, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = HEADER_RE.match(line)
            if m:
                flush()
                state["cur"] = m.group(1)
                state["start"] = pos + len(line)
            else:
                state["body"].append(line)
            pos += len(line)
        flush()
    return pages, state["n"]


def align_row(q, h, hoffs, realigned_min=REALIGNED_MIN):
    """(status, score, page_start, page_end, letters) for the matched letter
    string `q` against the HTR page stream `h`. Offsets are NFC page characters,
    end exclusive; `letters` is the stream window the offsets must slice back
    to, so the caller can verify rather than trust."""
    if len(q) < MIN_QUERY_LETTERS or not h:
        return ("unalignable", None, None, None, None)
    i = h.find(q)
    if i >= 0:
        if h.find(q, i + 1) >= 0:
            return ("ambiguous", 100.0, None, None, None)
        return ("exact", 100.0, hoffs[i], hoffs[i + len(q) - 1] + 1, q)
    al = partial_ratio_alignment(q, h)
    if al is None or al.dest_end <= al.dest_start:
        return ("unalignable", 0.0, None, None, None)
    letters = h[al.dest_start:al.dest_end]
    # A CONTENT check, independent of the offset arithmetic below: the letters
    # cut out of the page must reproduce the aligner's own score against the
    # part of the query it aligned. Re-slicing through hoffs can only prove the
    # arithmetic is self-consistent; an argument-order or window bug would put
    # an in-bounds but unrelated span here, and this is what would catch it.
    again = ratio(q[al.src_start:al.src_end], letters)
    if abs(again - al.score) > SCORE_TOLERANCE:
        raise GateError("alignment window does not reproduce its own score "
                        "(%.2f recomputed vs %.2f reported)" % (again, al.score))
    # The status is decided on the SAME rounded score that is stored and shown:
    # a row must never read "90.0" beside "uncertain" because 89.96 was
    # compared before it was rounded.
    sc = round(float(al.score), 1)
    status = "realigned_htr" if sc >= realigned_min else "realign_uncertain"
    return (status, sc, hoffs[al.dest_start], hoffs[al.dest_end - 1] + 1, letters)


def build(db, src, offsets_db, corpus_db, realigned_min=REALIGNED_MIN,
          selfcheck_pages=SELFCHECK_PAGES):
    t0 = time.time()
    for p in (db, src, offsets_db, corpus_db):
        if not os.path.exists(p):
            raise GateError("missing input: %s" % p)
    for side in ("-journal", "-wal"):
        if os.path.exists(db + side):
            raise GateError("%s%s exists -- an unfinished transaction or a live "
                            "writer; stop it first" % (db, side))

    # ---- 1. the file must be the one every other address was computed from --
    oc = _ro(offsets_db)
    try:
        imeta = dict(oc.execute("SELECT key, value FROM meta"))
        known = {p: (a, b) for p, a, b in oc.execute(
            "SELECT page_id, file_char_start, file_char_end FROM page_offsets "
            "ORDER BY page_id LIMIT ?", (int(selfcheck_pages),))}
    finally:
        oc.close()
    st = os.stat(src)
    if str(st.st_size) != imeta.get("src_size_bytes"):
        raise GateError("Transcriptions.txt size %d != offsets index %s -- the "
                        "file the db's addresses point into has changed"
                        % (st.st_size, imeta.get("src_size_bytes")))
    if str(int(st.st_mtime)) != imeta.get("src_mtime"):
        raise GateError("Transcriptions.txt mtime %d != offsets index %s"
                        % (int(st.st_mtime), imeta.get("src_mtime")))
    if not known:
        raise GateError("offsets index has no page_offsets to self-check against")

    # ---- 2. which pages were substituted, and what the corpus knows of them --
    cc = _ro(corpus_db)
    try:
        subs = {r[0]: r[1:] for r in cc.execute(
            "SELECT page_id, sys_id, provenance, fgp_score, n_chars, htr_n_chars "
            "FROM pages WHERE provenance != 'htr'")}
    finally:
        cc.close()
    if not subs:
        raise GateError("the corpus has no substituted pages")
    bad_prov = sorted({v[1] for v in subs.values()} - {"fgp", "pgp"})
    if bad_prov:
        raise GateError("unknown provenance in the corpus: %s" % bad_prov)

    # ---- 3. the rows to address ------------------------------------------
    con = sqlite3.connect(db, timeout=60)
    con.execute("PRAGMA foreign_keys=ON")
    try:
        rows = con.execute(
            "SELECT evidence_id, page_id, ms_match FROM review_row "
            "WHERE ms_provenance_status='offsets_missing'").fetchall()
        if not rows:
            raise GateError("no offsets_missing rows -- nothing to address")
        review_pages = {r[1] for r in rows}
        stray = sorted(review_pages - set(subs))
        if stray:
            raise GateError("%d offsets_missing rows sit on pages the corpus does "
                            "not call substituted, e.g. %s -- the row status and "
                            "the corpus disagree" % (
                                sum(1 for r in rows if r[1] in stray), stray[:3]))
        log("rows to address: %d on %d pages; substituted pages in corpus: %d"
            % (len(rows), len(review_pages), len(subs)))

        # ---- 4. one streaming pass over the file -------------------------
        want = set(subs) | set(known)
        pages, n_records = stream_pages(src, want)
        log("streamed %s: %d records, %d wanted pages captured (%.0fs)"
            % (os.path.basename(src), n_records, len(pages), time.time() - t0))
        if str(n_records) != imeta.get("records_streamed", str(n_records)):
            raise GateError("record count %d != offsets index %s"
                            % (n_records, imeta.get("records_streamed")))
        bad = [p for p, (a, b) in known.items()
               if p not in pages or (pages[p][1], pages[p][2]) != (a, b)]
        if bad:
            raise GateError("self-check: %d of %d known HTR pages do not reproduce "
                            "their page_offsets span, e.g. %s -- the coordinate "
                            "math has drifted from the index"
                            % (len(bad), len(known), bad[:3]))
        absent = sorted(p for p in subs if p not in pages)
        if absent:
            raise GateError("%d substituted pages are absent from the file, e.g. %s"
                            % (len(absent), absent[:3]))
        drift = sorted(p for p, v in subs.items() if len(pages[p][0]) != v[4])
        if drift:
            raise GateError("%d substituted pages: captured HTR length != corpus "
                            "htr_n_chars, e.g. %s -- not the same text"
                            % (len(drift), drift[:3]))
        log("self-check %d/%d pages exact; every substituted page present with "
            "its recorded length" % (len(known), len(known)))

        # ---- 5. align every row against the HTR text of its page ---------
        cache = {}
        results = []
        counts = Counter()
        n_file_addr = 0
        for eid, pid, ms_match in rows:
            text, fa, _fb = pages[pid]
            if pid not in cache:
                nfc = unicodedata.normalize("NFC", text)
                h, hoffs = norm_stream(nfc)
                cache[pid] = (nfc, h, hoffs, nfc == text)
            nfc, h, hoffs, nfc_ok = cache[pid]
            q = norm_stream(unicodedata.normalize("NFC", ms_match or ""))[0]
            status, score, pa, pb, letters = align_row(q, h, hoffs, realigned_min)
            f_a = f_b = None
            if pa is not None:
                got = norm_stream(nfc[pa:pb])[0]
                if got != letters:
                    raise GateError("row %s: the stored page address does not "
                                    "slice back to the aligned letters" % eid)
                if nfc_ok:
                    f_a, f_b = fa + pa, fa + pb
                    n_file_addr += 1
            counts[status] += 1
            results.append((pa, pb, f_a, f_b, score, status, eid))
        log("aligned %d rows (%.0fs): %s" % (
            len(results), time.time() - t0,
            ", ".join("%s %d" % (k, counts[k]) for k in STATUSES)))

        # ---- 6. write, in one transaction --------------------------------
        have = {r[1] for r in con.execute("PRAGMA table_info(review_row)")}
        con.execute("BEGIN IMMEDIATE")
        for col, typ in NEW_COLS:
            if col not in have:
                con.execute("ALTER TABLE review_row ADD COLUMN %s %s" % (col, typ))
        # a re-run clears only what a previous run stamped; the gate below
        # then proves nothing else ever carried a stamp
        con.execute("UPDATE review_row SET htr_page_char_start=NULL, "
                    "htr_page_char_end=NULL, htr_file_char_start=NULL, "
                    "htr_file_char_end=NULL, htr_align_score=NULL, "
                    "htr_align_status=NULL WHERE htr_align_status IS NOT NULL")
        con.executemany(
            "UPDATE review_row SET htr_page_char_start=?, htr_page_char_end=?, "
            "htr_file_char_start=?, htr_file_char_end=?, htr_align_score=?, "
            "htr_align_status=? WHERE evidence_id=?", results)
        con.execute("DROP TABLE IF EXISTS htr_page")
        con.execute(HTR_PAGE_DDL)
        n_nfc_shift = 0
        page_rows = []
        for pid, (sys_id, prov, score, n_chars, htr_n) in subs.items():
            text, fa, fb = pages[pid]
            nfc_ok = unicodedata.normalize("NFC", text) == text
            if not nfc_ok:
                n_nfc_shift += 1
            page_rows.append((pid, sys_id, prov, score, n_chars, text, len(text),
                              fa if nfc_ok else None, fb if nfc_ok else None,
                              1 if nfc_ok else 0, 1 if pid in review_pages else 0))
        con.executemany("INSERT INTO htr_page VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        page_rows)
        con.execute("CREATE INDEX ix_htr_page_sys ON htr_page(sys_id)")
        meta = {
            "htr_realign.built_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "htr_realign.method": (
                "norm_stream(ms_match) located in norm_stream(NFC(HTR page)): "
                "verbatim find first (exact / ambiguous), else rapidfuzz "
                "partial_ratio_alignment; offsets mapped back through the "
                "stream offsets; every address re-sliced and compared before "
                "the write"),
            "htr_realign.realigned_min": str(realigned_min),
            "htr_realign.min_query_letters": str(MIN_QUERY_LETTERS),
            "htr_realign.rows": str(len(results)),
            "htr_realign.review_pages": str(len(review_pages)),
            "htr_realign.substituted_pages": str(len(subs)),
            "htr_realign.status_counts": json.dumps(
                {k: counts.get(k, 0) for k in STATUSES}),
            "htr_realign.file_addresses": str(n_file_addr),
            "htr_realign.nfc_shift_pages": str(n_nfc_shift),
            "htr_realign.src": os.path.basename(src),
            "htr_realign.src_size_bytes": str(st.st_size),
            "htr_realign.src_mtime": str(int(st.st_mtime)),
            "htr_realign.records_streamed": str(n_records),
            "htr_realign.selfcheck_pages": str(len(known)),
            "doc.htr_realign": DOC,
        }
        for k, v in meta.items():
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))

        # ---- 7. reconcile before publishing ------------------------------
        n_set = con.execute("SELECT COUNT(*) FROM review_row WHERE "
                            "htr_align_status IS NOT NULL").fetchone()[0]
        n_stray = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE htr_align_status IS NOT NULL "
            "AND ms_provenance_status != 'offsets_missing'").fetchone()[0]
        n_pages = con.execute("SELECT COUNT(*) FROM htr_page").fetchone()[0]
        n_addr = con.execute("SELECT COUNT(*) FROM review_row WHERE "
                             "htr_file_char_start IS NOT NULL").fetchone()[0]
        n_badstat = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE htr_align_status IS NOT NULL "
            "AND htr_align_status NOT IN (%s)"
            % ",".join("?" * len(STATUSES)), STATUSES).fetchone()[0]
        # the number a reader sees must be the number the status was decided on
        n_contra = con.execute(
            "SELECT COUNT(*) FROM review_row WHERE "
            "(htr_align_status='realign_uncertain' AND htr_align_score >= ?) OR "
            "(htr_align_status='realigned_htr' AND htr_align_score < ?) OR "
            "(htr_align_status='exact' AND htr_align_score != 100.0)",
            (realigned_min, realigned_min)).fetchone()[0]
        problems = []
        if n_contra:
            problems.append("%d rows whose status contradicts their stored score"
                            % n_contra)
        if n_set != len(rows):
            problems.append("stamped rows %d != offsets_missing rows %d"
                            % (n_set, len(rows)))
        if n_stray:
            problems.append("%d stamps on rows that are not offsets_missing" % n_stray)
        if n_pages != len(subs):
            problems.append("htr_page %d != substituted pages %d" % (n_pages, len(subs)))
        if n_addr != n_file_addr:
            problems.append("file addresses %d != computed %d" % (n_addr, n_file_addr))
        if n_badstat:
            problems.append("%d rows carry a status outside the enum" % n_badstat)
        if problems:
            con.execute("ROLLBACK")
            raise GateError("reconciliation failed; nothing written: "
                            + "; ".join(problems))
        con.execute("COMMIT")
    finally:
        con.close()
    log("wrote %d row addresses (%d with a file address) and %d HTR pages "
        "(%d NFC-shifted) in %.0fs"
        % (len(results), n_file_addr, len(subs), n_nfc_shift, time.time() - t0))
    return dict(counts)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--src", default=DEFAULT_SRC)
    ap.add_argument("--offsets-db", default=DEFAULT_OFFSETS)
    ap.add_argument("--corpus", default=DEFAULT_CORPUS)
    ap.add_argument("--realigned-min", type=float, default=REALIGNED_MIN)
    args = ap.parse_args(argv)
    build(args.db, args.src, args.offsets_db, args.corpus, args.realigned_min)
    return 0


if __name__ == "__main__":
    sys.exit(main())
