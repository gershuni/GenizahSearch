# -*- coding: utf-8 -*-
"""Verify the review DB's character offsets against the real source files.

INDEPENDENCE IS THE POINT. This tool deliberately does NOT import the builder's
mapping machinery -- not `sub_offset_preserving`, not
`regen_stream_with_offsets`, not `clean_m_body_with_offsets`, not `seg3`. If it
called them, a bug in the map would confirm itself. Instead it recomputes each
work's letter stream and its character positions with a DIFFERENT algorithm:

    the builder    composes per-stage offset maps by chaining substitutions;
    this oracle    marks every character the cleanup removes in a boolean mask
                   and then walks the text once, keeping Hebrew letters.

Two implementations that agree on where 250,000 spans live are evidence; one
implementation checking itself is not. `test_verify_v3_review_offsets.py` proves
the check can fail, by mutating a stored offset onto an adjacent RETAINED
Hebrew letter (a +/-1 nudge into stripped whitespace would pass either way) and
onto a genuinely wrong duplicate locus, and asserting this PROCESS exits
non-zero.

WHAT IS CHECKED, per row with `ref_provenance_status='ok'`:
  * the oracle's own stream[w_start:w_end] equals the letters of `ref_match`
  * the oracle's own character position for w_start equals `ref_char_start`
  * likewise for the end
and per row with `ms_provenance_status='ok'`:
  * the corpus file's slice [file_char_start:file_char_end) equals
    the page text's slice [page_char_start:page_char_end)

Run:
    python -X utf8 -u scripts/verify_v3_review_offsets.py \
        --db discovery_data/discovery-v5-REVIEW.db \
        --sourcekeys %USERPROFILE%/.genizah-private/sourcekeys.json \
        [--sample 200 | --all] [--max-status-fail 0]
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import sys
import unicodedata
from array import array

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# The oracle's OWN normalization. Restated on purpose (it is the thing under
# test): base Hebrew letters only, finals folded, everything else dropped.
# ---------------------------------------------------------------------------
_FINALS = {"ך": "כ", "ם": "מ", "ן": "נ",
           "ף": "פ", "ץ": "צ"}
_LO, _HI = 0x05D0, 0x05EA


def oracle_stream(text: str, cut_spans=()):
    """(stream, positions) computed by MASKING removed regions, not by
    concatenating surviving chunks. `positions[i]` is the index in `text` of
    the stream's i-th letter."""
    cut = bytearray(len(text))
    for a, b in cut_spans:
        for k in range(max(0, a), min(len(text), b)):
            cut[k] = 1
    out, pos = [], array("i")
    for i, ch in enumerate(text):
        if cut[i]:
            continue
        c = _FINALS.get(ch, ch)
        if _LO <= ord(c) <= _HI:
            out.append(c)
            pos.append(i)
    return "".join(out), pos


def letters_only(text: str) -> str:
    return oracle_stream(unicodedata.normalize("NFC", text))[0]


# ---------------------------------------------------------------------------
# Per-kind cleanup, expressed as SPANS TO CUT -- again a different shape from
# the builder's substitute-and-chain. The patterns themselves must of course be
# the same ones; a check that used different patterns would be testing nothing.
# ---------------------------------------------------------------------------
M_HEADER = re.compile(r"##(?:[^#\n]|#(?!#))*##")
RS_HDR = re.compile(r"##[^#]*##|###.*$", re.M)
RS_EDMARK = re.compile(r"\+[^+]*\+")
RS_RAAVAD = re.compile(r"\+\s*/[^/]*הראב[^/]*/\s*([^+]*)\+")


def cut_spans_for(kind: str, nfc: str, raw_id: str, rs_meta):
    """The regions this corpus's cleanup removes, as (start, end) spans."""
    if kind == "M":
        return [m.span() for m in M_HEADER.finditer(nfc)]
    if kind != "RS":
        return []
    spans = []
    keep_gloss, section_drop = rs_meta
    # The policy constants are keyed by the CATALOGUE id ('R:11.0'), while
    # evidence rows carry the REFERENCE id ('RS:11.0'). Looking up the wrong
    # form silently skips the per-work rules -- which is not a small miss: for
    # the one work with a kept gloss it drops 380,000 letters, moving every
    # offset after the first gloss and failing 13,924 rows that were correct.
    raw_id = 'R:' + raw_id[3:] if raw_id.startswith('RS:') else raw_id
    if raw_id in keep_gloss:
        # label + delimiters go, the gloss body stays: cut around group(1).
        for m in RS_RAAVAD.finditer(nfc):
            spans.append((m.start(), m.start(1)))
            spans.append((m.end(1), m.end()))
    drop_labels, work_title = section_drop.get(raw_id, (None, None))
    if drop_labels:
        # Whole `###`-headed blocks whose header names a dropped layer -- but
        # the label is matched against the header AFTER the work's own title is
        # removed. That detail is load-bearing here: this work is titled
        # "... עם גידולי תרומה", so its title CONTAINS the drop label, and
        # matching the raw header would drop every section and leave an empty
        # stream (observed: 310 rows failed against a zero-length oracle).
        parts = list(re.finditer(r"^###.*$", nfc, re.M))
        for i, m in enumerate(parts):
            end = parts[i + 1].start() if i + 1 < len(parts) else len(nfc)
            hdr = m.group(0)
            tail = hdr.split(work_title, 1)[-1] if (work_title and
                                                    work_title in hdr) else hdr
            if any(lbl in tail for lbl in drop_labels):
                spans.append((m.start(), end))
    for m in RS_HDR.finditer(nfc):
        spans.append(m.span())
    for m in RS_EDMARK.finditer(nfc):
        # a kept gloss must not be re-cut by the general apparatus rule
        if raw_id in keep_gloss and RS_RAAVAD.match(nfc, m.start()):
            continue
        spans.append(m.span())
    return spans


def read_source(kind: str, path: str) -> str:
    errors = "strict" if kind == "RS" else "replace"
    try:
        return open(path, encoding="utf-8", errors=errors).read()
    except OSError:
        return open("\\\\?\\" + os.path.abspath(path).replace("/", "\\"),
                    encoding="utf-8", errors=errors).read()


def load_v4json(path: str) -> str:
    doc = json.load(open(path, encoding="utf-8"))
    return "".join(u.get("text") or "" for u in (doc.get("units") or []))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--sourcekeys", default=None,
                    help="id->path map for the restricted corpora "
                         "(never inside the repo)")
    ap.add_argument("--corpus-file", default=os.path.join(REPO_ROOT,
                                                          "Transcriptions.txt"))
    ap.add_argument("--staging", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "refs_staging"))
    ap.add_argument("--sample", type=int, default=200,
                    help="rows per source kind (ignored with --all)")
    ap.add_argument("--all", action="store_true",
                    help="every row of every TRANSFORMED kind (M, RS): the "
                         "final-mode requirement, since those are the kinds "
                         "whose offsets pass through a cleanup")
    ap.add_argument("--ms-sample", type=int, default=300)
    ap.add_argument("--seed", type=int, default=17)
    ap.add_argument("--max-status-fail", type=int, default=None,
                    help="fail if more than this many rows carry a NON-ok "
                         "provenance status (final mode: 0)")
    args = ap.parse_args(argv)

    keys = {}
    if args.sourcekeys and os.path.exists(args.sourcekeys):
        keys = json.load(open(args.sourcekeys, encoding="utf-8"))
        print("source keys: %d" % len(keys))

    # R-source policy constants, read from the cleaner's MODULE TEXT rather than
    # imported -- the whole point is not to execute the code under test. If they
    # are unreadable, RS rows are reported as unverifiable instead of assumed.
    keep_gloss, section_drop = set(), {}
    rs_mod = os.path.join(REPO_ROOT, "same_work_spike", "probe", "rsource",
                          "scripts", "gen2_clean_streams.py")
    if os.path.exists(rs_mod):
        src = open(rs_mod, encoding="utf-8").read()
        m = re.search(r"KEEP_GLOSS\s*=\s*\{([^}]*)\}", src)
        if m:
            keep_gloss = set(re.findall(r"'([^']+)'", m.group(1)))
        m = re.search(r"SECTION_DROP\s*=\s*\{(.*?)\n\}", src, re.S)
        if m:
            # The work TITLE is needed too (see cut_spans_for). It is read from
            # the catalogue DATA file, never from the module under test.
            cat_titles = {}
            cat_path = os.path.join(REPO_ROOT, "same_work_spike", "probe",
                                    "rsource", "data", "r_catalog.jsonl")
            if os.path.exists(cat_path):
                for cl in open(cat_path, encoding="utf-8"):
                    cl = cl.strip()
                    if cl:
                        cr = json.loads(cl)
                        cat_titles[cr.get("rid")] = cr.get("title")
            for line in m.group(1).splitlines():
                mm = re.match(r"\s*'([^']+)':\s*\((.*?)\),", line)
                if mm:
                    section_drop[mm.group(1)] = (
                        tuple(re.findall(r"'([^']+)'", mm.group(2))),
                        cat_titles.get(mm.group(1)))
    rs_meta = (keep_gloss, section_drop)

    con = sqlite3.connect("file:%s?mode=ro" % args.db, uri=True)
    con.row_factory = sqlite3.Row

    # ---- status budget -------------------------------------------------
    fails = 0
    status_bad = 0
    for col in ("ms_provenance_status", "ref_provenance_status"):
        rows = con.execute("SELECT %s AS s, COUNT(*) c FROM review_row "
                           "GROUP BY 1 ORDER BY 2 DESC" % col).fetchall()
        print("\n%s:" % col)
        for r in rows:
            print("   %-20s %d" % (r["s"], r["c"]))
            if r["s"] != "ok":
                status_bad += r["c"]

    # ---- reference side ------------------------------------------------
    kinds = con.execute("""
        SELECT sf.kind, sf.masked, sf.ref_id, sf.display_ref, rw.witness_id,
               rw.raw_id, rw.w_shift, COUNT(*) n
        FROM review_row r
        JOIN reference_witness rw ON rw.witness_id = r.witness_id
        JOIN source_file sf ON sf.id = rw.source_file_id
        WHERE r.ref_provenance_status = 'ok'
        GROUP BY rw.witness_id ORDER BY n DESC""").fetchall()
    print("\nwitnesses with ok rows: %d" % len(kinds))

    random.seed(args.seed)
    checked = ok = 0
    per_kind = {}
    for w in kinds:
        kind, raw_id = w["kind"], w["raw_id"]
        transformed = kind in ("M", "RS")
        if args.all and not transformed:
            continue
        # resolve the file
        if w["masked"]:
            path = keys.get(w["ref_id"])
            if not path:
                print("  ! no key for masked source %s -- cannot verify"
                      % w["ref_id"])
                fails += 1
                continue
        elif kind == "REF2" or kind == "R":
            path = os.path.join(args.staging, w["display_ref"] or "")
        else:
            path = w["display_ref"]
            if path and not os.path.isabs(path):
                # J / V4JSON: the basename alone; look beside known roots
                for root in (os.environ.get("V3_REVIEW_JA_DIR", ""),
                             os.path.join(REPO_ROOT, "discovery_builds")):
                    if not root:
                        continue
                    cand = os.path.join(root, path)
                    if os.path.exists(cand):
                        path = cand
                        break
        # ATTEMPT THE OPEN rather than trusting os.path.exists: many M-source
        # filenames begin with dots and a space, which Win32 normalizes away on
        # a plain path but `exists` reports as absent. Trusting `exists` here
        # made one witness look like a missing file when it reads fine.
        if not path:
            print("  ~ skip %s (%s): no path recorded" % (raw_id, kind))
            continue
        try:
            raw = (load_v4json(path) if kind == "V4JSON"
                   else read_source(kind, path))
        except OSError as exc:
            print("  ~ skip %s (%s): unreadable (%s)" % (raw_id, kind, exc))
            continue
        nfc = unicodedata.normalize("NFC", raw)
        if len(nfc) != len(raw):
            print("  ! NFC changes length for %s -- rows should be nfc_shift"
                  % raw_id)
            fails += 1
            continue
        stream, pos = oracle_stream(nfc, cut_spans_for(kind, nfc, raw_id,
                                                       rs_meta))

        q = ("SELECT evidence_id, w_start, w_end, ref_char_start, ref_char_end,"
             " ref_match FROM review_row WHERE witness_id=? "
             "AND ref_provenance_status='ok'")
        rows = con.execute(q, (w["witness_id"],)).fetchall()
        if not args.all and len(rows) > args.sample:
            rows = random.sample(list(rows), args.sample)
        shift = w["w_shift"] or 0
        for r in rows:
            checked += 1
            a, b = (r["w_start"] or 0) + shift, (r["w_end"] or 0) + shift
            bad = None
            if not (0 <= a < b <= len(stream)):
                bad = "span outside oracle stream (len %d)" % len(stream)
            elif stream[a:b] != letters_only(r["ref_match"] or ""):
                bad = "letters differ from ref_match"
            elif pos[a] != r["ref_char_start"]:
                bad = "start %s != oracle %s" % (r["ref_char_start"], pos[a])
            elif pos[b - 1] + 1 != r["ref_char_end"]:
                bad = "end %s != oracle %s" % (r["ref_char_end"], pos[b - 1] + 1)
            if bad:
                fails += 1
                if fails <= 15:
                    print("  FAIL %s [%s %s]: %s"
                          % (r["evidence_id"][:16], kind, raw_id, bad))
            else:
                ok += 1
                per_kind[kind] = per_kind.get(kind, 0) + 1

    print("\nreference side: %d checked, %d ok, per kind %s"
          % (checked, ok, per_kind))

    # ---- manuscript side ----------------------------------------------
    ms_ok = ms_bad = 0
    if os.path.exists(args.corpus_file):
        rows = con.execute(
            "SELECT page_id, page_char_start, page_char_end, file_char_start,"
            " file_char_end, ms_match FROM review_row "
            "WHERE ms_provenance_status='ok' AND file_char_start IS NOT NULL"
        ).fetchall()
        ms_far = 0
        if rows:
            pick = (list(rows) if args.all
                    else random.sample(list(rows),
                                       min(args.ms_sample, len(rows))))
            pick.sort(key=lambda r: r["file_char_start"])
            # ONE forward pass, but overlapping rows are VERIFIED, not skipped.
            # Many rows share a page (several works matching the same folio --
            # common since the R-source merge), so their file spans overlap; a
            # `continue` there silently exempted every such row, and a mutation
            # landing on one went undetected. A tail buffer of what was just
            # read lets an overlapping row be sliced without seeking (character
            # offsets cannot be seeked to in a text-mode file).
            BUF_KEEP = 4 << 20              # tail chars retained; pages are KBs
            with open(args.corpus_file, encoding="utf-8",
                      errors="replace") as fh:
                buf, buf_start = "", 0      # buf covers [buf_start, cur)
                cur = 0
                for r in pick:
                    a, b = r["file_char_start"], r["file_char_end"]
                    if b > cur:             # extend the buffer forward to b
                        need = b - cur
                        while need > 0:
                            chunk = fh.read(min(need, 1 << 20))
                            if not chunk:
                                break
                            buf += chunk
                            cur += len(chunk)
                            need -= len(chunk)
                        if len(buf) > BUF_KEEP:
                            drop = len(buf) - BUF_KEEP
                            buf = buf[drop:]
                            buf_start += drop
                    if a < buf_start:
                        # further back than the retained tail: COUNTED, never
                        # silent -- and it fails the run, because an unverified
                        # row is not a verified one.
                        ms_far += 1
                        fails += 1
                        if ms_far <= 10:
                            print("  MS UNREACHABLE %s: span [%d,%d) precedes "
                                  "the buffer (starts %d)"
                                  % (r["page_id"], a, b, buf_start))
                        continue
                    span = buf[a - buf_start:b - buf_start]
                    if span == (r["ms_match"] or ""):
                        ms_ok += 1
                    else:
                        ms_bad += 1
                        fails += 1
                        if ms_bad <= 10:
                            print("  MS FAIL %s: file slice != ms_match"
                                  % r["page_id"])
        print("manuscript side: %d ok, %d bad, %d beyond the buffer"
              % (ms_ok, ms_bad, ms_far))
    else:
        print("manuscript side: corpus file absent, skipped")
    con.close()

    if args.max_status_fail is not None and status_bad > args.max_status_fail:
        print("\n!!! %d rows carry a non-ok provenance status (budget %d)"
              % (status_bad, args.max_status_fail))
        fails += 1

    print("\nTOTAL FAILURES: %d" % fails)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
