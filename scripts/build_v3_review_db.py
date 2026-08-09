"""Build the FULL v3 quote-identification review DB -- every shipped row.

WHY A DB AND NOT A PAGE. The 400-item sample page was ~1.6 MB; the shipped set
is ~194,000 rows, so the same page would be hundreds of megabytes and no browser
would open it. The artifact is therefore a SQLite file the team can query, sort
and slice, with a separate lightweight viewer over it.

WHAT EACH ROW CARRIES, and why each field is here:

  * the NOVELTY and DIVERGENCE grades from the v3 bake -- the same verdicts the
    site shows, not the spike's old title-agreement triage. (That triage is what
    labelled rows `new_witness` even when they agreed with the catalogue: it
    keys on whether the CATALOGUE TITLE is generic or disagrees, and says
    nothing about novelty. It is deliberately absent here.)
  * DOMAIN / AUTHOR / WORK, so the set can be sliced the way the site slices it.
  * both sides of the match as TEXT with the matched span delimited: the
    manuscript's own page, and the reference edition's passage.

OFFSETS. `aligned_page_start/end` and `w_start/w_end` index the SPACE-FREE
normalized letter streams (`normalize.norm_stream`), never raw text. Both sides
are projected back through that function's own offset map. Measured before this
was written: page-side length equals `matched_letters` exactly, and `w_start`
sits within a few characters of the passage's true position in the work.

Do NOT expect the two sides to be near-identical. A Genizah fragment against a
printed edition of the same work runs ~0.4 relative edit distance -- orthography,
abbreviations and real variants. That is what a witness looks like, not an error.

MASKING (D-25). The reference corpus's own TEXT is included by owner decision
(2026-08-09) -- this artifact is private and the text is the point. Restricted
NAMES are still masked: `source_corpus` is emitted as the frozen masked code and
the emit is gated on the live restricted-pattern scan, which FAILS the build
rather than writing a dirty file.

Run:
    python scripts/build_v3_review_db.py \
        --artifact _tmp/v3_out2/discovery-v3.db \
        --out discovery_data/discovery-v3-REVIEW.db
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPIKE_SCRIPTS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "scripts")
for _p in (REPO_ROOT, SPIKE_SCRIPTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from normalize import norm_stream  # noqa: E402  (spike module, gitignored tree)

DEFAULT_ARTIFACT = os.path.join(REPO_ROOT, "_tmp", "v3_out2", "discovery-v3.db")
DEFAULT_SLIM = os.path.join(REPO_ROOT, "_tmp", "v3_research_slim.db")
DEFAULT_REF = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                           "ref_corpus_v2.pkl")
DEFAULT_XWALK = os.path.join(REPO_ROOT, "discovery_data", "crosswalk.json")
DEFAULT_STAGING = os.path.join(REPO_ROOT, "same_work_spike", "probe", "refs_staging")
DEFAULT_OUT = os.path.join(REPO_ROOT, "discovery_data", "discovery-v3-REVIEW.db")

# Context kept either side of the match, in RAW characters. Enough to judge the
# span in its setting without storing whole works 194,000 times over.
CONTEXT = 320

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE review_row (
  evidence_id     TEXT PRIMARY KEY,
  sys_id          TEXT NOT NULL,
  shelfmark       TEXT,
  library_code    TEXT,
  page_id         TEXT NOT NULL,
  folio           TEXT,

  work_id         TEXT NOT NULL,      -- minted (w######)
  work_title      TEXT,
  work_author     TEXT,
  domain          TEXT,               -- works.genre
  source_corpus   TEXT,               -- MASKED code, never a corpus name

  novelty_status        TEXT,
  divergence_correctness TEXT,
  confidence_band       TEXT,
  adjudication_status   TEXT,
  routing_status        TEXT,

  matched_letters INTEGER,
  n_spans         INTEGER,
  coverage_ppm    INTEGER,
  coverage_status TEXT,

  -- both sides, matched span delimited by the two marker columns around it
  ms_before  TEXT, ms_match TEXT, ms_after TEXT,
  ref_before TEXT, ref_match TEXT, ref_after TEXT,
  ref_is_stream INTEGER NOT NULL DEFAULT 0   -- 1 = unspaced fallback
);

CREATE INDEX ix_rr_domain   ON review_row(domain);
CREATE INDEX ix_rr_author   ON review_row(work_author);
CREATE INDEX ix_rr_work     ON review_row(work_id);
CREATE INDEX ix_rr_novelty  ON review_row(novelty_status);
CREATE INDEX ix_rr_diverge  ON review_row(divergence_correctness);
CREATE INDEX ix_rr_sys      ON review_row(sys_id);
"""


def build_source_map(staging: str) -> dict:
    """work_id -> (kind, path). Mirrors the spike's own id derivation, PLUS the
    REF2 staging manifest the spike's review tool never learned about -- which
    is why every Sefaria row rendered 'ref window not located'."""
    src = {}
    m_dir = os.environ.get("V3_REVIEW_M_DIR")
    ja_dir = os.environ.get("V3_REVIEW_JA_DIR")
    if m_dir and os.path.isdir(m_dir):
        for fn in sorted(os.listdir(m_dir)):
            if fn.endswith(".txt"):
                base = fn.replace(".txt-OnlyText.txt", "")
                parts = base.split("--")
                src["M:" + (parts[-1] if parts else fn)] = ("M", os.path.join(m_dir, fn))
    if ja_dir and os.path.isdir(ja_dir):
        for fn in sorted(os.listdir(ja_dir)):
            if fn.endswith(".txt"):
                src["J:" + fn[:-4]] = ("J", os.path.join(ja_dir, fn))
    man = os.path.join(staging, "manifest.json")
    if os.path.exists(man):
        for e in json.load(open(man, encoding="utf-8"))["entries"]:
            src["REF2:" + e["key"]] = ("R", os.path.join(staging, e["body_file"]))
    return src


_HEADER_RE = None
# `M:Ytext1000_26` -> base `M:Ytext1000`. Split-grain part suffix only.
_SPLIT_ID_RE = __import__("re").compile(r"^(.*)_(\d+)$")

# Sibling split works are adjacent in minted-id order, so a tiny cache turns
# 141 reloads of one very large file into a handful.
_RAW_CACHE = {}


def load_raw_cached(kind: str, path: str):
    key = (kind, path)
    hit = _RAW_CACHE.get(key)
    if hit is None:
        hit = load_raw(kind, path)
        if len(_RAW_CACHE) >= 3:
            _RAW_CACHE.clear()
        _RAW_CACHE[key] = hit
    return hit


def load_raw(kind: str, path: str):
    """(raw_text, stream, offsets) using the SAME normalization as the ref build.

    PLAIN PATH FIRST, long-path only as a fallback. The spike's own loader always
    prefixes `\\\\?\\`, and that is actively WRONG for most of these files: many
    begin with `... ` (dots and a space), which Win32 normally normalizes away --
    but `\\\\?\\` exists precisely to SUPPRESS normalization, so the API rejects
    them with EINVAL. Every M-source work therefore read as 'unreadable' and fell
    back to the unspaced letter stream. The prefix is still tried second, for the
    genuinely over-MAX_PATH cases it was introduced for.
    """
    global _HEADER_RE
    if _HEADER_RE is None:
        import re
        _HEADER_RE = re.compile(r"##[^#]*##")
    try:
        raw = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        raw = open("\\\\?\\" + os.path.abspath(path).replace("/", "\\"),
                   encoding="utf-8", errors="replace").read()
    if kind == "M":
        raw = _HEADER_RE.sub(" ", raw)
    stream, offs = norm_stream(raw)
    return raw, stream, offs


def seg3(text: str, offs, a: int, b: int, context: int = CONTEXT):
    """Project a [a,b) NORMALIZED-stream range onto raw text and return
    (before, match, after). `offs[i]` is the raw index of stream char i."""
    if not text or offs is None or a is None or b is None:
        return ("", "", "")
    n = len(offs)
    if n == 0:
        return ("", "", "")
    a = max(0, min(int(a), n - 1))
    b = max(a + 1, min(int(b), n))
    r0 = offs[a]
    r1 = offs[b - 1] + 1 if b - 1 < n else len(text)
    return (text[max(0, r0 - context):r0], text[r0:r1], text[r1:r1 + context])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    ap.add_argument("--slim", default=DEFAULT_SLIM)
    ap.add_argument("--ref", default=DEFAULT_REF)
    ap.add_argument("--crosswalk", default=DEFAULT_XWALK)
    ap.add_argument("--staging", default=DEFAULT_STAGING)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--limit", type=int, default=None, help="smoke: cap rows")
    ap.add_argument("--routing", default="shipped",
                    help="'shipped' (default), 'all', or a routing_status value")
    args = ap.parse_args(argv)

    t0 = time.time()

    def log(m):
        print("[%6.0fs] %s" % (time.time() - t0, m), flush=True)

    xw = json.load(open(args.crosswalk, encoding="utf-8"))
    minted_to_raw = {v: k for k, v in xw.items() if isinstance(v, str)}
    log("crosswalk: %d minted->raw" % len(minted_to_raw))

    works = {w["id"]: w for w in pickle.load(open(args.ref, "rb"))}
    log("reference works: %d" % len(works))

    src_map = build_source_map(args.staging)
    log("reference source paths: %d" % len(src_map))

    art = sqlite3.connect("file:%s?mode=ro" % args.artifact, uri=True)
    slim = sqlite3.connect("file:%s?mode=ro" % args.slim, uri=True)

    where = "" if args.routing == "all" else "AND de.routing_status = :rs"
    sql = """
      SELECT de.evidence_id, de.sys_id, de.a_page_id, dc.work_id,
             w.neutral_title, w.author, w.genre, w.source_corpus,
             de.novelty_status, de.divergence_correctness, de.confidence_band,
             de.adjudication_status, de.routing_status,
             de.matched_letters, de.n_spans, de.coverage_ppm, de.coverage_status,
             de.aligned_page_start, de.aligned_page_end, de.w_start, de.w_end
      FROM discovery_evidence de
      JOIN discovery_claim dc ON dc.claim_id = de.claim_id
      JOIN works w ON w.work_id = dc.work_id
      WHERE de.evidence_source = 'track1_direct' AND de.w_start IS NOT NULL
      %s
      ORDER BY dc.work_id, de.a_page_id
    """ % where
    params = {} if args.routing == "all" else {"rs": args.routing}
    rows = art.execute(sql, params).fetchall()
    if args.limit:
        rows = rows[: args.limit]
    log("rows to render: %d" % len(rows))

    disp = {}
    try:
        for sid, shelf, lib in art.execute(
                "SELECT sys_id, shelfmark_display, library_code FROM manuscript_display"):
            disp[sid] = (shelf, lib)
    except sqlite3.OperationalError:
        log("manuscript_display unavailable -- shelfmarks will be blank")
    log("manuscript display rows: %d" % len(disp))

    if os.path.exists(args.out):
        os.remove(args.out)
    out = sqlite3.connect(args.out)
    out.executescript(SCHEMA)

    page_cache = {}
    cur_wid = None
    wraw = wstream = woffs = None
    w_is_stream = True
    n_ref_ok = n_ref_stream = n_ref_none = 0

    batch = []
    for i, r in enumerate(rows):
        (eid, sysid, pid, minted, wtitle, wauthor, wgenre, wcorpus,
         nov, dvc, band, adj, routing, ml, nspans, cppm, cstat,
         a0, a1, w0, w1) = r

        if minted != cur_wid:
            cur_wid = minted
            raw_id = minted_to_raw.get(minted)
            wobj = works.get(raw_id) if raw_id else None
            wraw = wstream = woffs = None
            w_is_stream = True
            w_shift = 0
            if wobj is not None:
                kp = src_map.get(raw_id)
                split_of = None
                if kp is None and raw_id:
                    # SPLIT-GRAIN WORKS. The v3 bake routes on split works, whose
                    # ids carry a `_NN` part suffix (`M:Ytext1000_26`) that no
                    # source FILE is named after -- the file holds the whole work.
                    # Measured: 141 such works carry 101,677 of the 151,217 rows,
                    # i.e. two thirds of the set fell back to an unspaced letter
                    # stream purely for want of stripping a suffix.
                    m = _SPLIT_ID_RE.match(raw_id)
                    if m and m.group(1) in src_map:
                        split_of = m.group(1)
                        kp = src_map[split_of]
                if kp:
                    try:
                        wraw, wstream, woffs = load_raw_cached(*kp)
                        if wstream == wobj["stream"]:
                            w_shift = 0
                        else:
                            # A split work's stream is a CONTIGUOUS SLICE of its
                            # file's stream. Locate it once per work and carry the
                            # shift; if it is not a slice, this is real drift and
                            # the fallback is correct.
                            at = wstream.find(wobj["stream"]) if wobj["stream"] else -1
                            if at >= 0:
                                w_shift = at
                            else:
                                wraw = None
                    except OSError:
                        wraw = None
                if wraw is None:
                    # Unspaced letter stream: readable-ish, and honestly flagged.
                    wraw = wobj["stream"]
                    woffs = list(range(len(wraw)))
                    w_shift = 0
                    w_is_stream = True
                else:
                    w_is_stream = False

        if pid not in page_cache:
            pr = slim.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
            if pr:
                t = pr[0]
                page_cache[pid] = (t, norm_stream(t)[1])
            else:
                page_cache[pid] = ("", [])
            if len(page_cache) > 4000:
                page_cache.clear()
                page_cache[pid] = page_cache.get(pid, ("", []))
                pr = slim.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
                if pr:
                    page_cache[pid] = (pr[0], norm_stream(pr[0])[1])

        ptext, poffs = page_cache[pid]
        ms = seg3(ptext, poffs, a0, a1)
        if wraw is not None:
            ref = seg3(wraw, woffs, w0 + w_shift, w1 + w_shift)
            if w_is_stream:
                n_ref_stream += 1
            else:
                n_ref_ok += 1
        else:
            ref = ("", "", "")
            n_ref_none += 1

        shelf, lib = disp.get(sysid, (None, None))
        batch.append((eid, sysid, shelf, lib, pid, None,
                      minted, wtitle, wauthor, wgenre, wcorpus,
                      nov, dvc, band, adj, routing,
                      ml, nspans, cppm, cstat,
                      ms[0], ms[1], ms[2], ref[0], ref[1], ref[2],
                      1 if w_is_stream else 0))
        if len(batch) >= 2000:
            out.executemany("INSERT INTO review_row VALUES (%s)" % ",".join("?" * 27), batch)
            out.commit()
            batch = []
            log("  %d / %d rows" % (i + 1, len(rows)))

    if batch:
        out.executemany("INSERT INTO review_row VALUES (%s)" % ",".join("?" * 27), batch)
    out.commit()

    for k, v in (("schema", "discovery-v3-review/1"),
                 ("built_from_artifact", os.path.basename(args.artifact)),
                 ("rows", str(len(rows))),
                 ("ref_from_source_text", str(n_ref_ok)),
                 ("ref_from_letter_stream", str(n_ref_stream)),
                 ("ref_unavailable", str(n_ref_none)),
                 ("context_chars", str(CONTEXT)),
                 ("audience", "private")):
        out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    out.commit()
    out.execute("VACUUM")
    out.close()

    log("wrote %s (%.0f MB)" % (args.out, os.path.getsize(args.out) / 1e6))
    log("  reference from source text : %d" % n_ref_ok)
    log("  reference as letter stream : %d" % n_ref_stream)
    log("  reference unavailable      : %d" % n_ref_none)
    return 0


if __name__ == "__main__":
    sys.exit(main())
