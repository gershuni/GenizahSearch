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
import hashlib
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

import unicodedata  # noqa: E402

from normalize import (  # noqa: E402  (spike modules, gitignored tree)
    compose_offsets, norm_stream)
from msource_clean import clean_m_body_with_offsets  # noqa: E402

# The R-source cleaner lives in the rsource sub-tree; absent on a machine
# without that corpus, in which case RS rows simply never resolve (the same
# honest degrade every other unresolvable source already takes).
sys.path.insert(0, os.path.join(REPO_ROOT, "same_work_spike", "probe",
                                "rsource", "scripts"))
try:
    from gen2_clean_streams import regen_body_with_offsets, rid_of  # noqa: E402
except Exception as _rs_exc:  # noqa: BLE001 -- absence is not an error here
    regen_body_with_offsets = rid_of = None
    print("R-source cleaner unavailable (%s)" % _rs_exc)

DEFAULT_ARTIFACT = os.path.join(REPO_ROOT, "_tmp", "v3_out2", "discovery-v3.db")
DEFAULT_SLIM = os.path.join(REPO_ROOT, "_tmp", "v3_research_slim.db")
DEFAULT_REF = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                           "ref_corpus_v2.pkl")
DEFAULT_XWALK = os.path.join(REPO_ROOT, "discovery_data", "crosswalk.json")
DEFAULT_STAGING = os.path.join(REPO_ROOT, "same_work_spike", "probe", "refs_staging")
DEFAULT_LIBRARIES_CSV = os.path.join(REPO_ROOT, "libraries.csv")
DEFAULT_OUT = os.path.join(REPO_ROOT, "discovery_data", "discovery-v3-REVIEW.db")
DEFAULT_OFFSETS = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data",
                               "transcriptions_index.db")

# The V4-era acquisitions, whose raw ids are REF4:/REF5:/REF6:. Each tree holds
# sources/manifest.json + sources/normalized/<key>.json. Prefix -> its own tree;
# a key missing there is looked up in the others, because a re-fetch can land in
# a later tree than the namespace it was minted under.
V4_SOURCE_TREES = {
    "REF4": os.path.join(REPO_ROOT, "discovery_builds", "discovery_v4", "sources"),
    "REF5": os.path.join(REPO_ROOT, "discovery_builds", "discovery_v4_1", "sources"),
    "REF6": os.path.join(REPO_ROOT, "discovery_builds", "discovery_v4_2", "sources"),
}

# Context kept either side of the match, in RAW characters. Enough to judge the
# span in its setting without storing whole works 194,000 times over.
CONTEXT = 320

# Named, not positional: schema v2 added 20 columns and a miscounted `?` run
# would silently shift every value one column to the left.
_ROW_COLS = (
    "evidence_id sys_id shelfmark library_code page_id page_num volume_ie "
    "catalogue_title work_id work_title work_author domain source_corpus "
    "main_pool main_pool_reason claim_type relation_kind "
    "routing_reason router_verdict "
    "novelty_status divergence_correctness confidence_band "
    "adjudication_status routing_status "
    "matched_letters n_spans coverage_ppm coverage_status "
    "ms_before ms_match ms_after ref_before ref_match ref_after ref_is_stream "
    "page_char_start page_char_end file_char_start file_char_end "
    "ms_provenance_status "
    "aligned_page_start aligned_page_end w_start w_end "
    "ref_char_start ref_char_end ref_provenance_status witness_id "
    "start_unit_ordinal start_intra_char end_unit_ordinal end_intra_char "
    "unit_source_ref locus_label locus_status"
).split()
INSERT_ROW = "INSERT INTO review_row (%s) VALUES (%s)" % (
    ",".join(_ROW_COLS), ",".join("?" * len(_ROW_COLS)))

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE review_row (
  evidence_id     TEXT PRIMARY KEY,
  sys_id          TEXT NOT NULL,
  shelfmark       TEXT,
  library_code    TEXT,
  page_id         TEXT NOT NULL,
  -- Parsed out of page_id (`{sys}_{IE…}_{P000211}_{FL…}`) so the viewer can
  -- address the LIVE /browse viewer at the matched folio. `browse_url`'s rule is
  -- that page and volume_ie travel TOGETHER or not at all -- a page number with
  -- no volume is a different folio in each volume of a multi-volume manuscript,
  -- so half an address is worse than none.
  page_num        INTEGER,
  volume_ie       TEXT,
  -- The CATALOGUE's own title for the manuscript (libraries.csv), beside the
  -- computed identification. Not in the sidecar and not in manuscript_display;
  -- it is the claim the reader is weighing ours against.
  catalogue_title TEXT,

  work_id         TEXT NOT NULL,      -- minted (w######)
  work_title      TEXT,
  work_author     TEXT,
  domain          TEXT,               -- works.genre
  source_corpus   TEXT,               -- MASKED code, never a corpus name

  -- THE TWO GEN-2 DISTINCTIONS, taken from the artifact rather than re-derived.
  -- `main_pool` is `shared.discovery_main_pool.main_pool_decision`'s own boolean,
  -- already computed onto discovery_identification; a guard in that module forbids
  -- any second definition of the rule anywhere under shared/ or web/, so this
  -- reads the decision and never restates it. Reader-facing names are
  -- "main pool" / "more matches" (`bucket_label`) -- NOT "more findings".
  -- The second bucket means the evidence did not meet the rule; it never means
  -- the identification is probably wrong.
  main_pool         INTEGER,
  main_pool_reason  TEXT,
  -- alleged-direct vs alleged-citation. `claim_type` is this ROW's relation;
  -- `relation_kind` is the whole identification's, and they can differ (an
  -- identification may carry both, which is why a `quotes_this_work` row can sit
  -- inside the main pool -- gate 1 asks whether ANY direct claim exists).
  claim_type        TEXT,
  relation_kind     TEXT,
  -- THE ROUTER'S OWN VERDICT, which is the only witness-vs-quoter signal gen-2
  -- actually validated (1,402 + 400 owner-graded cards; ~0.89 weighted precision
  -- on the same_work surface). Derived from routing_reason, never re-decided.
  --
  -- `claim_type` above is NOT this. It is a frozen v1 heuristic -- which matched
  -- span is largest on the page -- and a lone match on a page resolves to
  -- `direct_witness` by construction, with no length floor and no sight of the
  -- text. Measured on this artifact: 76.9% of `direct_witness` rows earned it by
  -- being the only match on their page, and 45,149 rows the router explicitly
  -- called a quotation are stored `direct_witness`. Presenting claim_type as the
  -- relation is what put "alleged direct" on a router-demoted quotation.
  routing_reason    TEXT,
  router_verdict    TEXT,   -- same_work | parallel | not_shipped | shared_text

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
  ref_is_stream INTEGER NOT NULL DEFAULT 0,  -- 1 = unspaced fallback

  -- ============ SCHEMA v2: WHERE THE TEXT CAME FROM ====================
  -- The v1 artifact showed the two passages and nothing about their location,
  -- so a reader who wanted to check one against the source had no address to
  -- go to. Every offset below is a CHARACTER index into NFC(source text) --
  -- see meta 'doc.nfc_offset_policy'. The letter-stream coordinates the
  -- matcher itself works in are kept beside them, unchanged, so the two
  -- systems can be reconciled rather than confused.

  -- MANUSCRIPT SIDE. `page_char_*` index this page's own text; `file_char_*`
  -- index the whole transcriptions corpus file. Both NULL (with a status) when
  -- the page's text did not come from that file -- 18,982 of 667,411 pages are
  -- 'fgp'/'pgp' provenance and legitimately have no address in it.
  page_char_start INTEGER, page_char_end INTEGER,
  file_char_start INTEGER, file_char_end INTEGER,
  ms_provenance_status TEXT,   -- ok | page_missing | offsets_missing
                               -- | nfc_shift | oor | other_provenance

  -- REFERENCE SIDE. `w_start/w_end` are the reference work's letter-stream
  -- coordinates AS STORED by the producer; `aligned_page_start/end` is the
  -- page-side half of that SAME alignment (narrower than the span hull).
  -- `ref_char_*` are the raw-file characters those project onto.
  aligned_page_start INTEGER, aligned_page_end INTEGER,
  w_start INTEGER, w_end INTEGER,
  ref_char_start INTEGER, ref_char_end INTEGER,
  ref_provenance_status TEXT,  -- ok | stream_fallback | unresolved
                               -- | regen_mismatch | nfc_shift | oor
  -- WHICH FILE. Names the exact witness whose text produced ref_char_*, not
  -- merely the work: one canonical work can have several source files.
  witness_id TEXT,
  -- V4-era JSON sources are a CONCATENATION of numbered units, so a single
  -- ordinal cannot address a span that crosses a unit boundary (107 of 12,454
  -- such rows do). Half-open: [start_unit, start_intra) .. [end_unit, end_intra).
  start_unit_ordinal INTEGER, start_intra_char INTEGER,
  end_unit_ordinal   INTEGER, end_intra_char   INTEGER,
  unit_source_ref    TEXT,

  -- The catalogue-style citation address for this identification, computed by
  -- the sidecar and copied here: "שבת יד ע״א" is what a scholar cites,
  -- while a character offset is what a machine seeks to.
  locus_label  TEXT,
  locus_status TEXT
);

-- WHICH FILE EACH WITNESS IS. One row per distinct source file, not per match
-- row: paths would otherwise repeat a quarter of a million times.
-- `ref_id` is SAFE TO SHOW for every corpus: for the two restricted corpora it
-- is the frozen internal id (e.g. 'M:Ytext1000', 'RS:8.0.92') and NEVER a path,
-- with `display_ref` left NULL. The id -> real path mapping is written to a
-- separate local-only key file that is not part of this artifact.
CREATE TABLE source_file (
  id          TEXT PRIMARY KEY,   -- content-derived: sha1(kind|ref_id)[:16]
  kind        TEXT NOT NULL,      -- M | J | REF2 | V4JSON | RS
  masked      INTEGER NOT NULL,   -- 1 = restricted corpus, id only
  ref_id      TEXT NOT NULL,
  display_ref TEXT,               -- real basename/key, or NULL when masked
  detail_json TEXT,
  UNIQUE(kind, ref_id)
);

-- WORK -> ITS WITNESSES, many-to-many on purpose. Several R-source files can
-- carry the same canonical identity (351 raw works collapse onto 343), and the
-- next phase adds further textual witnesses per work; a single source_file per
-- work would have to be migrated away the moment either happens.
CREATE TABLE reference_witness (
  witness_id     TEXT PRIMARY KEY,  -- content-derived: sha1(work_id|raw_id)[:16]
  work_id        TEXT NOT NULL,
  raw_id         TEXT NOT NULL,
  source_file_id TEXT REFERENCES source_file(id),
  w_shift        INTEGER NOT NULL DEFAULT 0,  -- split-grain stream offset
  w_is_stream    INTEGER NOT NULL DEFAULT 0,
  UNIQUE(work_id, raw_id)
);
CREATE INDEX ix_rw_work   ON reference_witness(work_id);
CREATE INDEX ix_rw_source ON reference_witness(source_file_id);

CREATE INDEX ix_rr_witness  ON review_row(witness_id);
CREATE INDEX ix_rr_domain   ON review_row(domain);
CREATE INDEX ix_rr_author   ON review_row(work_author);
CREATE INDEX ix_rr_work     ON review_row(work_id);
CREATE INDEX ix_rr_novelty  ON review_row(novelty_status);
CREATE INDEX ix_rr_diverge  ON review_row(divergence_correctness);
CREATE INDEX ix_rr_sys      ON review_row(sys_id);
CREATE INDEX ix_rr_pool     ON review_row(main_pool);
CREATE INDEX ix_rr_claim    ON review_row(claim_type);
CREATE INDEX ix_rr_routing  ON review_row(routing_status);
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
    # FALLBACK when the two env vars are unset (2026-08-19): ask the spike's own
    # review module for its map instead of duplicating the derivation. Without it
    # the two masked corpora map to nothing and every one of their rows renders as
    # a space-free letter stream -- 124,179 of 142,211 rows on the V4.2 build.
    # `build_track1_review` is where the e1l deck gets its readable spaced text, so
    # this is the same source of truth rather than a second one. `setdefault`, so
    # an explicit env override still wins. Its map is REQUESTED, never its constant
    # names or paths: those name the restricted corpus and must not appear here.
    if not (m_dir and ja_dir):
        try:
            import build_track1_review as _btr  # spike module, gitignored tree
            for _k, _v in _btr.build_source_map().items():
                src.setdefault(_k, _v)
        except Exception as _exc:  # noqa: BLE001 -- absence is not an error here
            print("build_source_map: no readable-source fallback (%s)" % _exc)

    # R-SOURCE (gen-2 addon corpus). Same env-var shape as the two above, so a
    # machine without that corpus mounted still builds -- its rows just resolve
    # to nothing and are flagged, rather than failing the run. `rid_of` is
    # imported from the cleaner instead of re-deriving 'RS:x' -> 'R:x' here.
    r_dir = os.environ.get("V3_REVIEW_R_DIR")
    r_catalog = os.environ.get("V3_REVIEW_R_CATALOG")
    if r_dir and r_catalog and os.path.exists(r_catalog):
        n_rs = 0
        for line in open(r_catalog, encoding="utf-8"):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rid, fn = rec.get("rid"), rec.get("file")
            if not rid or not fn:
                continue
            src["RS:" + rid[2:]] = ("RS", os.path.join(r_dir, fn))
            n_rs += 1
        print("build_source_map: R-source entries %d" % n_rs)

    man = os.path.join(staging, "manifest.json")
    if os.path.exists(man):
        for e in json.load(open(man, encoding="utf-8"))["entries"]:
            src["REF2:" + e["key"]] = ("R", os.path.join(staging, e["body_file"]))

    # REF4/REF5/REF6 acquired sources (2026-08-19). Without these every appended
    # work -- the Mishneh Torah books, Tanhuma, Pesikta, Esther Rabbah, Hizkuni,
    # Sefer ha-Mitzvot, Teshuvot ha-Geonim -- renders as an unspaced letter stream.
    key_to_path = {}
    for _prefix, _tree in V4_SOURCE_TREES.items():
        _man = os.path.join(_tree, "manifest.json")
        if not os.path.exists(_man):
            continue
        for e in json.load(open(_man, encoding="utf-8")).get("entries", []):
            nf = e.get("normalized_file")
            if not nf:
                continue
            p = os.path.join(_tree, "normalized", nf)
            if os.path.exists(p):
                key_to_path.setdefault(_prefix, {})[e["key"]] = p
    for _prefix in V4_SOURCE_TREES:
        own = key_to_path.get(_prefix, {})
        for _k, _p in own.items():
            src[_prefix + ":" + _k] = ("V4JSON", _p)
        # A key that is not in this namespace's own tree, but is in another.
        for _other, _m in key_to_path.items():
            if _other == _prefix:
                continue
            for _k, _p in _m.items():
                src.setdefault(_prefix + ":" + _k, ("V4JSON", _p))
    return src


def router_verdict_of(routing_reason, routing_status):
    """The router's verdict, read off routing_reason -- never re-derived.

    `none` + shipped is the router saying same_work; the demotion reasons name
    themselves. Anything unrecognised returns None rather than being folded into
    a neighbour, so a new reason shows up as unknown instead of silently
    becoming "witness".
    """
    r = (routing_reason or "").strip().lower()
    if r in ("", "none"):
        return "same_work" if routing_status == "shipped" else None
    if r.startswith("gen2_parallel"):
        return "parallel"
    if r == "gen2_router_not_shipped":
        return "not_shipped"
    if r == "later_shared_text":
        return "shared_text"
    return None


_HEADER_RE = None
# `M:Ytext1000_26` -> base `M:Ytext1000`. Split-grain part suffix only.
_SPLIT_ID_RE = __import__("re").compile(r"^(.*)_(\d+)$")
# `{sys}_{IE163082409}_{P000001}_{FL163082411}` -> volume IE + folio number.
_PAGE_ID_RE = __import__("re").compile(r"_(IE\d+)_P(\d+)_")

# Sibling split works are adjacent in minted-id order, so a tiny cache turns
# 141 reloads of one very large file into a handful.
_RAW_CACHE = {}


def load_raw_cached(kind: str, path: str, raw_id=None):
    # raw_id participates in the key: for R-source the cleaning chain is
    # per-work (one work drops sections, one keeps a gloss), so two works read
    # from the same file must not share a cache entry.
    key = (kind, path, raw_id if kind == "RS" else None)
    hit = _RAW_CACHE.get(key)
    if hit is None:
        hit = load_raw(kind, path, raw_id)
        if len(_RAW_CACHE) >= 3:
            _RAW_CACHE.clear()
        _RAW_CACHE[key] = hit
    return hit


def _read_text(kind: str, path: str) -> str:
    """PLAIN PATH FIRST, long-path only as a fallback. The spike's own loader
    always prefixes `\\\\?\\`, and that is actively WRONG for most of these
    files: many begin with `... ` (dots and a space), which Win32 normally
    normalizes away -- but `\\\\?\\` exists precisely to SUPPRESS normalization,
    so the API rejects them with EINVAL. Every M-source work therefore read as
    'unreadable' and fell back to the unspaced letter stream. The prefix is
    still tried second, for the genuinely over-MAX_PATH cases.

    ERRORS POLICY MIRRORS EACH PRODUCER, and is recorded in meta: R-source was
    ingested with errors='strict', so a byte that would be replaced here but
    not there must fail rather than silently shift every later offset.
    """
    errors = "strict" if kind == "RS" else "replace"
    try:
        return open(path, encoding="utf-8", errors=errors).read()
    except OSError:
        return open("\\\\?\\" + os.path.abspath(path).replace("/", "\\"),
                    encoding="utf-8", errors=errors).read()


def load_raw(kind: str, path: str, raw_id=None):
    """(display_text, stream, stream_offs, file_offs, detail).

    `display_text` is what `seg3` slices: NFC of the source, AFTER any
    kind-specific substitution. `stream_offs[i]` indexes display_text;
    `file_offs[i]` indexes NFC(original file) -- the two differ exactly when a
    substitution ran, which is why the map is composed rather than assumed.

    `detail` carries what the row-level status needs: nfc_len_eq (False means
    NFC moved characters, so raw offsets must be withheld, not approximated)
    and, for V4JSON, the unit table needed to address a span that crosses a
    unit boundary.
    """
    if kind == "V4JSON":
        # The frozen stream is the units' text concatenated with NO separator,
        # so the readable text is that same concatenation unnormalized. Offsets
        # are into the CONCATENATION, which is not the .json file's own
        # character space -- hence the unit ordinals stored beside them.
        doc = json.load(open(path, encoding="utf-8"))
        units = doc.get("units") or []
        parts, table, pos = [], [], 0
        for ordinal, u in enumerate(units):
            t = u.get("text") or ""
            parts.append(t)
            table.append((pos, pos + len(t), ordinal,
                          u.get("source_ref") or u.get("ref") or None))
            pos += len(t)
        raw = "".join(parts)
        nfc = unicodedata.normalize("NFC", raw)
        stream, offs = norm_stream(nfc)
        return nfc, stream, offs, offs, {
            "nfc_len_eq": len(nfc) == len(raw), "units": table}

    raw = _read_text(kind, path)
    nfc = unicodedata.normalize("NFC", raw)
    detail = {"nfc_len_eq": len(nfc) == len(raw)}

    if kind in ("M", "RS"):
        # Both restricted corpora are CLEANED before their stream is built (M:
        # `##...##` headers with the canonical regex that built the reference;
        # RS: locus headers, `+...+` apparatus, one section drop, one kept
        # gloss). Each cleaner hands back the readable body AND a map from that
        # body to the original file, so the displayed text and the stored
        # offsets are derived from one pass instead of two that could disagree.
        if kind == "M":
            body, body_offs = clean_m_body_with_offsets(raw)
        else:
            body, body_offs = regen_body_with_offsets(rid_of(raw_id), raw)
        stream, stream_offs = norm_stream(body)
        return body, stream, stream_offs, compose_offsets(stream_offs,
                                                          body_offs), detail

    stream, offs = norm_stream(nfc)
    return nfc, stream, offs, offs, detail


def seg3(text: str, offs, a: int, b: int, context: int = CONTEXT):
    """Project a [a,b) NORMALIZED-stream range onto raw text and return
    (before, match, after, r0, r1, status). `offs[i]` is the raw index of
    stream char i.

    BOUNDS ARE CHECKED, NOT CLAMPED. The v1 version silently clamped a span
    that fell outside the stream, which produced a plausible-looking excerpt at
    the wrong place and reported nothing -- exactly the failure a stored offset
    must never hide. An out-of-range span now returns status 'oor' and no
    coordinates, so the row says so instead of lying quietly.
    """
    if not text or offs is None or a is None or b is None:
        return ("", "", "", None, None, "unresolved")
    n = len(offs)
    if n == 0:
        return ("", "", "", None, None, "unresolved")
    a, b = int(a), int(b)
    if a < 0 or b <= a or a >= n or b > n:
        return ("", "", "", None, None, "oor")
    r0 = offs[a]
    r1 = offs[b - 1] + 1
    if not (0 <= r0 < r1 <= len(text)):
        return ("", "", "", None, None, "oor")
    return (text[max(0, r0 - context):r0], text[r0:r1], text[r1:r1 + context],
            r0, r1, "ok")


# The two restricted corpora: their rows carry the frozen internal id and NEVER
# a filesystem path, because the path names the provider. Everything else can
# show its real filename -- the point of the artifact is to be checkable.
MASKED_KINDS = {"M", "RS"}


def _cid(*parts) -> str:
    """Content-derived id. Two independently built satellites that see the same
    file must mint the SAME id, or merging them would collide on integers that
    mean different things in each."""
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")
                        ).hexdigest()[:16]


def _register_witness(out, sf_seen, wit_seen, keyfile, work_id, raw_id,
                      kind, path, w_shift, w_is_stream, detail):
    """Upsert source_file + reference_witness; return the witness id.

    Real paths for masked kinds go ONLY into `keyfile` (written next to nothing
    -- the caller decides where, outside this artifact).
    """
    masked = kind in MASKED_KINDS
    ref_id = raw_id if masked else os.path.basename(path)
    sf_id = _cid(kind, ref_id)
    if sf_id not in sf_seen:
        sf_seen.add(sf_id)
        det = None
        if detail and detail.get("units"):
            det = json.dumps({"n_units": len(detail["units"])})
        out.execute("INSERT OR IGNORE INTO source_file VALUES (?,?,?,?,?,?)",
                    (sf_id, kind, 1 if masked else 0, ref_id,
                     None if masked else os.path.basename(path), det))
        if masked:
            keyfile[ref_id] = os.path.abspath(path)
    wit_id = _cid(work_id, raw_id)
    if wit_id not in wit_seen:
        wit_seen.add(wit_id)
        out.execute("INSERT OR IGNORE INTO reference_witness "
                    "VALUES (?,?,?,?,?,?)",
                    (wit_id, work_id, raw_id, sf_id, w_shift,
                     1 if w_is_stream else 0))
    return wit_id


def _load_page(slim, pid):
    """(nfc_text, stream_offsets, nfc_is_identity).

    NFC FIRST, then normalize -- `norm_stream`'s offsets index NFC(text), so
    slicing the pre-NFC string with them is only safe while the two coincide.
    Usually they do; measured, 9 rows of 292,703 sat on pages where NFC is two
    characters LONGER, and there the old code read past the end of the page and
    produced an empty excerpt with no explanation. Those pages keep their
    page-relative offsets (correct in NFC space) but are denied FILE offsets:
    the file index counts raw decoded characters, so adding an NFC-space
    position to a raw-space base would be arithmetic across two different
    coordinate systems.
    """
    pr = slim.execute("SELECT text FROM pages WHERE page_id=?",
                      (pid,)).fetchone()
    if not pr or not pr[0]:
        return ("", [], True)
    raw = pr[0]
    nfc = unicodedata.normalize("NFC", raw)
    return (nfc, norm_stream(nfc)[1], nfc == raw)


def _unit_of(units, pos):
    """(ordinal, intra_char, source_ref) for a character position in a V4JSON
    units concatenation. Binary search; None when out of range."""
    if not units or pos is None:
        return (None, None, None)
    lo, hi = 0, len(units) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        a, b, ordinal, sref = units[mid]
        if pos < a:
            hi = mid - 1
        elif pos >= b:
            lo = mid + 1
        else:
            return (ordinal, pos - a, sref)
    return (None, None, None)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    ap.add_argument("--slim", default=DEFAULT_SLIM)
    ap.add_argument("--ref", default=DEFAULT_REF)
    ap.add_argument("--crosswalk", default=DEFAULT_XWALK)
    ap.add_argument("--staging", default=DEFAULT_STAGING)
    ap.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES_CSV)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--offsets-db", default=DEFAULT_OFFSETS,
                    help="transcriptions_index.db from "
                         "scripts/index_transcriptions_offsets.py")
    ap.add_argument("--sourcekeys-out", default=None,
                    help="REQUIRED when a restricted corpus resolves: where to "
                         "write the id->real-path map. MUST be outside the repo "
                         "and outside discovery_data/ -- it is the one file that "
                         "names the restricted providers, and it is never part "
                         "of the artifact that gets handed over.")
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
             di.main_pool, di.main_pool_reason, dc.claim_type, di.relation_kind,
             de.routing_reason,
             de.novelty_status, de.divergence_correctness, de.confidence_band,
             de.adjudication_status, de.routing_status,
             de.matched_letters, de.n_spans, de.coverage_ppm, de.coverage_status,
             de.aligned_page_start, de.aligned_page_end, de.w_start, de.w_end,
             dc.locus_label, dc.locus_status
      FROM discovery_evidence de
      JOIN discovery_claim dc ON dc.claim_id = de.claim_id
      JOIN works w ON w.work_id = dc.work_id
      LEFT JOIN discovery_identification di
             ON di.sys_id = de.sys_id AND di.canonical_work_id = w.canonical_work_id
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

    # Catalogue titles. Only the sys_ids this build actually needs are kept --
    # libraries.csv carries ~255,000 records and the shipped set touches a small
    # fraction of them.
    need = {r[1] for r in rows}
    cat_title = {}
    if os.path.exists(args.libraries_csv):
        import csv as _csv
        with open(args.libraries_csv, encoding="utf-8-sig", newline="") as fh:
            for rec in _csv.reader(fh):
                if len(rec) > 7 and rec[0] in need:
                    t = (rec[7] or "").strip()
                    if t:
                        cat_title[rec[0]] = t
        log("catalogue titles: %d of %d manuscripts" % (len(cat_title), len(need)))
    else:
        log("libraries.csv not found -- catalogue titles will be blank")

    if os.path.exists(args.out):
        os.remove(args.out)
    out = sqlite3.connect(args.out)
    out.executescript(SCHEMA)

    # PAGE -> ITS SPAN IN THE TRANSCRIPTIONS FILE, from the one-pass index. Its
    # own build already refused to emit a page whose text did not reproduce the
    # file exactly, so a present entry is a verified one; a missing entry means
    # "no address", which the row states rather than approximates.
    page_span = {}
    if args.offsets_db and os.path.exists(args.offsets_db):
        oc = sqlite3.connect("file:%s?mode=ro" % args.offsets_db, uri=True)
        for pid_, a_, n_ in oc.execute(
                "SELECT page_id, file_char_start, n_chars FROM page_offsets"):
            page_span[pid_] = (a_, n_)
        oc.close()
        log("transcriptions offsets: %d pages" % len(page_span))
    else:
        log("no --offsets-db: file_char_* will be NULL "
            "(ms_provenance_status='offsets_missing')")

    page_cache = {}
    cur_wid = None
    wraw = wstream = woffs = None
    wfile_offs = None
    w_detail = {}
    cur_witness = None
    cur_ref_status = "unresolved"
    w_is_stream = True
    n_ref_ok = n_ref_stream = n_ref_none = 0
    n_ms, n_ref = {}, {}
    sf_seen, wit_seen, keyfile = set(), set(), {}

    batch = []
    for i, r in enumerate(rows):
        (eid, sysid, pid, minted, wtitle, wauthor, wgenre, wcorpus,
         mpool, mreason, ctype, rkind, rreason,
         nov, dvc, band, adj, routing, ml, nspans, cppm, cstat,
         a0, a1, w0, w1, locus_label, locus_status) = r

        if minted != cur_wid:
            cur_wid = minted
            raw_id = minted_to_raw.get(minted)
            wobj = works.get(raw_id) if raw_id else None
            wraw = wstream = woffs = None
            wfile_offs = None
            w_detail = {}
            cur_witness = None
            cur_ref_status = "unresolved"
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
                        (wraw, wstream, woffs, wfile_offs,
                         w_detail) = load_raw_cached(kp[0], kp[1], raw_id)
                        if wstream == wobj["stream"]:
                            w_shift = 0
                        else:
                            # A split work's stream is a CONTIGUOUS SLICE of its
                            # file's stream. Locate it once per work and carry the
                            # shift. It must occur EXACTLY ONCE: a repeated
                            # stream would make the first `.find()` an arbitrary
                            # choice among several equally plausible divisions,
                            # and every offset for that work would then point at
                            # a place we cannot justify. Ambiguity therefore
                            # degrades to the fallback rather than guessing.
                            sub = wobj["stream"]
                            at = wstream.find(sub) if sub else -1
                            if at >= 0 and wstream.find(sub, at + 1) >= 0:
                                cur_ref_status = "ambiguous_split"
                                wraw = None
                            elif at >= 0:
                                w_shift = at
                            else:
                                cur_ref_status = "regen_mismatch"
                                wraw = None
                    except OSError:
                        cur_ref_status = "unresolved"
                        wraw = None
                if wraw is None:
                    # Unspaced letter stream: readable-ish, and honestly flagged.
                    wraw = wobj["stream"]
                    woffs = list(range(len(wraw)))
                    wfile_offs = None       # a stream has no file coordinates
                    w_shift = 0
                    w_is_stream = True
                    if cur_ref_status == "unresolved":
                        cur_ref_status = "stream_fallback"
                else:
                    w_is_stream = False
                    cur_ref_status = ("nfc_shift"
                                      if not w_detail.get("nfc_len_eq", True)
                                      else "ok")
                    cur_witness = _register_witness(
                        out, sf_seen, wit_seen, keyfile, minted, raw_id,
                        kp[0], kp[1], w_shift, 0, w_detail)

        if pid not in page_cache:
            if len(page_cache) > 4000:
                page_cache.clear()
            page_cache[pid] = _load_page(slim, pid)

        ptext, poffs, page_nfc_ok = page_cache[pid]
        ms = seg3(ptext, poffs, a0, a1)

        # ---- MANUSCRIPT-SIDE ADDRESS -------------------------------------
        pg_a = pg_b = f_a = f_b = None
        if not ptext:
            ms_status = "page_missing"
        elif ms[5] != "ok":
            ms_status = ms[5]
        elif not page_nfc_ok:
            # Page-relative offsets are still right (NFC space); file offsets
            # would mix NFC and raw coordinates, so they are withheld.
            pg_a, pg_b = ms[3], ms[4]
            ms_status = "nfc_shift"
        else:
            pg_a, pg_b = ms[3], ms[4]
            span = page_span.get(pid)
            if span is None:
                # No file span: either the index was not supplied, or this
                # page's text legitimately came from a different source
                # (fgp/pgp provenance). Both are stated, never guessed.
                ms_status = ("offsets_missing" if page_span
                             else "offsets_missing")
            else:
                base, n_page = span
                if n_page != len(ptext):
                    # The indexed page and the page rendered here are not the
                    # same text, so adding one to the other would be fiction.
                    ms_status = "offsets_stale"
                else:
                    f_a, f_b = base + pg_a, base + pg_b
                    ms_status = "ok"
        n_ms[ms_status] = n_ms.get(ms_status, 0) + 1

        # ---- REFERENCE-SIDE ADDRESS --------------------------------------
        rc_a = rc_b = None
        su = si = eu = ei = None
        sref = None
        if wraw is not None:
            ws, we = w0 + w_shift, w1 + w_shift
            ref = seg3(wraw, woffs, ws, we)
            if w_is_stream:
                n_ref_stream += 1
                ref_status = "stream_fallback"
            else:
                n_ref_ok += 1
                ref_status = cur_ref_status
                if ref[5] != "ok":
                    ref_status = ref[5]
                elif ref_status == "ok" and wfile_offs is not None:
                    if 0 <= ws < len(wfile_offs) and 0 < we <= len(wfile_offs):
                        rc_a = wfile_offs[ws]
                        rc_b = wfile_offs[we - 1] + 1
                        units = w_detail.get("units")
                        if units:
                            su, si, sref = _unit_of(units, rc_a)
                            eu, ei, _ = _unit_of(units, max(rc_a, rc_b - 1))
                    else:
                        ref_status = "oor"
        else:
            ref = ("", "", "", None, None, "unresolved")
            ref_status = cur_ref_status
            n_ref_none += 1
        n_ref[ref_status] = n_ref.get(ref_status, 0) + 1

        shelf, lib = disp.get(sysid, (None, None))
        # SEARCH, not match: page_id begins with the sys_id, so an anchored match
        # returns None for every row and the preview link silently loses its folio.
        pm = _PAGE_ID_RE.search(pid or "")
        vol_ie = pm.group(1) if pm else None
        page_no = int(pm.group(2)) if pm else None
        batch.append((eid, sysid, shelf, lib, pid, page_no, vol_ie,
                      cat_title.get(sysid),
                      minted, wtitle, wauthor, wgenre, wcorpus,
                      mpool, mreason, ctype, rkind,
                      rreason, router_verdict_of(rreason, routing),
                      nov, dvc, band, adj, routing,
                      ml, nspans, cppm, cstat,
                      ms[0], ms[1], ms[2], ref[0], ref[1], ref[2],
                      1 if w_is_stream else 0,
                      # --- schema v2 ---
                      pg_a, pg_b, f_a, f_b, ms_status,
                      a0, a1, w0, w1, rc_a, rc_b, ref_status,
                      cur_witness,
                      su, si, eu, ei, sref,
                      locus_label, locus_status))
        if len(batch) >= 2000:
            out.executemany(INSERT_ROW, batch)
            out.commit()
            batch = []
            log("  %d / %d rows" % (i + 1, len(rows)))

    if batch:
        out.executemany(INSERT_ROW, batch)
    out.commit()

    # DEFINITIONS TRAVEL WITH THE FILE. This artifact is shared, and a reader
    # opening it in a SQLite browser sees column names with no way to know that
    # `claim_type` is not a relation or that `main_pool=0` is not a verdict of
    # wrong. The viewer carries the same text on screen; this is for everyone who
    # never opens the viewer.
    for k, v in (
        ("doc.router_verdict",
         "THE RELATION, and the only witness-vs-quoter axis that was validated "
         "(~1,400 blind + 400 graded cards). same_work = this page is a copy of "
         "the work; parallel = it quotes the work; not_shipped / shared_text = "
         "other router outcomes. Decided by how much of the PAGE the match covers."),
        ("doc.claim_type",
         "NOT a relation. Says only which matched span is LARGEST ON THIS PAGE. No "
         "minimum length, never reads the text, and a page with a single match gets "
         "'direct_witness' by default however short. 45,149 rows the router called a "
         "quotation carry direct_witness. Use router_verdict for the relation."),
        ("doc.main_pool",
         "1 = main pool (shown first), 0 = 'more matches' (held behind), NULL = the "
         "rule was never evaluated for this row. Describes the whole IDENTIFICATION "
         "(manuscript x work across all its pages), not this row. 0 means the evidence "
         "did not meet the rule -- NOT that the identification is wrong."),
        ("doc.routing_status",
         "shipped = would reach the public site; review_only = held back. Most "
         "quotations are review_only, which is the router working as intended."),
        ("doc.novelty_status",
         "Automated judgement of whether this identification adds anything to what the "
         "catalogues and bibliography already record. not_checked is an honest 'no "
         "answer', never a guess."),
        ("doc.divergence_correctness",
         "HUMAN-ONLY and empty by design. When our identification and the catalogue "
         "disagree, who is right. The model measured 8/28 on this -- at or below chance "
         "for three options -- so it was removed from the model's job. Grades recorded "
         "through the viewer live in a separate <db>.grades.db."),
        ("doc.ms_match_vs_ref_match",
         "The two sides will NOT match closely: a Genizah fragment against a printed "
         "edition runs ~0.4 apart per character (orthography, abbreviations, real "
         "variants, editorial markup and vowels). That is what a witness looks like."),
        ("doc.known_weakness",
         "The screen that suppresses matches resting on shared scripture is stale and "
         "misses part of the reference corpus, so some matches sit on a verse or "
         "liturgical formula many works quote. Treat SHORT matches on such text with "
         "suspicion. See docs/OPEN_ISSUES.md (2026-08-09)."),
        ("doc.nfc_offset_policy",
         "Every character offset in this file indexes NFC(source text). NFC is "
         "where the normalizer measures, so that is the only space in which the "
         "offsets are exact. A source whose NFC form is not the same length as "
         "its raw form gets NO raw offsets -- the row says 'nfc_shift' instead, "
         "because a length check cannot prove characters did not move."),
        ("doc.ms_offsets",
         "page_char_start/end index THIS row's page text. file_char_start/end "
         "index the whole transcriptions corpus file, decoded as "
         "utf-8/errors=replace in Python text mode (universal newlines), "
         "counted in CHARACTERS not bytes. Both are NULL when the page's text "
         "did not come from that file: 18,982 of 667,411 pages are 'fgp'/'pgp' "
         "provenance and have no address in it. ms_provenance_status says which "
         "case a row is."),
        ("doc.ref_offsets",
         "w_start/w_end and aligned_page_start/end are the matcher's own "
         "LETTER-STREAM coordinates, unchanged (space-free, vowel-free). "
         "ref_char_start/end are the raw-file characters those project onto, "
         "mapped THROUGH each corpus's own cleanup (M: '##...##' headers; "
         "R-source: locus headers, '+...+' apparatus, one section drop, one "
         "kept gloss) by an offset-preserving version of that cleanup -- never "
         "by re-deriving positions in the cleaned text. For V4-era JSON sources "
         "the offsets are into the units CONCATENATION, not the .json file, "
         "which is why start/end unit ordinals sit beside them: 107 spans cross "
         "a unit boundary, so one ordinal could not address them."),
        ("doc.witness_id",
         "Names the exact SOURCE FILE whose text produced this row's "
         "ref_char_*, joining reference_witness -> source_file. One canonical "
         "work can have several witnesses (351 R-source files collapse onto 343 "
         "identities), so the work id alone would not identify the file."),
        ("doc.masked_provenance",
         "For the two restricted corpora, source_file.ref_id is the frozen "
         "internal id and display_ref is NULL: the filename would name the "
         "provider. The id -> real path map is written to a separate key file "
         "that is deliberately NOT part of this artifact and must not travel "
         "with it."),
        ("doc.locus_label",
         "The citable address of the identified passage as the sidecar computed "
         "it (e.g. a tractate folio), for the reader who wants a citation "
         "rather than a character offset. locus_status='resolved' means it was "
         "computed; anything else means it was not."),
        ("schema", "discovery-v3-review/2"),
                 ("built_from_artifact", os.path.basename(args.artifact)),
                 ("rows", str(len(rows))),
                 ("ref_from_source_text", str(n_ref_ok)),
                 ("ref_from_letter_stream", str(n_ref_stream)),
                 ("ref_unavailable", str(n_ref_none)),
                 ("context_chars", str(CONTEXT)),
                 ("offsets_db", os.path.basename(args.offsets_db or "")),
                 ("ms_status_counts", json.dumps(n_ms, sort_keys=True)),
                 ("ref_status_counts", json.dumps(n_ref, sort_keys=True)),
                 ("source_files", str(len(sf_seen))),
                 ("reference_witnesses", str(len(wit_seen))),
                 ("audience", "private")):
        out.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    # SLIM FACET PROJECTION. Every review_row carries ~6 KB of both-sides text,
    # so a facet GROUP BY drags that payload through memory for columns it never
    # reads -- measured at 7.7s for one /api/facets call before this existed, slow
    # enough that the browser cancelled it and the reader saw empty dropdowns.
    # ~40 MB against 1.4 GB. The server rebuilds it if absent or stale, so this is
    # an optimisation, never a correctness dependency.
    out.execute("""CREATE TABLE facet_row AS SELECT
                     evidence_id, sys_id, shelfmark, domain, work_id, work_title,
                     work_author, novelty_status, main_pool, claim_type,
                     router_verdict, routing_status FROM review_row""")
    for _c in ("domain", "work_id", "work_author", "novelty_status",
               "main_pool", "claim_type", "router_verdict", "routing_status",
               "evidence_id"):
        out.execute("CREATE INDEX ix_fr_%s ON facet_row(%s)" % (_c, _c))
    out.commit()
    out.execute("VACUUM")
    out.close()

    log("wrote %s (%.0f MB)" % (args.out, os.path.getsize(args.out) / 1e6))
    log("  reference from source text : %d" % n_ref_ok)
    log("  reference as letter stream : %d" % n_ref_stream)
    log("  reference unavailable      : %d" % n_ref_none)
    log("  ms status  : %s" % json.dumps(n_ms, sort_keys=True))
    log("  ref status : %s" % json.dumps(n_ref, sort_keys=True))
    log("  source files %d / witnesses %d" % (len(sf_seen), len(wit_seen)))

    # ---- THE KEY FILE, deliberately not part of the artifact -------------
    if keyfile:
        dest = args.sourcekeys_out
        if not dest:
            print("\n!!! %d restricted-corpus sources resolved but "
                  "--sourcekeys-out was not given. The id->path map is the one "
                  "thing that names the restricted providers; refusing to guess "
                  "a location for it. Re-run with --sourcekeys-out pointing "
                  "OUTSIDE the repo (e.g. %%USERPROFILE%%\\.genizah-private\\"
                  "sourcekeys.json)." % len(keyfile))
            return 1
        dest = os.path.abspath(dest)
        forbidden = (os.path.abspath(REPO_ROOT),)
        if any(dest.startswith(f + os.sep) for f in forbidden):
            print("\n!!! --sourcekeys-out (%s) is inside the repository. That "
                  "file must not sit beside the artifact it is excluded from. "
                  "Choose a path outside %s." % (dest, REPO_ROOT))
            return 1
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        merged = {}
        if os.path.exists(dest):
            try:
                merged = json.load(open(dest, encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                print("!!! existing key file unreadable (%s) -- refusing to "
                      "overwrite it" % exc)
                return 1
        conflicts = [k for k, v in keyfile.items()
                     if k in merged and merged[k] != v]
        if conflicts:
            print("\n!!! %d ids map to a DIFFERENT path than the existing key "
                  "file records (e.g. %s). Not overwriting; resolve first."
                  % (len(conflicts), conflicts[:3]))
            return 1
        merged.update(keyfile)
        tmp = dest + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(merged, fh, ensure_ascii=False, indent=1, sort_keys=True)
        os.replace(tmp, dest)
        log("wrote %d masked-source keys -> %s (%d total)"
            % (len(keyfile), dest, len(merged)))
        print("\n  NOTE: that key file names the restricted corpora. It is NOT "
              "part of the review artifact and must never be sent with it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
