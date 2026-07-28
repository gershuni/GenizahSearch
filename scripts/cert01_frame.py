"""CERT-01 frozen estimand + hash primitives (Phase 135, plan 135-09).

Implements EXACTLY the protocol frozen in
`docs/specs/discovery-cert01-protocol.md`: section 1.1/1.2 (population
membership + the dedup/ranking SQL), 1.3 (page->physMS cluster mapping +
`cluster_map_hash`), 1.4 (cross-corpus stratum tie-break), 3 (strata), and
5.1/5.2 (the four frozen input hashes + `db_content_hash` + `crosswalk_sha256`
+ the self-referential `report_id` construction).

This module is the ONE source of truth shared by all three CERT-01 call
sites so the estimand/hash recipe is never re-derived independently (the
135-07 HALT-A lesson: an independently re-derived metric is exactly the bug
class that produced that HALT):

  - `scripts/cert01_freeze.py`     (Task 1 -- writes cert01_prereg.json + the OC table)
  - `scripts/cert01_draw_deck.py`  (Task 2 -- draws the deck against the frozen frame)
  - `scripts/verify_cert01_grading.py` (Task 3 -- RECOMPUTES + compares, never trusts)

Deliberately imports `build_discovery_sidecar` (for the frozen
`norm_stream_letter_count`/`compute_page_coverage` Lever-1 recipe -- the EXACT
function that produced the routing decisions this estimand measures) and
`discovery_ids` (frozen enum vocab), both tracked `scripts/` modules. No
import from the gitignored `same_work_spike/` research tree is required for
the estimand/hash logic itself -- only the deck-rendering step in
`cert01_draw_deck.py` needs that (via a runtime sys.path bridge; see that
file's module docstring for why `same_work_spike/probe/scripts/` is no longer
a place this plan adds NEW tracked files -- commit `5370c20f` deliberately
untracked that whole research tree as part of the M-source masking-history
remediation, and this plan does not reverse that decision. See the 135-09
SUMMARY "Deviations" section.)

Masking: this module and its docstrings carry ONLY opaque `w000xxx` ids,
table/column names, and generic statistics -- no restricted corpus name, no
raw research work_id, no reference text.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

# Self-bootstrap sys.path (mirrors build_discovery_sidecar.py /
# verify_discovery_sidecar.py) so this module's own bare imports below
# resolve correctly regardless of how a CALLER imported it (bare
# `import cert01_frame`, `from scripts import cert01_frame`, or
# `import scripts.cert01_frame`).
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import discovery_ids as ids  # noqa: F401  (frozen enum vocab -- imported for callers' convenience)
import build_discovery_sidecar as sidecar_build

# ---------------------------------------------------------------------------
# Frozen constants (protocol §1.4, §2, §3)
# ---------------------------------------------------------------------------

# Cross-corpus stratum tie-break priority order (protocol §1.4): sefaria wins
# over ja, ja wins over msource. A FIXED priority order, never per-card.
CORPUS_RANK = {"sefaria": 1, "ja": 2, "msource": 3}

# Coverage-band split within the shipped (>= Lever-1 cliff) tier_a estimand
# (protocol §3: "coverage band (high >=0.60 / medium 0.45-0.60)"). The
# medium floor is restated from the ALREADY-APPLIED Lever-1 routing cliff
# (never re-routes anything here -- every estimand row is shipped already).
COVERAGE_HIGH_FLOOR = 0.60
COVERAGE_MEDIUM_FLOOR = sidecar_build.LEVER1_COVERAGE_CLIFF  # 0.45

# w001239 (the RCh-Shabbat Sefaria copy, D-14) is dropped ENTIRELY at bake --
# it never appears in `works`. Restated here defensively (a no-op against the
# current deployed asset; a cheap guard against a future rebuild regression).
DROPPED_WORK_IDS = frozenset({"w001239"})

# The frozen protocol's D-17 demotion delta + the Strict pass gate, restated
# here (already baked upstream) purely for pre-registration completeness
# (protocol §5.1 "All cutoffs").
D17_DELTA_YEARS = 100
STRICT_FLOOR = 0.85

_NAMESPACE_REPORT = "cert01_prereg_v1"


# ---------------------------------------------------------------------------
# The frozen dedup/ranking SQL (protocol §1.2). One row per surviving
# (page_id, canonical_work_id) claim, using the SAME deterministic
# display-evidence precedence lattice already frozen in
# docs/specs/discovery-sidecar-schema-v1.md §6, applied ACROSS raw claims
# instead of across evidence rows within one claim.
# ---------------------------------------------------------------------------

_RANKED_ESTIMAND_SQL_TEMPLATE = """
WITH claim_display AS (
  SELECT
    dc.page_id,
    w.canonical_work_id,
    dc.work_id,
    dc.claim_id,
    de.evidence_id           AS display_evidence_id,
    de.evidence_source,
    de.confidence_band,
    de.adjudication_status,
    de.routing_status,
    de.a_page_id,
    de.sys_id,
    de.matched_letters,
    de.span_start,
    de.span_end,
    w.source_corpus
  FROM discovery_claim dc
  JOIN works w               ON w.work_id = dc.work_id
  JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
  WHERE de.routing_status = 'shipped'
    AND dc.work_id NOT IN ({dropped})
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY page_id, canonical_work_id
      ORDER BY
        -- (i) family-specific human_confirmed dominance (schema §6 rule 1)
        CASE WHEN evidence_source = 'track1_direct'
               AND adjudication_status = 'human_confirmed' THEN 0 ELSE 1 END,
        -- (ii) global band-rank, strongest first (schema §6 rule 2, verbatim;
        -- both the v1 key expert_verified and the v2 key
        -- high_confidence_algorithmic share rank 1 -- v1-read-compat)
        CASE evidence_source || ':' || confidence_band
          WHEN 'track1_direct:expert_verified'              THEN 1
          WHEN 'track1_direct:high_confidence_algorithmic'   THEN 1
          WHEN 'track1_direct:tier_a'                        THEN 2
          WHEN 'propagated:corroborated'                     THEN 3
          WHEN 'track1_direct:screening_rb'                  THEN 4
          WHEN 'track1_direct:screening_canon'                THEN 5
          WHEN 'propagated:weak'                              THEN 6
          WHEN 'propagated:not_evaluated'                      THEN 7
          ELSE 99
        END,
        -- (iii) adjudication_status tie-break (schema §6 rule 3)
        CASE adjudication_status
          WHEN 'human_confirmed' THEN 0
          WHEN 'provisional'     THEN 1
          WHEN 'unreviewed'      THEN 2
          ELSE 9
        END,
        -- (iv) evidence_id lexicographic tie-break (schema §6 rule 4)
        display_evidence_id ASC
    ) AS rn
  FROM claim_display
)
SELECT page_id, canonical_work_id, work_id, claim_id, display_evidence_id,
       a_page_id, sys_id, matched_letters, span_start, span_end, source_corpus
FROM ranked
WHERE rn = 1 AND confidence_band = 'tier_a'
ORDER BY page_id, canonical_work_id
"""


def _connect_ro(db_path) -> sqlite3.Connection:
    uri = "file:" + str(Path(db_path).resolve()).replace("\\", "/") + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _dropped_sql_literal(dropped_work_ids: Sequence[str]) -> str:
    if not dropped_work_ids:
        return "''"
    return ",".join("'" + w.replace("'", "''") + "'" for w in sorted(dropped_work_ids))


def resolve_stratum_corpus_map(conn: sqlite3.Connection) -> Dict[str, str]:
    """Protocol §1.4 cross-corpus stratum tie-break.

    For every `canonical_work_id` contributing >=1 SHIPPED raw claim, pick
    the LOWEST-`corpus_rank` `source_corpus` among every corpus contributing
    at least one shipped raw claim to that canonical work. A fixed priority
    order (sefaria < ja < msource), computed once, never per-card.
    """
    cur = conn.execute(
        """
        SELECT DISTINCT w.canonical_work_id, w.source_corpus
        FROM discovery_claim dc
        JOIN works w ON w.work_id = dc.work_id
        JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
        WHERE de.routing_status = 'shipped'
        """
    )
    best: Dict[str, str] = {}
    for canonical_work_id, source_corpus in cur.fetchall():
        rank = CORPUS_RANK.get(source_corpus, 99)
        cur_best = best.get(canonical_work_id)
        if cur_best is None or rank < CORPUS_RANK.get(cur_best, 99):
            best[canonical_work_id] = source_corpus
    return best


def load_unit_key_map(conn: sqlite3.Connection) -> Dict[str, str]:
    """`sys_id -> unit_id` for every `witness_unit_members` row (DATA-10)."""
    cur = conn.execute("SELECT sys_id, unit_id FROM witness_unit_members")
    return {sys_id: unit_id for sys_id, unit_id in cur.fetchall()}


def unit_key_of(sys_id: str, unit_map: Dict[str, str]) -> str:
    """Protocol §1.3: `COALESCE(witness_unit_members.unit_id, 'sys:'||sys_id)`."""
    return unit_map.get(sys_id) or ("sys:" + sys_id)


def _page_norm_letters_batch(research_conn: sqlite3.Connection, page_ids: Sequence[str]) -> Dict[str, int]:
    """Batch-fetch + normalize page text -> `{page_id: page_norm_letters}`
    (the Lever-1 coverage denominator, via the FROZEN
    `build_discovery_sidecar.norm_stream_letter_count` port -- never re-derived)."""
    out: Dict[str, int] = {}
    ids_list = list(dict.fromkeys(page_ids))
    chunk_size = 500
    for i in range(0, len(ids_list), chunk_size):
        chunk = ids_list[i:i + chunk_size]
        qmarks = ",".join("?" for _ in chunk)
        cur = research_conn.execute(
            f"SELECT page_id, text FROM pages WHERE page_id IN ({qmarks})", chunk
        )
        for page_id, text in cur.fetchall():
            out[page_id] = sidecar_build.norm_stream_letter_count(text)
    for pid in ids_list:
        out.setdefault(pid, 0)
    return out


def compute_estimand_rows(sidecar_db_path, research_db_path,
                          dropped_work_ids: Sequence[str] = DROPPED_WORK_IDS) -> List[dict]:
    """The frozen CERT-01 estimand (protocol §1.1/§1.2): one row per
    surviving (page_id, canonical_work_id) tier_a SHIPPED claim, with its
    stratum (source corpus x coverage band) and `unit_key` attached.

    Requires BOTH the deployed sidecar (for the claim/evidence/works/
    witness_unit_members tables) AND the research corpus DB (for the page
    text the Lever-1 coverage denominator is computed over -- the sidecar
    does not persist `page_norm_letters`, only `matched_letters`).
    """
    conn = _connect_ro(sidecar_db_path)
    try:
        sql = _RANKED_ESTIMAND_SQL_TEMPLATE.format(dropped=_dropped_sql_literal(dropped_work_ids))
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        raw_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        stratum_corpus_map = resolve_stratum_corpus_map(conn)
        unit_map = load_unit_key_map(conn)
    finally:
        conn.close()

    research_conn = _connect_ro(research_db_path)
    try:
        page_norm = _page_norm_letters_batch(research_conn, [r["a_page_id"] for r in raw_rows])
    finally:
        research_conn.close()

    out: List[dict] = []
    for r in raw_rows:
        coverage = sidecar_build.compute_page_coverage(r["matched_letters"], page_norm.get(r["a_page_id"]))
        coverage_band = "high" if (coverage or 0.0) >= COVERAGE_HIGH_FLOOR else "medium"
        stratum_corpus = stratum_corpus_map.get(r["canonical_work_id"], r["source_corpus"])
        stratum = f"{stratum_corpus}:{coverage_band}"
        out.append({
            "page_id": r["page_id"],
            "canonical_work_id": r["canonical_work_id"],
            "work_id": r["work_id"],
            "claim_id": r["claim_id"],
            "display_evidence_id": r["display_evidence_id"],
            "sys_id": r["sys_id"],
            "unit_key": unit_key_of(r["sys_id"], unit_map),
            "coverage": coverage,
            "stratum_corpus": stratum_corpus,
            "coverage_band": coverage_band,
            "stratum": stratum,
            # Rendering-only fields (NEVER part of population_hash/cluster_map_hash,
            # which key ONLY on page_id/canonical_work_id/stratum/unit_key -- see
            # population_hash()/cluster_map_hash() below). Carried here so a deck
            # renderer can look up the matched span without a second query.
            "a_page_id": r["a_page_id"],
            "span_start": r["span_start"],
            "span_end": r["span_end"],
        })
    return out


# ---------------------------------------------------------------------------
# Hashes (protocol §1.3, §5.1)
# ---------------------------------------------------------------------------

def population_hash(rows: Sequence[dict]) -> str:
    """SHA-256 over the sorted `(page_id, canonical_work_id, stratum)` triples
    (protocol §5.1), one triple per line, `|`-delimited, UTF-8 encoded."""
    lines = sorted(f"{r['page_id']}|{r['canonical_work_id']}|{r['stratum']}" for r in rows)
    key = "\n".join(lines)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def cluster_map_hash(rows: Sequence[dict]) -> str:
    """SHA-256 over the sorted `(page_id, canonical_work_id, unit_key)`
    triples (protocol §1.3), one triple per line, `|`-delimited, UTF-8."""
    lines = sorted(f"{r['page_id']}|{r['canonical_work_id']}|{r['unit_key']}" for r in rows)
    key = "\n".join(lines)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def stratum_counts(rows: Sequence[dict]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["stratum"]] = counts.get(r["stratum"], 0) + 1
    return dict(sorted(counts.items()))


def cluster_sizes(rows: Sequence[dict]) -> List[int]:
    """Realized physMS cluster (`unit_key`) size distribution over the WHOLE
    estimand -- the frame geometry the OC table's ICC-adjusted sizing (§6)
    and the deck draw's clustered bootstrap (§1.5) both depend on."""
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["unit_key"]] = counts.get(r["unit_key"], 0) + 1
    return sorted(counts.values(), reverse=True)


def hash_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_input_hashes(sidecar_db_path, manifest_path, *,
                      canonical_merges_path: Optional[str] = None,
                      composition_dates_path: Optional[str] = None,
                      seftja_dates_path: Optional[str] = None,
                      crosswalk_path: Optional[str] = None) -> Dict[str, str]:
    """Reads/recomputes the four frozen input hashes + `db_content_hash` +
    `crosswalk_sha256` (protocol §5.1).

    `canonical_merges_sha256` / `composition_dates_sha256` /
    `seftja_dates_sha256` / `crosswalk_sha256` are read from the deployed
    sidecar's OWN `meta` table (written there at bake time -- the
    discovery-sidecar-schema-v1.md §"Amendment 2026-07-24" #4 provenance
    keys). `db_content_hash` comes from `discovery_data/manifest.json`'s
    `content_hash` field (NOT `frame_content_hash` -- this is the literal
    content hash of the shipped `.db` asset itself). When the optional
    `*_path` arguments are supplied, this ALSO independently recomputes each
    hash from the actual input file on disk and asserts it matches the
    sidecar-stored value (fail loud on mismatch) -- the "recompute, never
    trust" discipline the protocol itself specifies for the verifier; the
    freeze script uses this same recompute-and-compare so a stale/tampered
    input can never silently enter the pre-registration.
    """
    conn = _connect_ro(sidecar_db_path)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        frame_content_hash = sidecar_build.compute_frame_content_hash(conn)
    finally:
        conn.close()
    if frame_content_hash != meta.get("frame_content_hash"):
        raise ValueError(
            "frame_content_hash recompute mismatch: stored "
            f"{meta.get('frame_content_hash')!r} != recomputed {frame_content_hash!r}"
        )
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    db_content_hash = hash_file(sidecar_db_path)
    if db_content_hash != manifest.get("content_hash"):
        raise ValueError(
            "db_content_hash recompute mismatch: manifest "
            f"{manifest.get('content_hash')!r} != recomputed {db_content_hash!r}"
        )

    out = {
        "canonical_merges_sha256": meta["canonical_merges_sha256"],
        "composition_dates_sha256": meta["composition_dates_sha256"],
        "seftja_dates_sha256": meta["seftja_dates_sha256"],
        "db_content_hash": db_content_hash,
        "crosswalk_sha256": meta["crosswalk_sha256"],
        "frame_content_hash": frame_content_hash,
    }

    checks = {
        "canonical_merges_sha256": canonical_merges_path,
        "composition_dates_sha256": composition_dates_path,
        "seftja_dates_sha256": seftja_dates_path,
        "crosswalk_sha256": crosswalk_path,
    }
    for key, path in checks.items():
        if path is None:
            continue
        recomputed = hash_file(path)
        if recomputed != out[key]:
            raise ValueError(
                f"{key} recompute mismatch: sidecar meta {out[key]!r} != "
                f"recomputed-from-{path} {recomputed!r}"
            )
    return out


# ---------------------------------------------------------------------------
# report_id construction (protocol §5.2) -- self-referential, finite,
# well-defined: SHA-256 over the canonical JSON serialization of the payload
# WITH its own `report_id` field OMITTED.
# ---------------------------------------------------------------------------

def canonical_json_minus_report_id(payload: dict) -> str:
    trimmed = {k: v for k, v in payload.items() if k != "report_id"}
    return json.dumps(trimmed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_report_id(payload: dict) -> str:
    """Bare hex digest -- callers prefix `cert-tier_a-` for the citable
    identifier (protocol §5.2)."""
    ser = canonical_json_minus_report_id(payload)
    return hashlib.sha256(ser.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Stratified card allocation (protocol §3/§7): largest-remainder method with
# a floor so no stratum is starved.
# ---------------------------------------------------------------------------

def allocate_stratum_cards(counts: Dict[str, int], total: int, min_per_stratum: int = 15) -> Dict[str, int]:
    """Proportional (largest-remainder / Hamilton) allocation of `total`
    cards across strata by realized frame size, with a floor of
    `min_per_stratum` per non-empty stratum (never exceeding that stratum's
    own available row count)."""
    strata = sorted(counts)
    n_strata = len(strata)
    if n_strata == 0:
        return {}
    grand_total = sum(counts.values())
    floor_alloc = {
        s: min(min_per_stratum, counts[s]) for s in strata
    }
    remaining = total - sum(floor_alloc.values())
    if remaining <= 0:
        return floor_alloc
    # Proportional shares of the REMAINING budget over the room each stratum
    # has left above its floor (never exceeding its own frame size).
    room = {s: counts[s] - floor_alloc[s] for s in strata}
    total_room = sum(room.values())
    alloc = dict(floor_alloc)
    if total_room <= 0:
        return alloc
    exact_shares = {s: remaining * (room[s] / total_room) for s in strata}
    base = {s: int(exact_shares[s]) for s in strata}
    for s in strata:
        base[s] = min(base[s], room[s])
        alloc[s] += base[s]
    leftover = total - sum(alloc.values())
    remainders = sorted(
        strata, key=lambda s: (exact_shares[s] - int(exact_shares[s])), reverse=True
    )
    i = 0
    while leftover > 0 and i < len(remainders) * 4:
        s = remainders[i % len(remainders)]
        if alloc[s] < counts[s]:
            alloc[s] += 1
            leftover -= 1
        i += 1
    return alloc
