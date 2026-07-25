"""Frozen id / validator / router primitives for the `discovery.db` sidecar.

This module implements EXACTLY the recipes frozen in
`docs/specs/discovery-sidecar-schema-v1.md` (Phase 134, plan 134-01, Task 2):
deterministic `claim_id`/`evidence_id`/`unit_id` hashing, opaque `work_id`
minting, enum validators, the `corroborated_predicate` two-seed test, the
work-witness `claim_type` dominance rule, the parent-claim `claim_type`
collision resolver, and the total display-evidence precedence lattice.

Every downstream 134 plan (fixture generation, distillation build, the
`shared/discovery_service.py` async chokepoint, and the release verifier)
imports this module as the ONE source of truth for these primitives and the
frozen enum vocabularies.

Deliberately stdlib-only (hashlib, json) -- NO web-framework import of any
kind -- so this module stays importable from both the offline build script
(which never needs a web framework) and `shared/discovery_service.py`
(which must stay web-free per the shared/->web/ back-edge convention used
throughout this codebase, e.g. `shared/api_errors.py`).

Masking note: this module names the reference corpora ONLY via the masked
`source_corpus` codes {sefaria, ja, msource} -- it contains no restricted
corpus name, no raw research work_id value, and no reference text. The
opaque `work_id` minted by `mint_work_id()` is a plain zero-padded counter
string; the function never sees or echoes a raw research identifier, so it
provably cannot emit an `M:`/`J:`/`REF`-shaped token or a filename stem --
the raw-id -> opaque-id crosswalk lives entirely with the (out-of-repo)
caller.
"""

from __future__ import annotations

import hashlib
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Frozen enum vocabularies (docs/specs/discovery-sidecar-schema-v1.md, "Frozen
# Enum Vocabularies"). ONE source of truth -- build/service/tests import these,
# never redeclare the literal string sets.
# ---------------------------------------------------------------------------

CLAIM_TYPE_DIRECT_WITNESS = "direct_witness"
CLAIM_TYPE_QUOTES_THIS_WORK = "quotes_this_work"
CLAIM_TYPE_SHARED_TEXT = "shared_text"
CLAIM_TYPES = frozenset({
    CLAIM_TYPE_DIRECT_WITNESS,
    CLAIM_TYPE_QUOTES_THIS_WORK,
    CLAIM_TYPE_SHARED_TEXT,
})

EVIDENCE_KIND_WITNESS = "witness"
EVIDENCE_KIND_SHARED_TEXT = "shared_text"
EVIDENCE_KINDS = frozenset({EVIDENCE_KIND_WITNESS, EVIDENCE_KIND_SHARED_TEXT})

EVIDENCE_SOURCE_TRACK1_DIRECT = "track1_direct"
EVIDENCE_SOURCE_PROPAGATED = "propagated"
EVIDENCE_SOURCES = frozenset({EVIDENCE_SOURCE_TRACK1_DIRECT, EVIDENCE_SOURCE_PROPAGATED})

# Reserved future evidence_source (C-9) -- documented only, NO v1 data, NEVER
# a member of EVIDENCE_SOURCES until a versioned gen-2 rebuild adds it.
RESERVED_EVIDENCE_SOURCE_CATALOG_PROPAGATED = "catalog_propagated"

CONFIDENCE_BAND_EXPERT_VERIFIED = "expert_verified"
# v2 rename of the track1_direct top band (discovery-band-labels-v1.md §5).
# `expert_verified` renames to `high_confidence_algorithmic` at the
# discovery-v2 bake; the v1 key is RETAINED here through the transition for
# read-compat (the live v1 asset + the v1 fixture tests still read it) and is
# not dropped until the v2 manifest is live (Codex #8). The 135-01 values
# module's `_canon_band_key` maps the v1 key to the v2 key for display so one
# label serves both.
CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC = "high_confidence_algorithmic"
CONFIDENCE_BAND_TIER_A = "tier_a"
CONFIDENCE_BAND_SCREENING_RB = "screening_rb"
CONFIDENCE_BAND_SCREENING_CANON = "screening_canon"
CONFIDENCE_BAND_CORROBORATED = "corroborated"
CONFIDENCE_BAND_WEAK = "weak"
CONFIDENCE_BAND_NOT_EVALUATED = "not_evaluated"

CONFIDENCE_BANDS_BY_SOURCE: Dict[str, frozenset] = {
    EVIDENCE_SOURCE_TRACK1_DIRECT: frozenset({
        # Both the v2 key and the v1 key are accepted through the rename
        # transition (v1-read-compat, Codex #8) -- the built v2 asset uses
        # ONLY the v2 key (the verifier's no-mixed-enum-state check enforces
        # that at 135-06/135-07), while the runtime accepts whichever key the
        # currently-loaded asset carries.
        CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
        CONFIDENCE_BAND_EXPERT_VERIFIED,
        CONFIDENCE_BAND_TIER_A,
        CONFIDENCE_BAND_SCREENING_RB,
        CONFIDENCE_BAND_SCREENING_CANON,
    }),
    EVIDENCE_SOURCE_PROPAGATED: frozenset({
        CONFIDENCE_BAND_CORROBORATED,
        CONFIDENCE_BAND_WEAK,
        CONFIDENCE_BAND_NOT_EVALUATED,
    }),
}

ADJUDICATION_STATUS_HUMAN_CONFIRMED = "human_confirmed"
ADJUDICATION_STATUS_PROVISIONAL = "provisional"
ADJUDICATION_STATUS_UNREVIEWED = "unreviewed"
ADJUDICATION_STATUSES = frozenset({
    ADJUDICATION_STATUS_HUMAN_CONFIRMED,
    ADJUDICATION_STATUS_PROVISIONAL,
    ADJUDICATION_STATUS_UNREVIEWED,
})

AUDIT_STATUS_AUDIT_PENDING = "audit_pending"
AUDIT_STATUS_AUDIT_PASSED = "audit_passed"  # registry-gated; NO v1 row ever sets this (C-6)
AUDIT_STATUS_NA = "n/a"
AUDIT_STATUSES = frozenset({AUDIT_STATUS_AUDIT_PENDING, AUDIT_STATUS_AUDIT_PASSED, AUDIT_STATUS_NA})

ROUTING_STATUS_SHIPPED = "shipped"
ROUTING_STATUS_REVIEW_ONLY = "review_only"
ROUTING_STATUSES = frozenset({ROUTING_STATUS_SHIPPED, ROUTING_STATUS_REVIEW_ONLY})

ROUTING_REASON_IMPURITY = "impurity"
ROUTING_REASON_RUNNER_UP_CONFLICT = "runner_up_conflict"
ROUTING_REASON_CO_CITATION = "co_citation"
ROUTING_REASON_NONE = "none"
# D-17 coarse chronological demotion (Phase 135, v2 bake): the later of two
# co-claiming works whose text is shared is routed to review_only with this
# reason. Added to the frozen vocab in 135-05; the demotion that WRITES it
# (and the discovery_routing_audit row recording each decision) lands in the
# v2 build logic (135-06).
ROUTING_REASON_LATER_SHARED_TEXT = "later_shared_text"
# Lever-1 coverage demotion (Phase 135, v2 bake, bake plan §4.4): a
# track1_direct witness whose page coverage (matched_letters / normalized page
# length) falls below the 0.45 cliff is routed to review_only with this reason.
# Added to the frozen vocab in 135-07 (the coverage-metric fix); WITHOUT a
# distinguishing reason a review_only row's cause (Lever-1 vs D-17
# 'later_shared_text' vs a §4.5 reband) could not be reconstructed from the
# shipped asset alone (Codex R3-BLOCKER -- breaks gate 10 replayability).
ROUTING_REASON_LOW_COVERAGE = "low_coverage"
ROUTING_REASONS = frozenset({
    ROUTING_REASON_IMPURITY,
    ROUTING_REASON_RUNNER_UP_CONFLICT,
    ROUTING_REASON_CO_CITATION,
    ROUTING_REASON_NONE,
    ROUTING_REASON_LATER_SHARED_TEXT,
    ROUTING_REASON_LOW_COVERAGE,
})

MERGE_BASIS_OXFORD_PART = "oxford_part"
MERGE_BASIS_PHYSICAL_JOIN = "physical_join"
MERGE_BASES = frozenset({MERGE_BASIS_OXFORD_PART, MERGE_BASIS_PHYSICAL_JOIN})

# Masked source_corpus codes ONLY -- internal, never displayed (D-03a).
SOURCE_CORPUS_SEFARIA = "sefaria"
SOURCE_CORPUS_JA = "ja"
SOURCE_CORPUS_MSOURCE = "msource"
SOURCE_CORPUS_CODES = frozenset({SOURCE_CORPUS_SEFARIA, SOURCE_CORPUS_JA, SOURCE_CORPUS_MSOURCE})


def validate_source_corpus_code(code: str) -> str:
    """Raise ValueError unless `code` is one of the frozen masked codes.

    Returns the code unchanged on success (so callers can chain this as a
    validating pass-through).
    """
    if code not in SOURCE_CORPUS_CODES:
        raise ValueError(
            f"invalid source_corpus code: {code!r} "
            f"(must be one of {sorted(SOURCE_CORPUS_CODES)})"
        )
    return code


# ---------------------------------------------------------------------------
# Frozen id recipes (docs/specs/discovery-sidecar-schema-v1.md §2). Exact
# field order + "|" delimiter + UTF-8 + SHA-256, pinned by the golden test in
# tests/test_discovery_ids.py.
# ---------------------------------------------------------------------------

_NAMESPACE_CLAIM = "discovery_claim_v1"
_NAMESPACE_EVIDENCE = "discovery_evidence_v1"


def claim_id(page_id: str, work_id: str) -> str:
    """Stable sha256 hex digest for a (page_id, work_id) claim key.

    NOT a function of claim_type (G5) -- claim_type is a stored DERIVED
    attribute that can flip when evidence is added, so hashing it would
    break claim_id stability across the claim's lifetime.
    """
    key = f"{_NAMESPACE_CLAIM}|{page_id}|{work_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _seed_spans_digest(seed_spans: Optional[Sequence[Mapping]]) -> str:
    """Deterministic digest over a sorted seed_spans list (R4/G3).

    Each element is a dict carrying at least occ0/occ1, optionally
    occ_class/seed_page_ids/seed_sys_ids. Sorted by (occ0, occ1) so the
    digest is order-invariant over how the caller assembled the list.
    Returns "" for an empty/None list (evidence rows with a single
    occurrence -- track1_direct/E1/shared_text rows -- pass None).
    """
    if not seed_spans:
        return ""
    ordered = sorted(seed_spans, key=lambda s: (s.get("occ0", 0), s.get("occ1", 0)))
    parts = []
    for s in ordered:
        seed_page_ids = "+".join(sorted(str(x) for x in (s.get("seed_page_ids") or [])))
        seed_sys_ids = "+".join(sorted(str(x) for x in (s.get("seed_sys_ids") or [])))
        parts.append(
            f"{s.get('occ0')}:{s.get('occ1')}:{s.get('occ_class', '')}:"
            f"{seed_page_ids}:{seed_sys_ids}"
        )
    digest_key = "|".join(parts)
    return hashlib.sha256(digest_key.encode("utf-8")).hexdigest()


def evidence_id(
    work_id: str,
    a_page_id: str,
    sys_id: str,
    evidence_kind: str,
    evidence_source: str,
    confidence_band: str,
    span_start,
    span_end,
    other_page_id: Optional[str] = None,
    seed_spans: Optional[Sequence[Mapping]] = None,
) -> str:
    """Stable sha256 hex digest identifying one discovery_evidence row.

    Folds a deterministic digest of the sorted seed_spans list so the id
    stays collision-free within a claim ACROSS the R4 multi-span expansion
    (1,912 propagated witness rows carry >=2 distinct candidate-side
    occurrences).
    """
    seed_spans_digest = _seed_spans_digest(seed_spans)
    fields = [
        _NAMESPACE_EVIDENCE,
        work_id,
        a_page_id,
        sys_id,
        evidence_kind,
        evidence_source,
        confidence_band,
        span_start,
        span_end,
        other_page_id or "",
        seed_spans_digest,
    ]
    key = "|".join(str(f) for f in fields)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def unit_id(member_sys_ids: Iterable[str]) -> str:
    """Stable sha256 hex digest over sorted member sys_ids (DATA-10).

    Sorted as STRINGS (byte/lexicographic collation) so the result is
    order-invariant over the caller's input membership order.
    """
    key = "unit|" + "|".join(sorted(str(x) for x in member_sys_ids))
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def mint_work_id(counter: int) -> str:
    """Mint an opaque, 1:1 product work_id from a monotonic counter.

    The raw research work_id -> opaque work_id crosswalk (persisted so the
    same raw id always mints the same opaque id) is a CALLER responsibility,
    entirely outside this module -- this function only ever sees and echoes
    an integer counter, so it provably never emits a raw `M:`/`J:`/`REF`
    token or a filename-shaped stem.
    """
    return f"w{int(counter):06d}"


def canonical_work_id(work_id: str, cross_corpus_map: Optional[Mapping[str, str]] = None) -> str:
    """Cross-corpus canonical work_id; DEFAULTS to work_id (F16).

    Cross-corpus canonical merges are a gen-2 versioned REBUILD, never a v1
    migration (D-03b) -- `cross_corpus_map` is None at v1 launch.
    """
    if cross_corpus_map:
        return cross_corpus_map.get(work_id, work_id)
    return work_id


# ---------------------------------------------------------------------------
# Propagated-witness banding (C-4/R1/R3): the literal corroborated predicate
# + its documented (but NOT auto-applied) impurity definition.
# ---------------------------------------------------------------------------

def is_impure(row: Mapping) -> bool:
    """Documented impurity definition for build-time re-validation ONLY.

    impurity == (runner_up >= 0.5 * support) AND (support > 0). This helper
    is NOT called by `corroborated_predicate` below -- that function trusts
    the already-router-cleaned collection's own `impurity` field, per the
    frozen recipe. `is_impure` exists so the offline build can independently
    re-derive impurity from `runner_up`/`support` when validating a fresh
    router export against the shipped collection.
    """
    runner_up = row.get("runner_up") or 0
    support = row.get("support") or 0
    return bool(support > 0 and runner_up >= 0.5 * support)


def corroborated_predicate(row: Mapping) -> bool:
    """The LITERAL two-seed `corroborated` test (C-4/R1/R3).

    True iff:
        row['_bucket'] == 'witness'
        AND row.get('is_new')
        AND NOT row.get('impurity')
        AND ('trials' in row AND row['trials'] >= 2)

    Trusts the router-cleaned collection's own `impurity` field (does NOT
    recompute it -- see `is_impure` above for the build-time re-derivation
    helper). Requiring `_bucket == 'witness'` means this ALWAYS returns
    False for a family-router row (`_bucket` in {tafsir_targum,
    with_arabic}, R3) -- the build must never run witness banding on
    router rows at all; they route to shared_text/not_evaluated/
    review_only/co_citation instead (docs/specs/discovery-sidecar-schema-v1.md §4.2).
    """
    return bool(
        row.get("_bucket") == "witness"
        and row.get("is_new")
        and not row.get("impurity")
        and "trials" in row
        and row.get("trials", 0) >= 2
    )


# ---------------------------------------------------------------------------
# claim_type routing (C-2/§3): the work-witness dominance rule + the
# parent-claim collision resolver (F7). NO flank_class -> claim_type router
# exists in this model (that MS-MS/Track-2 framing is superseded).
# ---------------------------------------------------------------------------

def claim_type_for_work_witness(span_lengths_on_page: Sequence[int], this_span_len: int) -> str:
    """TOTAL largest-span-dominates rule (docs §3).

    `direct_witness` when `this_span_len` is the largest (or ties for
    largest) among all witness spans on the page; `quotes_this_work`
    otherwise. A page with a single live witness identification (a
    one-element `span_lengths_on_page`, or an empty list meaning "no other
    competing span is known") always yields `direct_witness` -- there is no
    competing span to be dominated by.
    """
    if not span_lengths_on_page:
        return CLAIM_TYPE_DIRECT_WITNESS
    if this_span_len >= max(span_lengths_on_page):
        return CLAIM_TYPE_DIRECT_WITNESS
    return CLAIM_TYPE_QUOTES_THIS_WORK


def resolve_claim_type(evidence_rows: Iterable[Mapping]) -> str:
    """TOTAL parent-claim claim_type collision resolver (F7).

    `evidence_rows` is the complete set of discovery_evidence rows for ONE
    (page_id, work_id) claim. Each row is a mapping with at least
    `evidence_kind`; witness rows (`evidence_kind == 'witness'`) SHOULD also
    carry a `claim_type` key already resolved per-row by
    `claim_type_for_work_witness` (computed across the page, cross-claim,
    by the build). Witness evidence -- track1_direct OR propagated -- ALWAYS
    dominates: the claim's claim_type is the witness rule
    (direct_witness/quotes_this_work) whenever ANY witness evidence row is
    present, regardless of any co-occurring shared_text evidence (the
    43,046-row collision, F7). claim_type=shared_text ONLY when the claim
    has shared_text evidence and NO witness evidence at all.
    """
    rows = list(evidence_rows)
    witness_rows = [r for r in rows if r.get("evidence_kind") == EVIDENCE_KIND_WITNESS]
    if not witness_rows:
        return CLAIM_TYPE_SHARED_TEXT
    for r in witness_rows:
        if r.get("claim_type") == CLAIM_TYPE_DIRECT_WITNESS:
            return CLAIM_TYPE_DIRECT_WITNESS
    for r in witness_rows:
        if r.get("claim_type") == CLAIM_TYPE_QUOTES_THIS_WORK:
            return CLAIM_TYPE_QUOTES_THIS_WORK
    # Witness evidence present but no row carried a precomputed claim_type
    # (e.g. a caller passed bare evidence_kind markers only) -- a witness
    # claim with no competing span defaults to direct_witness, mirroring
    # claim_type_for_work_witness's own single-claim-page default.
    return CLAIM_TYPE_DIRECT_WITNESS


# ---------------------------------------------------------------------------
# Display-evidence precedence lattice (C-5/R6/§6). TOTAL over every
# (evidence_source, confidence_band, adjudication_status) combination.
# ---------------------------------------------------------------------------

# Global band-rank, strongest first (docs §6 item 2). Expressed as rank
# GROUPS: the track1_direct top tier occupies ONE shared rank across BOTH the
# v1 key (`expert_verified`) AND its v2 rename
# (`high_confidence_algorithmic`) -- DUAL-KEY, never version-branched (mirrors
# how `CONFIDENCE_BANDS_BY_SOURCE` accepts both keys through the transition).
# Without this, a v2 top-tier row would fall to `_UNRANKED_BAND` in
# `_display_sort_key` and CORRUPT `display_evidence_id` selection for the
# highest-confidence evidence (auditor catch #9). Within any single shipped
# asset only ONE of the two keys is ever present (the verifier's
# no-mixed-enum-state check enforces that), so their shared rank never needs a
# tie-break between them; every OTHER pair keeps its exact prior rank order.
_BAND_RANK_GROUPS: List[List] = [
    [(EVIDENCE_SOURCE_TRACK1_DIRECT, CONFIDENCE_BAND_EXPERT_VERIFIED),
     (EVIDENCE_SOURCE_TRACK1_DIRECT, CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC)],
    [(EVIDENCE_SOURCE_TRACK1_DIRECT, CONFIDENCE_BAND_TIER_A)],
    [(EVIDENCE_SOURCE_PROPAGATED, CONFIDENCE_BAND_CORROBORATED)],
    [(EVIDENCE_SOURCE_TRACK1_DIRECT, CONFIDENCE_BAND_SCREENING_RB)],
    [(EVIDENCE_SOURCE_TRACK1_DIRECT, CONFIDENCE_BAND_SCREENING_CANON)],
    [(EVIDENCE_SOURCE_PROPAGATED, CONFIDENCE_BAND_WEAK)],
    [(EVIDENCE_SOURCE_PROPAGATED, CONFIDENCE_BAND_NOT_EVALUATED)],
]
_BAND_RANK_INDEX = {
    pair: i for i, group in enumerate(_BAND_RANK_GROUPS) for pair in group
}
_UNRANKED_BAND = len(_BAND_RANK_GROUPS)  # any (source, band) combo not in the lattice sorts last

_ADJUDICATION_RANK = {
    ADJUDICATION_STATUS_HUMAN_CONFIRMED: 0,
    ADJUDICATION_STATUS_PROVISIONAL: 1,
    ADJUDICATION_STATUS_UNREVIEWED: 2,
}
_UNRANKED_ADJUDICATION = len(_ADJUDICATION_RANK)


def _display_sort_key(row: Mapping):
    # v2 (135-06, Pitfall 2 / bake plan §4.3): a `routing_status` tier ABOVE
    # every band signal -- a SHIPPED evidence row ALWAYS outranks a
    # `review_only` (D-17- or Lever-1-demoted, or §4.5-rebanded) sibling, so a
    # demoted row can never become the `display_evidence_id` of a claim that
    # also owns a shipped row. Backward compatible: a row WITHOUT a
    # `routing_status` key (or explicitly `shipped`) ranks 0 identically, so
    # every pre-v2 caller (whose selector rows carried no routing_status) sorts
    # exactly as before.
    routing_rank = 1 if row.get("routing_status") == ROUTING_STATUS_REVIEW_ONLY else 0
    is_human_confirmed_direct = (
        row.get("evidence_source") == EVIDENCE_SOURCE_TRACK1_DIRECT
        and row.get("adjudication_status") == ADJUDICATION_STATUS_HUMAN_CONFIRMED
    )
    dominance_rank = 0 if is_human_confirmed_direct else 1
    band_rank = _BAND_RANK_INDEX.get(
        (row.get("evidence_source"), row.get("confidence_band")), _UNRANKED_BAND
    )
    adjudication_rank = _ADJUDICATION_RANK.get(
        row.get("adjudication_status"), _UNRANKED_ADJUDICATION
    )
    return (routing_rank, dominance_rank, band_rank, adjudication_rank, str(row.get("evidence_id", "")))


def select_display_evidence(evidence_rows: Iterable[Mapping]) -> str:
    """Return the winning `evidence_id` under the frozen TOTAL lattice.

    Priority order: (1) family-specific human_confirmed dominance -- any
    track1_direct row with adjudication_status=human_confirmed outranks
    EVERY non-(human_confirmed track1_direct) row, across ALL four
    track1_direct bands; (2) global band-rank over (evidence_source,
    confidence_band); (3) adjudication_status (human_confirmed <
    provisional < unreviewed); (4) evidence_id lexicographic tie-break.
    """
    rows = list(evidence_rows)
    if not rows:
        raise ValueError(
            "select_display_evidence requires >=1 evidence row "
            "(the 'every claim has nonempty evidence' invariant)"
        )
    best = min(rows, key=_display_sort_key)
    return best.get("evidence_id")
