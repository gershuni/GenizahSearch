# -*- coding: utf-8 -*-
"""The surface-facing contract for every Discovery read (Phase 136, plan 136-14).

Two things live here, and NOTHING else may re-implement either of them:

1. **The status envelope** (D-13). Today's read wrappers collapse a query
   timeout, an overload rejection, an absent sidecar and a genuine zero all to
   `[]`, so an outage renders as "this manuscript has no identifications" --
   which is exactly the shape the panel's hide-on-zero rule would then act on.
   `make_envelope` returns `{status, items, total, meta}` with a CLOSED
   four-value status vocabulary (`SURFACE_STATUSES`), so a surface cannot
   invent a fifth state and cannot mistake an outage for a zero.

2. **The allowlist projection** (`surface_safe_*`). `serialize_banded_claim`
   ALWAYS emits `review_overlay`, and for a `human_confirmed` row that value is
   the literal "Expert-reviewed" badge D-13f has decided not to show. A
   renderer-level assertion cannot stop that string reaching an envelope, a
   JSON payload or an error message -- by then it has already left the service.
   So every row a surface receives is projected through an ALLOWLIST that names
   the keys a surface may see and drops everything else. An allowlist rather
   than a denylist, so a field added to the serializer later is excluded by
   DEFAULT instead of leaking by default.

`FORBIDDEN_SURFACE_FIELDS` (plus `_FORBIDDEN_SUBSTRINGS`) is the belt-and-braces
half: it validates the ALLOWLISTS THEMSELVES at import time, and
`make_envelope` re-checks every item and the meta block, so a forbidden key
cannot reach a surface even through a hand-built envelope.

Masking (D-25/NOVEL-02): nothing here names a corpus. `novelty_source_label` is
passed through because it is already the MASKED label set
(`shared.discovery_novelty.MASKED_PROVENANCE_LABELS`); the raw provenance value
never exists in the asset at all.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# D-13: the closed status vocabulary. FOUR values, pairwise distinct.
# ---------------------------------------------------------------------------

STATUS_OK = "ok"
STATUS_UNAVAILABLE = "unavailable"   # flag off / asset absent / loader not ready
STATUS_TIMEOUT = "timeout"           # the query exceeded its budget
STATUS_BUSY = "busy"                 # bounded-concurrency rejection

SURFACE_STATUSES: frozenset = frozenset({
    STATUS_OK, STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY,
})

# The three non-`ok` states share one property the surface depends on: they
# are OUTAGES, not zeros. Exported so a surface can ask the question directly
# rather than re-deriving `status != 'ok'` at each call site.
OUTAGE_STATUSES: frozenset = frozenset({
    STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY,
})


# ---------------------------------------------------------------------------
# The forbidden set (D-06 / D-13f / T-136-14-09).
# ---------------------------------------------------------------------------

#: Exact keys that may NEVER reach a surface, in a row, an envelope, a JSON
#: payload or an error path.
FORBIDDEN_SURFACE_FIELDS: frozenset = frozenset({
    # D-13f: the review overlay is the "Expert-reviewed" badge itself.
    "review_overlay",
    "review_badge",
    # D-06: no precision value, and no by-source breakdown, on ANY surface.
    "precision",
    "precision_copy",
    "precision_pct",
    "band_precision",
    "numerator",
    "denominator",
    "draw_size",
    # D-06: no confidence interval.
    "ci_low",
    "ci_high",
    "confidence_interval",
    "ci",
})

#: Substrings that make a key forbidden regardless of its exact spelling -- so a
#: future `estimated_precision` / `band_ci_low` cannot slip past the exact set.
_FORBIDDEN_SUBSTRINGS: Tuple[str, ...] = (
    "precision",
    "review_overlay",
    "confidence_interval",
    "ci_low",
    "ci_high",
)


def is_forbidden_surface_field(key: Any) -> bool:
    """True when `key` may never appear in surface-bound output."""
    if not isinstance(key, str):
        return False
    if key in FORBIDDEN_SURFACE_FIELDS:
        return True
    lowered = key.lower()
    return any(token in lowered for token in _FORBIDDEN_SUBSTRINGS)


# ---------------------------------------------------------------------------
# The allowlists -- one per surface row kind.
# ---------------------------------------------------------------------------

#: PANEL-01/02: one identification row on the browse panel. Every field the
#: panel renders, delivered by ONE query (a per-row follow-up would multiply
#: browse-enrichment latency by the row count).
SURFACE_CLAIM_FIELDS: Tuple[str, ...] = (
    # identity of the row itself
    "page_id",
    "sys_id",
    "claim_id",
    "evidence_id",
    # the identified work (D-13a: the DISPLAY work's title wins)
    "work_id",
    "canonical_work_id",
    "display_work_id",
    "neutral_title",
    "author",
    "genre",
    "title_missing",
    # relation + band (the chip carries the relation; the band label is the
    # tooltip -- A-2). `band_label` comes from `serialize_banded_claim`, never
    # from a hardcoded string in a query module.
    "relation_kind",
    "evidence_source",
    "confidence_band",
    "band_label",
    "band_rank",
    # coverage (D-08a: direct family only; `coverage_status` is the VALIDITY
    # axis, so an absent value is never misread as zero coverage)
    "coverage_ppm",
    "coverage_status",
    # bucket (A-1: main pool / more matches, materialized at build time)
    "main_pool",
    "main_pool_reason",
    "identification_id",
    "identification_page_count",
    # novelty (masked label only)
    "novelty_status",
    "novelty_source_label",
    # evidence shape
    "matched_letters",
    "span_start",
    "span_end",
    "n_spans",
    # D-13g: WHY this row is on the surface, and the coverage note it carries
    "eligibility_basis",
    "restored_by_human_confirmation",
    "low_coverage_marker",
    # stored vocabulary the surface reasons over (never rendered raw)
    "adjudication_status",
    "routing_status",
    "routing_reason",
    "measurement_status",
    "default_eligible",
)

#: D-13h: "Elsewhere in this manuscript" NAMES the works. One row per distinct
#: canonical work identified anywhere in the manuscript.
SURFACE_WORK_SUMMARY_FIELDS: Tuple[str, ...] = (
    "canonical_work_id",
    "display_work_id",
    "neutral_title",
    "author",
    "genre",
    "title_missing",
    "page_count",
    "best_band_rank",
    "gated",
    "main_pool",
    "relation_kind",
)

#: D-11/D-11a: an unevaluated candidate alignment. One row per DISTINCT
#: opposite page -- never one per evidence row and never one per directed pair.
SURFACE_RELATED_PAGE_FIELDS: Tuple[str, ...] = (
    "related_page_id",
    "evidence_id",
    "evidence_source",
    "confidence_band",
    "band_rank",
    "evidence_row_count",
)

#: A-6: the corpus-wide findings row, in whichever of the three offered units
#: the reader selected. One allowlist for all three, so a field cannot exist on
#: one unit and silently vanish on another.
SURFACE_FINDING_FIELDS: Tuple[str, ...] = (
    "unit",
    "identification_id",
    "sys_id",
    "canonical_work_id",
    "display_work_id",
    "neutral_title",
    "author",
    "genre",
    "domain",
    "library_code",
    "shelfmark_display",
    "main_pool",
    "main_pool_reason",
    "best_band_rank",
    "page_count",
    "max_coverage_ppm",
    "relation_kind",
    "novelty_status",
    "novelty_offered",
    "work_count",
    "manuscript_count",
    "multi_work_annotation",
)

#: The facet-cascade row (domain / author / work), mirroring the catalogue
#: page's accessor SHAPE but sourced from the IDENTIFIED WORK.
SURFACE_FACET_FIELDS: Tuple[str, ...] = (
    "level",
    "value",
    "label",
    "parent",
    "is_leaf",
    "count",
)

_ALL_ALLOWLISTS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("SURFACE_CLAIM_FIELDS", SURFACE_CLAIM_FIELDS),
    ("SURFACE_WORK_SUMMARY_FIELDS", SURFACE_WORK_SUMMARY_FIELDS),
    ("SURFACE_RELATED_PAGE_FIELDS", SURFACE_RELATED_PAGE_FIELDS),
    ("SURFACE_FINDING_FIELDS", SURFACE_FINDING_FIELDS),
    ("SURFACE_FACET_FIELDS", SURFACE_FACET_FIELDS),
)

# Import-time guard: an allowlist that names a forbidden field would defeat the
# whole mechanism silently. Fail at import, never at render time.
for _name, _fields in _ALL_ALLOWLISTS:
    _leaks = sorted(f for f in _fields if is_forbidden_surface_field(f))
    if _leaks:  # pragma: no cover -- structurally unreachable unless edited wrong
        raise RuntimeError(
            f"{_name} names forbidden surface field(s) {_leaks} -- a precision "
            "value, a confidence interval or the review overlay may never be "
            "allowlisted (D-06 / D-13f)"
        )
    if len(set(_fields)) != len(_fields):  # pragma: no cover -- defensive
        raise RuntimeError(f"{_name} contains a duplicate field name")
del _name, _fields, _leaks


# ---------------------------------------------------------------------------
# The projection itself
# ---------------------------------------------------------------------------

def _project(row: Mapping[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    """Project `row` onto exactly `fields`.

    TOTAL by construction: a projected row always carries every allowlisted key
    (missing ones become None) and NEVER carries anything else, so the key set
    is a stable contract a test can pin and a surface can rely on.
    """
    return {field: row.get(field) for field in fields}


def surface_safe_claim(row: Mapping[str, Any]) -> Dict[str, Any]:
    """The panel row a surface may receive (PANEL-01/02)."""
    return _project(row, SURFACE_CLAIM_FIELDS)


def surface_safe_work_summary(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One "elsewhere in this manuscript" work row (D-13h)."""
    return _project(row, SURFACE_WORK_SUMMARY_FIELDS)


def surface_safe_related_page(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One unevaluated candidate alignment (D-11a)."""
    return _project(row, SURFACE_RELATED_PAGE_FIELDS)


def surface_safe_finding(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One corpus-wide findings row, in any of the three offered units (A-6)."""
    return _project(row, SURFACE_FINDING_FIELDS)


def surface_safe_facet(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One domain / author / work facet row."""
    return _project(row, SURFACE_FACET_FIELDS)


# ---------------------------------------------------------------------------
# The envelope
# ---------------------------------------------------------------------------

def _assert_surface_safe(items: Iterable[Any], meta: Mapping[str, Any]) -> None:
    """Re-check what the projection already guarantees.

    Cheap (keys only, over <= one page of rows) and deliberately redundant: an
    envelope hand-built by a future caller that skipped `surface_safe_*` still
    cannot carry the badge, a precision value or an interval.
    """
    for item in items:
        if isinstance(item, Mapping):
            leaks = sorted(k for k in item.keys() if is_forbidden_surface_field(k))
            if leaks:
                raise ValueError(
                    f"envelope item carries forbidden surface field(s) {leaks} -- "
                    "project it through surface_safe_* first (T-136-14-09)"
                )
    leaks = sorted(k for k in meta.keys() if is_forbidden_surface_field(k))
    if leaks:
        raise ValueError(
            f"envelope meta carries forbidden surface field(s) {leaks} (T-136-14-09)"
        )


def make_envelope(
    status: str,
    items: Optional[Sequence[Any]] = None,
    total: Optional[int] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the ONE shape every Discovery read returns to a surface.

    `{status, items, total, meta}`:
      - `status` is one of `SURFACE_STATUSES` -- a value outside it raises,
        so a surface can never be handed a fifth, unhandled state.
      - a NON-`ok` status always carries an empty item list and a total of 0:
        an outage has no partial truth to report, and a caller that reads
        `total` without checking `status` still cannot render an outage as a
        count.
      - `meta` carries per-query facts (which bucket, whether the count is
        approximate, whether the page scope resolved) -- never a number that
        D-06 forbids.
    """
    if status not in SURFACE_STATUSES:
        raise ValueError(
            f"unknown envelope status {status!r} -- the vocabulary is closed to "
            f"{sorted(SURFACE_STATUSES)} (D-13)"
        )
    meta_dict: Dict[str, Any] = dict(meta or {})
    if status != STATUS_OK:
        _assert_surface_safe((), meta_dict)
        return {"status": status, "items": [], "total": 0, "meta": meta_dict}

    item_list: List[Any] = list(items or [])
    _assert_surface_safe(item_list, meta_dict)
    resolved_total = len(item_list) if total is None else int(total)
    return {
        "status": STATUS_OK,
        "items": item_list,
        "total": resolved_total,
        "meta": meta_dict,
    }


def ok_envelope(
    items: Sequence[Any], total: Optional[int] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return make_envelope(STATUS_OK, items, total, meta)


def unavailable_envelope(meta: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """The sidecar is not serving: flag off, asset absent, loader not ready."""
    return make_envelope(STATUS_UNAVAILABLE, meta=meta)


def timeout_envelope(meta: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """The query exceeded its budget -- distinct from `unavailable`, because the
    sidecar IS loaded and a retry is worth offering."""
    return make_envelope(STATUS_TIMEOUT, meta=meta)


def busy_envelope(meta: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Bounded concurrency rejected this call -- distinct from both above."""
    return make_envelope(STATUS_BUSY, meta=meta)


def is_outage(envelope: Mapping[str, Any]) -> bool:
    """True when the envelope reports an outage rather than a genuine result.

    The panel's hide-on-zero rule (D-13) reads THIS, never `not items`.
    """
    return envelope.get("status") in OUTAGE_STATUSES
