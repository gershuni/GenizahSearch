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

import re
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
    # The manuscript this page belongs to, NAMED (2026-08-05). The row used to
    # carry the composite `related_page_id` alone, and the panel rendered it, so
    # a scholarly surface showed an internal identifier where a shelfmark
    # belongs. `display_missing` is what lets the surface say the name could not
    # be resolved instead of falling back to the id it still has.
    "sys_id",
    "library_code",
    "shelfmark_display",
    "page_number",
    #: The volume that folio number belongs to. Required for the row's LINK, not
    #: for its label: folio numbering is PER VOLUME, so `page=3` with no volume
    #: addresses a different page in each one. The row rendered exactly that
    #: half-address until 2026-08-08.
    "volume_ie",
    "display_missing",
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
    #: ⟨CHANGED 2026-08-12 -- C-track⟩ Was `relation_kind` (the stored
    #: `claim_type`-shaped column). This surface now carries Contract 1's matrix
    #: output instead, and carries it INSTEAD OF rather than alongside, because
    #: `web/components/findings_rows.py` reads the relation for two purposes --
    #: the chip AND the gate on whether a coverage percentage may be shown -- and
    #: those two must not disagree. A row the matrix demoted to `shared_text`
    #: while still advertising "68% of page" would contradict its own demotion.
    #: One field means one answer.
    "rendered_relation",
    "novelty_status",
    "novelty_offered",
    #: Ruling F: does this row disagree with a catalogue identification?
    #:
    #: A BOOLEAN, computed in SQL, and deliberately NOT left to a renderer to
    #: derive from `novelty_status`. On the two GROUPED units `novelty_status`
    #: is NULL whenever the aggregated identifications do not all carry the
    #: same shade -- so a per-work row is never divergent by that test, and a
    #: manuscript row mixing one divergent identification with one confirming
    #: one is not either. Both would render as ordinary findings. This flag is
    #: `MAX(...)` over the group, so a row carrying ANY divergence says so.
    "divergent",
    "work_count",
    "manuscript_count",
    "multi_work_annotation",
    #: WHERE THE MATCH IS: the first matched folio's `/browse` address, as its
    #: TWO components and never as the composite page id they came from (owner
    #: report, 2026-08-08).
    #:
    #: The composite `{sys_id}_{ie_id}_P{n:06d}_{fl_id}` is deliberately NOT
    #: allowlisted. `get_related_pages_enveloped` already carries the scar from
    #: handing one to a surface -- the panel rendered
    #: `990051620920205171_IE167198813_P000003_FL167198817` where a shelfmark
    #: belongs -- and the fix there was to resolve the id in the service and let
    #: no surface hold it. A surface that had the id here would eventually print
    #: it, and the same defect would return under a different name.
    #:
    #: `first_match_volume_ie` is not optional decoration: a multi-volume
    #: manuscript numbers its folios PER VOLUME, so the page number alone
    #: addresses more than one page.
    #:
    #: BOTH ARE None TOGETHER when the folio did not resolve. That is a NAMED
    #: state the row's copy branches on, exactly as `display_missing` is on the
    #: related-page row -- never a silent fallback that keeps promising a folio.
    "first_match_page",
    "first_match_volume_ie",
)

#: PANEL-02 (plan 136-21): ONE row of the "Other manuscripts matching <work>"
#: expansion. A PAIR row, not a claim row -- it describes the relationship
#: between the ANCHOR the reader is looking at and ANOTHER carrier they are not.
#:
#: What it deliberately does NOT name, and why:
#:   * `review_overlay` -- `serialize_banded_claim` always emits it, and the
#:     import-time guard below would reject it anyway (D-13f).
#:   * `measurement_status` / `ci_low` -- the band-label inputs; they exist only
#:     inside the service (D-06).
#:   * `unit_key` / `rn` / `_total_rows` -- internal query discriminators.
#:   * the CARRIER's RAW `evidence_source` / `confidence_band`, and the ANCHOR's.
#:     DATA-01 says the surface displays the WEAKER of the two bands; handing it
#:     both raw pairs would invite a renderer to re-derive that comparison, and a
#:     second comparator is exactly how the displayed band drifts from the
#:     filtered one. The RESOLVED pair (plus its rank and its label) is the only
#:     band vocabulary a surface ever sees here.
#:
#: Relation kinds DO stay on the row -- they are stored vocabulary the surface
#: maps through `relation_chip()`, never renders raw, exactly as
#: `SURFACE_CLAIM_FIELDS` already treats `relation_kind`.
SURFACE_EXPANSION_FIELDS: Tuple[str, ...] = (
    # the other carrier's identity
    "work_id",
    "unit_id",
    "representative_sys_id",
    "representative_page_id",
    "representative_claim_id",
    #: The representative claim's folio, as the TWO components of a `/browse`
    #: address -- never parsed out of `representative_page_id` by a renderer.
    #: BOTH or NEITHER (`_browse_address_from_page_id`), so this row's link
    #: cannot be a folio number pointed at an unknown volume.
    "representative_page",
    "representative_volume_ie",
    "member_sys_ids",
    # what NAMES it (an absent manuscript_display row is FLAGGED, not blanked)
    "library_code",
    "shelfmark_display",
    "display_missing",
    # both sides' relation kinds (PANEL-02: "shows each side's own relation type
    # when they differ")
    "claim_type",
    "anchor_claim_type",
    "relations_differ",
    # the RESOLVED band presentation (DATA-01: the WEAKER of the pair)
    "displayed_evidence_source",
    "displayed_confidence_band",
    "band_label",
    "band_rank",
)

#: Ruling U (plan 136-22): ONE contribution-shade row of the launch statistics.
#:
#: Three fields and no fourth. What it deliberately does NOT name, and why:
#:   * a PERCENTAGE, a SHARE or a RATIO of any kind. "The finding aids did not
#:     already have it" is a claim about the AIDS, not about the match. A single
#:     ratio here would turn a provenance statement into a quality statement,
#:     which is exactly what ruling U constraint 3 forbids -- and the forbidden
#:     substrings below would not catch `contribution_share` on their own.
#:   * a `precision` or a `rank`. The shades are not ordered by quality; their
#:     frozen order is the order the ruling itself lists them in.
#:
#: `shade` is the STORED novelty vocabulary value (`fills_gap` /
#: `refines_granularity` / `container_predicts`) and is NEVER rendered raw --
#: the reader-facing, match-framed label lives in
#: `shared/discovery_display_strings.py`, exactly as `relation_kind` is treated
#: on `SURFACE_CLAIM_FIELDS`.
SURFACE_LAUNCH_SHADE_FIELDS: Tuple[str, ...] = (
    "shade",
    "identification_count",
    "manuscript_count",
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
    ("SURFACE_EXPANSION_FIELDS", SURFACE_EXPANSION_FIELDS),
    ("SURFACE_LAUNCH_SHADE_FIELDS", SURFACE_LAUNCH_SHADE_FIELDS),
    ("SURFACE_FACET_FIELDS", SURFACE_FACET_FIELDS),
)


def _assert_allowlist_safe(name: str, fields: Sequence[str]) -> None:
    """Raise if `fields` names a forbidden field or repeats one.

    A FUNCTION rather than an inline loop body so a test can run the guard
    itself over a seeded copy of an allowlist. A test that re-implemented the
    check, or that exercised `is_forbidden_surface_field` directly, would pass
    happily while the registration loop below skipped the allowlist under test
    -- which is the failure the registration exists to prevent.
    """
    leaks = sorted(f for f in fields if is_forbidden_surface_field(f))
    if leaks:
        raise RuntimeError(
            f"{name} names forbidden surface field(s) {leaks} -- a precision "
            "value, a confidence interval or the review overlay may never be "
            "allowlisted (D-06 / D-13f)"
        )
    if len(set(fields)) != len(fields):
        raise RuntimeError(f"{name} contains a duplicate field name")


# Import-time guard: an allowlist that names a forbidden field would defeat the
# whole mechanism silently. Fail at import, never at render time.
for _name, _fields in _ALL_ALLOWLISTS:
    _assert_allowlist_safe(_name, _fields)
del _name, _fields


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


def surface_safe_expansion(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One "Other manuscripts matching <work>" expansion row (PANEL-02)."""
    return _project(row, SURFACE_EXPANSION_FIELDS)


def surface_safe_launch_shade(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One launch contribution-shade row (ruling U, plan 136-22)."""
    return _project(row, SURFACE_LAUNCH_SHADE_FIELDS)


def surface_safe_facet(row: Mapping[str, Any]) -> Dict[str, Any]:
    """One domain / author / work facet row."""
    return _project(row, SURFACE_FACET_FIELDS)


# ---------------------------------------------------------------------------
# The envelope
# ---------------------------------------------------------------------------

#: The review badge, in BOTH languages, mirrored as literals rather than
#: imported from `shared.discovery_band_labels`. Mirroring is deliberate and
#: matches the standing independence rule for checks: a check that imports the
#: thing it is checking passes automatically when that thing changes.
_BADGE_VALUE_MARKERS: tuple = (
    "Expert-reviewed",
    "נבדק בידי מומחה",
)


#: Rendered rate/interval shapes, which may not reach a reader under ANY key.
#:
#: Codex code review 2A finding 7 kept this open: recursive KEY checking plus the
#: two badge strings still let a percentage or an accuracy value ride under an
#: innocuous key -- `{"label": "matches 91% of the time"}`, `{"note": "[0.88,
#: 0.94]"}`. Those are exactly the surfaces the no-precision rule exists for.
#:
#: Deliberately SHAPE rules, not a vocabulary. A general prohibited-word scan
#: over every string would reject correct envelopes, because the projection
#: legitimately carries machine values like `direct_witness` -- that is why the
#: badge markers stayed a closed two-item list, and the reasoning holds here.
#: These three match how a number is WRITTEN, so a machine enum cannot trip them.
_PERCENT_RE = re.compile(r"\d\s*%")
#: How a DECIMAL is written, and the single reason this gate ever rejected a
#: real manuscript.
#:
#: `\d*\.\d+` -- the previous spelling -- makes the integer part OPTIONAL, so the
#: `.2` in the Cambridge shelfmark `T-S F1(1).2` reads as a decimal, and `F1` is
#: a rate word four characters away. Every findings envelope containing one of
#: the 39 real `T-S F1(n).n` shelfmarks therefore raised, and because the check
#: guards the WHOLE envelope the reader lost the ENTIRE page rather than a row
#: (owner report, 2026-08-07: `item.shelfmark_display`; 51 identifications
#: across 37 manuscripts).
#:
#: So: require an explicit integer part, OR a bare `.NN` that is not preceded by
#: a word character or a closing paren. That second alternative is what keeps
#: `accuracy .88` caught while `F1(1).2` and `MS Heb c.57` pass -- a shelfmark's
#: fraction-shaped tail is always glued to the segment before it, and a written
#: rate never is.
#:
#: `tests/render_smoke/discovery_honesty_gate.py::_DECIMAL_RE` reached the same
#: conclusion from the same defect ("the integer part is what keeps a shelfmark
#: out of it -- `MS Heb c.57`"). The spellings stay SEPARATE rather than shared:
#: mirroring is the standing rule for these checks, because a check that imports
#: the thing it checks passes automatically when that thing changes.
_DECIMAL = r"(?:\d+\.\d+|(?<![\w)])\.\d+)"
#: A bracketed pair of decimals -- `[0.88, 0.94]`, `(0.88-0.94)`, `[.88, .94]`.
#:
#: Spelled through `_DECIMAL` for ONE spelling of "how a decimal is written" in
#: this module, so a future tightening cannot be applied to the rate rule and
#: forgotten here. That is hygiene, and it is worth stating plainly that it is
#: NOT a fix: a Codex review (2026-08-07) read the shared `\d*\.\d+` as the same
#: optional-integer-part defect and offered `MS Heb c.57 (.2, .3)` as a shelfmark
#: it would wrongly reject. That string is STILL rejected here, and deliberately
#: so on both counts --
#:
#:   * the lookbehind cannot help: `(` is neither a word character nor `)`, so a
#:     bracket-opened `.2` is "bare" by construction. Any rule that let it pass
#:     would also let `[.88, .94]` pass, which is a confidence interval written
#:     exactly as a paper writes one;
#:   * and it is not a real shelfmark. A bracketed pair of decimals joined by a
#:     comma or a dash occurs ZERO times across all 720,948 `call_numbers`
#:     variants in libraries.csv -- checked over every variant, not just the
#:     shortest one the artifact stores for display. In BOTH spellings: bare
#:     (`(.2, .3)`) and full (`(0.2, 0.3)`).
#:
#: So this rule has no live false positive and no plausible one; unlike the rate
#: rule, whose false positive was 39 real shelfmarks a reader actually lost the
#: page on.
_INTERVAL_RE = re.compile(
    r"[\[(]\s*" + _DECIMAL + r"\s*[,–-]\s*" + _DECIMAL + r"\s*[\])]")
#: A rate word within a short distance of a decimal. The distance bound is what
#: keeps `1.25 seconds` and a `v0.8`-style marker out of it.
_RATE_WORD_RE = re.compile(
    r"(precision|accuracy|recall|confidence|f1|ci)\b[^.]{0,24}?" + _DECIMAL
    + r"|" + _DECIMAL + r"[^.]{0,24}?\b(precision|accuracy|recall|confidence|f1)",
    re.IGNORECASE,
)
#: Version-shaped tokens are not rates. Bounded to explicit `v`/`version`
#: syntax at a word boundary -- a wider rule would also excuse `accuracy 0.9`.
_VERSION_RE = re.compile(r"\b(v|version\s*)\d+(\.\d+)*\b", re.IGNORECASE)


def _rate_or_interval_violation(value: str) -> Optional[str]:
    """The reason `value` is a rendered rate/interval, or None."""
    stripped = _VERSION_RE.sub(" ", value)
    if _PERCENT_RE.search(stripped):
        return "a percentage"
    if _INTERVAL_RE.search(stripped):
        return "a confidence interval"
    if _RATE_WORD_RE.search(stripped):
        return "an accuracy rate"
    return None


def _walk_nodes(node: Any, path: str = ""):
    """Yield `(path, key, value)` for EVERY key/value pair reachable in `node`.

    `key` is the mapping key, or None for sequence elements. Every pair is
    yielded regardless of the value's type, then recursed into.

    The type-independence is load-bearing and was got wrong first time round: an
    earlier version yielded only pairs whose VALUE was a string, so a forbidden
    key with a numeric value -- `{"ci_low": 0.5}`, exactly the shape this guards
    against -- was never surfaced at all. Its own positive control caught it."""
    if isinstance(node, Mapping):
        for k, v in node.items():
            child = f"{path}.{k}" if path else str(k)
            yield child, k, v
            yield from _walk_nodes(v, child)
    elif isinstance(node, (list, tuple)) and not isinstance(node, (str, bytes)):
        for i, v in enumerate(node):
            child = f"{path}[{i}]"
            yield child, None, v
            yield from _walk_nodes(v, child)


def _assert_surface_safe(items: Iterable[Any], meta: Mapping[str, Any]) -> None:
    """Re-check what the projection already guarantees.

    Deliberately redundant: an envelope hand-built by a future caller that
    skipped `surface_safe_*` still cannot carry the badge, a precision value or
    an interval.

    Two passes, and the distinction between them is the point:

    * **Forbidden KEYS, recursively.** The original check looked only at
      top-level keys, so a forbidden field one level down inside a nested
      mapping or a list of sub-rows was invisible while the docstring claimed
      hand-built envelopes "cannot carry" it (Codex code review 2026-08-03,
      finding 7).

    * **Badge VALUES, in both languages.** A review badge under an *allowed*
      key -- `{"label": "Expert-reviewed ✓"}` -- passed every key-based check
      ever written here.

    What this deliberately does NOT do is scan every string against a general
    prohibited vocabulary. The projection intentionally carries machine values
    like `direct_witness`, and a naive value scan would reject correct
    envelopes; a gate that fails on valid output costs as much as one that
    passes on invalid output. The badge markers below are a closed two-item
    list of *rendered* strings, not a vocabulary.
    """
    for item in items:
        for path, key, value in _walk_nodes(item, "item"):
            if key is not None and is_forbidden_surface_field(key):
                raise ValueError(
                    f"envelope item carries forbidden surface field at {path!r} -- "
                    "project it through surface_safe_* first (T-136-14-09)"
                )
            if isinstance(value, str) and any(
                    marker in value for marker in _BADGE_VALUE_MARKERS):
                raise ValueError(
                    f"envelope item carries the human-review badge as a VALUE at "
                    f"{path!r} -- the badge may never reach a surface, under any key "
                    "(T-136-14-09)"
                )
            if isinstance(value, str):
                reason = _rate_or_interval_violation(value)
                if reason:
                    raise ValueError(
                        f"envelope item carries {reason} as a VALUE at {path!r} -- "
                        "no precision figure, interval or accuracy rate may reach a "
                        "reader under any key (T-136-14-09)"
                    )
    for path, key, value in _walk_nodes(meta, "meta"):
        if key is not None and is_forbidden_surface_field(key):
            raise ValueError(
                f"envelope meta carries forbidden surface field at {path!r} "
                "(T-136-14-09)"
            )
        if isinstance(value, str) and any(
                marker in value for marker in _BADGE_VALUE_MARKERS):
            raise ValueError(
                f"envelope meta carries the human-review badge as a VALUE at "
                f"{path!r} (T-136-14-09)"
            )
        if isinstance(value, str):
            reason = _rate_or_interval_violation(value)
            if reason:
                raise ValueError(
                    f"envelope meta carries {reason} as a VALUE at {path!r} "
                    "(T-136-14-09)"
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
