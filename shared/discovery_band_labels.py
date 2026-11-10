# -*- coding: utf-8 -*-
"""Discovery band display + precision presentation values module (Phase 135,
plans 135-01, BAND-01/02/03/04, CERT-02).

THE contract this module renders is `docs/specs/discovery-band-labels-v1.md`
-- tunable ONLY by versioning that file (same discipline as
`docs/specs/discovery-budgets.md`). This module never hardcodes a competing
string; every discovery surface (Phase 135 methods page, Phase 136 browse
panel + `/work/{id}`, the atlas, the public API, the internal inspection
tool) renders bands/precision/eligibility THROUGH this module.

Three things live here, all invariant-tested by
`tests/test_discovery_band_labels.py`:

1. **Band display labels + precision copy** (BAND-01/02, CERT-02) -- a
   hand-typed, TOTAL values table over the frozen
   `scripts.discovery_ids.CONFIDENCE_BANDS_BY_SOURCE` enum, plus data-driven
   precision-copy rendering that FAILS CLOSED on a partial (one-sided)
   confidence interval and never shows a bare percentage for an unmeasured
   band.
2. **The SC#1 band-inseparable claim serializer** (`serialize_banded_claim`)
   -- makes band + review status structurally inseparable from any claim
   presentation: a row missing its band fields raises rather than silently
   omitting them.
3. **The central D-18 default-eligibility predicate** (`is_default_eligible`)
   -- FAILS CLOSED against the confidence interval (Codex #B3): a stored
   `measured_pass` that contradicts its own `ci_low` (missing or below
   `STRICT_FLOOR`) can never default-show `tier_a`.

Rule 1 (the word gate, band-labels-v1.md §1): "verified" / "confirmed" /
"reviewed" / "certified" appear ONLY as a function of
`adjudication_status == 'human_confirmed'` (via `review_overlay`) -- never as
a band label, never in `format_precision_copy` output. "Certified" is
prohibited everywhere, with no exception.

Per `135-PATTERNS.md`, this module (itself part of `shared/`) imports
`scripts.discovery_ids` directly for the frozen enum vocabulary rather than
re-inlining a fourth competing copy (the drift-guard tests assert totality
against that ONE source of truth).
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

import scripts.discovery_ids as ids

# ---------------------------------------------------------------------------
# D-07 Strict pass gate + the 135-05 stored measurement_status closed vocab.
# ---------------------------------------------------------------------------

STRICT_FLOOR = 0.85

MEASUREMENT_STATUSES = frozenset({
    "not_measured",
    "measured_pass",
    "measured_fail",
    "insufficient_evidence",
})

# ---------------------------------------------------------------------------
# v1 -> v2 band-key normalization (discovery-band-labels-v1.md §5). Only
# `expert_verified` renames (to `high_confidence_algorithmic`) in the v2
# bake; every other stored key is unchanged. Normalizing BEFORE lookup lets
# ONE label table serve both the pre- and post-bake stored key.
# ---------------------------------------------------------------------------

_V1_TO_V2_BAND_KEY: Dict[str, str] = {
    ids.CONFIDENCE_BAND_EXPERT_VERIFIED: "high_confidence_algorithmic",
}


def _canon_band_key(band_key: str) -> str:
    """Normalize a stored `confidence_band` key to its v2 canonical form.

    A v1-shaped key (`expert_verified`) and its v2 successor
    (`high_confidence_algorithmic`) both resolve to the SAME canonical key,
    so `BAND_LABELS` needs only one row per band regardless of which
    sidecar version is currently loaded."""
    return _V1_TO_V2_BAND_KEY.get(band_key, band_key)


# ---------------------------------------------------------------------------
# BAND-01: the hand-typed, bilingual, TOTAL band display label table
# (docs/specs/discovery-band-labels-v1.md §2). Keyed by
# (evidence_source, canonical_band_key). Drift-guarded by
# tests/test_discovery_band_labels.py against
# scripts.discovery_ids.CONFIDENCE_BANDS_BY_SOURCE.
# ---------------------------------------------------------------------------

BAND_LABELS: Dict[Tuple[str, str], Dict[str, str]] = {
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, "high_confidence_algorithmic"): {
        "en": "High-confidence match (algorithmic)",
        "he": "התאמה בוודאות גבוהה (אלגוריתמית)",
    },
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A): {
        "en": "Algorithmic match — tier A",
        "he": "התאמה אלגוריתמית — דרגה א׳",
    },
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_RB): {
        "en": "Screening — rule-based",
        "he": "סינון — מבוסס כללים",
    },
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_CANON): {
        "en": "Screening — canon",
        "he": "סינון — קנון",
    },
    (ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_CORROBORATED): {
        "en": "Corroborated by matching witnesses",
        "he": "מאושש בעדים תואמים",
    },
    (ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_WEAK): {
        "en": "Matching witnesses (weak)",
        "he": "עדים תואמים (חלש)",
    },
    (ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_NOT_EVALUATED): {
        "en": "Shared text — not evaluated",
        "he": "טקסט משותף — לא הוערך",
    },
}


def band_label(evidence_source: str, confidence_band: str, lang: str = "en") -> str:
    """The EN or HE display label for a stored (evidence_source,
    confidence_band) pair -- a surface never shows the raw stored key.
    Unknown `lang` defaults to EN. Both the v1 and v2 stored band key
    resolve to the SAME label via `_canon_band_key`."""
    lang_key = "he" if lang == "he" else "en"
    key = (evidence_source, _canon_band_key(confidence_band))
    entry = BAND_LABELS.get(key)
    if entry is None:
        raise ValueError(
            f"band_label: no label for (evidence_source={evidence_source!r}, "
            f"confidence_band={confidence_band!r})"
        )
    return entry[lang_key]


# ---------------------------------------------------------------------------
# Review overlay (the ONLY human-review signal, orthogonal to the band) --
# docs/specs/discovery-band-labels-v1.md §2 review-overlay table.
# ---------------------------------------------------------------------------

_HUMAN_CONFIRMED_BADGE: Dict[str, str] = {
    "en": "Expert-reviewed ✓",
    "he": "נבדק בידי מומחה ✓",
}

_UNREVIEWED_MARKER: Dict[str, str] = {
    "en": "unreviewed · algorithmic estimate",
    "he": "לא נבדק · הערכה אלגוריתמית",
}


def review_overlay(adjudication_status: Optional[str], lang: str = "en") -> str:
    """The review-status marker for a claim. Returns the
    "Expert-reviewed"/HE badge ONLY when `adjudication_status ==
    'human_confirmed'`; every other status (provisional/unreviewed/absent)
    returns the explicit "unreviewed - algorithmic estimate"/HE marker so
    the absence of review is always visible, never merely implied by a
    missing badge."""
    lang_key = "he" if lang == "he" else "en"
    if adjudication_status == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED:
        return _HUMAN_CONFIRMED_BADGE[lang_key]
    return _UNREVIEWED_MARKER[lang_key]


# ---------------------------------------------------------------------------
# BAND-02/CERT-02: data-driven precision presentation
# (docs/specs/discovery-band-labels-v1.md §3).
# ---------------------------------------------------------------------------

_NOT_YET_MEASURED: Dict[str, str] = {
    "en": "precision not yet measured",
    "he": "הדיוק טרם נמדד",
}

_PRECISION_LABEL: Dict[str, str] = {
    "en": "estimated band precision",
    "he": "דיוק משוער של הדרגה",
}

# Distinct labels for the raw sample-size figures that MAY accompany a
# precision number (numerator/denominator/draw-size) -- deliberately never
# "population" (Codex #9/#B1: population is the runtime
# get_band_claim_counts() figure, a completely different, larger count).
# Exposed as constants for future callers (Phase 136 certificate surfaces);
# not composed into format_precision_copy's return value here.
NUMERATOR_LABEL: Dict[str, str] = {"en": "successes", "he": "הצלחות"}
DENOMINATOR_LABEL: Dict[str, str] = {"en": "determinate graded", "he": "מדגם שהוכרע"}
DRAW_SIZE_LABEL: Dict[str, str] = {"en": "draw size", "he": "גודל הדגימה"}


def format_precision_copy(row: Mapping[str, Any], lang: str = "en") -> str:
    """Render a `band_precision` row's precision as user-facing copy, per
    docs/specs/discovery-band-labels-v1.md §3 rule 1:

    - `precision` IS NULL/absent -> "precision not yet measured"/HE (NEVER a
      bare percentage, NEVER per-item phrasing).
    - `precision` present + BOTH `ci_low`/`ci_high` present -> "estimated
      band precision <pct> [<lo>, <hi>]"/HE.
    - `precision` present + BOTH bounds absent -> "estimated band precision
      <pct>"/HE (the interval is OMITTED, per "CI when available").
    - EXACTLY ONE of `ci_low`/`ci_high` present (a partial interval) ->
      raises ValueError (FAIL CLOSED -- never render a one-sided interval).
    """
    lang_key = "he" if lang == "he" else "en"
    precision = row.get("precision")
    if precision is None:
        return _NOT_YET_MEASURED[lang_key]

    ci_low = row.get("ci_low")
    ci_high = row.get("ci_high")
    has_low = ci_low is not None
    has_high = ci_high is not None
    if has_low != has_high:
        raise ValueError(
            "format_precision_copy: partial confidence interval (exactly one "
            "of ci_low/ci_high present) -- failing closed rather than "
            "rendering a one-sided interval"
        )

    pct = f"{precision:.1%}"
    label = _PRECISION_LABEL[lang_key]
    if has_low and has_high:
        lo = f"{ci_low:.1%}"
        hi = f"{ci_high:.1%}"
        return f"{label} {pct} [{lo}, {hi}]"
    return f"{label} {pct}"


def band_measurement_status(row: Mapping[str, Any]) -> str:
    """A machine-readable status enum derived data-driven from a
    `band_precision`-shaped row, FAIL-CLOSED against the confidence
    interval (Codex #B3):

    - "not_measured" when `precision` IS NULL/absent AND no stored
      `measurement_status` is present.
    - Otherwise PREFERS the row's own stored `measurement_status` (the
      135-05 closed vocab) -- BUT DOWNGRADES a stored "measured_pass" to
      "measured_fail" whenever `ci_low` is missing OR below `STRICT_FLOOR`
      (a spec that stamps measured_pass while contradicting its own CI can
      never surface as a pass).
    - Otherwise (precision present, no stored status -- the current v1
      pre-registered-estimate shape) derives the display-only
      "measured_audit_pending" (parity with D-06's "expert-measured -
      independent audit pending" posture).

    This ONE status feeds BOTH `format_precision_copy` (indirectly, via
    callers) AND `is_default_eligible`, so a BAND-02 status change requires
    NO code edit -- only a sidecar data change."""
    precision = row.get("precision")
    stored_status = row.get("measurement_status")
    ci_low = row.get("ci_low")

    if precision is None and not stored_status:
        return "not_measured"

    if stored_status:
        if stored_status == "measured_pass" and (ci_low is None or ci_low < STRICT_FLOOR):
            return "measured_fail"
        return stored_status

    return "measured_audit_pending"


# ---------------------------------------------------------------------------
# The central D-18 default-eligibility predicate (band-labels-v1.md §4 +
# CONTEXT.md D-18 + Codex #B3). FAILS CLOSED on a missing/sub-floor CI.
# ---------------------------------------------------------------------------

def is_default_eligible(
    evidence_source: str,
    confidence_band: str,
    adjudication_status: Optional[str],
    routing_status: Optional[str],
    measurement_status: Optional[str],
    ci_low: Optional[float] = None,
) -> bool:
    """Whether a claim shows on the DEFAULT (non-expanded, non-toggled)
    surface. Implements docs/specs/discovery-band-labels-v1.md §4 + D-18 +
    the Codex #B3 CI-fail-closed gate:

    - `human_confirmed` -> True, unconditionally (any band, any routing).
    - Else `routing_status == 'review_only'` -> False.
    - Else the band is one of {screening_rb, screening_canon,
      not_evaluated} -> False (always behind the "show more" toggle).
    - Else `tier_a` -> True ONLY when `measurement_status == 'measured_pass'`
      AND `ci_low is not None` AND `ci_low >= STRICT_FLOOR` (D-18: held
      behind the toggle until its CERT-01 certificate passes; fails closed
      on a missing or sub-floor lower confidence bound).
    - Else -> True (`high_confidence_algorithmic`/`expert_verified`,
      `corroborated`, `weak` -- already-shipped/measured bands).
    """
    if adjudication_status == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED:
        return True
    if routing_status != ids.ROUTING_STATUS_SHIPPED:
        return False
    canon_band = _canon_band_key(confidence_band)
    if canon_band in {
        ids.CONFIDENCE_BAND_SCREENING_RB,
        ids.CONFIDENCE_BAND_SCREENING_CANON,
        ids.CONFIDENCE_BAND_NOT_EVALUATED,
    }:
        return False
    if canon_band == ids.CONFIDENCE_BAND_TIER_A:
        return (
            measurement_status == "measured_pass"
            and ci_low is not None
            and ci_low >= STRICT_FLOOR
        )
    return True


# ---------------------------------------------------------------------------
# SC#1: the band-inseparable claim presentation serializer. NO code path may
# emit a claim presentation without its band_label + review_overlay +
# measurement_status + default_eligible keys.
# ---------------------------------------------------------------------------

_REQUIRED_ROW_KEYS = ("confidence_band", "evidence_source", "adjudication_status")


def serialize_banded_claim(row: Mapping[str, Any], lang: str = "en") -> Dict[str, Any]:
    """Compose the ONE band-bearing presentation object every discovery
    surface renders (SC#1) -- band + review status are structurally
    INSEPARABLE from a claim: `row` MUST carry `confidence_band`,
    `evidence_source`, and `adjudication_status`, or this raises ValueError
    rather than silently emitting a bandless presentation.

    `routing_status` defaults conservatively to `review_only` when absent
    from `row` (an unknown routing status must never be treated as
    default-shown). `ci_low` / `precision` / `ci_high` / `measurement_status`
    are read via `.get()` (optional -- absent means "not yet measured" per
    `band_measurement_status`)."""
    missing = [k for k in _REQUIRED_ROW_KEYS if row.get(k) is None]
    if missing:
        raise ValueError(
            f"serialize_banded_claim: row missing required band field(s): {missing} "
            "(SC#1 -- a claim may never be serialized without its band + review status)"
        )

    evidence_source = row["evidence_source"]
    confidence_band = row["confidence_band"]
    adjudication_status = row["adjudication_status"]
    routing_status = row.get("routing_status") or ids.ROUTING_STATUS_REVIEW_ONLY

    measurement_status = band_measurement_status(row)
    ci_low = row.get("ci_low")
    default_eligible = is_default_eligible(
        evidence_source,
        confidence_band,
        adjudication_status,
        routing_status,
        measurement_status,
        ci_low=ci_low,
    )

    return {
        "evidence_source": evidence_source,
        "confidence_band": confidence_band,
        "adjudication_status": adjudication_status,
        "routing_status": routing_status,
        "band_label": band_label(evidence_source, confidence_band, lang),
        "review_overlay": review_overlay(adjudication_status, lang),
        "measurement_status": measurement_status,
        "default_eligible": default_eligible,
    }


# ---------------------------------------------------------------------------
# D-11 / D-12: shared bilingual toggle + disclaimer constants.
# ---------------------------------------------------------------------------

SHOW_MORE_TOGGLE: Dict[str, str] = {
    "en": "Show more possible matches",
    "he": "הצג התאמות אפשריות נוספות",
}

RECALL_DISCLAIMER: Dict[str, str] = {
    "en": "Not exhaustive — more identifications may exist.",
    "he": "אינו ממצה — ייתכנו זיהויים נוספים.",
}
