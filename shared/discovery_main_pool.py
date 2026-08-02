# -*- coding: utf-8 -*-
"""The main-pool bucket rule -- ONE shared, pure predicate for every discovery
surface (Phase 136, plan 136-07, PANEL-01/PANEL-02).

Contract source: `.claude/skills/sketch-findings-genizahsearch/references/
main-pool-rule.md` ("The rule", the four-gate table + measured catch counts
10,302 / 5,620 / 3,714 / 8,721, the `human_confirmed` override, and the
"Wording and internal state" reason-code list), ratified and extended by
`.planning/phases/136-read-surfaces-connections-panel-work-witnesses/
136-GATE1-DECISIONS.md` §§ A (D-13b/D-13c/D-13d/D-13e) and the schema's
`main_pool_reason` closed vocabulary
(`docs/specs/discovery-sidecar-schema-v1.md`, Amendment 2026-08-02 (B)).

Unit is the IDENTIFICATION (manuscript x canonical work) -- every one of an
identification's page-claims travels together (main-pool-rule.md, "The
rule"). This module never reads a table, never touches a DB handle, and
never imports `web/` -- it is the pure predicate the bake, the panel and the
corpus-wide findings page all call, so the three surfaces can never disagree
about which bucket an identification belongs to (the historical failure mode
this plan exists to close: sketch 003's hand-picked `confOf()` disagreed with
the predicate the codebase already had and rendered the best-measured
population in the system as "Weak").

**No weighted score, ever** (main-pool-rule.md: "a scored sum lets three
pages of boilerplate outvote an unresolved competitor, and the weights
cannot be explained honestly"). Gates run in a FIXED order and are
NON-COMPENSATING: once a gate rejects an identification, no later, weaker
signal can promote it back to main.

**A documented trap, deliberately never named here (main-pool-rule.md, the
section warning that a normalized Levenshtein edit-distance field on
`discovery_evidence` is NOT page coverage):** feeding that OTHER numeric
match-quality field in as coverage previously demoted ~100% of witnesses
(`scripts/build_discovery_sidecar.py`, the 135-07 field-name-collision fix).
This module never reads that field at all (its own literal name is kept out
of this file's text entirely, so a static "must never appear" scan of this
module stays trivially enforceable); the real coverage inputs
(`max_matched_letters` / `max_coverage`) must be supplied by the caller,
already computed against `page_norm_letters`.

Gate 2 (the screening-band exclusion) delegates to
`shared.discovery_band_labels.is_default_eligible` rather than picking its
own band allowlist -- gate 2 IS §4's screening-band exclusion, not a
re-derivation of band quality (136-07-PLAN.md's own key_links contract).

**One wording for the rule (Task 3, PANEL-01/PANEL-02).** `main_pool_sentence`
and `bucket_label` below are the ONLY place the two-bucket rule and its
bucket names are worded -- so the methods page (`web/pages/help.py`), the
panel and the corpus-wide findings page can never paraphrase the rule three
different ways. `main_pool_sentence` is asserted, by a test in
`tests/test_discovery_main_pool.py` that reads (never imports)
`web/pages/help.py`'s own source text, to equal that page's
`MAIN_POOL_SENTENCE` constant byte-for-byte in both languages.

**The second bucket means "not enough evidence for the rule" -- never
"probably wrong."** (main-pool-rule.md, "Wording and internal state": it
holds probable quotations, shared wording, unresolved ties, missing signals
and genuinely indeterminate cases alike; a reader must never read bucket
membership as a verdict on correctness.) `bucket_label`'s own docstring
repeats this so the distinction travels with the function, not only with
this module docstring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Tuple

import scripts.discovery_ids as ids
from shared.discovery_band_labels import is_default_eligible

# ---------------------------------------------------------------------------
# The closed `main_pool_reason` vocabulary. MUST equal, as a set, the
# `main_pool_reason` CHECK constraint in
# docs/specs/discovery-sidecar-schema-v1.md's Amendment 2026-08-02 (B) --
# `tests/test_discovery_main_pool.py` asserts this equality so a vocabulary
# drift between this module and the schema doc is a red suite, never a
# silent mismatch between the asset and the surfaces that read it.
# ---------------------------------------------------------------------------

REASON_SHARED_WORDING = "shared_wording"
REASON_OVERLAPPING_TIE = "overlapping_tie"
REASON_LOW_COVERAGE = "low_coverage"
REASON_INSUFFICIENT_LENGTH = "insufficient_length"
REASON_MISSING_SIGNAL = "missing_signal"
REASON_MAIN_MULTIFOLIO = "main_multifolio"
REASON_MAIN_FULL_COVERAGE = "main_full_coverage"
REASON_MAIN_HUMAN_CONFIRMED = "main_human_confirmed"

MAIN_POOL_REASONS = frozenset({
    REASON_SHARED_WORDING,
    REASON_OVERLAPPING_TIE,
    REASON_LOW_COVERAGE,
    REASON_INSUFFICIENT_LENGTH,
    REASON_MISSING_SIGNAL,
    REASON_MAIN_MULTIFOLIO,
    REASON_MAIN_FULL_COVERAGE,
    REASON_MAIN_HUMAN_CONFIRMED,
})

# ---------------------------------------------------------------------------
# Named, cited thresholds. Each constant states, in its own comment, whether
# the owner's ruling treats it as LOCKED or PROVISIONAL -- per
# 136-GATE1-DECISIONS.md's own "Provisional-value / omission audit" section,
# which explicitly distinguishes the two for these exact two numbers.
# ---------------------------------------------------------------------------

# RATIFIED / LOCKED. 136-GATE1-DECISIONS.md § A "D-13c": "KEEP 150 matched
# letters. Unchanged from the reviewed value. On record: 6,558 direct (4.5%)
# and 6,497 propagated (15.9%) fall below it, of which 8,457 short direct
# rows remain part of a Main identification via multi-folio agreement."
# Unit: matched Hebrew base letters (U+05D0-05EA), space-free, after NFC.
SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS = 150

# PROVISIONAL, NOT ratified. 136-GATE1-DECISIONS.md's own "Provisional-value
# / omission audit" section states this explicitly: the owner's five gate-1
# rulings (D-13b/c/d/e + the novelty domain-rows ruling) did NOT include this
# value; main-pool-rule.md's own reviewers said "Do not freeze the
# thresholds on these numbers... before 0.8 becomes a constant", and that
# ~300-case stratified hand review was "NOT authorized" by this record. This
# constant "must not be silently treated as ratified" (verbatim) until a
# future gate rules on it explicitly.
COVERAGE_FLOOR = 0.8


def _field(obj: Any, name: str, default: Any = None) -> Any:
    """Read `name` off `obj`, which may be a plain Mapping (dict) OR the
    `Identification` dataclass below -- so `main_pool_decision` accepts
    either shape, per this plan's own action text ("a dataclass or plain
    dict")."""
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


@dataclass(frozen=True)
class Identification:
    """One hand-built (manuscript x canonical work) identification record.

    A real caller (the bake) derives every field below from the
    identification's underlying `discovery_claim` / `discovery_evidence`
    rows; this module reads no table directly and is fully exercised by
    fabricated fixtures in `tests/test_discovery_main_pool.py`. Every field
    below defaults to a value that, taken together, passes every gate (a
    "vanilla" `Identification()` is `main_full_coverage`) -- so a test only
    ever overrides the field(s) relevant to the behavior it is pinning.

    - `has_same_work_claim` -- True iff ANY claim in this identification
      carries `claim_type == 'direct_witness'` (gate 1: quotes/shared-text
      claims alone are NOT a same-work claim).
    - `any_human_confirmed` -- True iff ANY claim's evidence carries
      `adjudication_status == 'human_confirmed'`, ACROSS THE WHOLE
      identification (not just its best-band evidence) -- D-13g: a
      low-band row can carry the human-confirmed status the winning row
      lacks.
    - `best_evidence_source` / `best_confidence_band` /
      `best_adjudication_status` / `best_routing_status` /
      `best_measurement_status` / `best_ci_low` -- the identification's own
      BEST (highest-ranked) evidence row's fields, fed straight into
      `is_default_eligible` for gate 2.
    - `page_has_unresolved_competitor` -- one entry per DISTINCT matched
      page (`page_id -> bool`); True means that page carries an unresolved
      near-tie / kept-tie competitor from another canonical work.
    - `max_matched_letters` / `max_coverage` -- this identification's best
      (highest) DIRECT-family matched-letters count / page-coverage ratio
      (`min(1.0, matched_letters / page_norm_letters)`), used ONLY for a
      single-page identification (a multi-page one is main via
      `main_multifolio` before either is consulted).
    """

    has_same_work_claim: bool = True
    any_human_confirmed: bool = False
    best_evidence_source: str = ids.EVIDENCE_SOURCE_TRACK1_DIRECT
    best_confidence_band: str = ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC
    best_adjudication_status: Optional[str] = None
    best_routing_status: Optional[str] = ids.ROUTING_STATUS_SHIPPED
    best_measurement_status: Optional[str] = None
    best_ci_low: Optional[float] = None
    page_has_unresolved_competitor: Mapping[str, bool] = field(
        default_factory=lambda: {"p1": False}
    )
    max_matched_letters: Optional[int] = 500
    max_coverage: Optional[float] = 0.95


def main_pool_decision(identification: Any) -> Tuple[bool, str]:
    """Decide whether `identification` belongs in the main pool.

    Returns `(in_main_pool, reason_code)`; `reason_code` is always a member
    of `MAIN_POOL_REASONS`. `identification` may be an `Identification`
    instance or a plain Mapping carrying the same keys (see `_field`).

    Evaluation order (FIXED, NON-COMPENSATING -- a later signal can never
    promote an identification a prior gate rejected):

    0. `any_human_confirmed` -> main, `main_human_confirmed`. Evaluated
       BEFORE every gate below, including when routing demoted the row
       (D-13g) -- this is the ONE unconditional override.
    1. Gate 1 -- no `direct_witness` claim anywhere in the identification
       (only quotes / shared wording) -> not-main, `shared_wording`.
    2. Gate 2 -- the identification's best evidence fails
       `shared.discovery_band_labels.is_default_eligible` (a screening
       band, `review_only` routing, or an uncertified `tier_a`) -> not-main,
       `missing_signal`. This is §4's screening-band exclusion; it agrees
       with `is_default_eligible` rather than re-deriving band quality.
    3. Gate 3 -- EVERY matched page carries an unresolved near-tie/kept-tie
       competitor -> not-main, `overlapping_tie`. One clean page is enough
       to survive this gate. (An identification with no per-page data at
       all is a caller data error -> not-main, `missing_signal`.)
    4. Multi-folio route -- >=2 distinct matched pages -> main,
       `main_multifolio` (D-13c's short-evidence floor never applies here:
       "UNLESS the identification it belongs to already qualifies as MAIN
       via multi_folio_agreement").
    5. Single-page route -- missing length/coverage data -> not-main,
       `missing_signal`; else `max_matched_letters <
       SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS` -> not-main,
       `insufficient_length` (D-13c); else `max_coverage < COVERAGE_FLOOR`
       -> not-main, `low_coverage`; else -> main, `main_full_coverage`.
    """
    if _field(identification, "any_human_confirmed", False):
        return True, REASON_MAIN_HUMAN_CONFIRMED

    if not _field(identification, "has_same_work_claim", False):
        return False, REASON_SHARED_WORDING

    gate2_eligible = is_default_eligible(
        _field(identification, "best_evidence_source"),
        _field(identification, "best_confidence_band"),
        _field(identification, "best_adjudication_status"),
        _field(identification, "best_routing_status"),
        _field(identification, "best_measurement_status"),
        ci_low=_field(identification, "best_ci_low"),
    )
    if not gate2_eligible:
        return False, REASON_MISSING_SIGNAL

    page_competition = _field(identification, "page_has_unresolved_competitor", {}) or {}
    if not page_competition:
        return False, REASON_MISSING_SIGNAL
    if all(page_competition.values()):
        return False, REASON_OVERLAPPING_TIE

    page_count = len(page_competition)
    if page_count >= 2:
        return True, REASON_MAIN_MULTIFOLIO

    max_matched_letters = _field(identification, "max_matched_letters")
    max_coverage = _field(identification, "max_coverage")
    if max_matched_letters is None or max_coverage is None:
        return False, REASON_MISSING_SIGNAL
    if max_matched_letters < SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS:
        return False, REASON_INSUFFICIENT_LENGTH
    if max_coverage < COVERAGE_FLOOR:
        return False, REASON_LOW_COVERAGE
    return True, REASON_MAIN_FULL_COVERAGE


# ---------------------------------------------------------------------------
# Task 3 (PANEL-01/PANEL-02): "One wording for the rule." `main_pool_sentence`
# and `bucket_label` are the SOLE place the two-bucket rule and its bucket
# names are worded, in EN and HE, so the methods page (`web/pages/help.py`),
# the panel and the corpus-wide findings page can never each phrase the rule
# differently. Source of the exact wording:
# `.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md`
# § "The rule" (the scholar sentence) and § "Wording and internal state" (the
# bucket names, and the "not enough evidence, never probably wrong" framing).
#
# `main_pool_sentence('en')`/`main_pool_sentence('he')` are pinned, by a test
# in `tests/test_discovery_main_pool.py`, to equal `web/pages/help.py`'s own
# `MAIN_POOL_SENTENCE` constant byte-for-byte -- that test reads (never
# imports) help.py's source text, because this module must not import
# `web/`. If either side is ever edited without the other, that test fails
# and prints both values.
# ---------------------------------------------------------------------------

_MAIN_POOL_SENTENCE: Mapping[str, str] = {
    "en": (
        "A fragment is treated as a probable identification when it matches the work across "
        "more than one leaf, or covers almost a whole page on its own. Everything else appears "
        "under ‘more matches’."
    ),
    "he": (
        "קטע נחשב לזיהוי סביר כאשר הוא תואם את החיבור ביותר מדף אחד, או מכסה כמעט עמוד שלם "
        "בפני עצמו. כל השאר מופיע תחת ‘התאמות נוספות’."
    ),
}


def main_pool_sentence(lang: str = "en") -> str:
    """The ONE reader-facing sentence describing the two-bucket rule, in EN
    or HE (unknown `lang` defaults to EN, matching
    `shared.discovery_band_labels.band_label`'s own convention). Contains no
    percentage and none of the prohibited relation words ("copy of",
    "quotes", "witness of") -- D-06/D-21's no-numbers, no-overclaiming
    posture. This exact string is pinned byte-for-byte against
    `web/pages/help.py`'s `MAIN_POOL_SENTENCE` constant by a test that reads
    (never imports) that module's source."""
    lang_key = "he" if lang == "he" else "en"
    return _MAIN_POOL_SENTENCE[lang_key]


# The two bilingual bucket names (main-pool-rule.md, "Wording and internal
# state": "Bucket names follow the owner: main pool / more matches"). Kept
# as a private table so `bucket_label` is the ONLY way to read them --
# nothing outside this module names a bucket directly.
_BUCKET_LABEL_MAIN: Mapping[str, str] = {
    "en": "main pool",
    "he": "מאגר עיקרי",
}

_BUCKET_LABEL_MORE: Mapping[str, str] = {
    "en": "more matches",
    "he": "התאמות נוספות",
}


def bucket_label(in_main_pool: bool, lang: str = "en") -> str:
    """The bilingual display name of the bucket `in_main_pool` selects --
    `main_pool_decision`'s own boolean, never re-derived. `bucket_label` is
    the ONLY definition of these two names anywhere under `shared/` (a
    standing guard in `tests/test_discovery_main_pool.py` asserts no module
    under `shared/` or `web/` defines a second bucket-membership predicate).

    **The second bucket ("more matches") means there was not enough
    evidence for the main-pool rule -- it never means the identification is
    probably wrong.** It holds probable quotations, shared wording,
    unresolved ties, missing signals and genuinely indeterminate cases
    alike (main-pool-rule.md, "Wording and internal state"); a caller must
    never render it as a confidence level or a correctness verdict."""
    lang_key = "he" if lang == "he" else "en"
    return _BUCKET_LABEL_MAIN[lang_key] if in_main_pool else _BUCKET_LABEL_MORE[lang_key]
