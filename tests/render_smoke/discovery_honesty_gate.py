# -*- coding: utf-8 -*-
"""The ONE shared discovery-surface honesty gate (Phase 136, plan 136-02).

`assert_discovery_honesty(rendered_html, *, scope_selector, lang)` is the
single implementation of the D-06 / D-06a "no numbers" posture (see
`docs/specs/discovery-band-labels-v1.md` Amendment 2026-08-02 and
`.planning/phases/136-read-surfaces-connections-panel-work-witnesses/
136-CONTEXT.md`). The methods-page suite (this plan) is its FIRST caller;
every later Phase-136 surface suite (the browse-page panel, `/work/{id}`,
the corpus-wide findings page, `/catalog-browse`) imports this SAME function
rather than re-implementing the rule, so the rule can drift in exactly one
place if it ever changes, and never silently diverges across surfaces.

What it checks, over the text of the scoped subtree ONLY:

1. No percent-formatted figure that could read as a precision estimate --
   EXCEPT the one legitimate exception (matched-letter coverage, D-08/D-21:
   "Matches <work> - 68% of page"), which is permitted ONLY in its qualified
   form (the percentage must sit next to the coverage qualifier). A bare
   percentage is always rejected.
2. No bracketed confidence interval (``[0.9084, 0.9644]`` or ``[87.5%,
   96.8%]``-shaped).
3. No human-review badge string, in either language (the D-13f/Note-2
   "Expert-reviewed" / "נבדק בידי
   מומחה" markers -- no Phase-136 surface renders
   the review overlay at all).
4. None of the prohibited relation words ``copy of`` / ``quotes`` /
   ``witness of`` (D-21), matched case-insensitively with word boundaries so
   a NEGATED use ("not... a copy of...") still fails -- exactly the trap the
   findings-page sketch fell into (see
   ``.claude/skills/sketch-findings-genizahsearch/references/
   findings-page.md``, "Verification").
5. No raw stored vocabulary key (``direct_witness``, ``tier_a``,
   ``screening_rb``, ...) leaking onto the surface as literal text -- a
   surface must always go through ``band_label()`` / relation display
   wording, never echo the underlying enum value. Scoped to snake_case
   (underscore-bearing) tokens only, so ordinary English words that happen
   to equal a *different* enum member without an underscore (e.g. "weak",
   "corroborated") are never flagged.

**Scope is mandatory, not optional.** ``scope_selector`` must be a non-empty
class name that matches at least one element in ``rendered_html``, or this
raises ``DiscoveryHonestyScopeError`` rather than silently checking the
(possibly unrelated) whole document. An assertion that can pass for the
wrong reason is worse than none -- the findings-page sketch's own
facet-header assertion PASSED while the header was wrong, because it tested
the whole rendered page and unrelated prose elsewhere on the page happened
to contain the grepped phrase.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any, List, Mapping, Tuple

import scripts.discovery_ids as ids
from shared.discovery_band_labels import MEASUREMENT_STATUSES
from shared.discovery_main_pool import MAIN_POOL_REASONS
from shared.discovery_novelty import NOVELTY_STATUSES
from shared.discovery_service import (
    FACET_LEVELS,
    FINDINGS_BUCKETS,
    FINDINGS_SORTS,
    FINDINGS_UNITS,
    LAUNCH_BASIS,
    LAUNCH_CONTRIBUTION_SHADES,
    _FINDINGS_SORT_BASIS,
)
from shared.discovery_surface_projection import _ALL_ALLOWLISTS

__all__ = [
    "ALLOWLIST_FIELD_UNION",
    "COVERAGE_STATUSES",
    "D06A_QUALITATIVE_SCOPES",
    "DiscoveryHonestyScopeError",
    "DiscoveryHonestyViolation",
    "ELIGIBILITY_BASES",
    "KNOWN_CARRIER_FLOOR",
    "MACHINE_VOCABULARY_FIELDS",
    "META_FREE_TEXT_KEYS",
    "META_VOCABULARY_FIELDS",
    "READER_TEXT_FIELDS",
    "REGISTRY_MATCH_EXCLUSIONS",
    "REGISTRY_MEMBERS",
    "assert_discovery_honesty",
    "assert_envelope_honesty",
    "assert_error_path_honesty",
    "assert_surface_honesty",
    "find_envelope_violations",
    "machine_shape_violation",
    "registry_membership_violation",
]


class DiscoveryHonestyScopeError(ValueError):
    """Raised when ``scope_selector`` is missing or matches nothing in the
    supplied markup -- refusing to run an honesty check over unscoped (or
    wrongly-scoped) content."""


class DiscoveryHonestyViolation(AssertionError):
    """Raised when the scoped text of a rendered discovery surface fails the
    D-06/D-06a honesty rules (a percentage/interval/review-badge/prohibited-
    wording/raw-vocab-key leak was found)."""


# ---------------------------------------------------------------------------
# Scoped text extraction -- stdlib-only (no new dependency; Rule 3 of the
# executor's deviation rules excludes package installs from auto-fixes, and
# none is needed here).
# ---------------------------------------------------------------------------

class _ClassScopedTextExtractor(HTMLParser):
    """Collects the text content of every element (and its descendants)
    whose ``class`` attribute contains ``target_class`` as one of its
    space-separated tokens.

    Tracks a stack of the depths at which a matching element was opened;
    text is captured whenever that stack is non-empty (i.e. the parser is
    currently inside at least one matched element). This correctly handles
    nested matches without double-counting: text inside a nested matched
    child is captured once, not once per ancestor."""

    def __init__(self, target_class: str):
        super().__init__(convert_charrefs=True)
        self._target_class = target_class
        self._depth = 0
        self._scope_open_depths: List[int] = []
        self.matched_any = False
        self._chunks: List[str] = []

    def _tag_has_target_class(self, attrs) -> bool:
        for name, value in attrs:
            if name == "class" and value:
                if self._target_class in value.split():
                    return True
        return False

    def handle_starttag(self, tag, attrs):
        if self._tag_has_target_class(attrs):
            self.matched_any = True
            self._scope_open_depths.append(self._depth)
        self._depth += 1

    def handle_startendtag(self, tag, attrs):
        # Self-closing tag (e.g. ``<br/>``) -- class match still counts, but
        # there is no separate end tag to balance the depth counter.
        if self._tag_has_target_class(attrs):
            self.matched_any = True

    def handle_endtag(self, tag):
        if self._depth > 0:
            self._depth -= 1
        if self._scope_open_depths and self._scope_open_depths[-1] == self._depth:
            self._scope_open_depths.pop()

    def handle_data(self, data):
        if self._scope_open_depths:
            self._chunks.append(data)

    def get_text(self) -> str:
        return "".join(self._chunks)


def _extract_scoped_text(rendered_html: str, scope_selector: str) -> str:
    extractor = _ClassScopedTextExtractor(scope_selector)
    extractor.feed(rendered_html or "")
    extractor.close()
    if not extractor.matched_any:
        raise DiscoveryHonestyScopeError(
            f"assert_discovery_honesty: scope_selector {scope_selector!r} matched no "
            "element in the rendered markup -- refusing to run an honesty check over "
            "unscoped (or wrongly-scoped) content. See findings-page.md 'Verification' "
            "for why an unscoped assertion is worse than none."
        )
    return extractor.get_text()


# ---------------------------------------------------------------------------
# Check 1 -- percent-formatted figures, with the qualified matched-letter
# coverage exception (D-08/D-21).
# ---------------------------------------------------------------------------

_PERCENT_RE = re.compile(r"\d+(?:\.\d+)?\s*%")

#: The SAME quantity spelled as a WORD. `91%` failed every detector while
#: `accuracy is 91 percent` passed all six -- the sign is not the claim, and a
#: surface that writes the word instead of the glyph makes exactly the assertion
#: D-06 prohibits (round 12, finding 3).
#:
#: A NUMBER is required immediately before the word, and that requirement is
#: load-bearing in both languages. Bare "percentage" is honest prose the shipped
#: methods page already carries -- "in words, never as a percentage or an
#: interval" / "לא כאחוז או כטווח" -- and a detector that fires on it would turn
#: an owner-approved page red on the sentence that promises no percentage.
_PERCENT_WORD_RE = re.compile(
    r"\d+(?:\.\d+)?\s*(?:percentage\s+points?|percent|per\s+cent|pct"
    r"|אחוזים|אחוזי|אחוז)",
    re.IGNORECASE,
)

# The coverage qualifier that must sit near a permitted percentage --
# "Matches <work> · 68% of page" (EN) / "התאמה
# ל<חיבור> · 68% מהדף" (HE).
_COVERAGE_QUALIFIER_BY_LANG = {
    "en": "of page",
    "he": "מהדף",  # מהדף
}
_COVERAGE_QUALIFIER_WINDOW = 32  # chars scanned after the percentage match


def _iter_percentages(text: str):
    """Every percentage in `text`, glyph-formatted OR word-formatted."""
    for m in _PERCENT_RE.finditer(text):
        yield m
    for m in _PERCENT_WORD_RE.finditer(text):
        yield m


def _find_unqualified_percentages(text: str, lang: str) -> List[str]:
    qualifier = _COVERAGE_QUALIFIER_BY_LANG.get("he" if lang == "he" else "en")
    violations = []
    for m in _iter_percentages(text):
        window = text[m.end():m.end() + _COVERAGE_QUALIFIER_WINDOW]
        if qualifier not in window:
            violations.append(f"unqualified percentage {m.group(0)!r}")
    return violations


# ---------------------------------------------------------------------------
# Check 2 -- bracketed confidence intervals.
# ---------------------------------------------------------------------------

_BRACKET_INTERVAL_RE = re.compile(r"\[\s*\d+(?:\.\d+)?%?\s*,\s*\d+(?:\.\d+)?%?\s*\]")


def _find_bracketed_intervals(text: str) -> List[str]:
    return [f"bracketed interval {m.group(0)!r}" for m in _BRACKET_INTERVAL_RE.finditer(text)]


# ---------------------------------------------------------------------------
# Check 3 -- human-review badge strings (D-13f / band-labels-v1.md Note 2).
# ---------------------------------------------------------------------------

_REVIEW_BADGE_EN = "expert-reviewed"
_REVIEW_BADGE_HE = "נבדק בידי מומחה"  # נבדק בידי מומחה


def _find_review_badges(text: str) -> List[str]:
    violations = []
    if _REVIEW_BADGE_EN in text.lower():
        violations.append("human-review badge 'Expert-reviewed'")
    if _REVIEW_BADGE_HE in text:
        violations.append(f"human-review badge {_REVIEW_BADGE_HE!r}")
    return violations


# ---------------------------------------------------------------------------
# Check 4 -- prohibited relation wording (D-21), negation-proof.
# ---------------------------------------------------------------------------

_PROHIBITED_PHRASES = ("copy of", "quotes", "witness of")


def _find_prohibited_phrases(text: str) -> List[str]:
    lowered = text.lower()
    violations = []
    for phrase in _PROHIBITED_PHRASES:
        if re.search(r"\b" + re.escape(phrase) + r"\b", lowered):
            violations.append(f"prohibited relation wording {phrase!r}")
    return violations


# ---------------------------------------------------------------------------
# Check 5 -- raw stored vocabulary keys leaking onto a surface. Scoped to the
# schema's frozen enum vocabularies (scripts.discovery_ids -- the ONE source
# of truth, same discipline as shared/discovery_band_labels.py) plus the
# band-labels module's measurement_status vocabulary. Restricted to
# UNDERSCORE-BEARING tokens only: "direct_witness"/"tier_a"/"screening_rb"
# could never appear in honest prose, whereas plain-English band names like
# "weak" or "corroborated" legitimately could and must not false-positive.
# ---------------------------------------------------------------------------

def _collect_prohibited_vocab_keys() -> frozenset:
    """Every stored vocabulary a discovery surface may NOT echo raw.

    Plan 136-17 (gap A) added `NOVELTY_STATUSES` and `MAIN_POOL_REASONS`. They
    were not an oversight of taste: the projection carries `novelty_status` on
    `SURFACE_CLAIM_FIELDS`/`SURFACE_FINDING_FIELDS` and `main_pool_reason` on
    both, and measurement against the public sidecar found an underscore-bearing
    `main_pool_reason` on every one of its 53,581 rows -- so `direct_witness`
    seeded into `band_label` FAILED while `fills_gap` in the SAME field passed.

    Enumerating the two that were missing is not the fix, and the fix is not
    here: `MACHINE_VOCABULARY_FIELDS` below is a field->vocabulary MAPPING, and
    every underscore-bearing member of every mapped vocabulary is UNIONED into
    this set at the bottom of the module. Declaring a carrier is therefore what
    prohibits its values everywhere else -- literally, not by anyone remembering
    to edit two places. The explicit union below is kept as the independently
    maintained half that the collection-time assertion (c) actually checks.
    """
    keys = set(ids.CLAIM_TYPES)
    for band_set in ids.CONFIDENCE_BANDS_BY_SOURCE.values():
        keys.update(band_set)
    keys.update(ids.ADJUDICATION_STATUSES)
    keys.update(ids.ROUTING_STATUSES)
    keys.update(ids.EVIDENCE_SOURCES)
    keys.update(MEASUREMENT_STATUSES)
    keys.update(NOVELTY_STATUSES)
    keys.update(MAIN_POOL_REASONS)
    return frozenset(k for k in keys if "_" in k)


_PROHIBITED_RAW_VOCAB_KEYS = _collect_prohibited_vocab_keys()


def _find_raw_vocab_keys(text: str) -> List[str]:
    violations = []
    for key in _PROHIBITED_RAW_VOCAB_KEYS:
        if re.search(r"\b" + re.escape(key) + r"\b", text):
            violations.append(f"raw stored vocabulary key {key!r}")
    return violations


# ---------------------------------------------------------------------------
# Check 6 (plan 136-17, gap B) -- an ACCURACY / RATE claim.
#
# The standing rule prohibits accuracy rates on every surface, envelopes and
# error paths included. Measured against the five detectors above, `accuracy
# 0.91` produced NO violation through any field, exempt or not, while a
# percent-formatted rate, a bracketed interval and an exact badge all failed.
#
# TWO RULES, and the restriction on each is what stops it rejecting correct
# output:
#
#   1. a RATE WORD within a short window of a RATE-SHAPED QUANTITY. Deliberately
#      NOT "any number": a methods surface may legitimately write "measured on
#      400 cards", and a detector that fires on that is one the next person
#      deletes. A rate-shaped quantity includes a percentage spelled as a WORD
#      ("91 percent" / "91 אחוז"), which is the form that escaped every detector
#      until round 12.
#   2. a bare decimal fraction in [0, 1] with two or more decimal places, which
#      is never legitimate discovery prose.
#
# A VERSION-SHAPED token is excluded from BOTH, bounded to explicit `v`/`V` at a
# word boundary or `version`/`גרסה` syntax -- a wider exclusion (any word
# character before the integer part) lets `accuracy score0.9` escape both rules,
# and raising rule 1's decimal minimum to two places lets `accuracy 0.9` escape.
# ---------------------------------------------------------------------------

_RATE_WORDS_EN: Tuple[str, ...] = (
    "accuracy", "accurate", "precision", "recall", "error rate", "hit rate",
    "success rate", "correct", "right", "ratio", "proportion", "f1",
    "of the time", "share", "misattributed",
)
_RATE_WORDS_HE: Tuple[str, ...] = (
    "דיוק", "מדויק", "שיעור", "נכונות", "נכונים", "שגיאה", "נתח",
    "משויך בטעות",
)

#: Quantity words that are themselves RATE-SHAPED. `minority` and `single-digit`
#: are here rather than among the rate words on purpose: putting all four of
#: D-06a's words on the rate side would leave that sentence with no rate-shaped
#: quantity at all, and positive control 12 could then never fire -- which makes
#: the control inert rather than the exception bounded.
_QUANTITY_WORDS_EN: Tuple[str, ...] = (
    "half", "most", "majority", "third", "quarter", "minority", "single-digit",
)
_QUANTITY_WORDS_HE: Tuple[str, ...] = (
    "מחצית", "רוב", "שליש", "רבע", "מיעוט", "חד-ספרתי",
)

_RATE_WINDOW = 48          # chars either side of the rate word
_VERSION_TOKEN_RE = re.compile(r"(?:(?<![\w])[vV]|version\s*|גרסה\s*|גירסה\s*)\d+(?:\.\d+)*")
#: A decimal with an EXPLICIT integer part, and NO lookbehind.
#:
#: The integer part is what keeps a shelfmark out of it -- `MS Heb c.57` would
#: otherwise read as the fraction `.57`, in [0, 1] with two decimal places, and
#: rule 2 would reject a legitimate shelfmark. The absence of a lookbehind is
#: what keeps `accuracy score0.9` IN: excluding a decimal preceded by any word
#: character was the previous revision's own defect (round 9, finding 6), and
#: the version exclusion below is the ONLY exclusion this detector has.
_DECIMAL_RE = re.compile(r"\d+\.\d+")
_N_OUT_OF_M_RE = re.compile(r"\b\d+\s*(?:out of|in|/|מתוך)\s*\d+\b", re.IGNORECASE)


def _strip_version_tokens(text: str) -> str:
    """Blank out explicit version syntax so neither rule sees its decimal.

    Bounded to `v`/`V` at a word boundary or an explicit `version` word: a
    glued alphanumeric prefix that is not `v`/`V` is NOT a version, so
    `accuracy score0.9` still fails (round 9, finding 6). Replaced by spaces of
    the same length so every offset in the caller's text is preserved.
    """
    return _VERSION_TOKEN_RE.sub(lambda m: " " * len(m.group(0)), text)


def _rate_shaped_spans(text: str) -> List[Tuple[int, int, str]]:
    """Every rate-SHAPED quantity in `text`, as `(start, end, what)`."""
    spans: List[Tuple[int, int, str]] = []
    # BOTH spellings of a percentage. `_iter_percentages` is the shared
    # authority: adding a form there arms check 1, this detector and the D-06a
    # words-only test at once, which is what stops the three drifting apart.
    for m in _iter_percentages(text):
        spans.append((m.start(), m.end(), f"percentage {m.group(0).strip()!r}"))
    for m in _DECIMAL_RE.finditer(text):
        try:
            value = float(m.group(0))
        except ValueError:                                  # pragma: no cover
            continue
        if 0.0 <= value <= 1.0:
            spans.append((m.start(), m.end(), f"fraction {m.group(0)!r}"))
    for m in _N_OUT_OF_M_RE.finditer(text):
        spans.append((m.start(), m.end(), f"an N-out-of-M form {m.group(0)!r}"))
    lowered = text.lower()
    for word in _QUANTITY_WORDS_EN:
        for m in re.finditer(r"\b" + re.escape(word) + r"\b", lowered):
            spans.append((m.start(), m.end(), f"quantity word {word!r}"))
    for word in _QUANTITY_WORDS_HE:
        for m in re.finditer(re.escape(word), text):
            spans.append((m.start(), m.end(), f"quantity word {word!r}"))
    return spans


def _find_accuracy_rates(text: str, lang: str = "en") -> List[str]:
    """Rule 1 + rule 2. `lang` is accepted for symmetry with the other
    detectors; BOTH lexicons always run, because a Hebrew surface can carry an
    English rate word and the reverse."""
    stripped = _strip_version_tokens(text)
    lowered = stripped.lower()
    violations: List[str] = []

    quantities = _rate_shaped_spans(stripped)
    for word in _RATE_WORDS_EN:
        for m in re.finditer(r"\b" + re.escape(word) + r"\b", lowered):
            for start, end, what in quantities:
                if start < m.end() + _RATE_WINDOW and end > m.start() - _RATE_WINDOW:
                    violations.append(
                        f"accuracy/rate claim: rate word {word!r} beside {what}")
    for word in _RATE_WORDS_HE:
        for m in re.finditer(re.escape(word), stripped):
            for start, end, what in quantities:
                if start < m.end() + _RATE_WINDOW and end > m.start() - _RATE_WINDOW:
                    violations.append(
                        f"accuracy/rate claim: rate word {word!r} beside {what}")

    for m in _DECIMAL_RE.finditer(stripped):
        token = m.group(0)
        if len(token.split(".")[1]) < 2:
            continue
        try:
            value = float(token)
        except ValueError:                                  # pragma: no cover
            continue
        if 0.0 <= value <= 1.0:
            violations.append(f"bare rate-shaped decimal {token!r}")

    return sorted(set(violations))


# ---------------------------------------------------------------------------
# D-06a's ONE named exception, bound to ONE registered ELEMENT.
#
# Prose characterising how OFTEN the system is wrong in WORDS ONLY -- no number,
# no percentage, no interval -- is compliant INSIDE the one element where the
# owner approved it, and is a VIOLATION everywhere else. The live instance is
# the methods page's limitations paragraph (`web/pages/help.py::_LIMITATIONS_TEXT`),
# the D-06a rewrite plan 136-02 delivered, whose own render test requires the
# wording.
#
# The class is held here as a LITERAL rather than imported: this module is
# deliberately dependency-light (`re`, `html.parser`, `typing`, `scripts.*`,
# `shared.*` and NOTHING from `web`), because every render-smoke suite imports
# it and a module-level `from web.pages.help import ...` would pull NiceGUI into
# all of them. `tests/render_smoke/test_panel_render_smoke.py` pins it to its
# authority with a LAZY-import equality assertion, so a rename fails by name
# instead of silently unbinding the exception.
#
# THE EXEMPTION IS A CONJUNCTION, NOT A SWITCH: it applies only when the
# statement is words-only AND the scan is scoped to the registered element.
# A NUMERIC rate inside the limitations paragraph still fails -- the percentage,
# interval and bare-decimal rules are not exempted at all.
# ---------------------------------------------------------------------------

D06A_QUALITATIVE_SCOPES: Tuple[str, ...] = ("discovery-methods-limitations",)


def _is_words_only_rate_statement(text: str) -> bool:
    """True when the text carries no number-shaped rate at all -- so the only
    thing the accuracy detector could have fired on is the qualitative
    vocabulary itself."""
    stripped = _strip_version_tokens(text)
    if _BRACKET_INTERVAL_RE.search(stripped):
        return False
    # A WORD-spelled percentage is a number-shaped rate too, so the D-06a
    # exemption never reaches it: "wrong in a minority of cases" is words-only,
    # "wrong 9 percent of the time" is not.
    for _m in _iter_percentages(stripped):
        return False
    if _N_OUT_OF_M_RE.search(stripped):
        return False
    for m in _DECIMAL_RE.finditer(stripped):
        try:
            value = float(m.group(0))
        except ValueError:                                  # pragma: no cover
            continue
        if 0.0 <= value <= 1.0:
            return False
    return True


# ---------------------------------------------------------------------------
# The public entry points.
# ---------------------------------------------------------------------------

def assert_discovery_honesty(
    rendered_html: str, *, scope_selector: str, lang: str,
    check_accuracy: bool = False,
) -> None:
    """Assert that the text of the ``scope_selector``-scoped subtree of
    ``rendered_html`` carries none of the D-06/D-06a dishonesty patterns.

    Raises ``DiscoveryHonestyScopeError`` if ``scope_selector`` is falsy or
    matches nothing. Raises ``DiscoveryHonestyViolation`` (with every
    violation found, not just the first) if the scoped text fails any check.
    Returns ``None`` on success.
    """
    if not scope_selector:
        raise DiscoveryHonestyScopeError(
            "assert_discovery_honesty: scope_selector is required and must be a "
            "non-empty class name -- an honesty check run over unscoped markup can "
            "pass for the wrong reason (see findings-page.md 'Verification')."
        )

    text = _extract_scoped_text(rendered_html, scope_selector)

    violations: List[str] = []
    violations.extend(_find_unqualified_percentages(text, lang))
    violations.extend(_find_bracketed_intervals(text))
    violations.extend(_find_review_badges(text))
    violations.extend(_find_prohibited_phrases(text))
    violations.extend(_find_raw_vocab_keys(text))
    if check_accuracy:
        # Exception (3): D-06a-sanctioned QUALITATIVE error-rate language,
        # available to the MARKUP scan ONLY, and only when this scan's mandatory
        # `scope_selector` is a registered D-06a element AND the statement is
        # words-only. The envelope and error-path scans have no scope and can
        # never receive it; 136-18's findings markup scopes to its own selectors
        # and therefore never receives it either.
        exempt = scope_selector in D06A_QUALITATIVE_SCOPES and \
            _is_words_only_rate_statement(text)
        if not exempt:
            violations.extend(_find_accuracy_rates(text, lang))

    if violations:
        raise DiscoveryHonestyViolation(
            f"assert_discovery_honesty: dishonest content found in scope "
            f"{scope_selector!r} (lang={lang!r}): " + "; ".join(violations)
        )


def assert_surface_honesty(rendered_html: str, *, scope_selector: str, lang: str) -> None:
    """THE entry point for every Phase-136 reader SURFACE (the browse-page
    connections panel, the corpus-wide findings page, `/work/{id}`): all SIX
    detectors, accuracy included.

    Separate from `assert_discovery_honesty` because that function's five-detector
    contract is what two ALREADY-SHIPPED suites call, over flattened
    card-scoped text that includes the owner-approved D-06a limitations
    sentence. Wiring the sixth detector into it unconditionally would turn
    `tests/render_smoke/test_help_methods_render_smoke.py` red on wording the
    owner approved -- and that file is outside plan 136-17's `files_modified`.
    See that plan's summary, "the one criterion pair in tension".
    """
    assert_discovery_honesty(
        rendered_html, scope_selector=scope_selector, lang=lang, check_accuracy=True)


# ===========================================================================
# THE CLASSIFICATION (plan 136-17, gap A, second half).
#
# `_ALL_ALLOWLISTS` already IS the authority on which fields a surface may
# receive. The classification below is an EXACT PARTITION of that union, and
# the suite asserts set EQUALITY against it -- so a carrier nobody classified
# fails at collection time, by name, rather than firing the strict reader-facing
# scan on tens of thousands of CORRECT rows at render time.
#
# Four earlier revisions of this mechanism proved a GLOBAL property ("no carrier
# is unclassified") by SAMPLING a value corpus with an escape list beside it,
# and each time the enumeration was short in a new place -- `novelty_status`,
# `main_pool_reason`, then `routing_reason` / `coverage_status` /
# `eligibility_basis`, then 136-22's `shade`. Enumeration is not the mechanism;
# the equality is.
# ===========================================================================

#: The ground truth, recomputed from `_ALL_ALLOWLISTS` itself and never retyped.
ALLOWLIST_FIELD_UNION: frozenset = frozenset(
    field for _name, _fields in _ALL_ALLOWLISTS for field in _fields
)

_ALL_CONFIDENCE_BANDS: frozenset = frozenset(
    band for band_set in ids.CONFIDENCE_BANDS_BY_SOURCE.values() for band in band_set
)

#: Two carriers with NO exported frozen constant anywhere. Declared here because
#: this module is the only file plan 136-17 owns that can hold them; exporting
#: both properly to `shared/` is OWED FOLLOW-UP. Each is PINNED to its authority
#: by a test that READS that authority at test time -- a hand-copied set that
#: cannot fail when its producer changes is the same defect in a smaller box.
#:
#: `coverage_status`: the `CHECK (coverage_status IN (...))` literal in
#: `scripts/build_discovery_sidecar.py`.
COVERAGE_STATUSES: frozenset = frozenset({"measured", "no_denominator", "not_applicable"})
#: `eligibility_basis`: the `CASE ... END AS eligibility_basis` literals in
#: `shared/discovery_service.py`.
ELIGIBILITY_BASES: frozenset = frozenset({"shipped", "human_confirmed", "review_opt_in"})

#: FIELD -> the frozen vocabulary that field carries. Classifying a field as a
#: machine carrier REQUIRES naming its vocabulary, and naming a vocabulary puts
#: every underscore-bearing member of it into the prohibited set for every OTHER
#: field -- so a false classification does not go quiet, it breaks the suite
#: somewhere else. Every mapping must also be NON-EMPTY and FAITHFUL to the
#: values observed under its field (assertion (h)): without that, a reader field
#: mapped to `frozenset()` keeps the partition exact, makes the
#: vocabulary-prohibition assertion vacuous, and is exempted from strict
#: scanning -- a broken implementation passing the stated gate.
MACHINE_VOCABULARY_FIELDS: Mapping[str, frozenset] = {
    "relation_kind": frozenset(ids.CLAIM_TYPES),
    "claim_type": frozenset(ids.CLAIM_TYPES),
    "anchor_claim_type": frozenset(ids.CLAIM_TYPES),
    "evidence_source": frozenset(ids.EVIDENCE_SOURCES),
    "displayed_evidence_source": frozenset(ids.EVIDENCE_SOURCES),
    "confidence_band": _ALL_CONFIDENCE_BANDS,
    "displayed_confidence_band": _ALL_CONFIDENCE_BANDS,
    "adjudication_status": frozenset(ids.ADJUDICATION_STATUSES),
    "routing_status": frozenset(ids.ROUTING_STATUSES),
    "routing_reason": frozenset(ids.ROUTING_REASONS),
    "measurement_status": frozenset(MEASUREMENT_STATUSES),
    "novelty_status": frozenset(NOVELTY_STATUSES),
    "main_pool_reason": frozenset(MAIN_POOL_REASONS),
    "coverage_status": COVERAGE_STATUSES,
    "eligibility_basis": ELIGIBILITY_BASES,
    "shade": frozenset(LAUNCH_CONTRIBUTION_SHADES),
    "unit": frozenset(FINDINGS_UNITS),
    "level": frozenset(FACET_LEVELS),
}

#: The NON-CARRIER half: FIELD -> a written reason naming both its KIND and its
#: PRODUCER. The name is kept for continuity with the reader-facing floor;
#: read it as "everything that is not a stored-vocabulary carrier". The kinds
#: are the ones the allowlists actually carry -- reader text, identity/digest,
#: numeric, boolean and id-list -- because a two-category split with no home for
#: `relations_differ` (bool), `band_rank` (int) or `member_sys_ids` (id list)
#: could not be satisfied by a CORRECT gate.
#:
#: NAMING THE PRODUCER IS REQUIRED, and it is the only bound available on the
#: one documented residual: a carrier whose vocabulary is single-word AND
#: exported nowhere AND declared here is invisible to both value rules. The
#: partition still forces it to be CLASSIFIED; what it cannot force is the
#: classification being right.
READER_TEXT_FIELDS: Mapping[str, str] = {
    # -- identity / digest ---------------------------------------------------
    "page_id": "identity: the corpus page header, web/services.py::discovery_page_id_from_header",
    "sys_id": "identity: the Alma system number, libraries.csv",
    "claim_id": "identity: a sha256 digest, scripts/discovery_ids.py::claim_id",
    "evidence_id": "identity: a sha256 digest, scripts/discovery_ids.py::evidence_id",
    "identification_id": "identity: a sha256 digest, scripts/discovery_ids.py",
    "work_id": "identity: a w-prefixed key, scripts/discovery_ids.py",
    "canonical_work_id": "identity: a w-prefixed key, scripts/discovery_ids.py",
    "display_work_id": "identity: a w-prefixed key, scripts/discovery_ids.py",
    "unit_id": "identity: the witness_unit key, shared/discovery_service.py witness_unit",
    "representative_sys_id": "identity: an Alma system number, witness_unit_members",
    "representative_page_id": "identity: a corpus page header, discovery_claim.page_id",
    "representative_claim_id": "identity: a sha256 digest, scripts/discovery_ids.py",
    "related_page_id": "identity: a corpus page header, discovery_evidence",
    "member_sys_ids": "id-list: Alma system numbers, witness_unit_members",
    "value": "identity: the facet's own key, DiscoveryService._project_facets",
    "parent": "identity: the parent facet's key, DiscoveryService._project_facets",
    # -- reader text ---------------------------------------------------------
    "neutral_title": "reader text: works.neutral_title (routed through display_work_title)",
    "author": "reader text: works.author",
    "genre": "reader text: the FJMS genre string, fjms_enrichment.db",
    "domain": "reader text: the FJMS domain string, fjms_enrichment.db",
    "library_code": "reader text: libraries.csv library_code",
    "shelfmark_display": "reader text: manuscript_display.shelfmark_display",
    "band_label": "reader text: shared/discovery_band_labels.py::serialize_banded_claim",
    "novelty_source_label": "reader text (masked): shared/discovery_novelty.py MASKED_PROVENANCE_LABELS",
    "label": "reader text: the facet's display label, DiscoveryService._project_facets",
    # -- numeric -------------------------------------------------------------
    "band_rank": "numeric: shared/discovery_band_labels.py::_band_rank",
    "best_band_rank": "numeric: MIN(band_rank) over the manuscript, DiscoveryService",
    "coverage_ppm": "numeric: discovery_evidence.coverage_ppm",
    "max_coverage_ppm": "numeric: MAX(coverage_ppm), DiscoveryService findings query",
    "matched_letters": "numeric: discovery_evidence.matched_letters",
    "span_start": "numeric: discovery_evidence.span_start",
    "span_end": "numeric: discovery_evidence.span_end",
    "n_spans": "numeric: discovery_evidence.n_spans",
    "page_count": "numeric: COUNT(DISTINCT page_id), DiscoveryService",
    "identification_page_count": "numeric: discovery_identification.page_count",
    "evidence_row_count": "numeric: COUNT(*) over discovery_evidence",
    "count": "numeric: the facet count, DiscoveryService._project_facets",
    "work_count": "numeric: COUNT(DISTINCT work_id), DiscoveryService findings query",
    "manuscript_count": "numeric: COUNT(DISTINCT sys_id), DiscoveryService",
    "identification_count": "numeric: COUNT(*) per shade, DiscoveryService launch stats",
    # -- boolean -------------------------------------------------------------
    "title_missing": "boolean: works.neutral_title IS NULL, DiscoveryService",
    "main_pool": "boolean: discovery_identification.main_pool, materialized by scripts/build_discovery_sidecar.py",
    "low_coverage_marker": "boolean: shared/discovery_main_pool.py coverage floor",
    "restored_by_human_confirmation": "boolean: routing_status<>shipped AND adjudication_status=human_confirmed",
    "default_eligible": "boolean: D-13g's two-limb predicate, shared/discovery_service.py::_CLAIMS_DEFAULT_ROUTING_CLAUSE",
    "gated": "boolean: the screening-gate flag, shared/discovery_service.py::get_manuscript_works_enveloped",
    "display_missing": "boolean: manuscript_display row absent, DiscoveryService expansion query",
    "relations_differ": "boolean: claim_type <> anchor_claim_type, DiscoveryService expansion query",
    "is_leaf": "boolean: the facet tree's leaf flag, DiscoveryService._project_facets",
    "novelty_offered": "boolean: whether the novelty axis is offered, shared/discovery_service.py::get_findings_enveloped",
    "multi_work_annotation": "boolean: work_count > 1, DiscoveryService findings query",
}

#: Every carrier known TODAY, named in ONE place. A FLOOR on the machine half --
#: never the definition of the carrier set (the partition is that) and never an
#: assertion that all of them appear in the OBSERVED values.
#:
#: `claim_type`, `anchor_claim_type`, `displayed_evidence_source` and
#: `displayed_confidence_band` arrive with plan 136-21's
#: `SURFACE_EXPANSION_FIELDS`, and `shade` with 136-22's
#: `SURFACE_LAUNCH_SHADE_FIELDS`; both are registered in `_ALL_ALLOWLISTS`
#: before this gate runs. If a member is ever missing from the union, establish
#: which namespace it belongs to BEFORE deleting it -- dropping a name because
#: an assertion failed is how a real carrier gets exempted, which is the failure
#: this whole mechanism exists to prevent.
KNOWN_CARRIER_FLOOR: frozenset = frozenset({
    "relation_kind", "claim_type", "anchor_claim_type",
    "evidence_source", "displayed_evidence_source",
    "confidence_band", "displayed_confidence_band",
    "adjudication_status", "routing_status", "routing_reason",
    "measurement_status", "novelty_status", "main_pool_reason",
    "coverage_status", "eligibility_basis", "shade",
})

# ---------------------------------------------------------------------------
# `meta` is NOT allowlist-governed, so it gets its own partition -- and the
# separation is the point: the field partition's equality is against
# `ALLOWLIST_FIELD_UNION`, and putting `meta` keys in the same mapping would
# make the two checks contradict each other.
#
# Being classified here EXEMPTS NOTHING from the strict reader-facing scan:
# meta['reason'] = 'direct_witness' must still fail loudly, and a test asserts
# it does.
# ---------------------------------------------------------------------------

#: KEY -> its closed value set. The key FLOOR (the keys known to need an entry
#: today) is `reason`, `sort`, `sort_basis`, `basis`, `filter_basis`, `unit` and
#: the expansion's `anchor_mode`. That is a floor on the KEYS, not the source of
#: the VALUES: each value set comes from the code that produces it, pinned to an
#: exported constant wherever one exists.
META_VOCABULARY_FIELDS: Mapping[str, frozenset] = {
    "reason": frozenset({
        "sidecar_not_serving", "query_failed", "query_timeout", "bounded_concurrency",
    }),
    "unit": frozenset(FINDINGS_UNITS) | frozenset({"distinct_opposite_pages"}),
    "sort": frozenset(FINDINGS_SORTS),
    "sort_basis": frozenset(_FINDINGS_SORT_BASIS.values()),
    "bucket": frozenset(FINDINGS_BUCKETS),
    "basis": frozenset({LAUNCH_BASIS}),
    "filter_basis": frozenset({"displayed_band", "other_carrier_band"}),
    "anchor_mode": frozenset({"anchored", "unanchored"}),
    "level": frozenset(FACET_LEVELS),
    "lang": frozenset({"en", "he"}),
}

#: KEY -> a written reason. `audience` and `sidecar_version` have NO authority to
#: pin to: both are read verbatim from the artifact's own `meta` table, so a
#: locally-declared value set here would be a hand-copied set that cannot fail
#: when the builder changes it.
META_FREE_TEXT_KEYS: Mapping[str, str] = {
    "page_id": "identity: the corpus page header the read was scoped to",
    "sys_id": "identity: the Alma system number the read was scoped to",
    "volume_ie": "identity: the active IE identifier, BrowsePage.volume_ie",
    "work_id": "identity: a w-prefixed key, scripts/discovery_ids.py",
    "domain": "reader text: the FJMS domain the request filtered on",
    "author": "reader text: the works.author the request filtered on",
    "sidecar_version": "identity: read verbatim from the artifact's own meta table",
    "audience": "identity: read verbatim from the artifact's own meta table",
}

# ---------------------------------------------------------------------------
# THE TWO VALUE RULES, UNIONED. Controls on the partition, never the means of
# discovery: the partition decides WHICH fields must be classified; these decide
# whether a classification is HONEST.
# ---------------------------------------------------------------------------

#: (1) SHAPE. Lowercase ASCII snake_case is the shape of every MULTIWORD closed
#: vocabulary in this schema and is never the shape of honest reader prose in
#: either language. Shape is what sees a carrier whose vocabulary is exported
#: NOWHERE (`coverage_status`, `eligibility_basis`) and a field a later plan adds.
_MACHINE_TOKEN_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")

#: (2) REGISTRY MEMBERSHIP, WHOLE-VALUE and case-sensitive. Registry is what
#: sees a carrier whose vocabulary is SINGLE-WORD only, which shape structurally
#: cannot: `ROUTING_STATUSES` holds `shipped`, `EVIDENCE_SOURCES` holds
#: `propagated`, `NOVELTY_STATUSES` holds `confirms` and `extends`. Whole-value
#: equality is the load-bearing restriction -- a substring match would fire on
#: "the work extends over three folios", which is honest reader prose.
REGISTRY_VOCABULARIES: Tuple[Tuple[str, frozenset], ...] = (
    ("CLAIM_TYPES", frozenset(ids.CLAIM_TYPES)),
    ("CONFIDENCE_BANDS_BY_SOURCE", _ALL_CONFIDENCE_BANDS),
    ("EVIDENCE_SOURCES", frozenset(ids.EVIDENCE_SOURCES)),
    ("EVIDENCE_KINDS", frozenset(ids.EVIDENCE_KINDS)),
    ("ADJUDICATION_STATUSES", frozenset(ids.ADJUDICATION_STATUSES)),
    ("AUDIT_STATUSES", frozenset(ids.AUDIT_STATUSES)),
    ("ROUTING_STATUSES", frozenset(ids.ROUTING_STATUSES)),
    ("ROUTING_REASONS", frozenset(ids.ROUTING_REASONS)),
    ("MEASUREMENT_STATUSES", frozenset(MEASUREMENT_STATUSES)),
    ("NOVELTY_STATUSES", frozenset(NOVELTY_STATUSES)),
    ("MAIN_POOL_REASONS", frozenset(MAIN_POOL_REASONS)),
    ("COVERAGE_STATUSES", COVERAGE_STATUSES),
    ("ELIGIBILITY_BASES", ELIGIBILITY_BASES),
)

REGISTRY_MEMBERS: frozenset = frozenset(
    member for _name, vocab in REGISTRY_VOCABULARIES for member in vocab
)

#: `(field, value, reason)` triples exempt from rule (2) DISCOVERY ONLY. The
#: field stays under the strict reader-facing prohibited-vocabulary scan, so an
#: underscore-bearing prohibited value there still fails loudly. Asserted SHORT.
REGISTRY_MATCH_EXCLUSIONS: Tuple[Tuple[str, str, str], ...] = ()


def machine_shape_violation(value: Any) -> bool:
    """Rule (1): the value has the shape of a multiword stored enum."""
    return isinstance(value, str) and bool(_MACHINE_TOKEN_RE.match(value))


def registry_membership_violation(field: Any, value: Any) -> bool:
    """Rule (2): the WHOLE value equals a member of a known closed vocabulary."""
    if not isinstance(value, str) or value not in REGISTRY_MEMBERS:
        return False
    for excluded_field, excluded_value, _reason in REGISTRY_MATCH_EXCLUSIONS:
        if field == excluded_field and value == excluded_value:
            return False
    return True


def value_rule_flags(field: Any, value: Any) -> List[str]:
    """Which value rules flag `(field, value)`, naming each rule."""
    flags = []
    if machine_shape_violation(value):
        flags.append("rule (1) SHAPE (lowercase snake_case)")
    if registry_membership_violation(field, value):
        flags.append("rule (2) REGISTRY MEMBERSHIP (whole-value)")
    return flags


# ---------------------------------------------------------------------------
# THE RECURSIVE ENVELOPE SCAN.
#
# `shared/discovery_surface_projection.py::_assert_surface_safe` validates
# forbidden KEY NAMES (plus two badge strings and three rendered rate SHAPES),
# not arbitrary VALUES under innocuous keys against a vocabulary -- so a
# stored-vocabulary leak under `band_label`, or an accuracy claim in
# meta['reason'], reaches a JSON payload untouched.
#
# FIVE of the six detectors apply to EVERY string value; the RAW-VOCABULARY
# detector applies ONLY to fields NOT declared in the machine mapping -- and
# that distinction is load-bearing, not a convenience. The projection
# deliberately carries machine vocabulary for the renderer to map, and every row
# in the public sidecar holds an underscore-bearing relation value AND an
# underscore-bearing `main_pool_reason`, so a naive recursive scan would REJECT
# every correct envelope. A gate that fails on correct output costs exactly as
# much as one that passes on wrong output.
#
# Strict scanning is therefore the DEFAULT: a field added later is scanned
# strictly until somebody classifies it, and classifying it puts its vocabulary
# into the prohibited set for every other field.
# ---------------------------------------------------------------------------

#: Envelope KEY names that read as a rate. A rate that arrives as a NUMBER is
#: invisible to a string-value scan, and a number is the likelier form.
_RATE_KEY_TOKENS: frozenset = frozenset({
    "accuracy", "accurate", "precision", "recall", "correct", "ratio",
    "proportion", "share", "rate", "f1",
    # `quality` and `score` are the two neutral-sounding names an estimate
    # actually arrives under -- neither is a token of ANY field in
    # `_ALL_ALLOWLISTS` (asserted by
    # `test_no_rate_key_token_collides_with_an_allowlisted_field`), so naming
    # them here costs no correct envelope.
    "quality", "score",
})

#: `(envelope_name, key)` pairs exempt from the NUMERIC rule, each with a
#: written reason. Asserted EMPTY by default, and NEVER permitted on a ROW
#: field, where nothing legitimately carries a fraction.
NUMERIC_RULE_EXEMPTIONS: Tuple[Tuple[str, str, str], ...] = ()


def _numeric_rate_violation(value: Any) -> bool:
    """A FRACTIONAL float in [0, 1]. Two exclusions, each with a reason.

    * Booleans -- `True == 1` in Python, and a flag is not a rate.
    * An INTEGRAL value (`0.0`, `1.0`). A counter that happens to arrive as a
      float is the realistic source of those two, and no measured figure this
      system reports is integral.

    The previous revision additionally required MORE THAN ONE significant
    decimal place, which let `{"quality": 0.9}` and `{"score": 0.9}` through
    (round 12, finding 3). One place is a rate as surely as four are -- and a
    rate rounded to one place is the likelier way an estimate gets "softened"
    onto a surface, not a less likely one.
    """
    if isinstance(value, bool) or not isinstance(value, float):
        return False
    if not (0.0 <= value <= 1.0):
        return False
    return float(value) != float(int(value))


def _rate_shaped_key(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    return bool(set(re.split(r"[^a-z0-9]+", key.lower())) & _RATE_KEY_TOKENS)


def _walk(node: Any, path: str, field: Any = None):
    """Yield `(path, field, value)` for every leaf reachable in `node`.

    `field` is the nearest enclosing MAPPING KEY, so a value nested inside a
    list under `band_label` is still attributed to `band_label`.
    """
    if isinstance(node, Mapping):
        for key, value in node.items():
            child = f"{path}.{key}" if path else str(key)
            yield child, key, value
            yield from _walk(value, child, key)
    elif isinstance(node, (list, tuple)) and not isinstance(node, (str, bytes)):
        for index, value in enumerate(node):
            child = f"{path}[{index}]"
            yield child, field, value
            yield from _walk(value, child, field)


#: `meta` is scanned with NO machine exemption at all. Being classified in
#: `META_VOCABULARY_FIELDS` is what makes a key's VALUE SET checkable (ground
#: truth 2); it exempts nothing from the strict reader-facing scan, so
#: `meta['reason'] = 'direct_witness'` still fails loudly. Verified against the
#: live values: no `meta` value this surface can emit is a member of any
#: prohibited vocabulary, so the two rules do not collide today and the leak
#: closes the moment one would.
_NO_MACHINE_EXEMPTION: Mapping[str, frozenset] = {}


def find_envelope_violations(
    envelope: Mapping[str, Any], *, lang: str = "en", where: str = "envelope",
    machine_fields: Mapping[str, frozenset] = MACHINE_VOCABULARY_FIELDS,
) -> List[str]:
    """Every honesty violation in a SERIALISED envelope, recursively."""
    violations: List[str] = []
    exempt_numeric = {(name, key) for name, key, _r in NUMERIC_RULE_EXEMPTIONS}

    def _scan(node: Any, root: str, machine: Mapping[str, frozenset]) -> None:
        for path, field, value in _walk(node, root):
            if isinstance(value, str):
                for finding in (
                    _find_unqualified_percentages(value, lang)
                    + _find_bracketed_intervals(value)
                    + _find_review_badges(value)
                    + _find_prohibited_phrases(value)
                    + _find_accuracy_rates(value, lang)
                ):
                    violations.append(f"{where} {path}: {finding}")
                if field not in machine:
                    for finding in _find_raw_vocab_keys(value):
                        violations.append(f"{where} {path}: {finding}")
            elif _numeric_rate_violation(value) and (where, field) not in exempt_numeric:
                violations.append(
                    f"{where} {path}: a rate-shaped float {value!r} under a key "
                    "that is not a declared exemption")
            if _rate_shaped_key(field) and value is not None:
                violations.append(
                    f"{where} {path}: the key {field!r} reads as a rate")

    _scan(list(envelope.get("items") or ()), "items", machine_fields)
    _scan(dict(envelope.get("meta") or {}), "meta", _NO_MACHINE_EXEMPTION)
    return sorted(set(violations))


def assert_envelope_honesty(
    envelope: Mapping[str, Any], *, lang: str = "en", where: str = "envelope"
) -> None:
    """Raise `DiscoveryHonestyViolation` if a SERIALISED envelope carries a
    percentage, an interval, a review badge, a prohibited relation word, an
    accuracy rate (as a string OR as a float), or a stored vocabulary key under
    a field nobody classified as a machine carrier."""
    violations = find_envelope_violations(envelope, lang=lang, where=where)
    if violations:
        raise DiscoveryHonestyViolation(
            f"assert_envelope_honesty: dishonest content in {where} "
            f"(lang={lang!r}): " + "; ".join(violations))


def assert_error_path_honesty(
    message: Any, *, lang: str = "en", where: str = "error path"
) -> None:
    """The same detectors over an EXCEPTION MESSAGE or a log line -- the one
    egress class that reaches a log and a reader without passing through either
    the markup scan or the envelope scan.

    The D-06a exemption is UNAVAILABLE here: it is bound to a rendered element,
    and an error path has none.
    """
    text = message if isinstance(message, str) else str(message)
    violations = (
        _find_unqualified_percentages(text, lang)
        + _find_bracketed_intervals(text)
        + _find_review_badges(text)
        + _find_prohibited_phrases(text)
        + _find_raw_vocab_keys(text)
        + _find_accuracy_rates(text, lang)
    )
    if violations:
        raise DiscoveryHonestyViolation(
            f"assert_error_path_honesty: dishonest content on the {where} "
            f"(lang={lang!r}): " + "; ".join(sorted(set(violations))))


# ---------------------------------------------------------------------------
# CLASSIFYING A FIELD AS A MACHINE CARRIER IS WHAT PROHIBITS ITS VOCABULARY.
#
# Rebound HERE, after `MACHINE_VOCABULARY_FIELDS` exists, so a carrier declared
# above cannot smuggle its vocabulary past the reader-facing scan by nobody
# remembering to edit `_collect_prohibited_vocab_keys` as well. `ROUTING_REASONS`,
# `COVERAGE_STATUSES` and `ELIGIBILITY_BASES` enter the prohibited set through
# exactly this route today -- the first two carry `co_citation` /
# `later_shared_text` / `runner_up_conflict` and `no_denominator` /
# `not_applicable`, none of which the explicit collector names.
#
# `_find_raw_vocab_keys` reads this name at CALL time, so the rebind takes
# effect for every caller.
# ---------------------------------------------------------------------------

_PROHIBITED_RAW_VOCAB_KEYS = frozenset(_PROHIBITED_RAW_VOCAB_KEYS) | frozenset(
    member
    for vocabulary in MACHINE_VOCABULARY_FIELDS.values()
    for member in vocabulary
    if "_" in member
)
