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
from typing import List

import scripts.discovery_ids as ids
from shared.discovery_band_labels import MEASUREMENT_STATUSES

__all__ = [
    "DiscoveryHonestyScopeError",
    "DiscoveryHonestyViolation",
    "assert_discovery_honesty",
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

# The coverage qualifier that must sit near a permitted percentage --
# "Matches <work> · 68% of page" (EN) / "התאמה
# ל<חיבור> · 68% מהדף" (HE).
_COVERAGE_QUALIFIER_BY_LANG = {
    "en": "of page",
    "he": "מהדף",  # מהדף
}
_COVERAGE_QUALIFIER_WINDOW = 32  # chars scanned after the percentage match


def _find_unqualified_percentages(text: str, lang: str) -> List[str]:
    qualifier = _COVERAGE_QUALIFIER_BY_LANG.get("he" if lang == "he" else "en")
    violations = []
    for m in _PERCENT_RE.finditer(text):
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
    keys = set(ids.CLAIM_TYPES)
    for band_set in ids.CONFIDENCE_BANDS_BY_SOURCE.values():
        keys.update(band_set)
    keys.update(ids.ADJUDICATION_STATUSES)
    keys.update(ids.ROUTING_STATUSES)
    keys.update(ids.EVIDENCE_SOURCES)
    keys.update(MEASUREMENT_STATUSES)
    return frozenset(k for k in keys if "_" in k)


_PROHIBITED_RAW_VOCAB_KEYS = _collect_prohibited_vocab_keys()


def _find_raw_vocab_keys(text: str) -> List[str]:
    violations = []
    for key in _PROHIBITED_RAW_VOCAB_KEYS:
        if re.search(r"\b" + re.escape(key) + r"\b", text):
            violations.append(f"raw stored vocabulary key {key!r}")
    return violations


# ---------------------------------------------------------------------------
# The public entry point.
# ---------------------------------------------------------------------------

def assert_discovery_honesty(rendered_html: str, *, scope_selector: str, lang: str) -> None:
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

    if violations:
        raise DiscoveryHonestyViolation(
            f"assert_discovery_honesty: dishonest content found in scope "
            f"{scope_selector!r} (lang={lang!r}): " + "; ".join(violations)
        )
