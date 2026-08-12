# -*- coding: utf-8 -*-
"""The honesty contract for `shared/discovery_display_strings.py`
(Phase 136, plan 136-10, PANEL-01/PANEL-02/NOVEL-01).

Two kinds of test live here.

**Seven behaviour tests** pin the individual guarantees the plan names: the
match-framing relation chips, the tooltip delegation, the qualified
matched-letter coverage on a direct-family row, the absence of any
percentage on a propagated row, the four novelty candidacy strings, the
filter short codes in BOTH directions, and the U+05BE maqaf in the Hebrew
section headers.

**One sweep** enumerates EVERY public callable in the module, calls it in
BOTH languages over a table of representative inputs, and runs every
returned string through the SHARED
``tests/render_smoke/discovery_honesty_gate.py::assert_discovery_honesty``
gate -- the same one implementation the Phase-136 render-smoke suites use,
never a second copy of the rule. A per-function assertion list rots the
moment somebody adds a function; the sweep plus its companion
"registry covers every public callable" test cannot.

Two positive controls prove the sweep can actually fail: one seeds a bare
precision percentage, one seeds a stored vocabulary key. A green check that
cannot go red is worthless (`findings-page.md`, "Verification").
"""

from __future__ import annotations

import ast
import html
import inspect
import pathlib
import re

import pytest

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
from shared.discovery_band_labels import (
    RECALL_DISCLAIMER,
    SHOW_MORE_TOGGLE,
    band_label,
)
from shared.discovery_main_pool import bucket_label, main_pool_sentence
from tests.render_smoke.discovery_honesty_gate import (
    DiscoveryHonestyViolation,
    assert_discovery_honesty,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "shared" / "discovery_display_strings.py"

LANGS = ("en", "he")

MAQAF = "־"  # HEBREW PUNCTUATION MAQAF -- D-21's Hebrew section headers

# The stored relation vocabulary. Read from the frozen enum module rather
# than re-typed here, so a schema rename cannot leave this suite asserting
# against a key that no longer exists.
STORED_RELATION_KEYS = (
    ids.CLAIM_TYPE_DIRECT_WITNESS,
    ids.CLAIM_TYPE_QUOTES_THIS_WORK,
    ids.CLAIM_TYPE_SHARED_TEXT,
)

WORK_EN = "Rashi on Genesis"
WORK_HE = "רש״י על בראשית"


# ---------------------------------------------------------------------------
# The shared gate, applied to a bare string.
# ---------------------------------------------------------------------------

def gate_string(value: str, lang: str, *, where: str = "") -> None:
    """Run ONE returned string through the shared discovery honesty gate.

    The gate reads scoped HTML, so the string is wrapped in a minimal
    element carrying a known class. Escaping is symmetric: `html.escape`
    on the way in, `convert_charrefs=True` in the gate's parser on the way
    out, so the gate sees exactly the original characters."""
    fragment = '<div class="dsx"><span>{}</span></div>'.format(html.escape(str(value)))
    try:
        assert_discovery_honesty(fragment, scope_selector="dsx", lang=lang)
    except DiscoveryHonestyViolation as exc:  # pragma: no cover - failure path
        raise AssertionError("{}: {}".format(where or "string", exc)) from exc


# ---------------------------------------------------------------------------
# The sweep registry. Maps every public callable's name to the kwargs sets it
# is exercised with. `lang` is injected by the sweep for any function whose
# signature accepts it, so an entry never has to repeat it.
# ---------------------------------------------------------------------------

SWEEP_INPUTS = {
    # Curated titles must clear the same honesty gate as any other reader-facing
    # string: both a curated work and an uncurated pass-through (ruling R).
    "display_work_title": [
        {"work_id": "w000176", "neutral_title": "משנה תורה, ספר אהבה"},
        {"work_id": "w000164", "neutral_title": "סדר עולם רבה"},
    ],
    "relation_chip": [{"relation_kind": k} for k in STORED_RELATION_KEYS],
    "relation_tooltip": [
        {"evidence_source": src, "confidence_band": band}
        for src, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items()
        for band in sorted(bands)
    ],
    "row_headline": [
        {"work_title": WORK_EN, "coverage_ppm": 680000,
         "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
         "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT},
        {"work_title": WORK_HE, "coverage_ppm": 25000,
         "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
         "evidence_source": ids.EVIDENCE_SOURCE_TRACK1_DIRECT},
        {"work_title": WORK_EN, "coverage_ppm": 680000,
         "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
         "evidence_source": ids.EVIDENCE_SOURCE_PROPAGATED},
        {"work_title": WORK_EN, "coverage_ppm": None,
         "relation_kind": ids.CLAIM_TYPE_QUOTES_THIS_WORK},
        {"work_title": WORK_HE, "coverage_ppm": 900000,
         "relation_kind": ids.CLAIM_TYPE_SHARED_TEXT},
    ],
    "coverage_label": [{}],
    "low_coverage_note": [{}],
    "granularity_subline": [{"other_work_title": WORK_EN},
                            {"other_work_title": WORK_HE}],
    "missing_title": [{}],
    "not_an_identification_note": [{}],
    "section_header": [
        {"section_key": ds.SECTION_ON_THIS_PAGE},
        {"section_key": ds.SECTION_ELSEWHERE_IN_MANUSCRIPT},
        {"section_key": ds.SECTION_OTHER_MANUSCRIPTS, "work_title": WORK_EN},
        {"section_key": ds.SECTION_OTHER_MANUSCRIPTS, "work_title": WORK_HE},
        {"section_key": ds.SECTION_PAGES_MATCHING_THIS_PAGE},
    ],
    "disclosure_toggle": [
        {"toggle_key": ds.TOGGLE_MORE_MATCHES},
        {"toggle_key": ds.TOGGLE_ALSO_SHARES_TEXT},
        {"toggle_key": ds.TOGGLE_DIVERGENCE},
    ],
    "divergence_warning": [{}],
    "divergence_chip": [{}],
    "related_pages_label": [{}],
    "related_pages_count_line": [{"count": 0}, {"count": 1}, {"count": 37397}],
    "bucket_name": [{"in_main_pool": True}, {"in_main_pool": False}],
    "rule_sentence": [{}],
    "recall_disclaimer": [{}],
    "novelty_strings": [{}],
    "novelty_unknown_badge": [{}],
    "filter_codes": [{}],
    "filter_code": [{"relation_kind": k} for k in STORED_RELATION_KEYS],
    "filter_label": [{"code": c} for c in ("dw", "qw", "st")],
    "is_filter_code": [{"code": "dw"}, {"code": "zz"}],
    "matches_filter_codes": [
        {"relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS, "codes": ()},
        {"relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS, "codes": ("dw",)},
        {"relation_kind": ids.CLAIM_TYPE_SHARED_TEXT, "codes": ("dw", "qw")},
    ],
    "service_state_message": [
        {"state": s} for s in ("ok", "unavailable", "timeout", "busy")
    ],
    "retry_label": [{}],
}


def public_callables():
    """Every public callable defined BY this module (imported delegates are
    excluded -- they are covered by their own home module's suite, and the
    sweep asserts the delegation separately)."""
    out = {}
    for name, obj in vars(ds).items():
        if name.startswith("_"):
            continue
        if not callable(obj):
            continue
        if getattr(obj, "__module__", None) != ds.__name__:
            continue
        out[name] = obj
    return out


def flatten_strings(value):
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out = []
        for k, v in value.items():
            out.extend(flatten_strings(v))
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        out = []
        for v in value:
            out.extend(flatten_strings(v))
        return out
    return []


# ---------------------------------------------------------------------------
# Behaviour test 1 -- relation chips are match framing, never a stored key.
# ---------------------------------------------------------------------------

def test_relation_chip_returns_match_framing_and_never_a_stored_key():
    """The softer register (owner ruling 2026-08-11), pinned pair by pair
    against `docs/specs/discovery-relation-matrix-v1.md` §1."""
    assert ds.relation_chip(ids.RENDERED_RELATION_DIRECT_WITNESS, "en") == (
        "Matches this work")
    assert ds.relation_chip(ids.RENDERED_RELATION_QUOTES_THIS_WORK, "en") == (
        "Includes a quotation")
    assert ds.relation_chip(ids.RENDERED_RELATION_SHARED_TEXT, "en") == (
        "Shares text with this work")
    assert ds.relation_chip(ids.RENDERED_RELATION_UNCERTAIN, "en") == "Needs review"

    assert ds.relation_chip(ids.RENDERED_RELATION_DIRECT_WITNESS, "he") == "מתאים לחיבור"
    assert ds.relation_chip(ids.RENDERED_RELATION_QUOTES_THIS_WORK, "he") == "כולל ציטוט"
    assert ds.relation_chip(ids.RENDERED_RELATION_SHARED_TEXT, "he") == "חולק טקסט"
    assert ds.relation_chip(ids.RENDERED_RELATION_UNCERTAIN, "he") == "דורש בדיקה"

    for lang in LANGS:
        for key in sorted(ids.RENDERED_RELATIONS - {ids.RENDERED_RELATION_WORK_QUOTES_PAGE}):
            chip = ds.relation_chip(key, lang)
            for stored in tuple(STORED_RELATION_KEYS) + tuple(ids.RENDERED_RELATIONS):
                assert stored not in chip

    # An unknown relation kind must raise, never render a blank chip.
    with pytest.raises(ValueError):
        ds.relation_chip("no_such_kind", "en")


def test_the_retired_labels_are_gone_from_every_chip():
    """A regression guard rather than a restatement of the test above. "Direct
    match" sat on 28,462 of 28,464 main-pool rows and "Partial match" was an
    outright misnomer for a quotation; either creeping back is the kind of edit
    that reads as harmless in a diff."""
    retired = ("Direct match", "Partial match", "התאמה ישירה", "התאמה חלקית")
    for lang in LANGS:
        for key in sorted(ids.RENDERED_RELATIONS - {ids.RENDERED_RELATION_WORK_QUOTES_PAGE}):
            chip = ds.relation_chip(key, lang)
            for gone in retired:
                assert gone != chip, f"{key}/{lang} still renders the retired {gone!r}"


def test_the_quotation_chip_avoids_the_wording_the_honesty_gate_prohibits():
    """Not a style preference: `discovery_honesty_gate._PROHIBITED_PHRASES`
    forbids the bare word "quotes" as relation wording, so the frozen
    vocabulary says "quotation". If someone "simplifies" the label back to
    "Quotes this work", every panel render-smoke test fails — this says why in
    one place instead of leaving the next person to rediscover it."""
    import re

    en = ds.relation_chip(ids.RENDERED_RELATION_QUOTES_THIS_WORK, "en")
    assert re.search(r"\bquotes\b", en.lower()) is None
    assert re.search(r"\bcopy of\b", en.lower()) is None
    assert re.search(r"\bwitness of\b", en.lower()) is None


def test_work_quotes_page_has_no_strings_and_raises():
    """Deliberate absence, per §1: assigning its reader strings is an owner item
    deferred until a validated direction signal ships, and the matrix provably
    never emits it in v1. The raise IS the proof that it cannot reach a reader;
    adding a placeholder label here would quietly remove that proof."""
    for lang in LANGS:
        with pytest.raises(ValueError):
            ds.relation_chip(ids.RENDERED_RELATION_WORK_QUOTES_PAGE, lang)


def test_every_state_the_matrix_can_emit_has_a_chip():
    """The load-bearing pairing: `relation_chip` raises on an unknown key and
    `web/components/findings_rows.py` wraps the call in `except ValueError`, so
    a missing entry does NOT fail loudly on a surface — it silently drops the
    chip. `uncertain` is the state this would have hit, on every fail-closed
    row, the moment a surface started reading the matrix column."""
    from shared import discovery_relation_matrix as matrix

    emittable = ids.RENDERED_RELATIONS - matrix.NEVER_RENDERED_IN_V1
    for lang in LANGS:
        for state in sorted(emittable):
            assert ds.relation_chip(state, lang)


# ---------------------------------------------------------------------------
# Behaviour test 2 -- the tooltip is the frozen band label, unmodified.
# ---------------------------------------------------------------------------

def test_relation_tooltip_returns_the_frozen_band_label_unmodified():
    for source, bands in ids.CONFIDENCE_BANDS_BY_SOURCE.items():
        for band in sorted(bands):
            for lang in LANGS:
                assert ds.relation_tooltip(source, band, lang) == band_label(source, band, lang)


# ---------------------------------------------------------------------------
# Behaviour test 3 -- direct-family coverage, qualified as matched letters.
# ---------------------------------------------------------------------------

def test_row_headline_direct_family_carries_qualified_matched_letter_coverage():
    en = ds.row_headline(
        WORK_EN, 680000, ids.CLAIM_TYPE_DIRECT_WITNESS, "en",
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
    )
    assert en.startswith("Matches " + WORK_EN)
    assert "68%" in en
    # The gate's own permitted-coverage anchor, and the plan's matched-letter
    # qualifier, must BOTH sit next to the percentage.
    after = en.split("68%", 1)[1]
    assert after.lstrip().startswith("of page")
    assert "matched letters" in after
    gate_string(en, "en", where="row_headline/en")

    he = ds.row_headline(
        WORK_HE, 680000, ids.CLAIM_TYPE_DIRECT_WITNESS, "he",
        evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
    )
    assert "68%" in he
    after_he = he.split("68%", 1)[1]
    assert after_he.lstrip().startswith("מהדף")  # "of page"
    gate_string(he, "he", where="row_headline/he")

    # A weaker relation is match framing WITHOUT a percentage, even when a
    # coverage value is supplied.
    partial = ds.row_headline(WORK_EN, 680000, ids.CLAIM_TYPE_QUOTES_THIS_WORK, "en")
    assert partial == "Partial match with " + WORK_EN
    assert "%" not in partial

    # Coverage absent (the 11,941 shared-wording claims carry no matched
    # letters at all) -> no percentage, no empty separator.
    none_cov = ds.row_headline(WORK_EN, None, ids.CLAIM_TYPE_DIRECT_WITNESS, "en",
                               evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT)
    assert none_cov == "Matches " + WORK_EN
    assert "%" not in none_cov


# ---------------------------------------------------------------------------
# Behaviour test 4 -- a propagated row never carries a percentage (D-08a).
# ---------------------------------------------------------------------------

def test_propagated_row_headline_contains_no_percentage():
    for lang in LANGS:
        for kind in STORED_RELATION_KEYS:
            headline = ds.row_headline(
                WORK_EN, 680000, kind, lang,
                evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED,
            )
            assert "%" not in headline, (
                "a propagated-family row must carry no coverage percentage at all "
                "(D-08a: coverage is direct-family only); got {!r}".format(headline)
            )


# ---------------------------------------------------------------------------
# Behaviour test 5 -- the four novelty candidacy strings.
# ---------------------------------------------------------------------------

def test_novelty_strings_are_the_four_candidacy_strings_under_the_hedge():
    for lang in LANGS:
        strings = ds.novelty_strings(lang)
        assert set(strings) == {"toggle", "badge", "subline", "help"}
        for value in strings.values():
            assert isinstance(value, str) and value.strip()

    en = ds.novelty_strings("en")
    assert en["toggle"] == "Candidates for new finds"
    assert en["badge"] == "Candidate new find"
    assert en["subline"] == "Findings you would not reach by searching the catalogues."
    assert "candidate, not a confirmed find" in en["help"]

    he = ds.novelty_strings("he")
    assert he["toggle"] == "מועמדים לממצאים חדשים"
    assert he["badge"] == "מועמד לממצא חדש"

    # The two lines D-23b / findings-page.md keep prohibited, in either
    # language, including inside the help text.
    prohibited = ("new discovery", "likely new find", "unknown to scholarship")
    for lang in LANGS:
        for value in ds.novelty_strings(lang).values():
            lowered = value.lower()
            for phrase in prohibited:
                assert phrase not in lowered, (
                    "novelty_strings({!r}) contains prohibited wording {!r}".format(lang, phrase)
                )


# ---------------------------------------------------------------------------
# Behaviour test 6 -- filter short codes, in both directions.
# ---------------------------------------------------------------------------

def test_filter_codes_round_trip_without_exposing_a_stored_key():
    assert ds.filter_codes() == ("dw", "qw", "st")
    assert ds.filter_code(ids.CLAIM_TYPE_DIRECT_WITNESS) == "dw"
    assert ds.filter_code(ids.CLAIM_TYPE_QUOTES_THIS_WORK) == "qw"
    assert ds.filter_code(ids.CLAIM_TYPE_SHARED_TEXT) == "st"

    # The REVERSE direction maps a code back to what a reader sees, never to
    # the stored key -- so neither direction can put an internal
    # classification into markup.
    for lang in LANGS:
        for kind in STORED_RELATION_KEYS:
            code = ds.filter_code(kind)
            assert ds.filter_label(code, lang) == ds.relation_chip(kind, lang)
            for stored in STORED_RELATION_KEYS:
                assert stored not in code
                assert stored not in ds.filter_label(code, lang)

    assert ds.is_filter_code("dw") is True
    assert ds.is_filter_code("direct_witness") is False

    # The server-side predicate a filtered query uses, so no caller ever
    # needs a code -> stored-key lookup.
    assert ds.matches_filter_codes(ids.CLAIM_TYPE_DIRECT_WITNESS, ()) is True  # empty = all
    assert ds.matches_filter_codes(ids.CLAIM_TYPE_DIRECT_WITNESS, ("dw",)) is True
    assert ds.matches_filter_codes(ids.CLAIM_TYPE_SHARED_TEXT, ("dw", "qw")) is False

    with pytest.raises(ValueError):
        ds.filter_label("zz", "en")
    with pytest.raises(ValueError):
        ds.filter_code("no_such_kind")


# ---------------------------------------------------------------------------
# Behaviour test 7 -- the Hebrew section headers use U+05BE.
# ---------------------------------------------------------------------------

def test_hebrew_section_headers_use_the_u05be_maqaf():
    other = ds.section_header(ds.SECTION_OTHER_MANUSCRIPTS, "he", work_title=WORK_HE)
    pages = ds.section_header(ds.SECTION_PAGES_MATCHING_THIS_PAGE, "he")
    elsewhere = ds.section_header(ds.SECTION_ELSEWHERE_IN_MANUSCRIPT, "he")

    for header in (other, pages, elsewhere):
        assert MAQAF in header, (
            "D-21 fixes the Hebrew maqaf at U+05BE; {!r} carries none".format(header)
        )
        # A plain ASCII hyphen or a Unicode dash is the failure mode this
        # assertion exists to catch.
        assert "-" not in header and "‐" not in header and "–" not in header

    assert other == "כתבי" + MAQAF + "יד נוספים התואמים ל" + WORK_HE
    assert pages == (
        "דפים התואמים לדף זה "
        "בכתבי" + MAQAF + "יד אחרים"
    )

    assert ds.section_header(ds.SECTION_ON_THIS_PAGE, "en") == "On this page"
    assert ds.section_header(ds.SECTION_ELSEWHERE_IN_MANUSCRIPT, "en") == "Elsewhere in this manuscript"
    assert ds.section_header(ds.SECTION_OTHER_MANUSCRIPTS, "en", work_title=WORK_EN) == (
        "Other manuscripts matching " + WORK_EN
    )
    assert ds.section_header(ds.SECTION_PAGES_MATCHING_THIS_PAGE, "en") == (
        "Pages matching this page in other manuscripts"
    )

    with pytest.raises(ValueError):
        ds.section_header("no_such_section", "en")


# ---------------------------------------------------------------------------
# The sweep, and the companion test that stops the registry rotting.
# ---------------------------------------------------------------------------

def test_sweep_registry_covers_every_public_callable():
    defined = set(public_callables())
    registered = set(SWEEP_INPUTS)
    assert defined == registered, (
        "the honesty sweep registry has drifted from the module's public API.\n"
        "  public but unswept: {}\n"
        "  registered but gone: {}".format(
            sorted(defined - registered), sorted(registered - defined)
        )
    )


@pytest.mark.parametrize("lang", LANGS)
def test_every_public_function_passes_the_shared_honesty_gate(lang):
    """Call EVERY public function in BOTH languages over the registry's
    representative inputs and run every returned string through the ONE
    shared honesty gate."""
    functions = public_callables()
    checked = 0
    for name, fn in sorted(functions.items()):
        signature = inspect.signature(fn)
        for kwargs in SWEEP_INPUTS[name]:
            call_kwargs = dict(kwargs)
            if "lang" in signature.parameters:
                call_kwargs["lang"] = lang
            result = fn(**call_kwargs)
            for value in flatten_strings(result):
                gate_string(value, lang, where="{}({!r})".format(name, kwargs))
                lowered = value.lower()
                for stored in STORED_RELATION_KEYS:
                    assert stored not in value, (
                        "{} returned a stored vocabulary key {!r}".format(name, stored)
                    )
                for phrase in ("new discovery", "likely new find", "unknown to scholarship"):
                    assert phrase not in lowered, (
                        "{} returned prohibited novelty wording {!r}".format(name, phrase)
                    )
                checked += 1
    assert checked > 50, "the sweep checked only {} strings -- too few to mean anything".format(checked)


def test_sweep_gate_rejects_a_seeded_precision_percentage():
    """Positive control: an unqualified percentage must turn the gate red."""
    with pytest.raises(AssertionError):
        gate_string("Algorithmic match, precision 93.8%", "en")


def test_sweep_gate_rejects_a_seeded_stored_vocabulary_key():
    """Positive control: a stored vocabulary key must turn the gate red."""
    with pytest.raises(AssertionError):
        gate_string("relation: " + ids.CLAIM_TYPE_DIRECT_WITNESS, "en")


def test_sweep_gate_rejects_a_seeded_review_badge_and_interval():
    """Positive controls for the remaining two rules the gate carries."""
    with pytest.raises(AssertionError):
        gate_string("Expert-reviewed", "en")
    with pytest.raises(AssertionError):
        gate_string("estimated band precision [0.9084, 0.9644]", "en")


# ---------------------------------------------------------------------------
# Module-level invariants: no review badge, no redefinition, no web import.
# ---------------------------------------------------------------------------

def test_module_defines_no_human_review_badge_string():
    """D-13f: the badge is dropped until the provenance of the 121
    human-confirmed rows is established, and the safest way to keep it off a
    surface is for the string not to exist."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    lowered = source.lower()
    for marker in ("expert-reviewed", "expert reviewed", "review_overlay"):
        assert marker not in lowered, (
            "shared/discovery_display_strings.py must define no human-review badge "
            "string; found {!r}".format(marker)
        )
    assert "נבדק בידי מומחה" not in source
    assert not hasattr(ds, "review_badge")


def test_bucket_names_rule_sentence_and_band_labels_are_delegated_not_redefined():
    """This module is composition, not a second vocabulary."""
    for lang in LANGS:
        assert ds.bucket_name(True, lang) == bucket_label(True, lang)
        assert ds.bucket_name(False, lang) == bucket_label(False, lang)
        assert ds.rule_sentence(lang) == main_pool_sentence(lang)
        assert ds.recall_disclaimer(lang) == RECALL_DISCLAIMER[lang]
        assert ds.disclosure_toggle(ds.TOGGLE_MORE_MATCHES, lang) == SHOW_MORE_TOGGLE[lang]

    source = MODULE_PATH.read_text(encoding="utf-8")
    # No literal bucket name and no literal band label may be typed here.
    for literal in ("main pool", "more matches",
                    "מאגר עיקרי",
                    "התאמות נוספות"):
        assert literal not in source, (
            "bucket names come from bucket_label; {!r} must not be redefined here".format(literal)
        )
    for entry in vars(__import__("shared.discovery_band_labels", fromlist=["BAND_LABELS"])).get(
        "BAND_LABELS", {}
    ).values():
        for label in entry.values():
            assert label not in source, (
                "band labels come from band_label(); {!r} must not be redefined here".format(label)
            )


def test_module_never_imports_web():
    source = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("web"), node.lineno
        elif isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("web"), node.lineno


def test_module_carries_the_key_links_the_plan_pins():
    """136-10's own key_links contract: this module reaches
    `shared/discovery_band_labels.py` for tooltips and `bucket_label` for
    bucket names, and never re-derives either."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "discovery_band_labels" in source
    assert "bucket_label" in source


def test_no_display_string_contains_a_prohibited_relation_word():
    """The D-21 word gate, applied to the module SOURCE as well as to its
    output -- a phrase sitting in a docstring or a disabled branch is one
    edit away from a surface."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    literals = [
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    for literal in literals:
        lowered = literal.lower()
        for phrase in ("copy of", "witness of"):
            assert not re.search(r"\b" + re.escape(phrase) + r"\b", lowered), (
                "prohibited relation wording {!r} in a module string literal".format(phrase)
            )


# ---------------------------------------------------------------------------
# Task 2 -- the tr() / display-strings split. Named so the plan's
# `-k "translation or i18n"` verification selects them.
# ---------------------------------------------------------------------------

DISCOVERY_TR_KEYS = (
    "Computed Identifications",
    "Computed identifications",
    "Computed identifications elsewhere in this manuscript",
    "Computed identifications across the Cairo Genizah corpus, produced by text matching. "
    "Every row is an algorithmic match, not a reviewed identification.",
    "All findings",
    "Screening leads",
    "My saved",
    "Coming soon",
    "Show as",
    "One row per identification",
    "One row per manuscript",
    "One row per work",
    "Strongest first",
    "Pages matched",
    "Matched text",
    "Showing {shown} of {total} findings",
    "Showing the {bucket} by default.",
    "Computed Identifications is not available right now",
    "This page will return as soon as the data is ready.",
)


def test_new_translation_keys_have_hebrew_values():
    import genizah_translations as gt

    for key in DISCOVERY_TR_KEYS:
        assert key in gt.TRANSLATIONS, "missing tr() entry: {!r}".format(key)
        value = gt.TRANSLATIONS[key]
        assert isinstance(value, str) and value.strip(), (
            "tr() entry {!r} has no Hebrew value -- a missing Hebrew value renders the "
            "English string in a Hebrew UI".format(key)
        )
        assert value != key, (
            "tr() entry {!r} maps to itself; that is an English string in a Hebrew UI".format(key)
        )


def test_no_new_translation_key_introduces_the_word_discoveries():
    """`/discoveries` is the pre-existing Community page and its nav label is
    taken; a bare "Identifications" collides with "Browse by Identification"."""
    for key in DISCOVERY_TR_KEYS:
        assert "Discoveries" not in key
        assert key != "Identifications"


def test_no_translation_key_duplicates_a_display_string():
    """A string lives in exactly ONE of `tr()` and
    `shared/discovery_display_strings.py` -- never both."""
    import genizah_translations as gt

    display_values = set()
    functions = public_callables()
    for name, fn in functions.items():
        signature = inspect.signature(fn)
        for kwargs in SWEEP_INPUTS[name]:
            for lang in LANGS:
                call_kwargs = dict(kwargs)
                if "lang" in signature.parameters:
                    call_kwargs["lang"] = lang
                for value in flatten_strings(fn(**call_kwargs)):
                    if value.strip():
                        display_values.add(value)

    # Delegated strings legitimately live in their own home modules, and the
    # methods page already pins the rule sentence -- exclude only those.
    delegated = set()
    for lang in LANGS:
        delegated.add(bucket_label(True, lang))
        delegated.add(bucket_label(False, lang))
        delegated.add(main_pool_sentence(lang))
        delegated.add(RECALL_DISCLAIMER[lang])
        delegated.add(SHOW_MORE_TOGGLE[lang])
        for entry in __import__(
            "shared.discovery_band_labels", fromlist=["BAND_LABELS"]
        ).BAND_LABELS.values():
            delegated.add(entry[lang])

    overlap = sorted(
        v for v in (display_values - delegated)
        if v in gt.TRANSLATIONS or v in set(gt.TRANSLATIONS.values())
    )
    assert overlap == [], (
        "these strings are defined in BOTH genizah_translations.py and "
        "shared/discovery_display_strings.py: {}".format(overlap)
    )


def test_translations_split_is_documented_in_a_comment():
    source = (REPO_ROOT / "genizah_translations.py").read_text(encoding="utf-8")
    assert "discovery_display_strings" in source, (
        "genizah_translations.py must state the tr()/display-strings split in a comment"
    )


# ---------------------------------------------------------------------------
# Task 3 -- the shared discovery CSS block. Guarded here because this plan
# owns exactly one test file and the block serves BOTH surfaces, so neither
# surface plan can be the one that pins it.
# ---------------------------------------------------------------------------

CSS_PATH = REPO_ROOT / "web" / "static" / "common.css"
CSS_BLOCK_MARKER = "/* discovery"


def discovery_css_block() -> str:
    source = CSS_PATH.read_text(encoding="utf-8")
    assert CSS_BLOCK_MARKER in source, (
        "the discovery CSS block must open with a literal {!r} comment".format(CSS_BLOCK_MARKER)
    )
    return source[source.index(CSS_BLOCK_MARKER):]


def test_discovery_css_block_uses_only_logical_directional_properties():
    """Physical directional properties break RTL, and both surfaces render in
    both directions."""
    physical = re.findall(
        r"(?:border|padding|margin)-(?:left|right)\s*:", discovery_css_block()
    )
    assert physical == [], (
        "the discovery CSS block uses physical directional properties: {}".format(physical)
    )


def test_no_confidence_chip_rule_exists_anywhere_in_the_stylesheet():
    """There is no confidence scale on either surface; the deleted
    confidence-chip rules are the one part of the sketch CSS not to copy."""
    source = CSS_PATH.read_text(encoding="utf-8")
    for banned in (".conf.strong", ".conf.medium", ".conf.weak", "fchip conf"):
        assert banned not in source, (
            "a confidence-chip rule reappeared in common.css: {!r}".format(banned)
        )


def test_relation_chip_css_is_not_keyed_on_a_relation_kind():
    """Colour-coding the chip by relation kind reintroduces per-tier
    confidence styling through the back door (D-24)."""
    block = discovery_css_block()
    for kind in STORED_RELATION_KEYS:
        assert kind not in block, (
            "the discovery CSS block keys a rule on the stored relation kind {!r}".format(kind)
        )
    for suffix in ("dw", "qw", "st"):
        assert ".rel.{}".format(suffix) not in block, (
            "the relation chip must stay visually neutral; found a per-kind rule "
            "`.rel.{}`".format(suffix)
        )


def test_discovery_css_block_is_scoped_and_carries_the_required_treatments():
    """Every rule is scoped, so no existing selector changes -- the sketch
    class names (`.row`, `.chip`, `.mode`, `.c`) are far too generic for a
    global stylesheet loaded beside Quasar."""
    block = discovery_css_block()

    # Every selector line in the block is scoped under .gs-discovery.
    unscoped = []
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped.endswith("{") or stripped.startswith(("@", "/*", "*")):
            continue
        if "gs-discovery" not in stripped:
            unscoped.append(stripped)
    assert unscoped == [], "unscoped discovery selectors: {}".format(unscoped)

    # The even two-pane grid: block on mobile, 1fr 1fr at >=900px, divider
    # on the inline start.
    assert ".gs-discovery .dpanes { display: block; }" in block
    assert "@media (min-width: 900px)" in block
    assert "grid-template-columns: 1fr 1fr;" in block
    assert "border-inline-start: 1px solid var(--border-light);" in block

    # The blocked-filter treatment and its amber tag: visibly disabled, never
    # silently absent.
    assert ".gs-discovery .fg.blocked" in block
    assert ".gs-discovery .needs" in block
    assert "var(--accent-amber)" in block

    # The novelty group is forced first in the filter bar.
    assert ".gs-discovery .fg.novgrp { order: -1; }" in block

    # The RTL disclosure-arrow flip and the sub-700px row stacking.
    assert '[dir="rtl"] .gs-discovery .disc > summary::before' in block
    assert "@media (max-width: 700px)" in block


# ---------------------------------------------------------------------------
# Curated display titles (owner ruling R)
# ---------------------------------------------------------------------------

from shared.discovery_display_strings import (  # noqa: E402
    CURATED_WORK_TITLES,
    display_work_title,
)


def test_curated_title_overrides_sefer_ahava_in_both_languages():
    assert display_work_title("w000176", "משנה תורה, ספר אהבה", "he") == "משנה תורה, ספר אהבה / סידור"
    assert display_work_title("w000176", "משנה תורה, ספר אהבה", "en") == "Mishneh Torah, Sefer Ahava / Siddur"


def test_uncurated_work_passes_its_recorded_title_through_unchanged():
    assert display_work_title("w000164", "סדר עולם רבה", "he") == "סדר עולם רבה"
    assert display_work_title("w999999", "Anything At All", "en") == "Anything At All"


def test_curated_title_names_both_possibilities_never_asserts_one():
    # Ruling R's whole point: the label must not assert the halakhic work, and
    # must not drop it either -- a reader has to see both readings.
    for lang in ("he", "en"):
        t = display_work_title("w000176", "x", lang)
        assert "/" in t, "curated label must name both readings, not pick one"
    assert "סידור" in display_work_title("w000176", "x", "he")
    assert "Siddur" in display_work_title("w000176", "x", "en")


def test_curated_titles_are_bilingual_and_nonempty():
    # A half-filled entry would silently fall back to the other language.
    for work_id, entry in CURATED_WORK_TITLES.items():
        assert set(entry) >= {"he", "en"}, f"{work_id} is missing a language"
        for lang, value in entry.items():
            assert value.strip(), f"{work_id}/{lang} is empty"
