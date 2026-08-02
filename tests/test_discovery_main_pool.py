# -*- coding: utf-8 -*-
"""Behavior + drift-guard test suite for `shared/discovery_main_pool.py`
(Phase 136, plan 136-07, Task 1: `main_pool_decision`).

Masking discipline (matches `tests/test_discovery_ids.py`): every
page_id/sys_id/work_id/evidence_id value below is a synthetic fixture
placeholder, never a real research-data identifier.
"""

from __future__ import annotations

import ast
import pathlib

import scripts.discovery_ids as ids
from shared.discovery_main_pool import (
    COVERAGE_FLOOR,
    MAIN_POOL_REASONS,
    REASON_INSUFFICIENT_LENGTH,
    REASON_LOW_COVERAGE,
    REASON_MAIN_FULL_COVERAGE,
    REASON_MAIN_HUMAN_CONFIRMED,
    REASON_MAIN_MULTIFOLIO,
    REASON_MISSING_SIGNAL,
    REASON_OVERLAPPING_TIE,
    REASON_SHARED_WORDING,
    SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS,
    Identification,
    bucket_label,
    main_pool_decision,
    main_pool_sentence,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "shared" / "discovery_main_pool.py"

_SCHEMA_MAIN_POOL_REASON_VOCAB = frozenset({
    "shared_wording", "overlapping_tie", "low_coverage",
    "insufficient_length", "missing_signal",
    "main_multifolio", "main_full_coverage", "main_human_confirmed",
})


# ---------------------------------------------------------------------------
# Task 1: one behavior test per <behavior> bullet in 136-07-PLAN.md.
# ---------------------------------------------------------------------------

def test_no_same_work_claim_is_shared_wording():
    """No same-work claim at all (only quotes / shared wording) -> not-main,
    shared_wording."""
    ident = Identification(has_same_work_claim=False)
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_SHARED_WORDING)


def test_screening_band_agrees_with_is_default_eligible():
    """Best band is only a screening band -> not-main; the decision agrees
    with `is_default_eligible` rather than re-deriving band quality."""
    from shared.discovery_band_labels import is_default_eligible

    ident = Identification(
        best_evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        best_confidence_band=ids.CONFIDENCE_BAND_SCREENING_CANON,
        best_adjudication_status=None,
        best_routing_status=ids.ROUTING_STATUS_SHIPPED,
    )
    in_main, reason = main_pool_decision(ident)
    assert in_main is False
    assert reason == REASON_MISSING_SIGNAL
    # The decision must agree with is_default_eligible for the SAME inputs.
    assert is_default_eligible(
        ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_CANON,
        None, ids.ROUTING_STATUS_SHIPPED, None, ci_low=None,
    ) is False


def test_unresolved_tie_on_every_page_rejects():
    """Unresolved near-tie competitor on EVERY matched page -> not-main,
    overlapping_tie."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": True, "p2": True},
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_OVERLAPPING_TIE)


def test_one_clean_page_is_enough_to_survive_gate3():
    """One clean (non-tied) page among several is enough to survive gate 3;
    with >=2 pages the identification then routes to main_multifolio."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": True, "p2": False},
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (True, REASON_MAIN_MULTIFOLIO)


def test_single_page_below_coverage_floor_is_low_coverage():
    """Single-page identification with page coverage < 0.8 -> not-main,
    low_coverage."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False},
        max_matched_letters=500,
        max_coverage=COVERAGE_FLOOR - 0.05,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_LOW_COVERAGE)


def test_same_identification_across_two_pages_is_main_multifolio():
    """The SAME identification (same claim quality) across two pages
    returns main, main_multifolio -- even though a single page alone at the
    same coverage would have been low_coverage."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False, "p2": False},
        max_matched_letters=500,
        max_coverage=COVERAGE_FLOOR - 0.05,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (True, REASON_MAIN_MULTIFOLIO)


def test_single_page_at_or_above_floor_is_main_full_coverage():
    """Single-page identification at or above the coverage floor -> main,
    main_full_coverage."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False},
        max_matched_letters=500,
        max_coverage=COVERAGE_FLOOR,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (True, REASON_MAIN_FULL_COVERAGE)


def test_human_confirmed_overrides_every_gate_even_when_routing_demoted():
    """adjudication_status='human_confirmed' on any claim -> main,
    main_human_confirmed, evaluated BEFORE every gate -- including when
    routing demoted it (D-13g) and even though every OTHER gate would have
    rejected this identification."""
    ident = Identification(
        any_human_confirmed=True,
        has_same_work_claim=False,  # would otherwise fail gate 1
        best_routing_status=ids.ROUTING_STATUS_REVIEW_ONLY,  # demoted by routing
        best_confidence_band=ids.CONFIDENCE_BAND_SCREENING_CANON,  # would otherwise fail gate 2
        page_has_unresolved_competitor={"p1": True},  # would otherwise fail gate 3
        max_matched_letters=1,
        max_coverage=0.0,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (True, REASON_MAIN_HUMAN_CONFIRMED)


def test_below_short_evidence_threshold_is_insufficient_length():
    """Below the ratified D-13c short-evidence threshold (150 matched
    letters) -> not-main, insufficient_length -- even though coverage alone
    would have passed."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False},
        max_matched_letters=SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS - 1,
        max_coverage=1.0,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_INSUFFICIENT_LENGTH)


def test_short_evidence_exempt_via_multi_folio_agreement():
    """D-13c's own exception: a short (< 150 matched letters) identification
    that ALREADY qualifies as main via multi-folio agreement is NOT
    demoted -- multi-folio wins before length is ever consulted."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False, "p2": False},
        max_matched_letters=10,  # far below the 150 threshold
        max_coverage=0.99,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (True, REASON_MAIN_MULTIFOLIO)


def test_missing_signal_when_gate_needs_data_that_is_absent():
    """An identification missing a signal a gate needs -> not-main,
    missing_signal -- never main by default."""
    ident = Identification(
        page_has_unresolved_competitor={"p1": False},
        max_matched_letters=None,
        max_coverage=None,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_MISSING_SIGNAL)

    # Also: no per-page competition data at all is a missing signal, never a
    # silent pass.
    ident_no_pages = Identification(page_has_unresolved_competitor={})
    in_main2, reason2 = main_pool_decision(ident_no_pages)
    assert (in_main2, reason2) == (False, REASON_MISSING_SIGNAL)


def test_gates_are_fixed_order_and_non_compensating():
    """Gates are evaluated in a fixed order and are non-compensating: an
    identification that fails an EARLIER gate is rejected for that gate's
    reason even when every LATER gate's signal would have passed easily."""
    # Fails gate 1 (no same-work claim) despite otherwise-perfect signals.
    ident = Identification(
        has_same_work_claim=False,
        page_has_unresolved_competitor={"p1": False, "p2": False},
        max_matched_letters=10_000,
        max_coverage=1.0,
    )
    in_main, reason = main_pool_decision(ident)
    assert (in_main, reason) == (False, REASON_SHARED_WORDING), (
        "a later, stronger signal (multi-folio, huge coverage) must never "
        "compensate for an earlier gate's rejection"
    )

    # Fails gate 2 (screening band) despite multi-folio agreement.
    ident2 = Identification(
        best_confidence_band=ids.CONFIDENCE_BAND_SCREENING_CANON,
        page_has_unresolved_competitor={"p1": False, "p2": False},
    )
    in_main2, reason2 = main_pool_decision(ident2)
    assert (in_main2, reason2) == (False, REASON_MISSING_SIGNAL)


# ---------------------------------------------------------------------------
# Task 1 acceptance criteria: vocabulary drift guard + no density + no
# weighted-sum arithmetic + gate 2 delegation + cited/provisional constants.
# ---------------------------------------------------------------------------

def test_reason_vocabulary_equals_schema_amendment():
    """MAIN_POOL_REASONS must equal, as a SET, the schema's
    `main_pool_reason` CHECK constraint vocabulary exactly."""
    assert MAIN_POOL_REASONS == _SCHEMA_MAIN_POOL_REASON_VOCAB


def test_module_never_reads_density():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "density" not in source


def test_module_has_no_weighted_sum_arithmetic():
    """No expression multiplying a signal by a weight and summing: scan for
    any multiplication operator anywhere in the module's AST."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        assert not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult)), (
            f"found a multiplication at line {getattr(node, 'lineno', '?')} -- "
            "main_pool_decision must never compute a weighted score"
        )


def test_gate2_delegates_to_is_default_eligible_no_local_band_allowlist():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "is_default_eligible(" in source
    # No raw band-name string literal anywhere in the module's actual code
    # (only via the shared ids.CONFIDENCE_BAND_* constants) -- a local
    # allowlist would restate one of these literally.
    band_literals = (
        "screening_canon", "screening_rb", "not_evaluated",
        "corroborated", "high_confidence_algorithmic",
    )
    tree = ast.parse(source)
    literal_strings = {
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    for band in band_literals:
        assert band not in literal_strings, (
            f"found raw band-name literal {band!r} in shared/discovery_main_pool.py -- "
            "gate 2 must delegate to is_default_eligible, never a local band allowlist"
        )


def test_constants_are_cited_and_state_provisional_status():
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "136-GATE1-DECISIONS.md" in source
    assert "SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS = 150" in source
    assert "COVERAGE_FLOOR = 0.8" in source
    assert "PROVISIONAL" in source
    assert SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS == 150
    assert COVERAGE_FLOOR == 0.8


# ---------------------------------------------------------------------------
# Task 3 (PANEL-01/PANEL-02): "One wording for the rule" -- main_pool_sentence,
# bucket_label, the parity assertion against web/pages/help.py's own
# MAIN_POOL_SENTENCE constant, and the standing "no second bucket-membership
# predicate" guard (T-136-07-01/02).
# ---------------------------------------------------------------------------

HELP_MODULE_PATH = REPO_ROOT / "web" / "pages" / "help.py"

_PROHIBITED_RELATION_WORDS = ("copy of", "quotes", "witness of")


def test_main_pool_sentence_has_no_percent_or_prohibited_words():
    """main_pool_sentence('en')/('he') are non-empty and contain no `%` and
    none of the prohibited relation words D-06/D-21 bar from every discovery
    surface."""
    for lang in ("en", "he"):
        sentence = main_pool_sentence(lang)
        assert sentence, f"main_pool_sentence({lang!r}) must be non-empty"
        assert "%" not in sentence
        lowered = sentence.lower()
        for phrase in _PROHIBITED_RELATION_WORDS:
            assert phrase not in lowered, (
                f"main_pool_sentence({lang!r}) contains the prohibited phrase {phrase!r}"
            )


def _extract_help_page_main_pool_sentence():
    """Read (never import) `web/pages/help.py`'s source text and extract its
    `MAIN_POOL_SENTENCE` dict via `ast.literal_eval` -- `shared/` must not
    import `web/`, and the literal's EN/HE values are each split across
    multiple adjacent string-literal lines in the source (Python's implicit
    string concatenation), so a plain substring search over the raw file
    text cannot locate the merged value. AST-based extraction reads the
    file's own source text -- exactly what the plan's action text asks
    for -- without ever executing or importing the module."""
    source = HELP_MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "MAIN_POOL_SENTENCE":
                    return ast.literal_eval(node.value)
    raise AssertionError(
        "web/pages/help.py has no top-level MAIN_POOL_SENTENCE assignment -- "
        "the parity test has nothing to compare against"
    )


def test_main_pool_sentence_parity_with_help_page():
    """`main_pool_sentence('en')`/`('he')` must equal `web/pages/help.py`'s
    own `MAIN_POOL_SENTENCE` constant byte-for-byte in BOTH languages, so the
    rule can never be described two different ways on two surfaces. Fails
    with both values printed when they diverge."""
    help_sentence = _extract_help_page_main_pool_sentence()
    for lang in ("en", "he"):
        ours = main_pool_sentence(lang)
        theirs = help_sentence.get(lang)
        assert ours == theirs, (
            f"main_pool_sentence({lang!r}) diverges from web/pages/help.py's "
            f"MAIN_POOL_SENTENCE[{lang!r}]:\n"
            f"  shared/discovery_main_pool.py -> {ours!r}\n"
            f"  web/pages/help.py             -> {theirs!r}"
        )


def test_bucket_label_returns_bilingual_names_and_is_sole_definition():
    """`bucket_label` returns the two owner-named bilingual bucket labels,
    and is the ONLY definition of a function by this name anywhere under
    `shared/`."""
    assert bucket_label(True, "en") == "main pool"
    assert bucket_label(False, "en") == "more matches"
    assert bucket_label(True, "he") == "מאגר עיקרי"
    assert bucket_label(False, "he") == "התאמות נוספות"

    other_definitions = []
    for path in sorted((REPO_ROOT / "shared").rglob("*.py")):
        if path.resolve() == MODULE_PATH.resolve():
            continue
        source = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "bucket_label"
            ):
                other_definitions.append(str(path))
    assert other_definitions == [], (
        f"found a SECOND bucket_label definition under shared/: {other_definitions} -- "
        "shared/discovery_main_pool.py must be the ONLY definition"
    )


# ---------------------------------------------------------------------------
# The standing "no second bucket-membership predicate" guard (T-136-07-01).
# Scans `shared/` and `web/` for a LOCALLY-DEFINED predicate that decides
# bucket membership by testing a band name against a hand-picked set of >= 2
# confidence-band-name literals -- exactly the shape of sketch 003's
# `confOf()`/`STRONG_BANDS`, which disagreed with the codebase's own rule
# and rendered the best-measured population in the system "Weak".
# `shared/discovery_band_labels.py` (the CANONICAL `is_default_eligible`
# implementation) and this module itself (already separately asserted, above,
# to carry no raw band-name literal at all) are the two legitimate places a
# band-name literal may appear in a membership test, and are exempted.
# ---------------------------------------------------------------------------

_BAND_NAME_LITERALS = frozenset({
    "expert_verified", "high_confidence_algorithmic", "tier_a",
    "screening_rb", "screening_canon", "corroborated", "weak", "not_evaluated",
})

_SECOND_IMPL_GUARD_EXEMPT = {
    (REPO_ROOT / "shared" / "discovery_band_labels.py").resolve(),
    MODULE_PATH.resolve(),
}


def _string_literals_in(node) -> set:
    literals = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            literals.add(n.value)
    return literals


def _resolve_named_literal_string_sets(tree) -> dict:
    """Map every Name assigned a set/list/tuple literal (or a `set(...)`/
    `frozenset(...)` call over one) ANYWHERE in the module to the set of
    string literals it contains -- so `STRONG_BANDS = {'tier_a', ...}`
    followed by `band in STRONG_BANDS` resolves through the named constant,
    exactly the shape of sketch 003's real `confOf()`/`STRONG_BANDS` bug
    (an inline literal alone would have missed the actual historical case)."""
    resolved: dict = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        value = node.value
        literals = None
        if isinstance(value, (ast.Set, ast.List, ast.Tuple)):
            literals = _string_literals_in(value)
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id in ("set", "frozenset")
        ):
            literals = _string_literals_in(value.args[0]) if value.args else set()
        if literals is None:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                resolved[target.id] = literals
    return resolved


def _find_second_bucket_membership_predicates(roots):
    """Scan every `.py` file under each root in `roots` (skipping the two
    exempted canonical files) for an `ast.Compare` node testing membership
    (`in` / `not in`) against a literal collection -- OR a named constant
    resolving to one, per `_resolve_named_literal_string_sets` -- containing
    >= 2 confidence-band-name strings. That is a local reimplementation of
    the bucket rule, never routed through `is_default_eligible` or
    `main_pool_decision`. Returns a list of `(path, lineno)` violations."""
    violations = []
    for root in roots:
        root = pathlib.Path(root)
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            if path.resolve() in _SECOND_IMPL_GUARD_EXEMPT:
                continue
            source = path.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            named_sets = _resolve_named_literal_string_sets(tree)
            for node in ast.walk(tree):
                if isinstance(node, ast.Compare):
                    for op, comparator in zip(node.ops, node.comparators):
                        if not isinstance(op, (ast.In, ast.NotIn)):
                            continue
                        if isinstance(comparator, ast.Name):
                            literals = named_sets.get(comparator.id, set())
                        else:
                            literals = _string_literals_in(comparator)
                        if len(literals & _BAND_NAME_LITERALS) >= 2:
                            violations.append((str(path), node.lineno))
    return violations


def test_second_implementation_guard_finds_none_on_the_real_tree():
    """The standing guard: scanning `shared/` and `web/` today finds no
    second, locally-defined bucket-membership predicate -- gate 2 delegates
    to `is_default_eligible` and no other module re-derives a band
    allowlist. Names what it scanned: every `.py` file under `shared/` and
    `web/`, excluding `shared/discovery_band_labels.py` (the canonical
    implementation) and `shared/discovery_main_pool.py` (this module,
    separately proven band-literal-free); the pattern is a membership test
    against >= 2 confidence-band-name string literals."""
    violations = _find_second_bucket_membership_predicates(
        [REPO_ROOT / "shared", REPO_ROOT / "web"]
    )
    assert violations == [], (
        f"found a candidate second bucket-membership predicate: {violations} -- "
        "this is exactly the class of bug sketch 003's confOf() was"
    )


def test_second_implementation_guard_catches_a_seeded_duplicate(tmp_path):
    """Positive control, proving the guard is not vacuously green: seed a
    scratch module containing a hand-picked band-set predicate (the
    `confOf()`/`STRONG_BANDS` shape) under a throwaway `tmp_path` root --
    never inside the tracked tree -- and assert the guard actually fails on
    it. The scratch file is removed at the end of this test (`finally:
    scratch_file.unlink()`), on top of pytest's own automatic `tmp_path`
    teardown, so this is the seed-then-revert exercise the plan's own
    acceptance criteria ask for, observed actually failing before being
    reverted."""
    scratch_dir = tmp_path / "shared"
    scratch_dir.mkdir()
    scratch_file = scratch_dir / "scratch_confof.py"
    scratch_file.write_text(
        "STRONG_BANDS = {'tier_a', 'high_confidence_algorithmic'}\n\n"
        "def confOf(band):\n"
        "    return band in STRONG_BANDS\n",
        encoding="utf-8",
    )
    try:
        violations = _find_second_bucket_membership_predicates([tmp_path])
        assert violations, (
            "the guard failed to catch a seeded confOf()-shaped band-set "
            "predicate -- it must fail on this input"
        )
    finally:
        scratch_file.unlink()
