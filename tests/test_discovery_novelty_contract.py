# -*- coding: utf-8 -*-
"""Tests for the discovery novelty axis contract (Phase 136, plan 136-04).

Task 1: `shared/discovery_novelty.py` -- the ten-value shade enum, the
identity key, the pinned LLM contract, masked provenance, the
verdict->column mapping.

Task 2: `scripts/discovery_novelty_funnel.py` -- the committed funnel
runner and its owner-label grading harness (see the second half of this
file, added alongside that script).

Owner rulings: `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
sections E, E', F, G, H, I, J.
"""

from __future__ import annotations

import io
import re

import pytest

from shared.discovery_novelty import (
    ABSTENTION_TOKEN,
    CACHE_KEY_FIELDS,
    CANDIDATE_STATUS,
    DEFAULT_STATUS,
    DIVERGENCE_CORRECTNESS_VALUES,
    DIVERGENCE_SHADES,
    HIDDEN_BY_DEFAULT_SHADES,
    INPUT_NORMALIZATION_SHA256,
    LLM_MODEL,
    LLM_MODEL_VERSION,
    LLM_REASONING_EFFORT,
    NOVELTY_STATUS_ORDER,
    NOVELTY_STATUSES,
    PROMPT_SHA256,
    SOURCE_LABEL_ELIGIBLE_SHADES,
    build_cache_key,
    divergence_correctness_applicable,
    is_candidate_for_new_finds,
    is_hidden_by_default,
    load_alias_groups,
    masked_provenance_label,
    normalize_free_text,
    novelty_columns_for,
    novelty_work_key,
    resolve_model_output,
)

# ---------------------------------------------------------------------------
# The ten-value shade enum
# ---------------------------------------------------------------------------

EXPECTED_TEN_VALUES = frozenset({
    "confirms",
    "refines_granularity",
    "aid_more_specific",
    "diverges_work",
    "diverges_part",
    "container_predicts",
    "fills_gap",
    "extends",
    "alias_merge",
    "not_checked",
})


def test_novelty_statuses_is_exactly_ten_values():
    assert NOVELTY_STATUSES == EXPECTED_TEN_VALUES
    assert len(NOVELTY_STATUSES) == 10


def test_novelty_status_order_is_a_permutation_of_the_frozenset():
    assert frozenset(NOVELTY_STATUS_ORDER) == NOVELTY_STATUSES
    assert len(NOVELTY_STATUS_ORDER) == len(set(NOVELTY_STATUS_ORDER))


def test_default_status_is_not_checked():
    assert DEFAULT_STATUS == "not_checked"
    assert DEFAULT_STATUS in NOVELTY_STATUSES


def test_retired_tristate_values_are_not_in_the_enum():
    """The RETIRED tri-state (not_in_finding_aids / already_recorded) and the
    RETIRED single `diverges` token (before ruling F split it) must never
    reappear."""
    retired = {"not_in_finding_aids", "already_recorded", "diverges", "known", "not_found", "indeterminate"}
    assert retired.isdisjoint(NOVELTY_STATUSES)


def test_schema_check_constraint_matches_module_vocabulary():
    """Drift guard against docs/specs/discovery-sidecar-schema-v1.md's SQL
    CHECK constraint -- the one place a second literal restatement is
    structurally unavoidable (SQLite CHECK constraints cannot import a
    shared constant). Extracts the literal IN (...) list and asserts
    equality with shared.discovery_novelty.NOVELTY_STATUSES."""
    with io.open("docs/specs/discovery-sidecar-schema-v1.md", encoding="utf-8") as fh:
        schema_text = fh.read()
    m = re.search(
        r"novelty_status\s+TEXT\s+NOT\s+NULL\s+CHECK\s*\(novelty_status\s+IN\s*\(([^)]*)\)\)",
        schema_text,
    )
    assert m is not None, "could not find the novelty_status CHECK constraint in the schema doc"
    tokens = frozenset(t.strip().strip("'\"") for t in m.group(1).split(","))
    assert tokens == NOVELTY_STATUSES


# ---------------------------------------------------------------------------
# Candidate-toggle predicate
# ---------------------------------------------------------------------------

def test_candidate_status_is_fills_gap():
    assert CANDIDATE_STATUS == "fills_gap"


def test_is_candidate_for_new_finds_selects_only_fills_gap():
    for status in NOVELTY_STATUSES:
        expected = status == "fills_gap"
        assert is_candidate_for_new_finds(status) is expected


def test_is_candidate_for_new_finds_raises_on_unknown_status():
    with pytest.raises(ValueError):
        is_candidate_for_new_finds("bogus_status")


# ---------------------------------------------------------------------------
# Ruling F / H: hidden-by-default posture
# ---------------------------------------------------------------------------

def test_hidden_by_default_shades_is_exactly_the_two_divergence_shades():
    assert HIDDEN_BY_DEFAULT_SHADES == frozenset({"diverges_work", "diverges_part"})


def test_container_predicts_is_not_hidden_by_default():
    """Ruling H is explicit: F's default-hidden rationale (rows the owner
    has measured reason to believe are OUR false positives) does not apply
    to container_predicts (no disagreement to warn about)."""
    assert "container_predicts" not in HIDDEN_BY_DEFAULT_SHADES
    assert is_hidden_by_default("container_predicts") is False


def test_is_hidden_by_default_matches_the_frozenset_for_every_status():
    for status in NOVELTY_STATUSES:
        assert is_hidden_by_default(status) is (status in HIDDEN_BY_DEFAULT_SHADES)


def test_is_hidden_by_default_raises_on_unknown_status():
    with pytest.raises(ValueError):
        is_hidden_by_default("bogus_status")


# ---------------------------------------------------------------------------
# Ruling F: divergence_correctness sibling axis
# ---------------------------------------------------------------------------

def test_divergence_correctness_values_is_exactly_three():
    assert DIVERGENCE_CORRECTNESS_VALUES == frozenset(
        {"catalogue_correct", "claim_correct", "unclear"}
    )


def test_divergence_correctness_applicable_only_for_divergence_shades():
    for status in NOVELTY_STATUSES:
        expected = status in {"diverges_work", "diverges_part"}
        assert divergence_correctness_applicable(status) is expected


def test_divergence_shades_frozenset_matches_hidden_by_default():
    # Coincidentally the same two values today; asserted independently so a
    # FUTURE divergence of the two concepts (e.g. a new hidden-by-default
    # shade that is not a divergence shade) is caught rather than silently
    # assumed identical forever.
    assert DIVERGENCE_SHADES == frozenset({"diverges_work", "diverges_part"})


def test_divergence_correctness_applicable_raises_on_unknown_status():
    with pytest.raises(ValueError):
        divergence_correctness_applicable("bogus_status")


# ---------------------------------------------------------------------------
# novelty_columns_for -- the pure verdict -> column mapping
# ---------------------------------------------------------------------------

def test_novelty_columns_for_maps_every_status():
    for status in NOVELTY_STATUSES:
        cols = novelty_columns_for(status)
        assert cols["novelty_status"] == status
        assert cols["divergence_correctness"] is None


def test_novelty_columns_for_raises_on_unknown_status():
    with pytest.raises(ValueError):
        novelty_columns_for("bogus_status")


def test_novelty_columns_for_accepts_correctness_on_divergence_shades():
    cols = novelty_columns_for("diverges_work", divergence_correctness="catalogue_correct")
    assert cols["divergence_correctness"] == "catalogue_correct"
    cols2 = novelty_columns_for("diverges_part", divergence_correctness="unclear")
    assert cols2["divergence_correctness"] == "unclear"


def test_novelty_columns_for_rejects_correctness_on_non_divergence_shade():
    """This is the acceptance-criteria-mandated test: constructs the case
    and asserts the RAISE, not a silent drop."""
    with pytest.raises(ValueError):
        novelty_columns_for("confirms", divergence_correctness="catalogue_correct")
    with pytest.raises(ValueError):
        novelty_columns_for("fills_gap", divergence_correctness="unclear")


def test_novelty_columns_for_rejects_out_of_vocab_correctness():
    with pytest.raises(ValueError):
        novelty_columns_for("diverges_work", divergence_correctness="bogus_value")


def test_novelty_columns_for_source_label_gated_by_eligibility():
    # Eligible shade: source_label passes through.
    cols = novelty_columns_for("confirms", source_label="recorded in the catalogue")
    assert cols["novelty_source_label"] == "recorded in the catalogue"
    # Ineligible shade (fills_gap): forced to None even if a caller
    # mistakenly supplied one.
    cols2 = novelty_columns_for("fills_gap", source_label="recorded in the catalogue")
    assert cols2["novelty_source_label"] is None
    # not_checked: also forced to None.
    cols3 = novelty_columns_for("not_checked", source_label="recorded in the catalogue")
    assert cols3["novelty_source_label"] is None


def test_source_label_eligible_shades_excludes_fills_gap_and_not_checked():
    assert "fills_gap" not in SOURCE_LABEL_ELIGIBLE_SHADES
    assert "not_checked" not in SOURCE_LABEL_ELIGIBLE_SHADES
    assert SOURCE_LABEL_ELIGIBLE_SHADES == NOVELTY_STATUSES - {"fills_gap", "not_checked"}


# ---------------------------------------------------------------------------
# masked_provenance_label -- D-25 / NOVEL-02, adversarial input table
# ---------------------------------------------------------------------------

_RESTRICTED_CODENAME_MARKERS = ("m_source_shelfmark", "m-source", "M-source", "MSOURCE")

ADVERSARIAL_SOURCE_CODES = (
    None,
    "",
    "bogus_unknown_source",
    "m_source_shelfmark",
    "M-SOURCE",
    "r_source_shelfmark",
    12345,
    3.14,
    True,
    ["a", "list"],
    {"a": "dict"},
    object(),
)


@pytest.mark.parametrize("source_code", ADVERSARIAL_SOURCE_CODES)
def test_masked_provenance_label_never_leaks_a_restricted_name(source_code):
    for lang in ("en", "he"):
        label = masked_provenance_label(source_code, lang)
        assert isinstance(label, str)
        for marker in _RESTRICTED_CODENAME_MARKERS:
            assert marker not in label


def test_masked_provenance_label_never_raises_on_adversarial_input():
    for source_code in ADVERSARIAL_SOURCE_CODES:
        masked_provenance_label(source_code, "en")
        masked_provenance_label(source_code, "he")


def test_masked_provenance_label_names_a_nameable_source():
    assert masked_provenance_label("fjms_catalogue", "en") == "recorded in the catalogue"


def test_masked_provenance_label_unknown_input_falls_back():
    assert masked_provenance_label("bogus_unknown_source", "en") == "recorded in another reference source"
    assert masked_provenance_label(None, "en") == "recorded in another reference source"


def test_masked_provenance_label_defaults_to_english_for_unknown_lang():
    assert masked_provenance_label("fjms_catalogue", "fr") == masked_provenance_label("fjms_catalogue", "en")


# ---------------------------------------------------------------------------
# novelty_work_key -- reviewed, alias-aware identity (D-23d)
# ---------------------------------------------------------------------------

def test_novelty_work_key_returns_none_for_no_reviewable_identity():
    assert novelty_work_key({}) is None
    assert novelty_work_key({"work_id": None}) is None
    assert novelty_work_key({"work_id": ""}) is None


def test_novelty_work_key_returns_none_maps_to_not_checked_by_caller():
    """The contract: a caller receiving None from novelty_work_key must map
    it to not_checked, never guess a key. This test asserts the CALLER
    convention directly (a tiny inline caller), not merely that None is
    returned."""
    def caller_resolve(work_row):
        key = novelty_work_key(work_row)
        return DEFAULT_STATUS if key is None else "would_check"

    assert caller_resolve({}) == "not_checked"


def test_novelty_work_key_singleton_when_no_curated_alias_group():
    assert novelty_work_key({"work_id": "w000123"}, alias_groups={}) == "w000123"


def test_novelty_work_key_alias_rule_is_order_independent():
    group_order_a = {"groups": [["w000005", "w000012", "w000099"]]}
    group_order_b = {"groups": [["w000099", "w000005", "w000012"]]}

    def _groups_from(raw):
        groups = {}
        for group in raw["groups"]:
            frozen = frozenset(group)
            for member in group:
                groups[member] = frozen
        return groups

    alias_groups_a = _groups_from(group_order_a)
    alias_groups_b = _groups_from(group_order_b)

    for member in ("w000005", "w000012", "w000099"):
        key_a = novelty_work_key({"work_id": member}, alias_groups=alias_groups_a)
        key_b = novelty_work_key({"work_id": member}, alias_groups=alias_groups_b)
        assert key_a == key_b == "w000005"  # lexicographically smallest


def test_novelty_work_key_known_via_any_alias_implies_same_key():
    """D-23d's rule, restated: every member of a curated alias group shares
    the SAME reviewed key, so a caller doing a 'known' membership test
    against a single key catches ALL aliases, not just the one the
    catalogue happens to use."""
    alias_groups = {"w000005": frozenset({"w000005", "w000012"}), "w000012": frozenset({"w000005", "w000012"})}
    known_keys = {novelty_work_key({"work_id": "w000005"}, alias_groups=alias_groups)}
    # A candidate claiming the OTHER alias resolves to the same key, so a
    # membership test against known_keys succeeds regardless of which
    # alias spelling is claimed.
    candidate_key = novelty_work_key({"work_id": "w000012"}, alias_groups=alias_groups)
    assert candidate_key in known_keys


def test_load_alias_groups_returns_empty_dict_when_artifact_absent():
    assert load_alias_groups("/nonexistent/path/does-not-exist.json") == {}


def test_novelty_work_key_never_fabricates_a_key():
    """A raw id with no curated group returns itself (a real, reviewable
    identity) -- never something invented from nothing when no id was
    supplied at all."""
    assert novelty_work_key({"work_id": "genuinely_unreviewed_id"}, alias_groups={}) == "genuinely_unreviewed_id"
    assert novelty_work_key({}, alias_groups={}) is None


# ---------------------------------------------------------------------------
# Pinned LLM contract constants -- module-level string literals a CI grep
# can see.
# ---------------------------------------------------------------------------

def test_pinned_constants_are_string_literals():
    assert isinstance(LLM_MODEL, str) and LLM_MODEL
    assert isinstance(LLM_MODEL_VERSION, str) and LLM_MODEL_VERSION
    assert isinstance(LLM_REASONING_EFFORT, str) and LLM_REASONING_EFFORT
    assert isinstance(PROMPT_SHA256, str) and len(PROMPT_SHA256) == 64
    assert isinstance(INPUT_NORMALIZATION_SHA256, str) and len(INPUT_NORMALIZATION_SHA256) == 64


def test_pinned_model_matches_owner_ruling_b():
    assert LLM_MODEL == "gemini-3.6-flash"
    assert LLM_REASONING_EFFORT == "low"


def test_grep_finds_each_pinned_constant_in_the_module_source():
    with io.open("shared/discovery_novelty.py", encoding="utf-8") as fh:
        source = fh.read()
    for name in ("LLM_MODEL", "LLM_MODEL_VERSION", "LLM_REASONING_EFFORT", "PROMPT_SHA256", "INPUT_NORMALIZATION_SHA256"):
        assert name in source


def test_prompt_states_ruling_g_and_ruling_h_directly():
    from shared.discovery_novelty import NOVELTY_PROMPT_TEMPLATE

    assert "ruling G" in NOVELTY_PROMPT_TEMPLATE
    assert "container_predicts" in NOVELTY_PROMPT_TEMPLATE
    assert "structured field points elsewhere" in NOVELTY_PROMPT_TEMPLATE


def test_cache_key_fields_order_is_fixed_and_pins_first():
    assert CACHE_KEY_FIELDS[0] == "llm_model"
    assert CACHE_KEY_FIELDS[1] == "llm_model_version"
    assert CACHE_KEY_FIELDS[2] == "llm_reasoning_effort"
    assert CACHE_KEY_FIELDS[3] == "prompt_sha256"
    assert CACHE_KEY_FIELDS[4] == "input_normalization_sha256"


def test_build_cache_key_is_deterministic():
    fields = {f: f"value-{f}" for f in CACHE_KEY_FIELDS}
    key1 = build_cache_key(fields)
    key2 = build_cache_key(dict(fields))  # different dict object, same content
    assert key1 == key2
    assert len(key1) == 64


def test_build_cache_key_raises_on_missing_field():
    fields = {f: "x" for f in CACHE_KEY_FIELDS[:-1]}  # missing the last field
    with pytest.raises(ValueError):
        build_cache_key(fields)


def test_build_cache_key_changes_when_pinned_constants_change():
    base = {f: f"value-{f}" for f in CACHE_KEY_FIELDS}
    changed = dict(base)
    changed["prompt_sha256"] = "a-different-hash"
    assert build_cache_key(base) != build_cache_key(changed)


# ---------------------------------------------------------------------------
# resolve_model_output -- structured abstention -> not_checked
# ---------------------------------------------------------------------------

def test_resolve_model_output_abstain_flag_maps_to_not_checked():
    out = resolve_model_output({"abstain": True, "reason": "ambiguous"})
    assert out["novelty_status"] == "not_checked"
    assert out["divergence_correctness"] is None


def test_resolve_model_output_abstention_token_maps_to_not_checked():
    out = resolve_model_output({"novelty_status": ABSTENTION_TOKEN})
    assert out["novelty_status"] == "not_checked"


def test_resolve_model_output_none_or_malformed_maps_to_not_checked():
    assert resolve_model_output(None)["novelty_status"] == "not_checked"
    assert resolve_model_output({})["novelty_status"] == "not_checked"
    assert resolve_model_output({"novelty_status": "bogus"})["novelty_status"] == "not_checked"
    assert resolve_model_output({"novelty_status": 12345})["novelty_status"] == "not_checked"


def test_resolve_model_output_valid_shade_passes_through():
    out = resolve_model_output({"novelty_status": "confirms"})
    assert out["novelty_status"] == "confirms"
    assert out["divergence_correctness"] is None


def test_resolve_model_output_divergence_with_correctness():
    out = resolve_model_output({"novelty_status": "diverges_work", "divergence_correctness": "claim_correct"})
    assert out["novelty_status"] == "diverges_work"
    assert out["divergence_correctness"] == "claim_correct"


def test_resolve_model_output_drops_malformed_correctness_keeps_shade():
    out = resolve_model_output({"novelty_status": "diverges_work", "divergence_correctness": "bogus"})
    assert out["novelty_status"] == "diverges_work"
    assert out["divergence_correctness"] is None


def test_resolve_model_output_drops_correctness_on_non_divergence_shade():
    out = resolve_model_output({"novelty_status": "confirms", "divergence_correctness": "claim_correct"})
    assert out["novelty_status"] == "confirms"
    assert out["divergence_correctness"] is None


# ---------------------------------------------------------------------------
# normalize_free_text
# ---------------------------------------------------------------------------

def test_normalize_free_text_empty_and_none():
    assert normalize_free_text(None) == ""
    assert normalize_free_text("") == ""


def test_normalize_free_text_collapses_whitespace():
    assert normalize_free_text("a   b\n\nc") == "a b c"


def test_normalize_free_text_strips_nikud_and_diacritics():
    # A nikud-bearing string and its bare-letter equivalent should normalize
    # to the same thing.
    with_nikud = "בְראשית"  # Bereshit with nikud
    bare = "בראשית"
    assert normalize_free_text(with_nikud) == normalize_free_text(bare)


# ---------------------------------------------------------------------------
# No dependency on the gitignored same_work_spike tree
# ---------------------------------------------------------------------------

def test_module_does_not_reference_same_work_spike():
    with io.open("shared/discovery_novelty.py", encoding="utf-8") as fh:
        source = fh.read()
    assert "same_work_spike" not in source
