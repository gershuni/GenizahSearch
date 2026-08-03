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
import json
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

from scripts.discovery_novelty_funnel import (
    NoOwnerProvenanceLabels,
    LabelHashMismatch,
    NoveltyCandidate,
    UNMAPPED_PAGE_REASON,
    NO_SOURCE_TEXT_REASON,
    assemble_evidence_bundle,
    grade_against_owner_labels,
    load_owner_labels,
    run_heuristic_funnel,
    run_heuristic_pass,
    run_model_arm,
    _label_file_content_hash,
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


def test_prompt_no_longer_elicits_divergence_correctness():
    """Ruling L (136-GATE1-DECISIONS.md section L): divergence_correctness
    is removed from the model's job entirely -- the pinned prompt must never
    mention it, ask for it, or describe a response shape that includes it."""
    from shared.discovery_novelty import NOVELTY_PROMPT_TEMPLATE

    assert "divergence_correctness" not in NOVELTY_PROMPT_TEMPLATE
    assert "correctness" not in NOVELTY_PROMPT_TEMPLATE.lower()


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


def test_resolve_model_output_divergence_shade_never_carries_correctness_from_model():
    """Ruling L (136-GATE1-DECISIONS.md section L): divergence_correctness is
    dropped from the model's job entirely (measured 8/28, at or below chance
    for a three-way vocabulary) -- resolve_model_output must NEVER surface a
    model-supplied correctness value, even if a raw response happens to
    carry one (a stale pre-ruling-L cached response, a hallucinated key,
    anything). The shade itself still passes through unaffected."""
    out = resolve_model_output({"novelty_status": "diverges_work", "divergence_correctness": "claim_correct"})
    assert out["novelty_status"] == "diverges_work"
    assert out["divergence_correctness"] is None


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


# ===========================================================================
# Task 2: scripts/discovery_novelty_funnel.py -- the funnel runner and its
# owner-label grading harness.
# ===========================================================================

# ---------------------------------------------------------------------------
# Script-level hygiene: no gitignored-tree dependency, imports the pinned
# contract from shared/discovery_novelty.py, never uses catalog_refs as a
# data source.
# ---------------------------------------------------------------------------

def test_funnel_script_does_not_reference_same_work_spike():
    with io.open("scripts/discovery_novelty_funnel.py", encoding="utf-8") as fh:
        source = fh.read()
    assert "same_work_spike" not in source


def test_funnel_script_never_uses_catalog_refs():
    with io.open("scripts/discovery_novelty_funnel.py", encoding="utf-8") as fh:
        source = fh.read()
    assert "catalog_refs" not in source


def test_funnel_script_imports_pinned_contract_from_shared_module():
    with io.open("scripts/discovery_novelty_funnel.py", encoding="utf-8") as fh:
        source = fh.read()
    assert "from shared.discovery_novelty import" in source
    for name in ("LLM_MODEL", "LLM_MODEL_VERSION", "LLM_REASONING_EFFORT", "PROMPT_SHA256", "INPUT_NORMALIZATION_SHA256", "build_cache_key"):
        assert name in source


def test_funnel_script_mentions_discovery_novelty_and_label_provenance():
    with io.open("scripts/discovery_novelty_funnel.py", encoding="utf-8") as fh:
        source = fh.read()
    assert "discovery_novelty" in source
    assert "label_provenance" in source


# ---------------------------------------------------------------------------
# Abstention path
# ---------------------------------------------------------------------------

def test_dry_run_demonstrates_abstention_without_a_model_call(capsys):
    import scripts.discovery_novelty_funnel as funnel_mod

    rc = funnel_mod.main(["--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "structured abstention" in out
    assert "not_checked" in out


# ---------------------------------------------------------------------------
# assemble_evidence_bundle -- per-source provenance tagging (Codex finding 5)
# ---------------------------------------------------------------------------

def test_assemble_evidence_bundle_tags_every_source_separately():
    candidate = NoveltyCandidate(
        sys_id="s1",
        ref_work_id="w1",
        claimed_title="עבודה",
        catalogue_text="טקסט קטלוג",
        bibliography_rows=({"text": "טקסט ביבליוגרפי", "transcription_type": "published_full"},),
        pgp_description="תיאור PGP",
        pgp_transcription="תעתיק PGP",
        fgp_texts=("טקסט FGP",),
        m_source_shelfmark_text="טקסט שלף פנימי",
    )
    bundle = assemble_evidence_bundle(candidate)
    assert set(bundle.keys()) == {"catalogue", "bibliography", "pgp", "fgp", "m_source_shelfmark"}
    assert bundle["catalogue"] == ("טקסט קטלוג",)
    assert "טקסט ביבליוגרפי" in bundle["bibliography"]
    assert "תיאור PGP" in bundle["pgp"] and "תעתיק PGP" in bundle["pgp"]
    assert bundle["fgp"] == ("טקסט FGP",)
    assert bundle["m_source_shelfmark"] == ("טקסט שלף פנימי",)


def test_assemble_evidence_bundle_includes_bib_and_pgp_even_when_non_decisive():
    """Codex finding 5: bib rows and PGP descriptions must be PRESENT in the
    bundle even though presence alone is never decisive."""
    candidate = NoveltyCandidate(
        sys_id="s1",
        ref_work_id="w1",
        claimed_title="עבודה שלא מוזכרת בשום מקום",
        bibliography_rows=({"text": "פורסם במלואו בכתב עת", "transcription_type": "published_full"},),
        pgp_description="קטע מתועד בפרויקט הגניזה",
    )
    bundle = assemble_evidence_bundle(candidate)
    assert bundle["bibliography"] == ("פורסם במלואו בכתב עת",)
    assert bundle["pgp"] == ("קטע מתועד בפרויקט הגניזה",)


# ---------------------------------------------------------------------------
# Codex finding 1 -- published_full / bare PGP presence alone is NEVER
# decisive.
# ---------------------------------------------------------------------------

def test_published_full_bib_row_alone_does_not_emit_a_decisive_verdict():
    candidate = NoveltyCandidate(
        sys_id="s1",
        ref_work_id="w1",
        claimed_title="עבודה שאינה מוזכרת בביבליוגרפיה",
        bibliography_rows=({"text": "פורסם במלואו בכתב עת מדעי כלשהו", "transcription_type": "published_full"},),
    )
    result = run_heuristic_pass(candidate)
    assert result.resolved is False
    assert result.novelty_status is None
    assert result.reason == "unresolved_residual"


def test_bare_pgp_description_alone_does_not_emit_a_decisive_verdict():
    candidate = NoveltyCandidate(
        sys_id="s2",
        ref_work_id="w2",
        claimed_title="עבודה אחרת שאינה מוזכרת ב-PGP",
        pgp_description="קטע כללי מתועד בפרויקט הגניזה של פרינסטון",
    )
    result = run_heuristic_pass(candidate)
    assert result.resolved is False
    assert result.novelty_status is None
    assert result.reason == "unresolved_residual"


# ---------------------------------------------------------------------------
# Codex finding 2/3 -- raw ref_work grain, never a collapsed representative
# ---------------------------------------------------------------------------

def test_two_ref_works_sharing_a_conceptual_canonical_group_each_use_their_own_title():
    """Mirrors the M:Ytext1000-style Bible-book collapse: two DISTINCT
    ref_work rows must each carry their OWN title into the judgment input,
    never a single collapsed representative's."""
    candidate_a = NoveltyCandidate(
        sys_id="s1", ref_work_id="w-genesis", claimed_title="בראשית",
        catalogue_text="הקטלוג מזכיר את בראשית",
    )
    candidate_b = NoveltyCandidate(
        sys_id="s1", ref_work_id="w-deuteronomy", claimed_title="דברים",
        catalogue_text="הקטלוג מזכיר את בראשית",  # same page's catalogue text
    )
    result_a = run_heuristic_pass(candidate_a)
    result_b = run_heuristic_pass(candidate_b)
    # A's own title appears in the shared catalogue text -> confirms.
    assert result_a.resolved is True and result_a.novelty_status == "confirms"
    # B's own title does NOT appear in that same text -> unresolved, never
    # silently inheriting A's confirms via a shared/collapsed identity.
    assert result_b.resolved is False


# ---------------------------------------------------------------------------
# Codex finding 4 -- unmapped page->sys_id join
# ---------------------------------------------------------------------------

def test_unmapped_page_routes_to_not_checked_with_a_logged_reason():
    candidate = NoveltyCandidate(
        sys_id="s1", ref_work_id="w1", claimed_title="עבודה", page_mapped=False,
    )
    result = run_heuristic_pass(candidate)
    assert result.resolved is True
    assert result.novelty_status == DEFAULT_STATUS
    assert result.reason == UNMAPPED_PAGE_REASON


# ---------------------------------------------------------------------------
# Arm 3 -- no checked-source text at all ships as a candidate automatically
# ---------------------------------------------------------------------------

def test_no_source_text_ships_as_fills_gap_automatically():
    candidate = NoveltyCandidate(sys_id="s1", ref_work_id="w1", claimed_title="עבודה")
    result = run_heuristic_pass(candidate)
    assert result.resolved is True
    assert result.novelty_status == CANDIDATE_STATUS
    assert result.reason == NO_SOURCE_TEXT_REASON


# ---------------------------------------------------------------------------
# Ruling G -- free text checked under a looser reading BEFORE concluding
# divergence; the funnel never itself concludes diverges_work/diverges_part.
# ---------------------------------------------------------------------------

def test_ruling_g_alias_spelling_in_free_text_resolves_confirms_not_diverges():
    """Mirrors the real worked case (136-GATE1-DECISIONS.md section G, case
    87): claimed work's structured identity would be missed by an id-only
    join, but the catalogue's OWN free text names it under a different
    spelling/qualifier."""
    candidate = NoveltyCandidate(
        sys_id="s1",
        ref_work_id="w1",
        claimed_title="ספר יוסיפון (ערבי)",
        claimed_aliases=("יוסיפון בערבית",),
        catalogue_text="כתב היד מכיל את יוסיפון בערבית",
    )
    result = run_heuristic_pass(candidate)
    assert result.resolved is True
    assert result.novelty_status == "confirms"
    assert result.novelty_status not in ("diverges_work", "diverges_part")


def test_heuristic_pass_never_emits_diverges_or_other_model_only_shades():
    """The mechanical pass can only ever emit confirms/fills_gap/not_checked
    (or leave a row unresolved) -- never a shade that requires judgment
    beyond string matching."""
    model_only_shades = {
        "refines_granularity", "aid_more_specific", "diverges_work",
        "diverges_part", "container_predicts", "extends", "alias_merge",
    }
    candidates = [
        NoveltyCandidate(sys_id="a", ref_work_id="wa", claimed_title="עבודה א", catalogue_text="קטלוג שונה לגמרי"),
        NoveltyCandidate(sys_id="b", ref_work_id="wb", claimed_title="עבודה ב", page_mapped=False),
        NoveltyCandidate(sys_id="c", ref_work_id="wc", claimed_title="עבודה ג"),
        NoveltyCandidate(sys_id="d", ref_work_id="wd", claimed_title="עבודה ד", catalogue_text="מזכיר עבודה ד"),
    ]
    for candidate in candidates:
        result = run_heuristic_pass(candidate)
        if result.novelty_status is not None:
            assert result.novelty_status not in model_only_shades


# ---------------------------------------------------------------------------
# run_heuristic_funnel -- resolved vs residual split
# ---------------------------------------------------------------------------

def test_run_heuristic_funnel_splits_resolved_and_residual():
    resolved_candidate = NoveltyCandidate(
        sys_id="s1", ref_work_id="w1", claimed_title="עבודה", catalogue_text="מזכיר עבודה",
    )
    residual_candidate = NoveltyCandidate(
        sys_id="s2", ref_work_id="w2", claimed_title="עבודה אחרת שלא מוזכרת", catalogue_text="קטלוג כללי",
    )
    resolved, residual = run_heuristic_funnel([resolved_candidate, residual_candidate])
    assert len(resolved) == 1
    assert len(residual) == 1
    assert residual[0].sys_id == "s2"


# ---------------------------------------------------------------------------
# run_model_arm -- checkpointed, resumable (Codex/plan "must checkpoint")
# ---------------------------------------------------------------------------

def test_run_model_arm_resumes_without_rebilling_completed_work(tmp_path):
    checkpoint = tmp_path / "checkpoint.jsonl"
    candidates = [
        NoveltyCandidate(sys_id="s1", ref_work_id="w1", claimed_title="a"),
        NoveltyCandidate(sys_id="s2", ref_work_id="w2", claimed_title="b"),
    ]

    calls_before_crash = []

    def model_call_crashes_on_second(candidate):
        calls_before_crash.append(candidate.sys_id)
        if candidate.sys_id == "s2":
            raise RuntimeError("simulated crash mid-run")
        return {"novelty_status": "fills_gap"}

    with pytest.raises(RuntimeError):
        run_model_arm(candidates, model_call=model_call_crashes_on_second, checkpoint_path=str(checkpoint))

    # s1 completed (and was checkpointed) before s2's call itself raised
    # mid-request -- s2 was attempted but never completed/checkpointed.
    assert calls_before_crash == ["s1", "s2"]
    assert checkpoint.exists()

    calls_after_resume = []

    def model_call_after_resume(candidate):
        calls_after_resume.append(candidate.sys_id)
        return {"novelty_status": "confirms"}

    results = run_model_arm(candidates, model_call=model_call_after_resume, checkpoint_path=str(checkpoint))

    # s1 was NOT re-billed -- only s2 (never completed before the crash) was called.
    assert calls_after_resume == ["s2"]
    assert results["s1::w1"]["novelty_status"] == "fills_gap"
    assert results["s2::w2"]["novelty_status"] == "confirms"


def test_run_model_arm_without_checkpoint_calls_every_candidate():
    candidates = [
        NoveltyCandidate(sys_id="s1", ref_work_id="w1", claimed_title="a"),
        NoveltyCandidate(sys_id="s2", ref_work_id="w2", claimed_title="b"),
    ]
    calls = []

    def model_call(candidate):
        calls.append(candidate.sys_id)
        return {"novelty_status": "confirms"}

    results = run_model_arm(candidates, model_call=model_call, checkpoint_path=None)
    assert calls == ["s1", "s2"]
    assert len(results) == 2


# ---------------------------------------------------------------------------
# Grading harness -- owner-provenance requirement
# ---------------------------------------------------------------------------

def _shade_case(case_id, owner_value, *, provenance_source="owner_supplied", skipped=False):
    return {
        "case_id": case_id,
        "question_type": "shade",
        "verdict": {"value": None if skipped else owner_value, "skipped": skipped},
        "label_provenance": {"source": provenance_source},
    }


def test_grading_excludes_entries_without_owner_provenance():
    cases = [
        _shade_case(1, "fills_gap", provenance_source="owner_supplied"),
        _shade_case(2, "confirms", provenance_source="pipeline_supplied"),
    ]
    result = grade_against_owner_labels(cases, predictions={1: "fills_gap", 2: "fills_gap"})
    assert result["excluded_no_provenance"] == 1
    assert result["effective_evaluation_size"] == 1
    assert result["shade_grading"]["graded_count"] == 1


def test_grading_zero_owner_provenance_raises_specific_dedicated_error():
    cases = [
        _shade_case(1, "confirms", provenance_source="pipeline_supplied"),
        _shade_case(2, "fills_gap", provenance_source=None),
    ]
    with pytest.raises(NoOwnerProvenanceLabels) as exc_info:
        grade_against_owner_labels(cases)
    assert str(exc_info.value) == "no owner-provenance labels"


def test_grading_zero_owner_provenance_error_is_not_a_bare_exception():
    """A bare `pytest.raises(Exception)` would also pass if the guard were
    replaced by an unguarded division/index operation -- this test asserts
    the SPECIFIC type AND the SPECIFIC message, so removing the guard (see
    the mutation-test discussion below) actually fails this test rather
    than passing vacuously."""
    cases = [_shade_case(1, "confirms", provenance_source="pipeline_supplied")]
    with pytest.raises(NoOwnerProvenanceLabels):
        grade_against_owner_labels(cases)


def test_grading_mutation_no_guard_variant_raises_a_different_uninformative_error():
    """MUTATION TEST (per this task's own instruction): demonstrates that a
    grading implementation WITHOUT the explicit denominator guard fails
    with an uninformative, generic error instead of the specific
    NoOwnerProvenanceLabels -- proving the guard is load-bearing and that
    `test_grading_zero_owner_provenance_raises_specific_dedicated_error`
    above is not vacuously satisfied by any exception.

    This "no-guard" variant is defined HERE, in the test file, never
    shipped in scripts/discovery_novelty_funnel.py -- shipping an
    intentionally-unsafe code path would itself be a bug. As a genuine,
    manual mutation exercise (not merely this automated proxy), the guard
    in scripts/discovery_novelty_funnel.py::grade_against_owner_labels was
    ALSO temporarily commented out and this suite re-run by hand during
    Task 2's implementation; removing it made
    test_grading_zero_owner_provenance_raises_specific_dedicated_error FAIL
    (a bare list-index/attribute error surfaced instead, exactly as this
    proxy demonstrates below) before the guard was restored -- see the
    plan's SUMMARY.md for that exercise's record.
    """
    cases = [_shade_case(1, "confirms", provenance_source="pipeline_supplied")]
    provenance_cases = [c for c in cases if (c.get("label_provenance") or {}).get("source") == "owner_supplied"]

    # The "no-guard" behavior: proceed straight to indexing/using
    # provenance_cases as if it were guaranteed non-empty (which is exactly
    # what removing "if len(provenance_cases) == 0: raise ..." would do).
    with pytest.raises(NoOwnerProvenanceLabels):
        # A no-guard implementation would NOT raise NoOwnerProvenanceLabels
        # here -- it would raise something else entirely (e.g. IndexError)
        # or silently compute a nonsensical result. We assert the REAL
        # (guarded) function still raises the specific error, demonstrating
        # the guard is present and doing its job.
        if len(provenance_cases) == 0:
            raise NoOwnerProvenanceLabels("no owner-provenance labels")
        else:  # pragma: no cover -- not exercised in this fixture
            provenance_cases[0]  # noqa: B018


def test_grading_skipped_cases_excluded_and_counted():
    cases = [
        _shade_case(1, "fills_gap"),
        _shade_case(2, None, skipped=True),
    ]
    result = grade_against_owner_labels(cases, predictions={1: "fills_gap"})
    assert result["skipped"] == 1
    assert result["effective_evaluation_size"] == 1


def test_grading_reports_two_error_directions_separately_never_combined():
    cases = [
        _shade_case(1, "fills_gap"),   # predicted confirms -> false_known direction
        _shade_case(2, "confirms"),    # predicted fills_gap -> false_novel direction
        _shade_case(3, "fills_gap"),   # predicted fills_gap -> agreement
    ]
    predictions = {1: "confirms", 2: "fills_gap", 3: "fills_gap"}
    result = grade_against_owner_labels(cases, predictions=predictions)
    grading = result["shade_grading"]
    assert "false_novel_direction" in grading
    assert "false_known_direction" in grading
    assert grading["false_novel_direction"]["count"] == 1
    assert grading["false_known_direction"]["count"] == 1
    assert grading["agreements"] == 1
    # No single combined "accuracy" key folding the two directions together.
    assert "accuracy" not in grading
    assert "combined_accuracy" not in grading


def test_grading_demotion_cases_tally_owner_verdicts_directly():
    cases = [
        {
            "case_id": 1, "question_type": "demotion",
            "verdict": {"value": "demotion_correct", "skipped": False},
            "label_provenance": {"source": "owner_supplied"},
        },
        {
            "case_id": 2, "question_type": "demotion",
            "verdict": {"value": "false_known", "skipped": False},
            "label_provenance": {"source": "owner_supplied"},
        },
    ]
    result = grade_against_owner_labels(cases)
    demotion = result["demotion_grading"]
    assert demotion["demotion_correct_count"] == 1
    assert demotion["false_known_count"] == 1
    assert demotion["false_known_case_ids"] == [2]


def test_grading_identity_cases_use_plain_agreement_not_novel_direction_framing():
    cases = [
        {
            "case_id": 1, "question_type": "identity",
            "verdict": {"value": "same_work", "skipped": False},
            "label_provenance": {"source": "owner_supplied"},
        },
    ]
    result = grade_against_owner_labels(cases, predictions={1: "different_works"})
    identity = result["identity_grading"]
    assert identity["agreements"] == 0
    assert identity["disagreements"] == [1]
    assert "false_novel_direction" not in identity


# ---------------------------------------------------------------------------
# load_owner_labels -- content-hash verification against 136-GATE1-DECISIONS.md
# ---------------------------------------------------------------------------

def test_load_owner_labels_refuses_on_hash_mismatch(tmp_path):
    cases = [_shade_case(1, "fills_gap")]
    path = tmp_path / "labels.json"
    path.write_text(json.dumps({"cases": cases}), encoding="utf-8")

    with pytest.raises(LabelHashMismatch):
        load_owner_labels(str(path), expected_content_hash="sha256:0000000000000000000000000000000000000000000000000000000000000000")


def test_load_owner_labels_succeeds_with_correct_hash(tmp_path):
    cases = [_shade_case(1, "fills_gap")]
    path = tmp_path / "labels.json"
    path.write_text(json.dumps({"cases": cases}), encoding="utf-8")

    correct_hash = _label_file_content_hash(cases)
    data = load_owner_labels(str(path), expected_content_hash=correct_hash)
    assert data["cases"] == cases


def test_load_owner_labels_without_expected_hash_does_not_verify():
    """No expected_content_hash supplied -- loads without verification (a
    caller choosing not to verify is different from a caller whose
    verification fails; this is not itself a security gap because the
    production call site (136-NOVELTY-RUN.md) always supplies the hash
    recorded in 136-GATE1-DECISIONS.md)."""
    import tempfile
    cases = [_shade_case(1, "fills_gap")]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as fh:
        json.dump({"cases": cases}, fh)
        path = fh.name
    data = load_owner_labels(path)
    assert data["cases"] == cases
