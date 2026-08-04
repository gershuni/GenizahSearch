# -*- coding: utf-8 -*-
"""The connections panel, proved honest across THREE egress classes
(Phase 136, plan 136-17, Task 3).

Markup is not the only egress. `shared/discovery_surface_projection.py::
_assert_surface_safe` validates forbidden KEY NAMES (plus two badge strings and
three rendered rate SHAPES), not arbitrary VALUES under innocuous keys against a
vocabulary -- so a stored novelty verdict under `band_label`, or an accuracy
claim in `meta['reason']`, reaches a JSON payload and a log line untouched by
any renderer assertion. This suite therefore scans:

  1. the RENDERED markup, element-scoped, in both languages, over seven
     manuscripts x four service states;
  2. the EXACT envelopes the panel consumes, recursively, every string VALUE and
     every numeric one;
  3. the FORCED error paths -- exception messages and log lines.

And it guards the CLASSIFICATION those scans stand on, as an exact set EQUALITY
against a ground truth recomputed from `_ALL_ALLOWLISTS`. Four earlier revisions
of that mechanism proved a global property by SAMPLING a corpus with an escape
list beside it, and each time the enumeration was short in a new place. The
equality is the mechanism; the value rules are controls on it.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
import web.components.discovery_panel as dp
from shared.discovery_band_labels import band_label
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_main_pool import MAIN_POOL_REASONS, bucket_label
from shared.discovery_novelty import NOVELTY_STATUSES
from shared.discovery_panel_model import (
    LIVE_OK_META_SHAPES,
    PanelServiceBundle,
    build_panel_rows,
    iter_rows,
)
from shared.discovery_service import LAUNCH_CONTRIBUTION_SHADES
from shared.discovery_surface_projection import (
    _ALL_ALLOWLISTS,
    STATUS_BUSY,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_UNAVAILABLE,
    busy_envelope,
    make_envelope,
    surface_safe_claim,
    surface_safe_expansion,
    surface_safe_launch_shade,
    surface_safe_related_page,
    surface_safe_work_summary,
    timeout_envelope,
    unavailable_envelope,
)
from tests.render_smoke.discovery_honesty_gate import (
    ALLOWLIST_FIELD_UNION,
    COVERAGE_STATUSES,
    D06A_QUALITATIVE_SCOPES,
    DiscoveryHonestyViolation,
    ELIGIBILITY_BASES,
    KNOWN_CARRIER_FLOOR,
    MACHINE_VOCABULARY_FIELDS,
    META_FREE_TEXT_KEYS,
    META_VOCABULARY_FIELDS,
    READER_TEXT_FIELDS,
    REGISTRY_MATCH_EXCLUSIONS,
    _PROHIBITED_RAW_VOCAB_KEYS,
    assert_envelope_honesty,
    assert_error_path_honesty,
    assert_surface_honesty,
    find_envelope_violations,
    value_rule_flags,
)

GATE_PATH = 'tests/render_smoke/discovery_honesty_gate.py'
HELP_PATH = 'web/pages/help.py'
BUILDER_PATH = 'scripts/build_discovery_sidecar.py'
SERVICE_PATH = 'shared/discovery_service.py'

#: A HARDCODED per-language SHA-256 digest of the limitations paragraph's
#: NORMALISED rendered text, computed once from the CURRENTLY SHIPPED wording.
#:
#: An INDEPENDENT pin, and independence is the whole point (round 11, finding 4):
#: `tests/render_smoke/test_help_methods_render_smoke.py` asserts a handful of
#: SUBSTRINGS, so a wording edit preserving them passes it unchanged; and
#: `FP-D06A-LIVE-PAGE`'s equality compares the render against the same mutable
#: `_LIMITATIONS_TEXT` authority such an edit would move. A literal digest is
#: something no authority can move with it.
#:
#: THIS IS A PIN, NEVER A LICENCE TO EDIT. The sentence is owner-approved D-06a
#: text; a revision of it is out of scope and contradicts D-06a.
LIMITATIONS_TEXT_SHA256 = {
    'en': 'c209693ccdcbcc9b7548a091cdf3d22c7078591014cdfcc9424bbaa9302aef3a',
    'he': '43144bc0cfd79abb080bcddb4219202c39c267402e159a6aa42f5e6f20694487',
}

_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation
        prepare_simulation()
        _SIM_READY = True


def _read(path: str) -> str:
    return io.open(path, encoding='utf-8').read()


def _normalise(text: str) -> str:
    return ' '.join((text or '').split())


# ===========================================================================
# THE VALUE CORPUS.
#
# (a) Every ROW enters through a `surface_safe_*` PROJECTION, never as a
#     hand-written dict -- which is what makes ground truth 3's key-set equality
#     hold by construction for correct fixtures and fail loudly for a dict that
#     bypassed the projection.
# (b) Every `meta` enters through REAL envelope CONSTRUCTION -- the outage
#     helpers and the shipped `make_envelope` -- never as a hand-written dict.
# ===========================================================================

_MASKED_SOURCE_LABEL = 'recorded in the catalogue'


def _claim_source(**overrides) -> Dict[str, Any]:
    """A claim row with EVERY allowlisted field NON-NULL.

    Every machine-classified value is a MEMBER of that field's mapped
    vocabulary, because assertion (h) requires exactly that and a fixture that
    could not satisfy it would be a fixture proving nothing.
    """
    row = {
        'page_id': '990000000000000944_IE1_P000002_FL3',
        'sys_id': '990000000000000944',
        'claim_id': 'a' * 64,
        'evidence_id': 'b' * 64,
        'work_id': 'w000001',
        'canonical_work_id': 'w000001',
        'display_work_id': 'w000001',
        'neutral_title': 'Commentary on Song of Songs',
        'author': 'Rashi',
        'genre': 'Responsa and Halakhic Decisions / Responsa- Gaonim',
        'title_missing': False,
        'relation_kind': ids.CLAIM_TYPE_DIRECT_WITNESS,
        'evidence_source': ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        'confidence_band': ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
        'band_label': band_label(ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
                                 ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC),
        'band_rank': 0,
        'coverage_ppm': 680000,
        'coverage_status': 'measured',
        'main_pool': True,
        'main_pool_reason': 'main_full_coverage',
        'identification_id': 'c' * 64,
        'identification_page_count': 3,
        'novelty_status': 'fills_gap',
        'novelty_source_label': _MASKED_SOURCE_LABEL,
        'matched_letters': 500,
        'span_start': 0,
        'span_end': 500,
        'n_spans': 1,
        'eligibility_basis': 'shipped',
        'restored_by_human_confirmation': False,
        'low_coverage_marker': False,
        'adjudication_status': ids.ADJUDICATION_STATUS_UNREVIEWED,
        'routing_status': ids.ROUTING_STATUS_SHIPPED,
        'routing_reason': 'none',
        'measurement_status': 'measured_pass',
        'default_eligible': True,
    }
    row.update(overrides)
    return row


def _work_summary_source(**overrides) -> Dict[str, Any]:
    row = {
        'canonical_work_id': 'w000001',
        'display_work_id': 'w000001',
        'neutral_title': 'Commentary on Song of Songs',
        'author': 'Rashi',
        'genre': 'Bible commentary',
        'title_missing': False,
        'page_count': 5,
        'best_band_rank': 0,
        'gated': False,
        'main_pool': True,
        'relation_kind': ids.CLAIM_TYPE_DIRECT_WITNESS,
    }
    row.update(overrides)
    return row


def _related_page_source(**overrides) -> Dict[str, Any]:
    row = {
        'related_page_id': '990000000000000945_IE1_P000001_FL9',
        'evidence_id': 'd' * 64,
        'evidence_source': ids.EVIDENCE_SOURCE_PROPAGATED,
        'confidence_band': ids.CONFIDENCE_BAND_NOT_EVALUATED,
        'band_rank': 6,
        'evidence_row_count': 3,
    }
    row.update(overrides)
    return row


def _expansion_source(**overrides) -> Dict[str, Any]:
    row = {
        'work_id': 'w000001',
        'unit_id': 'unit-2',
        'representative_sys_id': '990000000000000946',
        'representative_page_id': '990000000000000946_IE1_P000001_FL1',
        'representative_claim_id': 'e' * 64,
        'member_sys_ids': ['990000000000000946'],
        'library_code': 'CUL',
        'shelfmark_display': 'T-S 12.123',
        'display_missing': False,
        'claim_type': ids.CLAIM_TYPE_SHARED_TEXT,
        'anchor_claim_type': ids.CLAIM_TYPE_DIRECT_WITNESS,
        'relations_differ': True,
        'displayed_evidence_source': ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        'displayed_confidence_band': ids.CONFIDENCE_BAND_SCREENING_RB,
        'band_label': band_label(ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
                                 ids.CONFIDENCE_BAND_SCREENING_RB),
        'band_rank': 4,
    }
    row.update(overrides)
    return row


def _launch_shade_source(shade: str) -> Dict[str, Any]:
    return {'shade': shade, 'identification_count': 12, 'manuscript_count': 7}


def corpus_rows() -> List[Tuple[str, Dict[str, Any]]]:
    """`(allowlist name, PROJECTED row)`. Every row goes through the live
    `surface_safe_*` function, so its key set equals its allowlist by
    construction and a hand-written dict fails ground truth 3 loudly."""
    rows: List[Tuple[str, Dict[str, Any]]] = []
    rows.append(('SURFACE_CLAIM_FIELDS', surface_safe_claim(_claim_source())))
    # A second claim row so every field's ALTERNATIVE values are exercised, and
    # so `routing_reason` / `coverage_status` / `eligibility_basis` are observed
    # with more than one member each.
    rows.append(('SURFACE_CLAIM_FIELDS', surface_safe_claim(_claim_source(
        relation_kind=ids.CLAIM_TYPE_SHARED_TEXT,
        evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED,
        confidence_band=ids.CONFIDENCE_BAND_CORROBORATED,
        band_label=band_label(ids.EVIDENCE_SOURCE_PROPAGATED,
                              ids.CONFIDENCE_BAND_CORROBORATED),
        coverage_status='not_applicable',
        eligibility_basis='human_confirmed',
        routing_reason='low_coverage',
        adjudication_status=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
        routing_status=ids.ROUTING_STATUS_REVIEW_ONLY,
        restored_by_human_confirmation=True,
        low_coverage_marker=True,
        main_pool=False,
        main_pool_reason='low_coverage',
        novelty_status='not_checked',
        measurement_status='not_measured',
        title_missing=True,
        default_eligible=False,
    ))))
    rows.append(('SURFACE_WORK_SUMMARY_FIELDS',
                 surface_safe_work_summary(_work_summary_source())))
    rows.append(('SURFACE_WORK_SUMMARY_FIELDS',
                 surface_safe_work_summary(_work_summary_source(gated=True, main_pool=False))))
    rows.append(('SURFACE_RELATED_PAGE_FIELDS',
                 surface_safe_related_page(_related_page_source())))
    rows.append(('SURFACE_EXPANSION_FIELDS',
                 surface_safe_expansion(_expansion_source())))
    rows.append(('SURFACE_EXPANSION_FIELDS',
                 surface_safe_expansion(_expansion_source(
                     display_missing=True, relations_differ=False,
                     claim_type=ids.CLAIM_TYPE_DIRECT_WITNESS,
                     displayed_evidence_source=ids.EVIDENCE_SOURCE_PROPAGATED,
                     displayed_confidence_band=ids.CONFIDENCE_BAND_WEAK,
                     band_label=band_label(ids.EVIDENCE_SOURCE_PROPAGATED,
                                           ids.CONFIDENCE_BAND_WEAK)))))
    # SEEDED: one launch-shade row per shade, so `SURFACE_LAUNCH_SHADE_FIELDS`
    # is a CONSUMED allowlist and 136-22's `shade` is exercised through the real
    # controls rather than merely declared (round 10's remedy, both halves).
    for shade in LAUNCH_CONTRIBUTION_SHADES:
        rows.append(('SURFACE_LAUNCH_SHADE_FIELDS',
                     surface_safe_launch_shade(_launch_shade_source(shade))))
    return rows


def corpus_metas() -> List[Tuple[str, Mapping[str, Any]]]:
    """`(where, meta)` for every status branch this surface can produce, each
    built by REAL envelope construction -- the shipped outage helpers and
    `make_envelope` -- never as a hand-written dict."""
    metas: List[Tuple[str, Mapping[str, Any]]] = []
    for name, envelope in (
        ('unavailable', unavailable_envelope(meta={'reason': 'sidecar_not_serving'})),
        ('unavailable/query_failed', unavailable_envelope(meta={'reason': 'query_failed'})),
        ('timeout', timeout_envelope(meta={'reason': 'query_timeout'})),
        ('busy', busy_envelope(meta={'reason': 'bounded_concurrency'})),
        ('claims', make_envelope(STATUS_OK, [], 0, meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False})),
        ('page_ids', make_envelope(STATUS_OK, [], 0, meta={
            'sys_id': '990000000000000944', 'resolved': True, 'truncated': False,
            'volume_ie': 'IE1'})),
        ('manuscript_works', make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': True, 'lang': 'en'})),
        ('manuscript_works/unresolved', make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': False})),
        ('related_count', make_envelope(STATUS_OK, [], 0, meta={
            'unit': 'distinct_opposite_pages'})),
        ('expansion', make_envelope(STATUS_OK, [], 0, meta={
            'work_id': 'w000001', 'anchor_mode': 'anchored',
            'filter_basis': 'displayed_band', 'anchor_excluded': True})),
    ):
        metas.append((name, envelope['meta']))
    return metas


CONSUMED_ALLOWLISTS: frozenset = frozenset(name for name, _row in corpus_rows())
_ALLOWLIST_BY_NAME: Dict[str, Tuple[str, ...]] = dict(_ALL_ALLOWLISTS)


# ===========================================================================
# (a) THE PARTITION -- an exact set EQUALITY, both directions.
# ===========================================================================

def test_a_the_classification_partitions_the_allowlist_union_exactly():
    machine = set(MACHINE_VOCABULARY_FIELDS)
    reader = set(READER_TEXT_FIELDS)
    recomputed = frozenset(
        field for _name, fields in _ALL_ALLOWLISTS for field in fields)
    assert recomputed == ALLOWLIST_FIELD_UNION, (
        'ALLOWLIST_FIELD_UNION drifted from _ALL_ALLOWLISTS')
    assert machine & reader == set(), (
        f'classified twice: {sorted(machine & reader)}')
    assert machine | reader == recomputed, (
        'the classification is not an exact partition of the allowlist union.\n'
        f'  UNCLASSIFIED (a carrier nobody named): {sorted(recomputed - machine - reader)}\n'
        f'  DEAD ENTRIES (no longer allowlisted): {sorted((machine | reader) - recomputed)}'
    )


def test_a_control_a_short_classification_fails_by_name():
    """Positive control for (a): dropping ONE field breaks the equality and the
    message names it. Without this the equality could be vacuous."""
    machine = set(MACHINE_VOCABULARY_FIELDS)
    reader = set(READER_TEXT_FIELDS) - {'band_label'}
    assert machine | reader != ALLOWLIST_FIELD_UNION
    assert sorted(ALLOWLIST_FIELD_UNION - machine - reader) == ['band_label']


# ===========================================================================
# (b) THE READER-FACING FLOOR -- the mapping can never whitelist a rendered field.
# ===========================================================================

_READER_FACING_FLOOR = (
    'band_label', 'neutral_title', 'author', 'shelfmark_display',
    'library_code', 'novelty_source_label', 'label',
)

_REASON_KINDS = ('reader text', 'identity', 'numeric', 'boolean', 'id-list')


def test_b_the_reader_facing_floor_is_never_machine_classified():
    for field in _READER_FACING_FLOOR:
        assert field in READER_TEXT_FIELDS, f'{field} is not on the reader-text side'
        assert field not in MACHINE_VOCABULARY_FIELDS, (
            f'{field} was declared a machine carrier -- the mapping can never be '
            'used to whitelist a rendered field')


def test_b_every_reader_text_reason_names_a_kind_and_a_producer():
    """The ONE documented residual -- a single-word vocabulary exported nowhere
    and declared reader-text -- is bounded only documentarily, so the reason is
    required to name the code site, table or function that PRODUCES the values."""
    for field, reason in READER_TEXT_FIELDS.items():
        assert any(reason.startswith(kind) for kind in _REASON_KINDS), (
            f'{field}: the reason does not name a KIND (one of {_REASON_KINDS}): {reason!r}')
        body = reason.split(':', 1)[1]
        # A producer is a dotted or underscored code/table/file name: a bare
        # sentence of prose is not one.
        assert re.search(r'\w+\.\w+|\w+_\w+|COUNT|MIN|MAX|CASE', body), (
            f'{field}: the reason does not name a PRODUCER: {reason!r}')


# ===========================================================================
# (c) DECLARING A CARRIER PROHIBITS ITS VOCABULARY.
# ===========================================================================

def test_c_every_mapped_vocabulary_member_with_an_underscore_is_prohibited():
    for field, vocabulary in MACHINE_VOCABULARY_FIELDS.items():
        for member in vocabulary:
            if '_' in member:
                assert member in _PROHIBITED_RAW_VOCAB_KEYS, (
                    f'{field} carries {member!r}, which is prohibited nowhere -- '
                    'exactly how novelty_status and main_pool_reason came to be '
                    'exempt while their values were prohibited on no surface')


def test_c_the_two_vocabularies_gap_a_added_are_in_the_prohibited_set():
    """The broken implementation this catches is the LIVE pre-136-17 one:
    `direct_witness` in `band_label` failed while `fills_gap` in the SAME field
    passed, so a surface could echo a stored novelty verdict raw and the gate
    reported green."""
    assert 'fills_gap' in _PROHIBITED_RAW_VOCAB_KEYS
    assert 'main_full_coverage' in _PROHIBITED_RAW_VOCAB_KEYS
    for member in NOVELTY_STATUSES:
        if '_' in member:
            assert member in _PROHIBITED_RAW_VOCAB_KEYS, member
    for member in MAIN_POOL_REASONS:
        if '_' in member:
            assert member in _PROHIBITED_RAW_VOCAB_KEYS, member


# ===========================================================================
# (d) THE MISCLASSIFICATION CONTROL -- the two value rules, over the corpus.
# ===========================================================================

def test_d_no_reader_text_row_field_carries_a_value_either_rule_flags():
    problems = []
    for allowlist, row in corpus_rows():
        for field, value in row.items():
            if field not in READER_TEXT_FIELDS:
                continue
            for flag in value_rule_flags(field, value):
                problems.append(f'{allowlist}.{field} = {value!r} flagged by {flag}')
    assert problems == [], (
        'a real machine carrier is parked on the reader-text side:\n  '
        + '\n  '.join(problems)
        + '\nAvailable responses: move the field to MACHINE_VOCABULARY_FIELDS '
          'mapped to a vocabulary containing that value (which prohibits that '
          'vocabulary everywhere else), fix the leak, or -- for a rule (2) flag '
          'only -- record the (field, value) pair in REGISTRY_MATCH_EXCLUSIONS '
          'with a reason.')


def test_d_the_misclassification_control_can_fail():
    """Positive control: a genuine carrier value under a reader-text field is
    flagged, and BOTH rules are shown able to fire."""
    assert value_rule_flags('band_label', 'direct_witness'), 'rule (1) is inert'
    assert value_rule_flags('band_label', 'shipped'), 'rule (2) is inert'
    assert value_rule_flags('band_label', 'the work extends over three folios') == [], (
        'rule (2) is matching a SUBSTRING -- honest reader prose would fail')


def test_d_the_registry_exclusion_list_is_short_and_reasoned():
    assert len(REGISTRY_MATCH_EXCLUSIONS) <= 3, REGISTRY_MATCH_EXCLUSIONS
    for entry in REGISTRY_MATCH_EXCLUSIONS:
        field, value, reason = entry
        assert isinstance(reason, str) and len(reason) > 20, entry


# ===========================================================================
# (e) THE `meta` PARTITION -- by KEY, REGARDLESS OF VALUE SHAPE.
# ===========================================================================

def test_e_every_string_valued_meta_key_is_classified_exactly_once():
    overlap = set(META_VOCABULARY_FIELDS) & set(META_FREE_TEXT_KEYS)
    assert overlap == set(), f'classified twice: {sorted(overlap)}'
    for where, meta in corpus_metas():
        for key, value in meta.items():
            if not isinstance(value, str):
                continue
            classified = (key in META_VOCABULARY_FIELDS) + (key in META_FREE_TEXT_KEYS)
            assert classified == 1, (
                f'{where}: meta key {key!r} is classified {classified} times. '
                'A key carrying a one-word enum exported nowhere is invisible to '
                'BOTH value rules, which is why the KEY is classified regardless '
                'of value shape.')
            if key in META_VOCABULARY_FIELDS:
                assert value in META_VOCABULARY_FIELDS[key], (
                    f'{where}: meta[{key!r}] = {value!r} is not a member of its '
                    'declared set')


def test_e_every_meta_key_the_LIVE_producers_emit_is_classified():
    """Artifact-independent completeness over the model's own declared live
    shapes, which `tests/test_discovery_panel_model.py` pins to the producing
    functions' SOURCE. So the classification covers what the CODE emits, not
    what this fixture author remembered."""
    live_keys = {k for shapes in LIVE_OK_META_SHAPES.values()
                 for shape in shapes for k in shape}
    live_keys |= {'reason', 'anchor_mode', 'filter_basis', 'work_id', 'anchor_excluded'}
    unclassified = sorted(
        k for k in live_keys
        if k not in META_VOCABULARY_FIELDS and k not in META_FREE_TEXT_KEYS
        and k not in {'include_review', 'resolved', 'truncated',
                      'page_scope_resolved', 'anchor_excluded'})
    assert unclassified == [], (
        f'meta keys the live producers emit but nobody classified: {unclassified}')


def test_e_meta_stays_under_the_strict_reader_facing_scan():
    """Being classified exempts NOTHING: `meta['reason'] = 'direct_witness'`
    must still fail loudly."""
    envelope = make_envelope(STATUS_UNAVAILABLE)
    envelope = {**envelope, 'meta': {'reason': 'direct_witness'}}
    violations = find_envelope_violations(envelope)
    assert any('direct_witness' in v for v in violations), violations


def test_e_no_free_text_meta_key_carries_a_flagged_value():
    problems = []
    for where, meta in corpus_metas():
        for key, value in meta.items():
            if key not in META_FREE_TEXT_KEYS:
                continue
            for flag in value_rule_flags(key, value):
                problems.append(f'{where}: meta[{key!r}] = {value!r} flagged by {flag}')
    assert problems == [], problems


# ===========================================================================
# (f) CORPUS INTEGRITY, and the DERIVED coverage domain.
# ===========================================================================

def test_f_the_projection_is_total_as_this_check_assumes():
    """CONFIRMED against the live code rather than taken from the plan: an
    exact equality standing on an unverified premise is how four rounds began."""
    projected = surface_safe_claim({'page_id': 'x'})
    assert set(projected) == set(_ALLOWLIST_BY_NAME['SURFACE_CLAIM_FIELDS'])
    assert projected['routing_reason'] is None, (
        '_project is not total; ground truth 3 must degrade to the SUBSET form')


def test_f_every_corpus_row_key_set_equals_a_registered_allowlist():
    """A CONSTRUCTION check. What it catches is a hand-written dict that never
    went through a projection at all -- NOT the row that omitted a field:
    `_project` is TOTAL and backfills the missing key with None, so an omitting
    source row still carries the exact key set AFTER projection. Three earlier
    revisions of this plan said otherwise (round 11, finding 2)."""
    for allowlist, row in corpus_rows():
        assert set(row) == set(_ALLOWLIST_BY_NAME[allowlist]), (
            f'{allowlist}: key-set diff '
            f'{sorted(set(row) ^ set(_ALLOWLIST_BY_NAME[allowlist]))}')


def test_f_every_field_of_every_consumed_allowlist_is_non_null_somewhere():
    """The DERIVED coverage check -- and the one that actually catches the
    omit-a-field evasion `NEVER_POPULATED` used to invite, across the corpus
    rather than at a single row. `NEVER_POPULATED` is DELETED; the coverage
    domain is DERIVED from which allowlists the corpus consumes."""
    for allowlist in sorted(CONSUMED_ALLOWLISTS):
        fields = _ALLOWLIST_BY_NAME[allowlist]
        rows = [row for name, row in corpus_rows() if name == allowlist]
        for field in fields:
            assert any(row.get(field) is not None for row in rows), (
                f'{allowlist}.{field} is null in every corpus row -- seed it')


def test_f_the_launch_shade_allowlist_is_consumed_and_shade_is_exercised():
    assert 'SURFACE_LAUNCH_SHADE_FIELDS' in CONSUMED_ALLOWLISTS, (
        'the launch-shade envelope was not seeded, so 136-22\'s `shade` is '
        'declared rather than exercised')
    observed = {row['shade'] for name, row in corpus_rows()
                if name == 'SURFACE_LAUNCH_SHADE_FIELDS'}
    assert observed == set(LAUNCH_CONTRIBUTION_SHADES)


def test_f_the_findings_and_facet_allowlists_are_outside_the_coverage_domain():
    """By DERIVATION, not by anyone's say-so: this surface does not consume
    them, and 136-18 RUNS the same check over its own corpus rather than
    editing a gate file it does not own."""
    assert 'SURFACE_FINDING_FIELDS' not in CONSUMED_ALLOWLISTS
    assert 'SURFACE_FACET_FIELDS' not in CONSUMED_ALLOWLISTS
    # ...but they are still CLASSIFIED (ground truth 1 covers the whole union).
    for field in _ALLOWLIST_BY_NAME['SURFACE_FINDING_FIELDS']:
        assert field in MACHINE_VOCABULARY_FIELDS or field in READER_TEXT_FIELDS


# ===========================================================================
# (g) THE KNOWN FLOOR.
# ===========================================================================

def test_g_the_known_carrier_floor_is_a_subset_of_the_machine_half():
    missing = sorted(KNOWN_CARRIER_FLOOR - set(MACHINE_VOCABULARY_FIELDS))
    assert missing == [], f'floor members left unclassified: {missing}'


def test_g_every_floor_member_is_an_allowlist_field():
    """Four of them -- claim_type, anchor_claim_type, displayed_evidence_source,
    displayed_confidence_band -- arrive with 136-21's SURFACE_EXPANSION_FIELDS,
    and `shade` with 136-22's SURFACE_LAUNCH_SHADE_FIELDS. If one is ever
    missing, establish which namespace it belongs to BEFORE deleting it."""
    missing = sorted(KNOWN_CARRIER_FLOOR - ALLOWLIST_FIELD_UNION)
    assert missing == [], f'floor members outside the allowlist union: {missing}'
    for field in ('claim_type', 'anchor_claim_type', 'displayed_evidence_source',
                  'displayed_confidence_band'):
        assert field in _ALLOWLIST_BY_NAME['SURFACE_EXPANSION_FIELDS'], field
    assert 'shade' in _ALLOWLIST_BY_NAME['SURFACE_LAUNCH_SHADE_FIELDS']


def test_g_the_floor_is_not_asserted_to_appear_in_the_observed_values():
    """A classified field the corpus legitimately never populates is still
    CLASSIFIED, and (h) quantifies over OBSERVATIONS only. Demonstrated rather
    than asserted in prose: drop the launch rows, so `shade` is unobserved, and
    show that (g) and (h) both still hold.

    An assertion that conflated classification with observation would fail here,
    and would fail on any correct corpus that does not consume every allowlist.
    """
    subset = [(name, row) for name, row in corpus_rows()
              if name != 'SURFACE_LAUNCH_SHADE_FIELDS']
    observed = {field for _name, row in subset
                for field, value in row.items() if value is not None}
    assert 'shade' not in observed, 'the subset did not actually drop shade'
    assert KNOWN_CARRIER_FLOOR <= set(MACHINE_VOCABULARY_FIELDS), (
        'the floor is a claim about the CLASSIFICATION, not about the corpus')
    problems = [
        f'{name}.{field}' for name, row in subset for field, value in row.items()
        if field in MACHINE_VOCABULARY_FIELDS and isinstance(value, str) and value
        and value not in MACHINE_VOCABULARY_FIELDS[field]]
    assert problems == [], problems


# ===========================================================================
# (h) THE MACHINE MAPPING IS FAITHFUL.
# ===========================================================================

def test_h_every_mapped_vocabulary_is_non_empty():
    empty = sorted(f for f, v in MACHINE_VOCABULARY_FIELDS.items() if not v)
    assert empty == [], (
        f'{empty} are mapped to an EMPTY vocabulary -- which keeps the partition '
        'exact, makes assertion (c) vacuous, and exempts the field from the '
        'strict scan. A broken implementation passing the stated gate.')


def test_h_every_observed_machine_value_is_a_member_of_its_mapping():
    problems = []
    for allowlist, row in corpus_rows():
        for field, value in row.items():
            if field not in MACHINE_VOCABULARY_FIELDS:
                continue
            if value is None or value == '' or not isinstance(value, str):
                continue    # absent, not an observation
            if value not in MACHINE_VOCABULARY_FIELDS[field]:
                problems.append(
                    f'{allowlist}.{field} = {value!r} is not in its mapped vocabulary')
    assert problems == [], (
        '\n  '.join(problems) + '\nTwo honest responses: point the mapping at the '
        'authority that produces those values, or move the field to '
        'READER_TEXT_FIELDS, where it is scanned strictly. There is no exclusion '
        'list on (h).')


@pytest.mark.parametrize('vocabulary,clause', [
    (frozenset(), 'NON-EMPTY'),
    (frozenset(NOVELTY_STATUSES), 'MEMBERSHIP'),
])
def test_h_control_13_a_non_floored_reader_field_moved_to_the_machine_side(
        vocabulary, clause):
    """Positive control 13, in TWO halves. `genre` is a reader field the corpus
    carries, present on three allowlists, and NOT one of assertion (b)'s seven
    names -- so (b) does not protect it and only (h) can."""
    assert 'genre' in READER_TEXT_FIELDS and 'genre' not in _READER_FACING_FLOOR
    poisoned = dict(MACHINE_VOCABULARY_FIELDS)
    poisoned['genre'] = vocabulary

    if clause == 'NON-EMPTY':
        assert not poisoned['genre'], 'the control did not seed an empty vocabulary'
        empty = [f for f, v in poisoned.items() if not v]
        assert empty == ['genre'], 'the NON-EMPTY clause would not have fired'
    else:
        observed = [row['genre'] for name, row in corpus_rows()
                    if name == 'SURFACE_CLAIM_FIELDS' and row.get('genre')]
        assert observed, 'no live genre value in the corpus -- the control is inert'
        assert observed[0] not in poisoned['genre'], (
            'the MEMBERSHIP clause would not have fired')


# ===========================================================================
# Locally-declared vocabularies, PINNED to their authorities by READING them.
# ===========================================================================

def test_coverage_statuses_are_pinned_to_the_builder_check_constraint():
    src = _read(BUILDER_PATH)
    match = re.search(r"coverage_status\s+IN\s*\(([^)]*)\)", src)
    assert match, f'the CHECK constraint moved in {BUILDER_PATH}'
    authority = frozenset(re.findall(r"'([^']+)'", match.group(1)))
    assert authority == COVERAGE_STATUSES, (
        f'COVERAGE_STATUSES {sorted(COVERAGE_STATUSES)} has drifted from its '
        f'authority {sorted(authority)} ({BUILDER_PATH})')


def test_eligibility_bases_are_pinned_to_the_service_case_expression():
    src = _read(SERVICE_PATH)
    idx = src.index('END AS eligibility_basis')
    window = src[max(0, idx - 400):idx]
    window = window[window.rindex('CASE WHEN'):]
    authority = frozenset(re.findall(r"THEN\s+'([^']+)'|ELSE\s+'([^']+)'", window))
    authority = frozenset(v for pair in authority for v in pair if v)
    assert authority == ELIGIBILITY_BASES, (
        f'ELIGIBILITY_BASES {sorted(ELIGIBILITY_BASES)} has drifted from its '
        f'authority {sorted(authority)} ({SERVICE_PATH})')


def test_the_gate_module_has_no_module_level_web_import():
    """Every render-smoke suite imports this module; a `web.` import at module
    level would pull NiceGUI into all of them, and the regression would be
    invisible until an unrelated suite failed to collect."""
    import ast
    tree = ast.parse(_read(GATE_PATH))
    for node in tree.body:      # module level ONLY
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith('web'), alias.name
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or '').startswith('web'), node.module


# ===========================================================================
# THE PANEL: seven manuscripts x two languages x four service states.
# ===========================================================================

#: The seven standing regression manuscripts, as FIXTURE PROFILES built from the
#: live allowlists. The spread is the point: the single-manuscript pass missed
#: three real defects that only appeared across a spread of real manuscripts.
MANUSCRIPT_PROFILES: Tuple[Tuple[str, Dict[str, Any]], ...] = (
    ('clean', {'claims': 1, 'works': 1, 'related': 0}),
    ('commentary', {'claims': 2, 'works': 3, 'related': 2}),
    ('judeo-arabic-multi-register', {'claims': 3, 'works': 2, 'related': 1,
                                     'genre': 'Judeo-Arabic / Multi-register'}),
    ('expert-reviewed', {'claims': 1, 'works': 1, 'related': 0, 'confirmed': True}),
    ('problem-siddur', {'claims': 4, 'works': 8, 'related': 5, 'gated': True}),
    ('page-relation-heavy', {'claims': 1, 'works': 1, 'related': 61}),
    ('427-identification', {'claims': 6, 'works': 12, 'related': 3}),
)

SERVICE_STATES = (STATUS_OK, STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY)
LANGS = ('en', 'he')


def _envelope_for(status: str, items, total=None, meta=None):
    if status == STATUS_OK:
        return make_envelope(STATUS_OK, items, total, meta=meta)
    return {
        STATUS_UNAVAILABLE: unavailable_envelope,
        STATUS_TIMEOUT: timeout_envelope,
        STATUS_BUSY: busy_envelope,
    }[status](meta={'reason': {
        STATUS_UNAVAILABLE: 'sidecar_not_serving',
        STATUS_TIMEOUT: 'query_timeout',
        STATUS_BUSY: 'bounded_concurrency',
    }[status]})


def bundle_for(profile: Mapping[str, Any], lang: str, status: str,
               seed_row: Optional[Mapping[str, Any]] = None) -> PanelServiceBundle:
    claims = [surface_safe_claim(_claim_source(
        claim_id=f'{i:064d}', work_id=f'w{i:06d}', canonical_work_id=f'w{i:06d}',
        display_work_id=f'w{i:06d}', span_start=i * 10, span_end=i * 10 + 90,
        genre=profile.get('genre', 'Bible commentary'),
        adjudication_status=(ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED
                             if profile.get('confirmed') else
                             ids.ADJUDICATION_STATUS_UNREVIEWED),
    )) for i in range(1, int(profile['claims']) + 1)]
    if seed_row is not None:
        claims = [dict(claims[0], **seed_row)] + claims[1:]
    works = [surface_safe_work_summary(_work_summary_source(
        canonical_work_id=f'w{i:06d}', display_work_id=f'w{i:06d}',
        neutral_title=f'Work {i}', page_count=i,
        gated=bool(profile.get('gated')) and i % 2 == 0,
    )) for i in range(1, int(profile['works']) + 1)]
    return PanelServiceBundle(
        claims=_envelope_for(status, claims, len(claims), meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False}),
        page_ids=_envelope_for(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                               meta={'sys_id': '990000000000000944', 'resolved': True,
                                     'truncated': False, 'volume_ie': 'IE1'}),
        manuscript_works=_envelope_for(status, works, len(works), meta={
            'page_scope_resolved': True, 'lang': lang}),
        related_count=_envelope_for(status, [], int(profile['related']), meta={
            'unit': 'distinct_opposite_pages'}),
        related_rows=None,
        lang=lang,
    )


def render_panel(model, page_id: str = '990000000000000944_IE1_P000002_FL3'):
    """Render the panel with a retry handler, exactly as the live seam does --
    `update_discovery_panel_section` always passes one, and a harness that
    passed None would test a shape the browse page never produces (pinned by
    `test_the_live_seam_always_supplies_a_retry_handler`)."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _retry():                                     # pragma: no cover
        return None

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_panel_smoke_probe')) as client:
            with client:
                dp.render_discovery_panel_body(model, on_retry=_retry, page_id=page_id)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client']


def test_the_live_seam_always_supplies_a_retry_handler():
    """An outage must render a visible temporary-unavailable state WITH a
    retry. The renderer draws the retry only when it is given a handler, so the
    property is a property of the SEAM and is asserted there."""
    src = _read('web/pages/browse_enrichment.py')
    assert 'render_discovery_panel_body(model, on_retry=_retry' in src, (
        'the browse seam renders the panel without a retry handler')
    assert 'async def _retry()' in src


def _elements_with_class(client, marker: str) -> list:
    return [el for el in client.elements.values()
            if marker in (getattr(el, '_classes', None) or [])]


def _subtree_texts(element) -> List[str]:
    out = []
    for node in element.descendants(include_self=True):
        for attr in ('text', '_text', 'content'):
            value = getattr(node, attr, None)
            if isinstance(value, str) and value.strip():
                out.append(value)
        for value in (getattr(node, '_props', None) or {}).values():
            if isinstance(value, str) and value.strip():
                out.append(value)
    return out


def scoped_fragment(client, marker: str) -> str:
    """A class-scoped HTML fragment for the shared gate, which extracts by class
    token over real markup (its scope argument is mandatory)."""
    import html as _html
    parts: List[str] = []
    for element in _elements_with_class(client, marker):
        parts.extend(_subtree_texts(element))
    return f'<div class="{marker}">{_html.escape(chr(10).join(parts))}</div>'


ASSERTION_COUNT = {'n': 0}


@pytest.mark.parametrize('name,profile', MANUSCRIPT_PROFILES)
@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('status', SERVICE_STATES)
def test_the_rendered_panel_is_honest(name, profile, lang, status):
    """Every assertion is scoped to the element it is about; none searches the
    whole rendered page as a string."""
    model = build_panel_rows(bundle_for(profile, lang, status))
    client = render_panel(model)
    for marker in (dp.PANEL_ROOT_CLASS, dp.PANEL_MANUSCRIPT_PANE_CLASS):
        if not _elements_with_class(client, marker):
            continue
        assert_surface_honesty(scoped_fragment(client, marker),
                               scope_selector=marker, lang=lang)
        ASSERTION_COUNT['n'] += 1
    for row_el in _elements_with_class(client, dp.PANEL_ROW_CLASS):
        fragment = '<div class="%s">%s</div>' % (
            dp.PANEL_ROW_CLASS, '\n'.join(_subtree_texts(row_el)))
        assert_surface_honesty(fragment, scope_selector=dp.PANEL_ROW_CLASS, lang=lang)
        ASSERTION_COUNT['n'] += 1
    if status != STATUS_OK:
        text = '\n'.join(_subtree_texts(_elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]))
        assert ds.retry_label(lang) in text, (
            f'{name}/{lang}/{status}: an outage rendered without a retry')


@pytest.mark.parametrize('name,profile', MANUSCRIPT_PROFILES)
@pytest.mark.parametrize('lang', LANGS)
def test_bucket_membership_equals_the_shared_rule_for_every_rendered_row(name, profile, lang):
    model = build_panel_rows(bundle_for(profile, lang, STATUS_OK))
    for row in iter_rows(model):
        assert row['bucket'] == bucket_label(row['in_main_pool'], lang), (
            f'{name}/{lang}: the row bucket is not shared/discovery_main_pool\'s')


@pytest.mark.parametrize('lang', LANGS)
def test_the_manuscript_pane_names_works_rather_than_showing_a_bare_count(lang):
    profile = dict(MANUSCRIPT_PROFILES[4][1])
    client = render_panel(build_panel_rows(bundle_for(profile, lang, STATUS_OK)))
    text = '\n'.join(_subtree_texts(
        _elements_with_class(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)[0]))
    assert 'Work 1 (1)' in text, text


@pytest.mark.parametrize('lang', LANGS)
def test_the_curated_w000176_label_renders_where_that_work_appears(lang):
    raw = 'משנה תורה, ספר אהבה'
    curated = ds.display_work_title('w000176', raw, lang)
    model = build_panel_rows(bundle_for(
        dict(MANUSCRIPT_PROFILES[0][1]), lang, STATUS_OK,
        seed_row={'work_id': 'w000176', 'canonical_work_id': 'w000176',
                  'display_work_id': 'w000176', 'neutral_title': raw}))
    client = render_panel(model)
    text = '\n'.join(_subtree_texts(_elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]))
    assert curated in text
    expected_raw = text.count(curated) if raw in curated else 0
    assert text.count(raw) == expected_raw, 'the raw recorded title rendered uncurated'


def test_work_titles_are_not_links():
    client = render_panel(build_panel_rows(
        bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), 'en', STATUS_OK)))
    for row_el in _elements_with_class(client, dp.PANEL_ROW_CLASS):
        for node in row_el.descendants(include_self=True):
            assert node.tag != 'a', 'an anchor wraps a work title'


# ===========================================================================
# THE ENVELOPE SCAN.
# ===========================================================================

def panel_envelopes() -> List[Tuple[str, Mapping[str, Any]]]:
    """Every envelope this surface consumes: the four eager reads, the lazy
    related-pages read and the expansion."""
    claims = [surface_safe_claim(_claim_source())]
    works = [surface_safe_work_summary(_work_summary_source())]
    related = [surface_safe_related_page(_related_page_source())]
    expansion = [surface_safe_expansion(_expansion_source())]
    return [
        ('claims', make_envelope(STATUS_OK, claims, 1, meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False})),
        ('page_ids', make_envelope(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                                   meta={'sys_id': '990000000000000944', 'resolved': True,
                                         'truncated': False, 'volume_ie': 'IE1'})),
        ('manuscript_works', make_envelope(STATUS_OK, works, 1, meta={
            'page_scope_resolved': True, 'lang': 'en'})),
        ('related_count', make_envelope(STATUS_OK, [], 4, meta={
            'unit': 'distinct_opposite_pages'})),
        ('related_rows', make_envelope(STATUS_OK, related, 1, meta={
            'unit': 'distinct_opposite_pages'})),
        ('expansion', make_envelope(STATUS_OK, expansion, 5684, meta={
            'work_id': 'w000001', 'anchor_mode': 'anchored',
            'filter_basis': 'displayed_band', 'anchor_excluded': True})),
        ('launch_shades', make_envelope(
            STATUS_OK, [surface_safe_launch_shade(_launch_shade_source(s))
                        for s in LAUNCH_CONTRIBUTION_SHADES],
            3, meta={'basis': 'main_pool'})),
    ]


@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('name,envelope', panel_envelopes())
def test_every_consumed_envelope_is_clean_by_VALUE_not_just_by_key(name, envelope, lang):
    """The gap this closes, named: `_assert_surface_safe` checks key NAMES."""
    assert_envelope_honesty(envelope, lang=lang, where=name)


def test_the_envelope_validator_really_does_check_key_names_only():
    """The premise of the test above, verified rather than asserted in prose:
    a stored NOVELTY verdict under `band_label` passes `make_envelope` and is
    caught only by this suite's value scan."""
    row = surface_safe_claim(_claim_source(band_label='fills_gap'))
    envelope = make_envelope(STATUS_OK, [row], 1, meta={'page_id': 'p', 'include_review': False})
    assert envelope['items'][0]['band_label'] == 'fills_gap'
    with pytest.raises(DiscoveryHonestyViolation):
        assert_envelope_honesty(envelope)


# ===========================================================================
# THE FORCED ERROR PATHS -- six failure modes.
# ===========================================================================

def forced_error_paths() -> List[Tuple[str, str]]:
    """`(mode, message)` for each failure this surface can produce. Each message
    is the REAL one: it is obtained by driving the failure, never retyped."""
    out: List[Tuple[str, str]] = []

    # 1. sidecar absent -> the unavailable envelope's own reason.
    out.append(('sidecar_absent',
                str(unavailable_envelope(meta={'reason': 'sidecar_not_serving'}))))
    # 2. query timeout.
    try:
        raise DiscoveryUnavailable('temporarily unavailable')
    except DiscoveryUnavailable as exc:
        out.append(('query_timeout', str(exc)))
    # 3. bounded-concurrency rejection.
    try:
        raise DiscoveryOverload('temporarily unavailable')
    except DiscoveryOverload as exc:
        out.append(('bounded_concurrency', str(exc)))
    # 4. an unresolved page scope, as the model reports it.
    from shared.discovery_panel_model import PanelContractError
    try:
        PanelServiceBundle(claims=None, page_ids={}, manuscript_works={},
                           related_count={})
    except PanelContractError as exc:
        out.append(('unresolved_or_malformed_envelope', str(exc)))
    # 5. a malformed row (a claim outside its frozen vocabulary).
    try:
        build_panel_rows(PanelServiceBundle(
            claims=make_envelope(STATUS_OK, [surface_safe_claim(_claim_source(
                routing_status='not_a_status'))], 1,
                meta={'page_id': 'p', 'include_review': False}),
            page_ids=make_envelope(STATUS_OK, ['p'], 1, meta={
                'sys_id': 's', 'resolved': True, 'truncated': False, 'volume_ie': None}),
            manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
                'page_scope_resolved': True, 'lang': 'en'}),
            related_count=make_envelope(STATUS_OK, [], 0, meta={
                'unit': 'distinct_opposite_pages'})))
    except PanelContractError as exc:
        out.append(('malformed_row', str(exc)))
    # 6. a missing work title.
    try:
        build_panel_rows(PanelServiceBundle(
            claims=make_envelope(STATUS_OK, [surface_safe_claim(_claim_source(
                relation_kind=None))], 1,
                meta={'page_id': 'p', 'include_review': False}),
            page_ids=make_envelope(STATUS_OK, ['p'], 1, meta={
                'sys_id': 's', 'resolved': True, 'truncated': False, 'volume_ie': None}),
            manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
                'page_scope_resolved': True, 'lang': 'en'}),
            related_count=make_envelope(STATUS_OK, [], 0, meta={
                'unit': 'distinct_opposite_pages'})))
    except PanelContractError as exc:
        out.append(('missing_anchor_identity', str(exc)))
    return out


def test_at_least_six_failure_modes_are_driven():
    assert len(forced_error_paths()) >= 6, forced_error_paths()


@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('mode,message', forced_error_paths())
def test_every_forced_error_path_is_honest(mode, message, lang):
    assert_error_path_honesty(message, lang=lang, where=mode)


# ===========================================================================
# POSITIVE CONTROLS -- one mutation per property, each asserting the SPECIFIC
# expected failure. The list IS the count.
# ===========================================================================

def _panel_fragment_with(text: str, marker: str = dp.PANEL_ROW_CLASS) -> str:
    import html as _html
    return f'<div class="{marker}">{_html.escape(text)}</div>'


def test_control_1_a_precision_figure_in_a_rendered_row():
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with('Precision 91%'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert 'unqualified percentage' in str(exc.value)


def test_control_2_a_stored_vocabulary_key_in_a_chip():
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with('direct_witness'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert 'raw stored vocabulary key' in str(exc.value)


def test_control_3_a_review_badge_in_a_row():
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with('Expert-reviewed'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert 'human-review badge' in str(exc.value)


def test_control_4_a_percentage_in_an_envelope_meta_reason():
    envelope = {**make_envelope(STATUS_UNAVAILABLE),
                'meta': {'reason': 'failed on 91% of rows'}}
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert 'unqualified percentage' in str(exc.value)
    # ...and the MARKUP scan demonstrably cannot see it.
    assert_surface_honesty(_panel_fragment_with('nothing here'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')


def test_control_5_a_percentage_in_a_forced_exception_message():
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_error_path_honesty('the query failed on 91% of rows')
    assert 'unqualified percentage' in str(exc.value)


def test_control_6_fills_gap_seeded_as_the_value_of_band_label():
    """Today (pre-136-17) this PASSED, which is round 7's finding 3."""
    row = surface_safe_claim(_claim_source(band_label='fills_gap'))
    envelope = make_envelope(STATUS_OK, [row], 1, meta={'page_id': 'p', 'include_review': False})
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert "'fills_gap'" in str(exc.value)


def test_control_7_main_full_coverage_seeded_as_the_value_of_band_label():
    row = surface_safe_claim(_claim_source(band_label='main_full_coverage'))
    envelope = make_envelope(STATUS_OK, [row], 1, meta={'page_id': 'p', 'include_review': False})
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert "'main_full_coverage'" in str(exc.value)


def test_control_8_accuracy_0_91_in_a_rendered_row():
    """No detector fired on this string at all before the sixth was added."""
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with('accuracy 0.91'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert 'accuracy/rate claim' in str(exc.value) or 'bare rate-shaped decimal' in str(exc.value)


def test_control_9_accuracy_0_91_in_an_envelope_meta_reason():
    envelope = {**make_envelope(STATUS_TIMEOUT), 'meta': {'reason': 'accuracy 0.91'}}
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert 'accuracy' in str(exc.value)
    # companion: the markup scan cannot see an envelope.
    assert_surface_honesty(_panel_fragment_with('nothing here'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')


def test_control_10_a_float_0_91_under_an_envelope_meta_key():
    """A string-value scan cannot see this, which is why the numeric rule exists."""
    envelope = {**make_envelope(STATUS_OK, [], 0), 'meta': {'quality': 0.91}}
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert 'rate-shaped float' in str(exc.value)


def test_control_11_accuracy_0_91_in_a_forced_exception_message():
    """An exception message is the ONE egress class that reaches a log and a
    reader without passing through either of the other two scans."""
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_error_path_honesty('the model refused: accuracy 0.91')
    assert 'accuracy' in str(exc.value)
    assert_surface_honesty(_panel_fragment_with('nothing here'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert_envelope_honesty(make_envelope(STATUS_OK, [], 0, meta={'lang': 'en'}))


@pytest.mark.parametrize('lang,sentence', [
    ('en', 'accuracy is 91 percent'),
    ('en', 'accuracy is 91 per cent'),
    ('en', 'we are right 91 pct of the time'),
    ('he', 'הדיוק הוא 91 אחוז'),
    ('he', 'שיעור הנכונות הוא 91 אחוזים'),
])
def test_control_11a_a_word_spelled_percentage_in_a_rendered_row(lang, sentence):
    """The sign is not the claim (round 12, finding 3).

    `91%` failed the percentage detector from the start, while `91 percent`
    passed ALL SIX -- `_PERCENT_RE` requires the glyph, and a bare integer is
    not a rate-shaped quantity, so rule 1 had nothing to sit beside either.
    """
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with(sentence),
                               scope_selector=dp.PANEL_ROW_CLASS, lang=lang)
    assert 'unqualified percentage' in str(exc.value)


@pytest.mark.parametrize('lang', LANGS)
def test_control_11b_a_word_spelled_percentage_in_the_other_two_egress_classes(lang):
    """The same form through the envelope scan and the error path -- neither of
    which the markup scan can see."""
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty({**make_envelope(STATUS_TIMEOUT),
                                 'meta': {'reason': 'accuracy fell to 91 percent'}}, lang=lang)
    assert 'unqualified percentage' in str(exc.value)
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_error_path_honesty('the model reported 91 percent accuracy', lang=lang)
    assert 'unqualified percentage' in str(exc.value)


@pytest.mark.parametrize('key', ['quality', 'score', 'confidence_of_match'])
def test_control_11c_a_ONE_place_float_under_a_neutral_envelope_key(key):
    """The numeric envelope rule required MORE THAN ONE significant decimal
    place, so `{"quality": 0.9}` and `{"score": 0.9}` passed while
    `{"quality": 0.91}` failed (round 12, finding 3). A rate rounded to one
    place is not less of a rate -- it is the likelier way an estimate gets
    softened onto a surface."""
    envelope = {**make_envelope(STATUS_OK, [], 0), 'meta': {key: 0.9}}
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert 'rate-shaped float' in str(exc.value)


def test_control_11d_a_one_place_float_on_a_ROW_field_too():
    """NUMERIC_RULE_EXEMPTIONS is `NEVER permitted on a ROW field`; the rule it
    guards must therefore reach one."""
    row = surface_safe_claim(_claim_source(coverage_ppm=0.9))
    envelope = make_envelope(STATUS_OK, [row], 1, meta={
        'page_id': 'p', 'include_review': False})
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_envelope_honesty(envelope)
    assert 'rate-shaped float' in str(exc.value)


def _live_limitations_text(lang: str) -> str:
    from web.pages.help import _LIMITATIONS_TEXT
    return _LIMITATIONS_TEXT['he' if lang == 'he' else 'en']


@pytest.mark.parametrize('lang', LANGS)
def test_control_12_the_d06a_sentence_outside_its_registered_scope(lang):
    """The live sentence, taken from the authority it is about, seeded into all
    THREE egress classes outside `D06A_QUALITATIVE_SCOPES` -- each must fail."""
    sentence = _live_limitations_text(lang)
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(_panel_fragment_with(sentence),
                               scope_selector=dp.PANEL_ROW_CLASS, lang=lang)
    assert 'accuracy/rate claim' in str(exc.value)
    with pytest.raises(DiscoveryHonestyViolation):
        assert_envelope_honesty({**make_envelope(STATUS_TIMEOUT),
                                 'meta': {'reason': sentence}}, lang=lang)
    with pytest.raises(DiscoveryHonestyViolation):
        assert_error_path_honesty(sentence, lang=lang)


@pytest.mark.parametrize('lang', LANGS)
def test_control_12a_a_numeric_rate_inside_the_registered_scope_still_fails(lang):
    """The exemption is a CONJUNCTION, not a switch. Without this, an
    implementation that simply switches the detector off for the limitations
    element satisfies both control 12 and FP-D06A-LIVE-PAGE while exempting a
    numeric rate on the methods page."""
    scope = D06A_QUALITATIVE_SCOPES[0]
    with pytest.raises(DiscoveryHonestyViolation):
        assert_surface_honesty(_panel_fragment_with('accuracy 0.91', scope),
                               scope_selector=scope, lang=lang)


@pytest.mark.parametrize('lang', LANGS)
def test_control_12b_the_same_sentence_elsewhere_in_the_same_methods_card(lang):
    """The round-10 control: under a scope registered as the CARD's class this
    would PASS; under the limitations PARAGRAPH's class it fails. That is the
    entire difference between a binding and a claim to have one."""
    from web.pages.help import _CONFIDENCE_SECTION_CLASS, _BUCKET_RULE_HEADING
    sentence = _live_limitations_text(lang)
    fragment = _panel_fragment_with(
        f'{_BUCKET_RULE_HEADING[lang]} {sentence}', _CONFIDENCE_SECTION_CLASS)
    with pytest.raises(DiscoveryHonestyViolation) as exc:
        assert_surface_honesty(fragment,
                               scope_selector=_CONFIDENCE_SECTION_CLASS, lang=lang)
    assert 'accuracy/rate claim' in str(exc.value)


# ===========================================================================
# FALSE-POSITIVE CONTROLS. Both gate widenings are exactly the kind of fix that
# breaks correct output, so these matter as much as the positive ones.
# ===========================================================================

def test_FP_LIVE_VOCAB():
    """A real-shaped envelope whose machine-vocabulary fields ALL carry live
    stored values PASSES; `direct_witness` seeded as the VALUE of `band_label`
    FAILS. A scan that passes both halves is inert; one that fails both is the
    false positive this control exists to prevent."""
    row = surface_safe_claim(_claim_source(
        novelty_status='fills_gap', main_pool_reason='main_full_coverage',
        routing_reason='low_coverage', coverage_status='not_applicable',
        eligibility_basis='human_confirmed',
        adjudication_status=ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED,
        measurement_status='measured_pass'))
    expansion = surface_safe_expansion(_expansion_source())
    envelope = make_envelope(STATUS_OK, [row, expansion], 2, meta={
        'page_id': 'p', 'include_review': False})
    assert_envelope_honesty(envelope)

    poisoned = make_envelope(
        STATUS_OK, [surface_safe_claim(_claim_source(band_label='direct_witness'))], 1,
        meta={'page_id': 'p', 'include_review': False})
    with pytest.raises(DiscoveryHonestyViolation):
        assert_envelope_honesty(poisoned)


@pytest.mark.parametrize('lang', LANGS)
def test_FP_QUALIFIED_COVERAGE(lang):
    """The ONE permitted percentage must survive the sixth detector. If it does
    not, the detector is wrong, not the string."""
    qualified = ds.row_headline('Commentary on Song of Songs', 680000,
                                ids.CLAIM_TYPE_DIRECT_WITNESS, lang,
                                evidence_source=ids.EVIDENCE_SOURCE_TRACK1_DIRECT)
    assert '68%' in qualified
    assert_surface_honesty(_panel_fragment_with(qualified),
                           scope_selector=dp.PANEL_ROW_CLASS, lang=lang)


@pytest.mark.parametrize('lang,sentence', [
    ('en', 'we do not publish an accuracy rate for these matches'),
    ('he', 'איננו מפרסמים שיעור דיוק עבור התאמות אלה'),
])
def test_FP_RATE_DISCLAIMER(lang, sentence):
    """A caveat that DISCLAIMS a rate without quoting one. The caveat slot is
    the likeliest home for that sentence; a gate that fails on it is a gate
    somebody deletes."""
    assert_surface_honesty(_panel_fragment_with(sentence),
                           scope_selector=dp.PANEL_ROW_CLASS, lang=lang)


def test_FP_SAMPLE_SIZE():
    """A rate word beside a plain COUNT passes; an N-out-of-M correctness
    statement fails. This pair is what pins the rate-SHAPED restriction."""
    assert_surface_honesty(_panel_fragment_with('precision was measured on 400 cards'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    with pytest.raises(DiscoveryHonestyViolation):
        assert_surface_honesty(_panel_fragment_with('correct in 380 out of 400 cases'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')


@pytest.mark.parametrize('lang,permitted', [
    ('en', 'Matches Commentary on Song of Songs · 68 percent of page'),
    ('he', 'התאמה לפירוש שיר השירים · 68 אחוז מהדף'),
])
def test_FP_PERCENT_WORD_COVERAGE(lang, permitted):
    """The ONE permitted percentage, spelled as a WORD, must survive the
    widening exactly as the glyph form does -- the coverage qualifier is what
    licenses it, never the notation."""
    assert_surface_honesty(_panel_fragment_with(permitted),
                           scope_selector=dp.PANEL_ROW_CLASS, lang=lang)


@pytest.mark.parametrize('lang,sentence', [
    # The shipped methods page's own promise. A number is REQUIRED before the
    # word precisely so this sentence -- which exists to say there is no
    # percentage -- does not read as one.
    ('en', 'stated in words, never as a percentage or an interval'),
    ('he', 'מנוסח במילים, לא כאחוז או כטווח'),
    # Ordinary reader text that happens to carry a number and a percent-shaped
    # WORD far apart, or neither.
    ('en', 'MS Heb c.57, folio 12, dated 1157'),
    ('en', 'the percentage of the page that matched is not published'),
    ('he', 'אחוז הכיסוי אינו מפורסם'),
])
def test_FP_PERCENT_WORD_NEEDS_A_NUMBER(lang, sentence):
    """The bare word is honest prose in both languages; only `<number> word` is
    a claim. Without the number requirement the widening turns the
    owner-approved D-06a sentence red on its own promise."""
    assert_surface_honesty(_panel_fragment_with(sentence),
                           scope_selector=dp.PANEL_ROW_CLASS, lang=lang)


def test_FP_INTEGRAL_FLOAT_IS_NOT_A_RATE():
    """`0.0` and `1.0` stay out of the widened numeric rule: a counter that
    arrives as a float is the realistic source of both, and no figure this
    system measures is integral. `0.9` in the same slot still fails, so the
    exclusion is bounded rather than a hole."""
    for value in (0.0, 1.0):
        assert_envelope_honesty({**make_envelope(STATUS_OK, [], 0),
                                 'meta': {'weight': value}})
    with pytest.raises(DiscoveryHonestyViolation):
        assert_envelope_honesty({**make_envelope(STATUS_OK, [], 0),
                                 'meta': {'weight': 0.9}})


def test_no_rate_key_token_collides_with_an_allowlisted_field():
    """`_RATE_KEY_TOKENS` fires on the KEY regardless of the value's type, so a
    token that is also a word of a real allowlisted field would reject every
    correct envelope. Asserted against `_ALL_ALLOWLISTS` rather than against a
    remembered list."""
    from tests.render_smoke.discovery_honesty_gate import _RATE_KEY_TOKENS
    tokens = set()
    for field in ALLOWLIST_FIELD_UNION:
        tokens.update(re.split(r'[^a-z0-9]+', str(field).lower()))
    collisions = tokens & set(_RATE_KEY_TOKENS)
    assert not collisions, (
        f'_RATE_KEY_TOKENS collides with allowlisted field words: {sorted(collisions)}')


def test_FP_VERSION_BOUNDARY():
    """Four assertions, and each rules out one cheaper repair."""
    assert_surface_honesty(_panel_fragment_with('precision handling changed in V0.8'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    assert_surface_honesty(_panel_fragment_with('the query took 1.25 seconds of precision work'),
                           scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    with pytest.raises(DiscoveryHonestyViolation):
        # ONE decimal place, not version-shaped: raising the proximity rule's
        # minimum to two places would let this through.
        assert_surface_honesty(_panel_fragment_with('accuracy 0.9'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')
    with pytest.raises(DiscoveryHonestyViolation):
        # A real rate GLUED to a non-version token: the previous revision's
        # "preceded by any word character" exclusion let this escape both rules.
        assert_surface_honesty(_panel_fragment_with('accuracy score0.9'),
                               scope_selector=dp.PANEL_ROW_CLASS, lang='en')


# --- the LIVE methods page -------------------------------------------------

def _render_help(lang: str):
    """The live /help page, through the same server-render harness
    `tests/render_smoke/test_help_methods_render_smoke.py` uses. That file is
    NOT edited by this plan."""
    import os as _os
    from contextlib import ExitStack
    from unittest.mock import AsyncMock, patch
    import httpx
    import web.main as _web_main   # noqa: F401  (registers /help on core.app)
    from nicegui import core
    from nicegui.context import context as _ctx
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    holder: Dict[str, Any] = {}
    saved_slots = list(_ctx.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()

    async def _run():
        prepare_simulation()
        set_storage_secret('panel-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(patch('web.pages.help.discovery_available', return_value=True))
            stack.enter_context(patch('web.main.discovery_methods_noindex', return_value=True))
            stack.enter_context(patch('web.main.get_all_band_precision',
                                      new=AsyncMock(return_value={})))
            stack.enter_context(patch('web.main.get_band_claim_counts',
                                      new=AsyncMock(return_value={})))
            stack.enter_context(patch('web.main._resolve_ui_language', return_value=lang))
            _os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                            transport=httpx.ASGITransport(core.app),
                            base_url='http://test') as client:
                        user = User(client)
                        await user.open('/help')
                        holder['user'] = user
            finally:
                _os.environ.pop('NICEGUI_USER_SIMULATION', None)

    try:
        asyncio.run(_run())
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _ctx.slot_stack.clear()
        _ctx.slot_stack.extend(saved_slots)
    return holder['user']


def _help_scoped_text(user, marker: str) -> str:
    parts: List[str] = []
    with user._client:
        for element in user._client.elements.values():
            if marker in (getattr(element, '_classes', None) or []):
                for node in element.descendants(include_self=True):
                    for attr in ('text', 'content'):
                        value = getattr(node, attr, None)
                        if isinstance(value, str) and value:
                            parts.append(value)
    return '\n'.join(parts)


@pytest.mark.parametrize('lang', LANGS)
def test_FP_D06A_LIVE_PAGE(lang):
    """The full six-detector gate over the LIVE methods page's limitations
    PARAGRAPH -- the single entry in `D06A_QUALITATIVE_SCOPES`, not the methods
    card -- in both languages, with the text taken from the RENDER rather than
    retyped, and the scoped subtree's text asserted EQUAL to `_LIMITATIONS_TEXT`
    rather than merely containing it. A marker class accidentally left on the
    card, the subsection or the heading makes that subtree a strict superset and
    the equality fails BY NAME."""
    import html as _html
    from web.pages.help import _LIMITATIONS_PARAGRAPH_CLASS, _LIMITATIONS_TEXT

    assert D06A_QUALITATIVE_SCOPES == (_LIMITATIONS_PARAGRAPH_CLASS,), (
        'the registered scope has drifted from its authority in web/pages/help.py')

    user = _render_help(lang)
    text = _help_scoped_text(user, _LIMITATIONS_PARAGRAPH_CLASS)
    assert _normalise(text) == _normalise(_LIMITATIONS_TEXT[lang]), (
        'the scoped subtree is not EXACTLY the limitations paragraph')
    fragment = f'<div class="{_LIMITATIONS_PARAGRAPH_CLASS}">{_html.escape(text)}</div>'
    assert_surface_honesty(fragment, scope_selector=_LIMITATIONS_PARAGRAPH_CLASS, lang=lang)


@pytest.mark.parametrize('lang', LANGS)
def test_FP_D06A_CARD_BOUNDARY(lang):
    """The pair that proves the SELECTOR discriminates: the same sentence scoped
    to `_CONFIDENCE_SECTION_CLASS` FAILS, scoped to the registered entry it
    PASSES. With the scope registered as the card's class both halves pass,
    which is exactly the leak round 10 found."""
    import html as _html
    from web.pages.help import _CONFIDENCE_SECTION_CLASS, _LIMITATIONS_TEXT
    sentence = _LIMITATIONS_TEXT[lang]
    card = f'<div class="{_CONFIDENCE_SECTION_CLASS}">{_html.escape(sentence)}</div>'
    with pytest.raises(DiscoveryHonestyViolation):
        assert_surface_honesty(card, scope_selector=_CONFIDENCE_SECTION_CLASS, lang=lang)
    scope = D06A_QUALITATIVE_SCOPES[0]
    paragraph = f'<div class="{scope}">{_html.escape(sentence)}</div>'
    assert_surface_honesty(paragraph, scope_selector=scope, lang=lang)


def test_the_registered_d06a_scope_holds_exactly_one_entry():
    assert len(D06A_QUALITATIVE_SCOPES) == 1, (
        'the D-06a exception grew quietly: ' + repr(D06A_QUALITATIVE_SCOPES))


@pytest.mark.parametrize('lang', LANGS)
def test_the_limitations_paragraph_class_is_on_the_label_and_nowhere_else(lang):
    from web.pages.help import (
        _CONFIDENCE_SECTION_CLASS, _LIMITATIONS_HEADING,
        _LIMITATIONS_PARAGRAPH_CLASS, _LIMITATIONS_TEXT,
    )
    user = _render_help(lang)
    marked = []
    with user._client:
        for element in user._client.elements.values():
            classes = getattr(element, '_classes', None) or []
            if _LIMITATIONS_PARAGRAPH_CLASS in classes:
                marked.append(element)
    assert len(marked) == 1, f'{len(marked)} elements carry the marker class'
    element = marked[0]
    assert (getattr(element, 'text', '') or '') == _LIMITATIONS_TEXT[lang]
    assert _CONFIDENCE_SECTION_CLASS not in (element._classes or []), (
        'the marker class landed on the methods CARD')
    assert _LIMITATIONS_HEADING[lang] not in (getattr(element, 'text', '') or '')


@pytest.mark.parametrize('lang', LANGS)
def test_the_owner_approved_wording_is_pinned_by_a_hardcoded_digest(lang):
    """An INDEPENDENT pin. The sibling render test checks SUBSTRINGS and
    `FP-D06A-LIVE-PAGE` compares the render against the same mutable
    `_LIMITATIONS_TEXT` authority, so neither alone detects a wording edit that
    preserves those fragments. A hardcoded digest is a literal no authority can
    move with it."""
    from web.pages.help import _LIMITATIONS_PARAGRAPH_CLASS
    user = _render_help(lang)
    text = _normalise(_help_scoped_text(user, _LIMITATIONS_PARAGRAPH_CLASS))
    digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
    assert digest == LIMITATIONS_TEXT_SHA256[lang], (
        f'the D-06a limitations wording ({lang}) changed. It is OWNER-APPROVED '
        'text and this digest is a PIN on it, never a licence to edit it. If the '
        'owner has re-approved a new wording, update the digest in the SAME '
        'commit as the wording and say so.')


# ===========================================================================
# The masking sweep.
# ===========================================================================

def test_the_masking_scan_is_recorded_and_fails_closed_when_unconfigured():
    """`scripts/check_atlas_masking.py` fails CLOSED when
    `MASKING_SCAN_PATTERNS_FILE` is unset -- an explicit skip is recorded, never
    worked around. The capture path is outside the working tree."""
    script = 'scripts/check_atlas_masking.py'
    assert os.path.exists(script), script
    patterns = os.environ.get('MASKING_SCAN_PATTERNS_FILE') or '.masking_patterns'
    if not os.path.exists(patterns):
        pytest.skip(
            'MASKING_SCAN_PATTERNS_FILE is unset and .masking_patterns is absent; '
            'the scan FAILS CLOSED and the skip is recorded in the summary rather '
            'than worked around')
    env = dict(os.environ, MASKING_SCAN_PATTERNS_FILE=os.path.abspath(patterns))
    result = subprocess.run(
        [sys.executable, script, '--scan-repo'],
        capture_output=True, text=True, env=env)
    assert result.returncode == 0, (
        f'the repo masking scan failed:\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}')
