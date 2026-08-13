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
import dataclasses
import hashlib
import io
import json
import logging
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
from shared.discovery_service import (
    LAUNCH_CONTRIBUTION_SHADES,
    _browse_address_from_page_id,
    _page_number_from_page_id,
    _volume_ie_from_page_id,
)
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
        'rendered_relation': ids.RENDERED_RELATION_DIRECT_WITNESS,
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
        'rendered_relation': ids.RENDERED_RELATION_DIRECT_WITNESS,
    }
    row.update(overrides)
    return row


def _related_page_source(**overrides) -> Dict[str, Any]:
    _page_id = '990000000000000945_IE1_P000004_FL9'
    row = {
        'related_page_id': _page_id,
        # The manuscript this page belongs to, as the joined query resolves it
        # (2026-08-05). Non-null here so the allowlist-coverage assertion sees
        # every field, and so the capture paints a real shelfmark -- which is
        # projection text, i.e. exactly where a restricted corpus name arrives.
        'sys_id': '990000000000000945',
        'library_code': 'CUL',
        'shelfmark_display': 'T-S 12.945',
        # Folio FOUR, not folio one, for the same reason the expansion fixture
        # uses folio nine: a folio-1 fixture cannot distinguish a targeted link
        # from an untargeted one.
        'page_number': _page_number_from_page_id(_page_id),
        'volume_ie': _volume_ie_from_page_id(_page_id),
        'display_missing': False,
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
        # Folio NINE, not folio one: a folio-1 fixture makes a targeted link and
        # an untargeted one produce the same URL, so every assertion about the
        # difference would pass without checking it.
        'representative_page_id': '990000000000000946_IE1_P000009_FL9',
        # Parsed by the SERVICE's own accessor, never written as literals beside
        # the id -- the fixture then cannot claim an address the id does not
        # carry, and the pair stays atomic exactly as the service emits it.
        **dict(zip(('representative_page', 'representative_volume_ie'),
                   _browse_address_from_page_id(
                       '990000000000000946_IE1_P000009_FL9'))),
        'representative_claim_id': 'e' * 64,
        'member_sys_ids': ['990000000000000946'],
        'library_code': 'CUL',
        'shelfmark_display': 'T-S 12.123',
        'display_missing': False,
        'rendered_relation': ids.RENDERED_RELATION_SHARED_TEXT,
        'anchor_rendered_relation': ids.RENDERED_RELATION_DIRECT_WITNESS,
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
        # C-track step 3b: the fail-closed state, observed by the gate at least
        # once. It is the newest member of the rendered vocabulary and the one a
        # leak would be least expected on -- and it is a legitimate cap output
        # for this row (a shared-text member under an `uncertain` identification).
        rendered_relation=ids.RENDERED_RELATION_UNCERTAIN,
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
                     rendered_relation=ids.RENDERED_RELATION_DIRECT_WITNESS,
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
    """Four of them -- rendered_relation, anchor_rendered_relation,
    displayed_evidence_source, displayed_confidence_band -- arrive with 136-21's
    SURFACE_EXPANSION_FIELDS, and `shade` with 136-22's
    SURFACE_LAUNCH_SHADE_FIELDS. If one is ever missing, establish which
    namespace it belongs to BEFORE deleting it.

    ⟨CHANGED 2026-08-12 -- C-track step 3d⟩ The first two were `claim_type` and
    `anchor_claim_type`. The pane now carries each side's CAPPED matrix relation
    instead of its stored claim type, so those are the names the floor has to
    find on the expansion allowlist.
    """
    missing = sorted(KNOWN_CARRIER_FLOOR - ALLOWLIST_FIELD_UNION)
    assert missing == [], f'floor members outside the allowlist union: {missing}'
    for field in ('rendered_relation', 'anchor_rendered_relation',
                  'displayed_evidence_source', 'displayed_confidence_band'):
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
               seed_row: Optional[Mapping[str, Any]] = None,
               seed_title: Optional[str] = None) -> PanelServiceBundle:
    """`seed_title` replaces the recorded work title on EVERY claim and work
    summary. It is how the masking positive control gets its needle into the
    render through DATA -- the route a restricted name arriving from the sidecar
    would actually take -- instead of being appended to the capture file."""
    claims = [surface_safe_claim(_claim_source(
        claim_id=f'{i:064d}', work_id=f'w{i:06d}', canonical_work_id=f'w{i:06d}',
        display_work_id=f'w{i:06d}', span_start=i * 10, span_end=i * 10 + 90,
        genre=profile.get('genre', 'Bible commentary'),
        adjudication_status=(ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED
                             if profile.get('confirmed') else
                             ids.ADJUDICATION_STATUS_UNREVIEWED),
        **({'neutral_title': seed_title} if seed_title else {}),
    )) for i in range(1, int(profile['claims']) + 1)]
    if seed_row is not None:
        claims = [dict(claims[0], **seed_row)] + claims[1:]
    works = [surface_safe_work_summary(_work_summary_source(
        canonical_work_id=f'w{i:06d}', display_work_id=f'w{i:06d}',
        neutral_title=seed_title or f'Work {i}', page_count=i,
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
# The expansion row's own anatomy (owner report, 2026-08-07). Three defects,
# one row: no link, the wrong missing-state string, and no catalogue title.
# ===========================================================================

def _render_entry_control(model, *, highlight_new=False):
    """Paint `render_discovery_entry_control` alone and return the client."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    def _noop():                                             # pragma: no cover
        return None

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_entry_control_probe')) as client:
            with client:
                dp.render_discovery_entry_control(
                    model, on_toggle=_noop, highlight_new=highlight_new)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client']


@pytest.mark.parametrize('lang', LANGS)
def test_the_entry_control_is_marked_new_until_the_reader_opens_it(lang):
    """The panel is the release's one new BROWSE surface, and it arrives on a
    toolbar already carrying seven `flat dense` controls -- so without a marker
    it looks exactly like the things a reader has learned to ignore (owner,
    2026-08-07).

    The marker is a SEPARATE element, never text appended to the button's label:
    the label is already `Computed identifications (N)`, and anything matching on
    that string -- a screen reader listing controls, a test driver, the masking
    capture -- must keep finding it intact.
    """
    model = build_panel_rows(bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK))

    plain = _render_entry_control(model)
    assert not _elements_with_class(plain, dp.PANEL_ENTRY_NEW_CLASS), (
        'the highlight rendered for a reader who has already opened the panel')
    assert not _elements_with_class(plain, dp.PANEL_ENTRY_NEW_BADGE_CLASS)

    marked = _render_entry_control(model, highlight_new=True)
    assert _elements_with_class(marked, dp.PANEL_ENTRY_NEW_CLASS), (
        'the entry control carries no launch highlight for a first-time reader')
    badges = _elements_with_class(marked, dp.PANEL_ENTRY_NEW_BADGE_CLASS)
    assert len(badges) == 1, f'expected one NEW badge, got {len(badges)}'

    # The button's own accessible name is UNCHANGED by the highlight -- the
    # badge is a SIBLING, so the label a reader (or a driver) matches on is the
    # same string with and without it.
    def _button_labels(client):
        return sorted(
            el.text for el in client.elements.values()
            if el.tag == 'q-btn' and getattr(el, 'text', None))
    assert _button_labels(plain) == _button_labels(marked), (
        'the highlight altered the control label; it must be a sibling element')
    assert _button_labels(marked), 'no button was rendered at all'


def test_the_new_badge_is_hidden_from_assistive_technology():
    """The word adds nothing spoken that the button's own name does not already
    carry, and an unlabelled "New" read out beside a control is noise."""
    model = build_panel_rows(bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), 'en', STATUS_OK))
    badge = _elements_with_class(
        _render_entry_control(model, highlight_new=True),
        dp.PANEL_ENTRY_NEW_BADGE_CLASS)[0]
    assert (badge._props or {}).get('aria-hidden') in (True, 'true'), (
        f'the badge is exposed to assistive tech: {badge._props!r}')


def test_the_highlight_carries_no_claim_about_the_matches():
    """It says the CONTROL is new. It must never imply anything about the
    quality, novelty or confidence of what is behind it -- D-24 forbids
    confidence styling on this surface, and a "look at this" treatment keyed on
    findings would be exactly that through the back door.
    """
    model = build_panel_rows(bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), 'en', STATUS_OK))
    text = '\n'.join(_subtree_texts(list(
        _render_entry_control(model, highlight_new=True).elements.values())[0])).lower()
    for forbidden in ('important', 'best', 'top', 'verified', 'confirmed',
                      'high confidence', 'accurate', 'recommended', 'discovery',
                      'breakthrough'):
        assert forbidden not in text, (
            f'the launch highlight makes a claim about the matches: {forbidden!r}')


def test_the_seam_actually_computes_the_highlight_from_the_readers_state():
    """The gap the tests above cannot see.

    Every render test passes `highlight_new` in DIRECTLY, so all of them pass
    against a seam that hardcodes it to `False` and shows the highlight to
    nobody -- verified by mutation, which is how this test came to exist. The
    renderer being able to draw a marker is not the same property as the reader
    ever being given one.

    So: the seam must READ the seen-flag and PASS what it read. Checked over the
    AST, by dataflow rather than by text -- the value handed to
    `render_discovery_entry_control` has to be the name the `safe_user_get`
    result was bound to, not a constant that happens to sit in that argument.
    """
    import ast
    tree = ast.parse(_read('web/pages/browse_enrichment.py'))
    seam = next((n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef)
                 and n.name == 'update_discovery_panel_section'), None)
    assert seam is not None, 'update_discovery_panel_section is gone'

    # What name holds the result of the seen-flag READ?
    read_into = {
        target.id
        for node in ast.walk(seam) if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
        if any(isinstance(c, ast.Call) and getattr(c.func, 'id', None) == 'safe_user_get'
               for c in ast.walk(node.value))
    }
    assert read_into, (
        'the seam never reads the panel-seen flag, so the launch highlight is '
        'either shown to everyone forever or to nobody at all')

    # ...and is THAT name what reaches the renderer?
    passed = [
        kw.value for call in ast.walk(seam) if isinstance(call, ast.Call)
        and getattr(call.func, 'id', None) == 'render_discovery_entry_control'
        for kw in call.keywords if kw.arg == 'highlight_new'
    ]
    assert passed, (
        'the entry control is rendered without `highlight_new`, so the reader '
        'never sees the launch highlight')
    for value in passed:
        assert isinstance(value, ast.Name) and value.id in read_into, (
            'the seam passes a CONSTANT for `highlight_new` rather than what it '
            f'read from the reader\'s state ({ast.dump(value)[:60]}) -- the '
            'highlight would be shown to everyone forever, or to nobody')


def test_the_seam_retires_the_highlight_on_first_open_not_on_render():
    """WRITE-ON-OPEN, never write-on-render.

    The browse toolbar paints on every page load, so retiring the highlight when
    the control RENDERS would burn it before the reader had a chance to see it.
    It is retired when the panel is actually opened -- which is also why there is
    no dismiss control: the highlight removes itself by being used.

    Checked over the AST of the shipped seam, because the prose above the code
    says the same thing and a text scan would match its own explanation.
    """
    import ast
    tree = ast.parse(_read('web/pages/browse_enrichment.py'))
    seam = next((n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef)
                 and n.name == 'update_discovery_panel_section'), None)
    assert seam is not None, 'update_discovery_panel_section is gone'

    toggle = next((n for n in ast.walk(seam)
                   if isinstance(n, ast.FunctionDef) and n.name == '_toggle_panel'), None)
    assert toggle is not None, '_toggle_panel is gone -- re-point this guard'

    writes_in_toggle = [
        c for c in ast.walk(toggle) if isinstance(c, ast.Call)
        and getattr(c.func, 'id', None) == 'safe_user_set'
    ]
    assert writes_in_toggle, (
        'the seen-flag is not written when the panel is opened, so the highlight '
        'never retires and becomes a permanent badge')

    writes_anywhere = [
        c for c in ast.walk(seam) if isinstance(c, ast.Call)
        and getattr(c.func, 'id', None) == 'safe_user_set'
    ]
    assert len(writes_anywhere) == len(writes_in_toggle), (
        'the seen-flag is written outside `_toggle_panel` -- a write on the '
        'render path retires the highlight before the reader has seen it')


def _render_expansion_rows(items, lang='en', catalogue_title=None,
                           catalogue_identity=None):
    """Paint `_render_expansion_envelope` and return the client.

    Drives the SHIPPED renderer with PROJECTED rows, so a field the projection
    does not carry cannot be smuggled in by a hand-written dict."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _noop_reload():                                # pragma: no cover
        return None

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_expansion_row_probe')) as client:
            with client:
                dp._render_expansion_envelope(
                    make_envelope(STATUS_OK, items, len(items), meta={
                        'work_id': 'w000001', 'anchor_mode': 'anchored',
                        'filter_basis': 'displayed_band', 'anchor_excluded': True}),
                    lang, {'open': True, 'page': 1, 'loaded': True}, _noop_reload,
                    page_size=25, catalogue_title=catalogue_title,
                    catalogue_identity=catalogue_identity)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client']


def _client_hrefs(client) -> List[str]:
    """Every `href` in the client.

    Read off the PROP, never the tag name: NiceGUI's `ui.link` renders as a
    `nicegui-link` custom element, so a tag-name check silently finds nothing
    and the assertion inverts (`tests/render_smoke/test_findings_render_smoke.py
    ::_hrefs` records the same trap)."""
    out: List[str] = []
    for el in client.elements.values():
        href = (getattr(el, '_props', None) or {}).get('href')
        if isinstance(href, str) and href:
            out.append(href)
    return out


def test_an_expansion_row_links_its_shelfmark_to_that_manuscript():
    """These rows answer "what ELSE carries this work", so the manuscript is the
    next thing the reader wants. It was the one shelfmark in the panel they had
    to copy out and paste into the search box."""
    row = surface_safe_expansion(_expansion_source())
    hrefs = _client_hrefs(_render_expansion_rows([row]))
    assert any(h.startswith(f"/browse?sys_id={row['representative_sys_id']}")
               for h in hrefs), (
        f'the expansion row renders no link to its manuscript; hrefs: {hrefs}')


def test_an_expansion_row_links_to_the_FOLIO_its_representative_claim_is_on():
    """These rows answer "what else carries this work", and the answer is only
    useful if the reader lands where the work is (owner request, 2026-08-08).

    The row already ranked a representative claim; `representative_page_id` is
    that claim's page, so a page this work was matched on was available all
    along and the link threw it away. The comment that used to sit on this link
    said "the representative page is not necessarily where the match sits",
    which was simply wrong about what `best_row` is.
    """
    row = surface_safe_expansion(_expansion_source())
    hrefs = _client_hrefs(_render_expansion_rows([row]))
    assert any('page=9' in h and 'volume_ie=IE1' in h for h in hrefs), (
        f'the expansion row still opens the manuscript, not the folio: {hrefs}')


def test_an_expansion_row_withholds_a_folio_it_cannot_fully_address():
    """A representative page id with no volume component addresses a folio
    number in an unknown volume, which for a multi-volume manuscript is a
    different page in each one. The row keeps its manuscript link and drops the
    folio rather than guessing which volume the reader meant."""
    row = surface_safe_expansion(_expansion_source(
        representative_page_id='990000000000000946_P000009_FL9',
        representative_page=None, representative_volume_ie=None))
    hrefs = _client_hrefs(_render_expansion_rows([row]))
    assert hrefs == ['/browse?sys_id=990000000000000946'], hrefs


def test_an_expansion_row_links_its_OWN_representative_never_another_member():
    """A merged witness unit carries several `member_sys_ids`. The row is NAMED
    after its ranked representative, so linking any other member would take the
    reader to a manuscript this row does not describe."""
    row = surface_safe_expansion(_expansion_source(
        member_sys_ids=['990000000000000946', '990000000000000999']))
    hrefs = _client_hrefs(_render_expansion_rows([row]))
    assert any('990000000000000946' in h for h in hrefs), hrefs
    assert not any('990000000000000999' in h for h in hrefs), (
        f'the row links a non-representative unit member: {hrefs}')


def test_an_unnamed_expansion_row_offers_no_link_at_all():
    """A row we could not name has nothing to link -- and must not emit
    `/browse?sys_id=None`, which is a dead end dressed as a destination."""
    row = surface_safe_expansion(_expansion_source(
        display_missing=True, library_code=None, shelfmark_display=None))
    hrefs = _client_hrefs(_render_expansion_rows([row]))
    assert not hrefs, f'an unnamed row still rendered a link: {hrefs}'


@pytest.mark.parametrize('lang', LANGS)
def test_an_unnamed_expansion_row_says_the_MANUSCRIPT_is_unnamed(lang):
    """Not "Title unavailable", which is about a WORK.

    `display_missing` means there is no `manuscript_display` row -- 15% of
    claim-bearing manuscripts, essentially all of them `review_only`, i.e. exactly
    the rows that live behind "Show more possible matches". Borrowing the work's
    missing-title string told the reader a title was unavailable while the work
    was named in the heading directly above (owner report, 2026-08-07).
    """
    row = surface_safe_expansion(_expansion_source(
        display_missing=True, library_code=None, shelfmark_display=None))
    client = _render_expansion_rows([row], lang=lang)
    text = '\n'.join(_subtree_texts(
        _elements_with_class(client, dp.PANEL_EXPANSION_CLASS)[0]
        if _elements_with_class(client, dp.PANEL_EXPANSION_CLASS)
        else list(client.elements.values())[0]))
    assert dp._RELATED_ROW_COPY['display_missing'][lang] in text, text
    assert ds.missing_title(lang) not in text, (
        'the row still reports a missing WORK TITLE for an unnamed MANUSCRIPT')


@pytest.mark.parametrize('lang', LANGS)
def test_an_expansion_row_shows_what_the_catalogue_calls_the_manuscript(lang):
    """"Catalogued as: <title>", the same line the findings page carries -- so a
    reader comparing our identification against the library's can do it here
    too, which is the whole point of the expansion."""
    row = surface_safe_expansion(_expansion_source())
    marker = 'Catalogue Title Probe'
    client = _render_expansion_rows(
        [row], lang=lang, catalogue_title=lambda _row: marker)
    text = '\n'.join(_subtree_texts(list(client.elements.values())[0]))
    assert marker in text, f'the catalogue title is absent: {text}'
    from web.components.findings_rows import copy_text as _fc
    assert _fc('catalogue_title_label', lang) in text, (
        'the title renders with no label introducing it as the catalogue\'s')


@pytest.mark.parametrize('lang', LANGS)
def test_an_artifact_unnamed_row_is_named_from_the_catalogue_instead(lang):
    """The unresolved state is the LAST resort, not the first.

    `manuscript_display` covers only `shipped`/`human_confirmed` carriers, so
    every `review_only` one is absent -- 6,884 of 46,390, i.e. exactly the rows
    behind "Show more possible matches", which made the named-failure string the
    NORMAL state here rather than the rare one (owner report, 2026-08-07: three
    on a single screen).
    """
    row = surface_safe_expansion(_expansion_source(
        display_missing=True, library_code=None, shelfmark_display=None))
    client = _render_expansion_rows(
        [row], lang=lang,
        catalogue_identity=lambda _r: {'library_code': 'RNL',
                                       'shelfmark_display': 'Ms. EVR ARAB I 1868'})
    text = '\n'.join(_subtree_texts(list(client.elements.values())[0]))
    assert 'Ms. EVR ARAB I 1868' in text and 'RNL' in text, text
    assert dp._RELATED_ROW_COPY['display_missing'][lang] not in text, (
        'the row was named AND still reports itself unnamed')
    # ...and the recovered name is a LINK, like every other named row.
    assert any(h.startswith(f"/browse?sys_id={row['representative_sys_id']}")
               for h in _client_hrefs(client))


@pytest.mark.parametrize('lang', LANGS)
def test_a_row_the_catalogue_cannot_name_either_keeps_the_honest_state(lang):
    """A fallback that silently blanked would be worse than the string it
    replaced. When neither source can name the manuscript, the row says so."""
    row = surface_safe_expansion(_expansion_source(
        display_missing=True, library_code=None, shelfmark_display=None))
    for resolver in (None, lambda _r: None):
        client = _render_expansion_rows([row], lang=lang, catalogue_identity=resolver)
        text = '\n'.join(_subtree_texts(list(client.elements.values())[0]))
        assert dp._RELATED_ROW_COPY['display_missing'][lang] in text, (
            f'{resolver!r}: an unnameable row rendered neither a name nor the '
            'unresolved state')


def test_the_fallback_never_overrides_a_name_the_artifact_supplied():
    """The sidecar stays authoritative wherever it HAS an answer -- the fallback
    fills a gap and never competes. A resolver that fired on a named row would
    let a stale catalogue silently contradict the served artifact."""
    row = surface_safe_expansion(_expansion_source())          # display_missing=False
    client = _render_expansion_rows(
        [row], catalogue_identity=lambda _r: {'library_code': 'WRONG',
                                              'shelfmark_display': 'Wrong 1.1'})
    text = '\n'.join(_subtree_texts(list(client.elements.values())[0]))
    assert row['shelfmark_display'] in text
    assert 'Wrong 1.1' not in text and 'WRONG' not in text, (
        'the catalogue fallback overrode the artifact\'s own name')


def test_an_expansion_row_omits_the_catalogue_line_entirely_when_unresolved():
    """~14% of manuscripts have no title in libraries.csv, and an unresolved
    title renders NOTHING -- not an empty element and not a placeholder."""
    row = surface_safe_expansion(_expansion_source())
    for resolver in (None, lambda _row: None, lambda _row: ''):
        client = _render_expansion_rows([row], catalogue_title=resolver)
        classes = [c for el in client.elements.values()
                   for c in (getattr(el, '_classes', None) or [])]
        assert 'gs-findings-row-catalogue-title' not in classes, (
            f'an empty catalogue-title row was rendered for {resolver!r}')


# ===========================================================================
# THE ENVELOPE SCAN.
# ===========================================================================

def panel_envelopes(seed: Optional[str] = None) -> List[Tuple[str, Mapping[str, Any]]]:
    """Every envelope this surface consumes: the four eager reads, the lazy
    related-pages read and the expansion.

    `seed` plants a value in the fields that carry FREE TEXT out of the sidecar
    -- the recorded work title and the shelfmark -- for the masking positive
    control."""
    titled = {'neutral_title': seed} if seed else {}
    claims = [surface_safe_claim(_claim_source(**titled))]
    works = [surface_safe_work_summary(_work_summary_source(**titled))]
    related = [surface_safe_related_page(_related_page_source())]
    expansion = [surface_safe_expansion(_expansion_source(
        **({'shelfmark_display': seed} if seed else {})))]
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
                rendered_relation=None))], 1,
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
def test_FP_D06A_PRACTICAL_LIMITATIONS_NEED_NO_EXCEPTION(lang):
    """The replacement Help copy is qualitative in its own right.

    It passes the honesty gate whether scanned as part of the old broad card
    scope or through the legacy paragraph marker; the marker remains only for
    stable render coverage while the confidence-band report is retired.
    """
    import html as _html
    from web.pages.help import _CONFIDENCE_SECTION_CLASS, _LIMITATIONS_TEXT
    sentence = _LIMITATIONS_TEXT[lang]
    card = f'<div class="{_CONFIDENCE_SECTION_CLASS}">{_html.escape(sentence)}</div>'
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

MASKING_SCRIPT = 'scripts/check_atlas_masking.py'
PANEL_MODULE_PATH = 'web/components/discovery_panel.py'

#: A FABRICATED needle. The real restricted patterns live only in the gitignored
#: file `MASKING_SCAN_PATTERNS_FILE` points at; nothing in this repository may
#: ever carry one, which is the whole rule (D-25). This token exists so the
#: capture-and-scan PIPELINE can be proved able to fail without a secret.
_FABRICATED_NEEDLE = 'ZZQQ-FABRICATED-MASKING-NEEDLE-ZZQQ'


def _panel_ui_emitting_functions() -> Dict[str, Any]:
    """Every function in the panel module that can put something on a screen,
    DERIVED from what it CALLS -- never from what it is named.

    THE NAMING RULE WAS NOT A DERIVATION. The previous version collected
    top-level functions whose name (private prefix stripped) began with
    `render_`, and called that "derived". It is a convention, and this module
    already broke it twice: `_neutral_chip` draws the relation chip and
    `_service_state_block` draws every outage state, and neither was collected.
    A new data-bearing renderer with a different name could therefore put a
    restricted corpus name on a surface while `expected == exercised` stayed
    equal and the masking scan stayed green (code review round 15, finding 2).

    The rule here is a property of the CODE: a function emits UI if its body
    calls `ui.<anything>`, or if it calls another function that does. That
    closure is what makes it drift-proof -- rename every function in the module
    and the set is identical.

    Returns `{name: ast node}` so a caller can report a miss by NAME and LINE.
    """
    import ast
    tree = ast.parse(_read(PANEL_MODULE_PATH))
    by_name: Dict[str, Any] = {
        node.name: node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def _calls_ui(node) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                base = child.func.value
                if isinstance(base, ast.Name) and base.id == 'ui':
                    return True
        return False

    emitting = {name for name, node in by_name.items() if _calls_ui(node)}
    # ...and the transitive closure: a function that only delegates still puts
    # its arguments on a screen through the one it delegates to.
    changed = True
    while changed:
        changed = False
        for name, node in by_name.items():
            if name in emitting:
                continue
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) \
                        and child.func.id in emitting:
                    emitting.add(name)
                    changed = True
                    break
    return {name: by_name[name] for name in sorted(emitting)}


def _panel_render_entry_points() -> frozenset:
    """The UI-emitting function NAMES. Kept as a thin alias so the capture's
    instrumentation and the coverage assertions read from one derivation."""
    return frozenset(_panel_ui_emitting_functions())


def _module_executable_lines(path: str, source: str) -> frozenset:
    """Every line the COMPILER emitted bytecode for, including nested function
    bodies, comprehensions and lambdas.

    Asking Python's own compiler is the point: an AST walk has to decide for
    itself what counts as a statement, and every such decision is a place where
    "the line was not required" and "the line was never run" become
    indistinguishable.
    """
    import types
    code = compile(source, path, 'exec')
    lines: set = set()
    stack = [code]
    while stack:
        current = stack.pop()
        for _start, _end, lineno in current.co_lines():
            if lineno:
                lines.add(lineno)
        stack.extend(const for const in current.co_consts
                     if isinstance(const, types.CodeType))
    return frozenset(lines)


def _panel_required_lines() -> Dict[str, frozenset]:
    """`{function name: the lines its body must execute}`.

    The signature lines are excluded -- a `def` runs at IMPORT, so requiring it
    would assert nothing about the capture. Everything from the first body
    statement to the end of the function is required, INCLUDING nested handler
    bodies: a click handler that paints artifact text is a surface a reader
    reaches, and a capture that never takes it is a capture that never looked.
    """
    source = _read(PANEL_MODULE_PATH)
    executable = _module_executable_lines(PANEL_MODULE_PATH, source)
    required: Dict[str, frozenset] = {}
    for name, node in _panel_ui_emitting_functions().items():
        body = getattr(node, 'body', None)
        if not body:                                         # pragma: no cover
            continue
        first = body[0].lineno
        last = node.end_lineno or first
        required[name] = frozenset(
            line for line in executable if first <= line <= last)
    return required


class _PanelLineTracer:
    """Records which lines of the panel module actually ran.

    `sys.settrace` rather than a coverage dependency: `coverage` is not in any
    requirements file, and a masking gate that only runs where an optional
    package happens to be installed is a gate that reports success without
    performing its check -- the exact defect this suite keeps closing.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self.executed: set = set()
        self._previous = None

    def _trace(self, frame, event, _arg):
        if frame.f_code.co_filename != self.path:
            return None
        if event == 'line':
            self.executed.add(frame.f_lineno)
        return self._trace

    def __enter__(self):
        self._previous = sys.gettrace()
        sys.settrace(self._trace)
        return self

    def __exit__(self, *_exc):
        sys.settrace(self._previous)
        return False


def _own_texts(node) -> List[str]:
    """Every string this ONE element carries -- text and props alike.

    Deliberately unfiltered by class. The previous capture skipped any element
    with no CSS class, which is a rule about styling and not about what a reader
    can read.
    """
    out = []
    for attr in ('text', '_text', 'content'):
        value = getattr(node, attr, None)
        if isinstance(value, str) and value.strip():
            out.append(value)
    for value in (getattr(node, '_props', None) or {}).values():
        if isinstance(value, str) and value.strip():
            out.append(value)
    return out


async def _drive_click_handlers(client, rounds: int = 2) -> None:
    """Click EVERY control the render produced, twice, and paint what comes back.

    Generic on purpose. Half this panel's surface is behind a disclosure: the
    per-work expansion, the related-page rows and the manuscript pane's overflow
    are all painted by a handler, from artifact data, only after a reader clicks.
    A capture that renders and stops has therefore never looked at the surfaces
    where a restricted corpus name is MOST likely to arrive.

    Nothing here enumerates a control. The sweep walks whatever was rendered, so
    a disclosure added tomorrow is driven without this function being edited --
    which is the difference between a capture that keeps up with the renderer
    and a list that goes stale.

    Twice, because these handlers TOGGLE: the second pass takes the closing
    branch, and a lazily-loaded body is only painted on the pass that opens it.
    """
    import inspect

    for _ in range(max(1, rounds)):
        listeners = []
        for element in list(client.elements.values()):
            for listener in list(
                    getattr(element, '_event_listeners', {}).values()):
                if getattr(listener, 'type', None) == 'click':
                    listeners.append((element, listener.handler))
        for element, handler in listeners:
            if handler is None:
                continue
            slot = getattr(element, 'parent_slot', None)
            try:
                with slot if slot is not None else client:
                    result = (handler()
                              if not inspect.signature(handler).parameters
                              else handler(None))
                    if inspect.isawaitable(result):
                        await result
            except Exception:                                # noqa: BLE001
                # A handler that fails is a finding for another test, not a
                # reason to abandon the capture: the surfaces already painted
                # still have to be scanned.
                logging.getLogger(__name__).debug(
                    'capture: click handler raised', exc_info=True)


def _render_capture(paint, *, drive: bool = True) -> str:
    """Run `paint()` in a real client, DRIVE its controls, and return every
    rendered string."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_panel_masking_capture')) as client:
            with client:
                paint()
                if drive:
                    await _drive_click_handlers(client)
        holder['client'] = client

    asyncio.run(_run())
    parts: List[str] = []
    for element in holder['client'].elements.values():
        parts.extend(_own_texts(element))
    return '\n'.join(parts)


def _generic_group_bundle(lang: str, seed_title: Optional[str] = None
                          ) -> PanelServiceBundle:
    """Two DIFFERENT works on a BYTE-IDENTICAL span (D-13d).

    The only input that produces a generic-shared-text group, and therefore the
    only way `_render_generic_group` is ever painted. Different authors, so
    `works_related_by_title` cannot collapse them as one work at two
    granularities.
    """
    base = _claim_source(span_start=0, span_end=555, matched_letters=555)
    rows = [
        surface_safe_claim(dict(
            base, claim_id='a' * 64, work_id='w000101',
            canonical_work_id='w000101', display_work_id='w000101',
            neutral_title=seed_title or 'Tur Orach Chaim',
            author='Jacob ben Asher')),
        surface_safe_claim(dict(
            base, claim_id='b' * 64, work_id='w000102',
            canonical_work_id='w000102', display_work_id='w000102',
            neutral_title='Yalkut Shimoni on Nevi\'im',
            author='Shimon ha-Darshan')),
    ]
    return PanelServiceBundle(
        claims=make_envelope(STATUS_OK, rows, len(rows), meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False}),
        page_ids=make_envelope(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                               meta={'sys_id': '990000000000000944', 'resolved': True,
                                     'truncated': False, 'volume_ie': 'IE1'}),
        manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': True, 'lang': lang}),
        related_count=make_envelope(STATUS_OK, [], 3, meta={
            'unit': 'distinct_opposite_pages'}),
        related_rows=None,
        lang=lang,
    )


def _scope_variant_bundle(lang: str, variant: str,
                          seed_title: Optional[str] = None) -> PanelServiceBundle:
    """A bundle whose PAGE-SCOPE read varies, so the manuscript pane's three
    non-happy states are each reachable.

    Every fixture in this suite used to resolve the page scope, which made the
    unresolved, truncated and outage panes structurally unrenderable -- and a
    state nothing can render is a state nothing scans.
    """
    base = bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK,
                      seed_title=seed_title)
    page_ids = {
        # OUR plumbing failed: recoverable, so the model supplies a retry.
        'outage': unavailable_envelope(meta={'reason': 'sidecar_not_serving'}),
        # A fact about the manuscript: no retry can change it.
        'unresolved': make_envelope(STATUS_OK, [], 0, meta={
            'sys_id': '990000000000000944', 'resolved': False,
            'truncated': False, 'volume_ie': None}),
        # Resolved, but only in part -- the pane must say its total covers the
        # resolved pages only.
        'truncated': make_envelope(
            STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1, meta={
                'sys_id': '990000000000000944', 'resolved': True,
                'truncated': True, 'volume_ie': 'IE1'}),
    }[variant]
    return PanelServiceBundle(
        claims=base.claims, page_ids=page_ids,
        manuscript_works=base.manuscript_works,
        related_count=base.related_count, related_rows=base.related_rows,
        lang=lang,
    )


def _annotated_row_bundle(lang: str, seed_title: Optional[str] = None
                          ) -> PanelServiceBundle:
    """A claim carrying the row's TWO optional sub-lines.

    `granularity_subline` needs a second work RELATED BY TITLE on the same span
    (the D-13d nested entry), and `low_coverage_note` needs the coverage marker.
    Both are model-emitted TEXT on a shipped row, and neither was in the capture
    -- so the masking scan had never looked at either.
    """
    base = _claim_source(span_start=0, span_end=555, matched_letters=555,
                         low_coverage_marker=True)
    title = seed_title or 'Commentary on Song of Songs'
    rows = [
        surface_safe_claim(dict(
            base, claim_id='c' * 64, work_id='w000201',
            canonical_work_id='w000201', display_work_id='w000201',
            neutral_title=title, author='Rashi')),
        # Same author, title one granularity finer: the model folds it in as a
        # nested entry rather than as a separate generic group.
        surface_safe_claim(dict(
            base, claim_id='d' * 64, work_id='w000202',
            canonical_work_id='w000202', display_work_id='w000202',
            neutral_title=f'{title}, part two', author='Rashi')),
    ]
    return PanelServiceBundle(
        claims=make_envelope(STATUS_OK, rows, len(rows), meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False}),
        page_ids=make_envelope(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                               meta={'sys_id': '990000000000000944', 'resolved': True,
                                     'truncated': False, 'volume_ie': 'IE1'}),
        manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': True, 'lang': lang}),
        related_count=make_envelope(STATUS_OK, [], 3, meta={
            'unit': 'distinct_opposite_pages'}),
        related_rows=None,
        lang=lang,
    )


def _gated_rows_bundle(lang: str, seed_title: Optional[str] = None
                       ) -> PanelServiceBundle:
    """A row behind the "more matches" disclosure, WITH the disclosure open.

    `show_more` is what opens the collapsed level, and a gated row is the only
    thing inside it. Every fixture in this suite left `show_more` at its default
    and put every row in the default-visible level, so the collapsed level's own
    row loop and its `open` attribute had never been rendered -- a whole
    reader-reachable half of `_render_level`, with claim text on it.
    """
    base = _claim_source(main_pool=False, main_pool_reason='low_coverage',
                         low_coverage_marker=True)
    rows = [surface_safe_claim(dict(
        base, claim_id='e' * 64, work_id='w000301',
        canonical_work_id='w000301', display_work_id='w000301',
        neutral_title=seed_title or 'Commentary on Song of Songs'))]
    return PanelServiceBundle(
        claims=make_envelope(STATUS_OK, rows, len(rows), meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False}),
        page_ids=make_envelope(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                               meta={'sys_id': '990000000000000944', 'resolved': True,
                                     'truncated': False, 'volume_ie': 'IE1'}),
        manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': True, 'lang': lang}),
        related_count=make_envelope(STATUS_OK, [], 2, meta={
            'unit': 'distinct_opposite_pages'}),
        related_rows=None,
        lang=lang,
        show_more=True,
    )


def _lazy_read_envelopes(seed: Optional[str], lang: str) -> Dict[str, Any]:
    """What the two LAZY reads hand their painters, in each regime the capture
    drives: a populated answer, an outage, and a raise."""
    expansion_items = [
        surface_safe_expansion(_expansion_source(
            **({'shelfmark_display': seed} if seed else {}))),
        # An absent manuscript_display row is FLAGGED, never blanked -- and the
        # flag is rendered text, so it belongs in the capture.
        surface_safe_expansion(_expansion_source(
            unit_id='unit-3', display_missing=True, relations_differ=True,
            anchor_rendered_relation=ids.RENDERED_RELATION_SHARED_TEXT)),
    ]
    del lang
    return {
        'expansion_ok': make_envelope(
            STATUS_OK, expansion_items, 5684,
            meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                  'filter_basis': 'displayed_band', 'anchor_excluded': True}),
        'related_ok': make_envelope(
            STATUS_OK,
            [surface_safe_related_page(_related_page_source(
                **({'shelfmark_display': seed} if seed else {}))),
             # A related page whose manuscript is NOT in the display index. The
             # row then renders a NAMED state -- never the composite page id --
             # and that named state is rendered text, so it belongs in the
             # capture beside the shelfmark it replaces.
             surface_safe_related_page(_related_page_source(
                 related_page_id='990000000000000946_IE1_P000002_FL11',
                 sys_id=None, library_code=None, shelfmark_display=None,
                 page_number=None, volume_ie=None, display_missing=True)),
             # A row that IS named but has no manuscript to link to. It renders
             # the shelfmark as PLAIN TEXT rather than as a link -- the branch
             # `browse_url` returning None selects (2026-08-08) -- and plain text
             # on a screen is exactly what this scan exists to look at. Before
             # the shared builder, this row emitted `/browse?sys_id=None`, a dead
             # end dressed as a destination; the sibling expansion renderer
             # already had a test forbidding that and this one did not.
             surface_safe_related_page(_related_page_source(
                 related_page_id='990000000000000947_IE1_P000005_FL13',
                 sys_id=None, display_missing=False))],
            3, meta={'unit': 'distinct_opposite_pages'}),
        'outage': unavailable_envelope(meta={'reason': 'query_failed'}),
    }


def _divergent_rows_bundle(lang: str, seed_title: Optional[str] = None
                           ) -> PanelServiceBundle:
    """A row carrying the per-row `catalogue_divergent` chip (owner ruling,
    2026-08-13, amending ruling F).

    Nothing else in this suite produces one: every other fixture carries the
    fail-closed `not_checked` shade, so the `_render_identification_row`
    branch that paints `divergence_chip` was never reached.

    The row is deliberately main-pool and default-eligible: a level-1 row by
    every test except this one, which is what makes the capture cover the
    ORTHOGONAL case (a divergent row that STAYS in the default level) rather
    than a weak row that would have been screened onto "more matches" anyway.
    The old fourth disclosure level and its own `show_divergence` visibility
    flag are both gone -- the chip is unconditional on the row, not gated
    behind a separate toggle.
    """
    base = _claim_source(novelty_status='diverges_work')
    rows = [surface_safe_claim(dict(
        base, claim_id='f' * 64, work_id='w000302',
        canonical_work_id='w000302', display_work_id='w000302',
        neutral_title=seed_title or 'Commentary on Lamentations'))]
    return PanelServiceBundle(
        claims=make_envelope(STATUS_OK, rows, len(rows), meta={
            'page_id': '990000000000000944_IE1_P000002_FL3', 'include_review': False}),
        page_ids=make_envelope(STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1,
                               meta={'sys_id': '990000000000000944', 'resolved': True,
                                     'truncated': False, 'volume_ie': 'IE1'}),
        manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
            'page_scope_resolved': True, 'lang': lang}),
        related_count=make_envelope(STATUS_OK, [], 2, meta={
            'unit': 'distinct_opposite_pages'}),
        related_rows=None,
        lang=lang,
    )


def _excerpt_envelope(seed: Optional[str], lang: str):
    """An OK excerpt envelope with the seed standing in EVERY text piece --
    both context windows, both spans, the attribution -- plus word-highlight
    intervals and the JA brace markup, so the panel's loader-gated disclosure
    (excerpt-v1) paints every branch of the piece composer onto this surface.
    """
    from shared.discovery_surface_projection import surface_safe_excerpt
    piece = seed or f'excerpt capture text ({lang})'
    row = surface_safe_excerpt({
        'identification_id': 'c' * 64,
        'evidence_id': 'b' * 64,
        'a_page_id': '990000000000000944_IE1_P000002_FL3',
        'frag_before': piece, 'frag_span': piece, 'frag_after': piece,
        'frag_clipped': False,
        'work_before': piece, 'work_span': '{' + piece + '}',
        'work_after': piece, 'work_clipped': False,
        'work_source': 'reprojected',
        'attribution': piece, 'n_spans': 2, 'text_layer': 'htr',
        'frag_hl': [[0, 4]], 'work_hl': [[0, 4]],
        'work_markup': 'ja_braces',
    })
    return make_envelope(STATUS_OK, [row], 1, meta={})


def _render_every_panel_surface(seed: Optional[str] = None) -> str:
    """Every surface a READER can see on this panel, as text.

    The panel BODY is not the surface, and neither is the first paint. The live
    seam renders the ENTRY CONTROL through a second entry point; the per-work
    expansion, the related-page rows and the manuscript pane's overflow are
    painted from artifact data only after a reader CLICKS; and three of the
    manuscript pane's states need a page-scope read that did not simply succeed.
    Capturing the body's first paint alone established that the scanner can read
    a file.

    Coverage of this claim is not asserted in prose. `_capture_panel_surface`
    traces the panel module while this runs, and
    `test_the_capture_executes_every_line_of_every_ui_emitting_function` fails
    -- naming the function, the line and the source -- for anything that did not
    run. Every control the render produced is CLICKED (`_drive_click_handlers`),
    under three lazy-read regimes, so a disclosure added later is driven without
    this function being edited.
    """
    title = f'Commentary on {seed}' if seed else None
    parts: List[str] = []

    def _noop_retry():
        return None

    def _noop_toggle():
        return None

    async def _noop_reload():
        return None

    def _paint_body(model, entry: bool = False):
        def _paint():
            dp.render_discovery_panel_body(
                model, on_retry=_noop_retry,
                page_id='990000000000000944_IE1_P000002_FL3')
            if entry:
                # The LIVE entry control, rendered by the same second entry
                # point `update_discovery_panel_section` uses. A reader sees
                # this one BEFORE the body.
                dp.render_discovery_entry_control(model, on_toggle=_noop_toggle)
        return _paint

    for _name, profile in MANUSCRIPT_PROFILES:
        for lang in LANGS:
            for status in SERVICE_STATES:
                model = build_panel_rows(
                    bundle_for(profile, lang, status, seed_title=title))
                parts.append(_render_capture(_paint_body(model, entry=True)))

    for lang in LANGS:
        # The generic-shared-text group, the row's two optional sub-lines, and
        # the three non-happy page-scope states.
        for bundle in (
            _generic_group_bundle(lang, seed_title=title),
            _annotated_row_bundle(lang, seed_title=title),
            _scope_variant_bundle(lang, 'outage', seed_title=title),
            _scope_variant_bundle(lang, 'unresolved', seed_title=title),
            _scope_variant_bundle(lang, 'truncated', seed_title=title),
            _gated_rows_bundle(lang, seed_title=title),
            _divergent_rows_bundle(lang, seed_title=title),
        ):
            parts.append(_render_capture(_paint_body(build_panel_rows(bundle))))

        # The loader-gated text-vs-text disclosure (excerpt-v1, 2026-08-13):
        # rendered once WITH a loader, so the toggle exists, the click sweep
        # opens it, and the excerpt envelope -- the seed standing in every
        # text piece -- is painted onto THIS surface. Without this the new
        # branch in `_render_identification_row` never executes and the
        # line-granular gate rightly fails.
        excerpt_env = _excerpt_envelope(seed, lang)

        async def _load_excerpt(_row, _env=excerpt_env):
            return _env

        excerpt_model = build_panel_rows(
            bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK,
                       seed_title=title))
        parts.append(_render_capture(
            lambda m=excerpt_model, le=_load_excerpt: (
                dp.render_discovery_panel_body(
                    m, on_retry=_noop_retry,
                    page_id='990000000000000944_IE1_P000002_FL3',
                    load_excerpt=le))))

        # ...and once with NO page id, the shape the seam produces when the
        # page scope never resolved: the lazy related-pages read must decline
        # rather than query for nothing.
        no_page_id = build_panel_rows(
            bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK,
                       seed_title=title))
        parts.append(_render_capture(
            lambda m=no_page_id: dp.render_discovery_panel_body(
                m, on_retry=_noop_retry, page_id=None)))

        # A model whose entry control is HIDDEN -- the ONE branch of the entry
        # point that renders nothing, and the one a reader sees most often.
        empty = PanelServiceBundle(
            claims=make_envelope(STATUS_OK, [], 0, meta={
                'page_id': '990000000000000944_IE1_P000002_FL3',
                'include_review': False}),
            page_ids=make_envelope(
                STATUS_OK, ['990000000000000944_IE1_P000002_FL3'], 1, meta={
                    'sys_id': '990000000000000944', 'resolved': True,
                    'truncated': False, 'volume_ie': 'IE1'}),
            manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
                'page_scope_resolved': True, 'lang': lang}),
            related_count=make_envelope(STATUS_OK, [], 0, meta={
                'unit': 'distinct_opposite_pages'}),
            related_rows=None, lang=lang)
        empty_model = build_panel_rows(empty)
        parts.append(_render_capture(
            lambda m=empty_model: dp.render_discovery_entry_control(
                m, on_toggle=_noop_toggle)))

        # ...and the LAUNCH-HIGHLIGHT variant, whose badge is a rendered word the
        # line-granular gate requires a capture to paint.
        #
        # A VISIBLE model, deliberately NOT `empty_model` above: that one is the
        # hidden-entry-control fixture, so the renderer returns before it reaches
        # the highlight at all -- the capture ran, painted nothing, and the line
        # gate caught it. Exactly the failure this gate exists for, on the first
        # try.
        visible_model = build_panel_rows(
            bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK,
                       seed_title=title))
        parts.append(_render_capture(
            lambda m=visible_model: dp.render_discovery_entry_control(
                m, on_toggle=_noop_toggle, highlight_new=True)))

        # ...and the SAME true page zero with a NON-EMPTY manuscript read: a
        # claim-less folio of a claim-rich manuscript (the measured case is RNL
        # Ms. Evr. Antonin A 1, 483 claims across 396 of 492 pages and none on
        # page 1). The control is visible there and the panel leads with the
        # manuscript pane, so BOTH branches of the entry label and BOTH pane
        # orders reach the scanner -- the manuscript pane draws artifact titles.
        elsewhere = dataclasses.replace(
            empty,
            manuscript_works=make_envelope(STATUS_OK, [
                surface_safe_work_summary(_work_summary_source(
                    canonical_work_id='w000001', display_work_id='w000001',
                    neutral_title=title or 'Work 1', page_count=12))], 1,
                meta={'page_scope_resolved': True, 'lang': lang}))
        parts.append(_render_capture(
            _paint_body(build_panel_rows(elsewhere), entry=True)))

        with_rows = bundle_for(dict(MANUSCRIPT_PROFILES[1][1]), lang, STATUS_OK,
                               seed_title=title).with_related_rows(
            make_envelope(STATUS_OK, [surface_safe_related_page(
                _related_page_source())], 1,
                meta={'unit': 'distinct_opposite_pages'}))
        parts.append(_render_capture(_paint_body(build_panel_rows(with_rows))))

        # The per-work expansion body, driven DIRECTLY as well as through its
        # loader: the pager appears only above a page's worth of rows, and the
        # outage form is what a failed lazy read paints.
        envelopes = _lazy_read_envelopes(seed, lang)
        for envelope in (envelopes['expansion_ok'], envelopes['outage']):
            state: Dict[str, Any] = {'open': True, 'page': 1, 'loaded': True}
            parts.append(_render_capture(lambda e=envelope, s=state, ln=lang: (
                dp._render_expansion_envelope(e, ln, s, _noop_reload, page_size=25))))
            # ...and AGAIN with a catalogue title resolving, because that line
            # emits FREE TEXT from libraries.csv onto the surface and the scan is
            # line-granular: without this the "Catalogued as: <title>" line would
            # never be painted, and nothing scanned would cover what it emits.
            # `seed` plants the restricted-string probe in the title itself, so
            # the positive control can prove this path is really covered.
            parts.append(_render_capture(lambda e=envelope, s=state, ln=lang: (
                dp._render_expansion_envelope(
                    e, ln, s, _noop_reload, page_size=25,
                    catalogue_title=lambda _row, _t=(
                        seed or 'Commentary on Song of Songs'): _t))))
            # ...and once more with an ARTIFACT-UNNAMED row recovered from the
            # catalogue, because that path emits a library code and a shelfmark
            # that never passed through the sidecar's projection -- a second
            # egress for free text, and the scan is line-granular.
            unnamed = make_envelope(
                STATUS_OK,
                [surface_safe_expansion(_expansion_source(
                    display_missing=True, library_code=None,
                    shelfmark_display=None))],
                1, meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                         'filter_basis': 'displayed_band', 'anchor_excluded': True})
            parts.append(_render_capture(lambda e=unnamed, ln=lang: (
                dp._render_expansion_envelope(
                    e, ln, {'open': True, 'page': 1, 'loaded': True},
                    _noop_reload, page_size=25,
                    catalogue_identity=lambda _row, _s=(seed or 'Ms. EVR ARAB I 1868'): {
                        'library_code': 'RNL', 'shelfmark_display': _s}))))

    # ...and the SAME surfaces again with the lazy reads answering, so the
    # loaders, their painters and their failure branches all run. Three regimes:
    # a populated answer, a named outage, and a raise (the branch that must
    # never take the panel down with it).
    parts.extend(_capture_under_lazy_read_regimes(seed, title, _paint_body))

    return '\n'.join(parts)


def _capture_under_lazy_read_regimes(seed: Optional[str], title: Optional[str],
                                     paint_body) -> List[str]:
    """Re-render the panel with `web.discovery`'s two LAZY reads stubbed, so the
    disclosure handlers the click sweep drives have something to paint."""
    from unittest.mock import patch

    from web import discovery as _discovery

    parts: List[str] = []
    for lang in LANGS:
        envelopes = _lazy_read_envelopes(seed, lang)

        async def _ok_expansion(*_a, **_k):
            return envelopes['expansion_ok']

        async def _ok_related(*_a, **_k):
            return envelopes['related_ok']

        async def _outage(*_a, **_k):
            return envelopes['outage']

        async def _raises(*_a, **_k):
            raise DiscoveryUnavailable('capture regime: the lazy read failed')

        regimes = (
            (_ok_expansion, _ok_related),
            (_outage, _outage),
            (_raises, _raises),
        )
        for expansion_read, related_read in regimes:
            model = build_panel_rows(
                bundle_for(dict(MANUSCRIPT_PROFILES[6][1]), lang, STATUS_OK,
                           seed_title=title))
            with patch.object(_discovery, 'get_work_expansion_enveloped',
                              expansion_read), \
                 patch.object(_discovery, 'get_related_pages_enveloped',
                              related_read):
                parts.append(_render_capture(paint_body(model)))
    return parts


def _capture_panel_surface(seed: Optional[str] = None) -> Dict[str, Any]:
    """The whole capture, plus WHICH render functions producing it were reached.

    The instrumentation is the honest part. Without it "the capture includes
    every surface" is a claim in a docstring, and this suite's characteristic
    failure has been exactly such a claim being wrong in a new place.
    """
    exercised = set()
    originals = {name: getattr(dp, name) for name in _panel_render_entry_points()}

    def _wrap(name, fn):
        def _recorder(*args, **kwargs):
            exercised.add(name)
            return fn(*args, **kwargs)
        return _recorder

    for name, fn in originals.items():
        setattr(dp, name, _wrap(name, fn))
    tracer = _PanelLineTracer(dp.__file__)
    try:
        with tracer:
            rendered = _render_every_panel_surface(seed)
    finally:
        for name, fn in originals.items():
            setattr(dp, name, fn)

    envelopes = '\n'.join(
        f'{name}: {json.dumps(envelope, ensure_ascii=False, default=str)}'
        for name, envelope in panel_envelopes(seed=seed))
    error_paths = '\n'.join(f'{mode}: {message}'
                            for mode, message in forced_error_paths())
    return {
        'rendered': rendered,
        'envelopes': envelopes,
        'error_paths': error_paths,
        'exercised': frozenset(exercised),
        'executed_lines': frozenset(tracer.executed),
        'text': '\n'.join((rendered, envelopes, error_paths)),
    }


@pytest.fixture(scope='module')
def masking_capture(tmp_path_factory):
    """The capture, written OUTSIDE the working tree.

    `tmp_path_factory` roots under pytest's basetemp, never the repo, so a
    stray capture can never itself trip `--scan-repo` -- the failure mode that
    would make this gate report its own artifact as a leak.
    """
    directory = tmp_path_factory.mktemp('discovery-panel-masking')
    capture = _capture_panel_surface()
    path = directory / 'panel_surface_capture.txt'
    path.write_text(capture['text'], encoding='utf-8')
    return {**capture, 'path': path}


#: Lines of a UI-emitting function that the capture CANNOT execute, each with
#: the reason it cannot. Keyed by `(function, exact source text)` rather than by
#: line number, so an edit above moves the anchor with the code instead of
#: silently exempting whatever slid into that line.
#:
#: THIS LIST IS THE HONEST PART. Round 15's finding 2 was that instrumentation
#: recorded a function being ENTERED and called that coverage, so an unexercised
#: BRANCH inside a captured function could still put a restricted corpus name on
#: a surface with the scan green. Requiring every line closes that -- but only if
#: the escape hatch cannot be widened quietly, which is what
#: `test_the_capture_exemption_list_has_not_grown` is for.
_CAPTURE_EXEMPT: Dict[Tuple[str, str], str] = {
    ('_render_expansion_envelope', 'ui.label(title)'):
        'unreachable BY CONTRACT, not by omission: `_expansion_work_title` reads '
        '`neutral_title`, and `SURFACE_EXPANSION_FIELDS` deliberately carries no '
        'title, so the projection can never deliver one. The branch exists so '
        'that a title added to the projection later routes through ruling R '
        'instead of being formatted inline. If that field is ever added, this '
        'exemption must be deleted and the capture must seed it.',
    ('_render_expansion_envelope', 'ui.label(shelfmark)'):
        'the un-linked fallback for a NAMED row with no `representative_sys_id`, '
        'unreachable BY SCHEMA: `discovery_evidence.sys_id` is NOT NULL, and '
        '`manuscript_display` is reached only by joining ON that sys_id -- so a '
        'row that has a shelfmark to render always has the sys_id to link it '
        '(verified against the served artifact: 0 NULL sys_ids in either table). '
        'The branch mirrors `findings_rows._render_shelfmark`, whose own rows come '
        'from `discovery_identification` and can carry a NULL. Kept rather than '
        'dropped so a future projection that does deliver a nameless-but-named '
        'row degrades to plain text instead of rendering `/browse?sys_id=None`; if '
        'the schema ever relaxes, delete this and seed the capture.',
}


def test_the_capture_paints_every_render_entry_point_the_panel_has(masking_capture):
    """The gap the round-13 BLOCKER named, at function granularity.

    The capture drove `render_discovery_panel_body` and nothing else, while the
    live seam ALSO renders `render_discovery_entry_control`. The expected set is
    DERIVED from the panel module -- by what each function CALLS, never by what
    it is NAMED (round 15, finding 2: the naming rule missed `_neutral_chip` and
    `_service_state_block`, both of which draw reader-visible text) -- and the
    exercised set is RECORDED by instrumenting those same functions.

    Entering a function is necessary and NOT sufficient; the line-level test
    below is the one that can see an unexercised branch.
    """
    expected = _panel_render_entry_points()
    assert len(expected) >= 12, (
        f'only {len(expected)} UI-emitting functions found in {PANEL_MODULE_PATH} '
        '-- the derivation is scanning the wrong tree')
    # The two the NAMING rule missed, named here so a regression to a
    # name-based derivation fails on the specific functions that motivated it.
    for name in ('_neutral_chip', '_service_state_block'):
        assert name in expected, (
            f'{name} draws reader-visible text but is not in the derived set -- '
            'the derivation has gone back to matching function NAMES')
    missing = sorted(expected - masking_capture['exercised'])
    assert not missing, (
        'these UI-emitting functions were never painted into the masking '
        'capture, so nothing scanned covers what they emit: ' + ', '.join(missing)
        + '. Drive them in `_render_every_panel_surface`, or -- if a surface '
        'genuinely cannot be captured -- say so here by name rather than '
        'letting the capture cover less than this suite claims.')
    unknown = sorted(masking_capture['exercised'] - expected)
    assert not unknown, f'the instrumentation recorded non-emitting functions: {unknown}'


def test_the_capture_executes_every_line_of_every_ui_emitting_function(masking_capture):
    """Coverage by LINE, not by function entry — round 15, finding 2.

    "The capture entered this renderer" says nothing about the branch inside it
    that prints `granularity_subline`, or the disclosure handler that paints
    artifact rows only after a click. Both of those were unexercised while the
    previous test was green, and both put projection text on a screen. A
    restricted corpus name arriving on either would have passed the scan.

    So the requirement is every executable line of every UI-emitting function,
    with the executed set RECORDED by a tracer over the real capture and the
    required set taken from Python's own compiler. A line that genuinely cannot
    run is named in `_CAPTURE_EXEMPT` WITH ITS REASON, and the sibling test
    fails if that list grows.
    """
    source_lines = _read(PANEL_MODULE_PATH).splitlines()
    executed = masking_capture['executed_lines']
    assert executed, 'the tracer recorded nothing — it is watching the wrong file'

    exempt_texts = {(fn, text) for fn, text in _CAPTURE_EXEMPT}
    unexercised: List[str] = []
    for name, lines in sorted(_panel_required_lines().items()):
        for line in sorted(lines - executed):
            text = source_lines[line - 1].strip()
            if (name, text) in exempt_texts:
                continue
            unexercised.append(f'{name} (line {line}): {text}')
    assert not unexercised, (
        'these lines of the panel\'s UI-emitting functions never ran during the '
        'masking capture, so nothing scanned covers what they can emit:\n  '
        + '\n  '.join(unexercised)
        + '\n\nDrive them in `_render_every_panel_surface` (the click sweep '
        'already drives every rendered control, so a new disclosure usually '
        'needs only a fixture that produces it), or -- if a line genuinely '
        'cannot be executed -- add it to `_CAPTURE_EXEMPT` with the reason and '
        'update the pinned count.')


def test_the_capture_exemption_list_has_not_grown():
    """The escape hatch, pinned.

    An exemption list nobody counts is a list that absorbs every inconvenient
    line until the coverage assertion means nothing. Each entry must name a
    reason, and adding one is a deliberate edit to this number in the same
    commit — not a quiet widening.
    """
    assert len(_CAPTURE_EXEMPT) == 2, (
        'the masking capture\'s exemption list changed size: '
        + repr(sorted(_CAPTURE_EXEMPT)))
    for key, reason in _CAPTURE_EXEMPT.items():
        assert len(reason) > 60, f'{key} is exempted without a stated reason'
    # Every exempted anchor must still EXIST in the module, or it is exempting
    # nothing while reading as though it covers something.
    source = _read(PANEL_MODULE_PATH)
    for function_name, text in _CAPTURE_EXEMPT:
        assert function_name in source and text in source, (
            f'the exemption for {function_name}/{text!r} no longer matches any '
            'line in the panel — delete it rather than leaving a dead excuse')


def test_the_capture_really_holds_the_rendered_surface(masking_capture):
    """Guards the OTHER tests. A clean scan over an empty (or trivially small)
    capture is the purest false green available here, and both scans below would
    report exactly that."""
    text = masking_capture['text']
    assert len(text) > 5000, f'the capture is {len(text)} chars -- it captured nothing'
    # Text from each of the three egress classes, so a capture that silently
    # lost one is caught by name rather than by a byte count.
    assert dp._MANUSCRIPT_PANE_SCOPE_NOTE['en'] in masking_capture['rendered'], (
        'no RENDERED panel text in the capture')
    assert 'distinct_opposite_pages' in masking_capture['envelopes'], (
        'no ENVELOPE in the capture')
    assert 'temporarily unavailable' in masking_capture['error_paths'], (
        'no ERROR PATH in the capture')
    for lang in LANGS:
        assert ds.retry_label(lang) in masking_capture['rendered'], (
            f'{lang}: the outage renders are missing')
    # The ENTRY CONTROL specifically -- the surface the round-13 BLOCKER named.
    # Its label is read from the shipped renderer's own translation call, so a
    # wording change moves this with it.
    from web.translations import tr
    assert tr('Computed identifications') in masking_capture['rendered'], (
        'the entry control is not in the capture')

    # The surfaces round 15's finding 2 named: rendered text behind a BRANCH or
    # behind a CLICK. Each one is model- or projection-derived, i.e. exactly
    # where a restricted corpus name arrives, and none of them was in the
    # capture while the function-entry test was green. Asserted by their own
    # shipped wording, so a rewording moves these with it.
    rendered = masking_capture['rendered']
    for lang in LANGS:
        assert ds.low_coverage_note(lang) in rendered, (
            f'{lang}: the row\'s low-coverage note is not in the capture')
        subline_shape = ds.granularity_subline('', lang).replace('{}', '').strip()
        assert subline_shape and subline_shape.split()[0] in rendered, (
            f'{lang}: the row\'s granularity sub-line is not in the capture')
        # Painted ONLY by the expansion loader's own painter, which only runs
        # when a control is clicked.
        #
        # Reads the row's UNNAMED-MANUSCRIPT wording, which this canary followed
        # when the expansion row stopped borrowing `ds.missing_title` (2026-08-07):
        # that string means "the WORK's title could not be resolved", and the
        # expansion was using it for a manuscript with no `manuscript_display`
        # row, so three rows told the reader a title was unavailable while the
        # work was named in the heading directly above. Same canary, same
        # property -- it still fails if the click sweep stops reaching the
        # lazily-painted body -- now keyed to the string that path really emits.
        assert dp._RELATED_ROW_COPY['display_missing'][lang] in rendered, (
            f'{lang}: the expansion body is not in the capture, so the click '
            'sweep is not reaching the lazily-painted surfaces')
        assert ds.disclosure_toggle(ds.TOGGLE_MORE_MATCHES, lang) in rendered, (
            f'{lang}: the gated disclosure level is not in the capture')


def test_the_masking_scanner_can_actually_fail_on_this_capture(tmp_path):
    """The POSITIVE CONTROL, and it needs no secret -- which is the point.

    The needle enters through DATA and comes out through the RENDER. A whole
    second capture is built with a fabricated title planted on every claim and
    work summary, exactly where a restricted name arriving from the sidecar
    would sit; the needle is then required to be present in the RENDERED half
    before anything is scanned. Appending it to the finished capture file --
    what this test used to do -- proves only that the scanner can read a file,
    which was the second half of the round-13 BLOCKER.

    The planted capture must be reported and the clean one must not. Together
    those prove the pipeline this suite claims -- render -> capture -> scan --
    is wired end to end and is capable of returning non-zero.

    This test can NEVER skip. It is what stops
    `test_the_rendered_panel_and_repo_pass_the_real_masking_scan` from being the
    only evidence, and it is why a missing pattern file is a gap in COVERAGE
    (which patterns are searched for) rather than a gap in the MECHANISM.
    """
    planted = _capture_panel_surface(seed=_FABRICATED_NEEDLE)
    assert _FABRICATED_NEEDLE in planted['rendered'], (
        'the planted title did not survive the render, so scanning the capture '
        'would prove nothing about the render. Seed a field the panel actually '
        'draws.')
    assert _FABRICATED_NEEDLE in planted['envelopes'], (
        'the planted title is absent from the envelope egress class')
    clean = _capture_panel_surface()
    assert _FABRICATED_NEEDLE not in clean['text'], (
        'the unseeded capture already carries the needle -- the control is inert')

    patterns_file = tmp_path / 'fabricated_patterns'
    patterns_file.write_text(_FABRICATED_NEEDLE + '\n', encoding='utf-8')
    planted_path = tmp_path / 'planted_capture.txt'
    planted_path.write_text(planted['text'], encoding='utf-8')
    clean_path = tmp_path / 'clean_capture.txt'
    clean_path.write_text(clean['text'], encoding='utf-8')

    env = dict(os.environ, MASKING_SCAN_PATTERNS_FILE=str(patterns_file))
    planted_run = subprocess.run(
        [sys.executable, MASKING_SCRIPT, '--scan-asset', str(planted_path)],
        capture_output=True, text=True, env=env)
    assert planted_run.returncode != 0, (
        'the scanner passed a RENDERED surface with the needle in it -- it '
        f'cannot fail, so its clean runs prove nothing:\n{planted_run.stdout[-2000:]}')
    assert _FABRICATED_NEEDLE not in (planted_run.stdout + planted_run.stderr), (
        'the scanner ECHOED the matched pattern; a real one would leak into CI logs')

    clean_run = subprocess.run(
        [sys.executable, MASKING_SCRIPT, '--scan-asset', str(clean_path)],
        capture_output=True, text=True, env=env)
    assert clean_run.returncode == 0, (
        'the unplanted capture was reported -- the scan fires on correct output:'
        f'\n{clean_run.stdout[-2000:]}\n{clean_run.stderr[-2000:]}')


def test_the_error_paths_carry_no_artifact_VALUE_to_plant_a_needle_in():
    """Why the positive control seeds two egress classes and not three.

    The model refuses a malformed claim by CODE and FIELD NAME and never
    interpolates a value -- `_validate_claim_row`'s own contract, so that a
    refusal cannot put restricted text one `logger.exception` away from a log
    file. That is a real property and it is asserted here rather than assumed,
    because "the needle could not be routed here" and "nobody tried" look
    identical in a passing suite.
    """
    from shared.discovery_panel_model import PanelContractError
    try:
        build_panel_rows(PanelServiceBundle(
            claims=make_envelope(STATUS_OK, [surface_safe_claim(_claim_source(
                routing_status=_FABRICATED_NEEDLE,
                neutral_title=_FABRICATED_NEEDLE))], 1,
                meta={'page_id': 'p', 'include_review': False}),
            page_ids=make_envelope(STATUS_OK, ['p'], 1, meta={
                'sys_id': 's', 'resolved': True, 'truncated': False, 'volume_ie': None}),
            manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
                'page_scope_resolved': True, 'lang': 'en'}),
            related_count=make_envelope(STATUS_OK, [], 0, meta={
                'unit': 'distinct_opposite_pages'})))
    except PanelContractError as exc:
        assert _FABRICATED_NEEDLE not in str(exc), (
            'a refusal interpolated a row VALUE -- restricted artifact text can '
            'now reach a log line, and the error-path egress class needs its own '
            'planted-needle control')
    else:                                                    # pragma: no cover
        raise AssertionError('the malformed row was accepted')


def test_the_rendered_panel_and_repo_pass_the_real_masking_scan(masking_capture):
    """The D-25 scan, over the RENDERED OUTPUT and the repository.

    WHAT THIS DOES WHEN THE PATTERN FILE IS ABSENT: it FAILS, with the
    provisioning step named. It does not skip.

    Why that is the right behaviour and a skip is not: the real patterns are the
    only thing that makes this a check for RESTRICTED CORPUS NAMES rather than a
    check for nothing. Without them the assertion this test's name makes is
    unverified, and a required check that reports "unverified" as a pass is
    exactly the silent green the standing rule forbids -- the same rule that
    makes `scripts/check_atlas_masking.py` itself exit 1 on an unset
    `MASKING_SCAN_PATTERNS_FILE` instead of exiting 0 with zero patterns.

    Why a red run here is not a dead end: the MECHANISM is proved independently
    and unconditionally by
    `test_the_masking_scanner_can_actually_fail_on_this_capture`, which needs no
    secret. So a failure here means precisely one thing -- this environment has
    no pattern set -- and it has precisely one remedy, which the message states.
    Do not "fix" a red run by restoring the skip.

    Two surfaces, because they are blind to different leaks: `--scan-asset` over
    the capture sees a name that arrives from the sidecar or is assembled at
    render time (invisible to any repo scan), and `--scan-repo` sees a name
    hardcoded in a committed or staged file (invisible to any render).
    """
    assert os.path.exists(MASKING_SCRIPT), MASKING_SCRIPT
    configured = os.environ.get('MASKING_SCAN_PATTERNS_FILE')
    patterns = configured or '.masking_patterns'
    if not os.path.exists(patterns):
        pytest.fail(
            'the D-25 masking scan cannot run: MASKING_SCAN_PATTERNS_FILE is '
            f'{"set to a missing path" if configured else "unset"} and '
            '.masking_patterns is absent.\n'
            'This is a FAILURE and not a skip on purpose -- a masking check that '
            'searches for no patterns reports a clean surface it never inspected.\n'
            'To fix: write the restricted name/alias list to a gitignored file '
            '(one pattern per non-comment line) and point '
            'MASKING_SCAN_PATTERNS_FILE at it. In CI, inject it from a repository '
            'secret and delete it in an `if: always()` step; never commit it.')

    env = dict(os.environ, MASKING_SCAN_PATTERNS_FILE=os.path.abspath(patterns))
    result = subprocess.run(
        [sys.executable, MASKING_SCRIPT,
         '--scan-repo', '--scan-asset', str(masking_capture['path'])],
        capture_output=True, text=True, env=env)
    assert result.returncode == 0, (
        'the masking scan reported a restricted string on the rendered panel '
        'surface or in the repository. The report names a path, an offset and a '
        'pattern INDEX and never the pattern text:\n'
        f'{result.stdout[-2000:]}\n{result.stderr[-2000:]}')
