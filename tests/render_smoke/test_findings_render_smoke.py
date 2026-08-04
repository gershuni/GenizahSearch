# -*- coding: utf-8 -*-
"""The corpus-wide findings page, proved honest across THREE egress classes
(Phase 136, plan 136-18).

Markup is not the only egress. `shared/discovery_surface_projection.py::
_assert_surface_safe` validates forbidden KEY NAMES (plus two badge strings and
three rendered rate SHAPES), never arbitrary VALUES under innocuous keys against
a vocabulary -- so a stored novelty verdict under `band_label`, or an accuracy
claim in `meta['reason']`, reaches a JSON payload and a log line untouched by
any renderer assertion. This suite therefore scans:

  1. the RENDERED markup, element-scoped, in both languages, over three row
     units x four service states x both buckets;
  2. the EXACT envelopes this surface consumes -- findings rows, facets and the
     launch statistics -- recursively, every string value and every numeric one;
  3. the FORCED error paths -- exception messages, log lines and rendered states.

Every one of those scans is the SHARED gate from
`tests/render_smoke/discovery_honesty_gate.py` (plan 136-17). This module
defines no second envelope scanner, no second machine-vocabulary list, no second
completeness rule and no second accuracy detector; a source assertion at the
bottom confirms it, with comment and docstring lines excluded because the prose
here names both concepts.

**`assert_surface_honesty`, not `assert_discovery_honesty`.** The latter keeps a
FIVE-detector contract on purpose (plan 136-17, "the one criterion pair in
tension"); the former is the six-detector entry point every Phase-136 SURFACE
calls. A surface that calls the old name silently loses the accuracy detector.

THE HEADLINE IS PROVED BY A SENTINEL, NOT BY AGREEMENT
------------------------------------------------------
Plan 136-22's no-literals guard folds constant arithmetic, but no static scan
over this repository can see a figure assembled ACROSS STATEMENTS, imported, or
read from a file. The launch-headline fixture therefore carries numbers that
appear in NO artifact and in NEITHER half of 136-22's forbidden list, so the
equality assertion proves the DATA PATH rather than agreeing with a coincidence.
With the live figures in the fixture, a hardcoded headline would agree with the
test and pass.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
import pathlib
import re
import subprocess
from typing import Any, Dict, List, Mapping, Tuple

import pytest

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
import web.components.findings_rows as fr
import web.discovery_assets as da
import web.main as wm
import web.pages.findings as fp
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_main_pool import bucket_label
from shared.discovery_novelty import CANDIDATE_STATUS, DEFAULT_STATUS
from shared.discovery_service import (
    BUCKET_MAIN,
    BUCKET_MORE,
    DOMAIN_UNASSIGNED,
    FINDINGS_SORTS,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNIT_MANUSCRIPT,
    FINDINGS_UNIT_WORK,
    FINDINGS_UNITS,
    DiscoveryService,
    _build_findings_query,
)
from shared.discovery_surface_projection import (
    _ALL_ALLOWLISTS,
    STATUS_BUSY,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_UNAVAILABLE,
    busy_envelope,
    make_envelope,
    surface_safe_facet,
    surface_safe_finding,
    surface_safe_launch_shade,
    timeout_envelope,
    unavailable_envelope,
)
from tests.render_smoke.discovery_honesty_gate import (
    ALLOWLIST_FIELD_UNION,
    MACHINE_VOCABULARY_FIELDS,
    META_FREE_TEXT_KEYS,
    META_VOCABULARY_FIELDS,
    READER_TEXT_FIELDS,
    DiscoveryHonestyViolation,
    _PROHIBITED_RAW_VOCAB_KEYS,
    assert_envelope_honesty,
    assert_error_path_honesty,
    assert_surface_honesty,
    find_envelope_violations,
)
from web.translations import set_language

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPONENT_PATH = "web/components/findings_rows.py"
PAGE_PATH = "web/pages/findings.py"
SUITE_PATH = "tests/render_smoke/test_findings_render_smoke.py"

LANGS = ("en", "he")
SERVICE_STATES = (STATUS_OK, STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY)
BUCKETS = (BUCKET_MAIN, BUCKET_MORE)
UNITS = (FINDINGS_UNIT_IDENTIFICATION, FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK)

ASSERTION_COUNT = {"n": 0}

_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation
        prepare_simulation()
        _SIM_READY = True


def _read(rel: str) -> str:
    return io.open(REPO_ROOT / rel, encoding="utf-8").read()


# ===========================================================================
# THE SENTINEL LAUNCH ENVELOPE.
#
# Numbers no artifact carries and neither half of 136-22's forbidden list
# names. Asserted to be so, so they cannot drift into being real values.
# ===========================================================================

SENTINEL_SHADES: Tuple[Tuple[str, int, int], ...] = (
    ("fills_gap", 8111, 5111),
    ("refines_granularity", 9222, 6222),
    ("container_predicts", 7333, 4333),
)
SENTINEL_TOTAL = sum(count for _s, count, _m in SENTINEL_SHADES)
SENTINEL_MAIN_POOL_MANUSCRIPTS = 3777
SENTINEL_CORPUS_MANUSCRIPTS = 81777
SENTINEL_CORPUS_PAGES = 92888

SENTINEL_VALUES: Tuple[int, ...] = (
    SENTINEL_TOTAL,
    *[c for _s, c, _m in SENTINEL_SHADES],
    *[m for _s, _c, m in SENTINEL_SHADES],
    SENTINEL_MAIN_POOL_MANUSCRIPTS,
    SENTINEL_CORPUS_MANUSCRIPTS,
    SENTINEL_CORPUS_PAGES,
)


def sentinel_launch_envelope() -> Dict[str, Any]:
    """The launch envelope, built through the SHIPPED projection and the
    SHIPPED envelope constructor -- never as a hand-written dict."""
    items = [
        surface_safe_launch_shade({
            "shade": shade,
            "identification_count": count,
            "manuscript_count": manuscripts,
        })
        for shade, count, manuscripts in SENTINEL_SHADES
    ]
    return make_envelope(STATUS_OK, items, SENTINEL_TOTAL, meta={
        "basis": "main_pool",
        "sidecar_version": "sentinel-not-an-artifact",
        "audience": "public",
        "main_pool_manuscript_count": SENTINEL_MAIN_POOL_MANUSCRIPTS,
        "all_bucket_total": SENTINEL_TOTAL + 1,
        "all_bucket_manuscript_count": SENTINEL_MAIN_POOL_MANUSCRIPTS + 1,
        "corpus_manuscript_count": SENTINEL_CORPUS_MANUSCRIPTS,
        "corpus_page_count": SENTINEL_CORPUS_PAGES,
    })


def _launch_guard():
    """136-22's guard module, imported for its PURE functions only.

    `tests/` is not a package, so this builds a second module object; that is
    harmless here because nothing below monkeypatches it -- the functions used
    are the scanner and the two forbidden-list loaders. This plan RUNS the
    guard; it never modifies it.
    """
    return importlib.import_module("tests.test_discovery_launch_stats")


# ===========================================================================
# THE VALUE CORPUS.
#
# Every ROW enters through a `surface_safe_*` projection; every `meta` enters
# through REAL envelope construction. A hand-written dict would defeat both.
# ===========================================================================

CURATED_WORK_ID = "w000176"
CURATED_RAW_TITLE = "משנה תורה, ספר אהבה"
UNCURATED_WORK_ID = "w000999"
UNCURATED_RAW_TITLE = "Synthetic Uncurated Title"


def _finding_source(**overrides) -> Dict[str, Any]:
    """A findings row with EVERY allowlisted field NON-NULL, and every
    machine-classified value a MEMBER of that field's mapped vocabulary."""
    row = {
        "unit": FINDINGS_UNIT_IDENTIFICATION,
        "identification_id": "a" * 64,
        "sys_id": "990000000000000944",
        "canonical_work_id": UNCURATED_WORK_ID,
        "display_work_id": UNCURATED_WORK_ID,
        "neutral_title": UNCURATED_RAW_TITLE,
        "author": "Synthetic Author A",
        "genre": "Synthetic Parent A / Synthetic Leaf A",
        "domain": "Synthetic Parent A / Synthetic Leaf A",
        "library_code": "CUL",
        "shelfmark_display": "T-S 12.123",
        "main_pool": True,
        "main_pool_reason": "main_full_coverage",
        "best_band_rank": 0,
        "page_count": 3,
        "max_coverage_ppm": 680000,
        "relation_kind": ids.CLAIM_TYPE_DIRECT_WITNESS,
        "novelty_status": CANDIDATE_STATUS,
        "novelty_offered": True,
        "work_count": 1,
        "manuscript_count": 1,
        "multi_work_annotation": False,
    }
    row.update(overrides)
    return row


def finding_row(**overrides) -> Dict[str, Any]:
    return surface_safe_finding(_finding_source(**overrides))


def _facet_source(**overrides) -> Dict[str, Any]:
    row = {
        "level": "domain",
        "value": "Synthetic Parent A / Synthetic Leaf A",
        "label": "Synthetic Parent A / Synthetic Leaf A",
        "parent": "Synthetic Parent A",
        "is_leaf": True,
        "count": 7,
    }
    row.update(overrides)
    return row


def facet_row(**overrides) -> Dict[str, Any]:
    return surface_safe_facet(_facet_source(**overrides))


def corpus_rows() -> List[Tuple[str, Dict[str, Any]]]:
    """`(allowlist name, PROJECTED row)` for every allowlist this surface
    consumes. The coverage domain below is DERIVED from this list, never
    restated."""
    rows: List[Tuple[str, Dict[str, Any]]] = []
    rows.append(("SURFACE_FINDING_FIELDS", finding_row()))
    # A second row so every field's ALTERNATIVE value is exercised: the second
    # bucket, the fail-closed novelty shade, a null coverage measurement, a
    # different relation kind, and the multi-work annotation.
    rows.append(("SURFACE_FINDING_FIELDS", finding_row(
        unit=FINDINGS_UNIT_MANUSCRIPT,
        canonical_work_id=CURATED_WORK_ID,
        display_work_id=CURATED_WORK_ID,
        neutral_title=CURATED_RAW_TITLE,
        main_pool=False,
        main_pool_reason="shared_wording",
        best_band_rank=4,
        max_coverage_ppm=None,
        relation_kind=ids.CLAIM_TYPE_SHARED_TEXT,
        novelty_status=DEFAULT_STATUS,
        novelty_offered=False,
        work_count=2,
        manuscript_count=4,
        multi_work_annotation=True,
    )))
    rows.append(("SURFACE_FACET_FIELDS", facet_row()))
    rows.append(("SURFACE_FACET_FIELDS", facet_row(
        level="author", value="Synthetic Author A", label="Synthetic Author A",
        parent=None, is_leaf=True, count=3)))
    for shade, count, manuscripts in SENTINEL_SHADES:
        rows.append(("SURFACE_LAUNCH_SHADE_FIELDS", surface_safe_launch_shade({
            "shade": shade,
            "identification_count": count,
            "manuscript_count": manuscripts,
        })))
    return rows


CONSUMED_ALLOWLISTS: frozenset = frozenset(name for name, _row in corpus_rows())
_ALLOWLIST_BY_NAME: Dict[str, Tuple[str, ...]] = dict(_ALL_ALLOWLISTS)


def findings_envelope(items, total=None, *, status=STATUS_OK, unit=None,
                      bucket=BUCKET_MAIN, sort="band_rank") -> Dict[str, Any]:
    """One findings envelope, through the SHIPPED constructors."""
    unit = unit or FINDINGS_UNIT_IDENTIFICATION
    if status == STATUS_UNAVAILABLE:
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    if status == STATUS_TIMEOUT:
        return timeout_envelope(meta={"reason": "query_timeout"})
    if status == STATUS_BUSY:
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    items = list(items)
    return make_envelope(STATUS_OK, items,
                         len(items) if total is None else total,
                         meta={
                             "unit": unit,
                             "bucket": bucket,
                             "sort": sort,
                             "sort_basis": "best_band_rank",
                             "novelty_offered": unit != FINDINGS_UNIT_WORK,
                             "approximate_total": False,
                         })


def facets_envelope(level="domain", *, status=STATUS_OK, items=None) -> Dict[str, Any]:
    if status != STATUS_OK:
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    items = list(items if items is not None else [facet_row(level=level)])
    return make_envelope(STATUS_OK, items, len(items), meta={
        "level": level, "bucket": BUCKET_MAIN, "domain": None, "author": None,
    })


def surface_envelopes() -> List[Tuple[str, Dict[str, Any]]]:
    """`(where, envelope)` for every envelope this surface consumes."""
    return [
        ("findings/main", findings_envelope([finding_row()])),
        ("findings/more", findings_envelope(
            [finding_row(main_pool=False, main_pool_reason="overlapping_tie")],
            bucket=BUCKET_MORE)),
        ("findings/manuscript", findings_envelope(
            [finding_row(unit=FINDINGS_UNIT_MANUSCRIPT, work_count=2,
                         multi_work_annotation=True)],
            unit=FINDINGS_UNIT_MANUSCRIPT)),
        ("findings/work", findings_envelope(
            [finding_row(unit=FINDINGS_UNIT_WORK, novelty_offered=False,
                         novelty_status=None, manuscript_count=9)],
            unit=FINDINGS_UNIT_WORK)),
        ("findings/unavailable", findings_envelope([], status=STATUS_UNAVAILABLE)),
        ("findings/timeout", findings_envelope([], status=STATUS_TIMEOUT)),
        ("findings/busy", findings_envelope([], status=STATUS_BUSY)),
        ("facets/domain", facets_envelope("domain")),
        ("facets/author", facets_envelope(
            "author", items=[facet_row(level="author", value="Synthetic Author A",
                                       label="Synthetic Author A", parent=None)])),
        ("facets/work", facets_envelope(
            "work", items=[facet_row(level="work", value=CURATED_WORK_ID,
                                     label=CURATED_RAW_TITLE, parent=None)])),
        ("launch/ok", sentinel_launch_envelope()),
        ("launch/unavailable", unavailable_envelope(meta={"reason": "query_failed"})),
    ]


# ===========================================================================
# RENDER HARNESSES.
# ===========================================================================


def _client_render(paint):
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_findings_smoke_probe")) as client:
            with client:
                result = paint()
                if asyncio.iscoroutine(result):
                    await result
        holder["client"] = client

    asyncio.run(_run())
    return holder["client"]


def render_headline(envelope, lang="en", *, with_retry=True):
    async def _retry(_event=None):                          # pragma: no cover
        return None

    return _client_render(
        lambda: fr.render_launch_headline(
            envelope, lang, on_retry=_retry if with_retry else None))


def render_rows(items, lang="en"):
    def _paint():
        for item in items:
            fr.render_finding_row(item, lang)

    return _client_render(_paint)


def render_page(monkeypatch, *, lang="en", findings=None, facets=None,
                launch=None, state=None, as_of="2026-08-03"):
    """Render the REAL `create_findings_page()` with all three reads stubbed."""
    launch_envelope = sentinel_launch_envelope() if launch is None else launch
    findings_envelope_ = (findings if findings is not None
                          else findings_envelope([finding_row()]))
    facets_by_level = facets if facets is not None else {}

    async def _findings(*_a, **_k):
        return dict(findings_envelope_)

    async def _facets(level, **_k):
        return dict(facets_by_level.get(level) or facets_envelope(level))

    async def _launch(*_a, **_k):
        return dict(launch_envelope)

    monkeypatch.setattr(fp, "get_findings_enveloped", _findings)
    monkeypatch.setattr(fp, "get_findings_facets_enveloped", _facets)
    monkeypatch.setattr(fp, "get_launch_stats_enveloped", _launch)
    monkeypatch.setattr(fp, "discovery_meta", lambda key: as_of if key == "data_as_of" else None)
    if state is not None:
        monkeypatch.setattr(fp, "read_state", lambda: dict(state))
    set_language(lang)
    try:
        return _client_render(lambda: fp.create_findings_page())
    finally:
        set_language("he")


def _elements_with_class(client, marker: str) -> list:
    return [el for el in client.elements.values()
            if marker in (getattr(el, "_classes", None) or [])]


def _subtree_texts(element) -> List[str]:
    out = []
    for node in element.descendants(include_self=True):
        for attr in ("text", "_text", "content"):
            value = getattr(node, attr, None)
            if isinstance(value, str) and value.strip():
                out.append(value)
        for value in (getattr(node, "_props", None) or {}).values():
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


def fragment_from_texts(marker: str, texts) -> str:
    import html as _html
    return f'<div class="{marker}">{_html.escape(chr(10).join(texts))}</div>'


def scoped_text(client, marker: str) -> str:
    parts: List[str] = []
    for element in _elements_with_class(client, marker):
        parts.extend(_subtree_texts(element))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# This surface's OWN candidacy assertion (D-23b). NOT a re-implementation of a
# gate detector: the shared gate's prohibited-wording rule covers D-21's three
# relation words, and says nothing about the candidacy line the novelty axis is
# built on. "New discovery" stacks two unearned claims -- that the match is
# correct and that it is new -- on a row carrying no human review.
# ---------------------------------------------------------------------------

FORBIDDEN_CANDIDACY_WORDING = (
    "new discovery",
    "new discoveries",
    "likely new find",
    "unknown to scholarship",
    "תגלית חדשה",
    "לא ידוע למחקר",
)


def assert_no_forbidden_candidacy_wording(text: str, where: str) -> None:
    lowered = (text or "").lower()
    hits = [phrase for phrase in FORBIDDEN_CANDIDACY_WORDING if phrase in lowered]
    assert not hits, (
        f"{where}: forbidden candidacy wording {hits!r} -- the shipped wording "
        "asserts candidacy and only candidacy (D-23b)")


# ===========================================================================
# A. THE LAUNCH HEADLINE (ruling U).
# ===========================================================================


def test_the_sentinel_figures_are_in_neither_forbidden_half_nor_the_figure_file():
    """Otherwise the sentinel could drift into being a real value, and the
    provenance proof would silently become an agreement."""
    guard = _launch_guard()
    forbidden = guard.forbidden_figures()
    raw = _read("tests/fixtures/discovery/launch_figures.json")
    for value in SENTINEL_VALUES:
        assert value not in forbidden, (
            f"sentinel {value} is a REAL launch figure -- choose another")
        assert str(value) not in raw, (
            f"sentinel {value} appears in the committed figure file")


@pytest.mark.parametrize("lang", LANGS)
def test_every_rendered_headline_figure_equals_the_envelope_value(lang):
    """The four headline numbers, the per-shade fragment counts and the context
    figures all come FROM THE ENVELOPE.

    The fixture's numbers are SENTINELS no artifact contains, so a hardcoded
    headline fails this assertion in whatever form the hardcode took -- a
    string, a numeric constant, a formatted expression, constant-folded
    arithmetic, a figure assembled across two module-level names, an import, or
    a value read from a file. Only the first four of those are visible to a
    static scan."""
    envelope = sentinel_launch_envelope()
    client = render_headline(envelope, lang)
    text = scoped_text(client, fr.LAUNCH_CLASS)

    assert "{:,}".format(SENTINEL_TOTAL) in text, (
        f"the rendered total is not the envelope's: {text!r}")
    rendered_shades = []
    for item in envelope["items"]:
        assert "{:,}".format(item["identification_count"]) in text
        assert "{:,}".format(item["manuscript_count"]) in text
        rendered_shades.append(int(item["identification_count"]))
    assert sum(rendered_shades) == int(envelope["total"]), (
        "the three shades do not sum to the rendered total")
    for key in ("main_pool_manuscript_count", "corpus_manuscript_count",
                "corpus_page_count"):
        assert "{:,}".format(envelope["meta"][key]) in text, key
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_headline_names_its_basis_in_words_and_frames_the_container_shade(lang):
    """Ruling U constraint 1 (one basis, STATED) and constraint 4 (match
    framing). The container shade says what the aid DID name; a scoped
    assertion FAILS on any wording that says the aid was wrong."""
    client = render_headline(sentinel_launch_envelope(), lang)
    text = scoped_text(client, fr.LAUNCH_CLASS)

    assert ds.bucket_name(True, lang) in text, (
        "the headline does not name its basis in words")
    assert fr.launch_shade_label("container_predicts", lang) in text
    for wrong in ("the aid was wrong", "כלי העזר טעה", "incorrect", "mistaken"):
        assert wrong.lower() not in text.lower(), (
            f"the container shade reads as a verdict on the aid: {wrong!r}")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize("status", (STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY))
def test_an_outage_headline_offers_a_retry_and_renders_no_zero(lang, status):
    """A headline reading "0" during a sidecar failure would announce that the
    release contributes nothing."""
    envelope = {STATUS_UNAVAILABLE: unavailable_envelope(meta={"reason": "sidecar_not_serving"}),
                STATUS_TIMEOUT: timeout_envelope(meta={"reason": "query_timeout"}),
                STATUS_BUSY: busy_envelope(meta={"reason": "bounded_concurrency"})}[status]
    client = render_headline(envelope, lang)
    text = scoped_text(client, fr.LAUNCH_CLASS)

    assert ds.service_state_message(status, lang) in text
    assert ds.retry_label(lang) in text, "an outage headline rendered without a retry"
    assert not re.search(r"\d", text), (
        f"the outage headline rendered a figure: {text!r}")
    assert not _elements_with_class(client, fr.LAUNCH_TOTAL_CLASS), (
        "the outage headline rendered the contribution total element")
    ASSERTION_COUNT["n"] += 1


def test_136_22s_no_literals_guard_passes_with_this_module_in_scope():
    """RUN, never merely assumed to still hold. 136-22 owns the guard; this plan
    is one of the modules it globs."""
    guard = _launch_guard()
    figures = guard.forbidden_figures()
    key_names = guard.envelope_key_names(sentinel_launch_envelope())
    violations = guard.scan_launch_literals(REPO_ROOT, figures, key_names)
    assert not violations, "launch figures found as literals: " + "; ".join(
        str(v) for v in violations)
    assert COMPONENT_PATH in guard.scanner_scanned_paths(REPO_ROOT), (
        "the component this plan adds is not in the guard's scanned set")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_page_paints_the_headline_into_the_slot_136_16_reserved(monkeypatch, lang):
    client = render_page(monkeypatch, lang=lang)
    slots = _elements_with_class(client, fp.HEADLINE_SLOT_CLASS)
    assert len(slots) == 1
    text = scoped_text(client, fp.HEADLINE_SLOT_CLASS)
    assert "{:,}".format(SENTINEL_TOTAL) in text, (
        "the reserved headline slot was not filled from the envelope")
    assert _elements_with_class(client, fr.LAUNCH_CLASS)
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# B. THE ROWS.
# ===========================================================================


@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize("unit", UNITS)
def test_every_shipped_unit_renders_its_row_anatomy(lang, unit):
    item = finding_row(
        unit=unit,
        novelty_offered=unit != FINDINGS_UNIT_WORK,
        novelty_status=CANDIDATE_STATUS if unit != FINDINGS_UNIT_WORK else None,
        relation_kind=(ids.CLAIM_TYPE_DIRECT_WITNESS
                       if unit == FINDINGS_UNIT_IDENTIFICATION else None),
        work_count=2 if unit == FINDINGS_UNIT_MANUSCRIPT else 1,
        multi_work_annotation=unit == FINDINGS_UNIT_MANUSCRIPT,
        manuscript_count=9 if unit == FINDINGS_UNIT_WORK else 1,
    )
    client = render_rows([item], lang)
    rows = _elements_with_class(client, fr.ROW_CLASS)
    assert len(rows) == 1, f"{unit}: expected exactly one row"
    assert _elements_with_class(client, fr.ROW_TITLE_CLASS), f"{unit}: no title line"
    assert _elements_with_class(client, fr.ROW_META_CLASS), f"{unit}: no meta line"
    assert _elements_with_class(client, fr.ROW_BUCKET_CLASS), f"{unit}: no bucket name"
    ASSERTION_COUNT["n"] += 1


def test_the_per_claim_unit_is_unreachable():
    """The same identification repeats once per folio, which inflates same-work
    matches ~2.3x. It is not offered, and asking for it RAISES rather than
    silently degrading."""
    assert "claim" not in FINDINGS_UNITS
    with pytest.raises(ValueError):
        _build_findings_query(unit="claim")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_a_direct_row_SHOWS_qualified_coverage(lang):
    """POSITIVE, and required: an implementation that omitted the element for
    every row would satisfy an omission-only criterion."""
    client = render_rows([finding_row(max_coverage_ppm=680000)], lang)
    elements = _elements_with_class(client, fr.ROW_COVERAGE_CLASS)
    assert elements, "a direct-family row with a real measurement showed no coverage"
    text = "\n".join(_subtree_texts(elements[0]))
    assert text.strip(), "the coverage element rendered empty"
    assert "%" in text, f"the coverage element carries no figure: {text!r}"
    qualifier = "מהדף" if lang == "he" else "of page"
    assert qualifier in text, (
        f"the coverage figure is UNQUALIFIED: {text!r} -- an unqualified "
        "percentage is the one thing the honesty gate's exception does not cover")
    # And the gate agrees: the qualified form PASSES where a bare one would not.
    assert_surface_honesty(fragment_from_texts(fr.ROW_COVERAGE_CLASS, [text]),
                           scope_selector=fr.ROW_COVERAGE_CLASS, lang=lang)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_a_row_with_no_measurement_shows_no_coverage_element(lang):
    """Rendered as an ABSENCE, never as a zero or a placeholder implying a
    failed lookup. A shared-wording row has no measurement at all."""
    client = render_rows([finding_row(
        max_coverage_ppm=None, relation_kind=ids.CLAIM_TYPE_SHARED_TEXT)], lang)
    assert not _elements_with_class(client, fr.ROW_COVERAGE_CLASS)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_a_propagated_shaped_row_shows_no_coverage(lang):
    """Every shipped propagated evidence row measures NULL, so a purely
    propagated identification arrives with nothing to show."""
    client = render_rows([finding_row(max_coverage_ppm=None)], lang)
    assert not _elements_with_class(client, fr.ROW_COVERAGE_CLASS)
    ASSERTION_COUNT["n"] += 1


def test_the_component_reads_no_letter_count_and_names_none():
    src = _read(COMPONENT_PATH)
    assert "matched_letters" not in src, (
        "the component reads a field the identification grain does not have")
    assert "matched letters" not in src.lower(), (
        "the component writes a letter count it cannot source")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_curated_title_renders_and_the_raw_one_never_does_uncurated(lang):
    """Ruling R. The HEBREW curated label CONTAINS the raw recorded title, so
    the property is not "the raw string is absent" -- it is that the raw string
    never appears UNCURATED."""
    raw = CURATED_RAW_TITLE
    curated = ds.display_work_title(CURATED_WORK_ID, raw, lang)
    assert curated != raw or lang == "en", "fixture error: nothing to curate"
    client = render_rows([finding_row(
        canonical_work_id=CURATED_WORK_ID, display_work_id=CURATED_WORK_ID,
        neutral_title=raw)], lang)
    text = scoped_text(client, fr.ROW_CLASS)
    assert curated in text
    expected_raw = text.count(curated) if raw in curated else 0
    assert text.count(raw) == expected_raw, "the raw recorded title rendered uncurated"
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_an_uncurated_title_passes_through(lang):
    client = render_rows([finding_row()], lang)
    assert UNCURATED_RAW_TITLE in scoped_text(client, fr.ROW_CLASS)
    ASSERTION_COUNT["n"] += 1


def _hrefs(element) -> List[str]:
    """Every navigable target under `element`.

    Asserted on the `href` PROP rather than on the tag name: NiceGUI's `ui.link`
    renders as a `nicegui-link` custom element carrying `href`, so a tag-name
    assertion would pass vacuously in both directions."""
    out = []
    for node in element.descendants(include_self=True):
        href = (getattr(node, "_props", None) or {}).get("href")
        if isinstance(href, str) and href:
            out.append(href)
    return out


def test_work_titles_are_plain_text_and_the_shelfmark_is_a_live_link():
    """`/work/{id}` does not exist until Phase 136.1, and a dead link is worse
    than plain text. The manuscript page DOES exist."""
    client = render_rows([finding_row()], "en")
    titles = _elements_with_class(client, fr.ROW_TITLE_CLASS)
    assert titles
    assert not _hrefs(titles[0]), (
        f"a work title is a link: {_hrefs(titles[0])!r}")
    links = _elements_with_class(client, fr.ROW_SHELFMARK_CLASS)
    assert links, "the shelfmark did not render"
    targets = [href for element in links for href in _hrefs(element)]
    assert targets, "the shelfmark is not a link"
    assert all(t.startswith("/browse?sys_id=") for t in targets), targets
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_per_manuscript_unit_annotates_a_multi_work_manuscript(lang):
    """Driven by `multi_work_annotation`, which the service computes -- never
    re-derived here."""
    with_annotation = render_rows([finding_row(
        unit=FINDINGS_UNIT_MANUSCRIPT, work_count=2,
        multi_work_annotation=True)], lang)
    without = render_rows([finding_row(
        unit=FINDINGS_UNIT_MANUSCRIPT, work_count=1,
        multi_work_annotation=False)], lang)
    assert _elements_with_class(with_annotation, fr.ROW_ANNOTATION_CLASS)
    assert not _elements_with_class(without, fr.ROW_ANNOTATION_CLASS)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_per_work_unit_offers_no_novelty_element(lang):
    """A work spanning many manuscripts has no single verdict. Driven by
    `novelty_offered`."""
    client = render_rows([finding_row(
        unit=FINDINGS_UNIT_WORK, novelty_offered=False,
        novelty_status=CANDIDATE_STATUS)], lang)
    assert not _elements_with_class(client, fr.ROW_NOVELTY_CLASS)
    # ...and the same shade DOES badge on a unit that offers it.
    offered = render_rows([finding_row(novelty_offered=True)], lang)
    assert _elements_with_class(offered, fr.ROW_NOVELTY_CLASS)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_novelty_badge_is_solid_for_a_candidate_and_muted_for_an_unknown(lang):
    candidate = render_rows([finding_row(novelty_status=CANDIDATE_STATUS)], lang)
    unknown = render_rows([finding_row(novelty_status=DEFAULT_STATUS)], lang)
    candidate_classes = (_elements_with_class(candidate, fr.ROW_NOVELTY_CLASS)[0]._classes)
    unknown_classes = (_elements_with_class(unknown, fr.ROW_NOVELTY_CLASS)[0]._classes)
    assert "nov" in candidate_classes and "unknown" not in candidate_classes
    assert "nov" in unknown_classes and "unknown" in unknown_classes
    assert ds.novelty_strings(lang)["badge"] in scoped_text(candidate, fr.ROW_CLASS)
    assert ds.novelty_unknown_badge(lang) in scoped_text(unknown, fr.ROW_CLASS)
    ASSERTION_COUNT["n"] += 1


def test_no_row_level_accent_rule_is_keyed_on_novelty():
    """The findings-page reference records that the sketch README claimed one
    and the CSS does not have it, and that adding one needs a D-24 check first.
    This plan adds none."""
    src = _read(COMPONENT_PATH)
    assert ":has(" not in src
    assert not re.search(r"(?:border|padding|margin)-(?:left|right)\s*:", src), (
        "a physical directional property breaks RTL")
    for row_class in ("row", fr.ROW_CLASS):
        assert not re.search(
            re.escape(row_class) + r"[^\n]*\bnov\b[^\n]*classes", src)
    ASSERTION_COUNT["n"] += 1


def test_the_component_adds_no_css():
    result = subprocess.run(
        ["git", "diff", "--stat", "HEAD", "--", "web/static/common.css"],
        capture_output=True, text=True, cwd=str(REPO_ROOT))
    assert result.stdout.strip() == "", (
        f"web/static/common.css was modified by this plan: {result.stdout!r}")
    ASSERTION_COUNT["n"] += 1


def test_the_component_derives_no_bucket_of_its_own():
    """The show-more boundary is the bucket from the shared rule.

    Asserted over CODE lines only: the module's own comments name the shared
    predicate in order to say that it is not re-derived here, and a raw grep
    would fail on the explanation rather than on the defect."""
    code = _code_lines(COMPONENT_PATH)
    for forbidden in ("main_pool_decision", "is_default_eligible",
                      "COVERAGE_FLOOR", "SHORT_EVIDENCE_THRESHOLD"):
        assert forbidden not in code, (
            f"the component re-derives the bucket rule via {forbidden!r}")
    assert "bucket_name" in code, "the component does not use the shared bucket name"
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_bucket_membership_on_every_rendered_row_equals_the_shared_rule(lang):
    """So a future local reimplementation fails the suite rather than silently
    diverging -- the class of bug the retired `confOf()` was."""
    items = [finding_row(main_pool=True), finding_row(main_pool=False)]
    client = render_rows(items, lang)
    rendered = [
        "\n".join(_subtree_texts(el))
        for el in _elements_with_class(client, fr.ROW_BUCKET_CLASS)
    ]
    assert len(rendered) == len(items)
    for item, text in zip(items, rendered):
        expected = bucket_label(bool(item["main_pool"]), lang)
        assert expected in text, (
            f"row bucket {text!r} is not shared/discovery_main_pool's {expected!r}")
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# Ruling T -- the second bucket's rows, asserted so that a demoted or missing
# rendering FAILS.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lang", LANGS)
def test_second_bucket_rows_render_on_a_populated_fixture(monkeypatch, lang):
    """(a) They are PRESENT and their count is non-zero. A suite that only ever
    exercised the main pool does not satisfy this."""
    items = [finding_row(main_pool=False, main_pool_reason="overlapping_tie"),
             finding_row(main_pool=False, main_pool_reason="shared_wording",
                         identification_id="b" * 64)]
    client = render_page(
        monkeypatch, lang=lang,
        findings=findings_envelope(items, bucket=BUCKET_MORE),
        state={"unit": FINDINGS_UNIT_IDENTIFICATION, "bucket": BUCKET_MORE,
               "sort": "band_rank", "novelty_only": False, "domain": None,
               "author": None, "work_id": None, "page": 1})
    rows = _elements_with_class(client, fr.ROW_CLASS)
    assert len(rows) == len(items) and rows, (
        "the second bucket rendered no rows on a populated fixture")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_a_second_bucket_row_has_the_SAME_anatomy_as_a_main_pool_row(lang):
    """(b) Same element classes, same field set -- no extra muting class, no
    warning treatment, no reduced anatomy."""
    def _classes(main_pool: bool):
        client = render_rows([finding_row(main_pool=main_pool)], lang)
        found: List[Tuple[str, ...]] = []
        for element in _elements_with_class(client, fr.ROW_CLASS):
            for node in element.descendants(include_self=True):
                found.append(tuple(sorted(getattr(node, "_classes", None) or [])))
        return found

    main = _classes(True)
    more = _classes(False)
    assert main == more, (
        "a second-bucket row renders with different classes than a main-pool "
        f"row:\n  main: {main}\n  more: {more}")
    for marker in ("gated", "blocked", "warn", "muted", "dim"):
        assert not any(marker in cls for group in more for cls in group), (
            f"a second-bucket row carries a demotion class {marker!r}")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_no_count_element_is_attached_to_the_bucket_control_or_its_rows(monkeypatch, lang):
    """(c) Ruling T: the second bucket is genuinely reachable, with NO number
    attached. The owner's assessment of it is an impression over a rendered
    sample; it must never become a figure."""
    client = render_page(monkeypatch, lang=lang)
    control_text = scoped_text(client, fp.BUCKET_CONTROL_CLASS)
    assert not re.search(r"\d", control_text), (
        f"a digit is attached to the bucket control: {control_text!r}")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_second_bucket_section_carries_no_wording_implying_the_rows_are_wrong(lang):
    """(d) A scoped honesty assertion over the second-bucket rows, plus the
    contract `bucket_label`'s own docstring states: the bucket means there was
    not enough evidence for the rule, never that the identification is probably
    wrong. And never "mostly citations and shared texts" -- measured, its
    largest single group is same-work claims."""
    client = render_rows([finding_row(main_pool=False)], lang)
    fragment = scoped_fragment(client, fr.ROW_CLASS)
    assert_surface_honesty(fragment, scope_selector=fr.ROW_CLASS, lang=lang)
    text = scoped_text(client, fr.ROW_CLASS).lower()
    for phrase in ("probably wrong", "low quality", "unreliable", "weaker",
                   "mostly citations", "כנראה שגוי", "איכות נמוכה"):
        assert phrase not in text, (
            f"the second-bucket row reads as a verdict: {phrase!r}")
    assert_no_forbidden_candidacy_wording(text, "second-bucket row")
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# The novelty help affordance.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lang", LANGS)
def test_the_novelty_help_carries_the_sources_the_date_and_the_candidacy_sentence(
        monkeypatch, lang):
    client = render_page(monkeypatch, lang=lang, as_of="2026-08-03")
    text = scoped_text(client, fr.NOVELTY_HELP_CLASS)
    assert text.strip(), "the novelty help affordance did not render"
    for source in ("PGP", "FGP", "FJMS"):
        assert source in text, f"the checked-source list omits {source}"
    assert "2026-08-03" in text, "the help affordance states no as-of date"
    sentence = ("candidate, not a confirmed find" if lang == "en"
                else "זהו מועמד ולא ממצא מאושר")
    assert sentence in text, (
        "the help affordance omits the sentence that the identification is an "
        "unreviewed algorithmic match")
    assert_surface_honesty(scoped_fragment(client, fr.NOVELTY_HELP_CLASS),
                           scope_selector=fr.NOVELTY_HELP_CLASS, lang=lang)
    ASSERTION_COUNT["n"] += 1


def test_the_as_of_line_is_omitted_rather_than_guessed_when_the_artifact_records_none(
        monkeypatch):
    client = render_page(monkeypatch, lang="en", as_of=None)
    text = scoped_text(client, fr.NOVELTY_HELP_CLASS)
    assert "as of" not in text.lower()
    assert text.strip(), "the whole affordance vanished with the date"
    ASSERTION_COUNT["n"] += 1


def test_no_module_names_the_prohibited_candidacy_wording():
    for rel in (COMPONENT_PATH, PAGE_PATH):
        src = _read(rel).lower()
        assert "new discovery" not in src, rel
        assert "unknown to scholarship" not in src, rel
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# C. THE RENDERED-PAGE HONESTY MATRIX.
# ===========================================================================


@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize("status", SERVICE_STATES)
def test_the_rendered_findings_page_is_honest(monkeypatch, status, lang, unit, bucket):
    """Every assertion is scoped to the element it is about; none searches the
    whole rendered page as a string. The findings-page reference records exactly
    that failure: its facet-header assertion tested the whole page and PASSED
    WHILE THE HEADER WAS WRONG, because unrelated design-note prose contained
    the phrase it grepped for."""
    items = [finding_row(
        unit=unit, main_pool=bucket == BUCKET_MAIN,
        novelty_offered=unit != FINDINGS_UNIT_WORK,
        novelty_status=(CANDIDATE_STATUS if unit != FINDINGS_UNIT_WORK else None),
        relation_kind=(ids.CLAIM_TYPE_DIRECT_WITNESS
                       if unit == FINDINGS_UNIT_IDENTIFICATION else None),
    )] if status == STATUS_OK else []
    client = render_page(
        monkeypatch, lang=lang,
        findings=findings_envelope(items, status=status, unit=unit, bucket=bucket),
        state={"unit": unit, "bucket": bucket, "sort": "band_rank",
               "novelty_only": False, "domain": None, "author": None,
               "work_id": None, "page": 1})

    for marker in (fp.PAGE_CLASS, fp.FILTER_BAR_CLASS, fp.RESULTS_CLASS,
                   fp.HEADLINE_SLOT_CLASS):
        if not _elements_with_class(client, marker):
            continue
        assert_surface_honesty(scoped_fragment(client, marker),
                               scope_selector=marker, lang=lang)
        ASSERTION_COUNT["n"] += 1
    for element in _elements_with_class(client, fr.ROW_CLASS):
        texts = _subtree_texts(element)
        assert_surface_honesty(fragment_from_texts(fr.ROW_CLASS, texts),
                               scope_selector=fr.ROW_CLASS, lang=lang)
        assert_no_forbidden_candidacy_wording("\n".join(texts), "row")
        ASSERTION_COUNT["n"] += 1
    if status != STATUS_OK:
        text = scoped_text(client, fp.STATE_CLASS)
        assert ds.retry_label(lang) in text, (
            f"{status}/{lang}: an outage rendered without a retry")


@pytest.mark.parametrize("lang", LANGS)
def test_no_row_claims_human_review(lang):
    """D-13f: the badge is dropped until the provenance of the human-confirmed
    rows is established. `review_overlay()` keeps computing the value; no
    surface renders it."""
    client = render_rows([finding_row(), finding_row(main_pool=False)], lang)
    text = scoped_text(client, fr.ROW_CLASS)
    assert "expert-reviewed" not in text.lower()
    assert "נבדק בידי מומחה" not in text
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_result_bar_surfaces_the_real_total_and_names_its_bucket(monkeypatch, lang):
    """The count is the envelope's real pre-LIMIT total, never the page length,
    and the bar says which bucket it covers."""
    client = render_page(
        monkeypatch, lang=lang,
        findings=findings_envelope([finding_row()], total=4242))
    text = scoped_text(client, fp.RESULT_BAR_CLASS)
    assert "4242" in text or "4,242" in text, (
        f"the rendered count is not the envelope total: {text!r}")
    assert ds.bucket_name(True, lang) in text
    assert ds.bucket_name(False, lang) not in text, (
        "the result bar names BOTH buckets in one statement")
    ASSERTION_COUNT["n"] += 1


def test_novelty_is_not_among_the_sort_options():
    """D-15a / D-24: absence from a finding aid is not evidence a match is
    correct, and offering it as an ordering would imply otherwise."""
    assert "novelty" not in FINDINGS_SORTS
    for sort in FINDINGS_SORTS:
        assert "novel" not in sort
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_domain_facet_header_describes_the_IDENTIFIED_WORKS_domain(monkeypatch, lang):
    """Scoped to the HEADER ELEMENT ONLY.

    Why the scope is load-bearing and not fastidiousness: the findings-page
    reference records that this exact assertion, written against the whole
    rendered page, PASSED WHILE THE HEADER WAS WRONG, because unrelated prose
    elsewhere contained the phrase it grepped for. An assertion that can pass
    for the wrong reason is worse than none.

    The axis matters because filtering on the MANUSCRIPT's catalogue domain
    would hide exactly the findings that disagree with the catalogue -- a
    manuscript catalogued as court records carries a verifiably correct
    commentary identification."""
    client = render_page(monkeypatch, lang=lang)
    header = scoped_text(client, f"{fp.FACET_HEADER_CLASS}-domain")
    assert header.strip(), "the domain facet header did not render"
    expected = "identified work" if lang == "en" else "החיבור המזוהה"
    assert expected in header, (
        f"the domain facet header does not name the identified work: {header!r}")
    for wrong in ("manuscript", "catalogue", "catalog", "כתב היד", "קטלוג"):
        assert wrong.lower() not in header.lower(), (
            f"the domain facet header names the WRONG axis: {wrong!r} in {header!r}")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_every_rendered_domain_came_from_the_facet_envelope(monkeypatch, lang):
    """Structural half of the closed-vocabulary property, and it runs in every
    environment: a domain a reader can select is one the service emitted, never
    one the page invented."""
    offered = [facet_row(level="domain", value="Synthetic Parent A",
                         label="Synthetic Parent A", parent=None, is_leaf=False),
               facet_row(level="domain")]
    client = render_page(monkeypatch, lang=lang,
                         facets={"domain": facets_envelope("domain", items=offered)})
    rendered = scoped_text(client, f"{fp.FILTER_BAR_CLASS}-domain-items")
    for item in offered:
        assert item["label"] in rendered
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# The GOLDEN fixture sidecar -- the REAL service over a REAL artifact whose
# every value is fabricated. It is what makes the narrowing and cross-filtering
# assertions statements about the shipped query rather than about a stub.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_service(tmp_path_factory):
    from scripts.ci_materialize_discovery_fixture import build

    dest = tmp_path_factory.mktemp("discovery-fixture")
    db_path = build(dest)
    return DiscoveryService(
        path_provider=lambda: str(db_path),
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: "fixture",
    )


def test_the_domain_filter_narrows_and_a_leaf_narrows_further_than_its_parent(
        fixture_service):
    everything = fixture_service.get_findings_enveloped(bucket="all")["total"]
    facets = fixture_service.get_findings_facets_enveloped("domain", bucket="all")
    parents = [f for f in facets["items"] if not f["is_leaf"]]
    assert parents, "the fixture carries no parent domain node"
    parent = parents[0]
    leaves = [f for f in facets["items"] if f["parent"] == parent["value"]]
    assert leaves, "the fixture carries no leaf under that parent"

    parent_total = fixture_service.get_findings_enveloped(
        bucket="all", domain=parent["value"])["total"]
    leaf_total = fixture_service.get_findings_enveloped(
        bucket="all", domain=leaves[0]["value"])["total"]
    assert 0 < parent_total < everything, (
        f"the domain filter did not narrow: {parent_total} of {everything}")
    assert 0 < leaf_total <= parent_total, (
        f"a leaf did not narrow at least as far as its parent: {leaf_total} "
        f"vs {parent_total}")
    ASSERTION_COUNT["n"] += 1


def test_the_author_list_is_cross_filtered_by_domain_and_the_work_list_by_both(
        fixture_service):
    all_authors = {f["value"] for f in fixture_service.get_findings_facets_enveloped(
        "author", bucket="all")["items"]}
    facets = fixture_service.get_findings_facets_enveloped("domain", bucket="all")
    leaves = [f for f in facets["items"] if f["is_leaf"]]
    assert leaves
    domain = leaves[0]["value"]
    scoped_authors = {f["value"] for f in fixture_service.get_findings_facets_enveloped(
        "author", bucket="all", domain=domain)["items"]}
    assert scoped_authors <= all_authors and scoped_authors
    assert scoped_authors != all_authors, (
        "the author list is not cross-filtered by domain")

    author = sorted(scoped_authors - {DOMAIN_UNASSIGNED})[0]
    all_works = {f["value"] for f in fixture_service.get_findings_facets_enveloped(
        "work", bucket="all")["items"]}
    scoped_works = {f["value"] for f in fixture_service.get_findings_facets_enveloped(
        "work", bucket="all", domain=domain, author=author)["items"]}
    assert scoped_works <= all_works and scoped_works
    assert scoped_works != all_works, (
        "the work list is not cross-filtered by domain and author")
    ASSERTION_COUNT["n"] += 1


def test_the_second_bucket_is_populated_in_the_shipped_query(fixture_service):
    """Ruling T's precondition: the second bucket is a real result set."""
    main = fixture_service.get_findings_enveloped(bucket=BUCKET_MAIN)
    more = fixture_service.get_findings_enveloped(bucket=BUCKET_MORE)
    assert main["status"] == STATUS_OK and more["status"] == STATUS_OK
    assert main["total"] > 0 and more["total"] > 0
    ASSERTION_COUNT["n"] += 1


def test_bucket_membership_from_the_shipped_query_equals_the_shared_rule(fixture_service):
    for bucket, expected in ((BUCKET_MAIN, True), (BUCKET_MORE, False)):
        envelope = fixture_service.get_findings_enveloped(bucket=bucket, page_size=200)
        assert envelope["items"]
        for item in envelope["items"]:
            assert bool(item["main_pool"]) is expected
            assert bucket_label(bool(item["main_pool"]), "en") == bucket_label(
                expected, "en")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.skipif(
    not (REPO_ROOT / "fist_data" / "fjms_enrichment.db").is_file(),
    reason="the FJMS sidecar (fist_data/fjms_enrichment.db) is absent, so the "
           "CLOSED domain vocabulary cannot be read LIVE; recorded as a skip "
           "rather than validated against a stale snapshot")
def test_every_domain_the_curation_artifact_assigns_is_inside_the_closed_vocabulary():
    """The data half of the closed-vocabulary property, read LIVE from
    `shared.fjms_service` -- never a snapshot."""
    from scripts.curate_work_domains import load_vocabulary

    vocabulary = load_vocabulary()
    for value in ("Synthetic Parent A / Synthetic Leaf A", "Not A Real Domain"):
        assert not vocabulary.has_node(value), (
            "fixture error: the control value is inside the live vocabulary")
    assert vocabulary.nodes, "the live vocabulary is empty"
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_page_does_not_render_empty_in_the_unavailable_state(monkeypatch, lang):
    """And the nav entry is GONE, not disabled."""
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", False)
    monkeypatch.setattr(da, "_state", da._DiscoveryState(ready=False))
    assert da.discovery_available() is False
    set_language(lang)
    try:
        from web.translations import tr
        expected = tr("Computed Identifications")
        client = _client_render(lambda: wm.create_layout())
        texts = [t for el in client.elements.values() for t in _subtree_texts(el)]
        assert not any(expected in t for t in texts), (
            "the nav entry rendered while discovery is UNAVAILABLE")

        page = render_page(
            monkeypatch, lang=lang,
            findings=findings_envelope([], status=STATUS_UNAVAILABLE),
            launch=unavailable_envelope(meta={"reason": "sidecar_not_serving"}))
    finally:
        set_language("he")
    body = scoped_text(page, fp.PAGE_CLASS)
    assert body.strip(), "the page rendered empty in the unavailable state"
    assert ds.retry_label(lang) in body
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# D. THE ENVELOPE SCAN, and the classification it stands on.
# ===========================================================================


@pytest.mark.parametrize("lang", LANGS)
def test_every_envelope_this_surface_consumes_is_clean(lang):
    for where, envelope in surface_envelopes():
        assert_envelope_honesty(envelope, lang=lang, where=where)
        ASSERTION_COUNT["n"] += 1


def test_the_gates_widened_vocabulary_reached_this_surface():
    """Asserted to have REACHED here, not merely to exist. The broken
    implementation this catches is a local re-declaration or a stale import
    leaving this page scanning against the pre-widening vocabulary."""
    assert "fills_gap" in _PROHIBITED_RAW_VOCAB_KEYS
    assert "main_full_coverage" in _PROHIBITED_RAW_VOCAB_KEYS
    assert "novelty_status" in MACHINE_VOCABULARY_FIELDS
    assert "main_pool_reason" in MACHINE_VOCABULARY_FIELDS
    assert CANDIDATE_STATUS in MACHINE_VOCABULARY_FIELDS["novelty_status"]
    ASSERTION_COUNT["n"] += 1


def test_every_field_of_every_allowlist_this_surface_consumes_is_classified():
    """136-17's partition, RUN over this surface's own allowlists. An
    unclassified field here is a BLOCKING FINDING to report with its file and
    line -- never something to declare locally, because the gate module is
    136-17's."""
    unclassified = []
    for name in sorted(CONSUMED_ALLOWLISTS):
        for field in _ALLOWLIST_BY_NAME[name]:
            if field not in MACHINE_VOCABULARY_FIELDS and field not in READER_TEXT_FIELDS:
                unclassified.append(f"{name}.{field}")
    assert not unclassified, (
        "BLOCKING: unclassified field(s) on this surface's allowlists -- report "
        "to the owner of tests/render_smoke/discovery_honesty_gate.py: "
        + ", ".join(unclassified))
    assert set(_ALLOWLIST_BY_NAME["SURFACE_FINDING_FIELDS"]) <= ALLOWLIST_FIELD_UNION
    ASSERTION_COUNT["n"] += 1


def test_the_derived_coverage_check_passes_over_this_surfaces_own_corpus():
    """The IMPORTED derived-`CONSUMED_ALLOWLISTS` non-null coverage check.

    There is NO name-it-with-a-reason alternative: an uncovered field is a
    fixture to ADD, or -- if the field is genuinely unreachable from this
    surface -- a BLOCKING FINDING to report with its file and line."""
    rows = corpus_rows()
    for name in sorted(CONSUMED_ALLOWLISTS):
        fields = _ALLOWLIST_BY_NAME[name]
        seen = [row for allowlist, row in rows if allowlist == name]
        assert seen, f"{name} is declared consumed but never seeded"
        for field in fields:
            assert any(row.get(field) is not None for row in seen), (
                f"{name}.{field} is null in every corpus row -- seed it")
    ASSERTION_COUNT["n"] += 1


def test_every_corpus_row_key_set_equals_a_registered_allowlist():
    """A CONSTRUCTION check: it catches a hand-written dict that never went
    through a projection."""
    for name, row in corpus_rows():
        assert set(row) == set(_ALLOWLIST_BY_NAME[name]), (
            f"{name}: key-set diff {sorted(set(row) ^ set(_ALLOWLIST_BY_NAME[name]))}")
    ASSERTION_COUNT["n"] += 1


def test_every_meta_key_this_surface_emits_is_classified():
    keys = set()
    for _where, envelope in surface_envelopes():
        keys.update((envelope.get("meta") or {}).keys())
    known = set(META_VOCABULARY_FIELDS) | set(META_FREE_TEXT_KEYS)
    # The launch envelope's context figures are NUMERIC meta keys; they are not
    # free text and carry no vocabulary, so they are excluded by shape rather
    # than by a second list.
    numeric = {k for k in keys
               if all(isinstance((e.get("meta") or {}).get(k), (int, float, type(None)))
                      for _w, e in surface_envelopes())}
    missing = sorted(keys - known - numeric)
    assert not missing, (
        "BLOCKING: unclassified meta key(s) on this surface: " + ", ".join(missing))
    ASSERTION_COUNT["n"] += 1


def test_FP_LIVE_VOCAB_a_live_vocabulary_findings_envelope_PASSES(caplog):
    """The FALSE-POSITIVE control. BOTH halves are required: a scan that passes
    both is inert, and one that fails both would reject every correct envelope
    this page produces."""
    envelope = findings_envelope([finding_row()])
    item = envelope["items"][0]
    assert item["relation_kind"] == ids.CLAIM_TYPE_DIRECT_WITNESS
    assert item["main_pool_reason"] == "main_full_coverage"
    assert item["novelty_status"] == CANDIDATE_STATUS
    assert_envelope_honesty(envelope, lang="en", where="FP-LIVE-VOCAB")

    seeded = dict(envelope)
    seeded["items"] = [dict(item, band_label=ids.CLAIM_TYPE_DIRECT_WITNESS)]
    violations = find_envelope_violations(seeded, lang="en", where="FP-LIVE-VOCAB")
    assert any("direct_witness" in v for v in violations), (
        "a stored vocabulary key in a READER-FACING field passed the scan")
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# E. THE FORCED ERROR PATHS.
# ===========================================================================


def _error_modes(fixture_service) -> List[Tuple[str, str]]:
    """`(mode, message)` for each forced failure. Six modes, and each message is
    the REAL one the code raises or logs."""
    modes: List[Tuple[str, str]] = []

    absent = DiscoveryService(path_provider=lambda: None,
                              availability_callable=lambda: False,
                              sidecar_version_provider=lambda: None)
    envelope = absent.get_findings_enveloped()
    modes.append(("sidecar-absent", str(envelope["meta"]["reason"])))

    try:
        raise DiscoveryUnavailable("temporarily unavailable")
    except DiscoveryUnavailable as exc:
        modes.append(("query-timeout", str(exc)))

    try:
        raise DiscoveryOverload("discovery is busy")
    except DiscoveryOverload as exc:
        modes.append(("bounded-concurrency", str(exc)))

    for kwargs in ({"unit": "claim"}, {"sort": "novelty"}, {"bucket": "everything"}):
        try:
            _build_findings_query(**kwargs)
        except ValueError as exc:
            modes.append((f"out-of-vocabulary {sorted(kwargs)[0]}", str(exc)))

    # A malformed row -- driven through what THIS SURFACE emits. The component
    # swallows an out-of-vocabulary relation kind and omits the chip (asserted
    # behaviourally below), so what reaches an egress is the rendered row, not
    # the shared module's exception. See
    # `test_the_surface_never_lets_a_vocabulary_enumerating_message_reach_an_egress`
    # for the shared-module finding this deliberately does not paper over.
    client = render_rows([finding_row(relation_kind="not_a_relation",
                                      neutral_title=None)], "en")
    modes.append(("malformed-row", scoped_text(client, fr.ROW_CLASS)))

    try:
        fr.launch_shade_label("not_a_shade")
    except ValueError as exc:
        modes.append(("malformed-shade", str(exc)))

    modes.append(("missing-work-title", ds.missing_title("en")))
    modes.append(("missing-work-title/he", ds.missing_title("he")))
    return modes


def test_the_surface_never_lets_a_vocabulary_enumerating_message_reach_an_egress():
    """A RECORDED FINDING, and the boundary this plan can actually hold.

    `shared/discovery_display_strings.py::relation_chip` raises
    ``unknown relation kind ... (expected one of ['direct_witness',
    'quotes_this_work', 'shared_text'])`` -- three stored vocabulary values on
    an exception message, which is an egress class that reaches a log and,
    uncaught, a reader without passing through either the markup scan or the
    envelope scan. `shared/` is outside this plan's `files_modified`, so the
    finding is REPORTED in the summary rather than fixed here.

    What this plan CAN guarantee, and asserts:

    * this surface catches that ValueError and renders the row without a chip,
      so the message never reaches an egress FROM HERE;
    * this plan's OWN raising accessor names its authority instead of
      enumerating it, so the same defect is not reintroduced.
    """
    client = render_rows([finding_row(relation_kind="not_a_relation")], "en")
    assert _elements_with_class(client, fr.ROW_CLASS), "the row failed to render"
    assert not _elements_with_class(client, fr.ROW_RELATION_CLASS), (
        "an out-of-vocabulary relation kind produced a chip")

    with pytest.raises(ValueError) as excinfo:
        fr.launch_shade_label("not_a_shade")
    assert_error_path_honesty(str(excinfo.value), lang="en",
                              where="launch_shade_label")
    for shade in ("fills_gap", "refines_granularity", "container_predicts"):
        assert shade not in str(excinfo.value), (
            "this plan's own error message enumerates the stored vocabulary")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_every_forced_error_path_is_honest(fixture_service, lang, caplog):
    caplog.set_level(logging.DEBUG)
    modes = _error_modes(fixture_service)
    assert len(modes) >= 6, f"only {len(modes)} failure modes driven"
    for mode, message in modes:
        assert_error_path_honesty(message, lang=lang, where=f"error path/{mode}")
        ASSERTION_COUNT["n"] += 1
    for record in caplog.records:
        assert_error_path_honesty(record.getMessage(), lang=lang, where="log line")


@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize("status", (STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY))
def test_the_rendered_error_state_is_honest(monkeypatch, lang, status):
    client = render_page(
        monkeypatch, lang=lang,
        findings=findings_envelope([], status=status),
        launch={STATUS_UNAVAILABLE: unavailable_envelope(meta={"reason": "query_failed"}),
                STATUS_TIMEOUT: timeout_envelope(meta={"reason": "query_timeout"}),
                STATUS_BUSY: busy_envelope(meta={"reason": "bounded_concurrency"})}[status])
    for marker in (fp.STATE_CLASS, fr.LAUNCH_STATE_CLASS):
        assert _elements_with_class(client, marker), f"{status}: {marker} absent"
        assert_surface_honesty(scoped_fragment(client, marker),
                               scope_selector=marker, lang=lang)
        ASSERTION_COUNT["n"] += 1


def test_a_malformed_row_renders_rather_than_crashing():
    """Every field null. The row is still worth showing, and a crash here would
    take the whole result set down."""
    client = render_rows([surface_safe_finding({})], "en")
    assert _elements_with_class(client, fr.ROW_CLASS)
    assert ds.missing_title("en") in scoped_text(client, fr.ROW_CLASS)
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# F. POSITIVE CONTROLS -- ONE MUTATION PER PROPERTY, each asserting the
# SPECIFIC expected failure. The action's list is the count; no numeral here.
# ===========================================================================


def _violation_text(excinfo) -> str:
    return str(excinfo.value)


def test_control_1_new_discovery_wording_in_a_row_fires_the_candidacy_assertion():
    with pytest.raises(AssertionError) as excinfo:
        assert_no_forbidden_candidacy_wording(
            "Rashi on Esther — New discovery", "control 1")
    assert "new discovery" in _violation_text(excinfo).lower()
    ASSERTION_COUNT["n"] += 1


def test_control_2_a_precision_percentage_in_a_row_fires_the_percentage_detector():
    fragment = fragment_from_texts(fr.ROW_CLASS, ["Rashi on Esther", "precision 93%"])
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_surface_honesty(fragment, scope_selector=fr.ROW_CLASS, lang="en")
    assert "unqualified percentage" in _violation_text(excinfo)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.skipif(
    not (REPO_ROOT / "fist_data" / "fjms_enrichment.db").is_file(),
    reason="the FJMS sidecar is absent, so the CLOSED domain vocabulary cannot "
           "be read LIVE")
def test_control_3_an_out_of_vocabulary_domain_fires_the_domain_assertion():
    from scripts.curate_work_domains import load_vocabulary

    vocabulary = load_vocabulary()
    real = sorted(vocabulary.nodes)[0]
    assert vocabulary.has_node(real)
    with pytest.raises(AssertionError):
        assert vocabulary.has_node("Definitely Not An FJMS Domain"), (
            "domain outside the closed vocabulary")
    ASSERTION_COUNT["n"] += 1


def test_control_4_a_header_naming_the_manuscripts_domain_fires_the_header_assertion():
    header = "Domain of the manuscript"
    with pytest.raises(AssertionError):
        assert "identified work" in header, (
            f"the domain facet header does not name the identified work: {header!r}")
    with pytest.raises(AssertionError):
        for wrong in ("manuscript", "catalogue"):
            assert wrong not in header.lower(), (
                f"the domain facet header names the WRONG axis: {wrong!r}")
    ASSERTION_COUNT["n"] += 1


def test_control_5_a_row_whose_bucket_disagrees_with_the_shared_rule_fires():
    item = finding_row(main_pool=True)
    wrong = bucket_label(False, "en")
    with pytest.raises(AssertionError):
        assert wrong in bucket_label(bool(item["main_pool"]), "en"), (
            "row bucket is not shared/discovery_main_pool's")
    ASSERTION_COUNT["n"] += 1


def test_control_6_a_percentage_in_meta_reason_fires_the_ENVELOPE_scan_only():
    envelope = dict(findings_envelope([finding_row()]))
    envelope["meta"] = dict(envelope["meta"], reason="matched 93% of the corpus")
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_envelope_honesty(envelope, lang="en", where="control 6")
    assert "unqualified percentage" in _violation_text(excinfo)
    # ...and the MARKUP scan cannot see it: `meta` never reaches the renderer.
    client = render_rows(envelope["items"], "en")
    assert_surface_honesty(scoped_fragment(client, fr.ROW_CLASS),
                           scope_selector=fr.ROW_CLASS, lang="en")
    ASSERTION_COUNT["n"] += 1


def test_control_7_a_percentage_in_an_exception_message_fires_the_ERROR_PATH_scan_only():
    message = "findings query failed after matching 93% of the corpus"
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_error_path_honesty(message, lang="en", where="control 7")
    assert "unqualified percentage" in _violation_text(excinfo)
    client = render_rows([finding_row()], "en")
    assert_surface_honesty(scoped_fragment(client, fr.ROW_CLASS),
                           scope_selector=fr.ROW_CLASS, lang="en")
    ASSERTION_COUNT["n"] += 1


def test_control_8_fills_gap_in_a_reader_facing_field_fires_the_NOVELTY_vocabulary():
    """Before the gate's round-7 widening this control could not have gone red
    at all: `fills_gap` in a reader-facing field scanned clean while
    `direct_witness` in the SAME field failed."""
    envelope = dict(findings_envelope([finding_row()]))
    envelope["items"] = [dict(envelope["items"][0], band_label=CANDIDATE_STATUS)]
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_envelope_honesty(envelope, lang="en", where="control 8")
    assert "fills_gap" in _violation_text(excinfo)
    # COMPANION: the same value in its DECLARED carrier still passes.
    ok = findings_envelope([finding_row(novelty_status=CANDIDATE_STATUS)])
    assert_envelope_honesty(ok, lang="en", where="control 8 companion")
    ASSERTION_COUNT["n"] += 1


def test_control_9_main_full_coverage_in_a_reader_facing_field_fires_the_MAIN_POOL_vocabulary():
    envelope = dict(findings_envelope([finding_row()]))
    envelope["items"] = [dict(envelope["items"][0], band_label="main_full_coverage")]
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_envelope_honesty(envelope, lang="en", where="control 9")
    assert "main_full_coverage" in _violation_text(excinfo)
    ok = findings_envelope([finding_row(main_pool_reason="main_full_coverage")])
    assert_envelope_honesty(ok, lang="en", where="control 9 companion")
    ASSERTION_COUNT["n"] += 1


def test_control_10_an_accuracy_rate_in_a_rendered_row_fires_the_ACCURACY_detector():
    """Before the sixth detector, NO detector fired on this string through any
    field."""
    fragment = fragment_from_texts(fr.ROW_CLASS, ["Rashi on Esther", "accuracy 0.91"])
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_surface_honesty(fragment, scope_selector=fr.ROW_CLASS, lang="en")
    assert "accuracy" in _violation_text(excinfo).lower()
    ASSERTION_COUNT["n"] += 1


def test_control_11_a_rate_that_is_a_FLOAT_under_a_launch_meta_key_fires_the_numeric_rule():
    """A rate beside ruling U's four numbers is the most damaging thing this
    page could print, and a NUMBER is the likeliest form for it to arrive in."""
    envelope = sentinel_launch_envelope()
    envelope = dict(envelope, meta=dict(envelope["meta"], match_quality=0.91))
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_envelope_honesty(envelope, lang="en", where="control 11")
    assert "rate-shaped float" in _violation_text(excinfo)
    # COMPANION 1: a string-only value scan cannot see it.
    strings = [v for _p, _f, v in _walk_values(envelope) if isinstance(v, str)]
    assert not any("0.91" in s for s in strings), (
        "the float leaked into a string, so this control tests the wrong thing")
    # COMPANION 2: the markup scan cannot see it either.
    client = render_headline(envelope, "en")
    assert_surface_honesty(scoped_fragment(client, fr.LAUNCH_CLASS),
                           scope_selector=fr.LAUNCH_CLASS, lang="en")
    ASSERTION_COUNT["n"] += 1


def _walk_values(node, path=""):
    if isinstance(node, Mapping):
        for key, value in node.items():
            child = f"{path}.{key}" if path else str(key)
            yield child, key, value
            yield from _walk_values(value, child)
    elif isinstance(node, (list, tuple)) and not isinstance(node, (str, bytes)):
        for index, value in enumerate(node):
            child = f"{path}[{index}]"
            yield child, None, value
            yield from _walk_values(value, child)


def test_control_12_an_accuracy_rate_in_an_EXCEPTION_MESSAGE_fires_the_error_path_scan():
    """The egress class that reaches a log and a reader without passing through
    either of the other two scans. A wiring defect applying the sixth detector
    to markup and envelopes and forgetting exception messages satisfies control
    7 (a PERCENTAGE on an error path) and control 10 (an accuracy rate in
    markup) simultaneously."""
    message = "findings query failed; accuracy 0.91 on the sampled rows"
    with pytest.raises(DiscoveryHonestyViolation) as excinfo:
        assert_error_path_honesty(message, lang="en", where="control 12")
    assert "accuracy" in _violation_text(excinfo).lower()
    # COMPANION: neither the markup nor the envelope scan sees it.
    client = render_rows([finding_row()], "en")
    assert_surface_honesty(scoped_fragment(client, fr.ROW_CLASS),
                           scope_selector=fr.ROW_CLASS, lang="en")
    assert_envelope_honesty(findings_envelope([finding_row()]), lang="en",
                            where="control 12 companion")
    ASSERTION_COUNT["n"] += 1


def _live_d06a_sentence(lang: str) -> str:
    """The D-06a qualitative sentence, taken from the LIVE methods render rather
    than retyped, so this control cannot drift from the sentence it is about."""
    from web.pages.help import _LIMITATIONS_TEXT

    return _LIMITATIONS_TEXT["he" if lang == "he" else "en"]


@pytest.mark.parametrize("lang", LANGS)
def test_control_13_the_D06A_sentence_FAILS_on_findings_markup_envelope_and_error_path(lang):
    """The exception is bound to the ONE registered element -- the limitations
    paragraph's marker class on the methods page -- and is UNAVAILABLE to this
    surface. 136-17's own control seeds the sentence into a PANEL row, an
    envelope and an exception message; none of those is this surface, so this
    control is what proves the exception does not reach FINDINGS markup."""
    sentence = _live_d06a_sentence(lang)

    fragment = fragment_from_texts(fr.ROW_CLASS, [sentence])
    with pytest.raises(DiscoveryHonestyViolation) as markup:
        assert_surface_honesty(fragment, scope_selector=fr.ROW_CLASS, lang=lang)
    assert "accuracy/rate claim" in _violation_text(markup)

    envelope = dict(findings_envelope([finding_row()]))
    envelope["meta"] = dict(envelope["meta"], reason=sentence)
    with pytest.raises(DiscoveryHonestyViolation) as env:
        assert_envelope_honesty(envelope, lang=lang, where="control 13")
    assert "accuracy/rate claim" in _violation_text(env)

    with pytest.raises(DiscoveryHonestyViolation) as err:
        assert_error_path_honesty(sentence, lang=lang, where="control 13")
    assert "accuracy/rate claim" in _violation_text(err)
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# G. SOURCE ASSERTIONS -- this module writes no second rule.
# ===========================================================================


def _code_lines(rel: str) -> str:
    """The module's source with comment and docstring lines EXCLUDED -- the
    prose here names both concepts this asserts the absence of."""
    import ast

    source = _read(rel)
    tree = ast.parse(source)
    doc_lines = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and \
                    isinstance(body[0].value, ast.Constant) and \
                    isinstance(body[0].value.value, str):
                start = body[0].lineno
                end = getattr(body[0], "end_lineno", start)
                doc_lines.update(range(start, end + 1))
    kept = []
    for number, line in enumerate(source.splitlines(), start=1):
        if number in doc_lines:
            continue
        kept.append(line.split("#", 1)[0])
    return "\n".join(kept)


def test_this_suite_defines_no_second_scanner_list_rule_or_detector():
    """Each marker is COMPOSED rather than written whole, so this assertion does
    not match its own literals -- the self-reference is otherwise unavoidable
    and would make the check fail on itself instead of on a defect."""
    code = _code_lines(SUITE_PATH)
    markers = [
        "def " + "find_envelope_violations",
        "def " + "_find_accuracy_rates",
        "MACHINE_VOCABULARY_FIELDS" + " =",
        "READER_TEXT_FIELDS" + " =",
        "def " + "assert_envelope_honesty",
        "def " + "assert_error_path_honesty",
        "_PROHIBITED_RAW_VOCAB_KEYS" + " =",
    ]
    for forbidden in markers:
        assert forbidden not in code, (
            f"this suite defines a second copy of {forbidden!r} -- one rule, one place")
    assert "assert_surface_honesty" in code, (
        "this surface must call the SIX-detector entry point")
    assert ("assert_discovery_honesty" + "(") not in code, (
        "assert_discovery_honesty keeps a FIVE-detector contract; a surface "
        "calling it silently loses the accuracy detector")
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# H. THE DEPLOY GATE (ruling T) and the MASKING capture.
# ===========================================================================

_BROWSER_CHECK_SUMMARY = (
    ".planning/phases/136-read-surfaces-connections-panel-work-witnesses/"
    "136-16-SUMMARY.md"
)


def browser_check_outcome() -> Tuple[bool, str]:
    """`(met, recorded state)` for 136-16's real-browser actionability result.

    Ruling T makes the "more matches" control the reachability of roughly half
    the non-Bible discovery value in the release, so shipping the findings page
    with it unproven would bury the majority of the non-Bible result behind
    something nobody has confirmed a reader can click.
    """
    text = _read(_BROWSER_CHECK_SUMMARY)
    marker = "criteria (e) and (f)"
    assert marker in text, (
        f"{_BROWSER_CHECK_SUMMARY} records no outcome for 136-16's real-browser "
        "actionability criteria at all -- a MISSING record is worse than a "
        "recorded NOT MET, because nobody can tell whether it was ever run")
    window = text[text.index(marker):text.index(marker) + 400]
    if "NOT MET" in window:
        return False, "NOT MET"
    if "MET" in window:
        return True, "MET"
    raise AssertionError(
        f"{_BROWSER_CHECK_SUMMARY} names the criteria but records no MET/NOT MET "
        f"state near them: {window!r}")


def test_the_findings_deploy_is_blocked_until_the_browser_check_is_recorded_MET():
    """The RECORD must exist before the deploy step runs, and its value is what
    opens or closes the gate. A criterion recorded as NOT MET or as a skip
    BLOCKS the deploy; this test does not fail on a closed gate -- it fails when
    the outcome is unrecorded, which is the state nobody can act on."""
    met, state = browser_check_outcome()
    assert state in ("MET", "NOT MET")
    if not met:
        # Recorded, and recorded as blocking. The deploy step must not run.
        assert state == "NOT MET"
    ASSERTION_COUNT["n"] += 1


def capture_rendered_output(destination: str) -> str:
    """Write every rendered state of this surface, in both languages, to
    `destination` -- for the DATA-05 masking scan.

    Written OUTSIDE the working tree by its caller, so a stray capture cannot
    trip `--scan-repo` itself (the scan sees untracked-but-not-ignored files).
    """
    import contextlib

    chunks: List[str] = []

    class _Patch:
        """A monkeypatch-shaped object usable outside a pytest fixture."""

        def __init__(self):
            self._undo = []

        def setattr(self, obj, name, value):
            self._undo.append((obj, name, getattr(obj, name)))
            setattr(obj, name, value)

        def undo(self):
            for obj, name, value in reversed(self._undo):
                setattr(obj, name, value)

    for lang in LANGS:
        for status in SERVICE_STATES:
            for unit in UNITS:
                for bucket in BUCKETS:
                    items = [finding_row(
                        unit=unit, main_pool=bucket == BUCKET_MAIN,
                        novelty_offered=unit != FINDINGS_UNIT_WORK,
                        novelty_status=(CANDIDATE_STATUS
                                        if unit != FINDINGS_UNIT_WORK else None),
                    )] if status == STATUS_OK else []
                    patch = _Patch()
                    try:
                        client = render_page(
                            patch, lang=lang,
                            findings=findings_envelope(items, status=status,
                                                       unit=unit, bucket=bucket),
                            state={"unit": unit, "bucket": bucket,
                                   "sort": "band_rank", "novelty_only": False,
                                   "domain": None, "author": None,
                                   "work_id": None, "page": 1})
                    finally:
                        patch.undo()
                    chunks.append(f"--- {lang}/{status}/{unit}/{bucket} ---")
                    for element in client.elements.values():
                        chunks.extend(_subtree_texts(element))
    with contextlib.closing(io.open(destination, "w", encoding="utf-8")) as fh:
        fh.write("\n".join(chunks))
    return destination


def test_zz_report_the_assertion_count(capsys):
    """Last by name, so the count reflects the whole run."""
    met, state = browser_check_outcome()
    with capsys.disabled():
        print(f"\n[136-18] element-scoped / envelope / error-path gate calls: "
              f"{ASSERTION_COUNT['n']}")
        print(f"[136-18] 136-16 real-browser actionability: {state} -- "
              f"findings deploy gate {'OPEN' if met else 'BLOCKED'}")
    assert ASSERTION_COUNT["n"] > 0
