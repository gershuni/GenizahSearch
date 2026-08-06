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
    LAUNCH_CONTRIBUTION_SHADES,
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
#: The LEDE pair (2026-08-05): the UNCONDITIONAL main-pool population, which is
#: a different population from `total` and from the shade-filtered manuscript
#: count above. Sentinels like the rest, and swept by the same assertion that
#: they are in neither half of 136-22's forbidden list.
SENTINEL_POOL_TOTAL = 64111
SENTINEL_POOL_MANUSCRIPTS = 52111
#: The SECOND pool's size (owner ruling, 2026-08-05). A sentinel like the rest,
#: and swept by the same assertion that it is in neither half of 136-22's
#: forbidden list -- the figure the invitation shows is read from the artifact
#: at request time and may be a literal nowhere.
SENTINEL_MORE_POOL_TOTAL = 46111
#: The APPROVED headline's own two figures (owner ruling, 2026-08-05): the
#: all-in-all identification count and the distinct-work count. The fragment
#: figure the lede shows is `SENTINEL_CORPUS_MANUSCRIPTS`, which already exists.
SENTINEL_IDENTIFICATION_TOTAL = 39444
SENTINEL_WORK_TOTAL = 2555

SENTINEL_VALUES: Tuple[int, ...] = (
    SENTINEL_TOTAL,
    *[c for _s, c, _m in SENTINEL_SHADES],
    *[m for _s, _c, m in SENTINEL_SHADES],
    SENTINEL_MAIN_POOL_MANUSCRIPTS,
    SENTINEL_CORPUS_MANUSCRIPTS,
    SENTINEL_CORPUS_PAGES,
    SENTINEL_POOL_TOTAL,
    SENTINEL_POOL_MANUSCRIPTS,
    SENTINEL_MORE_POOL_TOTAL,
    SENTINEL_IDENTIFICATION_TOTAL,
    SENTINEL_WORK_TOTAL,
)

#: The APPROVED headline, driven from an envelope whose every figure is a
#: sentinel. Registered here so the capture below and the provenance assertion
#: read one definition.
_APPROVED_HEADLINE_FIGURES: Tuple[Tuple[str, int], ...] = (
    ("corpus_manuscript_count", SENTINEL_CORPUS_MANUSCRIPTS),
    ("work_total", SENTINEL_WORK_TOTAL),
    ("identification_total", SENTINEL_IDENTIFICATION_TOTAL),
    ("main_pool_total", SENTINEL_POOL_TOTAL),
    ("more_pool_total", SENTINEL_MORE_POOL_TOTAL),
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


def sentinel_launch_envelope_with_lede() -> Dict[str, Any]:
    """The same envelope PLUS the two unconditional main-pool keys.

    Kept separate from `sentinel_launch_envelope` deliberately: that one is what
    proves the headline still DEGRADES to the previous block when an older
    sidecar supplies no lede figure, and folding the new keys into it would
    delete that proof while looking like an improvement.
    """
    envelope = sentinel_launch_envelope()
    envelope["meta"]["main_pool_total"] = SENTINEL_POOL_TOTAL
    envelope["meta"]["main_pool_total_manuscript_count"] = SENTINEL_POOL_MANUSCRIPTS
    return envelope


def sentinel_launch_envelope_with_pool_size() -> Dict[str, Any]:
    """The same envelope PLUS the SECOND pool's size (owner ruling, 2026-08-05).

    Kept separate for the same reason as the lede pair above:
    `sentinel_launch_envelope` is what proves the pool invitation still
    DEGRADES to its digit-free sentence when the artifact supplies no size, and
    folding this key into it would delete that proof.
    """
    envelope = sentinel_launch_envelope()
    envelope["meta"]["more_pool_total"] = SENTINEL_MORE_POOL_TOTAL
    return envelope


def sentinel_launch_envelope_approved() -> Dict[str, Any]:
    """The APPROVED headline's envelope: the lede pair, the second pool's size
    and the two all-in-all figures.

    Separate from the three above for the same reason they are separate from
    each other: each one is what proves that the block below it still renders
    when its own key is absent, and folding them together would delete three
    degradation proofs while looking like tidying.
    """
    envelope = sentinel_launch_envelope_with_lede()
    envelope["meta"]["more_pool_total"] = SENTINEL_MORE_POOL_TOTAL
    envelope["meta"]["identification_total"] = SENTINEL_IDENTIFICATION_TOTAL
    envelope["meta"]["work_total"] = SENTINEL_WORK_TOTAL
    return envelope


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
        "divergent": False,
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
        divergent=True,
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
                      bucket=BUCKET_MAIN, sort="band_rank",
                      divergent_included=False) -> Dict[str, Any]:
    """One findings envelope, through the SHIPPED constructors.

    `divergent_included` defaults FALSE to match the shipped service under the
    default view, and is a parameter rather than a constant so a caller can
    paint the reconciliation line's other two wordings."""
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
                             "divergence": "hidden",
                             # THE KEY THE RESULT BAR ACTUALLY READS as of
                             # 2026-08-06. Omitting it made the reconciliation
                             # line take its "the envelope did not say" early
                             # return on every render in this fixture's reach --
                             # so the line never painted, and the masking
                             # capture's line-coverage gate caught that the scan
                             # had never looked at its wording. A fixture that
                             # silently withholds a key the shipped service
                             # always emits is not a smaller fixture; it is a
                             # different envelope shape than production's.
                             "divergent_included": divergent_included,
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


def render_rows(items, lang="en", sidecar_version=None):
    def _paint():
        for item in items:
            fr.render_finding_row(item, lang, sidecar_version=sidecar_version)

    return _client_render(_paint)


def render_rows_with_loader(*, unit, lang="en"):
    """ONE grouped row with an expansion attached but NOT opened.

    The loader is never called: these callers assert the CLOSED state's
    structure -- where the toggle sits relative to the children container, and
    how it is styled -- which is exactly what the reader meets first and exactly
    what a text scan cannot see. A loader that returned rows would add children
    and make the ordering assertion pass for the wrong reason.
    """
    return _client_render(
        lambda: fr.render_finding_row(
            finding_row(unit=unit), lang,
            load_children=lambda _row, _page=1: None))


def _the_row(client):
    """The single result row, for an assertion about its CHILD ORDER.

    Resolved by class and asserted UNIQUE rather than taken as "the first one": a
    helper that silently picked one of several rows would make an ordering claim
    about whichever happened to render first. Uniqueness holds because
    `render_rows_with_loader` paints one row and never opens it, so no child rows
    exist -- and if that ever changes, this fails loudly instead of quietly
    measuring a child.
    """
    rows_ = _elements_with_class(client, fr.ROW_CLASS)
    assert len(rows_) == 1, f"expected exactly one row, got {len(rows_)}"
    return rows_[0]


def _click_handlers(element) -> List[Any]:
    """Every click handler bound to `element`, however NiceGUI recorded it.

    Read defensively across the two shapes a version can use (`_event_listeners`
    holding listener objects, or a `_click_handlers` list) rather than pinning
    one: this helper exists to DRIVE the interaction, and a version bump that
    silently found no handler would turn every driven test below into a test
    that renders and clicks nothing -- passing while checking nothing.
    """
    out: List[Any] = []
    for listener in (getattr(element, "_event_listeners", None) or {}).values():
        if getattr(listener, "type", None) == "click":
            handler = getattr(listener, "handler", None)
            if handler is not None:
                out.append(handler)
    out.extend(getattr(element, "_click_handlers", None) or [])
    return out


def _fire_click(handler):
    """Invoke a click handler the way the SHIPPED click path does.

    NiceGUI's `ui.button(on_click=fn)` registers `lambda _: handle_event(fn, ...)`
    -- a one-argument wrapper -- while `element.on('click', fn)` registers `fn`
    itself. So the two binding styles need to be called differently, and a helper
    that always called with zero arguments could only ever drive one of them:
    against the other it raises `TypeError`, which in a test reads as a broken
    test rather than as the untested button it really is.

    The arity is read from the signature rather than guessed, and a `TypeError`
    is NOT swallowed -- a click that could not be delivered must fail loudly, or
    a driven test degenerates into one that renders the closed state and asserts
    against it.
    """
    import inspect as _inspect

    try:
        parameters = _inspect.signature(handler).parameters.values()
        required = len([p for p in parameters
                        if p.default is _inspect.Parameter.empty
                        and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)])
    except (TypeError, ValueError):                          # pragma: no cover
        required = 0
    return handler(None) if required else handler()


def _render_and_press(paint, markers, *, index: int = 0):
    """Paint, then press each marker in `markers` IN ORDER, all inside ONE
    client and ONE running loop. Returns `(client, presses_delivered)`.

    A sequence rather than a single click because the later affordances only
    EXIST once an earlier one has been pressed -- show-more is rendered by the
    expansion's own load -- so it has to be resolved after the press before it,
    not up front.

    Every step ABORTS BY NAME if its marker is absent or carries no handler, and
    the delivered-press count is returned so a caller can assert presses actually
    happened. A driven test that silently delivered nothing is the failure this
    whole helper exists to prevent.
    """
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {"presses": 0}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_findings_smoke_probe")) as client:
            with client:
                result = paint()
                if asyncio.iscoroutine(result):
                    await result
                for step, marker in enumerate(markers):
                    targets = _elements_with_class(client, marker)
                    assert targets, (
                        f"_render_and_press: step {step} found no element with "
                        f"{marker!r} -- the interaction stopped here, so nothing "
                        "after it is measured")
                    handlers = _click_handlers(targets[index])
                    assert handlers, (
                        f"_render_and_press: step {step}'s {marker!r} element "
                        "has no click handler")
                    for handler in handlers:
                        outcome = _fire_click(handler)
                        if asyncio.iscoroutine(outcome):
                            await outcome
                        holder["presses"] += 1
        holder["client"] = client

    asyncio.run(_run())
    return holder["client"], holder["presses"]


def render_and_click(paint, marker: str, *, index: int = 0):
    """Paint, then fire the click on the `marker` element INSIDE the same client
    and the same running loop, and return the client.

    One loop because `_client_render` closes its client on exit, so a handler
    fired afterwards paints into a dead slot stack; one client because the
    element only exists in it.

    ABORTS BY NAME when the marker matches nothing or carries no handler. A
    driven test whose click silently did nothing is the "gate that cannot fail"
    shape -- it would render the closed state and assert against it forever.
    """
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
                targets = _elements_with_class(client, marker)
                assert targets, (
                    f"render_and_click: no element carries {marker!r} -- the "
                    "interaction cannot be driven, so nothing below is measured")
                handlers = _click_handlers(targets[index])
                assert handlers, (
                    f"render_and_click: the {marker!r} element has no click "
                    "handler -- a click that fires nothing renders the closed "
                    "state and asserts against it")
                for handler in handlers:
                    outcome = _fire_click(handler)
                    if asyncio.iscoroutine(outcome):
                        await outcome
        holder["client"] = client

    asyncio.run(_run())
    return holder["client"]


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
def test_every_figure_of_the_APPROVED_headline_equals_the_envelope_value(lang):
    """The owner-approved block (2026-08-05), figure by figure.

    Five sentinels, none of which appears in any artifact or in either half of
    136-22's forbidden list, so a hardcode fails here in whatever form it took.
    The fragment figure is asserted as the LEDE element specifically -- the one
    number on this page that must not silently become a different population.
    """
    envelope = sentinel_launch_envelope_approved()
    client = render_headline(envelope, lang)
    text = scoped_text(client, fr.LAUNCH_CLASS)

    for key, sentinel in _APPROVED_HEADLINE_FIGURES:
        assert envelope["meta"][key] == sentinel, f"the fixture drifted at {key}"
        assert "{:,}".format(sentinel) in text, (
            f"{key} did not reach the headline: {text!r}")

    lede = _elements_with_class(client, fr.LAUNCH_FRAGMENTS_CLASS)
    assert len(lede) == 1
    assert getattr(lede[0], "text", None) == "{:,}".format(SENTINEL_CORPUS_MANUSCRIPTS)
    # ...and the CONTRIBUTION, on its own (shade-filtered) basis, is still here.
    assert "{:,}".format(SENTINEL_TOTAL) in text
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_approved_headline_claims_no_corpus_denominator(lang):
    """`corpus_manuscript_count` counts fragments this release identified
    something on and `corpus_page_count` counts pages carrying a claim, while
    the project's corpus is ~255,615 manuscript records. "In the whole corpus"
    over those two figures overstated coverage by roughly 6.6x, on the most
    prominent line of a scholarly surface."""
    text = scoped_text(render_headline(sentinel_launch_envelope_approved(), lang),
                       fr.LAUNCH_CLASS)
    for claim in ("whole corpus", "entire corpus", "כלל האוסף", "כל האוסף"):
        assert claim not in text, f"the headline still claims {claim!r}: {text!r}"
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


def _tooltip_text_for(client, element) -> str:
    """The text of the tooltip attached to `element`.

    NiceGUI attaches a `Tooltip` as a SIBLING targeting the element's own id,
    not as a descendant -- so `_subtree_texts` cannot see it and an assertion
    written over the subtree would silently prove nothing about the tooltip.
    """
    target = f"#c{element.id}"
    return "\n".join(
        str(getattr(node, "text", "") or "")
        for node in client.elements.values()
        if (getattr(node, "_props", None) or {}).get("target") == target
    )


@pytest.mark.parametrize("lang", LANGS)
def test_a_divergent_row_is_marked_and_an_ordinary_one_is_not(lang):
    """Ruling F: a reader who has opened the divergence axis must never be able
    to mistake one of its rows for an ordinary finding."""
    divergent = render_rows([finding_row(divergent=True)], lang)
    ordinary = render_rows([finding_row(divergent=False)], lang)

    marked = _elements_with_class(divergent, fr.ROW_DIVERGENCE_CLASS)
    assert len(marked) == 1, "a divergent row carries exactly one marker"
    assert not _elements_with_class(ordinary, fr.ROW_DIVERGENCE_CLASS), (
        "an ordinary finding must carry no divergence marker at all")

    # VISIBLE, and both facts are on its face: that the two records disagree,
    # and that neither has been adjudicated. `divergence_correctness` is NULL
    # on every shipped row, so a marker stating only the disagreement would
    # leave a reader free to supply the missing half themselves.
    assert set(scoped_text(divergent, fr.ROW_DIVERGENCE_CLASS).splitlines()) == {
        ds.divergence_chip(lang)}
    # And the full two-sentence statement is one hover away, on the marker's
    # own tooltip -- the same place the panel keeps its band labels.
    assert _tooltip_text_for(divergent, marked[0]) == ds.divergence_warning(lang)
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_divergence_marker_is_on_every_unit_not_only_the_default_one(lang):
    """`novelty_status` is NULL on a per-work row and on a mixed manuscript
    row, so a marker derived from the shade would be absent exactly where a
    reader has the least context to notice."""
    for unit in UNITS:
        client = render_rows(
            [finding_row(unit=unit, divergent=True, novelty_status=None,
                         novelty_offered=False)], lang)
        assert _elements_with_class(client, fr.ROW_DIVERGENCE_CLASS), unit
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_divergence_marker_is_neutral_and_takes_no_side(lang):
    """D-24: no colour-coding by kind, no confidence tiering, and -- ruling F --
    no assertion about which of the two records is right.

    The class list is the whole treatment: a plain `.chip`, the same neutral
    token the relation vocabulary already uses, with nothing keyed on WHICH
    divergence shade produced it."""
    client = render_rows([finding_row(divergent=True)], lang)
    element = _elements_with_class(client, fr.ROW_DIVERGENCE_CLASS)[0]
    assert set(element._classes) == {"chip", fr.ROW_DIVERGENCE_CLASS}, (
        "the marker has grown a treatment of its own")

    # The marker is ONE string with no per-shade branch, so a shade cannot
    # reach it and cannot be styled differently from its sibling.
    code = _code_lines(COMPONENT_PATH)
    for shade in ("diverges_work", "diverges_part"):
        assert shade not in code

    text = "\n".join((scoped_text(client, fr.ROW_DIVERGENCE_CLASS),
                      _tooltip_text_for(client, element)))
    assert ds.divergence_warning(lang) in text, "the tooltip half is in scope too"
    forbidden = {
        "en": ("wrong", "incorrect", "error", "probably", "likely", "false"),
        "he": ("שגוי", "טעות", "מוטעה", "כנראה"),
    }[lang]
    for word in forbidden:
        assert word not in text.lower(), (
            f"the marker adjudicates: {word!r} in {text!r}")
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


def test_the_component_styles_through_ratified_classes_not_new_rules():
    """The COMPONENT adds no stylesheet rule of its own -- it paints ratified
    classes and, where a state has no class, inline side-neutral values.

    This replaced a `git diff --stat HEAD -- common.css` assertion, and the swap
    was forced by a real defect rather than chosen: the landed block's
    `.gs-discovery .row` mobile rule matched every `ui.row()` on the page and
    stacked the filter controls on every phone, so the only thing the no-diff
    guard could still do was hold that bug in place. `tests/test_findings_page.py`
    now asserts the rule's SHAPE, which is what the guard was for.

    What this test keeps is the component-side half: the module must not carry a
    `<style>` block or an `ui.add_css`/`add_head_html` of its own, because a
    second place styling this surface is how the two drift.
    """
    # CODE LINES ONLY -- the same discipline this file's other source scans use.
    # A docstring that NAMES a forbidden construct in order to say it is
    # forbidden is not an instance of it, and a raw-file scan fails on the
    # explanation rather than on a defect.
    code = _code_lines(COMPONENT_PATH)
    for forbidden in ("add_css", "add_sass", "add_scss", "add_style",
                      "add_head_html", "<" + "style"):
        assert forbidden not in code, (
            f"the row component injects styling via {forbidden!r} -- this "
            "surface is styled in one place")
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
def test_no_row_names_its_bucket_and_neither_pool_is_the_exception(lang):
    """The chip was removed from BOTH pools on 2026-08-06, and BOTH is the part
    a test has to hold.

    It went because it could not vary: the page offers two buckets and no union
    (`_OFFERED_BUCKETS`), and an expansion's children inherit their parent's
    bucket, so the label was measured constant across every row of all six
    unit x bucket combinations -- fifty repetitions a page of what the result
    bar states once above them.

    D-24 is why this asserts on both pools rather than on one. Dropping the
    name from the main pool alone would leave the second pool's rows carrying a
    marker the first pool's do not, which is exactly the per-bucket differential
    treatment D-24 forbids. So the assertion is symmetric, and a re-introduction
    for one pool fails here even if someone believes they are restoring a
    feature.

    Asserted against the SHARED vocabulary, not a literal: the bucket names
    still have exactly one definition, and the result bar and launch statistics
    still read it."""
    items = [finding_row(main_pool=True), finding_row(main_pool=False)]
    client = render_rows(items, lang)
    # Scoped to the ROWS, not the whole client: the result bar legitimately
    # names the bucket once, and a client-wide scan would fail on the correct
    # rendering rather than on a row that misbehaves.
    rendered = scoped_text(client, fr.ROW_CLASS)
    for in_main in (True, False):
        name = bucket_label(in_main, lang)
        assert name not in rendered, (
            f"a row names its bucket ({name!r}) -- that label is constant for "
            "the whole page and belongs in the result bar, once")
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# THE OWNER'S 2026-08-06 LAYOUT REPORT. Four cosmetic findings from reading the
# live beta, each asserted over STRUCTURE rather than over rendered text -- a
# layout defect leaves the text identical, which is why none of these was caught
# by the ~570 assertions already in this suite and the sibling one.
# ---------------------------------------------------------------------------


def test_the_close_control_renders_ABOVE_the_children_it_reveals():
    """"Expanding requires excessive downward scrolling to collapse the list."

    The report read this as a missing toggle-back, and the button DOES already
    retitle itself to "Hide the individual matches" the moment it opens. The
    actual defect was DOM ORDER: `body` was created before `button`, so the only
    control that could close the group rendered BELOW every child it revealed --
    25 rows away on first open, and up to 2,981 on the heaviest work in the
    served artifact. The affordance to undo the click was reliably off-screen at
    the moment it was wanted.

    Asserted as an INDEX COMPARISON inside the row's own child list, because that
    is the fact that matters and it is invisible to any text scan: both orderings
    render exactly the same strings.
    """
    client = render_rows_with_loader(unit=FINDINGS_UNIT_WORK)
    row = _the_row(client)
    children = list(row)
    classes = [set(getattr(child, "_classes", None) or []) for child in children]
    expander = next(i for i, c in enumerate(classes) if fr.ROW_EXPANDER_CLASS in c)
    body = next(i for i, c in enumerate(classes) if fr.ROW_CHILDREN_CLASS in c)
    assert expander < body, (
        "the expansion's toggle renders after the children container, so the "
        "only control that closes the group sits below every row it revealed")
    ASSERTION_COUNT["n"] += 1


def test_the_close_control_sticks_so_it_stays_reachable_inside_a_long_group():
    """Ordering alone fixes the FIRST screen; a reader 300 rows into a group
    still has to scroll back up. The toggle is sticky so it stays on screen.

    The background is asserted too, and it is not decoration: a transparent
    sticky element lets the scrolling children pass through it and its own label
    becomes unreadable over its own list.

    Every property is block-axis or non-directional, so nothing here needs an RTL
    mirror -- which is what lets it be inline in a module that ships no CSS.
    """
    client = render_rows_with_loader(unit=FINDINGS_UNIT_WORK)
    button = _elements_with_class(client, fr.ROW_EXPANDER_CLASS)[0]
    style = getattr(button, "_style", None) or {}
    assert style.get("position") == "sticky", (
        "the expansion toggle is not sticky -- inside a 2,981-child group it "
        "scrolls out of reach")
    assert style.get("background"), (
        "a sticky toggle with no background lets the children scroll through it")

    # NOT `top: 0`, and this is the assertion that took a second attempt to get
    # right. `web/main.py` renders the site chrome as a Quasar `ui.header` at
    # `height: 64px`, which is `position: fixed` and therefore outside this
    # element's scroll flow -- so a sticky pinned at the viewport top parks
    # itself UNDER the header and is permanently invisible. "Always on screen
    # and never visible" is worse than the scroll-back-up it replaces, because a
    # reader has no reason to look for it. The offset must clear the chrome.
    top = style.get("top") or ""
    assert top not in ("", "0", "0px"), (
        f"the sticky toggle is pinned at {top!r} -- it sits underneath the "
        "64px fixed site header and can never be seen")
    for physical in ("left", "right", "margin-left", "margin-right"):
        assert physical not in style, (
            f"the sticky toggle gained the physical property {physical!r}")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_every_row_carries_the_same_separator_and_no_pool_is_the_exception(lang):
    """"List items blend together." A hairline between rows, on EVERY row.

    BOTH POOLS, and that symmetry is what keeps this clear of D-24. A separator
    carries no information about the row it sits under and cannot be read as a
    verdict on it -- but the moment it varied by pool, novelty or band it would
    become exactly the per-row treatment D-24 prohibits. So this asserts the
    two pools are styled IDENTICALLY, not merely that a border exists.
    """
    items = [finding_row(main_pool=True), finding_row(main_pool=False)]
    client = render_rows(items, lang)
    borders = [
        (getattr(row, "_style", None) or {}).get("border-block-end")
        for row in _elements_with_class(client, fr.ROW_CLASS)
    ]
    assert len(borders) == 2, f"expected two rows, got {len(borders)}"
    assert all(borders), "a result row carries no separator"
    assert borders[0] == borders[1], (
        f"the two pools' rows are separated differently ({borders!r}) -- a "
        "per-pool row treatment is what D-24 forbids")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("lang", LANGS)
def test_the_two_lede_figures_share_one_line_instead_of_stacking(lang):
    """The two display-size figures stacked, pushing the first result below the
    fold. They are the two halves of one statement -- this many fragments,
    matched to that many works -- so they now share a wrapping flex row.

    WHAT MUST NOT HAVE MOVED is asserted in the same test, because the risk of
    this change is not that it fails to wrap: it is that the wrapper reparents
    the figures and quietly breaks the bidi structure four sibling tests pin.
    Each figure keeps its own inner container, so the digit run and its label
    stay two elements that cannot reorder at the boundary.
    """
    client = render_headline(sentinel_launch_envelope_approved(), lang)
    wrappers = _elements_with_class(client, fr.LAUNCH_LEDE_ROW_CLASS)
    assert len(wrappers) == 1, (
        "the two lede figures are not in one shared row -- they stack and push "
        "the results below the fold")

    inner = [set(getattr(child, "_classes", None) or []) for child in wrappers[0]]
    assert any(fr.LAUNCH_LEDE_CLASS in c for c in inner), (
        "the fragment lede is not inside the shared row")
    assert any(fr.LAUNCH_MATCHED_CLASS in c for c in inner), (
        "the matched-works line is not inside the shared row")

    # The bidi structure the other tests depend on: figure and label are still
    # SEPARATE elements under the lede's own container, never one string.
    lede = [
        element for element in _elements_with_class(client, fr.LAUNCH_LEDE_CLASS)
        if fr.LAUNCH_LEDE_ROW_CLASS not in (getattr(element, "_classes", None) or [])
    ]
    assert len(lede) == 1
    figure = _elements_with_class(client, fr.LAUNCH_FRAGMENTS_CLASS)[0]
    label = _elements_with_class(client, fr.LAUNCH_FRAGMENTS_LABEL_CLASS)[0]
    assert figure.parent_slot.parent is lede[0]
    assert label.parent_slot.parent is lede[0]

    # ...and it must WRAP rather than depend on a breakpoint, so a phone stacks
    # them again with no media query to maintain.
    assert "flex-wrap" in (wrappers[0]._classes or []), (
        "the shared lede row does not wrap -- on a phone the two figures would "
        "overflow instead of stacking")
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
               "sort": "band_rank", "novelty_view": fp.NOVELTY_VIEW_ALL,
               "domain": None,
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
               "novelty_view": fp.NOVELTY_VIEW_ALL, "domain": None, "author": None,
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


@pytest.fixture(autouse=True)
def _domain_label_cache_is_deterministic():
    """Start every render with the Hebrew domain-label cache BUILT AND EMPTY.

    A Hebrew render primes it for real otherwise: 1.5 GB of FJMS opened inside
    the test process, and rendered labels that depend on whether the machine
    happens to carry a sidecar. Built-and-empty is a state production genuinely
    reaches (FJMS absent) and is the one the English-label assertions here are
    about; the test below injects a map explicitly for the Hebrew case.
    """
    import web.discovery_genre_labels as gl

    gl.reset_for_tests()
    gl._STATE["map"] = {}
    yield
    gl.reset_for_tests()


@pytest.mark.parametrize("lang", LANGS)
def test_a_hebrew_domain_label_is_honest_and_hides_no_english(monkeypatch, lang):
    """`works.genre` is stored in ENGLISH and is this page's main facet, so a
    Hebrew reader used to get an English filter list. The Hebrew names come from
    FJMS (`DomainHeb` / `ParentDomainHeb`) at DISPLAY time -- no re-bake, no
    invented translation -- and the translated surface must still pass the SIX
    detector gate, in the language it is rendered in."""
    import web.discovery_genre_labels as gl

    english = "Liturgy and Brakhot / Common Prayers"
    hebrew_parent, hebrew_leaf = "תפילה וברכות", "תפילות שכיחות"
    monkeypatch.setitem(gl._STATE, "map", {
        "Liturgy and Brakhot": hebrew_parent, "Common Prayers": hebrew_leaf})

    offered = [facet_row(level="domain", value="Liturgy and Brakhot",
                         label="Liturgy and Brakhot", parent=None, is_leaf=False),
               facet_row(level="domain", value=english, label=english,
                         parent="Liturgy and Brakhot", is_leaf=True)]
    client = render_page(monkeypatch, lang=lang,
                         facets={"domain": facets_envelope("domain", items=offered)})
    marker = f"{fp.FILTER_BAR_CLASS}-domain-items"
    rendered = scoped_text(client, marker)
    if lang == "he":
        assert hebrew_parent in rendered and hebrew_leaf in rendered, (
            f"the Hebrew reader still sees English domain names: {rendered!r}")
    else:
        assert "Liturgy and Brakhot" in rendered
        assert hebrew_parent not in rendered
    assert_surface_honesty(scoped_fragment(client, marker),
                           scope_selector=marker, lang=lang)
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


def _broken_service(tmp_path) -> DiscoveryService:
    """A sidecar whose findings query RAISES, so the service's own `logger.error`
    fires and there is a REAL log line to scan.

    The tables the probe checks for are present; `works` -- which
    `_FINDINGS_FROM` joins -- is not, so the failure happens inside the query
    rather than at the availability gate."""
    import sqlite3

    db_path = tmp_path / "broken.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            "CREATE TABLE discovery_identification (identification_id TEXT);"
            "CREATE TABLE manuscript_display (sys_id TEXT);")
        conn.commit()
    finally:
        conn.close()
    return DiscoveryService(path_provider=lambda: str(db_path),
                            availability_callable=lambda: True,
                            sidecar_version_provider=lambda: "broken")


@pytest.mark.parametrize("lang", LANGS)
def test_every_forced_error_path_is_honest(fixture_service, tmp_path, lang, caplog):
    caplog.set_level(logging.DEBUG)
    modes = _error_modes(fixture_service)
    assert len(modes) >= 6, f"only {len(modes)} failure modes driven"

    # A REAL log line, forced. Without this the log loop below is vacuous, and a
    # vacuous loop proves the scan ran, not that it saw anything.
    envelope = _broken_service(tmp_path).get_findings_enveloped()
    assert envelope["status"] == STATUS_UNAVAILABLE
    assert envelope["meta"]["reason"] == "query_failed"
    logged = [r for r in caplog.records if "get_findings" in r.getMessage()]
    assert logged, "the forced query failure produced no log line to scan"

    for mode, message in modes:
        assert_error_path_honesty(message, lang=lang, where=f"error path/{mode}")
        ASSERTION_COUNT["n"] += 1
    for record in caplog.records:
        assert_error_path_honesty(record.getMessage(), lang=lang, where="log line")
        ASSERTION_COUNT["n"] += 1


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
                                   "sort": "band_rank",
                                   "novelty_view": fp.NOVELTY_VIEW_ALL,
                                   "domain": None, "author": None,
                                   "work_id": None, "page": 1})
                    finally:
                        patch.undo()
                    chunks.append(f"--- {lang}/{status}/{unit}/{bucket} ---")
                    for element in client.elements.values():
                        chunks.extend(_subtree_texts(element))

        # TWO STATES THE MATRIX ABOVE CANNOT REACH, each one a branch that
        # PAINTS. A capture that never enters a painting branch is a masking
        # scan that has never looked at what that branch puts on a screen, and
        # the line-coverage gate fails by name when one is missed.
        #
        # (1) the four-level headline. The matrix drives the DEGRADED block --
        #     `sentinel_launch_envelope` deliberately carries no lede figure --
        #     so the lede, its fragment span and the separator are painted only
        #     here.
        # (2) an `ok` facet cascade with NO items, which paints the "no matches
        #     under the current filters" line. The matrix's facet fixture always
        #     has a row.
        # (3) the SIZED pool invitation. The matrix's launch envelope carries no
        #     `more_pool_total` -- that is what proves the invitation degrades --
        #     so the counted sentence is painted only here, and a scan that never
        #     saw it would never have looked at it.
        for label, kwargs in (
            ("lede-headline", {"launch": sentinel_launch_envelope_with_lede()}),
            ("empty-facets", {"facets": {
                level: facets_envelope(level, items=[])
                for level in ("domain", "author", "work")}}),
            ("sized-pool-invite",
             {"launch": sentinel_launch_envelope_with_pool_size()}),
            # (4) the APPROVED headline: the fragments/works lede, the
            #     all-in-all line and the pool split. Painted only from an
            #     envelope carrying all of those keys, so without this case the
            #     scan would never have looked at any of them.
            ("approved-headline",
             {"launch": sentinel_launch_envelope_approved()}),
            # (5) the AUTHOR/WORK selects with a selection the cascade does not
            #     offer. Two branches live only here: the option added back for
            #     an applied-but-unoffered filter (without it the control would
            #     render blank while the query still filtered), and the
            #     no-op guard in the pick handler that stops a re-render's own
            #     value assignment from looping through `refresh`.
            ("selected-facets", {"state": {
                "unit": "identification", "bucket": BUCKET_MAIN,
                "sort": "band_rank", "novelty_view": fp.NOVELTY_VIEW_ALL,
                "domain": None,
                "author": "AN AUTHOR THE CASCADE DOES NOT OFFER",
                "work_id": "w000404", "work_label": None, "page": 1}}),
        ):
            patch = _Patch()
            try:
                client = render_page(patch, lang=lang, **kwargs)
            finally:
                patch.undo()
            chunks.append(f"--- {lang}/{label} ---")
            for element in client.elements.values():
                chunks.extend(_subtree_texts(element))

    with contextlib.closing(io.open(destination, "w", encoding="utf-8")) as fh:
        fh.write("\n".join(chunks))
    return destination


def test_zz_report_the_assertion_count(capsys):
    """The gate-call count, reported as a FLOOR rather than as a total.

    The `zz_` prefix was chosen when this was the last test in the file, on the
    belief that the name ordered it last. IT DOES NOT: pytest collects in
    DEFINITION order, so every test appended below this line runs AFTER it and
    its increments are invisible here -- measured, not assumed (this test
    collects at position 167 of 183).

    Renaming or moving it would not fix that either; the next appended test
    would silently fall outside again. So the assertion is deliberately a FLOOR
    (`> 0`, "at least one gate call really ran"), which is what it can honestly
    prove from where it sits, and the printed figure is labelled a floor so a
    reader does not mistake it for a total. `test_every_gate_call_site_is_a_real
    _assertion`-style completeness is not what this test provides.
    """
    met, state = browser_check_outcome()
    with capsys.disabled():
        print(f"\n[136-18] element-scoped / envelope / error-path gate calls: "
              f"{ASSERTION_COUNT['n']} (a FLOOR -- tests defined below this one "
              f"run after it and are not counted)")
        print(f"[136-18] 136-16 real-browser actionability: {state} -- "
              f"findings deploy gate {'OPEN' if met else 'BLOCKED'}")
    assert ASSERTION_COUNT["n"] > 0


# ---------------------------------------------------------------------------
# §3.9 -- AN UNKNOWN LAUNCH SHADE WAS ECHOED VERBATIM INTO AN EXCEPTION MESSAGE.
#
# `shade` arrives from the launch envelope's own items, so it is ARTIFACT
# CONTENT, and an exception message is an egress that reaches a log and,
# uncaught, a reader -- without passing through the markup scan or the envelope
# scan. The message already refused to enumerate the valid vocabulary; it still
# echoed the value it was handed, on the reasoning that a value reaching that
# branch is by construction not one of OUR strings. That argues only about what
# it is NOT.
# ---------------------------------------------------------------------------

def test_the_shade_error_withholds_the_value_it_was_handed(lang="en"):
    """Driven with a value that would MATTER if it leaked.

    `not_a_shade` -- the value the existing error-path fixtures use -- is
    synthetic and harmless, so a test using it alone would pass against a
    message that echoes everything. The probe below is shaped like the class of
    content D-25 exists for: a shelfmark-ish token that could only have come out
    of an artifact.
    """
    probe = "m_source_shelfmark_probe_value_9f2c"
    with pytest.raises(ValueError) as excinfo:
        fr.launch_shade_label(probe, lang)
    message = str(excinfo.value)

    assert probe not in message, (
        "the exception echoes the artifact-derived value it received; an "
        "exception message reaches a log and, uncaught, a reader without "
        "passing through either scan")
    # The message must still be USEFUL: it names the field and the authority.
    assert "launch_shade_label" in message
    assert "LAUNCH_CONTRIBUTION_SHADES" in message
    # ...and still must not enumerate that authority's members.
    for shade in LAUNCH_CONTRIBUTION_SHADES:
        assert shade not in message
    assert_error_path_honesty(message, lang=lang, where="launch_shade_label")
    ASSERTION_COUNT["n"] += 1


def test_no_launch_row_field_reaches_an_exception_message_verbatim():
    """The same rule over the OTHER two fields of a launch shade row, so the
    boundary is about the row rather than about one field that was reported."""
    probes = {
        "shade": "probe_shade_a1b2",
        "identification_count": "probe_count_c3d4",
        "manuscript_count": "probe_count_e5f6",
    }
    messages = []
    with pytest.raises(ValueError) as excinfo:
        fr.launch_shade_label(probes["shade"])
    messages.append(str(excinfo.value))

    # The counts go through `_count`, which never raises -- it degrades to an
    # empty string. Recorded here so the claim covers the whole row rather than
    # only the field that raises.
    assert fr._count(probes["identification_count"]) == ""
    assert fr._count(probes["manuscript_count"]) == ""

    blob = "\n".join(messages)
    for value in probes.values():
        assert value not in blob, f"{value!r} reached an exception message"
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# H1 -- THE BETA NOTE, and H2 -- THE REPORT AFFORDANCE.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", LANGS)
def test_the_beta_note_appears_in_the_head_and_in_the_howto(monkeypatch, lang):
    """Two placements: a SHORT line where a reader meets the page, and the
    fuller one with the demoted prose one click away. The head must not grow
    back into the wall of prose `_render_howto` exists to hold, so the two
    strings are different lengths on purpose."""
    client = render_page(monkeypatch, lang=lang)

    head = scoped_text(client, f"{fp.HEAD_CLASS}-beta")
    assert fp.copy_text("beta_head", lang) in head, "the head carries no beta line"

    howto = scoped_text(client, f"{fp.HOWTO_CLASS}-beta")
    assert fp.copy_text("beta_howto", lang) in howto

    assert len(fp.copy_text("beta_howto", lang)) > len(
        fp.copy_text("beta_head", lang)), (
        "the head line is not the SHORT one -- the fuller note belongs behind "
        "the disclosure")


@pytest.mark.parametrize("lang", LANGS)
def test_the_beta_note_promises_no_accuracy_and_lists_no_roadmap(lang):
    """Two owner rulings, as assertions.

    "Better identifications" reads as MORE ACCURATE on this surface, which is a
    precision claim and is prohibited even as a forward-looking promise; and
    the general form was asked for rather than a feature list."""
    for key in ("beta_head", "beta_howto"):
        text = fp.copy_text(key, lang).lower()
        for forbidden in {
            "en": ("better", "more accurate", "improved", "accuracy",
                   "precision", "reliable"),
            "he": ("טוב יותר", "מדויק", "דיוק", "אמין"),
        }[lang]:
            assert forbidden not in text, f"{key}: {forbidden!r} is an accuracy claim"
        for roadmap in {
            "en": ("evidence view", "work page", "catalogue integration", "api"),
            "he": ("תצוגת ראיות", "דף חיבור"),
        }[lang]:
            assert roadmap not in text, f"{key}: enumerates a roadmap item"



@pytest.mark.parametrize("lang", LANGS)
def test_a_row_offers_a_report_prefilled_with_its_id_and_the_data_version(lang):
    """H2. A report has to be reproducible against the exact artifact that
    produced the row, so the identifier and the version are both in the body."""
    from urllib.parse import unquote

    version = "discovery-v1-SENTINEL-VERSION"
    row = finding_row()
    client = render_rows([row], lang, sidecar_version=version)

    links = _elements_with_class(client, fr.ROW_REPORT_CLASS)
    assert len(links) == 1, f"expected one report link, got {len(links)}"
    href = (links[0]._props or {}).get("href") or ""
    assert href.startswith(f"mailto:{fr.REPORT_ADDRESS}?")

    body = unquote(href.split("body=", 1)[1])
    assert row["identification_id"] in body, "the report cannot name its row"
    assert version in body, (
        "the report does not name the artifact that produced the row, so it "
        "cannot be reproduced against it")
    assert fr.copy_text("report_link", lang) in scoped_text(
        client, fr.ROW_REPORT_CLASS)


@pytest.mark.parametrize("lang", LANGS)
def test_the_report_wording_promises_no_outcome(lang):
    """It invites a report; it does not commit to a response, a correction or a
    timeline, because the ruling created none of those -- reports feed the next
    bake."""
    for key in ("report_link", "report_subject", "report_body"):
        text = fr.copy_text(key, lang).lower()
        for promise in {
            "en": ("we will", "we'll", "fix", "correct", "remove", "within",
                   "soon", "reply", "respond", "guarantee"),
            "he": ("נתקן", "נסיר", "נחזור אליכם", "בתוך", "מובטח"),
        }[lang]:
            assert promise not in text, f"{key}: {promise!r} promises an outcome"


@pytest.mark.parametrize("lang", LANGS)
def test_the_report_TEXT_A_READER_SEES_is_what_the_honesty_gate_scans(lang):
    """THE SCANNED SURFACE IS THE DECODED MESSAGE, not the URL.

    An earlier revision of this test asserted the URL was pure ASCII, on the
    reasoning that percent-encoded Hebrew (`%D7...`) reads as dozens of
    percentages. Two measurements retired that reasoning:

    1. The markup gate extracts TEXT and never reads an attribute, so a `mailto:`
       target is not scanned as markup at all -- `_extract_scoped_text` over
       `<a href="...9%0A">Report</a>` returns `Report`.
    2. ASCII is not sufficient anyway. A sha256 digest ending in a DIGIT -- ten
       of sixteen hex characters are digits -- puts `9%0A` in the body, and the
       gate flags that as an unqualified percentage. So the ASCII property both
       failed to achieve what it claimed and hid a case it did not cover.

    What a human reads is the DECODED subject and body in their compose window,
    and that is what this scans, through the SIX-detector entry point every
    reader surface uses. Percent-encoding is transport, not prose.
    """
    import html as _html
    from urllib.parse import unquote

    # A digest ending in a DIGIT, deliberately: the case that made the ASCII
    # property look sufficient when it was not.
    row = finding_row(identification_id="a1b2c3d4e5f6" + "0" * 51 + "9")
    href = fr.report_mailto(row, lang, "discovery-v1-SENTINEL")
    subject = unquote(href.split("subject=", 1)[1].split("&", 1)[0])
    body = unquote(href.split("body=", 1)[1])
    # The REAL gate over the REAL reader-visible text -- not a second copy of
    # the percentage rule, which would be a second definition of it.
    assert_surface_honesty(
        f'<div class="report-message-probe">'
        f'{_html.escape(subject)}\n{_html.escape(body)}</div>',
        scope_selector="report-message-probe", lang=lang)
    ASSERTION_COUNT["n"] += 1
    # The reader-facing half stays bilingual.
    assert fr.copy_text("report_link", "he") != fr.copy_text("report_link", "en")


def test_no_report_link_without_an_identifier_or_a_version():
    """Both halves are required, and each absence is withheld rather than
    papered over: a row with no identifier could not name what a report is
    about, and a report naming no version is not reproducible -- which is the
    whole point of prefilling it."""
    version = "discovery-v1-SENTINEL-VERSION"

    assert fr.report_mailto(finding_row(), "en", None) is None
    assert fr.report_mailto(finding_row(), "en", "") is None

    for unit in (FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK):
        grouped = finding_row(unit=unit, identification_id=None)
        assert fr.report_mailto(grouped, "en", version) is None, unit
        client = render_rows([grouped], "en", sidecar_version=version)
        assert not _elements_with_class(client, fr.ROW_REPORT_CLASS), unit

    assert fr.report_mailto(finding_row(), "en", version) is not None


def test_the_report_body_carries_nothing_but_the_id_and_the_version():
    """D-25 binds on an email body as it does on markup: an identifier and a
    version go in, and nothing drawn from a masked source does."""
    from urllib.parse import unquote

    version = "discovery-v1-SENTINEL-VERSION"
    row = finding_row(neutral_title="A SENTINEL WORK TITLE",
                      shelfmark_display="SENTINEL SHELFMARK",
                      sys_id="SENTINEL-SYS-ID")
    body = unquote(fr.report_mailto(row, "en", version).split("body=", 1)[1])
    for leaked in ("A SENTINEL WORK TITLE", "SENTINEL SHELFMARK",
                   "SENTINEL-SYS-ID"):
        assert leaked not in body, f"{leaked!r} reached the email body"
    assert row["identification_id"] in body and version in body


def test_the_report_link_url_quotes_every_value():
    """A field reaching a URL unquoted is one data change away from breaking
    the link or smuggling a header into it."""
    row = finding_row(identification_id="id with spaces & an ampersand")
    href = fr.report_mailto(row, "en", "version with spaces")
    assert " " not in href and "\\n" not in href
    assert href.count("&") == 1, "an unquoted value introduced a second parameter"


# ---------------------------------------------------------------------------
# THE EXPANSION (owner-approved, 2026-08-05): a grouped row opens IN PLACE onto
# its child identifications, and the identification LEAF previews its manuscript.
# ---------------------------------------------------------------------------

def test_the_expansion_is_offered_only_where_the_PREDICATE_can_pin_it():
    """THE defect this table exists to prevent, measured rather than argued.

    `get_findings_enveloped` takes its filters as KEYWORDS, so an axis
    `_build_findings_filter` does not implement is accepted and silently
    IGNORED. A manuscript expansion passing `sys_id` therefore did not raise --
    it returned every row matching the reader's filters instead of the ones in
    that manuscript. A reader cannot see that kind of wrongness; they see a
    plausible list that is the wrong list.

    So the offer is bounded by the predicate: `work` (the builder has
    `work_id`), and NOT `manuscript` until the axis exists.
    """
    work = finding_row(unit=FINDINGS_UNIT_WORK)
    assert fr.expansion_target(work) == ("work_id", work["display_work_id"])
    # The leaf has nothing underneath it, and the manuscript unit has no axis.
    assert fr.expansion_target(finding_row()) is None
    assert fr.expansion_target(finding_row(unit=FINDINGS_UNIT_MANUSCRIPT)) is None

    # ...and every axis the table DOES name is one the shipped builder accepts.
    for unit, pair in fr.EXPANSION_KEY_BY_UNIT.items():
        if pair is None:
            continue
        _field, axis = pair
        assert axis in fr.EXPANSION_SUPPORTED_AXES, (
            "the {!r} expansion pins {!r}, which _build_findings_filter does "
            "not implement -- it would be passed and silently dropped".format(
                unit, axis))
    ASSERTION_COUNT["n"] += 1


def test_the_supported_axis_set_is_READ_FROM_the_builder_not_listed():
    """A hand-written list is one edit away from claiming an axis the predicate
    lacks. Compared against `inspect` over the real builder, so this fails if
    the derivation is ever replaced by a literal."""
    import inspect

    from shared.discovery_service import _build_findings_filter

    authority = frozenset(
        inspect.signature(_build_findings_filter).parameters) - {"unit", "bucket"}
    assert fr.EXPANSION_SUPPORTED_AXES == authority
    assert fp._EXPANSION_SUPPORTED_AXES == authority, (
        "the page and the component disagree about which axes are real")


def test_a_grouped_row_expands_and_a_leaf_row_does_not():
    """The affordance appears on the unit where it has a meaning, and nowhere
    else. A leaf has no children; offering it an expander would open onto
    nothing."""
    async def _load(_row, _page=1):
        return findings_envelope([finding_row()])

    client = _client_render(lambda: fr.render_finding_row(
        finding_row(unit=FINDINGS_UNIT_WORK), "en", load_children=_load))
    assert _elements_with_class(client, fr.ROW_EXPANDER_CLASS), (
        "a work row offers no expander")

    leaf = _client_render(lambda: fr.render_finding_row(
        finding_row(), "en", load_children=_load))
    assert not _elements_with_class(leaf, fr.ROW_EXPANDER_CLASS)


def test_the_children_are_the_SAME_row_anatomy_as_a_top_level_row():
    """A child is rendered by the SAME renderer, so it cannot drift from a
    top-level row -- it carries its own report affordance and its own preview.

    A child is given NO loader: a leaf has nothing under it, and passing one
    would build a tree out of a list.
    """
    async def _load(_row, _page=1):
        return findings_envelope([finding_row()], total=1)

    client = render_and_click(
        lambda: fr.render_finding_row(
            finding_row(unit=FINDINGS_UNIT_WORK), "en",
            sidecar_version="discovery-v1-SENTINEL", load_children=_load,
            preview_url=lambda _i: "/browse?sys_id=X&embed=1"),
        fr.ROW_EXPANDER_CLASS)

    children = _elements_with_class(client, fr.ROW_CHILD_CLASS)
    assert len(children) == 1, "expected one child row, got {}".format(len(children))
    # The child inherited the version, so it carries the report affordance, and
    # the preview it is entitled to as a leaf.
    assert _elements_with_class(client, fr.ROW_REPORT_CLASS), (
        "the child row lost the report affordance")
    assert _elements_with_class(client, fr.ROW_PREVIEW_CLASS + "-toggle"), (
        "the child leaf lost its preview")
    # ...and exactly ONE expander on the whole subtree -- the parent's.
    assert len(_elements_with_class(client, fr.ROW_EXPANDER_CLASS)) == 1, (
        "a child was given a loader and became a tree")
    ASSERTION_COUNT["n"] += 1


@pytest.mark.parametrize("shape", ["raised", "non-ok"])
def test_a_FAILED_expansion_says_so_and_never_renders_an_empty_body(shape):
    """An empty body after a failed read is indistinguishable from "this row has
    no matches underneath it", and one of those is an outage. The panel's own
    expansion returns silently on an exception -- this one must not."""
    async def _raise(_row, _page=1):
        raise RuntimeError("probe")

    async def _bad(_row, _page=1):
        return findings_envelope([], status=STATUS_UNAVAILABLE)

    loader = _raise if shape == "raised" else _bad
    client = render_and_click(
        lambda: fr.render_finding_row(finding_row(unit=FINDINGS_UNIT_WORK), "en",
                                      load_children=loader),
        fr.ROW_EXPANDER_CLASS)
    assert fr.copy_text("expand_failed", "en") in scoped_text(
        client, fr.ROW_CHILDREN_STATE_CLASS), (
        "a failed expansion rendered no named failure")
    assert not _elements_with_class(client, fr.ROW_CHILD_CLASS)
    ASSERTION_COUNT["n"] += 1


def test_a_BOUNDED_expansion_says_how_many_it_withheld():
    """A page of children rendered with no extent line reads as the whole group,
    which is a number the reader will believe. Written from the envelope's own
    `total`, never from `len(items)` -- which cannot know what it was a page OF.
    """
    async def _load(_row, _page=1):
        return findings_envelope([finding_row()], total=97)

    client = render_and_click(
        lambda: fr.render_finding_row(finding_row(unit=FINDINGS_UNIT_WORK), "en",
                                      load_children=_load),
        fr.ROW_EXPANDER_CLASS)
    assert "97" in scoped_text(client, fr.ROW_CHILDREN_STATE_CLASS)

    # ...and NOT when the page IS the whole group, where the line would be noise.
    async def _all(_row, _page=1):
        return findings_envelope([finding_row()], total=1)

    whole = render_and_click(
        lambda: fr.render_finding_row(finding_row(unit=FINDINGS_UNIT_WORK), "en",
                                      load_children=_all),
        fr.ROW_EXPANDER_CLASS)
    assert not _elements_with_class(whole, fr.ROW_CHILDREN_STATE_CLASS)


@pytest.mark.parametrize("lang", LANGS)
def test_the_preview_is_offered_on_the_LEAF_only(lang):
    """A work row spans manuscripts and a manuscript row spans works, so a
    preview on either would have to CHOOSE which page to show -- and choosing
    between a row's candidates is adjudication, which no surface here does."""
    def _url(_item):
        return "/browse?sys_id=X&embed=1"

    leaf = _client_render(lambda: fr.render_finding_row(
        finding_row(), lang, preview_url=_url))
    assert _elements_with_class(leaf, fr.ROW_PREVIEW_CLASS + "-toggle"), (
        "the identification leaf offers no preview")

    for unit in (FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK):
        grouped = _client_render(lambda u=unit: fr.render_finding_row(
            finding_row(unit=u), lang, preview_url=_url))
        assert not _elements_with_class(
            grouped, fr.ROW_PREVIEW_CLASS + "-toggle"), unit


def test_the_preview_points_at_the_BARE_browse_viewer():
    """`?embed=1` is the route that disables snapshot restore AND persist
    (`web/pages/browse.py`, `embedded=True`), which is the property that matters:
    previewing a manuscript from this page must not overwrite wherever the reader
    had left `/browse`. Without `embed=1` it would."""
    url = fp.preview_url(finding_row(sys_id="990000895680205171"))
    assert url == "/browse?sys_id=990000895680205171&embed=1"
    # Withheld, not pointed at a page that cannot resolve.
    assert fp.preview_url(finding_row(sys_id=None)) is None
    ASSERTION_COUNT["n"] += 1


def test_the_preview_iframe_is_built_on_FIRST_OPEN_not_with_the_row():
    """A page of rows would otherwise issue one manuscript load per row against
    the image services `/browse` fetches from, none of which anybody asked
    for."""
    def _paint():
        fr.render_finding_row(finding_row(), "en",
                              preview_url=lambda _i: "/browse?sys_id=X&embed=1")

    closed = _client_render(_paint)
    assert not _elements_with_class(closed, fr.ROW_PREVIEW_CLASS + "-frame"), (
        "the preview iframe was created before the reader asked for it")

    opened = render_and_click(_paint, fr.ROW_PREVIEW_CLASS + "-toggle")
    frames = _elements_with_class(opened, fr.ROW_PREVIEW_CLASS + "-frame")
    assert len(frames) == 1, "expected one iframe, got {}".format(len(frames))
    assert "embed=1" in str(frames[0]._props), (
        "the preview does not point at the bare viewer")
    ASSERTION_COUNT["n"] += 1


def test_a_childs_filter_state_is_the_READERS_state_at_the_leaf_grain():
    """THE honesty property of the whole feature. The parent row was produced
    under the reader's filter set, so its count and the rows underneath it must
    come from ONE predicate -- otherwise a parent reading "3" opens onto 11 and
    the reader cannot tell which number to believe.

    Every axis carries over; only the grain and the pinned key differ."""
    # NON-DEFAULT on every axis on purpose: a state of all-defaults would pass
    # this test even if `_child_state` returned a fresh dict and carried nothing.
    # `novelty_view` is the four-state selector's axis, and `either` is the
    # state that would be most visibly wrong if it were dropped.
    state = {"unit": FINDINGS_UNIT_WORK, "bucket": BUCKET_MORE,
             "sort": "band_rank", "novelty_view": fp.NOVELTY_VIEW_EITHER,
             "domain": "D", "author": "A", "work_id": None, "page": 7}
    child = fp._child_state(state, "work_id", "w000404")

    assert child["unit"] == FINDINGS_UNIT_IDENTIFICATION, "not the leaf grain"
    assert child["page"] == 1, "a child list must start at its own first page"
    assert child["work_id"] == "w000404", "the group key was not pinned"
    for axis in ("bucket", "sort", "novelty_view", "domain", "author"):
        assert child[axis] == state[axis], (
            "{} was not carried into the children".format(axis))
    ASSERTION_COUNT["n"] += 1


def test_an_UNSUPPORTED_axis_never_reaches_the_service_as_a_dropped_keyword():
    """The measured defect, pinned as a test. Before the guard, a manuscript row
    handed `sys_id` to `get_findings_enveloped`, which accepted it and dropped
    it -- so the expansion returned every row matching the reader's filters
    instead of the ones under that parent.

    Driven through `_fetch_children` with the table temporarily naming an axis
    the builder does not implement, because that is the only way to reach the
    branch now that the shipped table names none."""
    calls = []

    async def _spy(unit, **kwargs):
        calls.append(kwargs)
        return findings_envelope([finding_row()])

    state = {"unit": FINDINGS_UNIT_MANUSCRIPT, "bucket": BUCKET_MAIN,
             "sort": "band_rank", "novelty_view": fp.NOVELTY_VIEW_ALL,
             "domain": None, "author": None, "work_id": None, "page": 1}
    row = finding_row(unit=FINDINGS_UNIT_MANUSCRIPT)

    original_table = dict(fr.EXPANSION_KEY_BY_UNIT)
    original_read = fp.get_findings_enveloped
    try:
        fr.EXPANSION_KEY_BY_UNIT[FINDINGS_UNIT_MANUSCRIPT] = ("sys_id", "sys_id")
        fp.get_findings_enveloped = _spy
        envelope = asyncio.run(fp._fetch_children(state, row))
    finally:
        fr.EXPANSION_KEY_BY_UNIT.clear()
        fr.EXPANSION_KEY_BY_UNIT.update(original_table)
        fp.get_findings_enveloped = original_read

    assert not calls, (
        "an unsupported axis reached the service, where it is silently dropped "
        "-- the expansion would have returned an UNPINNED page")
    assert envelope.get("status") != "ok", (
        "an expansion that cannot pin its group reported success")
    ASSERTION_COUNT["n"] += 1


# ---------------------------------------------------------------------------
# "SHOW MORE" inside an expansion, and the two click-binding shapes.
# ---------------------------------------------------------------------------

def _multi_page_loader(record: list):
    """A loader recording which child PAGE it was asked for, one row per page."""
    async def _load(_row, page=1):
        record.append(page)
        return findings_envelope(
            [finding_row(identification_id="child-page-%d" % page)], total=97)
    return _load


def test_show_more_appends_the_next_page_and_keeps_ONE_extent_line():
    """The heaviest work in the served artifact carries 2,981 identifications
    against a 25-row child page, so "Showing 25 of 2,981" with no route to the
    rest is a dead end that names its own incompleteness -- worse than a bounded
    list, because the reader can see what they are denied and cannot act.

    APPENDS rather than replaces: a reader who opened a work is building up a
    view of the group, and swapping the list under them loses the row they were
    reading. And exactly ONE extent line survives each page -- two would be two
    different claims about the same group.
    """
    pages: list = []
    # ONE loop and ONE client for the whole interaction. An earlier revision of
    # this test called `render_and_click` and then opened a SECOND event loop to
    # press show-more; the first client was already closed, so the presses landed
    # nowhere and `pages` stayed `[1]` -- the test failed loudly, which is the
    # only reason it is written this way rather than looking correct and
    # measuring one page.
    client, pressed = _render_and_press(
        lambda: fr.render_finding_row(
            finding_row(unit=FINDINGS_UNIT_WORK), "en",
            load_children=_multi_page_loader(pages)),
        [fr.ROW_EXPANDER_CLASS,
         fr.ROW_CHILDREN_STATE_CLASS + "-more",
         fr.ROW_CHILDREN_STATE_CLASS + "-more"])

    assert pressed == 3, f"only {pressed} of 3 presses were delivered"
    assert pages == [1, 2, 3], f"show-more requested {pages}"
    assert len(_elements_with_class(client, fr.ROW_CHILD_CLASS)) == 3, (
        "the next page REPLACED the rows already on screen instead of appending")
    assert len(_elements_with_class(client, fr.ROW_CHILDREN_STATE_CLASS)) == 1, (
        "each page added its own extent line -- two claims about one group")
    assert "3" in scoped_text(client, fr.ROW_CHILDREN_STATE_CLASS)
    ASSERTION_COUNT["n"] += 1


def test_every_click_handler_this_surface_binds_can_actually_BE_clicked():
    """Two binding styles, two arities, and the mismatch is invisible by reading.

    `ui.button(on_click=fn)` registers `lambda _: ...` -- ONE argument -- while
    `element.on("click", fn)` registers `fn` itself. A handler written
    zero-argument and bound with `on_click=` raises `TypeError` on the FIRST
    press: the button renders, looks right, and does nothing.

    Both of this expansion's `on_click=` handlers had exactly that defect when
    written (the retry button and show-more), and neither was caught by rendering
    or by reading -- only by pressing. So this drives EVERY click handler on a
    fully-opened row and asserts none raises.
    """
    pages: list = []

    async def _drive():
        from nicegui import core, ui
        from nicegui.client import Client
        core.loop = asyncio.get_running_loop()
        _ensure_sim()
        with Client(ui.page("/_findings_click_probe")) as client:
            with client:
                fr.render_finding_row(
                    finding_row(unit=FINDINGS_UNIT_WORK), "en",
                    sidecar_version="discovery-v1-SENTINEL",
                    load_children=_multi_page_loader(pages),
                    preview_url=lambda _i: "/browse?sys_id=X&embed=1")
                pressed = 0
                # Every clickable this row exposes, opened state included.
                for marker in (fr.ROW_EXPANDER_CLASS,
                               fr.ROW_CHILDREN_STATE_CLASS + "-more",
                               fr.ROW_PREVIEW_CLASS + "-toggle"):
                    for element in _elements_with_class(client, marker):
                        for handler in _click_handlers(element):
                            outcome = _fire_click(handler)
                            if asyncio.iscoroutine(outcome):
                                await outcome
                            pressed += 1
                return pressed

    pressed = asyncio.run(_drive())
    # The expander, its show-more (which only exists once opened) and the child
    # leaf's preview toggle -- so a real press count, not zero dressed as a pass.
    assert pressed >= 3, (
        f"only {pressed} click handler(s) were driven; this test cannot show "
        "that the surface's buttons work")
    assert pages, "the expansion never issued a child read"
    ASSERTION_COUNT["n"] += 1


def test_an_outage_AFTER_a_good_page_shows_the_failure_instead_of_raising():
    """A good page then an outage: the reader gets the NAMED failure.

    A note on what this test does and does not establish, because the first
    version of this docstring overclaimed and the measurement contradicted it.

    A control that made show-more reload with `append=False` raised `ValueError`
    out of NiceGUI's child list: `body.clear()` destroys the extent element, so
    the handle kept for replacing it was stale and `.delete()` failed. The fix is
    to forget the handle whenever the body is cleared, and it is right on its own
    terms -- clearing a container invalidates handles into it.

    But I could NOT construct a shipped sequence that reaches it: `_load` is
    re-entered only by retry and by show-more, and neither leaves a stale handle
    once the clear-path forgets it. So this is a REGRESSION test for the
    behaviour a reader depends on (an outage after a successful page is reported,
    not raised), not proof that the stale handle was reachable in production.
    Stated plainly rather than left implied, because "found by a control" and
    "reachable by a reader" are different claims and only the first is measured.
    """
    calls = {"n": 0}

    async def _load(_row, page=1):
        calls["n"] += 1
        if calls["n"] == 1:
            return findings_envelope([finding_row()], total=97)
        return findings_envelope([], status=STATUS_UNAVAILABLE)

    # Open (good page, renders an extent line), then press show-more, whose load
    # fails -- the sequence that used to raise.
    client, pressed = _render_and_press(
        lambda: fr.render_finding_row(finding_row(unit=FINDINGS_UNIT_WORK), "en",
                                      load_children=_load),
        [fr.ROW_EXPANDER_CLASS, fr.ROW_CHILDREN_STATE_CLASS + "-more"])

    assert pressed == 2, f"only {pressed} of 2 presses were delivered"
    assert calls["n"] == 2, "the second load never ran"
    assert fr.copy_text("expand_failed", "en") in scoped_text(
        client, fr.ROW_CHILDREN_STATE_CLASS), (
        "the outage after a good page rendered no named failure")
    ASSERTION_COUNT["n"] += 1


# ===========================================================================
# THE CATALOGUE TITLE (2026-08-05, coordinator-authorized addition). Beside
# the shelfmark, not the meta line; verbatim and one language, never the
# label; and truly absent -- no element at all -- on the ~14% of rows
# `libraries.csv` carries no title for. `render_finding_row`'s `catalogue_title`
# is INJECTED, the same shape as `load_children`/`preview_url`, so every test
# below drives it directly through `fr.render_finding_row` rather than through
# `render_rows`, which does not accept it.
# ===========================================================================

CURATED_CATALOGUE_TITLE = "משנה סדר זרעים (קטעים)"


@pytest.mark.parametrize("lang", LANGS)
def test_the_catalogue_title_renders_beside_the_shelfmark_attributed_and_verbatim(lang):
    """Present: an element carrying `ROW_CATALOGUE_TITLE_CLASS`, INSIDE the row
    title (with the shelfmark), never inside `ROW_META_CLASS` -- two placement
    facts a coordinate-only check on "did the text render somewhere" would
    miss entirely.

    The title text itself is BYTE-IDENTICAL regardless of the page's own
    language (ruling: verbatim, one language, never translated) -- only the
    LABEL introducing it is bilingual.
    """
    client = _client_render(lambda: fr.render_finding_row(
        finding_row(), lang, catalogue_title=lambda _i: CURATED_CATALOGUE_TITLE))

    title_elements = _elements_with_class(client, fr.ROW_CATALOGUE_TITLE_CLASS)
    assert title_elements, "the catalogue title did not render at all"

    fragment = scoped_fragment(client, fr.ROW_CATALOGUE_TITLE_CLASS)
    assert CURATED_CATALOGUE_TITLE in fragment, (
        "the catalogue's own title text did not render")
    assert fr.copy_text("catalogue_title_label", lang) in fragment, (
        "the bilingual attribution label did not render beside the title")

    # NOT on the meta line -- a leaf row already carries six grey meta
    # elements, and the coordinator's placement ruling was explicit that this
    # must live with the shelfmark (manuscript identity), not append a
    # seventh.
    meta_fragment = scoped_fragment(client, fr.ROW_META_CLASS)
    assert CURATED_CATALOGUE_TITLE not in meta_fragment, (
        "the catalogue title rendered on the meta line, not beside the shelfmark")

    # Scoped to the WHOLE row (`ROW_CLASS`), the honesty gate's own required
    # scope -- never to `ROW_CATALOGUE_TITLE_CLASS`, which the scanner would
    # refuse outright as an element it never sees a `gs-findings-row` on.
    assert_surface_honesty(
        scoped_fragment(client, fr.ROW_CLASS), scope_selector=fr.ROW_CLASS, lang=lang)
    ASSERTION_COUNT["n"] += 1


def test_the_catalogue_title_text_is_never_translated_by_language():
    """The title STRING is identical across an English and a Hebrew render of
    the SAME row -- only the label bilingual, per the coordinator's ruling
    (a deliberate departure from this page's usual bilingual discipline,
    documented in `_render_shelfmark`'s docstring).

    Deliberately NOT a substring check (`CURATED_CATALOGUE_TITLE in fragment`):
    that form is satisfied by e.g. a per-language prefix/suffix mutation
    (`"[en] " + title`) since the curated string still occurs inside the
    mutated one. Instead this pulls the title element's own text nodes,
    strips out the (expected-to-differ) label text, and asserts the
    remainder is EXACTLY the curated string -- byte for byte -- in both
    languages, and therefore identical to each other."""
    en_client = _client_render(lambda: fr.render_finding_row(
        finding_row(), "en", catalogue_title=lambda _i: CURATED_CATALOGUE_TITLE))
    he_client = _client_render(lambda: fr.render_finding_row(
        finding_row(), "he", catalogue_title=lambda _i: CURATED_CATALOGUE_TITLE))

    en_label = fr.copy_text("catalogue_title_label", "en")
    he_label = fr.copy_text("catalogue_title_label", "he")

    # `_subtree_texts` pulls BOTH the element's own text attrs (which can
    # duplicate across `text`/`_text`) AND every string `_props` value --
    # picking up e.g. `"auto"` from this element's own `dir="auto"` prop.
    # Known, expected noise (the label text and the `dir` prop value) is
    # filtered out; deduplicating via `set` absorbs the attr-name duplication.
    KNOWN_NOISE = {"auto"}
    en_texts = set()
    for element in _elements_with_class(en_client, fr.ROW_CATALOGUE_TITLE_CLASS):
        en_texts.update(_subtree_texts(element))
    he_texts = set()
    for element in _elements_with_class(he_client, fr.ROW_CATALOGUE_TITLE_CLASS):
        he_texts.update(_subtree_texts(element))

    en_title_only = en_texts - {en_label} - KNOWN_NOISE
    he_title_only = he_texts - {he_label} - KNOWN_NOISE

    assert en_title_only == {CURATED_CATALOGUE_TITLE}, (
        f"the English render's title text was {en_title_only!r}, not the "
        f"curated string verbatim")
    assert he_title_only == {CURATED_CATALOGUE_TITLE}, (
        f"the Hebrew render's title text was {he_title_only!r}, not the "
        f"curated string verbatim")
    assert en_title_only == he_title_only, (
        "the title text differed between languages -- it must be rendered "
        "verbatim, in one language, regardless of UI language")

    # The LABEL, unlike the title, DOES change with language.
    assert en_label in en_texts
    assert he_label in he_texts
    assert he_label not in en_texts
    assert en_label not in he_texts
    ASSERTION_COUNT["n"] += 1


def test_a_missing_catalogue_title_renders_nothing_not_a_placeholder():
    """~14% of rows carry no CSV title. Absence of data must be INVISIBLE --
    no element, no empty row, no dash, no "untitled" -- because the injected
    callable itself distinguishes "not asked" from "asked, found nothing":
    both must render nothing, and this drives both shapes of that call.
    """
    # The callable is injected but returns falsy for this row (the coverage
    # gap this page will see in production for ~14% of rows).
    absent = _client_render(lambda: fr.render_finding_row(
        finding_row(), "en", catalogue_title=lambda _i: None))
    assert not _elements_with_class(absent, fr.ROW_CATALOGUE_TITLE_CLASS), (
        "a row with no catalogue title rendered SOME element for it")

    # The empty string is exactly as absent as `None` -- a title lookup that
    # somehow returned "" must not paint an empty label into the row.
    empty = _client_render(lambda: fr.render_finding_row(
        finding_row(), "en", catalogue_title=lambda _i: ""))
    assert not _elements_with_class(empty, fr.ROW_CATALOGUE_TITLE_CLASS)

    # And the callable not being injected at all (a caller that never wires
    # `catalogue_title`, e.g. every EXISTING call site before this change)
    # must render nothing either -- the same behaviour `render_rows` already
    # exercises on every other test in this suite, confirmed once more here
    # for this specific element.
    not_injected = render_rows([finding_row()], "en")
    assert not _elements_with_class(not_injected, fr.ROW_CATALOGUE_TITLE_CLASS)

    for client in (absent, empty, not_injected):
        assert not scoped_text(client, fr.ROW_CATALOGUE_TITLE_CLASS).strip(), (
            "a missing catalogue title left rendered text behind")
    ASSERTION_COUNT["n"] += 1


def test_the_catalogue_title_is_offered_on_the_manuscript_and_leaf_units_only():
    """A work row spans many manuscripts and has no single one to title -- the
    same reason it never calls `_render_shelfmark` at all. The per-manuscript
    unit and the identification leaf both DO call it, so both are offered the
    title; the work unit must never render the element even when a title is
    injected and available.
    """
    for unit in (FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_IDENTIFICATION):
        client = _client_render(lambda u=unit: fr.render_finding_row(
            finding_row(unit=u), "en",
            catalogue_title=lambda _i: CURATED_CATALOGUE_TITLE))
        assert _elements_with_class(client, fr.ROW_CATALOGUE_TITLE_CLASS), unit

    work_client = _client_render(lambda: fr.render_finding_row(
        finding_row(unit=FINDINGS_UNIT_WORK), "en",
        catalogue_title=lambda _i: CURATED_CATALOGUE_TITLE))
    assert not _elements_with_class(work_client, fr.ROW_CATALOGUE_TITLE_CLASS), (
        "a work row rendered a catalogue title, but a work has no single "
        "manuscript to title")
    ASSERTION_COUNT["n"] += 1


def test_the_page_batches_catalogue_titles_off_the_event_loop_never_per_row(monkeypatch):
    """`_render_results` resolves every row's catalogue title from
    `state.meta_mgr.csv_bank` -- a plain in-memory dict populated once at
    process startup -- in ONE pass over `items`, before any row renders, and
    hands each row a closure over that already-built dict rather than a
    callable that reads anything itself.

    This is a hard performance constraint (one uvicorn worker; no per-row
    lookup, on or off the loop), and the shape that would violate it --
    `catalogue_title` closing over `state.meta_mgr` and indexing it PER CALL
    instead of over a pre-built dict -- would still pass every rendering
    assertion above, because the visible output is identical either way. So
    this test drives the REAL page (`render_page`, the same harness every
    other full-page test in this suite uses -- `_render_results` alone takes
    a fully-populated reader-state dict this test has no business
    constructing by hand) and counts `csv_bank.get` calls, which must equal
    the number of DISTINCT `sys_id`s on the page, not the number of rows and
    not zero.
    """
    from unittest import mock

    calls = {"n": 0}
    csv_bank = {
        "990000000000000944": {"title": CURATED_CATALOGUE_TITLE},
    }

    class _CountingBank(dict):
        def get(self, key, default=None):
            calls["n"] += 1
            return super().get(key, default)

    counting_bank = _CountingBank(csv_bank)
    fake_meta_mgr = mock.Mock()
    fake_meta_mgr.csv_bank = counting_bank

    # Three rows, only TWO distinct sys_ids -- a per-row read would make three
    # calls; the batched read this test is pinning makes exactly two (one per
    # distinct sys_id), and a correct implementation would also tolerate
    # de-duplication down to fewer, so the assertion is a ceiling, not an
    # exact-equality trap on an implementation detail.
    items = [
        finding_row(sys_id="990000000000000944"),
        finding_row(sys_id="990000000000000944"),
        finding_row(sys_id="990000000000000111"),
    ]
    envelope = findings_envelope(items, total=3)

    # `_render_results` imports `web.state.state` LOCALLY (inside the
    # function body, not at module scope -- confirmed above: `fp.state` does
    # not exist as a module attribute) precisely so the module-level `state`
    # parameter name used throughout this page is never shadowed. Patching
    # the real singleton is therefore the only way to intercept this read.
    with mock.patch("web.state.state") as patched_state:
        patched_state.meta_mgr = fake_meta_mgr
        client = render_page(monkeypatch, lang="en", findings=envelope)

    assert calls["n"] <= len(set(i["sys_id"] for i in items)), (
        f"csv_bank.get was called {calls['n']} times for "
        f"{len(set(i['sys_id'] for i in items))} distinct sys_ids -- a "
        "per-row read, not the required page-wide batch")
    assert calls["n"] > 0, "the batched lookup never ran at all"

    rows_rendered = _elements_with_class(client, fr.ROW_CATALOGUE_TITLE_CLASS)
    assert len(rows_rendered) == 2, (
        "the two rows sharing a sys_id with a title should both show it")
    ASSERTION_COUNT["n"] += 1
