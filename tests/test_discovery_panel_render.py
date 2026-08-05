# -*- coding: utf-8 -*-
"""The panel body renderer (Phase 136, plan 136-17, Task 2).

What this suite is FOR: proving the renderer adds nothing. Every honesty
invariant on this surface is a property of a pure model plus a renderer that
takes no display decision of its own, so the interesting assertions here are
about what is ABSENT -- a second band comparison, a raw title read, a
relation-keyed class, a catalogue description, a bucket rule.

The two LAZY reads are exercised for real: the related-pages toggle and the
per-row expansion are driven through their own handlers, with the executor
dispatch counted at `DiscoveryService._run_off_loop` -- the ONE place a
crossing happens.

Fixture builders are LOCAL rather than imported from
`tests/test_discovery_panel_model.py`: `tests/` has no `__init__.py`, so
`import tests.test_X` builds a SECOND module object and every monkeypatch lands
on the copy. They are built from the LIVE allowlists and assert their own key
sets, so they cannot drift from the projection.
"""

from __future__ import annotations

import asyncio
import ast
import inspect
import io
import sys
from typing import Any, Dict, List, Optional

import pytest

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
import web.components.discovery_panel as dp
from shared.discovery_band_labels import band_label
from shared.discovery_errors import DiscoveryUnavailable
from shared.discovery_main_pool import REASON_MAIN_FULL_COVERAGE
from shared.discovery_panel_model import (
    LEVEL_ALSO_SHARES_TEXT,
    ROWS_NOT_REQUESTED,
    PanelServiceBundle,
    build_panel_rows,
)
from shared.discovery_service import DiscoveryService
from shared.discovery_surface_projection import (
    SURFACE_CLAIM_FIELDS,
    SURFACE_EXPANSION_FIELDS,
    SURFACE_RELATED_PAGE_FIELDS,
    SURFACE_WORK_SUMMARY_FIELDS,
    STATUS_OK,
    make_envelope,
)

PANEL_PATH = 'web/components/discovery_panel.py'
CSS_PATH = 'web/static/common.css'

#: The raw recorded title of the ONE curated work (ruling R). The curation
#: exists because this bare title tells a reader "Maimonides' halakhic book"
#: over pages the owner ruled are mostly liturgy.
W000176 = 'w000176'
W000176_RAW_TITLE = 'משנה תורה, ספר אהבה'

_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation
        prepare_simulation()
        _SIM_READY = True


def _read(path: str) -> str:
    return io.open(path, encoding='utf-8').read()


# ---------------------------------------------------------------------------
# Fixture builders -- the EXACT live row/envelope shapes.
# ---------------------------------------------------------------------------

def claim_row(**overrides):
    row = {field: None for field in SURFACE_CLAIM_FIELDS}
    row.update({
        'page_id': 'page-1',
        'sys_id': '990051079570205171',
        'claim_id': 'claim-1',
        'evidence_id': 'ev-1',
        'work_id': 'w000001',
        'canonical_work_id': 'w000001',
        'display_work_id': 'w000001',
        'neutral_title': 'Some Recorded Work',
        'title_missing': False,
        'relation_kind': ids.CLAIM_TYPE_DIRECT_WITNESS,
        'evidence_source': ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        'confidence_band': ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC,
        'band_label': band_label(
            ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
            ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC),
        'band_rank': 0,
        'coverage_ppm': 680000,
        'coverage_status': 'measured',
        'main_pool': True,
        'main_pool_reason': REASON_MAIN_FULL_COVERAGE,
        'identification_id': 'ident-1',
        'identification_page_count': 1,
        'novelty_status': 'not_checked',
        'matched_letters': 500,
        'span_start': 0,
        'span_end': 500,
        'n_spans': 1,
        'eligibility_basis': 'shipped',
        'restored_by_human_confirmation': False,
        'low_coverage_marker': False,
        'adjudication_status': ids.ADJUDICATION_STATUS_UNREVIEWED,
        'routing_status': ids.ROUTING_STATUS_SHIPPED,
        'measurement_status': 'measured_pass',
        'default_eligible': True,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_CLAIM_FIELDS), 'fixture drifted from the live allowlist'
    return row


def work_summary_row(**overrides):
    row = {field: None for field in SURFACE_WORK_SUMMARY_FIELDS}
    row.update({
        'canonical_work_id': 'w000001',
        'display_work_id': 'w000001',
        'neutral_title': 'Some Recorded Work',
        'title_missing': False,
        'page_count': 5,
        'best_band_rank': 0,
        'gated': False,
        'main_pool': True,
        'relation_kind': ids.CLAIM_TYPE_DIRECT_WITNESS,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_WORK_SUMMARY_FIELDS)
    return row


#: The related-page row's NAME, as the joined query supplies it. The composite
#: `related_page_id` stays in the fixture because the projection carries it --
#: what changed is that nothing downstream may render it.
RELATED_SHELFMARK = 'T-S 12.999'
RELATED_SYS_ID = '990051079570205172'
RELATED_PAGE_ID = f'{RELATED_SYS_ID}_IE1_P000003_FL7'


def related_page_row(**overrides):
    row = {field: None for field in SURFACE_RELATED_PAGE_FIELDS}
    row.update({
        'related_page_id': RELATED_PAGE_ID,
        'sys_id': RELATED_SYS_ID,
        'library_code': 'CUL',
        'shelfmark_display': RELATED_SHELFMARK,
        'page_number': 3,
        'display_missing': False,
        'evidence_id': 'ev-99',
        'evidence_source': ids.EVIDENCE_SOURCE_PROPAGATED,
        'confidence_band': ids.CONFIDENCE_BAND_NOT_EVALUATED,
        'band_rank': 6,
        'evidence_row_count': 3,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_RELATED_PAGE_FIELDS)
    return row


def expansion_row(**overrides):
    row = {field: None for field in SURFACE_EXPANSION_FIELDS}
    row.update({
        'work_id': 'w000001',
        'unit_id': 'unit-2',
        'representative_sys_id': '990051079570205172',
        'representative_page_id': 'page-2',
        'representative_claim_id': 'claim-2',
        'member_sys_ids': ['990051079570205172'],
        'library_code': 'CUL',
        'shelfmark_display': 'T-S 12.123',
        'display_missing': False,
        'claim_type': ids.CLAIM_TYPE_DIRECT_WITNESS,
        'anchor_claim_type': ids.CLAIM_TYPE_DIRECT_WITNESS,
        'relations_differ': False,
        'displayed_evidence_source': ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
        'displayed_confidence_band': ids.CONFIDENCE_BAND_SCREENING_RB,
        'band_label': band_label(
            ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_RB),
        'band_rank': 4,
    })
    row.update(overrides)
    assert set(row) == set(SURFACE_EXPANSION_FIELDS)
    return row


def bundle(claim_items=(), works=(), related_total=0, related_rows=None,
           lang='en', show_more=False, show_divergence=False):
    return PanelServiceBundle(
        claims=make_envelope(STATUS_OK, list(claim_items), len(claim_items),
                             meta={'page_id': 'page-1', 'include_review': False}),
        page_ids=make_envelope(STATUS_OK, ['page-1'], 1, meta={
            'sys_id': '990051079570205171', 'resolved': True,
            'truncated': False, 'volume_ie': None}),
        manuscript_works=make_envelope(STATUS_OK, list(works), len(works),
                                       meta={'page_scope_resolved': True, 'lang': lang}),
        related_count=make_envelope(STATUS_OK, [], related_total,
                                    meta={'unit': 'distinct_opposite_pages'}),
        related_rows=related_rows,
        lang=lang,
        show_more=show_more,
        show_divergence=show_divergence,
    )


def model_for(**kwargs):
    return build_panel_rows(bundle(**kwargs))


# ---------------------------------------------------------------------------
# Render harness.
# ---------------------------------------------------------------------------

def _render(model, page_id: Optional[str] = 'page-1', driver=None):
    """Render the REAL panel body in a bare client context and return the
    client; `driver(client)` runs INSIDE the same event loop so interaction
    handlers can be awaited."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_discovery_panel_probe')) as client:
            with client:
                dp.render_discovery_panel_body(model, page_id=page_id)
                if driver is not None:
                    await driver(client)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client']


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
    return out


def _scoped_text(client, marker: str) -> str:
    parts: List[str] = []
    for element in _elements_with_class(client, marker):
        parts.extend(_subtree_texts(element))
    return '\n'.join(parts)


def _buttons(client, contains: Optional[str] = None) -> list:
    out = []
    for el in client.elements.values():
        if type(el).__name__ != 'Button':
            continue
        text = getattr(el, 'text', '') or ''
        if contains is None or contains in text:
            out.append(el)
    return out


async def _drain() -> None:
    """NiceGUI's `on_click` wrapper routes an ASYNC handler through
    `handle_event`, which schedules it as a background TASK rather than
    returning its coroutine -- so a test that merely awaits the click sees
    nothing. Drain the loop until the scheduled work has finished."""
    for _ in range(50):
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)
    raise AssertionError('background tasks did not settle')


async def _click(element) -> None:
    for listener in element._event_listeners.values():
        if listener.type != 'click' or listener.handler is None:
            continue
        handler = listener.handler
        n = len(inspect.signature(handler).parameters)
        result = handler(*([None] * n))
        if inspect.isawaitable(result):
            await result
        await _drain()
        return
    raise AssertionError('no click handler on this element')


# ---------------------------------------------------------------------------
# The dispatch spy for the two LAZY reads.
# ---------------------------------------------------------------------------

class _Spy:
    def __init__(self):
        self.calls: List[str] = []
        self.results: Dict[str, Any] = {}
        self.fail: Dict[str, BaseException] = {}

    async def __call__(self, sync_fn, *args, timeout=None, heavy=False):
        name = getattr(sync_fn, '__name__', repr(sync_fn))
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]
        return self.results[name]


@pytest.fixture
def spy(monkeypatch):
    import web.discovery as wd
    s = _Spy()
    s.results = {
        'get_related_pages_enveloped': make_envelope(
            STATUS_OK, [related_page_row()], 1,
            meta={'unit': 'distinct_opposite_pages'}),
        'get_work_expansion_enveloped': make_envelope(
            STATUS_OK, [expansion_row()], 1, meta={
                'work_id': 'w000001', 'anchor_mode': 'anchored',
                'filter_basis': 'displayed_band', 'anchor_excluded': True}),
    }
    monkeypatch.setattr(DiscoveryService, '_run_off_loop', s, raising=True)
    monkeypatch.setattr(wd, 'discovery_available', lambda: True)
    with wd._service._browse_lru_lock:
        wd._service._browse_lru.clear()
    yield s
    with wd._service._browse_lru_lock:
        wd._service._browse_lru.clear()


# ===========================================================================
# The renderer takes no display decision of its own.
# ===========================================================================

_FORBIDDEN_CALLS = frozenset({
    'sorted', 'bucket_label', 'is_default_eligible', '_band_rank',
    'main_pool_sentence', 'serialize_banded_claim',
})


def _called_names(tree) -> set:
    names = set()
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        func = call.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)
    return names


def test_renderer_makes_no_bucket_collapse_gating_or_ordering_decision():
    tree = ast.parse(_read(PANEL_PATH))
    called = _called_names(tree)
    leaks = sorted(called & _FORBIDDEN_CALLS)
    assert leaks == [], (
        f'{PANEL_PATH} calls {leaks} -- bucket membership, collapse, gating and '
        'ordering are the model\'s decisions (shared/discovery_panel_model.py)'
    )
    assert 'sort' not in called, f'{PANEL_PATH} sorts; ordering is the model\'s'
    src = _read(PANEL_PATH)
    for module in ('shared.discovery_grouping', 'shared.discovery_main_pool'):
        assert f'import {module}' not in src, f'{PANEL_PATH} imports {module}'


def test_the_no_decision_guard_can_fail():
    """Positive control: the AST walk sees a seeded `sorted(...)` call."""
    assert 'sorted' in _called_names(ast.parse('x = sorted(rows, key=f)\n'))


def test_renderer_contains_no_bare_neutral_title_read():
    """Ruling R. The ONE place a title could arrive uncurated is a 136-21
    expansion row, and `_expansion_work_title` routes it through
    `display_work_title` rather than formatting it."""
    tree = ast.parse(_read(PANEL_PATH))
    reads = []
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        if isinstance(call.func, ast.Attribute) and call.func.attr == 'get':
            for arg in call.args[:1]:
                if isinstance(arg, ast.Constant) and arg.value == 'neutral_title':
                    reads.append(call.lineno)
    assert len(reads) == 1, f'expected exactly one guarded read, found {reads}'
    fn = [n for n in ast.walk(tree)
          if isinstance(n, ast.FunctionDef) and n.name == '_expansion_work_title']
    assert fn, '_expansion_work_title is missing'
    assert reads[0] in range(fn[0].lineno, max(
        c.lineno for c in ast.walk(fn[0]) if hasattr(c, 'lineno')) + 1)
    assert 'display_work_title' in _called_names(fn[0])


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_the_curated_w000176_label_renders_and_the_raw_title_does_not(spy, lang):
    """The curated string reaches the expansion HEADING; the raw recorded title
    appears nowhere in the expansion subtree."""
    row = claim_row(work_id=W000176, canonical_work_id=W000176,
                    display_work_id=W000176, neutral_title=W000176_RAW_TITLE)
    model = model_for(claim_items=[row], lang=lang)
    curated = ds.display_work_title(W000176, W000176_RAW_TITLE, lang)
    assert curated != W000176_RAW_TITLE, 'the ruling-R curation table lost w000176'

    async def driver(client):
        for button in _buttons(client):
            if curated in (getattr(button, 'text', '') or ''):
                await _click(button)
                break
        else:
            raise AssertionError('no expansion control carried the curated title')

    client = _render(model, driver=driver)
    subtree = _scoped_text(client, dp.PANEL_EXPANSION_CLASS)
    assert subtree.strip(), (
        'the expansion subtree is EMPTY -- an absence assertion over nothing '
        'passes for the wrong reason')

    # "The raw title does not appear" cannot be a plain substring absence: the
    # HEBREW curated label is the raw title PLUS the ruled disjunct
    # ("... / סידור"), so every legitimate Hebrew render contains it. The real
    # property is that the raw title never appears UNCURATED -- every occurrence
    # is inside an occurrence of the curated string.
    expected_raw = subtree.count(curated) if W000176_RAW_TITLE in curated else 0
    assert subtree.count(W000176_RAW_TITLE) == expected_raw, (
        'the RAW recorded title reached the expansion subtree uncurated')
    whole = _scoped_text(client, dp.PANEL_ROOT_CLASS)
    expected_raw = whole.count(curated) if W000176_RAW_TITLE in curated else 0
    assert whole.count(W000176_RAW_TITLE) == expected_raw, (
        'the RAW recorded title reached the panel uncurated')

    headings = [b for b in _buttons(client) if curated in (getattr(b, 'text', '') or '')]
    assert headings, 'the curated title is not on the expansion heading'


# ===========================================================================
# Rows, chips and titles.
# ===========================================================================

def test_work_titles_render_as_plain_text_not_links():
    """`/work/{id}` does not exist until Phase 136.1, and a dead link is worse
    than plain text."""
    model = model_for(claim_items=[claim_row()])
    client = _render(model)
    rows = _elements_with_class(client, dp.PANEL_ROW_CLASS)
    assert rows
    for row in rows:
        for node in row.descendants(include_self=True):
            assert type(node).__name__ != 'Link', 'an anchor wraps a work title'
            assert node.tag != 'a', 'an <a> element is inside an identification row'


def test_the_relation_chip_carries_the_band_label_as_a_title_attribute_only():
    row = claim_row()
    model = model_for(claim_items=[row])
    client = _render(model)
    chips = _elements_with_class(client, 'rel')
    assert chips, 'no relation chip rendered'
    chip = chips[0]
    expected_tooltip = ds.relation_tooltip(
        row['evidence_source'], row['confidence_band'], 'en')
    assert (chip._props or {}).get('title') == expected_tooltip
    assert chip.text == ds.relation_chip(row['relation_kind'], 'en')
    assert expected_tooltip not in (chip.text or ''), (
        'the band label is VISIBLE text; it is tooltip-only (D-24)')


def test_no_relation_keyed_class_is_applied_to_any_chip():
    for kind in sorted(ids.CLAIM_TYPES):
        model = model_for(claim_items=[claim_row(
            relation_kind=kind,
            coverage_ppm=None if kind != ids.CLAIM_TYPE_DIRECT_WITNESS else 680000)])
        client = _render(model)
        for chip in _elements_with_class(client, 'rel'):
            classes = set(chip._classes or [])
            assert kind not in classes
            assert not (classes - {'rel'}), (
                f'the chip carries {sorted(classes)} for {kind} -- colour-coding '
                'by relation kind reintroduces per-tier styling (D-24)')


def test_the_discovery_css_block_is_the_only_source_of_chip_styling():
    """No CSS is added by this plan: the grid, the two chip states and the
    neutral chip all come from plan 136-10's block."""
    css = _read(CSS_PATH)
    assert '.gs-discovery .dpanes { display: block; }' in css
    assert 'grid-template-columns: 1fr 1fr;' in css
    assert '@media (min-width: 900px)' in css
    assert '.gs-discovery .chip.here' in css and '.gs-discovery .chip.gated' in css
    for kind in sorted(ids.CLAIM_TYPES):
        assert f'.rel.{kind}' not in css, 'a relation-keyed chip rule exists'
    panel = _read(PANEL_PATH)
    assert '<style' not in panel and '@media' not in panel, (
        'the renderer ships CSS of its own')


def test_the_panel_root_carries_the_scope_class_and_renders_page_pane_first():
    model = model_for(claim_items=[claim_row()], works=[work_summary_row()])
    client = _render(model)
    roots = _elements_with_class(client, dp.PANEL_ROOT_CLASS)
    assert roots and 'gs-discovery' in (roots[0]._classes or []), (
        'without .gs-discovery none of the discovery CSS applies')
    panes = _elements_with_class(client, 'dpanes')
    assert len(panes) == 1
    children = list(panes[0])
    assert len(children) == 2, 'the panel is not two panes'
    manuscript = _elements_with_class(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)
    assert manuscript, 'the manuscript pane is missing'
    assert manuscript[0] not in list(children[0]) + [children[0]], (
        'the manuscript pane is FIRST; on mobile the panes stack page-first')


# ===========================================================================
# The manuscript pane.
# ===========================================================================

def test_the_manuscript_pane_names_works_with_their_page_counts():
    works = [work_summary_row(display_work_id='w1', neutral_title='Alpha', page_count=5),
             work_summary_row(display_work_id='w2', neutral_title='Beta', page_count=2)]
    client = _render(model_for(claim_items=[claim_row()], works=works))
    text = _scoped_text(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)
    assert 'Alpha (5)' in text and 'Beta (2)' in text, text
    assert text.strip(), 'the pane rendered a bare count instead of names'


def test_a_gated_work_renders_dimmed_rather_than_being_absent():
    works = [work_summary_row(display_work_id='w1', neutral_title='Here', gated=False),
             work_summary_row(display_work_id='w2', neutral_title='Gated', gated=True)]
    client = _render(model_for(claim_items=[claim_row()], works=works))
    chips = {c.text: set(c._classes or []) for c in _elements_with_class(client, 'chip')}
    here = [t for t in chips if t.startswith('Here')]
    gated = [t for t in chips if t.startswith('Gated')]
    assert here and gated, f'a chip state is missing: {sorted(chips)}'
    assert 'here' in chips[here[0]] and 'gated' not in chips[here[0]]
    assert 'gated' in chips[gated[0]], 'the gated work is not dimmed'


def test_the_manuscript_pane_carries_no_catalogue_description():
    """D-13i, implemented as OMISSION plus an explicit scope note. The browse
    header's catalogue line describes the SHELFMARK, and on a composite
    shelfmark that means OTHER leaves."""
    client = _render(model_for(claim_items=[claim_row()], works=[work_summary_row()]))
    text = _scoped_text(client, dp.PANEL_ROOT_CLASS)
    assert 'No catalogue description is shown here' in text
    assert 'Computed for this manuscript only' in text


# ===========================================================================
# Related pages: NOT REQUESTED by default, and the toggle is what queries.
# ===========================================================================

def _related_section(model):
    for level in model.disclosure_levels:
        if level['key'] == LEVEL_ALSO_SHARES_TEXT:
            return level['related_pages']
    raise AssertionError('no related-pages section')


def test_related_pages_default_is_not_requested_never_an_empty_result(spy):
    model = model_for(claim_items=[claim_row()], related_total=4)
    assert _related_section(model)['rows_state'] == ROWS_NOT_REQUESTED
    client = _render(model)
    text = _scoped_text(client, dp.PANEL_RELATED_CLASS)
    assert ds.related_pages_count_line(4, 'en') in text, text
    assert ds.related_pages_label('en') in text
    assert RELATED_SHELFMARK not in text, 'rows rendered without the toggle'
    assert RELATED_PAGE_ID not in text
    assert spy.calls == [], 'the lazy read was issued with the panel'


def test_opening_the_related_toggle_issues_the_lazy_read_once_and_renders(spy):
    model = model_for(claim_items=[claim_row()], related_total=4)
    seen: Dict[str, Any] = {}

    async def driver(client):
        assert spy.calls == [], 'the read was issued before the toggle'
        toggle = [b for b in _buttons(client)
                  if ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, 'en') in (b.text or '')]
        assert toggle, 'no related-pages toggle'
        await _click(toggle[0])
        seen['after_open'] = list(spy.calls)
        await _click(toggle[0])   # close
        await _click(toggle[0])   # re-open, identical arguments
        seen['after_reopen'] = list(spy.calls)

    client = _render(model, driver=driver)
    assert seen['after_open'] == ['get_related_pages_enveloped'], seen
    assert seen['after_reopen'] == ['get_related_pages_enveloped'], (
        'a second open issued another read')
    text = _scoped_text(client, dp.PANEL_RELATED_CLASS)
    assert RELATED_SHELFMARK in text, 'the lazily-loaded rows never rendered'
    # ...and the COMPOSITE PAGE ID is not what the reader was shown.
    assert RELATED_PAGE_ID not in text, (
        'the panel rendered the internal page id to a reader')


# ===========================================================================
# TASK F (2026-08-05) -- a candidate alignment NAMES ITS MANUSCRIPT.
#
# The section rendered `990051620920205171_IE167198813_P000003_FL167198817` --
# a raw internal composite identifier, to a scholarly audience. The owner
# reported it. The fix resolves the name in the SERVICE (one joined query) and
# renders it in the findings page's own row anatomy: the library chip IS the
# badge, and the shelfmark is the link.
# ===========================================================================

@pytest.mark.parametrize('lang', ('en', 'he'))
def test_a_related_page_row_shows_a_library_chip_and_a_linked_shelfmark(spy, lang):
    """The whole defect, in one assertion pair: a shelfmark a reader can act on,
    and NO composite id anywhere in the section."""
    spy.results['get_related_pages_enveloped'] = make_envelope(
        STATUS_OK, [related_page_row()], 1,
        meta={'unit': 'distinct_opposite_pages'})
    model = model_for(claim_items=[claim_row()], related_total=1, lang=lang)

    async def driver(client):
        toggle = [b for b in _buttons(client)
                  if ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, lang) in (b.text or '')]
        assert toggle, 'no related-pages toggle'
        await _click(toggle[0])

    client = _render(model, driver=driver)
    section = _elements_with_class(client, dp.PANEL_RELATED_CLASS)[0]
    text = '\n'.join(_subtree_texts(section))

    assert RELATED_PAGE_ID not in text, (
        f'the internal page id reached a reader: {text!r}')
    assert RELATED_SHELFMARK in text, f'no shelfmark in the row: {text!r}'

    chips = [el for el in section.descendants()
             if 'chip' in (el._classes or []) and (el.text or '') == 'CUL']
    assert chips, 'the library chip -- the badge the owner asked for -- is absent'

    links = [el for el in section.descendants() if type(el).__name__ == 'Link']
    assert len(links) == 1, f'the shelfmark is not a link ({len(links)} links)'
    assert links[0].text == RELATED_SHELFMARK
    # The FOLIO, not just the manuscript: `/browse` takes `page` as an int, and
    # an alignment is about one page of the other manuscript.
    assert (links[0]._props or {}).get('href') == (
        f'/browse?sys_id={RELATED_SYS_ID}&page=3'), (
        'the shelfmark does not link to the folio the alignment is about')


@pytest.mark.parametrize('lang', ('en', 'he'))
def test_a_manuscript_missing_from_the_display_index_is_named_not_raw(spy, lang):
    """The degraded path is where a raw-id fallback would live, so it is
    asserted directly: an unresolvable manuscript gets a NAMED state and the
    composite id still does not reach the reader."""
    spy.results['get_related_pages_enveloped'] = make_envelope(
        STATUS_OK, [related_page_row(
            sys_id=None, library_code=None, shelfmark_display=None,
            display_missing=True)], 1,
        meta={'unit': 'distinct_opposite_pages'})
    model = model_for(claim_items=[claim_row()], related_total=1, lang=lang)

    async def driver(client):
        toggle = [b for b in _buttons(client)
                  if ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, lang) in (b.text or '')]
        await _click(toggle[0])

    client = _render(model, driver=driver)
    section = _elements_with_class(client, dp.PANEL_RELATED_CLASS)[0]
    text = '\n'.join(_subtree_texts(section))

    assert RELATED_PAGE_ID not in text, (
        'the composite id was printed as a fallback -- exactly how this defect '
        'comes back')
    assert dp._RELATED_ROW_COPY['display_missing'][lang] in text, text
    assert not [el for el in section.descendants() if type(el).__name__ == 'Link'], (
        'a row with no shelfmark rendered a link to nowhere')


def test_the_renderer_cannot_print_the_composite_page_id_at_all():
    """A SOURCE-level guarantee beside the behavioural ones: the panel module
    never reads `related_page_id`, and the model never emits it, so there is
    nothing to fall back to on any path anybody adds later."""
    import shared.discovery_panel_model as pm

    assert 'related_page_id' not in _read(PANEL_PATH), (
        'the panel reads the composite page id again')
    row = pm.related_page_row(related_page_row())
    assert 'related_page_id' not in row, (
        'the model emits the composite page id to the renderer')


def test_a_repeat_lazy_read_with_identical_arguments_costs_no_crossing(spy):
    """The renderer does not re-issue, AND the wrapper's version-keyed LRU means
    an identical call would not cross either. Both, because either alone would
    let the other regress unnoticed."""
    import web.discovery as wd
    asyncio.run(wd.get_related_pages_enveloped('page-1'))
    assert spy.calls == ['get_related_pages_enveloped']
    asyncio.run(wd.get_related_pages_enveloped('page-1'))
    assert spy.calls == ['get_related_pages_enveloped'], 'the cache key changed'


def test_an_outage_on_the_lazy_read_renders_a_retry_not_an_empty_list(spy):
    spy.fail['get_related_pages_enveloped'] = DiscoveryUnavailable('t')
    model = model_for(claim_items=[claim_row()], related_total=4)

    async def driver(client):
        toggle = [b for b in _buttons(client)
                  if ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, 'en') in (b.text or '')]
        await _click(toggle[0])

    client = _render(model, driver=driver)
    text = _scoped_text(client, dp.PANEL_RELATED_CLASS)
    assert ds.service_state_message('timeout', 'en') in text, text
    assert [b for b in _buttons(client) if ds.retry_label('en') in (b.text or '')], (
        'an outage on the lazy read rendered without a retry')


# ===========================================================================
# The per-work expansion.
# ===========================================================================

async def _open_expansion(client) -> None:
    heading = ds.section_header(ds.SECTION_OTHER_MANUSCRIPTS, 'en', 'Some Recorded Work')
    buttons = [b for b in _buttons(client) if (b.text or '') == heading]
    assert buttons, f'no expansion control carrying {heading!r}'
    await _click(buttons[0])


def test_the_expansion_is_lazy(spy):
    client = _render(model_for(claim_items=[claim_row()]))
    del client
    assert spy.calls == [], 'an expansion query ran with the panel'


def test_an_opened_expansion_excludes_the_anchor_and_shows_the_weaker_band(spy):
    captured: Dict[str, Any] = {}
    import web.discovery as wd
    real = wd.get_work_expansion_enveloped

    async def _capture(work_id, enabled_bands=None, **kwargs):
        captured.update(kwargs)
        captured['work_id'] = work_id
        return await real(work_id, enabled_bands, **kwargs)

    dp_disc = wd
    setattr(dp_disc, 'get_work_expansion_enveloped', _capture)
    try:
        client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    finally:
        setattr(dp_disc, 'get_work_expansion_enveloped', real)

    assert captured['anchor_sys_id'] == '990051079570205171', captured
    assert captured['anchor_claim_type'] == ids.CLAIM_TYPE_DIRECT_WITNESS
    assert captured['anchor_evidence_source'] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
    assert captured['anchor_confidence_band'] == ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC
    # The WEAKER band of the pair is resolved server-side and arrives as
    # `band_label`; the renderer never compares bands.
    chips = _elements_with_class(client, 'rel')
    weaker = band_label(ids.EVIDENCE_SOURCE_TRACK1_DIRECT,
                        ids.CONFIDENCE_BAND_SCREENING_RB)
    tooltips = {(c._props or {}).get('title') for c in chips}
    assert weaker in tooltips, tooltips


def test_an_outage_on_the_expansion_read_renders_a_retry_not_an_empty_list(spy):
    """Found by mutation N12: removing the expansion's own outage branch left
    every test green, because the only outage test drove the RELATED-PAGES
    path. Two lazy reads, two branches, two controls."""
    spy.fail['get_work_expansion_enveloped'] = DiscoveryUnavailable('t')
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    text = _scoped_text(client, dp.PANEL_EXPANSION_CLASS)
    assert ds.service_state_message('timeout', 'en') in text, text
    assert [b for b in _buttons(client) if ds.retry_label('en') in (b.text or '')], (
        'an outage on the expansion read rendered without a retry')


def test_every_rendered_carrier_row_is_named_by_library_and_shelfmark(spy):
    spy.results['get_work_expansion_enveloped'] = make_envelope(
        STATUS_OK,
        [expansion_row(library_code='CUL', shelfmark_display='T-S 12.123'),
         expansion_row(unit_id='unit-3', library_code='JTS',
                       shelfmark_display='ENA 2556.1')],
        2, meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                 'filter_basis': 'displayed_band', 'anchor_excluded': True})
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    text = _scoped_text(client, dp.PANEL_EXPANSION_CLASS)
    for token in ('CUL', 'T-S 12.123', 'JTS', 'ENA 2556.1'):
        assert token in text, f'{token!r} missing from the expansion: {text!r}'


def test_a_display_missing_carrier_gets_an_explicit_unnamed_treatment(spy):
    spy.results['get_work_expansion_enveloped'] = make_envelope(
        STATUS_OK,
        [expansion_row(display_missing=True, library_code=None,
                       shelfmark_display=None)],
        1, meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                 'filter_basis': 'displayed_band', 'anchor_excluded': True})
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    text = _scoped_text(client, dp.PANEL_EXPANSION_CLASS)
    assert ds.missing_title('en') in text, (
        'an unresolvable manuscript rendered as a blank cell')


def test_differing_relations_render_two_distinct_chips(spy):
    spy.results['get_work_expansion_enveloped'] = make_envelope(
        STATUS_OK,
        [expansion_row(claim_type=ids.CLAIM_TYPE_SHARED_TEXT,
                       anchor_claim_type=ids.CLAIM_TYPE_DIRECT_WITNESS,
                       relations_differ=True)],
        1, meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                 'filter_basis': 'displayed_band', 'anchor_excluded': True})
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    expansion = _elements_with_class(client, dp.PANEL_EXPANSION_CLASS)[0]
    chips = [n for n in expansion.descendants(include_self=True)
             if 'rel' in (getattr(n, '_classes', None) or [])]
    labels = [c.text for c in chips]
    assert labels == [ds.relation_chip(ids.CLAIM_TYPE_SHARED_TEXT, 'en'),
                      ds.relation_chip(ids.CLAIM_TYPE_DIRECT_WITNESS, 'en')], labels


def test_agreeing_relations_render_exactly_one_chip(spy):
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    expansion = _elements_with_class(client, dp.PANEL_EXPANSION_CLASS)[0]
    chips = [n for n in expansion.descendants(include_self=True)
             if 'rel' in (getattr(n, '_classes', None) or [])]
    assert len(chips) == 1, [c.text for c in chips]


def test_no_expansion_chip_carries_a_stored_vocabulary_key(spy):
    spy.results['get_work_expansion_enveloped'] = make_envelope(
        STATUS_OK,
        [expansion_row(claim_type=ids.CLAIM_TYPE_QUOTES_THIS_WORK,
                       anchor_claim_type=ids.CLAIM_TYPE_DIRECT_WITNESS,
                       relations_differ=True)],
        1, meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
                 'filter_basis': 'displayed_band', 'anchor_excluded': True})
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    expansion = _elements_with_class(client, dp.PANEL_EXPANSION_CLASS)[0]
    text = '\n'.join(_subtree_texts(expansion))
    for key in sorted(ids.CLAIM_TYPES) + [ids.CONFIDENCE_BAND_SCREENING_RB]:
        assert key not in text, f'the stored key {key!r} reached the expansion markup'
    for node in expansion.descendants(include_self=True):
        classes = set(getattr(node, '_classes', None) or [])
        assert not (classes & set(ids.CLAIM_TYPES)), 'a relation-keyed class is applied'


def test_the_expansion_total_is_the_counted_total_not_the_page_length(spy):
    spy.results['get_work_expansion_enveloped'] = make_envelope(
        STATUS_OK, [expansion_row()], 5684,
        meta={'work_id': 'w000001', 'anchor_mode': 'anchored',
              'filter_basis': 'displayed_band', 'anchor_excluded': True})
    client = _render(model_for(claim_items=[claim_row()]), driver=_open_expansion)
    text = _scoped_text(client, dp.PANEL_EXPANSION_CLASS)
    assert '5684' in text, text
    assert '\n1\n' not in f'\n{text}\n', 'the page length was rendered as the total'


def test_the_expansion_heading_uses_match_framing(spy):
    heading = ds.section_header(ds.SECTION_OTHER_MANUSCRIPTS, 'en', 'Some Recorded Work')
    assert 'matching' in heading.lower()
    for word in ('copy of', 'witness of', 'quotes'):
        assert word not in heading.lower(), heading
    client = _render(model_for(claim_items=[claim_row()]))
    assert [b for b in _buttons(client) if (b.text or '') == heading]


# ===========================================================================
# Vote placeholders and disclosure levels.
# ===========================================================================

def test_vote_controls_are_inert():
    client = _render(model_for(claim_items=[claim_row()]))
    votes = [b for b in _buttons(client)
             if 'thumb' in ((b._props or {}).get('icon') or '')]
    assert len(votes) == 2, 'the two vote placeholders are missing'
    for button in votes:
        assert 'disable' in (button._props or {}), 'a vote placeholder is enabled'
        handlers = [ln for ln in button._event_listeners.values() if ln.type == 'click']
        assert handlers == [], 'a vote placeholder carries a click handler'
    # ...and nothing in the renderer can show a confirmation: it neither
    # notifies nor writes. A word scan over the rendered text would be the wrong
    # check here -- "Some Recorded Work" is a legitimate work title.
    src = _read(PANEL_PATH)
    for forbidden in ('ui.notify', 'supabase', 'record_vote', 'save_vote'):
        assert forbidden not in src, (
            f'{PANEL_PATH} names {forbidden!r}; the vote controls are inert')


def test_exactly_the_disclosure_levels_the_model_emits_are_rendered():
    model = model_for(claim_items=[claim_row()])
    client = _render(model)
    details = [e for e in client.elements.values() if e.tag == 'details']
    collapsed = [lv for lv in model.disclosure_levels if not lv['default_visible']]
    assert len(details) == len(collapsed), (
        f'{len(details)} disclosure elements for {len(collapsed)} collapsed levels')


def test_hebrew_renders_rtl_and_bilingual():
    row = claim_row()
    model = model_for(claim_items=[row], works=[work_summary_row()], lang='he')
    client = _render(model)
    root = _elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]
    assert (root._props or {}).get('dir') == 'rtl'
    text = _scoped_text(client, dp.PANEL_ROOT_CLASS)
    assert ds.relation_chip(row['relation_kind'], 'he') in text
    assert ds.recall_disclaimer('he') in text


# ===========================================================================
# THE FOUR EAGER READS, FAILING INDEPENDENTLY (code review round 12, finding 2).
#
# Every fixture in this phase's suites assigned ONE status to claims, works and
# the related count together, so a MIXED state was structurally unreachable and
# three of the four outage paths were rendering without a retry with every test
# green. The parametrisation below is the fix, and the fixture that makes mixed
# states reachable is the point of it -- not the extra assertions.
# ===========================================================================

_EAGER_READS = ('claims', 'page_ids', 'manuscript_works', 'related_count')
_OUTAGE_STATUSES = ('unavailable', 'timeout', 'busy')


def _outage(status: str, meta_reason: str = 'forced'):
    from shared.discovery_surface_projection import (
        busy_envelope, timeout_envelope, unavailable_envelope,
    )
    return {'unavailable': unavailable_envelope,
            'timeout': timeout_envelope,
            'busy': busy_envelope}[status](meta={'reason': meta_reason})


def mixed_bundle(failing: Optional[str], status: str, lang: str = 'en'):
    """A bundle in which EXACTLY ONE of the four eager reads is an outage.

    `failing=None` builds the all-`ok` control, which is what proves the other
    three envelopes really were healthy in each mixed case rather than the whole
    bundle having quietly degraded together.
    """
    envelopes = {
        'claims': make_envelope(STATUS_OK, [claim_row()], 1,
                                meta={'page_id': 'page-1', 'include_review': False}),
        'page_ids': make_envelope(STATUS_OK, ['page-1'], 1, meta={
            'sys_id': '990051079570205171', 'resolved': True,
            'truncated': False, 'volume_ie': None}),
        'manuscript_works': make_envelope(STATUS_OK, [work_summary_row()], 1,
                                          meta={'page_scope_resolved': True, 'lang': lang}),
        'related_count': make_envelope(STATUS_OK, [], 4,
                                       meta={'unit': 'distinct_opposite_pages'}),
    }
    if failing is not None:
        envelopes[failing] = _outage(status)
    return PanelServiceBundle(related_rows=None, lang=lang, **envelopes)


def _render_with_retry(model, page_id: Optional[str] = 'page-1', driver=None,
                       on_retry=None):
    """The panel exactly as the LIVE seam renders it: with a retry handler.

    `_render` above deliberately passes none, which is why it could not have
    caught this -- the renderer draws a retry only when it is given one.
    `driver(client)` runs INSIDE the same loop and client context, so a click
    handler reaches NiceGUI's `handle_event` with a live slot stack.

    `on_retry` overrides the built-in async recorder with a caller-supplied one
    (the behavioural coverage test needs a SYNC handler so a click is observable
    without draining the loop). `fired` still counts the built-in one.
    """
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    holder: Dict[str, Any] = {}
    fired = {'n': 0}

    async def _retry():
        fired['n'] += 1

    handler = on_retry if on_retry is not None else _retry

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_discovery_panel_retry_probe')) as client:
            with client:
                dp.render_discovery_panel_body(model, on_retry=handler, page_id=page_id)
                if driver is not None:
                    await driver(client)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client'], fired


def _retry_buttons(client, lang: str = 'en') -> list:
    return [b for b in _buttons(client) if ds.retry_label(lang) in (b.text or '')]


@pytest.mark.parametrize('lang', ('en', 'he'))
@pytest.mark.parametrize('status', _OUTAGE_STATUSES)
@pytest.mark.parametrize('failing', _EAGER_READS)
def test_every_outage_a_reader_can_reach_offers_a_retry(failing, status, lang):
    """One eager read fails; the other three succeed. In EVERY such state the
    reader must be able to reach the panel and must find a retry in it.

    Codex reproduced three mixed states that failed this: successful claims plus
    a timed-out works or related-count read rendered an outage with no way to
    retry, and a claims-level zero plus a page-scope timeout hid the entry
    control entirely -- which removes the panel that carries the only retry.
    """
    model = build_panel_rows(mixed_bundle(failing, status, lang=lang))
    assert model.entry_control['hidden'] is False, (
        f'{failing}/{status}: the entry control hid on an outage, so the panel '
        'and its retry are unreachable')
    client, _fired = _render_with_retry(model)
    text = _scoped_text(client, dp.PANEL_ROOT_CLASS)
    assert ds.service_state_message(status, lang) in text, (
        f'{failing}/{status}: no outage state rendered\n{text}')
    assert _retry_buttons(client, lang), (
        f'{failing}/{status}: an outage rendered without a retry')


@pytest.mark.parametrize('status', _OUTAGE_STATUSES)
@pytest.mark.parametrize('failing', _EAGER_READS)
def test_the_retry_offered_on_a_mixed_outage_actually_invokes_the_handler(failing, status):
    """A rendered button proves markup, not wiring. Clicking it must reach the
    seam's handler -- the one that re-issues all four eager reads."""
    model = build_panel_rows(mixed_bundle(failing, status))

    async def driver(client):
        buttons = _retry_buttons(client)
        assert buttons, f'{failing}/{status}: no retry button to click'
        await _click(buttons[0])

    _client, fired = _render_with_retry(model, driver=driver)
    assert fired['n'] == 1, f'{failing}/{status}: the retry button fired nothing'


def _entry_control_client(model):
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page('/_discovery_entry_probe')) as client:
            with client:
                dp.render_discovery_entry_control(model, on_toggle=lambda: None)
        holder['client'] = client

    asyncio.run(_run())
    return holder['client']


@pytest.mark.parametrize('lang', ('en', 'he'))
@pytest.mark.parametrize('status', _OUTAGE_STATUSES)
def test_the_entry_control_says_so_when_the_scope_read_failed(status, lang):
    """The consequence of unhiding the control on a page-scope outage.

    `entry_control['status']` is the CLAIMS status, so on that combination it is
    `ok` and the count is a true zero FOR THIS PAGE -- while the pane that would
    have spoken for the whole manuscript is an outage the reader cannot see
    until the panel is opened. A bare "(0)" standing beside an unknown reads as
    "this manuscript has nothing", which is the claim the unhiding was meant to
    stop making.

    Parametrised over all three outage statuses because the control reports the
    FAILED READ'S OWN status: a scope `timeout` must say "this took longer than
    expected", not "temporarily unavailable". A constant substituted by the
    renderer would pass a single-status check and be wrong on the other two.
    """
    model = build_panel_rows(mixed_bundle('page_ids', status, lang=lang))
    assert model.entry_control['hidden'] is False
    assert model.entry_control['status'] == STATUS_OK, (
        'the fixture no longer isolates the page-ID read')
    assert model.entry_control['degraded_status'] == status
    text = '\n'.join(_subtree_texts(
        _elements_with_class(_entry_control_client(model), dp.PANEL_ENTRY_CLASS)[0]))
    assert ds.service_state_message(status, lang) in text, text
    for other in set(_OUTAGE_STATUSES) - {status}:
        assert ds.service_state_message(other, lang) not in text, (
            f'the control reported {other!r} for a {status!r} scope read')


@pytest.mark.parametrize('lang', ('en', 'he'))
def test_a_healthy_entry_control_carries_no_outage_chip(lang):
    """The other half: the chip must not fire on a healthy panel, or it becomes
    decoration everyone learns to ignore."""
    model = build_panel_rows(mixed_bundle(None, 'timeout', lang=lang))
    assert model.entry_control['degraded_status'] is None
    text = '\n'.join(_subtree_texts(
        _elements_with_class(_entry_control_client(model), dp.PANEL_ENTRY_CLASS)[0]))
    for status in _OUTAGE_STATUSES:
        assert ds.service_state_message(status, lang) not in text, text


# ===========================================================================
# A claim-less folio of a claim-rich manuscript. The measured case is RNL
# Ms. Evr. Antonin A 1: 483 claims across 396 of its 492 pages, none on page 1,
# and -- under the bare hide-on-zero rule -- no control there at all.
# ===========================================================================

@pytest.mark.parametrize('lang', ('en', 'he'))
def test_a_claim_less_folio_renders_a_control_that_names_its_scope(lang):
    from web.translations import tr

    model = model_for(claim_items=[], works=[work_summary_row()], lang=lang)
    assert model.entry_control['manuscript_elsewhere_only'] is True, (
        'the fixture no longer produces the state under test')

    client = _entry_control_client(model)
    controls = _elements_with_class(client, dp.PANEL_ENTRY_CLASS)
    assert controls, 'the control is absent on a folio whose manuscript has rows'
    text = '\n'.join(_subtree_texts(controls[0]))

    assert tr('Computed identifications elsewhere in this manuscript') in text, text
    # NOT the page-scoped wording, and NOT a page count of any kind. "(0)" is
    # the specific rendering this state exists to prevent; the general rule is
    # that the control carries no digit at all here.
    assert '(0)' not in text
    assert not any(ch.isdigit() for ch in text), text
    assert tr('Computed identifications') + ' (' not in text, text


def test_a_claim_less_folio_opens_onto_the_manuscript_pane():
    """"Works" is not enough: the reader was told there are identifications
    elsewhere in this manuscript, so the pane carrying them must be what the
    opened panel leads with -- not an empty claims list."""
    model = model_for(claim_items=[], works=[work_summary_row(
        display_work_id='w1', neutral_title='Alpha', page_count=5)])
    assert model.lead_with_manuscript_pane is True
    client = _render(model)
    panes = _elements_with_class(client, 'dpanes')
    assert len(panes) == 1
    children = list(panes[0])
    assert len(children) == 2, 'the panel is not two panes'
    manuscript = _elements_with_class(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)
    assert manuscript and manuscript[0] is children[0], (
        'the empty page pane is still first, so opening the panel from this '
        'state lands the reader on nothing')
    assert 'Alpha (5)' in _scoped_text(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)


def test_a_folio_with_claims_is_unchanged_by_the_new_branch():
    """The normal path: the page count is still the label's number and the page
    pane still leads."""
    from web.translations import tr

    model = model_for(claim_items=[claim_row()], works=[work_summary_row()])
    assert model.entry_control['manuscript_elsewhere_only'] is False
    assert model.lead_with_manuscript_pane is False
    text = '\n'.join(_subtree_texts(_elements_with_class(
        _entry_control_client(model), dp.PANEL_ENTRY_CLASS)[0]))
    assert f"{tr('Computed identifications')} (1)" in text, text

    client = _render(model)
    children = list(_elements_with_class(client, 'dpanes')[0])
    manuscript = _elements_with_class(client, dp.PANEL_MANUSCRIPT_PANE_CLASS)
    assert manuscript[0] is children[1], 'the page pane is no longer first'


def test_a_folio_whose_manuscript_is_also_empty_still_renders_nothing():
    """The common case, and the one the hide rule is right about."""
    model = model_for(claim_items=[], works=[])
    assert model.entry_control['hidden'] is True
    assert _elements_with_class(
        _entry_control_client(model), dp.PANEL_ENTRY_CLASS) == []


def _service_state_call_sites() -> List[Any]:
    """Every `_service_state_block(...)` call in the renderer, as AST nodes."""
    tree = ast.parse(_read(PANEL_PATH))
    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, 'attr', None)
        if name == '_service_state_block':
            sites.append(node)
    return sorted(sites, key=lambda n: n.lineno)


def test_no_service_state_block_call_site_takes_the_DEFAULT_handler():
    """A cheap STRUCTURAL backstop, and it is honest about what it proves.

    The AST can see that a keyword is written and that it is not the literal
    `None`. It cannot see what the forwarded name is worth at run time --
    `_service_state_block(state, on_retry=on_retry)` inside a function whose own
    `on_retry` defaults to `None` satisfies every syntactic check and still
    renders an outage with no way out. That is exactly what round 13's finding 5
    named, and the BEHAVIOURAL test below is what actually enforces the
    property. This one survives because it is the check that runs without
    rendering anything, and it names the line.
    """
    sites = _service_state_call_sites()
    assert len(sites) >= 5, (
        f'only {len(sites)} service-state call sites found; the guard is scanning '
        'the wrong tree')
    for node in sites:
        keywords = {kw.arg: kw.value for kw in node.keywords}
        assert 'on_retry' in keywords, (
            f'{PANEL_PATH}:{node.lineno} renders a service-state block without an '
            'on_retry handler -- an outage the reader cannot leave')
        value = keywords['on_retry']
        assert not (isinstance(value, ast.Constant) and value.value is None), (
            f'{PANEL_PATH}:{node.lineno} passes on_retry=None explicitly, which '
            'renders the outage message with no retry button at all')


def test_the_no_retry_guard_can_fail():
    """The positive control on the guard above, in BOTH of its clauses: an
    omitted keyword and an explicit `None` must each be reported, and a real
    handler must not be."""
    tree = ast.parse('_service_state_block(state)\n'
                     '_service_state_block(s, on_retry=None)\n'
                     '_service_state_block(s, on_retry=cb)\n')
    calls = [node for node in ast.walk(tree)
             if isinstance(node, ast.Call)
             and getattr(node.func, 'id', None) == '_service_state_block']
    keywords = [{kw.arg: kw.value for kw in node.keywords} for node in calls]
    defaulted = [kw for kw in keywords if 'on_retry' not in kw]
    explicit_none = [kw for kw in keywords
                     if isinstance(kw.get('on_retry'), ast.Constant)
                     and kw['on_retry'].value is None]
    assert len(defaulted) == 1 and len(explicit_none) == 1
    assert len(calls) == 3


class _ServiceStateProbe:
    """Records WHICH `_service_state_block` call site fired and with what.

    The site is identified by the caller frame's line number mapped into the
    AST node's own `[lineno, end_lineno]` range, so the identification survives
    a multi-line call and does not depend on which line CPython attributes the
    CALL instruction to.
    """

    def __init__(self, sites):
        self.sites = sites
        self.seen: Dict[int, List[Any]] = {}

    def install(self, monkeypatch):
        real = dp._service_state_block

        def _recorder(state, on_retry=None):
            index = self._site_for(sys._getframe(1).f_lineno)
            self.seen.setdefault(index, []).append(on_retry)
            return real(state, on_retry=on_retry)

        monkeypatch.setattr(dp, '_service_state_block', _recorder)

    def _site_for(self, lineno: int) -> Optional[int]:
        for i, node in enumerate(self.sites):
            if node.lineno <= lineno <= (node.end_lineno or node.lineno):
                return i
        return None                                          # pragma: no cover


def test_every_service_state_block_the_renderer_can_draw_gets_a_WORKING_retry(
        spy, monkeypatch):
    """The property the guard above only NAMES, enforced by rendering it.

    Round 13, finding 5: asserting that an `on_retry` KEYWORD is present is not
    asserting that a retry exists. `_service_state_block(state, on_retry=None)`
    passes the keyword check while rendering no button, and so does forwarding a
    parameter that defaulted to `None` two frames up -- which is the shape four
    of the six call sites actually have.

    So this drives EVERY call site the renderer contains, mapped back to the AST
    by line range rather than enumerated, and asserts three things about each:
    it was reached at all, it was handed a CALLABLE, and the element it drew
    carries a retry BUTTON with a bound click listener. Then it clicks one and
    checks the handler really runs.
    """
    sites = _service_state_call_sites()
    probe = _ServiceStateProbe(sites)
    probe.install(monkeypatch)

    clicked: List[bool] = []

    def _retry():
        clicked.append(True)

    clients = []
    # The four EAGER reads, each degrading a different section: the panel-level
    # outage, the manuscript pane's OUTAGE, its UNRESOLVED-scope outage (a
    # FAILED page-scope read, which does get a retry -- unlike an unresolvable
    # scope, which the model gives no service_state at all), and the
    # related-pages COUNT.
    for failing in _EAGER_READS:
        client, _fired = _render_with_retry(
            build_panel_rows(mixed_bundle(failing, 'timeout')), on_retry=_retry)
        clients.append(client)

    # The two LAZY reads, each with its own outage branch inside its own
    # renderer -- neither reachable from an eager-read fixture.
    spy.fail['get_related_pages_enveloped'] = DiscoveryUnavailable('t')
    spy.fail['get_work_expansion_enveloped'] = DiscoveryUnavailable('t')

    async def _open_related(client):
        toggle = [b for b in _buttons(client)
                  if ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, 'en') in (b.text or '')]
        assert toggle, 'no related-pages toggle to drive'
        await _click(toggle[0])

    client, _fired = _render_with_retry(
        model_for(claim_items=[claim_row()], related_total=4),
        driver=_open_related, on_retry=_retry)
    clients.append(client)
    client, _fired = _render_with_retry(
        model_for(claim_items=[claim_row()]), driver=_open_expansion, on_retry=_retry)
    clients.append(client)

    # (a) COVERAGE -- every call site the renderer contains was actually driven.
    #     A new outage branch nobody drives fails here by line number, which an
    #     enumeration of today's reads cannot do.
    undriven = [f'{PANEL_PATH}:{node.lineno}'
                for i, node in enumerate(sites) if i not in probe.seen]
    assert not undriven, (
        'these service-state call sites were never rendered by this test, so '
        'nothing here proves they offer a retry: ' + ', '.join(undriven))
    assert None not in probe.seen, (
        'a service-state block fired from a line outside every known call site '
        '-- the AST mapping is stale')

    # (b) Every driven site was handed a CALLABLE, never the silent default.
    for index, handlers in probe.seen.items():
        for handler in handlers:
            assert callable(handler), (
                f'{PANEL_PATH}:{sites[index].lineno} rendered a service-state '
                'block with on_retry=None -- an outage the reader cannot leave')

    # (c) Every service-state element RENDERED carries a retry button with a
    #     bound click listener. This is the assertion the keyword check could
    #     never make.
    rendered = 0
    for client in clients:
        for element in _elements_with_class(client, dp.PANEL_SERVICE_STATE_CLASS):
            buttons = [d for d in element.descendants(include_self=True)
                       if type(d).__name__ == 'Button']
            assert buttons, 'a service-state block rendered with no retry button'
            assert any(
                listener.type == 'click' and listener.handler is not None
                for button in buttons
                for listener in button._event_listeners.values()), (
                'the retry button carries no click handler')
            rendered += 1
    assert rendered >= len(sites), (
        f'{rendered} service-state elements rendered for {len(sites)} call sites')

    # (d) ...and clicking one really reaches the handler the seam supplied.
    retry_buttons = [b for b in _buttons(clients[0])
                     if ds.retry_label('en') in (b.text or '')]
    assert retry_buttons, 'no retry button on the panel-level outage'
    asyncio.run(_click(retry_buttons[0]))
    assert clicked, 'the retry button rendered but its click reached nothing'


@pytest.mark.parametrize('failing', _EAGER_READS)
def test_the_mixed_fixture_really_does_vary_the_four_reads_independently(failing):
    """The control on the parametrisation above. If `mixed_bundle` degraded the
    whole bundle together -- the defect in the fixture it replaces -- every case
    would render the same outage and the parametrisation would be inert."""
    healthy = build_panel_rows(mixed_bundle(None, 'timeout'))
    assert healthy.panel_status == STATUS_OK
    assert healthy.manuscript_pane['state'] not in {'outage', 'unresolved_scope'}

    model = build_panel_rows(mixed_bundle(failing, 'timeout'))
    # Exactly the sections downstream of the FAILING read degrade; the rest keep
    # reporting real facts.
    claims_ok = model.panel_status == STATUS_OK
    pane_ok = model.manuscript_pane['state'] not in {'outage', 'unresolved_scope'}
    count_ok = _related_section(model).get('count') is not None
    observed = (claims_ok, pane_ok, count_ok)
    expected = {
        'claims': (False, True, True),
        # the page-ID read is what RESOLVES the scope, so the pane is the
        # section it degrades -- the claims and the count are untouched
        'page_ids': (True, False, True),
        'manuscript_works': (True, False, True),
        'related_count': (True, True, False),
    }[failing]
    assert observed == expected, f'{failing}: {observed} != {expected}'


# ---------------------------------------------------------------------------
# RULING F -- the fourth disclosure level, RENDERED.
#
# The model decides which claims belong there; these assert what a reader
# actually meets. The one thing that cannot be tested in the model is WHERE the
# warning ends up: everything under a `<details>` except its `<summary>` is
# hidden while the element is closed, so a warning in the body would be met
# only AFTER the decision it exists to inform.
# ---------------------------------------------------------------------------

def _details_elements(client):
    return [el for el in client.elements.values()
            if (getattr(el, 'tag', '') or '') == 'details']


def _divergence_details(client, lang='en'):
    label = ds.disclosure_toggle(ds.TOGGLE_DIVERGENCE, lang)
    found = [el for el in _details_elements(client)
             if label in _subtree_texts(el)]
    assert len(found) == 1, f'expected one divergence <details>, got {len(found)}'
    return found[0]


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_a_divergent_claim_renders_in_a_closed_fourth_disclosure(lang):
    model = model_for(claim_items=[claim_row(novelty_status='diverges_work')],
                      lang=lang)
    client = _render(model)

    details = _divergence_details(client, lang)
    assert 'open' not in (details._props or {}), (
        'ruling F requires the level be hidden by default; it rendered open')
    assert 'notid' not in (details._classes or []), (
        "the level took the 'not identifications' treatment -- the catalogue "
        'names a DIFFERENT identification and ours is still one')

    body = '\n'.join(_subtree_texts(details))
    assert claim_row()['neutral_title'] in body, 'the claim is not reachable at all'


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_the_warning_renders_OUTSIDE_the_collapsed_body(lang):
    """The whole reason it is a `warning` key and not a `note`."""
    model = model_for(claim_items=[claim_row(novelty_status='diverges_part')],
                      lang=lang)
    client = _render(model)

    warning = ds.divergence_warning(lang)
    details = _divergence_details(client, lang)
    assert warning not in _subtree_texts(details), (
        'the warning is inside the collapsed <details> -- a reader meets it '
        'only after opening, i.e. after the decision it exists to inform')

    root = _elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]
    assert warning in _subtree_texts(root), (
        'the warning is not on the panel at all')


def test_an_undivergent_panel_renders_no_divergence_warning():
    """The other direction, so the assertion above cannot pass by rendering the
    warning unconditionally."""
    client = _render(model_for(claim_items=[claim_row()]))
    root = _elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]
    assert ds.divergence_warning('en') not in _subtree_texts(root)


def test_opening_the_axis_renders_the_fourth_disclosure_open():
    model = model_for(claim_items=[claim_row(novelty_status='diverges_work')],
                      show_divergence=True)
    details = _divergence_details(_render(model))
    assert 'open' in (details._props or {})


def test_a_divergent_claim_is_absent_from_the_default_section_on_screen():
    """Not merely filed elsewhere in the model -- absent from what the reader
    sees without opening anything."""
    client = _render(model_for(
        claim_items=[claim_row(novelty_status='diverges_work')]))
    root = _elements_with_class(client, dp.PANEL_ROOT_CLASS)[0]
    title = claim_row()['neutral_title']

    inside_a_disclosure = set()
    for details in _details_elements(client):
        inside_a_disclosure.update(id(el) for el in details.descendants())
    visible = [
        text
        for el in root.descendants(include_self=True)
        if id(el) not in inside_a_disclosure
        for text in _subtree_texts(el)
    ]
    assert title not in visible, (
        'the divergent claim renders outside every disclosure -- it is in the '
        'default view')
