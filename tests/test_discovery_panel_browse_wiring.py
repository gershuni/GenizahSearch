# -*- coding: utf-8 -*-
"""The browse seam the discovery_panel attaches to (Phase 136, plan 136-17, Task 1).

Four things this suite proves that no existing suite can:

1. **The offload contract, as an AST/source guard.** `tests/test_no_await_sync_function.py`
   catches `await <sync fn>()`; it cannot see a `run.io_bound` wrapped around an
   async wrapper, a NESTED offload, a synchronous `shared.discovery_service`
   call from a page module, or an import of the private `web.discovery._service`.
   All four are state-INDEPENDENT and are the primary control here.
2. **The executor-dispatch COUNT PER SERVICE STATE**, derived from one rule:
   *exactly one crossing per read that is ISSUED and reaches `_run_off_loop`.*
   A read refused before dispatch costs zero; a read the panel never issues
   costs zero; a read that crosses and then fails still costs one.
3. **A non-`ok` page-ID envelope suppresses the manuscript-works read**, exactly
   as `meta['resolved'] is False` does -- asserted as NOT ISSUED, never as
   "the render looks right".
4. **The staleness and liveness obligations** of the enrichment seam.

The `busy` rows are INJECTED: the panel's reads are `heavy=False`, so
`_acquire_heavy_slot` is never called and `DiscoveryOverload` is unreachable
through the live gate. The injection happens at the point the live gate would
raise -- BEFORE the executor dispatch -- or the counts would not describe what
is being tested.
"""

from __future__ import annotations

import ast
import asyncio
import io
from typing import Any, Dict, List, Optional

import pytest

import web.pages.browse_enrichment as be
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_service import DiscoveryService
from shared.discovery_panel_model import (
    PANE_UNRESOLVED,
    SCOPE_TRUNCATED,
    build_panel_rows,
)
from shared.discovery_surface_projection import (
    STATUS_BUSY,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_UNAVAILABLE,
    make_envelope,
)

_ENRICHMENT_SRC = 'web/pages/browse_enrichment.py'
_PANEL_SRC = 'web/components/discovery_panel.py'
_BROWSE_SRC = 'web/pages/browse.py'

#: The functions that make up the PANEL PATH inside `browse_enrichment.py`.
#: The module's four OTHER fetchers legitimately use `run.io_bound` (they wrap
#: genuinely synchronous services), so a module-wide scan here would be a scan
#: that can never pass -- and a scan that is then deleted.
_PANEL_PATH_FUNCTIONS = (
    'discovery_panel_enabled',
    'fetch_discovery_panel_bundle',
    'fetch_discovery_panel',
    'update_discovery_panel_section',
)


def _read(path: str) -> str:
    return io.open(path, encoding='utf-8').read()


def _panel_path_nodes():
    """Every AST node on the panel path, from BOTH owned modules."""
    nodes = []
    tree = ast.parse(_read(_ENRICHMENT_SRC))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in _PANEL_PATH_FUNCTIONS:
                nodes.append((f'{_ENRICHMENT_SRC}::{node.name}', node))
    nodes.append((_PANEL_SRC, ast.parse(_read(_PANEL_SRC))))
    return nodes


def _attribute_chain(node: ast.AST) -> str:
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return '.'.join(reversed(parts))


# ===========================================================================
# (1) The offload guard -- all four modes.
# ===========================================================================

def test_panel_path_contains_no_run_io_bound():
    """Mode (i). `run.io_bound(<async wrapper>)` hands a coroutine object to a
    sync worker and never executes the query."""
    for where, node in _panel_path_nodes():
        for call in (n for n in ast.walk(node) if isinstance(n, ast.Call)):
            chain = _attribute_chain(call.func)
            assert not chain.endswith('io_bound'), (
                f'{where} calls {chain!r} -- the panel path awaits the '
                'web.discovery async wrappers directly; they already dispatch '
                'off the loop internally (offload contract, plan 136-17)'
            )
            assert not chain.endswith('cpu_bound'), f'{where} calls {chain!r}'


def test_panel_path_contains_no_nested_offload():
    """Mode (ii). A `run.*_bound` worker that itself awaits a wrapper burns two
    threadpool slots per panel load on a single-worker server."""
    for where, node in _panel_path_nodes():
        for outer in ast.walk(node):
            if not isinstance(outer, ast.Call):
                continue
            if not _attribute_chain(outer.func).endswith(('io_bound', 'cpu_bound')):
                continue
            for inner in ast.walk(outer):
                assert not isinstance(inner, ast.Await), (
                    f'{where} awaits inside an offload worker (nested offload)')


def _imported_modules(tree: ast.AST):
    """Every module name imported anywhere in `tree`, with the names bound."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.extend((a.name, a.asname or a.name) for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.extend(((node.module or ''), a.name) for a in node.names)
    return out


def test_panel_path_makes_no_direct_synchronous_service_call():
    """Mode (iii). A page module must never reach into
    `shared.discovery_service` -- the async wrappers in `web/discovery.py` are
    the ONLY supported entry point, and they are what carry the envelope.

    Checked over the AST, never over the source TEXT: both modules explain this
    rule in prose, and a text scan that fails on its own explanation is a scan
    the next person deletes (the failure 136-10 recorded).
    """
    for path in (_ENRICHMENT_SRC, _PANEL_SRC):
        tree = ast.parse(_read(path))
        for module, bound in _imported_modules(tree):
            assert 'discovery_service' not in module, (
                f'{path} imports {module!r}; every discovery read goes through '
                'the enveloped web.discovery wrappers'
            )
            assert 'discovery_service' not in bound, f'{path} binds {bound!r}'


def test_panel_path_never_touches_the_private_service_singleton():
    """Mode (iv). `web.discovery._service` bypasses the fail-open wrappers, so a
    `DiscoveryUnavailable` would escape onto the browse hot path."""
    for path in (_ENRICHMENT_SRC, _PANEL_SRC):
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                assert node.attr != '_service', (
                    f'{path} accesses the private `_service` singleton')
        for module, bound in _imported_modules(tree):
            assert bound != '_service', f'{path} imports _service from {module!r}'


def test_the_private_singleton_guard_can_fail():
    """Positive control: the AST walk really does see the access it forbids."""
    tree = ast.parse('from web import discovery\nx = discovery._service.get_version()\n')
    hits = [n.attr for n in ast.walk(tree)
            if isinstance(n, ast.Attribute) and n.attr == '_service']
    assert hits == ['_service']


def test_the_offload_guard_can_fail():
    """Positive control for the guard itself: the checks are run against a
    seeded source string, so a guard that silently matched nothing is caught."""
    seeded = 'async def fetch_discovery_panel_bundle():\n    x = await run.io_bound(f)\n'
    tree = ast.parse(seeded)
    found = [
        _attribute_chain(n.func) for n in ast.walk(tree)
        if isinstance(n, ast.Call) and _attribute_chain(n.func).endswith('io_bound')
    ]
    assert found == ['run.io_bound']


# ===========================================================================
# Fixtures: envelopes, a fake page, and the dispatch spy.
# ===========================================================================

_PAGE_ID = '990000000000000944_IE1_P000002_FL3'
_SYS_ID = '990000000000000944'
_VOLUME_IE = 'IE1'


class _FakePage:
    def __init__(self, page_id: str = _PAGE_ID, volume_ie: Optional[str] = _VOLUME_IE):
        self.sys_id = _SYS_ID
        self.full_header = page_id.replace('_P000002_', '_P2_')
        self.volume_ie = volume_ie
        self.p_num = 2
        self.text = 'שלום'


def _claims_envelope(items=(), page_id: str = _PAGE_ID):
    return make_envelope(STATUS_OK, list(items), len(items),
                         meta={'page_id': page_id, 'include_review': False})


def _page_ids_envelope(page_ids=(_PAGE_ID,), resolved=True, truncated=False):
    return make_envelope(STATUS_OK, list(page_ids), len(page_ids), meta={
        'sys_id': _SYS_ID, 'resolved': resolved, 'truncated': truncated,
        'volume_ie': _VOLUME_IE,
    })


def _works_envelope(items=()):
    return make_envelope(STATUS_OK, list(items), len(items),
                         meta={'page_scope_resolved': True, 'lang': 'en'})


def _count_envelope(total=0):
    return make_envelope(STATUS_OK, [], total, meta={'unit': 'distinct_opposite_pages'})


class _Spy:
    """Counts executor crossings and fabricates each read's result.

    Patched onto `DiscoveryService._run_off_loop`, which is the ONE place a
    crossing happens -- `_browse_cached_call` calls it on a MISS and not on a
    HIT, so the version-keyed LRU's real behaviour is preserved and the counts
    describe the live cache.
    """

    def __init__(self):
        self.calls: List[str] = []
        self.results: Dict[str, Any] = {}
        self.refuse_before_dispatch: Dict[str, BaseException] = {}
        self.fail_after_dispatch: Dict[str, BaseException] = {}

    async def __call__(self, sync_fn, *args, timeout=None, heavy=False):
        # NOTE: assigned to the CLASS attribute `_run_off_loop`, but a plain
        # object is not a descriptor, so `self` is NOT passed -- the first
        # positional argument really is the sync callable.
        name = getattr(sync_fn, '__name__', repr(sync_fn))
        if name in self.refuse_before_dispatch:
            # The live `busy` gate (`_acquire_heavy_slot`) raises BEFORE
            # `run_in_executor`, so a refused read costs no crossing.
            raise self.refuse_before_dispatch[name]
        self.calls.append(name)
        if name in self.fail_after_dispatch:
            raise self.fail_after_dispatch[name]
        return self.results[name]

    @property
    def count(self) -> int:
        return len(self.calls)


class _FakeManuscriptPageIds:
    def __init__(self, page_ids, resolved=True, truncated=False):
        self.sys_id = _SYS_ID
        self.page_ids = list(page_ids)
        self.total = len(page_ids)
        self.resolved = resolved
        self.truncated = truncated


@pytest.fixture
def spy(monkeypatch):
    import web.discovery as wd

    s = _Spy()
    s.results = {
        'get_manuscript_page_ids': _FakeManuscriptPageIds([_PAGE_ID]),
        'get_claims_for_page_enveloped': _claims_envelope(),
        'get_manuscript_works_enveloped': _works_envelope(),
        'get_related_page_count_enveloped': _count_envelope(),
        'get_related_pages_enveloped': make_envelope(
            STATUS_OK, [], 0, meta={'unit': 'distinct_opposite_pages'}),
    }
    monkeypatch.setattr(DiscoveryService, '_run_off_loop', s, raising=True)
    monkeypatch.setattr(wd, 'discovery_available', lambda: True)

    class _Svc:
        def get_manuscript_page_ids(self, *a, **k):  # pragma: no cover -- never run
            raise AssertionError('the sync accessor must run off the loop')

    import web.services as web_services
    monkeypatch.setattr(web_services, 'get_service', lambda: _Svc())
    # The version-keyed LRU is shared process-wide; a stale entry from another
    # test would make the cold-cache row unreachable.
    with wd._service._browse_lru_lock:
        wd._service._browse_lru.clear()
    yield s
    with wd._service._browse_lru_lock:
        wd._service._browse_lru.clear()


def _fetch(page=None, lang='en', is_stale=None):
    return asyncio.run(be.fetch_discovery_panel_bundle(
        page or _FakePage(), lang, is_stale=is_stale))


# ===========================================================================
# (2) The dispatch count, per service state. ONE rule, applied ten ways.
# ===========================================================================

def test_dispatch_ok_resolved_cold_cache_is_four(spy):
    bundle = _fetch()
    assert bundle is not None
    assert spy.count == 4, spy.calls
    assert sorted(spy.calls) == sorted([
        'get_manuscript_page_ids', 'get_claims_for_page_enveloped',
        'get_related_page_count_enveloped', 'get_manuscript_works_enveloped',
    ])


def test_dispatch_page_turn_within_one_manuscript_is_three(spy):
    _fetch()
    spy.calls.clear()
    other = _FakePage(page_id='990000000000000944_IE1_P000003_FL4')
    spy.results['get_claims_for_page_enveloped'] = _claims_envelope(
        page_id='990000000000000944_IE1_P000003_FL4')
    _fetch(other)
    # `manuscript_works_enveloped` is keyed on the STABLE page-id tuple and HITS;
    # the claims and related-count keys carry `page_id`, which changed.
    assert 'get_manuscript_works_enveloped' not in spy.calls, spy.calls
    assert spy.count == 3, spy.calls


def test_dispatch_repeat_of_the_same_folio_is_one(spy):
    _fetch()
    spy.calls.clear()
    _fetch()
    assert spy.calls == ['get_manuscript_page_ids'], spy.calls


def test_dispatch_unresolved_scope_cold_is_three(spy):
    spy.results['get_manuscript_page_ids'] = _FakeManuscriptPageIds([], resolved=False)
    _fetch()
    assert 'get_manuscript_works_enveloped' not in spy.calls
    assert spy.count == 3, spy.calls


def test_dispatch_unavailable_is_zero(spy, monkeypatch):
    """Every wrapper short-circuits in `web/discovery.py` BEFORE the service --
    and the panel still renders its outage state, which is the point."""
    import web.discovery as wd
    monkeypatch.setattr(wd, 'discovery_available', lambda: False)
    bundle = _fetch()
    assert spy.count == 0, spy.calls
    model = build_panel_rows(bundle)
    assert model.panel_status == STATUS_UNAVAILABLE
    assert model.entry_control['hidden'] is False
    assert model.service_state['retry']


def test_dispatch_timeout_on_the_page_id_read_is_three(spy):
    spy.fail_after_dispatch['get_manuscript_page_ids'] = DiscoveryUnavailable('t')
    bundle = _fetch()
    assert spy.count == 3, spy.calls
    assert bundle.page_ids['status'] == STATUS_TIMEOUT
    assert 'get_manuscript_works_enveloped' not in spy.calls


def test_dispatch_timeout_on_a_downstream_read_is_four(spy):
    spy.fail_after_dispatch['get_claims_for_page_enveloped'] = DiscoveryUnavailable('t')
    bundle = _fetch()
    assert spy.count == 4, spy.calls
    assert bundle.claims['status'] == STATUS_TIMEOUT


def test_dispatch_busy_on_the_page_id_read_is_two(spy):
    spy.refuse_before_dispatch['get_manuscript_page_ids'] = DiscoveryOverload('b')
    bundle = _fetch()
    assert spy.count == 2, spy.calls
    assert bundle.page_ids['status'] == STATUS_BUSY
    assert 'get_manuscript_works_enveloped' not in spy.calls


def test_dispatch_busy_on_a_downstream_read_is_three(spy):
    spy.refuse_before_dispatch['get_claims_for_page_enveloped'] = DiscoveryOverload('b')
    bundle = _fetch()
    assert spy.count == 3, spy.calls
    assert bundle.claims['status'] == STATUS_BUSY


# ===========================================================================
# (3) A non-`ok` page-ID envelope suppresses the works read, for EVERY status.
# ===========================================================================

@pytest.mark.parametrize('status', [STATUS_TIMEOUT, STATUS_BUSY, STATUS_UNAVAILABLE])
def test_non_ok_page_id_envelope_suppresses_the_manuscript_works_read(spy, monkeypatch, status):
    """The broken implementation this catches branches only on
    `meta['resolved']`, finds the key missing on an outage envelope, treats it
    as falsy-but-present or defaults it to True, and queries the empty page
    set -- rendering "nothing elsewhere in this manuscript" during an outage."""
    import web.discovery as wd
    if status == STATUS_UNAVAILABLE:
        real = wd.get_manuscript_page_ids

        async def _unavailable(*a, **k):
            from shared.discovery_surface_projection import unavailable_envelope
            return unavailable_envelope(meta={'reason': 'sidecar_not_serving'})
        monkeypatch.setattr(wd, 'get_manuscript_page_ids', _unavailable)
        assert real is not _unavailable
    elif status == STATUS_TIMEOUT:
        spy.fail_after_dispatch['get_manuscript_page_ids'] = DiscoveryUnavailable('t')
    else:
        spy.refuse_before_dispatch['get_manuscript_page_ids'] = DiscoveryOverload('b')

    bundle = _fetch()
    assert bundle.page_ids['status'] == status
    assert 'get_manuscript_works_enveloped' not in spy.calls, (
        'the works read was ISSUED over an unresolved page scope')
    model = build_panel_rows(bundle)
    assert model.manuscript_pane['state'] == PANE_UNRESOLVED
    assert 'total' not in model.manuscript_pane


def test_unresolved_meta_branches_without_querying_the_empty_page_set(spy):
    spy.results['get_manuscript_page_ids'] = _FakeManuscriptPageIds([], resolved=False)
    bundle = _fetch()
    assert 'get_manuscript_works_enveloped' not in spy.calls
    model = build_panel_rows(bundle)
    assert model.manuscript_pane['state'] == PANE_UNRESOLVED


def test_truncated_scope_renders_the_partial_marker(spy):
    spy.results['get_manuscript_page_ids'] = _FakeManuscriptPageIds(
        [_PAGE_ID], resolved=True, truncated=True)
    bundle = _fetch()
    model = build_panel_rows(bundle)
    assert model.manuscript_pane['partial_scope'] is True
    assert model.manuscript_pane['scope_state'] == SCOPE_TRUNCATED
    assert model.manuscript_pane['total_covers_resolved_pages_only'] is True


def test_volume_ie_is_passed_to_the_page_id_accessor(spy, monkeypatch):
    """A multi-volume manuscript resolves the ACTIVE volume's page set, not the
    whole manuscript's."""
    seen: Dict[str, Any] = {}
    import web.discovery as wd
    real = wd.get_manuscript_page_ids

    async def _capture(sys_id, *, volume_ie=None, limit=None):
        seen['sys_id'] = sys_id
        seen['volume_ie'] = volume_ie
        return await real(sys_id, volume_ie=volume_ie, limit=limit)

    monkeypatch.setattr(wd, 'get_manuscript_page_ids', _capture)
    _fetch(_FakePage(volume_ie='IE89040977'))
    assert seen == {'sys_id': _SYS_ID, 'volume_ie': 'IE89040977'}


# ===========================================================================
# (4) The three obligations of the enrichment seam.
# ===========================================================================

def test_a_generation_change_mid_await_paints_nothing(spy):
    """The token is re-checked after EVERY await. A fast page navigation
    otherwise paints a stale panel over the wrong folio -- and issues a second
    query for a folio the reader has already left."""
    flips = {'n': 0}

    def _is_stale() -> bool:
        flips['n'] += 1
        return flips['n'] >= 1   # stale immediately after the first gather

    bundle = _fetch(is_stale=_is_stale)
    assert bundle is None, 'a stale load produced a bundle to paint'
    assert 'get_manuscript_works_enveloped' not in spy.calls, (
        'a second query was issued for a folio the reader had left')


def test_a_generation_change_after_the_works_read_paints_nothing(spy):
    flips = {'n': 0}

    def _is_stale() -> bool:
        flips['n'] += 1
        return flips['n'] >= 2   # stale only after the works read

    assert _fetch(is_stale=_is_stale) is None
    assert 'get_manuscript_works_enveloped' in spy.calls


def test_the_client_liveness_guard_bails_when_the_container_is_deleted():
    """`load_enrichment` returns BEFORE `update_enrichment_sections` when the
    visitor left the page entirely -- the generation check alone cannot see
    that, because the generation is unchanged."""
    src = _read(_ENRICHMENT_SRC)
    guard = src[src.index('_cc = refs.content_container'):]
    assert '_cc.is_deleted' in guard
    assert 'if _page_gone:' in guard
    body = guard[guard.index('if _page_gone:'):]
    assert body.split('\n')[1].strip() == 'return'
    # ...and the retry handler carries the SAME guard, because it re-renders on
    # a path `load_enrichment` never runs again.
    retry = src[src.index('async def _retry()'):src.index('def _toggle_panel()')]
    assert 'is_deleted' in retry


def test_page_client_is_bound_at_render_time():
    """`run.io_bound` degrades `safe_user_*` to `{}` and `ensure_future` empties
    the slot stack, so `ui.context.*` RAISES. The client is therefore captured
    synchronously, during the render, never inside a coroutine."""
    src = _read(_BROWSE_SRC)
    assert 'refs.page_client = ui.context.client' in src
    idx = src.index('refs.page_client = ui.context.client')
    prefix = src[:idx]
    assert 'async def load_page' not in prefix.split('def create_browse_page')[-1], (
        'page_client is bound after an async definition took over the flow')


def test_per_user_state_is_captured_into_plain_locals_before_any_await():
    src = _read(_ENRICHMENT_SRC)
    closure = src[src.index('async def fetch_discovery_panel()'):src.index('    try:\n        (all_sources')]
    lang_at = closure.index('_lang = get_language()')
    await_at = closure.index('await ')
    assert lang_at < await_at, 'get_language() is read after an await'


# ===========================================================================
# The seam's structural obligations.
# ===========================================================================

def test_a_fifth_enrichment_placeholder_exists_in_the_same_shape_as_the_four():
    browse = _read(_BROWSE_SRC)
    for key in ('pgp_link_container', 'version_container', 'joins_container',
                'bib_catalog_container', 'discovery_panel_container'):
        assert f"enrichment_refs['{key}']" in browse, key
    assert "enrichment_refs['discovery_entry_container']" in browse
    enrich = _read(_ENRICHMENT_SRC)
    assert "refs.enrichment_refs.get('discovery_panel_container')" in enrich
    assert 'update_discovery_panel_section(state, refs)' in enrich


def test_fetch_discovery_panel_is_part_of_the_existing_gather():
    tree = ast.parse(_read(_ENRICHMENT_SRC))
    gathered = set()
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        if _attribute_chain(call.func) != 'asyncio.gather':
            continue
        for arg in call.args:
            if isinstance(arg, ast.Call):
                gathered.add(_attribute_chain(arg.func))
    for name in ('fetch_pgp', 'fetch_fjms', 'fetch_crossref',
                 'fetch_browse_enrichment', 'fetch_discovery_panel'):
        assert name in gathered, f'{name}() is not in an enrichment gather; got {gathered}'


def test_the_panel_is_absent_entirely_when_the_flag_is_off(monkeypatch):
    """"Deployed with the flag off for the public" has to mean the browse page
    is what it was. The flag gates EXISTENCE; `discovery_available()` (flag AND
    sidecar readiness) gates the envelope STATUS."""
    import web.feature_flags as ff
    monkeypatch.setattr(ff, 'DISCOVERY_ENABLED', False)
    assert be.discovery_panel_enabled() is False
    monkeypatch.setattr(ff, 'DISCOVERY_ENABLED', True)
    assert be.discovery_panel_enabled() is True


def test_a_page_with_no_resolvable_page_id_yields_no_bundle(spy):
    page = _FakePage()
    page.full_header = 'not a discovery header'
    assert _fetch(page) is None
    assert spy.count == 0


# ===========================================================================
# Entry-control behaviour: hidden ONLY on a successful zero.
# ===========================================================================

def test_entry_control_hidden_on_a_true_zero(spy):
    bundle = _fetch()
    model = build_panel_rows(bundle)
    assert model.entry_control == {'hidden': True, 'status': STATUS_OK, 'count': 0}


@pytest.mark.parametrize('status,inject', [
    (STATUS_UNAVAILABLE, 'unavailable'),
    (STATUS_TIMEOUT, 'timeout'),
    (STATUS_BUSY, 'busy'),
])
def test_entry_control_visible_with_a_retry_on_every_outage(spy, monkeypatch, status, inject):
    if inject == 'unavailable':
        import web.discovery as wd
        monkeypatch.setattr(wd, 'discovery_available', lambda: False)
    elif inject == 'timeout':
        spy.fail_after_dispatch['get_claims_for_page_enveloped'] = DiscoveryUnavailable('t')
    else:
        spy.refuse_before_dispatch['get_claims_for_page_enveloped'] = DiscoveryOverload('b')

    model = build_panel_rows(_fetch())
    assert model.entry_control['hidden'] is False, (
        f'{status}: an outage was hidden as though the manuscript had nothing')
    assert model.entry_control['status'] == status
    assert model.entry_control['count'] is None
    assert model.service_state['status'] == status
    assert model.service_state['message']
    assert model.service_state['retry']
