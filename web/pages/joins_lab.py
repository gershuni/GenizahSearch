# -*- coding: utf-8 -*-
"""Joins Lab page — /joins-lab vertical spine (Phase 117, Plan 04).

This module wires all Wave-1 pieces into the working end-to-end slice:
  anchor pane (AnchorViewer) + minimal textarea builder + off-loop search
  (compose -> execute -> dedup_candidates) + read-only candidate grid +
  safe_storage anchor persistence (D-13).

Requirements satisfied: FND-02, FND-03, FND-08, BLD-01, BLD-05, CND-01.

SECURITY & MULTITENANT INVARIANTS
-----------------------------------
- Zero raw ``app.storage.user`` access — all per-user state through
  ``web.joins_lab_storage`` helpers (Phase 87 CI guard, allowlist ``[]``).
- All image fetches through existing per-provider proxy endpoints + Phase-98
  NLI circuit breaker — never a direct IIIF URL (ANC-02, D-11).
- Off-loop: ``executor.execute_search`` ONLY inside the sync
  ``run_search_core`` closure dispatched via ``run.io_bound`` (MEDIUM-4,
  SC#3; ``tests/test_joins_lab_off_loop.py`` enforces this statically).
- ``asyncio.wait_for`` timeout bounds each search (HIGH-3, T-117-12).
- Latest-wins cancellation: a new Run Search click cancels the in-flight
  asyncio.Task so rapid re-runs show only the latest result (HIGH-3).
- Stale-generation discard: ``_should_apply_results`` ensures a
  cooperatively-cancelled run's partial results (the core catches
  InterruptedError and returns partial results — genizah_core.py:9000/:9071)
  never update the UI (HIGH-3, third SC#3 leg).
- Cooperative worker cancellation: the per-generation ``_make_progress_cb``
  raises ``InterruptedError`` when superseded, aborting the core scan loop
  early and freeing the run.io_bound worker thread (MEDIUM, mirroring
  search.py:4055-4058 and parallels.py:2143).

NOTE — "Choose from my lists" login gate (D-06, LOCKED DECISION)
-----------------------------------------------------------------
An anonymous web visitor's ``UserListsManager.data`` DOES fall back to a
local ``ListsManager`` (``web/user_lists.py:92-96``) and ``web/main.py:2270``
wires one process-wide (``state._local_lists_mgr``; ``web/state.py:20-21/
45-52``).  However, that local store is a SINGLE process-global server-side
pkl shared across ALL anonymous sessions — not per-user, not per-session.
Surfacing it as "My Lists" to anonymous web visitors would mix data across
users (the very reason Phase 87–89 moved per-user state to Supabase /
safe_storage).  D-06 (locked) therefore gates "Choose from my lists" on
login: "my lists" routes ONLY to the per-user Supabase lists.  An explicit
login-prompt dialog is shown for anonymous visitors — NOT a silent failure,
NOT the shared local store.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from nicegui import run, ui

from shared.joins_lab import BuilderRow, SideQuery, compose, dedup_candidates
from web.components.anchor_viewer import AnchorViewer, inject_viewer_assets
from web.components.candidate_grid import create_candidate_grid
from web.joins_executor import WebSearchExecutor
from web.joins_lab_storage import read_anchor, write_anchor
from web.services import get_service
from web.state import state
from web.translations import is_rtl, tr

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SEARCH_TIMEOUT_SECONDS = 120
"""Per-search asyncio.wait_for budget (seconds).

Matches the interactive search tier.  Tune via this constant;
Phase 118+ can expose it as an env knob if needed.
"""


# ---------------------------------------------------------------------------
# Pure module-level helpers (importable without NiceGUI — tested headlessly)
# ---------------------------------------------------------------------------


def decide_initial_anchor(
    initial_sys_id: Optional[str],
    initial_shelfmark: Optional[str],
    stored: Optional[dict],
) -> Optional[dict]:
    """Decide which anchor to load on page entry (D-13: URL wins over storage).

    Args:
        initial_sys_id:    sys_id URL param (wins over everything else).
        initial_shelfmark: shelfmark URL param (used only when sys_id absent).
        stored:            dict from ``read_anchor()`` (may be None).

    Returns a dict with at least ``{'source': ..., 'sys_id': ...}`` or None.

    Priority:
      1. URL ``sys_id`` param — wins immediately.
      2. URL ``shelfmark`` param — needs async resolution (caller handles).
      3. Stored anchor (``stored['anchor_sys_id']``) from safe_storage.
      4. None — cold start, show empty state.
    """
    if initial_sys_id:
        return {'source': 'url_sys_id', 'sys_id': initial_sys_id}
    if initial_shelfmark:
        return {'source': 'url_shelfmark', 'shelfmark': initial_shelfmark}
    if stored and stored.get('anchor_sys_id'):
        return {
            'source': 'stored',
            'sys_id': stored['anchor_sys_id'],
            'fl_id': stored.get('anchor_fl_id'),
            'volume_ie': stored.get('anchor_volume_ie'),
        }
    return None


def lines_to_side_query(text: str) -> SideQuery:
    """Map a multi-line textarea string to a SideQuery (D-08/D-09, spine builder).

    Each non-empty stripped line becomes a BuilderRow(term=<line>).  Blank
    lines are dropped.  Spine defaults: ``variants=False``, ``line_start=False``,
    ``line_end=False``, ``gap_to_next=0``, ``page_position=None``.

    Returns a SideQuery (may have zero rows if all lines are blank).
    """
    rows = tuple(
        BuilderRow(term=line.strip())
        for line in text.splitlines()
        if line.strip()
    )
    return SideQuery(rows=rows, variants=False, page_position=None)


def _should_apply_results(my_gen: int, gen_ref: dict) -> bool:
    """Return True iff *my_gen* is still the current search generation.

    Pure helper — module-level so tests can assert the discard decision
    headlessly without instantiating the page.

    Args:
        my_gen:  The generation counter captured when this search started.
        gen_ref: The page's ``_search_generation`` mutable dict (``{'value': N}``).

    This is the PRIMARY discard mechanism for a cooperatively-cancelled run:
    the search core catches the ``InterruptedError`` raised by the progress_cb
    (genizah_core.py:9000) and returns PARTIAL results — it does NOT re-raise.
    ``run_search_core`` therefore returns normally with a partial list.  This
    guard is what prevents that partial list from updating the UI.
    """
    return my_gen == gen_ref['value']


def _make_progress_cb(my_gen: int, gen_ref: dict):
    """Return a progress callback that cooperatively cancels superseded searches.

    The returned ``progress_cb(arg1, arg2=None)`` is called by the search core
    on every chunk.  When the page has started a newer search (``_search_generation``
    bumped above ``my_gen``), the callback raises ``InterruptedError`` — which the
    core CATCHES internally (genizah_core.py:9000 ``except InterruptedError:
    was_interrupted=True``), aborts the scan loop early (frees the run.io_bound
    worker thread), and returns the partial deduped results gathered so far
    (genizah_core.py:9005/:9071).  The caller's stale-generation guard
    (``_should_apply_results``) then discards those partial results.

    NOTE: ``.cancel()`` on the asyncio.Task only cancels the asyncio.wait_for
    wrapper coroutine.  It does NOT stop the already-running run.io_bound
    worker THREAD (Python threads cannot be force-killed).  True worker
    cancellation is achieved HERE via the per-generation InterruptedError raise.

    Also implements the dual-protocol guard from parallels.py:2140-2154 to
    prevent the 2026-06-12 TypeError prod bug: if arg1 is a string status
    message, the callback returns silently (not superseded logic).

    Module-level for testability.
    """
    def progress_cb(arg1, arg2=None):
        # COOPERATIVE CANCEL CHECK (MEDIUM): fires first — if a newer search
        # has started, abort the old scan loop early to free the worker thread.
        if my_gen != gen_ref['value']:
            raise InterruptedError('joins-lab search superseded')
        # DUAL-PROTOCOL GUARD (2026-06-12): core may call cb('Scanning...')
        # as a single-string status; ignore to prevent TypeError.
        if isinstance(arg1, str):
            return
        # Numeric progress (current, total) — ignore in the spine (no progress UI).

    return progress_cb


# ---------------------------------------------------------------------------
# Main page factory
# ---------------------------------------------------------------------------


def create_joins_lab_page(
    initial_sys_id: Optional[str] = None,
    initial_shelfmark: Optional[str] = None,
    initial_fl_id: Optional[str] = None,
    initial_page: Optional[int] = None,
    initial_volume_ie: Optional[str] = None,
) -> None:
    """Render the /joins-lab page inside the existing create_layout() shell.

    Entry points (FND-03, FND-08, D-13):
      - ``?sys_id=<N>``      — deep-link to a known anchor (URL wins, D-13).
      - ``?shelfmark=<S>``   — cold-start by shelfmark (URL wins when sys_id absent).
      - bare ``/joins-lab``  — restore last anchor from safe_storage (D-13), or
                               show empty state when no anchor is stored.

    Phase 87 invariant: zero raw ``app.storage.user`` — all per-user state
    through ``web.joins_lab_storage`` helpers.
    """
    # Inject the manuscriptViewer JS/CSS at PAGE-BUILD time (initial render) so
    # the <script> actually executes. AnchorViewer is constructed dynamically on
    # "Load Anchor", and scripts injected into a live SPA page do not run — so
    # the viewer MUST be created here, before any anchor loads (zoom/pan fix).
    inject_viewer_assets()

    # -----------------------------------------------------------------------
    # Per-render transient state (mutable-dict containers — NOT safe_storage;
    # these are per-render closures, not per-user persisted values)
    # -----------------------------------------------------------------------
    _anchor_state: dict = {'sys_id': None, 'fl_id': None, 'volume_ie': None}
    _search_generation: dict = {'value': 0}
    _is_running: dict = {'value': False}
    _current_task: dict = {'task': None}  # in-flight asyncio.Task (for cancel)

    # One WebSearchExecutor per page render (used for SEARCH only —
    # NOT for anchor image data; AnchorViewer resolves its own rich BrowsePage,
    # HIGH-1).
    executor = WebSearchExecutor()

    # -----------------------------------------------------------------------
    # Direction-aware layout (D-01 / D-02)
    # -----------------------------------------------------------------------
    direction_class = 'flex-row-reverse' if is_rtl() else 'flex-row'

    # -----------------------------------------------------------------------
    # UI containers (defined up-front so callbacks close over them)
    # -----------------------------------------------------------------------
    page_container = ui.column().classes('w-full flex-grow min-h-0')

    with page_container:
        # Outer two-column flex (direction-aware)
        with ui.element('div').classes(f'flex {direction_class} w-full min-h-0 flex-grow').style(
            'align-items: flex-start;'
        ) as two_col:
            # -- Anchor pane (sticky, 380 px wide) --
            anchor_pane = ui.column().classes('gap-2').style(
                'width: 380px; flex-shrink: 0; position: sticky; top: 64px;'
                ' max-height: calc(100vh - 80px); overflow-y: auto;'
            )

            # -- Work column (flex: 1, min-width: 0) --
            # D-04: plain ui.column with NO hardcoded child structure so Phase
            # 118/119 can insert panels (known-joins, other-side builder, table)
            # without forcing a re-layout.
            work_column = ui.column().classes('gap-4 p-4').style(
                'flex: 1; min-width: 0;'
            )

    # -----------------------------------------------------------------------
    # Empty-state panel (shown until an anchor is loaded)
    # -----------------------------------------------------------------------
    with work_column:
        empty_state = ui.column().classes('items-center justify-center gap-4 py-12 w-full')
        with empty_state:
            ui.icon('join_inner').classes('text-4xl').style(
                'color: var(--primary-400);'
            )
            ui.label(tr('Pin an Anchor Fragment')).classes('text-xl font-semibold').style(
                'color: var(--text-primary);'
            )
            ui.label(
                tr('Enter a shelfmark or fragment ID to begin hunting for physical joins.')
            ).classes('text-sm text-center').style(
                'color: var(--text-secondary); max-width: 400px;'
            )

            # Smart box: accepts shelfmark or raw sys_id
            anchor_input = ui.input(
                placeholder=tr('Shelfmark or fragment ID (e.g. T-S 12.123)')
            ).classes('w-full').style('max-width: 400px;')

            error_label = ui.label('').classes('text-sm').style(
                'color: var(--error, #c62828); display: none;'
            )

            with ui.row().classes('gap-2'):
                load_btn = ui.button(tr('Load Anchor')).props('color=primary unelevated')

                # D-06 "Choose from my lists" — login-gated for anonymous web visitors.
                # See module-level NOTE for the full rationale.
                lists_btn = ui.button(tr('Choose from my lists')).props('flat').tooltip(
                    tr('Sign in to access your saved research lists')
                )

        # Builder + search area (hidden until anchor loaded)
        builder_area = ui.column().classes('w-full gap-3')
        builder_area.set_visibility(False)

        # Candidates section (below builder)
        candidates_container = ui.column().classes('w-full gap-2')

    # -----------------------------------------------------------------------
    # AnchorViewer placeholder (populated when anchor loads)
    # -----------------------------------------------------------------------
    with anchor_pane:
        anchor_viewer_container = ui.column().classes('w-full gap-2 anchor-viewer-container')

    # -----------------------------------------------------------------------
    # Narrow-screen collapsible anchor strip (D-03)
    # Below 768 px the sticky pane is hidden and this strip replaces it.
    # -----------------------------------------------------------------------
    anchor_chip_label = ui.label('').classes('text-sm font-mono').style(
        'color: var(--text-secondary);'
    )

    # -----------------------------------------------------------------------
    # Builder: textarea + Run Search button (BLD-01, D-08/D-09)
    # Built inside builder_area (populated after anchor load).
    # -----------------------------------------------------------------------
    with builder_area:
        # "Change anchor" — return to the cold-start picker to load a DIFFERENT
        # fragment (without this, the smart box is hidden once an anchor loads
        # and there is no way to switch). Full clear/reset is Phase 120.
        change_anchor_btn = ui.button(
            tr('Change anchor'), icon='swap_horiz'
        ).props('flat dense').classes('self-start')

        ui.label(tr('Search lines')).classes('text-sm font-semibold').style(
            'color: var(--text-secondary); letter-spacing: 0.05em; text-transform: uppercase;'
        )
        search_textarea = ui.textarea(
            placeholder=tr('Type manuscript lines, one per line')
        ).props(
            'aria-label="' + tr('Search lines -- one line per row') + '" rows=5 outlined'
        ).classes('w-full').style(
            'direction: rtl; text-align: right;'
            ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif;'
        )

        search_btn = ui.button(tr('Run Search')).props('color=primary unelevated icon=search')
        search_status = ui.label('').classes('text-sm').style(
            'color: var(--text-secondary); display: none;'
        )

    # -----------------------------------------------------------------------
    # Async helpers
    # -----------------------------------------------------------------------

    async def resolve_anchor_input(query: str) -> Optional[str]:
        """Resolve a shelfmark or sys_id string to a sys_id.

        Fast path: if query looks like a sys_id (all digits, starts with '99'),
        return it directly (mirrors browse.py:729).
        Otherwise call service.search_by_shelfmark off the event loop.
        Returns None when not found.
        """
        query = query.strip()
        if not query:
            return None
        # sys_id fast path
        if query.isdigit() and query.startswith('99'):
            return query
        # Shelfmark resolution (I/O-bound SQLite — off the event loop)
        results, _ = await run.io_bound(
            lambda: get_service().search_by_shelfmark(query, limit=20)
        )
        if results:
            return results[0].sys_id
        return None

    async def load_anchor(
        sys_id: str,
        fl_id: Optional[str] = None,
        page: Optional[int] = None,
        volume_ie: Optional[str] = None,
        show_restored_toast: bool = False,
    ) -> None:
        """Set sys_id as the current anchor, swap UI, and persist (D-13).

        Instantiates AnchorViewer inside anchor_viewer_container, awaits
        update_content() (it is async — new-HIGH; resolves the rich BrowsePage
        via service.get_browse_page() + resolve_external_images() off-loop).

        HIGH-1: AnchorViewer resolves its own rich BrowsePage via
        service.get_browse_page() — do NOT pass executor or image data here.
        """
        _anchor_state['sys_id'] = sys_id
        _anchor_state['fl_id'] = fl_id
        _anchor_state['volume_ie'] = volume_ie

        # Swap empty state → anchor + builder
        empty_state.set_visibility(False)
        builder_area.set_visibility(True)

        # Update anchor chip label (narrow screens)
        anchor_chip_label.set_text(sys_id)

        # Build AnchorViewer in the anchor pane
        anchor_viewer_container.clear()
        with anchor_viewer_container:
            viewer = AnchorViewer(
                sys_id=sys_id,
                fl_id=fl_id,
                p_num=page,
                volume_ie=volume_ie,
                # HIGH-1: NO executor= / browse-dict image arg — AnchorViewer
                # self-resolves the rich BrowsePage via service.get_browse_page()
            )
            # update_content() is async — must be awaited (new-HIGH; I/O runs
            # off the event loop via run.io_bound inside AnchorViewer)
            await viewer.update_content(p_num=page)

        # Persist the anchor (D-13) — only identity fields, no blobs
        write_anchor(sys_id, anchor_fl_id=fl_id, anchor_volume_ie=volume_ie)

        if show_restored_toast:
            ui.notify(tr('Restored your last anchor'), timeout=4000, type='info')

    async def _on_load_btn_click() -> None:
        """Handle the Load Anchor button / Enter key in the smart box."""
        query = anchor_input.value or ''
        if not query.strip():
            return
        error_label.style('display: none;')
        load_btn.props('loading=true disabled=true')
        try:
            resolved = await resolve_anchor_input(query)
            if resolved:
                await load_anchor(resolved)
            else:
                error_label.set_text(
                    tr('Fragment not found. Check the shelfmark and try again.')
                )
                error_label.style('display: block;')
        finally:
            load_btn.props(remove='loading disabled')

    def _on_lists_btn_click() -> None:
        """D-06: anonymous users see a login-prompt dialog.

        "My lists" routes ONLY to the per-user Supabase lists (login required).
        See the module-level NOTE for the full D-06 rationale.
        """
        from web.auth_state import GlobalAuthState  # local import to avoid cycles
        if GlobalAuthState.is_logged_in():
            # Logged-in path: open a list picker dialog.
            # Full list picker UI is Phase 120 scope — for the spine, show a
            # placeholder "coming soon" notice inside a dialog.
            with ui.dialog() as picker_dialog, ui.card().classes('p-4 min-w-[320px]'):
                ui.label(tr('Choose from my lists')).classes('text-lg font-semibold mb-2')
                ui.label(
                    tr('Full list picker is available in the next phase. '
                       'Go to /lists to pick a fragment, then return here.')
                ).classes('text-sm').style('color: var(--text-secondary);')
                ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).props('flat')
                ui.button(tr('Close'), on_click=picker_dialog.close).props('flat')
            picker_dialog.open()
        else:
            # Anonymous visitor — show login prompt (D-06 locked decision)
            with ui.dialog() as login_dialog, ui.card().classes('p-4 min-w-[320px]'):
                ui.label(tr('Sign in required')).classes('text-lg font-semibold mb-2')
                ui.label(
                    tr('Sign in to access your saved research lists.')
                ).classes('text-sm').style('color: var(--text-secondary);')
                ui.button(
                    tr('Sign in'),
                    on_click=lambda: ui.navigate.to('/settings')
                ).props('color=primary unelevated')
                ui.button(tr('Cancel'), on_click=login_dialog.close).props('flat')
            login_dialog.open()

    # Wire button handlers
    def _on_change_anchor() -> None:
        """Return to the cold-start picker so the user can load a DIFFERENT anchor.

        Re-shows the empty-state smart box and hides the builder/candidates.
        Does NOT clear the persisted anchor (D-13) — picking a new one overwrites
        it via load_anchor. Full clear/reset is Phase 120.
        """
        anchor_input.value = ''
        error_label.style('display: none;')
        anchor_viewer_container.clear()
        candidates_container.clear()
        search_textarea.value = ''
        builder_area.set_visibility(False)
        empty_state.set_visibility(True)

    load_btn.on('click', _on_load_btn_click)
    change_anchor_btn.on('click', _on_change_anchor)
    anchor_input.on('keydown.enter', _on_load_btn_click)
    lists_btn.on('click', _on_lists_btn_click)

    # -----------------------------------------------------------------------
    # Off-loop search with timeout + cancellation + latest-wins + stale-gen
    # (BLD-05, CND-01, HIGH-3, MEDIUM-4)
    # -----------------------------------------------------------------------

    async def execute_joins_search() -> None:
        """Run the Joins Lab search off the event loop.

        HIGH-3 implementation (all three SC#3 legs):
          1. TIMEOUT: asyncio.wait_for(..., timeout=_SEARCH_TIMEOUT_SECONDS)
          2. CANCELLATION / LATEST-WINS: prev task is cancelled before the new
             one starts; a new click supersedes in-flight search.
          3. STALE-GENERATION: _should_apply_results() discards a cancelled
             run's partial results AFTER the await returns normally (the core
             catches InterruptedError internally and returns partial results,
             not an exception — genizah_core.py:9000/:9071).

        MEDIUM cooperative worker cancel: the per-generation progress_cb raises
        InterruptedError when superseded, aborting the core scan loop early and
        freeing the run.io_bound worker THREAD (not just the asyncio wrapper).
        """
        # Step 1: Bump generation FIRST (latest-wins counter).
        _search_generation['value'] += 1
        my_gen = _search_generation['value']

        # Step 2: Cancel any in-flight search task (latest-wins).
        # NOTE (MEDIUM): prev.cancel() cancels the asyncio.wait_for wrapper
        # ONLY — it does NOT stop the already-running run.io_bound worker thread
        # (Python threads cannot be force-killed).  True worker cancellation is
        # achieved cooperatively via the bumped _search_generation making the OLD
        # search's progress_cb raise InterruptedError (see _make_progress_cb).
        prev = _current_task['task']
        if prev and not prev.done():
            prev.cancel()

        # Step 3: Validate inputs
        side = lines_to_side_query(search_textarea.value or '')
        if not side.rows:
            return
        if not state.is_ready():
            ui.notify(tr('Engine not ready.'), type='warning')
            return

        query_str, responsa_options, page_position = compose(side)
        if not query_str:
            return

        # Step 4: UI — loading state
        _is_running['value'] = True
        search_btn.props('loading=true disabled=true')
        candidates_container.clear()
        search_status.set_text(tr('Searching...'))
        search_status.style('display: block;')

        # Step 5: Build the sync closure (MEDIUM-4: execute_search appears ONLY
        # here, inside the synchronous function that is passed to run.io_bound).
        def run_search_core():
            return executor.execute_search(
                query_str,
                mode='exact',
                gap=0,
                progress_callback=_make_progress_cb(my_gen, _search_generation),
                responsa_options=responsa_options,
                text_position=page_position,
                corpus_scope='genizah',
            )

        # Step 6: TIMEOUT wrap + LATEST-WINS task reference.
        search_coro = run.io_bound(run_search_core)
        _current_task['task'] = asyncio.ensure_future(
            asyncio.wait_for(search_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
        )

        try:
            raw_results = await _current_task['task']

        except asyncio.TimeoutError:
            search_status.set_text(
                tr('Search timed out. Try fewer or shorter lines.')
            )
            return

        except asyncio.CancelledError:
            # The asyncio.wait_for wrapper was cancelled by a newer search click
            # (latest-wins path).  The newer run owns the UI — return quietly.
            # NOTE (MEDIUM): a COOPERATIVELY cancelled run does NOT surface as
            # CancelledError — the core catches InterruptedError internally and
            # returns PARTIAL results normally.  This branch handles ONLY the
            # asyncio-layer cancel from prev.cancel() above.
            return

        except Exception as exc:
            logger.exception('Joins Lab search error: %s', exc)
            search_status.set_text(
                tr('Search failed. Check your connection and try again.')
            )
            return

        finally:
            # Clean up the task reference if this is still the current task.
            if _current_task['task'] is not None and _current_task['task'].done():
                _current_task['task'] = None
            if _is_running['value'] and my_gen == _search_generation['value']:
                _is_running['value'] = False
                search_btn.props(remove='loading disabled')

        # Step 7: STALE-GENERATION discard — the PRIMARY guard for a
        # cooperatively-cancelled run (core returns partial results normally;
        # _should_apply_results is what prevents them reaching the UI).
        if not _should_apply_results(my_gen, _search_generation):
            return

        # Step 8: Dedup + render
        anchor_sid = _anchor_state.get('sys_id') or ''
        candidates, anchor_matched = dedup_candidates(raw_results, anchor_sid)

        candidates_container.clear()
        search_status.style('display: none;')
        with candidates_container:
            if candidates:
                create_candidate_grid(candidates)
            else:
                ui.label(tr('No candidates found. Try different lines.')).style(
                    'color: var(--text-secondary);'
                )

    search_btn.on('click', execute_joins_search)

    # -----------------------------------------------------------------------
    # Initial anchor resolution / restore (D-13: URL wins over storage)
    # -----------------------------------------------------------------------

    async def _bootstrap_anchor() -> None:
        """Resolve and load the initial anchor (deferred off the page handler)."""
        stored = read_anchor()
        decision = decide_initial_anchor(initial_sys_id, initial_shelfmark, stored)

        if decision is None:
            # Cold start — show the empty state (already visible by default)
            return

        if decision['source'] == 'url_sys_id':
            await load_anchor(
                decision['sys_id'],
                fl_id=initial_fl_id,
                page=initial_page,
                volume_ie=initial_volume_ie,
            )

        elif decision['source'] == 'url_shelfmark':
            # Shelfmark needs async resolution
            resolved = await resolve_anchor_input(decision['shelfmark'])
            if resolved:
                await load_anchor(
                    resolved,
                    fl_id=initial_fl_id,
                    page=initial_page,
                    volume_ie=initial_volume_ie,
                )
            # else: fall through to empty state (shelfmark not found)

        elif decision['source'] == 'stored':
            await load_anchor(
                decision['sys_id'],
                fl_id=decision.get('fl_id'),
                volume_ie=decision.get('volume_ie'),
                show_restored_toast=True,
            )

    # Defer the initial async resolution so it runs after the page handler
    # returns (NiceGUI-safe pattern — do NOT block the page handler).
    ui.timer(0.05, callback=_bootstrap_anchor, once=True)
