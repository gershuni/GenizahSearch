# -*- coding: utf-8 -*-
"""Joins Lab page — /joins-lab (Phase 117 spine + Phase 118 full builders).

Phase 118 extends the Phase-117 vertical spine with:
  - _merge_globals_web (BLD-04): re-injects flex_spacing + bidirectional into
    compose()-produced ``ro`` dicts on BOTH the anchor side and the other side.
  - create_joins_builder (BLD-03): replaces the Phase-117 textarea with the
    row-based line-builder widget (Text Position + Exact/Variants/Fuzzy mode).
  - Other-side builder (BLD-02): second create_joins_builder inside Advanced
    search options; drives apply_cross_side over the web page contract (p_num
    not internal_index, total_pages=0→None, multi-IE volume_ie) off the loop.
  - Known-joins group (ANC-04): fetch_connected_fragments(confirmed_only=True)
    dispatched via run.io_bound from load_anchor; renders via
    render_known_joins_group below the transcription in the sticky pane.
  - Auto-collapse summary bar (D-14): on non-empty Run Search, the builder
    collapses to a summary bar; Edit button re-expands it.

Requirements satisfied: FND-02, FND-03, FND-08, BLD-01..BLD-05, CND-01,
ANC-04, ANC-05.

SECURITY & MULTITENANT INVARIANTS
-----------------------------------
- Zero raw ``app.storage.user`` access — all per-user state through
  ``web.joins_lab_storage`` helpers (Phase 87 CI guard, allowlist ``[]``).
- All image fetches through existing per-provider proxy endpoints + Phase-98
  NLI circuit breaker — never a direct IIIF URL (ANC-02, D-11).
- Off-loop: ``executor.execute_search`` ONLY inside the sync
  ``run_search_core`` closure and ``apply_cross_side`` ONLY inside the sync
  ``run_cross_side_core`` closure, both dispatched via ``run.io_bound``
  (MEDIUM-4, SC#3; ``tests/test_joins_lab_off_loop.py`` enforces this
  statically).
- ``asyncio.wait_for`` timeout bounds each search (HIGH-3, T-117-12).
- Latest-wins cancellation: a new Run Search click cancels the in-flight
  asyncio.Task so rapid re-runs show only the latest result (HIGH-3).
- Stale-generation discard: ``_should_apply_results`` ensures a
  cooperatively-cancelled run's partial results never update the UI (HIGH-3).
- Cooperative worker cancellation: the per-generation ``_make_progress_cb``
  raises ``InterruptedError`` when superseded, aborting the core scan loop
  early (MEDIUM, mirroring search.py:4055-4058 and parallels.py:2143).

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
import csv
import io
import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from nicegui import run, ui

from shared.export_dossier import build_image_url_for_row
from shared.fjms_service import get_fjms_service
from shared.joins_lab import (
    BuilderRow, Candidate, SideQuery, apply_cross_side, compose, dedup_candidates,
    detect_self_match, merge_candidates,
)
from shared.visual_similarity_service import get_vs_service
from web.auth_state import GlobalAuthState, create_login_dialog
from web.components.anchor_viewer import AnchorViewer, inject_viewer_assets
from web.components.candidate_grid import (
    compute_filtered, create_candidate_grid, create_candidate_table, open_filter_dialog,
)
from web.components.compare_modal import create_compare_modal
from web.components.joins_builder import create_joins_builder
from web.components.joins_panel import fetch_connected_fragments
from web.components.known_joins_group import render_known_joins_group
from web.joins_executor import WebSearchExecutor
from web.joins_lab_storage import (
    clear_joins_lab_state,
    read_anchor,
    read_full_state,
    write_anchor,
    write_full_state,
)
from web.safe_storage import safe_user_pop, safe_user_set
from web.services import get_service
from web.state import state
from web.supabase_client import (
    add_list_item, create_fragment_join, delete_fragment_join,
    get_list_item_counts, get_list_items, get_user_lists,
)
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

_PENDING_ACTION_TTL_SECONDS = 900  # 15 minutes — pending Add-as-Join expires after this
"""TTL for the 'joins_lab_pending' add-as-join descriptor stored before login.

Phase-120 R2-M2: descriptors older than this are cleared without replaying
(prevents a stale descriptor from executing against a different anchor).
"""

_EXPORT_CANDIDATE_CAP = 500
"""Maximum number of candidates exported per D-06 (matches SEARCH_API_FUZZY_MAX_LIMIT).

Aligns with the search API's fuzzy result-cap ceiling so a researcher can always
export the full visible candidate pool in one pass.
"""

_EXPORT_TEXT_CAP = 4000
"""Per-cell transcription text cap in characters (D-06 / SEARCH_API_BROWSE_TEXT_CAP).

Prevents single-cell text from bloating the export file.  4000 chars ≈ one page
of dense manuscript transcription.
"""

_EXPORT_BATCH_SIZE = 10
"""Number of candidates fetched per off-loop batch during export text fetch (D-06).

Small batches keep the per-batch I/O latency manageable and give the cancel check
a chance to fire between batches.
"""

_PREFETCH_SLOTS = 5
"""Max concurrent image prefetch tasks (D-10/M3).

Mirrors desktop join_workbench.py _pump_images 5-slot bounded pool.
"""


# ---------------------------------------------------------------------------
# Pure module-level helpers (importable without NiceGUI — tested headlessly)
# ---------------------------------------------------------------------------


def _merge_globals_web(ro: dict, global_opts: dict) -> dict:
    """Re-inject flex_spacing + bidirectional into a compose()-produced ro.

    compose() at shared/joins_lab.py:741-749 hardcodes ja/flex_spacing/
    bidirectional=False. This step pulls the actual UI-toggle state back in
    (BLD-04 / D-11 / RR-14 parity with desktop/join_workbench.py:2475-2489).

    ja intentionally excluded — stays False per D-10 (user decision to drop JA).
    variants flows via SideQuery.variants and is NOT touched here.

    Applied to BOTH the anchor ro (after compose(anchor_side)) and the other-side
    b_ro (inside run_cross_side_core after compose(other_side)). Mirrors the
    desktop pattern: desktop/join_workbench.py:2519 (anchor) and :2580 (other-side).
    """
    ro['flex_spacing'] = global_opts.get('flex_spacing', False)
    ro['bidirectional'] = global_opts.get('bidirectional', False)
    return ro


def _coerce_combine_mode(v) -> str:
    """Normalize a Combine-mode value to 'AND' or 'OR'.

    Same Quasar dict-options ``q-select`` hazard as Text Position: the raw
    ``update:model-value`` payload is the option OBJECT ``{'label','value'}``, a
    dict, not the bare key. A dict here silently mis-routes ``apply_cross_side``
    (neither 'AND' nor 'OR'). Coerce defensively; unknown -> 'AND' (narrow default).
    """
    if isinstance(v, dict):
        v = v.get('value', 'AND')
    return v if v in ('AND', 'OR') else 'AND'


def build_collapsed_summary(
    anchor_summary: str,
    other_enabled: bool,
    other_summary: Optional[str],
    combine_mode: str,
) -> str:
    """Compose the collapsed summary-bar text (D-14), incl. the other side.

    When the other-side search is enabled AND has content, append a compact
    other-side segment (combine mode + the other-side builder's own summary) so
    the collapsed bar reflects the FULL two-sided query, not just the anchor side.
    All literals go through tr() (bilingual; the default UI is Hebrew).
    """
    if not other_enabled or not other_summary:
        return anchor_summary
    combine_label = (
        tr('Narrow (AND)') if _coerce_combine_mode(combine_mode) == 'AND'
        else tr('Widen (OR)')
    )
    other_seg = tr('Other side') + ': ' + combine_label + ' · ' + other_summary
    return anchor_summary + '   ⇄   ' + other_seg


def build_collapsed_query_text(
    anchor_query: Optional[str],
    other_query: Optional[str] = None,
) -> str:
    """Build the collapsed-bar text from the ACTUAL composed responsa query strings.

    Shows what the engine will run — the anchor side first, then the other side
    after ' || ' when present, each wrapped in quotes. e.g.::

        "אמר [|3] %רבי" || "*עקיבא"

    The query strings come straight from shared.joins_lab.compose(), so the bar
    reflects the real line-break / gap / modifier syntax (not a paraphrase).
    """
    parts = []
    if anchor_query:
        parts.append(f'"{anchor_query}"')
    if other_query:
        parts.append(f'"{other_query}"')
    return ' || '.join(parts)


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

    Kept for import-compat with tests/test_joins_lab_page.py that may import it.
    The Phase-118 builder replaces its usage in execute_joins_search.
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


def _make_progress_cb(my_gen: int, gen_ref: dict, stop_ref: Optional[dict] = None):
    """Return a progress callback that cooperatively cancels superseded searches.

    The returned ``progress_cb(arg1, arg2=None)`` is called by the search core
    on every chunk.  When the page has started a newer search (``_search_generation``
    bumped above ``my_gen``), the callback raises ``InterruptedError`` — which the
    core CATCHES internally (genizah_core.py:9000 ``except InterruptedError:
    was_interrupted=True``), aborts the scan loop early (frees the run.io_bound
    worker thread), and returns the partial deduped results gathered so far
    (genizah_core.py:9005/:9071).  The caller's stale-generation guard
    (``_should_apply_results``) then discards those partial results.

    D-11 Stop-with-partials: when ``stop_ref`` is provided and its ``value`` is
    True, raises ``InterruptedError('joins-lab search stopped by user')`` BEFORE
    the generation check.  Because the generation is NOT bumped on a user Stop,
    ``_should_apply_results(my_gen, gen_ref)`` returns True after the await —
    so the partial results ARE applied to the UI.

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
        # D-11 STOP-WITH-PARTIALS: check the user Stop flag FIRST, before the
        # generation check.  stop_ref is NOT bumped on user Stop, so
        # _should_apply_results(my_gen, gen_ref) will still return True →
        # partial results are applied (not discarded) after the InterruptedError.
        if stop_ref is not None and stop_ref['value']:
            raise InterruptedError('joins-lab search stopped by user')
        # COOPERATIVE CANCEL CHECK (MEDIUM): fires second — if a newer search
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
# Phase 119 — VS adapter, enrichment batch, conditional merge (pure + off-loop)
# ---------------------------------------------------------------------------


def _map_vs_suggestions_to_candidates(raw: list) -> list:
    """Map raw get_suggestions output to Candidate objects (D-05, VSM-01).

    Each ``{'alma_id': str, 'svm_score': float, 'rank': int}`` dict is the PARTNER
    sys_id (alma_id_b from the DB query ``SELECT alma_id_b WHERE alma_id_a = ?``).
    The partner IS the candidate — Candidate(sys_id=alma_id).

    Critical field mapping (Pitfall 4 — do NOT transpose):
      svm_score → vs_score  (float 0–1, NOT rank)
      rank      → vs_rank   (int 1-indexed, NOT svm_score)

    Returns a list of Candidate objects with via_vs=True.
    Module-level and pure so tests can import directly.
    """
    candidates = []
    for r in raw:
        c = Candidate(
            sys_id=r['alma_id'],
            page=None,                        # VS-only: no specific folio page
            uid=f"{r['alma_id']}|vs",
            via_vs=True,
            vs_rank=r['rank'],
            vs_score=r['svm_score'],          # NOT swapped (Pitfall 4)
        )
        candidates.append(c)
    return candidates


def _apply_vs_merge(
    text_candidates: list,
    vs_candidates: list,
    vs_on: bool,
    builder_has_query: bool,
) -> list:
    """Apply the D-04 conditional VS merge model.

    Conditional model (desktop parity join_workbench.py:2788-2802):
      ON + builder has query  → INTERSECTION: keep only (via_text AND via_vs)
      ON + empty builder      → UNION: pure VS browse = merge_candidates([], vs)
      OFF                     → text-only; tier0+tier1; VS-only (tier2) excluded
                                BUT look-alikes among text hits keep via_vs=True badge

    Pure function — headlessly testable.
    """
    if vs_on:
        if builder_has_query:
            # INTERSECTION: tier0 candidates have both via_text AND via_vs after merge
            merged = merge_candidates(text_candidates, vs_candidates)
            return [c for c in merged if c.via_text and c.via_vs]
        else:
            # UNION: pure VS browse — merge_candidates([], vs) gives tier2 only
            return merge_candidates([], vs_candidates)
    else:
        # OFF: text-only but look-alikes among text hits carry 👁 badge
        # merge_candidates annotates shared sys_ids with via_vs=True;
        # filter keeps via_text (tier0 + tier1), excludes VS-only (tier2).
        merged = merge_candidates(text_candidates, vs_candidates)
        return [c for c in merged if c.via_text]


def _get_enrichment_sys_ids(candidates: list) -> list:
    """Extract unique sys_ids from a candidate list for the enrichment batch.

    Covers the FULL filtered set (not just the current page) so material/dims
    filter predicates evaluate correctly for candidates on later pages (D-16).
    Module-level and pure so tests can import directly.
    """
    seen = set()
    result = []
    for c in candidates:
        sid = c.sys_id
        if sid and sid not in seen:
            seen.add(sid)
            result.append(sid)
    return result


def _check_vs_service_available() -> bool:
    """F-VSavail: probe VS availability OFF the event loop (sync, blocking I/O).

    get_vs_service(thread_safe=True).__init__ opens a LOCAL SQLite connection.
    This function is meant to be dispatched via run.io_bound — never called
    directly on the NiceGUI event loop. Returns True when the DB is available.
    """
    try:
        return get_vs_service(thread_safe=True).is_available()
    except Exception:
        return False


async def _fetch_vs_candidates(anchor_sid: str) -> list:
    """Fetch VS look-alikes for *anchor_sid* off the event loop (D-05, VSM-01).

    Dispatches via run.io_bound — LOCAL visual_similarity.db SQLite read.
    No NLI circuit breaker (only thumbnail image fetches need the breaker).

    CI guard (tests/test_joins_lab_off_loop.py): the sync closure named EXACTLY
    ``run_vs_core`` MUST be the first positional arg to run.io_bound.

    Returns [] when the VS service is unavailable or on any exception (graceful).
    """
    def run_vs_core():
        vs_svc = get_vs_service(thread_safe=True)
        if not vs_svc.is_available():
            return []
        return vs_svc.get_suggestions(anchor_sid, 200)

    try:
        raw = await run.io_bound(run_vs_core)
        return _map_vs_suggestions_to_candidates(raw or [])
    except Exception:
        logger.debug('VS lookup failed for anchor %s', anchor_sid, exc_info=True)
        return []


async def _enrich_candidates(sys_ids: list) -> dict:
    """Batch-enrich candidates with material/dimensions data off the event loop (D-16).

    Dispatches via run.io_bound — LOCAL fjms_enrichment.db SQLite read.
    No NLI circuit breaker (only thumbnail image fetches need the breaker).

    CI guard (tests/test_joins_lab_off_loop.py): the sync closure named EXACTLY
    ``run_enrich_core`` MUST be the first positional arg to run.io_bound.

    Returns {} for empty input or when the FJMS service is unavailable (graceful).
    """
    if not sys_ids:
        return {}

    def run_enrich_core():
        fjms_svc = get_fjms_service(thread_safe=True)
        if not fjms_svc.is_available():
            return {}
        return fjms_svc.get_measurement_summaries_batch(sys_ids)

    try:
        return await run.io_bound(run_enrich_core)
    except Exception:
        logger.debug('Enrichment batch failed', exc_info=True)
        return {}


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
    _anchor_state: dict = {
        'sys_id': None, 'fl_id': None, 'volume_ie': None,
        'page': None,   # A1: anchor's resolved folio (set in load_anchor)
        'shelfmark': '', # A1: anchor's resolved shelfmark (set in load_anchor)
    }
    _search_generation: dict = {'value': 0}
    # Bumped on every anchor swap so a slow known-joins fetch for a prior anchor
    # cannot render under a newer one (MED, CR fire-and-forget guard).
    _anchor_generation: dict = {'value': 0}
    _is_running: dict = {'value': False}
    _current_task: dict = {'task': None}  # in-flight asyncio.Task (for cancel)
    # D-11 Stop-with-partials: set True on explicit Stop click (does NOT bump
    # _search_generation so _should_apply_results returns True → partials applied).
    # Reset to False at the start of every new search.
    _stop_requested: dict = {'value': False}

    # Global options toggles — shared between both sides (D-11)
    # Mutable dict so closures see current values.
    _global_opts: dict = {'flex_spacing': False, 'bidirectional': False}

    # Other-side state (D-13, UI-SPEC §3)
    _other_side: dict = {'enabled': False, 'builder': None, 'combine': 'AND'}

    # -----------------------------------------------------------------------
    # Phase 119 candidate surface state (in-memory page state — NOT safe_storage)
    # MUST NEVER be written to app.storage.user (Phase-87 invariant).
    # -----------------------------------------------------------------------
    _triage: dict = {}            # sys_id → 'yes'|'maybe'|'no' (D-11)
    _selected: set = set()        # table multi-select sys_ids
    _filter_state: dict = {       # filter dialog state
        'materials': [],
        'has_dims': False,
        'exclude_mismatch': False,
        'triage_states': [],
        'text_q': '',
    }
    _enrichment: dict = {}        # sys_id → {width_cm, height_cm, material, ...} (D-16)
    _enrichment_ready: dict = {'value': False}
    _view_mode: dict = {'value': 'grid'}      # 'grid' | 'table'
    _current_page: dict = {'value': 0}        # 0-indexed page for pagination
    _raw_text_candidates: list = []           # G2: RAW text+cross-side baseline BEFORE any VS merge
    _all_candidates: list = []                # current DISPLAY set (post-merge, derived)
    _filtered_candidates: list = []           # after compute_filtered

    # VS toggle state (D-04/D-06) — MUST NEVER be written to storage
    _vs_on: dict = {'value': False}           # 👁 toggle ON/OFF
    _vs_candidates: list = []                 # cached VS look-alike Candidate objects
    _vs_anchor_sid: dict = {'value': None}    # anchor sid used for _vs_candidates
    _vs_loading: dict = {'value': False}      # True while VS fetch in-flight
    _vs_available: dict = {'checked': False, 'available': True}  # F-VSavail: probed off-loop

    # Late-bound UI refs (populated after UI is built)
    _vs_switch_ref: dict = {'el': None}       # the ui.switch element
    _vs_status_ref: dict = {'el': None}       # inline VS status label
    # Phase 120-03 PST-02: restoring indicator (shown during auto-restore re-run)
    _restore_indicator_ref: dict = {'el': None}

    # Enter-to-search: the builders are created before execute_joins_search is
    # defined, so wire the word-box Enter key through a mutable ref set later.
    _submit_ref: dict = {'fn': None}

    # Phase-120 ACT-03: Export state (D-06).
    # _export_ref: late-bound ref to _export_candidates coroutine (set after definition).
    # _export_cancel_ref: set True to cancel an in-flight export text-fetch loop.
    # _export_progress_ref: reference to the inline progress card element.
    _export_ref: dict = {'fn': None}
    _export_cancel_ref: dict = {'value': False}
    _export_progress_ref: dict = {'el': None}

    # Phase-120 D-10: Compare image prefetch — silent bounded 5-slot pool.
    # _prefetch_cache: {sys_id → img_url} — resolved proxy URL per candidate.
    # _prefetch_running: {sys_id} — currently in-flight prefetch tasks.
    # _prefetch_anchor_gen: generation token — bumped on re-anchor so stale
    #   prefetch results (for the old anchor's candidate set) are discarded.
    _prefetch_cache: dict = {}
    _prefetch_running: set = set()
    _prefetch_anchor_gen: dict = {'value': 0}

    async def _trigger_search() -> None:
        if _submit_ref['fn'] is not None:
            await _submit_ref['fn']()

    def _cancel_current_search() -> None:
        """Supersede any in-flight search (CR HIGH-1).

        Bumps the generation counter (so a still-running run's results are
        discarded by ``_should_apply_results``), cancels the in-flight task, and
        clears the loading affordance. Called from New Search, Change anchor, and
        every anchor swap so a search started for a PRIOR builder/anchor state can
        never repopulate the candidate grid after a reset. ``search_btn`` is
        late-bound — this only ever runs after the full page is built.
        """
        _search_generation['value'] += 1
        task = _current_task.get('task')
        if task is not None and not task.done():
            task.cancel()
        _current_task['task'] = None
        if _is_running['value']:
            _is_running['value'] = False
            # D-11: also restore the Stop/Run Search swap if Stop was showing.
            stop_btn.set_visibility(False)
            stop_btn.props(remove='loading disabled')
            stop_btn.set_text(tr('Stop'))
            search_btn.set_visibility(True)
            _stop_requested['value'] = False

    # -----------------------------------------------------------------------
    # Phase 119: Candidate surface helpers (close over page state)
    # All state is in-memory — NEVER written to app.storage.user (Phase-87).
    # -----------------------------------------------------------------------

    def _on_triage_verdict(sys_id: str, verdict: str) -> None:
        """Record a verdict into the shared triage dict + restyle (D-11).

        Keyed by sys_id (D-11); called from grid card buttons, table triage
        column, bulk-triage bar, and Compare verdict callback.
        """
        _triage[sys_id] = verdict
        # Since TriageState._data IS _triage (CR-01 backing dict), ts.set() is
        # redundant but kept for safety (validates the verdict string).
        ts = _triage_state_ref.get('obj')
        if ts is not None:
            try:
                ts.set(sys_id, verdict)
            except Exception:
                pass
        # WR-01: call render-scoped restyle so card borders update immediately.
        rf = _triage_state_ref.get('restyle')
        if rf is not None:
            try:
                rf(sys_id, _triage)
            except Exception:
                pass
        # PST-01: persist triage change (on event loop — not inside run.io_bound)
        _persist_state()

    def _on_compare_verdict(sys_id: str, verdict: str) -> None:
        """Compare verdict callback — writes shared triage + restyle (D-03, WR-01)."""
        _triage[sys_id] = verdict
        ts = _triage_state_ref.get('obj')
        if ts is not None:
            try:
                ts.set(sys_id, verdict)
            except Exception:
                pass
        # WR-01: restyle the card grid so the verdict border updates behind the modal.
        rf = _triage_state_ref.get('restyle')
        if rf is not None:
            try:
                rf(sys_id, _triage)
            except Exception:
                pass
        # PST-01: persist triage change
        _persist_state()

    # Late-bound reference to the TriageState object + render-scoped restyle fn.
    # _restyle_fn is set inside _render_candidates_surface to the closure returned
    # by _make_restyle_fn — it captures the per-render card_refs dict (CR-04).
    _triage_state_ref: dict = {'obj': None, 'restyle': None}

    def _compute_display_candidates() -> list:
        """G2: Single source of truth for the current display candidate list.

        Recomputes the display list from the RAW text baseline (_raw_text_candidates)
        and the current VS state every call. Never merges an already-merged set —
        so toggling VS on/off (and filter/page changes) genuinely changes the candidate
        set rather than re-filtering a pre-merged snapshot.

        Returns _apply_vs_merge(_raw_text_candidates, _vs_candidates, _vs_on, builder_has_query).
        """
        builder_has_query = not anchor_builder['is_empty']()
        return _apply_vs_merge(
            _raw_text_candidates,
            _vs_candidates,
            vs_on=_vs_on['value'],
            builder_has_query=builder_has_query,
        )

    def _on_set_as_anchor(sys_id: str) -> None:
        """D-07: re-anchor the workbench to the chosen candidate (no confirm dialog).

        Calls the existing load_anchor() re-anchor flow (which clears _triage per
        Phase-119 D-11). Dispatched as fire-and-forget via asyncio.ensure_future
        so it can be used as a sync callback from the candidate grid.
        """
        asyncio.ensure_future(load_anchor(sys_id))

    def _metadata_prefetcher_sync(sys_id: str) -> dict:
        """D-09/H3: Sync fetcher for per-pane Compare metadata (catalog + bibliography).

        Called via run.io_bound by compare_modal's _on_show show-handler so it
        executes off the event loop.  Returns a dict with:
            'fjms_bib'       — list[dict] from get_bibliography(sys_id)
            'catalog_detail' — dict|None from get_catalog_detail(sys_id)
        """
        svc = get_fjms_service(thread_safe=True)
        try:
            fjms_bib = svc.get_bibliography(sys_id)
        except Exception:
            fjms_bib = []
        try:
            catalog_detail = svc.get_catalog_detail(sys_id)
        except Exception:
            catalog_detail = None
        return {'fjms_bib': fjms_bib, 'catalog_detail': catalog_detail}

    def _prefetch_image_sync(sys_id: str) -> Optional[str]:
        """D-10/M3: Sync image resolver for off-loop prefetch via run.io_bound.

        Uses the RICH resolver path AnchorViewer uses:
          service.get_browse_page() + resolve_external_images() + resolve_image_url()
        Returns the resolved proxy img_url string, or None on failure.

        DO NOT use executor.get_browse_page() — that narrow text dict does NOT
        enrich images (joins_executor.py:87-94, M3 note).
        All image traffic routes through proxy + Phase-98 NLI circuit breaker.
        """
        from web.components.image_resolution import resolve_external_images, resolve_image_url
        try:
            svc = get_service()
            page = svc.get_browse_page(sys_id, p_num=1)
            if page is None:
                return None
            ext = resolve_external_images(page.sys_id)
            cambridge_images = ext.get('cambridge_images') or page.cambridge_images
            external_provider = ext.get('external_provider') or page.external_provider
            cambridge_alignment = (
                ext['cambridge_alignment']
                if ext.get('cambridge_alignment') is not None
                else page.cambridge_alignment
            )
            resolved = resolve_image_url(
                sys_id=page.sys_id,
                p_num=page.p_num,
                is_oxford=page.is_oxford,
                shelfmark=page.shelfmark,
                volume_suffix=page.volume_suffix,
                cambridge_images=cambridge_images,
                external_provider=external_provider,
                cambridge_alignment=cambridge_alignment,
                volumes=page.volumes,
                total_pages=page.total_pages,
            )
            return resolved.get('img_url') if isinstance(resolved, dict) else None
        except Exception:
            return None

    async def _prefetch_one(sys_id: str, my_gen: int) -> None:
        """D-10: Fire-and-forget prefetch coroutine for a single candidate image.

        SEED-008 guarded (entire body) + generation-guarded (stale results
        from before a re-anchor are discarded).
        """
        try:
            if _prefetch_anchor_gen['value'] != my_gen:
                return
            img_url = await run.io_bound(_prefetch_image_sync, sys_id)
            # Generation re-check after the await (may have changed during I/O)
            if _prefetch_anchor_gen['value'] != my_gen:
                return
            if img_url:
                _prefetch_cache[sys_id] = img_url
        except RuntimeError:
            return  # SEED-008: NiceGUI client disconnected
        finally:
            _prefetch_running.discard(sys_id)

    def _schedule_image_prefetch(center_idx: int) -> None:
        """D-10: Schedule prefetch of adjacent candidates around center_idx.

        Fires fire-and-forget coroutines for offsets -2,-1,+1,+2 relative
        to center_idx, bounded to _PREFETCH_SLOTS concurrent tasks.
        Already-cached or in-flight candidates are skipped.
        """
        candidates = _filtered_candidates
        if not candidates:
            return
        gen = _prefetch_anchor_gen['value']
        for offset in (-2, -1, 1, 2):
            idx = center_idx + offset
            if idx < 0 or idx >= len(candidates):
                continue
            cand = candidates[idx]
            sid = cand.sys_id
            if sid in _prefetch_cache or sid in _prefetch_running:
                continue
            if len(_prefetch_running) >= _PREFETCH_SLOTS:
                break
            _prefetch_running.add(sid)
            asyncio.ensure_future(_prefetch_one(sid, gen))

    def _open_compare(cand) -> None:
        """Open the Compare modal for a clicked candidate (F2, D-02).

        Receives the FULL candidate object (not sys_id alone — same sys_id can
        appear on multiple folios, Candidate.key == (sys_id, page), Pitfall 6).
        Passes the full candidate list (filtered) so flip-through works correctly.

        D-10: schedules image prefetch for adjacent candidates on open, and wires
        on_candidate_change so flips also trigger prefetch.
        """
        anchor_sid = _anchor_state.get('sys_id') or ''
        anchor_fl_id = _anchor_state.get('fl_id')
        anchor_vol = _anchor_state.get('volume_ie')
        # A1: use the RESOLVED anchor page/shelfmark stored during load_anchor.
        # Fall back to 1 / '' only when load_anchor hasn't resolved them yet.
        anchor_page_num = _anchor_state.get('page') or 1
        anchor_shelfmark = _anchor_state.get('shelfmark') or ''
        # CR-03: Candidate has no fl_id field — it's an anchor-pane concept stored
        # in _anchor_state. The AnchorViewer inside create_compare_modal resolves
        # the folio independently from sys_id + page. Remove the spurious kwarg.
        anchor_cand = Candidate(
            sys_id=anchor_sid,
            page=anchor_page_num,
            shelfmark=anchor_shelfmark,
            uid=f'{anchor_sid}|anchor',
            volume_ie=anchor_vol,
            is_anchor_self=True,
        )
        # D-10: schedule prefetch of adjacent candidates for the initial candidate.
        try:
            initial_idx = list(_filtered_candidates).index(cand)
        except (ValueError, AttributeError):
            initial_idx = 0
        _schedule_image_prefetch(initial_idx)

        modal = create_compare_modal(
            anchor_cand=anchor_cand,
            initial_candidate=cand,
            filtered_candidates=list(_filtered_candidates),
            triage=_triage,
            on_verdict=_on_compare_verdict,
            enrichment=_enrichment,
            metadata_prefetcher=_metadata_prefetcher_sync,
            on_candidate_change=_schedule_image_prefetch,
        )
        modal.open()

    async def _do_enrich_and_update(candidates_snap: list) -> None:
        """Fetch enrichment off-loop for all filtered candidates, then re-render.

        Fires after Step-9 renders the surface (Pitfall 7 — enrichment is async;
        material/dims columns populate once this completes). Updates _enrichment +
        _enrichment_ready and refreshes the current surface with the new data.

        A3: includes the anchor sys_id in the batch so _enrichment[anchor_sid]
        is populated and is_size_mismatch() has the anchor's width/height.

        SEED-008 (D-20): the WHOLE body is wrapped in ``try/except RuntimeError``
        so a client-deleted teardown does not propagate out of the task.
        """
        try:
            if not candidates_snap:
                return
            sys_ids = list(_get_enrichment_sys_ids(candidates_snap))
            # A3: include anchor sys_id in the enrichment batch (deduped)
            anchor_sid = _anchor_state.get('sys_id')
            if anchor_sid and anchor_sid not in sys_ids:
                sys_ids.append(anchor_sid)
            enrichment = await _enrich_candidates(sys_ids)
            # Store in the page-level dict (shared with filter dialog + table cells)
            _enrichment.clear()
            _enrichment.update(enrichment)
            _enrichment_ready['value'] = True
            # Re-render the surface so material/dims populate (same render path)
            _re_render_candidates_surface()
        except RuntimeError:
            return  # client/tab deleted mid-fetch — benign teardown (SEED-008 D-20)

    def _re_render_candidates_surface() -> None:
        """Re-render the candidate grid/table into candidates_container in-place.

        Called after enrichment completes (Pitfall 7) or filter/page change.
        Preserves _triage and _current_page across re-renders.

        G2 fix: recomputes the display list from the RAW baseline via
        _compute_display_candidates() before passing to compute_filtered — so
        toggling VS on/off, filtering, and pagination all reflect the correct
        intersection/union/text-only state.
        """
        anchor_sid = _anchor_state.get('sys_id') or ''
        # G2: recompute display from RAW baseline first, THEN compute_filtered
        display = _compute_display_candidates()
        _all_candidates.clear()
        _all_candidates.extend(display)
        filtered = compute_filtered(
            _all_candidates, _filter_state, _enrichment, _triage, anchor_sid
        )
        _filtered_candidates.clear()
        _filtered_candidates.extend(filtered)

        candidates_container.clear()
        with candidates_container:
            _render_candidates_surface()

    def _render_candidates_surface() -> None:
        """Render the candidate grid/table for the current page into candidates_container.

        Uses compute_filtered result already stored in _filtered_candidates.
        Always called inside a `with candidates_container:` block.

        A2 fix: branches on _view_mode['value'] — calls create_candidate_table when
        'table', create_candidate_grid when 'grid'. Both share the same _triage dict
        and _open_compare handler (D-10: triage is never reset on view switch).
        """
        if not _filtered_candidates:
            ui.label(tr('No candidates found. Try different lines.')).style(
                'color: var(--text-secondary);'
            )
            return

        # Build the TriageState wrapper (shared with grid/table/Compare).
        # CR-01: pass _triage as the backing dict so both share the SAME object —
        # verdicts set via either _triage[sid] or triage_obj.set(sid, v) are
        # instantly visible to the other path (fixes WR-01/WR-02 at the source).
        from web.components.candidate_grid import TriageState
        triage_obj = TriageState(backing=_triage)
        _triage_state_ref['obj'] = triage_obj

        def _on_restyle_ready(restyle_fn) -> None:
            """Store the render-scoped restyle fn so Compare verdicts can update cards (WR-01)."""
            _triage_state_ref['restyle'] = restyle_fn

        sort_mode = 'vs_rank' if _vs_on['value'] else 'score'

        # A2: branch on _view_mode to reach the table (not just the grid)
        if _view_mode['value'] == 'table':
            # Phase-120 H2: wire table selection to the page _selected set so
            # SELECTION-scoped bulk actions (Add-to-Puzzle / Add-to-List) see it.
            def _on_table_selection_change(sids: list) -> None:
                _selected.clear()
                _selected.update(sids)

            create_candidate_table(
                _filtered_candidates,
                triage=_triage,
                enrichment=_enrichment,
                sort_mode=sort_mode,
                on_compare=_open_compare,
                restyle_fn=_triage_state_ref.get('restyle'),
                on_selection_change=_on_table_selection_change,
                on_add_to_puzzle=_on_add_to_puzzle_click,
                on_add_to_list=_on_add_to_list_click,
            )
        else:
            # Render the Phase-02 grid surface
            create_candidate_grid(
                _filtered_candidates,
                triage=_triage,
                page=_current_page['value'],
                on_compare=_open_compare,
                enrichment=_enrichment,
                enrichment_ready=_enrichment_ready['value'],
                filter_state=_filter_state,
                anchor_sys_id=_anchor_state.get('sys_id') or '',
                on_page_change=_on_page_change,
                on_filter_open=_on_filter_open,
                on_restyle_ready=_on_restyle_ready,
                on_set_as_anchor=_on_set_as_anchor,
            )

    def _on_page_change(page: int) -> None:
        """Page nav callback — changes page WITHOUT resetting triage (D-08)."""
        _current_page['value'] = page
        _re_render_candidates_surface()

    def _on_filter_open() -> None:
        """Open the filter dialog (D-14) with enrichment-ready gate (Pitfall 7)."""
        # CR-02: open_filter_dialog signature is (filter_state, enrichment,
        # enrichment_ready, on_apply, on_reset).  The old call passed
        # candidates=/anchor_sys_id= (unknown kwargs) and omitted on_reset.
        # The on_apply contract is no-arg (filter_state is mutated in place by
        # the dialog's _do_apply before calling on_apply()).

        def _on_filter_apply() -> None:
            # filter_state was already mutated in place by the dialog.
            _current_page['value'] = 0  # reset to page 0 on filter change
            _re_render_candidates_surface()
            # PST-01: persist filter change (on event loop; filter dialog runs synchronously)
            _persist_state()

        def _on_filter_reset() -> None:
            # filter_state was already reset to defaults by the dialog.
            _current_page['value'] = 0
            _re_render_candidates_surface()
            # PST-01: persist filter reset
            _persist_state()

        open_filter_dialog(
            filter_state=_filter_state,
            enrichment=_enrichment,
            enrichment_ready=_enrichment_ready['value'],
            on_apply=_on_filter_apply,
            on_reset=_on_filter_reset,
        )

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

        # Phase-120 ACT-03 D-06: Export progress indicator (hidden by default).
        # Shown below the toolbar while the text-fetch batch runs; hidden on complete/cancel.
        # Must be created here so the element exists when the export handler runs.
        export_progress_container = ui.column().classes('w-full gap-2')
        export_progress_container.set_visibility(False)
        _export_progress_ref['el'] = export_progress_container

        # Candidates section (below builder)
        candidates_container = ui.column().classes('w-full gap-2')

    # -----------------------------------------------------------------------
    # AnchorViewer placeholder + Known-Joins container (populated when anchor loads)
    # -----------------------------------------------------------------------
    with anchor_pane:
        anchor_viewer_container = ui.column().classes('w-full gap-2 anchor-viewer-container')
        # Known-joins group (ANC-04) — below the transcription in the sticky pane
        # (UI-SPEC §4)
        known_joins_container = ui.column().classes('w-full')

    # -----------------------------------------------------------------------
    # Narrow-screen collapsible anchor strip (D-03)
    # Below 768 px the sticky pane is hidden and this strip replaces it.
    # -----------------------------------------------------------------------
    anchor_chip_label = ui.label('').classes('text-sm font-mono').style(
        'color: var(--text-secondary);'
    )

    # -----------------------------------------------------------------------
    # Builder: row-based builder (BLD-03) + Advanced options + Run Search
    # Built inside builder_area (populated after anchor load).
    # -----------------------------------------------------------------------
    with builder_area:
        # "Change anchor" — return to the cold-start picker to load a DIFFERENT
        # fragment (without this, the smart box is hidden once an anchor loads
        # and there is no way to switch). Full clear/reset is Phase 120.
        change_anchor_btn = ui.button(
            tr('Change anchor'), icon='swap_horiz'
        ).props('flat dense').classes('self-start')

        # Responsa-only options row (Variants / Flexible spacing / Bidirectional)
        # lives in the options area below; it is hidden outside Responsa-style mode.
        # The builder notifies us of type changes via on_type_change.
        _responsa_opts_ref: dict = {'el': None}

        def _on_search_type_change(t: str) -> None:
            el = _responsa_opts_ref['el']
            if el is not None:
                el.set_visibility(t == 'responsa')

        # Anchor builder (BLD-03) — replaces Phase-117 textarea (:333-340).
        # create_joins_builder() builds its container with `with ui.column() as
        # container:`, so calling it inside this `with builder_area:` block mounts
        # the container here automatically — no manual reparenting needed.
        anchor_builder = create_joins_builder(
            allow_page_position=True,
            on_submit=_trigger_search,
            on_type_change=_on_search_type_change,
        )

        # Summary bar (D-14) — shown when builder is collapsed after a search
        # Tracks visibility state in a mutable dict so the close-over callbacks
        # can update it.
        _builder_vis: dict = {'expanded': True}
        summary_bar_container = ui.row().classes('w-full items-center gap-2 px-1').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
            ' border-radius: 6px; padding: 6px 10px;'
        )
        summary_bar_container.set_visibility(False)

        with summary_bar_container:
            _summary_label = ui.label('').classes('text-sm flex-grow').style(
                'color: var(--text-secondary);'
            )
            ui.button(
                icon='edit', on_click=lambda: _expand_builder()
            ).props('flat dense size=sm').tooltip(tr('Edit search'))

            # Phase 120-03 PST-03: Clear/Reset control (D-16).
            # Appears at the trailing end of the collapsed summary bar ONLY (not on
            # cold start — summary_bar_container is hidden until first search runs).
            def _on_reset_click() -> None:
                """Open the Clear/Reset confirmation dialog."""
                with ui.dialog() as _reset_dialog, ui.card().classes('p-4 gap-3'):
                    ui.label(tr('Clear Joins Lab')).classes('text-base font-bold')
                    ui.label(tr(
                        'This clears your anchor, builder lines, triage verdicts, and filters.'
                        ' You will start fresh. This cannot be undone.'
                    )).classes('text-sm').style('max-width: 340px; white-space: normal;')
                    with ui.row().classes('gap-2 justify-end'):
                        ui.button(tr('Cancel')).props('flat').on(
                            'click', lambda: _reset_dialog.close()
                        )
                        ui.button(
                            tr('Clear everything')
                        ).props('color=negative unelevated').on('click', _do_reset)
                _reset_dialog.open()

            def _do_reset() -> None:
                """Confirm: wipe all Joins Lab state and reload to cold start."""
                clear_joins_lab_state()
                ui.navigate.to('/joins-lab')

            ui.button(
                tr('Reset'), icon='clear_all'
            ).props('flat dense size=sm').style(
                'color: var(--negative);'
            ).tooltip(
                tr('Clear all Joins Lab state: anchor, builder, triage, filters')
            ).on('click', _on_reset_click)

        def _collapse_builder(summary_text: str) -> None:
            """Collapse the builder to the summary bar (D-14).

            Also hides the inline Advanced options (#2) so the other-side search
            compacts away with the anchor builder — the summary bar carries the
            other-side info instead.
            """
            anchor_builder['container'].set_visibility(False)
            advanced_options_container.set_visibility(False)
            _summary_label.set_text(summary_text)
            summary_bar_container.set_visibility(True)
            _builder_vis['expanded'] = False

        def _expand_builder() -> None:
            """Re-expand the builder from the summary bar (D-14 Edit button)."""
            summary_bar_container.set_visibility(False)
            anchor_builder['container'].set_visibility(True)
            advanced_options_container.set_visibility(True)
            _builder_vis['expanded'] = True

        # Search options (D-12, UI-SPEC §3) — INLINE, not hidden in an expansion
        # (#1): the global toggles + other-side toggle fit on a compact wrap-row, so
        # they are always visible. The whole container collapses with the builder
        # after a search (see _collapse_builder, #2).
        advanced_options_container = ui.column().classes('w-full gap-2').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
            ' border-radius: 8px; padding: 8px;'
        )
        with advanced_options_container:
            # One compact row: Responsa-only options + the always-visible other-side
            # toggle. Checkboxes are self-labelled (no sub-headers needed).
            with ui.row().classes('items-center gap-4 flex-wrap'):
                # Responsa-style-only options (Variants / Flexible spacing /
                # Bidirectional). Hidden in the single-line modes via
                # _on_search_type_change (those modes search like the main bar and
                # have no responsa_options). Variants drives BOTH builders.
                responsa_opts_row = ui.row().classes('items-center gap-4 flex-wrap')
                _responsa_opts_ref['el'] = responsa_opts_row
                with responsa_opts_row:
                    variants_cb = ui.checkbox(tr('Variants'), value=False)

                    def _on_variants_change() -> None:
                        on = bool(variants_cb.value)
                        anchor_builder['set_variants'](on)
                        _ob = _other_side.get('builder')
                        if _ob is not None and 'set_variants' in _ob:
                            _ob['set_variants'](on)
                        # PST-01: variants is already fired via set_variants → on_change
                        # (Task 1 B1 wires _persist_state via anchor_builder on_change);
                        # but if it doesn't fire (e.g. cached value), persist explicitly.
                        _persist_state()

                    variants_cb.on_value_change(_on_variants_change)

                    flex_cb = ui.checkbox(
                        tr('Flexible spacing'),
                        value=_global_opts['flex_spacing'],
                    )

                    def _on_flex_change(e) -> None:
                        _global_opts.update({'flex_spacing': bool(e.args)})
                        _persist_state()

                    flex_cb.on('update:model-value', _on_flex_change)

                    bidir_cb = ui.checkbox(
                        tr('Bidirectional'),
                        value=_global_opts['bidirectional'],
                    )

                    def _on_bidir_change(e) -> None:
                        _global_opts.update({'bidirectional': bool(e.args)})
                        _persist_state()

                    bidir_cb.on('update:model-value', _on_bidir_change)

                other_side_cb = ui.checkbox(
                    tr('Search the other side of the leaf'),
                    value=False,
                )

            # Other-side controls container (shown/hidden by checkbox)
            other_side_controls = ui.column().classes('gap-2 w-full')
            other_side_controls.set_visibility(False)

            with other_side_controls:
                # Combine mode: Narrow (AND) / Widen (OR), default AND
                with ui.row().classes('items-center gap-2'):
                    ui.label(tr('Combine mode')).classes('text-xs').style(
                        'color: var(--text-secondary);'
                    )
                    combine_select = ui.select(
                        options={
                            'AND': tr('Narrow (AND)'),
                            'OR': tr('Widen (OR)'),
                        },
                        value='AND',
                    ).props('outlined dense').classes('w-36')
                    # Store the element's normalized `.value` (the option KEY) via
                    # on_value_change — NOT the raw `update:model-value` payload,
                    # which for a dict-options select is the Quasar option object
                    # {'label','value'} (a dict) and silently mis-routes
                    # apply_cross_side. _coerce_combine_mode is belt-and-braces.
                    def _on_combine_change():
                        _other_side.update(combine=_coerce_combine_mode(combine_select.value))
                        _persist_state()

                    combine_select.on_value_change(_on_combine_change)

                # Other-side builder: its OWN Text Position selector, independent of
                # the anchor side (allow_page_position=True; UAT). Built inside this
                # `with other_side_controls:` block, so its container mounts here
                # automatically. Responsa-style only — show_search_type=False (D-Q1).
                other_builder = create_joins_builder(
                    allow_page_position=True,
                    on_submit=_trigger_search,
                    show_search_type=False,
                )
                _other_side['builder'] = other_builder

            def _on_other_side_toggle() -> None:
                # Read the checkbox's own .value (reliable bool) via on_value_change
                # — hides the other-side module again when unchecked.
                enabled = bool(other_side_cb.value)
                _other_side['enabled'] = enabled
                other_side_controls.set_visibility(enabled)
                # PST-01: persist other-side enabled/disabled change
                _persist_state()

            other_side_cb.on_value_change(_on_other_side_toggle)

        # Phase 120-03 PST-02: restoring indicator — shown during auto-restore re-run.
        # Hidden by default (cold start); shown in _bootstrap_anchor when a persisted
        # anchor is restored; hidden again once the first search results render.
        _restore_indicator = ui.row().classes('items-center gap-2 px-2 py-1 rounded').style(
            'background: var(--bg-tertiary); display: none;'
        )
        _restore_indicator_ref['el'] = _restore_indicator
        with _restore_indicator:
            ui.spinner(size='xs').style('color: var(--text-secondary);')
            ui.label(tr('Restoring your search…')).classes('text-sm').style(
                'color: var(--text-secondary);'
            )
            ui.label(tr('(from last session)')).classes('text-xs').style(
                'color: var(--text-tertiary);'
            )

        with ui.row().classes('gap-2 items-center flex-wrap'):
            search_btn = ui.button(tr('Run Search')).props('color=primary unelevated icon=search')
            # D-11 Stop button — occupies the SAME slot as Run Search (replaces it
            # while a user-initiated search is in flight).  Hidden by default; swap
            # visibility with search_btn during execute_joins_search.
            # NOT shown during auto-restore re-run (_bootstrap_anchor path).
            stop_btn = (
                ui.button(tr('Stop'))
                .props('color=negative unelevated icon=stop_circle')
                .mark('stop_search_btn')
                .tooltip(tr('Stop search and show partial results'))
            )
            # NOTE: set_visibility() returns None, so it must NOT terminate the
            # builder chain above (that would make stop_btn None and 500 the page
            # at stop_btn.on('click', ...)). Hide it on its own statement instead.
            stop_btn.set_visibility(False)
            # New Search (reset) — parity with /search's restart_alt button. Clears
            # both builders + results but KEEPS the loaded anchor ("Change anchor"
            # switches fragments). Wired below, next to the other handlers.
            new_search_btn = ui.button(icon='restart_alt').props('flat dense round').tooltip(
                tr('New Search — clear the query and results (keeps the anchor)')
            )
            search_status = ui.label('').classes('text-sm').style(
                'color: var(--text-secondary); display: none;'
            )

            # Phase 119 — 👁 Visual Similarity toggle (D-04/D-06, VSM-01)
            # This switch is defined here so it close over all page state; the
            # on_value_change handler is wired after execute_joins_search is defined.
            vs_switch = ui.switch(tr('Visual Similarity')).props('icon=visibility').style(
                'color: var(--text-secondary);'
            )
            _vs_switch_ref['el'] = vs_switch

            vs_status_label = ui.label('').classes('text-xs').style(
                'color: var(--text-secondary); display: none;'
            )
            _vs_status_ref['el'] = vs_status_label

            # A2: Grid/Table view toggle — flips _view_mode and re-renders WITHOUT
            # resetting _triage or _current_page (D-10: triage/page survive a view switch).
            def _on_view_toggle_click() -> None:
                """Toggle between Grid and Table view without resetting triage/page (D-10)."""
                if _view_mode['value'] == 'grid':
                    _view_mode['value'] = 'table'
                    view_toggle_btn.set_text(tr('Grid'))
                else:
                    _view_mode['value'] = 'grid'
                    view_toggle_btn.set_text(tr('Table'))
                # Re-render without clearing _triage or resetting _current_page
                _re_render_candidates_surface()
                # PST-01: persist view mode change
                _persist_state()

            view_toggle_btn = ui.button(tr('Table')).props('flat dense').tooltip(
                tr('Switch between Grid and Table view')
            )
            view_toggle_btn.on('click', _on_view_toggle_click)

            # Phase-120 ACT-03 D-06 R2-H2: Export — PERSISTENT toolbar control.
            # Operates on the FULL filtered/sorted candidate set (≤500), NOT the
            # table selection.  Visible in both Grid and Table view.
            # _on_export_click is defined below after all handlers are defined;
            # wired via _export_ref (late-bind pattern matching _submit_ref).
            _export_btn = ui.button(tr('Export'), icon='download').props(
                'flat dense icon-right=arrow_drop_down'
            ).tooltip(tr('Export candidates to CSV or Excel'))
            # The menu MUST be a child of the button so the q-menu anchors to the
            # button and opens only on the button's click. Previously it was a
            # child of the toolbar row, so q-menu's default parent-click listener
            # popped it open whenever ANY sibling in the row (Run Search, the VS
            # toggle) was clicked. Nesting inside the button also auto-opens it on
            # button click — no explicit .on('click', menu.open) needed.
            with _export_btn:
                with ui.menu().props('auto-close'):
                    ui.menu_item(
                        tr('CSV'),
                        on_click=lambda: asyncio.ensure_future(
                            _export_ref['fn']('csv') if _export_ref['fn'] else asyncio.sleep(0)
                        ),
                    ).props('dense')
                    ui.menu_item(
                        tr('Excel (XLSX)'),
                        on_click=lambda: asyncio.ensure_future(
                            _export_ref['fn']('xlsx') if _export_ref['fn'] else asyncio.sleep(0)
                        ),
                    ).props('dense')

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

    # -----------------------------------------------------------------------
    # Phase-120 ACT-01: Add-as-Join login gate + pending-action replay (H4)
    # -----------------------------------------------------------------------

    def _on_add_as_join_click(candidate_sys_id: str, candidate_shelfmark: str) -> None:
        """Handle "Add as Join" click on a candidate card/row.

        Logged-in: show confirm dialog → create_fragment_join (no status kwarg) off-loop
          → force-refresh known-joins so proposed join appears immediately (D-02).
        Anonymous: persist pending descriptor to safe_storage and open login dialog (H4).
          After login, the page reloads and _bootstrap_anchor replays the pending action.
        """
        anchor_sys_id = _anchor_state.get('sys_id') or ''
        anchor_shelfmark = _anchor_state.get('shelfmark') or ''

        if not GlobalAuthState.is_logged_in():
            # H4: page reloads on successful login — must persist before opening dialog.
            # R2-M2: include schema_version + created_at + expected_anchor_sys_id.
            descriptor = {
                'schema_version': 1,
                'action': 'add_as_join',
                'created_at': datetime.now(tz=timezone.utc).isoformat(),
                'expected_anchor_sys_id': anchor_sys_id,
                'anchor_sys_id': anchor_sys_id,
                'anchor_shelfmark': anchor_shelfmark,
                'candidate_sys_id': candidate_sys_id,
                'candidate_shelfmark': candidate_shelfmark,
            }
            safe_user_set('joins_lab_pending', descriptor)
            create_login_dialog().open()
            return

        # Logged-in: show confirm dialog
        user = GlobalAuthState.get_user()
        user_id = user['id'] if user else None
        if not user_id:
            return

        confirm_body = tr(
            'This records a scholarly claim that {a} and {b} physically join. '
            'It will be immediately visible to all users.'
        ).replace('{a}', anchor_shelfmark or anchor_sys_id).replace(
            '{b}', candidate_shelfmark or candidate_sys_id
        )

        with ui.dialog() as confirm_dialog, ui.card().classes('p-4 gap-3').style(
            'background: var(--bg-card); color: var(--text-primary);'
        ):
            ui.label(tr('Add as Join')).classes('text-base font-semibold')
            ui.label(confirm_body).classes('text-sm').style(
                'color: var(--text-secondary); max-width: 360px;'
            )
            with ui.row().classes('gap-2 justify-end'):
                ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                async def _do_add_join(
                    _uid=user_id, _asid=anchor_sys_id, _asm=anchor_shelfmark,
                    _csid=candidate_sys_id, _csm=candidate_shelfmark
                ) -> None:
                    """Off-loop Supabase write; force-refresh known-joins on success."""
                    try:
                        confirm_dialog.close()

                        def _run_create():
                            return create_fragment_join(
                                user_id=_uid,
                                fragment_a_sys_id=_asid,
                                fragment_a_shelfmark=_asm,
                                fragment_b_sys_id=_csid,
                                fragment_b_shelfmark=_csm,
                                # NO status kwarg — stays 'proposed' (D-02 LOCKED)
                            )

                        # Authenticated write — MUST run on the event loop, NOT
                        # via run.io_bound: app.storage.user auth context is
                        # contextvar-scoped and does not reach thread-pool
                        # workers, so an off-loop write hits RLS as the anon
                        # client and is denied (UAT 2026-06-21).
                        result = _run_create()
                        if result.get('success'):
                            # D-02: force-refresh so proposed join shows immediately
                            await _load_known_joins(
                                _asid, _asm,
                                anchor_gen=_anchor_generation['value'],
                                force_refresh=True,
                            )
                            ui.notify(tr('Add as Join'), type='positive', timeout=3000)
                        else:
                            err = result.get('error', '')
                            ui.notify(
                                tr('Could not add join. Check your connection.'),
                                type='negative', timeout=8000,
                            )
                            logger.warning('create_fragment_join error: %s', err)
                    except RuntimeError:
                        return  # SEED-008 D-20: client/tab deleted mid-fetch

                ui.button(tr('Add as Join'), on_click=_do_add_join).props('color=primary unelevated')

        confirm_dialog.open()

    # -----------------------------------------------------------------------
    # Phase-120 D-03: Remove-my-join — self-scoped delete on own joins only
    # -----------------------------------------------------------------------

    def _on_remove_join_click(join_id: int) -> None:
        """Handle remove-join click on own join row.

        Opens confirm dialog → delete_fragment_join off-loop → force-refresh
        known-joins to remove the row. RLS enforces self-scope on the server.
        """
        anchor_sys_id = _anchor_state.get('sys_id') or ''
        anchor_shelfmark = _anchor_state.get('shelfmark') or ''

        with ui.dialog() as remove_dialog, ui.card().classes('p-4 gap-3').style(
            'background: var(--bg-card); color: var(--text-primary);'
        ):
            ui.label(tr('Remove Join')).classes('text-base font-semibold')
            body_text = tr(
                'Remove the join between {a} and {b}? This cannot be undone.'
            ).replace('{a}', anchor_shelfmark or anchor_sys_id).replace('{b}', '…')
            ui.label(body_text).classes('text-sm').style(
                'color: var(--text-secondary); max-width: 360px;'
            )
            with ui.row().classes('gap-2 justify-end'):
                ui.button(tr('Cancel'), on_click=remove_dialog.close).props('flat')

                async def _do_remove(
                    _jid=join_id, _asid=anchor_sys_id, _asm=anchor_shelfmark
                ) -> None:
                    """Off-loop Supabase delete; force-refresh known-joins on success."""
                    try:
                        remove_dialog.close()

                        def _run_delete():
                            return delete_fragment_join(_jid)

                        # Authenticated delete — on the event loop, NOT
                        # run.io_bound (off-loop loses the user's auth context;
                        # RLS would deny as anon). UAT 2026-06-21.
                        result = _run_delete()
                        if result.get('success'):
                            # Force-refresh so the removed join disappears immediately
                            await _load_known_joins(
                                _asid, _asm,
                                anchor_gen=_anchor_generation['value'],
                                force_refresh=True,
                            )
                        else:
                            err = result.get('error', '')
                            ui.notify(
                                tr('Could not remove join. Check your connection.'),
                                type='negative', timeout=8000,
                            )
                            logger.warning('delete_fragment_join error: %s', err)
                    except RuntimeError:
                        return  # SEED-008 D-20: client/tab deleted mid-fetch

                ui.button(tr('Remove'), on_click=_do_remove).props('color=negative unelevated')

        remove_dialog.open()

    # -----------------------------------------------------------------------
    # Phase-120 ACT-02: Bulk Add-to-Puzzle — anchor + selected candidates
    # -----------------------------------------------------------------------

    def _on_add_to_puzzle_click() -> None:
        """Handle "Add to Puzzle" click in the bulk action bar (TABLE view, ≥1 selected).

        R2-H2: the bulk bar appears only on ≥1 table selection, so ``_selected``
        is always non-empty here.  The anchor is ALWAYS included as fragments[0]
        IN ADDITION to the selected candidates.

        Builds a ``puzzle_staging`` payload (schema_version=1, capped at
        anchor + 20 candidates), writes it via ``safe_user_set`` (Phase-87
        chokepoint — never raw app.storage.user), then navigates to /puzzle.
        The puzzle page pops + validates the key one-shot on load (T-120-stale /
        Pitfall 6) and schedules deferred sequential adds via auto_add_bulk.
        """
        anchor_sys_id = _anchor_state.get('sys_id') or ''
        if not anchor_sys_id:
            ui.notify(tr('No anchor loaded'), type='warning', timeout=3000)
            return

        selected_list = list(_selected)
        MAX_CANDIDATES = 20

        if len(selected_list) > MAX_CANDIDATES:
            ui.notify(
                tr('Only the first 20 selected candidates will be added to the Puzzle.'),
                type='info', timeout=5000,
            )

        capped = selected_list[:MAX_CANDIDATES]
        fragments = [anchor_sys_id] + capped

        safe_user_set('puzzle_staging', {
            'schema_version': 1,
            'fragments': fragments,
            'source': 'joins_lab',
            'created_at': datetime.now(tz=timezone.utc).isoformat(),
        })
        ui.navigate.to('/puzzle')

    # -----------------------------------------------------------------------
    # Phase-120 ACT-03/D-05: Add-to-List — login-gated cloud write (selection-scoped)
    # -----------------------------------------------------------------------

    def _on_add_to_list_click() -> None:
        """Handle "Add to List" click in the bulk action bar (TABLE view, ≥1 selected).

        Login-gated: anonymous users get the standard login dialog (no pending-replay
        for Add-to-List per the plan — the user re-clicks after sign-in since the
        action is selection-dependent and a page reload clears the selection anyway).
        Logged-in: opens a single-level list-picker sub-dialog; on list pick all
        selected candidates are written via add_list_item off-loop.

        SEED-008 (D-20): the whole coroutine body is wrapped in ``except RuntimeError``
        so a client teardown at any point does not propagate out of the task.
        """
        if not GlobalAuthState.is_logged_in():
            # Not logged in: show login gate (no pending replay — user re-clicks)
            with ui.dialog().props('persistent') as login_dlg:
                with ui.card().classes('gap-4 p-4'):
                    ui.label(tr('Sign in to add candidates to a list')).classes(
                        'text-base font-semibold'
                    )
                    with ui.row().classes('gap-2 justify-end'):
                        ui.button(tr('Cancel'), on_click=login_dlg.close).props('flat')
                        ui.button(
                            tr('Sign in'),
                            on_click=lambda: (login_dlg.close(), create_login_dialog().open()),
                        ).props('color=primary unelevated')
            login_dlg.open()
            return

        # Logged in — open list-picker sub-dialog
        selected_list = list(_selected)
        if not selected_list:
            ui.notify(tr('No candidates selected'), type='warning', timeout=3000)
            return

        async def _open_list_picker() -> None:
            """Open the list-picker sub-dialog and handle list selection."""
            try:
                user = GlobalAuthState.get_user()
                if not user:
                    return
                user_id = user['id']

                # Fetch lists + counts ON THE EVENT LOOP. Authenticated Supabase
                # reads MUST NOT run via run.io_bound: app.storage.user is
                # contextvar-scoped and does not propagate to the thread-pool
                # worker, so get_user_client() would fall back to the anon client
                # and RLS would return 0 rows ("no lists found"). counts are
                # best-effort (the get_list_item_counts_for_user RPC may be
                # denied -> Postgres 42501). UAT 2026-06-21.
                user_lists = get_user_lists(user_id)
                try:
                    counts = get_list_item_counts()
                except Exception as _counts_err:
                    logger.warning(
                        "Add-to-list picker: item counts unavailable: %s",
                        _counts_err,
                    )
                    counts = {}
                if counts is None:
                    counts = {}

                with ui.dialog().props('persistent') as picker_dlg:
                    with ui.card().classes('gap-2 p-4').style('min-width:320px'):
                        ui.label(
                            tr('Add N candidates to list:').replace('N', str(len(selected_list)))
                        ).classes('text-base font-semibold')

                        picker_status = ui.label('').classes('text-sm').style(
                            'color:var(--error); display:none;'
                        )

                        if not user_lists:
                            ui.label(tr('No lists found. Create a list first.')).classes(
                                'text-sm'
                            ).style('color:var(--text-muted);')
                        else:
                            for lst in user_lists:
                                list_id = lst.get('id')
                                list_name = lst.get('name') or ''
                                item_count = counts.get(list_id, 0) if list_id else 0

                                async def _pick_list(
                                    _lid=list_id, _lname=list_name, _dlg=picker_dlg,
                                    _status=picker_status,
                                ) -> None:
                                    """Dispatch add_list_item for each selected candidate."""
                                    try:
                                        errors = []
                                        for c in _filtered_candidates:
                                            if c.sys_id not in selected_list:
                                                continue
                                            # Authenticated write — on the event
                                            # loop, NOT run.io_bound (off-loop ->
                                            # anon -> RLS denies). UAT 2026-06-21.
                                            result = add_list_item(
                                                _lid,
                                                c.sys_id,
                                                shelfmark=getattr(c, 'shelfmark', None),
                                                title=getattr(c, 'title', None),
                                            )
                                            if isinstance(result, dict) and result.get('error'):
                                                errors.append(result['error'])

                                        if errors:
                                            _status.set_text(tr('Could not add to list. Check your connection.'))
                                            _status.style('display:block;')
                                        else:
                                            _dlg.close()
                                            ui.notify(
                                                tr('{N} candidates added to "{list_name}"').replace(
                                                    '{N}', str(len(selected_list))
                                                ).replace('{list_name}', _lname),
                                                type='positive',
                                                timeout=4000,
                                            )
                                    except RuntimeError:
                                        return

                                with ui.row().classes(
                                    'w-full items-center justify-between gap-2 cursor-pointer'
                                ).style(
                                    'padding:8px 0; border-bottom:1px solid var(--border-light);'
                                ).on('click', lambda lst_pick=_pick_list: asyncio.ensure_future(lst_pick())):
                                    ui.label(list_name).classes('text-sm font-semibold')
                                    ui.label(f'({item_count})').classes('text-xs').style(
                                        'color:var(--text-muted);'
                                    )

                        with ui.row().classes('gap-2 justify-end mt-2'):
                            ui.button(tr('Cancel'), on_click=picker_dlg.close).props('flat')

                picker_dlg.open()

            except RuntimeError:
                return

        asyncio.ensure_future(_open_list_picker())

    # -----------------------------------------------------------------------
    # Phase-120 ACT-03/D-06: Export — flat CSV/XLSX with off-loop batched text fetch
    # -----------------------------------------------------------------------

    async def _export_candidates(fmt: str) -> None:
        """Export the FULL filtered/sorted candidate set (≤500) as CSV or XLSX.

        R2-H2: Export operates on ``_filtered_candidates`` (the full current filtered
        set), NOT on ``_selected`` (the table selection).  This is a PERSISTENT action
        available in both Grid and Table view regardless of selection state.

        D-06 off-loop discipline: transcription text is fetched via
        ``run.io_bound(fetch_text_batch, batch)`` (never a blocking loop on the event
        loop).  A cancel flag is checked between batches so the user can abort.

        SEED-008 (D-20): the whole body is wrapped in ``except RuntimeError: return``
        so a client teardown at any point does not crash the server.
        """
        try:
            # Snapshot the filtered set (not _selected) at click time (R2-H2 / D-06)
            candidates_snapshot = list(_filtered_candidates)
            if not candidates_snapshot:
                ui.notify(tr('No candidates to export'), type='info', timeout=3000)
                return

            # Cap at 500 (D-06 / _EXPORT_CANDIDATE_CAP)
            show_cap_notice = len(candidates_snapshot) > _EXPORT_CANDIDATE_CAP
            candidates_snapshot = candidates_snapshot[:_EXPORT_CANDIDATE_CAP]
            total = len(candidates_snapshot)

            if show_cap_notice:
                ui.notify(tr('Exporting the first 500 candidates.'), type='info', timeout=5000)

            # Reset cancel flag
            _export_cancel_ref['value'] = False

            # Build the progress card
            progress_el = _export_progress_ref.get('el')
            progress_card = None
            progress_bar = None
            progress_label = None

            if progress_el is not None:
                progress_el.clear()
                progress_el.set_visibility(True)
                with progress_el:
                    with ui.card().classes('w-full p-3 gap-2') as progress_card:
                        with ui.row().classes('items-center gap-2 w-full'):
                            ui.spinner(size='sm').style('color:var(--primary-700);')
                            progress_label = ui.label(
                                tr('Preparing export…') + ' 0 / ' + str(total) + ' ' + tr('fragments fetched')
                            ).classes('text-sm flex-1')
                            ui.button(
                                tr('Cancel'),
                                on_click=lambda: _export_cancel_ref.update({'value': True}),
                            ).props('flat dense')
                        progress_bar = ui.linear_progress(value=0.0).props('color=primary')

            # Fetch text in batches off-loop.
            # Each iteration creates a fresh fetch_export_text_batch closure that
            # captures the current `batch` list. The closure is passed DIRECTLY to
            # run.io_bound so the AST guard (test_joins_lab_off_loop.py) sees:
            #   run.io_bound(fetch_export_text_batch, ...)
            # and confirms the sync function is dispatched off the event loop.
            texts: dict = {}  # sys_id → str | None
            fetched = 0

            def fetch_export_text_batch(batch_items):
                """Sync I/O closure: fetch get_browse_page text for a batch of candidates.

                Passed directly to run.io_bound — never called on the event loop.
                Returns dict[sys_id → capped_text_str].
                """
                results = {}
                for cand in batch_items:
                    try:
                        page_data = executor.get_browse_page(
                            cand.sys_id,
                            p_num=cand.page,  # None → first text page (A1 assumption)
                        )
                        if page_data and isinstance(page_data, dict):
                            text = page_data.get('text') or ''
                            results[cand.sys_id] = text[:_EXPORT_TEXT_CAP]
                        else:
                            results[cand.sys_id] = ''
                    except Exception:
                        results[cand.sys_id] = ''
                return results

            try:
                for batch_start in range(0, total, _EXPORT_BATCH_SIZE):
                    if _export_cancel_ref['value']:
                        break
                    batch = candidates_snapshot[batch_start:batch_start + _EXPORT_BATCH_SIZE]
                    batch_texts = await run.io_bound(fetch_export_text_batch, batch)
                    texts.update(batch_texts)
                    fetched += len(batch)

                    # Update progress UI
                    if progress_bar is not None:
                        progress_bar.set_value(fetched / total)
                    if progress_label is not None:
                        progress_label.set_text(
                            tr('Preparing export…') + f' {fetched} / {total} ' + tr('fragments fetched')
                        )

            except RuntimeError:
                # Client disconnected mid-fetch
                return
            except Exception as exc:
                logger.error('Export text fetch failed: %s', exc)
                # Show error in the progress card area
                if progress_el is not None:
                    progress_el.clear()
                    with progress_el:
                        with ui.card().classes('w-full p-3 gap-2'):
                            with ui.row().classes('items-center gap-2'):
                                ui.label(tr('Export failed. Check your connection and try again.')).classes(
                                    'text-sm flex-1'
                                ).style('color:var(--error);')
                                ui.button(
                                    tr('Retry'),
                                    on_click=lambda: asyncio.ensure_future(_export_candidates(fmt)),
                                ).props('flat dense')
                                ui.button(
                                    tr('Cancel'),
                                    on_click=lambda: (
                                        progress_el.clear(),
                                        progress_el.set_visibility(False),
                                    ),
                                ).props('flat dense')
                return

            if _export_cancel_ref['value']:
                # User cancelled — hide progress card, no download
                if progress_el is not None:
                    progress_el.clear()
                    progress_el.set_visibility(False)
                return

            # Build the flat export (10 columns per UI-SPEC §7)
            headers = [
                tr('Shelfmark'), tr('Library'), tr('Title'), tr('Triage'), tr('Score'),
                tr('Material'), tr('Dimensions'), tr('Page'),
                tr('Transcription (page)'), tr('Image URL'),
            ]

            def _triage_display(sys_id: str) -> str:
                verdict = _triage.get(sys_id)
                if verdict == 'yes':
                    return 'Y'
                if verdict == 'maybe':
                    return '?'
                if verdict == 'no':
                    return 'N'
                return '—'

            def _build_rows() -> list:
                rows = []
                for c in candidates_snapshot:
                    enrich = _enrichment.get(c.sys_id, {})
                    w = enrich.get('width_cm')
                    h = enrich.get('height_cm')
                    dims = f'{w:.1f}×{h:.1f}cm' if (w and h) else ''
                    library_code = getattr(c, 'library_code', '') or getattr(c, 'library', '') or ''
                    img_url = build_image_url_for_row(c.sys_id, library_code=library_code, img_page=c.page)
                    rows.append([
                        getattr(c, 'shelfmark', '') or '',
                        library_code,
                        getattr(c, 'title', '') or '',
                        _triage_display(c.sys_id),
                        f'{getattr(c, "score", 0.0):.2f}',
                        enrich.get('material') or '',
                        dims,
                        str(c.page) if c.page is not None else '',
                        texts.get(c.sys_id) or '',
                        img_url,
                    ])
                return rows

            data_rows = _build_rows()

            # Produce the download bytes
            filename_base = f'joins_lab_candidates_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

            if fmt == 'csv':
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(headers)
                writer.writerows(data_rows)
                content = buf.getvalue().encode('utf-8-sig')
                filename = filename_base + '.csv'
                media_type = 'text/csv'
            else:
                # XLSX via openpyxl (already a dependency — web/export_service.py uses it)
                import openpyxl  # noqa: PLC0415 (late import, avoids optional-dep cost at module load)
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.title = 'Candidates'
                ws.append(headers)
                for row in data_rows:
                    ws.append(row)
                xlsx_buf = io.BytesIO()
                wb.save(xlsx_buf)
                content = xlsx_buf.getvalue()
                filename = filename_base + '.xlsx'
                media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

            # Hide the progress card before triggering download
            if progress_el is not None:
                progress_el.clear()
                progress_el.set_visibility(False)

            # Trigger the file download (NiceGUI download pattern)
            ui.download(content, filename=filename, media_type=media_type)

        except RuntimeError:
            # SEED-008 D-20: client teardown at any point — swallow silently
            return

    # Wire _on_export_click into the toolbar button's menu items (late-bind)
    _export_ref['fn'] = _export_candidates

    async def _load_known_joins(
        sys_id: str, shelfmark: str, pgpid: Optional[int] = None,
        anchor_gen: int = 0, force_refresh: bool = False
    ) -> None:
        """Fetch all community joins (proposed + confirmed) for the anchor; render
        in known_joins_container.

        Phase-120 ACT-01 D-02 override: uses confirmed_only=False so both proposed
        and confirmed community joins appear in the Lab (parity with /browse).
        Pass force_refresh=True after an insert/delete so the new join appears
        immediately (bypasses the ~30s cache).

        ANC-04: the fetch is I/O-bound (Supabase + SQLite) so it MUST be dispatched
        via run.io_bound.

        MED (CR): this is fire-and-forget; ``anchor_gen`` is the anchor generation
        captured at dispatch. Every UI mutation below is guarded so a slow fetch for
        a PRIOR anchor cannot clear/render under a newer one.

        Re-anchor callback: calls load_anchor (does NOT reset builder state — D-16).
        Open-in-browse callback: navigates to /browse?shelfmark=... (same tab).

        SEED-008 (D-20): the WHOLE coroutine body is wrapped in ``try/except
        RuntimeError`` so a client-deleted teardown at ANY point (including the PRE-await
        spinner clear/render at the top of this function) does not propagate out of the
        task.  M4 requires the ``try`` to open BEFORE the first UI mutation — the spinner
        clear at ``known_joins_container.clear()`` below runs before the first ``await``
        and can raise RuntimeError if the tab was torn down before this task ran.
        """
        try:
            if anchor_gen != _anchor_generation['value']:
                return
            known_joins_container.clear()
            with known_joins_container:
                ui.spinner(size='sm').style('color: var(--text-muted);')

            try:
                data = await run.io_bound(
                    fetch_connected_fragments,
                    shelfmark=shelfmark,
                    document_id=sys_id,
                    pgpid=pgpid,
                    confirmed_only=False,  # Phase-120 D-02: show proposed + confirmed
                    force_refresh=force_refresh,
                )
            except Exception:
                if anchor_gen != _anchor_generation['value']:
                    return
                known_joins_container.clear()
                with known_joins_container:
                    ui.label(tr('Could not load joins. Check your connection.')).classes(
                        'text-xs'
                    ).style('color: var(--text-muted);')
                return

            # A newer anchor superseded this fetch while it was in flight — discard.
            if anchor_gen != _anchor_generation['value']:
                return

            def _on_reanchor(member_sys_id: str, member_shelfmark: str) -> None:
                """Re-anchor to a known-join member (D-16: does NOT clear builder state)."""
                asyncio.ensure_future(load_anchor(member_sys_id, show_restored_toast=False))

            def _on_open_browse(member_shelfmark: str) -> None:
                """Open a known-join member in Browse (same tab)."""
                sm_encoded = quote(member_shelfmark, safe='')
                ui.navigate.to(f'/browse?shelfmark={sm_encoded}')

            # Phase-120 D-03: get current user_id for own-join detection.
            # None when anonymous — no remove button rendered.
            _user = GlobalAuthState.get_user() if GlobalAuthState.is_logged_in() else None
            _current_uid = _user['id'] if _user else None

            known_joins_container.clear()
            with known_joins_container:
                render_known_joins_group(
                    data,
                    current_shelfmark=shelfmark,
                    current_sys_id=sys_id,
                    on_reanchor=_on_reanchor,
                    on_open_browse=_on_open_browse,
                    on_remove_join=_on_remove_join_click,
                    current_user_id=_current_uid,
                )
        except RuntimeError:
            return  # client/tab deleted mid-fetch — benign teardown (SEED-008 D-20)

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

        ANC-04: after building the AnchorViewer, fetches and renders the
        confirmed-only known-joins group (via _load_known_joins).
        """
        # CR-02: defensive guard — never re-anchor to an empty sys_id. A
        # community known-join member that could not be resolved to a sys_id
        # would otherwise set _anchor_state['sys_id']='', build an
        # AnchorViewer(sys_id=''), and persist write_anchor('') — corrupting the
        # stored anchor. The known-joins pin is also hidden in that case
        # (known_joins_group._render_member_row), so this is belt-and-braces.
        if not sys_id:
            return
        # CR HIGH-1: an anchor swap supersedes any search in flight for the old
        # anchor (the candidate grid is cleared below; without this a slow prior
        # search could repopulate it under the new anchor).
        _cancel_current_search()
        # MED (CR): new anchor generation — supersedes any in-flight known-joins
        # fetch for the previous anchor (see _load_known_joins guard).
        _anchor_generation['value'] += 1
        _my_anchor_gen = _anchor_generation['value']
        _anchor_state['sys_id'] = sys_id
        _anchor_state['fl_id'] = fl_id
        _anchor_state['volume_ie'] = volume_ie

        # Swap empty state → anchor + builder
        empty_state.set_visibility(False)
        builder_area.set_visibility(True)

        # WR-02: on every anchor swap (including the known-joins re-anchor pin),
        # clear the stale candidate grid from the PREVIOUS anchor and restore the
        # builder to its expanded state (hiding the collapsed summary bar left
        # behind by the previous search). Per D-16 we preserve the TYPED builder
        # rows — this only resets candidate output + builder visibility, never the
        # row content. (_on_change_anchor resets the same state on the manual
        # "Change anchor" path; the re-anchor path previously did not.)
        candidates_container.clear()
        anchor_builder['container'].set_visibility(True)
        summary_bar_container.set_visibility(False)
        _builder_vis['expanded'] = True
        search_status.style('display: none;')

        # Phase 119: D-11 triage resets on re-anchor; D-06 VS invalidates (D-11)
        _triage.clear()
        _raw_text_candidates.clear()  # G2: clear RAW baseline on re-anchor
        # D-10: bump prefetch generation + clear stale cache on re-anchor.
        # Any in-flight _prefetch_one coroutines will see the new generation
        # and discard their results without writing to _prefetch_cache.
        _prefetch_anchor_gen['value'] += 1
        _prefetch_cache.clear()
        _prefetch_running.clear()
        _all_candidates.clear()
        _filtered_candidates.clear()
        _enrichment.clear()
        _enrichment_ready['value'] = False
        _current_page['value'] = 0

        # Phase 119 D-06: VS invalidate on re-anchor — clear cached look-alikes
        # and schedule a re-fetch if the toggle is currently ON.
        _vs_candidates.clear()
        _vs_anchor_sid['value'] = None
        if _vs_on['value']:
            # Schedule a re-fetch for the NEW anchor (fire-and-forget)
            asyncio.ensure_future(_do_vs_fetch_and_update(sys_id))

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

        # ANC-04: resolve the shelfmark for known-joins (best-effort)
        # Prefer whatever AnchorViewer may have surfaced; fall back to executor.
        shelfmark: str = ''
        try:
            # get_meta_for_id is blocking I/O — must be called inside run.io_bound.
            # A4 F-A4-guard: use a NAMED sync closure (not lambda) so the AST off-loop
            # guard can verify this call is correctly dispatched via run.io_bound.
            # CORRECT await precedence: await the coroutine FIRST, THEN unpack
            # the (shelfmark, meta) tuple (do NOT write `await run.io_bound(...)[0]`
            # which subscripts the coroutine before awaiting it — Plan 04 precedence
            # bug flagged by Codex).
            def run_get_meta_for_anchor():
                return executor.get_meta_for_id(sys_id)
            meta_result = await run.io_bound(run_get_meta_for_anchor)
            shelfmark, _ = meta_result
        except Exception:
            shelfmark = ''

        # A1: store the resolved page + shelfmark into _anchor_state so Compare
        # can pass the REAL folio and shelfmark to the anchor pane (not hardcoded 1/'').
        _anchor_state['page'] = page  # page arg is the resolved folio (or None for first)
        _anchor_state['shelfmark'] = shelfmark

        # Fire-and-forget the known-joins load (non-blocking for the anchor swap).
        # Pass the captured anchor generation so a slow fetch for THIS anchor is
        # discarded if a newer anchor is loaded before it returns (MED, CR).
        asyncio.ensure_future(_load_known_joins(sys_id, shelfmark, anchor_gen=_my_anchor_gen))

        # D-12/L1: fire-and-forget VS-data probe — hide the VS toggle when the
        # anchor has zero look-alikes (not just disable — set_visibility(False)).
        asyncio.ensure_future(_probe_vs_data_and_update_toggle(sys_id, _my_anchor_gen))

    # -----------------------------------------------------------------------
    # Phase 120 D-12: VS toggle visibility probe
    # -----------------------------------------------------------------------

    async def _probe_vs_data_and_update_toggle(anchor_sid: str, my_anchor_gen: int) -> None:
        """D-12/L1: Probe VS data availability for anchor_sid and hide/show the toggle.

        Called fire-and-forget from load_anchor (each re-anchor triggers a fresh probe).

        Off-loop discipline: the probe call `svc.get_suggestions(anchor_sid, 1)` is a
        METHOD on the service instance (L1 — NOT a free function); it is dispatched via
        run.io_bound so it runs off the NiceGUI event loop.

        Generation guard: if the anchor changed while the probe was in flight, the result
        is stale — we discard it and do NOT mutate the VS toggle for the new anchor.

        SEED-008: the entire body is wrapped in try/except RuntimeError: return.

        Behaviour:
          empty result  → set_visibility(False) — toggle removed from flex row (no placeholder)
          non-empty     → set_visibility(True)  — toggle rendered normally (Phase-119 contract)
        """
        try:
            if my_anchor_gen != _anchor_generation['value']:
                return  # stale probe from previous anchor

            def run_vs_probe():
                svc = get_vs_service(thread_safe=True)
                return svc.get_suggestions(anchor_sid, 1)

            probe_result = await run.io_bound(run_vs_probe)

            # Generation re-check after the await (anchor may have changed during I/O)
            if my_anchor_gen != _anchor_generation['value']:
                return

            vs_el = _vs_switch_ref.get('el')
            if vs_el is None:
                return  # VS toggle not yet built

            has_vs_data = bool(probe_result)
            vs_el.set_visibility(has_vs_data)

        except RuntimeError:
            return  # SEED-008: NiceGUI client disconnected

    # -----------------------------------------------------------------------
    # Phase 119: VS toggle helpers (D-04/D-06/VSM-01)
    # -----------------------------------------------------------------------

    def _update_vs_status_label() -> None:
        """Update the VS status label to reflect the current toggle state."""
        label_el = _vs_status_ref.get('el')
        if label_el is None:
            return
        if _vs_loading['value']:
            label_el.set_text(tr('Loading visual similarity…'))
            label_el.style('display: inline; color: var(--text-secondary);')
        elif not _vs_on['value']:
            label_el.set_text('')
            label_el.style('display: none;')
        else:
            n = len([c for c in _filtered_candidates if c.via_vs])
            if n > 0:
                label_el.set_text(tr('Visual Similarity') + f' ({n})')
                label_el.style('display: inline; color: #f59e0b;')
            elif _vs_candidates:
                # VS returned results but none survived the intersection
                label_el.set_text(
                    tr('No candidates match both text and visual similarity. '
                       'Try clearing the builder for VS-only browse.')
                )
                label_el.style('display: inline; color: #f59e0b; font-size:0.8rem;')
            else:
                # VS service returned empty (no data for this anchor)
                label_el.set_text(tr('No visual similarity data for this fragment'))
                label_el.style('display: inline; color: var(--text-secondary);')

    def _apply_vs_unavailable_affordance() -> None:
        """F-VSavail: disable the VS switch and surface 'unavailable' string.

        Called after the off-loop availability probe returns False.
        Distinct from 'no data for this fragment' (which means available + 0 results).
        """
        vs_switch_el = _vs_switch_ref.get('el')
        if vs_switch_el is not None:
            vs_switch_el.disable()
        label_el = _vs_status_ref.get('el')
        if label_el is not None:
            label_el.set_text(tr('Visual similarity unavailable'))
            label_el.style('display: inline; color: var(--text-secondary);')

    async def _do_vs_fetch_and_update(anchor_sid: str) -> None:
        """Fetch VS candidates off-loop and update the display (D-06).

        Used by the toggle ON handler and by re-anchor invalidation when VS is ON.
        After fetch: recomputes _apply_vs_merge, updates _filtered_candidates,
        and re-renders the surface.

        F-VSavail: first time called, probes VS availability via run.io_bound
        (_check_vs_service_available). When unavailable: disables the switch +
        surfaces tr('Visual similarity unavailable') INSTEAD of silently returning [].

        SEED-008 (D-20): the WHOLE body is wrapped in ``try/except RuntimeError``
        so a client-deleted teardown (tab closed mid-fetch) does not propagate out.
        """
        try:
            # F-VSavail: probe availability off-loop on first VS fetch attempt
            if not _vs_available['checked']:
                available = await run.io_bound(_check_vs_service_available)
                _vs_available['checked'] = True
                _vs_available['available'] = available
                if not available:
                    _apply_vs_unavailable_affordance()
                    return  # VS is not available — do not proceed

            # If we already know it's unavailable, bail immediately
            if not _vs_available['available']:
                _apply_vs_unavailable_affordance()
                return

            _vs_loading['value'] = True
            _update_vs_status_label()
            try:
                vs_cands = await _fetch_vs_candidates(anchor_sid)
            except Exception:
                vs_cands = []
            finally:
                _vs_loading['value'] = False

            # Check the anchor hasn't changed while we were fetching
            if _anchor_state.get('sys_id') != anchor_sid:
                return  # stale fetch — discard

            # A4: enrich VS-only candidates with shelfmark/title/library_code off-loop.
            # R2-8: also fetch the transcription BEGINNING (first ~200 chars via get_browse_page)
            # so VS-only card snippet is non-blank instead of empty.
            # VS suggestions carry only sys_id/rank/score (page=None, page-agnostic by design
            # — F-A4-api). Resolve metadata via the PAGE-LOCAL executor (F-A4-scope: the
            # executor is a page-local closure; _fetch_vs_candidates is module-level and
            # cannot see it — resolve here inside _do_vs_fetch_and_update which CAN).
            # The I/O runs via run.io_bound (F-A4-guard) naming get_meta_for_id /
            # get_library_for_id / get_browse_page.
            if vs_cands:
                def run_vs_meta_core():
                    import dataclasses
                    _TRANSCRIPTION_PREFIX_LEN = 200  # R2-8: show the beginning, not the full text
                    meta_by_sid = {}
                    for c in vs_cands:
                        try:
                            shelfmark_v, title_v = executor.get_meta_for_id(c.sys_id)
                        except Exception:
                            shelfmark_v, title_v = '', ''
                        try:
                            library_code_v = executor.get_library_for_id(c.sys_id)
                        except Exception:
                            library_code_v = ''
                        # R2-8: fetch transcription beginning (dict key "text")
                        # get_browse_page returns a DICT (or None) — read via .get()
                        # NOT .text (it's not a BrowsePage object here, just a narrow dict).
                        transcription_prefix = ''
                        try:
                            page_data = executor.get_browse_page(c.sys_id)
                            if page_data:
                                raw_text = page_data.get('text', '') or ''
                                transcription_prefix = raw_text[:_TRANSCRIPTION_PREFIX_LEN]
                        except Exception:
                            pass  # graceful: card stays blank (no crash)
                        meta_by_sid[c.sys_id] = {
                            'shelfmark': shelfmark_v or '',
                            'title': title_v or '',
                            'library_code': library_code_v or '',
                            'full_text': transcription_prefix,
                        }
                    # Apply metadata via dataclasses.replace (Candidate is frozen=True)
                    # page stays None — VS suggestions are page-agnostic (F-A4-api)
                    enriched = []
                    for c in vs_cands:
                        m = meta_by_sid.get(c.sys_id, {})
                        replacements = {}
                        if m.get('shelfmark'):
                            replacements['shelfmark'] = m['shelfmark']
                        if m.get('title'):
                            replacements['title'] = m['title']
                        if m.get('library_code'):
                            replacements['library_code'] = m['library_code']
                        if m.get('full_text'):
                            replacements['full_text'] = m['full_text']
                        enriched.append(dataclasses.replace(c, **replacements) if replacements else c)
                    return enriched

                try:
                    vs_cands = await run.io_bound(run_vs_meta_core)
                except Exception:
                    logger.debug('VS metadata enrichment failed', exc_info=True)
                    # vs_cands stays as-is (no metadata, but still usable)

            _vs_candidates.clear()
            _vs_candidates.extend(vs_cands)
            _vs_anchor_sid['value'] = anchor_sid

            vs_switch_el = _vs_switch_ref.get('el')
            if vs_switch_el is not None and vs_switch_el.value:
                # Toggle is still ON — recompute from RAW baseline via single helper (G2)
                anchor_sid_now = _anchor_state.get('sys_id') or ''
                display = _compute_display_candidates()
                _all_candidates.clear()
                _all_candidates.extend(display)
                filtered = compute_filtered(display, _filter_state, _enrichment, _triage, anchor_sid_now)
                _filtered_candidates.clear()
                _filtered_candidates.extend(filtered)
                _current_page['value'] = 0
                # Re-render
                candidates_container.clear()
                if filtered:
                    with candidates_container:
                        _render_candidates_surface()
                else:
                    # VS on + empty builder + no VS data OR empty intersection
                    with candidates_container:
                        if not _vs_candidates:
                            ui.label(tr('No visual similarity data for this fragment')).style(
                                'color: var(--text-secondary);'
                            )
                        else:
                            ui.label(
                                tr('No candidates match both text and visual similarity. '
                                   'Try clearing the builder for VS-only browse.')
                            ).style('color: var(--text-secondary);')
            _update_vs_status_label()
        except RuntimeError:
            return  # client/tab deleted mid-fetch — benign teardown (SEED-008 D-20)

    def _on_vs_toggle_change() -> None:
        """Handle 👁 VS toggle ON/OFF (D-04/D-06)."""
        vs_switch_el = _vs_switch_ref.get('el')
        if vs_switch_el is None:
            return
        is_on = bool(vs_switch_el.value)
        _vs_on['value'] = is_on

        anchor_sid = _anchor_state.get('sys_id') or ''
        if not is_on:
            # Toggle OFF — recompute from RAW baseline via single helper (G2)
            # _vs_on['value'] is already False; _compute_display_candidates reads it
            display = _compute_display_candidates()
            _all_candidates.clear()
            _all_candidates.extend(display)
            _current_page['value'] = 0
            _re_render_candidates_surface()
            _update_vs_status_label()
            return

        # Toggle ON — check if we need to fetch VS candidates
        if _vs_anchor_sid['value'] != anchor_sid or not _vs_candidates:
            # Need a fresh fetch
            asyncio.ensure_future(_do_vs_fetch_and_update(anchor_sid))
        else:
            # Already have VS candidates for this anchor — recompute from RAW baseline (G2)
            display = _compute_display_candidates()
            _all_candidates.clear()
            _all_candidates.extend(display)
            _current_page['value'] = 0
            _re_render_candidates_surface()
            _update_vs_status_label()

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
        """D-06 / D-17: anonymous users see a login-prompt dialog; logged-in users
        get a two-level drill-down picker (lists → fragments) that loads the chosen
        fragment as the anchor.

        "My lists" routes ONLY to the per-user Supabase lists (login required).
        See the module-level NOTE for the full D-06 rationale.

        D-17: the two-level picker replaces the old "Go to Lists" placeholder.
        Level 1 = list selection (filterable); Level 2 = fragment selection within
        the chosen list (filterable, back-button preserves Level-1 filter).
        On fragment click: close dialog + call load_anchor(sys_id).

        SEED-008 (D-20): off-loop fetches inside fire-and-forget are wrapped in
        ``except RuntimeError: return`` so client-deleted teardowns are benign.
        """
        if not GlobalAuthState.is_logged_in():
            # Anonymous visitor — open the canonical in-page login overlay (D-18).
            # D-18: replaced the old custom dialog (which lost Lab state by routing
            # the user away) with create_login_dialog().open() — an in-page overlay.
            create_login_dialog().open()
            return

        # Logged-in path — open the two-level drill-down picker (D-17).
        def _open_picker() -> None:
            """Build + open the choose-from-lists picker dialog (D-17).

            Level 1: user's lists. Level 2: fragments in the chosen list.
            Fragment click → load_anchor(sys_id) + close dialog.

            T-119-09 / client-context regression rule: the dialog is built
            SYNCHRONOUSLY here so it mounts in the live NiceGUI client slot, and
            the off-loop lists/counts fetch runs in the async ``_on_show`` handler
            wired via ``picker_dialog.on('show', ...)`` below — NOT in a naked
            background coroutine that awaits BEFORE creating the dialog. Awaiting
            first detaches the slot, so ``ui.dialog()`` mounts nowhere and
            ``.open()`` silently does nothing (this is the compare_modal pattern).

            SEED-008: the async loaders below are wrapped in ``except RuntimeError: return``.
            """
            try:
                user = GlobalAuthState.get_user()
                if not user:
                    return
                user_id = user['id']

                # Picker data populated asynchronously by _on_show (off-loop).
                # counts are BEST-EFFORT (the get_list_item_counts_for_user RPC may
                # be denied -> Postgres 42501); counts_available gates the per-list
                # count badge so we never render a misleading "(0)".
                pdata: dict = {'lists': [], 'counts': {}, 'counts_available': False}

                # --- Build dialog with reactive state ---
                # level_state tracks which level is shown and the chosen list.
                level_state: dict = {
                    'level': 1,
                    'list_id': None,
                    'list_name': '',
                    'list_filter': '',   # Level-1 filter text (preserved on Back)
                    'frag_filter': '',   # Level-2 filter text
                }

                with ui.dialog().props('persistent') as picker_dialog:
                    with ui.card().classes('p-4 gap-2').style(
                        'min-width:360px; max-width:540px; '
                        'max-height:calc(100vh - 128px); overflow-y:auto; '
                        'background:var(--bg-card,#fff); color:var(--text-primary,#111);'
                    ):
                        # Dynamic heading label (updated on drill-down / back)
                        heading_label = ui.label(tr('Choose a List')).classes(
                            'text-base font-semibold'
                        )

                        # Filter input (shared; placeholder changes per level)
                        filter_input = ui.input(
                            placeholder=tr('Filter lists…'),
                        ).classes('w-full')

                        # Scrollable list container (Level 1 or Level 2)
                        list_container = ui.column().classes('w-full gap-1')

                        # Bottom row: back + cancel
                        with ui.row().classes('gap-2 justify-end mt-2 w-full'):
                            back_btn = ui.button(
                                tr('Back'),
                                icon='arrow_back',
                                on_click=lambda: None,   # wired below
                            ).props('flat').tooltip(tr('Back to lists'))
                            back_btn.set_visibility(False)
                            ui.button(tr('Cancel'), on_click=picker_dialog.close).props('flat')

                    def _render_level1() -> None:
                        """Render Level-1: list rows filtered by level_state['list_filter']."""
                        filter_val = level_state['list_filter'].lower()
                        list_container.clear()
                        heading_label.set_text(tr('Choose a List'))
                        filter_input.props(f'placeholder="{tr("Filter lists…")}"')
                        back_btn.set_visibility(False)

                        all_lists = pdata['lists']
                        visible = [
                            lst for lst in all_lists
                            if filter_val in (lst.get('name') or '').lower()
                        ] if filter_val else all_lists

                        if not visible:
                            with list_container:
                                ui.label(tr('No lists found.')).classes('text-sm').style(
                                    'color:var(--text-muted,#475569);'
                                )
                            return

                        for lst in visible:
                            list_id = lst.get('id')
                            list_name = lst.get('name') or ''
                            item_count = pdata['counts'].get(list_id, 0) if list_id is not None else 0

                            def _make_list_click(lid=list_id, lname=list_name):
                                def _on_click() -> None:
                                    level_state['list_id'] = lid
                                    level_state['list_name'] = lname
                                    level_state['frag_filter'] = ''
                                    filter_input.value = ''
                                    asyncio.ensure_future(_load_level2(lid, lname))
                                return _on_click

                            with list_container:
                                with ui.row().classes(
                                    'w-full items-center justify-between gap-2 cursor-pointer '
                                    'px-4 py-2 rounded hover:bg-gray-100'
                                ).style(
                                    'border-bottom:1px solid var(--border-light,#e2e8f0);'
                                ).on('click', _make_list_click()):
                                    ui.label(list_name).classes('text-sm font-semibold flex-1')
                                    with ui.row().classes('items-center gap-1'):
                                        # Only show the count badge when counts
                                        # actually loaded; a "(0)" on every list
                                        # when the RPC is denied is misleading.
                                        if pdata['counts_available']:
                                            ui.label(f'({item_count})').classes(
                                                'text-xs'
                                            ).style('color:var(--text-muted,#475569);')
                                        ui.icon('chevron_right').classes('text-base').style(
                                            'color:var(--text-muted,#475569);'
                                        )

                    async def _load_level2(list_id: int, list_name: str) -> None:
                        """Fetch fragments for list_id off-loop, then render Level 2.

                        SEED-008: wrapped in ``except RuntimeError: return``.
                        """
                        try:
                            list_container.clear()
                            with list_container:
                                ui.spinner(size='sm').style('color:var(--primary-700,#047857);')

                            items = get_list_items(list_id)  # authed read: on-loop, not run.io_bound

                            level_state['level'] = 2
                            heading_label.set_text(f'← {list_name}')
                            filter_input.props(f'placeholder="{tr("Filter fragments…")}"')
                            back_btn.set_visibility(True)
                            level_state['frag_filter'] = ''

                            _render_level2_items(items, list_name)
                        except RuntimeError:
                            return  # SEED-008: client/tab deleted mid-fetch

                    def _render_level2_items(items: list, list_name: str) -> None:
                        """Render Level-2 fragment rows (filtered by level_state['frag_filter'])."""
                        filter_val = level_state['frag_filter'].lower()
                        list_container.clear()

                        if not items:
                            with list_container:
                                ui.label(tr('No fragments in this list.')).classes(
                                    'text-sm'
                                ).style('color:var(--text-muted,#475569);')
                            return

                        visible = [
                            it for it in items
                            if filter_val in (it.get('shelfmark') or '').lower()
                               or filter_val in (it.get('title') or '').lower()
                        ] if filter_val else items

                        if not visible:
                            with list_container:
                                ui.label(tr('No matches.')).classes('text-sm').style(
                                    'color:var(--text-muted,#475569);'
                                )
                            return

                        for item in visible:
                            sys_id = item.get('sys_id') or ''
                            shelfmark = item.get('shelfmark') or sys_id
                            title = item.get('title') or ''
                            library = item.get('library_code') or ''

                            def _make_frag_click(sid=sys_id):
                                async def _on_frag_click() -> None:
                                    try:
                                        picker_dialog.close()
                                        await load_anchor(sid)
                                    except RuntimeError:
                                        return  # SEED-008
                                return _on_frag_click

                            with list_container:
                                with ui.row().classes(
                                    'w-full items-center gap-2 cursor-pointer '
                                    'px-4 py-2 rounded hover:bg-gray-100'
                                ).style(
                                    'border-bottom:1px solid var(--border-light,#e2e8f0); '
                                    'min-height:44px;'
                                ).on('click', lambda fn=_make_frag_click(): asyncio.ensure_future(fn())):
                                    if library:
                                        ui.badge(library).classes(
                                            'text-xs'
                                        ).props('color=primary outline')
                                    with ui.column().classes('flex-1 gap-0'):
                                        ui.label(shelfmark).classes('text-sm font-semibold')
                                        if title:
                                            ui.label(title).classes('text-xs').style(
                                                'color:var(--text-muted,#475569); '
                                                'direction:rtl; text-align:right;'
                                            )

                    # Wire Back button
                    def _on_back() -> None:
                        level_state['level'] = 1
                        filter_input.value = level_state['list_filter']
                        _render_level1()

                    back_btn.on('click', _on_back)

                    # Wire filter input
                    def _on_filter_change(e) -> None:
                        val = e.value if hasattr(e, 'value') else (e.args if isinstance(e.args, str) else '')
                        if level_state['level'] == 1:
                            level_state['list_filter'] = val
                            _render_level1()
                        else:
                            level_state['frag_filter'] = val
                            # Re-render level 2 with the current cached items
                            # (trigger a fresh re-render from in-memory state)
                            list_id = level_state.get('list_id')
                            list_name_cur = level_state.get('list_name', '')
                            if list_id is not None:
                                asyncio.ensure_future(_reload_level2_filter(list_id, list_name_cur))

                    async def _reload_level2_filter(list_id: int, list_name: str) -> None:
                        """Re-render Level-2 with the current filter without re-fetching."""
                        try:
                            items = get_list_items(list_id)  # authed read: on-loop, not run.io_bound
                            _render_level2_items(items, list_name)
                        except RuntimeError:
                            return  # SEED-008

                    filter_input.on('update:model-value', _on_filter_change)

                    # Initial loading spinner — replaced once _on_show populates
                    # pdata and calls _render_level1() (avoids a "No lists" flash).
                    with list_container:
                        ui.spinner(size='sm').style('color:var(--primary-700,#047857);')

                # T-119-09: fetch lists+counts off-loop when the dialog mounts in
                # the LIVE client context (via on('show')) — never before the
                # dialog exists. Counts are best-effort (get_list_item_counts()
                # re-raises on a denied RPC), so isolate exceptions and degrade.
                async def _on_show(_evt: object = None) -> None:
                    try:
                        list_container.clear()
                        with list_container:
                            ui.spinner(size='sm').style(
                                'color:var(--primary-700,#047857);'
                            )
                        # Authenticated reads run ON THE EVENT LOOP here (this
                        # on('show') handler runs in the live client context).
                        # They must NOT use run.io_bound: app.storage.user is
                        # contextvar-scoped and does not reach thread-pool
                        # workers, so off-loop get_user_client() would fall back
                        # to the anon client and RLS would return 0 rows ("no
                        # lists found"). UAT 2026-06-21.
                        try:
                            user_lists = get_user_lists(user_id)
                        except Exception as _lists_err:
                            # Lists are essential — show a non-fatal notice in-dialog.
                            logger.warning(
                                "Choose-from-lists picker: get_user_lists failed: %s",
                                _lists_err,
                            )
                            list_container.clear()
                            with list_container:
                                ui.label(
                                    tr('Could not load your lists. Please try again.')
                                ).classes('text-sm').style(
                                    'color:var(--text-muted,#475569);'
                                )
                            return
                        # counts are best-effort (the get_list_item_counts_for_user
                        # RPC may be denied -> Postgres 42501). The count badge is
                        # hidden (see _render_level1) rather than rendering a
                        # misleading "(0)" on every list.
                        try:
                            counts = get_list_item_counts()
                            counts_available = isinstance(counts, dict)
                            if not counts_available:
                                counts = {}
                        except Exception as _counts_err:
                            logger.warning(
                                "Choose-from-lists picker: item counts unavailable "
                                "(showing lists without counts): %s",
                                _counts_err,
                            )
                            counts = {}
                            counts_available = False
                        pdata['lists'] = user_lists
                        pdata['counts'] = counts
                        pdata['counts_available'] = counts_available
                        _render_level1()
                    except RuntimeError:
                        return  # SEED-008: client/tab deleted mid-fetch

                picker_dialog.on('show', _on_show)
                picker_dialog.open()

            except RuntimeError:
                return  # SEED-008: client/tab deleted before dialog could open

        _open_picker()

    # Wire button handlers
    def _on_change_anchor() -> None:
        """Return to the cold-start picker so the user can load a DIFFERENT anchor.

        Re-shows the empty-state smart box and hides the builder/candidates.
        Does NOT clear the persisted anchor (D-13) — picking a new one overwrites
        it via load_anchor. Full clear/reset is Phase 120.
        """
        # CR HIGH-1: cancel any in-flight search before tearing down the builder.
        _cancel_current_search()
        anchor_input.value = ''
        error_label.style('display: none;')
        anchor_viewer_container.clear()
        known_joins_container.clear()
        candidates_container.clear()
        # Restore the builder to visible (cancel any collapse)
        anchor_builder['container'].set_visibility(True)
        summary_bar_container.set_visibility(False)
        _builder_vis['expanded'] = True
        builder_area.set_visibility(False)
        empty_state.set_visibility(True)

    def _reset_search() -> None:
        """New Search (parity with /search): clear both builders + results.

        Resets the anchor builder (one empty line / Exact / Anywhere), the
        other-side builder + its toggles, the global option toggles, and the
        candidate grid — but KEEPS the loaded anchor. Use "Change anchor" to
        switch fragments. Mirrors the restart_alt reset on /search.
        """
        # CR HIGH-1: supersede any in-flight search so its results cannot land in
        # the freshly-cleared grid.
        _cancel_current_search()

        # Anchor builder back to clean defaults
        anchor_builder['reset']()

        # Other side: uncheck + hide + reset combine + clear its builder.
        # Programmatic .value set fires on_value_change (hides the controls via
        # _on_other_side_toggle / updates _other_side['combine']) but NOT the raw
        # .on('update:model-value') handlers — so set explicit state too.
        _other_side['enabled'] = False
        _other_side['combine'] = 'AND'
        other_side_cb.value = False
        combine_select.value = 'AND'
        other_side_controls.set_visibility(False)
        _ob = _other_side.get('builder')
        if _ob is not None:
            _ob['reset']()

        # Responsa options (Variants / flex / bidi). variants_cb.on_value_change
        # fires on programmatic set → resets both builders' variants; flex/bidir use
        # raw .on() so update their state manually.
        variants_cb.value = False
        _global_opts['flex_spacing'] = False
        _global_opts['bidirectional'] = False
        flex_cb.value = False
        bidir_cb.value = False

        # Results + builder visibility (anchor_builder.reset() restored type to
        # Responsa-style, so the responsa-only options row is shown again).
        candidates_container.clear()
        summary_bar_container.set_visibility(False)
        anchor_builder['container'].set_visibility(True)
        advanced_options_container.set_visibility(True)
        _builder_vis['expanded'] = True
        search_status.set_text('')
        search_status.style('display: none;')

    def _on_stop_click() -> None:
        """D-11 Stop-with-partials: request cooperative search abort.

        Sets ``_stop_requested['value'] = True`` (checked by the progress_cb
        inside run.io_bound) WITHOUT bumping ``_search_generation``.  Because
        the generation is unchanged, ``_should_apply_results(my_gen,
        _search_generation)`` returns True in ``execute_joins_search`` after the
        await — so the partial results ARE rendered (not discarded).

        The UI immediately transitions to the "Stopping…" affordance so the user
        knows the stop was registered. The actual UI switch back to Run Search
        happens in execute_joins_search's outer ``finally`` block (WR-01), which
        fires once the run.io_bound worker thread has been cooperatively aborted.

        NOT called during auto-restore re-run (stop_btn is never shown during
        ``_bootstrap_anchor`` — see execute_joins_search for the guard).
        """
        if not _is_running['value']:
            return  # idempotent — ignore if no search is running
        _stop_requested['value'] = True
        # Transition to "Stopping…" affordance (spinner on stop_btn label)
        stop_btn.set_text(tr('Stopping…'))
        stop_btn.props('loading=true disabled=true')

    load_btn.on('click', _on_load_btn_click)
    change_anchor_btn.on('click', _on_change_anchor)
    new_search_btn.on('click', _reset_search)
    anchor_input.on('keydown.enter', _on_load_btn_click)
    lists_btn.on('click', _on_lists_btn_click)
    stop_btn.on('click', _on_stop_click)

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

        BLD-03/BLD-04 additions:
          - Reads from anchor_builder (not the Phase-117 textarea)
          - Text Position routing: 'line_start'/'line_end' go directly to
            execute_search(text_position=...) — NOT into SideQuery (would raise
            ValueError at shared/joins_lab.py:67-71)
          - _merge_globals_web(ro, _global_opts) applied after compose() to
            re-inject flex_spacing + bidirectional (BLD-04)

        BLD-02 addition (cross-side block):
          - Other-side builder drives apply_cross_side via run_cross_side_core
            dispatched via run.io_bound (CI guard; literal function name)
          - _merge_globals_web applied to b_ro inside run_cross_side_core too
        """
        # CR HIGH-2: validate / build / compose BEFORE bumping the generation or
        # cancelling the in-flight task. An early return here (empty input, engine
        # not ready, compose() ValueError) must NOT supersede a running search —
        # otherwise the older run's finally would see a stale generation and never
        # clear the loading affordance. The latest-wins bump+cancel happens only
        # once we know a real replacement search will start (just below compose()).

        # Step 1: Validate inputs
        # F1: VS-only empty-builder branch — when 👁 is ON + builder is empty,
        # BYPASS the text-search early return and render the pure VS union instead.
        if anchor_builder['is_empty']():
            if _vs_on['value']:
                # VS-ONLY branch: fetch look-alikes and render merge_candidates([], vs)
                # WR-04: cancel any in-flight search before starting this branch so
                # the prior task's cooperative-cancel fires immediately and frees the
                # worker thread (the generation bump alone was sufficient for result
                # gating, but explicit cancel avoids resource waste).
                _cancel_current_search()
                anchor_sid_f1 = _anchor_state.get('sys_id') or ''
                # D-11: reset stop flag before this user-initiated VS-only search.
                _stop_requested['value'] = False
                _search_generation['value'] += 1
                my_gen_f1 = _search_generation['value']
                _is_running['value'] = True
                # D-11: swap Run Search → Stop (VS-only is user-initiated).
                search_btn.set_visibility(False)
                stop_btn.set_text(tr('Stop'))
                stop_btn.props(remove='loading disabled')
                stop_btn.set_visibility(True)
                candidates_container.clear()
                search_status.set_text(tr('Loading visual similarity…'))
                search_status.style('display: block;')
                try:
                    if _vs_anchor_sid['value'] != anchor_sid_f1 or not _vs_candidates:
                        vs_cands_f1 = await _fetch_vs_candidates(anchor_sid_f1)
                        _vs_candidates.clear()
                        _vs_candidates.extend(vs_cands_f1)
                        _vs_anchor_sid['value'] = anchor_sid_f1
                    else:
                        vs_cands_f1 = list(_vs_candidates)
                    if not _should_apply_results(my_gen_f1, _search_generation):
                        return
                    final_f1 = _apply_vs_merge([], vs_cands_f1, vs_on=True, builder_has_query=False)
                    _all_candidates.clear()
                    _all_candidates.extend(final_f1)
                    _triage.clear()
                    _current_page['value'] = 0
                    anchor_sid_now = _anchor_state.get('sys_id') or ''
                    filtered_f1 = compute_filtered(final_f1, _filter_state, _enrichment, _triage, anchor_sid_now)
                    _filtered_candidates.clear()
                    _filtered_candidates.extend(filtered_f1)
                    candidates_container.clear()
                    search_status.style('display: none;')
                    with candidates_container:
                        if _filtered_candidates:
                            _render_candidates_surface()
                        else:
                            ui.label(
                                tr('No visual similarity data for this fragment')
                            ).style('color: var(--text-secondary);')
                    # Fire enrichment off-loop for the full filtered set
                    asyncio.ensure_future(_do_enrich_and_update(list(_filtered_candidates)))
                finally:
                    if _is_running['value'] and my_gen_f1 == _search_generation['value']:
                        _is_running['value'] = False
                        # D-11: swap Stop → Run Search.
                        stop_btn.set_visibility(False)
                        stop_btn.props(remove='loading disabled')
                        stop_btn.set_text(tr('Stop'))
                        search_btn.set_visibility(True)
                        _stop_requested['value'] = False
                return
            else:
                ui.notify(
                    tr('Enter at least one search line to run'),
                    type='warning',
                )
                return

        if not state.is_ready():
            ui.notify(tr('Engine not ready.'), type='warning')
            return

        # D-Q1: unified query descriptor — Responsa-style structured side, or a
        # single-line standard-mode query (Exact/Variants/Fuzzy/Regex).
        q = anchor_builder['build_query']()

        # Text Position applies to BOTH kinds (full join workflow kept in
        # single-line modes). 'line_start'/'line_end' always go directly to
        # execute_search(text_position=...).
        tp_val = anchor_builder['get_text_position']()

        if q['kind'] == 'responsa':
            side = q['side']
            if side is None or not side.rows:
                ui.notify(tr('Enter at least one search line to run'), type='warning')
                return
            # Responsa-style: variant expansion driven by the Variants checkbox.
            mode_str = 'variants' if side.variants else 'exact'
            # CR-01: compose() raises ValueError when page_position='start'/'end'
            # but the anchoring (first/last) row is empty.
            try:
                query_str, ro, page_position = compose(side)
            except ValueError:
                ui.notify(
                    tr('Text Position requires content on that line. '
                       'Add a word to the first/last line or set Text Position to Anywhere.'),
                    type='warning',
                )
                return
            # BLD-04: re-inject flex_spacing + bidirectional from the UI toggles
            # (compose() hardcodes both to False at shared/joins_lab.py:741-749).
            _merge_globals_web(ro, _global_opts)
            # line_start/line_end go direct; start/end already live in page_position.
            direct_text_position: Optional[str] = (
                tp_val if tp_val in ('line_start', 'line_end') else page_position
            )
        else:
            # Single-line standard search — NO responsa_options (responsa_mode off);
            # behaves like the main search bar. Here Fuzzy is real edit-distance and
            # Regex is a real regex (CR HIGH-7).
            query_str = q['query']
            if not query_str:
                ui.notify(tr('Enter a search query to run'), type='warning')
                return
            mode_str = q['mode']
            ro = None
            # All non-default positions map straight to execute_search(text_position=).
            direct_text_position = (
                tp_val if tp_val in ('start', 'end', 'line_start', 'line_end') else None
            )

        if not query_str:
            return

        # Step 2 (CR HIGH-2): a real search WILL start — NOW supersede any in-flight
        # run (latest-wins). prev.cancel() cancels the asyncio.wait_for wrapper only;
        # the already-running run.io_bound worker thread is aborted cooperatively via
        # the bumped _search_generation making the OLD search's progress_cb raise
        # InterruptedError (see _make_progress_cb).
        # D-11: reset the user Stop flag so a prior Stop cannot bleed into the new search.
        _stop_requested['value'] = False
        _search_generation['value'] += 1
        my_gen = _search_generation['value']
        prev = _current_task['task']
        if prev and not prev.done():
            prev.cancel()

        # Step 4: UI — loading state; collapse builder to summary bar (D-14)
        # D-11: swap Run Search → Stop (user-initiated search; NOT auto-restore path).
        _is_running['value'] = True
        search_btn.set_visibility(False)
        stop_btn.set_text(tr('Stop'))
        stop_btn.props(remove='loading disabled')
        stop_btn.set_visibility(True)
        candidates_container.clear()
        search_status.set_text(tr('Searching...'))
        search_status.style('display: block;')

        # D-14 (#2): auto-collapse the builder to the summary bar, showing the
        # ACTUAL composed responsa query string the engine will run — anchor side,
        # then the other side after ' || ' when enabled. `query_str` is the anchor
        # query composed above; compose the other side here for display.
        _other_query = None
        _ob = _other_side.get('builder')
        if _other_side.get('enabled') and _ob is not None:
            _osq = _ob['build_side_query']()
            if _osq is not None and _osq.rows:
                try:
                    _bq, _, _ = compose(_osq)
                except ValueError:
                    _bq = None
                _other_query = _bq or None
        _collapse_builder(build_collapsed_query_text(query_str, _other_query))

        # WR-01: a SINGLE outer try/finally wraps BOTH the anchor leg and the
        # cross-side leg so the loading affordance (button + _is_running) is held
        # for the full duration of the search and is restored on every exit path
        # (return / exception). Previously the button was re-enabled in the inner
        # finally after the FIRST await, leaving it clickable for up to
        # _SEARCH_TIMEOUT_SECONDS while the cross-side leg still ran.
        try:
            # Step 5: Build the sync closure (MEDIUM-4: execute_search appears ONLY
            # here, inside the synchronous function that is passed to run.io_bound).
            # CI guard (tests/test_joins_lab_off_loop.py): the LITERAL name
            # "run_search_core" MUST be the first positional arg to run.io_bound.
            def run_search_core():
                return executor.execute_search(
                    query_str,
                    mode=mode_str,
                    gap=0,
                    # D-11: pass stop_ref so user-clicked Stop raises InterruptedError
                    # BEFORE the generation check — partials are applied (not discarded).
                    progress_callback=_make_progress_cb(
                        my_gen, _search_generation, stop_ref=_stop_requested
                    ),
                    responsa_options=ro,
                    text_position=direct_text_position,
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
                return

            except Exception as exc:
                logger.exception('Joins Lab search error: %s', exc)
                search_status.set_text(
                    tr('Search failed. Check your connection and try again.')
                )
                return

            finally:
                # Clean up the task reference if this is still the current task.
                # (The button/_is_running restore now lives in the OUTER finally so
                # it also covers the cross-side leg — WR-01.)
                if _current_task['task'] is not None and _current_task['task'].done():
                    _current_task['task'] = None

            # Step 7: STALE-GENERATION discard — the PRIMARY guard for a
            # cooperatively-cancelled run (core returns partial results normally;
            # _should_apply_results is what prevents them reaching the UI).
            if not _should_apply_results(my_gen, _search_generation):
                return

            # Step 8: Dedup anchor results
            anchor_sid = _anchor_state.get('sys_id') or ''
            base_candidates, anchor_matched = dedup_candidates(raw_results, anchor_sid)

            # ---------------------------------------------------------------
            # BLD-02: Cross-side block — if other-side builder is enabled and
            # has content, run apply_cross_side via run_cross_side_core off the
            # event loop (CI guard: literal name "run_cross_side_core" MUST be
            # the first positional arg to run.io_bound).
            # ---------------------------------------------------------------
            final_candidates = base_candidates  # default: no cross-side filtering

            if _other_side['enabled'] and _other_side['builder'] is not None:
                other_side_sq = _other_side['builder']['build_side_query']()
                if other_side_sq is not None and other_side_sq.rows:
                    # Snapshot inputs that are safe to read on the event loop
                    _combine_mode_snap = _coerce_combine_mode(_other_side['combine'])
                    _other_sq_snap = other_side_sq   # SideQuery is immutable (frozen dataclass)
                    _base_snapshot = list(base_candidates)
                    _global_opts_snap = dict(_global_opts)
                    # Other side's OWN Text Position (independent of the anchor side).
                    # line_start/line_end go directly to execute_search(text_position=);
                    # start/end live in the SideQuery's page_position (set below).
                    _other_tp_snap = _other_side['builder']['get_text_position']()

                    # CI guard (tests/test_joins_lab_off_loop.py): the sync closure
                    # named EXACTLY "run_cross_side_core" MUST be the first positional
                    # arg to run.io_bound.  apply_cross_side is I/O-bound (calls
                    # execute_search internally) and must NOT run on the event loop.
                    # Per the plan, compose + _merge_globals_web both run INSIDE this
                    # closure so the full b_ro lifecycle is off the event loop.
                    def run_cross_side_core():
                        b_query, b_ro, b_page_position = compose(_other_sq_snap)
                        # CR-05: compose() returns (None, None, None) when all other-side
                        # rows have whitespace-only terms.  _merge_globals_web(None, ...)
                        # would raise TypeError.  Guard: treat as "no other-side query"
                        # and return the base snapshot unchanged.
                        if b_query is None:
                            from shared.joins_lab import MergeResult
                            return MergeResult(
                                candidates=tuple(_base_snapshot),
                                note='b_query empty — all other-side rows were whitespace',
                            )
                        # BLD-04: re-inject flex_spacing + bidirectional into b_ro
                        # (Pitfall 2 — compose() hardcodes flex/bidir=False on b_ro also)
                        _merge_globals_web(b_ro, _global_opts_snap)
                        # Other-side Text Position: line_start/line_end go direct;
                        # start/end come from compose()'s page_position.
                        b_text_position = (
                            _other_tp_snap
                            if _other_tp_snap in ('line_start', 'line_end')
                            else b_page_position
                        )
                        return apply_cross_side(
                            executor,
                            _base_snapshot,
                            b_query,
                            b_ro,
                            _combine_mode_snap,
                            # CR HIGH-3: make the query-B scan cooperatively
                            # cancellable when a newer search supersedes this run.
                            # D-11: also pass stop_ref so user-clicked Stop propagates
                            # through the cross-side leg too.
                            progress_callback=_make_progress_cb(
                                my_gen, _search_generation, stop_ref=_stop_requested
                            ),
                            text_position=b_text_position,
                        )

                    cross_coro = run.io_bound(run_cross_side_core)
                    cross_task = asyncio.ensure_future(
                        asyncio.wait_for(cross_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
                    )
                    # Update _current_task so a cancellation also covers the cross-side leg
                    _current_task['task'] = cross_task

                    try:
                        merge_result = await cross_task
                        # Re-check stale generation after the second await
                        if not _should_apply_results(my_gen, _search_generation):
                            return
                        final_candidates = list(merge_result.candidates)
                    except asyncio.TimeoutError:
                        # MED (CR): the final render hides search_status immediately,
                        # so surface the other-side timeout as a persistent notify
                        # instead (otherwise the user sees base candidates with no clue
                        # the other-side leg failed).
                        ui.notify(
                            tr('Other-side search timed out — showing this-side results only.'),
                            type='warning',
                        )
                        final_candidates = list(base_candidates)
                    except asyncio.CancelledError:
                        return
                    except Exception as exc:
                        logger.exception('Joins Lab cross-side error: %s', exc)
                        ui.notify(
                            tr('Could not resolve the other side of this leaf. '
                               'Try navigating to a specific folio first.'),
                            type='warning',
                        )
                        final_candidates = list(base_candidates)

            # Step 9: Store RAW text baseline, apply VS merge, render surface
            # G2 fix: _raw_text_candidates is the RAW text+cross-side result BEFORE
            # any VS merge. _compute_display_candidates() derives the display list
            # from this raw baseline on demand so toggling VS later is always clean.
            anchor_sid_step9 = _anchor_state.get('sys_id') or ''

            # Store the RAW text baseline (never the merged/display set)
            _raw_text_candidates.clear()
            _raw_text_candidates.extend(final_candidates)

            # Derive the display set via the single helper (G2: one source of truth)
            display_candidates = _compute_display_candidates()

            # Store the derived display set
            _all_candidates.clear()
            _all_candidates.extend(display_candidates)

            # D-11: triage resets on new search; D-13: detect_self_match runs (silently)
            _triage.clear()
            _self_matched = detect_self_match(raw_results, anchor_sid_step9)
            # D-13: self-match is detected but not surfaced in Phase 119. Phase 120
            # exposes it as a UI badge.  Captured to prevent accidental removal as
            # "dead code" — the call has observability value for future callers.
            _ = _self_matched  # noqa: F841

            # Compute initial filtered list and reset pagination
            filtered = compute_filtered(
                _all_candidates, _filter_state, _enrichment, _triage, anchor_sid_step9
            )
            _filtered_candidates.clear()
            _filtered_candidates.extend(filtered)
            _current_page['value'] = 0

            # Render the candidate surface
            candidates_container.clear()
            search_status.style('display: none;')
            with candidates_container:
                _render_candidates_surface()

            # Fire enrichment off-loop for the FULL filtered set (D-16, Pitfall 7)
            asyncio.ensure_future(_do_enrich_and_update(list(_filtered_candidates)))

            # If VS is ON but we haven't fetched VS candidates for this anchor yet, fetch now
            if _vs_on['value'] and _vs_anchor_sid['value'] != anchor_sid_step9:
                asyncio.ensure_future(_do_vs_fetch_and_update(anchor_sid_step9))

        finally:
            # WR-01: restore the loading affordance for the WHOLE search (both the
            # anchor leg AND the cross-side leg), only if this run is still the
            # current generation. A superseding run owns the UI and must not have
            # its loading state cleared by an older run's finally.
            if _is_running['value'] and my_gen == _search_generation['value']:
                _is_running['value'] = False
                # D-11: swap Stop → Run Search and reset the stop flag.
                stop_btn.set_visibility(False)
                stop_btn.props(remove='loading disabled')
                stop_btn.set_text(tr('Stop'))  # reset label from 'Stopping…' if set
                search_btn.set_visibility(True)
                _stop_requested['value'] = False  # clean up for next search

    _submit_ref['fn'] = execute_joins_search  # enables Enter-to-search in word boxes
    search_btn.on('click', execute_joins_search)

    # Wire the VS toggle — must happen AFTER execute_joins_search is defined
    # (the toggle handler reads _vs_on, _all_candidates, etc. which are closures)
    vs_switch.on_value_change(_on_vs_toggle_change)

    # -----------------------------------------------------------------------
    # Phase 120-03 PST-01: Persistence — save full state on every input/triage/
    # filter/view change.  All calls to _persist_state are on the event loop
    # (never inside run.io_bound — Pitfall 4).
    # -----------------------------------------------------------------------

    def _persist_state() -> None:
        """Gather the full joins_lab blob and write it via write_full_state.

        Persists INPUTS + TRIAGE only — never candidate lists, full_text, or
        images (D-13; avoids the search-history payload-bloat bug class).

        Called from:
          - anchor_builder on_change (builder word/mode/gap/anchor mutations)
          - other_builder on_change
          - global toggle changes (variants, flex_spacing, bidirectional)
          - other-side toggle + combine changes
          - triage verdict change
          - filter apply / reset
          - view-mode switch
        """
        anchor_sys_id = _anchor_state.get('sys_id') or ''
        anchor_fl_id = _anchor_state.get('fl_id') or ''
        if not anchor_sys_id:
            return  # cold start — nothing to persist yet

        # Anchor builder snapshot (rows + mode + text_position)
        ab_state = anchor_builder['get_state']()

        # Other-side builder snapshot (only when enabled)
        ob = _other_side.get('builder')
        ob_state = ob['get_state']() if (ob is not None and _other_side['enabled']) else None

        write_full_state(
            # Anchor identity
            anchor_sys_id=anchor_sys_id,
            anchor_fl_id=anchor_fl_id,
            # Builder inputs (D-13): rows, mode, text_position
            builder_rows=ab_state.get('lines_state', []),
            builder_mode=anchor_builder['get_mode'](),
            text_position=anchor_builder['get_text_position'](),
            # Global options
            flex_spacing=_global_opts['flex_spacing'],
            bidirectional=_global_opts['bidirectional'],
            # Other side
            other_side_enabled=_other_side['enabled'],
            other_side_rows=ob_state.get('lines_state', []) if ob_state else [],
            other_side_combine=_other_side.get('combine', 'AND'),
            # Triage (sys_id keyed verdicts — no result blobs or images, D-13)
            triage=dict(_triage),
            # Filter + view
            active_filter=dict(_filter_state),
            view_mode=_view_mode['value'],
        )

    # Register persistence hooks on both builders (Task 1 B1 API)
    anchor_builder['on_change'](_persist_state)
    other_builder['on_change'](_persist_state)

    # -----------------------------------------------------------------------
    # Initial anchor resolution / restore (D-13: URL wins over storage)
    # -----------------------------------------------------------------------

    async def _bootstrap_anchor() -> None:
        """Resolve and load the initial anchor (deferred off the page handler).

        Phase 120-03 PST-02 extension: when the source is 'stored', read the
        full persisted blob (read_full_state) and perform a full state restore:
          1. Show restoring indicator (slim bar).
          2. Load anchor + restore builder via set_state.
          3. Restore global toggles + other-side.
          4. Restore filter + view_mode.
          5. Auto-run search (Stop NOT shown — auto-restore path).
          6. Re-attach triage by sys_id AFTER execute_joins_search clears it.
          7. Re-render candidates surface with restored triage.
          8. Hide restoring indicator.
        """
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
            # Phase-120 H4: replay pending add-as-join if present (url-anchor path)
            await _replay_pending_action()

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
                # Phase-120 H4: replay pending add-as-join if present (url-shelfmark path)
                await _replay_pending_action()
            # else: fall through to empty state (shelfmark not found)

        elif decision['source'] == 'stored':
            # --- PST-02: full restore flow ---
            full_state = read_full_state()  # returns dict or None

            # Show restoring indicator
            ind_el = _restore_indicator_ref.get('el')
            if ind_el is not None:
                ind_el.style('display: flex;')

            # Step 1: load anchor
            await load_anchor(
                decision['sys_id'],
                fl_id=decision.get('fl_id'),
                volume_ie=decision.get('volume_ie'),
                show_restored_toast=False,  # we show our own restoring indicator
            )

            if full_state:
                # Step 2: restore anchor builder state via B1 set_state
                stored_ab = {
                    'lines_state': full_state.get('builder_rows', []),
                    'search_type': full_state.get('builder_mode', 'responsa'),
                    'variants_on': full_state.get('variants_on', False),
                    'single_text': '',
                    'text_position': full_state.get('text_position', 'anywhere'),
                }
                # Infer variants_on from builder_mode if not stored explicitly
                if 'variants_on' not in full_state:
                    stored_ab['variants_on'] = full_state.get('builder_mode') == 'variants'
                anchor_builder['set_state'](stored_ab)

                # Step 3: restore global toggles
                flex_val = bool(full_state.get('flex_spacing', False))
                bidir_val = bool(full_state.get('bidirectional', False))
                _global_opts['flex_spacing'] = flex_val
                _global_opts['bidirectional'] = bidir_val
                flex_cb.value = flex_val
                bidir_cb.value = bidir_val

                variants_val = stored_ab['variants_on']
                variants_cb.value = variants_val
                anchor_builder['set_variants'](variants_val)

                # Step 4: restore other-side
                other_enabled = bool(full_state.get('other_side_enabled', False))
                if other_enabled:
                    _other_side['enabled'] = True
                    other_side_cb.value = True
                    other_side_controls.set_visibility(True)
                    combine_val = full_state.get('other_side_combine', 'AND')
                    combine_select.value = combine_val
                    _other_side['combine'] = _coerce_combine_mode(combine_val)
                    _ob = _other_side.get('builder')
                    if _ob is not None:
                        stored_ob = {
                            'lines_state': full_state.get('other_side_rows', []),
                            'search_type': 'responsa',
                            'variants_on': variants_val,
                            'single_text': '',
                            'text_position': full_state.get('text_position', 'anywhere'),
                        }
                        _ob['set_state'](stored_ob)
                        _ob['set_variants'](variants_val)

                # Step 5: restore filter state (before execute so display is correct)
                stored_filter = full_state.get('active_filter')
                if stored_filter and isinstance(stored_filter, dict):
                    _filter_state.update(stored_filter)

                # Step 6: restore view_mode
                stored_view = full_state.get('view_mode', 'grid')
                _view_mode['value'] = stored_view if stored_view in ('grid', 'table') else 'grid'
                if _view_mode['value'] == 'table':
                    view_toggle_btn.set_text(tr('Grid'))
                else:
                    view_toggle_btn.set_text(tr('Table'))

                # Step 7: capture persisted triage for re-attachment AFTER execute
                stored_triage = dict(full_state.get('triage') or {})
            else:
                stored_triage = {}

            # Step 8: auto-run search (Stop NOT shown on auto-restore path, D-11)
            # execute_joins_search resets _triage to {} — we re-attach afterwards.
            await execute_joins_search()

            # Step 9: re-attach persisted triage by sys_id (D-15).
            # Orphan keys for sys_ids not in the current result set are harmless.
            if stored_triage:
                _triage.update(stored_triage)
                # Re-render to show verdict borders on the restored candidate cards.
                candidates_container.clear()
                with candidates_container:
                    _render_candidates_surface()

            # Step 10: hide restoring indicator
            if ind_el is not None:
                ind_el.style('display: none;')

            # Phase-120 H4: replay pending add-as-join if present (stored-anchor path)
            await _replay_pending_action()

    # -----------------------------------------------------------------------
    # Phase-120 H4: pending-action replay helper (one-shot safe_user_pop)
    # -----------------------------------------------------------------------

    async def _replay_pending_action() -> None:
        """Replay a pending Add-as-Join descriptor after the post-login page reload.

        H4: create_login_dialog() reloads the page on success — in-memory pending
        actions are lost.  The anonymous path persists a compact descriptor to
        safe_storage under 'joins_lab_pending'; this function reads and pops it
        (one-shot — prevents double-fire on subsequent loads).

        R2-M2 replay guards (ALL must hold to replay):
          1. Descriptor is a dict with schema_version == 1 and action == 'add_as_join'.
          2. created_at is within _PENDING_ACTION_TTL_SECONDS of now (not expired).
          3. GlobalAuthState.is_logged_in() is True.
          4. expected_anchor_sys_id == the currently-loaded anchor sys_id.

        If ANY guard fails → descriptor is already popped; cleared without writing.
        """
        # One-shot pop: cleared whether or not we replay (no double-fire)
        descriptor = safe_user_pop('joins_lab_pending', None)
        if not descriptor or not isinstance(descriptor, dict):
            return

        # Guard 1: schema + action
        if descriptor.get('schema_version') != 1 or descriptor.get('action') != 'add_as_join':
            logger.debug('_replay_pending_action: schema/action mismatch — cleared')
            return

        # Guard 2: TTL expiry
        try:
            created_at_str = descriptor.get('created_at', '')
            created_at = datetime.fromisoformat(created_at_str)
            age_seconds = (datetime.now(tz=timezone.utc) - created_at).total_seconds()
            if age_seconds > _PENDING_ACTION_TTL_SECONDS:
                logger.debug('_replay_pending_action: descriptor expired (age=%.0fs) — cleared', age_seconds)
                return
        except Exception:
            logger.debug('_replay_pending_action: unparseable created_at — cleared')
            return

        # Guard 3: must be logged in
        if not GlobalAuthState.is_logged_in():
            logger.debug('_replay_pending_action: not logged in — cleared')
            return

        user = GlobalAuthState.get_user()
        user_id = user['id'] if user else None
        if not user_id:
            return

        # Guard 4: anchor must match
        expected_anchor = descriptor.get('expected_anchor_sys_id', '')
        current_anchor = _anchor_state.get('sys_id') or ''
        if expected_anchor != current_anchor:
            logger.debug(
                '_replay_pending_action: anchor mismatch (expected=%r, current=%r) — cleared',
                expected_anchor, current_anchor,
            )
            return

        # All guards passed — replay the create_fragment_join write
        anchor_sys_id = descriptor.get('anchor_sys_id', current_anchor)
        anchor_shelfmark = descriptor.get('anchor_shelfmark', '')
        candidate_sys_id = descriptor.get('candidate_sys_id', '')
        candidate_shelfmark = descriptor.get('candidate_shelfmark', '')

        if not candidate_sys_id:
            return

        try:
            def _run_create():
                return create_fragment_join(
                    user_id=user_id,
                    fragment_a_sys_id=anchor_sys_id,
                    fragment_a_shelfmark=anchor_shelfmark,
                    fragment_b_sys_id=candidate_sys_id,
                    fragment_b_shelfmark=candidate_shelfmark,
                    # NO status kwarg — stays 'proposed' (D-02 LOCKED)
                )

            # Authenticated write — on the event loop, NOT run.io_bound (the
            # off-loop worker has no app.storage.user auth context -> anon ->
            # RLS denies the INSERT). UAT 2026-06-21.
            result = _run_create()
            if result.get('success'):
                # D-02: force-refresh so proposed join shows immediately
                await _load_known_joins(
                    anchor_sys_id, anchor_shelfmark,
                    anchor_gen=_anchor_generation['value'],
                    force_refresh=True,
                )
                ui.notify(tr('Add as Join'), type='positive', timeout=3000)
            else:
                err = result.get('error', '')
                ui.notify(
                    tr('Could not add join. Check your connection.'),
                    type='negative', timeout=8000,
                )
                logger.warning('_replay_pending_action create_fragment_join error: %s', err)
        except RuntimeError:
            return  # SEED-008 D-20: client/tab deleted mid-fetch

    # Defer the initial async resolution so it runs after the page handler
    # returns (do NOT block the page handler). asyncio.call_later (NOT ui.timer):
    # if the visitor navigates away within the delay, a ui.timer fires on a
    # deleted slot and raises 'parent_slot has been deleted' in NiceGUI's timer
    # machinery; re-entering the captured client context here is slot-safe and
    # swallows the teardown race.
    _page_client = ui.context.client

    def _schedule_bootstrap() -> None:
        async def _runner() -> None:
            try:
                with _page_client:
                    await _bootstrap_anchor()
            except RuntimeError as exc:
                if 'slot' not in str(exc) and 'deleted' not in str(exc):
                    logger.error('joins-lab bootstrap error: %s', exc)
            except Exception:
                logger.exception('joins-lab bootstrap error')
        asyncio.ensure_future(_runner())

    asyncio.get_event_loop().call_later(0.05, _schedule_bootstrap)
