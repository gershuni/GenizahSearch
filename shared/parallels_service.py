# -*- coding: utf-8 -*-
"""Phase 80 D-11 -- pure-data parallels execution + cap for POST /api/parallels.

Mirrors shared/browse_service.py (Phase 79 D-23 extraction precedent): pure-data
async fan-out, importable from shared/, with no UI-framework dependency.

Layering contract (SEED-016 #3):
- This module lives in shared/ and MUST NOT import from web/ at runtime. It used
  to do `from web.state import state` inline (twice) to reach the process-
  singleton SearchEngine + MetadataManager, which inverted the layering
  (shared -> web). The dependency is now INVERTED: the caller
  (web/search_api.py) passes `searcher` (a CompositionSearcher) and `meta_mgr`
  (a UidComponentParser) INTO fetch_parallels_results. shared/ stays framework-
  and web-agnostic.

Statelessness contract (D-20 inherited from Phase 78 / D-22 inherited from Phase 79):
- The injected searcher/meta_mgr are the process-singletons (formerly read off
  web.state). MUST NOT touch any per-session UI state -- last-results caches,
  parallels-results caches, current-search-query, browser-storage,
  request-cookies, or any UI-coupled page state. Verified by grep at acceptance
  time.

D-07 cap policy: main_results capped at 200 groups (one group per sys_id) AFTER
sorting groups desc by aggregate_score (sum of per-row final_score within group).
The cap is applied to GROUPS, not raw chunk-hit rows. When raw main result group
count exceeds 200, the bundle's truncated_to_200 flag is True; the route handler
is responsible for appending 'truncated_to_200' to the envelope warnings[] list.

filtered_results (the high-freq filtered set) is NOT capped in v7.10 -- typically
small, driven by user's max_freq threshold. v7.11 may add if load testing warrants.

D-02 mode enum: 'exact' | 'variants' | 'fuzzy' -- locked. Lab Engine path
(out of scope; see D-02) is NOT invoked by this service in v7.10.

D-03 boundary_mode: 'full' | 'boundary' | 'combined'. The other 4 boundary
parameters use search_composition_logic's existing defaults (boundary_delimiter='\\n',
boundary_boost=1.5, min_boundary_matches=0, min_delimiter_distance=3) -- NOT
exposed via this service signature in v7.10.

R-09 operational note inherited from Phase 79: asyncio.wait_for() does NOT cancel
the underlying executor thread doing sync CPU work. search_composition_logic can
take seconds on long compositions; the route handler imposes no fan-out timeout
(unlike browse). This is acceptable for v7.10 -- the rate limiter is the load
shield. Phase 81+ may add an explicit composition timeout if observed needed.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Protocol

logger = logging.getLogger(__name__)


class CompositionSearcher(Protocol):
    """Structural type for the injected SearchEngine (SEED-016 #3).

    The process-singleton SearchEngine (genizah_core.py) satisfies this without
    importing it here. Only the one method fetch_parallels_results needs is
    declared. Kept loose (**kwargs) so the real engine's wider signature is
    compatible.
    """

    def search_composition_logic(self, *args, **kwargs) -> dict:
        ...


class UidComponentParser(Protocol):
    """Structural type for the injected MetadataManager (SEED-016 #3).

    Only the parse method _group_parallels_by_sys_id needs is declared.
    """

    def parse_full_id_components(self, uid_or_header: str) -> dict:
        ...


# D-07 hardcoded group cap -- no env override in v7.10 per CONTEXT deferred ideas.
PARALLELS_GROUP_CAP = 200


@dataclass
class ParallelsResultBundle:
    """Pure-data result of /api/parallels execution.

    main_results / filtered_results are raw row lists shaped by
    SearchEngine.search_composition_logic -- each row has at minimum:
      uid, raw_header, src_lbl, source_ctx, text, score, final_score,
      has_boundary_matches, boundary_match_count, chunk_count, chunk_hits
    Plan 03's route handler passes these directly to the parallels payload
    serializer in shared.search_serializer -- this service does NOT serialize.

    boundary_options echoes the boundary parameters used by the search call
    so the serializer envelope can echo them (D-06 of Phase 77 / Phase 80
    inheritance).

    truncated_to_200 is True when raw group count exceeded
    PARALLELS_GROUP_CAP and main_results was capped. The handler appends
    'truncated_to_200' to the envelope warnings[] list in this case.

    NOTE on filtered_results: filtered_results is NOT subject to
    PARALLELS_GROUP_CAP in v7.10. filtered_results is the set of chunks whose
    frequency exceeds the user's max_freq threshold -- typical max_freq values
    produce small filtered sets, making capping unnecessary for v7.10. This is
    an explicit v7.10 decision, not an oversight. v7.11 may add a filtered cap
    if load testing reveals large filtered payloads.
    """
    main_results: list[dict]
    filtered_results: list[dict]
    boundary_options: dict
    truncated_to_200: bool = False


async def _run_sync(func, *args, _executor=None, **kwargs):
    """Run blocking sync work in an executor.

    R-09 note: asyncio.wait_for() applied around this WOULD cancel the awaiting
    coroutine but NOT the underlying thread; v7.10 imposes no fan-out timeout
    (rate limiter is the load shield).

    Phase 145: `_executor` (keyword-only, named with a leading underscore so
    it can never collide with a `func` kwarg) selects which
    ThreadPoolExecutor `run_in_executor` dispatches into. Defaulting to None
    (the default executor) is what makes the chunk path byte-for-byte
    unchanged -- every existing caller omits it. The passage path's own
    bounded executor is passed in by `web/search_api.py` (per-request, so
    this module stays framework-agnostic and never constructs one itself).
    """
    loop = asyncio.get_event_loop()
    if kwargs:
        # run_in_executor signature is (executor, func, *args). Wrap kwargs.
        from functools import partial
        return await loop.run_in_executor(_executor, partial(func, *args, **kwargs))
    return await loop.run_in_executor(_executor, func, *args)


def _cap_main_results_by_group(
    main_results: list[dict],
    meta_mgr: UidComponentParser,
    cap: int = PARALLELS_GROUP_CAP,
) -> tuple[list[dict], bool]:
    """Apply D-07 group cap.

    Groups main_results by sys_id (using the same helper the serializer uses
    so cap and envelope group identically), sorts groups desc by aggregate_score
    (sum of per-row final_score within the group, falling back to score), takes
    top `cap` groups, flattens back to a row list.

    Returns (capped_rows, truncated_flag). truncated_flag=True iff raw group
    count exceeded `cap`.

    SEED-016 #3: `meta_mgr` is injected by the caller (was read off web.state).
    _group_parallels_by_sys_id needs it to parse uid components.

    NOTE: This cap applies ONLY to main_results. filtered_results is NOT capped
    in v7.10 -- see ParallelsResultBundle docstring for rationale.
    """
    if not main_results:
        return [], False

    # Late import to keep service import-time fast and avoid circular imports.
    from shared.search_serializer import _group_parallels_by_sys_id

    groups = _group_parallels_by_sys_id(main_results, meta_mgr=meta_mgr)

    if len(groups) <= cap:
        return main_results, False

    # Sort by aggregate_score desc, then take top `cap` groups.
    # _group_parallels_by_sys_id already sets aggregate_score per group
    # (sum of final_score / score values). Defensively recompute if missing.
    def _agg(g: dict) -> float:
        v = g.get('aggregate_score')
        if isinstance(v, (int, float)):
            return float(v)
        items = g.get('items') or []
        return float(sum(
            (it.get('final_score') if it.get('final_score') is not None else it.get('score', 0)) or 0
            for it in items
        ))

    groups_sorted = sorted(groups, key=_agg, reverse=True)
    kept_groups = groups_sorted[:cap]
    # Flatten kept groups back to a row list, preserving original row order
    # WITHIN each group (the serializer re-groups identically downstream).
    capped_rows: list[dict] = []
    for g in kept_groups:
        capped_rows.extend(g.get('items') or [])
    return capped_rows, True


async def fetch_parallels_results(
    *,
    searcher: CompositionSearcher,
    meta_mgr: UidComponentParser,
    text: str,
    chunk_size: int,
    mode: str,
    max_freq: Optional[float] = None,
    boundary_mode: str = 'full',
    restrict_sys_ids: Optional[set] = None,
    method: str = 'chunk',
    executor=None,
) -> ParallelsResultBundle:
    """Run search_composition_logic via run_in_executor + apply group cap.

    All input validation (text length, chunk_size bounds, mode enum, etc.) is
    the route handler's responsibility (Plan 03). This function trusts its
    inputs and only translates them to search_composition_logic's call shape
    + applies the D-07 group cap on the way out.

    SEED-016 #3: `searcher` (SearchEngine) and `meta_mgr` (MetadataManager) are
    injected by the caller (web/search_api.py) -- this module no longer imports
    web.state. Keeps shared/ from importing web/.

    Args:
        searcher: process-singleton SearchEngine exposing
                  search_composition_logic, OR (Phase 145, method='passage') a
                  `shared.passage_parallels.PassageSearcher` exposing the same
                  method. Injected by the caller -- this function does not
                  care which concrete object it is, only that `method` and
                  `searcher` agree (the caller's responsibility; see
                  web/search_api.py).
        meta_mgr: process-singleton MetadataManager exposing
                  parse_full_id_components (used by the group cap). Injected by
                  the caller.
        text: composition source. Mapped to search_composition_logic's
              `full_text` arg. The handler has already stripped + length-
              capped this.
        chunk_size: int in [2, 20] -- handler validated.
        mode: 'exact' | 'variants' | 'fuzzy' (D-02 enum) -- handler validated.
        max_freq: Optional[float]. None -> no high-frequency filtering (passed
                  as a sentinel to search_composition_logic -- see note below).
        boundary_mode: 'full' | 'boundary' | 'combined' (D-03). Other
                       boundary parameters use search_composition_logic
                       defaults.
        restrict_sys_ids: Optional[set] -- handler resolved filters via
                          shared.fjms_service.get_filter_sys_ids BEFORE calling.
                          None means no filter; empty set means filter intersected
                          to nothing (handler short-circuits before calling
                          this function -- service should never receive an
                          explicit empty set in practice).
        method: 'chunk' (default) | 'passage' (Phase 145). Purely a ROUTING
                hint here -- it does NOT change which `searcher` is called
                (the caller already picked the right one); it only selects
                the executor `_run_sync` dispatches into (see `executor`
                below). 'chunk' or omitted is BYTE-FOR-BYTE identical to the
                pre-Phase-145 behavior: `executor` stays unused unless the
                caller also passes one.
        executor: Optional[concurrent.futures.Executor] (Phase 145). Passed
                  straight through to `_run_sync`'s `_executor` kwarg. The
                  chunk path never sets this (stays on the default executor,
                  preserving pre-Phase-145 behavior exactly); the passage path
                  is expected to pass its own dedicated, bounded executor
                  (web/search_api.py's own budget -- see the two-budgets
                  lesson in docs/specs/discovery-budgets.md SS2/SS3: two
                  semaphores over one shared pool are two names for one
                  budget).

    Returns:
        ParallelsResultBundle with main_results / filtered_results / boundary_options
        / truncated_to_200.

        main_results is capped at PARALLELS_GROUP_CAP (200) groups per D-07.
        filtered_results is NOT capped in v7.10 -- it is the high-freq filtered
        set driven by the user's max_freq threshold, and is typically small.
        This is an explicit decision (not an oversight); see ParallelsResultBundle
        docstring for full rationale.
    """
    # max_freq sentinel: search_composition_logic compares `len(hits) > max_freq`
    # internally. None breaks the comparison -- substitute float('inf') to disable
    # high-frequency filtering when the caller did not specify a threshold.
    effective_max_freq = float('inf') if max_freq is None else float(max_freq)

    def _sync_call() -> dict:
        return searcher.search_composition_logic(
            full_text=text,
            chunk_size=chunk_size,
            max_freq=effective_max_freq,
            mode=mode,
            filter_text=None,                     # Not exposed in v7.10 (deferred).
            progress_callback=None,                # Synchronous fan-out -- no progress UI.
            boundary_mode=boundary_mode,
            # Other boundary parameters use search_composition_logic's defaults
            # per D-03 (boundary_delimiter='\\n', boundary_boost=1.5,
            # min_boundary_matches=0, min_delimiter_distance=3). Passing them
            # explicitly with the same values would be a no-op; rely on the
            # function's default arguments instead.
            restrict_sys_ids=restrict_sys_ids,
        )

    # `method` never changes WHICH object gets called (the caller already
    # picked `searcher`); it only decides whether `_run_sync` dispatches into
    # the caller-supplied `executor` or the default one. For method='chunk'
    # (the default), `executor` is None unless a caller explicitly passes one
    # (none do today), so this line is a no-op change from pre-Phase-145
    # behavior -- `_run_sync(_sync_call)` dispatched into the default executor
    # then and still does now.
    result = await _run_sync(_sync_call, _executor=executor if method == 'passage' else None)

    main_results = (result or {}).get('main') or []
    filtered_results = (result or {}).get('filtered') or []
    # NOTE: filtered_results is intentionally NOT capped here (v7.10 decision).
    # filtered_results is driven by the user's max_freq threshold and is typically
    # small. The primary response-size concern (large main result sets) is addressed
    # by the 200-group cap on main_results above. Capping filtered in v7.10 adds
    # implementation complexity for a rare edge case. v7.11 can add a filtered cap
    # if load testing reveals large filtered payloads.

    # D-07 cap on main groups only.
    capped_main, truncated = _cap_main_results_by_group(main_results, meta_mgr)

    # Boundary options for envelope echo (D-06 inherited from Phase 77).
    boundary_options = {
        'boundary_mode': boundary_mode,
        # Other knobs are core defaults per D-03; echo them as well for
        # observability so the skill consumer sees what was actually used.
        'boundary_delimiter': '\n',
        'boundary_boost': 1.5,
        'min_boundary_matches': 0,
        'min_delimiter_distance': 3,
    }

    return ParallelsResultBundle(
        main_results=capped_main,
        filtered_results=filtered_results,
        boundary_options=boundary_options,
        truncated_to_200=truncated,
    )
