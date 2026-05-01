# -*- coding: utf-8 -*-
"""Phase 80 D-11 -- pure-data parallels execution + cap for POST /api/parallels.

Mirrors shared/browse_service.py (Phase 79 D-23 extraction precedent): pure-data
async fan-out, importable from shared/, with no UI-framework dependency.

Statelessness contract (D-20 inherited from Phase 78 / D-22 inherited from Phase 79):
- Reads via the process-singleton SearchEngine (web.state).
- MUST NOT touch any per-session UI state -- last-results caches, parallels-results
  caches, current-search-query, browser-storage, request-cookies, or any UI-coupled
  page state. Verified by grep at acceptance time.

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
from typing import Optional

logger = logging.getLogger(__name__)


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


async def _run_sync(func, *args, **kwargs):
    """Run blocking sync work in the default executor.

    R-09 note: asyncio.wait_for() applied around this WOULD cancel the awaiting
    coroutine but NOT the underlying thread; v7.10 imposes no fan-out timeout
    (rate limiter is the load shield).
    """
    loop = asyncio.get_event_loop()
    if kwargs:
        # run_in_executor signature is (executor, func, *args). Wrap kwargs.
        from functools import partial
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))
    return await loop.run_in_executor(None, func, *args)


def _cap_main_results_by_group(
    main_results: list[dict],
    cap: int = PARALLELS_GROUP_CAP,
) -> tuple[list[dict], bool]:
    """Apply D-07 group cap.

    Groups main_results by sys_id (using the same helper the serializer uses
    so cap and envelope group identically), sorts groups desc by aggregate_score
    (sum of per-row final_score within the group, falling back to score), takes
    top `cap` groups, flattens back to a row list.

    Returns (capped_rows, truncated_flag). truncated_flag=True iff raw group
    count exceeded `cap`.

    NOTE: This cap applies ONLY to main_results. filtered_results is NOT capped
    in v7.10 -- see ParallelsResultBundle docstring for rationale.
    """
    if not main_results:
        return [], False

    # Late import to keep service import-time fast and avoid circular imports.
    # _group_parallels_by_sys_id needs meta_mgr to parse uid components -- use
    # the process-singleton from web.state.
    from shared.search_serializer import _group_parallels_by_sys_id
    from web.state import state as _state

    groups = _group_parallels_by_sys_id(main_results, meta_mgr=_state.meta_mgr)

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
    text: str,
    chunk_size: int,
    mode: str,
    max_freq: Optional[float] = None,
    boundary_mode: str = 'full',
    restrict_sys_ids: Optional[set] = None,
) -> ParallelsResultBundle:
    """Run search_composition_logic via run_in_executor + apply group cap.

    All input validation (text length, chunk_size bounds, mode enum, etc.) is
    the route handler's responsibility (Plan 03). This function trusts its
    inputs and only translates them to search_composition_logic's call shape
    + applies the D-07 group cap on the way out.

    Args:
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

    # Late import -- process-singleton SearchEngine. Same pattern as Phase 79.
    from web.state import state

    def _sync_call() -> dict:
        return state.searcher.search_composition_logic(
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

    result = await _run_sync(_sync_call)

    main_results = (result or {}).get('main') or []
    filtered_results = (result or {}).get('filtered') or []
    # NOTE: filtered_results is intentionally NOT capped here (v7.10 decision).
    # filtered_results is driven by the user's max_freq threshold and is typically
    # small. The primary response-size concern (large main result sets) is addressed
    # by the 200-group cap on main_results above. Capping filtered in v7.10 adds
    # implementation complexity for a rare edge case. v7.11 can add a filtered cap
    # if load testing reveals large filtered payloads.

    # D-07 cap on main groups only.
    capped_main, truncated = _cap_main_results_by_group(main_results)

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
