"""Helpers for deterministic search-page bootstrap state."""

from __future__ import annotations

from typing import Any, Dict


VALID_SEARCH_MODES = {
    'exact',
    'variants',
    'variants_extended',
    'variants_maximum',
    'responsa',
    'fuzzy',
    'Regex',
    'Shelfmark',
    'Title',
    'pgp_tags',
}


def resolve_search_bootstrap(
    *,
    initial_query: str | None,
    initial_tag: str | None,
    initial_mode: str | None,
    initial_domain: str | None,
    from_browse: int | None,
    saved_mode: str | None,
    saved_query: str | None,
    saved_results_count: int = 0,
    use_slider: bool,
) -> Dict[str, Any]:
    """Resolve whether persisted search UI state should be reused for this request.

    The ``saved_results_count`` parameter enables browser-Back detection from
    /browse to /search?q=<stamped-query>: when positive and the URL query
    matches ``saved_query``, the saved snapshot is restored (see 75-03-PLAN.md).
    Defaults to 0 so pre-fix callers fall back to the has_route_context path.
    """
    explicit_mode = initial_mode if initial_mode in VALID_SEARCH_MODES else None

    # Back-navigation detection: when the URL carries only `q` (no tag/mode/domain,
    # no from_browse flag) AND that query matches the saved snapshot's query AND
    # the snapshot holds results, treat this as browser-Back from /browse to
    # /search?q=<stamped-query> and restore the saved snapshot. The /search URL
    # was stamped by this session's own history.replaceState at the end of a
    # successful search (web/pages/search.py ~line 4176), so the snapshot IS
    # the authoritative state for that query.
    #
    # This preserves the 829cd7cf (2026-03-27) intent that genuinely-fresh
    # /search?q=X requests (shared links, homepage nav, different query than
    # saved) do NOT inherit stale filters/exclusions — those paths fall through
    # to the existing has_route_context branch below.
    is_back_navigation = (
        not from_browse
        and initial_tag in (None, '')
        and explicit_mode is None
        and (initial_domain in (None, ''))
        and initial_query is not None
        and initial_query != ''
        and saved_query is not None
        and initial_query == saved_query
        and saved_results_count > 0
    )

    has_route_context = bool(from_browse) or any(
        value not in (None, '')
        for value in (initial_query, initial_tag, explicit_mode, initial_domain)
    )
    # Back-navigation is a SPECIAL CASE of has_route_context where restore is
    # still desired. Without this override, the snapshot would be ignored and a
    # fresh search would fire, losing chips/scroll/results/exclusions on every
    # browser Back from /browse to /search.
    restore_saved_state = (not has_route_context) or is_back_navigation

    if initial_tag not in (None, ''):
        resolved_mode = 'pgp_tags'
    elif explicit_mode:
        resolved_mode = explicit_mode
    elif is_back_navigation:
        # Back-navigation: use the saved mode (snapshot is authoritative).
        # Covers the case where user was searching in e.g. 'Title' mode and
        # hits browser Back — the snapshot's mode must be restored.
        resolved_mode = saved_mode or 'exact'
    elif has_route_context:
        # URL-driven searches should be deterministic and not inherit hidden mode state.
        resolved_mode = 'exact'
    else:
        resolved_mode = saved_mode or 'exact'

    if use_slider and resolved_mode in ('variants_extended', 'variants_maximum'):
        resolved_mode = 'variants'

    if initial_query is not None:
        resolved_query = initial_query
    elif restore_saved_state:
        resolved_query = saved_query or ''
    else:
        resolved_query = ''

    return {
        'mode': resolved_mode,
        'query': resolved_query,
        'restore_saved_results': restore_saved_state,
        'restore_saved_filters': restore_saved_state and not bool(from_browse) and not is_back_navigation,
        'restore_saved_exclusions': restore_saved_state,
    }
