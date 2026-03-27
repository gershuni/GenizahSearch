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
    use_slider: bool,
) -> Dict[str, Any]:
    """Resolve whether persisted search UI state should be reused for this request."""
    explicit_mode = initial_mode if initial_mode in VALID_SEARCH_MODES else None
    has_route_context = bool(from_browse) or any(
        value not in (None, '')
        for value in (initial_query, initial_tag, explicit_mode, initial_domain)
    )
    restore_saved_state = not has_route_context

    if initial_tag not in (None, ''):
        resolved_mode = 'pgp_tags'
    elif explicit_mode:
        resolved_mode = explicit_mode
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
        'restore_saved_filters': restore_saved_state and not bool(from_browse),
        'restore_saved_exclusions': restore_saved_state,
    }
