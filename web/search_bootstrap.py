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

# The Responsa options a search stamps into its URL (search.py's history.replaceState),
# as the engine's option names.
RESPONSA_URL_OPTIONS = ('variants', 'ja', 'flex_spacing', 'bidirectional')


def matches_saved_run(initial_query: str | None, explicit_mode: str | None,
                      initial_responsa: Dict[str, Any] | None, initial_run: str | None,
                      saved_run: Dict[str, Any] | None) -> bool:
    """True when a stamped URL is the address of *saved_run* (a run's last_run).

    Every search stamps its address when it starts: the query, the page's mode, the
    Responsa options and a random ``run=`` code, which its run keeps as
    ``last_run['url_stamp']``. Equal values alone are not enough: a typed or pasted
    link with the same query and mode, or another tab's run, could have been
    searched with settings the URL does not carry (gap, NOT-words, filters, a
    refinement). The code says it is this run; the query, mode and options must
    still agree with it, and the run must carry the filters it was searched with.
    """
    if not isinstance(saved_run, dict) or not isinstance(initial_run, str) or not initial_run:
        return False
    if saved_run.get('url_stamp') != initial_run:
        return False
    if not isinstance(saved_run.get('pre_search_filters'), dict):
        return False
    if saved_run.get('query') != initial_query or saved_run.get('page_mode') != explicit_mode:
        return False
    if explicit_mode != 'responsa':
        return True
    options = saved_run.get('responsa_options')
    if not isinstance(options, dict):
        return False
    url_options = initial_responsa or {}
    return all(bool(options.get(name)) == bool(url_options.get(name)) for name in RESPONSA_URL_OPTIONS)


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
    initial_responsa: Dict[str, Any] | None = None,
    initial_run: str | None = None,
    saved_run: Dict[str, Any] | None = None,
    live_run: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Resolve whether persisted search UI state should be reused for this request.

    A URL a search stamped (``initial_run``, with its mode and Responsa options) is
    Back when it matches the run whose snapshot the page found (``saved_run``: that
    snapshot's last_run, the run's own when it is kept, else the user-storage
    copy's). Its rows come back -- empty ones too -- with the filters saved with the
    run (``restore_run_filters``), never the shared current ones
    (``restore_saved_filters``). When it instead matches a run that is still
    running (``live_run``), the page waits for that search (``rejoin_run``) with the
    run's filters, rather than starting it again.

    A URL with only ``q`` (addresses stamped before runs had codes) is Back when its
    query matches ``saved_query`` and ``saved_results_count`` is positive (see
    75-03-PLAN.md); it restores no filters (829cd7cf).
    """
    explicit_mode = initial_mode if initial_mode in VALID_SEARCH_MODES else None

    # Back-navigation detection. The /search URL was stamped by this session's own
    # history.replaceState when a search started (web/pages/search.py), so a match
    # means the saved snapshot IS the authoritative state for that address.
    #
    # This preserves the 829cd7cf (2026-03-27) intent that genuinely-fresh
    # /search?q=X requests (shared links, homepage nav, different query than
    # saved) do NOT inherit stale filters/exclusions -- those paths fall through
    # to the existing has_route_context branch below.
    plain_route = (
        not from_browse
        and initial_tag in (None, '')
        and (initial_domain in (None, ''))
        and initial_query is not None
        and initial_query != ''
    )
    stamped = explicit_mode is not None and matches_saved_run(
        initial_query, explicit_mode, initial_responsa, initial_run, saved_run)
    if explicit_mode is None:
        url_is_the_saved_search = saved_query is not None and initial_query == saved_query
    else:
        url_is_the_saved_search = stamped
    is_back_navigation = plain_route and url_is_the_saved_search and (stamped or saved_results_count > 0)
    rejoin = (plain_route and not is_back_navigation and explicit_mode is not None
              and matches_saved_run(initial_query, explicit_mode, initial_responsa, initial_run, live_run))

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
        # hits browser Back -- the snapshot's mode must be restored.
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
        'restore_run_filters': (is_back_navigation and stamped) or rejoin,
        'restore_saved_exclusions': restore_saved_state,
        'rejoin_run': rejoin,
    }
