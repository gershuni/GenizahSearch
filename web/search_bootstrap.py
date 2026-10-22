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

# The Responsa options a finished search stamps into its URL (search.py's
# history.replaceState), as the engine's option names.
RESPONSA_URL_OPTIONS = ('variants', 'ja', 'flex_spacing', 'bidirectional')


def _matches_saved_responsa_run(initial_query: str | None, initial_responsa: Dict[str, Any] | None,
                                initial_run: str | None, saved_run: Dict[str, Any] | None) -> bool:
    """True when a Responsa URL is the one the saved run stamped.

    A Responsa search stamps ``mode=responsa``, its option flags and a random
    ``run=`` stamp into the URL, and keeps the stamp in the saved run
    (``SearchUIState.last_run['url_stamp']``). Equal values alone are not enough: a
    typed or pasted link with the same query and options, or the snapshot of
    another tab, could have been searched with settings the URL does not carry
    (gap, NOT-words, filters, a refinement). The stamp says it is this run; the
    query and options must still agree with it.
    """
    if not isinstance(saved_run, dict) or saved_run.get('mode') != 'responsa':
        return False
    if not isinstance(initial_run, str) or not initial_run or saved_run.get('url_stamp') != initial_run:
        return False
    if saved_run.get('query') != initial_query:
        return False
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
) -> Dict[str, Any]:
    """Resolve whether persisted search UI state should be reused for this request.

    The ``saved_results_count`` parameter enables browser-Back detection from
    /browse to /search?q=<stamped-query>: when positive and the URL query
    matches ``saved_query``, the saved snapshot is restored (see 75-03-PLAN.md).
    Defaults to 0 so pre-fix callers fall back to the has_route_context path.

    A Responsa search's URL also carries ``mode=responsa``, its options
    (``initial_responsa``, by the engine's option names) and its run stamp
    (``initial_run``); it is Back when the stamp, the query and the options match
    the saved run (``saved_run``, the snapshot's last_run). That Back restores the
    snapshot's filters too: the stamp ties them to this address.
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
    #
    # A Responsa search stamps its mode, options and a run stamp too
    # (q=..&mode=responsa&variants=1&run=..), so its Back is recognised by the
    # saved run instead: the same stamp, query and options.
    if explicit_mode is None:
        url_is_the_saved_search = saved_query is not None and initial_query == saved_query
    elif explicit_mode == 'responsa':
        url_is_the_saved_search = _matches_saved_responsa_run(
            initial_query, initial_responsa, initial_run, saved_run)
    else:
        url_is_the_saved_search = False
    is_back_navigation = (
        not from_browse
        and initial_tag in (None, '')
        and (initial_domain in (None, ''))
        and initial_query is not None
        and initial_query != ''
        and url_is_the_saved_search
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

    # The q-only Back keeps 829cd7cf's rule (the URL holds no proof the filters
    # belong to it). A stamped Responsa Back has that proof: its rows come back with
    # the saved filters (saved as they change), so the page is the one that was left
    # and no restricted result shows under empty filter chips.
    stamped_back = is_back_navigation and explicit_mode == 'responsa'
    return {
        'mode': resolved_mode,
        'query': resolved_query,
        'restore_saved_results': restore_saved_state,
        'restore_saved_filters': restore_saved_state and not bool(from_browse)
                                 and (not is_back_navigation or stamped_back),
        'restore_saved_exclusions': restore_saved_state,
    }
