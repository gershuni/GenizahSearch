# -*- coding: utf-8 -*-
"""
Shared filter panel logic for search and parallels pages.

Extracts duplicated filter functions that were identical in search.py and parallels.py.
UI layout construction remains in each page -- only data/logic functions are shared.
"""

from nicegui import run
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Module-level pure functions (no NiceGUI UI dependency)
# ============================================================================

def build_domain_options(lang: str) -> dict:
    """Build domain select options from FJMS hierarchy.

    Args:
        lang: Language code ('he' or 'en'). Passed explicitly: inside
              run.io_bound() no visitor is in context, so get_language() there
              gives the process default.
    """
    from shared.fjms_service import get_fjms_service, qualify_domain_name
    fjms = get_fjms_service(thread_safe=True)
    if not fjms.is_available():
        return {}
    hierarchy = fjms.get_domain_hierarchy()
    is_heb = lang == 'he'
    options = {}
    for parent_name, info in hierarchy.items():
        parent_heb = info.get('parent_domain_heb', '')
        parent_count = info.get('count', 0)
        display = parent_heb if is_heb and parent_heb else parent_name
        display += f" ({parent_count:,})"
        options[parent_name] = display
        for child in info.get('children', []):
            child_name = child.get('domain', '')
            child_heb = child.get('domain_heb', '')
            child_count = child.get('count', 0)
            qname = qualify_domain_name(child_name, parent_name)
            if is_heb and child_heb:
                c_label = f"{child_heb} ({parent_heb})" if qname != child_name else child_heb
            else:
                c_label = qname
            c_display = f"  \u2514 {c_label} ({child_count:,})"
            options[qname] = c_display
            # Third level: sub-sub-domains
            for sc in child.get('children', []):
                sc_name = sc.get('domain', '')
                sc_heb = sc.get('domain_heb', '')
                sc_count = sc.get('count', 0)
                sc_qname = qualify_domain_name(sc_name, child_name)
                if is_heb and sc_heb:
                    sc_label = f"{sc_heb} ({child_heb})" if sc_qname != sc_name else sc_heb
                else:
                    sc_label = sc_qname
                sc_display = f"    \u2514 {sc_label} ({sc_count:,})"
                options[sc_qname] = sc_display
    return options


def build_author_options(lang: str, domain=None) -> dict:
    """Build author select options from FJMS.

    Args:
        lang: Language code (currently unused, reserved for future Hebrew author names).
        domain: Optional domain filter (list or string).
    """
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    if not fjms.is_available():
        return {}
    _first_domain = domain[0] if isinstance(domain, list) and domain else (domain or None)
    authors = fjms.get_browse_authors(domain=_first_domain)
    options = {}
    for a in authors:
        pid = a.get('person_id') or a.get('author_id')
        name = a.get('heb_desc') or a.get('eng_desc') or a.get('author_name', '')
        eng = a.get('eng_desc', '')
        count = a.get('count', 0)
        key = str(pid) if pid else name
        display = name
        if eng and eng != name:
            display = f"{name} / {eng}"
        display += f" ({count:,})"
        options[key] = display
    return options


def build_work_options(lang: str, domain=None, author=None) -> dict:
    """Build work select options from FJMS.

    Args:
        lang: Language code (currently unused, reserved for future).
        domain: Optional domain filter (list or string).
        author: Optional author filter (list or string).
    """
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    if not fjms.is_available():
        return {}
    _first_domain = domain[0] if isinstance(domain, list) and domain else (domain or None)
    _first_author = author[0] if isinstance(author, list) and author else (author or None)
    works = fjms.get_browse_works(domain=_first_domain, author=_first_author)
    options = {}
    for w in works:
        tid = w.get('title_id')
        org = w.get('org_title', '')
        eng = w.get('eng_title', '')
        count = w.get('count', 0)
        key = str(tid) if tid else org
        display = org or eng
        if eng and eng != org:
            display = f"{org} / {eng}"
        display += f" ({count:,})"
        options[key] = display
    return options


# The catalogue criteria a "Search only in..." search runs with (PR 1b of
# docs/plans/CATALOGUE_CRITERIA_PLAN.md): one value, ``state.filter_criteria``,
# stored under '{prefix}_filter_criteria'. The keys below are what it replaced.
LEGACY_DATE_KEYS = ('filter_date_from', 'filter_date_to')
LEGACY_SCRIPT_KEYS = ('filter_script_styles', 'filter_script_places')
# Saved measurement filters are not restored (R2, "the least work"): the
# page drops them, and the rows stored with them, before it reads either.
LEGACY_MEASUREMENT_KEYS = (
    'filter_width_min', 'filter_width_max', 'filter_height_min', 'filter_height_max',
    'filter_line_count_min', 'filter_line_count_max', 'filter_line_height_min',
    'filter_line_height_max', 'filter_text_density_min', 'filter_text_density_max',
    'filter_measurement_material',
)


def saved_criteria_of(filters: dict) -> dict:
    """The criteria a saved filter dict (a history entry, a run, an incoming
    handoff) carries: its 'criteria'; a dict saved before the criteria
    existed (no 'criteria' at all, not even empty) has its old date range
    read in as the period of copy instead (D18). Old measurement keys are
    not read (R2)."""
    from shared.catalog_criteria import clean_criteria, period_from_dates
    filters = filters if isinstance(filters, dict) else {}
    if filters.get('criteria') is not None:
        return clean_criteria(filters.get('criteria'), strict=False)[0]
    return period_from_dates(filters.get('date_from'), filters.get('date_to'),
                             bool(filters.get('include_undated')))


def restore_saved_criteria(filters: dict, notify: bool = True) -> dict:
    """``saved_criteria_of`` without the criteria the open catalogue file is
    KNOWN not to answer (rule 9) -- every restore path goes through it (a
    history entry, a run, a page snapshot, a handoff), so none brings back a
    criterion the file cannot answer. Says so when it drops one."""
    criteria, dropped = drop_unsupported_criteria(saved_criteria_of(filters))
    if dropped and notify:
        notify_dropped_filters()
    return criteria


def notify_dropped_filters() -> None:
    """The one notice for saved filters the open catalog cannot answer (#17)."""
    try:
        from nicegui import ui
        from web.translations import tr
        ui.notify(tr('Some saved filters were removed: this catalog data is not available.'),
                  type='info')
    except Exception:  # no page context (a worker, a test): the drop stands, unannounced
        logger.debug("notify_dropped_filters: no page context", exc_info=True)


def drop_unsupported_criteria(criteria: dict) -> tuple[dict, list]:
    """*criteria* without those the open catalogue file is KNOWN not to
    answer (a 5.0.0 file, rule 9), and the keys dropped."""
    from shared.fjms_service import get_fjms_service
    from web.components.criteria_panel import drop_unsupported
    try:
        fjms = get_fjms_service(thread_safe=True)
    except Exception:
        return dict(criteria or {}), []
    return drop_unsupported(criteria, fjms)


def retire_old_saved_filters(storage_prefix: str, reset=None) -> bool:
    """R2: saved measurement filters from before the criteria are not
    restored. When any is set, clear them -- and the search saved with them
    (``reset``: storage key -> its empty value: the rows, and the run that
    would restore them as a finished search with no rows), so that what is
    shown is what runs: the search runs again. Called at page open, before
    the saved rows are read. True when something was retired."""
    from web.safe_storage import safe_user_get as _sg
    pfx = storage_prefix
    stale = [k for k in LEGACY_MEASUREMENT_KEYS if _sg(f'{pfx}_{k}', None) not in (None, [], ())]
    for key in LEGACY_MEASUREMENT_KEYS + LEGACY_SCRIPT_KEYS:
        if _sg(f'{pfx}_{key}', None) is not None:
            persist_value(f'{pfx}_{key}', None)
    if stale:
        for key, empty in (reset or {}).items():
            persist_value(key, empty)
    return bool(stale)


def build_filter_summary(filters: dict, tr_func, get_language_func, max_len: int = 50) -> str:
    """Build compact filter summary like [include: Bible, Tosefta. 1000-1300].

    Args:
        filters: Dict with keys: include_mode, domains, authors, works,
                 material_exclude, material_include, criteria (an entry saved
                 before the criteria: date_from, date_to).
        tr_func: Translation function (page-context-dependent).
        get_language_func: Language getter function (page-context-dependent).
        max_len: Maximum summary string length.
    """
    if not filters:
        return ''
    prefix = tr_func('include') if filters.get('include_mode', True) else tr_func('exclude')
    # Build en->heb domain name map from cached hierarchy
    domain_heb_map = {}
    if get_language_func() == 'he' and filters.get('domains'):
        try:
            from shared.fjms_service import get_fjms_service, qualify_domain_name
            fjms = get_fjms_service(thread_safe=True)
            if fjms.is_available():
                for pn, info in fjms.get_domain_hierarchy().items():
                    p_heb = info.get('parent_domain_heb', '')
                    if p_heb:
                        domain_heb_map[pn] = p_heb
                    for ch in info.get('children', []):
                        c_heb = ch.get('domain_heb', '')
                        qn = qualify_domain_name(ch['domain'], pn)
                        if c_heb:
                            domain_heb_map[qn] = f"{c_heb} ({p_heb})" if qn != ch['domain'] else c_heb
                        for sc in ch.get('children', []):
                            s_heb = sc.get('domain_heb', '')
                            sq = qualify_domain_name(sc['domain'], ch['domain'])
                            if s_heb:
                                domain_heb_map[sq] = f"{s_heb} ({c_heb})" if sq != sc['domain'] else s_heb
        except Exception:
            pass  # Catalog/FJMS operation failed; continue with available data
    parts = []
    for d in filters.get('domains', []):
        parts.append(domain_heb_map.get(str(d), str(d)))
    n_auth = len(filters.get('authors', []))
    if n_auth:
        parts.append(f"{tr_func('Author')} \u00d7{n_auth}")
    n_work = len(filters.get('works', []))
    if n_work:
        parts.append(f"{tr_func('Work')} \u00d7{n_work}")
    if filters.get('material_exclude'):
        parts.append(tr_func("No printed"))
    elif filters.get('material_include'):
        parts.append(tr_func("Printed only"))
    # The criteria: one chip text each. Exclude mode never turns them around
    # (rule 7), so they are never listed under "exclude".
    criteria_parts = []
    criteria = saved_criteria_of(filters)
    if criteria:
        lang = get_language_func()
        try:
            from shared.fjms_service import get_fjms_service
            chips = get_fjms_service(thread_safe=True).criteria_chips(criteria, lang)
        except Exception:
            from shared.catalog_criteria import criteria_chips
            chips = criteria_chips(criteria, lang)
        criteria_parts = [text for _key, text in chips]
    if filters.get('include_mode', True):
        parts += criteria_parts
        criteria_parts = []
    groups = [(prefix, parts), (tr_func('include'), criteria_parts)]
    summary = ' '.join(f"[{p}: {', '.join(g)}]" for p, g in groups if g)
    if not summary:
        return ''
    if len(summary) > max_len:
        cut = summary[:max_len - 4].rstrip()
        if cut.endswith(']'):  # cut between two brackets: continue the first
            cut = cut[:-1]
        summary = cut + '...]'
    return summary


def has_active_filters(state) -> bool:
    """Check if any pre-search filters are active on the given state object.

    Args:
        state: Any object with filter_domains, filter_authors, filter_works,
               filter_material_exclude, filter_text_all, filter_text_any,
               filter_text_not and filter_criteria attributes.
    """
    from shared.catalog_criteria import clean_criteria
    return any([
        state.filter_domains,
        state.filter_authors,
        state.filter_works,
        state.filter_material_exclude,
        state.filter_text_all,
        state.filter_text_any,
        state.filter_text_not,
        bool(clean_criteria(getattr(state, 'filter_criteria', None), strict=False)[0]),
    ])


def persist_value(key, value):
    """Save to storage if session persistence is enabled.

    2026-05-12 Codex 3rd-pass CRITICAL fix: routed through safe_user_get /
    safe_user_set because this is also called during bootstrap from
    consume_incoming_filters (browse → search/parallels handoff), so a
    pruned-session race could 500 the page on the WRITE path too —
    fixing only reads at this site was insufficient.
    """
    from web.safe_storage import safe_user_get, safe_user_set
    if safe_user_get('session_persistence_enabled', True):
        safe_user_set(key, value)


# ============================================================================
# Session state functions
# ============================================================================

def load_filter_state(state, storage_prefix: str) -> list:
    """Restore filter state from session storage.

    2026-05-12: routed through ``safe_user_get`` because this is called
    from search/parallels page bootstrap. A prune_user_storage race here
    would otherwise return 500 instead of falling back to defaults.

    Args:
        state: State object to populate with filter values.
        storage_prefix: Storage key prefix (e.g., 'search' or 'parallels').

    Returns:
        The keys of saved criteria removed (from state AND from storage)
        because the open catalog is KNOWN not to have that data (rule 9,
        #17); empty otherwise -- including when there is no catalog at all,
        which may be temporary, so nothing is dropped then. The caller shows
        one notice when the list is non-empty.

    A session saved before the criteria keeps its date range as the period
    of copy (D18); its measurement filters were retired before this runs
    (``retire_old_saved_filters``, R2).
    """
    from web.safe_storage import safe_user_get as _sg
    pfx = storage_prefix
    # Migrate from legacy single-value keys to multi-select lists
    _legacy_domain = _sg(f'{pfx}_filter_domain', None)
    _legacy_author = _sg(f'{pfx}_filter_author', None)
    _legacy_work = _sg(f'{pfx}_filter_work', None)
    _fd = _sg(f'{pfx}_filter_domains')
    state.filter_domains = _fd if _fd is not None else ([_legacy_domain] if _legacy_domain else [])
    _fa = _sg(f'{pfx}_filter_authors')
    state.filter_authors = _fa if _fa is not None else ([_legacy_author] if _legacy_author else [])
    _fw = _sg(f'{pfx}_filter_works')
    state.filter_works = _fw if _fw is not None else ([_legacy_work] if _legacy_work else [])
    state.filter_include_mode = _sg(f'{pfx}_filter_include_mode', True)
    _fme = _sg(f'{pfx}_filter_material_exclude')
    state.filter_material_exclude = _fme if _fme is not None else []
    _fta = _sg(f'{pfx}_filter_text_all')
    state.filter_text_all = _fta if _fta is not None else []
    _ftany = _sg(f'{pfx}_filter_text_any')
    state.filter_text_any = _ftany if _ftany is not None else []
    _ftn = _sg(f'{pfx}_filter_text_not')
    state.filter_text_not = _ftn if _ftn is not None else []
    # Catalogue criteria; a session from before them: its dates (D18).
    saved = {'criteria': _sg(f'{pfx}_filter_criteria', None),
             'date_from': _sg(f'{pfx}_filter_date_from', None),
             'date_to': _sg(f'{pfx}_filter_date_to', None)}
    criteria = saved_criteria_of(saved)
    if saved['date_from'] is not None or saved['date_to'] is not None:
        persist_value(f'{pfx}_filter_date_from', None)
        persist_value(f'{pfx}_filter_date_to', None)
        persist_value(f'{pfx}_filter_criteria', criteria)

    # #17 / rule 9: a saved criterion the open catalog is known not to
    # answer would make every search fail; drop it here, once, and say so.
    criteria, dropped = drop_unsupported_criteria(criteria)
    if dropped:
        persist_value(f'{pfx}_filter_criteria', criteria)
    state.filter_criteria = criteria
    return dropped


def corpus_universe():
    """The corpus list for get_filter_sys_ids (D17 of
    docs/plans/CATALOGUE_CRITERIA_PLAN.md: uncatalogued manuscripts pass
    filters that only exclude), as a callable the service calls only when
    it needs it, or None before the app state has a metadata manager."""
    from web.state import state as _app_state
    return getattr(getattr(_app_state, 'meta_mgr', None), 'corpus_sys_ids', None)


def filter_unavailable_message(exc, tr_func) -> str:
    """The notice for a search not run because its filters could not be
    applied (#17), chosen by ``FilterUnavailable.reason``."""
    if getattr(exc, 'reason', 'query_failed') in ('query_failed', 'corpus_unavailable'):
        return tr_func("The filters could not be applied, so the search was not run. "
                       "Try again, or remove the filters.")
    return tr_func("The catalog data these filters need is not available, so the search "
                   "was not run. Remove the filters to search.")


def consume_incoming_filters(state, storage_prefix: str, require_from_browse: bool = False) -> bool:
    """Consume incoming filters from catalog browse navigation.

    Args:
        state: State object to populate.
        storage_prefix: Storage key prefix (e.g., 'search' or 'parallels').
        require_from_browse: If True, only consume when 'from_browse' flag is set
                            (search page behavior). If False, consume whenever
                            incoming_filters exist (parallels page behavior).

    Returns:
        True if filters were consumed, False otherwise.
    """
    from web.safe_storage import safe_user_get as _sg
    if require_from_browse:
        # Search page: only consume if from_browse flag is set (passed via URL param)
        # The caller must check this condition before calling
        incoming = _sg('incoming_filters', {})
        if not incoming:
            return False
    else:
        # Parallels page: consume whenever incoming_filters exist
        if not _sg('incoming_filters'):
            return False
        incoming = _sg('incoming_filters', {})
        if not incoming:
            return False

    pfx = storage_prefix
    if incoming.get('domain'):
        state.filter_domains = [incoming['domain']]
        persist_value(f'{pfx}_filter_domains', [incoming['domain']])
    if incoming.get('author'):
        state.filter_authors = [str(incoming['author'])]
        persist_value(f'{pfx}_filter_authors', [str(incoming['author'])])
    if incoming.get('work'):
        state.filter_works = [str(incoming['work'])]
        persist_value(f'{pfx}_filter_works', [str(incoming['work'])])
    if incoming.get('material_exclude'):
        state.filter_material_exclude = incoming['material_exclude']
        persist_value(f'{pfx}_filter_material_exclude', incoming['material_exclude'])
    # Catalogue criteria: a browse list hands them on unchanged (§6.3), to
    # /search and /parallels alike.
    # Authoritative even when empty: a list with no criteria must not search
    # under criteria an earlier visit saved.
    criteria = restore_saved_criteria(incoming)
    state.filter_criteria = criteria
    persist_value(f'{pfx}_filter_criteria', criteria)
    # GAP-F (2026-06-28): thread library selection from catalog browse to /search.
    # DMF (T-130-02-06 / HIGH-1): browse->search handoff stamps mode='show_only' and
    # persists the new {'mode','codes'} dict shape (NOT a bare list).  Without the mode
    # stamp, Plan 01's library_mode='hide' default would silently mis-interpret the
    # Show-only intent as a Hide-set after the handoff.
    # Persist key is the FLAT literal 'search_library_filter' (NOT {pfx}_filter_*) because
    # search.py loads it via _safe_get('search_library_filter', None).
    # setattr (live this render) + persist (durable for the next fresh render) are BOTH
    # required for the persist→reload lifecycle.
    # Only persist 'search_library_filter' when storage_prefix == 'search'.
    # The parallels page has its own 'parallels_library_filter' key (Phase 131);
    # writing 'search_library_filter' during a parallels handoff would silently
    # infect a subsequent fresh /search render.
    if incoming.get('library_filter') and storage_prefix == 'search':
        # HIGH-1: sanitize incoming codes against the canonical set; exclude 'LOCAL'
        # (desktop-only library code that must never appear as a web filter option).
        # Codex code-review (R2): route through the shared sanitizer so a malformed
        # incoming value (non-list scalar, dict items, etc.) cannot raise TypeError at
        # this entry point — same hardening as search.py's restore path.
        from shared.browse_map_utils import sanitize_library_codes
        _lf_raw = incoming.get('library_filter')
        # Task 1 (131-04): accept both the new dict {mode, codes} shape and the legacy
        # bare-list shape for backward compatibility (Codex R1 HIGH #3 + MED #7).
        if isinstance(_lf_raw, dict):
            # New dict shape from catalog handoff: read mode + codes, validate mode.
            _lib_mode = _lf_raw.get('mode', 'hide')
            _lib_mode = _lib_mode if _lib_mode in ('show_only', 'hide') else 'hide'
            _lib_codes = sanitize_library_codes(_lf_raw.get('codes'))
        else:
            # Legacy bare-list shape: always interpret as Show-only allowlist (backward compat).
            _lib_codes = sanitize_library_codes(_lf_raw)
            _lib_mode = 'show_only'
        # WR-03: only apply when sanitized codes are non-empty (or hide-with-codes).
        # If all incoming codes were LOCAL/invalid, leave state as neutral (hide/[]).
        if _lib_codes:
            state.library_filter = _lib_codes
            state.library_mode = _lib_mode
            # Persist as {'mode','codes'} dict shape (D-09) so the restore path reads
            # it back correctly with the correct mode (not just Show-only).
            persist_value('search_library_filter', {'mode': _lib_mode, 'codes': _lib_codes})
        # else: all codes sanitized away — leave state as neutral (hide/[]); no persist needed.
    # Clear incoming_filters from storage after consuming.
    # 2026-05-12 Codex 3rd-pass CRITICAL: safe_user_pop so prune races don't 500.
    from web.safe_storage import safe_user_pop
    safe_user_pop('incoming_filters', None)
    return True


# ============================================================================
# Async recompute function
# ============================================================================

async def recompute_filter_count(state, update_chip_bar_fn, on_state=None):
    """Recompute manuscript count for current filters (background).

    Uses a generation counter to prevent out-of-order completion races.

    Args:
        state: State object with filter attributes + filter_manuscript_count,
               restrict_sys_ids, and _filter_recompute_gen (auto-created).
        update_chip_bar_fn: Callback to update the chip bar UI after recompute.
        on_state: Optional callback receiving a status string for per-op feedback
                  (#11): 'pending' when the background compute starts, 'done' on
                  success, 'error' if the FJMS lookup raised -- including
                  FilterUnavailable, a lookup that could not run (#17). Stale
                  completions (superseded by a newer recompute) do NOT emit a
                  terminal state. Both /search and /parallels pass it.
    """
    def _emit(status):
        if on_state is not None:
            try:
                on_state(status)
            except Exception:
                logger.debug("recompute_filter_count on_state(%s) raised", status, exc_info=True)

    # Generation guard: increment BEFORE any early return so in-flight
    # older recomputes see a newer generation and discard their results.
    if not hasattr(state, '_filter_recompute_gen'):
        state._filter_recompute_gen = 0
    state._filter_recompute_gen += 1
    gen = state._filter_recompute_gen

    if not has_active_filters(state):
        state.filter_manuscript_count = None
        state.restrict_sys_ids = None
        _emit('done')
        update_chip_bar_fn()
        return

    _emit('pending')

    from shared.fjms_service import get_fjms_service

    include_mode = state.filter_include_mode
    _domains = state.filter_domains or None
    _authors = state.filter_authors or None
    _works = state.filter_works or None
    # Snapshot the filter values before io_bound (they live on state, not closure)
    import copy as _copy
    _material_exclude = state.filter_material_exclude or None
    _text_all = state.filter_text_all or None
    _text_any = state.filter_text_any or None
    _text_not = state.filter_text_not or None
    _criteria = _copy.deepcopy(getattr(state, 'filter_criteria', None) or {}) or None

    def _compute():
        # No is_available() short-cut: with no sidecar get_filter_sys_ids
        # raises FilterUnavailable, which the except below shows as 'error'
        # -- never as "all manuscripts" (#17).
        fjms = get_fjms_service(thread_safe=True)
        kwargs = dict(
            material_exclude=_material_exclude,
            text_all=_text_all,
            text_any=_text_any,
            text_not=_text_not,
            criteria=_criteria,
        )
        if include_mode:
            kwargs['domains'] = _domains
            kwargs['authors'] = _authors
            kwargs['works'] = _works
        else:
            kwargs['domains_exclude'] = _domains
            kwargs['authors_exclude'] = _authors
            kwargs['works_exclude'] = _works
        return fjms.get_filter_sys_ids(universe=corpus_universe(), **kwargs)

    try:
        result = await run.io_bound(_compute)
    except Exception:
        logger.error("recompute_filter_count: filter lookup failed", exc_info=True)
        # Stale guard: a newer recompute supersedes this failure.
        if state._filter_recompute_gen != gen:
            return
        state.filter_manuscript_count = None
        state.restrict_sys_ids = None
        _emit('error')
        update_chip_bar_fn()
        return

    # Stale guard: skip update if a newer recompute was triggered
    if state._filter_recompute_gen != gen:
        return

    if result is not None:
        state.filter_manuscript_count = len(result)
        state.restrict_sys_ids = result
    else:
        state.filter_manuscript_count = None
        state.restrict_sys_ids = None
    _emit('done')
    update_chip_bar_fn()


# ============================================================================
# Change handler factory
# ============================================================================

def create_filter_handlers(state, storage_prefix, filter_refs, refresh_author_fn,
                           refresh_work_fn, recompute_fn, update_chip_fn) -> dict:
    """Create filter change handler functions.

    Args:
        state: State object with filter attributes.
        storage_prefix: Storage key prefix (e.g., 'search' or 'parallels').
        filter_refs: Dict of UI element references (domain, author, work, mode,
                     date_from, date_to, exclude_printed).
        refresh_author_fn: Async function to refresh author select options.
        refresh_work_fn: Async function to refresh work select options.
        recompute_fn: Async function to recompute filter counts.
        (The criteria are set by ``apply_criteria``, from the criteria panel.)
        update_chip_fn: Function to update the chip bar UI.

    Returns:
        Dict of handler functions keyed by event name.
    """
    pfx = storage_prefix

    async def on_domain_change(e=None):
        val = filter_refs['domain'].value or []
        state.filter_domains = val if isinstance(val, list) else [val] if val else []
        persist_value(f'{pfx}_filter_domains', state.filter_domains)
        await refresh_author_fn()
        await refresh_work_fn()
        await recompute_fn()
        update_chip_fn()

    async def on_author_change(e=None):
        val = filter_refs['author'].value or []
        state.filter_authors = val if isinstance(val, list) else [val] if val else []
        persist_value(f'{pfx}_filter_authors', state.filter_authors)
        await refresh_work_fn()
        await recompute_fn()
        update_chip_fn()

    async def on_work_change(e=None):
        val = filter_refs['work'].value or []
        state.filter_works = val if isinstance(val, list) else [val] if val else []
        persist_value(f'{pfx}_filter_works', state.filter_works)
        await recompute_fn()
        update_chip_fn()

    async def on_mode_change(e=None):
        state.filter_include_mode = filter_refs['mode'].value
        persist_value(f'{pfx}_filter_include_mode', state.filter_include_mode)
        await recompute_fn()
        update_chip_fn()

    async def on_exclude_printed_change(e=None):
        if filter_refs['exclude_printed'].value:
            if 'Printed' not in state.filter_material_exclude:
                state.filter_material_exclude.append('Printed')
        else:
            if 'Printed' in state.filter_material_exclude:
                state.filter_material_exclude.remove('Printed')
        persist_value(f'{pfx}_filter_material_exclude', state.filter_material_exclude)
        await recompute_fn()
        update_chip_fn()

    return {
        'on_domain_change': on_domain_change,
        'on_author_change': on_author_change,
        'on_work_change': on_work_change,
        'on_mode_change': on_mode_change,
        'on_exclude_printed_change': on_exclude_printed_change,
    }


def apply_criteria(state, storage_prefix: str, criteria) -> bool:
    """Set the page's criteria (the panel's Apply, a chip removed, Clear
    All) and save them. True when they changed."""
    from shared.catalog_criteria import clean_criteria
    criteria = clean_criteria(criteria, strict=False)[0]
    if criteria == (getattr(state, 'filter_criteria', None) or {}):
        return False
    state.filter_criteria = criteria
    persist_value(f'{storage_prefix}_filter_criteria', criteria)
    return True
