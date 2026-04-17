# -*- coding: utf-8 -*-
"""
Shared filter panel logic for search and parallels pages.

Extracts duplicated filter functions that were identical in search.py and parallels.py.
UI layout construction remains in each page -- only data/logic functions are shared.
"""

from nicegui import app, run
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Module-level pure functions (no NiceGUI UI dependency)
# ============================================================================

def build_domain_options(lang: str) -> dict:
    """Build domain select options from FJMS hierarchy.

    Args:
        lang: Language code ('he' or 'en'). Passed explicitly to avoid
              calling get_language() inside run.io_bound() which is process-global.
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


def build_filter_summary(filters: dict, tr_func, get_language_func, max_len: int = 50) -> str:
    """Build compact filter summary like [include: Bible, Tosefta. 1000-1300].

    Args:
        filters: Dict with keys: include_mode, domains, authors, works,
                 date_from, date_to, material_exclude, material_include.
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
    df, dt = filters.get('date_from'), filters.get('date_to')
    if df and dt:
        parts.append(f"{df}-{dt}")
    elif df:
        parts.append(f"{df}+")
    elif dt:
        parts.append(f"-{dt}")
    if filters.get('material_exclude'):
        parts.append(tr_func("No printed"))
    elif filters.get('material_include'):
        parts.append(tr_func("Printed only"))
    if not parts:
        return ''
    summary = f"[{prefix}: {', '.join(parts)}]"
    if len(summary) > max_len:
        summary = summary[:max_len - 4] + '...]'
    return summary


def has_active_filters(state) -> bool:
    """Check if any pre-search filters are active on the given state object.

    Args:
        state: Any object with filter_domains, filter_authors, filter_works,
               filter_date_from, filter_date_to, filter_material_exclude,
               filter_text_all, filter_text_any, filter_text_not attributes.
    """
    return any([
        state.filter_domains,
        state.filter_authors,
        state.filter_works,
        state.filter_date_from is not None,
        state.filter_date_to is not None,
        state.filter_material_exclude,
        state.filter_text_all,
        state.filter_text_any,
        state.filter_text_not,
        # Measurement filters (Phase 54)
        getattr(state, 'filter_width_min', None) is not None,
        getattr(state, 'filter_width_max', None) is not None,
        getattr(state, 'filter_height_min', None) is not None,
        getattr(state, 'filter_height_max', None) is not None,
        getattr(state, 'filter_line_count_min', None) is not None,
        getattr(state, 'filter_line_count_max', None) is not None,
        getattr(state, 'filter_line_height_min', None) is not None,
        getattr(state, 'filter_line_height_max', None) is not None,
        getattr(state, 'filter_text_density_min', None) is not None,
        getattr(state, 'filter_text_density_max', None) is not None,
        bool(getattr(state, 'filter_measurement_material', None)),
    ])


def persist_value(key, value):
    """Save to storage if session persistence is enabled."""
    if app.storage.user.get('session_persistence_enabled', True):
        app.storage.user[key] = value


# ============================================================================
# Session state functions
# ============================================================================

def load_filter_state(state, storage_prefix: str):
    """Restore filter state from session storage.

    Args:
        state: State object to populate with filter values.
        storage_prefix: Storage key prefix (e.g., 'search' or 'parallels').
    """
    pfx = storage_prefix
    # Migrate from legacy single-value keys to multi-select lists
    _legacy_domain = app.storage.user.get(f'{pfx}_filter_domain', None)
    _legacy_author = app.storage.user.get(f'{pfx}_filter_author', None)
    _legacy_work = app.storage.user.get(f'{pfx}_filter_work', None)
    _fd = app.storage.user.get(f'{pfx}_filter_domains')
    state.filter_domains = _fd if _fd is not None else ([_legacy_domain] if _legacy_domain else [])
    _fa = app.storage.user.get(f'{pfx}_filter_authors')
    state.filter_authors = _fa if _fa is not None else ([_legacy_author] if _legacy_author else [])
    _fw = app.storage.user.get(f'{pfx}_filter_works')
    state.filter_works = _fw if _fw is not None else ([_legacy_work] if _legacy_work else [])
    state.filter_include_mode = app.storage.user.get(f'{pfx}_filter_include_mode', True)
    state.filter_date_from = app.storage.user.get(f'{pfx}_filter_date_from', None)
    state.filter_date_to = app.storage.user.get(f'{pfx}_filter_date_to', None)
    _fme = app.storage.user.get(f'{pfx}_filter_material_exclude')
    state.filter_material_exclude = _fme if _fme is not None else []
    _fta = app.storage.user.get(f'{pfx}_filter_text_all')
    state.filter_text_all = _fta if _fta is not None else []
    _ftany = app.storage.user.get(f'{pfx}_filter_text_any')
    state.filter_text_any = _ftany if _ftany is not None else []
    _ftn = app.storage.user.get(f'{pfx}_filter_text_not')
    state.filter_text_not = _ftn if _ftn is not None else []
    # Measurement filters (Phase 54)
    state.filter_width_min = app.storage.user.get(f'{pfx}_filter_width_min', None)
    state.filter_width_max = app.storage.user.get(f'{pfx}_filter_width_max', None)
    state.filter_height_min = app.storage.user.get(f'{pfx}_filter_height_min', None)
    state.filter_height_max = app.storage.user.get(f'{pfx}_filter_height_max', None)
    state.filter_line_count_min = app.storage.user.get(f'{pfx}_filter_line_count_min', None)
    state.filter_line_count_max = app.storage.user.get(f'{pfx}_filter_line_count_max', None)
    state.filter_line_height_min = app.storage.user.get(f'{pfx}_filter_line_height_min', None)
    state.filter_line_height_max = app.storage.user.get(f'{pfx}_filter_line_height_max', None)
    state.filter_text_density_min = app.storage.user.get(f'{pfx}_filter_text_density_min', None)
    state.filter_text_density_max = app.storage.user.get(f'{pfx}_filter_text_density_max', None)
    _fmm = app.storage.user.get(f'{pfx}_filter_measurement_material')
    state.filter_measurement_material = _fmm if _fmm is not None else []


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
    if require_from_browse:
        # Search page: only consume if from_browse flag is set (passed via URL param)
        # The caller must check this condition before calling
        incoming = app.storage.user.get('incoming_filters', {})
        if not incoming:
            return False
    else:
        # Parallels page: consume whenever incoming_filters exist
        if not app.storage.user.get('incoming_filters'):
            return False
        incoming = app.storage.user.get('incoming_filters', {})
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
    if incoming.get('date_from') is not None:
        state.filter_date_from = int(incoming['date_from'])
        persist_value(f'{pfx}_filter_date_from', int(incoming['date_from']))
    if incoming.get('date_to') is not None:
        state.filter_date_to = int(incoming['date_to'])
        persist_value(f'{pfx}_filter_date_to', int(incoming['date_to']))
    if incoming.get('material_exclude'):
        state.filter_material_exclude = incoming['material_exclude']
        persist_value(f'{pfx}_filter_material_exclude', incoming['material_exclude'])
    # Clear incoming_filters from storage after consuming
    app.storage.user.pop('incoming_filters', None)
    return True


# ============================================================================
# Async recompute function
# ============================================================================

async def recompute_filter_count(state, update_chip_bar_fn):
    """Recompute manuscript count for current filters (background).

    Uses a generation counter to prevent out-of-order completion races.

    Args:
        state: State object with filter attributes + filter_manuscript_count,
               restrict_sys_ids, and _filter_recompute_gen (auto-created).
        update_chip_bar_fn: Callback to update the chip bar UI after recompute.
    """
    # Generation guard: increment BEFORE any early return so in-flight
    # older recomputes see a newer generation and discard their results.
    if not hasattr(state, '_filter_recompute_gen'):
        state._filter_recompute_gen = 0
    state._filter_recompute_gen += 1
    gen = state._filter_recompute_gen

    if not has_active_filters(state):
        state.filter_manuscript_count = None
        state.restrict_sys_ids = None
        update_chip_bar_fn()
        return

    from shared.fjms_service import get_fjms_service

    include_mode = state.filter_include_mode
    _domains = state.filter_domains or None
    _authors = state.filter_authors or None
    _works = state.filter_works or None
    # Snapshot date/text filter values before io_bound (they live on state, not closure)
    _date_from = state.filter_date_from
    _date_to = state.filter_date_to
    _material_exclude = state.filter_material_exclude or None
    _text_all = state.filter_text_all or None
    _text_any = state.filter_text_any or None
    _text_not = state.filter_text_not or None
    # Measurement filter snapshots (Phase 54)
    _width_min = getattr(state, 'filter_width_min', None)
    _width_max = getattr(state, 'filter_width_max', None)
    _height_min = getattr(state, 'filter_height_min', None)
    _height_max = getattr(state, 'filter_height_max', None)
    _line_count_min = getattr(state, 'filter_line_count_min', None)
    _line_count_max = getattr(state, 'filter_line_count_max', None)
    _line_height_min = getattr(state, 'filter_line_height_min', None)
    _line_height_max = getattr(state, 'filter_line_height_max', None)
    _text_density_min = getattr(state, 'filter_text_density_min', None)
    _text_density_max = getattr(state, 'filter_text_density_max', None)
    _measurement_material = getattr(state, 'filter_measurement_material', None) or None

    def _compute():
        fjms = get_fjms_service(thread_safe=True)
        if not fjms.is_available():
            return None
        kwargs = dict(
            date_from=_date_from,
            date_to=_date_to,
            material_exclude=_material_exclude,
            text_all=_text_all,
            text_any=_text_any,
            text_not=_text_not,
            width_min=_width_min, width_max=_width_max,
            height_min=_height_min, height_max=_height_max,
            line_count_min=_line_count_min, line_count_max=_line_count_max,
            line_height_min=_line_height_min, line_height_max=_line_height_max,
            text_density_min=_text_density_min, text_density_max=_text_density_max,
            measurement_material=_measurement_material,
        )
        if include_mode:
            kwargs['domains'] = _domains
            kwargs['authors'] = _authors
            kwargs['works'] = _works
        else:
            kwargs['domains_exclude'] = _domains
            kwargs['authors_exclude'] = _authors
            kwargs['works_exclude'] = _works
        return fjms.get_filter_sys_ids(**kwargs)

    result = await run.io_bound(_compute)

    # Stale guard: skip update if a newer recompute was triggered
    if state._filter_recompute_gen != gen:
        return

    if result is not None:
        state.filter_manuscript_count = len(result)
        state.restrict_sys_ids = result
    else:
        state.filter_manuscript_count = None
        state.restrict_sys_ids = None
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

    async def on_date_from_change(e=None):
        val = filter_refs['date_from'].value
        state.filter_date_from = int(val) if val is not None and val != '' else None
        persist_value(f'{pfx}_filter_date_from', state.filter_date_from)
        await recompute_fn()
        update_chip_fn()

    async def on_date_to_change(e=None):
        val = filter_refs['date_to'].value
        state.filter_date_to = int(val) if val is not None and val != '' else None
        persist_value(f'{pfx}_filter_date_to', state.filter_date_to)
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
        'on_date_from_change': on_date_from_change,
        'on_date_to_change': on_date_to_change,
        'on_exclude_printed_change': on_exclude_printed_change,
    }
