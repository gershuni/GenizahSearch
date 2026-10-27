# -*- coding: utf-8 -*-
"""
Visual Similarity Dialog Component — Join Discovery Workbench

Displays ranked visual similarity suggestions from FJMS image analysis
alongside the original manuscript in a side-by-side layout.

Left pane: Original manuscript (image thumbnail + text snippet)
Right pane: Ranked suggestions, filterable by library and domain (each shows
            only the chosen values, or hides them), shown either
            as a list with expandable rows or as a grid of low-resolution
            thumbnails (the visitor's choice is remembered), with action
            buttons (Browse, Puzzle, Add to List, Add as Join).

Orange color scheme (#e65100 / #ff9800) distinct from other enrichment dialogs.

Data sourced from visual_similarity.db sidecar via VisualSimilarityService.
"""

import asyncio
import logging

from nicegui import ui, run

from shared.joins_lab import (
    counted_options, domain_hebrew_names, domain_path_of, domain_tree_options, filter_passes,
    normalize_filter_mode, path_passes, primary_domain,
)
from web.components.candidate_grid import build_thumbnail_url, nest_options
from web.safe_storage import safe_user_get, safe_user_set
from web.services import get_oxford_direct_image_url, is_oxford_manuscript
from web.translations import tr, get_language

logger = logging.getLogger(__name__)

# Number of suggestions to show per batch
_PAGE_SIZE = 20
_PREVIEW_IMAGE_WIDTH = 600
# The visitor's last choice between the list and the thumbnail grid.
_VIEW_STORAGE_KEY = 'vs_dialog_view'

# Thumbnail size in the grid (owner, 2026-10-19): the grid column's minimum width
# and the image height, in px. Thumbnails are fetched 300 px wide, so Large still
# needs no bigger image. Remembered per visitor, like the view.
_THUMB_SIZE_STORAGE_KEY = 'vs_dialog_thumb_size'
_THUMB_SIZES = {'s': 170, 'm': 230, 'l': 300}

# The Original pane, normal and enlarged (owner): its width and its image's
# maximum height. Enlarged trades thumbnails per row for a bigger original;
# min(..., 45vw) keeps it from swallowing a narrow screen.
_ORIGINAL_LARGE_STORAGE_KEY = 'vs_dialog_original_large'
_ORIGINAL_PANE = {False: ('260px', '200px'), True: ('min(450px, 45vw)', '50vh')}


def _normalize_view(value) -> str:
    """'grid' for the thumbnail view; anything else is the list (the default)."""
    return 'grid' if value == 'grid' else 'list'


def _normalize_thumb_size(value) -> str:
    """'s', 'm' or 'l'; anything else is Small (the default, the size before)."""
    return value if isinstance(value, str) and value in _THUMB_SIZES else 's'


def _domain_label(s: dict, is_heb: bool) -> str:
    """The suggestion's domain as the visitor reads it: Hebrew in the Hebrew UI.

    ``domain`` (English) stays the filter key. A domain with no Hebrew name
    falls back to the English one, as on the browse page's Subject Domains.
    """
    if is_heb and s.get('domain_heb'):
        return s['domain_heb']
    return s.get('domain', '--')


def _library_ok(s: dict, filter_state: dict) -> bool:
    return filter_passes(s.get('library_code'), filter_state['libraries'],
                         filter_state['libraries_mode'])


def _domain_ok(s: dict, filter_state: dict) -> bool:
    # A chosen parent takes in its whole branch (shared.joins_lab.path_passes)
    return path_passes(domain_path_of(s), filter_state['domains'], filter_state['domains_mode'])


def _passes_filters(s: dict, filter_state: dict) -> bool:
    """Whether a suggestion passes the Library and Domain filters, each showing
    only its chosen values or hiding them (shared.joins_lab.filter_passes)."""
    return _library_ok(s, filter_state) and _domain_ok(s, filter_state)


def _facet_options(suggestions: list, filter_state: dict, is_heb: bool) -> tuple:
    """The Library and Domain options, value -> "label (count)", and the domains'
    depths (for nest_options).

    Every library and domain among the suggestions keeps its row; each counts the
    suggestions the OTHER filter keeps, so choosing domains updates the library
    counts and the other way round. Libraries in code order; domains as the FJMS
    tree (shared.joins_lab.domain_tree_options), a parent counting its branch,
    unclassified ones ('--') left out.
    """
    libraries = counted_options(
        ((s.get('library_code'), s.get('library_code')) for s in suggestions),
        counted=[s.get('library_code') for s in suggestions if _domain_ok(s, filter_state)])
    domains = domain_tree_options(
        [domain_path_of(s) for s in suggestions],
        [domain_path_of(s) for s in suggestions if _library_ok(s, filter_state)],
        is_heb)
    return ({value: f'{label} ({n})' for value, label, n in libraries},
            {value: f'{label} ({n})' for value, label, n, _depth in domains},
            [depth for *_rest, depth in domains])


def _wire_facet_filter(sel, label: str, filter_state: dict, key: str, refilter) -> None:
    """A Library or Domain select (*key* 'libraries' or 'domains'): its values
    filter at once, shown only or hidden as the Show only | Hide switch at the top
    of its open list says (as in the Joins Lab toolbar). While values are hidden,
    the label says so ("Library · Hiding"), leaving the field to the values.
    *refilter(key)* re-filters after a change to this filter.
    """
    mode_key = f'{key}_mode'

    def _relabel() -> None:
        hiding = bool(filter_state[key]) and filter_state[mode_key] == 'hide'
        sel.label = f"{label} · {tr('Hiding')}" if hiding else label

    def _on_values(e) -> None:
        filter_state[key] = e.value or []
        _relabel()
        refilter(key)

    def _on_mode(e) -> None:
        filter_state[mode_key] = normalize_filter_mode(e.value)
        _relabel()
        if filter_state[key]:            # nothing chosen: nothing changes
            refilter(key)

    with sel.add_slot('before-options'):
        ui.toggle(
            {'show_only': tr('Show only'), 'hide': tr('Hide')}, value=filter_state[mode_key],
        ).props('dense no-caps no-wrap spread').classes('w-full q-pa-xs').on_value_change(_on_mode)
    sel.on_value_change(_on_values)


def _suggestion_thumbnail_url(s: dict) -> str | None:
    """Low-resolution first-image URL for a suggestion's thumbnail card.

    The same proxy URL the Joins Lab grid shows for these look-alikes (first
    image, width 300). It is built from local metadata only, so a page of cards
    costs no metadata enrichment; the list's larger preview still enriches on
    expand.
    """
    return build_thumbnail_url(
        s['alma_id'],
        None,
        shelfmark=s.get('shelfmark', ''),
        library_code=s.get('library_code', ''),
    )


def _pick_preview_image_url(
    sys_id: str,
    *,
    shelfmark: str = '',
    library_code: str = '',
    cached: dict | None = None,
) -> str | None:
    """Choose the same best-effort preview source family as the browse page."""
    cached = cached or {}
    images_ext = cached.get('images_ext') or []
    ext_provider = str(cached.get('external_provider') or '').strip().lower()
    cambridge_alignment = cached.get('cambridge_alignment') or {}
    width_param = f'&width={_PREVIEW_IMAGE_WIDTH}'

    if is_oxford_manuscript(shelfmark, library_code):
        thumb_url = (images_ext[0] or {}).get('thumb_url') if images_ext else ''
        if thumb_url:
            return thumb_url
        direct = get_oxford_direct_image_url(shelfmark, 0, folio_offset=0)
        return direct or f"/api/oxford_image/{sys_id}?page=0{width_param}"

    if ext_provider == 'jts' and images_ext:
        return f"/api/jts_image/{sys_id}?page=0{width_param}"

    if ext_provider == 'manchester' and images_ext:
        return f"/api/manchester_image/{sys_id}?page=0{width_param}"

    if images_ext:
        if library_code == 'CUL' and cambridge_alignment.get('verdict') == 'misaligned':
            return f"/api/nli_image_by_sysid/{sys_id}?page=0&width={_PREVIEW_IMAGE_WIDTH}"
        return f"/api/cambridge_image/{sys_id}?page=0{width_param}"

    return f"/api/nli_image_by_sysid/{sys_id}?page=0&width={_PREVIEW_IMAGE_WIDTH}"


def _resolve_preview_image_url_sync(sys_id: str, shelfmark: str = '') -> str | None:
    """Populate browse metadata on demand, then pick the preview image URL."""
    from web.state import state

    meta_mgr = getattr(state, 'meta_mgr', None)
    if not meta_mgr:
        return f"/api/nli_image_by_sysid/{sys_id}?page=0&width={_PREVIEW_IMAGE_WIDTH}"

    csv_meta = meta_mgr.csv_bank.get(sys_id, {}) if getattr(meta_mgr, 'csv_bank', None) else {}
    library_code = csv_meta.get('library_code', '')
    shelfmark = shelfmark or csv_meta.get('shelfmark', '')

    cached = meta_mgr.nli_cache.get(sys_id, {}) if getattr(meta_mgr, 'nli_cache', None) else {}
    if not cached.get('images_ext') and not cached.get('image_source_info'):
        try:
            meta_mgr.enrich_metadata(sys_id)
            cached = meta_mgr.nli_cache.get(sys_id, {})
        except Exception as e:
            logger.debug("VS dialog: enrich_metadata error for %s: %s", sys_id, e)

    return _pick_preview_image_url(
        sys_id,
        shelfmark=shelfmark,
        library_code=library_code,
        cached=cached,
    )


async def _fetch_original_info(sys_id: str, shelfmark: str = '') -> dict:
    """Fetch image URL and text snippet for the original manuscript."""
    info = {'image_url': None, 'text_snippet': '', 'fl_ids': []}

    def _do_fetch():
        result = {'image_url': None, 'text_snippet': '', 'fl_ids': []}

        # Get FL IDs from NLI crossref for image
        try:
            from shared.nli_crossref_service import get_nli_crossref_service
            nli_svc = get_nli_crossref_service(thread_safe=True)
            if nli_svc.is_available():
                images = nli_svc.get_images(sys_id)
                if images:
                    result['fl_ids'] = [img['fgp_image_number_id'] for img in images]
        except Exception as e:
            logger.debug(f"VS dialog: NLI crossref error for {sys_id}: {e}")

        result['image_url'] = _resolve_preview_image_url_sync(sys_id, shelfmark)

        # Get text snippet from Tantivy
        try:
            from web.state import state
            if state.searcher:
                text, head, src, uid = state.searcher._get_best_text_for_id(sys_id)
                if text:
                    result['text_snippet'] = text[:200].strip()
        except Exception as e:
            logger.debug(f"VS dialog: text fetch error for {sys_id}: {e}")

        return result

    try:
        # None (cancelled or app stopping): keep the empty default above.
        info = await run.io_bound(_do_fetch) or info
    except Exception:
        pass  # Dialog operation failed; continue with available data
    return info


async def _fetch_preview_image_url(sys_id: str, shelfmark: str = '') -> str | None:
    """Resolve the best preview image URL without blocking the UI event loop."""
    try:
        return await run.io_bound(_resolve_preview_image_url_sync, sys_id, shelfmark)
    except Exception:
        return f"/api/nli_image_by_sysid/{sys_id}?page=0&width={_PREVIEW_IMAGE_WIDTH}"


async def _fetch_suggestion_text(alma_id: str) -> str:
    """Fetch text snippet for a suggestion manuscript."""
    def _do():
        try:
            from web.state import state
            if state.searcher:
                text, head, src, uid = state.searcher._get_best_text_for_id(alma_id)
                if text:
                    return text[:150].strip()
        except Exception:
            pass  # Dialog operation failed; continue with available data
        return ''
    try:
        return await run.io_bound(_do)
    except Exception:
        return ''  # Lookup failed; return empty string


async def show_visual_similarity_dialog(sys_id: str, shelfmark: str, vs_service=None):
    """Show visual similarity suggestions dialog for a manuscript.

    Args:
        sys_id: System number of the manuscript.
        shelfmark: Display shelfmark for the dialog title.
        vs_service: Optional VisualSimilarityService instance. If None, auto-create.

    Async to avoid blocking UI thread during data fetch (run.io_bound).
    """
    if vs_service is None:
        from shared.visual_similarity_service import get_vs_service
        vs_service = get_vs_service(thread_safe=True)

    # Fetch suggestions off the event loop
    try:
        data = await run.io_bound(vs_service.get_suggestions, sys_id, 200)
    except Exception:
        data = []  # Lookup failed; use empty list

    # Enrich each suggestion with shelfmark, library_code, domain
    if data:
        def _enrich(suggestions):
            from web.state import state
            csv_bank = state.meta_mgr.csv_bank if state.meta_mgr else None
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            # Batch domain lookup (one SQL query instead of N+1)
            all_ids = [s['alma_id'] for s in suggestions]
            domain_map = {}
            hebrew_names = {}
            try:
                if fjms.is_available():
                    domain_map = fjms.get_domains_for_sys_ids(all_ids)
                    hebrew_names = domain_hebrew_names(fjms.get_domain_hierarchy())
            except Exception:
                pass  # Enrichment failed for this item; continue with available data
            for s in suggestions:
                meta = csv_bank.get(s['alma_id']) if csv_bank else None
                s['shelfmark'] = meta.get('shelfmark', s['alma_id']) if meta else s['alma_id']
                s['library_code'] = meta.get('library_code', '') if meta else ''
                # The first of its domains, with its place in the domain tree
                primary = primary_domain(domain_map.get(s['alma_id'], []), hebrew_names)
                s['domain'] = primary.get('domain', '--')
                s['domain_heb'] = primary.get('domain_heb', '')
                s['domain_path'] = primary.get('domain_path', [])
            return suggestions

        data = await run.io_bound(_enrich, data)
        if data is None:
            return  # cancelled or app stopping

    lang = get_language()
    is_heb = lang == 'he'

    dialog = ui.dialog().props('maximized=false full-width')
    with dialog, ui.card().classes('w-full max-w-[1100px]').style(
        'height: 85vh; overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with orange gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #e65100, #ff9800); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('compare').classes('text-xl')
                ui.label(f'{tr("Visual Similarity")} \u2014 {shelfmark}').classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        if not data:
            # Empty state
            with ui.column().classes('w-full items-center justify-center p-8'):
                ui.icon('info_outline').classes('text-3xl').style('color: var(--text-muted);')
                ui.label(tr("No visual similarity suggestions")).classes('text-sm mt-2').style(
                    'color: var(--text-muted);'
                )
                ui.label(tr("No visual similarity suggestions are available for this manuscript.")).classes('text-xs mt-1').style(
                    'color: var(--text-muted);'
                )
        else:
            # Description
            ui.label(tr("Visual similarity suggestions from FJMS image analysis")).classes('text-xs px-4 pt-2').style(
                'color: var(--text-muted);'
            )

            # Sort and filter controls
            sort_options = [
                {'label': tr('Rank'), 'value': 'rank'},
                {'label': tr('Library'), 'value': 'library'},
                {'label': tr('Domain'), 'value': 'domain'},
            ]

            filter_state = {
                'sort': 'rank',
                'libraries': [],
                'libraries_mode': 'show_only',   # or 'hide' (_wire_facet_filter)
                'domains': [],
                'domains_mode': 'show_only',
                'visible_count': _PAGE_SIZE,
                'view': _normalize_view(safe_user_get(_VIEW_STORAGE_KEY)),
                'thumb': _normalize_thumb_size(safe_user_get(_THUMB_SIZE_STORAGE_KEY)),
                'orig_large': safe_user_get(_ORIGINAL_LARGE_STORAGE_KEY) is True,
            }
            # Unique libraries and domains for the filter options, with counts
            all_libraries, all_domains, domain_depths = _facet_options(
                data, filter_state, is_heb)

            with ui.row().classes('w-full items-center gap-2 px-4 pt-2 pb-1 flex-wrap'):
                sort_sel = ui.select(
                    options={o['value']: o['label'] for o in sort_options},
                    value='rank',
                    label=tr('Sort'),
                ).props('dense outlined').classes('w-28')

                lib_filter = ui.select(
                    options=all_libraries,
                    value=[],
                    label=tr('Library'),
                    multiple=True,
                ).props('dense outlined use-chips').classes('w-36') if all_libraries else None

                dom_filter = ui.select(
                    options=all_domains,
                    value=[],
                    label=tr('Domain'),
                    multiple=True,
                ).props('dense outlined use-chips').classes('w-36') if all_domains else None
                if dom_filter:
                    nest_options(dom_filter, domain_depths)    # the domain tree

                view_toggle = ui.toggle(
                    {'list': tr('List'), 'grid': tr('Thumbnails')},
                    value=filter_state['view'],
                ).props('dense no-caps toggle-color=deep-orange')

                # Thumbnail size: only the grid has thumbnails to size.
                size_toggle = ui.toggle(
                    {'s': tr('Small'), 'm': tr('Medium'), 'l': tr('Large')},
                    value=filter_state['thumb'],
                ).props('dense no-caps toggle-color=deep-orange').mark(
                    'vs-thumb-size'
                ).tooltip(tr('Thumbnail size'))
                size_toggle.set_visibility(filter_state['view'] == 'grid')

                orig_size_btn = ui.button(
                    icon='zoom_in_map' if filter_state['orig_large'] else 'zoom_out_map',
                ).props(
                    f'flat dense round aria-label="{tr("Larger or smaller original")}"'
                ).mark('vs-original-size').tooltip(tr('Larger or smaller original'))

            # Main content: side pane (left) + suggestion list (right)
            with ui.element('div').classes('w-full').style(
                'flex: 1 1 0; overflow: hidden; display: flex; flex-direction: row; min-height: 0;'
            ):
                # ── Left pane: Original manuscript ──
                orig_pane_width, orig_image_height = _ORIGINAL_PANE[filter_state['orig_large']]
                orig_pane = ui.scroll_area().classes('shrink-0').mark('vs-original-pane').style(
                    f'width: {orig_pane_width}; '
                    'border-right: 2px solid var(--border-light, #e5e7eb);'
                )
                orig_image_ref: dict = {'el': None}
                with orig_pane, ui.column().classes('gap-2 p-3'):
                    ui.label(tr('Original')).classes('text-xs font-bold uppercase').style(
                        'color: #e65100; letter-spacing: 0.05em;'
                    )
                    ui.label(shelfmark).classes('text-sm font-bold').style(
                        'color: var(--primary-700); word-break: break-word;'
                    )

                    # Image placeholder — filled async
                    orig_img_container = ui.column().classes('w-full items-center')
                    with orig_img_container:
                        ui.spinner('dots', size='lg').style('color: #ff9800;')

                    # Text placeholder — filled async
                    orig_text_container = ui.column().classes('w-full')

                    async def _load_original():
                        info = await _fetch_original_info(sys_id, shelfmark)
                        orig_img_container.clear()
                        with orig_img_container:
                            if info.get('image_url'):
                                orig_image_ref['el'] = ui.image(info['image_url']).classes(
                                    'w-full object-contain rounded'
                                ).style(
                                    f"max-height: {_ORIGINAL_PANE[filter_state['orig_large']][1]};"
                                )
                            else:
                                ui.icon('hide_image').classes('text-2xl').style('color: var(--text-muted);')

                        orig_text_container.clear()
                        with orig_text_container:
                            snippet = info.get('text_snippet', '')
                            if snippet:
                                dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                                ui.label(snippet).classes('text-xs mt-2').style(
                                    f'color: var(--text-secondary); line-height: 1.4; '
                                    f'max-height: 6em; overflow: hidden; {dir_style}'
                                )
                            else:
                                ui.label(tr('No text available')).classes('text-xs mt-2').style(
                                    'color: var(--text-muted); font-style: italic;'
                                )

                    # Fire async load without blocking dialog render
                    asyncio.ensure_future(_load_original())

                # ── Right pane: Suggestion list ──
                with ui.element('div').style(
                    'flex: 1 1 0; min-width: 0; min-height: 0; height: 100%; overflow-y: auto;'
                ):
                    rows_container = ui.column().classes('w-full gap-0 p-0')

            # Track expanded rows and their lazy-loaded preview state
            expanded_rows = set()
            text_cache = {}   # alma_id -> text snippet
            image_cache = {}  # sys_id -> best preview image URL

            def _get_filtered():
                """Apply filters and sorting, return full filtered list."""
                filtered = [s for s in data if _passes_filters(s, filter_state)]

                sort_key = filter_state['sort']
                if sort_key == 'library':
                    filtered.sort(key=lambda s: (s.get('library_code', ''), s.get('rank', 999)))
                elif sort_key == 'domain':
                    filtered.sort(key=lambda s: (_domain_label(s, is_heb), s.get('rank', 999)))
                else:
                    filtered.sort(key=lambda s: s.get('rank', 999))
                return filtered

            def _render_rows():
                rows_container.clear()
                filtered = _get_filtered()
                display = filtered[:filter_state['visible_count']]

                with rows_container:
                    if not display:
                        with ui.column().classes('w-full items-center justify-center p-4'):
                            ui.label(tr("No visual similarity suggestions")).classes('text-sm').style(
                                'color: var(--text-muted);'
                            )
                        return

                    if filter_state['view'] == 'grid':
                        thumb_px = _THUMB_SIZES[filter_state['thumb']]
                        with ui.element('div').classes('w-full p-3').mark('vs-thumb-grid').style(
                            'display: grid; gap: 12px; '
                            f'grid-template-columns: repeat(auto-fill, minmax(min({thumb_px}px, 100%), 1fr));'
                        ):
                            for s in display:
                                _render_suggestion_card(
                                    s,
                                    dialog,
                                    is_heb,
                                    original_sys_id=sys_id,
                                    original_shelfmark=shelfmark,
                                    thumb_px=thumb_px,
                                )
                    else:
                        for s in display:
                            _render_suggestion_row(
                                s,
                                dialog,
                                expanded_rows,
                                text_cache,
                                image_cache,
                                is_heb,
                                original_sys_id=sys_id,
                                original_shelfmark=shelfmark,
                            )

                    remaining = len(filtered) - filter_state['visible_count']
                    if remaining > 0:
                        with ui.row().classes('w-full justify-center py-3'):
                            def _show_more():
                                filter_state['visible_count'] += _PAGE_SIZE
                                _render_rows()
                            ui.button(
                                f'{tr("Show more")} ({remaining} {tr("remaining")})',
                                on_click=_show_more,
                            ).props('flat dense no-caps').style(
                                'color: #e65100;'
                            )

            # Wire up sort/filter changes. on_value_change, not a raw
            # 'update:model-value' listener: that one is handed
            # GenericEventArguments, which has no .value, so these handlers used
            # to raise and the list was never re-filtered or re-sorted.
            def _on_sort_change(e):
                filter_state['sort'] = e.value
                filter_state['visible_count'] = _PAGE_SIZE
                _render_rows()

            sort_sel.on_value_change(_on_sort_change)

            def _refilter(changed_key):
                filter_state['visible_count'] = _PAGE_SIZE
                _render_rows()
                # Each filter counts what the other keeps: recount the other one
                # (its list is shut; its choice is kept).
                lib_opts, dom_opts, _depths = _facet_options(data, filter_state, is_heb)
                other, opts = ((dom_filter, dom_opts) if changed_key == 'libraries'
                               else (lib_filter, lib_opts))
                if other:
                    other.set_options(opts)

            if lib_filter:
                _wire_facet_filter(lib_filter, tr('Library'), filter_state, 'libraries', _refilter)
            if dom_filter:
                _wire_facet_filter(dom_filter, tr('Domain'), filter_state, 'domains', _refilter)

            # Switching views keeps the filters and how many are shown.
            def _on_view_change(e):
                filter_state['view'] = _normalize_view(e.value)
                safe_user_set(_VIEW_STORAGE_KEY, filter_state['view'])
                size_toggle.set_visibility(filter_state['view'] == 'grid')
                _render_rows()

            view_toggle.on_value_change(_on_view_change)

            def _on_thumb_size_change(e):
                filter_state['thumb'] = _normalize_thumb_size(e.value)
                safe_user_set(_THUMB_SIZE_STORAGE_KEY, filter_state['thumb'])
                _render_rows()

            size_toggle.on_value_change(_on_thumb_size_change)

            def _on_original_size_click():
                """Enlarge the Original pane (fewer thumbnails per row) or shrink it back."""
                filter_state['orig_large'] = not filter_state['orig_large']
                safe_user_set(_ORIGINAL_LARGE_STORAGE_KEY, filter_state['orig_large'])
                width, image_height = _ORIGINAL_PANE[filter_state['orig_large']]
                orig_pane.style(f'width: {width};')
                if orig_image_ref['el'] is not None:
                    orig_image_ref['el'].style(f'max-height: {image_height};')
                orig_size_btn.props(
                    f"icon={'zoom_in_map' if filter_state['orig_large'] else 'zoom_out_map'}"
                )

            orig_size_btn.on_click(_on_original_size_click)

            # Initial render
            _render_rows()

            # Text snippets load on-demand when rows are expanded (no eager preload)

        # Bottom bar
        with ui.row().classes('w-full justify-between items-center p-2'):
            if data:
                ui.button(
                    tr('Search in visual suggestions'), icon='search',
                    on_click=lambda: (
                        ui.navigate.to(f'/search?vs_src={sys_id}'),
                        dialog.close(),
                    ),
                ).props('flat dense no-caps').style('color: #e65100;')
            ui.element('div').classes('flex-grow')
            ui.button(tr('Close'), on_click=dialog.close).props('flat dense')

    dialog.open()
    return dialog


def _render_suggestion_row(
    s,
    dialog,
    expanded_rows,
    text_cache,
    image_cache,
    is_heb,
    original_sys_id=None,
    original_shelfmark=None,
):
    """Render a single suggestion row with expandable detail section."""
    alma_id = s['alma_id']
    browse_url = f'/browse?sys_id={alma_id}'

    # Row container
    row_el = ui.column().classes('w-full gap-0').style(
        'border-bottom: 1px solid var(--border-light, #e5e7eb);'
    )

    # Mutable ref for the detail container (created after the main row)
    dc_ref = [None]

    # Async click handler for expand/collapse
    async def _on_row_click(aid=alma_id):
        dc = dc_ref[0]
        if dc is None:
            return
        if aid in expanded_rows:
            # Collapse
            expanded_rows.discard(aid)
            dc.set_visibility(False)
            return

        # Expand
        expanded_rows.add(aid)
        dc.set_visibility(True)

        # Populate detail content
        dc.clear()
        with dc:
            with ui.row().classes('w-full gap-4 items-start').style(
                'background: rgba(230, 81, 0, 0.08); border-radius: 6px; padding: 8px;'
            ):
                # Image thumbnail
                with ui.column().classes('shrink-0 items-center').style('width: 180px;'):
                    img_container = ui.column().classes('w-full items-center')
                    with img_container:
                        ui.spinner('dots', size='sm').style('color: #ff9800;')

                # Text snippet (lazy loaded)
                with ui.column().classes('flex-1 gap-1'):
                    cached_text = text_cache.get(aid, '')
                    has_cached_image = aid in image_cache
                    cached_image_url = image_cache.get(aid) if has_cached_image else None

                    if cached_text:
                        dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                        ui.label(cached_text).classes('text-xs').style(
                            f'color: var(--text-secondary); line-height: 1.5; {dir_style}'
                        )
                    else:
                        spinner = ui.spinner('dots', size='sm').style('color: #ff9800;')
                        text_label_container = ui.column().classes('w-full')

                    if has_cached_image:
                        img_container.clear()
                        with img_container:
                            if cached_image_url:
                                ui.image(cached_image_url).classes(
                                    'w-full max-h-[160px] object-contain rounded'
                                )
                            else:
                                ui.icon('hide_image').classes('text-2xl').style('color: var(--text-muted);')

                    text_task = None if cached_text else _fetch_suggestion_text(aid)
                    image_task = None if has_cached_image else _fetch_preview_image_url(aid, s.get('shelfmark', ''))

                    if text_task or image_task:
                        if text_task and image_task:
                            image_url, text = await asyncio.gather(image_task, text_task)
                        elif text_task:
                            text = await text_task
                            image_url = cached_image_url
                        else:
                            image_url = await image_task
                            text = cached_text

                        if image_task:
                            image_cache[aid] = image_url
                            img_container.clear()
                            with img_container:
                                if image_url:
                                    ui.image(image_url).classes(
                                        'w-full max-h-[160px] object-contain rounded'
                                    )
                                else:
                                    ui.icon('hide_image').classes('text-2xl').style('color: var(--text-muted);')

                        if text_task:
                            text_cache[aid] = text
                            try:
                                spinner.delete()
                            except Exception:
                                pass  # Cache operation failed; continue without cached data
                            with text_label_container:
                                if text:
                                    dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                                    ui.label(text).classes('text-xs').style(
                                        f'color: var(--text-secondary); line-height: 1.5; {dir_style}'
                                    )
                                else:
                                    ui.label(tr('No text available')).classes('text-xs').style(
                                        'color: var(--text-muted); font-style: italic;'
                                    )

    with row_el:
        # Main row (always visible)
        with ui.row().classes('w-full items-center gap-2 px-3 py-2 cursor-pointer').style(
            'min-height: 2.5em;'
        ).on('click', _on_row_click):
            # Expand chevron
            ui.icon(
                'expand_more' if alma_id in expanded_rows else 'chevron_right'
            ).classes('text-sm').style('color: var(--text-muted); transition: transform 0.2s;')

            # Rank badge (orange)
            ui.badge(f'#{s["rank"]}').props(
                'color=deep-orange-1 text-color=deep-orange-9'
            ).classes('text-xs')

            # Real anchor: selectable text + browser-native Ctrl/Cmd-click and middle-click.
            # Propagation must be stopped CLIENT-side (the row's _on_row_click would
            # otherwise toggle expansion); GenericEventArguments has no stop_propagation.
            ui.link(
                s['shelfmark'],
                browse_url,
            ).classes('text-sm font-semibold').style(
                'color: var(--primary-700); text-decoration: none; user-select: text;'
            ).on('click', js_handler='(e) => e.stopPropagation()').tooltip(tr('Browse manuscript'))

            # Domain
            ui.label(_domain_label(s, is_heb)).classes('text-xs').style(
                'color: var(--text-secondary); min-width: 60px;'
            )

            # Library code badge
            if s.get('library_code'):
                ui.label(s['library_code']).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                    'background: var(--primary-100); color: var(--primary-700);'
                )

            # Text snippet preview (first ~80 chars inline)
            cached_text = text_cache.get(alma_id, '')
            if cached_text:
                dir_style = 'direction: rtl;' if is_heb else ''
                ui.label(cached_text[:80] + ('...' if len(cached_text) > 80 else '')).classes(
                    'text-xs truncate'
                ).style(
                    f'color: var(--text-muted); max-width: 200px; {dir_style}'
                )

            # Spacer
            ui.element('div').classes('flex-grow')

            # Action buttons
            _render_suggestion_actions(
                s, dialog, browse_url,
                original_sys_id=original_sys_id,
                original_shelfmark=original_shelfmark,
            )

        # Expandable detail section (below the main row)
        detail_container = ui.column().classes('w-full gap-2 px-4 pb-3')
        detail_container.set_visibility(False)
        dc_ref[0] = detail_container


def _render_suggestion_actions(
    s,
    dialog,
    browse_url,
    original_sys_id=None,
    original_shelfmark=None,
):
    """Render the Browse / Puzzle / List / Join buttons for one suggestion.

    Shared by the list row and the thumbnail card. Clicks stop client-side so
    they never toggle the list row they sit in.
    """
    alma_id = s['alma_id']

    with ui.link(target=browse_url, new_tab=True).classes('no-underline').on(
        'click', js_handler='(e) => e.stopPropagation()'
    ).tooltip(tr('Browse manuscript')):
        ui.button(icon='open_in_new').props('flat dense round size=sm')

    ui.button(
        icon='extension',
    ).props('flat dense round size=sm').on(
        'click.stop', lambda aid=alma_id: (ui.navigate.to(f'/puzzle?add={aid}'), dialog.close())
    ).tooltip(tr('Add to puzzle'))

    # Add to List button
    def _add_to_list(aid=alma_id, sm=s['shelfmark']):
        try:
            from web.state import state
            from web.components import show_add_to_list_dialog
            if state.lists_mgr:
                show_add_to_list_dialog(
                    sys_id=aid,
                    shelfmark=sm,
                    lists_mgr=state.lists_mgr,
                )
        except Exception as e:
            logger.debug(f"VS dialog: add to list error: {e}")

    ui.button(
        icon='star_border',
    ).props('flat dense round size=sm').on(
        'click.stop', _add_to_list
    ).tooltip(tr('Add to List'))

    # Add as Join button
    if original_sys_id:
        def _add_as_join(aid=alma_id, sm=s['shelfmark'], orig_id=original_sys_id, orig_sm=original_shelfmark):
            from web.auth_state import GlobalAuthState
            from web.components.joins_panel import show_add_join_form
            if not GlobalAuthState.is_logged_in():
                # Check before closing, so an anonymous click does not
                # dismiss this dialog for nothing (sweep C2).
                ui.notify(tr('Login to create joins'), type='warning')
                return
            dialog.close()
            show_add_join_form(
                current_shelfmark=orig_sm or '',
                document_id=orig_id,
                prefill_shelfmark=sm,
                prefill_sys_id=aid,
            )

        ui.button(
            icon='add_link',
        ).props('flat dense round size=sm').style('color: #388e3c;').on(
            'click.stop', _add_as_join
        ).tooltip(tr('Add as Join'))


def _render_suggestion_card(
    s,
    dialog,
    is_heb,
    original_sys_id=None,
    original_shelfmark=None,
    thumb_px=_THUMB_SIZES['s'],
):
    """Render one suggestion as a thumbnail card (the grid view).

    The image opens the fragment in a new tab, so the filtered grid stays open
    while the visitor looks at a candidate. *thumb_px* is the image height (the
    visitor's Small / Medium / Large).
    """
    alma_id = s['alma_id']
    browse_url = f'/browse?sys_id={alma_id}'
    thumb_url = _suggestion_thumbnail_url(s)
    thumb_style = (
        f'width: 100%; height: {thumb_px}px; '
        'background: var(--bg-tertiary, #f3f4f6);'
    )

    with ui.card().classes('w-full p-0 gap-0').style(
        'border: 1px solid var(--border-light, #e5e7eb); border-radius: 8px; overflow: hidden;'
    ):
        with ui.link(target=browse_url, new_tab=True).classes('block w-full no-underline').tooltip(
            tr('Browse manuscript')
        ):
            if thumb_url:
                image = ui.image(thumb_url).props('fit=contain').style(thumb_style)
                with image.add_slot('error'):
                    ui.icon('hide_image').classes('text-2xl absolute-center').style(
                        'color: var(--text-muted);'
                    )
            else:
                with ui.element('div').classes('flex items-center justify-center').style(thumb_style):
                    ui.icon('hide_image').classes('text-2xl').style('color: var(--text-muted);')

        with ui.column().classes('w-full gap-1 p-2'):
            with ui.row().classes('w-full items-center gap-1 no-wrap'):
                ui.badge(f'#{s["rank"]}').props(
                    'color=deep-orange-1 text-color=deep-orange-9'
                ).classes('text-xs')
                ui.link(s['shelfmark'], browse_url).classes('text-sm font-semibold ellipsis').style(
                    'color: var(--primary-700); text-decoration: none; user-select: text; min-width: 0;'
                ).tooltip(s['shelfmark'])

            with ui.row().classes('w-full items-center gap-1 no-wrap'):
                ui.label(_domain_label(s, is_heb)).classes('text-xs ellipsis').style(
                    'color: var(--text-secondary); min-width: 0;'
                )
                if s.get('library_code'):
                    ui.label(s['library_code']).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                        'background: var(--primary-100); color: var(--primary-700);'
                    )

            with ui.row().classes('w-full items-center gap-0 no-wrap justify-end'):
                _render_suggestion_actions(
                    s, dialog, browse_url,
                    original_sys_id=original_sys_id,
                    original_shelfmark=original_shelfmark,
                )
