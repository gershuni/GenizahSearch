# -*- coding: utf-8 -*-
"""
Professional Manuscript Viewer page for GenizahSearch web application.

Features:
- Two-panel layout with image viewer and transcription
- IIIF image support with zoom controls
- Navigation controls with keyboard shortcuts hint
- RTL Hebrew text display with search term highlighting
- Metadata header with external links
"""

import logging

from nicegui import ui, app, run
from typing import Optional, List, Dict, Any
import asyncio
import re
import html as html_module
from urllib.parse import quote

logger = logging.getLogger(__name__)

from web.services import (
    get_service,
    BrowsePage,
    DocumentPage,
    get_oxford_direct_image_url,
    is_oxford_manuscript,
)
from web.translations import tr, is_rtl, get_language
from web.auth_state import GlobalAuthState
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.supabase_client import create_correction, update_correction, get_corrections
from web.components.typography import h1, h2, h3
from web.document_service import get_document_for_fragment, get_section_for_page, get_all_sources_for_fragment
from web.components.joins_panel import fetch_connected_fragments
from web.pages.browse_state import (
    BrowseState, _crossref_cache,
)
from web.pages.browse_enrichment import (
    BrowsePageRefs,
    load_enrichment as _load_enrichment_fn,
    update_enrichment_sections as _update_enrichment_sections_fn,
    populate_bib_catalog_buttons,
)


# ============================================================================
# Custom Styles for Manuscript Viewer
# ============================================================================

VIEWER_STYLES = '''
<script src="/static/manuscript_viewer.js"></script>
<script>
// Progressive image loading: show spinner → thumbnail (400px) → full (2000px)
function progressiveLoad(img) {
    var container = img.closest('.img-loading-container');
    var fullSrc = img.getAttribute('data-full-src');
    // Mark loaded on first successful paint
    img.addEventListener('load', function onThumbLoad() {
        if (container) container.classList.add('img-loaded');
        img.removeEventListener('load', onThumbLoad);
        // Upgrade to full resolution if available
        if (fullSrc && img.src !== fullSrc) {
            var full = new Image();
            full.onload = function() {
                img.src = fullSrc;
            };
            full.src = fullSrc;
        }
    });
    // Also mark loaded on error (hide spinner)
    img.addEventListener('error', function onErr() {
        if (container) container.classList.add('img-loaded');
        img.removeEventListener('error', onErr);
    });
}
// Auto-init all progressive images on page
function initProgressiveImages() {
    document.querySelectorAll('img[data-full-src]').forEach(function(img) {
        if (!img.dataset.progressiveInit) {
            img.dataset.progressiveInit = 'true';
            progressiveLoad(img);
        }
    });
}

// fetchFlIdsFromManifest, handleImageError, NLI_IIIF_BASE are in /static/manuscript_viewer.js
</script>
<style>
    /* Image viewer container */
    .image-viewer-container {
        position: relative;
        background-color: #1a1a1a;
        border-radius: 8px;
        overflow: hidden;
        min-height: 500px;
    }

    .image-container {
        position: relative;
        width: 100%;
        height: 70vh;
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden; /* Hide scrollbars for custom drag */
        background: linear-gradient(45deg, #1a1a1a 25%, #222 25%, #222 50%, #1a1a1a 50%, #1a1a1a 75%, #222 75%);
        background-size: 20px 20px;
    }

    .image-container img {
        max-width: 100%;
        max-height: 100%;
        object-fit: contain;
        transition: transform 0.2s ease-out;
        cursor: grab;
    }

    .image-container img:active {
        cursor: grabbing;
    }

    /* Image controls overlay */
    .image-controls {
        position: absolute;
        bottom: 16px;
        left: 50%;
        transform: translateX(-50%);
        display: flex;
        gap: 8px;
        padding: 8px 16px;
        background: rgba(0, 0, 0, 0.75);
        border-radius: 24px;
        backdrop-filter: blur(8px);
        z-index: 10;
    }

    .image-controls button {
        color: white !important;
    }

    /* Loading placeholder */
    .image-loading {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        color: #888;
        text-align: center;
    }

    /* Transcription panel */
    .transcription-panel {
        background: linear-gradient(to bottom, #fffef5, #fff9e6);
        border: 1px solid #e8e4d4;
        border-radius: 8px;
        height: 70vh;
        display: flex;
        flex-direction: column;
    }

    .transcription-header {
        padding: 12px 16px;
        background: #f5f0e0;
        border-bottom: 1px solid #e8e4d4;
        border-radius: 8px 8px 0 0;
    }

    .transcription-content {
        flex: 1;
        overflow-y: auto;
        padding: 24px;
    }

    .transcription-text {
        white-space: pre-wrap;
        line-height: 2.4;
        font-size: 1.6rem;
        font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", "SBL Hebrew", serif;
        direction: rtl;
        text-align: right;
    }

    /* Highlight search terms */
    .highlight-term {
        background-color: #fef08a;
        padding: 2px 4px;
        border-radius: 3px;
        font-weight: 600;
    }

    /* Metadata header */
    .metadata-header {
        background: linear-gradient(135deg, #15803d 0%, #166534 50%, #14532d 100%);
        color: white;
        padding: 28px 32px;
        border-radius: 16px;
        margin-bottom: 24px;
        box-shadow: 0 6px 20px rgba(22, 101, 52, 0.3);
        position: relative;
        overflow: hidden;
    }
    .metadata-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: repeating-linear-gradient(
            45deg,
            transparent,
            transparent 10px,
            rgba(255,255,255,0.03) 10px,
            rgba(255,255,255,0.03) 20px
        );
        pointer-events: none;
    }

    .shelfmark-title {
        font-size: 2.2rem;
        font-weight: 800;
        margin-bottom: 12px;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
        letter-spacing: 0.5px;
        position: relative;
    }

    .metadata-row {
        display: flex;
        flex-wrap: wrap;
        gap: 20px;
        align-items: center;
        font-size: 1rem;
        opacity: 0.95;
    }

    .metadata-item {
        display: flex;
        align-items: center;
        gap: 8px;
        background: rgba(255, 255, 255, 0.15);
        padding: 6px 12px;
        border-radius: 8px;
    }

    /* Navigation bar */
    .navigation-bar {
        background: linear-gradient(to bottom, #f8fafc, #f1f5f9);
        border: 2px solid #c8e6c9;
        border-radius: 12px;
        padding: 18px 24px;
        margin-bottom: 24px;
        box-shadow: 0 3px 12px rgba(0, 0, 0, 0.08);
        transition: all 0.3s ease;
    }
    .navigation-bar:hover {
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
        border-color: #4caf50;
    }

    /* Source badge styling */
    .source-badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }

    .source-v08 {
        background: #dcfce7;
        color: #166534;
    }

    .source-v07 {
        background: #dbeafe;
        color: #1e40af;
    }

    /* Collapsible panels for tablet */
    @media (max-width: 1024px) {
        .viewer-panels {
            flex-direction: column !important;
        }
        .image-panel, .transcription-panel-wrapper {
            width: 100% !important;
        }
        .image-container {
            height: 50vh;
        }
        .transcription-panel {
            height: auto;
            max-height: 50vh;
        }
    }

    /* Mobile-specific styles */
    @media (max-width: 768px) {
        .metadata-header {
            padding: 16px 20px;
            border-radius: 12px;
            margin-bottom: 16px;
        }
        .shelfmark-title {
            font-size: 1.5rem;
            margin-bottom: 8px;
        }
        .metadata-row {
            gap: 12px;
            font-size: 0.875rem;
        }
        .metadata-item {
            padding: 4px 8px;
            font-size: 0.8rem;
        }
        .navigation-bar {
            padding: 12px 16px;
            margin-bottom: 16px;
        }
        .image-container {
            height: 40vh;
            min-height: 250px;
        }
        .transcription-panel {
            max-height: 45vh;
        }
        .transcription-content {
            padding: 16px;
        }
        .transcription-text {
            font-size: 1.3rem;
            line-height: 2.2;
        }
        .image-controls {
            padding: 6px 12px;
            gap: 4px;
        }
        .image-controls button {
            min-width: 44px !important;
            min-height: 44px !important;
        }
    }

    @media (max-width: 480px) {
        .metadata-header {
            padding: 12px 16px;
            border-radius: 10px;
            margin-bottom: 12px;
        }
        .shelfmark-title {
            font-size: 1.25rem;
            margin-bottom: 6px;
        }
        .metadata-row {
            gap: 8px;
            font-size: 0.8rem;
            flex-wrap: wrap;
        }
        .metadata-item {
            padding: 3px 6px;
            font-size: 0.75rem;
        }
        .navigation-bar {
            padding: 10px 12px;
            margin-bottom: 12px;
            border-radius: 10px;
        }
        .nav-controls-row {
            flex-wrap: wrap !important;
            gap: 8px !important;
        }
        .nav-btn {
            min-width: 44px !important;
            min-height: 44px !important;
        }
        .image-container {
            height: 35vh;
            min-height: 200px;
        }
        .image-viewer-container {
            min-height: 200px;
            border-radius: 6px;
        }
        .transcription-panel {
            max-height: 40vh;
            border-radius: 6px;
        }
        .transcription-header {
            padding: 10px 12px;
        }
        .transcription-content {
            padding: 12px;
        }
        .transcription-text {
            font-size: 1.15rem;
            line-height: 2;
        }
        /* Floating zoom controls for mobile */
        .image-controls {
            bottom: 12px;
            padding: 6px 10px;
            gap: 2px;
            border-radius: 20px;
        }
        .shortcuts-hint {
            display: none !important;
        }
        /* Page input narrower */
        .page-input {
            width: 60px !important;
        }
    }

    /* Touch-friendly navigation buttons */
    .nav-btn {
        min-width: 44px;
        min-height: 44px;
    }

    /* Keyboard shortcuts hint */
    .shortcuts-hint {
        font-size: 0.75rem;
        color: #6b7280;
        padding: 8px 12px;
        background: #f9fafb;
        border-radius: 6px;
        border: 1px solid #e5e7eb;
    }

    .kbd {
        display: inline-block;
        padding: 2px 6px;
        background: #fff;
        border: 1px solid #d1d5db;
        border-radius: 4px;
        font-family: monospace;
        font-size: 0.7rem;
        box-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }

    /* Fullscreen mode */
    .fullscreen-mode {
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        width: 100vw !important;
        height: 100vh !important;
        z-index: 9999 !important;
        border-radius: 0 !important;
        margin: 0 !important;
        background: #000;
    }

    .fullscreen-mode .image-container {
        height: 100vh !important;
        width: 100vw !important;
    }
    
    /* Zoomable Image Class */
    .zoomable-image {
        transform-origin: center center;
        transition: transform 0.1s ease-out;
        will-change: transform;
        max-width: 100%;
        max-height: 100%;
        object-fit: contain;
        user-select: none;
    }
</style>
<script>
    // Create viewer via shared factory (manuscript_viewer.js loaded with defer)
    window.manuscriptViewer = createManuscriptViewer({
        imageSelector: '.zoomable-image',
        containerSelector: '.image-container',
        zoomLabelSelector: '.zoom-level-label',
        gammaFilterId: 'gamma-main'
    });
</script>
<svg style="position:absolute;width:0;height:0"><filter id="gamma-main"><feComponentTransfer><feFuncR type="gamma" amplitude="1" exponent="1.0"/><feFuncG type="gamma" amplitude="1" exponent="1.0"/><feFuncB type="gamma" amplitude="1" exponent="1.0"/></feComponentTransfer></filter></svg>
<svg style="position:absolute;width:0;height:0"><filter id="gamma-fs"><feComponentTransfer><feFuncR type="gamma" amplitude="1" exponent="1.0"/><feFuncG type="gamma" amplitude="1" exponent="1.0"/><feFuncB type="gamma" amplitude="1" exponent="1.0"/></feComponentTransfer></filter></svg>
'''


# BrowseState and _crossref_cache imported from web.pages.browse_state (Phase 73, Plan 01)
# BrowsePageRefs imported from web.pages.browse_enrichment (Phase 73, Plan 01)


def create_browse_page(initial_sys_id: Optional[str] = None, highlight: Optional[str] = None, initial_fl_id: Optional[str] = None, initial_page: Optional[int] = None, initial_shelfmark: Optional[str] = None, initial_volume_ie: Optional[str] = None):
    """Create the professional manuscript viewer page UI."""
    state = BrowseState()
    refs = BrowsePageRefs()
    service = get_service()

    # Track metadata panel visibility
    show_metadata = {'value': False}

    # UI component references
    content_container = None
    metadata_panel = None
    image_element = None
    viewer_container = None
    initial_fl_id_value = initial_fl_id
    # Local aliases to BrowsePageRefs fields -- these are mutable container aliases,
    # so mutations through either name are visible to both (dict/dict/dict).
    # IMPORTANT: _load_generation MUST stay a dict (mutable container), not an int.
    # If changed to a primitive, the alias would be a copy and stale checks break silently.
    slider_refs = refs.slider_refs
    enrichment_refs = refs.enrichment_refs
    _load_generation = refs.load_generation  # dict alias -- safe, see BrowsePageRefs docstring
    refs.page_client = ui.context.client
    _page_client = refs.page_client
    _url_state = {'highlight': highlight, 'last_sys_id': initial_sys_id}  # Track across navigations

    if initial_sys_id:
        state.sys_id = initial_sys_id
    if initial_volume_ie:
        state.volume_ie = initial_volume_ie
    if highlight:
        state.highlight_terms = highlight

    def _update_browser_url():
        """Sync the browser URL bar with the current manuscript/page for sharing.

        Uses the captured _page_client to dispatch JS even when called from
        detached asyncio.ensure_future() tasks that lack NiceGUI client context.
        Clears highlight when manuscript changes (stale highlight from search entry).
        """
        page = state.current_page
        if not page or not state.sys_id:
            return
        # Clear highlight when navigating to a different manuscript
        if _url_state['last_sys_id'] and _url_state['last_sys_id'] != state.sys_id:
            _url_state['highlight'] = None
        _url_state['last_sys_id'] = state.sys_id
        from urllib.parse import urlencode
        import json as _json
        params = {'sys_id': state.sys_id}
        if page.p_num and page.p_num > 0:
            params['page'] = page.p_num
        if state.volume_ie:
            params['volume_ie'] = state.volume_ie
        if _url_state['highlight']:
            params['highlight'] = _url_state['highlight']
        qs = urlencode(params)
        new_url = f'/browse?{qs}'
        shelfmark = page.shelfmark or state.sys_id
        safe_url = _json.dumps(new_url)
        safe_title = _json.dumps(f'{shelfmark} | Dicta Genizah Search')
        js = f'try {{ history.replaceState(null, "", {safe_url}); document.title = {safe_title}; }} catch(e) {{}}'
        _page_client.run_javascript(js)
    # If a shelfmark was passed via URL (not already resolved to sys_id), set it for auto-search on load
    _pending_shelfmark = initial_shelfmark

    # Add custom styles
    ui.add_head_html(VIEWER_STYLES)

    def update_search_error():
        """Update the inline search error display below the search bar."""
        error_container = slider_refs.get('search_error_container')
        if error_container:
            error_container.clear()
            if state.search_error:
                with error_container:
                    ui.icon('error_outline', size='sm').classes('text-red-500')
                    ui.label(state.search_error).classes('text-red-500 text-sm ml-1')

    async def search_shelfmark():
        """Search for manuscripts by shelfmark or sys_id."""
        if not state.shelfmark_query.strip():
            return

        state.is_loading = True
        state.error = None
        state.search_error = None  # Clear inline error
        update_search_error()  # Clear the error display
        update_content()

        try:
            _query = state.shelfmark_query.strip()

            # If input looks like a sys_id (starts with 99, all digits), load directly
            if _query.isdigit() and _query.startswith('99'):
                state.sys_id = _query
                state.current_page = None
                state.view_joined = False
                state.reading_desk_entries = []
                state.active_source = 'nli'
                state.source_user_override = False
                state.volume_ie = None  # Reset volume when navigating to new manuscript
                enrichment_refs.clear()
                await load_page(p_num=1)
                return

            results, exact_match = await run.io_bound(
                lambda: service.search_by_shelfmark(_query, limit=20)
            )

            if not results:
                # Show inline error below search bar instead of full-page error
                state.search_error = tr('No manuscript found') + f": '{state.shelfmark_query}'"
                state.is_loading = False
                # Clear stale page so update_content shows error/welcome, not old manuscript
                state.current_page = None
                state.view_joined = False
                update_search_error()
                update_content()
                return

            # Clear any previous inline error on success
            state.search_error = None
            update_search_error()

            # If exact match or single result, load directly
            if exact_match or len(results) == 1:
                state.sys_id = results[0].sys_id
                state.current_page = None  # Reset to avoid using old page number
                state.view_joined = False  # Exit reading desk if active
                state.reading_desk_entries = []
                state.active_source = 'nli'  # Reset image source for new manuscript
                state.source_user_override = False
                state.volume_ie = None  # Reset volume when navigating to new manuscript
                enrichment_refs.clear()  # Prevent stale ref usage
                await load_page(p_num=1)  # Always start at page 1 for new manuscript
            else:
                # Multiple results - show selection dialog
                state.is_loading = False
                update_content()
                show_shelfmark_suggestions(results)

        except Exception as e:
            state.error = str(e)
            state.is_loading = False
            update_content()

    def show_shelfmark_suggestions(results):
        """Show a dialog with shelfmark suggestions for user to select."""
        from genizah_core import get_library_display

        # Explicit slot context — this may be called from a detached ensure_future task
        with content_container:
          with ui.dialog() as dialog, ui.card().classes('p-4 min-w-96 max-w-lg'):
            # Header
            with ui.row().classes('w-full items-center justify-between mb-4'):
                h3(tr('Select Manuscript'), classes='text-lg font-semibold m-0')
                ui.button(icon='close', on_click=dialog.close).props('flat round dense')

            ui.label(
                f"{tr('Multiple matches found for')}: \"{state.shelfmark_query}\""
            ).classes('mb-4').style('color: var(--text-secondary);')

            # Results list
            with ui.column().classes('w-full gap-1 max-h-80 overflow-y-auto'):
                for result in results:
                    async def select_result(r=result):
                        state.sys_id = r.sys_id
                        state.shelfmark_query = r.shelfmark  # Update state with selected shelfmark
                        # Update the search input field if available
                        if slider_refs.get('search_input'):
                            slider_refs['search_input'].value = r.shelfmark
                        state.current_page = None
                        state.view_joined = False  # Exit reading desk if active
                        state.reading_desk_entries = []
                        state.active_source = 'nli'  # Reset image source for new manuscript
                        state.source_user_override = False
                        state.volume_ie = None  # Reset volume for new manuscript
                        enrichment_refs.clear()  # Prevent stale ref usage
                        dialog.close()
                        await load_page(p_num=1)

                    # Get library display name (short form)
                    library_short = ''
                    if result.library_code:
                        library_short = get_library_display(result.library_code, short=True)

                    with ui.card().classes(
                        'w-full p-3 cursor-pointer hover:bg-gray-100'
                    ).style(
                        'background: var(--bg-surface); border: 1px solid var(--border-subtle);'
                    ).on('click', select_result):
                        # Library and Shelfmark row
                        with ui.row().classes('items-center gap-2'):
                            # Library badge (if available)
                            if library_short:
                                ui.badge(library_short).classes(
                                    'text-xs'
                                ).style('background: var(--primary-100); color: var(--primary-700);')
                            # Shelfmark (LTR)
                            ui.label(result.shelfmark).classes('font-medium').style(
                                'color: var(--text-primary); direction: ltr; text-align: left;'
                            )
                        # Title (RTL for Hebrew)
                        if result.title:
                            ui.label(result.title).classes('text-sm').style(
                                'color: var(--text-secondary); direction: rtl; text-align: right;'
                            )

        dialog.open()

    async def load_page(direction: int = 0, p_num: Optional[int] = None, fl_id: Optional[str] = None):
        """Load a page of the manuscript with async two-phase loading.

        Phase A (fast): Fetch page data via run.io_bound → render image + header immediately.
        Phase B (background): Fetch PGP + FJMS enrichment → update placeholder containers.
        """
        if not state.sys_id and not fl_id:
            state.error = tr('No manuscript found')
            update_content()
            return

        # Generation guard: prevent stale updates from superseded loads
        _load_generation['value'] += 1
        my_gen = _load_generation['value']

        # Reset enrichment state
        state.enrichment_loaded = False
        state.enrichment_loading = False
        state.pgp_transcription = None
        state.pgp_metadata = None
        state.all_sources = None
        state.fjms_data = None
        state.crossref_data = None
        state.title_translation = None
        state.oxford_translations = {}

        state.is_loading = True
        state.error = None
        state.zoom_level = 1.0  # Reset zoom on page change
        # Reset image adjustments on page change (must use content_container context for async tasks)
        with content_container:
            for key in ('brightness', 'contrast'):
                if slider_refs.get(key): slider_refs[key].value = 0
            if slider_refs.get('gamma'): slider_refs['gamma'].value = 100
            ui.run_javascript('if(window.manuscriptViewer) window.manuscriptViewer.resetAdjustments()')
        update_content()  # Show loading spinner

        # === Phase A: Fast page fetch ===
        try:
            _sys_id = state.sys_id
            _current_p_num = state.current_page.p_num if state.current_page else None

            _volume_ie = state.volume_ie

            def _fetch_page():
                if fl_id:
                    return service.get_browse_page_by_fl(fl_id, sys_id=_sys_id)
                elif p_num is not None:
                    return service.get_browse_page(_sys_id, p_num=p_num, volume_ie=_volume_ie)
                elif _current_p_num is not None:
                    return service.get_browse_page(
                        _sys_id,
                        p_num=_current_p_num,
                        direction=direction,
                        volume_ie=_volume_ie,
                    )
                else:
                    return service.get_browse_page(_sys_id, p_num=1, volume_ie=_volume_ie)

            page = await run.io_bound(_fetch_page)

            if my_gen != _load_generation['value']:
                return  # Superseded by newer load

            if page:
                state.sys_id = page.sys_id  # Update in case fl_id resolved differently
                state.current_page = page
                state.page_input_value = page.p_num
                state.error = None
                # Update volume_ie from the loaded page (auto-detect if not set)
                if page.volume_ie:
                    state.volume_ie = page.volume_ie

                # Save position to storage for persistence
                try:
                    app.storage.user['browse_position'] = {
                        'sys_id': state.sys_id,
                        'p_num': page.p_num,
                        'shelfmark': page.shelfmark,
                        'volume_ie': state.volume_ie,  # persist volume across refresh
                    }
                except Exception:
                    pass  # Browser storage operation failed; preference not persisted

                # PostHog: track manuscript views
                from web.analytics import posthog_capture
                posthog_capture('browse_manuscript', {
                    'sys_id': state.sys_id,
                    'shelfmark': page.shelfmark[:80] if page.shelfmark else '',
                    'page_num': page.p_num,
                })

                # Sync browser URL bar for sharing/bookmarking
                _update_browser_url()

                # Track recently viewed item
                if state.sys_id and service.is_ready:
                    try:
                        from web.state import state as app_state
                        if app_state.lists_mgr:
                            app_state.lists_mgr.add_to_recent_sync(state.sys_id, fl_id=page.fl_id)
                    except Exception as track_err:
                        logger.error(f"Failed to track recent item: {track_err}")
            else:
                # No Tantivy page — try metadata-only fallback from csv_bank
                _fallback_sys = state.sys_id
                def _fetch_metadata_only():
                    return service.get_metadata_only_browse_page(_fallback_sys)
                meta_page = await run.io_bound(_fetch_metadata_only)
                if my_gen != _load_generation['value']:
                    return
                if meta_page:
                    state.sys_id = meta_page.sys_id
                    state.current_page = meta_page
                    state.page_input_value = 0
                    state.error = None
                    _update_browser_url()
                else:
                    if fl_id:
                        state.error = tr('No text available') + f" (fl_id: {fl_id})"
                    else:
                        state.error = tr('No text available') + f" (sys_id: {state.sys_id})"

        except Exception as e:
            if my_gen != _load_generation['value']:
                return
            state.error = f"{tr('Error')}: {str(e)}"

        # Title translations: fast SQLite query (~1ms), safe for Phase A
        # Note: Oxford translations moved to Phase B (oxford_part_metadata is deferred)
        if state.current_page and state.current_page.sys_id:
            _title_sys_id = state.current_page.sys_id
            def _fetch_title_translations():
                title_result = None
                try:
                    from shared.translation_service import TranslationService
                    svc = TranslationService(thread_safe=True)
                    if svc.titles_available():
                        title_result = svc.get_title_translations_batch([_title_sys_id]).get(_title_sys_id)
                    svc.close()
                except Exception:
                    pass  # Translation lookup failed; continue without translation
                return title_result
            try:
                state.title_translation = await run.io_bound(_fetch_title_translations)
                state.oxford_translations = {}  # Will be populated in Phase B
            except Exception:
                pass  # Translation lookup failed; continue without translation

        state.is_loading = False
        update_content()  # Phase A complete: show image + header + title translation

        # === Phase B: Background enrichment ===
        if state.current_page and state.current_page.sys_id and not state.error:
            await _load_enrichment(state.current_page, my_gen)

    # Enrichment thin wrappers (inject state + refs into extracted module functions)
    async def _load_enrichment(page, generation):
        await _load_enrichment_fn(state, refs, page, generation)

    def _update_enrichment_sections():
        _update_enrichment_sections_fn(state, refs)

    async def go_to_page(new_page: int):
        """Navigate to a specific page number."""
        if new_page < 1:
            new_page = 1
        if state.current_page and new_page > state.current_page.total_pages:
            new_page = state.current_page.total_pages
        await load_page(p_num=new_page)

    async def navigate_shelfmark(direction: int):
        """Navigate to next/prev shelfmark based on file order."""
        if not state.sys_id:
            return

        state.is_loading = True
        update_content()

        try:
            _sys_id = state.sys_id
            adjacent_sys_id = await run.io_bound(
                lambda: service.get_adjacent_shelfmark(_sys_id, direction)
            )
            if adjacent_sys_id:
                state.sys_id = adjacent_sys_id
                state.view_all = False  # Reset to single page view
                state.view_joined = False  # Exit reading desk if active
                state.full_manuscript = []
                state.reading_desk_entries = []
                state.active_source = 'nli'  # Reset image source for new manuscript
                state.volume_ie = None  # Reset volume for new manuscript
                state.source_user_override = False
                enrichment_refs.clear()  # Prevent stale ref usage across navigations
                await load_page(p_num=1)  # Load first page of new manuscript
            else:
                state.is_loading = False
                # Show message: at first/last manuscript
                state.error = tr('No more manuscripts') if direction > 0 else tr('At first manuscript')
                update_content()
        except Exception as e:
            state.error = f"{tr('Error')}: {str(e)}"
            state.is_loading = False
            update_content()

    async def toggle_view_all():
        """Toggle between single page and full manuscript view."""
        if state.view_all:
            # Switch back to single page
            state.view_all = False
            state.full_manuscript = []
            update_content()
        else:
            # Load full manuscript
            state.is_loading = True
            update_content()

            try:
                _sys_id = state.sys_id
                pages = await run.io_bound(lambda: service.get_full_manuscript(_sys_id))
                if pages:
                    state.full_manuscript = pages
                    state.view_all = True
                    state.error = None
                else:
                    state.error = tr('Could not load full manuscript')
            except Exception as e:
                state.error = f"{tr('Error')}: {str(e)}"
            finally:
                state.is_loading = False
                update_content()

    def enter_joined_view(fragments_info: list, pgpid: int = None):
        """Switch to reading desk mode showing all fragments in dual-pane layout."""
        state.view_joined = True
        state.view_all = False
        state.joined_fragments_info = fragments_info
        state.joined_pgpid = pgpid

        # Load page data and PGP sources for each fragment
        from shared.document_service import get_all_sources_for_fragment, get_document_for_fragment
        entries = []
        for frag_info in fragments_info:
            frag_sid = frag_info.get('document_id', '')
            frag_sm = frag_info.get('shelfmark', '')
            if not frag_sid:
                continue
            pages = service.get_full_manuscript(frag_sid)
            # Get PGP sources for version selector
            sources = []
            pgp_doc = None
            try:
                sources = get_all_sources_for_fragment(frag_sid) or []
                pgp_doc = get_document_for_fragment(frag_sid)
            except Exception:
                pass  # Shelfmark lookup failed; use fallback identifier
            entries.append({
                'sys_id': frag_sid,
                'shelfmark': frag_sm,
                'pages': pages or [],
                'sources': sources,
                'pgp_doc': pgp_doc or {}
            })
        state.reading_desk_entries = entries
        state.reading_desk_selected_sources = {}
        _persist_reading_desk_state()
        update_content()

    refs.enter_joined_view = enter_joined_view

    def exit_joined_view():
        """Return to single page view from joined fragments mode."""
        state.view_joined = False
        state.joined_fragments_info = []
        state.joined_pgpid = None
        state.reading_desk_entries = []
        state.reading_desk_selected_sources = {}
        # Clear persisted reading desk state
        try:
            app.storage.user.pop('reading_desk_state', None)
        except Exception:
            pass  # Browser storage operation failed; preference not persisted
        update_content()

    def add_to_reading_desk():
        """Add current manuscript to the reading desk, or start reading desk if not active."""
        if not state.sys_id or not state.current_page:
            ui.notify(tr('Please load a manuscript first'), type='warning')
            return

        current_sid = state.sys_id
        current_sm = state.current_page.shelfmark or f"ID: {current_sid}"

        if state.view_joined:
            # Reading desk is already active -- add the current manuscript if not already present
            existing_sids = {e.get('sys_id') for e in state.reading_desk_entries}
            if current_sid in existing_sids:
                ui.notify(tr('Already in Reading Desk'), type='info')
                return

            # Load data for the new entry
            from shared.document_service import get_all_sources_for_fragment as rd_get_sources, get_document_for_fragment as rd_get_doc
            pages = service.get_full_manuscript(current_sid)
            sources = []
            pgp_doc = None
            try:
                sources = rd_get_sources(current_sid) or []
                pgp_doc = rd_get_doc(current_sid)
            except Exception:
                pass  # Join operation failed; continue with available data
            state.reading_desk_entries.append({
                'sys_id': current_sid,
                'shelfmark': current_sm,
                'pages': pages or [],
                'sources': sources,
                'pgp_doc': pgp_doc or {}
            })
            _persist_reading_desk_state()
            update_content()
            ui.notify(f'{current_sm} {tr("added to Reading Desk")}', type='positive')
        else:
            # Start reading desk with this manuscript
            frag_info = [{'shelfmark': current_sm, 'document_id': current_sid}]
            enter_joined_view(frag_info, pgpid=None)
            _persist_reading_desk_state()

    def _add_sys_id_to_reading_desk(sys_id: str, shelfmark: str):
        """Internal helper: add a fragment by sys_id/shelfmark to the active reading desk."""
        existing_sids = {e.get('sys_id') for e in state.reading_desk_entries}
        if sys_id in existing_sids:
            ui.notify(tr('Already in Reading Desk'), type='info')
            return

        from shared.document_service import get_all_sources_for_fragment as rd_get_sources, get_document_for_fragment as rd_get_doc
        pages = service.get_full_manuscript(sys_id)
        sources = []
        pgp_doc = None
        try:
            sources = rd_get_sources(sys_id) or []
            pgp_doc = rd_get_doc(sys_id)
        except Exception:
            pass  # Shelfmark lookup failed; use fallback identifier
        state.reading_desk_entries.append({
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'pages': pages or [],
            'sources': sources,
            'pgp_doc': pgp_doc or {}
        })
        _persist_reading_desk_state()
        update_content()
        ui.notify(f'{shelfmark} {tr("added to Reading Desk")}', type='positive')

    def _persist_reading_desk_state():
        """Save reading desk state to app.storage.user for language-switch persistence."""
        try:
            if state.view_joined and state.reading_desk_entries:
                rd_data = []
                for entry in state.reading_desk_entries:
                    rd_data.append({
                        'sys_id': entry.get('sys_id', ''),
                        'shelfmark': entry.get('shelfmark', '')
                    })
                app.storage.user['reading_desk_state'] = {
                    'entries': rd_data,
                    'pgpid': state.joined_pgpid,
                    'selected_sources': state.reading_desk_selected_sources or {}
                }
            else:
                app.storage.user.pop('reading_desk_state', None)
        except Exception as e:
            logger.error(f"[ReadingDesk] Error persisting state: {e}")

    def _restore_reading_desk_state():
        """Restore reading desk state from app.storage.user after language switch."""
        try:
            saved = app.storage.user.get('reading_desk_state')
            logger.info(f"[ReadingDesk] Attempting restore, saved state: {bool(saved)}")
            if saved and saved.get('entries'):
                frag_info = saved['entries']
                pgpid = saved.get('pgpid')
                selected_sources = saved.get('selected_sources', {})
                enter_joined_view(
                    [{'shelfmark': e['shelfmark'], 'document_id': e['sys_id']} for e in frag_info],
                    pgpid=pgpid
                )
                # Restore selected source preferences after enter_joined_view resets them
                if selected_sources:
                    state.reading_desk_selected_sources = selected_sources
                logger.info(f"[ReadingDesk] Restored {len(frag_info)} entries")
                return True
        except Exception as e:
            logger.error(f"[ReadingDesk] Error restoring state: {e}")
        return False

    def search_for_parallels():
        """Navigate to parallels page with current text."""
        if not state.sys_id:
            return

        # Get text to search for parallels
        if state.view_all and state.full_manuscript:
            # Use all pages
            text_content = "\n\n".join([p.text for p in state.full_manuscript if p.text])
        elif state.current_page:
            # Use current page only
            text_content = state.current_page.text
        else:
            return

        if not text_content:
            ui.notify(tr('No text available'), type='warning')
            return

        # Navigate to parallels page with text as URL parameter
        try:
            from urllib.parse import quote
            encoded_text = quote(text_content)
            ui.navigate.to(f'/parallels?text={encoded_text}')
        except Exception as e:
            logger.error(f"Error navigating to parallels: {e}")
            ui.notify(tr('Error'), type='negative')

    def toggle_metadata():
        """Toggle metadata panel visibility."""
        show_metadata['value'] = not show_metadata['value']
        update_content()

    def export_browse_data():
        """Prepare browse data for export."""
        if not state.current_page:
            ui.notify(tr('No text available'), type='warning')
            return

        # Get library info
        library_code = ''
        library_name = ''
        if state.current_page and state.current_page.library_code:
            library_code = state.current_page.library_code
            from genizah_core import get_library_display
            library_name = get_library_display(library_code, short=False, lang=get_language())

        # Prepare export data
        export_data = {
            'shelfmark': state.current_page.shelfmark,
            'title': state.current_page.title,
            'sys_id': state.sys_id,
            'view_all': state.view_all,
            'library_code': library_code,
            'library_name': library_name
        }

        if state.view_all and state.full_manuscript:
            # Export all pages
            export_data['pages'] = [
                {
                    'p_num': p.p_num,
                    'text': p.text,
                    'full_header': p.full_header
                }
                for p in state.full_manuscript
            ]
        else:
            # Export current page
            export_data['p_num'] = state.current_page.p_num
            export_data['text'] = state.current_page.text

        # Store in session storage
        app.storage.user['browse_export_data'] = export_data

        # Trigger download
        ui.download('/api/export/browse/word')

    def add_manuscript_to_list():
        """Add entire manuscript to a list."""
        if not state.sys_id or not state.current_page:
            ui.notify(tr('Please load a manuscript first'), type='warning')
            return

        from web.state import state as app_state
        from web.components import show_add_to_list_dialog
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        show_add_to_list_dialog(
            sys_id=state.sys_id,
            shelfmark=state.current_page.shelfmark,
            lists_mgr=app_state.lists_mgr,
            note_default='',  # Empty by default
            fl_id=None
        )

    def add_page_to_list():
        """Add specific page/image to a list."""
        if not state.sys_id or not state.current_page:
            ui.notify(tr('Please load a manuscript first'), type='warning')
            return

        from web.state import state as app_state
        from web.components import show_add_to_list_dialog
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        # Use FL ID if available for specific page reference
        fl_id = state.current_page.fl_id

        show_add_to_list_dialog(
            sys_id=state.sys_id,
            shelfmark=f"{state.current_page.shelfmark} - {tr('Page')} {state.current_page.p_num}",
            lists_mgr=app_state.lists_mgr,
            note_default='',  # Empty by default (user requested)
            fl_id=fl_id
        )

    def zoom_in():
        """Increase zoom level."""
        state.zoom_level = min(state.zoom_level + 0.25, 4.0)
        update_image_transform()

    def zoom_out():
        """Decrease zoom level."""
        state.zoom_level = max(state.zoom_level - 0.25, 0.25)
        update_image_transform()

    def zoom_reset():
        """Reset zoom to 100%."""
        state.zoom_level = 1.0
        state.rotation = 0  # Also reset rotation
        if slider_refs.get('rotate'):
            slider_refs['rotate'].value = 0
        # Reset image adjustments
        for key in ('brightness', 'contrast'):
            if slider_refs.get(key): slider_refs[key].value = 0
            if slider_refs.get(f'fs_{key}'): slider_refs[f'fs_{key}'].value = 0
        if slider_refs.get('gamma'): slider_refs['gamma'].value = 100
        if slider_refs.get('fs_gamma'): slider_refs['fs_gamma'].value = 100
        ui.run_javascript('''
            if(window.manuscriptViewer) window.manuscriptViewer.reset();
            if(window.fsEditViewer) {
                window.fsEditViewer.state = { x: 0, y: 0, scale: 1, rotation: 0, isDragging: false, startX: 0, startY: 0, brightness: 0, contrast: 0, gamma: 1.0, invert: false };
                window.fsEditViewer.applyTransform();
                window.fsEditViewer._applyFilters();
            }
        ''')
        update_image_transform()

    def fit_width():
        """Fit image to container width."""
        state.zoom_level = 1.0
        update_image_transform()

    def fit_height():
        """Fit image to container height."""
        state.zoom_level = 0.9
        update_image_transform()

    def rotate_left():
        """Rotate image 90 degrees counter-clockwise."""
        state.rotation = (state.rotation - 90) % 360
        if slider_refs.get('rotate'):
            slider_refs['rotate'].value = state.rotation
        update_image_transform()

    def rotate_right():
        """Rotate image 90 degrees clockwise."""
        state.rotation = (state.rotation + 90) % 360
        if slider_refs.get('rotate'):
            slider_refs['rotate'].value = state.rotation
        update_image_transform()

    def rotate_reset():
        """Reset rotation to 0."""
        state.rotation = 0
        if slider_refs.get('rotate'):
            slider_refs['rotate'].value = 0
        update_image_transform()

    def handle_rotation_slider(e):
        """Handle rotation slider change."""
        if e.value is not None:
             state.rotation = int(e.value)
             update_image_transform()

    def toggle_fullscreen():
        """Toggle fullscreen mode."""
        state.is_fullscreen = not state.is_fullscreen
        update_content()

    def toggle_image_fullscreen():
        """Toggle fullscreen mode for image only."""
        ui.run_javascript('''
            const imageCard = document.querySelector('.image-container')?.closest('.q-card');
            if (imageCard) {
                imageCard.classList.toggle('fullscreen-mode');
                // Add ESC key handler to exit
                const escHandler = (e) => {
                    if (e.key === 'Escape' && imageCard.classList.contains('fullscreen-mode')) {
                        imageCard.classList.remove('fullscreen-mode');
                        document.removeEventListener('keydown', escHandler);
                    }
                };
                if (imageCard.classList.contains('fullscreen-mode')) {
                    document.addEventListener('keydown', escHandler);
                }
            }
        ''')

    def toggle_fullscreen_edit():
        """Toggle fullscreen edit mode with side-by-side image and text."""
        state.fullscreen_edit = not state.fullscreen_edit
        update_content()

    def update_image_transform():
        """Update the image transform (zoom/rotate) via JavaScript."""
        zoom_percent = int(state.zoom_level * 100)
        # Update Python state on client side - both regular and fullscreen viewers
        ui.run_javascript(f'''
            // Update regular viewer
            if (window.manuscriptViewer) {{
                window.manuscriptViewer.update({state.zoom_level}, {state.rotation});
            }}
            // Update fullscreen edit viewer if active
            if (window.fsEditViewer) {{
                window.fsEditViewer.state.scale = {state.zoom_level};
                window.fsEditViewer.state.rotation = {state.rotation};
                window.fsEditViewer.applyTransform();
            }}
            // Update all zoom labels
            document.querySelectorAll('.zoom-level-label').forEach(label => {{
                label.textContent = '{zoom_percent}%';
            }});
        ''')

    async def handle_submit_correction():
        """Submit the correction to the backend."""
        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login first'), type='negative')
            return

        if state.edit_text == state.original_edit_text and not state.edit_notes:
            ui.notify(tr('No changes to submit'), type='warning')
            return

        state.error_message = None

        user_id = GlobalAuthState.get_user_id()
        if not user_id:
            ui.notify(tr('User not found'), type='negative')
            return

        # Determine status based on role
        status = 'approved' if (GlobalAuthState.is_admin() or GlobalAuthState.is_editor()) else 'pending'

        # Do the Supabase call BEFORE any UI rebuild
        try:
            if state.draft_id:
                result = update_correction(state.draft_id, {
                    'corrected_text': state.edit_text,
                    'notes': state.edit_notes,
                    'status': status
                })
            else:
                result = create_correction(
                    author_id=user_id,
                    sys_id=state.current_page.sys_id,
                    shelfmark=state.current_page.shelfmark or '',
                    page_number=state.current_page.p_num,
                    original_text=state.original_edit_text,
                    corrected_text=state.edit_text,
                    notes=state.edit_notes if state.edit_notes else '',
                    status=status,
                    ie_id=state.volume_ie
                )
        except Exception as e:
            ui.notify(f'{tr("Error")}: {e}', type='negative')
            logger.error(f"Submit error: {e}")
            return

        if "error" in result:
            error_msg = result["error"]
            if '42501' in str(error_msg) or 'row-level security' in str(error_msg).lower():
                ui.notify(tr('Session expired. Please log out and log back in, then try again.'),
                          type='negative', timeout=10000)
            else:
                ui.notify(error_msg, type='negative')
            return

        # Success — update state and reload
        state.edit_mode = False
        state.draft_saved = False
        state.draft_id = None
        ui.notify(tr('Correction submitted successfully'), type='positive')
        asyncio.ensure_future(load_page(direction=0))

    def handle_save_draft():
        """Save draft locally (simulated for now, or use backend draft)."""
        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login to save drafts'), type='warning')
            return

        user_id = GlobalAuthState.get_user_id()
        if not user_id:
            ui.notify(tr('User not found'), type='negative')
            return

        if state.draft_id:
            # Update existing draft
            result = update_correction(state.draft_id, {
                'corrected_text': state.edit_text,
                'notes': state.edit_notes,
                'status': 'draft'
            })
        else:
            # Create new draft
            result = create_correction(
                author_id=user_id,
                sys_id=state.current_page.sys_id,
                shelfmark=state.current_page.shelfmark or '',
                page_number=state.current_page.p_num,
                original_text=state.original_edit_text,
                corrected_text=state.edit_text,
                notes=state.edit_notes if state.edit_notes else '',
                status='draft',
                ie_id=state.volume_ie
            )

        if "error" in result:
            error_msg = result["error"]
            if '42501' in str(error_msg) or 'row-level security' in str(error_msg).lower():
                ui.notify(tr('Session expired. Please log out and log back in, then try again.'),
                          type='negative', timeout=10000)
            else:
                ui.notify(error_msg, type='negative')
        else:
            state.draft_saved = True
            correction = result.get('correction', {})
            state.draft_id = correction.get('id')
            ui.notify(tr('Draft saved'), type='positive')
            update_content()  # Update UI to show green border

    def toggle_edit_mode():
        """Toggle edit mode with fetching existing corrections."""
        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login to edit'), type='warning')
            return

        if state.edit_mode:
            cancel_edit()
        else:
            # Enter edit mode - Show loading state
            state.edit_loading = True
            update_content()

            try:
                # Fetch existing corrections to see if we should resume one
                user_id = GlobalAuthState.get_user_id()
                my_corrections = []
                if user_id:
                    try:
                        # Fetch corrections for this document
                        all_corrections = get_corrections(sys_id=state.current_page.sys_id, author_id=user_id, ie_id=state.volume_ie)
                        # Filter for THIS page
                        current_p_num = state.current_page.p_num
                        my_corrections = [
                            c for c in all_corrections
                            if c.get('page_number') == current_p_num
                        ]
                        # Sort by created_at desc
                        my_corrections.sort(key=lambda x: x.get('created_at', ''), reverse=True)
                    except Exception as e:
                        logger.error(f"Error fetching corrections: {e}")

                latest = my_corrections[0] if my_corrections else None

                state.edit_mode = True

                # Default to page text
                base_text = state.current_page.text or ""
                base_notes = ""
                
                # Determine state from latest correction
                if latest:
                    # Use the latest correction text as baseline for editing
                    base_text = latest.get('corrected_text', base_text)
                    base_notes = latest.get('notes', "") or ""
                    req_status = latest.get('status')
                    
                    if req_status in ('draft', 'pending', 'needs_revision'):
                        # Resume editing this correction
                        state.draft_id = latest.get('id')
                        state.draft_saved = True 
                    else:
                        # It's approved/rejected, start fresh but with this content
                        state.draft_id = None
                        state.draft_saved = False
                else:
                    state.draft_id = None
                    state.draft_saved = False

                state.edit_text = base_text
                # Original manuscript text acts as the "original_text" reference
                state.original_edit_text = state.current_page.text or ""
                state.edit_notes = base_notes
                
                state.edit_loading = False
                update_content()
            except Exception as e:
                state.edit_loading = False
                update_content()
                ui.notify(f"Error loading edit mode: {str(e)}", type='negative')

    def cancel_edit():
        """Cancel edit and revert to view mode."""
        state.edit_mode = False
        state.edit_text = ""
        state.edit_notes = ""
        update_content()

    def highlight_text(text: str) -> str:
        """Apply highlighting to search terms in text, safely escaping HTML."""
        if not text:
            return ""

        # First escape HTML to prevent XSS
        escaped_text = html_module.escape(text)

        if not state.highlight_terms:
            return escaped_text

        # Split highlight terms and apply highlighting
        terms = state.highlight_terms.split()
        highlighted = escaped_text

        for term in terms:
            # Escape the term for safe display, then do case-insensitive replacement
            escaped_term = html_module.escape(term)
            pattern = re.compile(re.escape(escaped_term), re.IGNORECASE)
            highlighted = pattern.sub(
                f'<span class="highlight-term">{escaped_term}</span>',
                highlighted
            )

        return highlighted

    def get_source_badge_class(full_header: str) -> str:
        """Determine source badge class based on header."""
        if 'V0.8' in full_header:
            return 'source-v08'
        elif 'V0.7' in full_header:
            return 'source-v07'
        return 'source-v08'

    def extract_folio_number(full_header: str) -> str:
        """Extract folio number from header if available."""
        if not full_header:
            return ''
        # Try to extract folio info like "1r", "2v", etc. (but not long sys_ids)
        match = re.search(r'\b(\d{1,3}[rv]?)\b', full_header)
        if match:
            folio = match.group(1)
            # Only return if it looks like a valid folio (not a long ID)
            if len(folio) <= 4:
                return folio
        return ''

    def update_content():
        """Update the content display."""
        try:
            _ = content_container.client
        except (RuntimeError, AttributeError):
            return  # Client/session gone (user navigated away)
        content_container.clear()

        with content_container:
            if state.is_loading:
                with ui.row().classes('w-full justify-center py-16'):
                    ui.spinner(size='xl', color='green')
                    ui.label(tr('Loading...')).classes('ml-3 text-lg').style('color: var(--text-secondary);')
                return

            if state.error and not state.current_page:
                with ui.card().classes('w-full p-8 text-center'):
                    ui.icon('error_outline', size='4rem').classes('text-red-400')
                    ui.label(state.error).classes('text-red-600 mt-4 text-lg')
                    ui.button(tr('Back'), icon='arrow_forward' if is_rtl() else 'arrow_back', on_click=lambda: asyncio.ensure_future(load_page())).classes('mt-4')
                return

            if not state.current_page and not state.view_joined:
                # Show welcome/search prompt
                with ui.column().classes('w-full items-center py-16'):
                    ui.icon('auto_stories', size='6rem').classes('text-green-400')
                    ui.label(tr('Enter a shelfmark to browse the manuscript')).classes(
                        'mt-6 text-xl rtl-text hebrew-text'
                    ).style('color: var(--text-secondary);')
                    with ui.column().classes('mt-8 text-center'):
                        ui.label(tr('Examples')).classes('text-sm mb-2').style('color: var(--text-tertiary);')
                        with ui.row().classes('gap-2'):
                            for example in ['T-S 8J6.1', 'T-S 13J2.5', 'T-S AS 145.295']:
                                ui.button(
                                    example,
                                    on_click=lambda e=example: set_shelfmark_and_search(e)
                                ).props('flat dense').classes('text-green-700')
                return

            page = state.current_page

            # Reference to hold the notes panel refresh function
            # Will be set after the notes panel is created
            notes_refresh_ref = {'refresh': None}

            async def refresh_notes_after_comment():
                """Refresh the notes panel after a comment is submitted."""
                if notes_refresh_ref['refresh']:
                    await notes_refresh_ref['refresh']()

            # === Compact Metadata Header ===
            with ui.card().classes('w-full p-3 mb-3').style(
                'background: linear-gradient(135deg, #15803d 0%, #166534 100%) !important; '
                'border: none;'
            ):
                with ui.row().classes('w-full items-center justify-between'):
                    # Prev Shelfmark Button
                    ui.button(
                        icon='skip_next' if is_rtl() else 'skip_previous',
                        on_click=lambda: asyncio.ensure_future(navigate_shelfmark(-1))
                    ).props('flat round').style('color: white !important;').tooltip(tr('Previous manuscript'))

                    # Shelfmark and Title
                    with ui.row().classes('flex-1 items-center justify-center gap-4'):
                        # Shelfmark with Library Name - H2
                        display_shelfmark = page.shelfmark or f"ID: {page.sys_id}"
                        if page.library_name:
                            display_shelfmark = f"{page.library_name}, {display_shelfmark}"
                        h2(display_shelfmark, classes='text-xl font-bold', style='color: #ffffff !important; text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);')

                        # Oxford Part Label (e.g. [part 6])
                        if page.oxford_part_display:
                            # Extract just the "part X" portion if possible, or show full
                            # Logic: if shelfmark contains the first part, show only suffix.
                            # But simple display is fine.
                            # User requested: [part 6] immediately after shelfmark
                            # Let's extract "part X" from "heb. d. 29 part 2"
                            part_suffix = page.oxford_part_display
                            if "part" in part_suffix:
                                part_suffix = part_suffix.split("part")[-1].strip()
                                part_label = f"[part {part_suffix}]"
                            else:
                                part_label = f"[{page.oxford_part_display}]"

                            ui.label(part_label).classes(
                                'text-lg font-bold'
                            ).style(
                                'color: #3b82f6 !important; ' # Blue color as seen in screenshot (approx)
                                'text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);'
                            )

                        # Title (truncated with tooltip, language-aware)
                        _overlay_title = page.title
                        if state.title_translation:
                            _ol_lang = get_language()
                            if _ol_lang == 'he':
                                _overlay_title = state.title_translation.get('hebrew_title') or state.title_translation.get('english_title') or page.title
                            else:
                                _overlay_title = state.title_translation.get('english_title') or state.title_translation.get('hebrew_title') or page.title
                        if _overlay_title:
                            _ol_dir = 'rtl-text hebrew-text' if not (state.title_translation and get_language() != 'he' and state.title_translation.get('english_title')) else ''
                            words = _overlay_title.split()
                            if len(words) > 5:
                                short_title = ' '.join(words[:5]) + '...'
                            else:
                                short_title = _overlay_title
                            _ol_has_toggle = state.title_translation and _overlay_title != page.title
                            if _ol_has_toggle:
                                _ol_st = {'showing_original': False}
                                with ui.row().classes('items-center gap-0'):
                                    _ol_lbl = ui.label(short_title).classes(
                                        _ol_dir
                                    ).style(
                                        'color: #ffffff !important; opacity: 0.95;'
                                    )
                                    if len(words) > 5:
                                        _ol_lbl.tooltip(_overlay_title)
                                    def _make_ol_toggle(lbl, orig_title, trans_title, orig_short, trans_short, flag, trans_dir):
                                        def handler():
                                            flag['showing_original'] = not flag['showing_original']
                                            if flag['showing_original']:
                                                lbl.text = orig_short
                                                lbl.classes(remove=trans_dir, add='rtl-text hebrew-text')
                                                lbl.tooltip(orig_title)
                                            else:
                                                lbl.text = trans_short
                                                lbl.classes(remove='rtl-text hebrew-text', add=trans_dir)
                                                lbl.tooltip(trans_title)
                                        return handler
                                    _orig_words = page.title.split() if page.title else []
                                    _orig_short = (' '.join(_orig_words[:5]) + '...') if len(_orig_words) > 5 else (page.title or '')
                                    ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                        'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.5; color: white !important;'
                                    ).tooltip(tr('Show original title')).on(
                                        'click.stop', _make_ol_toggle(_ol_lbl, page.title, _overlay_title, _orig_short, short_title, _ol_st, _ol_dir)
                                    )
                            else:
                                ui.label(short_title).classes(
                                    _ol_dir
                                ).style(
                                    'color: #ffffff !important; opacity: 0.95;'
                                ).tooltip(_overlay_title if len(words) > 5 else '')

                        # Ktiv link
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
                        with ui.link(target=ktiv_url, new_tab=True).classes(
                            'flex items-center gap-1 px-2 py-1 rounded'
                        ).style(
                            'text-decoration: none; '
                            'color: #ffffff !important; '
                            'background: rgba(255, 255, 255, 0.2);'
                        ):
                            ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                            ui.label(tr('Ktiv')).classes('text-sm font-semibold').style('color: #ffffff !important;')

                        # PGP link button placeholder (populated by enrichment Phase B)
                        pgp_link_el = ui.element('span')
                        enrichment_refs['pgp_link_container'] = pgp_link_el
                        if state.enrichment_loaded and state.pgp_metadata and state.pgp_metadata.get('pgp_url'):
                            with pgp_link_el:
                                with ui.link(target=state.pgp_metadata['pgp_url'], new_tab=True).classes(
                                    'flex items-center gap-1 px-2 py-1 rounded'
                                ).style(
                                    'text-decoration: none; '
                                    'color: #ffffff !important; '
                                    'background: rgba(255, 255, 255, 0.2);'
                                ):
                                    ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                                    ui.label('PGP').classes('text-sm font-semibold').style('color: #ffffff !important;')

                        # Library digital collection link (header)
                        if page.library_viewer_url and page.library_viewer_url.get('url'):
                            lib_info = page.library_viewer_url
                            if not (page.is_oxford and page.external_url) and not (page.is_cambridge and page.external_url):
                                with ui.link(target=lib_info['url'], new_tab=True).classes(
                                    'flex items-center gap-1 px-2 py-1 rounded'
                                ).style(
                                    'text-decoration: none; '
                                    'color: #ffffff !important; '
                                    'background: rgba(255, 255, 255, 0.2);'
                                ):
                                    ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                                    ui.label(lib_info.get('label', tr('Library'))).classes('text-sm font-semibold').style('color: #ffffff !important;')

                        # Search for Parallels button
                        ui.button(
                            tr('Search for Parallels'),
                            icon='search',
                            on_click=search_for_parallels
                        ).props(f'flat dense aria-label="{tr("Search for Parallels")}"').style(
                            'color: #ffffff !important; '
                            'background: rgba(255, 255, 255, 0.15);'
                        ).tooltip(tr('Search for Parallels'))

                        # Metadata button
                        ui.button(
                            tr('Hide Metadata') if show_metadata['value'] else tr('Show Metadata'),
                            icon='info',
                            on_click=toggle_metadata
                        ).props(f'flat dense aria-label="{tr("Show Metadata")}"').style(
                            'color: #ffffff !important; '
                            'background: rgba(255, 255, 255, 0.15);'
                        ).tooltip(tr('Show Metadata'))

                        # Add manuscript to list (star button)
                        from web.state import state as app_state
                        from web.components import get_star_icon
                        star_icon = get_star_icon(app_state.lists_mgr, page.sys_id) if page else 'star_border'
                        ui.button(
                            icon=star_icon,
                            on_click=add_manuscript_to_list
                        ).props(f'flat round dense aria-label="{tr("Add to List")}"').style('color: #ffffff !important;').tooltip(tr('In List') if star_icon == 'star' else tr('Add to List'))

                        if WEB_PUZZLE_ENABLED:
                            # Add to Puzzle button (Phase 49)
                            def add_to_puzzle():
                                sid = state.sys_id
                                fl = state.current_page.fl_id if state.current_page else None
                                if fl:
                                    param = f'{sid},{fl}'
                                elif state.current_page and state.active_source != 'nli':
                                    # External library — pass page index (0-based from p_num)
                                    page_idx = max(0, state.current_page.p_num - 1) if state.current_page.p_num else 0
                                    param = f'{sid},page:{page_idx}'
                                else:
                                    param = str(sid)
                                ui.navigate.to(f'/puzzle?add={param}')
                            ui.button(
                                icon='extension',
                                on_click=add_to_puzzle
                            ).props('flat round dense').style('color: #ffffff !important;').tooltip(tr('Add to Puzzle'))

                        # Share / Copy Link button
                        def _copy_share_link():
                            ui.run_javascript('''
                                navigator.clipboard.writeText(window.location.href).then(() => {
                                    // Brief visual feedback via a toast-like notification
                                    const t = document.createElement('div');
                                    t.textContent = ''' + repr(tr('Link copied!')) + ''';
                                    t.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#059669;color:#fff;padding:8px 16px;border-radius:6px;z-index:9999;font-size:14px;';
                                    document.body.appendChild(t);
                                    setTimeout(() => t.remove(), 2000);
                                });
                            ''')
                        ui.button(
                            icon='share',
                            on_click=_copy_share_link
                        ).props('flat round dense').style('color: #ffffff !important;').tooltip(tr('Copy Link'))

                    # Next Shelfmark Button
                    ui.button(
                        icon='skip_previous' if is_rtl() else 'skip_next',
                        on_click=lambda: asyncio.ensure_future(navigate_shelfmark(1))
                    ).props('flat round').style('color: white !important;').tooltip(tr('Next manuscript'))

            # === Action Buttons Row ===
            # Removed - buttons moved to appropriate headers

            # === Metadata Panel (Expandable) ===
            if show_metadata['value']:
                with ui.card().classes('w-full p-4 mb-3').style('background: var(--bg-tertiary); border: 1px solid var(--border-light);'):
                    with ui.row().classes('w-full items-center justify-between mb-3'):
                        # Changed to H3
                        h3(tr('Metadata'), classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.button(
                            icon='close',
                            on_click=toggle_metadata
                        ).props('flat round dense size=sm').tooltip(tr('Close'))

                    # Metadata grid
                    with ui.grid(columns=2).classes('w-full gap-4'):
                        # Library
                        if page.library_name:
                            with ui.column().classes('gap-1 col-span-2'):
                                ui.label(tr('Library')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(page.library_name).classes('text-sm').style('color: var(--text-primary);')

                        # Shelfmark (with Neubauer-Cowley catalog entry)
                        # Read crossref metadata from enrichment state (fetched in parallel by _load_enrichment)
                        _crossref = state.crossref_data or {}
                        _catalog_entry_str = _crossref.get('catalog_entry', '') or ''
                        _collection_storage = _crossref.get('collection_storage')

                        with ui.column().classes('gap-1'):
                            ui.label(tr('Shelfmark')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            with ui.row().classes('items-center gap-2'):
                                ui.label(page.shelfmark or 'N/A').classes('text-sm').style('color: var(--text-primary);')
                            if _catalog_entry_str:
                                ui.label(_catalog_entry_str).classes('text-xs text-gray-500').style('color: var(--text-tertiary);')

                        # System ID
                        with ui.column().classes('gap-1'):
                            ui.label(tr('System ID')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(page.sys_id).classes('text-sm font-mono').style('color: var(--text-primary);')

                        # Title (language-aware with swap toggle)
                        if page.title:
                            with ui.column().classes('gap-1 col-span-2'):
                                ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                _meta_title = page.title
                                _meta_dir = 'rtl-text hebrew-text'
                                if state.title_translation:
                                    _mt_lang = get_language()
                                    if _mt_lang == 'he':
                                        _meta_title = state.title_translation.get('hebrew_title') or state.title_translation.get('english_title') or page.title
                                    else:
                                        _meta_title = state.title_translation.get('english_title') or state.title_translation.get('hebrew_title') or page.title
                                        if state.title_translation.get('english_title'):
                                            _meta_dir = ''
                                if state.title_translation and _meta_title != page.title:
                                    _mt_st = {'showing_original': False}
                                    with ui.row().classes('items-center gap-0'):
                                        _mt_lbl = ui.label(_meta_title).classes(f'text-sm {_meta_dir}').style('color: var(--text-primary);')
                                        def _make_mt_toggle(lbl, orig, resolved, flag, resolved_dir):
                                            def handler():
                                                flag['showing_original'] = not flag['showing_original']
                                                if flag['showing_original']:
                                                    lbl.text = orig
                                                    lbl.classes(remove=resolved_dir, add='rtl-text hebrew-text')
                                                else:
                                                    lbl.text = resolved
                                                    lbl.classes(remove='rtl-text hebrew-text', add=resolved_dir)
                                            return handler
                                        ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                            'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                                        ).tooltip(tr('Show original title')).on(
                                            'click.stop', _make_mt_toggle(_mt_lbl, page.title, _meta_title, _mt_st, _meta_dir)
                                        )
                                else:
                                    ui.label(_meta_title).classes(f'text-sm {_meta_dir}').style('color: var(--text-primary);')

                        # Total Pages
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Pages')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(str(page.total_pages)).classes('text-sm').style('color: var(--text-primary);')

                        # FL ID (if available)
                        if page.fl_id:
                            with ui.column().classes('gap-1'):
                                ui.label('FL ID').classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(f'FL{page.fl_id}').classes('text-sm font-mono').style('color: var(--text-primary);')

                        # Material type (NLI crossref)
                        if page.physical_metadata and page.physical_metadata.get('material'):
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Material')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(tr(page.physical_metadata['material'])).classes('text-sm').style('color: var(--text-primary);')

                        # Folio count (NLI crossref)
                        if page.physical_metadata:
                            num_folio = page.physical_metadata.get('num_folio', '')
                            num_bifolio = page.physical_metadata.get('num_bifolio', '')
                            folio_parts = []
                            if num_folio and num_folio != '0':
                                folio_parts.append(f"{num_folio} {tr('Folios')}")
                            if num_bifolio and num_bifolio != '0':
                                folio_parts.append(f"{num_bifolio} {tr('Bifolios')}")
                            if folio_parts:
                                with ui.column().classes('gap-1'):
                                    ui.label(tr('Folios')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(' + '.join(folio_parts)).classes('text-sm').style('color: var(--text-primary);')

                        # Oxford Metadata (Part Title, Contents, Provenance)
                        if page.oxford_part_metadata:
                            _ox_trans = state.oxford_translations or {}
                            _ox_is_heb = get_language() == 'he'
                            for _ox_field, _ox_label in [('title', tr('Part Title')), ('contents', tr('Contents')), ('provenance', tr('Provenance'))]:
                                _ox_eng = page.oxford_part_metadata.get(_ox_field, '').strip()
                                if not _ox_eng:
                                    continue
                                _ox_heb = _ox_trans.get(_ox_eng)
                                _ox_display = _ox_heb if (_ox_is_heb and _ox_heb) else _ox_eng
                                _ox_dir = 'direction: rtl; text-align: right;' if (_ox_is_heb and _ox_heb) else ''
                                with ui.column().classes('gap-1 col-span-2'):
                                    ui.label(_ox_label).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    _ox_lbl = ui.label(_ox_display).classes('text-sm whitespace-pre-wrap').style(f'color: var(--text-primary); {_ox_dir}')
                                    if _ox_heb and _ox_heb != _ox_eng:
                                        _ox_st = {'showing_original': False if _ox_is_heb else True}
                                        _ox_badge_ref = [None]
                                        def _make_ox_toggle(lbl, badge_ref, eng, heb, flag):
                                            def handler():
                                                flag['showing_original'] = not flag['showing_original']
                                                if flag['showing_original']:
                                                    lbl.text = eng
                                                    lbl.style('color: var(--text-primary);')
                                                    badge_ref[0].text = 'Original'
                                                else:
                                                    lbl.text = heb
                                                    lbl.style('color: var(--text-primary); direction: rtl; text-align: right;')
                                                    badge_ref[0].text = tr('Translated')
                                            return handler
                                        _init_badge = tr('Translated') if _ox_is_heb else 'Original'
                                        _ox_badge = ui.badge(_init_badge, color='light-blue').props('dense outline').classes('text-xs cursor-pointer')
                                        _ox_badge_ref[0] = _ox_badge
                                        _ox_badge.on('click', _make_ox_toggle(_ox_lbl, _ox_badge_ref, _ox_eng, _ox_heb, _ox_st))
                                        from web.components.translation_report import create_report_button
                                        create_report_button(
                                            dataset='oxford', record_id=str(page.sys_id),
                                            field_name=_ox_field, direction='en2he',
                                            source_text=_ox_eng, translated_text=_ox_heb,
                                        )

                    # External Links
                    ui.separator().classes('my-3')
                    # Changed to H3 (or small title)
                    h3(tr('External link'), classes='text-xs font-bold mb-2', style='color: var(--text-secondary);')
                    with ui.row().classes('gap-2 flex-wrap'):
                        # NLI Ktiv
                        # NLI Ktiv
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
                        ui.link('NLI Ktiv', ktiv_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                        # Oxford Bodleian
                        if page.is_oxford and page.external_url:
                            ui.link('Oxford Bodleian', page.external_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                        # Cambridge CUDL
                        if page.is_cambridge and page.external_url:
                            ui.link('Cambridge CUDL', page.external_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                        # Princeton Geniza Project
                        if state.pgp_metadata and state.pgp_metadata.get('pgp_url'):
                            ui.link('Princeton Geniza Project', state.pgp_metadata['pgp_url'], new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                        # Library digital collection (from NLI crossref)
                        if page.library_viewer_url and page.library_viewer_url.get('url'):
                            lib_url = page.library_viewer_url
                            # Only show if not already covered by Oxford/Cambridge links above
                            if not (page.is_oxford and page.external_url) and not (page.is_cambridge and page.external_url):
                                ui.link(
                                    lib_url.get('label', tr('View in Library Catalog')),
                                    lib_url['url'], new_tab=True
                                ).classes('text-sm').style('color: var(--primary-600);')

                    # === PGP Metadata Section ===
                    if state.pgp_metadata:
                        ui.separator().classes('my-3')
                        # Header with link to PGP
                        with ui.row().classes('items-center gap-2 mb-2'):
                            h3(tr('Princeton Geniza Project'), classes='text-xs font-bold', style='color: var(--text-secondary);')
                            if state.pgp_metadata.get('pgp_url'):
                                ui.link('', state.pgp_metadata['pgp_url'], new_tab=True).props(
                                    'icon=open_in_new flat dense round size=xs'
                                ).style('color: var(--primary-600);').tooltip(tr('View on PGP'))

                        # Document Type + Languages (inline row)
                        doc_type = state.pgp_metadata.get('document_type')
                        lang_primary = state.pgp_metadata.get('languages_primary')
                        lang_secondary = state.pgp_metadata.get('languages_secondary')
                        if doc_type or lang_primary:
                            with ui.column().classes('gap-1 mb-2'):
                                ui.label(tr('Document Type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                type_parts = []
                                if doc_type:
                                    type_parts.append(doc_type)
                                if lang_primary:
                                    type_parts.append(lang_primary)
                                if lang_secondary:
                                    type_parts.append(lang_secondary)
                                type_text = ' \u00b7 '.join(type_parts)
                                # Show document_type_he when UI is Hebrew and translation toggle is on
                                _browse_type_he = None
                                _browse_type_lang = get_language()
                                _browse_pgpid = state.pgp_metadata.get('pgpid')
                                _browse_show_trans = False
                                try:
                                    _browse_show_trans = app.storage.user.get('show_translations', False)
                                except Exception:
                                    pass  # Translation lookup failed; continue without translation
                                if _browse_show_trans and _browse_type_lang == 'he' and _browse_pgpid:
                                    try:
                                        from shared.translation_service import TranslationService
                                        _tsvc_type = TranslationService(thread_safe=True)
                                        _browse_type_he = _tsvc_type.get_pgp_document_type_he(_browse_pgpid)
                                        _tsvc_type.close()
                                    except Exception:
                                        pass  # Translation lookup failed; continue without translation
                                if _browse_type_he:
                                    ui.label(_browse_type_he).classes('text-sm').style('color: var(--text-primary); direction: rtl;')
                                else:
                                    ui.label(type_text).classes('text-sm').style('color: var(--text-primary);')

                        # Tags (clickable badges)
                        tags = state.pgp_metadata.get('tags', [])
                        if tags:
                            with ui.column().classes('gap-1 mb-2'):
                                ui.label(tr('Tags')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                with ui.row().classes('gap-1 flex-wrap'):
                                    for tag in tags:
                                        ui.badge(tag, color='green').props('outline clickable').classes(
                                            'text-xs cursor-pointer'
                                        ).on('click', lambda t=tag: ui.navigate.to(f'/search?tag={quote(t)}'))

                        # Description (full length, with translate button or pre-translated)
                        description = (state.pgp_metadata.get('description') or '').strip()
                        if description:
                            with ui.column().classes('gap-1 mb-2'):
                                ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                # Phase 46: Show translated description when toggle is on
                                _show_trans_browse = False
                                try:
                                    _show_trans_browse = app.storage.user.get('show_translations', False)
                                except Exception:
                                    pass  # Translation lookup failed; continue without translation
                                _pgpid_browse = state.pgp_metadata.get('pgpid')
                                _trans_desc_he = None
                                _browse_lang = get_language()
                                if _show_trans_browse and _browse_lang == 'he' and _pgpid_browse:
                                    try:
                                        from shared.translation_service import TranslationService
                                        _tsvc = TranslationService(thread_safe=True)
                                        _trans_desc_he = _tsvc.get_pgp_description_he(_pgpid_browse)
                                        _tsvc.close()
                                    except Exception:
                                        pass  # Translation lookup failed; continue without translation
                                if _trans_desc_he:
                                    _br_st = {'showing_original': False}
                                    with ui.row().classes('w-full items-start gap-1'):
                                        _br_lbl = ui.label(_trans_desc_he).classes('flex-1 text-sm whitespace-pre-wrap').style(
                                            'color: var(--text-primary); direction: rtl;'
                                        )
                                        _br_badge_ref = [None]
                                        def _make_browse_toggle(lbl, badge_ref, orig, trans, flag):
                                            def handler():
                                                flag['showing_original'] = not flag['showing_original']
                                                if flag['showing_original']:
                                                    lbl.text = orig
                                                    lbl.style('color: var(--text-primary); direction: ltr; white-space: pre-wrap;')
                                                    badge_ref[0].text = tr('Original')
                                                else:
                                                    lbl.text = trans
                                                    lbl.style('color: var(--text-primary); direction: rtl; white-space: pre-wrap;')
                                                    badge_ref[0].text = tr('Translated')
                                            return handler
                                        _br_btn = ui.button(tr('Translated')).props(
                                            'flat dense no-caps size=xs'
                                        ).classes('text-xs px-1 py-0 rounded shrink-0 self-start mt-1').style(
                                            'background: #e0f2fe !important; color: #0369a1 !important; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                                        )
                                        _br_btn.on('click.stop', _make_browse_toggle(_br_lbl, _br_badge_ref, description, _trans_desc_he, _br_st))
                                        _br_badge_ref[0] = _br_btn
                                        from web.components.translation_report import create_report_button
                                        create_report_button(
                                            dataset='pgp', record_id=str(_pgpid_browse),
                                            field_name='description', direction='en2he',
                                            source_text=description, translated_text=_trans_desc_he,
                                        )
                                else:
                                    ui.label(description).classes('text-sm whitespace-pre-wrap').style('color: var(--text-primary);')

                        # Dates
                        inferred_display = state.pgp_metadata.get('inferred_date_display')
                        doc_date_standard = state.pgp_metadata.get('doc_date_standard')
                        doc_date_original = state.pgp_metadata.get('doc_date_original')
                        date_rationale = state.pgp_metadata.get('inferred_date_rationale')

                        # Show dates section if any date info exists
                        if inferred_display or doc_date_standard or doc_date_original:
                            with ui.column().classes('gap-1 mb-2'):
                                ui.label(tr('Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                # Primary: inferred display date, fallback to standard date
                                primary_date = inferred_display or doc_date_standard
                                if primary_date:
                                    ui.label(primary_date).classes('text-sm font-medium').style('color: var(--text-primary);')
                                # Secondary: original date (only if different from primary)
                                if doc_date_original and doc_date_original != primary_date:
                                    ui.label(f"({doc_date_original})").classes('text-xs').style('color: var(--text-tertiary);')
                                # Rationale
                                if date_rationale:
                                    ui.label(date_rationale).classes('text-sm').style('color: var(--text-tertiary); font-style: italic; font-size: 0.75rem;')

                    # === FJMS Catalog Metadata (pre-fetched in load_page) ===
                    from shared.fjms_service import merge_catalog_records, parse_textual_frame
                    fjms_data = state.fjms_data or {}
                    catalog_records = fjms_data.get('catalog_records')
                    if catalog_records:
                            ui.separator().classes('my-3')
                            # Section header with purple FJMS badge
                            with ui.row().classes('items-center gap-2 mb-2'):
                                h3(tr('FJMS Catalog'), classes='text-xs font-bold', style='color: var(--text-secondary);')
                                ui.badge('FJMS', color='purple').props('outline dense').classes('text-xs')

                            merged = merge_catalog_records(catalog_records)
                            lang = get_language()

                            # Title (language-aware)
                            title = merged.get('title_heb') if lang == 'he' else merged.get('title')
                            if title and title.strip():
                                with ui.column().classes('gap-1 mb-2'):
                                    ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(title).classes('text-sm').style('color: var(--text-primary);')

                            # Author (prominent, clickable link to catalog browse)
                            if merged.get('author_text') and merged['author_text'].strip():
                                with ui.column().classes('gap-1 mb-2'):
                                    ui.label(tr('Author')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.link(
                                        merged['author_text'],
                                        f'/catalog-browse?author={quote(merged["author_text"])}'
                                    ).classes('text-sm').style('color: var(--primary-600);')

                            # Copy Date and Place (inline row)
                            date = merged.get('copy_date')
                            place = merged.get('copy_place')
                            if date or place:
                                with ui.row().classes('gap-6 mb-2'):
                                    if date:
                                        with ui.column().classes('gap-1'):
                                            ui.label(tr('Copy Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                            ui.label(str(date)).classes('text-sm').style('color: var(--text-primary);')
                                    if place:
                                        with ui.column().classes('gap-1'):
                                            ui.label(tr('Place')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                            ui.label(place).classes('text-sm').style('color: var(--text-primary);')

                            # Content Identifications (TextualFrames)
                            frames = merged.get('textual_frames', [])
                            if frames:
                                with ui.column().classes('gap-1 mb-2'):
                                    ui.label(tr('Content Identification')).classes('text-xs font-bold').style('color: var(--text-secondary);')

                                    # Determine how many to show initially
                                    max_initial = 10
                                    show_frames = frames[:max_initial] if len(frames) > max_initial else frames

                                    for frame in show_frames:
                                        text = frame.get('heb') if lang == 'he' else frame.get('eng')
                                        if not text or not text.strip():
                                            # Fallback to other language if preferred is empty
                                            text = frame.get('eng') if lang == 'he' else frame.get('heb')
                                        if text and text.strip():
                                            category, content = parse_textual_frame(text)
                                            source = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                            with ui.row().classes('gap-1 items-baseline'):
                                                if category:
                                                    ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                                    ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                                else:
                                                    ui.label(text).classes('text-sm').style('color: var(--text-primary);')
                                                if source and source.strip():
                                                    ui.label(f'({source})').classes('text-xs').style('color: var(--text-tertiary);')

                                    # "Show all N identifications" expansion for 10+ frames
                                    if len(frames) > max_initial:
                                        remaining = frames[max_initial:]
                                        with ui.expansion(f'{tr("Show all")} {len(frames)} {tr("identifications")}').classes('text-xs'):
                                            for frame in remaining:
                                                text = frame.get('heb') if lang == 'he' else frame.get('eng')
                                                if not text or not text.strip():
                                                    text = frame.get('eng') if lang == 'he' else frame.get('heb')
                                                if text and text.strip():
                                                    category, content = parse_textual_frame(text)
                                                    source = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                                    with ui.row().classes('gap-1 items-baseline'):
                                                        if category:
                                                            ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                                            ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                                        else:
                                                            ui.label(text).classes('text-sm').style('color: var(--text-primary);')
                                                        if source and source.strip():
                                                            ui.label(f'({source})').classes('text-xs').style('color: var(--text-tertiary);')

                    # === FJMS Domain Classifications ===
                    domains = fjms_data.get('domains')
                    if domains:
                        with ui.column().classes('gap-1 mb-2'):
                            ui.label(tr('Subject Domains')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            with ui.row().classes('gap-2 flex-wrap'):
                                lang = get_language()
                                # Deduplicate: skip parent if child already shown
                                all_domain_names = {d['domain'] for d in domains}
                                for dom in domains:
                                    parent = dom.get('parent_domain')
                                    if parent and parent in all_domain_names and parent != dom['domain']:
                                        continue
                                    display_name = dom['domain_heb'] if lang == 'he' else dom['domain']
                                    ui.link(
                                        display_name,
                                        f'/catalog-browse?domain={quote(dom["domain"])}'
                                    ).classes('text-sm').style('color: var(--primary-600);')

                    # === Phase 33: Catalog Cross-References ===
                    cat_refs = fjms_data.get('catalog_refs')
                    if cat_refs:
                        ui.separator().classes('my-2')
                        with ui.row().classes('items-center gap-2 mb-2'):
                            h3(tr('Catalog References'), classes='text-xs font-bold', style='color: var(--text-secondary);')
                            ui.badge('FJMS', color='purple').props('outline dense').classes('text-xs')

                        with ui.column().classes('gap-1'):
                            for ref in cat_refs:
                                acronym = ref.get('cat_acronym', '')
                                cat_entry = ref.get('catalog_entry', '')
                                display = f"{acronym} #{cat_entry}" if cat_entry else acronym
                                ui.label(display).classes('text-sm').style('color: var(--text-primary);')

                    # === Phase 33: Scholarly Source Names ===
                    source_names = fjms_data.get('source_names')
                    if source_names:
                        with ui.column().classes('gap-1 mb-2 mt-2'):
                            ui.label(tr('Scholarly Sources')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            with ui.row().classes('gap-2 flex-wrap'):
                                for sn in source_names:
                                    ui.label(sn).classes('text-sm').style('color: var(--text-primary);')

                    # === Phase 33: Collection & Storage ===
                    if _collection_storage:
                        coll = _collection_storage.get('collection_name', '')
                        box = _collection_storage.get('ob_box', '')
                        vol_cs = _collection_storage.get('ob_volume', '')
                        folio_cs = _collection_storage.get('ob_folio', '')
                        if coll or box or vol_cs or folio_cs:
                            with ui.column().classes('gap-1 mb-2'):
                                ui.label(tr('Collection & Storage')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                parts = []
                                if coll:
                                    parts.append(coll)
                                storage = []
                                if box:
                                    storage.append(f'Box {box}')
                                if vol_cs:
                                    storage.append(f'Vol. {vol_cs}')
                                if folio_cs:
                                    storage.append(f'Fol. {folio_cs}')
                                if storage:
                                    parts.append(', '.join(storage))
                                ui.label(' - '.join(parts)).classes('text-sm').style('color: var(--text-primary);')

                    # === Related Fragments Section ===
                    pgpid_for_joins = state.pgp_metadata.get('pgpid') if state.pgp_metadata else None
                    joins_data = fetch_connected_fragments(
                        shelfmark=page.shelfmark,
                        document_id=page.sys_id,
                        pgpid=pgpid_for_joins
                    )

                    if joins_data.get('total_fragments', 1) > 1:
                        ui.separator().classes('my-3')
                        total_frags = joins_data['total_fragments']
                        other_count = total_frags - 1  # Exclude current fragment from badge count

                        # Header row: title + count badge
                        with ui.row().classes('items-center gap-2 mb-2'):
                            h3(tr('Related Fragments'), classes='text-xs font-bold', style='color: var(--text-secondary);')
                            ui.badge(str(other_count), color='green').props('dense').classes('text-xs')

                        # Build relationship/source lookup from joins data
                        current_shelfmark_upper = (page.shelfmark or '').upper()
                        joins_list = joins_data.get('joins', [])
                        frag_info_map = {}  # shelfmark_upper -> {sources, relationship_type, scholar_name}
                        for join_entry in joins_list:
                            fa = join_entry.get('fragment_a', '')
                            fb = join_entry.get('fragment_b', '')
                            sources = join_entry.get('sources', [join_entry.get('source', 'user')])
                            rel = join_entry.get('relationship_type', '')
                            scholar = join_entry.get('scholar_name', '')
                            # Map the OTHER fragment in each join pair, aggregating sources
                            for target_key, check_key in [(fb.upper(), fa.upper()), (fa.upper(), fb.upper())]:
                                if check_key == current_shelfmark_upper and target_key:
                                    if target_key in frag_info_map:
                                        existing = frag_info_map[target_key]
                                        for s in sources:
                                            if s not in existing['sources']:
                                                existing['sources'].append(s)
                                        if scholar and not existing.get('scholar_name'):
                                            existing['scholar_name'] = scholar
                                    else:
                                        frag_info_map[target_key] = {'sources': list(sources), 'relationship_type': rel, 'scholar_name': scholar}

                        # Clickable fragment rows (skip current fragment)
                        for frag_shelfmark in joins_data.get('fragments', []):
                            if frag_shelfmark.upper() == current_shelfmark_upper:
                                continue

                            info = frag_info_map.get(frag_shelfmark.upper(), {})
                            frag_sources = info.get('sources', [info.get('source', 'user')])
                            frag_rel_type = info.get('relationship_type', '')

                            # Navigation handler using search_shelfmark pattern
                            def make_nav_to(target=frag_shelfmark):
                                async def nav():
                                    state.shelfmark_query = target
                                    await search_shelfmark()
                                return nav

                            with ui.row().classes(
                                'items-center gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded w-full'
                            ).on('click', make_nav_to()):
                                ui.icon('description').classes('text-gray-500').style('font-size: 1.1rem;')
                                ui.label(frag_shelfmark).classes('text-sm font-medium')
                                for frag_src in frag_sources:
                                    if frag_src == 'FJMS':
                                        ui.badge('FJMS', color='purple').props('outline dense').classes('text-xs')
                                    elif frag_src and frag_src != 'user':
                                        ui.badge(frag_src, color='blue').props('outline dense').classes('text-xs')
                                if frag_rel_type:
                                    rel_label = {
                                        'physical_join': tr('Physical join'),
                                        'physical': tr('Physical join'),
                                        'same_composition': tr('Same composition'),
                                        'content': tr('Same composition'),
                                        'uncertain': tr('Unknown'),
                                    }.get(frag_rel_type, frag_rel_type)
                                    ui.label(rel_label).classes('text-xs text-gray-500')
                                scholar = info.get('scholar_name', '')
                                if scholar:
                                    ui.label(f"({scholar})").classes('text-xs text-gray-400 italic')
                                ui.element('div').classes('flex-grow')
                                ui.icon('arrow_back' if is_rtl() else 'arrow_forward').classes('text-gray-400')

                        # View whole document button + dialog
                        frag_details_for_dialog = joins_data.get('fragment_details', [])
                        doc_pgpid = pgpid_for_joins

                        def open_document_viewer():
                            """Open dialog showing all fragment images and full transcription."""
                            from web.document_service import get_transcription_for_document
                            dialog = ui.dialog()
                            with dialog, ui.card().classes('w-[90vw] max-w-[900px] max-h-[90vh] p-0'):
                                # Header
                                with ui.row().classes('w-full items-center justify-between p-4 border-b').style(
                                    'background: linear-gradient(135deg, #15803d 0%, #166534 100%);'
                                ):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('auto_stories').classes('text-xl').style('color: white !important;')
                                        header_text = f'{tr("Document")} #{doc_pgpid}' if doc_pgpid else tr('Document')
                                        ui.label(header_text).classes('text-lg font-bold').style('color: white !important;')
                                        ui.badge(f'{total_frags} {tr("fragments")}', color='white').props('outline dense').classes('text-xs').style('color: white !important;')
                                    ui.button(icon='close', on_click=dialog.close).props('flat round size=sm text-color=white')

                                # Scrollable content
                                with ui.scroll_area().classes('w-full').style('max-height: calc(90vh - 70px);'):
                                    with ui.column().classes('w-full p-4 gap-4'):
                                        # === Images for each fragment ===
                                        for fd in frag_details_for_dialog:
                                            frag_sm = fd.get('shelfmark', '')
                                            frag_sid = fd.get('document_id', '')
                                            if not frag_sid:
                                                continue

                                            # Fragment header
                                            is_current = frag_sm.upper() == current_shelfmark_upper
                                            with ui.row().classes('items-center gap-2 mt-2'):
                                                ui.icon('description', size='xs').classes('text-green-600')
                                                ui.label(frag_sm).classes('text-sm font-bold').style('color: var(--text-primary);')
                                                if is_current:
                                                    ui.badge(tr('Current'), color='green').props('dense').classes('text-xs')

                                            # Oxford detection for correct image endpoint
                                            frag_library_code = ''
                                            if getattr(state, 'meta_mgr', None) and frag_sid:
                                                try:
                                                    frag_library_code = state.meta_mgr.get_library_for_id(frag_sid) or ''
                                                except Exception:
                                                    frag_library_code = ''  # Library code lookup failed; use empty string
                                            frag_is_oxford = is_oxford_manuscript(frag_sm, frag_library_code)

                                            # Show recto and verso images side by side
                                            with ui.row().classes('w-full gap-2 flex-wrap justify-center'):
                                                for pg_idx in range(2):  # 0=recto, 1=verso
                                                    pg_label = tr('Recto') if pg_idx == 0 else tr('Verso')
                                                    if frag_is_oxford:
                                                        img_src = get_oxford_direct_image_url(frag_sm, pg_idx)
                                                        if not img_src:
                                                            img_src = f'/api/oxford_image/{frag_sid}?page={pg_idx}'
                                                    else:
                                                        img_src = f'/api/nli_image_by_sysid/{frag_sid}?page={pg_idx}'

                                                    with ui.column().classes('items-center'):
                                                        ui.label(pg_label).classes('text-xs text-gray-500 mb-1')
                                                        safe_sid = frag_sid.replace("'", "\\'")
                                                        is_ox_js = 'true' if frag_is_oxford else 'false'
                                                        ui.html(f'''
                                                            <img src="{img_src}"
                                                                 style="max-height: 350px; max-width: 400px; object-fit: contain; border: 1px solid #e5e7eb; border-radius: 4px;"
                                                                 loading="lazy"
                                                                 onerror="
                                                                     if ({is_ox_js}) {{
                                                                         this.style.display='none';
                                                                         this.parentElement.style.display='none';
                                                                     }} else {{
                                                                         var ox='/api/oxford_image/{safe_sid}?page={pg_idx}';
                                                                         if (this.src.indexOf('oxford_image')===-1) {{
                                                                             this.onerror=function(){{ this.style.display='none'; this.parentElement.style.display='none'; }};
                                                                             this.src=ox;
                                                                         }} else {{
                                                                             this.style.display='none';
                                                                             this.parentElement.style.display='none';
                                                                         }}
                                                                     }}
                                                                 "
                                                            />
                                                        ''', sanitize=False)
                                            ui.separator().classes('my-2')

                                        # === Full Transcription ===
                                        if doc_pgpid:
                                            full_text = get_transcription_for_document(doc_pgpid)
                                            if full_text:
                                                with ui.row().classes('items-center gap-2 mb-2'):
                                                    ui.icon('text_snippet', size='xs').classes('text-green-600')
                                                    ui.label(tr('Full Transcription')).classes('text-sm font-bold').style('color: var(--text-primary);')
                                                    ui.badge('PGP', color='blue').props('outline dense').classes('text-xs')
                                                ui.html(f'''
                                                    <div dir="rtl" style="
                                                        white-space: pre-wrap;
                                                        font-family: 'SBL Hebrew', 'Frank Ruehl CLM', 'Ezra SIL', serif;
                                                        font-size: 1.1rem;
                                                        line-height: 1.8;
                                                        padding: 12px;
                                                        background: var(--bg-secondary, #f9fafb);
                                                        border-radius: 8px;
                                                        border: 1px solid #e5e7eb;
                                                        color: var(--text-primary);
                                                    ">{full_text}</div>
                                                ''', sanitize=False)

                            dialog.open()

                        ui.button(
                            tr('View whole document'), icon='auto_stories',
                            on_click=open_document_viewer
                        ).props('dense outline color=green').classes('w-full mt-2')

                    # Export
                    ui.separator().classes('my-3')
                    # Changed to H3 (or small title)
                    h3(tr('Export'), classes='text-xs font-bold mb-2', style='color: var(--text-secondary);')
                    with ui.row().classes('gap-2'):
                        ui.button(
                            tr('Export Word'),
                            icon='description',
                            on_click=export_browse_data
                        ).props('flat dense color=green')

            # === Main Content ===
            if state.view_all:
                # Show all pages
                with ui.card().classes('w-full').style('min-height: 60vh;'):
                    # Header
                    with ui.row().classes('w-full items-center justify-between p-4 border-b').style('background: var(--bg-tertiary);'):
                        with ui.row().classes('items-center gap-2'):
                            # Changed to H2
                            h2(tr('Full Manuscript View'), classes='font-bold text-lg')
                            ui.label(f"({len(state.full_manuscript)} {tr('pages')})").classes('ml-2').style('color: var(--text-secondary);')

                        # Back to single page button
                        ui.button(
                            tr('Back to Page View'),
                            icon='arrow_forward' if is_rtl() else 'arrow_back',
                            on_click=toggle_view_all
                        ).props('flat dense color=green')

                    # All pages in scroll area
                    with ui.scroll_area().classes('w-full').style('height: 70vh; padding: 24px;'):
                        for idx, doc_page in enumerate(state.full_manuscript):
                            # Page separator
                            if idx > 0:
                                ui.separator().classes('my-6')

                            # Page header
                            with ui.row().classes('w-full items-center gap-2 mb-2'):
                                # Changed to H3
                                h3(f"{tr('Page')} {doc_page.p_num}", classes='font-bold text-green-700')
                                if doc_page.full_header:
                                    ui.label(doc_page.full_header).classes('text-xs font-mono').style('color: var(--text-tertiary);')

                            # Page text
                            if doc_page.text:
                                display_text = doc_page.text
                                if state.highlight_terms:
                                    display_text = highlight_text(doc_page.text)
                                    ui.html(f'<div class="transcription-text" style="font-size: 1.3rem; line-height: 2.0;">{display_text}</div>', sanitize=False)
                                else:
                                    ui.label(doc_page.text).style(
                                        'font-size: 1.3rem; line-height: 2.0; direction: rtl; text-align: right; '
                                        'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap;'
                                    )
                            else:
                                ui.label(tr('No text available')).classes('italic').style('color: var(--text-muted);')
            elif state.view_joined:
                # === V3 Reading Desk: Dual-Pane Synchronized View ===
                # Left pane: stacked images with per-image zoom/rotate/drag
                # Right pane: stacked texts with per-fragment PGP version selector
                # IntersectionObserver synchronizes scrolling between panes
                current_shelfmark_upper = (page.shelfmark or '').upper()

                async def rd_navigate_to_fragment(target_sm):
                    """Exit reading desk and navigate to a specific fragment."""
                    exit_joined_view()
                    state.shelfmark_query = target_sm
                    await search_shelfmark()

                def remove_from_desk(sys_id_to_remove):
                    """Remove a fragment from the reading desk by sys_id."""
                    state.reading_desk_entries = [
                        e for e in state.reading_desk_entries
                        if e.get('sys_id') != sys_id_to_remove
                    ]
                    if not state.reading_desk_entries:
                        # All removed -- exit reading desk
                        exit_joined_view()
                        return
                    _persist_reading_desk_state()
                    update_content()

                def toolbar_add_by_shelfmark(shelfmark_text):
                    """Add a manuscript to the reading desk by shelfmark search."""
                    if not shelfmark_text or not shelfmark_text.strip():
                        return
                    query = shelfmark_text.strip()
                    try:
                        results, exact_match = service.search_by_shelfmark(query, limit=5)
                        if not results:
                            ui.notify(f'{tr("No manuscript found")}: {query}', type='warning')
                            return
                        # Use first/exact match
                        result = results[0]
                        found_sid = result.sys_id
                        found_sm = result.shelfmark or query
                        _add_sys_id_to_reading_desk(found_sid, found_sm)
                    except RuntimeError:
                        # Stale slot reference after UI rebuild -- silently ignore
                        pass
                    except Exception as e:
                        try:
                            ui.notify(f'{tr("Error")}: {str(e)}', type='negative')
                        except RuntimeError:
                            pass

                def show_add_from_list_dialog():
                    """Show dialog to add manuscripts from personal lists to the reading desk."""
                    from web.state import state as app_state
                    lists_mgr = app_state.lists_mgr
                    if not lists_mgr:
                        ui.notify(tr('Lists not available'), type='warning')
                        return

                    with ui.dialog() as dlg, ui.card().classes('p-0 min-w-[400px] max-w-[500px]'):
                        # Header
                        with ui.row().classes('w-full items-center justify-between p-4 border-b').style(
                            'background: linear-gradient(135deg, #15803d 0%, #166534 100%);'
                        ):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('playlist_add').classes('text-xl').style('color: white !important;')
                                ui.label(tr('Add from List')).classes('text-lg font-bold').style('color: white !important;')
                            ui.button(icon='close', on_click=dlg.close).props('flat round size=sm text-color=white')

                        # Content: list of user's lists with expandable manuscript details
                        with ui.scroll_area().classes('w-full').style('max-height: 400px;'):
                            with ui.column().classes('w-full p-4 gap-2'):
                                all_lists = lists_mgr.get_all_lists(include_recent=False)
                                if not all_lists:
                                    with ui.column().classes('items-center py-6'):
                                        ui.icon('list_alt', size='3rem').classes('text-gray-300')
                                        ui.label(tr('No lists found')).classes('text-gray-500')
                                else:
                                    # Track per-list checkbox selections for "Add Selected" feature
                                    selections = {}  # list_id -> list of (sys_id, shelfmark, checkbox) tuples

                                    for lst in all_lists:
                                        list_id = lst.get('id', '')
                                        list_name = lst.get('name', lst.get('name_en', list_id))
                                        list_color = lst.get('color', '#FFD700')

                                        # Pre-fetch items for this list to show count and contents
                                        list_items = []
                                        try:
                                            list_items = lists_mgr.get_items_in_list_sync(list_id) or []
                                        except Exception:
                                            pass  # Shelfmark lookup failed; use fallback identifier

                                        # Resolve shelfmarks for display
                                        resolved_items = []
                                        for item in list_items:
                                            item_sid = item.get('sys_id', '')
                                            if not item_sid:
                                                continue
                                            item_sm = item.get('shelfmark') or item.get('shelfmark_override', '')
                                            if not item_sm and app_state.meta_mgr:
                                                try:
                                                    item_sm, _ = app_state.meta_mgr.get_meta_for_id(item_sid)
                                                except Exception:
                                                    pass  # Shelfmark lookup failed; use fallback identifier
                                            if not item_sm:
                                                item_sm = item_sid
                                            resolved_items.append({'sys_id': item_sid, 'shelfmark': item_sm})

                                        item_count = len(resolved_items)

                                        def make_add_list_handler(lid=list_id, lname=list_name, dialog_ref=dlg, r_items=resolved_items):
                                            def add_list_items():
                                                if not r_items:
                                                    ui.notify(f'{lname}: {tr("No items")}', type='info')
                                                    return
                                                added_count = 0
                                                existing_sids = {e.get('sys_id') for e in state.reading_desk_entries}
                                                for ri in r_items:
                                                    item_sid = ri['sys_id']
                                                    item_sm = ri['shelfmark']
                                                    if item_sid in existing_sids:
                                                        continue
                                                    # Load full data for the entry
                                                    from shared.document_service import get_all_sources_for_fragment as ld_src, get_document_for_fragment as ld_doc
                                                    pages = service.get_full_manuscript(item_sid)
                                                    sources = []
                                                    pgp_doc_data = None
                                                    try:
                                                        sources = ld_src(item_sid) or []
                                                        pgp_doc_data = ld_doc(item_sid)
                                                    except Exception:
                                                        pass  # Shelfmark lookup failed; use fallback identifier
                                                    state.reading_desk_entries.append({
                                                        'sys_id': item_sid,
                                                        'shelfmark': item_sm,
                                                        'pages': pages or [],
                                                        'sources': sources,
                                                        'pgp_doc': pgp_doc_data or {}
                                                    })
                                                    existing_sids.add(item_sid)
                                                    added_count += 1
                                                if added_count > 0:
                                                    _persist_reading_desk_state()
                                                    update_content()
                                                    ui.notify(f'{added_count} {tr("manuscripts added")}', type='positive')
                                                else:
                                                    ui.notify(tr('All items already in Reading Desk'), type='info')
                                                dialog_ref.close()
                                            return add_list_items

                                        with ui.expansion(
                                            group='rd-lists'
                                        ).classes('w-full').style(
                                            'border: 1px solid var(--border-subtle, #e5e7eb); border-radius: 8px; margin-bottom: 4px;'
                                        ) as expansion:
                                            # Custom header slot
                                            with expansion.add_slot('header'):
                                                with ui.row().classes('items-center gap-3 w-full'):
                                                    ui.icon('circle').style(f'color: {list_color}; font-size: 0.8rem;')
                                                    ui.label(list_name).classes('font-medium flex-1')
                                                    ui.badge(str(item_count), color='green').props('dense').classes('text-xs')

                                            # Expansion content: list of manuscripts + Add All button
                                            with ui.column().classes('w-full gap-1 pb-2'):
                                                if not resolved_items:
                                                    ui.label(tr('No items')).classes('text-sm italic text-gray-400 px-2')
                                                else:
                                                    for ri in resolved_items:
                                                        already_in = ri['sys_id'] in {e.get('sys_id') for e in state.reading_desk_entries}
                                                        with ui.row().classes('items-center gap-2 px-2 py-1').style(
                                                            'border-bottom: 1px solid var(--border-subtle, #f0f0f0);'
                                                        ):
                                                            if already_in:
                                                                ui.icon('check', size='xs').classes('text-green-500').tooltip(tr('Already in Reading Desk'))
                                                                ui.label(ri['shelfmark']).classes('text-sm flex-1').style('color: var(--text-primary);')
                                                            else:
                                                                cb = ui.checkbox(ri['shelfmark']).classes('text-sm flex-1')
                                                                if list_id not in selections:
                                                                    selections[list_id] = []
                                                                selections[list_id].append((ri['sys_id'], ri['shelfmark'], cb))

                                                    # Add Selected handler factory
                                                    def make_add_selected_handler(lid=list_id, dialog_ref=dlg):
                                                        def add_selected_items():
                                                            if lid not in selections:
                                                                return
                                                            added_count = 0
                                                            existing_sids = {e.get('sys_id') for e in state.reading_desk_entries}
                                                            for s_sid, s_sm, s_cb in selections[lid]:
                                                                if s_cb.value and s_sid not in existing_sids:
                                                                    _add_sys_id_to_reading_desk(s_sid, s_sm)
                                                                    existing_sids.add(s_sid)
                                                                    added_count += 1
                                                            if added_count > 0:
                                                                try:
                                                                    ui.notify(f'{added_count} {tr("manuscripts added")}', type='positive')
                                                                except RuntimeError:
                                                                    pass
                                                                dialog_ref.close()
                                                            else:
                                                                try:
                                                                    ui.notify(tr('No items selected'), type='info')
                                                                except RuntimeError:
                                                                    pass
                                                        return add_selected_items

                                                    # Button row: Add Selected + Add All
                                                    with ui.row().classes('w-full justify-end pt-2 px-2 gap-2'):
                                                        ui.button(
                                                            tr('Add Selected'),
                                                            icon='check_circle',
                                                            on_click=make_add_selected_handler()
                                                        ).props('dense outline color=green').classes('text-sm')

                                                        ui.button(
                                                            f'{tr("Add All")} ({item_count})',
                                                            icon='playlist_add',
                                                            on_click=make_add_list_handler()
                                                        ).props('dense color=green').classes('text-sm')

                    dlg.open()

                # Header bar
                with ui.card().classes('w-full mb-2').style(
                    'background: linear-gradient(135deg, #15803d 0%, #166534 100%) !important;'
                ):
                    with ui.row().classes('w-full items-center justify-between p-3'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('auto_stories').classes('text-xl').style('color: white !important;')
                            if state.joined_pgpid:
                                header_txt = f'{tr("Document")} #{state.joined_pgpid}'
                            else:
                                header_txt = tr('Reading Desk')
                            ui.label(header_txt).classes('text-lg font-bold').style('color: white !important;')
                            ui.badge(
                                f'{len(state.reading_desk_entries)} {tr("fragments")}',
                            ).props('dense').classes('text-xs').style(
                                'border: 1px solid white; color: white !important; background: transparent;'
                            )

                        ui.button(
                            tr('Back to Page View'),
                            icon='arrow_forward' if is_rtl() else 'arrow_back',
                            on_click=exit_joined_view
                        ).props('flat dense text-color=white')

                # === Reading Desk Toolbar ===
                with ui.card().classes('w-full mb-2 p-2').style(
                    'background: var(--bg-tertiary, #f3f4f6); border: 1px solid var(--border-light, #e5e7eb);'
                ):
                    with ui.row().classes('w-full items-center gap-3'):
                        # Shelfmark input for adding manuscripts
                        rd_shelfmark_input = ui.input(
                            placeholder=tr('Add by shelfmark...'),
                        ).props('dense outlined clearable color=green').classes('flex-1').style('max-width: 300px;')

                        def rd_toolbar_add():
                            toolbar_add_by_shelfmark(rd_shelfmark_input.value)
                            rd_shelfmark_input.value = ''

                        rd_shelfmark_input.on('keydown.enter', rd_toolbar_add)

                        ui.button(
                            tr('Add'), icon='add',
                            on_click=rd_toolbar_add
                        ).props('dense color=green')

                        # Separator
                        ui.separator().props('vertical').classes('h-6')

                        # Add from List button
                        ui.button(
                            tr('Add from List'), icon='playlist_add',
                            on_click=show_add_from_list_dialog
                        ).props('flat dense color=green')

                        ui.element('div').classes('flex-grow')

                        # Fragment count
                        ui.label(
                            f'{len(state.reading_desk_entries)} {tr("fragments")}'
                        ).classes('text-sm').style('color: var(--text-secondary);')

                # Two-panel flex row (same pattern as single-page view)
                with ui.element('div').classes('reading-desk-panels').style(
                    'display: flex; flex-direction: row; gap: 16px; min-height: 70vh; width: 100%;'
                ):

                    # === LEFT PANE: Image Stack ===
                    with ui.card().style('flex: 0 0 50%; min-height: 70vh; display: flex; flex-direction: column;'):
                        ui.label(tr('Manuscript Images')).classes('text-sm font-semibold p-2 border-b').style(
                            'color: var(--text-secondary); background: #1a1a1a; color: white; border-radius: 8px 8px 0 0;'
                        )
                        with ui.scroll_area().classes('rd-image-pane w-full').style('flex: 1; height: calc(70vh - 40px);'):
                            for frag_idx, entry in enumerate(state.reading_desk_entries):
                                frag_sm = entry.get('shelfmark', '')
                                frag_sid = entry.get('sys_id', '')
                                frag_pages = entry.get('pages', [])
                                is_current_frag = frag_sm.upper() == current_shelfmark_upper

                                # Fragment separator
                                if frag_idx > 0:
                                    ui.separator().classes('my-4')

                                # Fragment header (clickable link to navigate)
                                with ui.element('div').props(f'id="rd-img-frag-{frag_idx}"').classes('reading-desk-fragment'):
                                    with ui.row().classes('items-center gap-2 p-2 w-full').style(
                                        'background: var(--bg-tertiary, #f3f4f6); border-radius: 4px;'
                                    ):
                                        ui.icon('description').classes('text-green-600')

                                        def make_nav_handler(sm=frag_sm):
                                            return lambda: rd_navigate_to_fragment(sm)

                                        ui.label(frag_sm).classes(
                                            'font-bold text-base cursor-pointer hover:underline'
                                        ).style('color: var(--text-primary);').on('click', make_nav_handler())
                                        if is_current_frag:
                                            ui.badge(tr('Current'), color='green').props('dense').classes('text-xs')
                                        ui.element('div').classes('flex-grow')

                                        def make_remove_img(sid=frag_sid):
                                            return lambda: remove_from_desk(sid)

                                        ui.button(
                                            icon='close', on_click=make_remove_img()
                                        ).props('flat round size=xs').classes('text-gray-400 hover:text-red-500').tooltip(tr('Remove'))

                                # Oxford detection
                                frag_library_code = ''
                                if getattr(state, 'meta_mgr', None) and frag_sid:
                                    try:
                                        frag_library_code = state.meta_mgr.get_library_for_id(frag_sid) or ''
                                    except Exception:
                                        frag_library_code = ''  # Library code lookup failed; use empty string
                                frag_is_oxford = is_oxford_manuscript(frag_sm, frag_library_code)

                                if not frag_pages:
                                    frag_pages = [type('P', (), {'p_num': 1, 'text': '', 'full_header': '', 'fl_id': ''})(),
                                                  type('P', (), {'p_num': 2, 'text': '', 'full_header': '', 'fl_id': ''})()]

                                # Render each page image with its own controls
                                for pg_i, pg in enumerate(frag_pages):
                                    pg_num = pg.p_num if hasattr(pg, 'p_num') else (pg_i + 1)
                                    pg_idx = max(0, pg_num - 1)
                                    pg_label = tr('Recto') if pg_idx == 0 else tr('Verso')
                                    viewer_id = f'rd-viewer-{frag_idx}-{pg_i}'

                                    # Image URL
                                    if frag_is_oxford:
                                        frag_img_url = get_oxford_direct_image_url(frag_sm, pg_idx)
                                        if not frag_img_url:
                                            frag_img_url = f'/api/oxford_image/{frag_sid}?page={pg_idx}'
                                    else:
                                        frag_img_url = f'/api/nli_image_by_sysid/{frag_sid}?page={pg_idx}'

                                    # Page label
                                    ui.label(f'{pg_label}').classes('text-xs font-medium text-gray-500 mt-2 ml-2')

                                    # Per-image controls bar
                                    with ui.row().classes('items-center gap-1 px-2 py-1').style(
                                        'background: #1a1a1a; border-radius: 4px; margin: 0 4px;'
                                    ):
                                        ui.button(
                                            icon='remove',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdZoom('{vid}', -0.25)")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Zoom out'))
                                        ui.html(f'<span id="{viewer_id}-zoom-label" style="color: white; font-size: 0.75rem; min-width: 36px; text-align: center;">100%</span>', sanitize=False)
                                        ui.button(
                                            icon='add',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdZoom('{vid}', 0.25)")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Zoom in'))
                                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                        ui.button(
                                            icon='rotate_left',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdRotate('{vid}', -90)")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Rotate Left'))
                                        ui.button(
                                            icon='rotate_right',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdRotate('{vid}', 90)")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Rotate Right'))
                                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                        ui.button(
                                            icon='restart_alt',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdResetView('{vid}')")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Reset View'))

                                    # Per-image adjustment controls
                                    with ui.row().classes('items-center gap-1 px-2 py-1').style(
                                        'background: #1a1a1a; margin: 0 4px; border-top: 1px solid #333;'
                                    ):
                                        ui.icon('brightness_6').style('color: white; font-size: 0.85rem;').tooltip(tr('Brightness'))
                                        _rd_b_sl = ui.slider(
                                            min=-100, max=100, step=1, value=0,
                                            on_change=lambda e, vid=viewer_id: ui.run_javascript(f"window.rdSetBrightness('{vid}', {e.value})")
                                        ).props('dark dense').classes('w-16')
                                        ui.icon('contrast').style('color: white; font-size: 0.85rem;').tooltip(tr('Contrast'))
                                        _rd_c_sl = ui.slider(
                                            min=-100, max=100, step=1, value=0,
                                            on_change=lambda e, vid=viewer_id: ui.run_javascript(f"window.rdSetContrast('{vid}', {e.value})")
                                        ).props('dark dense').classes('w-16')
                                        ui.icon('timeline').style('color: white; font-size: 0.85rem;').tooltip(tr('Gamma'))
                                        _rd_g_sl = ui.slider(
                                            min=20, max=300, step=1, value=100,
                                            on_change=lambda e, vid=viewer_id: ui.run_javascript(f"window.rdSetGamma('{vid}', {e.value / 100})")
                                        ).props('dark dense').classes('w-16')
                                        ui.button(
                                            icon='exposure',
                                            on_click=lambda vid=viewer_id: ui.run_javascript(f"window.rdToggleInvert('{vid}')")
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Invert Colors'))
                                        # Store refs for reset
                                        _rd_b_ref, _rd_c_ref, _rd_g_ref = _rd_b_sl, _rd_c_sl, _rd_g_sl
                                        def _rd_reset(vid=viewer_id, b_sl=_rd_b_ref, c_sl=_rd_c_ref, g_sl=_rd_g_ref):
                                            b_sl.value = 0; c_sl.value = 0; g_sl.value = 100
                                            ui.run_javascript(f"window.rdResetAdjustments('{vid}')")
                                        ui.button(
                                            icon='restart_alt',
                                            on_click=_rd_reset
                                        ).props('flat round size=xs text-color=white').tooltip(tr('Reset Image'))

                                    # Image display area with zoom/drag support
                                    safe_sid = frag_sid.replace("'", "\\'")
                                    is_ox_js = 'true' if frag_is_oxford else 'false'
                                    with ui.element('div').style(
                                        'background: #1a1a1a; border-radius: 0 0 4px 4px; overflow: hidden; '
                                        'display: flex; align-items: center; justify-content: center; '
                                        'min-height: 350px; margin: 0 4px 8px 4px; position: relative;'
                                    ):
                                        ui.html(f'''
                                            <img id="{viewer_id}"
                                                 class="rd-zoomable"
                                                 src="{frag_img_url}"
                                                 style="max-height: 500px; max-width: 100%; object-fit: contain; cursor: grab; transition: none;"
                                                 loading="lazy"
                                                 draggable="false"
                                                 onerror="
                                                     if ({is_ox_js}) {{
                                                         this.style.display='none';
                                                     }} else {{
                                                         var ox='/api/oxford_image/{safe_sid}?page={pg_idx}';
                                                         if (this.src.indexOf('oxford_image')===-1) {{
                                                             this.onerror=function(){{ this.style.display='none'; }};
                                                             this.src=ox;
                                                         }} else {{
                                                             this.style.display='none';
                                                         }}
                                                     }}
                                                 "
                                            />
                                        ''', sanitize=False)

                    # === RIGHT PANE: Text Stack ===
                    with ui.card().style('flex: 1 1 auto; min-width: 0; min-height: 70vh; display: flex; flex-direction: column;'):
                        ui.label(tr('Transcriptions')).classes('text-sm font-semibold p-2 border-b').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.scroll_area().classes('rd-text-pane w-full').style('flex: 1; height: calc(70vh - 40px);'):
                            for frag_idx, entry in enumerate(state.reading_desk_entries):
                                frag_sm = entry.get('shelfmark', '')
                                frag_sid = entry.get('sys_id', '')
                                frag_pages = entry.get('pages', [])
                                frag_sources = entry.get('sources', [])
                                frag_pgp_doc = entry.get('pgp_doc', {})
                                is_current_frag = frag_sm.upper() == current_shelfmark_upper

                                # Fragment separator
                                if frag_idx > 0:
                                    ui.separator().classes('my-4')

                                # Fragment header (clickable link)
                                with ui.element('div').props(f'id="rd-text-frag-{frag_idx}"').classes('reading-desk-fragment'):
                                    with ui.row().classes('items-center gap-2 p-2 w-full').style(
                                        'background: var(--bg-tertiary, #f3f4f6); border-radius: 4px;'
                                    ):
                                        ui.icon('description').classes('text-green-600')

                                        def make_nav_handler_text(sm=frag_sm):
                                            return lambda: rd_navigate_to_fragment(sm)

                                        ui.label(frag_sm).classes(
                                            'font-bold text-base cursor-pointer hover:underline'
                                        ).style('color: var(--text-primary);').on('click', make_nav_handler_text())
                                        if is_current_frag:
                                            ui.badge(tr('Current'), color='green').props('dense').classes('text-xs')
                                        ui.element('div').classes('flex-grow')

                                        def make_remove_text(sid=frag_sid):
                                            return lambda: remove_from_desk(sid)

                                        ui.button(
                                            icon='close', on_click=make_remove_text()
                                        ).props('flat round size=xs').classes('text-gray-400 hover:text-red-500').tooltip(tr('Remove'))

                                # Per-fragment version selector
                                # Build source options for dropdown
                                source_options = {}
                                source_map = {}  # option_key -> source dict
                                if frag_sources:
                                    for src_i, src in enumerate(frag_sources):
                                        doc_rel = src.get('doc_relation', '')
                                        scholar = src.get('source_scholar', 'Unknown')
                                        lang = src.get('language', '')
                                        if 'Edition' in doc_rel:
                                            label = f"PGP Edition: {scholar}"
                                        elif 'Translation' in doc_rel:
                                            label = f"{lang} Translation: {scholar}"
                                        else:
                                            label = f"{doc_rel}: {scholar}"
                                        key = f"src_{src_i}"
                                        source_options[key] = label
                                        source_map[key] = src

                                # Always add V0.8 as fallback
                                source_options['v08'] = 'V0.8 (HTR)'

                                # Default selection: first PGP edition if available, else first translation, else V0.8
                                default_key = 'v08'
                                for k, src in source_map.items():
                                    if 'Edition' in (src.get('doc_relation') or ''):
                                        default_key = k
                                        break
                                if default_key == 'v08':
                                    for k, src in source_map.items():
                                        if 'Translation' in (src.get('doc_relation') or ''):
                                            default_key = k
                                            break

                                # Text containers for each page (for version switching)
                                text_containers = {}

                                if len(source_options) > 1:
                                    with ui.row().classes('items-center gap-2 px-2 py-1'):
                                        ui.icon('history', size='xs').classes('text-green-600')

                                        def make_version_handler(f_sid=frag_sid, f_pages=frag_pages, s_map=source_map, t_containers=text_containers):
                                            def on_change(e):
                                                selected = e.value
                                                state.reading_desk_selected_sources[f_sid] = selected
                                                # Update text display for this fragment
                                                src = s_map.get(selected)
                                                if src and src.get('content'):
                                                    content = src['content']
                                                    is_translation = 'Translation' in (src.get('doc_relation') or '')
                                                    is_english = src.get('language') == 'English'
                                                    text_dir = 'ltr' if (is_translation and is_english) else 'rtl'
                                                    text_align = 'left' if text_dir == 'ltr' else 'right'
                                                    from shared.document_service import parse_transcription_sections
                                                    sections = parse_transcription_sections(content)
                                                    for pg_key, container in t_containers.items():
                                                        container.clear()
                                                        with container:
                                                            pg_section = 'recto' if pg_key == 0 else 'verso'
                                                            section_texts = sections.get(pg_section, [])
                                                            section_text = '\n\n'.join(section_texts) if section_texts else content
                                                            if section_text:
                                                                ui.label(section_text).style(
                                                                    f'font-size: 1.2rem; line-height: 1.9; direction: {text_dir}; text-align: {text_align}; '
                                                                    f'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap; '
                                                                    f'overflow-wrap: break-word; word-break: break-word; '
                                                                    f'color: var(--text-primary);'
                                                                )
                                                            else:
                                                                ui.label(tr('No text for this page')).classes('italic text-gray-400')
                                                else:
                                                    # V0.8 - use original page text
                                                    for pg_key, container in t_containers.items():
                                                        container.clear()
                                                        with container:
                                                            pg_text = ''
                                                            for p in f_pages:
                                                                p_num = p.p_num if hasattr(p, 'p_num') else 0
                                                                if max(0, p_num - 1) == pg_key:
                                                                    pg_text = p.text if hasattr(p, 'text') else ''
                                                                    break
                                                            if pg_text:
                                                                ui.label(pg_text).style(
                                                                    'font-size: 1.2rem; line-height: 1.9; direction: rtl; text-align: right; '
                                                                    'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap; '
                                                                    'overflow-wrap: break-word; word-break: break-word; '
                                                                    'color: var(--text-primary);'
                                                                )
                                                            else:
                                                                ui.label(tr('No text available')).classes('italic text-gray-400')
                                            return on_change

                                        ui.select(
                                            options=source_options,
                                            value=default_key,
                                            on_change=make_version_handler()
                                        ).props('dense outlined').classes('text-sm').style('min-width: 200px;')

                                if not frag_pages:
                                    frag_pages = [type('P', (), {'p_num': 1, 'text': '', 'full_header': '', 'fl_id': ''})(),
                                                  type('P', (), {'p_num': 2, 'text': '', 'full_header': '', 'fl_id': ''})()]

                                # Determine initial text content based on default source selection
                                initial_source = source_map.get(default_key)
                                initial_sections = None
                                initial_is_ltr = False
                                if initial_source and initial_source.get('content'):
                                    from shared.document_service import parse_transcription_sections
                                    initial_sections = parse_transcription_sections(initial_source['content'])
                                    is_translation = 'Translation' in (initial_source.get('doc_relation') or '')
                                    is_english = initial_source.get('language') == 'English'
                                    initial_is_ltr = is_translation and is_english

                                # Render each page's text
                                for pg_i, pg in enumerate(frag_pages):
                                    pg_num = pg.p_num if hasattr(pg, 'p_num') else (pg_i + 1)
                                    pg_idx = max(0, pg_num - 1)
                                    pg_text = pg.text if hasattr(pg, 'text') else ''
                                    pg_label = tr('Recto') if pg_idx == 0 else tr('Verso')

                                    ui.label(f'{pg_label}').classes('text-xs font-medium text-gray-500 mt-2 ml-2')

                                    # Text content container (replaceable by version selector)
                                    tc = ui.column().classes('w-full px-3 py-2').style('min-width: 0;')
                                    text_containers[pg_idx] = tc

                                    with tc:
                                        # Show PGP source text if available and selected by default
                                        display_text = pg_text
                                        text_dir = 'rtl'
                                        text_align = 'right'

                                        if initial_sections is not None:
                                            pg_section = 'recto' if pg_idx == 0 else 'verso'
                                            section_texts = initial_sections.get(pg_section, [])
                                            if section_texts:
                                                display_text = '\n\n'.join(section_texts)
                                            elif pg_idx == 0 and not initial_sections.get('verso'):
                                                # No markers, use full content for recto
                                                display_text = initial_source.get('content', pg_text)
                                            if initial_is_ltr:
                                                text_dir = 'ltr'
                                                text_align = 'left'

                                        if display_text:
                                            ui.label(display_text).style(
                                                f'font-size: 1.2rem; line-height: 1.9; direction: {text_dir}; text-align: {text_align}; '
                                                f'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap; '
                                                f'overflow-wrap: break-word; word-break: break-word; '
                                                f'color: var(--text-primary);'
                                            )
                                        else:
                                            ui.label(tr('No text available')).classes('italic text-gray-400')

                # Inject per-image viewer JS (zoom/rotate/drag)
                ui.run_javascript('''
                    window.rdViewers = window.rdViewers || {};

                    function rdInitState(viewerId) {
                        if (!window.rdViewers[viewerId]) window.rdViewers[viewerId] = {scale: 1, rotation: 0, x: 0, y: 0, isDragging: false, brightness: 0, contrast: 0, gamma: 1.0, invert: false};
                        return window.rdViewers[viewerId];
                    }

                    function rdApplyFilters(viewerId) {
                        const s = window.rdViewers[viewerId];
                        if (!s) return;
                        const img = document.getElementById(viewerId);
                        if (!img) return;
                        const b = 1 + s.brightness / 100;
                        const c = 1 + s.contrast / 100;
                        const inv = s.invert ? 1 : 0;
                        let f = 'brightness(' + b + ') contrast(' + c + ') invert(' + inv + ')';
                        if (s.gamma !== 1.0) {
                            const filterId = 'gamma-' + viewerId;
                            let svgFilter = document.getElementById(filterId);
                            if (!svgFilter) {
                                const svgNS = 'http://www.w3.org/2000/svg';
                                const svg = document.createElementNS(svgNS, 'svg');
                                svg.style.cssText = 'position:absolute;width:0;height:0';
                                const filter = document.createElementNS(svgNS, 'filter');
                                filter.setAttribute('id', filterId);
                                const ct = document.createElementNS(svgNS, 'feComponentTransfer');
                                ['R','G','B'].forEach(ch => {
                                    const fn = document.createElementNS(svgNS, 'feFunc' + ch);
                                    fn.setAttribute('type', 'gamma');
                                    fn.setAttribute('amplitude', '1');
                                    fn.setAttribute('exponent', '1.0');
                                    ct.appendChild(fn);
                                });
                                filter.appendChild(ct);
                                svg.appendChild(filter);
                                document.body.appendChild(svg);
                                svgFilter = filter;
                            }
                            const exp = 1.0 / s.gamma;
                            svgFilter.querySelectorAll('feFuncR, feFuncG, feFuncB').forEach(fn => fn.setAttribute('exponent', exp));
                            f += ' url(#' + filterId + ')';
                        }
                        img.style.filter = f;
                    }

                    window.rdZoom = function(viewerId, delta) {
                        const state = rdInitState(viewerId);
                        state.scale = Math.max(0.25, Math.min(4, state.scale + delta));
                        const img = document.getElementById(viewerId);
                        if (img) img.style.transform = `translate(${state.x}px, ${state.y}px) rotate(${state.rotation}deg) scale(${state.scale})`;
                        const label = document.getElementById(viewerId + '-zoom-label');
                        if (label) label.textContent = Math.round(state.scale * 100) + '%';
                    };

                    window.rdRotate = function(viewerId, degrees) {
                        const state = rdInitState(viewerId);
                        state.rotation = (state.rotation + degrees + 360) % 360;
                        const img = document.getElementById(viewerId);
                        if (img) img.style.transform = `translate(${state.x}px, ${state.y}px) rotate(${state.rotation}deg) scale(${state.scale})`;
                    };

                    window.rdResetView = function(viewerId) {
                        if (!window.rdViewers[viewerId]) return;
                        const state = window.rdViewers[viewerId];
                        state.scale = 1; state.rotation = 0; state.x = 0; state.y = 0;
                        state.brightness = 0; state.contrast = 0; state.gamma = 1.0; state.invert = false;
                        const img = document.getElementById(viewerId);
                        if (img) { img.style.transform = 'translate(0px, 0px) rotate(0deg) scale(1)'; img.style.filter = ''; }
                        const label = document.getElementById(viewerId + '-zoom-label');
                        if (label) label.textContent = '100%';
                    };

                    window.rdSetBrightness = function(viewerId, val) { rdInitState(viewerId).brightness = val; rdApplyFilters(viewerId); };
                    window.rdSetContrast = function(viewerId, val) { rdInitState(viewerId).contrast = val; rdApplyFilters(viewerId); };
                    window.rdSetGamma = function(viewerId, val) { rdInitState(viewerId).gamma = val; rdApplyFilters(viewerId); };
                    window.rdToggleInvert = function(viewerId) { const s = rdInitState(viewerId); s.invert = !s.invert; rdApplyFilters(viewerId); };
                    window.rdResetAdjustments = function(viewerId) {
                        const s = rdInitState(viewerId);
                        s.brightness = 0; s.contrast = 0; s.gamma = 1.0; s.invert = false;
                        rdApplyFilters(viewerId);
                    };

                    // Initialize drag support for all rd-zoomable images
                    window.rdInitDrag = function(viewerId) {
                        const img = document.getElementById(viewerId);
                        if (!img || img.dataset.rdDragInit) return;
                        img.dataset.rdDragInit = 'true';
                        const state = rdInitState(viewerId);
                        img.style.cursor = 'grab';
                        img.ondragstart = (e) => e.preventDefault();

                        img.onmousedown = function(e) {
                            if (e.button !== 0) return;
                            e.preventDefault();
                            state.isDragging = true;
                            state.startX = e.clientX - state.x;
                            state.startY = e.clientY - state.y;
                            img.style.cursor = 'grabbing';
                        };
                        const moveHandler = function(e) {
                            if (!state.isDragging) return;
                            state.x = e.clientX - state.startX;
                            state.y = e.clientY - state.startY;
                            img.style.transform = `translate(${state.x}px, ${state.y}px) rotate(${state.rotation}deg) scale(${state.scale})`;
                        };
                        const upHandler = function() {
                            if (state.isDragging) {
                                state.isDragging = false;
                                img.style.cursor = 'grab';
                            }
                        };
                        window.addEventListener('mousemove', moveHandler);
                        window.addEventListener('mouseup', upHandler);

                        img.onwheel = function(e) {
                            e.preventDefault();
                            const delta = e.deltaY > 0 ? -0.25 : 0.25;
                            window.rdZoom(viewerId, delta);
                        };
                    };

                    // Init drag for all existing rd-zoomable images
                    setTimeout(function() {
                        document.querySelectorAll('.rd-zoomable').forEach(function(img) {
                            if (img.id) window.rdInitDrag(img.id);
                        });
                    }, 300);
                ''')

                # Inject synchronized scrolling JS
                rd_frag_count = len(state.reading_desk_entries)
                ui.run_javascript(f'''
                    (function() {{
                        const fragCount = {rd_frag_count};
                        if (fragCount <= 1) return;

                        // Wait for scroll areas to render
                        setTimeout(function() {{
                            const imgPane = document.querySelector('.rd-image-pane .q-scrollarea__container');
                            const textPane = document.querySelector('.rd-text-pane .q-scrollarea__container');
                            if (!imgPane || !textPane) {{
                                console.log('[ReadingDesk] Scroll panes not found');
                                return;
                            }}

                            const imgHeaders = document.querySelectorAll('[id^="rd-img-frag-"]');
                            const textHeaders = document.querySelectorAll('[id^="rd-text-frag-"]');

                            let syncing = false;

                            const imgObserver = new IntersectionObserver((entries) => {{
                                if (syncing) return;
                                for (const entry of entries) {{
                                    if (entry.isIntersecting) {{
                                        const idx = entry.target.id.replace('rd-img-frag-', '');
                                        const textTarget = document.getElementById('rd-text-frag-' + idx);
                                        if (textTarget) {{
                                            syncing = true;
                                            textTarget.scrollIntoView({{behavior: 'smooth', block: 'start'}});
                                            setTimeout(() => syncing = false, 600);
                                        }}
                                        break;
                                    }}
                                }}
                            }}, {{root: imgPane, threshold: 0.3}});

                            const textObserver = new IntersectionObserver((entries) => {{
                                if (syncing) return;
                                for (const entry of entries) {{
                                    if (entry.isIntersecting) {{
                                        const idx = entry.target.id.replace('rd-text-frag-', '');
                                        const imgTarget = document.getElementById('rd-img-frag-' + idx);
                                        if (imgTarget) {{
                                            syncing = true;
                                            imgTarget.scrollIntoView({{behavior: 'smooth', block: 'start'}});
                                            setTimeout(() => syncing = false, 600);
                                        }}
                                        break;
                                    }}
                                }}
                            }}, {{root: textPane, threshold: 0.3}});

                            imgHeaders.forEach(h => imgObserver.observe(h));
                            textHeaders.forEach(h => textObserver.observe(h));
                            console.log('[ReadingDesk] Synchronized scrolling initialized for ' + fragCount + ' fragments');
                        }}, 500);
                    }})();
                ''')

            else:
                # Single page view
                # Extract FL ID and check if we have an image
                fl_id = page.fl_id
                if not fl_id and page.image_url:
                    match = re.search(r'FL(\d+)', page.image_url)
                    if match:
                        fl_id = match.group(1)

                # Prepare image URLs
                img_url = None
                fallback_url = None
                has_image = False

                # Compute FL ID digits once (for proper fallback logic)
                fl_digits = ""
                if fl_id:
                    fl_digits = re.sub(r"\D", "", str(fl_id))

                # Use pre-computed is_oxford from BrowsePage (computed in services.py)
                # Also check shelfmark pattern as fallback
                is_oxford = page.is_oxford
                if not is_oxford:
                    is_oxford = is_oxford_manuscript(page.shelfmark, page.library_code)


                # Choose image endpoint based on source
                # Prioritize page-specific fl_id over sys_id for correct page images
                # Add cache-buster to force image refresh on page navigation
                cache_bust_value = f"_cb={page.p_num}" if page.p_num else ""
                cache_bust_api = f"&{cache_bust_value}" if cache_bust_value else ""
                cache_bust_direct = f"?{cache_bust_value}" if cache_bust_value else ""

                # Page index (0-based) for multi-page manuscripts
                page_idx = max(0, page.p_num - 1)

                # NLI IIIF base URL for direct browser access (bypasses server blocking)
                NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"

                if is_oxford and page.sys_id:
                    has_image = True
                    # For multi-IE Oxford manuscripts, each volume = next folio in sequence
                    # e.g., d.50/19 Volume 1 = folio 19, Volume 2 = folio 20
                    # Oxford folios have 2 sides (recto 'a' + verso 'b'), so offset by
                    # number of preceding volumes, not pages.
                    _ox_folio_offset = max(0, (page.volume_suffix or 1) - 1)
                    # Prefer direct Bodleian URL in browser to avoid production /api proxy failures.
                    oxford_direct = get_oxford_direct_image_url(page.shelfmark, page_idx, folio_offset=_ox_folio_offset)
                    if oxford_direct:
                        img_url = f"{oxford_direct}{cache_bust_direct}"
                    else:
                        # Fallback to proxy when direct URL cannot be derived from shelfmark.
                        img_url = f"/api/oxford_image/{page.sys_id}?page={page_idx}{cache_bust_api}"
                    fallback_url = None
                elif page.sys_id:
                    # Use server-side NLI proxy for ALL NLI items
                    # This works reliably for all collections (Cambridge, Russian, etc.)
                    # Direct browser requests to NLI are blocked for some collections
                    has_image = True
                    _suffix_param = f'&suffix={page.volume_suffix}' if page.volume_suffix > 1 else ''
                    img_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}{_suffix_param}{cache_bust_api}"
                    fallback_url = None

                # External source override: if user switched to Cambridge/Manchester/JTS and images are available
                _has_ext_images = bool(page.cambridge_images)
                _has_cambridge_images = _has_ext_images and page.external_provider not in ('manchester', 'jts')
                _has_manchester_images = _has_ext_images and page.external_provider == 'manchester'
                _has_jts_images = _has_ext_images and page.external_provider == 'jts'

                # Auto-default to external sources when available (before image URL construction)
                # When NLI IIIF is down, these ensure images load from alternate providers
                if _has_jts_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'jts'
                if _has_manchester_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'manchester'
                if _has_cambridge_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'cambridge'

                if state.active_source == 'cambridge' and _has_cambridge_images and not is_oxford:
                    has_image = True
                    img_url = f"/api/cambridge_image/{page.sys_id}?page={page_idx}{cache_bust_api}"
                    fallback_url = None

                # Manchester source override
                if state.active_source == 'manchester' and _has_manchester_images and not is_oxford:
                    has_image = True
                    # For multi-IE manuscripts, Manchester canvases span all volumes
                    # sequentially. Compute absolute canvas index by adding preceding
                    # volumes' transcription page counts as offset.
                    _manch_page_idx = page_idx
                    if page.volume_suffix and page.volume_suffix > 1 and page.volumes:
                        _vol_offset = 0
                        for v in page.volumes:
                            if v.get('suffix', 1) < page.volume_suffix:
                                _vol_offset += v.get('transcription_pages', 0)
                        _manch_page_idx = page_idx + _vol_offset
                    img_url = f"/api/manchester_image/{page.sys_id}?page={_manch_page_idx}{cache_bust_api}"
                    fallback_url = None

                # JTS source override
                if state.active_source == 'jts' and _has_jts_images and not is_oxford:
                    has_image = True
                    img_url = f"/api/jts_image/{page.sys_id}?page={page_idx}{cache_bust_api}"
                    fallback_url = None

                # Header bar with folio info, navigation, controls
                # Determine effective image count and folio data
                _src_info = page.image_source_info or {}
                _folio_images = page.folio_images or []
                _effective_count = _src_info.get('image_count', 0) or page.total_pages
                _is_single_page = _effective_count <= 1 and page.total_pages <= 1
                _has_nli = _src_info.get('nli_fgp', False)
                _has_cambridge = _src_info.get('cambridge', False) or page.is_cambridge
                _has_manchester = _src_info.get('manchester', False)
                _has_jts = _src_info.get('jts', False)

                # Source switching setup -- any external source with NLI enables toggling
                _any_ext_images = _has_cambridge_images or _has_manchester_images or _has_jts_images
                _both_sources = _has_nli and _any_ext_images
                _is_nli_active = state.active_source == 'nli' or not _any_ext_images
                _is_cambridge_active = state.active_source == 'cambridge' and _has_cambridge_images
                _is_manchester_active = state.active_source == 'manchester' and _has_manchester_images
                _is_jts_active = state.active_source == 'jts' and _has_jts_images

                async def switch_to_nli():
                    state.active_source = 'nli'
                    state.source_user_override = True
                    await load_page(direction=0)

                async def switch_to_cambridge():
                    state.active_source = 'cambridge'
                    state.source_user_override = True
                    await load_page(direction=0)

                async def switch_to_manchester():
                    state.active_source = 'manchester'
                    state.source_user_override = True
                    await load_page(direction=0)

                async def switch_to_jts():
                    state.active_source = 'jts'
                    state.source_user_override = True
                    await load_page(direction=0)

                # NLI viewer deep link handler
                if _has_nli:
                    _nli_sys_id = page.sys_id
                    _nli_page_idx = page_idx

                    async def open_nli_viewer(sys_id=_nli_sys_id, pidx=_nli_page_idx):
                        js = f'''
                        (async () => {{
                            let docid = "PNX_MANUSCRIPTS{sys_id}-1";
                            try {{
                                const resp = await fetch("/api/fl_ids/{sys_id}");
                                if (resp.ok) {{
                                    const flIds = await resp.json();
                                    if (flIds.length > {pidx}) docid += ",FL" + flIds[{pidx}];
                                }}
                            }} catch(e) {{}}
                            window.open("https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=MANUSCRIPTS&docid=" + docid, "_blank");
                        }})()
                        '''
                        await ui.run_javascript(js)

                with ui.card().classes('w-full mb-2').style('background: var(--bg-tertiary);'):
                    with ui.row().classes('w-full items-center flex-wrap gap-2 px-3 py-2'):
                        # -- Left group: folio label, page count, version badge, source chips --
                        with ui.row().classes('items-center gap-2'):
                            # Folio/Page label
                            if page.folio_label:
                                ui.label(f"{tr('Folio')} {page.folio_label}").classes('text-sm font-bold')
                            else:
                                folio = extract_folio_number(page.full_header)
                                if folio:
                                    ui.label(f"{tr('Folio')} {folio}").classes('text-sm font-bold')
                                else:
                                    ui.label(f"{tr('Page')} {page.p_num}").classes('text-sm font-bold')

                            # Source badge
                            source_class = get_source_badge_class(page.full_header)
                            source_text = 'V0.7' if 'V0.7' in page.full_header else 'V0.8'
                            ui.label(source_text).classes(f'source-badge {source_class}')

                            # Source chips with matching external link icon colors
                            if _has_nli:
                                nli_style = (
                                    'background: #4caf50; color: white; border: 1.5px solid #4caf50; border-radius: 12px; min-height: 22px; font-weight: 600;'
                                    if _is_nli_active else
                                    'border: 1.5px solid #4caf50; border-radius: 12px; min-height: 22px; color: #4caf50; font-weight: 600;'
                                )
                                if _both_sources:
                                    ui.button('NLI', on_click=switch_to_nli).props(
                                        'flat dense size=sm no-caps'
                                    ).classes('text-xs px-2 py-0').style(nli_style).tooltip(
                                        tr('View NLI images') if not _is_nli_active else tr('Viewing NLI images')
                                    )
                                else:
                                    ui.button('NLI', on_click=open_nli_viewer).props(
                                        'flat dense size=sm no-caps'
                                    ).classes('text-xs px-2 py-0').style(nli_style).tooltip(tr('Open in NLI KTIV'))
                                ui.button(icon='open_in_new', on_click=open_nli_viewer).props(
                                    'flat round dense size=xs'
                                ).style('min-width: 20px; min-height: 20px; color: #4caf50; opacity: 0.8;').tooltip(tr('Open in NLI KTIV'))

                            if _has_cambridge:
                                cudl_url = page.external_url or ''
                                if not cudl_url and page.shelfmark:
                                    cudl_url = f"https://cudl.lib.cam.ac.uk/view/{page.shelfmark.replace(' ', '-')}"
                                if cudl_url:
                                    cudl_style = (
                                        'background: #2196f3; color: white; border: 1.5px solid #2196f3; border-radius: 12px; min-height: 22px; font-weight: 600;'
                                        if _is_cambridge_active else
                                        'border: 1.5px solid #2196f3; border-radius: 12px; min-height: 22px; color: #2196f3; font-weight: 600;'
                                    )
                                    if _both_sources:
                                        ui.button('Cambridge', on_click=switch_to_cambridge).props(
                                            'flat dense size=sm no-caps'
                                        ).classes('text-xs px-2 py-0').style(cudl_style).tooltip(
                                            tr('View Cambridge images') if not _is_cambridge_active else tr('Viewing Cambridge images')
                                        )
                                    else:
                                        ui.button('Cambridge', on_click=lambda u=cudl_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                            'flat dense size=sm no-caps'
                                        ).classes('text-xs px-2 py-0').style(cudl_style).tooltip(tr('Open in Cambridge Digital Library'))
                                    ui.button(icon='open_in_new', on_click=lambda u=cudl_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                        'flat round dense size=xs'
                                    ).style('min-width: 20px; min-height: 20px; color: #2196f3; opacity: 0.8;').tooltip(tr('Open in Cambridge Digital Library'))

                            if _has_manchester:
                                manchester_color = '#e91e63'
                                man_style = (
                                    f'background: {manchester_color}; color: white; border: 1.5px solid {manchester_color}; border-radius: 12px; min-height: 22px; font-weight: 600;'
                                    if _is_manchester_active else
                                    f'border: 1.5px solid {manchester_color}; border-radius: 12px; min-height: 22px; color: {manchester_color}; font-weight: 600;'
                                )
                                if _has_nli and _has_manchester_images:
                                    ui.button('Manchester', on_click=switch_to_manchester).props(
                                        'flat dense size=sm no-caps'
                                    ).classes('text-xs px-2 py-0').style(man_style).tooltip(
                                        tr('View Manchester images') if not _is_manchester_active else tr('Viewing Manchester images')
                                    )
                                # External link to LUNA detail page
                                lib_viewer = page.library_viewer_url or {}
                                luna_url = lib_viewer.get('url', '')
                                if luna_url:
                                    if not (_has_nli and _has_manchester_images):
                                        ui.button('Manchester', on_click=lambda u=luna_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                            'flat dense size=sm no-caps'
                                        ).classes('text-xs px-2 py-0').style(man_style).tooltip(tr('Open in Manchester LUNA'))
                                    ui.button(icon='open_in_new', on_click=lambda u=luna_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                        'flat round dense size=xs'
                                    ).style(f'min-width: 20px; min-height: 20px; color: {manchester_color}; opacity: 0.8;').tooltip(tr('Open in Manchester LUNA'))

                            if _has_jts:
                                jts_color = '#ff9800'
                                jts_style = (
                                    f'background: {jts_color}; color: white; border: 1.5px solid {jts_color}; border-radius: 12px; min-height: 22px; font-weight: 600;'
                                    if _is_jts_active else
                                    f'border: 1.5px solid {jts_color}; border-radius: 12px; min-height: 22px; color: {jts_color}; font-weight: 600;'
                                )
                                if _has_nli and _has_jts_images:
                                    ui.button('JTS', on_click=switch_to_jts).props(
                                        'flat dense size=sm no-caps'
                                    ).classes('text-xs px-2 py-0').style(jts_style).tooltip(
                                        tr('View JTS images') if not _is_jts_active else tr('Viewing JTS images')
                                    )
                                # External link to DPUL catalog page
                                lib_viewer_jts = page.library_viewer_url or {}
                                dpul_url = lib_viewer_jts.get('url', '')
                                if dpul_url:
                                    if not (_has_nli and _has_jts_images):
                                        ui.button('JTS', on_click=lambda u=dpul_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                            'flat dense size=sm no-caps'
                                        ).classes('text-xs px-2 py-0').style(jts_style).tooltip(tr('Open in Princeton Digital Library'))
                                    ui.button(icon='open_in_new', on_click=lambda u=dpul_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                        'flat round dense size=xs'
                                    ).style(f'min-width: 20px; min-height: 20px; color: {jts_color}; opacity: 0.8;').tooltip(tr('Open in Princeton Digital Library'))

                            if page.is_oxford and page.external_url:
                                ui.button('Oxford', on_click=lambda u=page.external_url: ui.run_javascript(f'window.open("{u}", "_blank")')).props(
                                    'flat dense size=sm no-caps'
                                ).classes('text-xs px-2 py-0').style(
                                    'border: 1.5px solid #ff9800; border-radius: 12px; min-height: 22px; color: #ff9800; font-weight: 600;'
                                ).tooltip(tr('Open in Bodleian Libraries'))

                            # === Bibliography & Catalog Buttons (inline, deferred) ===
                            bib_catalog_el = ui.element('span').classes('inline-flex items-center gap-1')
                            enrichment_refs['bib_catalog_container'] = bib_catalog_el
                            if state.enrichment_loaded:
                                populate_bib_catalog_buttons(bib_catalog_el, state, page)

                        # Spacer
                        ui.element('div').classes('flex-grow')

                        # -- Volume selector for multi-IE manuscripts --
                        if page.volume_count > 1 and page.volumes:
                            _vol_options = {}
                            for _v in page.volumes:
                                _vp = _v.get('transcription_pages') or _v['page_count']
                                _label = f"{tr('Volume')} {_v['suffix']} ({_vp} {tr('Pages').lower()})"
                                _vol_options[_v['ie_id']] = _label
                            _vol_current = page.volume_ie or page.volumes[0]['ie_id']

                            def _handle_volume_change(e):
                                new_ie = e.value
                                if new_ie and new_ie != state.volume_ie:
                                    state.volume_ie = new_ie
                                    asyncio.ensure_future(load_page(p_num=1))

                            ui.select(
                                options=_vol_options,
                                value=_vol_current,
                                on_change=_handle_volume_change,
                            ).classes('w-48').props('dense outlined color=orange').tooltip(
                                tr('This manuscript has multiple volumes (IEs)')
                            )

                        # -- Right group: navigation, full manuscript, star --
                        with ui.row().classes('items-center gap-1'):
                            prev_disabled = _is_single_page or page.current_idx <= 1
                            ui.button(
                                icon='chevron_right' if is_rtl() else 'chevron_left',
                                on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
                            ).props(f'flat round dense size=sm {"disabled" if prev_disabled else ""} data-action="prev" aria-label="{tr("Previous Page")}"').classes(
                                'text-green-700' if not prev_disabled else 'text-gray-300'
                            )

                            # Only use folio-label dropdown when crossref image count
                            # matches the page count from the search index.  When they
                            # differ the labels would be misleading (e.g. crossref starts
                            # at leaf 4 while pages start at 1).
                            _folio_count_matches = (
                                _folio_images
                                and len(_folio_images) > 1
                                and len(_folio_images) == page.total_pages
                            )
                            if _folio_count_matches:
                                folio_options = {
                                    str(i + 1): img.get('folio_label', str(i + 1))
                                    for i, img in enumerate(_folio_images)
                                }

                                def handle_folio_select(e):
                                    try:
                                        val = int(e.value) if e.value is not None else 1
                                    except (ValueError, TypeError):
                                        return
                                    asyncio.ensure_future(go_to_page(val))

                                # Clamp p_num to valid folio range to prevent ValueError
                                _folio_val = str(min(page.p_num, len(_folio_images)))
                                ui.select(
                                    options=folio_options,
                                    value=_folio_val,
                                    on_change=handle_folio_select
                                ).classes('w-20').props('dense outlined')
                            else:
                                page_input = ui.number(
                                    value=page.p_num, min=1, max=page.total_pages
                                ).classes('w-14').props('dense outlined')

                                def handle_go_click():
                                    try:
                                        val = int(page_input.value) if page_input.value is not None else 1
                                    except (ValueError, TypeError):
                                        val = 1
                                    asyncio.ensure_future(go_to_page(val))

                                ui.button(tr('Go'), on_click=handle_go_click).props('flat dense color=green size=sm')

                            ui.label(f"/ {page.total_pages}").classes('text-xs').style('color: var(--text-secondary);')

                            next_disabled = _is_single_page or page.current_idx >= page.total_pages
                            ui.button(
                                icon='chevron_left' if is_rtl() else 'chevron_right',
                                on_click=lambda: asyncio.ensure_future(load_page(direction=1))
                            ).props(f'flat round dense size=sm {"disabled" if next_disabled else ""} data-action="next" aria-label="{tr("Next Page")}"').classes(
                                'text-green-700' if not next_disabled else 'text-gray-300'
                            )

                            ui.button(
                                tr('Hide Full Manuscript') if state.view_all else tr('Show Full Manuscript'),
                                icon='view_agenda' if not state.view_all else 'view_day',
                                on_click=toggle_view_all
                            ).props(f'flat dense color=green size=sm aria-label="{tr("Hide Full Manuscript") if state.view_all else tr("Show Full Manuscript")}"')

                            # Add page to list (star button)
                            from web.state import state as app_state
                            from web.components import get_star_icon
                            page_star_icon = get_star_icon(app_state.lists_mgr, state.sys_id)
                            ui.button(
                                icon=page_star_icon,
                                on_click=add_page_to_list
                            ).props(f'flat round dense aria-label="{tr("Add to List")}"').classes('text-green-700').tooltip(tr('In List') if page_star_icon == 'star' else tr('Add to List'))

                            # Image toggle button - placeholder, will be connected later
                            image_toggle_btn = None
                            if has_image:
                                image_toggle_btn = ui.button(
                                    icon='image'
                                ).props('flat dense').classes('text-green-700').tooltip(tr('Toggle Image'))

                            # Edit, Comment, Notes, and Joins buttons
                            if page.text:
                                from web.components import create_comment_button, create_version_selector, create_notes_button, create_joins_button

                                # Refresh callback to reload page after edits/comments
                                def refresh_page():
                                    asyncio.ensure_future(load_page(direction=0))

                                # Navigation callback for joins
                                async def navigate_to_shelfmark(target_shelfmark: str):
                                    state.shelfmark_query = target_shelfmark
                                    await search_shelfmark()

                                # Custom Edit Button
                                ui.button(
                                    tr('Edit'),
                                    icon='edit',
                                    on_click=toggle_edit_mode
                                ).props('flat dense size=sm').classes('text-xs').tooltip(tr('Edit Transcription'))
                                create_comment_button(
                                    document_id=page.sys_id,
                                    page_number=page.p_num,
                                    shelfmark=page.shelfmark or page.sys_id,
                                    on_submit=refresh_notes_after_comment,
                                    ie_id=state.volume_ie
                                )
                                create_notes_button(
                                    document_id=page.sys_id,
                                    page_number=page.p_num,
                                    shelfmark=page.shelfmark or page.sys_id,
                                    ie_id=state.volume_ie
                                )

                                # Joins button placeholder (populated by enrichment Phase B)
                                if page.shelfmark:
                                    joins_el = ui.element('span')
                                    enrichment_refs['joins_container'] = joins_el
                                    enrichment_refs['navigate_to_shelfmark'] = navigate_to_shelfmark
                                    if state.enrichment_loaded:
                                        pgpid_for_joins = state.pgp_metadata.get('pgpid') if state.pgp_metadata else None
                                        with joins_el:
                                            create_joins_button(
                                                shelfmark=page.shelfmark,
                                                document_id=page.sys_id,
                                                pgpid=pgpid_for_joins,
                                                on_navigate=navigate_to_shelfmark,
                                                on_view_all=enter_joined_view
                                            )

                            # "Add to View" button -- start or extend reading desk with current manuscript
                            ui.button(
                                icon='library_add',
                                on_click=add_to_reading_desk
                            ).props(f'flat dense aria-label="{tr("Add to Reading Desk")}"').classes(
                                'text-green-700'
                            ).tooltip(tr('Add to Reading Desk'))

                # === SIDE-BY-SIDE LAYOUT: Image (left) + Text (right) ===
                # State for image panel visibility
                show_image_panel = {'value': has_image}  # Start visible if image available
                image_panel_ref = {'container': None}

                def toggle_image_panel():
                    show_image_panel['value'] = not show_image_panel['value']
                    if image_panel_ref['container']:
                        if show_image_panel['value']:
                            image_panel_ref['container'].style('display: block;')
                        else:
                            image_panel_ref['container'].style('display: none;')

                # Connect toggle button click handler
                if image_toggle_btn:
                    image_toggle_btn.on('click', toggle_image_panel)

                # Main container with flex row
                with ui.element('div').classes('viewer-panels').style(
                    'display: flex; flex-direction: row; gap: 16px; min-height: 60vh; width: 100%;'
                ):

                    # === LEFT PANEL: Image (only if available) ===
                    if has_image:
                        image_panel_ref['container'] = ui.card().style(
                            'flex: 0 0 50%; min-height: 60vh; display: block;'
                        )
                        with image_panel_ref['container']:
                            # Image header with zoom controls
                            with ui.row().classes('w-full items-center justify-between p-3').style(
                                'background: #1a1a1a; border-radius: 8px 8px 0 0;'
                            ):
                                ui.label(tr('Manuscript Image')).classes('text-white font-semibold')
                                with ui.row().classes('gap-1'):
                                    ui.button(icon='remove', on_click=zoom_out).props(f'flat round size=sm text-color=white aria-label="{tr("Zoom out")}" data-action="zoom-out"').tooltip(tr('Zoom out'))
                                    ui.label(f'{int(state.zoom_level * 100)}%').classes('zoom-level-label text-white text-sm px-2')
                                    ui.button(icon='add', on_click=zoom_in).props(f'flat round size=sm text-color=white aria-label="{tr("Zoom in")}" data-action="zoom-in"').tooltip(tr('Zoom in'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='rotate_left', on_click=rotate_left).props(f'flat round size=sm text-color=white aria-label="{tr("Rotate Left")}"').tooltip(tr('Rotate Left'))
                                    
                                    # Rotation Slider
                                    slider_refs['rotate'] = ui.slider(
                                        min=0, max=360, step=1, value=state.rotation,
                                        on_change=handle_rotation_slider
                                    ).props(f'dark dense aria-label="{tr("Rotation")}"').classes('w-32 mx-2').style('transition: none;')
                                    
                                    ui.button(icon='rotate_right', on_click=rotate_right).props(f'flat round size=sm text-color=white aria-label="{tr("Rotate Right")}"').tooltip(tr('Rotate Right'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='restart_alt', on_click=zoom_reset).props(f'flat round size=sm text-color=white aria-label="{tr("Reset View")}"').tooltip(tr('Reset View'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='fullscreen', on_click=toggle_image_fullscreen).props(f'flat round size=sm text-color=white aria-label="{tr("Fullscreen Image")}" data-action="fullscreen"').tooltip(tr('Fullscreen Image'))

                            # Image adjustment controls row
                            with ui.row().classes('w-full items-center gap-2 px-3 py-1').style(
                                'background: #1a1a1a; border-top: 1px solid #333;'
                            ):
                                ui.icon('brightness_6').classes('text-white text-sm').tooltip(tr('Brightness'))
                                slider_refs['brightness'] = ui.slider(
                                    min=-100, max=100, step=1, value=0,
                                    on_change=lambda e: ui.run_javascript(f'if(window.manuscriptViewer) window.manuscriptViewer.setBrightness({e.value})')
                                ).props('dark dense').classes('w-24')
                                ui.icon('contrast').classes('text-white text-sm').tooltip(tr('Contrast'))
                                slider_refs['contrast'] = ui.slider(
                                    min=-100, max=100, step=1, value=0,
                                    on_change=lambda e: ui.run_javascript(f'if(window.manuscriptViewer) window.manuscriptViewer.setContrast({e.value})')
                                ).props('dark dense').classes('w-24')
                                ui.icon('timeline').classes('text-white text-sm').tooltip(tr('Gamma'))
                                slider_refs['gamma'] = ui.slider(
                                    min=20, max=300, step=1, value=100,
                                    on_change=lambda e: ui.run_javascript(f'if(window.manuscriptViewer) window.manuscriptViewer.setGamma({e.value / 100})')
                                ).props('dark dense').classes('w-24')
                                ui.button(
                                    icon='exposure',
                                    on_click=lambda: ui.run_javascript('if(window.manuscriptViewer) window.manuscriptViewer.toggleInvert()')
                                ).props('flat round size=sm text-color=white').tooltip(tr('Invert Colors'))
                                def _reset_image_adj():
                                    if slider_refs.get('brightness'): slider_refs['brightness'].value = 0
                                    if slider_refs.get('contrast'): slider_refs['contrast'].value = 0
                                    if slider_refs.get('gamma'): slider_refs['gamma'].value = 100
                                    ui.run_javascript('if(window.manuscriptViewer) window.manuscriptViewer.resetAdjustments()')
                                ui.button(
                                    icon='restart_alt',
                                    on_click=_reset_image_adj
                                ).props('flat round size=sm text-color=white').tooltip(tr('Reset Image'))

                            # Image display area - using div instead of scroll_area for drag support
                            with ui.element('div').classes('image-container w-full').style(
                                'background: #1a1a1a; height: calc(60vh - 100px); overflow: hidden; position: relative;'
                            ):
                                with ui.element('div').classes('img-loading-container').style(
                                    'display: flex; align-items: center; justify-content: center; width: 100%; height: 100%;'
                                ):
                                    safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                    safe_sys_id = (page.sys_id or '').replace("'", "\\'").replace('"', '\\"')

                                    is_oxford_js = 'true' if is_oxford else 'false'
                                    # Progressive loading: thumbnail first, then full resolution
                                    _thumb_url = safe_img_url
                                    _full_url = safe_img_url
                                    if '/api/nli_image_by_sysid/' in safe_img_url:
                                        _sep = '&' if '?' in safe_img_url else '?'
                                        _thumb_url = f"{safe_img_url}{_sep}width=400"
                                    img_html = f'''
                                    <img
                                        src="{_thumb_url}"
                                        data-full-src="{_full_url}"
                                        class="zoomable-image"
                                        style="transform: translate(0px, 0px) rotate({state.rotation}deg) scale({state.zoom_level}); cursor: grab;"
                                        draggable="false"
                                        onload="if(window.manuscriptViewer) window.manuscriptViewer.init()"
                                        onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'manuscriptViewer')"
                                    />
                                    '''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('if(window.manuscriptViewer) setTimeout(() => window.manuscriptViewer.init(), 100); initProgressiveImages();')

                            # === Image Credit/Attribution Footer ===
                            if page.attribution:
                                with ui.row().classes('w-full items-center justify-center gap-2 py-2').style(
                                    'background: #2a2a2a; border-radius: 0 0 8px 8px; border-top: 1px solid #333;'
                                ):
                                    ui.icon('photo_library', size='xs').style('color: #888; font-size: 14px;')
                                    credit_text = page.attribution
                                    # Route credit link based on image source
                                    if page.is_oxford:
                                        credit_link = 'https://digital.bodleian.ox.ac.uk/'
                                    elif page.external_provider == 'manchester':
                                        credit_link = 'https://luna.manchester.ac.uk/'
                                    elif page.is_cambridge:
                                        credit_link = 'https://cudl.lib.cam.ac.uk/'
                                    elif page.external_provider == 'jts':
                                        credit_link = 'https://dpul.princeton.edu/cairo_geniza'
                                    elif page.library_code == 'BL':
                                        credit_link = 'https://searcharchives.bl.uk/'
                                    else:
                                        credit_link = f'https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}'
                                    with ui.link(target=credit_link, new_tab=True).style('text-decoration: none;'):
                                        ui.label(credit_text).classes('text-xs').style(
                                            'color: #aaa; font-style: italic;'
                                        )

                    # === RIGHT PANEL: Transcription ===
                    text_panel_flex = 'flex: 1 1 auto; min-width: 0;' if has_image else 'flex: 1 1 100%; min-width: 0;'
                    
                    # Style based on edit mode
                    panel_style = f'{text_panel_flex} min-height: 60vh; display: flex; flex-direction: column;'
                    if state.edit_mode:
                        if state.draft_saved:
                            panel_style += ' border: 3px solid #27ae60;'  # Green for Saved
                        else:
                             # Orange for Unsaved/Edit Mode
                            panel_style += ' border: 3px solid #f39c12;'

                    with ui.card().style(panel_style):
                        if state.edit_mode:
                            # === EDIT MODE ===
                            # Edit Bar
                            with ui.row().classes('w-full items-center justify-between p-2 bg-gray-100 border-b'):
                                with ui.row().classes('items-center gap-2'):
                                    ui.label(tr('Edit Mode')).classes('font-bold').style('color: var(--text-primary);')
                                    if state.draft_saved:
                                        ui.label(tr('Saved')).classes('text-green-600 text-sm font-bold')
                                    else:
                                        ui.label(tr('Unsaved changes')).classes('text-orange-600 text-sm')
                                
                                with ui.row().classes('gap-2'):
                                    ui.button(icon='fullscreen', on_click=toggle_fullscreen_edit).props('flat round dense').tooltip(tr('Fullscreen Edit'))
                                    ui.button(tr('Cancel'), icon='close', on_click=cancel_edit).props('flat dense color=grey')
                                    ui.button(tr('Save Draft'), icon='save', on_click=handle_save_draft).props('flat dense color=primary')
                                    ui.button(tr('Submit'), on_click=handle_submit_correction).props('unelevated dense color=green')

                            # Error Message Display
                            if state.error_message:
                                ui.markdown(f"**Error:** {state.error_message}").classes('w-full p-2 text-red-600 bg-red-100 border-b border-red-200 text-sm')

                            # Text Area - use readable Hebrew font
                            textarea = ui.textarea(value=state.edit_text).classes('w-full h-full text-lg').props(
                                'borderless autofocus input-style="height: 100%; min-height: 500px;"'
                            ).style(
                                'direction: rtl; text-align: right; resize: none; flex: 1; padding: 16px; '
                                'font-family: "Noto Sans Hebrew", "Segoe UI", "Arial Hebrew", sans-serif; '
                                'font-size: 1.2rem; line-height: 1.8; min-height: 500px;'
                            )
                            # Bind value manually
                            textarea.bind_value(state, 'edit_text')

                            def on_edit_input():
                                if state.draft_saved:
                                    state.draft_saved = False
                                    update_content()
                            
                            textarea.on('input', on_edit_input)
                            
                            # Notes field (Expanded by default if notes exist)
                            with ui.expansion(tr('Add Notes'), icon='note_add', value=bool(state.edit_notes)).classes('w-full border-t bg-gray-50'):
                                ui.textarea(label=tr('Notes'), value=state.edit_notes).bind_value(state, 'edit_notes').classes('w-full p-2').props('outlined dense')
                        elif state.edit_loading:
                            # === LOADING EDIT MODE ===
                            with ui.column().classes('w-full h-full items-center justify-center'):
                                ui.spinner(size='lg', color='primary')
                                ui.label(tr('Loading...')).classes('mt-2 font-bold').style('color: var(--text-tertiary);')
                        else:
                            # === VIEW MODE ===
                            # Text content container
                            text_container = ui.column().classes('w-full h-full')
                            current_text = {'value': page.text}

                            def render_text_content(text: str):
                                """Render text content with optional highlighting."""
                                text_container.clear()
                                with text_container:
                                    with ui.scroll_area().classes('w-full').style('height: calc(60vh - 80px); padding: 20px;'):
                                        if text:
                                            if state.highlight_terms:
                                                display_text = highlight_text(text)
                                                ui.html(f'<div class="transcription-text" style="font-size: 1.4rem; line-height: 2.2;">{display_text}</div>', sanitize=False)
                                            else:
                                                ui.label(text).style(
                                                    'font-size: 1.4rem; line-height: 2.2; direction: rtl; text-align: right; '
                                                    'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap; '
                                                    'overflow-wrap: break-word; word-break: break-word;'
                                                )
                                        else:
                                            with ui.column().classes('items-center justify-center h-full'):
                                                ui.icon('text_snippet', size='4rem').style('color: var(--text-muted);')
                                                ui.label(tr('No text available')).classes('mt-4 text-xl').style('color: var(--text-muted);')
                                text_container.update()

                            def handle_version_change(new_text: str, version_info: dict):
                                """Handle version selection - update displayed text."""
                                current_text['value'] = new_text
                                render_text_content(new_text)
                                source = version_info.get('source', 'unknown')
                                author = version_info.get('author', '')

                                if source == 'pgp':
                                    attribution = version_info.get('attribution', 'PGP')
                                    ui.notify(
                                        f"{tr('PGP Transcription')} - {attribution}",
                                        type='positive'
                                    )
                                elif source == 'translation':
                                    attribution = version_info.get('attribution', '')
                                    language = version_info.get('language', '')
                                    ui.notify(
                                        f"{language} {tr('Translation')} - {attribution}",
                                        type='info'
                                    )
                                elif source == 'user' and author:
                                    ui.notify(f"{tr('Showing version by')} {author}", type='info')
                                elif source in ('V0.7', 'V0.8'):
                                    ui.notify(f"{tr('Showing')} {source}", type='info')

                            # Version selector placeholder (populated by enrichment Phase B)
                            if page.text:
                                version_row = ui.row().classes('items-center p-2 border-b')
                                enrichment_refs['version_container'] = version_row
                                enrichment_refs['version_change_handler'] = handle_version_change
                                if state.enrichment_loaded:
                                    with version_row:
                                        create_version_selector(
                                            document_id=page.sys_id,
                                            page_number=page.p_num,
                                            original_text=page.text,
                                            on_version_change=handle_version_change,
                                            pgp_transcription=state.pgp_transcription,
                                            all_sources=state.all_sources
                                        )

                            # Initial render
                            render_text_content(page.text if page.text else None)

                # Comments section - below panels
                from web.components import create_notes_panel
                _, notes_refresh_fn = create_notes_panel(
                    document_id=page.sys_id,
                    page_number=page.p_num,
                    shelfmark=page.shelfmark or page.sys_id,
                    ie_id=state.volume_ie
                )
                # Store the refresh function so comment dialog can call it
                notes_refresh_ref['refresh'] = notes_refresh_fn

                # === FULLSCREEN EDIT OVERLAY ===
                if state.fullscreen_edit and state.edit_mode:
                    with ui.element('div').classes('fullscreen-edit-overlay'):
                        # Toolbar
                        with ui.element('div').classes('fullscreen-edit-toolbar'):
                            with ui.row().classes('items-center gap-4'):
                                ui.label(tr('Fullscreen Edit')).classes('font-bold').style('color: var(--text-primary);')
                                if page.shelfmark:
                                    ui.label(f"• {page.shelfmark}").classes('text-sm').style('color: var(--text-secondary);')
                                if state.draft_saved:
                                    ui.label(tr('Saved')).classes('text-green-600 text-sm font-bold')
                                else:
                                    ui.label(tr('Unsaved changes')).classes('text-orange-600 text-sm')

                            with ui.row().classes('items-center gap-2'):
                                # Save/Submit/Exit controls only
                                ui.button(tr('Save Draft'), icon='save', on_click=handle_save_draft).props('flat dense color=primary')
                                ui.button(tr('Submit'), on_click=handle_submit_correction).props('unelevated dense color=green')
                                ui.button(icon='fullscreen_exit', on_click=toggle_fullscreen_edit).props('flat round dense data-action="exit-fullscreen-edit"').tooltip(tr('Exit Fullscreen'))

                        # Content area: Image + Splitter + Text
                        with ui.element('div').classes('fullscreen-edit-content').props('id="fs-edit-content"'):
                            # Image panel (left) with its own toolbar
                            with ui.element('div').classes('fullscreen-edit-image-wrapper').props('id="fs-image-wrapper"'):
                                # Image controls toolbar
                                with ui.element('div').classes('fullscreen-image-toolbar'):
                                    ui.button(icon='remove', on_click=zoom_out).props('flat round dense size=sm').tooltip(tr('Zoom out'))
                                    ui.label(f'{int(state.zoom_level * 100)}%').classes('text-xs zoom-level-label').style('color: #ccc; min-width: 40px; text-align: center;')
                                    ui.button(icon='add', on_click=zoom_in).props('flat round dense size=sm').tooltip(tr('Zoom in'))
                                    ui.separator().props('vertical').classes('mx-1 h-4')
                                    ui.button(icon='rotate_left', on_click=rotate_left).props('flat round dense size=sm').tooltip(tr('Rotate left'))
                                    ui.button(icon='rotate_right', on_click=rotate_right).props('flat round dense size=sm').tooltip(tr('Rotate right'))
                                    ui.button(icon='restart_alt', on_click=zoom_reset).props('flat round dense size=sm').tooltip(tr('Reset view'))

                                # Fullscreen image adjustment controls
                                with ui.element('div').classes('fullscreen-image-toolbar').style(
                                    'border-top: 1px solid #444; padding: 2px 8px;'
                                ):
                                    ui.icon('brightness_6').style('color: #ccc; font-size: 1rem;').tooltip(tr('Brightness'))
                                    slider_refs['fs_brightness'] = ui.slider(
                                        min=-100, max=100, step=1, value=0,
                                        on_change=lambda e: ui.run_javascript(f'if(window.fsEditViewer) window.fsEditViewer.setBrightness({e.value})')
                                    ).props('dark dense').classes('w-20')
                                    ui.icon('contrast').style('color: #ccc; font-size: 1rem;').tooltip(tr('Contrast'))
                                    slider_refs['fs_contrast'] = ui.slider(
                                        min=-100, max=100, step=1, value=0,
                                        on_change=lambda e: ui.run_javascript(f'if(window.fsEditViewer) window.fsEditViewer.setContrast({e.value})')
                                    ).props('dark dense').classes('w-20')
                                    ui.icon('timeline').style('color: #ccc; font-size: 1rem;').tooltip(tr('Gamma'))
                                    slider_refs['fs_gamma'] = ui.slider(
                                        min=20, max=300, step=1, value=100,
                                        on_change=lambda e: ui.run_javascript(f'if(window.fsEditViewer) window.fsEditViewer.setGamma({e.value / 100})')
                                    ).props('dark dense').classes('w-20')
                                    ui.button(
                                        icon='exposure',
                                        on_click=lambda: ui.run_javascript('if(window.fsEditViewer) window.fsEditViewer.toggleInvert()')
                                    ).props('flat round size=sm').tooltip(tr('Invert Colors'))
                                    def _fs_reset_adj():
                                        if slider_refs.get('fs_brightness'): slider_refs['fs_brightness'].value = 0
                                        if slider_refs.get('fs_contrast'): slider_refs['fs_contrast'].value = 0
                                        if slider_refs.get('fs_gamma'): slider_refs['fs_gamma'].value = 100
                                        ui.run_javascript('if(window.fsEditViewer) window.fsEditViewer.resetAdjustments()')
                                    ui.button(
                                        icon='restart_alt',
                                        on_click=_fs_reset_adj
                                    ).props('flat round size=sm').tooltip(tr('Reset Image'))

                                # Image display area
                                with ui.element('div').classes('fullscreen-edit-image img-loading-container').props('id="fs-image-panel"'):
                                    if has_image and img_url:
                                        safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                        safe_sys_id = (page.sys_id or '').replace("'", "\\'").replace('"', '\\"')
                                        is_oxford_js = 'true' if is_oxford else 'false'
                                        _fs_thumb = safe_img_url
                                        _fs_full = safe_img_url
                                        if '/api/nli_image_by_sysid/' in safe_img_url:
                                            _sep = '&' if '?' in safe_img_url else '?'
                                            _fs_thumb = f"{safe_img_url}{_sep}width=400"
                                        img_html = f'<img src="{_fs_thumb}" data-full-src="{_fs_full}" class="zoomable-image" id="fs-zoomable-image" style="transform: translate(0px, 0px) rotate({state.rotation}deg) scale({state.zoom_level}); cursor: grab;" draggable="false" onerror="handleImageError(this, \'{safe_sys_id}\', {page_idx}, {is_oxford_js}, \'fsEditViewer\')" />'
                                        ui.html(img_html, sanitize=False)
                                        ui.run_javascript('initProgressiveImages();')
                                    else:
                                        with ui.column().classes('items-center justify-center h-full'):
                                            ui.icon('image_not_supported', size='4rem').style('color: #666;')
                                            ui.label(tr('No image available')).style('color: #888;')

                            # Draggable splitter
                            ui.element('div').classes('fullscreen-edit-splitter').props('id="fs-splitter"')

                            # Text panel (right)
                            with ui.element('div').classes('fullscreen-edit-text').props('id="fs-text-panel"'):
                                fs_textarea = ui.textarea(value=state.edit_text).classes('w-full fullscreen-textarea').props(
                                    'outlined autogrow id="fs-textarea"'
                                ).style(
                                    'direction: rtl; text-align: right; '
                                    'font-family: "Noto Sans Hebrew", "Segoe UI", "Arial Hebrew", sans-serif; '
                                    'font-size: 1.2rem; line-height: 1.8;'
                                )
                                fs_textarea.bind_value(state, 'edit_text')

                                def on_fs_edit():
                                    if state.draft_saved:
                                        state.draft_saved = False
                                fs_textarea.on('input', on_fs_edit)

                                # JavaScript to force textarea height after render
                                ui.run_javascript('''
                                    setTimeout(() => {
                                        const panel = document.getElementById('fs-text-panel');
                                        const textarea = panel?.querySelector('textarea');
                                        if (panel && textarea) {
                                            const setHeight = () => {
                                                const h = panel.clientHeight - 20;
                                                textarea.style.height = h + 'px';
                                                textarea.style.minHeight = h + 'px';
                                                textarea.style.maxHeight = h + 'px';
                                            };
                                            setHeight();
                                            // Also handle resize
                                            new ResizeObserver(setHeight).observe(panel);
                                            textarea.focus();
                                        }
                                    }, 50);
                                ''')

                        # JavaScript: ESC key, splitter drag, image pan/zoom
                        ui.run_javascript('''
                            (function() {
                                const fsOverlay = document.querySelector('.fullscreen-edit-overlay');
                                if (!fsOverlay) return;

                                // ESC key to exit
                                const escHandler = (e) => {
                                    if (e.key === 'Escape') {
                                        document.querySelector('[data-action="exit-fullscreen-edit"]')?.click();
                                    }
                                };
                                document.addEventListener('keydown', escHandler);

                                // Initialize image pan/zoom for fullscreen
                                setTimeout(() => {
                                    const fsImage = document.getElementById('fs-zoomable-image');
                                    const fsImagePanel = document.getElementById('fs-image-panel');

                                    if (fsImage && fsImagePanel) {
                                        // Create dedicated viewer state for fullscreen
                                        const fsViewer = {
                                            el: fsImage,
                                            state: { x: 0, y: 0, scale: 1, rotation: 0, isDragging: false, startX: 0, startY: 0, brightness: 0, contrast: 0, gamma: 1.0, invert: false },

                                            applyTransform: function() {
                                                this.el.style.transform = `translate(${this.state.x}px, ${this.state.y}px) rotate(${this.state.rotation}deg) scale(${this.state.scale})`;
                                                this._applyFilters();
                                            },

                                            _applyFilters: function() {
                                                const s = this.state;
                                                const b = 1 + s.brightness / 100;
                                                const c = 1 + s.contrast / 100;
                                                const inv = s.invert ? 1 : 0;
                                                let f = 'brightness(' + b + ') contrast(' + c + ') invert(' + inv + ')';
                                                if (s.gamma !== 1.0) {
                                                    const svgFilter = document.getElementById('gamma-fs');
                                                    if (svgFilter) {
                                                        const exp = 1.0 / s.gamma;
                                                        svgFilter.querySelectorAll('feFuncR, feFuncG, feFuncB').forEach(fn => fn.setAttribute('exponent', exp));
                                                    }
                                                    f += ' url(#gamma-fs)';
                                                }
                                                this.el.style.filter = f;
                                            },

                                            setBrightness: function(val) { this.state.brightness = val; this._applyFilters(); },
                                            setContrast: function(val) { this.state.contrast = val; this._applyFilters(); },
                                            setGamma: function(val) { this.state.gamma = val; this._applyFilters(); },
                                            toggleInvert: function() { this.state.invert = !this.state.invert; this._applyFilters(); },
                                            resetAdjustments: function() {
                                                this.state.brightness = 0; this.state.contrast = 0;
                                                this.state.gamma = 1.0; this.state.invert = false;
                                                this._applyFilters();
                                            },

                                            onWheel: function(e) {
                                                e.preventDefault();
                                                e.stopPropagation();
                                                const delta = e.deltaY > 0 ? -0.15 : 0.15;
                                                this.state.scale = Math.max(0.25, Math.min(5, this.state.scale + delta));
                                                this.applyTransform();
                                                // Update zoom label in image toolbar
                                                const label = document.querySelector('.fullscreen-image-toolbar .zoom-level-label');
                                                if (label) label.textContent = Math.round(this.state.scale * 100) + '%';
                                            },

                                            onMouseDown: function(e) {
                                                if (e.button !== 0) return;
                                                e.preventDefault();
                                                this.state.isDragging = true;
                                                this.state.startX = e.clientX - this.state.x;
                                                this.state.startY = e.clientY - this.state.y;
                                                this.el.style.cursor = 'grabbing';
                                            },

                                            onMouseMove: function(e) {
                                                if (!this.state.isDragging) return;
                                                e.preventDefault();
                                                this.state.x = e.clientX - this.state.startX;
                                                this.state.y = e.clientY - this.state.startY;
                                                this.applyTransform();
                                            },

                                            onMouseUp: function() {
                                                this.state.isDragging = false;
                                                this.el.style.cursor = 'grab';
                                            }
                                        };

                                        // Bind events
                                        fsImagePanel.addEventListener('wheel', (e) => fsViewer.onWheel(e), { passive: false });
                                        fsImage.addEventListener('mousedown', (e) => fsViewer.onMouseDown(e));
                                        document.addEventListener('mousemove', (e) => fsViewer.onMouseMove(e));
                                        document.addEventListener('mouseup', () => fsViewer.onMouseUp());
                                        fsImage.ondragstart = (e) => e.preventDefault();
                                        fsImage.style.cursor = 'grab';

                                        // Store reference for button controls
                                        window.fsEditViewer = fsViewer;
                                    }
                                }, 100);

                                // Splitter drag functionality
                                const splitter = document.getElementById('fs-splitter');
                                const imageWrapper = document.getElementById('fs-image-wrapper');
                                const textPanel = document.getElementById('fs-text-panel');
                                const content = document.getElementById('fs-edit-content');

                                if (splitter && imageWrapper && textPanel && content) {
                                    let isDragging = false;
                                    let startX, startWidth;

                                    splitter.addEventListener('mousedown', (e) => {
                                        isDragging = true;
                                        startX = e.clientX;
                                        startWidth = imageWrapper.offsetWidth;
                                        splitter.classList.add('dragging');
                                        document.body.style.cursor = 'col-resize';
                                        document.body.style.userSelect = 'none';
                                        e.preventDefault();
                                    });

                                    document.addEventListener('mousemove', (e) => {
                                        if (!isDragging) return;
                                        const delta = e.clientX - startX;
                                        const newWidth = Math.max(200, Math.min(startWidth + delta, content.offsetWidth - 250));
                                        imageWrapper.style.flex = 'none';
                                        imageWrapper.style.width = newWidth + 'px';
                                    });

                                    document.addEventListener('mouseup', () => {
                                        if (isDragging) {
                                            isDragging = false;
                                            splitter.classList.remove('dragging');
                                            document.body.style.cursor = '';
                                            document.body.style.userSelect = '';
                                            // Recalculate textarea height
                                            const textarea = textPanel.querySelector('textarea');
                                            if (textarea) {
                                                const h = textPanel.clientHeight - 20;
                                                textarea.style.height = h + 'px';
                                            }
                                        }
                                    });
                                }
                            })();
                        ''')

    refs.update_content = update_content

    async def set_shelfmark_and_search(shelfmark: str):
        """Set shelfmark and trigger search."""
        state.shelfmark_query = shelfmark
        await search_shelfmark()

    # === Main Layout ===
    with ui.column().classes('w-full max-w-7xl mx-auto p-4'):
        # Page title
        # Changed to H1
        h1(tr('Browse Manuscripts'), classes='text-3xl font-bold mb-6 text-center text-green-800')

        # Shelfmark Search Box - Simple and Working
        with ui.card().classes('w-full p-4 mb-6').style('background: var(--bg-tertiary); border: 1px solid var(--border-light);'):
            with ui.row().classes('w-full gap-4 items-center'):
                # Search icon
                ui.icon('search', size='md').classes('text-green-600')

                # Simple input that works
                search_input = ui.input(
                    placeholder=tr('e.g. T-S 8J6.1'),
                    label=tr('Enter shelfmark or NLI system ID')
                ).classes('flex-1').props('outlined dense clearable color=green')

                # Store reference for updates from other functions (e.g. suggestion dialog)
                slider_refs['search_input'] = search_input

                # Set initial value if we have one
                if state.shelfmark_query:
                    search_input.value = state.shelfmark_query

                async def do_search():
                    state.shelfmark_query = search_input.value or ''
                    state.search_error = None  # Clear previous error
                    if state.shelfmark_query.strip():
                        await search_shelfmark()

                search_input.on('keydown.enter', do_search)

                # Go button
                ui.button(
                    tr('Go'),
                    on_click=do_search
                ).props('color=green').classes('px-6')

            # Error notice area (below search bar)
            search_error_container = ui.row().classes('w-full mt-2 items-center')
            slider_refs['search_error_container'] = search_error_container

        # Service status warning
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 border border-yellow-300 mb-4'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')

        # Main content container
        content_container = ui.column().classes('w-full')

        # Load initial page if sys_id or fl_id provided (async via ensure_future)
        if initial_fl_id_value:
            # Load by FL ID — delegate to async load_page with fl_id parameter
            state.is_loading = True
            update_content()  # Show spinner synchronously before async kicks in
            asyncio.ensure_future(load_page(fl_id=initial_fl_id_value))
        elif initial_sys_id:
            # Determine if this is a language-switch reload (same manuscripts)
            # or cross-page navigation (different manuscript requested)
            saved_rd = None
            try:
                saved_rd = app.storage.user.get('reading_desk_state')
            except Exception:
                pass  # Browser storage operation failed; preference not persisted

            if saved_rd and saved_rd.get('entries'):
                # Check if initial_sys_id matches one of the persisted desk entries
                persisted_sids = {e.get('sys_id', '') for e in saved_rd['entries']}
                if initial_sys_id in persisted_sids:
                    # Language-switch case: sys_id is one of the desk's manuscripts
                    # Restore the full reading desk
                    if not _restore_reading_desk_state():
                        state.is_loading = True
                        update_content()
                        asyncio.ensure_future(load_page(p_num=initial_page))
                else:
                    # Cross-page navigation: user wants a DIFFERENT manuscript
                    # Clear stale reading desk state and load the requested manuscript
                    try:
                        app.storage.user.pop('reading_desk_state', None)
                    except Exception:
                        pass  # Browser storage operation failed; preference not persisted
                    state.is_loading = True
                    update_content()
                    asyncio.ensure_future(load_page(p_num=initial_page))
            else:
                # No saved reading desk state, normal page load
                state.is_loading = True
                update_content()
                asyncio.ensure_future(load_page(p_num=initial_page))
        elif _pending_shelfmark:
            # Shelfmark passed via URL param — auto-search on load
            state.shelfmark_query = _pending_shelfmark
            asyncio.ensure_future(search_shelfmark())
        else:
            # No sys_id in URL -- try to restore reading desk (language-switch case)
            if _restore_reading_desk_state():
                pass  # Reading desk restored successfully
            else:
                # Try to restore previous position
                saved_position = app.storage.user.get('browse_position')
                if saved_position and saved_position.get('sys_id'):
                    state.sys_id = saved_position['sys_id']
                    state.shelfmark_query = saved_position.get('shelfmark', '')
                    # Restore volume_ie with validation (D-12: invalid falls back to None)
                    restored_vie = saved_position.get('volume_ie')
                    if restored_vie:
                        from genizah_core import get_volumes_for_sys_id
                        volumes = get_volumes_for_sys_id(saved_position['sys_id'])
                        if any(v['ie_id'] == restored_vie for v in volumes):
                            state.volume_ie = restored_vie
                        # else: silently fall back to None (primary IE)
                    state.is_loading = True
                    update_content()
                    asyncio.ensure_future(load_page(p_num=saved_position.get('p_num', 1)))
                else:
                    update_content()

        # Add keyboard event handlers
        ui.add_body_html('''
        <script>
            document.addEventListener('keydown', function(e) {
                // Ignore if focused on any text input element
                const tag = document.activeElement.tagName;
                if (tag === 'INPUT' || tag === 'TEXTAREA' || document.activeElement.isContentEditable) return;

                switch(e.key) {
                    case 'ArrowLeft':
                        // Navigate to next page (RTL)
                        document.querySelector('[data-action="next"]')?.click();
                        break;
                    case 'ArrowRight':
                        // Navigate to previous page (RTL)
                        document.querySelector('[data-action="prev"]')?.click();
                        break;
                    case '+':
                    case '=':
                        document.querySelector('[data-action="zoom-in"]')?.click();
                        break;
                    case '-':
                        document.querySelector('[data-action="zoom-out"]')?.click();
                        break;
                    case 'f':
                    case 'F':
                        document.querySelector('[data-action="fullscreen"]')?.click();
                        break;
                }
            });
        </script>
        ''')
