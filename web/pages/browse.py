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

from nicegui import ui, app
from typing import Optional, List
import re
import html as html_module

from web.services import get_service, BrowsePage, DocumentPage, get_thumbnail_url, get_full_image_url
from web.translations import tr, is_rtl
from web.auth_state import GlobalAuthState
from web.supabase_client import create_correction, update_correction, get_corrections
from web.components.typography import h1, h2, h3


# ============================================================================
# Custom Styles for Manuscript Viewer
# ============================================================================

VIEWER_STYLES = '''
<script>
// NLI IIIF base URL for direct browser access
const NLI_IIIF_BASE = 'https://iiif.nli.org.il/IIIFv21';

// Cache for FL IDs fetched from IIIF manifests
const flIdCache = {};

// Fetch FL IDs from IIIF manifest (client-side, bypasses server blocking)
async function fetchFlIdsFromManifest(sysId) {
    if (flIdCache[sysId]) {
        console.log(`[Manifest] Cache hit for ${sysId}`);
        return flIdCache[sysId];
    }

    const manifestUrl = `${NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS${sysId}-1/manifest`;
    console.log(`[Manifest] Fetching ${manifestUrl}`);
    try {
        const resp = await fetch(manifestUrl);
        console.log(`[Manifest] Response status: ${resp.status} for ${sysId}`);
        if (!resp.ok) {
            console.log(`[Manifest] Failed (${resp.status}) for ${sysId}`);
            return [];
        }

        const data = await resp.json();
        const flIds = [];

        if (data.sequences && data.sequences[0] && data.sequences[0].canvases) {
            console.log(`[Manifest] Found ${data.sequences[0].canvases.length} canvases for ${sysId}`);
            for (const canvas of data.sequences[0].canvases) {
                const images = canvas.images || [];
                if (images[0] && images[0].resource && images[0].resource.service) {
                    const serviceId = images[0].resource.service['@id'] || '';
                    const match = serviceId.match(/FL(\d+)/);
                    if (match) {
                        flIds.push(match[1]);
                    }
                }
            }
        } else {
            console.log(`[Manifest] No canvases found in manifest for ${sysId}`);
        }

        if (flIds.length > 0) {
            flIdCache[sysId] = flIds;
            console.log(`[Manifest] Cached ${flIds.length} FL IDs for ${sysId}`);
        } else {
            console.log(`[Manifest] No FL IDs extracted for ${sysId}`);
        }
        return flIds;
    } catch (e) {
        console.error(`[Manifest] Error fetching for ${sysId}:`, e);
        return [];
    }
}

// Global function for handling image errors with fallback
async function handleImageError(img, sysId, pageIdx, isOxford = false) {
    const currentSrc = img.src || '';
    const isOxfordApiUrl = currentSrc.includes('/api/oxford_image/');
    console.log(`[handleImageError] src=${currentSrc}, sysId=${sysId}, pageIdx=${pageIdx}, isOxford=${isOxford}, isOxfordApiUrl=${isOxfordApiUrl}`);

    // Try 1: If Oxford and the CURRENT src is NOT already the Oxford API, try the server proxy
    // This handles the case where direct NLI URL failed for an Oxford manuscript
    if (isOxford && sysId && !isOxfordApiUrl && !img.dataset.triedOxford) {
        img.dataset.triedOxford = 'true';
        const oxfordUrl = `/api/oxford_image/${sysId}?page=${pageIdx || 0}`;
        console.log(`Trying Oxford API: ${oxfordUrl}`);
        img.src = oxfordUrl;
        img.onload = function() {
            console.log('Oxford API image loaded');
            if (window.manuscriptViewer) window.manuscriptViewer.init();
        };
        return;
    }

    // If Oxford API already failed, mark it as tried
    if (isOxfordApiUrl) {
        img.dataset.triedOxford = 'true';
    }

    // Try 2: Fetch FL IDs from NLI IIIF manifest
    if (sysId && !img.dataset.triedManifest) {
        img.dataset.triedManifest = 'true';
        console.log(`Trying NLI manifest for sysId: ${sysId}, page: ${pageIdx}`);

        const flIds = await fetchFlIdsFromManifest(sysId);
        if (flIds.length > 0) {
            const idx = Math.min(pageIdx || 0, flIds.length - 1);
            const newUrl = `${NLI_IIIF_BASE}/FL${flIds[idx]}/full/max/0/default.jpg`;
            console.log(`Trying FL ID from manifest: ${flIds[idx]}`);
            img.src = newUrl;
            img.onload = function() {
                console.log('Manifest-based image loaded, initializing viewer');
                if (window.manuscriptViewer) window.manuscriptViewer.init();
            };
            return;
        }
    }

    // Try 3: Use server-side NLI proxy (handles collections that block browser requests)
    if (sysId && !img.dataset.triedServerProxy) {
        img.dataset.triedServerProxy = 'true';
        const proxyUrl = `/api/nli_image_by_sysid/${sysId}?page=${pageIdx || 0}`;
        console.log(`Trying server-side NLI proxy: ${proxyUrl}`);
        img.src = proxyUrl;
        img.onload = function() {
            console.log('Server proxy image loaded');
            if (window.manuscriptViewer) window.manuscriptViewer.init();
        };
        return;
    }

    // All fallbacks exhausted
    console.log('All image sources failed for:', currentSrc);
    img.style.display = 'none';
    const parent = img.parentElement;
    if (parent) {
        parent.innerHTML = '<div style="text-align: center; color: #888;"><i class="material-icons" style="font-size: 4rem;">image_not_supported</i><p>Image not available</p></div>';
    }
}
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
    // Global viewer state management
    window.manuscriptViewer = {
        el: null,
        container: null,
        state: {
            scale: 1,
            rotation: 0,
            x: 0,
            y: 0,
            isDragging: false,
            startX: 0,
            startY: 0
        },

        init: function() {
            this.el = document.querySelector('.zoomable-image');
            this.container = document.querySelector('.image-container');
            
            if (!this.el) {
                console.log('manuscriptViewer: image not found');
                return;
            }
            if (!this.container) {
                console.log('manuscriptViewer: container not found');
                return;
            }
            
            console.log('manuscriptViewer: initializing drag on image');
            
            // Attach mousedown directly to the IMAGE element
            this.el.onmousedown = this.onMouseDown.bind(this);
            window.onmousemove = this.onMouseMove.bind(this);
            window.onmouseup = this.onMouseUp.bind(this);
            this.el.ondragstart = (e) => e.preventDefault();
            
            // Mouse wheel zoom - attach to image
            this.el.onwheel = this.onWheel.bind(this);
            
            // Set initial cursor on the image
            this.el.style.cursor = 'grab';
        },
        
        onWheel: function(e) {
            e.preventDefault();
            const delta = e.deltaY > 0 ? -0.25 : 0.25;
            this.state.scale = Math.max(0.25, Math.min(4, this.state.scale + delta));
            this.applyTransform();
            // Update zoom label
            const zoomLabel = document.querySelector('.zoom-level-label');
            if (zoomLabel) {
                zoomLabel.textContent = Math.round(this.state.scale * 100) + '%';
            }
        },
        
        update: function(scale, rotation) {
            this.state.scale = scale;
            this.state.rotation = rotation;
            this.applyTransform();
        },
        
        setTransform: function(x, y, scale, rotation) {
            this.state.x = x;
            this.state.y = y;
            this.state.scale = scale;
            this.state.rotation = rotation;
            this.applyTransform();
        },

        onMouseDown: function(e) {
            if (e.button !== 0) return; // Only left click
            e.preventDefault();
            e.stopPropagation();
            this.state.isDragging = true;
            this.state.startX = e.clientX - this.state.x;
            this.state.startY = e.clientY - this.state.y;
            this.el.style.cursor = 'grabbing';
            console.log('manuscriptViewer: drag started');
        },

        onMouseMove: function(e) {
            if (!this.state.isDragging) return;
            e.preventDefault();
            
            this.state.x = e.clientX - this.state.startX;
            this.state.y = e.clientY - this.state.startY;
            
            requestAnimationFrame(() => this.applyTransform());
        },

        onMouseUp: function() {
            if (this.state.isDragging) {
                console.log('manuscriptViewer: drag ended');
            }
            this.state.isDragging = false;
            if (this.el) this.el.style.cursor = 'grab';
        },

        applyTransform: function() {
            if (!this.el) {
                 this.el = document.querySelector('.zoomable-image');
                 if (!this.el) return;
            }
            // Translate is applied first (screen coordinates), then rotate/scale
            this.el.style.transform = `translate(${this.state.x}px, ${this.state.y}px) rotate(${this.state.rotation}deg) scale(${this.state.scale})`;
        },
        
        reset: function() {
            this.state.x = 0;
            this.state.y = 0;
            this.state.rotation = 0;
            this.state.scale = 1;
            this.applyTransform();
        }
    };
    
    // Auto-init when DOM loads or changes
    document.addEventListener('DOMContentLoaded', () => {
        setTimeout(() => window.manuscriptViewer.init(), 500);
    });
</script>
'''


class BrowseState:
    """Holds the state for the browse page."""

    def __init__(self):
        self.shelfmark_query: str = ''
        self.current_page: Optional[BrowsePage] = None
        self.sys_id: Optional[str] = None
        self.is_loading: bool = False
        self.error: Optional[str] = None
        self.search_error: Optional[str] = None  # Inline error for shelfmark not found
        self.zoom_level: float = 1.0
        self.rotation: int = 0
        self.is_fullscreen: bool = False
        self.highlight_terms: Optional[str] = None
        self.page_input_value: int = 1
        self.view_all: bool = False
        self.full_manuscript: List[DocumentPage] = []
        # Edit state
        self.edit_mode: bool = False
        self.edit_text: str = ""
        self.edit_notes: str = ""
        self.original_edit_text: str = ""  # Text when editing started
        self.draft_saved: bool = False
        self.draft_id: Optional[str] = None
        self.edit_loading: bool = False
        self.error_message: Optional[str] = None
        self.fullscreen_edit: bool = False  # Fullscreen edit mode


def create_browse_page(initial_sys_id: Optional[str] = None, highlight: Optional[str] = None, initial_fl_id: Optional[str] = None, initial_page: Optional[int] = None):
    """Create the professional manuscript viewer page UI."""
    state = BrowseState()
    service = get_service()

    # Track metadata panel visibility
    show_metadata = {'value': False}

    # UI component references
    content_container = None
    metadata_panel = None
    image_element = None
    viewer_container = None
    initial_fl_id_value = initial_fl_id
    slider_refs = {}  # References for UI controls to allow updates from code

    if initial_sys_id:
        state.sys_id = initial_sys_id
    if highlight:
        state.highlight_terms = highlight

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

    def search_shelfmark():
        """Search for manuscripts by shelfmark."""
        if not state.shelfmark_query.strip():
            return

        state.is_loading = True
        state.error = None
        state.search_error = None  # Clear inline error
        update_search_error()  # Clear the error display
        update_content()

        try:
            results, exact_match = service.search_by_shelfmark(state.shelfmark_query.strip(), limit=20)

            if not results:
                # Show inline error below search bar instead of full-page error
                state.search_error = tr('No manuscript found') + f": '{state.shelfmark_query}'"
                state.is_loading = False
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
                load_page(p_num=1)  # Always start at page 1 for new manuscript
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
                    def select_result(r=result):
                        dialog.close()
                        state.sys_id = r.sys_id
                        state.shelfmark_query = r.shelfmark  # Update state with selected shelfmark
                        # Update the search input field if available
                        if slider_refs.get('search_input'):
                            slider_refs['search_input'].value = r.shelfmark
                        state.current_page = None
                        load_page(p_num=1)

                    with ui.card().classes(
                        'w-full p-3 cursor-pointer hover:bg-gray-100'
                    ).style(
                        'background: var(--bg-surface); border: 1px solid var(--border-subtle);'
                    ).on('click', select_result):
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

    def load_page(direction: int = 0, p_num: Optional[int] = None):
        """Load a page of the manuscript."""
        if not state.sys_id:
            state.error = tr('No manuscript found')
            update_content()
            return

        state.is_loading = True
        state.error = None
        state.zoom_level = 1.0  # Reset zoom on page change
        update_content()  # Show loading state

        try:
            if p_num is not None:
                page = service.get_browse_page(state.sys_id, p_num=p_num)
            elif state.current_page:
                page = service.get_browse_page(
                    state.sys_id,
                    p_num=state.current_page.p_num,
                    direction=direction
                )
            else:
                page = service.get_browse_page(state.sys_id, p_num=1)

            if page:
                state.current_page = page
                state.page_input_value = page.p_num
                state.error = None

                # Save position to storage for persistence
                try:
                    app.storage.user['browse_position'] = {
                        'sys_id': state.sys_id,
                        'p_num': page.p_num,
                        'shelfmark': page.shelfmark
                    }
                except Exception:
                    pass

                # Track recently viewed item
                if state.sys_id and service.is_ready:
                    try:
                        from web.state import state as app_state
                        if app_state.lists_mgr:
                            # Use sync version to avoid async/await issues
                            app_state.lists_mgr.add_to_recent_sync(state.sys_id, fl_id=page.fl_id)
                    except Exception as track_err:
                        print(f"Failed to track recent item: {track_err}")
            else:
                state.error = tr('No text available') + f" (sys_id: {state.sys_id})"

        except Exception as e:
            state.error = f"{tr('Error')}: {str(e)}"

        finally:
            state.is_loading = False
            update_content()

    def go_to_page(new_page: int):
        """Navigate to a specific page number."""
        if new_page < 1:
            new_page = 1
        if state.current_page and new_page > state.current_page.total_pages:
            new_page = state.current_page.total_pages
        load_page(p_num=new_page)

    def navigate_shelfmark(direction: int):
        """Navigate to next/prev shelfmark based on file order."""
        if not state.sys_id:
            return

        state.is_loading = True
        update_content()

        try:
            adjacent_sys_id = service.get_adjacent_shelfmark(state.sys_id, direction)
            if adjacent_sys_id:
                state.sys_id = adjacent_sys_id
                state.view_all = False  # Reset to single page view
                state.full_manuscript = []
                load_page(p_num=1)  # Load first page of new manuscript
            else:
                state.is_loading = False
                # Show message: at first/last manuscript
                state.error = tr('No more manuscripts') if direction > 0 else tr('At first manuscript')
                update_content()
        except Exception as e:
            state.error = f"{tr('Error')}: {str(e)}"
            state.is_loading = False
            update_content()

    def toggle_view_all():
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
                pages = service.get_full_manuscript(state.sys_id)
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
            print(f"Error navigating to parallels: {e}")
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

        # Prepare export data
        export_data = {
            'shelfmark': state.current_page.shelfmark,
            'title': state.current_page.title,
            'sys_id': state.sys_id,
            'view_all': state.view_all
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
        print(f"[DEBUG] add_manuscript_to_list called, sys_id={state.sys_id}, current_page={state.current_page}")
        if not state.sys_id or not state.current_page:
            ui.notify(tr('Please load a manuscript first'), type='warning')
            return

        from web.state import state as app_state
        from web.components import show_add_to_list_dialog
        print(f"[DEBUG] app_state.lists_mgr = {app_state.lists_mgr}")
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        print(f"[DEBUG] Opening dialog for sys_id={state.sys_id}, shelfmark={state.current_page.shelfmark}")
        show_add_to_list_dialog(
            sys_id=state.sys_id,
            shelfmark=state.current_page.shelfmark,
            lists_mgr=app_state.lists_mgr,
            note_default='',  # Empty by default
            fl_id=None
        )
        print(f"[DEBUG] Dialog should have opened")

    def add_page_to_list():
        """Add specific page/image to a list."""
        print(f"[DEBUG] add_page_to_list called")
        print(f"[DEBUG] state.sys_id = {state.sys_id}")
        print(f"[DEBUG] state.current_page = {state.current_page}")
        if not state.sys_id or not state.current_page:
            ui.notify(tr('Please load a manuscript first'), type='warning')
            return

        from web.state import state as app_state
        from web.components import show_add_to_list_dialog
        print(f"[DEBUG] app_state.lists_mgr = {app_state.lists_mgr}")
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        # Use FL ID if available for specific page reference
        fl_id = state.current_page.fl_id

        print(f"[DEBUG] Opening dialog for sys_id={state.sys_id}, fl_id={fl_id}")
        show_add_to_list_dialog(
            sys_id=state.sys_id,
            shelfmark=f"{state.current_page.shelfmark} - {tr('Page')} {state.current_page.p_num}",
            lists_mgr=app_state.lists_mgr,
            note_default='',  # Empty by default (user requested)
            fl_id=fl_id
        )
        print(f"[DEBUG] Dialog function returned")

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
        ui.run_javascript('''
            if(window.manuscriptViewer) window.manuscriptViewer.reset();
            if(window.fsEditViewer) {
                window.fsEditViewer.state = { x: 0, y: 0, scale: 1, rotation: 0, isDragging: false, startX: 0, startY: 0 };
                window.fsEditViewer.applyTransform();
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

        # Show loading in panel instead of notification
        state.edit_loading = True
        state.error_message = None # Clear previous errors
        update_content()

        try:
            user_id = GlobalAuthState.get_user_id()
            if not user_id:
                state.edit_loading = False
                state.error_message = "User not found"
                update_content()
                return

            # Determine status based on role
            status = 'approved' if (GlobalAuthState.is_admin() or GlobalAuthState.is_editor()) else 'pending'

            if state.draft_id:
                # Update existing draft and change status to pending/approved
                result = update_correction(state.draft_id, {
                    'corrected_text': state.edit_text,
                    'notes': state.edit_notes,
                    'status': status
                })
            else:
                # Create new correction
                result = create_correction(
                    author_id=user_id,
                    sys_id=state.current_page.sys_id,
                    shelfmark=state.current_page.shelfmark or '',
                    page_number=state.current_page.p_num,
                    original_text=state.original_edit_text,
                    corrected_text=state.edit_text,
                    notes=state.edit_notes if state.edit_notes else '',
                    status=status
                )

            state.edit_loading = False

            if "error" in result:
                state.error_message = result["error"]
                update_content()
            else:
                state.edit_mode = False
                state.draft_saved = False
                state.draft_id = None

                # Reload page to see changes
                load_page(direction=0)
        except Exception as e:
            state.edit_loading = False
            state.error_message = f"Error submitting: {str(e)}"
            update_content()
            print(f"Submit error: {e}")

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
                status='draft'
            )

        if "error" in result:
            ui.notify(result["error"], type='negative')
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
                        all_corrections = get_corrections(sys_id=state.current_page.sys_id, author_id=user_id)
                        # Filter for THIS page
                        current_p_num = state.current_page.p_num
                        my_corrections = [
                            c for c in all_corrections
                            if c.get('page_number') == current_p_num
                        ]
                        # Sort by created_at desc
                        my_corrections.sort(key=lambda x: x.get('created_at', ''), reverse=True)
                    except Exception as e:
                        print(f"Error fetching corrections: {e}")

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
                    ui.button(tr('Back'), icon='arrow_forward' if is_rtl() else 'arrow_back', on_click=lambda: load_page()).classes('mt-4')
                return

            if not state.current_page:
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
                        on_click=lambda: navigate_shelfmark(-1)
                    ).props('flat round').style('color: white !important;').tooltip(tr('Previous manuscript'))

                    # Shelfmark and Title
                    with ui.row().classes('flex-1 items-center justify-center gap-4'):
                        # Shelfmark - H2
                        h2(page.shelfmark or f"ID: {page.sys_id}", classes='text-xl font-bold', style='color: #ffffff !important; text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);')

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

                        # Title (truncated with tooltip)
                        if page.title:
                            words = page.title.split()
                            if len(words) > 5:
                                short_title = ' '.join(words[:5]) + '...'
                                ui.label(short_title).classes(
                                    'rtl-text hebrew-text'
                                ).style(
                                    'color: #ffffff !important; '
                                    'opacity: 0.95;'
                                ).tooltip(page.title)
                            else:
                                ui.label(page.title).classes(
                                    'rtl-text hebrew-text'
                                ).style(
                                    'color: #ffffff !important; '
                                    'opacity: 0.95;'
                                )

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
                        ui.button(
                            icon='star_border',
                            on_click=add_manuscript_to_list
                        ).props(f'flat round dense aria-label="{tr("Add to List")}"').style('color: #ffffff !important;').tooltip(tr('Add to List'))

                    # Next Shelfmark Button
                    ui.button(
                        icon='skip_previous' if is_rtl() else 'skip_next',
                        on_click=lambda: navigate_shelfmark(1)
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
                        # Shelfmark
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Shelfmark')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(page.shelfmark or 'N/A').classes('text-sm').style('color: var(--text-primary);')

                        # System ID
                        with ui.column().classes('gap-1'):
                            ui.label(tr('System ID')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(page.sys_id).classes('text-sm font-mono').style('color: var(--text-primary);')

                        # Title
                        if page.title:
                            with ui.column().classes('gap-1 col-span-2'):
                                ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(page.title).classes('text-sm rtl-text hebrew-text').style('color: var(--text-primary);')

                        # Total Pages
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Pages')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(str(page.total_pages)).classes('text-sm').style('color: var(--text-primary);')

                        # FL ID (if available)
                        if page.fl_id:
                            with ui.column().classes('gap-1'):
                                ui.label('FL ID').classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(f'FL{page.fl_id}').classes('text-sm font-mono').style('color: var(--text-primary);')

                        # Oxford Metadata (Part Title, Contents, Provenance)
                        if page.oxford_part_metadata:
                            # Part Title
                            if page.oxford_part_metadata.get('title'):
                                with ui.column().classes('gap-1 col-span-2'):
                                    ui.label(tr('Part Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(page.oxford_part_metadata['title']).classes('text-sm').style('color: var(--text-primary);')
                            
                            # Contents
                            if page.oxford_part_metadata.get('contents'):
                                with ui.column().classes('gap-1 col-span-2'):
                                    ui.label(tr('Contents')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(page.oxford_part_metadata['contents']).classes('text-sm').style('color: var(--text-primary);')

                            # Provenance
                            if page.oxford_part_metadata.get('provenance'):
                                with ui.column().classes('gap-1 col-span-2'):
                                    ui.label(tr('Provenance')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(page.oxford_part_metadata['provenance']).classes('text-sm').style('color: var(--text-primary);')

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
                if not is_oxford and page.shelfmark:
                    shelfmark_lower = page.shelfmark.lower()
                    # Oxford shelfmarks: "MS heb. f.21/21", "MS. Heb. a. 1", etc.
                    if shelfmark_lower.startswith('ms heb') or shelfmark_lower.startswith('ms. heb'):
                        is_oxford = True


                # Choose image endpoint based on source
                # Prioritize page-specific fl_id over sys_id for correct page images
                # Add cache-buster to force image refresh on page navigation
                cache_bust = f"&_cb={page.p_num}" if page.p_num else ""

                # Page index (0-based) for multi-page manuscripts
                page_idx = max(0, page.p_num - 1)

                # NLI IIIF base URL for direct browser access (bypasses server blocking)
                NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"

                if is_oxford and page.sys_id:
                    has_image = True
                    # Oxford images - use proxy with proper page parameter
                    # The API will fetch the correct recto/verso based on page_idx
                    img_url = f"/api/oxford_image/{page.sys_id}?page={page_idx}{cache_bust}"
                    fallback_url = None
                elif page.sys_id:
                    # Use server-side NLI proxy for ALL NLI items
                    # This works reliably for all collections (Cambridge, Russian, etc.)
                    # Direct browser requests to NLI are blocked for some collections
                    has_image = True
                    img_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}{cache_bust}"
                    fallback_url = None


                # Header bar with folio info, navigation, controls
                with ui.card().classes('w-full mb-2').style('background: var(--bg-tertiary);'):
                    with ui.row().classes('w-full items-center justify-between p-3'):
                        with ui.row().classes('items-center gap-4'):
                            # Folio/Page info
                            folio = extract_folio_number(page.full_header)
                            if folio:
                                # Changed to H2
                                h2(f"{tr('Folio')} {folio}", classes='font-bold text-lg')
                            else:
                                # Changed to H2
                                h2(f"{tr('Page')} {page.p_num}", classes='font-bold text-lg')

                            # Source badge - default to V0.8 unless explicitly V0.7
                            source_class = get_source_badge_class(page.full_header)
                            source_text = 'V0.7' if 'V0.7' in page.full_header else 'V0.8'
                            ui.label(source_text).classes(f'source-badge {source_class}')

                        # Navigation and controls
                        with ui.row().classes('items-center gap-2'):
                            # Previous page button (arrow pointing back)
                            prev_disabled = page.current_idx <= 1
                            ui.button(
                                icon='chevron_right' if is_rtl() else 'chevron_left',
                                on_click=lambda: load_page(direction=-1)
                            ).props(f'flat round dense {"disabled" if prev_disabled else ""} data-action="prev" aria-label="{tr("Previous Page")}"').classes(
                                'text-green-700' if not prev_disabled else 'text-gray-300'
                            )

                            # Page input
                            page_input = ui.number(
                                value=page.p_num,
                                min=1,
                                max=page.total_pages
                            ).classes('w-16').props('dense outlined')

                            ui.label(f"/ {page.total_pages}").classes('text-sm').style('color: var(--text-secondary);')

                            # Go button
                            def handle_go_click():
                                try:
                                    page_num = int(page_input.value) if page_input.value is not None else 1
                                    go_to_page(page_num)
                                except (ValueError, TypeError):
                                    go_to_page(1)

                            ui.button(
                                tr('Go'),
                                on_click=handle_go_click
                            ).props('flat dense color=green')

                            # Next page button (arrow pointing forward)
                            next_disabled = page.current_idx >= page.total_pages
                            ui.button(
                                icon='chevron_left' if is_rtl() else 'chevron_right',
                                on_click=lambda: load_page(direction=1)
                            ).props(f'flat round dense {"disabled" if next_disabled else ""} data-action="next" aria-label="{tr("Next Page")}"').classes(
                                'text-green-700' if not next_disabled else 'text-gray-300'
                            )

                            # Show Full Manuscript button
                            ui.button(
                                tr('Hide Full Manuscript') if state.view_all else tr('Show Full Manuscript'),
                                icon='view_agenda' if not state.view_all else 'view_day',
                                on_click=toggle_view_all
                            ).props(f'flat dense color=green aria-label="{tr("Hide Full Manuscript") if state.view_all else tr("Show Full Manuscript")}"')

                            # Add page to list (star button)
                            ui.button(
                                icon='star_border',
                                on_click=add_page_to_list
                            ).props(f'flat round dense aria-label="{tr("Add to List")}"').classes('text-green-700').tooltip(tr('Add to List'))

                            # Image toggle button - placeholder, will be connected later
                            image_toggle_btn = None
                            if has_image:
                                image_toggle_btn = ui.button(
                                    icon='image'
                                ).props('flat dense').classes('text-green-700').tooltip(tr('Toggle Image'))

                            # Edit, Comment, Notes, and Joins buttons
                            if page.text:
                                from web.components import create_edit_button, create_comment_button, create_version_selector, create_notes_button, create_joins_button

                                # Refresh callback to reload page after edits/comments
                                def refresh_page():
                                    load_page(direction=0)

                                # Navigation callback for joins
                                def navigate_to_shelfmark(target_shelfmark: str):
                                    state.shelfmark_query = target_shelfmark
                                    search_shelfmark()

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
                                    on_submit=refresh_notes_after_comment
                                )
                                create_notes_button(
                                    document_id=page.sys_id,
                                    page_number=page.p_num,
                                    shelfmark=page.shelfmark or page.sys_id
                                )

                                # Joins button - show connected fragments
                                if page.shelfmark:
                                    create_joins_button(
                                        shelfmark=page.shelfmark,
                                        document_id=page.sys_id,
                                        on_navigate=navigate_to_shelfmark
                                    )

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
                                    ).props(f'dark dense aria-label="{tr("Rotation")}"').classes('w-32 mx-2').style('transition: none;').on(
                                        'update:model-value', 
                                        'if(window.manuscriptViewer) window.manuscriptViewer.update(window.manuscriptViewer.state.scale, $event)'
                                    )
                                    
                                    ui.button(icon='rotate_right', on_click=rotate_right).props(f'flat round size=sm text-color=white aria-label="{tr("Rotate Right")}"').tooltip(tr('Rotate Right'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='restart_alt', on_click=zoom_reset).props(f'flat round size=sm text-color=white aria-label="{tr("Reset View")}"').tooltip(tr('Reset View'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='fullscreen', on_click=toggle_image_fullscreen).props(f'flat round size=sm text-color=white aria-label="{tr("Fullscreen Image")}" data-action="fullscreen"').tooltip(tr('Fullscreen Image'))


                            # Image display area - using div instead of scroll_area for drag support
                            with ui.element('div').classes('image-container w-full').style(
                                'background: #1a1a1a; height: calc(60vh - 100px); overflow: hidden; position: relative;'
                            ):
                                with ui.element('div').style(
                                    'display: flex; align-items: center; justify-content: center; width: 100%; height: 100%;'
                                ):
                                    safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                    safe_sys_id = (page.sys_id or '').replace("'", "\\'").replace('"', '\\"')

                                    is_oxford_js = 'true' if is_oxford else 'false'
                                    img_html = f'''
                                    <img
                                        src="{safe_img_url}"
                                        class="zoomable-image"
                                        style="transform: translate(0px, 0px) rotate({state.rotation}deg) scale({state.zoom_level}); cursor: grab;"
                                        loading="lazy"
                                        draggable="false"
                                        onload="if(window.manuscriptViewer) window.manuscriptViewer.init()"
                                        onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js})"
                                    />
                                    '''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('if(window.manuscriptViewer) setTimeout(() => window.manuscriptViewer.init(), 100);') 

                            # === Image Credit/Attribution Footer ===
                            if page.attribution:
                                with ui.row().classes('w-full items-center justify-center gap-2 py-2').style(
                                    'background: #2a2a2a; border-radius: 0 0 8px 8px; border-top: 1px solid #333;'
                                ):
                                    ui.icon('photo_library', size='xs').style('color: #888; font-size: 14px;')
                                    credit_text = page.attribution
                                    # Make it a link for Oxford
                                    if page.is_oxford:
                                        with ui.link(target='https://digital.bodleian.ox.ac.uk/', new_tab=True).style('text-decoration: none;'):
                                            ui.label(credit_text).classes('text-xs').style(
                                                'color: #aaa; font-style: italic;'
                                            )
                                    else:
                                        # NLI - link to ktiv
                                        with ui.link(target=f'https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}', new_tab=True).style('text-decoration: none;'):
                                            ui.label(credit_text).classes('text-xs').style(
                                                'color: #aaa; font-style: italic;'
                                            )

                    # === RIGHT PANEL: Transcription ===
                    text_panel_flex = 'flex: 1 1 auto;' if has_image else 'flex: 1 1 100%;'
                    
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
                                                    'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap;'
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
                                if source == 'user' and author:
                                    ui.notify(f"{tr('Showing version by')} {author}", type='info')
                                elif source in ('V0.7', 'V0.8'):
                                    ui.notify(f"{tr('Showing')} {source}", type='info')

                            # Version selector
                            if page.text:
                                with ui.row().classes('items-center p-2 border-b'):
                                    create_version_selector(
                                        document_id=page.sys_id,
                                        page_number=page.p_num,
                                        original_text=page.text,
                                        on_version_change=handle_version_change
                                    )

                            # Initial render
                            render_text_content(page.text if page.text else None)

                # Comments section - below panels
                from web.components import create_notes_panel
                _, notes_refresh_fn = create_notes_panel(
                    document_id=page.sys_id,
                    page_number=page.p_num,
                    shelfmark=page.shelfmark or page.sys_id
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

                                # Image display area
                                with ui.element('div').classes('fullscreen-edit-image').props('id="fs-image-panel"'):
                                    if has_image and img_url:
                                        safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                        safe_sys_id = (page.sys_id or '').replace("'", "\\'").replace('"', '\\"')
                                        is_oxford_js = 'true' if is_oxford else 'false'
                                        img_html = f'<img src="{safe_img_url}" class="zoomable-image" id="fs-zoomable-image" style="transform: translate(0px, 0px) rotate({state.rotation}deg) scale({state.zoom_level}); cursor: grab;" loading="lazy" draggable="false" onerror="handleImageError(this, \'{safe_sys_id}\', {page_idx}, {is_oxford_js})" />'
                                        ui.html(img_html, sanitize=False)
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
                                            state: { x: 0, y: 0, scale: 1, rotation: 0, isDragging: false, startX: 0, startY: 0 },

                                            applyTransform: function() {
                                                this.el.style.transform = `translate(${this.state.x}px, ${this.state.y}px) rotate(${this.state.rotation}deg) scale(${this.state.scale})`;
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

    def set_shelfmark_and_search(shelfmark: str):
        """Set shelfmark and trigger search."""
        state.shelfmark_query = shelfmark
        search_shelfmark()

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
                    label=tr('Enter shelfmark')
                ).classes('flex-1').props('outlined dense clearable color=green')

                # Store reference for updates from other functions (e.g. suggestion dialog)
                slider_refs['search_input'] = search_input

                # Set initial value if we have one
                if state.shelfmark_query:
                    search_input.value = state.shelfmark_query

                def do_search():
                    state.shelfmark_query = search_input.value or ''
                    state.search_error = None  # Clear previous error
                    if state.shelfmark_query.strip():
                        search_shelfmark()

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

        # Load initial page if sys_id or fl_id provided
        if initial_fl_id_value:
            # Load by FL ID
            state.is_loading = True
            update_content()
            try:
                page = service.get_browse_page_by_fl(initial_fl_id_value, sys_id=initial_sys_id)
                if page:
                    state.sys_id = page.sys_id
                    state.current_page = page
                    state.page_input_value = page.p_num
                    state.error = None
                else:
                    state.error = tr('No text available') + f" (fl_id: {initial_fl_id_value})"
            except Exception as e:
                state.error = f"{tr('Error')}: {str(e)}"
            finally:
                state.is_loading = False
                update_content()
        elif initial_sys_id:
            load_page(p_num=initial_page)
        else:
            # Try to restore previous position
            saved_position = app.storage.user.get('browse_position')
            if saved_position and saved_position.get('sys_id'):
                state.sys_id = saved_position['sys_id']
                state.shelfmark_query = saved_position.get('shelfmark', '')
                load_page(p_num=saved_position.get('p_num', 1))
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
