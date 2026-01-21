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


# ============================================================================
# Custom Styles for Manuscript Viewer
# ============================================================================

VIEWER_STYLES = '''
<script>
// Global function for handling image errors with fallback
function handleImageError(img, fallbackUrl) {
    if (fallbackUrl && img.src !== fallbackUrl) {
        console.log('Primary image failed, trying fallback:', fallbackUrl);
        img.src = fallbackUrl;
    } else {
        console.log('All image sources failed for:', img.src);
        img.style.display = 'none';
        const parent = img.parentElement;
        if (parent) {
            parent.innerHTML = '<div style="text-align: center; color: #888;"><i class="material-icons" style="font-size: 4rem;">image_not_supported</i><p>Image not available</p></div>';
        }
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
        overflow: auto;
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
    }

    .fullscreen-mode .image-container {
        height: 100vh !important;
    }
</style>
'''


class BrowseState:
    """Holds the state for the browse page."""

    def __init__(self):
        self.shelfmark_query: str = ''
        self.current_page: Optional[BrowsePage] = None
        self.sys_id: Optional[str] = None
        self.is_loading: bool = False
        self.error: Optional[str] = None
        self.zoom_level: float = 1.0
        self.is_fullscreen: bool = False
        self.highlight_terms: Optional[str] = None
        self.page_input_value: int = 1
        self.view_all: bool = False
        self.full_manuscript: List[DocumentPage] = []


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

    if initial_sys_id:
        state.sys_id = initial_sys_id
    if highlight:
        state.highlight_terms = highlight

    # Add custom styles
    ui.add_head_html(VIEWER_STYLES)

    def search_shelfmark():
        """Search for manuscripts by shelfmark."""
        if not state.shelfmark_query.strip():
            return

        state.is_loading = True
        state.error = None
        update_content()

        try:
            results = service.search_by_shelfmark(state.shelfmark_query.strip(), limit=20)

            if results:
                state.sys_id = results[0].sys_id
                load_page()
            else:
                state.error = tr('No manuscript found')
                state.is_loading = False
                update_content()

        except Exception as e:
            state.error = str(e)
            state.is_loading = False
            update_content()

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
                            app_state.lists_mgr.add_to_recent(state.sys_id, fl_id=page.fl_id)
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
        if not state.sys_id or not state.current_page:
            return

        from web.state import state as app_state
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            ui.label(tr('Add to List')).classes('text-xl font-bold mb-2')
            ui.label(f"{tr('Item')}: {state.current_page.shelfmark}").style('color: var(--text-secondary);')

            lists = app_state.lists_mgr.data.get('lists', {})
            list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

            if list_options:
                selected_list = ui.select(list_options, label=tr('Select List')).classes('w-full mt-4').props('outlined').style('color: var(--text-primary);')
                note_input = ui.input(label=tr('Note (optional)')).classes('w-full mt-2').props('outlined')

                def do_add():
                    if app_state.lists_mgr.add_item(state.sys_id, selected_list.value, note=note_input.value):
                        ui.notify(tr('Added to list'), type='positive')
                        dialog.close()
                    else:
                        ui.notify(tr('Already in list'), type='info')

                with ui.row().classes('w-full justify-end gap-2 mt-6'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                    ui.button(tr('Add'), on_click=do_add).classes('btn-primary')
            else:
                ui.label(tr('No lists available. Create a list first.')).style('color: var(--text-muted);')
                ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('btn-primary mt-4')

        dialog.open()

    def add_page_to_list():
        """Add specific page/image to a list."""
        if not state.sys_id or not state.current_page:
            return

        from web.state import state as app_state
        if not app_state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return

        # Use FL ID if available for specific page reference
        fl_id = state.current_page.fl_id
        note_text = f"Page {state.current_page.p_num}"
        if fl_id:
            note_text += f" (FL{fl_id})"

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            ui.label(tr('Add to List')).classes('text-xl font-bold mb-2')
            ui.label(f"{tr('Item')}: {state.current_page.shelfmark} - {tr('Page')} {state.current_page.p_num}").style('color: var(--text-secondary);')

            lists = app_state.lists_mgr.data.get('lists', {})
            list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

            if list_options:
                selected_list = ui.select(list_options, label=tr('Select List')).classes('w-full mt-4').props('outlined').style('color: var(--text-primary);')
                note_input = ui.input(label=tr('Note (optional)'), value=note_text).classes('w-full mt-2').props('outlined')

                def do_add():
                    # Add with FL ID if available
                    if app_state.lists_mgr.add_item(state.sys_id, selected_list.value, note=note_input.value, fl_id=fl_id):
                        ui.notify(tr('Added to list'), type='positive')
                        dialog.close()
                    else:
                        ui.notify(tr('Already in list'), type='info')

                with ui.row().classes('w-full justify-end gap-2 mt-6'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                    ui.button(tr('Add'), on_click=do_add).classes('btn-primary')
            else:
                ui.label(tr('No lists available. Create a list first.')).style('color: var(--text-muted);')
                ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('btn-primary mt-4')

        dialog.open()

    def zoom_in():
        """Increase zoom level."""
        state.zoom_level = min(state.zoom_level + 0.25, 4.0)
        update_image_zoom()

    def zoom_out():
        """Decrease zoom level."""
        state.zoom_level = max(state.zoom_level - 0.25, 0.25)
        update_image_zoom()

    def zoom_reset():
        """Reset zoom to 100%."""
        state.zoom_level = 1.0
        update_image_zoom()

    def fit_width():
        """Fit image to container width."""
        state.zoom_level = 1.0
        update_image_zoom()

    def fit_height():
        """Fit image to container height."""
        state.zoom_level = 0.9
        update_image_zoom()

    def toggle_fullscreen():
        """Toggle fullscreen mode."""
        state.is_fullscreen = not state.is_fullscreen
        update_content()

    def update_image_zoom():
        """Update the image zoom transform via JavaScript."""
        zoom_percent = int(state.zoom_level * 100)
        ui.run_javascript(f'''
            const img = document.querySelector('.zoomable-image');
            if (img) {{
                img.style.transform = 'scale({state.zoom_level})';
            }}
            const zoomLabel = document.querySelector('.zoom-level-label');
            if (zoomLabel) {{
                zoomLabel.textContent = '{zoom_percent}%';
            }}
        ''')

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
                    ui.label(tr('Loading...')).classes('ml-3 text-lg text-gray-600')
                return

            if state.error and not state.current_page:
                with ui.card().classes('w-full p-8 text-center'):
                    ui.icon('error_outline', size='4rem').classes('text-red-400')
                    ui.label(state.error).classes('text-red-600 mt-4 text-lg')
                    ui.button(tr('Back'), icon='arrow_back', on_click=lambda: load_page()).classes('mt-4')
                return

            if not state.current_page:
                # Show welcome/search prompt
                with ui.column().classes('w-full items-center py-16'):
                    ui.icon('auto_stories', size='6rem').classes('text-green-400')
                    ui.label(tr('Enter a shelfmark to browse the manuscript')).classes(
                        'text-gray-600 mt-6 text-xl rtl-text hebrew-text'
                    )
                    with ui.column().classes('mt-8 text-center'):
                        ui.label(tr('Examples')).classes('text-gray-500 text-sm mb-2')
                        with ui.row().classes('gap-2'):
                            for example in ['T-S 8J6.1', 'T-S 13J2.5', 'T-S AS 145.295']:
                                ui.button(
                                    example,
                                    on_click=lambda e=example: set_shelfmark_and_search(e)
                                ).props('flat dense').classes('text-green-700')
                return

            page = state.current_page

            # === Compact Metadata Header ===
            with ui.card().classes('w-full p-3 mb-3').style(
                'background: linear-gradient(135deg, #15803d 0%, #166534 100%) !important; '
                'border: none;'
            ):
                with ui.row().classes('w-full items-center justify-between'):
                    # Prev Shelfmark Button
                    ui.button(
                        icon='skip_previous',
                        on_click=lambda: navigate_shelfmark(-1)
                    ).props('flat round').style('color: white !important;').tooltip(tr('Previous manuscript'))

                    # Shelfmark and Title
                    with ui.row().classes('flex-1 items-center justify-center gap-4'):
                        # Shelfmark
                        ui.label(page.shelfmark or f"ID: {page.sys_id}").classes(
                            'text-xl font-bold'
                        ).style(
                            'color: #ffffff !important; '
                            'text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);'
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
                        ).props('flat dense').style(
                            'color: #ffffff !important; '
                            'background: rgba(255, 255, 255, 0.15);'
                        ).tooltip(tr('Search for Parallels'))

                        # Metadata button
                        ui.button(
                            tr('Hide Metadata') if show_metadata['value'] else tr('Show Metadata'),
                            icon='info',
                            on_click=toggle_metadata
                        ).props('flat dense').style(
                            'color: #ffffff !important; '
                            'background: rgba(255, 255, 255, 0.15);'
                        ).tooltip(tr('Show Metadata'))

                        # Add manuscript to list (star button)
                        ui.button(
                            icon='star_border',
                            on_click=add_manuscript_to_list
                        ).props('flat round dense').style('color: #ffffff !important;').tooltip(tr('Add to Favorites'))

                    # Next Shelfmark Button
                    ui.button(
                        icon='skip_next',
                        on_click=lambda: navigate_shelfmark(1)
                    ).props('flat round').style('color: white !important;').tooltip(tr('Next manuscript'))

            # === Action Buttons Row ===
            # Removed - buttons moved to appropriate headers

            # === Metadata Panel (Expandable) ===
            if show_metadata['value']:
                with ui.card().classes('w-full p-4 mb-3').style('background: var(--bg-tertiary); border: 1px solid var(--border-light);'):
                    with ui.row().classes('w-full items-center justify-between mb-3'):
                        ui.label(tr('Metadata')).classes('text-lg font-bold').style('color: var(--text-primary);')
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

                    # External Links
                    ui.separator().classes('my-3')
                    ui.label(tr('External link')).classes('text-xs font-bold mb-2').style('color: var(--text-secondary);')
                    with ui.row().classes('gap-2 flex-wrap'):
                        # NLI Ktiv
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
                        ui.link('NLI Ktiv', ktiv_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                        # Friedberg (if applicable)
                        friedberg_url = f"https://fjms.genizah.org/{page.sys_id}"
                        ui.link('Friedberg', friedberg_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                    # Export
                    ui.separator().classes('my-3')
                    ui.label(tr('Export')).classes('text-xs font-bold mb-2').style('color: var(--text-secondary);')
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
                            ui.label(tr('Full Manuscript View')).classes('font-bold text-lg')
                            ui.label(f"({len(state.full_manuscript)} {tr('pages')})").classes('text-gray-600 ml-2')

                        # Back to single page button
                        ui.button(
                            tr('Back to Page View'),
                            icon='arrow_back',
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
                                ui.label(f"{tr('Page')} {doc_page.p_num}").classes('font-bold text-green-700')
                                if doc_page.full_header:
                                    ui.label(doc_page.full_header).classes('text-xs text-gray-500 font-mono')

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
                                ui.label(tr('No text available')).classes('text-gray-400 italic')
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

                # Detect Oxford manuscripts by shelfmark pattern
                is_oxford = False
                if page.shelfmark:
                    shelfmark_lower = page.shelfmark.lower()
                    # Oxford shelfmarks: "MS heb. f.21/21", "MS. Heb. a. 1", etc.
                    if shelfmark_lower.startswith('ms heb') or shelfmark_lower.startswith('ms. heb'):
                        is_oxford = True

                # Choose image endpoint based on source
                if is_oxford and page.sys_id:
                    has_image = True
                    img_url = f"/api/oxford_image/{page.sys_id}"
                    fallback_url = f"/api/nli_image_by_sysid/{page.sys_id}"  # Fallback to NLI
                elif page.sys_id:
                    has_image = True
                    # Use NLI system ID endpoint - dynamically fetches correct FL IDs from NLI
                    img_url = f"/api/nli_image_by_sysid/{page.sys_id}"
                    fallback_url = None
                elif fl_id:
                    digits = re.sub(r"\D", "", str(fl_id))
                    if digits:
                        has_image = True
                        img_url = f"/api/nli_image/{digits}"
                        fallback_url = None

                # Header bar with folio info, navigation, controls
                with ui.card().classes('w-full mb-2').style('background: var(--bg-tertiary);'):
                    with ui.row().classes('w-full items-center justify-between p-3'):
                        with ui.row().classes('items-center gap-4'):
                            # Folio/Page info
                            folio = extract_folio_number(page.full_header)
                            if folio:
                                ui.label(f"{tr('Folio')} {folio}").classes('font-bold text-lg')
                            else:
                                ui.label(f"{tr('Page')} {page.p_num}").classes('font-bold text-lg')

                            # Source badge - default to V0.8 unless explicitly V0.7
                            source_class = get_source_badge_class(page.full_header)
                            source_text = 'V0.7' if 'V0.7' in page.full_header else 'V0.8'
                            ui.label(source_text).classes(f'source-badge {source_class}')

                        # Navigation and controls
                        with ui.row().classes('items-center gap-2'):
                            # Previous page button (left arrow < for going backwards)
                            prev_disabled = page.current_idx <= 1
                            ui.button(
                                icon='chevron_left',
                                on_click=lambda: load_page(direction=-1)
                            ).props(f'flat round dense {"disabled" if prev_disabled else ""}').classes(
                                'text-green-700' if not prev_disabled else 'text-gray-300'
                            )

                            # Page input
                            page_input = ui.number(
                                value=page.p_num,
                                min=1,
                                max=page.total_pages
                            ).classes('w-16').props('dense outlined')

                            ui.label(f"/ {page.total_pages}").classes('text-gray-600 text-sm')

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

                            # Next page button (right arrow > for going forwards)
                            next_disabled = page.current_idx >= page.total_pages
                            ui.button(
                                icon='chevron_right',
                                on_click=lambda: load_page(direction=1)
                            ).props(f'flat round dense {"disabled" if next_disabled else ""}').classes(
                                'text-green-700' if not next_disabled else 'text-gray-300'
                            )

                            # Show Full Manuscript button
                            ui.button(
                                tr('Hide Full Manuscript') if state.view_all else tr('Show Full Manuscript'),
                                icon='view_agenda' if not state.view_all else 'view_day',
                                on_click=toggle_view_all
                            ).props('flat dense color=green')

                            # Add page to list (star button)
                            ui.button(
                                icon='star_border',
                                on_click=add_page_to_list
                            ).props('flat round dense').classes('text-green-700').tooltip(tr('Add to Favorites'))

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

                                create_edit_button(
                                    document_id=page.sys_id,
                                    page_number=page.p_num,
                                    original_text=page.text,
                                    shelfmark=page.shelfmark or page.sys_id,
                                    on_save=refresh_page,
                                    image_url=img_url if has_image else None
                                )
                                create_comment_button(
                                    document_id=page.sys_id,
                                    page_number=page.p_num,
                                    shelfmark=page.shelfmark or page.sys_id,
                                    on_submit=refresh_page
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
                                    ui.button(icon='remove', on_click=zoom_out).props('flat round size=sm text-color=white').tooltip(tr('Zoom out'))
                                    ui.label(f'{int(state.zoom_level * 100)}%').classes('zoom-level-label text-white text-sm px-2')
                                    ui.button(icon='add', on_click=zoom_in).props('flat round size=sm text-color=white').tooltip(tr('Zoom in'))
                                    ui.button(icon='fit_screen', on_click=zoom_reset).props('flat round size=sm text-color=white').tooltip(tr('Reset'))

                            # Image display area
                            with ui.scroll_area().classes('w-full').style(
                                'background: #1a1a1a; height: calc(60vh - 60px);'
                            ):
                                with ui.element('div').style(
                                    'display: flex; align-items: center; justify-content: center; min-height: 100%; padding: 16px;'
                                ):
                                    safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                    safe_fallback = fallback_url.replace("'", "\\'").replace('"', '\\"') if fallback_url else ''

                                    img_html = f'''
                                    <img
                                        src="{safe_img_url}"
                                        class="zoomable-image"
                                        style="transform: scale({state.zoom_level}); transform-origin: center; max-width: 100%; max-height: 55vh; object-fit: contain;"
                                        loading="lazy"
                                        onerror="handleImageError(this, {f"'{safe_fallback}'" if safe_fallback else 'null'})"
                                    />
                                    '''
                                    ui.html(img_html, sanitize=False)

                    # === RIGHT PANEL: Transcription ===
                    text_panel_flex = 'flex: 1 1 auto;' if has_image else 'flex: 1 1 100%;'
                    with ui.card().style(f'{text_panel_flex} min-height: 60vh;'):
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
                                            ui.icon('text_snippet', size='4rem').classes('text-gray-300')
                                            ui.label(tr('No text available')).classes('text-gray-400 mt-4 text-xl')
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
                create_notes_panel(
                    document_id=page.sys_id,
                    page_number=page.p_num,
                    shelfmark=page.shelfmark or page.sys_id
                )

    def set_shelfmark_and_search(shelfmark: str):
        """Set shelfmark and trigger search."""
        state.shelfmark_query = shelfmark
        search_shelfmark()

    # === Main Layout ===
    with ui.column().classes('w-full max-w-7xl mx-auto p-4'):
        # Page title
        ui.label(tr('Browse Manuscripts')).classes(
            'text-3xl font-bold mb-6 text-center text-green-800'
        )

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

                # Set initial value if we have one
                if state.shelfmark_query:
                    search_input.value = state.shelfmark_query

                def do_search():
                    state.shelfmark_query = search_input.value or ''
                    if state.shelfmark_query.strip():
                        search_shelfmark()

                search_input.on('keydown.enter', do_search)

                # Go button
                ui.button(
                    tr('Go'),
                    icon='arrow_forward',
                    on_click=do_search
                ).props('color=green').classes('px-6')

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
                // Only if not focused on input
                if (document.activeElement.tagName === 'INPUT') return;

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
