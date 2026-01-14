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

from nicegui import ui
from typing import Optional
import re

from web.services import get_service, BrowsePage, get_thumbnail_url, get_full_image_url
from web.translations import tr, is_rtl


# ============================================================================
# Custom Styles for Manuscript Viewer
# ============================================================================

VIEWER_STYLES = '''
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
        max-width: none;
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
        line-height: 2.2;
        font-size: 1.2rem;
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

    /* Collapsible panels for mobile */
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


def create_browse_page(initial_sys_id: Optional[str] = None, highlight: Optional[str] = None):
    """Create the professional manuscript viewer page UI."""
    state = BrowseState()
    service = get_service()

    # UI component references
    content_container = None
    image_element = None
    viewer_container = None

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
        """Apply highlighting to search terms in text."""
        if not state.highlight_terms or not text:
            return text

        # Split highlight terms and escape for HTML
        terms = state.highlight_terms.split()
        highlighted = text

        for term in terms:
            # Simple case-insensitive replacement
            pattern = re.compile(re.escape(term), re.IGNORECASE)
            highlighted = pattern.sub(
                f'<span class="highlight-term">{term}</span>',
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
        # Try to extract folio info like "1r", "2v", etc.
        match = re.search(r'(\d+[rv]?)', full_header)
        if match:
            return match.group(1)
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

            # === Metadata Header ===
            with ui.element('div').classes('metadata-header'):
                with ui.row().classes('w-full items-start justify-between'):
                    with ui.column().classes('flex-1'):
                        # Shelfmark (prominent)
                        ui.label(page.shelfmark or f"ID: {page.sys_id}").classes('shelfmark-title')

                        # Metadata row
                        with ui.element('div').classes('metadata-row'):
                            if page.title:
                                with ui.element('span').classes('metadata-item'):
                                    ui.icon('description', size='sm')
                                    ui.label(page.title).classes('rtl-text hebrew-text')

                            # Page info
                            with ui.element('span').classes('metadata-item'):
                                ui.icon('layers', size='sm')
                                ui.label(f"{page.p_num} / {page.total_pages} {tr('pages')}")

                    # External links
                    with ui.column().classes('items-end gap-2'):
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
                        with ui.link(target=ktiv_url, new_tab=True).classes(
                            'flex items-center gap-2 px-4 py-2 rounded-lg transition-all'
                        ).style('text-decoration: none; color: white; background: rgba(255, 255, 255, 0.25); border: 2px solid rgba(255, 255, 255, 0.5); backdrop-filter: blur(4px);'):
                            ui.icon('open_in_new', size='sm')
                            ui.label(tr('Open in Ktiv')).classes('font-semibold')

            # === Navigation Bar ===
            with ui.element('div').classes('navigation-bar'):
                with ui.row().classes('w-full items-center justify-between'):
                    # Previous button
                    prev_disabled = page.current_idx <= 1
                    ui.button(
                        icon='chevron_right' if is_rtl() else 'chevron_left',
                        on_click=lambda: load_page(direction=-1)
                    ).props(f'flat round {"disabled" if prev_disabled else ""}').classes(
                        'text-green-700' if not prev_disabled else 'text-gray-300'
                    )

                    # Page selector
                    with ui.row().classes('items-center gap-3'):
                        ui.label(tr('Page')).classes('text-gray-600')

                        page_input = ui.number(
                            value=page.p_num,
                            min=1,
                            max=page.total_pages
                        ).classes('w-20').props('dense outlined')

                        ui.label(f"{tr('of')} {page.total_pages}").classes('text-gray-600')

                        def handle_go_click():
                            go_to_page(int(page_input.value))

                        ui.button(
                            tr('Go'),
                            on_click=handle_go_click
                        ).props('flat dense color=green')

                    # Next button
                    next_disabled = page.current_idx >= page.total_pages
                    ui.button(
                        icon='chevron_left' if is_rtl() else 'chevron_right',
                        on_click=lambda: load_page(direction=1)
                    ).props(f'flat round {"disabled" if next_disabled else ""}').classes(
                        'text-green-700' if not next_disabled else 'text-gray-300'
                    )

                    # Keyboard shortcuts hint
                    with ui.element('div').classes('shortcuts-hint hidden lg:block'):
                        ui.html(f'''
                            <span class="kbd">←</span> <span class="kbd">→</span> {tr('Navigate')} |
                            <span class="kbd">+</span> <span class="kbd">-</span> {tr('Zoom')} |
                            <span class="kbd">F</span> {tr('Fullscreen')}
                        ''', sanitize=False)

            # === Main Viewer Panels ===
            fullscreen_class = 'fullscreen-mode' if state.is_fullscreen else ''

            with ui.row().classes(f'w-full gap-4 viewer-panels {fullscreen_class}').style('display: flex; flex-direction: row;'):
                # LEFT: Image Viewer (60%)
                with ui.column().classes('image-panel').style('width: 60%;'):
                    with ui.element('div').classes('image-viewer-container'):
                        # Image container
                        with ui.element('div').classes('image-container'):
                            # Determine image URL with fallback logic
                            img_url = None
                            fallback_url = None
                            if page.image_url and page.image_url.strip():
                                img_url = page.image_url
                            elif page.fl_id:
                                digits = re.sub(r"\D", "", str(page.fl_id))
                                if digits:
                                    img_url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/max/0/default.jpg"
                                    # Prepare Rosetta fallback URL
                                    fallback_url = f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=stream&dps_pid=FL{digits}"

                            if img_url:
                                # Add JavaScript to handle image load errors with fallback
                                if fallback_url:
                                    ui.add_head_html(f'''
                                    <script>
                                    function handleImageError(img) {{
                                        if (img.src !== '{fallback_url}') {{
                                            console.log('IIIF image failed, trying Rosetta fallback');
                                            img.src = '{fallback_url}';
                                        }} else {{
                                            console.log('Rosetta fallback also failed');
                                            img.style.display = 'none';
                                            const parent = img.parentElement;
                                            if (parent) {{
                                                parent.innerHTML = '<div style="text-align: center; color: #888;"><i class="material-icons" style="font-size: 4rem;">image_not_supported</i><p>{tr("Image not available")}</p></div>';
                                            }}
                                        }}
                                    }}
                                    </script>
                                    ''')
                                    ui.image(img_url).classes(
                                        'zoomable-image'
                                    ).style(
                                        f'transform: scale({state.zoom_level}); transform-origin: center;'
                                    ).props('loading="lazy" onerror="handleImageError(this)"')
                                else:
                                    ui.image(img_url).classes(
                                        'zoomable-image'
                                    ).style(
                                        f'transform: scale({state.zoom_level}); transform-origin: center;'
                                    ).props('loading="lazy"')
                            else:
                                with ui.element('div').classes('image-loading'):
                                    ui.icon('image_not_supported', size='4rem')
                                    ui.label(tr('Image not available')).classes('mt-2')

                        # Image controls overlay
                        with ui.element('div').classes('image-controls'):
                            ui.button(icon='remove', on_click=zoom_out).props(
                                'flat round size=sm'
                            ).tooltip(tr('Zoom out'))

                            ui.label(f'{int(state.zoom_level * 100)}%').classes(
                                'zoom-level-label text-white text-sm px-2'
                            )

                            ui.button(icon='add', on_click=zoom_in).props(
                                'flat round size=sm'
                            ).tooltip(tr('Zoom in'))

                            ui.separator().props('vertical').classes('mx-1')

                            ui.button(icon='fit_screen', on_click=zoom_reset).props(
                                'flat round size=sm'
                            ).tooltip(tr('Reset zoom'))

                            ui.button(icon='width_full', on_click=fit_width).props(
                                'flat round size=sm'
                            ).tooltip(tr('Fit to width'))

                            ui.button(icon='height', on_click=fit_height).props(
                                'flat round size=sm'
                            ).tooltip(tr('Fit to height'))

                            ui.separator().props('vertical').classes('mx-1')

                            ui.button(
                                icon='fullscreen_exit' if state.is_fullscreen else 'fullscreen',
                                on_click=toggle_fullscreen
                            ).props('flat round size=sm').tooltip(
                                tr('Exit fullscreen') if state.is_fullscreen else tr('Fullscreen')
                            )

                # RIGHT: Transcription Panel (40%)
                with ui.column().classes('transcription-panel-wrapper').style('width: 40%;'):
                    with ui.element('div').classes('transcription-panel'):
                        # Header with folio and source
                        with ui.element('div').classes('transcription-header'):
                            with ui.row().classes('w-full items-center justify-between'):
                                # Folio number
                                folio = extract_folio_number(page.full_header)
                                if folio:
                                    ui.label(f"{tr('Folio')} {folio}").classes(
                                        'font-semibold text-gray-700'
                                    )
                                else:
                                    ui.label(f"{tr('Page')} {page.p_num}").classes(
                                        'font-semibold text-gray-700'
                                    )

                                # Source badge
                                source_class = get_source_badge_class(page.full_header)
                                source_text = 'V0.8' if 'V0.8' in page.full_header else 'V0.7'
                                ui.label(source_text).classes(
                                    f'source-badge {source_class}'
                                )

                        # Transcription content
                        with ui.scroll_area().classes('transcription-content'):
                            if page.text:
                                # Apply highlighting if we have search terms
                                display_text = page.text
                                if state.highlight_terms:
                                    display_text = highlight_text(page.text)
                                    ui.html(f'<div class="transcription-text">{display_text}</div>', sanitize=False)
                                else:
                                    ui.label(page.text).classes('transcription-text')
                            else:
                                with ui.column().classes('items-center justify-center h-full'):
                                    ui.icon('text_snippet', size='3rem').classes('text-gray-300')
                                    ui.label(tr('No text available')).classes(
                                        'text-gray-400 mt-2'
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

        # Shelfmark Search Box
        with ui.card().classes('w-full p-4 mb-6 bg-green-50 border border-green-200'):
            with ui.row().classes('w-full gap-4 items-end'):
                # Search icon
                ui.icon('search', size='md').classes('text-green-600 mb-2')

                # Shelfmark input
                search_input = ui.input(
                    placeholder=tr('e.g. T-S 8J6.1'),
                    label=tr('Enter shelfmark'),
                    value=state.shelfmark_query
                ).classes('flex-1').props('outlined dense clearable color=green')

                search_input.bind_value(state, 'shelfmark_query')
                search_input.on('keydown.enter', search_shelfmark)

                # Go button
                ui.button(
                    tr('Go'),
                    icon='arrow_forward',
                    on_click=search_shelfmark
                ).props('color=green').classes('px-6')

        # Service status warning
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 border border-yellow-300 mb-4'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')

        # Main content container
        content_container = ui.column().classes('w-full')

        # Load initial page if sys_id provided
        if initial_sys_id:
            load_page()
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
