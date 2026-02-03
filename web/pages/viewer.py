from nicegui import ui
from web.state import state
from web.translations import tr
import re
import html

# --- Helpers ---
NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
ROSETTA_BASE = "https://rosetta.nli.org.il/delivery/DeliveryManagerServlet"

def get_image_urls(fl_id):
    """Get primary (Rosetta thumbnail) and fallback (IIIF) image URLs for direct browser loading."""
    if not fl_id:
        return None, None
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return None, None
    # Primary: Rosetta thumbnail (works for all FL IDs)
    primary = f"{ROSETTA_BASE}?dps_func=thumbnail&dps_pid=FL{digits}"
    # Fallback: IIIF full image (works for some FL IDs)
    fallback = f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"
    return primary, fallback

def get_full_image_url(fl_id):
    """Legacy helper - returns IIIF URL."""
    if not fl_id: return None
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits: return None
    return f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"

def format_text_html(text, pattern=None):
    if not text: return ""

    # First escape HTML to prevent XSS
    escaped_text = html.escape(text)

    # 1. Apply Pattern Highlighting if Regex provided
    if pattern:
        try:
            # Escape the pattern for safe matching
            escaped_pattern = html.escape(pattern)
            regex = re.compile(f"({re.escape(escaped_pattern)})", re.IGNORECASE)
            # Replace with a marker first to avoid messing up HTML tags
            escaped_text = regex.sub(r'___HL_START___\1___HL_END___', escaped_text)
        except Exception:
            pass

    # 2. Handle Asterisk Markers (*word*) from Indexer
    # Replace *...* with highlight span (markers are already escaped)
    escaped_text = re.sub(r'\*(.*?)\*', r'___HL_START___\1___HL_END___', escaped_text)

    # 3. HTML formatting
    formatted = escaped_text.replace('\n', '<br>')

    # 4. Apply actual HTML for markers
    formatted = formatted.replace('___HL_START___', '<span class="bg-yellow-200 text-black font-bold px-1 rounded">')
    formatted = formatted.replace('___HL_END___', '</span>')

    return f"<div dir='rtl' class='text-lg leading-loose font-serif text-right'>{formatted}</div>"

# --- Viewer Component ---

def load_result(container, result):
    container.clear()

    # Extract Data
    uid = result.get('uid')
    display = result.get('display', {})
    shelfmark = display.get('shelfmark', 'Unknown')
    title = display.get('title', '')
    sys_id = display.get('id') or state.meta_mgr.extract_unique_id(uid)

    # Extract FL ID for image
    fl_id = None
    if 'raw_header' in result:
        parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
        fl_id = parsed.get('fl_id')

    # Get Full Text (fetch if missing)
    full_text = result.get('full_text') or result.get('text', '')
    snippet = result.get('snippet', '')

    # For search results, prefer to show snippet + link to browse
    # Don't load the full manuscript text as it's too large
    display_text = snippet if snippet else full_text
    is_snippet_only = bool(snippet and not full_text)

    with container:

        # --- Header ---
        with ui.row().classes('w-full items-start justify-between border-b pb-4'):
            with ui.column().classes('gap-1'):
                ui.label(shelfmark).classes('text-xl font-bold text-primary')
                if title:
                    ui.label(title).classes('text-sm text-gray-500')
                with ui.row().classes('gap-2 text-xs text-gray-400'):
                    ui.label(f"System ID: {sys_id}")
                    if fl_id: ui.label(f"FL: {fl_id}")

            with ui.row().classes('gap-2'):
                # Add to list button
                def show_add_to_list_local():
                    from web.components import show_add_to_list_dialog
                    show_add_to_list_dialog(
                        sys_id=sys_id,
                        shelfmark=shelfmark,
                        lists_mgr=state.lists_mgr,
                        note_default='',
                        fl_id=fl_id
                    )

                # Check if item is in any list
                viewer_in_list = state.lists_mgr and sys_id and state.lists_mgr.is_item_in_any_list(sys_id)
                ui.button(
                    icon='star' if viewer_in_list else 'star_border',
                    on_click=show_add_to_list_local
                ).props('flat round dense').tooltip(tr('In List') if viewer_in_list else tr('Add to List'))

                ui.button(icon='download', on_click=lambda: ui.notify('Exporting...')).props('flat round dense')

        # --- Quick Actions ---
        with ui.row().classes('w-full gap-2 mt-4'):
            # Browse button - go to full manuscript viewer with navigation
            # Navigate to specific FL page if available
            if sys_id:
                browse_url = f'/browse?sys_id={sys_id}'
                if fl_id:
                    browse_url += f'&fl_id={fl_id}'
                ui.button(
                    tr('Browse Full Manuscript'),
                    icon='menu_book',
                    on_click=lambda url=browse_url: ui.navigate.to(url)
                ).classes('bg-primary text-white')

        # --- Content Tabs ---
        with ui.tabs().classes('w-full text-primary') as tabs:
            tab_text = ui.tab('Text', icon='description')
            tab_img = ui.tab('Image', icon='image')
            tab_meta = ui.tab('Metadata', icon='info')
            if result.get('source_ctx'):
                tab_src = ui.tab('Source', icon='compare_arrows')

        with ui.tab_panels(tabs, value=tab_text).classes('w-full flex-grow p-0'):

            # 1. Text Panel
            with ui.tab_panel(tab_text).classes('p-4 bg-white'):
                if display_text:
                    # Show snippet with context indicator
                    if is_snippet_only:
                        with ui.card().classes('w-full bg-yellow-50 border-l-4 border-yellow-400 p-3 mb-4'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('info', color='orange')
                                ui.label(tr('Showing match context only')).classes('font-semibold')
                            ui.label(tr('Click "Browse Full Manuscript" to view the complete page with navigation')).classes('text-sm text-gray-600 mt-1')

                    pattern = result.get('highlight_pattern')
                    ui.html(format_text_html(display_text, pattern), sanitize=False).classes('w-full')

                    # Add button to go to full view with specific FL page
                    if is_snippet_only and sys_id:
                        browse_url = f'/browse?sys_id={sys_id}'
                        if fl_id:
                            browse_url += f'&fl_id={fl_id}'
                        ui.button(
                            tr('View Complete Page'),
                            icon='open_in_new',
                            on_click=lambda url=browse_url: ui.navigate.to(url)
                        ).classes('mt-4 bg-primary text-white')
                else:
                    with ui.column().classes('w-full items-center py-8'):
                        ui.icon('text_snippet', size='3rem').classes('text-gray-300')
                        ui.label(tr('No text available')).classes('text-gray-400 mt-2')

            # 2. Image Panel
            with ui.tab_panel(tab_img).classes('p-0 h-full bg-black flex items-center justify-center relative'):
                if fl_id:
                    digits = re.sub(r"\D", "", str(fl_id))

                    with ui.scroll_area().classes('w-full h-full flex items-center justify-center bg-gray-900'):
                        # Direct NLI IIIF URL (browser fetches directly, bypasses server blocking)
                        NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
                        img_url = f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"
                        ui.image(img_url).props('fit=contain').classes('h-[80vh] w-auto max-w-none').style(
                            'max-height: 80vh; object-fit: contain;'
                        )

                    ui.label(tr("NLI Image")).classes('absolute bottom-2 right-2 text-xs text-white/50 bg-black/30 px-2 rounded')
                else:
                    ui.label(tr("No image available")).classes('text-white')

            # 3. Metadata Panel
            with ui.tab_panel(tab_meta).classes('p-4'):
                # Fetch fresh meta
                def load_meta_content():
                    if not sys_id:
                        ui.label("System ID missing.").classes('text-red-500')
                        return

                    meta = state.meta_mgr.enrich_metadata(sys_id)
                    with ui.column().classes('gap-2'):
                        if 'marc' in meta and meta['marc']:
                            marc = meta['marc']
                            for k, v in marc.items():
                                if not v: continue
                                label = k.replace('_', ' ').title()
                                if isinstance(v, str):
                                    ui.markdown(f"**{label}:** {v}")
                                elif isinstance(v, list):
                                    ui.markdown(f"**{label}:** {', '.join(str(x) for x in v)}")
                        else:
                            # Show basic meta if MARC missing
                            ui.markdown(f"**Shelfmark:** {meta.get('shelfmark', 'Unknown')}")
                            ui.markdown(f"**Title:** {meta.get('title', 'Unknown')}")
                            ui.label(tr("No extended bibliographic data available."))

                load_meta_content()

            # 4. Source Panel (Lab)
            if result.get('source_ctx'):
                with ui.tab_panel(tab_src).classes('p-4'):
                    ui.markdown(f"**Source Context:**")
                    ui.html(format_text_html(result['source_ctx'], result.get('highlight_pattern')), sanitize=False)
