from nicegui import ui
from web.state import state
from web.translations import tr
import re

# --- Helpers ---
NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"

def get_full_image_url(fl_id):
    if not fl_id: return None
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits: return None
    return f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"

def format_text_html(text, pattern=None):
    if not text: return ""

    # 1. Apply Pattern Highlighting if Regex provided
    if pattern:
        try:
            # We use a simple replacement for now.
            # In a real app we might want to be more careful with overlapping HTML.
            regex = re.compile(f"({pattern})", re.IGNORECASE)
            # Replace with a marker first to avoid messing up HTML tags
            text = regex.sub(r'___HL_START___\1___HL_END___', text)
        except:
            pass

    # 2. Handle Asterisk Markers (*word*) from Indexer
    # Replace *...* with highlight span
    text = re.sub(r'\*(.*?)\*', r'___HL_START___\1___HL_END___', text)

    # 3. HTML formatting
    html = text.replace('\n', '<br>')

    # 4. Apply actual HTML for markers
    html = html.replace('___HL_START___', '<span class="bg-yellow-200 text-black font-bold px-1 rounded">')
    html = html.replace('___HL_END___', '</span>')

    return f"<div dir='rtl' class='text-lg leading-loose font-serif text-right'>{html}</div>"

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

    # If no full text, try to fetch it
    if not full_text and uid and state.searcher:
        try:
            full_text = state.searcher.get_full_text_by_id(uid)
        except Exception as e:
            print(f"Error fetching text for {uid}: {e}")
            # Fall back to snippet if available
            if snippet:
                full_text = snippet.replace('*', '')  # Remove highlight markers
            else:
                full_text = tr("Error loading text.")

    # If still no text, use snippet
    if not full_text and snippet:
        full_text = snippet.replace('*', '')

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
                ui.button(icon='star_border', on_click=lambda: ui.notify('Add to list implemented in desktop app')).props('flat round dense')
                ui.button(icon='download', on_click=lambda: ui.notify('Exporting...')).props('flat round dense')

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
                pattern = result.get('highlight_pattern')
                ui.html(format_text_html(full_text, pattern)).classes('w-full')

            # 2. Image Panel
            with ui.tab_panel(tab_img).classes('p-0 h-full bg-black flex items-center justify-center relative'):
                if fl_id:
                    img_url = get_full_image_url(fl_id)
                    # Proxy URL
                    proxy_url = f"/api/proxy_image?url={img_url}"

                    with ui.scroll_area().classes('w-full h-full flex items-center justify-center bg-gray-900'):
                        # Use min-h-0 to allow proper flex sizing in scroll area
                        ui.image(proxy_url).props('fit=contain').classes('h-[80vh] w-auto max-w-none')

                    ui.label(tr("High-Res via NLI Proxy")).classes('absolute bottom-2 right-2 text-xs text-white/50 bg-black/30 px-2 rounded')
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
                    ui.html(format_text_html(result['source_ctx'], result.get('highlight_pattern')))
