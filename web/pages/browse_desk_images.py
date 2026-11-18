# -*- coding: utf-8 -*-
"""Reading Desk, image side: one image at a time beside a strip of thumbnails, or all
images in one scrolling stack.

Two views of the same list (``desk_images``: every page of every fragment on the desk, in
desk order):

* ``one``  -- a filmstrip of thumbnails and one large image with one toolbar. The default
  for a desk that shows a document's edition: the reader follows one text against one image.
* ``all``  -- every image under its fragment's header, one compact row of controls each.
  The default for a hand-built desk, and what the desk always was.

* ``join`` -- the assembled picture of a saved join of these fragments
  (``web/pages/browse_desk_puzzle.py``), offered on a desk of two or more fragments.

Each image keeps the id ``rd-viewer-<fragment>-<page>`` and the ``rd-zoomable`` class the
desk's viewer script (zoom, rotate, drag, brightness) works on; the fragment headers of the
stack keep ``rd-img-frag-<n>``, which the per-fragment text scrolls in step with.
"""
from __future__ import annotations

import html as html_module
import re
from typing import Any, Callable, Dict, List, Optional

from nicegui import ui

from shared.synthetic_sys_id import is_synthetic_sys_id
from web.services import get_oxford_direct_image_url, is_oxford_manuscript
from web.translations import tr

VIEW_ONE = 'one'
VIEW_ALL = 'all'
VIEW_JOIN = 'join'

# Keys in ``state.reading_desk_selected_sources`` (beside the text side's; strings only).
KEY_IMAGE_VIEW = '__image_view__'
KEY_IMAGE_SHOWN = '__image_shown__'

_THUMB_WIDTH = 240  # px asked of the image routes for a filmstrip thumbnail
_BODL_PREFIX = re.compile(r'^\s*Bodl\.\s*', re.IGNORECASE)


def image_view(selected: Dict[str, Any], document_text: bool, join_offered: bool = False) -> str:
    """The reader's choice, else one at a time beside a document's edition and all images
    on any other desk. The assembled picture is theirs only while it is on offer."""
    wanted = (selected or {}).get(KEY_IMAGE_VIEW)
    if wanted in (VIEW_ONE, VIEW_ALL) or (wanted == VIEW_JOIN and join_offered):
        return wanted
    return VIEW_ONE if document_text else VIEW_ALL


def join_is_offered(entries: List[Dict[str, Any]], can_arrange: bool) -> bool:
    """The assembled picture is on offer: the Fragment Puzzle is there to arrange in, and the
    desk holds two or more fragments. One rule for the image side and for the text side,
    which must agree on the view the reader is in."""
    return bool(can_arrange) and len([e for e in entries if e.get('sys_id')]) >= 2


def page_label(page_index: int, pages_in_fragment: int) -> str:
    """Recto / Verso for a fragment of one leaf (the site's convention for its two images);
    a plain number beyond that, where the side cannot be told from the position."""
    if pages_in_fragment <= 2:
        return tr('Recto') if page_index == 0 else tr('Verso')
    return str(page_index + 1)


def desk_images(entries: List[Dict[str, Any]], library_for: Optional[Callable[[str], str]] = None) -> List[Dict[str, Any]]:
    """Every page image of every fragment on the desk, in desk order.

    A fragment the index has no pages for is given the two images of one leaf, as before.
    ``library_for``: sys_id -> library code (to tell an Oxford fragment); it may fail.
    """
    images: List[Dict[str, Any]] = []
    for frag_idx, entry in enumerate(entries):
        sys_id = entry.get('sys_id', '') or ''
        shelfmark = entry.get('shelfmark', '') or ''
        library = ''
        if library_for and sys_id:
            try:
                library = library_for(sys_id) or ''
            except Exception:
                library = ''
        oxford = is_oxford_manuscript(shelfmark, library)
        page_numbers = [getattr(p, 'p_num', i + 1) for i, p in enumerate(entry.get('pages') or [])] or [1, 2]
        for pg_i, p_num in enumerate(page_numbers):
            pg_idx = max(0, (p_num or 1) - 1)
            fallback = f'/api/oxford_image/{sys_id}?page={pg_idx}'
            if oxford:
                # PGP writes "Bodl. MS heb. f 56/59"; the Bodleian's image name is built
                # from the shelfmark without that prefix.
                url = (get_oxford_direct_image_url(shelfmark, pg_idx)
                       or get_oxford_direct_image_url(_BODL_PREFIX.sub('', shelfmark), pg_idx) or fallback)
            elif is_synthetic_sys_id(sys_id):
                url = ''  # a catalogue-only record has no NLI image
            else:
                url = f'/api/nli_image_by_sysid/{sys_id}?page={pg_idx}'
            images.append({
                'key': f'{sys_id}:{pg_idx}', 'viewer_id': f'rd-viewer-{frag_idx}-{pg_i}',
                'frag_idx': frag_idx, 'sys_id': sys_id, 'shelfmark': shelfmark,
                'page_index': pg_idx, 'label': page_label(pg_idx, len(page_numbers)),
                'url': url, 'oxford': oxford, 'fallback': '' if oxford else fallback,
            })
    return images


def shown_image(images: List[Dict[str, Any]], selected: Dict[str, Any], current_shelfmark: str = '') -> Optional[Dict[str, Any]]:
    """The image on show in the one-at-a-time view: the reader's, else the first image of
    the fragment they came from, else the first."""
    if not images:
        return None
    wanted = (selected or {}).get(KEY_IMAGE_SHOWN)
    current = (current_shelfmark or '').upper()
    return (next((i for i in images if i['key'] == wanted), None)
            or next((i for i in images if current and i['shelfmark'].upper() == current), None)
            or images[0])


def shown_text_image(entries: List[Dict[str, Any]], selected: Dict[str, Any], *, document_text: bool,
                     current_shelfmark: str = '', can_arrange: bool = False,
                     library_for: Optional[Callable[[str], str]] = None) -> Optional[Dict[str, Any]]:
    """The image on show in the one-at-a-time view (its ``frag_idx`` and ``page_index``);
    None in the all-images view and in the assembled picture, which is of several fragments.
    Where the text is one per fragment, the text side shows this image's alone: its
    fragment, and its side of the fragment.

    A document's edition is not cut to the fragment: it is one text, and the sidecar does not
    say which part belongs to which image. Measured 2026-11-18 on the 607 editions of
    documents on two or more fragments: 147 carry two or more parts that can all be found in
    their text, and the parts are numbered by PGP's own canvases (two for most two-fragment
    documents, whose desk shows four images), not by the images shown here.
    """
    if image_view(selected, document_text, join_is_offered(entries, can_arrange)) != VIEW_ONE:
        return None
    return shown_image(desk_images(entries, library_for), selected, current_shelfmark)


def thumbnail_url(image: Dict[str, Any]) -> str:
    url = image['url']
    if url.startswith('/api/'):
        return f'{url}&width={_THUMB_WIDTH}'
    return url  # an Oxford image served by its library, at the size it comes in


def _image_html(image: Dict[str, Any], max_height: str) -> str:
    """The <img> the viewer script drives; a missing NLI image falls back to the Oxford
    route once, then hides."""
    hide = "this.style.display='none'"
    if image['fallback']:
        fallback = html_module.escape(image['fallback'], quote=True)
        onerror = (f"if (this.src.indexOf('oxford_image')===-1) {{ this.onerror=function(){{ {hide}; }}; "
                   f"this.src='{fallback}'; }} else {{ {hide}; }}")
    else:
        onerror = hide
    return (f'<img id="{image["viewer_id"]}" class="rd-zoomable" src="{html_module.escape(image["url"], quote=True)}" '
            f'style="max-height: {max_height}; max-width: 100%; object-fit: contain; cursor: grab; transition: none;" '
            f'loading="lazy" draggable="false" onerror="{onerror}" />')


def _toolbar(viewer_id: str) -> None:
    """Zoom, rotate, reset and (behind one button) the image adjustments, in one row."""
    def js(call: str):
        return lambda: ui.run_javascript(call)

    with ui.row().classes('rd-image-toolbar items-center no-wrap').style('gap: 0;'):
        flat = 'flat round dense size=sm'
        ui.button(icon='remove', on_click=js(f"window.rdZoom('{viewer_id}', -0.25)")).props(flat).tooltip(tr('Zoom out'))
        ui.html(f'<span id="{viewer_id}-zoom-label" style="font-size: 0.75rem; min-width: 36px; '
                f'display: inline-block; text-align: center; color: var(--text-secondary);">100%</span>',
                sanitize=False)
        ui.button(icon='add', on_click=js(f"window.rdZoom('{viewer_id}', 0.25)")).props(flat).tooltip(tr('Zoom in'))
        ui.button(icon='rotate_left', on_click=js(f"window.rdRotate('{viewer_id}', -90)")).props(flat).tooltip(tr('Rotate Left'))
        ui.button(icon='rotate_right', on_click=js(f"window.rdRotate('{viewer_id}', 90)")).props(flat).tooltip(tr('Rotate Right'))
        with ui.button(icon='tune').props(flat).classes('rd-image-adjust').tooltip(tr('Image Adjustments')):
            with ui.menu():
                with ui.column().classes('p-3').style('gap: 6px; min-width: 220px;'):
                    sliders = []
                    for icon, tip, lo, hi, start, call in (
                            ('brightness_6', 'Brightness', -100, 100, 0, 'rdSetBrightness'),
                            ('contrast', 'Contrast', -100, 100, 0, 'rdSetContrast'),
                            ('timeline', 'Gamma', 20, 300, 100, 'rdSetGamma')):
                        with ui.row().classes('items-center no-wrap w-full').style('gap: 8px;'):
                            ui.icon(icon, size='xs').tooltip(tr(tip))
                            scale = 100 if call == 'rdSetGamma' else 1
                            sliders.append((ui.slider(
                                min=lo, max=hi, step=1, value=start,
                                on_change=lambda e, call=call, scale=scale: ui.run_javascript(
                                    f"window.{call}('{viewer_id}', {e.value / scale})"),
                            ).props('dense color=green').classes('flex-grow'), start))
                    with ui.row().classes('items-center w-full').style('gap: 4px;'):
                        ui.button(tr('Invert Colors'), icon='exposure',
                                  on_click=js(f"window.rdToggleInvert('{viewer_id}')")).props('flat dense no-caps size=sm')
                        ui.element('div').classes('flex-grow')

                        def reset_adjustments():
                            for slider, start in sliders:
                                slider.value = start
                            ui.run_javascript(f"window.rdResetAdjustments('{viewer_id}')")

                        ui.button(tr('Reset Image'), icon='restart_alt',
                                  on_click=reset_adjustments).props('flat dense no-caps size=sm')
        ui.button(icon='restart_alt', on_click=js(f"window.rdResetView('{viewer_id}')")).props(flat).tooltip(tr('Reset View'))


def _image_box(image: Dict[str, Any], min_height: str, max_height: str) -> None:
    with ui.element('div').classes('rd-image-box').style(
            f'background: #1a1a1a; border-radius: 6px; overflow: hidden; display: flex; align-items: center; '
            f'justify-content: center; min-height: {min_height}; position: relative; width: 100%;'):
        ui.html(_image_html(image, max_height), sanitize=False)


def _fragment_name(image: Dict[str, Any], current: str, on_navigate: Callable[[str], Any]) -> None:
    ui.label(image['shelfmark']).classes('font-bold text-sm cursor-pointer hover:underline').style(
        'color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%;'
    ).on('click', lambda sm=image['shelfmark']: on_navigate(sm))
    if current and image['shelfmark'].upper() == current:
        ui.badge(tr('Current'), color='green').props('dense').classes('text-xs')


def _remove_button(image: Dict[str, Any], on_remove: Callable[[str], Any]) -> None:
    ui.button(icon='close', on_click=lambda sid=image['sys_id']: on_remove(sid)).props(
        'flat round dense size=sm').classes('rd-remove-fragment text-gray-400 hover:text-red-500').tooltip(tr('Remove'))


def _render_all(images: List[Dict[str, Any]], current: str, on_navigate, on_remove) -> None:
    with ui.scroll_area().classes('rd-image-pane w-full').style('flex: 1; height: calc(70vh - 40px);'):
        last_fragment = None
        for image in images:
            if image['frag_idx'] != last_fragment:
                last_fragment = image['frag_idx']
                with ui.element('div').props(f'id="rd-img-frag-{image["frag_idx"]}"').classes('reading-desk-fragment w-full'):
                    with ui.row().classes('items-center no-wrap w-full px-2 py-1').style(
                            'gap: 6px; background: var(--bg-tertiary, #f3f4f6); border-radius: 4px;'):
                        ui.icon('description', size='xs').classes('text-green-600')
                        _fragment_name(image, current, on_navigate)
                        ui.element('div').classes('flex-grow')
                        _remove_button(image, on_remove)
            with ui.row().classes('items-center no-wrap w-full').style('gap: 6px;'):
                ui.label(image['label']).classes('text-xs font-medium text-gray-500')
                ui.element('div').classes('flex-grow')
                _toolbar(image['viewer_id'])
            _image_box(image, '300px', '500px')


def _render_one(images: List[Dict[str, Any]], state: Any, current: str, on_navigate, on_remove,
                on_image_change: Optional[Callable[[], None]],
                on_choice: Optional[Callable[[], None]] = None) -> None:
    selected = state.reading_desk_selected_sources
    refs: Dict[str, Any] = {}

    def draw_main() -> None:
        image = shown_image(images, selected, current)
        refs['main'].clear()
        with refs['main']:
            # Wraps in a narrow pane: the name keeps its line, the toolbar takes the next.
            with ui.row().classes('rd-image-shown items-center w-full').style('gap: 2px 6px;'):
                _fragment_name(image, current, on_navigate)
                ui.label(image['label']).classes('text-xs font-medium text-gray-500')
                ui.element('div').classes('flex-grow')
                _toolbar(image['viewer_id'])
                _remove_button(image, on_remove)
            _image_box(image, 'calc(70vh - 96px)', 'calc(70vh - 96px)')
        for key, thumb in refs['thumbs'].items():
            thumb.classes(remove='rd-thumb-on', add='rd-thumb-on' if key == image['key'] else '')
        # A redrawn <img> starts unzoomed; forget the view the script kept for its id.
        ui.run_javascript(
            f"window.rdResetView && window.rdResetView('{image['viewer_id']}'); "
            f"setTimeout(function(){{ window.rdInitDrag && window.rdInitDrag('{image['viewer_id']}'); }}, 200);")

    def show(key: str) -> None:
        selected[KEY_IMAGE_SHOWN] = key
        if on_choice:
            on_choice()
        draw_main()
        if on_image_change:
            on_image_change()  # the text side shows this image's text

    with ui.row().classes('rd-image-one no-wrap w-full').style('flex: 1; gap: 8px; min-height: 0; align-items: stretch;'):
        with ui.scroll_area().classes('rd-filmstrip').style('flex: 0 0 84px; height: calc(70vh - 40px);'):
            refs['thumbs'] = {}
            last_fragment = None
            for image in images:
                if image['frag_idx'] != last_fragment:
                    last_fragment = image['frag_idx']
                    ui.label(image['shelfmark']).classes('rd-thumb-fragment').tooltip(image['shelfmark'])
                with ui.element('button').classes('rd-thumb').props(
                        f'type="button" aria-label="{html_module.escape(image["shelfmark"] + " " + image["label"], quote=True)}"'
                ).on('click', lambda key=image['key']: show(key)) as thumb:
                    if image['url']:
                        ui.html(f'<img src="{html_module.escape(thumbnail_url(image), quote=True)}" loading="lazy" '
                                f'draggable="false" onerror="this.style.visibility=\'hidden\'" />', sanitize=False)
                    ui.label(image['label']).classes('rd-thumb-label')
                refs['thumbs'][image['key']] = thumb
        refs['main'] = ui.column().classes('rd-image-main').style('flex: 1 1 auto; min-width: 0; gap: 4px;')
    draw_main()


def render_image_pane(entries: List[Dict[str, Any]], state: Any, *, document_text: bool, text_per_fragment: bool,
                      current_shelfmark: str,
                      library_for: Optional[Callable[[str], str]], on_navigate: Callable[[str], Any],
                      on_remove: Callable[[str], Any], on_layout_change: Callable[[], None],
                      on_image_change: Optional[Callable[[], None]] = None,
                      on_arrange: Optional[Callable[[], None]] = None,
                      on_choice: Optional[Callable[[], None]] = None) -> None:
    """The image side of the desk: the view switcher, then the view.

    ``text_per_fragment``: the text side is one text per fragment, so choosing another image
    calls ``on_image_change`` for the text to follow. ``on_layout_change`` redraws the desk.
    ``on_arrange`` takes the desk's fragments to the Fragment Puzzle; with it, a desk of two
    or more fragments also offers the assembled picture of a saved join. ``on_choice`` is
    called after every choice, before anything is redrawn (the desk's snapshot is saved there)."""
    selected = state.reading_desk_selected_sources
    join_offered = join_is_offered(entries, on_arrange is not None)
    view = image_view(selected, document_text, join_offered)
    images = desk_images(entries, library_for)
    current = (current_shelfmark or '').upper()

    def on_view(e) -> None:
        selected[KEY_IMAGE_VIEW] = e.value
        if on_choice:
            on_choice()
        on_layout_change()

    with ui.row().classes('items-center w-full px-1').style('gap: 8px;'):
        views = {VIEW_ONE: tr('One at a time'), VIEW_ALL: tr('All images')}
        if join_offered:
            views[VIEW_JOIN] = tr('Assembled')
        ui.toggle(views, value=view, on_change=on_view).props(
            'no-caps unelevated size=sm padding="4px 12px" color=white text-color=green-9 '
            'toggle-color=green-7 toggle-text-color=white'
        ).classes('rd-image-view').style('border: 1px solid #15803d; border-radius: 6px; overflow: hidden;')
        ui.element('div').classes('flex-grow')
        ui.label(f'{len(images)} {tr("images")}').classes('text-xs').style('color: var(--text-secondary);')
    if view == VIEW_JOIN:
        from web.pages.browse_desk_puzzle import render_join_view
        render_join_view(entries, state, toolbar=_toolbar, on_arrange=on_arrange, on_choice=on_choice)
        return
    if not images:
        return
    if view == VIEW_ONE:
        _render_one(images, state, current, on_navigate, on_remove,
                    on_image_change if text_per_fragment else None, on_choice)
    else:
        _render_all(images, current, on_navigate, on_remove)


IMAGES_CSS = '''
.rd-filmstrip .q-scrollarea__content { gap: 6px !important; padding: 2px 4px !important; align-items: center; }
.rd-thumb-fragment {
    font-size: 0.68rem; line-height: 1.2; color: var(--text-secondary); max-width: 72px; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap; margin-top: 4px; direction: ltr;
}
.rd-thumb {
    width: 68px; padding: 2px; border: 2px solid transparent; border-radius: 6px; background: #1a1a1a;
    cursor: pointer; display: flex; flex-direction: column; align-items: center; gap: 2px;
}
.rd-thumb img { width: 100%; height: 72px; object-fit: contain; display: block; }
.rd-thumb-label { font-size: 0.68rem; line-height: 1.1; color: #e5e7eb; }
.rd-thumb:hover { border-color: #86efac; }
.rd-thumb.rd-thumb-on { border-color: #16a34a; }
.rd-thumb:focus-visible { outline: 2px solid #16a34a; outline-offset: 2px; }
@media (max-width: 900px) {
    .rd-image-one { flex-direction: column !important; }
    .rd-filmstrip { flex: 0 0 auto !important; height: 112px !important; width: 100%; }
    .rd-filmstrip .q-scrollarea__content { flex-direction: row !important; flex-wrap: nowrap; align-items: flex-end; }
}
'''
