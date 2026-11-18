# -*- coding: utf-8 -*-
"""Reading Desk, document mode: a PGP document's text, shown once for the whole document.

A document spread over several fragments has ONE edition (or several rival editions of the
whole document), not one per fragment. The desk used to ask for sources fragment by fragment
(``get_all_sources_for_fragment``), which returns the sources of EVERY document the fragment
belongs to: the same text was repeated under each fragment, an edition of another document
could be the one shown, and each text was cut into "recto" and "verso" by its headings and
dealt out to every fragment's pages.

Here the sources are asked for by document. ``document_desk`` is the data and the rule for
when the desk is "the document"; ``render_document_text_panel`` draws the text side.

The rule: the desk shows the document's text while every fragment on it belongs to the
document it was opened for. Removing a fragment keeps it (the edition is still the
document's; the panel says how many of its fragments are on the desk). Adding a fragment
from elsewhere ends it: the desk is then a hand-built one and goes back to one text per
fragment, until that fragment is removed again.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

from nicegui import ui

from web.translations import tr

logger = logging.getLogger(__name__)

VIEW_EDITION = 'edition'
VIEW_TRANSLATION = 'translation'
VIEW_FRAGMENTS = 'fragments'

# Keys in ``state.reading_desk_selected_sources`` (sys_id -> choice for the per-fragment
# selectors; these three cannot collide with a sys_id). Values are strings, so the dict
# stays what the browse snapshot already persists.
KEY_VIEW = '__document_view__'
KEY_EDITION = '__document_edition__'
KEY_TRANSLATION = '__document_translation__'

_TEXT_STYLE = (
    'font-size: 1.2rem; line-height: 1.9; '
    'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; white-space: pre-wrap; '
    'overflow-wrap: break-word; word-break: break-word; color: var(--text-primary);'
)

# The first three rules are for every desk (owner, 2026-11-18: compact the headers and
# toolbars); the rest is scoped to document mode. The rd-text-pane rule
# holds the text to the pane's width: a Quasar scroll area lets its content grow as wide
# as its widest child (a long attribution in the selector), which pushed the text's first
# letters out of sight. The media query stacks the two panes on a narrow screen, images
# above text.
DESK_CSS = '''
/* Every desk: the titles, the fragment headers and the image toolbars sit close together.
   NiceGUI's default is 1rem of padding and 1rem between every two children, in a card and
   again in a scroll area, which spread them over half a screen before the first image. */
.reading-desk-panels > .q-card { gap: 4px !important; padding: 10px !important; }
.reading-desk-panels .q-scrollarea__content { gap: 4px !important; padding: 4px 8px !important; }
.reading-desk-panels .rd-document-body { padding-top: 2px !important; }
.reading-desk-panels.rd-document-mode .rd-text-pane .q-scrollarea__content { width: 100%; max-width: 100%; }
.reading-desk-panels.rd-document-mode .rd-document-source { min-width: 0; max-width: 100%; }
/* The chosen source's attribution in full, on as many lines as it needs, not cut with "..." */
.rd-document-source .q-field__native span {
    white-space: normal; overflow: visible; text-overflow: clip; unicode-bidi: plaintext; text-align: start;
    line-height: 1.4; padding: 4px 0;
}
.rd-document-source .q-field__control, .rd-document-source .q-field__native { height: auto; min-height: 40px; }
@media (max-width: 900px) {
    .reading-desk-panels.rd-document-mode { flex-direction: column !important; min-height: 0 !important; }
    .reading-desk-panels.rd-document-mode > .q-card {
        flex: 0 0 auto !important; width: 100% !important; min-height: 0 !important;
    }
    /* flex: 1 has a zero basis, which in a stacked card of no fixed height is no height at all */
    .reading-desk-panels.rd-document-mode .rd-image-pane { flex: 0 0 auto !important; height: 55vh !important; }
    .reading-desk-panels.rd-document-mode .rd-text-pane { flex: 0 0 auto !important; height: 70vh !important; }
}
'''


def _relation(source: Dict[str, Any]) -> str:
    return source.get('doc_relation') or ''


def _has_text(source: Dict[str, Any]) -> bool:
    return bool((source.get('content') or '').strip())


def document_desk(pgpid: Optional[int], entry_sys_ids: Iterable[str]) -> Dict[str, Any]:
    """What the desk may show for document ``pgpid`` with these fragments on it.

    ``known``: the sidecar has the document's fragments. ``whole``: every fragment on the
    desk is one of them (the desk is the document, or part of it). ``editions`` and
    ``translations`` are the document's own sources, never another document's; a source
    that is both ("Edition ; Translation") counts as an edition.
    """
    from shared.document_service import (
        get_document_metadata, get_editions_for_document, get_fragments_for_document,
        get_translations_for_document,
    )
    on_desk = {str(s) for s in entry_sys_ids if s}
    desk: Dict[str, Any] = {
        'pgpid': pgpid, 'known': False, 'whole': False, 'editions': [], 'translations': [],
        'fragments_total': 0, 'fragments_on_desk': len(on_desk), 'pgp_url': None,
    }
    if not pgpid or not on_desk:
        return desk
    of_document = {str(f['sys_id']) for f in get_fragments_for_document(pgpid) if f.get('sys_id')}
    desk['known'] = bool(of_document)
    desk['fragments_total'] = len(of_document)
    desk['whole'] = bool(of_document) and on_desk <= of_document
    if not desk['whole']:
        return desk
    desk['editions'] = [s for s in get_editions_for_document(pgpid) if _has_text(s)]
    desk['translations'] = [s for s in get_translations_for_document(pgpid)
                            if _has_text(s) and 'Edition' not in _relation(s)]
    desk['pgp_url'] = (get_document_metadata(pgpid) or {}).get('pgp_url')
    return desk


def is_other_desk(desk: Dict[str, Any]) -> bool:
    """The desk was opened for a document but now holds a fragment from outside it."""
    return bool(desk.get('known')) and not desk.get('whole')


def has_document_text(desk: Dict[str, Any]) -> bool:
    return bool(desk.get('whole')) and bool(desk.get('editions') or desk.get('translations'))


def document_view(desk: Dict[str, Any], selected: Dict[str, Any]) -> Optional[str]:
    """Which text view the desk is in: an edition, a translation, one text per fragment --
    or None when the document's text is not on offer at all (the per-fragment desk)."""
    if not has_document_text(desk):
        return None
    wanted = (selected or {}).get(KEY_VIEW)
    if wanted == VIEW_FRAGMENTS:
        return VIEW_FRAGMENTS
    if wanted == VIEW_TRANSLATION and desk['translations']:
        return VIEW_TRANSLATION
    return VIEW_EDITION if desk['editions'] else VIEW_TRANSLATION


def chosen_source(desk: Dict[str, Any], selected: Dict[str, Any], view: str) -> Optional[Dict[str, Any]]:
    """The edition (or translation) on show: the reader's choice if it is still one of the
    document's, else the first."""
    sources = desk['translations'] if view == VIEW_TRANSLATION else desk['editions']
    if not sources:
        return None
    wanted = str((selected or {}).get(KEY_TRANSLATION if view == VIEW_TRANSLATION else KEY_EDITION) or '')
    return next((s for s in sources if str(s.get('id')) == wanted), sources[0])


def text_direction(source: Dict[str, Any]) -> str:
    """'rtl' or 'ltr' for a source's text: the sidecar's own word, else English reads left
    to right and everything else right to left."""
    direction = source.get('source_direction')
    if direction in ('rtl', 'ltr'):
        return direction
    return 'ltr' if source.get('language') == 'English' and 'Edition' not in _relation(source) else 'rtl'


def source_label(source: Dict[str, Any], view: str) -> str:
    """The attribution as PGP gives it; a translation is led by its language."""
    scholar = (source.get('source_scholar') or '').strip()
    if view != VIEW_TRANSLATION:
        return scholar or tr('PGP edition')
    language = tr(source['language']) if source.get('language') else tr('Translation')
    return f'{language}: {scholar}' if scholar else language


def _numbered(labels: List[str]) -> List[str]:
    """Labels that read alike get a number, so two sources of one scholar can be told apart."""
    from shared.fgp_service import number_repeated_labels
    return number_repeated_labels(labels)


def render_document_text_panel(desk: Dict[str, Any], state: Any, view: str,
                               on_layout_change: Callable[[], None],
                               on_choice: Optional[Callable[[], None]] = None) -> None:
    """The text side of the desk in document mode: one selector, one text.

    ``on_layout_change`` rebuilds the desk; it is called when the reader moves to (or from)
    one text per fragment. A change of edition or translation redraws this panel alone.
    ``on_choice`` is called after every choice, before anything is redrawn: the desk's
    snapshot is saved there, so a reload or a change of language comes back to it.
    """
    selected = state.reading_desk_selected_sources
    views = {}
    if desk['editions']:
        views[VIEW_EDITION] = tr('Edition')
    if desk['translations']:
        views[VIEW_TRANSLATION] = tr('Translation')
    views[VIEW_FRAGMENTS] = tr('By fragment')

    body = None

    def draw_body() -> None:
        current = document_view(desk, selected)
        body.clear()
        with body:
            source = chosen_source(desk, selected, current)
            if source is None:
                ui.label(tr('No text available')).classes('italic text-gray-400')
                return
            sources = desk['translations'] if current == VIEW_TRANSLATION else desk['editions']
            if len(sources) > 1:
                labels = _numbered([source_label(s, current) for s in sources])
                options = {str(s.get('id')): label for s, label in zip(sources, labels)}

                def on_source(e, current=current):
                    selected[KEY_TRANSLATION if current == VIEW_TRANSLATION else KEY_EDITION] = e.value
                    if on_choice:
                        on_choice()
                    draw_body()

                ui.select(options=options, value=str(source.get('id')), on_change=on_source).props(
                    'dense outlined options-dense'
                ).classes('rd-document-source text-sm w-full')
            else:
                # One source: its attribution alone (the selector above says it otherwise).
                with ui.row().classes('rd-document-attribution items-start gap-2 w-full no-wrap'):
                    ui.icon('menu_book', size='xs').classes('text-green-600').style('margin-top: 3px;')
                    ui.label(source_label(source, current)).classes('text-sm').style(
                        'color: var(--text-secondary); min-width: 0; overflow-wrap: anywhere; '
                        'unicode-bidi: plaintext; text-align: start;')
            # unicode-bidi: plaintext orders each line by its own first strong letter, so an
            # English note inside a Hebrew edition keeps its brackets and full stop in place;
            # the side the text hangs from stays the source's.
            direction = text_direction(source)
            ui.label(source['content']).classes('rd-document-text w-full').style(
                f'{_TEXT_STYLE} direction: {direction}; unicode-bidi: plaintext; '
                f'text-align: {"left" if direction == "ltr" else "right"};')

    def on_view(e) -> None:
        was_fragments = selected.get(KEY_VIEW) == VIEW_FRAGMENTS
        selected[KEY_VIEW] = e.value
        if on_choice:
            on_choice()
        if e.value == VIEW_FRAGMENTS or was_fragments:
            on_layout_change()
        else:
            draw_body()

    with ui.row().classes('items-center gap-2 px-3 pt-2 pb-1 w-full'):
        ui.toggle(views, value=view, on_change=on_view).props(
            'no-caps unelevated size=sm padding="4px 12px" color=white text-color=green-9 '
            'toggle-color=green-7 toggle-text-color=white'
        ).classes('rd-document-view').style('border: 1px solid #15803d; border-radius: 6px; overflow: hidden;')
        ui.element('div').classes('flex-grow')
        if desk.get('pgp_url'):
            ui.link(tr('View on PGP'), desk['pgp_url'], new_tab=True).classes('text-sm')
    if desk['fragments_on_desk'] < desk['fragments_total']:
        ui.label(tr("{on} of this document's {total} fragments are on the desk").format(
            on=desk['fragments_on_desk'], total=desk['fragments_total'],
        )).classes('rd-document-partial text-xs px-2').style('color: var(--text-secondary);')
    if view == VIEW_FRAGMENTS:
        return  # the caller draws one text per fragment below these controls
    with ui.scroll_area().classes('rd-text-pane w-full').style('flex: 1; height: calc(70vh - 40px);'):
        body = ui.column().classes('rd-document-body w-full px-3 py-2 gap-2').style('min-width: 0;')
    draw_body()
