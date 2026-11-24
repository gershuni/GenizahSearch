# -*- coding: utf-8 -*-
"""The desktop Reading Desk's text side: what the text pane shows, and the texts to choose from.

Two ways to show a desk's text:

* **Per fragment** -- every fragment's text, one under another, each with its own
  [Change version] (or, with one image on show, that image's fragment alone, and of its
  automatic transcription that image's page alone).
* **The document** -- on a PGP document's desk (``shared/reading_desk_document.py``, the
  website's document mode): the document's own edition or translation, once, never another
  document's, with a switch between Edition, Translation and By fragment (GS-307). The edition
  on show is chosen with [Change version] or with the Version chooser above the text, whose item
  the Cite bar credits. By fragment opens each fragment on a transcription of its own (its FGP
  edition, else the automatic one): the PGP sources it lists are the document's.

``genizah_app.py`` keeps the desk's state and calls in here; nothing here imports it.
"""
from __future__ import annotations

import html as _html_mod
from typing import Any, Callable, Dict, List, Optional, Tuple

from shared.reading_desk_document import (
    VIEW_FRAGMENTS, VIEW_TRANSLATION, chosen_source, document_view, source_labels, text_direction,
    view_sources, views_on_offer,
)

# The links of the document text (genizah_app.py: the text pane's anchorClicked).
LINK_VIEW = 'genizah://rd-doc-view/'
LINK_SOURCE = 'genizah://rd-doc-source'

_VIEW_LABELS = {'edition': 'Edition', 'translation': 'Translation', VIEW_FRAGMENTS: 'By fragment'}

# The text pane's colours, light and dark: its rows (a fragment's name, the edition's attribution, a
# page break) are drawn in the pane's own theme, never a light block on the dark one.
_LIGHT = {'row': '#ecf0f1', 'page': '#f8f9fa', 'page_line': '#ced4da', 'muted': '#6c757d', 'empty': '#95a5a6',
          'link': '#2980b9', 'version': '#8e44ad', 'remove': '#e74c3c', 'chosen': '#15803d'}
_DARK = {'row': '#2a323c', 'page': '#262b31', 'page_line': '#3b434d', 'muted': '#9aa3ad', 'empty': '#8a949e',
         'link': '#5dade2', 'version': '#c39bd3', 'remove': '#f1948a', 'chosen': '#4ade80'}


def theme_colours(widget=None) -> Dict[str, str]:
    """The pane's colours for ``widget``'s theme: dark when its window is dark (the app's own test,
    genizah_app.py), else light."""
    try:
        from PyQt6.QtGui import QPalette
        dark = widget is not None and widget.palette().color(QPalette.ColorRole.Window).lightness() < 128
    except Exception:
        dark = False
    return _DARK if dark else _LIGHT


# -- one fragment's texts ----------------------------------------------------------------------

def source_key(src: Dict[str, Any], position: int) -> str:
    """A source's key for the reader's choice (``ReadingDeskEntry.chosen``): its provider and
    id (PGP and FGP ids may collide), else its place in the list."""
    from shared.fgp_service import namespaced_source_id
    return namespaced_source_id(src) or f"#{position}"


def fragment_scope(state, document: Dict[str, Any]) -> Optional[int]:
    """The desk's PGP document while its text is By fragment, else None. Each fragment's texts are
    then that document's PGP sources and the fragment's own: a fragment's sources hold those of
    every document it belongs to, and another document's edition is not this desk's (GS-307)."""
    return state.pgpid if desk_view(state, document) == VIEW_FRAGMENTS else None


def _offered(src: Dict[str, Any], pgpid: Optional[int]) -> bool:
    """A fragment's source is on offer: always, except another document's PGP source while the
    desk is a document's (``fragment_scope``)."""
    from shared.fgp_service import source_provider
    if pgpid is None or source_provider(src) == 'fgp':
        return True
    return str(src.get('pgpid') or '') == str(pgpid)


def source_options(entry, tr: Callable[[str], str], pgpid: Optional[int] = None) -> List[Tuple[str, str]]:
    """(label, key) for each text of a fragment that has one: its sources, then the automatic
    transcription (key 'v08'). ``pgpid``: ``fragment_scope``, which leaves out another document's
    PGP sources."""
    from shared.fgp_service import source_provider
    options = []
    for position, src in enumerate(entry.sources or []):
        if not (src.get('content') or '').strip() or not _offered(src, pgpid):
            continue
        relation = src.get('doc_relation', '') or ''
        scholar = src.get('source_scholar', 'Unknown')
        language = src.get('language', '')
        if 'Edition' in relation:
            label = f"{tr('Edition')}: {scholar}"
        elif 'Translation' in relation:
            label = (f"{tr('Translation')} ({language}): {scholar}" if language
                     else f"{tr('Translation')}: {scholar}")
        else:
            label = f"{relation}: {scholar}"
        if source_provider(src) == 'fgp':
            label = f"FGP {label}"
        options.append((label, source_key(src, position)))
    options.append(("V0.8 (HTR)", 'v08'))
    return options


def entry_text(entry, pgpid: Optional[int] = None) -> Tuple[str, str, str]:
    """(text, direction, key) the desk shows for a fragment: the reader's choice, else its
    first edition, else its first source with text; ('', 'rtl', 'v08') for the automatic
    transcription, shown page by page.

    ``pgpid``: ``fragment_scope``, the desk is that PGP document's, in By fragment. Its PGP
    sources are then the document's, the same text under every fragment, so the fragment opens on
    what it has of its own: its FGP transcription, else the automatic one. The document's PGP
    sources stay on offer, and the reader's choice among them is kept; another document's is
    neither offered nor shown, even when chosen before (``source_options``)."""
    from shared.fgp_service import source_provider
    own_only = pgpid is not None

    def direction(src):
        return 'ltr' if src.get('language', '') == 'English' else 'rtl'

    sources = entry.sources or []
    chosen = getattr(entry, 'chosen', '') or ''
    if chosen == 'v08':
        return '', 'rtl', 'v08'
    if chosen:
        for position, src in enumerate(sources):
            if source_key(src, position) == chosen and src.get('content') and _offered(src, pgpid):
                return src['content'], direction(src), chosen
    candidates = [(position, src) for position, src in enumerate(sources)
                  if not own_only or source_provider(src) == 'fgp']
    for position, src in candidates:
        if 'Edition' in (src.get('doc_relation') or '') and src.get('content'):
            return src['content'], 'rtl', source_key(src, position)
    for position, src in candidates:
        if src.get('content'):
            return src['content'], direction(src), source_key(src, position)
    return '', 'rtl', 'v08'


def version_data(entry, key: str, lang: str) -> Dict[str, Any]:
    """A desk chooser item's data: the fragment and its text's key (``desk``, ``sys_id``,
    ``key``), in the browse chooser's own vocabulary as well, so that whatever reads the chosen
    item -- the Cite bar's credit (genizah_app.py::_credit_version_info) -- credits the right
    provider rather than falling through to the automatic transcription's."""
    data = {"source": "original", "desk": True, "sys_id": entry.sys_id, "key": key}
    for position, src in enumerate(entry.sources or []):
        if key == 'v08' or source_key(src, position) != key:
            continue
        data.update(_credit_fields(src, lang))
        break
    return data


def _credit_fields(src: Dict[str, Any], lang: str) -> Dict[str, Any]:
    """A source in the browse chooser's vocabulary (what the Cite bar credits)."""
    from shared.fgp_service import pick_fgp_credit, source_provider, source_relation_kind
    translation = source_relation_kind(src) == 'translation'
    fgp = source_provider(src) == 'fgp'
    return {
        "source": (("fgp_translation" if translation else "fgp_edition") if fgp
                   else ("pgp_translation" if translation else "pgp_edition")),
        "content": src.get('content', ''),
        "scholar": src.get('source_scholar', 'FGP' if fgp else 'Unknown'),
        "language": src.get('language') or '',
        "attribution": src.get('attribution'),
        "source_credit": pick_fgp_credit(src, lang) if fgp else None,
        "source_id": src.get('id'),
        "pgpid": src.get('pgpid'),
    }


# -- the document's text -----------------------------------------------------------------------

def desk_view(state, document: Dict[str, Any]) -> Optional[str]:
    """The text view of the desk: an edition, a translation, By fragment, or None (not a
    document's desk, or no document text: one text per fragment)."""
    return document_view(document or {}, getattr(state, 'text_view', '') or None)


def _wanted(state, view: str) -> str:
    return getattr(state, 'translation' if view == VIEW_TRANSLATION else 'edition', '') or ''


def shown_source(state, document: Dict[str, Any], view: str) -> Optional[Dict[str, Any]]:
    """The edition or translation on show: the reader's, if still one of the document's."""
    return chosen_source(document, view, _wanted(state, view))


def choose_view(state, view: str) -> bool:
    """The reader chose a view. True when it changed."""
    if view not in _VIEW_LABELS or (state.text_view or '') == view:
        return False
    state.text_view = view
    return True


def choose_source(state, view: str, source_id: Any) -> bool:
    """The reader chose the edition (or translation) on show. True when it changed."""
    name = 'translation' if view == VIEW_TRANSLATION else 'edition'
    value = '' if source_id is None else str(source_id)
    if getattr(state, name, '') == value:
        return False
    setattr(state, name, value)
    return True


def document_options(state, document: Dict[str, Any], view: str,
                     tr: Callable[[str], str], lang: str) -> Tuple[List[Tuple[str, Dict[str, Any]]], int]:
    """The Version chooser's items in a document view: the document's editions (or
    translations), each in the browse chooser's vocabulary, and the index of the one on show."""
    sources = view_sources(document, view)
    shown = shown_source(state, document, view)
    items = []
    for src, label in zip(sources, source_labels(sources, view, tr)):
        data = {"desk": True, "document": True, "view": view}
        data.update(_credit_fields(src, lang))
        items.append((label, data))
    current = next((i for i, src in enumerate(sources) if shown is not None and src is shown), 0)
    return items, current


def ask_source(parent, state, document: Dict[str, Any], tr: Callable[[str], str]) -> bool:
    """[Change version] of the document's text: the reader picks one of its editions (or
    translations). True when the choice changed."""
    from PyQt6.QtWidgets import QInputDialog
    view = desk_view(state, document)
    sources = view_sources(document, view) if view not in (None, VIEW_FRAGMENTS) else []
    if len(sources) < 2:
        return False
    labels = source_labels(sources, view, tr)
    shown = shown_source(state, document, view)
    current = next((i for i, src in enumerate(sources) if src is shown), 0)
    choice, ok = QInputDialog.getItem(parent, tr("Select Version"), tr("Select Version"), labels, current, False)
    if not ok or choice not in labels:
        return False
    return choose_source(state, view, sources[labels.index(choice)].get('id'))


def follow_link(parent, state, document: Dict[str, Any], url: str, tr: Callable[[str], str]) -> bool:
    """A link of the document text: a view, or [Change version]. True when the text changed."""
    if url.startswith(LINK_VIEW):
        return choose_view(state, url[len(LINK_VIEW):])
    if url == LINK_SOURCE:
        return ask_source(parent, state, document, tr)
    return False


def _document_head(state, document: Dict[str, Any], view: str, tr: Callable[[str], str],
                   c: Dict[str, str]) -> List[str]:
    """The switch between the views, and how much of the document is on the desk."""
    parts = []
    for name in views_on_offer(document):
        label = _html_mod.escape(tr(_VIEW_LABELS[name]))
        if name == view:
            parts.append(f"<b style='color: {c['chosen']};'>{label}</b>")
        else:
            parts.append(f"<a href='{LINK_VIEW}{name}' style='color: {c['link']}; text-decoration: none;'>{label}</a>")
    html = ["<div style='padding: 2px 10px 6px 10px;'>" + ' &nbsp;|&nbsp; '.join(parts) + "</div>"]
    if document['fragments_on_desk'] < document['fragments_total']:
        line = tr("{on} of this document's {total} fragments are on the desk").format(
            on=document['fragments_on_desk'], total=document['fragments_total'])
        html.append(f"<div style='padding: 0 10px 6px 10px; color: {c['muted']}; font-size: 11px;'>"
                    f"{_html_mod.escape(line)}</div>")
    return html


def _document_text(state, document: Dict[str, Any], view: str, tr: Callable[[str], str], c: Dict[str, str],
                   html_parts: List[str], raw_text_parts: List[str]) -> None:
    """The document's edition (or translation), once: its attribution, then the text."""
    from desktop.widgets import text_lines_to_html
    source = shown_source(state, document, view)
    if source is None:
        html_parts.append(f"<div style='padding: 10px; color: {c['empty']}; font-style: italic;'>"
                          f"{_html_mod.escape(tr('No text available'))}</div>")
        return
    sources = view_sources(document, view)
    label = source_labels(sources, view, tr)[next(i for i, s in enumerate(sources) if s is source)]
    change = (f" <a href='{LINK_SOURCE}' style='color: {c['version']}; text-decoration: none; font-size: 11px; "
              f"margin-left: 10px;'>[{_html_mod.escape(tr('Change version'))}]</a>") if len(sources) > 1 else ''
    html_parts.append(
        f"<div style='background: {c['row']}; padding: 6px 10px; border-bottom: 2px solid {c['chosen']};'>"
        f"<span dir='auto'>{_html_mod.escape(label)}</span>{change}</div>")
    direction = text_direction(source)
    html_parts.append(
        f"<div dir='{direction}' style='padding: 10px; font-family: SBL Hebrew, serif; "
        f"font-size: 16px; line-height: 1.6;'>{text_lines_to_html(source['content'])}</div>")
    raw_text_parts.append(source['content'])


# -- the text pane -----------------------------------------------------------------------------

def desk_text_html(state, *, shown: Optional[Dict[str, Any]], current_sid: str,
                   document: Dict[str, Any], tr: Callable[[str], str], theme_of=None) -> Tuple[str, List[str]]:
    """The text pane's HTML, and each text's raw lines for the line-number gutter (one entry per
    text: per page of an automatic transcription, so its numbering restarts at each page).

    ``shown``: the image on show in "One at a time" (None in the other views); ``document``: the
    desk's PGP document (``document_desk``), or {} when it is not one; ``theme_of``: the widget
    whose theme the pane is drawn in (``theme_colours``)."""
    c = theme_colours(theme_of)
    from desktop.widgets import text_lines_to_html
    from shared.reading_desk_views import text_page_for_image
    view = desk_view(state, document)
    html_parts: List[str] = []
    raw_text_parts: List[str] = []
    count = len(state.entries)

    # Header bar
    html_parts.append(
        "<div style='background: #2c3e50; color: white; padding: 8px 12px; "
        "margin-bottom: 10px; border-radius: 4px;'>"
        f"<b>{tr('Reading Desk')}</b>"
    )
    if state.pgpid:
        html_parts.append(f" &mdash; PGP #{state.pgpid}")
    html_parts.append(f" ({count} {tr('fragment') if count == 1 else tr('fragments')})</div>")

    if view is not None:
        html_parts.extend(_document_head(state, document, view, tr, c))
        if view != VIEW_FRAGMENTS:
            _document_text(state, document, view, tr, c, html_parts, raw_text_parts)
            return "\n".join(html_parts), raw_text_parts

    for idx, entry in enumerate(state.entries):
        if shown is not None and idx != shown['frag_idx']:
            continue  # one image on show: its fragment's text alone
        sid = entry.sys_id
        shelfmark = entry.shelfmark

        # Fragment header (clickable to navigate to single view). With one image on show
        # there is none: the fragment's name and its remove are over the image, and its
        # version is the "Version" chooser above the text (_browse_rd_sync_version_combo).
        if shown is None:
            current_badge = (f" <span style='background: #27ae60; color: white; padding: 1px 6px; border-radius: 3px; "
                             f"font-size: 10px;'>{_html_mod.escape(tr('Current'))}</span>") if sid == current_sid else ""
            html_parts.append(
                f"<div id='rd-text-frag-{idx}' style='background: {c['row']}; padding: 6px 10px; "
                f"margin-top: {'0' if idx == 0 else '20'}px; border-bottom: 2px solid #3498db;'>"
                f"<a href='genizah://rd-navigate/{sid}' style='color: {c['link']}; text-decoration: none; font-weight: bold;'>"
                f"{shelfmark}</a>{current_badge}"
                f" <a href='genizah://rd-remove/{sid}' style='color: {c['remove']}; text-decoration: none; font-size: 11px; margin-left: 10px;'>[{_html_mod.escape(tr('Remove'))}]</a>"
            )
            if entry.sources:  # version selector link
                html_parts.append(
                    f" <a href='genizah://rd-version/{sid}/{idx}' style='color: {c['version']}; text-decoration: none; font-size: 11px; margin-left: 10px;'>[{_html_mod.escape(tr('Change version'))}]</a>"
                )
            html_parts.append("</div>")

        # The text to show: the reader's choice, else an edition, else V0.8
        display_text, direction, _key = entry_text(entry, fragment_scope(state, document))

        # Fallback to V0.8 text from pages: ONE `<div>` (and ONE raw_text_parts entry) per page,
        # so the gutter restarts numbering at each page. A PGP or FGP source has no pages.
        text_pages = [p for p in entry.pages if p.get('text')] if (not display_text and entry.pages) else []
        # One image on show: its page alone (none, when that image has no text page),
        # never every page of the fragment together.
        shown_page = text_page_for_image(shown, text_pages) if (shown is not None and text_pages) else None
        if shown is not None and text_pages:
            text_pages = [text_pages[shown_page]] if shown_page is not None else []
        page_texts_v08 = [p.get('text', '') for p in text_pages]

        def _emit_content_div(text_body, direction=direction):
            # Escaped: a PGP edition's `<...>` added letters would vanish.
            html_parts.append(
                f"<div dir='{direction}' style='padding: 10px; font-family: SBL Hebrew, serif; "
                f"font-size: 16px; line-height: 1.6;'>"
                f"{text_lines_to_html(text_body)}</div>"
            )
            raw_text_parts.append(text_body)

        if display_text:
            _emit_content_div(display_text)
        elif page_texts_v08:
            if shown_page is not None:
                html_parts.append(
                    f"<div style='padding: 2px 10px; color: {c['muted']}; font-size: 11px;'>"
                    f"{_html_mod.escape(shown.get('label') or '')}</div>"
                )
            for page_idx, page_text in enumerate(page_texts_v08):
                if page_idx > 0:
                    # Visible page separator inside the fragment, where the gutter restarts.
                    html_parts.append(
                        "<div style='margin: 12px 0; padding: 4px 10px; "
                        f"background: {c['page']}; color: {c['muted']}; font-size: 11px; "
                        f"border-top: 1px dashed {c['page_line']};'>"
                        f"{_html_mod.escape(tr('Page'))} {page_idx + 1}</div>"
                    )
                _emit_content_div(page_text)
        else:
            html_parts.append(
                f"<div style='padding: 10px; color: {c['empty']}; font-style: italic;'>"
                f"{_html_mod.escape(tr('No text available'))}</div>"
            )
    return "\n".join(html_parts), raw_text_parts
