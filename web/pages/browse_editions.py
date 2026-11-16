# -*- coding: utf-8 -*-
"""Opening a scholarly transcription's hit on /browse (editions plan section 3.4).

A search result found in an FGP or PGP edition links to ``/browse?...&version=fgp:<id>&span=a-b``
(web/pages/search_editions.py::browse_url):

* ``version`` chooses that edition in the version chooser on the page the link arrived for,
  before the page and coverage filters (shared/fgp_service.py::filter_sources_for_page,
  choose_default_source) could leave it out or prefer another;
* ``span`` is the match in the edition's *search text* (the text the editions index holds,
  shared/edition_search_text.py). It is mapped onto the edition's own characters by re-running
  the versioned cleaner on the sidecar's text -- signs, points and line numbers included -- and
  that exact stretch is marked: the match is never guessed again from a phrase;
* when the edition is gone from the sidecar, or its text is no longer the indexed one, the hit
  is stale: the manuscript opens as usual and the reader is told the edition has changed.
"""
import html
import re
from typing import Any, Dict, List, Optional, Tuple

_VERSION_RE = re.compile(r'^(fgp|pgp):(\d+)$')
_SPAN_RE = re.compile(r'^(\d+)-(\d+)$')
_FP_RE = re.compile(r'^[0-9a-f]{12}$')


def parse_target(version: Optional[str], span: Optional[str],
                 placed: bool = True, fp: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """``{'version', 'span'}`` from the link, or None when it names no edition. A malformed
    span is dropped (the edition still opens). *placed*: the link names a page or folio; a
    hit whose page could not be identified names none (search_editions.browse_url) and is
    marked ``unplaced``: the edition opens with its match on the folio the manuscript opens
    on (editions plan section 3.4), and the reader is told its page is not known. *fp*: what
    the span held when the search read it (editions_search.span_fingerprint); a link from
    before it was carried has none."""
    if not version or not _VERSION_RE.match(str(version)):
        return None
    target: Dict[str, Any] = {'version': str(version), 'span': None}
    if not placed:
        target['unplaced'] = True
    m = _SPAN_RE.match(str(span or ''))
    if m and int(m.group(1)) < int(m.group(2)):
        target['span'] = (int(m.group(1)), int(m.group(2)))
        if fp and _FP_RE.match(str(fp)):
            target['fp'] = str(fp)
    return target


parse_edition_target = parse_target


def link_target(version: Optional[str], span: Optional[str], *, placed: bool = True,
                fp: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """parse_target for this reader: None where the scholarly transcriptions are not served
    to them (EDITIONS_SEARCH_ENABLED=editors and not an editor, or the switch off) -- an
    editor's link opens the manuscript as usual, with no edition chosen or marked from the
    editions index (Codex review of PR #18)."""
    from web.auth_state import GlobalAuthState
    from web.editions_assets import served_to
    if not served_to(GlobalAuthState.is_editor()):
        return None
    return parse_target(version, span, placed=placed, fp=fp)


def owns_target(state, page) -> bool:
    """Is *page* the folio the edition link arrived for? The first folio rendered with a target
    claims it (keyed as browse.py's highlight scope: sys_id, volume, page); every other folio is
    browsed as usual."""
    target = getattr(state, 'edition_target', None)
    if not target or page is None:
        return False
    key = (getattr(page, 'sys_id', None), getattr(page, 'volume_ie', None) or getattr(state, 'volume_ie', None),
           getattr(page, 'p_num', None))
    if target.get('scope') is None:
        target['scope'] = key
    return target['scope'] == key


def on_target_folio(state, page) -> bool:
    """Is *page* the folio the edition link arrived for, or has none claimed it yet? Unlike
    owns_target it claims nothing (the address is written for whatever folio shows)."""
    target = getattr(state, 'edition_target', None)
    if not target or page is None:
        return False
    key = (getattr(page, 'sys_id', None), getattr(page, 'volume_ie', None) or getattr(state, 'volume_ie', None),
           getattr(page, 'p_num', None))
    return target.get('scope') in (None, key)


def claim_target(state, page) -> bool:
    """Claim the edition link for *page* if no folio has: called on the event loop when a
    folio's enrichment starts, so the arrival folio claims it before any worker runs and a
    folio reached meanwhile cannot (Codex review of PR #18). Returns whether *page* owns it."""
    return owns_target(state, page)


def resolve_target(state, page) -> None:
    """Resolve the target's mark once, for the folio that owns it (blocking: off the loop).
    It claims nothing: claim_target did, on the loop."""
    target = getattr(state, 'edition_target', None)
    if not target or target.get('resolved') or not on_target_folio(state, page) \
            or target.get('scope') is None:
        return
    target.update(resolve_mark(page.sys_id, target['version'], target.get('span'), target.get('fp')))
    target['resolved'] = True


def _fresh(state, page) -> Optional[Dict[str, Any]]:
    target = getattr(state, 'edition_target', None)
    if target and target.get('resolved') and not target.get('stale') and owns_target(state, page):
        return target
    return None


def target_version_for(state, page) -> Optional[str]:
    """The edition the chooser opens on (version_selector's ``prefer_version``), on the folio
    the link arrived for, when the edition is still the one the search read."""
    target = _fresh(state, page)
    return target['version'] if target else None


def info_version(info: Optional[Dict[str, Any]]) -> Optional[str]:
    """The version of what the chooser shows (its ``version_info``), or None."""
    info = info or {}
    if info.get('source') == 'fgp' and info.get('uid'):
        return str(info['uid'])
    if info.get('source') == 'pgp' and info.get('source_id') is not None:
        return f'pgp:{info["source_id"]}'
    return None


def target_mark_for(state, page, info) -> Optional[Dict[str, Any]]:
    """The exact stretch to mark while the linked edition is the text shown, else None (another
    version shows the usual yellow marks)."""
    target = _fresh(state, page)
    if not target or not target.get('mark') or info_version(info) != target['version']:
        return None
    return {'mark': target['mark'], 'occurrence': int(target.get('occurrence') or 0)}


def note_stale_edition(state) -> None:
    """Say once what the reader should know about the linked edition: that it has changed
    since the search (the manuscript shows as usual), or that the page of its match could
    not be identified (it shows on the folio the manuscript opens on; Codex review of PR
    #18)."""
    message = edition_target_notice(getattr(state, 'edition_target', None))
    if message:
        state.edition_target['notified'] = True
        from nicegui import ui
        from web.translations import tr
        ui.notify(tr(message), type='info')


def edition_target_notice(target: Optional[Dict[str, Any]]) -> Optional[str]:
    """The notice (in English, translated where shown) a resolved edition link calls for,
    once; None when it needs none or has had it."""
    if not target or not target.get('resolved') or target.get('notified'):
        return None
    if target.get('stale'):
        return 'The scholarly transcription has changed since the search; the manuscript is shown'
    if target.get('unplaced'):
        return ('The page of this match could not be identified; the edition is shown with the '
                'page the manuscript opens on')
    return None


def source_version(source: Dict[str, Any]) -> Optional[str]:
    """A chooser source's version (``fgp:<id>`` / ``pgp:<id>``), the edition records' key."""
    from shared.fgp_service import SOURCE_FGP, source_provider
    if source.get('id') is None:
        return None
    if source_provider(source) == SOURCE_FGP:
        return source.get('uid') or f'fgp:{source.get("id")}'
    return f'pgp:{source.get("id")}'


def keep_target(all_sources: List[Dict[str, Any]], filtered: Optional[List[Dict[str, Any]]],
                version: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    """The page's sources after its filter, with the linked edition kept when the filter left
    it out (it is the text the search matched on this page)."""
    if not version:
        return filtered
    kept = list(filtered or [])
    if any(source_version(s) == version for s in kept):
        return kept
    wanted = [s for s in all_sources or [] if source_version(s) == version]
    return (kept + wanted[:1]) or filtered


def resolve_mark(sys_id: str, version: str, span: Optional[Tuple[int, int]],
                 fp: Optional[str] = None) -> Dict[str, Any]:
    """``{'stale': bool, 'mark': str or None, 'occurrence': int}``: the edition's own characters
    the search matched, and which occurrence of them in the edition it is. *fp*: what the
    span held when the search read it; a span that no longer holds it is stale, even where
    the edition's version and its index agree (both rebuilt since; Codex review of PR #18).
    Blocking (a sidecar read and an index lookup): call it off the event loop."""
    from web.editions_assets import edition_for_page
    from web.state import state
    found = edition_for_page(sys_id, version)
    if found is None:
        return {'stale': True, 'mark': None, 'occurrence': 0}
    entry, resolved = found
    try:
        indexed = state.searcher.get_full_text_by_id(entry['uid'])
    except Exception:
        indexed = None
    search = resolved.get('search')
    if indexed is None or search is None or search.text != indexed:
        # The edition's text is not the one the search read (refreshed since the build).
        return {'stale': True, 'mark': None, 'occurrence': 0}
    if not span or not (0 <= span[0] < span[1] <= len(search.text)):
        # A span past the end of the text the edition has now cannot be the search's.
        return {'stale': bool(span and fp), 'mark': None, 'occurrence': 0}
    from shared.editions_search import span_fingerprint
    if fp and span_fingerprint(search.text, span) != fp:
        return {'stale': True, 'mark': None, 'occurrence': 0}
    start, end = search.span_to_original(*span)
    original = resolved.get('original') or ''
    mark = original[start:end]
    return {'stale': False, 'mark': mark or None, 'occurrence': original[:start].count(mark) if mark else 0}


def mark_html(text: str, mark: Optional[str], occurrence: int = 0) -> Optional[str]:
    """*text*, escaped, with the *occurrence*-th stretch equal to *mark* marked as a search hit
    (the first one when the shown text has fewer: the chooser may show the edition's sections
    apart). None when the shown text does not hold it."""
    if not text or not mark:
        return None
    positions = []
    at = text.find(mark)
    while at != -1:
        positions.append(at)
        at = text.find(mark, at + len(mark))
    if not positions:
        return None
    at = positions[occurrence] if occurrence < len(positions) else positions[0]
    # A match that crosses a line break is marked on each of its lines: the reading view
    # lays the text out line by line.
    marked = html.escape(mark).replace('\n', '</span>\n<span class="highlight-term">')
    return (html.escape(text[:at]) + '<span class="highlight-term">' + marked + '</span>'
            + html.escape(text[at + len(mark):]))
