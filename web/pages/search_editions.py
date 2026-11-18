# -*- coding: utf-8 -*-
"""The scholarly transcriptions (FGP and PGP editions) on the search page (editions plan
section 3.4): the source choice, what a result card says matched, and the count line.

A search with them returns one result per page (shared/editions_search.py::group_results):
``members`` (each matching text of the page, each with its ``edition`` record or None for the
automatic transcription), ``sources``, ``edition_match`` and ``page_identified``. Rows without
``members`` (no choice shown, or the automatic transcriptions only) are today's and get nothing
from here.
"""
from typing import Any, Dict, List, Optional

from web.translations import get_language, tr

#: The Transcriptions selector beside the mode, in English (translated where it is shown).
SOURCE_CHOICES = {
    'all': 'All',
    'editions': 'Scholarly',
    'automatic': 'Automatic',
}

#: Modes that read no transcription text: no choice is shown or sent.
NO_SOURCES_MODES = frozenset({'Shelfmark', 'Title', 'Catalog', 'pgp_tags'})

_SOURCE_NAMES = {
    'automatic': 'Automatic transcription',
    'fgp_edition': 'FGP edition',
    'pgp_edition': 'PGP edition',
    'local': 'My Library',
}

#: A credit longer than this is cut on the card (the full credit is in the reading view).
_CREDIT_CHARS = 40


def _credit(edition: Optional[Dict[str, Any]]) -> Optional[str]:
    credit = (edition or {}).get('credit') or {}
    name = credit.get(get_language()) or credit.get('en') or credit.get('he')
    if not name:
        return None
    name = ' '.join(str(name).split())
    return name if len(name) <= _CREDIT_CHARS else name[:_CREDIT_CHARS - 1].rstrip() + '…'


def member_label(member: Dict[str, Any]) -> str:
    """A member's source as the card names it: "PGP edition (Ben-Sasson)"."""
    from shared.editions_search import source_label
    name = tr(_SOURCE_NAMES.get(source_label(member), 'Edition'))
    credit = _credit(member.get('edition'))
    return f'{name} ({credit})' if credit else name


def matched_in(result: Dict[str, Any]) -> Optional[str]:
    """The card's line naming every text of the page that matched -- "Matched in: Automatic
    transcription, PGP edition (Ben-Sasson)" -- for a result found with the scholarly
    transcriptions; None for a today's row, or a page found in its automatic transcription
    alone."""
    members: List[Dict[str, Any]] = result.get('members') or []
    if not members or not any(m.get('edition') for m in members):
        return None
    labels = list(dict.fromkeys(member_label(m) for m in members))
    return f"{tr('Matched in')}: {', '.join(labels)}"


def page_not_identified(result: Dict[str, Any]) -> bool:
    """A scholarly transcription's hit whose page could not be named: its own result, opened
    as the manuscript (never page zero)."""
    return result.get('page_identified') is False


def edition_text(row: Dict[str, Any]) -> Optional[str]:
    """The text of the edition leading *row* (a result or a member) as the sidecar holds it,
    whole -- what the reader sees for that hit, never the page's automatic transcription -- or
    None for automatic text. An edition gone from the sidecar, or changed under the match
    since the search (a restored search after a rebuild: editions_search.edition_unchanged),
    gives the text the row was found in, as far as it kept it, never another text under the
    same version (Codex review of PR #18). Blocking (a sidecar read)."""
    edition = row.get('edition') or {}
    version = edition.get('version')
    if not version:
        return None
    from shared.editions_search import edition_unchanged
    from web.editions_assets import resolver
    resolved = resolver().resolve(version)
    if resolved and resolved.get('original') and edition_unchanged(edition, resolved):
        return resolved['original']
    return row.get('full_text') or row.get('full_text_excerpt') or None


def browse_url(row: Dict[str, Any], meta_mgr) -> Optional[str]:
    """The /browse address of a scholarly transcription's hit (a result led by one, or a
    member): its page when it was identified (sys_id, fl_id, volume_ie, page), else the
    manuscript, never page zero; ``version`` (that edition, chosen before any page or coverage
    filter) and ``span`` (the match in the edition's search text, which the browse page maps
    onto the edition's own characters -- never a phrase guessed again). None without a
    manuscript."""
    from urllib.parse import urlencode
    edition = row.get('edition') or {}
    sys_id = (row.get('display') or {}).get('id') or edition.get('sys_id')
    if not sys_id:
        return None
    params = {'sys_id': sys_id}
    parsed = {}
    if meta_mgr is not None and row.get('page_identified', True) is not False:
        for source in (edition.get('page_uid'), row.get('raw_header')):
            if source:
                try:
                    parsed = meta_mgr.parse_full_id_components(source) or {}
                except Exception:
                    parsed = {}
                if parsed.get('p_num'):
                    break
    if parsed.get('fl_id'):
        params['fl_id'] = parsed['fl_id']
    if parsed.get('ie_id'):
        params['volume_ie'] = parsed['ie_id']
    if parsed.get('p_num'):
        params['page'] = parsed['p_num']
    if edition.get('version'):
        params['version'] = edition['version']
        span = edition.get('span')
        if isinstance(span, (list, tuple)) and len(span) == 2:
            params['span'] = f'{int(span[0])}-{int(span[1])}'
            if edition.get('span_fp'):
                # What the span held when the search read it: the reading page marks it
                # only while it holds the same (a rebuilt edition, same version).
                params['fp'] = edition['span_fp']
    return '/browse?' + urlencode(params)


def drop_source_choice(search_state) -> None:
    """Forget the source choice of a restored search -- its last run and every search-within
    step -- for a reader the editions are not served to: what they replay or complete then
    reads the automatic transcriptions only (an editor's preview stays an editor's)."""
    run = getattr(search_state, 'last_run', None)
    if isinstance(run, dict):
        run.pop('sources', None)
    for step in getattr(search_state, 'refinement_chain', None) or []:
        if getattr(step, 'sources', None) is not None:
            step.sources = None


def editions_count_suffix(search_state) -> str:
    """What the count line adds when the scholarly transcriptions were searched: how many
    results matched in one (never "only in one": a capped automatic search cannot know), and
    a note when they could not be searched this time."""
    from shared.editions_search import results_summary
    rows = search_state.results or []
    parts = []
    if any(isinstance(r, dict) and 'members' in r for r in rows):
        n = results_summary(rows)['with_edition']
        if n:
            parts.append(f"{n} {tr('in scholarly transcriptions')}")
    if getattr(search_state, 'editions_skipped', False):
        parts.append(tr('The scholarly transcriptions could not be searched this time'))
    return ''.join(' · ' + p for p in parts)
