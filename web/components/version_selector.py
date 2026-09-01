# -*- coding: utf-8 -*-
"""
Version Selector Component

Allows users to switch between different versions of a transcription:
- PGP transcription (curated, when available)
- V0.7 (original transcription)
- V0.8 (updated transcription)
- User corrections (approved)
"""

import asyncio
import logging
from nicegui import run, ui
from web.translations import tr, get_language
from web.supabase_client import get_corrections
from web.auth_state import GlobalAuthState
from web.corrections_service import get_pending_corrections_for_page
from web.supabase_client import get_user_client
from shared.fgp_service import (
    group_transcription_sources, source_relation_kind, pick_fgp_credit,
    choose_default_source,
)
from typing import Optional, Callable, List, Dict, Any

logger = logging.getLogger(__name__)

# FGP group accent — a distinct violet so FGP never reads as the PGP green group.
_FGP_COLOR = '#7c3aed'


def fetch_page_versions(sys_id: str, page_num: int = 1, *, client=None) -> dict:
    """Fetch all versions for a page from Supabase corrections."""
    default_response = {'all_versions': [], 'current_default': None, 'total': 0}
    try:
        # Get approved corrections for this document/page
        corrections = get_corrections(
            sys_id=sys_id, status='approved', client=client
        )
        if page_num:
            corrections = [c for c in corrections if c.get('page_number') == page_num]

        # Format as versions
        versions = []
        for c in corrections:
            profile = c.get('profiles', {}) or {}
            versions.append({
                'id': c.get('id'),
                'source': 'user',
                'user_name': profile.get('full_name') or profile.get('username') or 'Unknown',
                'content': c.get('corrected_text', ''),
                'created_at': c.get('created_at'),
                'is_current_default': False,
                'user': {
                    'full_name': profile.get('full_name'),
                    'username': profile.get('username')
                }
            })

        # Set most recent as default if exists
        current_default = versions[0] if versions else None
        if current_default:
            current_default['is_current_default'] = True

        return {
            'all_versions': versions,
            'current_default': current_default,
            'total': len(versions)
        }
    except Exception as e:
        logger.error("Error fetching versions: %s", e)
        return default_response


def fetch_document_corrections(
    document_id: str, page_number: int = None, *, client=None
) -> List[dict]:
    """Fetch approved corrections for a document from Supabase."""
    try:
        corrections = get_corrections(
            sys_id=document_id, status='approved', client=client
        )
        if page_number is not None:
            corrections = [c for c in corrections if c.get('page_number') == page_number]

        # Format for display
        formatted = []
        for c in corrections:
            profile = c.get('profiles', {}) or {}
            formatted.append({
                'id': c.get('id'),
                'corrected_text': c.get('corrected_text', ''),
                'created_at': c.get('created_at'),
                'author': {
                    'full_name': profile.get('full_name'),
                    'username': profile.get('username')
                }
            })
        return formatted
    except Exception as e:
        logger.error("Error fetching corrections: %s", e)
        return []


async def fetch_page_versions_async(sys_id: str, page_num: int = 1) -> dict:
    """Fetch approved versions off-loop with a client captured on-loop."""
    reader_client = get_user_client()
    return await run.io_bound(
        lambda: fetch_page_versions(sys_id, page_num, client=reader_client)
    )


async def fetch_version_menu_data_async(
    document_id: str, page_number: int
) -> tuple[List[dict], List[dict]]:
    """Fetch approved and current-user pending corrections off the event loop."""
    is_authenticated = GlobalAuthState.is_logged_in()
    user_id = GlobalAuthState.get_user_id() if is_authenticated else None
    reader_client = get_user_client()

    def _read():
        corrections = fetch_document_corrections(
            document_id, page_number, client=reader_client
        )
        pending_corrections = []
        if is_authenticated and user_id:
            try:
                pending_corrections = get_pending_corrections_for_page(
                    client=reader_client,
                    sys_id=document_id,
                    page_number=page_number,
                    user_id=user_id,
                )
            except Exception as e:
                logger.error("Error fetching pending corrections: %s", e)
        return corrections, pending_corrections

    return await run.io_bound(_read)


def create_version_selector(
    document_id: str,
    page_number: int,
    original_text: str,
    on_version_change: Optional[Callable[[str, dict], None]] = None,
    size: str = "sm",
    pgp_transcription: Optional[Dict[str, Any]] = None,
    all_sources: Optional[List[Dict[str, Any]]] = None,
    full_original_text: Optional[str] = None,
):
    """Create a version selector dropdown.

    Args:
        document_id: The sys_id of the document
        page_number: The page number
        original_text: The original V0.8 transcription text
        on_version_change: Callback when version is selected (text, version_info)
        size: Button size ('sm', 'md', etc.)
        pgp_transcription: Optional dict with PGP transcription data (for backward compat):
            - content: The transcription text
            - attribution: Scholar/source name
            - pgp_url: URL to PGP document
            - pgpid: PGP document ID
        all_sources: Optional list of all sources from document_sources table.
            If provided, enables multi-source display with multiple scholars and translations.
    """
    container = ui.row().classes('items-center gap-2')

    # Helper functions to separate editions and translations from all_sources.
    # PGP and FGP are split by the shared source-kind classifier so FGP editions
    # never fold into the green PGP group (they share 'Digital Edition'); FGP
    # renders as its own additive group below (FGP-03/07).
    def get_editions(sources: List[Dict]) -> List[Dict]:
        """PGP Digital Editions with content (FGP excluded — own group)."""
        return group_transcription_sources(sources)['pgp_editions']

    def get_translations(sources: List[Dict]) -> List[Dict]:
        """PGP Digital Translations with content (shown when editions exist)."""
        return group_transcription_sources(sources)['pgp_translations']

    def get_fgp_sources(sources: List[Dict]) -> List[Dict]:
        """FGP transcription sources with content (editions + any translations)."""
        groups = group_transcription_sources(sources)
        return groups['fgp_editions'] + groups['fgp_translations']

    def has_editions_for_page(sources: List[Dict]) -> bool:
        """Check if there are any PGP editions with content for current page."""
        return len(get_editions(sources)) > 0

    with container:
        # Version indicator
        version_label = ui.label('V0.8').classes('text-xs font-medium').style(
            'color: var(--text-secondary);'
        )

        async def load_and_apply_latest():
            """Load versions and apply the latest/default one."""
            # If multi-source available, use first edition as default
            if all_sources:
                editions = get_editions(all_sources)
                if editions:
                    first_edition = editions[0]
                    attribution = first_edition.get('source_scholar', 'PGP')
                    version_label.text = 'PGP'
                    version_label.style('color: var(--q-positive);')  # Green for verified
                    if on_version_change:
                        on_version_change(first_edition.get('content', ''), {
                            'source': 'pgp',
                            'attribution': attribution,
                            'pgp_url': pgp_transcription.get('pgp_url') if pgp_transcription else None,
                            'pgpid': first_edition.get('pgpid'),
                            'is_pgp': True,
                            'is_default': True,
                            'source_id': first_edition.get('id')
                        })
                    return

                # No PGP edition — fall back to FGP (additive, PGP-first rule),
                # UNLESS the FGP edition is a partial/selected excerpt of the
                # folio: choose_default_source demotes low-coverage FGP below the
                # V0.8/HTR default so the reader sees the fuller MiDRASH
                # transcription (SEED-030). The FGP source stays in the menu.
                fgp_sources = get_fgp_sources(all_sources)
                if fgp_sources:
                    _decision = choose_default_source(all_sources, original_text, full_htr_getter=lambda: full_original_text)
                    if _decision['eligible'] or _decision['reason'] == 'no_fgp_edition':
                        # Default to the coverage-cleared FGP edition; when there
                        # is no FGP *edition* (translation-only), preserve the
                        # prior behavior and default to the first FGP source.
                        first_fgp = _decision['source'] or fgp_sources[0]
                        version_label.text = 'FGP'
                        version_label.style(f'color: {_FGP_COLOR};')
                        # Same bilingual credit + translation labelling as the menu
                        # path, so the initial auto-loaded FGP source shows the same
                        # metadata it would after the user reselects it (Codex #309 P2).
                        _attr = first_fgp.get('attribution') or first_fgp.get('source_scholar', 'FGP')
                        _credit = pick_fgp_credit(first_fgp, get_language()) or _attr
                        if on_version_change:
                            on_version_change(first_fgp.get('content', ''), {
                                'source': 'fgp',
                                'attribution': _attr,
                                'source_credit': _credit,
                                'is_fgp': True,
                                'is_translation': source_relation_kind(first_fgp) == 'translation',
                                'is_default': True,
                                'source_id': first_fgp.get('id'),
                                'uid': first_fgp.get('uid'),
                            })
                        return
                    # else: every FGP edition is a low-coverage excerpt of this
                    # folio → fall through to the HTR/V0.8 default (the FGP rows
                    # stay selectable in the menu, tagged "shorter than V0.8").

            # Fallback to pgp_transcription for backward compatibility
            if pgp_transcription and pgp_transcription.get('content'):
                attribution = pgp_transcription.get('attribution', 'PGP')
                version_label.text = 'PGP'
                version_label.style('color: var(--q-positive);')  # Green for verified
                if on_version_change:
                    on_version_change(pgp_transcription['content'], {
                        'source': 'pgp',
                        'attribution': attribution,
                        'pgp_url': pgp_transcription.get('pgp_url'),
                        'pgpid': pgp_transcription.get('pgpid'),
                        'is_pgp': True,
                        'is_default': True
                    })
                return

            # Fall back to user corrections or V0.8
            versions_data = await fetch_page_versions_async(
                document_id, page_number
            )
            current_default = versions_data.get('current_default')

            if current_default:
                user_info = current_default.get('user', {})
                user_name = user_info.get('full_name') or user_info.get('username', 'User')
                version_label.text = f"{tr('by')} {user_name}"
                if on_version_change:
                    on_version_change(current_default.get('content', original_text), {
                        'source': 'user', 'version_id': current_default.get('id'), 'author': user_name, 'is_default': True
                    })
            else:
                version_label.text = 'V0.8'

        async def _safe_load():
            try:
                await load_and_apply_latest()
            except RuntimeError:
                pass  # Parent element was deleted (NiceGUI timer lifecycle)

        # Use call_later instead of ui.timer to avoid parent_slot RuntimeError
        # when content_container.clear() destroys the timer's parent element
        asyncio.get_event_loop().call_later(
            0.1, lambda: asyncio.create_task(_safe_load())
        )

        with ui.button(icon='history').props(f'flat dense size={size}').tooltip(tr('Version History')) as btn:
            menu = ui.menu()
            with menu:
                ui.menu_item(tr('Loading...')).props('disable')

            async def load_versions():
                corrections, pending_corrections = (
                    await fetch_version_menu_data_async(
                        document_id, page_number
                    )
                )

                # Rebuild the menu
                menu.clear()
                with menu:
                    # Check if we have multi-source data
                    editions = get_editions(all_sources) if all_sources else []
                    translations = get_translations(all_sources) if all_sources else []
                    has_multi_source = len(editions) > 0 or len(translations) > 0

                    # PGP Transcriptions section (multi-source mode)
                    if has_multi_source and editions:
                        # Section header for multiple editions
                        if len(editions) > 1:
                            ui.label(tr('PGP Transcriptions')).classes('text-xs px-4 py-1 font-semibold').style('color: var(--q-positive);')

                        for idx, edition in enumerate(editions):
                            scholar = edition.get('source_scholar', 'Unknown')

                            def make_select_edition(ed=edition, scholar_name=scholar):
                                def select_edition():
                                    version_label.text = 'PGP'
                                    version_label.style('color: var(--q-positive);')
                                    menu.close()
                                    if on_version_change:
                                        on_version_change(ed.get('content', ''), {
                                            'source': 'pgp',
                                            'attribution': scholar_name,
                                            'pgp_url': pgp_transcription.get('pgp_url') if pgp_transcription else None,
                                            'pgpid': ed.get('pgpid'),
                                            'is_pgp': True,
                                            'source_id': ed.get('id')
                                        })
                                return select_edition

                            with ui.menu_item(on_click=make_select_edition()).classes('text-sm'):
                                with ui.row().classes('items-center gap-2'):
                                    ui.icon('verified', size='xs').classes('text-green-600')
                                    with ui.column().classes('gap-0'):
                                        if len(editions) == 1:
                                            ui.label(tr('PGP Transcription')).classes('font-medium text-green-700')
                                        ui.label(f"{tr('by')} {scholar}").classes('text-xs' if len(editions) == 1 else 'text-sm').style('color: var(--text-muted);' if len(editions) == 1 else '')
                                        # External link to PGP website (only for first edition or single)
                                        if idx == 0 and pgp_transcription and pgp_transcription.get('pgp_url'):
                                            with ui.row().classes('items-center gap-1'):
                                                # Stop propagation CLIENT-side so the external link doesn't
                                                # trigger the parent menu_item's select_edition.
                                                with ui.link(target=pgp_transcription.get('pgp_url'), new_tab=True).classes('ml-1').on('click', js_handler='(e) => e.stopPropagation()'):
                                                    ui.icon('open_in_new', size='xs').classes('text-green-600')
                                                    ui.tooltip(tr('View on PGP'))

                        ui.separator()

                        # Translations section
                        if translations:
                            ui.label(tr('Translations')).classes('text-xs px-4 py-1 font-semibold').style('color: var(--q-primary);')

                            # Group by language
                            hebrew_trans = [t for t in translations if t.get('language') == 'Hebrew']
                            english_trans = [t for t in translations if t.get('language') == 'English']
                            other_trans = [t for t in translations if t.get('language') not in ('Hebrew', 'English')]

                            for lang_group, lang_name, lang_key in [
                                (hebrew_trans, 'Hebrew', 'Hebrew Translation'),
                                (english_trans, 'English', 'English Translation'),
                                (other_trans, 'Other', 'Translation')
                            ]:
                                for trans in lang_group:
                                    scholar = trans.get('source_scholar', 'Unknown')
                                    language = trans.get('language', lang_name)

                                    def make_select_translation(tr_data=trans, scholar_name=scholar, lang=language):
                                        def select_translation():
                                            version_label.text = f"{lang} {tr('Translation')}"
                                            version_label.style('color: var(--q-primary);')
                                            menu.close()
                                            if on_version_change:
                                                on_version_change(tr_data.get('content', ''), {
                                                    'source': 'translation',
                                                    'attribution': scholar_name,
                                                    'language': lang,
                                                    'pgpid': tr_data.get('pgpid'),
                                                    'is_translation': True,
                                                    'source_id': tr_data.get('id')
                                                })
                                        return select_translation

                                    with ui.menu_item(on_click=make_select_translation()).classes('text-sm'):
                                        with ui.row().classes('items-center gap-2'):
                                            ui.icon('translate', size='xs').classes('text-blue-600')
                                            with ui.column().classes('gap-0'):
                                                ui.label(f"{tr(lang_key)}").classes('text-sm')
                                                ui.label(f"{tr('by')} {scholar}").classes('text-xs').style('color: var(--text-muted);')

                            ui.separator()

                    # Fallback: Single PGP transcription (backward compatibility mode)
                    elif pgp_transcription and pgp_transcription.get('content'):
                        def select_pgp():
                            version_label.text = 'PGP'
                            version_label.style('color: var(--q-positive);')
                            menu.close()
                            if on_version_change:
                                on_version_change(pgp_transcription['content'], {
                                    'source': 'pgp',
                                    'attribution': pgp_transcription.get('attribution', 'PGP'),
                                    'pgp_url': pgp_transcription.get('pgp_url'),
                                    'pgpid': pgp_transcription.get('pgpid'),
                                    'is_pgp': True
                                })

                        with ui.menu_item(on_click=select_pgp).classes('text-sm'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('verified', size='xs').classes('text-green-600')
                                with ui.column().classes('gap-0'):
                                    ui.label(tr('PGP Transcription')).classes('font-medium text-green-700')
                                    attribution = pgp_transcription.get('attribution', '')
                                    if attribution:
                                        with ui.row().classes('items-center gap-1'):
                                            ui.label(f"{tr('Transcription by')} {attribution}").classes('text-xs').style('color: var(--text-muted);')
                                            # External link to PGP website
                                            pgp_url = pgp_transcription.get('pgp_url')
                                            if pgp_url:
                                                # Stop propagation CLIENT-side so the external link doesn't
                                                # trigger the parent menu_item's select_pgp; server-side
                                                # GenericEventArguments has no stop_propagation.
                                                with ui.link(target=pgp_url, new_tab=True).classes('ml-1').on('click', js_handler='(e) => e.stopPropagation()'):
                                                    ui.icon('open_in_new', size='xs').classes('text-green-600')
                                                    ui.tooltip(tr('View on PGP'))

                        ui.separator()

                    # FGP Transcriptions group — its own violet group/badge so it
                    # is never mistaken for the green PGP group (FGP-07). Additive:
                    # rendered after the PGP section, before V0.8.
                    fgp_sources = get_fgp_sources(all_sources) if all_sources else []
                    if fgp_sources:
                        # If the FGP edition was demoted below V0.8 for low folio
                        # coverage (SEED-030), tag it so the reader knows why it is
                        # not the default (phrased "shorter than V0.8" — the HTR
                        # baseline is imperfect, so we do not overclaim "partial").
                        _fgp_dec = choose_default_source(all_sources, original_text, full_htr_getter=lambda: full_original_text)
                        _fgp_demoted = (not _fgp_dec['eligible']
                                        and _fgp_dec['reason'] == 'demote_low_coverage')
                        if len(fgp_sources) > 1 or editions:
                            ui.label(tr('FGP Transcriptions')).classes(
                                'text-xs px-4 py-1 font-semibold'
                            ).style(f'color: {_FGP_COLOR};')
                        for fed in fgp_sources:
                            attribution = fed.get('attribution') or fed.get('source_scholar') or 'FGP'
                            # Specific FGP team credit (e.g. "יעקב זוסמן, ראש צוות
                            # FGP…"), in the UI language (HE/EN bilingual split,
                            # falling back across languages then to the generic
                            # attribution).
                            credit = pick_fgp_credit(fed, get_language()) or attribution
                            # An FGP source can be a transcription OR a translation
                            # (e.g. a Hebrew translation of a Judeo-Arabic letter).
                            # Label it by its actual kind — never call a translation
                            # a "transcription" (matches desktop + reading desk).
                            _is_trans = source_relation_kind(fed) == 'translation'
                            _lang = fed.get('language') or ''
                            if _is_trans:
                                _fgp_main = f"FGP {_lang} {tr('Translation')}".replace('  ', ' ').strip()
                                _fgp_icon = 'translate'
                            else:
                                _fgp_main = tr('FGP Transcription')
                                _fgp_icon = 'menu_book'

                            def make_select_fgp(ed=fed, attr=attribution, cr=credit, is_tr=_is_trans):
                                def select_fgp():
                                    version_label.text = 'FGP'
                                    version_label.style(f'color: {_FGP_COLOR};')
                                    menu.close()
                                    if on_version_change:
                                        on_version_change(ed.get('content', ''), {
                                            'source': 'fgp',
                                            'attribution': attr,
                                            'source_credit': cr,
                                            'is_fgp': True,
                                            'is_translation': is_tr,
                                            'source_id': ed.get('id'),
                                            'uid': ed.get('uid'),
                                        })
                                return select_fgp

                            with ui.menu_item(on_click=make_select_fgp()).classes('text-sm'):
                                with ui.row().classes('items-center gap-2'):
                                    ui.icon(_fgp_icon, size='xs').style(f'color: {_FGP_COLOR};')
                                    with ui.column().classes('gap-0'):
                                        # No folio suffix: the chooser is already
                                        # filtered to the displayed image's folio,
                                        # so the folio only routes placement.
                                        ui.label(_fgp_main).classes(
                                            'font-medium'
                                        ).style(f'color: {_FGP_COLOR};')
                                        ui.label(credit).classes('text-xs').style(
                                            'color: var(--text-muted);'
                                        )
                                        # SEED-030: demoted (partial) FGP edition.
                                        if _fgp_demoted and not _is_trans:
                                            ui.label(tr('shorter than V0.8')).classes(
                                                'text-xs italic'
                                            ).style('color: var(--text-muted);')
                        ui.separator()

                    # Original V0.8
                    def select_original():
                        version_label.text = 'V0.8'
                        version_label.style('color: var(--text-secondary);')
                        menu.close()
                        if on_version_change:
                            on_version_change(original_text, {'source': 'V0.8', 'is_original': True})
                    ui.menu_item(f"V0.8 ({tr('Original')})", on_click=select_original).classes('text-sm')

                    # User corrections
                    if corrections:
                        ui.separator()
                        ui.label(tr('User Corrections')).classes('text-xs px-4 py-1').style('color: var(--text-muted);')

                        for corr in corrections:
                            author = corr.get('author', {})
                            author_name = author.get('full_name') or author.get('username') or 'Unknown'
                            date = corr.get('created_at', '')

                            def make_select_correction(c=corr, name=author_name):
                                def select_correction():
                                    version_label.text = f"{tr('by')} {name}"
                                    version_label.style('color: var(--text-secondary);')
                                    menu.close()
                                    if on_version_change:
                                        on_version_change(c.get('corrected_text', ''), {
                                            'source': 'user', 'correction_id': c.get('id'), 'author': name
                                        })
                                return select_correction

                            with ui.menu_item(on_click=make_select_correction()).classes('text-sm'):
                                with ui.column().classes('gap-0'):
                                    ui.label(author_name).classes('font-medium')
                                    if date:
                                        ui.label(str(date)[:10]).classes('text-xs').style('color: var(--text-muted);')

                    # Pending corrections (only for logged-in user)
                    if pending_corrections:
                        ui.separator()
                        ui.label(tr('My Pending Corrections')).classes(
                            'text-xs px-4 py-1 font-semibold'
                        ).style('color: var(--q-warning);')

                        for pc in pending_corrections:
                            status_label = pc.get('status', 'pending').replace('_', ' ').title()
                            date = pc.get('created_at', '')

                            def make_select_pending(c=pc, status=status_label):
                                def select_pending():
                                    version_label.text = f"{tr('Pending')} ({status})"
                                    version_label.style('color: var(--q-warning);')
                                    menu.close()
                                    if on_version_change:
                                        on_version_change(c.get('corrected_text', ''), {
                                            'source': 'pending',
                                            'correction_id': c.get('id'),
                                            'status': c.get('status'),
                                            'is_pending': True
                                        })
                                return select_pending

                            with ui.menu_item(on_click=make_select_pending()).classes('text-sm'):
                                with ui.row().classes('items-center gap-2'):
                                    ui.icon('schedule', size='xs').classes('text-amber-600')
                                    with ui.column().classes('gap-0'):
                                        ui.label(f"{tr('Pending')} ({status_label})").classes(
                                            'text-sm text-amber-700'
                                        )
                                        if date:
                                            ui.label(str(date)[:10]).classes(
                                                'text-xs'
                                            ).style('color: var(--text-muted);')

                    if not corrections and not pending_corrections:
                        ui.separator()
                        ui.menu_item(tr('No other versions')).props('disable').classes('text-sm')

            btn.on('click', load_versions)

    return container


def create_version_badge(source: str = 'original', author: str = None):
    """Create a badge showing the current version source."""
    if source == 'pgp':
        return ui.badge('PGP').props('color=positive').classes('text-xs')
    elif source == 'fgp':
        return ui.badge('FGP').props('color=deep-purple').classes('text-xs')
    elif source in ('original', 'V0.8'):
        return ui.badge('V0.8').props('color=grey').classes('text-xs')
    elif source == 'V0.7':
        return ui.badge('V0.7').props('color=grey-7').classes('text-xs')
    else:
        label = f"{tr('by')} {author}" if author else tr('User correction')
        return ui.badge(label).props('color=blue').classes('text-xs')
