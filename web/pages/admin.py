# -*- coding: utf-8 -*-
"""
Admin Panel - Dicta Genizah Search

User management, corrections review, and system administration for admins.
Uses Supabase directly for all data operations.
"""

import asyncio
import logging
from nicegui import run, ui
from web.translations import get_language, tr
from web.auth_state import GlobalAuthState
from web.state import state
from web.components.typography import h1, h2, h3
from web.components import discovery_links
from web.components.identification_review import (
    direct_novelty_text,
    relation_verdict_text,
)
from web.identification_reviews import (
    DIRECT_NOVELTY_ALREADY_KNOWN,
    DIRECT_NOVELTY_OTHER_UNSURE,
    DIRECT_NOVELTY_POTENTIALLY_NEW,
    RELATION_DIRECT_WITNESS,
    RELATION_MANUSCRIPT_QUOTES_WORK,
    RELATION_NOT_MEANINGFUL,
    RELATION_OTHER_UNSURE,
    RELATION_SHARED_SOURCE,
    RELATION_WORK_QUOTES_MANUSCRIPT,
    moderate_review,
    pending_reviews as fetch_pending_identification_reviews,
)
from web.supabase_client import get_client, get_user_client

logger = logging.getLogger(__name__)


_REVIEW_ADMIN_COPY = {
    "queue_empty": {"en": "No pending identification reviews",
                    "he": "אין בדיקות התאמה הממתינות לאישור"},
    "queue_empty_note": {"en": "All submitted match reviews have been checked",
                         "he": "כל בדיקות ההתאמה שהוגשו כבר נבדקו"},
    "pending": {"en": "identification reviews pending",
                "he": "בדיקות התאמה ממתינות"},
    "anonymous": {"en": "Anonymous", "he": "משתמש/ת אנונימי/ת"},
    "registered": {"en": "Registered user", "he": "משתמש/ת רשום/ה"},
    "finding_id": {"en": "Identification ID", "he": "מזהה ההתאמה"},
    "catalogue_context": {"en": "Catalogue and artifact identifiers",
                          "he": "מזהי קטלוג ונתוני ההתאמה"},
    "sys_id": {"en": "System ID", "he": "מזהה מערכת"},
    "page_id": {"en": "Page ID", "he": "מזהה דף"},
    "page_number": {"en": "Page number", "he": "מספר דף"},
    "work_id": {"en": "Work ID", "he": "מזהה חיבור"},
    "evidence_id": {"en": "Evidence ID", "he": "מזהה ראיה"},
    "excerpt_page_id": {"en": "Excerpt page ID", "he": "מזהה דף הקטע"},
    "data_version": {"en": "Data version", "he": "גרסת נתונים"},
    "displayed_relation": {"en": "Displayed by the site",
                           "he": "הקשר שהוצג באתר"},
    "submitted": {"en": "Submitted", "he": "נשלח"},
    "submitted_assessment": {"en": "Submitted assessment",
                             "he": "ההערכה שנשלחה"},
    "moderated_assessment": {"en": "Assessment to publish",
                             "he": "ההערכה שתפורסם"},
    "relation": {"en": "How well the match fits",
                 "he": "עד כמה ההתאמה נכונה"},
    "novelty": {"en": "How new the identification is",
                "he": "עד כמה הזיהוי חדש"},
    "no_novelty": {"en": "Not assessed", "he": "לא הוערך"},
    "comment": {"en": "Comment", "he": "הערה"},
    "publish_comment": {"en": "Publish this comment",
                        "he": "לפרסם את ההערה"},
    "publish_comment_note": {
        "en": "The structured assessment is published on approval. The comment remains private unless this is checked.",
        "he": "ההערכה המובנית תפורסם לאחר האישור. ההערה תישאר פרטית אלא אם אפשרות זו מסומנת.",
    },
    "manuscript_text": {"en": "Manuscript text", "he": "טקסט כתב היד"},
    "work_text": {"en": "Compared work text", "he": "טקסט החיבור להשוואה"},
    "no_excerpt": {"en": "No text comparison is available for this identification.",
                   "he": "אין קטעי טקסט זמינים להשוואה זו."},
    "excerpt_unavailable": {"en": "The text comparison could not be loaded.",
                            "he": "לא ניתן לטעון את קטעי הטקסט להשוואה."},
    "work_text_unavailable": {"en": "No work-side excerpt is available.",
                              "he": "אין קטע זמין מצד החיבור."},
    "version_mismatch": {
        "en": "This vote was submitted against a different data version; the excerpts below come from the currently loaded asset.",
        "he": "ההצבעה נשלחה ביחס לגרסת נתונים אחרת; קטעי הטקסט שלהלן מגיעים מהגרסה הטעונה כעת.",
    },
    "private_note": {"en": "Private moderation note", "he": "הערת ניהול פרטית"},
    "approve": {"en": "Approve assessment", "he": "אישור ההערכה"},
    "reject": {"en": "Reject", "he": "דחייה"},
    "update_failed": {"en": "The review could not be updated",
                      "he": "לא ניתן לעדכן את הבדיקה"},
    "approved": {"en": "Identification review approved",
                 "he": "בדיקת ההתאמה אושרה"},
    "rejected": {"en": "Identification review rejected",
                 "he": "בדיקת ההתאמה נדחתה"},
}


def _review_admin_text(key: str, lang: str) -> str:
    return _REVIEW_ADMIN_COPY[key]["he" if lang == "he" else "en"]


def get_shelfmark_for_id(sys_id: str) -> tuple:
    """Get shelfmark and title for a system ID."""
    try:
        if state.meta_mgr:
            shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)
            return shelfmark or sys_id, title or ''
    except Exception:
        pass  # Translation lookup failed; continue without translation
    return sys_id, ''


def get_pending_corrections():
    """Get pending corrections from Supabase.

    Uses separate queries for corrections and profiles because there is no
    direct FK between corrections.author_id and profiles.id (both reference
    auth.users(id) independently).
    """
    try:
        client = get_client()
        response = client.table('corrections').select('*').eq(
            'status', 'pending'
        ).order('created_at', desc=True).execute()
        corrections = response.data or []
        if not corrections:
            return []
        # Enrich with profile data
        author_ids = list(set(c.get('author_id') for c in corrections if c.get('author_id')))
        if author_ids:
            profiles_resp = client.table('profiles').select(
                'id, username, full_name'
            ).in_('id', author_ids).execute()
            profiles_map = {p['id']: p for p in (profiles_resp.data or [])}
            for c in corrections:
                c['profiles'] = profiles_map.get(c.get('author_id'), {})
        return corrections
    except Exception as e:
        logger.error("Error fetching pending corrections: %s", e)
        return []


def get_all_users():
    """Get all users from Supabase profiles table."""
    try:
        client = get_client()
        response = client.table('profiles').select('*').order('created_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        logger.error("Error fetching users: %s", e)
        return []


def get_all_corrections_count():
    """Get total corrections count."""
    try:
        client = get_client()
        response = client.table('corrections').select('id', count='exact').execute()
        return response.count or 0
    except Exception as e:
        logger.error("Error fetching corrections count: %s", e)
        return 0


def update_correction_status(correction_id: int, status: str, review_notes: str = None,
                             rejection_reason: str = None, client=None):
    """Update correction status in Supabase.

    Up to four round trips (update, then read + write reputation). Runs in a
    worker thread, so `client` is resolved by the caller on the event loop --
    see get_pending_identification_reviews for why building it here would
    silently produce an anonymous client.
    """
    try:
        client = get_user_client() if client is None else client
        data = {'status': status}
        if review_notes:
            data['notes'] = review_notes
        if rejection_reason:
            data['rejection_reason'] = rejection_reason

        # Get current user as reviewer
        user_id = GlobalAuthState.get_user_id()
        if user_id:
            data['reviewed_by'] = user_id
            from datetime import datetime, timezone
            data['reviewed_at'] = datetime.now(timezone.utc).isoformat()

        response = client.table('corrections').update(data).eq('id', correction_id).execute()

        # Increment author reputation when approving correction
        if status == 'approved' and response.data:
            try:
                correction = response.data[0]
                author_id = correction.get('author_id')

                if author_id:
                    # Get current reputation
                    profile_response = client.table('profiles').select('reputation').eq('id', author_id).single().execute()
                    current_reputation = profile_response.data.get('reputation', 0) if profile_response.data else 0

                    # Increment reputation by 1
                    client.table('profiles').update({'reputation': current_reputation + 1}).eq('id', author_id).execute()
            except Exception as e:
                logger.warning("Failed to update reputation for correction %s: %s", correction_id, e)
                # Don't fail the approval if reputation update fails

        return {'success': True} if response.data else {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def update_user_role(user_id: str, new_role: str, client=None):
    """Update user role in Supabase profiles. `client` per update_correction_status."""
    try:
        client = get_user_client() if client is None else client
        response = client.table('profiles').update({'role': new_role}).eq('id', user_id).execute()
        return {'success': True} if response.data else {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_user(user_id: str, client=None):
    """Delete user from Supabase (requires service role, typically done via dashboard).

    `client` per update_correction_status.
    """
    # Note: Deleting auth users requires the service role key
    # For now, we'll just mark them or remove from profiles
    try:
        client = get_user_client() if client is None else client
        # Delete profile (user can't access anything without profile)
        response = client.table('profiles').delete().eq('id', user_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def get_pending_identification_reviews(client=None):
    """Fetch the admin-only queue with the authenticated request client.

    `client` is resolved by the CALLER, on the event loop, and passed in. This
    function now runs inside `run.io_bound`, and `get_user_client()` reads the
    auth session through `safe_user_get`, which returns {} outside the NiceGUI
    context -- so building it here would silently downgrade the moderation queue
    to an ANONYMOUS client and return an empty queue that looks like "nothing
    pending". The argument stays optional so the function is still usable on the
    loop, where resolving it here is correct.
    """
    try:
        return fetch_pending_identification_reviews(
            client=get_user_client() if client is None else client)
    except Exception as e:
        logger.error("Error fetching identification reviews: %s", type(e).__name__)
        return ()


def _load_admin_data(user_client):
    """Every Supabase round trip the Admin page needs, in ONE worker thread.

    Gathered here rather than inside each tab builder for two reasons, both
    visible in the 2026-08-19 perf-watch logs where /admin logged 2-4.5 s
    responses with the event loop blocked for most of it:

    * **Blocking.** These are synchronous round trips to the Supabase cloud, and
      NiceGUI builds a page ON the event loop. uvicorn runs a single worker, so
      each one stalled every concurrent request -- including static assets --
      for its full duration. `run.io_bound` is the same shape the review-decision
      handler already uses.
    * **Duplication.** The Statistics tab re-fetched the users and the pending
      corrections that the Users and Pending tabs had already loaded, so one page
      build made SIX loader calls -- up to eight queries, since the corrections
      loader issues two -- to show four numbers. Fetching once and sharing makes
      it four calls and at most five queries.

    Deliberately sequential inside the single thread rather than four concurrent
    ones: the anonymous Supabase client is a module-level singleton, and nothing
    in this repo establishes that concurrent use of it is safe. Serial-but-off-
    the-loop already removes the defect that mattered.
    """
    return {
        'pending_corrections': get_pending_corrections(),
        'identification_reviews': get_pending_identification_reviews(user_client),
        'users': get_all_users(),
        'total_corrections': get_all_corrections_count(),
    }


async def create_admin_page():
    """Create the Admin Panel page."""

    # Check if user is admin
    if not GlobalAuthState.is_admin():
        with ui.column().classes('w-full max-w-3xl mx-auto gap-8 fade-in items-center py-12'):
            ui.icon('lock').classes('text-6xl').style('color: var(--text-muted);')
            h2(tr('Access Denied'), classes='text-2xl font-bold', style='color: var(--text-primary);')
            ui.label(tr('You need admin privileges to access this page')).style('color: var(--text-secondary);')
            ui.button(tr('Go Home'), on_click=lambda: ui.navigate.to('/')).props('color=primary')
        return

    # Resolve the authenticated client HERE, on the loop -- see
    # get_pending_identification_reviews for why it cannot be built in the
    # worker thread -- then do every round trip off the loop, once.
    try:
        user_client = get_user_client()
    except Exception as e:
        logger.error("Admin page: no authenticated client (%s)", type(e).__name__)
        user_client = None
    data = await run.io_bound(_load_admin_data, user_client)

    with ui.column().classes('w-full max-w-6xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                h1(tr('Admin Panel'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('User management and system administration')).style('color: var(--text-secondary);')

        # === Tabs ===
        with ui.tabs().classes('w-full') as tabs:
            pending_tab = ui.tab(tr('Pending Corrections'))
            identification_reviews_tab = ui.tab(tr('Identification Reviews'))
            users_tab = ui.tab(tr('Users'))
            stats_tab = ui.tab(tr('Statistics'))

        with ui.tab_panels(tabs, value=pending_tab).classes('w-full'):
            # Pending Corrections panel
            with ui.tab_panel(pending_tab):
                await create_pending_corrections_view(data['pending_corrections'])

            with ui.tab_panel(identification_reviews_tab):
                await create_identification_reviews_view(
                    data['identification_reviews'])

            # All Users panel
            with ui.tab_panel(users_tab):
                await create_users_list_view(data['users'])

            # Statistics panel
            with ui.tab_panel(stats_tab):
                await create_stats_view(
                    data['users'], data['pending_corrections'],
                    data['total_corrections'])


async def create_pending_corrections_view(pending=None):
    """View for reviewing pending corrections.

    `pending` is preloaded off the event loop by create_admin_page; the fallback
    keeps the view self-contained if it is ever rendered on its own.
    """
    if pending is None:
        pending = await run.io_bound(get_pending_corrections)

    if not pending:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
            h3(tr('No pending corrections'), classes='text-xl', style='color: var(--text-secondary);')
            ui.label(tr('All corrections have been reviewed')).style('color: var(--text-muted);')
    else:
        h3(f"{len(pending)} {tr('corrections pending review')}", classes='text-lg font-medium mb-4')

        for corr in pending:
            await create_pending_correction_card(corr)


async def create_identification_reviews_view(pending=None):
    """Moderation queue for the public computed-identification beta."""
    if pending is None:
        # get_user_client() on the loop, the fetch off it -- never both in the
        # worker, or the queue silently comes back anonymous and empty.
        pending = await run.io_bound(
            get_pending_identification_reviews, get_user_client())
    lang = get_language()

    if not pending:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('fact_check').classes('text-6xl').style(
                'color: var(--success);')
            h3(_review_admin_text('queue_empty', lang), classes='text-xl',
               style='color: var(--text-secondary);')
            ui.label(_review_admin_text('queue_empty_note', lang)).style(
                'color: var(--text-muted);')
        return

    h3(f"{len(pending)} {_review_admin_text('pending', lang)}",
       classes='text-lg font-medium mb-4')
    evidence = await asyncio.gather(*(
        _load_identification_review_excerpt(review) for review in pending
    ))
    for review, excerpt in zip(pending, evidence):
        create_identification_review_card(review, lang, excerpt)


async def _load_identification_review_excerpt(review):
    """Load the frozen sidecar's two-pane evidence for one pending vote."""
    identification_id = str(review.get('identification_id') or '')
    if not identification_id:
        return None
    try:
        from web.discovery import get_excerpt_enveloped

        return await get_excerpt_enveloped(identification_id)
    except Exception as e:
        logger.info("Admin review excerpt unavailable (%s)", type(e).__name__)
        return None


def _review_excerpt_row(envelope):
    if not envelope or envelope.get('status') != 'ok':
        return None
    rows = list(envelope.get('items') or ())
    return dict(rows[0]) if rows else {}


def _review_excerpt_text(row, side: str) -> str:
    return ''.join(str(row.get(f'{side}_{part}') or '')
                   for part in ('before', 'span', 'after')).strip()


def _render_review_evidence(review, lang: str, envelope) -> None:
    """The manuscript/work text panes plus excerpt-level artifact ids."""
    row = _review_excerpt_row(envelope)
    if row is None:
        ui.label(_review_admin_text('excerpt_unavailable', lang)).classes(
            'text-sm').style('color: var(--text-muted);')
        return
    if not row:
        ui.label(_review_admin_text('no_excerpt', lang)).classes('text-sm').style(
            'color: var(--text-muted);')
        return

    with ui.element('div').classes('w-full').style(
        'display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); '
        'gap: 12px; min-width: 0;'
    ):
        with ui.column().classes('flex-1 min-w-80 gap-2 p-3 rounded').style(
            'background: var(--surface-secondary); '
            'border: 1px solid var(--border-light);'
        ):
            ui.label(_review_admin_text('manuscript_text', lang)).classes(
                'font-semibold')
            manuscript = _review_excerpt_text(row, 'frag')
            ui.label(manuscript or _review_admin_text('no_excerpt', lang)).classes(
                'whitespace-pre-wrap text-base').props('dir=rtl').style(
                    'line-height: 1.8;')
        with ui.column().classes('flex-1 min-w-80 gap-2 p-3 rounded').style(
            'background: var(--surface-secondary); '
            'border: 1px solid var(--border-light);'
        ):
            ui.label(_review_admin_text('work_text', lang)).classes('font-semibold')
            work = _review_excerpt_text(row, 'work')
            ui.label(work or _review_admin_text(
                'work_text_unavailable', lang)).classes(
                    'whitespace-pre-wrap text-base').props('dir=rtl').style(
                        'line-height: 1.8;')
            if row.get('attribution'):
                ui.label(str(row['attribution'])).classes('text-xs').style(
                    'color: var(--text-muted);')

    excerpt_ids = (
        ('evidence_id', row.get('evidence_id')),
        ('excerpt_page_id', row.get('a_page_id')),
    )
    with ui.element('div').classes('w-full').style(
        'display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); '
        'gap: 4px 24px; min-width: 0;'
    ):
        for label_key, value in excerpt_ids:
            if value:
                ui.label(
                    f"{_review_admin_text(label_key, lang)}: {value}"
                ).classes('text-xs font-mono').style(
                    'color: var(--text-muted); overflow-wrap: anywhere; '
                    'word-break: break-all; min-width: 0;')


def _render_review_identifiers(review, lang: str) -> None:
    fields = (
        ('sys_id', review.get('sys_id')),
        ('page_id', review.get('page_id')),
        ('page_number', review.get('page_number')),
        ('work_id', review.get('work_id')),
        ('data_version', review.get('sidecar_version')),
        ('displayed_relation', review.get('displayed_relation')),
        ('submitted', review.get('submitted_at')),
    )
    ui.label(_review_admin_text('catalogue_context', lang)).classes(
        'text-sm font-semibold')
    with ui.element('div').classes('w-full').style(
        'display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); '
        'gap: 8px 24px; min-width: 0;'
    ):
        for label_key, value in fields:
            with ui.column().classes('gap-0').style('min-width: 0;'):
                ui.label(_review_admin_text(label_key, lang)).classes(
                    'text-xs').style('color: var(--text-muted);')
                ui.label(str(value or '-')).classes(
                    'text-sm font-mono').props('dir=auto').style(
                        'overflow-wrap: anywhere; word-break: break-all; '
                        'max-width: 100%;')


def create_identification_review_card(review, lang: str, excerpt=None):
    """One editable assessment with the evidence needed to adjudicate it."""
    sys_id = str(review.get('sys_id') or '')
    page_number = review.get('page_number')
    shelfmark, title = get_shelfmark_for_id(sys_id) if sys_id else ('', '')
    target = discovery_links.browse_url(sys_id, page=page_number) if sys_id else None
    relation_order = (
        RELATION_DIRECT_WITNESS,
        RELATION_MANUSCRIPT_QUOTES_WORK,
        RELATION_SHARED_SOURCE,
        RELATION_WORK_QUOTES_MANUSCRIPT,
        RELATION_NOT_MEANINGFUL,
        RELATION_OTHER_UNSURE,
    )
    novelty_order = (
        DIRECT_NOVELTY_POTENTIALLY_NEW,
        DIRECT_NOVELTY_ALREADY_KNOWN,
        DIRECT_NOVELTY_OTHER_UNSURE,
    )
    relation_options = {
        value: relation_verdict_text(value, lang) for value in relation_order
    }
    no_novelty = '__not_assessed__'
    novelty_options = {
        no_novelty: _review_admin_text('no_novelty', lang),
        **{value: direct_novelty_text(value, lang) for value in novelty_order},
    }

    with ui.card().classes('w-full p-4 mb-4'):
        with ui.column().classes('w-full gap-3'):
            with ui.row().classes('w-full items-center justify-between gap-3'):
                with ui.column().classes('gap-0'):
                    if target:
                        ui.link(shelfmark or sys_id, target).classes(
                            'font-bold text-primary')
                    elif shelfmark or sys_id:
                        ui.label(shelfmark or sys_id).classes('font-bold')
                    if title:
                        ui.label(title).classes('text-sm').props('dir=auto').style(
                            'color: var(--text-secondary);')
                ui.badge(
                    _review_admin_text(
                        'anonymous' if not review.get('reviewer_user_id')
                        else 'registered', lang)
                ).props('outline')

            with ui.column().classes('w-full gap-1 p-3 rounded').style(
                'background: var(--surface-secondary);'):
                ui.label(_review_admin_text('finding_id', lang)).classes(
                    'text-xs font-semibold')
                ui.label(str(review.get('identification_id') or '-')).classes(
                    'font-mono text-base').style(
                        'overflow-wrap: anywhere; word-break: break-all; '
                        'max-width: 100%;')

            _render_review_identifiers(review, lang)

            try:
                from web.discovery_assets import discovery_sidecar_version

                current_version = discovery_sidecar_version()
            except Exception:
                current_version = None
            if (current_version and review.get('sidecar_version')
                    and current_version != review.get('sidecar_version')):
                ui.label(_review_admin_text('version_mismatch', lang)).classes(
                    'text-sm p-2 rounded').style(
                        'background: var(--bg-secondary); color: var(--warning); '
                        'border: 1px solid var(--warning);')

            _render_review_evidence(review, lang, excerpt)

            ui.separator()
            ui.label(_review_admin_text('submitted_assessment', lang)).classes(
                'text-sm font-semibold')
            with ui.row().classes('w-full gap-4 flex-wrap'):
                ui.label(relation_verdict_text(
                    review.get('relation_verdict'), lang)).classes('text-sm')
                if review.get('direct_novelty'):
                    ui.label(direct_novelty_text(
                        review.get('direct_novelty'), lang)).classes('text-sm')

            ui.label(_review_admin_text('moderated_assessment', lang)).classes(
                'text-base font-bold mt-2')
            ui.label(_review_admin_text('relation', lang)).classes(
                'text-sm font-medium')
            relation_input = ui.radio(
                relation_options,
                value=str(review.get('relation_verdict') or RELATION_OTHER_UNSURE),
            ).classes('w-full')

            with ui.column().classes('w-full gap-1') as novelty_block:
                ui.label(_review_admin_text('novelty', lang)).classes(
                    'text-sm font-medium')
                novelty_input = ui.radio(
                    novelty_options,
                    value=str(review.get('direct_novelty') or no_novelty),
                ).classes('w-full')
            novelty_block.set_visibility(
                relation_input.value == RELATION_DIRECT_WITNESS)

            def _relation_changed(event) -> None:
                is_direct = event.value == RELATION_DIRECT_WITNESS
                novelty_block.set_visibility(is_direct)
                if not is_direct:
                    novelty_input.value = no_novelty

            relation_input.on_value_change(_relation_changed)

            comment_editor = ui.textarea(
                label=_review_admin_text('comment', lang),
                value=str(review.get('comment') or ''),
            ).classes('w-full').props('outlined autogrow counter maxlength=1500')
            publish_comment = ui.checkbox(
                _review_admin_text('publish_comment', lang),
                value=bool(review.get('publish_comment')),
            )
            ui.label(_review_admin_text('publish_comment_note', lang)).classes(
                'text-xs').style('color: var(--text-muted);')

            moderation_note = ui.input(
                _review_admin_text('private_note', lang)).classes(
                'w-full').props('outlined dense maxlength=1000')
            review_id = str(review.get('id') or '')

            async def _decide(status: str, rid=review_id,
                              note_input=moderation_note) -> None:
                client = get_user_client()
                ok = await run.io_bound(
                    moderate_review, rid, status, note_input.value,
                    relation_verdict=relation_input.value,
                    direct_novelty=(
                        None if novelty_input.value == no_novelty
                        else novelty_input.value),
                    comment=comment_editor.value,
                    publish_comment=publish_comment.value,
                    client=client)
                if not ok:
                    ui.notify(_review_admin_text('update_failed', lang),
                              type='negative')
                    return
                ui.notify(
                    _review_admin_text('approved', lang)
                    if status == 'approved'
                    else _review_admin_text('rejected', lang),
                    type='positive' if status == 'approved' else 'info')
                ui.navigate.reload()

            with ui.row().classes('gap-2'):
                ui.button(
                    _review_admin_text('approve', lang),
                    on_click=lambda: _decide('approved')).props('color=positive')
                ui.button(
                    _review_admin_text('reject', lang),
                    on_click=lambda: _decide('rejected')).props(
                        'flat color=negative')


async def create_pending_correction_card(corr):
    """Create a card for a pending correction."""
    doc_id = corr.get('sys_id', 'Unknown')
    page_num = corr.get('page_number', 1)
    shelfmark, title = get_shelfmark_for_id(doc_id)

    with ui.card().classes('w-full p-4 mb-4'):
        with ui.column().classes('w-full gap-3'):
            # Header row
            with ui.row().classes('w-full items-center justify-between'):
                with ui.row().classes('items-center gap-2'):
                    def go_to_browse(sid=doc_id, pnum=page_num):
                        ui.navigate.to(f'/browse?sys_id={sid}&page={pnum}')

                    with ui.element('a').classes('cursor-pointer hover:underline').on('click', go_to_browse):
                        ui.label(f"{shelfmark}").classes('font-bold text-primary')
                        if page_num:
                            ui.label(f" • {tr('Image')} {page_num}").classes('text-sm')

                with ui.row().classes('items-center gap-3'):
                    # Author info
                    profiles = corr.get('profiles', {}) or {}
                    author_name = profiles.get('full_name') or profiles.get('username') or 'Unknown'
                    ui.label(f"{tr('by')} {author_name}").style('color: var(--text-secondary);')

                    # Vote display
                    upvotes = corr.get('upvotes', 0)
                    downvotes = corr.get('downvotes', 0)
                    vote_score = upvotes - downvotes

                    with ui.row().classes('items-center gap-1'):
                        ui.icon('thumb_up').classes('text-sm').style('color: var(--success);')
                        ui.label(str(upvotes)).classes('text-sm').style('color: var(--success);')
                        ui.icon('thumb_down').classes('text-sm ml-2').style('color: var(--danger);')
                        ui.label(str(downvotes)).classes('text-sm').style('color: var(--danger);')
                        if vote_score != 0:
                            score_color = 'var(--success)' if vote_score > 0 else 'var(--danger)'
                            ui.label(f"({'+' if vote_score > 0 else ''}{vote_score})").classes('text-sm ml-1').style(f'color: {score_color};')

            # Text comparison
            with ui.row().classes('w-full gap-4'):
                with ui.column().classes('flex-1'):
                    ui.label(tr('Original')).classes('font-medium text-sm')
                    ui.label(corr.get('original_text', '-')).classes('font-mono text-sm p-2 rounded whitespace-pre-wrap').style('background: var(--surface-secondary); direction: rtl; text-align: right;')

                with ui.column().classes('flex-1'):
                    ui.label(tr('Corrected')).classes('font-medium text-sm')
                    ui.label(corr.get('corrected_text', '-')).classes('font-mono text-sm p-2 rounded whitespace-pre-wrap').style('background: var(--surface-secondary); direction: rtl; text-align: right;')

            # Notes if any
            if corr.get('notes'):
                ui.label(f"{tr('Notes')}: {corr['notes']}").style('color: var(--text-secondary);')

            # Review actions
            review_notes = ui.input(tr('Review notes')).classes('w-full').props('outlined dense')

            corr_id = corr.get('id')

            async def approve(cid=corr_id, notes=review_notes):
                result = await run.io_bound(
                    update_correction_status, cid, 'approved',
                    review_notes=notes.value, client=get_user_client())
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Correction approved'), type='positive')
                    ui.navigate.reload()

            async def reject(cid=corr_id, notes=review_notes):
                rejection_text = notes.value or tr('Rejected by reviewer')
                result = await run.io_bound(
                    update_correction_status, cid, 'rejected',
                    rejection_reason=rejection_text, client=get_user_client())
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Correction rejected'), type='info')
                    ui.navigate.reload()

            with ui.row().classes('gap-2'):
                ui.button(tr('Approve'), on_click=approve).props('color=positive')
                ui.button(tr('Reject'), on_click=reject).props('flat color=negative')


async def create_users_list_view(users=None):
    """View all users with management options."""
    if users is None:
        users = await run.io_bound(get_all_users)

    if not users:
        ui.label(tr('No users found')).style('color: var(--text-secondary);')
        return

    # Filter controls
    with ui.row().classes('w-full items-center gap-4 mb-4'):
        search_input = ui.input(placeholder=tr('Search users...')).props('outlined dense').classes('flex-1')
        role_filter = ui.select(
            {
                'all': tr('All Roles'),
                'user': tr('User'),
                'editor': tr('Editor'),
                'admin': tr('Admin')
            },
            value='all',
            label=tr('Filter by role')
        ).props('outlined dense').classes('w-40')

    # Users table
    h3(f"{len(users)} {tr('users')}", classes='text-lg font-medium mb-2')

    with ui.column().classes('w-full gap-2') as users_container:
        for user in users:
            create_user_row(user)


def create_user_row(user):
    """Create a row for a user in the users list."""
    role_colors = {
        'user': 'grey',
        'contributor': 'blue',
        'editor': 'purple',
        'reviewer': 'orange',
        'admin': 'red'
    }

    with ui.card().classes('w-full p-3'):
        with ui.row().classes('w-full items-center justify-between'):
            # User info
            with ui.row().classes('items-center gap-3 flex-1'):
                ui.icon('account_circle').classes('text-2xl').style('color: var(--primary-600);')
                with ui.column().classes('gap-0'):
                    ui.label(user.get('full_name') or user.get('username') or 'Unknown').classes('font-medium')
                    # Note: email is not in profiles table, would need to join with auth.users
                    if user.get('username'):
                        ui.label(f"@{user.get('username')}").classes('text-xs').style('color: var(--text-muted);')

            # Affiliation
            if user.get('affiliation'):
                ui.label(user.get('affiliation')).classes('text-sm flex-1').style('color: var(--text-secondary);')
            else:
                ui.element('div').classes('flex-1')

            # Role badge
            role = user.get('role', 'user')
            ui.badge(role.title()).props(f'color={role_colors.get(role, "grey")}').classes('w-20 justify-center')

            # Stats
            with ui.row().classes('items-center gap-4 w-32'):
                ui.label(f"{user.get('reputation', 0)} pts").classes('text-sm font-medium')

            # Actions
            user_id = user.get('id')

            with ui.row().classes('gap-1'):
                async def change_role(uid, new_role):
                    result = await run.io_bound(
                        update_user_role, uid, new_role, client=get_user_client())
                    if "error" in result:
                        ui.notify(result['error'], type='negative')
                    else:
                        ui.notify(tr('Role updated'), type='positive')
                        ui.navigate.reload()

                def confirm_delete_user(uid, uname):
                    with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                        h3(tr('Delete User?'), classes='text-lg font-bold')
                        ui.label(f"{tr('Are you sure you want to delete')} {uname}?").classes('text-sm')
                        ui.label(tr('This action cannot be undone.')).classes('text-sm text-red-500')
                        with ui.row().classes('justify-end gap-2 mt-4'):
                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                            async def do_delete():
                                result = await run.io_bound(
                                    delete_user, uid, client=get_user_client())
                                confirm_dialog.close()
                                if "error" in result:
                                    ui.notify(result['error'], type='negative')
                                else:
                                    ui.notify(tr('User deleted'), type='positive')
                                    ui.navigate.reload()

                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                    confirm_dialog.open()

                with ui.button(icon='more_vert').props('flat round dense'):
                    with ui.menu():
                        ui.menu_item(tr('Set as User'), lambda uid=user_id: change_role(uid, 'user'))
                        ui.menu_item(tr('Set as Editor'), lambda uid=user_id: change_role(uid, 'editor'))
                        ui.menu_item(tr('Set as Admin'), lambda uid=user_id: change_role(uid, 'admin'))
                        ui.separator()
                        ui.menu_item(
                            tr('Delete User'),
                            lambda uid=user_id, uname=user.get('username', 'user'): confirm_delete_user(uid, uname)
                        ).classes('text-red-500')


async def create_stats_view(users=None, pending=None, total_corrections=None):
    """Display system statistics.

    All three arrive preloaded from create_admin_page. This view used to fetch
    the users and the pending corrections a SECOND time, duplicating what the
    other two tabs had already loaded on the same page build.
    """
    if users is None:
        users = await run.io_bound(get_all_users)
    if pending is None:
        pending = await run.io_bound(get_pending_corrections)
    if total_corrections is None:
        total_corrections = await run.io_bound(get_all_corrections_count)

    # Calculate stats
    total_users = len(users)
    editors = sum(1 for u in users if u.get('role') in ('editor', 'admin', 'reviewer'))
    pending_count = len(pending)

    with ui.row().classes('w-full gap-4 flex-wrap'):
        # Users stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('people').classes('text-4xl').style('color: var(--primary-600);')
                h3(str(total_users), classes='text-3xl font-bold')
                ui.label(tr('Total Users')).style('color: var(--text-secondary);')

        # Pending corrections stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('hourglass_empty').classes('text-4xl').style('color: var(--accent-amber);')
                h3(str(pending_count), classes='text-3xl font-bold')
                ui.label(tr('Pending Corrections')).style('color: var(--text-secondary);')

        # Editors stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('edit').classes('text-4xl').style('color: var(--success);')
                h3(str(editors), classes='text-3xl font-bold')
                ui.label(tr('Editors & Admins')).style('color: var(--text-secondary);')

        # Corrections stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('rate_review').classes('text-4xl').style('color: var(--info);')
                h3(str(total_corrections), classes='text-3xl font-bold')
                ui.label(tr('Total Corrections')).style('color: var(--text-secondary);')
