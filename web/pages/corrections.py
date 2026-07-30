# -*- coding: utf-8 -*-
"""
My Edits & Comments Page - Dicta Genizah Search

User corrections and comments system: view, edit, and manage your contributions.
"""

import asyncio
import logging
from typing import Dict, List

from nicegui import run, ui
from web.translations import tr
from web.auth_state import GlobalAuthState, create_login_dialog, do_logout
from web.supabase_client import (
    get_corrections,
    update_correction,
    get_comments,
    get_client,
    get_user_client,
    get_user_corrections_count,
)
from web.state import state
from web.components.typography import h1, h2, h3

logger = logging.getLogger(__name__)

# Max simultaneous leaderboard count queries. `run.io_bound` shares ONE
# process-wide thread pool with every other offloaded call in the app, so an
# unbounded fan-out of ~20 would crowd it out for concurrent visitors.
_LEADERBOARD_COUNT_CONCURRENCY = 4


def _fetch_top_profiles(limit: int) -> List[Dict]:
    """Fetch the top-reputation profiles. Blocking -- call inside ``run.io_bound``.

    Uses the anonymous module singleton client (`get_client`), which reads no
    per-user storage, so it is safe off the event loop. Do NOT switch this to
    `get_user_client()`: that reads `safe_user_get('auth_session')`, which
    degrades to anonymous in a worker thread (see web/safe_storage.py).
    """
    client = get_client()
    response = client.table('profiles').select('*').order('reputation', desc=True).limit(limit).execute()
    return response.data or []


async def fetch_leaderboard_users(limit: int = 20) -> List[Dict]:
    """Top contributors with their approved-correction counts, fetched OFF the event loop.

    PERF (2026-07-30): this replaces a sequential in-render fanout -- one
    profiles query followed by up to `limit` `get_user_corrections_count`
    queries, all executed synchronously ON the event loop while the page was
    being constructed. uvicorn runs a SINGLE worker, so that stalled every
    other concurrent request (static assets, `/api/*`) for the duration, which
    is why `/corrections` was the slowest route. The counts now fan out
    concurrently in the thread pool, so wall time is ~2 round trips instead of
    ~21, and none of it occupies the loop.

    Counts are exact (per-user `count='exact'`), deliberately not collapsed into
    one `in_()` query: a single fetch of author_id rows would be subject to
    PostgREST's default row cap and could silently undercount a prolific author.
    A server-side aggregate RPC (as `get_list_item_counts` does for lists) would
    be the better long-term fix, but that needs a Supabase migration.

    Concurrency is BOUNDED at `_LEADERBOARD_COUNT_CONCURRENCY`. `run.io_bound`
    submits to NiceGUI's single process-wide `ThreadPoolExecutor`, so firing all
    ~20 counts at once would occupy most of that shared pool; several concurrent
    visitors could then starve unrelated offloaded work (image resolution, search
    execution) and pile load onto Supabase.
    """
    users = await run.io_bound(_fetch_top_profiles, limit)
    if not users:
        return []
    ids = [u.get('id') for u in users]
    countable = [uid for uid in ids if uid]

    semaphore = asyncio.Semaphore(_LEADERBOARD_COUNT_CONCURRENCY)

    async def _count(uid):
        async with semaphore:
            return await run.io_bound(get_user_corrections_count, uid)

    results = await asyncio.gather(
        *[_count(uid) for uid in countable],
        return_exceptions=True,
    )
    counts: Dict[str, int] = {}
    for uid, result in zip(countable, results):
        if isinstance(result, BaseException) or result is None:
            # get_user_corrections_count already degrades to 0; None means the
            # app is shutting down mid-flight. Either way show 0, never crash.
            counts[uid] = 0
        else:
            counts[uid] = int(result)
    for u in users:
        u['_corrections_count'] = counts.get(u.get('id'), 0)
    return users


def get_shelfmark_for_id(sys_id: str) -> tuple:
    """Get shelfmark and title for a system ID."""
    try:
        if state.meta_mgr:
            shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)
            return shelfmark or sys_id, title or ''
    except Exception:
        pass  # Translation lookup failed; continue without translation
    return sys_id, ''


async def create_corrections_page():
    """Create the My Edits & Comments page."""

    with ui.column().classes('w-full max-w-5xl mx-auto gap-8 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('My Edits & Comments'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Manage your edits and comments')).style('color: var(--text-secondary);')

        # Main content container
        main_container = ui.column().classes('w-full gap-6')

        def refresh_page():
            """Refresh the page content."""
            main_container.clear()
            with main_container:
                if GlobalAuthState.is_logged_in():
                    create_logged_in_view()
                else:
                    create_login_view()

        def create_login_view():
            """Create login/register view - use global login dialog."""
            with ui.card().classes('w-full p-6'):
                with ui.column().classes('w-full items-center gap-4 py-8'):
                    ui.icon('login').classes('text-6xl').style('color: var(--text-tertiary);')
                    # Changed to H2
                    h2(tr('Please login to view your edits'), classes='text-xl', style='color: var(--text-secondary);')

                    login_dialog = create_login_dialog()

                    def open_login():
                        login_dialog.open()

                    ui.button(tr('Login / Register'), icon='login', on_click=open_login).props('color=primary size=lg')

        def create_logged_in_view():
            """Create view for logged in users."""
            user = GlobalAuthState.get_user()

            # User info bar
            with ui.card().classes('w-full p-4'):
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('account_circle').classes('text-3xl').style('color: var(--primary-600);')
                        with ui.column().classes('gap-0'):
                            # Changed to H3
                            h3(user.get('full_name', user.get('username', '')), classes='font-bold')
                            ui.label(f"{tr('Role')}: {user.get('role', 'contributor').title()} | {tr('Reputation')}: {user.get('reputation', 0)}").classes('text-sm').style('color: var(--text-secondary);')

                    def handle_logout():
                        do_logout()
                        ui.notify(tr('Logged out'), type='info')
                        ui.navigate.reload()

                    ui.button(tr('Logout'), on_click=handle_logout).props('flat color=negative')

            # Tabs for different views
            leaderboard_tab_name = tr('Leaderboard')
            with ui.tabs().classes('w-full') as tabs:
                my_edits_tab = ui.tab(tr('My Edits'))
                my_comments_tab = ui.tab(tr('My Comments'))
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    review_tab = ui.tab(tr('Review'))
                leaderboard_tab = ui.tab(leaderboard_tab_name)  # noqa: F841 -- panel key below

            with ui.tab_panels(tabs, value=my_edits_tab).classes('w-full'):
                # My Edits panel
                with ui.tab_panel(my_edits_tab):
                    create_my_edits_view()

                # My Comments panel
                with ui.tab_panel(my_comments_tab):
                    create_my_comments_view()

                # Review panel (for reviewers+)
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    with ui.tab_panel(review_tab):
                        create_review_view()

                # Leaderboard panel — builds the spinner and returns its loader;
                # the fetch starts on first activation of the tab (below).
                with ui.tab_panel(leaderboard_tab):
                    load_leaderboard = create_leaderboard_view()

            leaderboard_started = False

            async def _on_tab_change(event):
                """Start the leaderboard fetch once, the first time its tab is shown.

                `ui.tabs`'s value is the tab NAME (a str), not the Tab element --
                `ui.tab(name)` stores it in `_props['name']` -- so compare against
                the name we passed in.
                """
                nonlocal leaderboard_started
                if leaderboard_started or event.value != leaderboard_tab_name:
                    return
                leaderboard_started = True
                await load_leaderboard()

            tabs.on_value_change(_on_tab_change)

        def create_my_edits_view():
            """View user's own corrections/edits."""
            user_id = GlobalAuthState.get_user_id()
            if not user_id:
                ui.label(tr('User not found')).style('color: var(--danger);')
                return

            # Container for content
            content_container = ui.column().classes('w-full')

            # Show loading spinner initially
            with content_container:
                with ui.column().classes('w-full items-center py-8'):
                    ui.spinner('dots', size='lg', color='primary')
                    ui.label(tr('Loading your edits...')).classes('mt-2').style('color: var(--text-secondary);')

            def render_edits(corrections_raw):
                content_container.clear()
                with content_container:
                    try:
                        # Format corrections for display
                        corrections = []
                        for c in corrections_raw:
                            profile = c.get('profiles', {}) or {}
                            corrections.append({
                                'id': c.get('id'),
                                'document_id': c.get('sys_id'),
                                'system_id': c.get('sys_id'),
                                'page_number': c.get('page_number'),
                                'original_text': c.get('original_text'),
                                'corrected_text': c.get('corrected_text'),
                                'notes': c.get('notes'),
                                'status': c.get('status', 'pending'),
                                'created_at': c.get('created_at', ''),
                                'upvotes': c.get('upvotes', 0),
                                'downvotes': c.get('downvotes', 0),
                                'user_vote': None  # TODO: Implement vote tracking
                            })
                    except Exception as e:
                        ui.label(f"{tr('Error')}: {str(e)}").style('color: var(--danger);')
                        return

                    if not corrections:
                        with ui.column().classes('w-full items-center py-8'):
                            ui.icon('edit_note').classes('text-6xl').style('color: var(--text-tertiary);')
                            h3(tr('No edits yet'), classes='text-xl', style='color: var(--text-secondary);')
                            ui.label(tr('Edit transcriptions to help improve the corpus')).style('color: var(--text-tertiary);')
                    else:
                        def delete_correction(corr_id: int):
                            """Delete a correction after confirmation."""
                            try:
                                client = get_client()
                                client.table('corrections').delete().eq('id', corr_id).execute()
                                ui.notify(tr('Correction deleted'), type='positive')
                                ui.navigate.reload()
                            except Exception as e:
                                ui.notify(str(e), type='negative')

                        for corr in corrections:
                            create_edit_card(corr, delete_correction)

            # Load data after brief delay to show spinner
            async def _deferred_load_edits():
                # Fetch OFF the loop, render ON it. The explicit client is
                # mandatory: `run.io_bound` dispatches via
                # `loop.run_in_executor`, which does not propagate contextvars,
                # so `get_user_client()` inside the worker would find no UI
                # context and degrade to anonymous -- and corrections RLS is dual
                # (`TO public` approved + `TO authenticated` own-any-status), so
                # the user's own PENDING corrections would silently disappear.
                await asyncio.sleep(0.1)
                try:
                    reader_client = get_user_client()
                    corrections_raw = await run.io_bound(
                        get_corrections, author_id=user_id, client=reader_client
                    )
                    if corrections_raw is None:
                        return  # app shutting down mid-flight
                    if content_container.client.has_been_deleted:
                        return  # SEED-008: navigated away while fetching
                    render_edits(corrections_raw)
                except RuntimeError as e:
                    logger.debug("edits render skipped (client gone): %s", e)
                except Exception as e:
                    logger.warning("deferred edits load failed: %s", e, exc_info=False)
            asyncio.ensure_future(_deferred_load_edits())

        def create_edit_card(corr: dict, delete_callback):
            """Create a card for a single edit/correction."""
            doc_id = corr.get('document_id') or corr.get('system_id', 'Unknown')
            page_num = corr.get('page_number', 1)

            # Get shelfmark
            shelfmark, title = get_shelfmark_for_id(doc_id)

            with ui.card().classes('w-full p-4 mb-3'):
                with ui.row().classes('w-full items-start justify-between'):
                    with ui.column().classes('gap-2 flex-1'):
                        # Status badge
                        status_colors = {
                            'draft': 'orange',
                            'pending': 'blue',
                            'under_review': 'purple',
                            'approved': 'green',
                            'rejected': 'red',
                            'merged': 'teal'
                        }
                        status = corr.get('status', 'draft')

                        with ui.row().classes('items-center gap-2'):
                            ui.badge(status.replace('_', ' ').title()).props(f'color={status_colors.get(status, "grey")}')

                        # Shelfmark + Image with link
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('description').classes('text-lg').style('color: var(--primary-600);')

                            # Link to browse page
                            def go_to_browse(sid=doc_id, pnum=page_num):
                                ui.navigate.to(f'/browse?sys_id={sid}&page={pnum}')

                            with ui.element('a').classes('cursor-pointer hover:underline').on('click', go_to_browse):
                                ui.label(f"{shelfmark}").classes('font-medium text-primary')
                                if page_num:
                                    ui.label(f" • {tr('Image')} {page_num}").classes('text-sm').style('color: var(--text-secondary);')

                        if title:
                            ui.label(title).classes('text-sm').style('color: var(--text-tertiary);')

                        # Expandable text sections
                        if corr.get('original_text'):
                            with ui.expansion(tr('Original'), icon='article').classes('w-full').props('dense'):
                                ui.label(corr['original_text']).classes('font-mono text-sm whitespace-pre-wrap').style('direction: rtl; text-align: right;')

                        if corr.get('corrected_text'):
                            with ui.expansion(tr('Corrected'), icon='edit').classes('w-full').props('dense'):
                                ui.label(corr['corrected_text']).classes('font-mono text-sm whitespace-pre-wrap').style('direction: rtl; text-align: right;')

                        if corr.get('notes'):
                            ui.label(f"{tr('Notes')}: {corr['notes']}").classes('text-sm').style('color: var(--text-secondary);')

                    # Right side - votes, date and actions
                    with ui.column().classes('items-end gap-2'):
                        # Voting section
                        upvotes = corr.get('upvotes', 0)
                        downvotes = corr.get('downvotes', 0)
                        user_vote = corr.get('user_vote')  # 1, -1, or None
                        corr_id = corr.get('id')

                        with ui.row().classes('items-center gap-1'):
                            def do_vote(vote_val: int, cid=corr_id):
                                # Voting feature not yet migrated to Supabase
                                ui.notify(tr('Voting feature coming soon'), type='info')

                            def upvote(cid=corr_id):
                                do_vote(1, cid)

                            def downvote(cid=corr_id):
                                do_vote(-1, cid)

                            # Upvote button
                            upvote_btn = ui.button(icon='thumb_up', on_click=upvote).props('flat round dense size=sm')
                            if user_vote == 1:
                                upvote_btn.props('color=primary')
                            upvote_btn.tooltip(tr('Upvote'))

                            ui.label(str(upvotes)).classes('text-sm min-w-[20px] text-center').style('color: var(--success);')

                            # Downvote button
                            downvote_btn = ui.button(icon='thumb_down', on_click=downvote).props('flat round dense size=sm')
                            if user_vote == -1:
                                downvote_btn.props('color=negative')
                            downvote_btn.tooltip(tr('Downvote'))

                            ui.label(str(downvotes)).classes('text-sm min-w-[20px] text-center').style('color: var(--danger);')

                        ui.label(corr.get('created_at', '')[:10]).classes('text-sm').style('color: var(--text-tertiary);')

                        with ui.row().classes('gap-1'):
                            # View in browse
                            def view_in_browse(sid=doc_id, pnum=page_num):
                                ui.navigate.to(f'/browse?sys_id={sid}&page={pnum}')

                            ui.button(icon='visibility', on_click=view_in_browse).props('flat round dense').tooltip(tr('View in Browse'))

                            # Edit button - for drafts
                            corr_status = corr.get('status', 'draft')
                            if corr_status == 'draft':
                                def edit_correction(c=corr):
                                    open_edit_dialog(c)

                                ui.button(icon='edit', on_click=edit_correction).props('flat round dense').tooltip(tr('Edit'))

                            # Delete button
                            user_role = GlobalAuthState.get_role()
                            can_delete = corr_status == 'draft' or user_role == 'admin'

                            if can_delete:
                                corr_id = corr.get('id')

                                def confirm_delete(cid=corr_id):
                                    with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                                        # Changed to H3
                                        h3(tr('Delete Correction?'), classes='text-lg font-bold')
                                        ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')
                                        with ui.row().classes('justify-end gap-2 mt-4'):
                                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                            def do_delete():
                                                confirm_dialog.close()
                                                delete_callback(cid)
                                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                    confirm_dialog.open()

                                ui.button(icon='delete', on_click=confirm_delete).props('flat round dense color=negative').tooltip(tr('Delete'))

        def open_edit_dialog(corr: dict):
            """Open dialog to edit a correction."""
            dialog = ui.dialog().props('maximized persistent')

            with dialog, ui.card().classes('w-full h-full').style('display: flex; flex-direction: column;'):
                doc_id = corr.get('document_id') or corr.get('system_id', '')
                page_num = corr.get('page_number', 1)
                shelfmark, title = get_shelfmark_for_id(doc_id)

                # Header
                with ui.row().classes('w-full items-center justify-between p-4 border-b'):
                    with ui.column().classes('gap-1'):
                        # Changed to H2
                        h2(tr('Edit your version'), classes='text-xl font-bold')
                        ui.label(f"{shelfmark} • {tr('Image')} {page_num}").classes('text-sm').style('color: var(--text-secondary);')

                    ui.button(icon='close', on_click=dialog.close).props('flat round')

                # Content
                with ui.column().classes('flex-1 p-4 gap-4 overflow-auto'):
                    if corr.get('original_text'):
                        ui.label(tr('Original Text')).classes('font-medium')
                        ui.label(corr['original_text']).classes('font-mono text-sm p-3 rounded').style(
                            'background: var(--surface-secondary); direction: rtl; text-align: right;'
                        )

                    ui.label(tr('Corrected Text')).classes('font-medium')
                    text_area = ui.textarea(value=corr.get('corrected_text', '')).classes('w-full').props('outlined rows=10').style(
                        'direction: rtl; text-align: right;'
                    )

                    ui.label(tr('Notes')).classes('font-medium')
                    notes_area = ui.textarea(value=corr.get('notes', '')).classes('w-full').props('outlined rows=3').style(
                        'direction: rtl; text-align: right;'
                    )

                # Footer
                with ui.row().classes('w-full justify-end gap-2 p-4 border-t'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

                    def save_changes():
                        result = update_correction(corr['id'], {
                            'corrected_text': text_area.value,
                            'notes': notes_area.value or None
                        })
                        if "error" in result:
                            ui.notify(result["error"], type='negative')
                        else:
                            ui.notify(tr('Correction updated'), type='positive')
                            dialog.close()
                            ui.navigate.reload()

                    ui.button(tr('Save'), icon='save', on_click=save_changes).props('color=primary')

            dialog.open()

        def create_my_comments_view():
            """View user's own comments."""
            user_id = GlobalAuthState.get_user_id()
            if not user_id:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('comment').classes('text-6xl').style('color: var(--text-tertiary);')
                    h3(tr('No comments yet'), classes='text-xl', style='color: var(--text-secondary);')
                return

            # Container for content
            content_container = ui.column().classes('w-full')

            # Show loading spinner initially
            with content_container:
                with ui.column().classes('w-full items-center py-8'):
                    ui.spinner('dots', size='lg', color='primary')
                    ui.label(tr('Loading your comments...')).classes('mt-2').style('color: var(--text-secondary);')

            def render_comments(comments_raw):
                content_container.clear()
                with content_container:
                    try:
                        comments = []
                        for c in comments_raw:
                            profile = c.get('profiles', {}) or {}
                            comments.append({
                                'id': c.get('id'),
                                'document_id': c.get('sys_id'),
                                'page_number': c.get('page_number'),
                                'content': c.get('content', ''),
                                'comment_type': c.get('scope', 'general'),
                                'created_at': c.get('created_at', '')
                            })
                    except Exception as e:
                        with ui.column().classes('w-full items-center py-8'):
                            ui.icon('comment').classes('text-6xl').style('color: var(--text-tertiary);')
                            h3(tr('No comments yet'), classes='text-xl', style='color: var(--text-secondary);')
                            ui.label(tr('Share your insights and questions')).style('color: var(--text-tertiary);')
                        return

                    if not comments:
                        with ui.column().classes('w-full items-center py-8'):
                            ui.icon('comment').classes('text-6xl').style('color: var(--text-tertiary);')
                            h3(tr('No comments yet'), classes='text-xl', style='color: var(--text-secondary);')
                            ui.label(tr('Share your insights and questions')).style('color: var(--text-tertiary);')
                    else:
                        for comment in comments:
                            create_comment_card(comment)

            # Load data after brief delay to show spinner
            async def _deferred_load_comments():
                # Same shape as `_deferred_load_edits`: client built on the loop,
                # blocking read in a worker, render back on the loop. The explicit
                # client keeps the `TO authenticated` half of the dual comments RLS
                # reachable -- without it the worker degrades to anonymous and the
                # user's own PRIVATE comments silently disappear.
                await asyncio.sleep(0.1)
                try:
                    reader_client = get_user_client()
                    comments_raw = await run.io_bound(
                        get_comments, author_id=user_id, client=reader_client
                    )
                    if comments_raw is None:
                        return  # app shutting down mid-flight
                    if content_container.client.has_been_deleted:
                        return  # SEED-008: navigated away while fetching
                    render_comments(comments_raw)
                except RuntimeError as e:
                    logger.debug("comments render skipped (client gone): %s", e)
                except Exception as e:
                    logger.warning("deferred comments load failed: %s", e, exc_info=False)
            asyncio.ensure_future(_deferred_load_comments())

        def create_comment_card(comment: dict):
            """Create a card for a single comment."""
            doc_id = comment.get('document_id', 'Unknown')
            page_num = comment.get('page_number')  # Page/image number in manuscript

            shelfmark, title = get_shelfmark_for_id(doc_id)

            with ui.card().classes('w-full p-4 mb-3'):
                with ui.row().classes('w-full items-start justify-between'):
                    with ui.column().classes('gap-2 flex-1'):
                        # Document link
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('description').classes('text-lg').style('color: var(--primary-600);')

                            def go_to_browse(sid=doc_id, pnum=page_num):
                                url = f'/browse?sys_id={sid}'
                                if pnum:
                                    url += f'&page={pnum}'
                                ui.navigate.to(url)

                            with ui.element('a').classes('cursor-pointer hover:underline').on('click', go_to_browse):
                                ui.label(f"{shelfmark}").classes('font-medium text-primary')
                                if page_num:
                                    ui.label(f" • {tr('Image')} {page_num}").classes('text-sm').style('color: var(--text-secondary);')

                        # Comment content
                        ui.label(comment.get('content', '')).classes('text-sm whitespace-pre-wrap').style(
                            'direction: rtl; text-align: right; color: var(--text-primary);'
                        )

                        # Comment type badge
                        comment_type = comment.get('comment_type', 'general')
                        if comment_type != 'general':
                            ui.badge(comment_type.title()).props('color=grey').classes('text-xs')

                    with ui.column().classes('items-end gap-2'):
                        ui.label(comment.get('created_at', '')[:10]).classes('text-sm').style('color: var(--text-tertiary);')

                        with ui.row().classes('gap-1'):
                            # View in browse
                            def view_in_browse(sid=doc_id, pnum=page_num):
                                url = f'/browse?sys_id={sid}'
                                if pnum:
                                    url += f'&page={pnum}'
                                ui.navigate.to(url)

                            ui.button(icon='visibility', on_click=view_in_browse).props('flat round dense').tooltip(tr('View in Browse'))

                            # Edit button
                            def edit_comment(c=comment):
                                open_comment_edit_dialog(c)

                            ui.button(icon='edit', on_click=edit_comment).props('flat round dense').tooltip(tr('Edit'))

                            # Delete button
                            comment_id = comment.get('id')

                            def confirm_delete(cid=comment_id):
                                with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                                    # Changed to H3
                                    h3(tr('Delete Comment?'), classes='text-lg font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                        def do_delete():
                                            try:
                                                client = get_client()
                                                client.table('comments').delete().eq('id', cid).execute()
                                                confirm_dialog.close()
                                                ui.notify(tr('Comment deleted'), type='positive')
                                                ui.navigate.reload()
                                            except Exception as e:
                                                confirm_dialog.close()
                                                ui.notify(str(e), type='negative')
                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=confirm_delete).props('flat round dense color=negative').tooltip(tr('Delete'))

        def open_comment_edit_dialog(comment: dict):
            """Open dialog to edit a comment."""
            dialog = ui.dialog()

            with dialog, ui.card().classes('w-96 p-6'):
                # Changed to H3
                h3(tr('Edit Comment'), classes='text-xl font-bold mb-4')

                text_area = ui.textarea(value=comment.get('content', '')).classes('w-full').props('outlined rows=5').style(
                    'direction: rtl; text-align: right;'
                )

                with ui.row().classes('w-full justify-end gap-2 mt-4'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

                    def save_comment():
                        try:
                            client = get_client()
                            client.table('comments').update({
                                'content': text_area.value
                            }).eq('id', comment['id']).execute()
                            ui.notify(tr('Comment updated'), type='positive')
                            dialog.close()
                            ui.navigate.reload()
                        except Exception as e:
                            ui.notify(str(e), type='negative')

                    ui.button(tr('Save'), icon='save', on_click=save_comment).props('color=primary')

            dialog.open()

        def create_review_view():
            """View for reviewers to review pending corrections."""
            # Get pending corrections from Supabase
            try:
                pending_raw = get_corrections(status='pending')
                pending = []
                for c in pending_raw:
                    profile = c.get('profiles', {}) or {}
                    pending.append({
                        'id': c.get('id'),
                        'document_id': c.get('sys_id'),
                        'system_id': c.get('sys_id'),
                        'page_number': c.get('page_number'),
                        'original_text': c.get('original_text'),
                        'corrected_text': c.get('corrected_text'),
                        'notes': c.get('notes'),
                        'upvotes': c.get('upvotes', 0),
                        'downvotes': c.get('downvotes', 0),
                        'author': {
                            'username': profile.get('username'),
                            'full_name': profile.get('full_name')
                        }
                    })
            except Exception as e:
                ui.label(f"{tr('Error loading pending corrections')}: {str(e)}").style('color: var(--danger);')
                return

            if not pending:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
                    # Changed to H3
                    h3(tr('No pending corrections'), classes='text-xl', style='color: var(--text-secondary);')
            else:
                ui.label(f"{len(pending)} {tr('corrections pending review')}").classes('text-lg font-medium mb-4')

                for corr in pending:
                    doc_id = corr.get('document_id') or corr.get('system_id', 'Unknown')
                    page_num = corr.get('page_number', 1)
                    shelfmark, title = get_shelfmark_for_id(doc_id)

                    with ui.card().classes('w-full p-4 mb-4'):
                        with ui.column().classes('w-full gap-3'):
                            with ui.row().classes('w-full items-center justify-between'):
                                with ui.row().classes('items-center gap-2'):
                                    def go_to_browse(sid=doc_id, pnum=page_num):
                                        ui.navigate.to(f'/browse?sys_id={sid}&page={pnum}')

                                    with ui.element('a').classes('cursor-pointer hover:underline').on('click', go_to_browse):
                                        ui.label(f"{shelfmark}").classes('font-bold text-primary')
                                        if page_num:
                                            ui.label(f" • {tr('Image')} {page_num}").classes('text-sm')

                                with ui.row().classes('items-center gap-3'):
                                    ui.label(f"by {corr.get('author', {}).get('username', 'Unknown')}").style('color: var(--text-secondary);')

                                    # Vote display for reviewers
                                    upvotes = corr.get('upvotes', 0)
                                    downvotes = corr.get('downvotes', 0)
                                    vote_score = corr.get('vote_score', upvotes - downvotes)

                                    with ui.row().classes('items-center gap-1 ml-4'):
                                        ui.icon('thumb_up').classes('text-sm').style('color: var(--success);')
                                        ui.label(str(upvotes)).classes('text-sm').style('color: var(--success);')
                                        ui.icon('thumb_down').classes('text-sm ml-2').style('color: var(--danger);')
                                        ui.label(str(downvotes)).classes('text-sm').style('color: var(--danger);')
                                        if vote_score != 0:
                                            score_color = 'var(--success)' if vote_score > 0 else 'var(--danger)'
                                            ui.label(f"({'+' if vote_score > 0 else ''}{vote_score})").classes('text-sm ml-2').style(f'color: {score_color};')

                            with ui.row().classes('w-full gap-4'):
                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Original')).classes('font-medium text-sm')
                                    ui.label(corr.get('original_text', '-')).classes('font-mono text-sm p-2 rounded whitespace-pre-wrap').style('background: var(--surface-secondary); direction: rtl; text-align: right;')

                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Corrected')).classes('font-medium text-sm')
                                    ui.label(corr.get('corrected_text', '-')).classes('font-mono text-sm p-2 rounded whitespace-pre-wrap').style('background: var(--surface-secondary); direction: rtl; text-align: right;')

                            if corr.get('notes'):
                                ui.label(f"{tr('Notes')}: {corr['notes']}").style('color: var(--text-secondary);')

                            review_notes = ui.input(tr('Review notes')).classes('w-full').props('outlined dense')

                            def approve(c=corr, notes=review_notes):
                                try:
                                    result = update_correction(c['id'], {
                                        'status': 'approved',
                                        'notes': notes.value or None
                                    })
                                    if "error" in result:
                                        ui.notify(result["error"], type='negative')
                                    else:
                                        ui.notify(tr('Correction approved'), type='positive')
                                        ui.navigate.reload()
                                except Exception as e:
                                    ui.notify(str(e), type='negative')

                            def reject(c=corr, notes=review_notes):
                                try:
                                    rejection_text = notes.value or tr('Rejected by reviewer')
                                    result = update_correction(c['id'], {
                                        'status': 'rejected',
                                        'notes': rejection_text
                                    })
                                    if "error" in result:
                                        ui.notify(result["error"], type='negative')
                                    else:
                                        ui.notify(tr('Correction rejected'), type='info')
                                        ui.navigate.reload()
                                except Exception as e:
                                    ui.notify(str(e), type='negative')

                            with ui.row().classes('gap-2'):
                                ui.button(tr('Approve'), on_click=approve).props('color=positive')
                                ui.button(tr('Reject'), on_click=reject).props('color=negative flat')

        def create_leaderboard_view():
            """Show top contributors.

            The fetch is deferred and runs off the event loop (see
            `fetch_leaderboard_users`); only rendering happens on the loop.
            """
            leaderboard_container = ui.column().classes('w-full')

            with leaderboard_container:
                with ui.column().classes('w-full items-center py-8'):
                    ui.spinner('dots', size='lg', color='primary')
                    ui.label(tr('Loading leaderboard...')).classes('mt-2').style('color: var(--text-secondary);')

            def render_leaderboard(users: List[Dict]) -> None:
                leaderboard_container.clear()
                with leaderboard_container:
                    # Changed to H3
                    h3(tr('Top Contributors'), classes='text-xl font-bold mb-4')

                    if not users:
                        ui.label(tr('No contributors yet')).style('color: var(--text-secondary);')
                    else:
                        for i, contributor in enumerate(users, 1):
                            with ui.card().classes('w-full p-3 mb-2'):
                                with ui.row().classes('w-full items-center justify-between'):
                                    with ui.row().classes('items-center gap-3'):
                                        if i == 1:
                                            ui.icon('emoji_events').style('color: gold;')
                                        elif i == 2:
                                            ui.icon('emoji_events').style('color: silver;')
                                        elif i == 3:
                                            ui.icon('emoji_events').style('color: #cd7f32;')
                                        else:
                                            ui.label(f"#{i}").classes('w-6 text-center')

                                        ui.label(contributor.get('full_name') or contributor.get('username', 'Unknown')).classes('font-medium')

                                    with ui.row().classes('items-center gap-4'):
                                        ui.label(f"{contributor.get('_corrections_count', 0)} {tr('corrections')}").style('color: var(--text-secondary);')
                                        ui.badge(f"{contributor.get('reputation', 0)} pts").props('color=primary')

            async def load_leaderboard():
                """Fetch + render. Started on first activation of the tab, not on page build."""
                try:
                    users = await fetch_leaderboard_users()
                    if users is None:
                        return  # app shutting down mid-flight
                    if leaderboard_container.client.has_been_deleted:
                        return  # SEED-008: navigated away while fetching
                    render_leaderboard(users)
                except RuntimeError as e:
                    logger.debug("leaderboard render skipped (client gone): %s", e)
                except Exception as e:
                    logger.warning("leaderboard load failed: %s", e, exc_info=False)
                    # The error path mutates the UI too, so it needs the same guard.
                    if leaderboard_container.client.has_been_deleted:
                        return
                    try:
                        leaderboard_container.clear()
                        with leaderboard_container:
                            ui.label(f"{tr('Error loading leaderboard')}: {str(e)}").style('color: var(--danger);')
                    except RuntimeError:
                        pass

            # Hand the loader back to the caller, which wires it to tab activation.
            # Deliberately NOT started here: this panel is built while the page is
            # constructed but the Leaderboard tab is NOT the default, so eagerly
            # firing the fan-out would spend up to `limit` Supabase queries and
            # occupy the shared thread pool for a tab most visitors never open.
            return load_leaderboard

        # Initial render
        refresh_page()
