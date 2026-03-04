# -*- coding: utf-8 -*-
"""
Admin Panel - Dicta Genizah Search

User management, corrections review, and system administration for admins.
Uses Supabase directly for all data operations.
"""

import logging
from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState
from web.state import state
from web.components.typography import h1, h2, h3
from web.supabase_client import get_client, get_user_client

logger = logging.getLogger(__name__)


def get_shelfmark_for_id(sys_id: str) -> tuple:
    """Get shelfmark and title for a system ID."""
    try:
        if state.meta_mgr:
            shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)
            return shelfmark or sys_id, title or ''
    except Exception:
        pass
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


def update_correction_status(correction_id: int, status: str, review_notes: str = None, rejection_reason: str = None):
    """Update correction status in Supabase."""
    try:
        client = get_user_client()
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


def update_user_role(user_id: str, new_role: str):
    """Update user role in Supabase profiles."""
    try:
        client = get_user_client()
        response = client.table('profiles').update({'role': new_role}).eq('id', user_id).execute()
        return {'success': True} if response.data else {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_user(user_id: str):
    """Delete user from Supabase (requires service role, typically done via dashboard)."""
    # Note: Deleting auth users requires the service role key
    # For now, we'll just mark them or remove from profiles
    try:
        client = get_user_client()
        # Delete profile (user can't access anything without profile)
        response = client.table('profiles').delete().eq('id', user_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


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

    with ui.column().classes('w-full max-w-6xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                h1(tr('Admin Panel'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('User management and system administration')).style('color: var(--text-secondary);')

        # === Tabs ===
        with ui.tabs().classes('w-full') as tabs:
            pending_tab = ui.tab(tr('Pending Corrections'))
            users_tab = ui.tab(tr('Users'))
            stats_tab = ui.tab(tr('Statistics'))

        with ui.tab_panels(tabs, value=pending_tab).classes('w-full'):
            # Pending Corrections panel
            with ui.tab_panel(pending_tab):
                await create_pending_corrections_view()

            # All Users panel
            with ui.tab_panel(users_tab):
                await create_users_list_view()

            # Statistics panel
            with ui.tab_panel(stats_tab):
                await create_stats_view()


async def create_pending_corrections_view():
    """View for reviewing pending corrections."""
    pending = get_pending_corrections()

    if not pending:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
            h3(tr('No pending corrections'), classes='text-xl', style='color: var(--text-secondary);')
            ui.label(tr('All corrections have been reviewed')).style('color: var(--text-muted);')
    else:
        h3(f"{len(pending)} {tr('corrections pending review')}", classes='text-lg font-medium mb-4')

        for corr in pending:
            await create_pending_correction_card(corr)


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
                result = update_correction_status(cid, 'approved', review_notes=notes.value)
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Correction approved'), type='positive')
                    ui.navigate.reload()

            async def reject(cid=corr_id, notes=review_notes):
                rejection_text = notes.value or tr('Rejected by reviewer')
                result = update_correction_status(cid, 'rejected', rejection_reason=rejection_text)
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Correction rejected'), type='info')
                    ui.navigate.reload()

            with ui.row().classes('gap-2'):
                ui.button(tr('Approve'), on_click=approve).props('color=positive')
                ui.button(tr('Reject'), on_click=reject).props('flat color=negative')


async def create_users_list_view():
    """View all users with management options."""
    users = get_all_users()

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
                def change_role(uid, new_role):
                    result = update_user_role(uid, new_role)
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

                            def do_delete():
                                result = delete_user(uid)
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


async def create_stats_view():
    """Display system statistics."""
    # Get stats from Supabase
    users = get_all_users()
    pending = get_pending_corrections()
    total_corrections = get_all_corrections_count()

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
