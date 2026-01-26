# -*- coding: utf-8 -*-
"""
Admin Panel - Dicta Genizah Search

User management, corrections review, and system administration for admins.
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from web.state import state
from web.components.typography import h1, h2, h3


def get_shelfmark_for_id(sys_id: str) -> tuple:
    """Get shelfmark and title for a system ID."""
    try:
        if state.meta_mgr:
            shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)
            return shelfmark or sys_id, title or ''
    except:
        pass
    return sys_id, ''


async def create_admin_page():
    """Create the Admin Panel page."""

    # Check if user is admin
    if not GlobalAuthState.is_admin():
        with ui.column().classes('w-full max-w-3xl mx-auto gap-8 fade-in items-center py-12'):
            ui.icon('lock').classes('text-6xl').style('color: var(--text-muted);')
            # Changed to H2
            h2(tr('Access Denied'), classes='text-2xl font-bold', style='color: var(--text-primary);')
            ui.label(tr('You need admin privileges to access this page')).style('color: var(--text-secondary);')
            ui.button(tr('Go Home'), on_click=lambda: ui.navigate.to('/')).props('color=primary')
        return

    with ui.column().classes('w-full max-w-6xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
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
    result = await api_call("GET", "/corrections/pending")

    if "error" in result:
        ui.label(f"{tr('Error')}: {result['error']}").style('color: var(--danger);')
        return

    pending = result.get('items', []) if isinstance(result, dict) else result

    if not pending:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
            # Changed to H3
            h3(tr('No pending corrections'), classes='text-xl', style='color: var(--text-secondary);')
            ui.label(tr('All corrections have been reviewed')).style('color: var(--text-muted);')
    else:
        # Changed to H3
        h3(f"{len(pending)} {tr('corrections pending review')}", classes='text-lg font-medium mb-4')

        for corr in pending:
            await create_pending_correction_card(corr)


async def create_pending_correction_card(corr):
    """Create a card for a pending correction."""
    doc_id = corr.get('document_id') or corr.get('system_id', 'Unknown')
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
                    author = corr.get('author', {})
                    ui.label(f"{tr('by')} {author.get('full_name') or author.get('username', 'Unknown')}").style('color: var(--text-secondary);')

                    # Vote display
                    upvotes = corr.get('upvotes', 0)
                    downvotes = corr.get('downvotes', 0)
                    vote_score = corr.get('vote_score', upvotes - downvotes)

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
                result = await api_call("POST", f"/corrections/{cid}/review", {
                    "action": "approve",
                    "review_notes": notes.value or None
                })
                if "error" in result:
                    ui.notify(result.get("detail", result["error"]), type='negative')
                else:
                    ui.notify(tr('Correction approved'), type='positive')
                    ui.navigate.reload()

            async def reject(cid=corr_id, notes=review_notes):
                rejection_text = notes.value or tr('Rejected by reviewer')
                result = await api_call("POST", f"/corrections/{cid}/review", {
                    "action": "reject",
                    "rejection_reason": rejection_text
                })
                if "error" in result:
                    ui.notify(result.get("detail", result["error"]), type='negative')
                else:
                    ui.notify(tr('Correction rejected'), type='info')
                    ui.navigate.reload()

            with ui.row().classes('gap-2'):
                ui.button(tr('Approve'), on_click=approve).props('color=positive')
                ui.button(tr('Reject'), on_click=reject).props('flat color=negative')


async def create_users_list_view():
    """View all users with management options."""
    result = await api_call("GET", "/users/", {"skip": 0, "limit": 100})

    if "error" in result:
        ui.label(f"{tr('Error')}: {result['error']}").style('color: var(--danger);')
        return

    users = result if isinstance(result, list) else result.get('items', result.get('users', []))

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
    with ui.column().classes('w-full gap-2') as users_container:
        for user in users:
            create_user_row(user)


def create_user_row(user):
    """Create a row for a user in the users list."""
    role_colors = {
        'user': 'grey',
        'contributor': 'blue',
        'editor': 'purple',
        'admin': 'red'
    }

    with ui.card().classes('w-full p-3'):
        with ui.row().classes('w-full items-center justify-between'):
            # User info
            with ui.row().classes('items-center gap-3 flex-1'):
                ui.icon('account_circle').classes('text-2xl').style('color: var(--primary-600);')
                with ui.column().classes('gap-0'):
                    ui.label(user.get('full_name') or user.get('username', 'Unknown')).classes('font-medium')
                    ui.label(user.get('email', '')).classes('text-xs').style('color: var(--text-muted);')

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
                ui.label(f"{user.get('corrections_count', 0)}").classes('text-sm').style('color: var(--text-secondary);')
                ui.label(f"{user.get('reputation_score', 0)} pts").classes('text-sm font-medium')

            # Actions
            with ui.row().classes('gap-1'):
                async def change_role(uid=user.get('id'), new_role=None):
                    if new_role is None:
                        return
                    # Map frontend role names to backend enum values
                    role_map = {
                        'user': 'contributor',
                        'editor': 'editor',
                        'admin': 'admin'
                    }
                    backend_role = role_map.get(new_role, new_role)
                    result = await api_call("PUT", f"/users/{uid}/role?role={backend_role}", None)
                    if "error" in result:
                        ui.notify(result['error'], type='negative')
                    else:
                        ui.notify(tr('Role updated'), type='positive')
                        ui.navigate.reload()

                async def delete_user(uid=user.get('id'), uname=user.get('username')):
                    with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                        # Changed to H3
                        h3(tr('Delete User?'), classes='text-lg font-bold')
                        ui.label(f"{tr('Are you sure you want to delete')} {uname}?").classes('text-sm')
                        ui.label(tr('This action cannot be undone.')).classes('text-sm text-red-500')
                        with ui.row().classes('justify-end gap-2 mt-4'):
                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                            async def do_delete():
                                result = await api_call("DELETE", f"/admin/users/{uid}")
                                confirm_dialog.close()
                                if "error" in result:
                                    ui.notify(result.get('detail', result['error']), type='negative')
                                else:
                                    ui.notify(tr('User deleted'), type='positive')
                                    ui.navigate.reload()
                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                    confirm_dialog.open()

                with ui.button(icon='more_vert').props('flat round dense'):
                    with ui.menu():
                        ui.menu_item(tr('Set as User'), lambda u=user: change_role(u.get('id'), 'user'))
                        ui.menu_item(tr('Set as Editor'), lambda u=user: change_role(u.get('id'), 'editor'))
                        ui.menu_item(tr('Set as Admin'), lambda u=user: change_role(u.get('id'), 'admin'))
                        ui.separator()
                        ui.menu_item(tr('Delete User'), lambda u=user: delete_user(u.get('id'), u.get('username'))).classes('text-red-500')


async def create_stats_view():
    """Display system statistics."""
    # Get various stats
    users_result = await api_call("GET", "/users/", {"limit": 1000})
    corrections_result = await api_call("GET", "/corrections/", {"limit": 1})
    pending_result = await api_call("GET", "/corrections/pending")

    users = users_result if isinstance(users_result, list) else users_result.get('items', users_result.get('users', []))
    pending_corrections = pending_result.get('items', []) if isinstance(pending_result, dict) else pending_result

    # Calculate stats
    total_users = len(users) if isinstance(users, list) else 0
    editors = sum(1 for u in users if u.get('role') in ('editor', 'admin')) if isinstance(users, list) else 0
    pending_count = len(pending_corrections) if isinstance(pending_corrections, list) else 0

    total_corrections = corrections_result.get('total', 0) if isinstance(corrections_result, dict) else 0

    with ui.row().classes('w-full gap-4 flex-wrap'):
        # Users stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('people').classes('text-4xl').style('color: var(--primary-600);')
                # Changed to H3
                h3(str(total_users), classes='text-3xl font-bold')
                ui.label(tr('Total Users')).style('color: var(--text-secondary);')

        # Pending corrections stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('hourglass_empty').classes('text-4xl').style('color: var(--accent-amber);')
                # Changed to H3
                h3(str(pending_count), classes='text-3xl font-bold')
                ui.label(tr('Pending Corrections')).style('color: var(--text-secondary);')

        # Editors stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('edit').classes('text-4xl').style('color: var(--success);')
                # Changed to H3
                h3(str(editors), classes='text-3xl font-bold')
                ui.label(tr('Editors & Admins')).style('color: var(--text-secondary);')

        # Corrections stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('rate_review').classes('text-4xl').style('color: var(--info);')
                # Changed to H3
                h3(str(total_corrections), classes='text-3xl font-bold')
                ui.label(tr('Total Corrections')).style('color: var(--text-secondary);')
