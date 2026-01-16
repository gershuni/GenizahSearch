# -*- coding: utf-8 -*-
"""
Admin Panel - Genizah Search Pro

User management and system administration for admins.
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call


async def create_admin_page():
    """Create the Admin Panel page."""

    # Check if user is admin
    if not GlobalAuthState.is_admin():
        with ui.column().classes('w-full max-w-3xl mx-auto gap-8 fade-in items-center py-12'):
            ui.icon('lock').classes('text-6xl').style('color: var(--text-muted);')
            ui.label(tr('Access Denied')).classes('text-2xl font-bold').style('color: var(--text-primary);')
            ui.label(tr('You need admin privileges to access this page')).style('color: var(--text-secondary);')
            ui.button(tr('Go Home'), on_click=lambda: ui.navigate.to('/')).props('color=primary')
        return

    with ui.column().classes('w-full max-w-6xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                ui.label(tr('Admin Panel')).classes('text-3xl font-bold').style('color: var(--text-primary);')
                ui.label(tr('User management and system administration')).style('color: var(--text-secondary);')

        # === Tabs ===
        with ui.tabs().classes('w-full') as tabs:
            users_tab = ui.tab(tr('Users'))
            pending_tab = ui.tab(tr('Pending Approval'))
            stats_tab = ui.tab(tr('Statistics'))

        with ui.tab_panels(tabs, value=pending_tab).classes('w-full'):
            # Pending Approval panel
            with ui.tab_panel(pending_tab):
                await create_pending_users_view()

            # All Users panel
            with ui.tab_panel(users_tab):
                await create_users_list_view()

            # Statistics panel
            with ui.tab_panel(stats_tab):
                await create_stats_view()


async def create_pending_users_view():
    """View for approving pending users."""
    result = await api_call("GET", "/admin/users/pending")

    if "error" in result:
        ui.label(f"{tr('Error')}: {result['error']}").style('color: var(--danger);')
        return

    pending = result if isinstance(result, list) else result.get('users', [])

    if not pending:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
            ui.label(tr('No pending users')).classes('text-xl').style('color: var(--text-secondary);')
            ui.label(tr('All registration requests have been processed')).style('color: var(--text-muted);')
    else:
        ui.label(f"{len(pending)} {tr('users pending approval')}").classes('text-lg font-medium mb-4')

        for user in pending:
            await create_pending_user_card(user)


async def create_pending_user_card(user):
    """Create a card for a pending user."""
    with ui.card().classes('w-full p-4 mb-3'):
        with ui.row().classes('w-full items-start justify-between'):
            with ui.column().classes('gap-2 flex-1'):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('person_add').classes('text-2xl').style('color: var(--primary-600);')
                    with ui.column().classes('gap-0'):
                        ui.label(user.get('full_name') or user.get('username', 'Unknown')).classes('font-bold text-lg')
                        ui.label(user.get('email', '')).classes('text-sm').style('color: var(--text-secondary);')

                with ui.row().classes('gap-4 mt-2'):
                    if user.get('affiliation'):
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('business').classes('text-sm').style('color: var(--text-muted);')
                            ui.label(user.get('affiliation')).classes('text-sm')

                    if user.get('created_at'):
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('schedule').classes('text-sm').style('color: var(--text-muted);')
                            ui.label(user.get('created_at', '')[:10]).classes('text-sm')

            with ui.row().classes('gap-2'):
                async def approve_user(uid=user.get('id')):
                    result = await api_call("POST", f"/admin/users/{uid}/approve")
                    if "error" in result:
                        ui.notify(result['error'], type='negative')
                    else:
                        ui.notify(tr('User approved'), type='positive')
                        ui.navigate.reload()

                async def reject_user(uid=user.get('id')):
                    result = await api_call("POST", f"/admin/users/{uid}/reject")
                    if "error" in result:
                        ui.notify(result['error'], type='negative')
                    else:
                        ui.notify(tr('User rejected'), type='info')
                        ui.navigate.reload()

                ui.button(tr('Approve'), on_click=approve_user).props('color=positive')
                ui.button(tr('Reject'), on_click=reject_user).props('flat color=negative')


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
                    result = await api_call("PUT", f"/users/{uid}", {"role": new_role})
                    if "error" in result:
                        ui.notify(result['error'], type='negative')
                    else:
                        ui.notify(tr('Role updated'), type='positive')
                        ui.navigate.reload()

                with ui.button(icon='more_vert').props('flat round dense'):
                    with ui.menu():
                        ui.menu_item(tr('Set as User'), lambda u=user: change_role(u.get('id'), 'user'))
                        ui.menu_item(tr('Set as Editor'), lambda u=user: change_role(u.get('id'), 'editor'))
                        ui.menu_item(tr('Set as Admin'), lambda u=user: change_role(u.get('id'), 'admin'))


async def create_stats_view():
    """Display system statistics."""
    # Get various stats
    users_result = await api_call("GET", "/users/", {"limit": 1000})
    corrections_result = await api_call("GET", "/corrections/", {"limit": 1})

    users = users_result if isinstance(users_result, list) else users_result.get('items', users_result.get('users', []))

    # Calculate stats
    total_users = len(users) if isinstance(users, list) else 0
    pending_users = sum(1 for u in users if u.get('role') == 'pending') if isinstance(users, list) else 0
    editors = sum(1 for u in users if u.get('role') in ('editor', 'admin')) if isinstance(users, list) else 0

    total_corrections = corrections_result.get('total', 0) if isinstance(corrections_result, dict) else 0

    with ui.row().classes('w-full gap-4 flex-wrap'):
        # Users stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('people').classes('text-4xl').style('color: var(--primary-600);')
                ui.label(str(total_users)).classes('text-3xl font-bold')
                ui.label(tr('Total Users')).style('color: var(--text-secondary);')

        # Pending stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('hourglass_empty').classes('text-4xl').style('color: var(--accent-amber);')
                ui.label(str(pending_users)).classes('text-3xl font-bold')
                ui.label(tr('Pending Approval')).style('color: var(--text-secondary);')

        # Editors stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('edit').classes('text-4xl').style('color: var(--success);')
                ui.label(str(editors)).classes('text-3xl font-bold')
                ui.label(tr('Editors & Admins')).style('color: var(--text-secondary);')

        # Corrections stat card
        with ui.card().classes('p-6 flex-1 min-w-48'):
            with ui.column().classes('items-center gap-2'):
                ui.icon('rate_review').classes('text-4xl').style('color: var(--info);')
                ui.label(str(total_corrections)).classes('text-3xl font-bold')
                ui.label(tr('Total Corrections')).style('color: var(--text-secondary);')
