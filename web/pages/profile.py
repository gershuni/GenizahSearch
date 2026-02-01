# -*- coding: utf-8 -*-
"""
User Profile Page - Dicta Genizah Search

Edit user profile details and change password.
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, create_login_dialog
from web.supabase_client import update_profile, get_client
from web.components.typography import h1, h2, h3


async def create_profile_page():
    """Create the User Profile page."""

    # Check if user is logged in
    if not GlobalAuthState.is_logged_in():
        with ui.column().classes('w-full max-w-3xl mx-auto gap-8 fade-in items-center py-12'):
            ui.icon('account_circle').classes('text-6xl').style('color: var(--text-muted);')
            # Changed to H2
            h2(tr('Login Required'), classes='text-2xl font-bold', style='color: var(--text-primary);')
            ui.label(tr('Please login to view your profile')).style('color: var(--text-secondary);')

            login_dialog = create_login_dialog()
            ui.button(tr('Login'), on_click=login_dialog.open).props('color=primary')
        return

    user = GlobalAuthState.get_user()

    with ui.column().classes('w-full max-w-3xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('My Profile'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Edit your details and password')).style('color: var(--text-secondary);')

        # === Profile Details Card ===
        with ui.card().classes('w-full p-6'):
            # Changed to H2
            h2(tr('Profile Details'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-4'):
                # Username (read-only)
                ui.input(
                    label=tr('Username'),
                    value=user.get('username', '')
                ).classes('w-full').props('outlined readonly').style('direction: ltr;')

                # Email (read-only)
                ui.input(
                    label=tr('Email'),
                    value=user.get('email', '')
                ).classes('w-full').props('outlined readonly').style('direction: ltr;')

                # Full Name (editable)
                full_name_input = ui.input(
                    label=tr('Full Name'),
                    value=user.get('full_name', '')
                ).classes('w-full').props('outlined')

                # Affiliation (editable)
                affiliation_input = ui.input(
                    label=tr('Affiliation'),
                    value=user.get('affiliation', '')
                ).classes('w-full').props('outlined')

                # Bio (editable)
                bio_input = ui.textarea(
                    label=tr('Bio'),
                    value=user.get('bio', '')
                ).classes('w-full').props('outlined rows=3')

                # Save button
                def save_profile():
                    user_id = GlobalAuthState.get_user_id()
                    if not user_id:
                        ui.notify(tr('User not found'), type='negative')
                        return

                    update_data = {
                        'full_name': full_name_input.value or None,
                        'affiliation': affiliation_input.value or None,
                        'bio': bio_input.value or None
                    }

                    result = update_profile(user_id, update_data)

                    if "error" in result:
                        ui.notify(result.get('error', 'Update failed'), type='negative')
                    else:
                        # Update cached profile
                        profile = result.get('profile', {})
                        GlobalAuthState.update_profile_cache(profile)
                        ui.notify(tr('Profile updated'), type='positive')

                with ui.row().classes('w-full justify-end'):
                    ui.button(tr('Save'), icon='save', on_click=save_profile).props('color=primary')

        # === Change Password Card ===
        with ui.card().classes('w-full p-6'):
            # Changed to H2
            h2(tr('Change Password'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-4'):
                current_password_input = ui.input(
                    label=tr('Current Password'),
                    password=True,
                    password_toggle_button=True
                ).classes('w-full').props('outlined').style('direction: ltr;')

                new_password_input = ui.input(
                    label=tr('New Password'),
                    password=True,
                    password_toggle_button=True
                ).classes('w-full').props('outlined').style('direction: ltr;')

                confirm_password_input = ui.input(
                    label=tr('Confirm New Password'),
                    password=True,
                    password_toggle_button=True
                ).classes('w-full').props('outlined').style('direction: ltr;')

                password_error = ui.label('').classes('text-red-500 text-sm hidden')

                def change_password():
                    password_error.classes('hidden', remove='visible')

                    if not current_password_input.value:
                        password_error.text = tr('Please enter current password')
                        password_error.classes('visible', remove='hidden')
                        return

                    if not new_password_input.value:
                        password_error.text = tr('Please enter new password')
                        password_error.classes('visible', remove='hidden')
                        return

                    if new_password_input.value != confirm_password_input.value:
                        password_error.text = tr('Passwords do not match')
                        password_error.classes('visible', remove='hidden')
                        return

                    if len(new_password_input.value) < 8:
                        password_error.text = tr('Password must be at least 8 characters')
                        password_error.classes('visible', remove='hidden')
                        return

                    try:
                        # Use Supabase to update password
                        client = get_client()
                        response = client.auth.update_user({'password': new_password_input.value})

                        if response and response.user:
                            ui.notify(tr('Password changed successfully'), type='positive')
                            # Clear password fields
                            current_password_input.value = ''
                            new_password_input.value = ''
                            confirm_password_input.value = ''
                        else:
                            password_error.text = tr('Failed to change password')
                            password_error.classes('visible', remove='hidden')
                    except Exception as e:
                        password_error.text = str(e)
                        password_error.classes('visible', remove='hidden')

                with ui.row().classes('w-full justify-end'):
                    ui.button(tr('Change Password'), icon='lock', on_click=change_password).props('color=primary')

        # === Account Info Card ===
        with ui.card().classes('w-full p-6'):
            # Changed to H2
            h2(tr('Account Information'), classes='text-xl font-bold mb-4')

            with ui.row().classes('w-full gap-8 flex-wrap'):
                with ui.column().classes('gap-1'):
                    ui.label(tr('Role')).classes('text-sm').style('color: var(--text-muted);')
                    role = user.get('role', 'contributor')
                    ui.label(role.title()).classes('font-medium')

                with ui.column().classes('gap-1'):
                    ui.label(tr('Reputation')).classes('text-sm').style('color: var(--text-muted);')
                    ui.label(str(user.get('reputation_score', 0))).classes('font-medium')

                with ui.column().classes('gap-1'):
                    ui.label(tr('Corrections')).classes('text-sm').style('color: var(--text-muted);')
                    ui.label(str(user.get('corrections_count', 0))).classes('font-medium')

                with ui.column().classes('gap-1'):
                    ui.label(tr('Member Since')).classes('text-sm').style('color: var(--text-muted);')
                    created_at = user.get('created_at', '')
                    if created_at:
                        # Handle both datetime objects and strings
                        if hasattr(created_at, 'strftime'):
                            ui.label(created_at.strftime('%Y-%m-%d')).classes('font-medium')
                        elif isinstance(created_at, str) and len(created_at) >= 10:
                            ui.label(created_at[:10]).classes('font-medium')
                        else:
                            ui.label('-').classes('font-medium')
                    else:
                        ui.label('-').classes('font-medium')
