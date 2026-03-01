# -*- coding: utf-8 -*-
"""
Global Authentication State Management (Supabase Version)

Provides a singleton auth state that persists across all pages using NiceGUI's
app.storage.user mechanism. This allows consistent login state throughout the app.

Uses Supabase for authentication instead of the FastAPI backend.
"""

from typing import Optional, Dict, Any
from nicegui import app, ui
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import Supabase client
from web.supabase_client import (
    get_client, sign_in as supabase_sign_in, sign_up as supabase_sign_up,
    sign_out as supabase_sign_out, get_profile, update_profile,
    get_oauth_url, set_session_from_url
)


class GlobalAuthState:
    """
    Singleton class for managing authentication state globally.
    Uses NiceGUI's app.storage.user for persistence across page navigations.
    """

    # Storage keys
    USER_KEY = 'auth_user'
    PROFILE_KEY = 'auth_profile'

    @classmethod
    def get_user(cls) -> Optional[Dict]:
        """Get the current user info."""
        return app.storage.user.get(cls.USER_KEY)

    @classmethod
    def get_profile(cls) -> Optional[Dict]:
        """Get the current user's profile."""
        return app.storage.user.get(cls.PROFILE_KEY)

    @classmethod
    def is_logged_in(cls) -> bool:
        """Check if user is logged in."""
        return cls.get_user() is not None

    @classmethod
    def get_user_id(cls) -> Optional[str]:
        """Get current user's ID."""
        user = cls.get_user()
        return user.get('id') if user else None

    @classmethod
    def get_role(cls) -> Optional[str]:
        """Get current user's role."""
        profile = cls.get_profile()
        return profile.get('role') if profile else None

    @classmethod
    def is_admin(cls) -> bool:
        """Check if current user is admin."""
        return cls.get_role() == 'admin'

    @classmethod
    def is_editor(cls) -> bool:
        """Check if current user has editor permissions."""
        role = cls.get_role()
        return role in ('editor', 'admin')

    @classmethod
    def can_edit(cls) -> bool:
        """Check if user can edit (editor or admin)."""
        return cls.is_editor()

    @classmethod
    def can_comment(cls) -> bool:
        """Check if user can comment (any logged in user)."""
        return cls.is_logged_in()

    @classmethod
    def set_auth(cls, user: Dict, profile: Dict = None):
        """Set authentication after successful login."""
        app.storage.user[cls.USER_KEY] = user
        if profile:
            app.storage.user[cls.PROFILE_KEY] = profile
        # Identify user in PostHog for session tracking
        cls._posthog_identify(user, profile)

    @classmethod
    def _posthog_identify(cls, user: Dict, profile: Dict = None):
        """Send posthog.identify() to the browser if PostHog is loaded."""
        try:
            uid = user.get('id', '')
            email = user.get('email', '')
            name = (profile or {}).get('full_name', '') or (profile or {}).get('username', '')
            js = (
                f"if(window.posthog)posthog.identify('{uid}',"
                f"{{email:'{email}',name:'{name}'}})"
            )
            ui.run_javascript(js)
        except Exception:
            pass  # PostHog not loaded or no client connection yet

    @classmethod
    def update_profile_cache(cls, profile: Dict):
        """Update the cached profile."""
        app.storage.user[cls.PROFILE_KEY] = profile

    @classmethod
    def clear_auth(cls):
        """Clear authentication (logout)."""
        app.storage.user.pop(cls.USER_KEY, None)
        app.storage.user.pop(cls.PROFILE_KEY, None)
        app.storage.user.pop('auth_session', None)
        # Reset PostHog identity on logout
        try:
            ui.run_javascript('if(window.posthog)posthog.reset()')
        except Exception:
            pass
        # Also sign out from Supabase client
        try:
            supabase_sign_out()
        except Exception:
            pass

    @classmethod
    def get_username(cls) -> str:
        """Get display name for current user."""
        profile = cls.get_profile()
        if profile:
            return profile.get('full_name') or profile.get('username') or 'User'
        user = cls.get_user()
        if user:
            return user.get('email', '').split('@')[0] or 'User'
        return ''

    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Get auth headers for API calls (legacy compatibility)."""
        # Supabase handles auth internally, but keep this for any legacy code
        return {}


async def do_login(email: str, password: str) -> Dict:
    """
    Perform login and update global auth state.

    Returns:
        Success dict with user info or error dict
    """
    result = supabase_sign_in(email, password)

    if "error" in result:
        return result

    # Store session tokens for per-user Supabase client
    session = result.get('session', {})
    if session:
        app.storage.user['auth_session'] = {
            'access_token': session.get('access_token'),
            'refresh_token': session.get('refresh_token'),
        }

    user = result.get('user')
    if not user:
        return {"error": "No user returned"}

    # Get user profile
    profile = get_profile(user['id'])

    # Update global state
    GlobalAuthState.set_auth(user, profile)

    return {"success": True, "user": user, "profile": profile}


async def do_register(email: str, username: str, password: str,
                      full_name: str = None, affiliation: str = None) -> Dict:
    """
    Perform registration and automatically log in.

    Returns:
        Success dict with user info or error dict
    """
    # Register with metadata
    metadata = {}
    if full_name:
        metadata['full_name'] = full_name
    if affiliation:
        metadata['affiliation'] = affiliation

    result = supabase_sign_up(email, password, metadata if metadata else None)

    if "error" in result:
        return result

    user = result.get('user')
    if not user:
        return {"error": "Registration failed - no user returned"}

    # Update profile with additional info
    if username or full_name or affiliation:
        profile_data = {}
        if username:
            profile_data['username'] = username
        if full_name:
            profile_data['full_name'] = full_name
        if affiliation:
            profile_data['affiliation'] = affiliation

        update_profile(user['id'], profile_data)

    # Auto-login after registration
    return await do_login(email, password)


def do_logout():
    """Perform logout."""
    GlobalAuthState.clear_auth()


def create_login_dialog():
    """
    Create and return a login/register dialog.
    Returns the dialog object so it can be opened.
    """
    from web.translations import tr

    # Keys for browser storage (localStorage)
    REMEMBER_EMAIL_KEY = 'genizah_remember_email'
    REMEMBER_CHECKED_KEY = 'genizah_remember_checked'

    dialog = ui.dialog()  # Removed 'persistent' to allow Esc to close

    with dialog, ui.card().classes('w-96 p-6').style('background: var(--bg-card); color: var(--text-primary);'):
        with ui.tabs().classes('w-full') as tabs:
            login_tab = ui.tab(tr('Login'))
            register_tab = ui.tab(tr('Register'))

        with ui.tab_panels(tabs, value=login_tab).classes('w-full'):
            # Login panel
            with ui.tab_panel(login_tab):
                with ui.column().classes('w-full gap-4'):
                    login_email = ui.input(tr('Email')).classes('w-full').props('outlined')
                    login_password = ui.input(tr('Password'), password=True).classes('w-full').props('outlined')

                    # Remember me checkbox
                    remember_me = ui.checkbox(tr('Remember me')).classes('w-full')

                    login_error = ui.label('').classes('text-red-500 text-sm hidden')

                    # Load saved email from browser storage
                    def load_saved_email():
                        try:
                            saved_email = app.storage.browser.get(REMEMBER_EMAIL_KEY, '')
                            was_checked = app.storage.browser.get(REMEMBER_CHECKED_KEY, False)
                            if saved_email and was_checked:
                                login_email.value = saved_email
                                remember_me.value = True
                        except Exception:
                            pass  # Browser storage may not be available

                    # Load saved email when dialog opens
                    dialog.on('show', load_saved_email)

                    async def handle_login():
                        login_error.classes('hidden', remove='visible')
                        if not login_email.value or not login_password.value:
                            login_error.text = tr('Please fill in all fields')
                            login_error.classes('visible', remove='hidden')
                            return

                        result = await do_login(login_email.value, login_password.value)
                        if "error" in result:
                            login_error.text = result["error"]
                            login_error.classes('visible', remove='hidden')
                        else:
                            # Save or clear email based on "Remember me" checkbox
                            try:
                                if remember_me.value:
                                    app.storage.browser[REMEMBER_EMAIL_KEY] = login_email.value
                                    app.storage.browser[REMEMBER_CHECKED_KEY] = True
                                else:
                                    app.storage.browser.pop(REMEMBER_EMAIL_KEY, None)
                                    app.storage.browser.pop(REMEMBER_CHECKED_KEY, None)
                            except Exception:
                                pass  # Browser storage may not be available
                            dialog.close()
                            ui.navigate.reload()

                    with ui.row().classes('w-full justify-end gap-2'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Login'), on_click=handle_login).props('color=primary')

                    # Enter key to submit
                    login_password.on('keydown.enter', handle_login)

                    # Divider
                    with ui.row().classes('w-full items-center gap-2 my-2'):
                        ui.element('div').classes('flex-1 h-px bg-gray-300')
                        ui.label(tr('or')).classes('text-gray-500 text-sm')
                        ui.element('div').classes('flex-1 h-px bg-gray-300')

                    # Google login button
                    async def handle_google_login():
                        login_error.classes('hidden', remove='visible')
                        # Get current URL for redirect
                        redirect_url = f"{os.environ.get('SITE_URL', 'https://genizahsearch.com')}/auth/callback"
                        result = get_oauth_url('google', redirect_url)
                        if "error" in result:
                            login_error.text = result["error"]
                            login_error.classes('visible', remove='hidden')
                        else:
                            # Redirect to Google OAuth
                            ui.navigate.to(result['url'], new_tab=False)

                    ui.button(tr('Login with Google'), icon='img:https://www.google.com/favicon.ico',
                              on_click=handle_google_login).classes('w-full').props('outline')

            # Register panel
            with ui.tab_panel(register_tab):
                with ui.column().classes('w-full gap-3'):
                    reg_email = ui.input(tr('Email')).classes('w-full').props('outlined dense')
                    reg_username = ui.input(tr('Username')).classes('w-full').props('outlined dense')
                    reg_fullname = ui.input(tr('Full Name')).classes('w-full').props('outlined dense')
                    reg_affiliation = ui.input(tr('Affiliation (optional)')).classes('w-full').props('outlined dense')
                    reg_password = ui.input(tr('Password'), password=True).classes('w-full').props('outlined dense')
                    reg_confirm = ui.input(tr('Confirm Password'), password=True).classes('w-full').props('outlined dense')
                    reg_error = ui.label('').classes('text-red-500 text-sm hidden')

                    async def handle_register():
                        reg_error.classes('hidden', remove='visible')

                        if not all([reg_email.value, reg_username.value, reg_password.value, reg_confirm.value]):
                            reg_error.text = tr('Please fill in required fields')
                            reg_error.classes('visible', remove='hidden')
                            return

                        if reg_password.value != reg_confirm.value:
                            reg_error.text = tr('Passwords do not match')
                            reg_error.classes('visible', remove='hidden')
                            return

                        result = await do_register(
                            reg_email.value,
                            reg_username.value,
                            reg_password.value,
                            reg_fullname.value or None,
                            reg_affiliation.value or None
                        )

                        if "error" in result:
                            reg_error.text = result["error"]
                            reg_error.classes('visible', remove='hidden')
                        else:
                            dialog.close()
                            ui.navigate.reload()

                    with ui.row().classes('w-full justify-end gap-2'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Register'), on_click=handle_register).props('color=primary')

                    # Enter key to submit
                    reg_confirm.on('keydown.enter', handle_register)

                    # Divider
                    with ui.row().classes('w-full items-center gap-2 my-2'):
                        ui.element('div').classes('flex-1 h-px bg-gray-300')
                        ui.label(tr('or')).classes('text-gray-500 text-sm')
                        ui.element('div').classes('flex-1 h-px bg-gray-300')

                    # Google signup button (same as login - will create account if needed)
                    async def handle_google_signup():
                        reg_error.classes('hidden', remove='visible')
                        redirect_url = f"{os.environ.get('SITE_URL', 'https://genizahsearch.com')}/auth/callback"
                        result = get_oauth_url('google', redirect_url)
                        if "error" in result:
                            reg_error.text = result["error"]
                            reg_error.classes('visible', remove='hidden')
                        else:
                            ui.navigate.to(result['url'], new_tab=False)

                    ui.button(tr('Sign up with Google'), icon='img:https://www.google.com/favicon.ico',
                              on_click=handle_google_signup).classes('w-full').props('outline')

                    # Note about desktop app
                    ui.label(tr('For desktop app login, set a password in your profile after signing up.')).classes('text-xs text-center mt-1').style('color: var(--text-muted);')

    # Store tabs reference for external access
    dialog.tabs = tabs
    dialog.login_tab = login_tab
    dialog.register_tab = register_tab
    return dialog


def create_auth_buttons():
    """
    Create auth buttons for the header.
    Shows login/register if not logged in, or user info + logout if logged in.
    """
    from web.translations import tr

    if GlobalAuthState.is_logged_in():
        profile = GlobalAuthState.get_profile()
        username = GlobalAuthState.get_username()
        role = GlobalAuthState.get_role() or 'user'

        with ui.row().classes('items-center gap-2'):
            # User avatar/icon
            ui.icon('account_circle').classes('text-2xl text-white/80')

            # User info with dropdown menu
            with ui.button(username, icon='arrow_drop_down').props('flat text-color=white dense').classes('text-sm'):
                with ui.menu().classes('min-w-40').style('background: var(--bg-card); color: var(--text-primary);'):
                    ui.menu_item(f"Role: {role.title()}", auto_close=False).props('disable')
                    ui.separator()
                    if GlobalAuthState.is_admin():
                        ui.menu_item(tr('Admin Panel'), lambda: ui.navigate.to('/admin'))
                    ui.menu_item(tr('My Profile'), lambda: ui.navigate.to('/profile'))
                    ui.menu_item(tr('My Corrections'), lambda: ui.navigate.to('/corrections'))
                    ui.separator()

                    def handle_logout():
                        do_logout()
                        ui.notify(tr('Logged out'), type='info')
                        ui.navigate.reload()

                    ui.menu_item(tr('Logout'), handle_logout)
    else:
        # Login/Register buttons (dialog built lazily on first click)
        dialog = None

        def _ensure_dialog():
            nonlocal dialog
            if dialog is None:
                dialog = create_login_dialog()
            return dialog

        def open_login():
            d = _ensure_dialog()
            d.tabs.set_value(d.login_tab)
            d.open()

        def open_register():
            d = _ensure_dialog()
            d.tabs.set_value(d.register_tab)
            d.open()

        ui.button(tr('Login'), on_click=open_login).props('flat text-color=white dense').classes('text-sm')
        ui.button(tr('Register'), on_click=open_register).props('outline text-color=white dense').classes('text-sm')


# ============================================================================
# LEGACY COMPATIBILITY - Keep these for code that still uses the old API
# ============================================================================

def get_api_base() -> str:
    """Legacy function - returns empty string as we use Supabase now."""
    return ""


async def api_call(method: str, endpoint: str, data: Dict = None,
                   headers: Dict = None, timeout: int = None,
                   _retry_after_refresh: bool = True) -> Dict:
    """
    Legacy API call function - redirects to Supabase client functions.

    This maintains backward compatibility with existing code while we migrate.
    """
    # Map old endpoints to Supabase functions
    if '/auth/login' in endpoint:
        if data:
            return await do_login(data.get('email', ''), data.get('password', ''))
        return {'error': 'Missing credentials'}

    if '/auth/register' in endpoint:
        if data:
            return await do_register(
                data.get('email', ''),
                data.get('username', ''),
                data.get('password', ''),
                data.get('full_name'),
                data.get('affiliation')
            )
        return {'error': 'Missing registration data'}

    # For other endpoints, return an error indicating migration needed
    return {'error': f'Endpoint {endpoint} needs migration to Supabase'}
