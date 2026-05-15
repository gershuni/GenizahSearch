# -*- coding: utf-8 -*-
"""
Global Authentication State Management (Supabase Version)

Provides a singleton auth state that persists across all pages using NiceGUI's
app.storage.user mechanism. This allows consistent login state throughout the app.

Uses Supabase for authentication instead of the FastAPI backend.
"""

from typing import Optional, Dict
from nicegui import app, ui
import asyncio
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import Supabase client
from web.supabase_client import (
    sign_in as supabase_sign_in, sign_up as supabase_sign_up,
    sign_out as supabase_sign_out, get_profile, update_profile,
    get_oauth_url
)

# Phase 91 AUTHW-01: chokepoint helpers replace raw app.storage.user access.
# Note (Rule-1 deviation from NEW-H2): the plan claimed `app` could be
# dropped from this import line after migrating all app.storage.user sites.
# That claim is incorrect — `create_login_dialog()` below still uses
# `app.storage.browser.*` (lines ~382, 383, 408, 409, 411, 412) for the
# "Remember me" feature, which is a separate NiceGUI storage backend
# (browser localStorage, not server-side per-user storage) and is OUTSIDE
# the Phase 87 lint scanner's scope (the scanner only flags
# `app.storage.user`). Dropping `app` would NameError at first login dialog
# render. ruff F401 will NOT fire because `app` is genuinely referenced.
# We therefore keep `from nicegui import app, ui` unchanged — recorded as
# a deviation in 91-01-SUMMARY.md.
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop


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
        """Get the current user info.

        Phase 91 AUTHW-01 (D-01): safe_user_get already handles AssertionError
        (prune race, debug-logged) and other Exception (warning-logged) per
        web/safe_storage.py contract -- the manual try/except wrapper here
        would suppress the warning-level logging that safe_user_get provides
        for unexpected failures.
        """
        return safe_user_get(cls.USER_KEY)

    @classmethod
    def get_profile(cls) -> Optional[Dict]:
        """Get the current user's profile.

        Phase 91 AUTHW-01 (D-01): see get_user docstring.
        """
        return safe_user_get(cls.PROFILE_KEY)

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
    def set_auth(cls, user: Dict, profile: Dict = None) -> bool:
        """Set authentication after successful login. Returns False if any
        write fails (prune race) -- caller MUST handle by surfacing an error
        and rolling back any pre-existing partial state.

        Phase 91 D-04 (REVISED per Revision MUST-2 / Codex HIGH catch in
        91-REVIEWS.md): multi-write atomicity is best-effort under NiceGUI
        storage semantics (no compare-and-swap). On partial-write failure we
        roll back ALL successful writes SYMMETRICALLY across set_auth's OWN
        keys -- USER_KEY + PROFILE_KEY (2 keys; auth_session is the outer
        caller's responsibility, not set_auth's) so callers never observe a
        half-state AND so stale auth_profile from a prior session cannot
        survive (GlobalAuthState.get_role()/is_admin()/is_editor() read profile
        independently of user -- leaving stale auth_profile would be a
        security/correctness leak per Codex's HIGH).

        NEW-M1 (round-2 cross-AI review) wording note: "SYMMETRIC rollback"
        here means USER_KEY + PROFILE_KEY only (2 keys). Do NOT add an
        auth_session pop inside set_auth -- that key is owned by the outer
        caller (do_login / _oauth_complete_login), which performs its own
        DEFENSIVE 3-key cleanup if set_auth returns False.

        Semantic shift for profile=None:
            Previously: `if profile:` skipped the profile branch entirely,
                        leaving any stale auth_profile from a prior session.
            Now:        `profile is None` triggers a best-effort pop of the
                        PROFILE_KEY so a new login with no profile cannot
                        inherit a stale role.
        """
        if not safe_user_set(cls.USER_KEY, user):
            return False
        if profile is not None:
            if not safe_user_set(cls.PROFILE_KEY, profile):
                # SYMMETRIC 2-key rollback: pop BOTH the user write AND any
                # stale profile to ensure no half-state and no role leak.
                # auth_session is NOT touched here -- outer caller owns it.
                safe_user_pop(cls.USER_KEY, None)
                safe_user_pop(cls.PROFILE_KEY, None)
                return False
        else:
            # profile is None: clear any stale auth_profile from a prior
            # session (best-effort -- safe_user_pop returns the default on
            # any failure including no-such-key, so this is idempotent).
            safe_user_pop(cls.PROFILE_KEY, None)
        # Identify user in PostHog for session tracking
        cls._posthog_identify(user, profile)
        return True

    @classmethod
    def _posthog_identify(cls, user: Dict, profile: Dict = None):
        """Send posthog.identify() to the browser if PostHog is loaded."""
        try:
            import json
            uid = json.dumps(user.get('id', ''))
            email = json.dumps(user.get('email', ''))
            name = json.dumps((profile or {}).get('full_name', '') or (profile or {}).get('username', ''))
            js = f"if(window.posthog)posthog.identify({uid},{{email:{email},name:{name}}})"
            ui.run_javascript(js)
        except Exception:
            pass  # PostHog not loaded or no client connection yet

    @classmethod
    def update_profile_cache(cls, profile: Dict):
        """Update the cached profile.

        Phase 91 D-04 (best-effort note): this is an update-on-existing-state
        path, not a login boundary. A failed write leaves the prior profile
        in storage unchanged -- correct half-state for a profile-only update.
        No return value (void) and no rollback.

        Note: Revision MAY-8 (Codex defensive suggestion to also check
        profile['id'] == auth_user['id'] before writing) is DEFERRED to
        Phase 92's final-sweep audit per 91-REVIEWS.md item 8. update_profile_cache
        is called only after a successful Supabase profile fetch by the
        current user; the cross-user case is theoretical and best handled
        as a cross-cutting defensive hardening pass.
        """
        safe_user_set(cls.PROFILE_KEY, profile)

    @classmethod
    def clear_auth(cls):
        """Clear authentication (logout). Revoke server-side BEFORE local cleanup
        so the token is actually invalidated on Supabase's side (Phase 90 D-11).

        Local key cleanup happens in a finally block so it runs even when
        server revocation fails -- half-state (revoked server-side but local
        keys still present) is worse than no revocation at all.

        Phase 91 AUTHW-01 (D-01): 3 raw pops -> safe_user_pop. The deferred
        safe_user_get import is now redundant -- module-top import covers it.
        """
        auth_session = safe_user_get('auth_session') or {}
        access_token = auth_session.get('access_token')
        try:
            # AUTHW-04: server-side revocation FIRST, with the user's own token
            supabase_sign_out(access_token)
        except Exception:
            pass  # Server revocation failed; local cleanup still runs below
        finally:
            # AUTHW-03 + AUTHW-01: local keys popped unconditionally (no half-state)
            safe_user_pop(cls.USER_KEY, None)
            safe_user_pop(cls.PROFILE_KEY, None)
            safe_user_pop('auth_session', None)
        # PostHog reset stays as-is
        try:
            ui.run_javascript('if(window.posthog)posthog.reset()')
        except Exception:
            pass  # PostHog analytics optional; failure is non-fatal

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
    """Perform login and update global auth state.

    Phase 91 D-05 (REVISED per Revision MUST-2): session-first ordering.
    The smallest-blast-radius write (the auth_session key) goes first;
    on failure we return an explicit error so the caller doesn't reload
    into a half-logged-in state. If the set_auth call later fails, we
    DEFENSIVELY pop ALL 3 auth keys (set_auth's own SYMMETRIC 2-key
    rollback should have handled user/profile, but the outer 3-key
    cleanup covers the case where set_auth's internal rollback also
    failed -- Codex HIGH catch in 91-REVIEWS.md round 1).

    NEW-M1 wording note: this is the DEFENSIVE 3-key cleanup at the
    CALLER level. set_auth itself does SYMMETRIC 2-key rollback
    (USER_KEY + PROFILE_KEY only). Together they form layered defense.

    NEW-L2 (round-2 cross-AI review polish): both storage-failure
    posthog events emit `'method': 'password'` for parity with
    `_oauth_complete_login`'s `'method': 'google_oauth'` -- consistent
    dashboard slicing for partial-write failures by login method.

    Returns:
        Success dict with user info or error dict.
    """
    from nicegui import run
    from web.analytics import posthog_capture
    result = await run.io_bound(supabase_sign_in, email, password)

    if "error" in result:
        posthog_capture('login_failed', {
            'reason': str(result.get('error', ''))[:100],
            'error_code': str(result.get('error_code', ''))[:50],
            'status_code': result.get('status_code', ''),
            'method': 'password',
        })
        return result

    session = result.get('session', {})
    user = result.get('user')
    if not user:
        posthog_capture('login_failed', {'reason': 'No user returned', 'method': 'password'})
        return {"error": "No user returned"}

    profile = get_profile(user['id'])

    # D-05 (REVISED): session-first; defensive 3-key cleanup on later failure.
    if session:
        if not safe_user_set('auth_session', {
            'access_token': session.get('access_token'),
            'refresh_token': session.get('refresh_token'),
        }):
            posthog_capture('login_failed', {
                'reason': 'session_storage_unavailable',
                'method': 'password',
            })
            return {"error": "Session storage unavailable. Please try again."}
    if not GlobalAuthState.set_auth(user, profile):
        # set_auth should have rolled back its own user/profile writes
        # SYMMETRICALLY (2 keys) per Revision MUST-2. We DEFENSIVELY pop
        # all 3 keys in case set_auth's internal rollback also failed
        # (prune race during rollback). Best-effort -- safe_user_pop
        # returns the default on any failure including no-such-key.
        safe_user_pop('auth_session', None)
        safe_user_pop('auth_user', None)
        safe_user_pop('auth_profile', None)
        posthog_capture('login_failed', {
            'reason': 'auth_state_storage_unavailable',
            'method': 'password',
        })
        return {"error": "Session storage unavailable. Please try again."}
    posthog_capture('login_success', {'method': 'password'})
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

    from nicegui import run
    result = await run.io_bound(supabase_sign_up, email, password, metadata if metadata else None)

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

        await run.io_bound(update_profile, user['id'], profile_data)

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
                            # Brief delay so PostHog login_success JS capture can flush before page reload
                            await asyncio.sleep(0.3)
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
