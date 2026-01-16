# -*- coding: utf-8 -*-
"""
Global Authentication State Management

Provides a singleton auth state that persists across all pages using NiceGUI's
app.storage.user mechanism. This allows consistent login state throughout the app.
"""

from typing import Optional, Dict, Any
from nicegui import app, ui
import httpx
import os


def get_api_base() -> str:
    """
    Get the full API base URL for the backend API.

    The backend API runs on port 8000 (separate from web interface on 8081).
    Can be overridden with GENIZAH_BACKEND_PORT environment variable.
    """
    port = int(os.environ.get('GENIZAH_BACKEND_PORT', 8000))
    return f"http://localhost:{port}/api/v1"


class GlobalAuthState:
    """
    Singleton class for managing authentication state globally.
    Uses NiceGUI's app.storage.user for persistence across page navigations.
    """

    # Storage keys
    TOKEN_KEY = 'auth_token'
    USER_KEY = 'auth_user'

    @classmethod
    def get_token(cls) -> Optional[str]:
        """Get the current auth token."""
        return app.storage.user.get(cls.TOKEN_KEY)

    @classmethod
    def get_user(cls) -> Optional[Dict]:
        """Get the current user info."""
        return app.storage.user.get(cls.USER_KEY)

    @classmethod
    def is_logged_in(cls) -> bool:
        """Check if user is logged in."""
        return cls.get_token() is not None and cls.get_user() is not None

    @classmethod
    def get_role(cls) -> Optional[str]:
        """Get current user's role."""
        user = cls.get_user()
        return user.get('role') if user else None

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
    def get_headers(cls) -> Dict[str, str]:
        """Get auth headers for API calls."""
        token = cls.get_token()
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}

    @classmethod
    def set_auth(cls, token: str, user: Dict):
        """Set authentication after successful login."""
        app.storage.user[cls.TOKEN_KEY] = token
        app.storage.user[cls.USER_KEY] = user

    @classmethod
    def clear_auth(cls):
        """Clear authentication (logout)."""
        app.storage.user.pop(cls.TOKEN_KEY, None)
        app.storage.user.pop(cls.USER_KEY, None)

    @classmethod
    def get_username(cls) -> str:
        """Get display name for current user."""
        user = cls.get_user()
        if user:
            return user.get('full_name') or user.get('username') or 'User'
        return ''


async def api_call(method: str, endpoint: str, data: Dict = None, headers: Dict = None) -> Dict:
    """
    Make API call to corrections backend.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE)
        endpoint: API endpoint (e.g., "/auth/login")
        data: Request data (query params for GET, JSON body for POST/PUT)
        headers: Additional headers

    Returns:
        Response JSON or error dict
    """
    base_url = get_api_base()
    url = f"{base_url}{endpoint}"
    all_headers = GlobalAuthState.get_headers()
    if headers:
        all_headers.update(headers)

    async with httpx.AsyncClient() as client:
        try:
            if method == "GET":
                resp = await client.get(url, headers=all_headers, params=data, timeout=10)
            elif method == "POST":
                resp = await client.post(url, headers=all_headers, json=data, timeout=10)
            elif method == "PUT":
                resp = await client.put(url, headers=all_headers, json=data, timeout=10)
            elif method == "DELETE":
                resp = await client.delete(url, headers=all_headers, timeout=10)
            else:
                return {"error": f"Unknown method: {method}"}

            if resp.status_code in (200, 201):
                return resp.json()
            elif resp.status_code == 401:
                # Token expired or invalid - clear auth
                GlobalAuthState.clear_auth()
                return {"error": "Session expired. Please login again.", "status": 401}
            else:
                try:
                    error_detail = resp.json()
                    return {"error": error_detail.get("detail", resp.text), "status": resp.status_code}
                except (ValueError, KeyError, TypeError):
                    return {"error": resp.text, "status": resp.status_code}
        except httpx.TimeoutException:
            return {"error": "Request timeout"}
        except httpx.RequestError as e:
            return {"error": f"Request failed: {type(e).__name__}"}


async def do_login(email: str, password: str) -> Dict:
    """
    Perform login and update global auth state.

    Returns:
        Success dict with user info or error dict
    """
    result = await api_call("POST", "/auth/login", {
        "email": email,
        "password": password
    })

    if "error" in result:
        return result

    # Store token
    token = result.get("access_token")
    if not token:
        return {"error": "No token received"}

    # Get user profile
    GlobalAuthState.set_auth(token, {})  # Temporary set to get headers
    profile = await api_call("GET", "/users/me")

    if "error" in profile:
        GlobalAuthState.clear_auth()
        return profile

    # Update with full user info
    GlobalAuthState.set_auth(token, profile)
    return {"success": True, "user": profile}


async def do_register(email: str, username: str, password: str,
                      full_name: str = None, affiliation: str = None) -> Dict:
    """
    Perform registration and update global auth state.

    Returns:
        Success dict with user info or error dict
    """
    result = await api_call("POST", "/auth/register", {
        "email": email,
        "username": username,
        "password": password,
        "confirm_password": password,
        "full_name": full_name,
        "affiliation": affiliation
    })

    if "error" in result:
        return result

    # Store token
    token = result.get("access_token")
    if not token:
        return {"error": "No token received"}

    # Get user profile
    GlobalAuthState.set_auth(token, {})
    profile = await api_call("GET", "/users/me")

    if "error" in profile:
        GlobalAuthState.clear_auth()
        return profile

    GlobalAuthState.set_auth(token, profile)
    return {"success": True, "user": profile}


def do_logout():
    """Perform logout."""
    GlobalAuthState.clear_auth()


def create_login_dialog():
    """
    Create and return a login/register dialog.
    Returns the dialog object so it can be opened.
    """
    from web.translations import tr

    dialog = ui.dialog().props('persistent')

    with dialog, ui.card().classes('w-96 p-6'):
        with ui.tabs().classes('w-full') as tabs:
            login_tab = ui.tab(tr('Login'))
            register_tab = ui.tab(tr('Register'))

        with ui.tab_panels(tabs, value=login_tab).classes('w-full'):
            # Login panel
            with ui.tab_panel(login_tab):
                with ui.column().classes('w-full gap-4'):
                    login_email = ui.input(tr('Email')).classes('w-full').props('outlined')
                    login_password = ui.input(tr('Password'), password=True).classes('w-full').props('outlined')
                    login_error = ui.label('').classes('text-red-500 text-sm hidden')

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
                            ui.notify(tr('Login successful'), type='positive')
                            dialog.close()
                            ui.navigate.reload()

                    with ui.row().classes('w-full justify-end gap-2'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Login'), on_click=handle_login).props('color=primary')

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
                            ui.notify(tr('Registration successful'), type='positive')
                            dialog.close()
                            ui.navigate.reload()

                    with ui.row().classes('w-full justify-end gap-2'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Register'), on_click=handle_register).props('color=primary')

    return dialog


def create_auth_buttons():
    """
    Create auth buttons for the header.
    Shows login/register if not logged in, or user info + logout if logged in.
    """
    from web.translations import tr

    if GlobalAuthState.is_logged_in():
        user = GlobalAuthState.get_user()
        username = GlobalAuthState.get_username()
        role = GlobalAuthState.get_role() or 'user'

        with ui.row().classes('items-center gap-2'):
            # User avatar/icon
            ui.icon('account_circle').classes('text-2xl text-white/80')

            # User info with dropdown menu
            with ui.button(username, icon='arrow_drop_down').props('flat text-color=white dense').classes('text-sm'):
                with ui.menu().classes('min-w-40'):
                    ui.menu_item(f"Role: {role.title()}", auto_close=False).props('disable')
                    ui.separator()
                    if GlobalAuthState.is_admin():
                        ui.menu_item(tr('Admin Panel'), lambda: ui.navigate.to('/admin'))
                    ui.menu_item(tr('My Corrections'), lambda: ui.navigate.to('/corrections'))
                    ui.separator()

                    def handle_logout():
                        do_logout()
                        ui.notify(tr('Logged out'), type='info')
                        ui.navigate.reload()

                    ui.menu_item(tr('Logout'), handle_logout)
    else:
        # Login/Register buttons
        dialog = create_login_dialog()

        ui.button(tr('Login'), on_click=dialog.open).props('flat text-color=white dense').classes('text-sm')
        ui.button(tr('Register'), on_click=lambda: (dialog.open(), setattr(dialog, '_active_tab', 'register'))).props('outline text-color=white dense').classes('text-sm')
