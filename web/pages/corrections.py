# -*- coding: utf-8 -*-
"""
Corrections Page - Genizah Search Pro

User corrections system: submit, review, and manage transcription corrections.
"""

import os
from nicegui import ui, app
from web.state import state
from web.translations import tr
import httpx
from typing import Optional, Dict, Any

# API base URL (same server) - will be set dynamically
API_BASE = "/api/v1"


def get_api_base():
    """
    Get the full API base URL.

    Returns the API base URL, preferring the current request's origin
    to support both local and remote access.
    """
    # Try to get the current request's origin
    try:
        # Get the host from the request if available
        from starlette.requests import Request
        from nicegui import context

        if hasattr(context, 'client') and context.client:
            # Use the same protocol and host as the current page
            request = context.client.request
            if request:
                scheme = request.url.scheme
                host = request.url.netloc
                return f"{scheme}://{host}/api/v1"
    except:
        pass

    # Fallback to localhost with configured port
    port = os.environ.get('GENIZAH_PORT', 8081)
    return f"http://localhost:{port}/api/v1"


class CorrectionsState:
    """State management for corrections page."""

    def __init__(self):
        self.current_user: Optional[Dict] = None
        self.token: Optional[str] = None
        self.corrections: list = []
        self.pending_count: int = 0

    def is_logged_in(self) -> bool:
        return self.token is not None

    def get_headers(self) -> Dict[str, str]:
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        return {}

    def clear(self):
        self.current_user = None
        self.token = None
        self.corrections = []


corrections_state = CorrectionsState()


async def api_call(method: str, endpoint: str, data: Dict = None, headers: Dict = None) -> Dict:
    """Make API call to corrections backend with error handling."""
    base_url = get_api_base()
    url = f"{base_url}{endpoint}"
    all_headers = corrections_state.get_headers()
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
                # Token expired or invalid - clear auth state
                corrections_state.clear()
                app.storage.user.pop('corrections_token', None)
                app.storage.user.pop('corrections_user', None)
                return {"error": "Session expired. Please log in again.", "status": 401, "expired": True}
            else:
                # Try to parse error detail from response
                try:
                    error_data = resp.json()
                    error_msg = error_data.get("detail", resp.text)
                except:
                    error_msg = resp.text
                return {"error": error_msg, "status": resp.status_code}
        except httpx.TimeoutException:
            return {"error": "Request timed out. Please try again."}
        except httpx.ConnectError:
            return {"error": "Cannot connect to server. Please check your connection."}
        except Exception as e:
            return {"error": f"Request failed: {str(e)}"}


async def create_corrections_page():
    """Create the Corrections page."""

    # Load stored auth if available
    stored_token = app.storage.user.get('corrections_token')
    stored_user = app.storage.user.get('corrections_user')
    if stored_token and stored_user:
        corrections_state.token = stored_token
        corrections_state.current_user = stored_user

    with ui.column().classes('w-full max-w-5xl mx-auto gap-8 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                ui.label(tr('Corrections')).classes('text-3xl font-bold').style('color: var(--text-primary);')
                ui.label(tr('Submit and review transcription corrections')).style('color: var(--text-secondary);')

        # Main content container
        main_container = ui.column().classes('w-full gap-6')

        async def refresh_page():
            """Refresh the page content."""
            main_container.clear()
            with main_container:
                if corrections_state.is_logged_in():
                    await create_logged_in_view()
                else:
                    create_login_view()

        def create_login_view():
            """Create login/register view."""
            with ui.card().classes('w-full p-6'):
                with ui.tabs().classes('w-full') as tabs:
                    login_tab = ui.tab(tr('Login'))
                    register_tab = ui.tab(tr('Register'))

                with ui.tab_panels(tabs, value=login_tab).classes('w-full'):
                    # Login panel
                    with ui.tab_panel(login_tab):
                        with ui.column().classes('w-full gap-4'):
                            email_input = ui.input(tr('Email')).classes('w-full').props('outlined')
                            password_input = ui.input(tr('Password'), password=True).classes('w-full').props('outlined')

                            async def do_login():
                                result = await api_call("POST", "/auth/login", {
                                    "email": email_input.value,
                                    "password": password_input.value
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    corrections_state.token = result["access_token"]
                                    # Get user profile
                                    profile = await api_call("GET", "/users/me")
                                    if "error" not in profile:
                                        corrections_state.current_user = profile
                                        app.storage.user['corrections_token'] = corrections_state.token
                                        app.storage.user['corrections_user'] = profile
                                    ui.notify(tr('Login successful'), type='positive')
                                    await refresh_page()

                            ui.button(tr('Login'), on_click=do_login).classes('w-full').props('color=primary')

                    # Register panel
                    with ui.tab_panel(register_tab):
                        with ui.column().classes('w-full gap-4'):
                            reg_email = ui.input(tr('Email')).classes('w-full').props('outlined')
                            reg_username = ui.input(tr('Username')).classes('w-full').props('outlined')
                            reg_fullname = ui.input(tr('Full Name')).classes('w-full').props('outlined')
                            reg_affiliation = ui.input(tr('Affiliation (optional)')).classes('w-full').props('outlined')
                            reg_password = ui.input(tr('Password'), password=True).classes('w-full').props('outlined')
                            reg_confirm = ui.input(tr('Confirm Password'), password=True).classes('w-full').props('outlined')

                            async def do_register():
                                if reg_password.value != reg_confirm.value:
                                    ui.notify(tr('Passwords do not match'), type='negative')
                                    return

                                result = await api_call("POST", "/auth/register", {
                                    "email": reg_email.value,
                                    "username": reg_username.value,
                                    "full_name": reg_fullname.value,
                                    "affiliation": reg_affiliation.value or None,
                                    "password": reg_password.value,
                                    "confirm_password": reg_confirm.value
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    corrections_state.token = result["access_token"]
                                    profile = await api_call("GET", "/users/me")
                                    if "error" not in profile:
                                        corrections_state.current_user = profile
                                        app.storage.user['corrections_token'] = corrections_state.token
                                        app.storage.user['corrections_user'] = profile
                                    ui.notify(tr('Registration successful'), type='positive')
                                    await refresh_page()

                            ui.button(tr('Register'), on_click=do_register).classes('w-full').props('color=primary')

        async def create_logged_in_view():
            """Create view for logged in users."""
            user = corrections_state.current_user

            # User info bar
            with ui.card().classes('w-full p-4'):
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('account_circle').classes('text-3xl').style('color: var(--primary-600);')
                        with ui.column().classes('gap-0'):
                            ui.label(user.get('full_name', user.get('username', ''))).classes('font-bold')
                            ui.label(f"{tr('Role')}: {user.get('role', 'contributor').title()} | {tr('Reputation')}: {user.get('reputation_score', 0)}").classes('text-sm').style('color: var(--text-secondary);')

                    async def do_logout():
                        corrections_state.clear()
                        app.storage.user.pop('corrections_token', None)
                        app.storage.user.pop('corrections_user', None)
                        ui.notify(tr('Logged out'), type='info')
                        await refresh_page()

                    ui.button(tr('Logout'), on_click=do_logout).props('flat color=negative')

            # Tabs for different views
            with ui.tabs().classes('w-full') as tabs:
                my_corrections_tab = ui.tab(tr('My Corrections'))
                submit_tab = ui.tab(tr('Submit Correction'))
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    review_tab = ui.tab(tr('Review'))
                leaderboard_tab = ui.tab(tr('Leaderboard'))

            with ui.tab_panels(tabs, value=my_corrections_tab).classes('w-full'):
                # My Corrections panel
                with ui.tab_panel(my_corrections_tab):
                    await create_my_corrections_view()

                # Submit Correction panel
                with ui.tab_panel(submit_tab):
                    create_submit_correction_view()

                # Review panel (for reviewers+)
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    with ui.tab_panel(review_tab):
                        await create_review_view()

                # Leaderboard panel
                with ui.tab_panel(leaderboard_tab):
                    await create_leaderboard_view()

        async def create_my_corrections_view():
            """View user's own corrections."""
            # Show loading indicator
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading corrections...')).classes('ml-3')

            result = await api_call("GET", "/corrections/my")

            # Remove loading indicator
            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                # Check if session expired
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                ui.label(f"{tr('Error loading corrections')}: {result['error']}").style('color: var(--danger);')
                return

            corrections = result.get('items', [])

            if not corrections:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('edit_note').classes('text-6xl').style('color: var(--text-tertiary);')
                    ui.label(tr('No corrections yet')).classes('text-xl').style('color: var(--text-secondary);')
                    ui.label(tr('Submit your first correction to help improve transcriptions')).style('color: var(--text-tertiary);')
            else:
                for corr in corrections:
                    with ui.card().classes('w-full p-4 mb-3'):
                        with ui.row().classes('w-full items-start justify-between'):
                            with ui.column().classes('gap-1 flex-1'):
                                status_colors = {
                                    'draft': 'orange',
                                    'pending': 'blue',
                                    'under_review': 'purple',
                                    'approved': 'green',
                                    'rejected': 'red',
                                    'merged': 'teal'
                                }
                                status = corr.get('status', 'draft')
                                ui.badge(status.replace('_', ' ').title()).props(f'color={status_colors.get(status, "grey")}')

                                ui.label(f"Document: {corr.get('document_id', 'Unknown')}").classes('font-medium')

                                if corr.get('original_text'):
                                    with ui.expansion(tr('Original')).classes('w-full'):
                                        ui.label(corr['original_text']).classes('font-mono text-sm whitespace-pre-wrap')

                                if corr.get('corrected_text'):
                                    with ui.expansion(tr('Corrected')).classes('w-full'):
                                        ui.label(corr['corrected_text']).classes('font-mono text-sm whitespace-pre-wrap')

                                if corr.get('notes'):
                                    ui.label(f"{tr('Notes')}: {corr['notes']}").classes('text-sm').style('color: var(--text-secondary);')

                            ui.label(corr.get('created_at', '')[:10]).classes('text-sm').style('color: var(--text-tertiary);')

        def create_submit_correction_view():
            """Form to submit a new correction."""
            with ui.card().classes('w-full p-6'):
                with ui.column().classes('w-full gap-4'):
                    ui.label(tr('Submit New Correction')).classes('text-xl font-bold')

                    doc_id_input = ui.input(tr('Document ID (System ID)')).classes('w-full').props('outlined')
                    page_input = ui.number(tr('Page Number (optional)'), min=1).classes('w-full').props('outlined')
                    line_input = ui.number(tr('Line Number (optional)'), min=1).classes('w-full').props('outlined')

                    original_input = ui.textarea(tr('Original Text')).classes('w-full').props('outlined rows=4')
                    corrected_input = ui.textarea(tr('Corrected Text')).classes('w-full').props('outlined rows=4')
                    notes_input = ui.textarea(tr('Notes (explain your correction)')).classes('w-full').props('outlined rows=2')

                    correction_type = ui.select({
                        'text_correction': tr('Text Correction'),
                        'text_addition': tr('Text Addition'),
                        'text_deletion': tr('Text Deletion'),
                        'metadata': tr('Metadata Correction'),
                        'translation': tr('Translation'),
                        'reading_suggestion': tr('Reading Suggestion'),
                        'paleographic': tr('Paleographic Note'),
                        'uncertain': tr('Uncertain Reading')
                    }, label=tr('Correction Type'), value='text_correction').classes('w-full').props('outlined')

                    async def submit_correction():
                        if not doc_id_input.value or not corrected_input.value:
                            ui.notify(tr('Please fill in Document ID and Corrected Text'), type='warning')
                            return

                        data = {
                            "document_id": doc_id_input.value,
                            "original_text": original_input.value or None,
                            "corrected_text": corrected_input.value,
                            "notes": notes_input.value or None,
                            "correction_type": correction_type.value,
                            "page_number": int(page_input.value) if page_input.value else None,
                            "line_number": int(line_input.value) if line_input.value else None
                        }

                        result = await api_call("POST", "/corrections/", data)

                        if "error" in result:
                            ui.notify(result.get("detail", result["error"]), type='negative')
                        else:
                            ui.notify(tr('Correction submitted successfully'), type='positive')
                            # Clear form
                            doc_id_input.value = ""
                            page_input.value = None
                            line_input.value = None
                            original_input.value = ""
                            corrected_input.value = ""
                            notes_input.value = ""

                    with ui.row().classes('w-full gap-4'):
                        ui.button(tr('Submit'), on_click=submit_correction).props('color=primary')

        async def create_review_view():
            """View for reviewers to review pending corrections."""
            # Show loading indicator
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading pending corrections...')).classes('ml-3')

            result = await api_call("GET", "/corrections/pending")

            # Remove loading indicator
            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                # Check if session expired
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                ui.label(f"{tr('Error loading pending corrections')}: {result['error']}").style('color: var(--danger);')
                return

            pending = result.get('items', [])

            if not pending:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('check_circle').classes('text-6xl').style('color: var(--success);')
                    ui.label(tr('No pending corrections')).classes('text-xl').style('color: var(--text-secondary);')
            else:
                ui.label(f"{len(pending)} {tr('corrections pending review')}").classes('text-lg font-medium mb-4')

                for corr in pending:
                    with ui.card().classes('w-full p-4 mb-4'):
                        with ui.column().classes('w-full gap-3'):
                            with ui.row().classes('w-full items-center justify-between'):
                                ui.label(f"Document: {corr.get('document_id', 'Unknown')}").classes('font-bold')
                                ui.label(f"by {corr.get('author', {}).get('username', 'Unknown')}").style('color: var(--text-secondary);')

                            with ui.row().classes('w-full gap-4'):
                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Original')).classes('font-medium text-sm')
                                    ui.label(corr.get('original_text', '-')).classes('font-mono text-sm p-2 rounded').style('background: var(--surface-secondary);')

                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Corrected')).classes('font-medium text-sm')
                                    ui.label(corr.get('corrected_text', '-')).classes('font-mono text-sm p-2 rounded').style('background: var(--surface-secondary);')

                            if corr.get('notes'):
                                ui.label(f"{tr('Notes')}: {corr['notes']}").style('color: var(--text-secondary);')

                            review_notes = ui.input(tr('Review notes')).classes('w-full').props('outlined dense')

                            async def approve(c=corr, notes=review_notes):
                                result = await api_call("POST", f"/corrections/{c['id']}/review", {
                                    "action": "approve",
                                    "notes": notes.value or None
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    ui.notify(tr('Correction approved'), type='positive')
                                    await refresh_page()

                            async def reject(c=corr, notes=review_notes):
                                result = await api_call("POST", f"/corrections/{c['id']}/review", {
                                    "action": "reject",
                                    "notes": notes.value or None
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    ui.notify(tr('Correction rejected'), type='info')
                                    await refresh_page()

                            with ui.row().classes('gap-2'):
                                ui.button(tr('Approve'), on_click=approve).props('color=positive')
                                ui.button(tr('Reject'), on_click=reject).props('color=negative flat')

        async def create_leaderboard_view():
            """Show top contributors."""
            # Show loading indicator
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading leaderboard...')).classes('ml-3')

            result = await api_call("GET", "/users/leaderboard", {"limit": 20})

            # Remove loading indicator
            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                # Check if session expired
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                ui.label(f"{tr('Error loading leaderboard')}: {result['error']}").style('color: var(--danger);')
                return

            users = result if isinstance(result, list) else result.get('items', [])

            with ui.column().classes('w-full'):
                ui.label(tr('Top Contributors')).classes('text-xl font-bold mb-4')

                if not users:
                    ui.label(tr('No contributors yet')).style('color: var(--text-secondary);')
                else:
                    for i, user in enumerate(users, 1):
                        with ui.card().classes('w-full p-3 mb-2'):
                            with ui.row().classes('w-full items-center justify-between'):
                                with ui.row().classes('items-center gap-3'):
                                    # Medal for top 3
                                    if i == 1:
                                        ui.icon('emoji_events').style('color: gold;')
                                    elif i == 2:
                                        ui.icon('emoji_events').style('color: silver;')
                                    elif i == 3:
                                        ui.icon('emoji_events').style('color: #cd7f32;')
                                    else:
                                        ui.label(f"#{i}").classes('w-6 text-center')

                                    ui.label(user.get('full_name') or user.get('username', 'Unknown')).classes('font-medium')

                                with ui.row().classes('items-center gap-4'):
                                    ui.label(f"{user.get('corrections_count', 0)} {tr('corrections')}").style('color: var(--text-secondary);')
                                    ui.badge(f"{user.get('reputation_score', 0)} pts").props('color=primary')

        # Initial render
        await refresh_page()
