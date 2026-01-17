# -*- coding: utf-8 -*-
"""
Corrections Page - Genizah Search Pro

User corrections system: submit, review, and manage transcription corrections.
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call, create_login_dialog, do_logout
from typing import Optional, Dict, Any


async def create_corrections_page():
    """Create the Corrections page."""

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
                if GlobalAuthState.is_logged_in():
                    await create_logged_in_view()
                else:
                    create_login_view()

        def create_login_view():
            """Create login/register view - use global login dialog."""
            with ui.card().classes('w-full p-6'):
                with ui.column().classes('w-full items-center gap-4 py-8'):
                    ui.icon('login').classes('text-6xl').style('color: var(--text-tertiary);')
                    ui.label(tr('Please login to view your corrections')).classes('text-xl').style('color: var(--text-secondary);')

                    login_dialog = create_login_dialog()

                    def open_login():
                        login_dialog.open()

                    ui.button(tr('Login / Register'), icon='login', on_click=open_login).props('color=primary size=lg')

        async def create_logged_in_view():
            """Create view for logged in users."""
            user = GlobalAuthState.get_user()

            # User info bar
            with ui.card().classes('w-full p-4'):
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('account_circle').classes('text-3xl').style('color: var(--primary-600);')
                        with ui.column().classes('gap-0'):
                            ui.label(user.get('full_name', user.get('username', ''))).classes('font-bold')
                            ui.label(f"{tr('Role')}: {user.get('role', 'contributor').title()} | {tr('Reputation')}: {user.get('reputation_score', 0)}").classes('text-sm').style('color: var(--text-secondary);')

                    async def handle_logout():
                        do_logout()
                        ui.notify(tr('Logged out'), type='info')
                        await refresh_page()

                    ui.button(tr('Logout'), on_click=handle_logout).props('flat color=negative')

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
                async def delete_correction(corr_id: int):
                    """Delete a correction after confirmation."""
                    result = await api_call("DELETE", f"/corrections/{corr_id}")
                    if "error" in result:
                        ui.notify(result["error"], type='negative')
                    else:
                        ui.notify(tr('Correction deleted'), type='positive')
                        await refresh_page()

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
                                        ui.label(corr['original_text']).classes('font-mono text-sm whitespace-pre-wrap').style('direction: rtl; text-align: right;')

                                if corr.get('corrected_text'):
                                    with ui.expansion(tr('Corrected')).classes('w-full'):
                                        ui.label(corr['corrected_text']).classes('font-mono text-sm whitespace-pre-wrap').style('direction: rtl; text-align: right;')

                                if corr.get('notes'):
                                    ui.label(f"{tr('Notes')}: {corr['notes']}").classes('text-sm').style('color: var(--text-secondary);')

                            with ui.column().classes('items-end gap-2'):
                                ui.label(corr.get('created_at', '')[:10]).classes('text-sm').style('color: var(--text-tertiary);')

                                # Delete button - always available for drafts, admins can delete any
                                corr_status = corr.get('status', 'draft')
                                user_role = GlobalAuthState.get_role()
                                can_delete = corr_status == 'draft' or user_role == 'admin'

                                if can_delete:
                                    corr_id = corr.get('id')

                                    async def confirm_delete(cid=corr_id):
                                        with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                                            ui.label(tr('Delete Correction?')).classes('text-lg font-bold')
                                            ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                            with ui.row().classes('justify-end gap-2 mt-4'):
                                                ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                                async def do_delete():
                                                    confirm_dialog.close()
                                                    await delete_correction(cid)
                                                ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                        confirm_dialog.open()

                                    ui.button(icon='delete', on_click=confirm_delete).props('flat round dense color=negative').tooltip(tr('Delete'))

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
