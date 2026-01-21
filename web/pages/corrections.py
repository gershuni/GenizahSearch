# -*- coding: utf-8 -*-
"""
My Edits & Comments Page - Genizah Search Pro

User corrections and comments system: view, edit, and manage your contributions.
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call, create_login_dialog, do_logout
from web.state import state
from typing import Optional, Dict, Any, List
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
                    # Changed to H2
                    h2(tr('Please login to view your edits'), classes='text-xl', style='color: var(--text-secondary);')

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
                            # Changed to H3
                            h3(user.get('full_name', user.get('username', '')), classes='font-bold')
                            ui.label(f"{tr('Role')}: {user.get('role', 'contributor').title()} | {tr('Reputation')}: {user.get('reputation_score', 0)}").classes('text-sm').style('color: var(--text-secondary);')

                    async def handle_logout():
                        do_logout()
                        ui.notify(tr('Logged out'), type='info')
                        await refresh_page()

                    ui.button(tr('Logout'), on_click=handle_logout).props('flat color=negative')

            # Tabs for different views
            with ui.tabs().classes('w-full') as tabs:
                my_edits_tab = ui.tab(tr('My Edits'))
                my_comments_tab = ui.tab(tr('My Comments'))
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    review_tab = ui.tab(tr('Review'))
                leaderboard_tab = ui.tab(tr('Leaderboard'))

            with ui.tab_panels(tabs, value=my_edits_tab).classes('w-full'):
                # My Edits panel
                with ui.tab_panel(my_edits_tab):
                    await create_my_edits_view()

                # My Comments panel
                with ui.tab_panel(my_comments_tab):
                    await create_my_comments_view()

                # Review panel (for reviewers+)
                if user.get('role') in ('reviewer', 'editor', 'admin'):
                    with ui.tab_panel(review_tab):
                        await create_review_view()

                # Leaderboard panel
                with ui.tab_panel(leaderboard_tab):
                    await create_leaderboard_view()

        async def create_my_edits_view():
            """View user's own corrections/edits."""
            # Show loading indicator
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading...')).classes('ml-3')

            result = await api_call("GET", "/corrections/my")

            # Remove loading indicator
            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                ui.label(f"{tr('Error')}: {result['error']}").style('color: var(--danger);')
                return

            corrections = result.get('items', [])

            if not corrections:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('edit_note').classes('text-6xl').style('color: var(--text-tertiary);')
                    # Changed to H3
                    h3(tr('No edits yet'), classes='text-xl', style='color: var(--text-secondary);')
                    ui.label(tr('Edit transcriptions to help improve the corpus')).style('color: var(--text-tertiary);')
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
                    await create_edit_card(corr, delete_correction)

        async def create_edit_card(corr: dict, delete_callback):
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
                            async def do_vote(vote_val: int, cid=corr_id):
                                result = await api_call("POST", f"/corrections/{cid}/vote", {
                                    "vote_value": vote_val
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    ui.notify(tr('Vote recorded'), type='positive')
                                    await refresh_page()

                            async def upvote(cid=corr_id):
                                await do_vote(1, cid)

                            async def downvote(cid=corr_id):
                                await do_vote(-1, cid)

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
                                async def edit_correction(c=corr):
                                    await open_edit_dialog(c)

                                ui.button(icon='edit', on_click=edit_correction).props('flat round dense').tooltip(tr('Edit'))

                            # Delete button
                            user_role = GlobalAuthState.get_role()
                            can_delete = corr_status == 'draft' or user_role == 'admin'

                            if can_delete:
                                corr_id = corr.get('id')

                                async def confirm_delete(cid=corr_id):
                                    with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                                        # Changed to H3
                                        h3(tr('Delete Correction?'), classes='text-lg font-bold')
                                        ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                        with ui.row().classes('justify-end gap-2 mt-4'):
                                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                            async def do_delete():
                                                confirm_dialog.close()
                                                await delete_callback(cid)
                                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                    confirm_dialog.open()

                                ui.button(icon='delete', on_click=confirm_delete).props('flat round dense color=negative').tooltip(tr('Delete'))

        async def open_edit_dialog(corr: dict):
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

                    async def save_changes():
                        result = await api_call("PUT", f"/corrections/{corr['id']}", {
                            "corrected_text": text_area.value,
                            "notes": notes_area.value or None
                        })
                        if "error" in result:
                            ui.notify(result["error"], type='negative')
                        else:
                            ui.notify(tr('Correction updated'), type='positive')
                            dialog.close()
                            await refresh_page()

                    ui.button(tr('Save'), icon='save', on_click=save_changes).props('color=primary')

            dialog.open()

        async def create_my_comments_view():
            """View user's own comments."""
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading...')).classes('ml-3')

            result = await api_call("GET", "/comments/my")

            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                # Might not have this endpoint yet - show empty state
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('comment').classes('text-6xl').style('color: var(--text-tertiary);')
                    # Changed to H3
                    h3(tr('No comments yet'), classes='text-xl', style='color: var(--text-secondary);')
                    ui.label(tr('Share your insights and questions')).style('color: var(--text-tertiary);')
                return

            comments = result.get('items', []) if isinstance(result, dict) else result

            if not comments:
                with ui.column().classes('w-full items-center py-8'):
                    ui.icon('comment').classes('text-6xl').style('color: var(--text-tertiary);')
                    # Changed to H3
                    h3(tr('No comments yet'), classes='text-xl', style='color: var(--text-secondary);')
                    ui.label(tr('Share your insights and questions')).style('color: var(--text-tertiary);')
            else:
                for comment in comments:
                    await create_comment_card(comment)

        async def create_comment_card(comment: dict):
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
                            async def edit_comment(c=comment):
                                await open_comment_edit_dialog(c)

                            ui.button(icon='edit', on_click=edit_comment).props('flat round dense').tooltip(tr('Edit'))

                            # Delete button
                            comment_id = comment.get('id')

                            async def confirm_delete(cid=comment_id):
                                with ui.dialog() as confirm_dialog, ui.card().classes('p-4'):
                                    # Changed to H3
                                    h3(tr('Delete Comment?'), classes='text-lg font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                        async def do_delete():
                                            result = await api_call("DELETE", f"/comments/{cid}")
                                            confirm_dialog.close()
                                            if "error" in result:
                                                ui.notify(result["error"], type='negative')
                                            else:
                                                ui.notify(tr('Comment deleted'), type='positive')
                                                await refresh_page()
                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=confirm_delete).props('flat round dense color=negative').tooltip(tr('Delete'))

        async def open_comment_edit_dialog(comment: dict):
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

                    async def save_comment():
                        result = await api_call("PUT", f"/comments/{comment['id']}", {
                            "content": text_area.value
                        })
                        if "error" in result:
                            ui.notify(result["error"], type='negative')
                        else:
                            ui.notify(tr('Comment updated'), type='positive')
                            dialog.close()
                            await refresh_page()

                    ui.button(tr('Save'), icon='save', on_click=save_comment).props('color=primary')

            dialog.open()

        async def create_review_view():
            """View for reviewers to review pending corrections."""
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading pending corrections...')).classes('ml-3')

            result = await api_call("GET", "/corrections/pending")

            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
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

                            async def approve(c=corr, notes=review_notes):
                                result = await api_call("POST", f"/corrections/{c['id']}/review", {
                                    "action": "approve",
                                    "review_notes": notes.value or None
                                })
                                if "error" in result:
                                    ui.notify(result.get("detail", result["error"]), type='negative')
                                else:
                                    ui.notify(tr('Correction approved'), type='positive')
                                    await refresh_page()

                            async def reject(c=corr, notes=review_notes):
                                rejection_text = notes.value or tr('Rejected by reviewer')
                                result = await api_call("POST", f"/corrections/{c['id']}/review", {
                                    "action": "reject",
                                    "rejection_reason": rejection_text
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
            with ui.row().classes('w-full items-center justify-center py-8'):
                loading_spinner = ui.spinner(size='lg')
                loading_label = ui.label(tr('Loading leaderboard...')).classes('ml-3')

            result = await api_call("GET", "/users/leaderboard", {"limit": 20})

            loading_spinner.delete()
            loading_label.delete()

            if "error" in result:
                if result.get("expired"):
                    ui.notify(result["error"], type='warning')
                    await refresh_page()
                    return
                ui.label(f"{tr('Error loading leaderboard')}: {result['error']}").style('color: var(--danger);')
                return

            users = result if isinstance(result, list) else result.get('items', [])

            with ui.column().classes('w-full'):
                # Changed to H3
                h3(tr('Top Contributors'), classes='text-xl font-bold mb-4')

                if not users:
                    ui.label(tr('No contributors yet')).style('color: var(--text-secondary);')
                else:
                    for i, user in enumerate(users, 1):
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

                                    ui.label(user.get('full_name') or user.get('username', 'Unknown')).classes('font-medium')

                                with ui.row().classes('items-center gap-4'):
                                    ui.label(f"{user.get('corrections_count', 0)} {tr('corrections')}").style('color: var(--text-secondary);')
                                    ui.badge(f"{user.get('reputation_score', 0)} pts").props('color=primary')

        # Initial render
        await refresh_page()
