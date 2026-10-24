# -*- coding: utf-8 -*-
"""Ask a signed-in person, once, how their name should appear on their contributions.

Since migrations/profiles_public_aliases.sql (2026-10-24) every account has a
public name: the username or full name the person gave, or else an alias such
as "Curious Scribe". ``profiles.public_name_confirmed`` is false for an account
whose name the site chose; while it is false and the full name is empty, the
header opens a dialog that either saves the name the person types (prefilled
with the name their Google sign-in carries) or keeps the alias. Either answer
sets the flag, so the question is asked once per account, on any device.

Nothing happens before the migration has run: a profile without the column is
never asked, since its answer could not be stored.
"""

import datetime
import logging
from typing import Dict, Optional

from nicegui import background_tasks, run, ui

from web.auth_state import GlobalAuthState
from web.safe_storage import safe_user_get, safe_user_set
from web.supabase_client import get_profile, get_user_client, update_profile
from web.translations import tr

logger = logging.getLogger(__name__)

CONFIRMED_KEY = 'public_name_confirmed'
MAX_LENGTH = 200  # profiles.full_name, as handle_new_user cuts it

# Browser storage. The account whose save failed here: not asked again on
# every page of this browser. And the day a stale cached profile was last
# re-read to learn the flag (sessions that began before the migration).
_SKIP_KEY = 'public_name_skip'
_CHECKED_KEY = 'public_name_checked'


def clean_public_name(value) -> Optional[str]:
    """The name as it will be stored, or None if it cannot be one.

    Whitespace collapses; an address is refused (profiles is public, and the
    2026-10-21 hardening took every address out of it).
    """
    text = ' '.join(str(value or '').split())
    if not text or '@' in text:
        return None
    return text[:MAX_LENGTH]


def suggested_public_name(user: Optional[Dict]) -> str:
    """The name the person's own sign-in carries (Google's), to prefill -- never published unasked."""
    meta = (user or {}).get('user_metadata') or {}
    for key in ('full_name', 'name'):
        name = clean_public_name(meta.get(key))
        if name:
            return name
    return ''


def needs_public_name(profile: Optional[Dict]) -> bool:
    """True when the site chose this account's public name and the person has not answered yet."""
    profile = profile or {}
    return profile.get(CONFIRMED_KEY) is False and not (profile.get('full_name') or '').strip()


def maybe_ask_public_name(name_button=None) -> None:
    """Called while the header is built. ``name_button`` shows the name in the header."""
    user = GlobalAuthState.get_user() or {}
    user_id = user.get('id')
    if not user_id or safe_user_get(_SKIP_KEY) == user_id:
        return
    profile = GlobalAuthState.get_profile() or {}
    if CONFIRMED_KEY not in profile:
        _refresh_cached_profile(user_id)
        return
    if needs_public_name(profile):
        show_public_name_dialog(user, profile, name_button)


def _refresh_cached_profile(user_id: str) -> None:
    """Re-read a profile cached before the column existed, at most once a day.

    The dialog then opens on the next page. A background task keeps the
    request's context variables, so the cache write reaches this visitor's
    storage; it creates no UI.
    """
    stamp = f'{user_id}:{datetime.date.today().isoformat()}'
    if safe_user_get(_CHECKED_KEY) == stamp:
        return
    safe_user_set(_CHECKED_KEY, stamp)

    async def _refresh():
        try:
            fresh = await run.io_bound(get_profile, user_id)
            if fresh and fresh.get('id') == user_id and GlobalAuthState.get_user_id() == user_id:
                GlobalAuthState.update_profile_cache(fresh)
        except Exception:
            logger.warning("public name: profile re-read failed", exc_info=True)

    background_tasks.create(_refresh(), name='public-name-profile-refresh')


def show_public_name_dialog(user: Dict, profile: Dict, name_button=None):
    """The one-time question. Persistent: it closes only through one of its two answers."""
    user_id = user.get('id')
    current = profile.get('username') or ''

    with ui.dialog().props('persistent') as dialog, \
            ui.card().classes('p-6 gap-3').style('max-width: 32rem;'):
        ui.label(tr('How should your name appear?')).classes('text-lg font-bold')
        ui.label(tr('Your corrections, comments and joins are credited to this name. '
                    'Until you choose one, they appear under the name the site gave you:')
                 ).classes('text-sm').style('color: var(--text-secondary);')
        ui.label(current).classes('font-medium')
        name_input = ui.input(label=tr('Public name'), value=suggested_public_name(user)) \
            .props('outlined').classes('w-full')
        error_label = ui.label('').classes('text-sm text-red-500')
        error_label.set_visibility(False)
        ui.label(tr('You can change it later in My Profile.')).classes('text-xs') \
            .style('color: var(--text-muted);')

        async def answer(full_name: Optional[str]):
            # The browser's storage is shared by its tabs: another tab may have
            # signed in as someone else since this dialog opened. Then its
            # client is theirs and its cache theirs; write nothing.
            if GlobalAuthState.get_user_id() != user_id:
                dialog.close()
                return
            data = {CONFIRMED_KEY: True}
            if full_name:
                data['full_name'] = full_name
            # The authenticated client is built here, on the loop: in the
            # worker it would silently be anonymous and update nothing.
            client = get_user_client()
            for button in buttons:
                button.disable()
            result = await run.io_bound(update_profile, user_id, data, client=client)
            if result is None:
                for button in buttons:
                    button.enable()
                return  # cancelled or app stopping
            dialog.close()
            if GlobalAuthState.get_user_id() != user_id:
                return  # the account changed during the save (as above)
            if 'error' in result:
                logger.warning("public name not saved for this account")
                safe_user_set(_SKIP_KEY, user_id)
                ui.notify(tr('Your name could not be saved. You can set it in My Profile.'),
                          type='negative')
                return
            saved = result.get('profile') or {**profile, **data}
            GlobalAuthState.update_profile_cache(saved)
            if full_name:
                if name_button is not None and not name_button.is_deleted:
                    name_button.text = full_name
                ui.notify(tr('Name saved'), type='positive')

        async def save():
            name = clean_public_name(name_input.value)
            if name is None:
                error_label.text = (tr('A public name cannot contain @.') if '@' in (name_input.value or '')
                                    else tr('Please enter a name, or keep the one you have.'))
                error_label.set_visibility(True)
                return
            await answer(name)

        async def keep():
            await answer(None)

        with ui.row().classes('w-full justify-end gap-2 mt-2'):
            buttons = [
                ui.button(tr('Keep this name'), on_click=keep).props('flat'),
                ui.button(tr('Save'), on_click=save).props('color=primary'),
            ]

    dialog.open()
    return dialog
