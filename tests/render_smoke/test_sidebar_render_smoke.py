# -*- coding: utf-8 -*-
"""Render-smoke: the sidebar, the card of keyboard shortcuts, and the signed-out header
(owner, 2026-11-05, before 9.14.0).

- The sidebar has no section titles and lists the pages in the readers' order
  (PostHog, 30 days to 2026-11-05): the pages people work in, then the pages that
  explain the site, then "Keyboard shortcuts".
- Its rail button collapses it to icons (Quasar's ``mini``) and the choice is
  remembered on the next page.
- "?" (web/static/js/shortcuts.js dispatches ``gs-shortcuts`` on the hidden
  trigger) and the sidebar's "Keyboard shortcuts" open the card.
- Signed out, the header has one "Sign in" button; its dialog has the Register tab.

Harness: the homepage User context of ``test_home_teaser_render_smoke.py``.
"""
from __future__ import annotations

import asyncio
import re
from unittest.mock import patch

from nicegui import events, ui

from tests.render_smoke.test_home_teaser_render_smoke import (
    _all_elements,
    _click_element,
    _descendant_texts,
    _marked_element,
    _run_home_smoke,
)

WORK_ORDER = ['Home', 'Search', 'Browse by Shelfmark', 'Find Parallels', 'Browse by Identification',
              'Joins Lab', 'Community', 'My Lists']
EXPLAIN_ORDER = ['Start Here', 'About the Genizah', 'AI tools', 'Help Center', 'Download App', 'Settings',
                 'Keyboard shortcuts']


def _classes(el) -> list:
    return list(getattr(el, '_classes', []) or [])


def _nav_labels(user) -> list[str]:
    return [el.text for el in _all_elements(user)
            if isinstance(el, ui.label) and 'nav-item-label' in _classes(el)]


def _drawer(user):
    drawers = [el for el in _all_elements(user) if 'gs-main-drawer' in _classes(el)]
    assert len(drawers) == 1, len(drawers)
    return drawers[0]


def _fire(user, element, event: str) -> None:
    camel = re.sub(r'-(\w)', lambda m: m.group(1).upper(), event)
    with user._client:
        for listener in element._event_listeners.values():
            if listener.element_id == element.id and listener.type in (event, camel):
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args=None))


def _cards(user) -> list:
    return [el for el in _all_elements(user) if 'gs-shortcuts-card' in _classes(el)]


def test_the_sidebar_lists_the_pages_in_the_readers_order_without_titles():
    async def driver(user):
        await user.open('/about')
        labels = _nav_labels(user)
        work = [x for x in labels if x in WORK_ORDER]
        explain = [x for x in labels if x in EXPLAIN_ORDER]
        assert work == WORK_ORDER, labels
        assert explain == EXPLAIN_ORDER, labels
        assert labels.index('My Lists') < labels.index('Start Here'), labels
        assert labels[-1] == 'Keyboard shortcuts', labels
        texts = {getattr(el, 'text', None) for el in _all_elements(user)}
        assert not texts & {'NAVIGATION', 'TOOLS'}

    _run_home_smoke(driver, lang='en')



def test_every_row_keeps_its_name_in_the_rail_and_the_shortcuts_button_answers_space():
    """Codex, PR #484: in the icon rail the labels are hidden, so each row carries its own
    aria-label; the "Keyboard shortcuts" row is a button and opens on Space too."""
    async def driver(user):
        await user.open('/about')
        rows = [el for el in _all_elements(user) if 'nav-item' in _classes(el)]
        named = {r.props.get('aria-label') for r in rows}
        for label in WORK_ORDER + EXPLAIN_ORDER:
            assert label in named, label
        shortcuts = _marked_element(user, 'nav-shortcuts')
        assert shortcuts.props.get('role') == 'button'
        types = {l.type for l in shortcuts._event_listeners.values()}
        assert {'click', 'keydown.enter', 'keydown.space.prevent'} <= types, types
        home = [r for r in rows if r.props.get('aria-label') == 'Home'][0]
        assert home.props.get('role') == 'link'
        assert 'keydown.space.prevent' not in {l.type for l in home._event_listeners.values()}
        _fire(user, shortcuts, 'keydown.space.prevent')
        await asyncio.sleep(0.1)
        assert len(_cards(user)) == 1

    _run_home_smoke(driver, lang='en')

def test_the_rail_button_collapses_the_sidebar_and_it_stays_collapsed():
    async def driver(user):
        await user.open('/about')
        assert 'mini' not in _drawer(user).props
        toggle = _marked_element(user, 'nav-rail-toggle')
        assert toggle is not None and toggle.props.get('aria-label') == 'Collapse menu'
        _click_element(user, toggle)
        assert 'mini' in _drawer(user).props
        assert toggle.props.get('aria-label') == 'Expand menu'

        await user.open('/about')  # the next page keeps the visitor's choice
        assert 'mini' in _drawer(user).props
        toggle = _marked_element(user, 'nav-rail-toggle')
        assert toggle.props.get('aria-label') == 'Expand menu'
        _click_element(user, toggle)
        assert 'mini' not in _drawer(user).props

    _run_home_smoke(driver, lang='en')


def test_question_mark_and_the_sidebar_open_the_card_of_shortcuts():
    async def driver(user):
        await user.open('/about')
        assert _cards(user) == []
        triggers = [el for el in _all_elements(user) if 'gs-shortcuts-trigger' in _classes(el)]
        assert len(triggers) == 1
        assert '/static/js/shortcuts.js?v=' in user._client._head_html, 'the key script comes with the trigger'
        _fire(user, triggers[0], 'gs-shortcuts')
        await asyncio.sleep(0.1)
        [card] = _cards(user)
        texts = [getattr(el, 'text', '') for el in _all_elements(user)]
        for wanted in ('Keyboard shortcuts', 'Show this list', 'Jump to the search box', 'Next Page',
                       'Fullscreen Image', 'Run the search'):
            assert wanted in texts, wanted

        await user.open('/about')
        row = _marked_element(user, 'nav-shortcuts')
        assert row is not None
        with patch('nicegui.ui.navigate.to') as nav:
            _click_element(user, row)
        assert not nav.called, 'the shortcuts row opens the card, it is not a page'
        assert len(_cards(user)) == 1

    _run_home_smoke(driver, lang='en')


def test_signed_out_the_header_has_one_sign_in_button():
    async def driver(user):
        await user.open('/about')
        buttons = [el for el in _all_elements(user) if isinstance(el, ui.button)]
        texts = [b.text for b in buttons]
        assert texts.count('Sign in') == 1, texts
        assert 'Login' not in texts and 'Register' not in texts
        [sign_in] = [b for b in buttons if b.text == 'Sign in']
        assert sign_in.props.get('aria-label') == 'Sign in or register'

    _run_home_smoke(driver, lang='en')


def test_signed_in_the_header_shows_the_account_icon_and_my_lists_in_its_menu():
    from web import auth_state

    asked = []

    async def driver(user):
        await user.open('/about')
        buttons = [el for el in _all_elements(user) if isinstance(el, ui.button)]
        [account] = [b for b in buttons if 'auth-account-btn' in _classes(b)]
        assert account.text == '' and account.props.get('icon') == 'account_circle'
        assert 'Reader Name' not in [b.text for b in buttons], 'the name is not on a header button'
        tips = [el.text for el in _all_elements(user) if isinstance(el, ui.tooltip)]
        assert 'Reader Name' in tips
        names = [el for el in _all_elements(user) if 'auth-menu-name' in _classes(el)]
        assert [n.text for n in names] == ['Reader Name']
        items = [t for el in _all_elements(user) if isinstance(el, ui.menu_item)
                 for t in _descendant_texts(el)]
        assert items.index('My Profile') < items.index('My Lists') < items.index('My Corrections'), items
        # The public-name prompt renames what the header shows: the menu line and the tooltip.
        [shown] = asked
        shown.text = 'Chosen Name'
        assert names[0].text == 'Chosen Name'
        assert 'Chosen Name' in [el.text for el in _all_elements(user) if isinstance(el, ui.tooltip)]

    with patch.object(auth_state.GlobalAuthState, 'is_logged_in', return_value=True), \
            patch.object(auth_state.GlobalAuthState, 'get_profile', return_value={}), \
            patch.object(auth_state.GlobalAuthState, 'get_username', return_value='Reader Name'), \
            patch.object(auth_state.GlobalAuthState, 'get_role', return_value='user'), \
            patch.object(auth_state.GlobalAuthState, 'is_admin', return_value=False), \
            patch('web.public_name.maybe_ask_public_name', side_effect=asked.append):
        _run_home_smoke(driver, lang='en')


def test_the_rail_arrows_point_the_same_way_in_hebrew():
    """The sidebar is at the screen's left in both languages, so collapsing points left in
    both; in Hebrew they were reversed (owner's screenshot, 2026-11-05)."""
    async def driver(user):
        await user.open('/about')
        toggle = _marked_element(user, 'nav-rail-toggle')
        assert toggle.props.get('icon') == 'chevron_left'
        _click_element(user, toggle)
        assert toggle.props.get('icon') == 'chevron_right'
        _click_element(user, toggle)
        assert toggle.props.get('icon') == 'chevron_left'

    _run_home_smoke(driver, lang='he')
