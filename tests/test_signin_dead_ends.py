# -*- coding: utf-8 -*-
"""Sign-in dead ends: a failed password sign-in offers the way out.

invalid_credentials -> "Forgot password?" under the error;
email_not_confirmed -> "Resend confirmation email" (60 s per dialog, one
neutral answer whatever Supabase says, `confirmation_resent` without the email).
The resend call is proven against a mocked Supabase client; no network.
"""
from __future__ import annotations

import asyncio
import itertools

import pytest

import web.auth_state as auth_state

_paths = itertools.count()


@pytest.mark.parametrize('code, kind', [
    ('invalid_credentials', 'forgot'),
    ('email_not_confirmed', 'resend'),
    ('over_request_rate_limit', None),
    ('', None),
    (None, None),
])
def test_error_code_to_affordance(code, kind):
    assert auth_state.login_error_affordance(code) == kind


def test_resend_wait_is_sixty_seconds():
    assert auth_state.resend_wait_seconds(None, 100.0) == 0
    assert auth_state.resend_wait_seconds(100.0, 100.0) == 60
    assert auth_state.resend_wait_seconds(100.0, 130.5) == 30
    assert auth_state.resend_wait_seconds(100.0, 160.0) == 0


def test_resend_confirmation_email_calls_supabase_resend(monkeypatch):
    import web.supabase_client as sc
    calls = []

    class _Auth:
        def resend(self, params):
            calls.append(params)

    class _Client:
        auth = _Auth()

    monkeypatch.setattr(sc, 'create_client', lambda *a, **k: _Client())
    assert sc.resend_confirmation_email('a@b.c') == {'success': True}
    assert calls == [{'type': 'signup', 'email': 'a@b.c'}]


def _drive(monkeypatch, login_result, clicks, oauth_error=None, edit_email_to=None):
    """Open the login dialog, press Login with `login_result`, then press the
    buttons named in `clicks`. Returns (buttons visible, notes, resends, events)."""
    from nicegui import core, ui
    from nicegui.client import Client
    from nicegui.testing.general import prepare_simulation

    import web.analytics as analytics
    import web.supabase_client as sc

    async def fake_login(email, password):
        return login_result

    events, resends = [], []
    if oauth_error:
        monkeypatch.setattr(auth_state, 'get_oauth_url', lambda *a, **k: {'error': oauth_error})
    monkeypatch.setattr(auth_state, 'do_login', fake_login)
    monkeypatch.setattr(analytics, 'posthog_capture', lambda name, props=None: events.append((name, props)))
    monkeypatch.setattr(sc, 'resend_confirmation_email',
                        lambda email: resends.append(email) or {'error': 'boom'})
    import web.translations as wt
    monkeypatch.setattr(wt, 'get_language', lambda: 'en')
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    seen = {}

    def press(client, text):
        btn = [e for e in client.elements.values() if isinstance(e, ui.button) and e.text == text]
        assert len(btn) == 1, (text, btn)
        for listener in btn[0]._event_listeners.values():
            if listener.type == 'click':
                listener.handler(None)

    async def main():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_signin_probe_{next(_paths)}')) as client:
            dialog = auth_state.create_login_dialog()
            dialog.open()
        panel = [e for e in client.elements.values() if isinstance(e, ui.tab_panel)][0]
        email, pw = [e for e in panel.descendants() if isinstance(e, ui.input)][:2]
        email.value, pw.value = 'a@b.c', 'pw'
        press(client, 'Login')
        await asyncio.sleep(0.2)
        if edit_email_to:
            email.value = edit_email_to
        for text in clicks:
            press(client, text)
            await asyncio.sleep(0.2)
        seen['visible'] = {e.text for e in client.elements.values()
                           if isinstance(e, ui.button)
                           and e.text in ('Forgot password?', 'Resend confirmation email')
                           and 'hidden' not in e.classes}
        seen['notes'] = [e.text for e in client.elements.values()
                         if isinstance(e, ui.label) and e.text and 'hidden' not in e.classes]

    asyncio.run(main())
    return seen['visible'], seen['notes'], resends, events


def test_invalid_credentials_shows_forgot_password(monkeypatch):
    visible, notes, _, _ = _drive(monkeypatch, {'error': 'Wrong', 'error_code': 'invalid_credentials'}, [])
    assert visible == {'Forgot password?'}
    assert 'Wrong' in notes


def test_not_confirmed_shows_resend_and_sends_once_per_minute(monkeypatch):
    visible, notes, resends, events = _drive(
        monkeypatch, {'error': 'Unconfirmed', 'error_code': 'email_not_confirmed'},
        ['Resend confirmation email', 'Resend confirmation email'])
    assert visible == {'Resend confirmation email'}
    assert resends == ['a@b.c'], 'second click inside 60 s must not send'
    assert 'If that address is registered, a new confirmation email is on its way.' in notes \
        or 'Please wait a minute before sending another.' in notes
    sent = [e for e in events if e[0] == 'confirmation_resent']
    assert len(sent) == 1 and not (sent[0][1] or {}), 'event must carry no email'


def test_other_errors_show_neither(monkeypatch):
    visible, _, _, _ = _drive(monkeypatch, {'error': 'Down', 'error_code': 'unexpected_failure'}, [])
    assert visible == set()


def test_google_login_failure_clears_the_password_affordance(monkeypatch):
    visible, notes, _, _ = _drive(
        monkeypatch, {'error': 'Wrong', 'error_code': 'invalid_credentials'},
        ['Login with Google'], oauth_error='OAuth down')
    assert visible == set(), 'the OAuth error sits beside an irrelevant button'
    assert 'OAuth down' in notes


def test_resend_uses_the_address_that_failed_not_the_edited_field(monkeypatch):
    _, _, resends, _ = _drive(
        monkeypatch, {'error': 'Unconfirmed', 'error_code': 'email_not_confirmed'},
        ['Resend confirmation email'], edit_email_to='someone@else.org')
    assert resends == ['a@b.c']
