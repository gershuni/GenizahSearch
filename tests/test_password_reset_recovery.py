# -*- coding: utf-8 -*-
"""Tests for the password-reset-link-dead-end fix (debug session
.planning/debug/resolved/password-reset-link-dead-end.md, 2026-07-28).

Root cause recap: `reset_password_for_email` never registers a PKCE
code_challenge, so Supabase always redirects the recovery link to the bare
Site URL with session tokens in the URL FRAGMENT (or an error fragment for
an expired/consumed/invalid token). NiceGUI never sees URL fragments
server-side -- a client-side JS interceptor
(`web.main._RECOVERY_FRAGMENT_INTERCEPT_SCRIPT`) reads + scrubs it, and the
Python-side dispatch logic (`_complete_password_recovery_session` /
`_handle_recovery_payload`) completes the recovery session or shows the
expired-link path back to a fresh reset request.

Covers, per the fix checkpoint's minimum test list:
  1. `request_password_reset` -- no `redirect_to` (Group A).
  2. Recovery-fragment detection + forwarding -- happy path (Group B).
  3. The error-fragment / expired-token branch -- REQUIRED case, not an
     edge case (Group C).
  4. The safe_storage chokepoint invariant still holds for the new code
     (Group D -- exercised via the SAME storage-failure patterns as
     tests/test_auth_callback_resilience.py; the permanent AST guard in
     tests/test_no_raw_storage_access.py additionally scans this file's
     production code automatically).
  5. The new tr() keys exist in both languages (Group E).

All tests are pure-mock -- no network, no NiceGUI render context required
(mirrors tests/test_auth_callback_resilience.py and
tests/test_auth_revocation_and_headers.py).
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, call


# ---------------------------------------------------------------------------
# Group A: request_password_reset -- no redirect_to (web.supabase_client)
# ---------------------------------------------------------------------------

def test_request_password_reset_sends_no_redirect_to(monkeypatch):
    """The web 'Forgot password?' path must send NO redirect_to -- an
    unlisted redirect_to target is silently downgraded to Site URL by
    Supabase (confirmed via the 2026-07-28 dashboard read-back), and the
    fix's entire design depends on both the desktop- and web-originated
    recovery emails landing on the SAME homepage handler.
    """
    import web.supabase_client as mod

    mock_client = MagicMock()
    mock_client.auth.reset_password_for_email = MagicMock(return_value=None)

    def fake_create_client(url, key, options=None):
        return mock_client

    monkeypatch.setattr(mod, 'create_client', fake_create_client)

    result = mod.request_password_reset('user@example.com')

    assert result == {'success': True}, result
    assert mock_client.auth.reset_password_for_email.call_args == call('user@example.com'), (
        f"Expected reset_password_for_email('user@example.com') with NO "
        f"second (options/redirect_to) argument; got "
        f"{mock_client.auth.reset_password_for_email.call_args}"
    )


def test_request_password_reset_returns_error_on_exception(monkeypatch):
    """Failure path surfaces an 'error' key, never raises."""
    import web.supabase_client as mod

    mock_client = MagicMock()
    mock_client.auth.reset_password_for_email = MagicMock(side_effect=Exception('boom'))

    monkeypatch.setattr(mod, 'create_client', lambda url, key, options=None: mock_client)

    result = mod.request_password_reset('user@example.com')
    assert 'error' in result, result
    assert 'boom' in result['error']


# ---------------------------------------------------------------------------
# Group B: recovery-fragment detection + forwarding (happy path)
# ---------------------------------------------------------------------------

def test_complete_password_recovery_session_happy_path(monkeypatch):
    """Valid recovery tokens -> session established + all keys persisted."""
    storage = {}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)

    user = {'id': 'u1', 'email': 'a@b.c'}
    profile = {'username': 'u1'}
    session = {'access_token': 'recovered-at', 'refresh_token': 'recovered-rt'}

    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'success': True, 'user': user, 'session': session},
    )
    monkeypatch.setattr('web.supabase_client.get_profile', lambda uid: profile)

    from web.main import _complete_password_recovery_session

    result = asyncio.run(_complete_password_recovery_session('raw-at', 'raw-rt'))

    assert result == {'success': True, 'user': user, 'profile': profile}, result
    assert storage.get('auth_user') == user
    assert storage.get('auth_profile') == profile
    assert storage.get('auth_session') == {
        'access_token': 'recovered-at',
        'refresh_token': 'recovered-rt',
    }
    posthog_mock.assert_any_call('password_recovery_started', {})


def test_handle_recovery_payload_recovery_kind_opens_set_password_dialog(monkeypatch):
    """kind == 'recovery' + successful session establishment -> the
    set-new-password dialog opens, NOT the error dialog."""
    async def fake_complete(access_token, refresh_token):
        assert access_token == 'tok-123'
        assert refresh_token == 'ref-456'
        return {'success': True, 'user': {'id': 'u1'}, 'profile': None}

    monkeypatch.setattr('web.main._complete_password_recovery_session', fake_complete)

    opened = {'set_password': 0, 'error': 0}
    from web.main import _handle_recovery_payload

    asyncio.run(_handle_recovery_payload(
        {'kind': 'recovery', 'access_token': 'tok-123', 'refresh_token': 'ref-456'},
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))

    assert opened == {'set_password': 1, 'error': 0}, opened


def test_handle_recovery_payload_ignores_ordinary_homepage_load():
    """No recovery-shaped fragment (payload is None) -> neither dialog opens."""
    from web.main import _handle_recovery_payload

    opened = {'set_password': 0, 'error': 0}
    asyncio.run(_handle_recovery_payload(
        None,
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))
    assert opened == {'set_password': 0, 'error': 0}, opened


def test_handle_recovery_payload_recovery_kind_missing_token_is_noop():
    """A malformed/tampered payload claiming kind='recovery' but missing
    access_token must not crash and must not open either dialog."""
    from web.main import _handle_recovery_payload

    opened = {'set_password': 0, 'error': 0}
    asyncio.run(_handle_recovery_payload(
        {'kind': 'recovery'},
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))
    assert opened == {'set_password': 0, 'error': 0}, opened


# ---------------------------------------------------------------------------
# Group C: the error-fragment / expired-token branch (REQUIRED case)
# ---------------------------------------------------------------------------

def test_handle_recovery_payload_error_kind_opens_error_dialog_and_emits_posthog(monkeypatch):
    """kind == 'error' (Supabase's #error=access_denied&error_code=otp_expired
    fragment) -> the expired-link dialog opens, the set-password dialog does
    NOT, and a password_recovery_failed PostHog event is emitted with the
    error_code -- this is the required, not-an-edge-case path for burned /
    single-use recovery links."""
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)

    opened = {'set_password': 0, 'error': 0}
    from web.main import _handle_recovery_payload

    asyncio.run(_handle_recovery_payload(
        {'kind': 'error', 'error_code': 'otp_expired'},
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))

    assert opened == {'set_password': 0, 'error': 1}, opened
    posthog_mock.assert_any_call('password_recovery_failed', {'error_code': 'otp_expired'})


def test_complete_password_recovery_session_expired_token_surfaces_error(monkeypatch):
    """set_session_from_url failing (e.g. an already-consumed/expired token
    that somehow reached this far) returns a session_exchange_failed error
    without raising, and does NOT touch storage."""
    storage = {}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'error': 'Session error: invalid or expired token'},
    )

    from web.main import _complete_password_recovery_session

    result = asyncio.run(_complete_password_recovery_session('stale-at', 'stale-rt'))

    assert result == {'error': 'session_exchange_failed'}, result
    assert storage == {}, f"storage must be untouched on exchange failure: {storage}"
    posthog_mock.assert_any_call('password_recovery_failed', {'error_code': 'session_exchange_failed'})


def test_complete_password_recovery_session_missing_session_fails_closed(monkeypatch):
    """set_session_from_url returning a truthy user but a falsy/None
    session (a malformed Supabase response) must fail closed -- return an
    error and NEVER write auth_session -- rather than proceeding to
    GlobalAuthState.set_auth()/success:True with no session ever
    persisted. (Specialist-review MUST-FIX, 2026-07-28: without this, the
    set-password dialog opens believing login succeeded, and
    change_password() then fails with 'Not logged in' -- the same class
    of dead-end bug this whole debug session exists to close.)"""
    storage = {}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)

    user = {'id': 'u1', 'email': 'a@b.c'}
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'success': True, 'user': user, 'session': None},
    )

    from web.main import _complete_password_recovery_session

    result = asyncio.run(_complete_password_recovery_session('at', 'rt'))

    assert result == {'error': 'no_session_returned'}, result
    assert storage == {}, f"storage must be untouched when session is falsy: {storage}"
    posthog_mock.assert_any_call('password_recovery_failed', {'error_code': 'no_session_returned'})


def test_handle_recovery_payload_missing_session_routes_to_error_dialog(monkeypatch):
    """End-to-end: a falsy session from set_session_from_url must route
    _handle_recovery_payload to the SAME expired-link error dialog (not a
    silent success) -- confirms _handle_recovery_payload's generic
    `'error' in result` handling already covers this new error code with
    no additional dispatch wiring needed."""
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={})),
    )
    monkeypatch.setattr('web.main.posthog_capture', MagicMock())
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'success': True, 'user': {'id': 'u1'}, 'session': None},
    )

    opened = {'set_password': 0, 'error': 0}
    from web.main import _handle_recovery_payload

    asyncio.run(_handle_recovery_payload(
        {'kind': 'recovery', 'access_token': 'tok', 'refresh_token': 'ref'},
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))

    assert opened == {'set_password': 0, 'error': 1}, opened


def test_handle_recovery_payload_recovery_kind_session_failure_opens_error_dialog(monkeypatch):
    """kind == 'recovery' but _complete_password_recovery_session fails ->
    falls through to the SAME expired-link dialog (not a dead end)."""
    async def fake_complete(access_token, refresh_token):
        return {'error': 'session_exchange_failed'}

    monkeypatch.setattr('web.main._complete_password_recovery_session', fake_complete)

    opened = {'set_password': 0, 'error': 0}
    from web.main import _handle_recovery_payload

    asyncio.run(_handle_recovery_payload(
        {'kind': 'recovery', 'access_token': 'tok', 'refresh_token': 'ref'},
        lambda: opened.__setitem__('set_password', opened['set_password'] + 1),
        lambda: opened.__setitem__('error', opened['error'] + 1),
    ))

    assert opened == {'set_password': 0, 'error': 1}, opened


def test_recovery_fragment_intercept_script_handles_both_fragment_shapes():
    """Structural regression guard on the embedded JS interceptor (cannot be
    executed directly under pytest -- no JS engine in this suite). Pins the
    critical literal behaviors: recovery-token detection, error detection,
    the synchronous history.replaceState scrub (must run BEFORE
    ANALYTICS_SCRIPT/POSTHOG_SCRIPT are added later in the same <head>),
    and that tokens are stashed in a transient window global, never left
    in the URL.
    """
    from web.main import _RECOVERY_FRAGMENT_INTERCEPT_SCRIPT, _RECOVERY_PAYLOAD_READ_JS

    script = _RECOVERY_FRAGMENT_INTERCEPT_SCRIPT
    assert "type') === 'recovery'" in script
    assert "params.get('error')" in script
    assert 'history.replaceState' in script
    assert '__genizahRecovery' in script
    assert 'error_code' in script
    # The read-and-clear helper must delete the stash after reading (one-shot).
    assert '__genizahRecovery' in _RECOVERY_PAYLOAD_READ_JS
    assert 'delete window.__genizahRecovery' in _RECOVERY_PAYLOAD_READ_JS


def test_recovery_script_is_first_head_html_call_on_dashboard_page():
    """The interceptor MUST be the first ui.add_head_html(...) call inside
    dashboard_page() -- ordering is what guarantees the scrub runs before
    ANALYTICS_SCRIPT / POSTHOG_SCRIPT can observe the fragment. A pure
    source-order regression guard (cheaper and more robust than parsing
    the AST call order for this one invariant).
    """
    import inspect
    import web.main as mod

    source = inspect.getsource(mod.dashboard_page)
    first_call_start = source.index('ui.add_head_html(')
    first_call_snippet = source[first_call_start:first_call_start + 80]
    assert '_RECOVERY_FRAGMENT_INTERCEPT_SCRIPT' in first_call_snippet, (
        "The recovery interceptor must be the FIRST ui.add_head_html(...) "
        f"call in dashboard_page(); first call was: {first_call_snippet!r}"
    )
    # And the actual CALL SITE must precede the ANALYTICS_SCRIPT / POSTHOG_SCRIPT
    # call sites (not merely be mentioned somewhere, e.g. in a comment).
    assert first_call_start < source.index('ui.add_head_html(ANALYTICS_SCRIPT)')
    assert first_call_start < source.index('ui.add_head_html(POSTHOG_SCRIPT)')


def test_recovery_dialogs_are_built_lazily_not_on_every_homepage_load():
    """AST guard: every recovery dialog must be constructed INSIDE the nested
    `_build_dialogs()` closure, never in `_render_password_recovery_handler`'s
    own body.

    `/` is the highest-traffic route and is hammered by crawlers, while a
    recovery fragment is present on a vanishingly small fraction of loads.
    Building three dialogs eagerly on every render was measurable waste on a
    site with a documented history of bot-driven RSS pressure on `/` (the
    2026-07-08 allocator-ratchet remediation). The laziness is load-bearing,
    so pin it structurally -- a future refactor that hoists a `ui.dialog()`
    back out of the closure silently restores the per-visit cost, and no
    behavioural test would catch it.
    """
    import ast
    import inspect
    import textwrap
    import web.main as mod

    src = textwrap.dedent(inspect.getsource(mod._render_password_recovery_handler))
    handler = ast.parse(src).body[0]

    build = next(
        (n for n in ast.walk(handler)
         if isinstance(n, ast.FunctionDef) and n.name == '_build_dialogs'),
        None,
    )
    assert build is not None, (
        "_render_password_recovery_handler must keep a nested _build_dialogs() "
        "closure -- that closure IS the lazy-build mechanism."
    )
    build_lines = set(range(build.lineno, (build.end_lineno or build.lineno) + 1))

    def _calls_named(*names):
        """Every Call node in the handler whose callee ends in one of `names`
        (matches both `ui.dialog()` attribute calls and bare `foo()` calls)."""
        for node in ast.walk(handler):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            attr = getattr(func, 'attr', None) or getattr(func, 'id', None)
            if attr in names:
                yield node

    dialog_calls = list(_calls_named('dialog', 'create_forgot_password_dialog'))
    assert dialog_calls, (
        "expected ui.dialog() / create_forgot_password_dialog() calls in "
        "_render_password_recovery_handler -- test is stale if these moved"
    )
    for call in dialog_calls:
        assert call.lineno in build_lines, (
            f"dialog construction at relative line {call.lineno} is OUTSIDE "
            "_build_dialogs() -- it would run on EVERY homepage load. Move it "
            "back inside the closure."
        )

    # ...and the single cheap anchor element MUST stay eager (outside the
    # closure), because it supplies the parent slot the deferred build needs.
    anchor_calls = [c for c in _calls_named('element') if c.lineno not in build_lines]
    assert anchor_calls, (
        "the anchor ui.element(...) must be created eagerly, outside "
        "_build_dialogs() -- the deferred build needs it as a parent slot"
    )


# ---------------------------------------------------------------------------
# Group D: safe_storage chokepoint invariant (storage-failure resilience)
# ---------------------------------------------------------------------------

_PRUNE_MSG = (
    'user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be '
    'created before accessing it'
)


def test_complete_password_recovery_session_prune_race_on_session_write(monkeypatch):
    """Storage prune race on the auth_session write -> friendly error,
    no AssertionError propagates (Phase 87/91 safe_storage contract)."""
    storage = MagicMock()
    storage.__setitem__.side_effect = AssertionError(_PRUNE_MSG)
    storage.get.side_effect = AssertionError(_PRUNE_MSG)
    storage.pop.side_effect = AssertionError(_PRUNE_MSG)
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)

    user = {'id': 'u1', 'email': 'a@b.c'}
    session = {'access_token': 'at', 'refresh_token': 'rt'}
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'success': True, 'user': user, 'session': session},
    )

    from web.main import _complete_password_recovery_session

    # Must not raise.
    result = asyncio.run(_complete_password_recovery_session('at', 'rt'))
    assert result == {'error': 'session_storage_unavailable'}, result
    posthog_mock.assert_any_call('password_recovery_failed', {'error_code': 'session_storage_unavailable'})


class _RoutingStorage(dict):
    """Same helper shape as tests/test_auth_callback_resilience.py's
    _RoutingStorage -- routes writes by key, everything else is plain dict
    semantics."""
    def __init__(self, *args, fail_writes_for=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._fail_writes_for = set(fail_writes_for or ())

    def __setitem__(self, key, value):
        if key in self._fail_writes_for:
            raise AssertionError(_PRUNE_MSG)
        super().__setitem__(key, value)


def test_complete_password_recovery_session_defensive_rollback_on_set_auth_failure(monkeypatch):
    """auth_session write succeeds, but the auth_user write (inside
    GlobalAuthState.set_auth) fails -> DEFENSIVE 3-key cleanup, matching
    _oauth_complete_login's T-E pattern exactly."""
    storage = _RoutingStorage(
        {
            'auth_user': {'id': 'old_u', 'email': 'old@b.c'},
            'auth_profile': {'role': 'admin'},
        },
        fail_writes_for={'auth_user'},
    )
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)

    user = {'id': 'u1', 'email': 'a@b.c'}
    session = {'access_token': 'at', 'refresh_token': 'rt'}
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'success': True, 'user': user, 'session': session},
    )
    monkeypatch.setattr('web.supabase_client.get_profile', lambda uid: {'username': 'new'})

    from web.main import _complete_password_recovery_session

    result = asyncio.run(_complete_password_recovery_session('at', 'rt'))

    assert result == {'error': 'auth_state_storage_unavailable'}, result
    assert 'auth_session' not in storage, f"auth_session NOT cleaned: {dict(storage)}"
    assert 'auth_user' not in storage, f"stale auth_user NOT cleaned: {dict(storage)}"
    assert 'auth_profile' not in storage, f"stale auth_profile NOT cleaned: {dict(storage)}"
    posthog_mock.assert_any_call('password_recovery_failed', {'error_code': 'auth_state_storage_unavailable'})


# ---------------------------------------------------------------------------
# Group E: new tr() keys exist in both languages
# ---------------------------------------------------------------------------

_NEW_KEYS = [
    'Set a new password',
    'Set Password',
    'This password reset link has expired or was already used.',
    'Send reset link',
    'Please enter your email address',
    'If an account exists for that email, a password reset link has been sent.',
]

# Pre-existing keys the new dialogs reuse -- pinning that they still exist
# so a future unrelated edit can't silently break this feature's bilingual
# coverage.
_REUSED_KEYS = [
    'Forgot password?',
    'Forgot Password',
    'Enter your email address:',
    'Email',
    'Cancel',
    'New Password',
    'Confirm New Password',
    'Passwords do not match',
    'Password must be at least 8 characters',
    'Password changed successfully',
    'Please enter new password',
    'Failed to change password',
]


def test_new_recovery_translation_keys_present_and_translated():
    from genizah_translations import TRANSLATIONS

    for key in _NEW_KEYS:
        assert key in TRANSLATIONS, f"Missing Hebrew translation for new key: {key!r}"
        value = TRANSLATIONS[key]
        assert isinstance(value, str) and value.strip(), (
            f"Hebrew translation for {key!r} is empty: {value!r}"
        )
        assert value != key, f"Hebrew translation for {key!r} is identical to the English key (untranslated)"


def test_reused_translation_keys_still_present():
    from genizah_translations import TRANSLATIONS

    missing = [k for k in _REUSED_KEYS if k not in TRANSLATIONS]
    assert not missing, f"Keys the recovery feature reuses are missing from TRANSLATIONS: {missing}"


def test_tr_returns_english_key_verbatim_in_english_mode():
    """tr() must return the English source text unchanged when the UI
    language is English -- new keys must not accidentally only work in
    Hebrew."""
    from web.translations import tr, set_language, get_language

    original_lang = get_language()
    try:
        set_language('en')
        for key in _NEW_KEYS:
            assert tr(key) == key, f"tr({key!r}) changed under English mode: {tr(key)!r}"
    finally:
        set_language(original_lang)


def test_tr_returns_hebrew_translation_in_hebrew_mode():
    from web.translations import tr, set_language, get_language

    original_lang = get_language()
    try:
        set_language('he')
        for key in _NEW_KEYS:
            translated = tr(key)
            assert translated != key, f"tr({key!r}) did not translate under Hebrew mode"
    finally:
        set_language(original_lang)


# ---------------------------------------------------------------------------
# Group F: Codex code review 2026-07-28 regressions
#
# These cover the three defects an external adversarial review found that the
# Group A-E tests structurally COULD NOT catch, because those exercise
# `_complete_password_recovery_session` / `_handle_recovery_payload` as pure
# helpers and never cross the event-handler <-> storage boundary.
# ---------------------------------------------------------------------------

class _OkResp:
    status_code = 200

    @staticmethod
    def json():
        return {'id': 'u1'}


def test_change_password_uses_explicit_token_without_touching_storage(monkeypatch):
    """BLOCKER-1 regression. `change_password` MUST accept the caller's token
    and, when given one, never read app.storage.user.

    Why this was a blocker: the recovery dialog calls change_password via
    `run.io_bound`, i.e. `loop.run_in_executor`, which does NOT propagate
    contextvars. `nicegui.storage.Storage.user` reads `request_contextvar`
    and raises 'can only be used within a UI context' when unset;
    `safe_user_get` then DELIBERATELY degrades that to {} (its own docstring
    names run.io_bound explicitly). So the pre-fix code read no token in the
    worker and returned {'error': 'Not logged in'} on EVERY attempt -- the
    user could never change their password, and each retry burned another
    single-use recovery link.

    Here the storage read is made to raise exactly as it does off-context, so
    any residual dependence on it fails the test.
    """
    import httpx
    import web.supabase_client as mod

    def _explode(*a, **kw):
        raise RuntimeError('app.storage.user can only be used within a UI context')

    monkeypatch.setattr('web.safe_storage.safe_user_get', _explode)

    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured['headers'] = headers
        captured['json'] = json
        return _OkResp()

    monkeypatch.setattr(httpx, 'put', fake_put)

    result = mod.change_password('new-secret-pw', access_token='worker-token')

    assert result.get('success') is True, result
    assert captured['headers']['Authorization'] == 'Bearer worker-token'
    assert captured['json'] == {'password': 'new-secret-pw'}


def test_change_password_still_falls_back_to_storage_for_profile_caller(monkeypatch):
    """The pre-existing synchronous caller (web/pages/profile.py) calls
    change_password() with NO token, on the event-loop thread where the UI
    context exists. That path must keep working unchanged."""
    import httpx
    import web.supabase_client as mod

    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(
            user={'auth_session': {'access_token': 'ui-token'}},
        )),
    )

    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured['headers'] = headers
        return _OkResp()

    monkeypatch.setattr(httpx, 'put', fake_put)

    result = mod.change_password('another-pw')

    assert result.get('success') is True, result
    assert captured['headers']['Authorization'] == 'Bearer ui-token'


def test_recovery_handler_passes_token_into_io_bound_call():
    """Source guard for BLOCKER-1: the recovery handler must hand an explicit
    token to change_password. A refactor that drops the second argument
    silently reintroduces the always-'Not logged in' failure, and no unit test
    on the helper alone would notice."""
    import inspect
    import web.main as mod

    src = inspect.getsource(mod._render_password_recovery_handler)
    assert 'run.io_bound(' in src
    idx = src.index('supabase_change_password,')
    call_window = src[idx:idx + 220]
    assert 'access_token' in call_window, (
        "change_password must be called with an explicit access_token read on "
        f"the event-loop thread; call site was: {call_window!r}"
    )
    # And that token must come through the safe_storage chokepoint, not raw.
    assert "safe_user_get('auth_session')" in src


def test_recovery_session_sets_pending_flag_for_resume(monkeypatch):
    """HIGH-2 regression: once the recovery session is established, a durable
    non-secret flag must exist so a reload / websocket drop / run_javascript
    timeout can re-offer the dialog. By that point the browser-side payload
    has already been read-and-deleted and the fragment scrubbed, and the email
    link is single-use -- without the flag the user is back to the silent dead
    end this whole fix removes."""
    storage = {}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    monkeypatch.setattr('web.main.posthog_capture', MagicMock())
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {
            'success': True,
            'user': {'id': 'u1'},
            'session': {'access_token': 'at', 'refresh_token': 'rt'},
        },
    )
    monkeypatch.setattr('web.supabase_client.get_profile', lambda uid: {'username': 'u'})

    from web.main import _complete_password_recovery_session

    asyncio.run(_complete_password_recovery_session('raw-at', 'raw-rt'))

    # Must be a plain boolean -- never a token.
    assert storage.get('password_recovery_pending') is True, (
        "password_recovery_pending must be set on a successful recovery "
        f"session; storage was {storage!r}"
    )


def test_pending_flag_not_set_when_session_exchange_fails(monkeypatch):
    """The resume flag must NOT be set when the recovery session never
    established -- otherwise every later `/` visit would offer a set-password
    dialog that cannot possibly work."""
    storage = {}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    monkeypatch.setattr('web.main.posthog_capture', MagicMock())
    monkeypatch.setattr(
        'web.supabase_client.set_session_from_url',
        lambda at, rt: {'error': 'expired'},
    )

    from web.main import _complete_password_recovery_session

    result = asyncio.run(_complete_password_recovery_session('raw-at', 'raw-rt'))

    assert 'error' in result
    assert 'password_recovery_pending' not in storage, storage


def test_deferred_probe_uses_captured_client_not_ui_context():
    """ROOT-CAUSE regression guard for the 2026-07-28 live failure.

    NiceGUI keys its slot stack PER asyncio task. `asyncio.ensure_future` starts
    a new task whose slot stack is empty, so any `ui.context.*` lookup from
    inside it raises:

        RuntimeError('The current slot cannot be determined because the slot
                      stack for this task is empty.')

    `ui.run_javascript` is `ui.context.client.run_javascript(...)` under the
    hood, so the probe raised this on EVERY homepage load and the original
    `except Exception -> logger.debug` swallowed it. The recovery flow could
    never fire -- identically for a real link and a fake one, which is exactly
    why the symptom was misread as a Supabase wire-format problem.

    The fix is to bind the Client at RENDER time (where the slot stack is
    populated) and use that object in the task. This guard pins it, because the
    failure mode is invisible: it raises inside a background task, produces no
    user-visible error, and no mock-based unit test touches a real slot stack.
    """
    import ast
    import inspect
    import textwrap
    import web.main as mod

    src = textwrap.dedent(inspect.getsource(mod._render_password_recovery_handler))
    handler = ast.parse(src).body[0]

    # The client must be captured in the handler body, outside any nested def.
    nested_spans = set()
    for node in ast.walk(handler):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node is not handler:
            nested_spans.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))

    captures = [
        n for n in ast.walk(handler)
        if isinstance(n, ast.Attribute) and n.attr == 'client'
        and isinstance(n.value, ast.Attribute) and n.value.attr == 'context'
        and n.lineno not in nested_spans
    ]
    assert captures, (
        "_render_password_recovery_handler must capture `ui.context.client` at "
        "RENDER time (outside every nested function) so the deferred task does "
        "not perform a ui.context lookup with an empty slot stack."
    )

    # And nothing inside the deferred task may touch ui.context / ui.run_javascript.
    deferred = next(
        (n for n in ast.walk(handler)
         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
         and n.name == '_deferred_check'),
        None,
    )
    assert deferred is not None, "_deferred_check closure is gone — test is stale"

    for node in ast.walk(deferred):
        if isinstance(node, ast.Attribute) and node.attr == 'context':
            raise AssertionError(
                f"ui.context lookup at relative line {node.lineno} inside "
                "_deferred_check — this raises 'slot stack for this task is "
                "empty' in a background task. Use the captured client instead."
            )
        if isinstance(node, ast.Attribute) and node.attr == 'run_javascript':
            # Must be <captured_client>.run_javascript, never ui.run_javascript.
            base = getattr(node.value, 'id', None)
            assert base != 'ui', (
                f"ui.run_javascript at relative line {node.lineno} inside "
                "_deferred_check resolves via ui.context.client and raises in a "
                "background task. Call it on the captured client instead."
            )


def test_backend_error_text_is_never_rendered_to_the_user():
    """MEDIUM-4 regression: raw Supabase/httpx error strings must not reach a
    UI label. They are English-only (leaking into the Hebrew UI, breaking the
    i18n invariant) and expose backend operational detail -- throttle windows,
    SMTP failures -- including on an ANONYMOUS form."""
    import inspect
    import web.auth_state as auth_mod
    import web.main as main_mod

    forgot_src = inspect.getsource(auth_mod.create_forgot_password_dialog)
    assert "status_label.text = result['error']" not in forgot_src, (
        "raw backend error assigned to a UI label in the forgot-password dialog"
    )
    assert "tr('Failed to send reset email')" in forgot_src

    recovery_src = inspect.getsource(main_mod._render_password_recovery_handler)
    assert "result.get('error') or tr(" not in recovery_src, (
        "raw backend error preferred over the translated message in the "
        "set-password dialog"
    )
    assert "tr('Failed to change password')" in recovery_src
