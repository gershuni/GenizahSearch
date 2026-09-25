"""Web session hardening: the storage secret comes from the environment, and
session ids are validated before NiceGUI user storage is created.

Two halves:

* ``web/main.py`` reads the storage secret through
  ``web.session_hardening.resolve_storage_secret()`` inside its startup block
  (never at import time -- many test files import ``web.main`` with no secret
  set), and refuses to start without the validating middleware installed.
* ``web.framework_patches._CacheSafeRequestTrackingMiddleware`` replaces any
  session id that is not a canonical lowercase uuid4 string with a fresh one
  before NiceGUI looks up or creates user storage for it. Every other key in
  the session is kept.

The middleware tests compose the middleware classes directly around a raw
ASGI endpoint (as ``tests/test_static_asset_sessions.py`` does), so they do not
depend on whatever NiceGUI middleware an earlier test in the same process
installed. The one test that goes through ``nicegui.storage.set_storage_secret``
clears and restores that process-global state in a fixture. Every malformed-id
case carries an in-test positive control: a canonical id sent with the same
secret must reach storage unchanged, which proves the cookie signature was
accepted (a rejected signature would also yield a fresh id and hide a failure).
"""
from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import sys
from base64 import b64decode, b64encode
from pathlib import Path
from types import SimpleNamespace

import pytest
from itsdangerous import TimestampSigner
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_MAIN = REPO_ROOT / 'web' / 'main.py'

TEST_SECRET = 'session-hardening-test-secret-0123456789'
CANONICAL_ID = 'c9a3e1f2-7b4d-4e8a-9f10-2b3c4d5e6f70'
OTHER_CANONICAL_ID = '0d6f5a4e-3c2b-4a19-8e7d-6c5b4a392817'
UUID4_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}')

MALFORMED_IDS = [
    pytest.param('not-a-uuid', id='plain-text'),
    pytest.param('', id='empty-string'),
    pytest.param(12345, id='integer'),
    pytest.param(None, id='null'),
    pytest.param(['x'], id='list'),
    pytest.param(CANONICAL_ID.upper(), id='uppercase-uuid'),
    pytest.param(CANONICAL_ID.replace('-', ''), id='uuid-without-hyphens'),
    pytest.param('{' + CANONICAL_ID + '}', id='uuid-in-braces'),
]

NON_OBJECT_SESSION_BODIES = [
    pytest.param(['x'], id='json-list'),
    pytest.param('text', id='json-string'),
    pytest.param(7, id='json-number'),
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _signed_cookie(payload, secret: str = TEST_SECRET) -> str:
    """Build a session cookie exactly as Starlette's SessionMiddleware signs it."""
    data = b64encode(json.dumps(payload).encode('utf-8'))
    return TimestampSigner(secret).sign(data).decode('utf-8')


def _decode_set_cookie(response, secret: str = TEST_SECRET) -> dict:
    header = response.headers['set-cookie']
    assert header.startswith('session=')
    value = header.split(';', 1)[0][len('session='):]
    return json.loads(b64decode(TimestampSigner(secret).unsign(value.encode('utf-8'))))


def _endpoint_recording_session(seen: list):
    async def app(scope, receive, send):
        seen.append(dict(scope['session']))
        await PlainTextResponse('page')(scope, receive, send)
    return app


def _direct_chain(inner_app):
    from web.framework_patches import _CacheSafeRequestTrackingMiddleware, _CacheSafeSessionMiddleware
    return _CacheSafeSessionMiddleware(
        _CacheSafeRequestTrackingMiddleware(inner_app),
        secret_key=TEST_SECRET,
    )


@pytest.fixture
def recorded_storage(monkeypatch):
    """Replace NiceGUI's user-storage creation with a recorder (restored by monkeypatch)."""
    from nicegui import core

    recorded: list = []

    async def _recorder(session_id):
        recorded.append(session_id)
        core.app.storage._users[session_id] = {}

    monkeypatch.setattr(core.app.storage, '_users', {})
    monkeypatch.setattr(core.app.storage, '_create_user_storage', _recorder)
    return recorded


def _request(client: TestClient, payload):
    return client.get('/search', headers={'Cookie': f'session={_signed_cookie(payload)}'})


def _assert_positive_control(client: TestClient, recorded: list) -> None:
    """A canonical id with the test secret reaches storage unchanged."""
    response = _request(client, {'id': CANONICAL_ID})
    assert response.status_code == 200
    assert recorded[-1] == CANONICAL_ID, 'positive control: the signed cookie was not accepted'


# ---------------------------------------------------------------------------
# storage secret: web/main.py reads it from the environment at startup
# ---------------------------------------------------------------------------

def _is_startup_block(node: ast.AST) -> bool:
    """``if __name__ in {'__main__', '__mp_main__'}:`` at module level."""
    if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
        return False
    left = node.test.left
    return (
        isinstance(left, ast.Name)
        and left.id == '__name__'
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.In)
    )


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_ui_run(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == 'run'
        and isinstance(func.value, ast.Name)
        and func.value.id == 'ui'
    )


def _storage_secret_problems(source: str) -> list[str]:
    """Return why ``ui.run``'s storage secret is not read from the environment."""
    tree = ast.parse(source)
    problems: list[str] = []
    blocks = [n for n in tree.body if _is_startup_block(n)]
    if len(blocks) != 1:
        return [f'expected one startup block, found {len(blocks)}']
    block = blocks[0]

    runs = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and _is_ui_run(n)]
    if len(runs) != 1:
        return [f'expected one ui.run call, found {len(runs)}']
    run_call = runs[0]
    secret_kw = [kw for kw in run_call.keywords if kw.arg == 'storage_secret']
    if len(secret_kw) != 1:
        return ['ui.run has no storage_secret keyword']
    value = secret_kw[0].value
    if isinstance(value, ast.Constant):
        problems.append('storage_secret is a literal constant')
        return problems
    if not isinstance(value, ast.Name):
        problems.append('storage_secret is neither a name nor a resolver result')
        return problems

    assigned_from_resolver = False
    for node in ast.walk(block):
        if not isinstance(node, ast.Assign):
            continue
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if (
            value.id in targets
            and isinstance(node.value, ast.Call)
            and _call_name(node.value) == 'resolve_storage_secret'
        ):
            assigned_from_resolver = True
    if not assigned_from_resolver:
        problems.append(f'{value.id} is not assigned from resolve_storage_secret() in the startup block')
    return problems


def _startup_call_problems(source: str) -> list[str]:
    """The resolver and the validation check run once each, in the startup block, before ui.run."""
    tree = ast.parse(source)
    blocks = [n for n in tree.body if _is_startup_block(n)]
    if len(blocks) != 1:
        return [f'expected one startup block, found {len(blocks)}']
    block = blocks[0]
    in_block = {id(n) for n in ast.walk(block)}

    runs = [n for n in ast.walk(block) if isinstance(n, ast.Call) and _is_ui_run(n)]
    if len(runs) != 1:
        return [f'expected one ui.run call in the startup block, found {len(runs)}']
    run_pos = (runs[0].lineno, runs[0].col_offset)

    names = ('resolve_storage_secret', 'require_session_id_validation')
    problems: list[str] = []
    # Local names the helpers are imported under (``from ... import x as y``),
    # so a call through an alias is still counted. Importing either helper
    # outside the startup block is itself a problem: it is the first step of
    # running it at import time.
    local_names = {name: {name} for name in names}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not (node.module or '').endswith('session_hardening'):
            continue
        for alias in node.names:
            if alias.name not in local_names:
                continue
            local_names[alias.name].add(alias.asname or alias.name)
            if id(node) not in in_block:
                problems.append(f'{alias.name} is imported outside the startup block')

    for name in names:
        calls = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and (
                _call_name(n) == name
                or (isinstance(n.func, ast.Name) and n.func.id in local_names[name])
            )
        ]
        if len(calls) != 1:
            problems.append(f'{name}() is called {len(calls)} times in web/main.py (expected 1)')
            continue
        call = calls[0]
        if id(call) not in in_block:
            problems.append(f'{name}() runs outside the startup block')
        elif (call.lineno, call.col_offset) >= run_pos:
            problems.append(f'{name}() runs after ui.run')
    return problems


def test_storage_secret_comes_from_environment():
    problems = _storage_secret_problems(WEB_MAIN.read_text(encoding='utf-8'))
    assert problems == []


def test_secret_resolver_runs_only_in_the_startup_block():
    problems = _startup_call_problems(WEB_MAIN.read_text(encoding='utf-8'))
    assert problems == []


_GOOD_SOURCE = '''
from nicegui import ui
if __name__ in {'__main__', '__mp_main__'}:
    from web.session_hardening import require_session_id_validation, resolve_storage_secret
    storage_secret = resolve_storage_secret()
    require_session_id_validation()
    ui.run(storage_secret=storage_secret)
'''


@pytest.mark.parametrize('source, checker', [
    pytest.param(
        _GOOD_SOURCE.replace('storage_secret=storage_secret', "storage_secret='literal'"),
        _storage_secret_problems, id='literal-secret',
    ),
    pytest.param(
        _GOOD_SOURCE.replace('storage_secret = resolve_storage_secret()', "storage_secret = 'x'"),
        _storage_secret_problems, id='name-not-from-resolver',
    ),
    pytest.param(
        _GOOD_SOURCE.replace(
            "if __name__ in {'__main__', '__mp_main__'}:",
            "from web.session_hardening import resolve_storage_secret\n"
            "resolve_storage_secret()\n"
            "if __name__ in {'__main__', '__mp_main__'}:",
        ),
        _startup_call_problems, id='resolver-at-import-time',
    ),
    pytest.param(
        _GOOD_SOURCE.replace('    require_session_id_validation()\n', '')
        + '    require_session_id_validation()\n',
        _startup_call_problems, id='check-after-ui-run',
    ),
    pytest.param(
        _GOOD_SOURCE.replace('    require_session_id_validation()\n', '    # require_session_id_validation()\n'),
        _startup_call_problems, id='check-only-in-a-comment',
    ),
    pytest.param(
        _GOOD_SOURCE.replace(
            "if __name__ in {'__main__', '__mp_main__'}:",
            "from web.session_hardening import resolve_storage_secret as _early\n"
            "_early()\n"
            "if __name__ in {'__main__', '__mp_main__'}:",
        ),
        _startup_call_problems, id='aliased-resolver-at-import-time',
    ),
    pytest.param(
        _GOOD_SOURCE.replace(
            "if __name__ in {'__main__', '__mp_main__'}:",
            "from web.session_hardening import require_session_id_validation\n"
            "if __name__ in {'__main__', '__mp_main__'}:",
        ).replace(
            '    from web.session_hardening import require_session_id_validation, resolve_storage_secret\n',
            '    from web.session_hardening import resolve_storage_secret\n',
        ),
        _startup_call_problems, id='helper-imported-at-module-level',
    ),
    pytest.param(
        _GOOD_SOURCE.replace(
            'require_session_id_validation, resolve_storage_secret',
            'require_session_id_validation as _check, resolve_storage_secret',
        ).replace('    require_session_id_validation()\n', '')
        + '    _check()\n',
        _startup_call_problems, id='aliased-check-after-ui-run',
    ),
])
def test_startup_guards_catch_a_seeded_violation(source, checker):
    """The two AST guards above must be able to fail."""
    assert checker(_GOOD_SOURCE) == []
    assert checker(source) != []


def test_startup_guard_counts_aliased_calls_in_the_startup_block():
    """Importing the two helpers under other names inside the startup block is
    still the correct shape, so the guard must count those calls, not report
    them as missing."""
    source = (
        _GOOD_SOURCE
        .replace(
            'require_session_id_validation, resolve_storage_secret',
            'require_session_id_validation as _check, resolve_storage_secret as _resolve',
        )
        .replace('= resolve_storage_secret()', '= _resolve()')
        .replace('    require_session_id_validation()\n', '    _check()\n')
    )
    assert '_resolve()' in source and '_check()' in source
    assert _startup_call_problems(source) == []


@pytest.mark.parametrize('environ', [
    pytest.param({}, id='unset'),
    pytest.param({'GENIZAH_STORAGE_SECRET': ''}, id='empty'),
    pytest.param({'GENIZAH_STORAGE_SECRET': '   '}, id='blank'),
    pytest.param({'GENIZAH_STORAGE_SECRET': 'a' * 31}, id='31-characters'),
    pytest.param({'GENIZAH_STORAGE_SECRET': '  ' + 'b' * 31 + '  '}, id='31-characters-after-strip'),
])
def test_secret_resolver_refuses_missing_blank_or_short_values(environ):
    from web.session_hardening import resolve_storage_secret

    with pytest.raises(SystemExit) as excinfo:
        resolve_storage_secret(environ=environ)
    message = str(excinfo.value.code)
    assert 'GENIZAH_STORAGE_SECRET' in message
    assert 'secrets.token_urlsafe(32)' in message
    raw = environ.get('GENIZAH_STORAGE_SECRET', '').strip()
    if raw:
        assert raw not in message, 'the refusal must never echo the value'


def test_secret_resolver_returns_stripped_value():
    import secrets

    from web.session_hardening import resolve_storage_secret

    value = secrets.token_urlsafe(32)
    assert len(value) >= 32
    assert resolve_storage_secret(environ={'GENIZAH_STORAGE_SECRET': f'  {value}\n'}) == value
    exact = 'c' * 32
    assert resolve_storage_secret(environ={'GENIZAH_STORAGE_SECRET': exact}) == exact


def test_secret_resolver_reads_the_process_environment_by_default(monkeypatch):
    from web.session_hardening import resolve_storage_secret

    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'd' * 40)
    assert resolve_storage_secret() == 'd' * 40
    monkeypatch.delenv('GENIZAH_STORAGE_SECRET')
    with pytest.raises(SystemExit):
        resolve_storage_secret()


# ---------------------------------------------------------------------------
# session id validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('bad_id', MALFORMED_IDS)
def test_malformed_session_id_is_replaced_before_user_storage(bad_id, recorded_storage):
    seen: list = []
    client = TestClient(_direct_chain(_endpoint_recording_session(seen)), raise_server_exceptions=False)

    _assert_positive_control(client, recorded_storage)

    response = _request(client, {'id': bad_id, 'remember': 'kept'})
    assert response.status_code == 200
    new_id = recorded_storage[-1]
    assert isinstance(new_id, str) and UUID4_RE.fullmatch(new_id), new_id
    assert new_id != bad_id
    assert new_id != CANONICAL_ID
    # The endpoint and the re-signed cookie see the same new id; other keys survive.
    assert seen[-1] == {'id': new_id, 'remember': 'kept'}
    assert _decode_set_cookie(response) == {'id': new_id, 'remember': 'kept'}


@pytest.mark.parametrize('body', NON_OBJECT_SESSION_BODIES)
def test_non_object_session_body_gets_a_fresh_session(body, recorded_storage):
    seen: list = []
    client = TestClient(_direct_chain(_endpoint_recording_session(seen)), raise_server_exceptions=False)

    _assert_positive_control(client, recorded_storage)

    response = _request(client, body)
    assert response.status_code == 200
    new_id = recorded_storage[-1]
    assert isinstance(new_id, str) and UUID4_RE.fullmatch(new_id), new_id
    assert new_id != CANONICAL_ID
    assert seen[-1] == {'id': new_id}
    assert _decode_set_cookie(response) == {'id': new_id}


def test_canonical_session_id_is_kept(recorded_storage):
    """Regression guard, green before and after: a valid id and its session survive.

    Validation must not log anyone out -- only a change of secret does.
    """
    seen: list = []
    client = TestClient(_direct_chain(_endpoint_recording_session(seen)))

    response = _request(client, {'id': OTHER_CANONICAL_ID, 'remember': 'kept'})
    assert response.status_code == 200
    assert recorded_storage == [OTHER_CANONICAL_ID]
    assert seen[-1] == {'id': OTHER_CANONICAL_ID, 'remember': 'kept'}
    assert _decode_set_cookie(response) == {'id': OTHER_CANONICAL_ID, 'remember': 'kept'}


def test_request_without_a_cookie_gets_a_canonical_id(recorded_storage):
    """Regression guard, green before and after: NiceGUI's own minting still applies."""
    seen: list = []
    client = TestClient(_direct_chain(_endpoint_recording_session(seen)))

    response = client.get('/search')
    assert response.status_code == 200
    assert len(recorded_storage) == 1
    assert UUID4_RE.fullmatch(recorded_storage[0])
    assert _decode_set_cookie(response) == {'id': recorded_storage[0]}


def test_public_asset_requests_still_skip_the_session(recorded_storage):
    """Regression guard, green before and after: public assets never touch the session."""
    async def app(scope, receive, send):
        assert 'session' not in scope
        await PlainTextResponse('asset')(scope, receive, send)

    client = TestClient(_direct_chain(app))
    response = client.get(
        '/static/common.css',
        headers={'Cookie': f'session={_signed_cookie({"id": "not-a-uuid"})}'},
    )
    assert response.status_code == 200
    assert 'set-cookie' not in response.headers
    assert recorded_storage == []


FILE_NAME_CASES = [
    pytest.param('not-a-uuid', id='plain-text'),
    pytest.param(12345, id='integer'),
    pytest.param(None, id='null'),
    pytest.param(CANONICAL_ID.upper(), id='uppercase-uuid'),
    pytest.param(CANONICAL_ID.replace('-', ''), id='uuid-without-hyphens'),
    pytest.param('{' + CANONICAL_ID + '}', id='uuid-in-braces'),
]


@pytest.mark.parametrize('bad_id', FILE_NAME_CASES)
def test_user_storage_uses_canonical_session_ids(bad_id, tmp_path, monkeypatch):
    """With NiceGUI's REAL user-storage creation, storage is created only for canonical session ids."""
    from nicegui import core
    from nicegui import storage as nicegui_storage

    monkeypatch.setattr(nicegui_storage.Storage, 'path', tmp_path)
    monkeypatch.setattr(nicegui_storage.Storage, 'redis_url', None)
    monkeypatch.setattr(core.app.storage, '_users', {})
    monkeypatch.setattr(core, 'loop', None)  # FilePersistentDict.backup then writes synchronously
    # Under pytest NiceGUI can believe it is in its script-mode preflight, where
    # app.storage.user is a throwaway dict that is never written to disk.
    monkeypatch.setattr(core, 'script_mode', False)

    async def app(scope, receive, send):
        core.app.storage.user['k'] = 1
        await PlainTextResponse('page')(scope, receive, send)

    client = TestClient(_direct_chain(app), raise_server_exceptions=False)

    control = _request(client, {'id': CANONICAL_ID})
    assert control.status_code == 200
    assert (tmp_path / f'storage-user-{CANONICAL_ID}.json').is_file(), 'positive control'

    response = _request(client, {'id': bad_id})
    assert response.status_code == 200

    name_re = re.compile(r'storage-user-' + UUID4_RE.pattern + r'\.json')
    entries = sorted(tmp_path.iterdir())
    assert len(entries) == 2, [e.name for e in entries]
    for entry in entries:
        assert entry.is_file(), entry.name
        assert name_re.fullmatch(entry.name), entry.name


# ---------------------------------------------------------------------------
# the validating class is what NiceGUI installs -- and startup refuses otherwise
# ---------------------------------------------------------------------------

@pytest.fixture
def cleared_nicegui_middleware(monkeypatch):
    """Clear NiceGUI's process-global middleware state; restore it afterwards."""
    from nicegui import core
    from nicegui import storage as nicegui_storage

    saved_user_middleware = core.app.user_middleware
    saved_stack = core.app.middleware_stack
    saved_secret = nicegui_storage.Storage.secret
    saved_session_cls = nicegui_storage.SessionMiddleware
    saved_tracking_cls = nicegui_storage.RequestTrackingMiddleware
    core.app.user_middleware = []
    core.app.middleware_stack = None
    try:
        yield core.app
    finally:
        core.app.user_middleware = saved_user_middleware
        core.app.middleware_stack = saved_stack
        nicegui_storage.Storage.secret = saved_secret
        nicegui_storage.SessionMiddleware = saved_session_cls
        nicegui_storage.RequestTrackingMiddleware = saved_tracking_cls


def test_nicegui_installs_the_validating_middleware(cleared_nicegui_middleware, recorded_storage):
    """Call site: the chain ``set_storage_secret`` really installs validates ids."""
    from nicegui import storage as nicegui_storage

    from web.framework_patches import _patch_static_asset_session_middleware

    _patch_static_asset_session_middleware()
    nicegui_storage.set_storage_secret(TEST_SECRET, {})

    seen: list = []
    chain = _endpoint_recording_session(seen)
    for middleware in reversed(cleared_nicegui_middleware.user_middleware):
        cls, args, kwargs = middleware
        chain = cls(chain, *args, **kwargs)
    client = TestClient(chain, raise_server_exceptions=False)

    _assert_positive_control(client, recorded_storage)

    response = _request(client, {'id': 'not-a-uuid'})
    assert response.status_code == 200
    new_id = recorded_storage[-1]
    assert UUID4_RE.fullmatch(new_id), new_id
    assert _decode_set_cookie(response) == {'id': new_id}


def test_startup_check_passes_with_the_validating_class(cleared_nicegui_middleware):
    from web.framework_patches import _patch_static_asset_session_middleware
    from web.session_hardening import require_session_id_validation

    _patch_static_asset_session_middleware()
    require_session_id_validation()  # must not raise


def test_session_id_validation_survives_a_newer_nicegui(cleared_nicegui_middleware, monkeypatch):
    """On a NiceGUI newer than the audited version the swap is skipped -- startup must refuse."""
    from nicegui import storage as nicegui_storage
    from packaging.version import Version

    import web.framework_patches as framework_patches
    from web.session_hardening import require_session_id_validation

    monkeypatch.setattr(framework_patches, '_NV', Version('9.9.9'))
    nicegui_storage.RequestTrackingMiddleware = framework_patches._NiceGUIRequestTrackingMiddleware
    framework_patches._patch_static_asset_session_middleware()
    assert nicegui_storage.RequestTrackingMiddleware is framework_patches._NiceGUIRequestTrackingMiddleware

    with pytest.raises(SystemExit) as excinfo:
        require_session_id_validation()
    assert 'session id validation' in str(excinfo.value.code).lower()


# ---------------------------------------------------------------------------
# the dev launcher refuses before it spawns the server
# ---------------------------------------------------------------------------

def _load_server_script():
    spec = importlib.util.spec_from_file_location('_server_cli_under_test', REPO_ROOT / 'scripts' / 'server.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_server_launcher_refuses_without_a_storage_secret(monkeypatch, tmp_path):
    server = _load_server_script()
    monkeypatch.setattr(sys, 'path', list(sys.path))  # the launcher may prepend its root
    monkeypatch.delenv('GENIZAH_STORAGE_SECRET', raising=False)
    monkeypatch.setattr(server, 'PROJECT_DIR', tmp_path)  # no .env there
    monkeypatch.setattr(server, 'PID_FILE', tmp_path / '.server.pid')
    monkeypatch.setattr(server, 'is_server_running', lambda: (False, None))

    spawned: list = []

    def _fake_popen(*args, **kwargs):
        spawned.append(args)
        return SimpleNamespace(pid=0)

    monkeypatch.setattr(server.subprocess, 'Popen', _fake_popen)
    monkeypatch.setattr(server.time, 'sleep', lambda _seconds: None)
    monkeypatch.chdir(tmp_path)

    exit_code = 'did not exit'
    try:
        server.start_server()
    except SystemExit as exc:
        exit_code = exc.code
    assert spawned == [], 'the launcher spawned the web app without a storage secret'
    assert exit_code not in (0, None, 'did not exit')
    assert 'GENIZAH_STORAGE_SECRET' in str(exit_code)


# ---------------------------------------------------------------------------
# the end-to-end test fixture starts the real web app with a storage secret
# ---------------------------------------------------------------------------

E2E_CONFTEST = REPO_ROOT / 'tests' / 'e2e' / 'conftest.py'


def _load_e2e_conftest():
    spec = importlib.util.spec_from_file_location('_e2e_conftest_under_test', E2E_CONFTEST)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_e2e_storage_secret_is_set_for_the_test_and_removed_after(monkeypatch):
    from web.session_hardening import resolve_storage_secret

    monkeypatch.delenv('GENIZAH_STORAGE_SECRET', raising=False)
    conftest = _load_e2e_conftest()
    with conftest.e2e_storage_secret():
        resolve_storage_secret()  # would raise SystemExit if missing or short
    assert 'GENIZAH_STORAGE_SECRET' not in os.environ


def test_e2e_storage_secret_keeps_a_value_already_set(monkeypatch):
    existing = 'e2e-existing-secret-' + 'x' * 32
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', existing)
    conftest = _load_e2e_conftest()
    with conftest.e2e_storage_secret():
        assert os.environ['GENIZAH_STORAGE_SECRET'] == existing
    assert os.environ['GENIZAH_STORAGE_SECRET'] == existing


def _screen_fixture_problems(source: str) -> list[str]:
    """The ``screen`` fixture yields inside ``with e2e_storage_secret():``."""
    tree = ast.parse(source)
    fixtures = [
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == 'screen'
    ]
    if len(fixtures) != 1:
        return [f'expected one screen fixture, found {len(fixtures)}']
    guarded_yields = set()
    for node in ast.walk(fixtures[0]):
        if not isinstance(node, ast.With):
            continue
        if any(
            isinstance(item.context_expr, ast.Call) and _call_name(item.context_expr) == 'e2e_storage_secret'
            for item in node.items
        ):
            guarded_yields |= {id(n) for n in ast.walk(node) if isinstance(n, ast.Yield)}
    yields = [n for n in ast.walk(fixtures[0]) if isinstance(n, ast.Yield)]
    if not yields:
        return ['the screen fixture never yields']
    if any(id(y) not in guarded_yields for y in yields):
        return ['the screen fixture yields outside with e2e_storage_secret()']
    return []


def test_e2e_screen_fixture_starts_the_app_with_a_storage_secret():
    assert _screen_fixture_problems(E2E_CONFTEST.read_text(encoding='utf-8')) == []


@pytest.mark.parametrize('source', [
    pytest.param('def screen():\n    yield 1\n', id='no-guard'),
    pytest.param('def screen():\n    # with e2e_storage_secret():\n    yield 1\n', id='guard-only-in-a-comment'),
    pytest.param(
        'def screen():\n    with e2e_storage_secret():\n        pass\n    yield 1\n',
        id='yield-after-the-guard',
    ),
])
def test_screen_fixture_guard_catches_a_seeded_violation(source):
    """The guard above must be able to fail."""
    good = 'def screen():\n    with e2e_storage_secret():\n        yield 1\n'
    assert _screen_fixture_problems(good) == []
    assert _screen_fixture_problems(source) != []
