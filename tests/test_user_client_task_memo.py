# -*- coding: utf-8 -*-
"""Phase 92.2 D-MEMO-01..04 behavioral tests for the task-scoped WeakKeyDictionary
memo wrapping get_user_client().

9 tests covering:
- Test 1: same task returns cached client
- Test 2: different tasks each build their own client
- Test 3: token rotation invalidates memo key
- Test 4: sync context (no asyncio task) bypasses memo
- Test 5: anonymous fallback path bypasses memo
- Test 6: memo entry GC'd when task ends
- Test 7: Phase 90 D-12 invariant (no cross-request cache introduced)
- Test 8: run.io_bound / asyncio.to_thread context bypasses memo (Reviews Codex-MEDIUM-3)
- Test 9: get_persisted_session_uuid() is used (NOT safe_user_get('_session_uuid')) (Reviews Codex-MEDIUM-2)
"""

import asyncio
import gc
import weakref
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

import web.supabase_client as mod


def _seed_logged_in_storage(monkeypatch):
    """Seed storage with a valid auth session (mirrors test_supabase_client_reader_rls pattern)."""
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            '_session_uuid': 'abcd1234efab5678abcd1234efab5678',
            'auth_session': {
                'access_token': 'good.future.jwt',
                'refresh_token': 'good-refresh-token',
            },
        })),
    )


def _seed_anonymous_storage(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={})),
    )


def _install_common_stubs(m, monkeypatch):
    """Stub supabase plumbing so get_user_client() runs without network."""
    monkeypatch.setattr(m, '_access_token_near_expiry', lambda _t: False)

    apply_mock = MagicMock(name='apply_user_auth')
    monkeypatch.setattr(m, '_apply_user_auth_to_client', apply_mock)

    fake_client = MagicMock(name='fake_client')
    # Each create_client call must return a DISTINCT object so id() comparisons work.
    # We use a counter-based factory.
    call_count = [0]

    def make_fresh_client(*args, **kwargs):
        call_count[0] += 1
        c = MagicMock(name=f'client_{call_count[0]}')
        return c

    monkeypatch.setattr(m, 'create_client', make_fresh_client)

    anon_sentinel = MagicMock(name='anonymous_singleton')
    get_client_mock = MagicMock(return_value=anon_sentinel)
    monkeypatch.setattr(m, 'get_client', get_client_mock)

    return apply_mock, anon_sentinel, get_client_mock


def test_memo_within_same_task(monkeypatch):
    """Test 1: two calls within the same asyncio.Task return the same Client instance."""
    _seed_logged_in_storage(monkeypatch)
    apply_mock, _, _ = _install_common_stubs(mod, monkeypatch)

    # Clear memo state before test
    mod._user_client_memo.clear()

    async def runner():
        client_a = mod.get_user_client()
        client_b = mod.get_user_client()
        return client_a, client_b

    client_a, client_b = asyncio.run(runner())
    assert id(client_a) == id(client_b), "same task must return the same memo'd client"


def test_memo_different_tasks_each_build_fresh(monkeypatch):
    """Test 2: two different tasks with the same session/token each get their OWN client."""
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    result_a = [None]
    result_b = [None]

    async def task_a():
        result_a[0] = mod.get_user_client()

    async def task_b():
        result_b[0] = mod.get_user_client()

    async def runner():
        await asyncio.gather(asyncio.ensure_future(task_a()), asyncio.ensure_future(task_b()))

    asyncio.run(runner())
    assert result_a[0] is not None and result_b[0] is not None
    assert id(result_a[0]) != id(result_b[0]), "different tasks must each build their own client"


def test_memo_token_rotation_builds_fresh(monkeypatch):
    """Test 3: if access_token rotates between calls, a fresh client is built."""
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    auth_session_store = {
        '_session_uuid': 'abcd1234efab5678abcd1234efab5678',
        'auth_session': {'access_token': 'token-v1', 'refresh_token': 'good-refresh-token'},
    }
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=auth_session_store)),
    )

    async def runner():
        client_a = mod.get_user_client()
        # Rotate token
        auth_session_store['auth_session']['access_token'] = 'token-v2'
        client_b = mod.get_user_client()
        return client_a, client_b

    client_a, client_b = asyncio.run(runner())
    assert id(client_a) != id(client_b), "token rotation must invalidate memo key"


def test_memo_sync_context_bypasses_memo(monkeypatch):
    """Test 4: when called outside asyncio (current_task() is None), memo is bypassed."""
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    # Call synchronously (no running event loop)
    client_a = mod.get_user_client()
    client_b = mod.get_user_client()
    # In sync context, each call builds fresh (no memo)
    assert id(client_a) != id(client_b), "sync context must bypass memo and build fresh each call"


def test_memo_anonymous_bypasses_memo(monkeypatch):
    """Test 5: when no auth_session, anonymous path is taken; no memo entry written."""
    _seed_anonymous_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    async def runner():
        client = mod.get_user_client()
        # Check that no memo entry was written
        task = asyncio.current_task()
        memo_entry = mod._user_client_memo.get(task)
        return client, memo_entry

    client, memo_entry = asyncio.run(runner())
    assert memo_entry is None, "anonymous path must not write to memo"


def test_memo_gc_on_task_end(monkeypatch):
    """Test 6: memo entry is GC'd when the task ends."""
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    task_ref = [None]

    async def task_fn():
        mod.get_user_client()
        task_ref[0] = weakref.ref(asyncio.current_task())

    asyncio.run(task_fn())

    gc.collect()
    # After task ends and GC runs, the WeakKeyDictionary entry should be gone
    assert len(mod._user_client_memo) == 0, "memo must be empty after task GC"


def test_memo_no_cross_request_cache_introduced(monkeypatch):
    """Test 7: Phase 90 D-12 — no non-WeakKeyDictionary cross-request cache introduced.

    Checks that the 3 names deleted in Phase 90 D-12 are not ASSIGNED anywhere in the
    module (assignment = re-introduction of a cross-request cache variable).
    Comment references to these names are allowed (they document why they were deleted).
    """
    import ast
    src = open('web/supabase_client.py', encoding='utf-8').read()

    # Parse AST and check no assignment targets use the deleted Phase 90 names.
    tree = ast.parse(src)
    assigned_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assigned_names.add(target.id)
        elif isinstance(node, (ast.AnnAssign,)):
            if isinstance(node.target, ast.Name):
                assigned_names.add(node.target.id)

    assert '_client_cache' not in assigned_names, "_client_cache was deleted in Phase 90 D-12; must remain absent as an assignment"
    assert '_CLIENT_CACHE_TTL' not in assigned_names, "_CLIENT_CACHE_TTL deleted in Phase 90 D-12"
    assert '_prune_session_client_cache' not in assigned_names, "_prune_session_client_cache deleted in Phase 90 D-12"
    # _user_client_memo IS present as the task-scoped memo (the ONLY cache re-introduction)
    assert '_user_client_memo' in assigned_names, "_user_client_memo must be assigned"
    assert 'WeakKeyDictionary' in src, "_user_client_memo WeakKeyDictionary must be present"


def test_memo_bypass_in_to_thread(monkeypatch):
    """Test 8: asyncio.to_thread context bypasses memo (Reviews Codex-MEDIUM-3).

    Inside asyncio.to_thread, asyncio.current_task() returns None (the thread
    runs in a thread pool, not an event loop coroutine). The memo must be bypassed
    and a fresh client built each call.
    """
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    mod._user_client_memo.clear()

    async def runner():
        client_a = await asyncio.to_thread(mod.get_user_client)
        client_b = await asyncio.to_thread(mod.get_user_client)
        return client_a, client_b

    client_a, client_b = asyncio.run(runner())
    # In to_thread context, asyncio.current_task() is None, so memo is bypassed
    # and fresh clients are built each time.
    assert id(client_a) != id(client_b), "to_thread context must bypass memo (no event-loop task)"


def test_memo_uses_get_persisted_session_uuid(monkeypatch):
    """Test 9: get_persisted_session_uuid() is used for the memo key (Reviews Codex-MEDIUM-2).

    Verifies the memo path calls get_persisted_session_uuid() (the validated UUID
    accessor) rather than the raw safe_user_get('_session_uuid') path.

    FRAGILITY NOTE: this test depends on the local-import pattern inside get_user_client().
    The body does `from web.safe_storage import get_persisted_session_uuid as _gp_uuid`
    (or equivalent). The monkeypatch targets web.safe_storage.get_persisted_session_uuid.
    If a future refactor moves the import to module-top, update to patch
    web.supabase_client.get_persisted_session_uuid instead.
    """
    _seed_logged_in_storage(monkeypatch)
    _install_common_stubs(mod, monkeypatch)

    gp_mock = MagicMock(return_value='abcd1234efab5678abcd1234efab5678')
    monkeypatch.setattr('web.safe_storage.get_persisted_session_uuid', gp_mock)

    async def runner():
        return mod.get_user_client()

    asyncio.run(runner())
    assert gp_mock.called, "memo must call get_persisted_session_uuid() (validated UUID path)"
