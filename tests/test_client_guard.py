# -*- coding: utf-8 -*-
"""Behavioural tests for the deferred-render teardown guard.

REGRESSION ORIGIN (v8.5.2, 2026-07-31)
--------------------------------------
Nine call sites guarded their deferred renders with::

    if container.client.has_been_deleted:
        return

``has_been_deleted`` is not a NiceGUI attribute and never was. Reading it raised
``AttributeError`` on EVERY call -- the ordinary happy path, not just teardown --
and each caller's ``except Exception`` swallowed it, so /corrections sat on
"Loading your edits..." forever and the homepage recent-items panel and discovery
replies never populated.

The tests that were supposed to cover this asserted the *string*
``'has_been_deleted'`` appeared in the loader's source. They pinned the
misspelling. Nothing ever executed a guard against a real ``nicegui.Client``.

So these tests deliberately use REAL ``Client`` objects, never mocks: a
``Mock()`` returns a truthy attribute for any name, which is precisely the trap
that produced the bug. ``test_dead_attribute_is_really_absent`` fails loudly if a
future NiceGUI ever adds ``has_been_deleted``, at which point this note (and the
choice of membership check) should be revisited.
"""

import ast
from pathlib import Path

import pytest

from nicegui import Client

from web.client_guard import client_gone

REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / 'web'

# The attribute that did not exist. Pinned as a literal so the repo-wide guard
# below cannot be satisfied by a rename that reintroduces the same class of bug.
DEAD_ATTR = 'has_been_deleted'


# ---------------------------------------------------------------------------
# The core claim: the old attribute is absent, the new check is real
# ---------------------------------------------------------------------------

def test_dead_attribute_is_really_absent():
    """The whole bug in one assertion: NiceGUI has no such attribute."""
    assert not hasattr(Client, DEAD_ATTR), (
        f'nicegui.Client now defines {DEAD_ATTR!r}. The v8.5.2 hotfix assumed it '
        'does not exist; re-evaluate web/client_guard.client_gone.'
    )


def test_instances_membership_is_the_real_signal():
    """`Client.delete()` removes the client from `Client.instances` -- that is
    the public behaviour `client_gone` relies on."""
    source = __import__('inspect').getsource(Client.delete)
    assert 'del Client.instances[self.id]' in source, (
        'Client.delete no longer removes itself from Client.instances; '
        'client_gone needs a new signal.'
    )


# ---------------------------------------------------------------------------
# Behaviour against real Client objects
# ---------------------------------------------------------------------------

class _FakeElement:
    """Minimal stand-in for a NiceGUI element: it only needs `.client`."""

    def __init__(self, client):
        self.client = client


@pytest.fixture()
def live_client():
    """A real Client registered in Client.instances, cleaned up afterwards."""
    client = object.__new__(Client)          # bypass __init__'s page machinery
    client.id = 'test-live-client'
    Client.instances[client.id] = client
    try:
        yield client
    finally:
        Client.instances.pop(client.id, None)


def test_live_client_is_not_gone(live_client):
    """The happy path. This is the assertion the old tests never made -- and it
    is the one that fails against `has_been_deleted`."""
    assert client_gone(_FakeElement(live_client)) is False


def test_deleted_client_is_gone(live_client):
    """Once the client leaves Client.instances, the guard must report gone."""
    element = _FakeElement(live_client)
    assert client_gone(element) is False

    del Client.instances[live_client.id]

    assert client_gone(element) is True


def test_guard_never_raises_on_a_live_client(live_client):
    """The actual production symptom was an exception, not a wrong answer."""
    try:
        client_gone(_FakeElement(live_client))
    except Exception as exc:  # pragma: no cover - the point is that it doesn't
        pytest.fail(f'client_gone raised on a live client: {exc!r}')


# ---------------------------------------------------------------------------
# Fail-open behaviour
# ---------------------------------------------------------------------------

def test_element_without_client_fails_open():
    """No resolvable client -> render anyway. A spurious True silently blanks a
    working surface, which is worse than a RuntimeError the caller handles."""
    assert client_gone(object()) is False


def test_client_without_id_fails_open():
    class _NoId:
        pass

    assert client_gone(_FakeElement(_NoId())) is False


def test_none_element_fails_open():
    assert client_gone(None) is False


# ---------------------------------------------------------------------------
# Repo-wide guard: the dead attribute must not come back
# ---------------------------------------------------------------------------

def _python_sources():
    for path in WEB_DIR.rglob('*.py'):
        if '__pycache__' in path.parts:
            continue
        yield path


def test_dead_attribute_is_not_used_anywhere_in_web():
    """AST-level, so a comment or docstring mentioning the name (as
    web/client_guard.py does, to explain the history) does not trip it."""
    offenders = []
    for path in _python_sources():
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == DEAD_ATTR:
                offenders.append(f'{path.relative_to(REPO_ROOT)}:{node.lineno}')

    assert not offenders, (
        f'`.{DEAD_ATTR}` is not a NiceGUI attribute and raises AttributeError '
        f'on every access. Use web.client_guard.client_gone(). Found at: '
        + ', '.join(offenders)
    )


def test_client_guard_docstring_explains_the_history():
    """Cheap guard against someone 'tidying away' the explanation and losing the
    reason this module exists as a shared helper rather than an inline check."""
    text = (WEB_DIR / 'client_guard.py').read_text(encoding='utf-8')
    assert DEAD_ATTR in text, 'client_guard.py no longer records what went wrong'
    assert 'has_socket_connection' in text, (
        'client_guard.py no longer explains why has_socket_connection is the '
        'wrong signal (it is False during initial page build)'
    )
