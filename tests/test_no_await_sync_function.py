# -*- coding: utf-8 -*-
"""Guard + unit tests for the 2026-07-30 event-loop blocking fixes.

Part 1 -- AST guard (mirrors tests/test_no_raw_storage_access.py):
    Forbid ``await some_function()`` where ``some_function`` is defined in the
    SAME module with a plain ``def`` (not ``async def``).

    Why this is a real bug and not a style nit: Python evaluates the call first,
    so the entire synchronous body runs -- on the event loop -- and only then
    does ``await None`` raise
    ``TypeError: object NoneType can't be used in 'await' expression``.
    Every occurrence found in production was wrapped in
    ``except Exception: pass``, so the TypeError was swallowed and the pattern
    looked like it worked: the data DID load (the body ran), while the
    "deferred" loader blocked the loop and any genuine failure was invisible.

    uvicorn runs a SINGLE worker here (``ui.run`` in web/main.py, no
    ``workers=``), so a blocked loop stalls every OTHER concurrent request too --
    unrelated static assets and ``/api/*`` routes included. That is why an
    almost idle box (load average 0.03) could still serve multi-second TTFBs.

    Five occurrences existed on 2026-07-30: web/pages/corrections.py (x2),
    web/pages/home.py, web/pages/discoveries.py, web/components/joins_panel.py.

Part 2 -- unit tests for ``web.pages.corrections.fetch_leaderboard_users``,
    which replaced a sequential in-render fanout (1 profiles query + up to 20
    correction-count queries, all on the loop, all before the page's HTML was
    returned) with an off-loop concurrent fetch.
"""

import ast
import asyncio
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / "web"


# --------------------------------------------------------------------------
# Part 1: AST guard
# --------------------------------------------------------------------------

def _find_awaited_sync_calls(source: str):
    """Return [(lineno, name)] for ``await name()`` where name is a local sync def."""
    tree = ast.parse(source)
    sync_defs: dict[str, int] = {}
    async_defs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            sync_defs[node.name] = node.lineno
        elif isinstance(node, ast.AsyncFunctionDef):
            async_defs.add(node.name)

    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Await):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        func = node.value.func
        if not isinstance(func, ast.Name):
            continue
        # A name can be BOTH (e.g. redefined); only flag when no async def shadows it.
        if func.id in sync_defs and func.id not in async_defs:
            violations.append((node.lineno, func.id))
    return violations


def test_no_awaited_sync_functions_in_web():
    """No production module under web/ may await a locally-defined sync function."""
    offenders = []
    for path in sorted(WEB_DIR.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:  # pragma: no cover - unreadable file
            continue
        for lineno, name in _find_awaited_sync_calls(source):
            offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno} await {name}()")

    assert not offenders, (
        "Found `await <sync function>()` -- the body runs ON the event loop and then "
        "raises TypeError (usually swallowed). Call it directly, or move its blocking "
        "I/O into run.io_bound and render on the loop:\n  " + "\n  ".join(offenders)
    )


def test_detector_fires_on_synthetic_violation():
    """The detector must actually catch the pattern it claims to catch."""
    bad = textwrap.dedent(
        """
        def load_data():
            return blocking_fetch()

        async def deferred():
            try:
                await load_data()
            except Exception:
                pass
        """
    )
    found = _find_awaited_sync_calls(bad)
    assert [name for _, name in found] == ["load_data"]


def test_detector_accepts_direct_call():
    """Calling the sync function without await is the fixed form."""
    good = textwrap.dedent(
        """
        def load_data():
            return blocking_fetch()

        async def deferred():
            load_data()
        """
    )
    assert _find_awaited_sync_calls(good) == []


def test_detector_accepts_awaiting_a_real_coroutine():
    """Awaiting an async def defined locally is correct and must not be flagged."""
    good = textwrap.dedent(
        """
        async def load_data():
            return await fetch()

        async def deferred():
            await load_data()
        """
    )
    assert _find_awaited_sync_calls(good) == []


def test_detector_ignores_run_io_bound_offload():
    """The off-loop form (await run.io_bound(sync_fn, ...)) is correct, not a violation."""
    good = textwrap.dedent(
        """
        def blocking():
            return 1

        async def deferred():
            data = await run.io_bound(blocking)
            return data
        """
    )
    assert _find_awaited_sync_calls(good) == []


# --------------------------------------------------------------------------
# Part 2: leaderboard fetch
# --------------------------------------------------------------------------

@pytest.fixture()
def corrections_module():
    return pytest.importorskip("web.pages.corrections")


def _patch_io_bound(monkeypatch, module):
    """Replace run.io_bound with a passthrough so no executor/loop plumbing is needed."""
    async def fake_io_bound(fn, *args, **kwargs):
        return fn(*args, **kwargs)
    monkeypatch.setattr(module.run, "io_bound", fake_io_bound)


def test_leaderboard_attaches_counts_and_queries_each_user_once(monkeypatch, corrections_module):
    profiles = [
        {"id": "u1", "full_name": "A", "reputation": 30},
        {"id": "u2", "full_name": "B", "reputation": 20},
        {"id": "u3", "full_name": "C", "reputation": 10},
    ]
    calls: list[str] = []

    monkeypatch.setattr(corrections_module, "_fetch_top_profiles", lambda limit: list(profiles))
    monkeypatch.setattr(
        corrections_module,
        "get_user_corrections_count",
        lambda uid: calls.append(uid) or {"u1": 7, "u2": 0, "u3": 4}[uid],
    )
    _patch_io_bound(monkeypatch, corrections_module)

    result = asyncio.run(corrections_module.fetch_leaderboard_users(limit=3))

    assert [u["_corrections_count"] for u in result] == [7, 0, 4]
    # Exactly one count query per user -- no duplicate fanout.
    assert sorted(calls) == ["u1", "u2", "u3"]


def test_leaderboard_empty_profiles_skips_count_queries(monkeypatch, corrections_module):
    called = []
    monkeypatch.setattr(corrections_module, "_fetch_top_profiles", lambda limit: [])
    monkeypatch.setattr(
        corrections_module, "get_user_corrections_count", lambda uid: called.append(uid) or 0
    )
    _patch_io_bound(monkeypatch, corrections_module)

    assert asyncio.run(corrections_module.fetch_leaderboard_users()) == []
    assert called == []


def test_leaderboard_count_failure_degrades_to_zero(monkeypatch, corrections_module):
    """One failing count must not lose the whole leaderboard."""
    monkeypatch.setattr(
        corrections_module,
        "_fetch_top_profiles",
        lambda limit: [{"id": "ok", "reputation": 5}, {"id": "boom", "reputation": 4}],
    )

    def flaky(uid):
        if uid == "boom":
            raise RuntimeError("supabase down")
        return 3

    monkeypatch.setattr(corrections_module, "get_user_corrections_count", flaky)
    _patch_io_bound(monkeypatch, corrections_module)

    result = asyncio.run(corrections_module.fetch_leaderboard_users())
    assert [u["_corrections_count"] for u in result] == [3, 0]


def test_leaderboard_tolerates_profile_without_id(monkeypatch, corrections_module):
    monkeypatch.setattr(
        corrections_module,
        "_fetch_top_profiles",
        lambda limit: [{"id": "u1", "reputation": 5}, {"reputation": 4}],
    )
    monkeypatch.setattr(corrections_module, "get_user_corrections_count", lambda uid: 2)
    _patch_io_bound(monkeypatch, corrections_module)

    result = asyncio.run(corrections_module.fetch_leaderboard_users())
    assert [u["_corrections_count"] for u in result] == [2, 0]
