---
phase: 88-state-separation-by-deletion
plan: 02
type: execute
wave: 2
depends_on: [88-01]
files_modified:
  - web/export_state.py
  - web/api.py
  - tests/test_export_cross_user_isolation.py
  - tests/test_export_state_selection.py
  - tests/test_api_export_json.py
  - tests/test_api_legacy_unchanged.py
  - .planning/phase87_storage_allowlist.yaml
autonomous: true
requirements: [STATE-03, STATE-04, STATE-05, STATE-06]
must_haves:
  truths:
    - "web/export_state.py no longer defines _TEST_BACKEND or _backend() — all functions call web.safe_storage.safe_user_get/safe_user_set/safe_user_pop directly."
    - "All setter/updater/clearer functions return None (not bool); the boolean from safe_user_set is absorbed internally to preserve the silent-failure contract."
    - "update_* functions guard against poisoned-shape payloads with isinstance(payload, dict) and adopt copy-on-update before mutation."
    - "The 4 test files (test_export_cross_user_isolation, test_export_state_selection, test_api_export_json, test_api_legacy_unchanged) monkeypatch web.safe_storage.app directly — no _TEST_BACKEND, no _StateProxy, no state.X = ... fixture setup for the 10 fields."
    - "parallels_source_text reader-side fallback at api.py lines 1928-1931, 1962-1964, 2063-2066 is deleted; source_text reads exclusively from meta['source_text']."
    - "The web/export_state.py entry is removed from .planning/phase87_storage_allowlist.yaml."
    - "tests/test_no_raw_storage_access.py still passes — Phase 87 lint scanner sees zero new raw accesses and one fewer allowlist entry."
    - "New test in test_export_cross_user_isolation.py proves the legacy source_text fallback is genuinely dead by asserting User A's source_text cannot leak into User B's parallels-export response."
    - "Plan-boundary green: pytest + ruff check + python scripts/check_docs.py all exit 0."
  artifacts:
    - path: "web/export_state.py"
      provides: "Fully rewritten module; routes through safe_storage helpers; no _TEST_BACKEND; no _backend()"
      contains: "from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop"
    - path: "tests/test_export_cross_user_isolation.py"
      provides: "Rewritten; monkeypatches web.safe_storage.app; documents sequential-simulation caveat in module docstring; includes new source_text leak regression test"
      contains: "web.safe_storage.app"
    - path: "tests/test_export_state_selection.py"
      provides: "Rewritten; _StateProxy class deleted; calls export_state helpers directly"
      contains: "from web import export_state"
    - path: ".planning/phase87_storage_allowlist.yaml"
      provides: "Allowlist with web/export_state.py entry removed"
      contains: "allowed_raw_access:"
  key_links:
    - from: "web/export_state.py"
      to: "web/safe_storage.py"
      via: "safe_user_get/safe_user_set/safe_user_pop imports"
      pattern: "from web\\.safe_storage import"
    - from: "tests/test_export_cross_user_isolation.py"
      to: "web/safe_storage.app"
      via: "monkeypatch.setattr web.safe_storage.app stub"
      pattern: "web\\.safe_storage\\.app"
    - from: "web/api.py reader sites"
      to: "set_parallels_export meta dict"
      via: "meta['source_text'] sole source after legacy fallback deletion"
      pattern: "meta\\.get\\('source_text'"
---

<objective>
Rewrite web/export_state.py to route through the Phase 87 safe_storage chokepoint instead of the _TEST_BACKEND shim; harden update_* functions with payload-shape guard + copy-on-update; delete the reader-side parallels_source_text legacy fallback in web/api.py; rewrite the 4 affected test files to monkeypatch web.safe_storage.app directly; delete the web/export_state.py entry from the Phase 87 allowlist; preserve plan-boundary green status.

Purpose: Eliminate the _TEST_BACKEND production-code shim — a defense-in-depth hardening per Phase 87 chokepoint discipline. Complete the D-13 fold-in Plan 88-01 staged (writer side wrote source_text into meta) by removing the now-dead reader-side fallback. Set up Plan 88-03 to delete the now write-orphaned AppState fields without test churn (tests must already be state.*-free by end of Plan 88-02).

Output: 7 modified files. AppState fields are still physically present on the class (Plan 88-03 deletes). The Phase 87 lint scanner sees one fewer allowlist entry. Source-text leak test proves the legacy fallback is genuinely dead.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@./CLAUDE.md
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md
@web/safe_storage.py
@web/export_state.py
@.planning/phase87_storage_allowlist.yaml
@tests/test_browse_state.py

<interfaces>
web/safe_storage.py helpers (already exists, do NOT modify):

```
safe_user_get(key: str, default: Any = None) -> Any     # returns default on any failure
safe_user_set(key: str, value: Any) -> bool             # returns False on any failure
safe_user_pop(key: str, default: Any = None) -> Any     # returns default on any failure
```

Test stub pattern (canonical from tests/test_browse_state.py lines 5-29):

```python
# Plain-dict-as-storage pattern with unittest.mock.patch context manager:
storage = {'export_search_payload': {...}}
with patch('web.safe_storage.app') as mock_app:
    mock_app.storage.user = storage
    # ... call into export_state or run a request through TestClient ...
```

Equivalent monkeypatch form (preferred for fixtures that yield then teardown):

```python
class _StubApp:
    class storage:
        user: dict = {}
def _make_stub(initial_storage: dict):
    stub = _StubApp()
    stub.storage.user = initial_storage
    return stub

monkeypatch.setattr('web.safe_storage.app', _make_stub({...}))
```

Either form is acceptable — `with patch(...)` mirrors the Phase 87 canonical tests, `monkeypatch.setattr` mirrors the Phase 87 fixture pattern.

The 4 tests being rewritten and their current shim usage:
- tests/test_export_cross_user_isolation.py — uses _TEST_BACKEND swap between User A and User B requests
- tests/test_export_state_selection.py — uses _StateProxy wrapper + _TEST_BACKEND
- tests/test_api_export_json.py — uses _TEST_BACKEND + state.X = ... fixtures
- tests/test_api_legacy_unchanged.py — uses _TEST_BACKEND + state.X = ... fixtures
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Rewrite web/export_state.py to route through safe_storage chokepoint</name>
  <files>web/export_state.py</files>
  <read_first>
    - web/export_state.py (full current file, 178 lines)
    - web/safe_storage.py (full file — helper contracts)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-09, D-10, D-11, D-12)
  </read_first>
  <action>
Full rewrite of web/export_state.py. The 7 public functions (set_search_export, update_search_export_results, update_search_export_selection, clear_search_export, set_parallels_export, update_parallels_export_filtered, clear_parallels_export) keep their exact signatures and return types. The 2 read functions (get_search_export, get_parallels_export) keep their signatures. Internal implementation routes through web.safe_storage helpers.

Per D-09: Delete _TEST_BACKEND module-level variable. Delete _backend() helper function. No module-level mutable test-injection state remains.

Per D-10: Setter/updater/clearer functions return None explicitly. The safe_user_set boolean return is wrapped (assigned to a discard variable or simply not assigned) — preserves the silent-failure contract today's callers in search.py/search_results.py/parallels.py depend on.

Per D-11: update_search_export_results, update_search_export_selection, update_parallels_export_filtered add `isinstance(payload, dict)` guard immediately after the safe_user_get retrieval and before any payload[k] = v mutation. If payload is None or not a dict, the function returns silently (preserves current "no-op when payload missing" behavior).

Per D-12: Same 3 update_* functions adopt copy-on-update: after retrieving the payload, do `payload = dict(payload)` BEFORE mutating, then `safe_user_set(_KEY, payload)`. This guards against the (theoretical) race where two same-session requests share a payload reference and interleave read-modify-write.

Concrete new module body to write verbatim:

```python
# -*- coding: utf-8 -*-
"""Per-session export payload storage.

Reads/writes export payloads through ``web.safe_storage`` chokepoint helpers
(``safe_user_get`` / ``safe_user_set`` / ``safe_user_pop``), which route to
``app.storage.user`` per NiceGUI session. The Phase 87 chokepoint provides:

  - prune-race protection (AssertionError absorbed -> default/no-op)
  - aliased-import enforcement (lint scanner verifies no raw access)
  - sole legal access pattern for per-user state outside the allowlist

Phase 88 (this rewrite) removed the ``_TEST_BACKEND`` shim and the
``_backend()`` helper that selected between it and ``app.storage.user``.
Tests now monkeypatch ``web.safe_storage.app`` directly (mirrors the
Phase 87 pattern in tests/test_browse_state.py), so production-code
shims are no longer required.

Update functions adopt:
  - ``isinstance(payload, dict)`` guard before mutating retrieved payloads
    (defends against poisoned-shape storage state)
  - copy-on-update (``payload = dict(payload)``) before reassigning
    (defends against shared-reference races between same-session requests)
"""
from typing import Optional, List, Dict, Any

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

_SEARCH_KEY = 'export_search_payload'
_PARALLELS_KEY = 'export_parallels_payload'


# ---------------------------------------------------------------------------
# Search export payload
# ---------------------------------------------------------------------------

def set_search_export(
    results: List[Dict[str, Any]],
    query: str,
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    selected_uids: Optional[List[str]] = None,
) -> None:
    """Write the search export payload to this user's session."""
    safe_user_set(_SEARCH_KEY, {
        'results': results,
        'query': query,
        'mode': mode,
        'gap': gap,
        'filters': filters,
        'warnings': warnings or [],
        'selected_uids': selected_uids,
    })


def get_search_export() -> Optional[Dict[str, Any]]:
    """Read this session's search export payload, or None if unset/pruned."""
    return safe_user_get(_SEARCH_KEY, None)


def update_search_export_results(results: List[Dict[str, Any]]) -> None:
    """Patch only the ``results`` field (post-display-filter sync)."""
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return  # D-11: poisoned-shape or missing payload
    payload = dict(payload)  # D-12: copy-on-update
    payload['results'] = results
    safe_user_set(_SEARCH_KEY, payload)


def update_search_export_selection(selected_uids: Optional[List[str]]) -> None:
    """Patch only the ``selected_uids`` field (per-row checkbox sync)."""
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    payload['selected_uids'] = selected_uids
    safe_user_set(_SEARCH_KEY, payload)


def clear_search_export() -> None:
    """Remove the search export payload (New Search reset)."""
    safe_user_pop(_SEARCH_KEY, None)


# ---------------------------------------------------------------------------
# Parallels export payload
# ---------------------------------------------------------------------------

def set_parallels_export(
    results: List[Dict[str, Any]],
    filtered: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write the parallels export payload to this user's session.

    The ``meta`` dict carries ``source_text`` plus envelope-echo metadata
    (chunk_size, mode, max_freq, filters, boundary_options, warnings).
    Per Phase 88 D-13, ``source_text`` is folded into ``meta`` instead of
    living in a separate ``app.storage.user['parallels_source_text']`` key.
    """
    safe_user_set(_PARALLELS_KEY, {
        'results': results,
        'filtered': filtered,
        'meta': meta,
    })


def get_parallels_export() -> Optional[Dict[str, Any]]:
    """Read this session's parallels export payload, or None."""
    return safe_user_get(_PARALLELS_KEY, None)


def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None:
    """Patch only the ``filtered`` field (post-filter sync)."""
    payload = safe_user_get(_PARALLELS_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    payload['filtered'] = filtered
    safe_user_set(_PARALLELS_KEY, payload)


def clear_parallels_export() -> None:
    """Remove the parallels export payload (New Search reset)."""
    safe_user_pop(_PARALLELS_KEY, None)
```

Verification of preservation:
- All 7 setter/updater/clearer functions return None (signatures unchanged from current).
- get_search_export and get_parallels_export keep Optional[Dict[str, Any]] return type.
- No `from nicegui import app` remains — only safe_storage imports.
- No _TEST_BACKEND, no _backend().
  </action>
  <verify>
    <automated>python -c "from web import export_state; assert not hasattr(export_state, '_TEST_BACKEND'); assert not hasattr(export_state, '_backend'); assert export_state.set_search_export([], 'q') is None; assert export_state.set_parallels_export([], [], None) is None; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "_TEST_BACKEND" web/export_state.py` returns 0 matches.
    - `grep -n "def _backend" web/export_state.py` returns 0 matches.
    - `grep -n "from nicegui import app" web/export_state.py` returns 0 matches.
    - `grep -n "from web.safe_storage import" web/export_state.py` returns 1 match importing safe_user_get, safe_user_set, safe_user_pop.
    - `python -c "from web import export_state; assert export_state.set_search_export([], 'q') is None"` exits 0.
    - `python -c "from web import export_state; assert export_state.set_parallels_export([], [], None) is None"` exits 0.
    - `python -c "import ast; ast.parse(open('web/export_state.py', encoding='utf-8').read())"` exits 0.
    - `python -m ruff check web/export_state.py` exits 0.
  </acceptance_criteria>
  <done>web/export_state.py rewritten: no _TEST_BACKEND, no _backend, all functions route through safe_storage helpers, update_* functions hardened with isinstance guard + copy-on-update, ABI preserved (None returns).</done>
</task>

<task type="auto">
  <name>Task 2: Delete parallels_source_text reader-side fallback in web/api.py (D-14)</name>
  <files>web/api.py</files>
  <read_first>
    - web/api.py (lines 1918-1980 for the two parallels export handlers; lines 2049-2070 for the JSON handler)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-14)
  </read_first>
  <action>
Delete the 3 reader-side legacy fallback blocks for parallels_source_text. After Plan 88-01 folded source_text into meta['source_text'] on every writer path (parallels.py:2323-2331), the fallback at these 3 reader sites is dead code.

Site 1 — export_parallels_excel at lines 1918-1950:

BEFORE (lines 1921-1931 area):
```
from web.export_state import get_parallels_export
from web.safe_storage import safe_user_get

session_payload = get_parallels_export() or {}
parallels_results = session_payload.get('results') or []
filtered_results = session_payload.get('filtered') or []
meta = session_payload.get('meta') or {}
# source_text: prefer meta, fall back to legacy app.storage.user key.
source_text = meta.get('source_text') or ''
if not source_text:
    source_text = safe_user_get('parallels_source_text', '') or ''
```

AFTER:
```
from web.export_state import get_parallels_export

session_payload = get_parallels_export() or {}
parallels_results = session_payload.get('results') or []
filtered_results = session_payload.get('filtered') or []
meta = session_payload.get('meta') or {}
# Phase 88 D-14: source_text reads exclusively from per-session meta.
# Legacy app.storage.user['parallels_source_text'] fallback removed;
# writer at parallels.py:2323-2331 populates meta['source_text'] on every
# completion path.
source_text = meta.get('source_text') or ''
```

DELETE: the `from web.safe_storage import safe_user_get` line, the `if not source_text:` block (2 lines).

Site 2 — export_parallels_word at lines 1952-1980:

Same shape as Site 1. Delete the `from web.safe_storage import safe_user_get` line, the `if not source_text:` block.

Site 3 — export_parallels_json at lines 2049-2070:

BEFORE:
```
from web.export_state import get_parallels_export
from web.safe_storage import safe_user_get

session_payload = get_parallels_export() or {}
parallels_results = session_payload.get('results') or []
filtered_results = session_payload.get('filtered') or []

# Empty-state check first - avoids touching app.storage.user when there's
# nothing to export (storage requires a NiceGUI request context which is
# absent in tests / non-NiceGUI callers).
if not parallels_results and not filtered_results:
    return Response("No parallels results to export", status_code=400)

meta = session_payload.get('meta') or {}
# Fallback: source_text from app.storage.user (legacy parallels write).
# safe_user_get absorbs the prune-race AssertionError internally.
storage_source_text = safe_user_get('parallels_source_text', '') or ''
source_text = (meta.get('source_text') or storage_source_text or '')
```

AFTER:
```
from web.export_state import get_parallels_export

session_payload = get_parallels_export() or {}
parallels_results = session_payload.get('results') or []
filtered_results = session_payload.get('filtered') or []

# Empty-state check first - avoids touching storage when there's nothing
# to export. Per Phase 88 D-14, source_text reads exclusively from meta.
if not parallels_results and not filtered_results:
    return Response("No parallels results to export", status_code=400)

meta = session_payload.get('meta') or {}
source_text = meta.get('source_text') or ''
```

DELETE: the `from web.safe_storage import safe_user_get` line, the `storage_source_text = ...` line, replace the 2-line "Fallback: source_text from app.storage.user" comment block. Net: -3 lines, -1 import.

Import cleanup: after Sites 1-3, check if `from web.safe_storage import safe_user_get` is still used anywhere ELSE in web/api.py via `grep -n "safe_user_get" web/api.py`. The expected remaining count after this task is 0 — if any matches remain, leave the import; otherwise delete every import line for safe_user_get from this file.

D-16 comment cleanup at api.py:1846-1848 (deferred to Plan 88-03). DO NOT TOUCH the 2026-05-12 cross-user-fix comment in this task — Plan 88-03 handles it.

Note on the bootstrap writer at parallels.py:457: this writer (`safe_user_set('parallels_source_text', text)`) is preserved by Plan 88-01 because it serves the textarea page-reload bootstrap reader at parallels.py:323, NOT the export path. Plan 88-02 does NOT touch this writer. The legacy key continues to live in app.storage.user for browser-reload UX; only the EXPORT-PATH fallback is being deleted here.
  </action>
  <verify>
    <automated>python -c "import re; src=open('web/api.py', encoding='utf-8').read(); assert 'parallels_source_text' not in src, 'reader-side parallels_source_text fallback still present'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "parallels_source_text" web/api.py` returns 0 matches.
    - `grep -n "storage_source_text" web/api.py` returns 0 matches.
    - `grep -nE "from web\\.export_state import get_parallels_export" web/api.py` returns 3 matches (one per handler).
    - `python -c "import ast; ast.parse(open('web/api.py', encoding='utf-8').read())"` exits 0.
    - `python -m ruff check web/api.py` exits 0.
  </acceptance_criteria>
  <done>3 reader-side legacy fallback blocks deleted; api.py reads source_text exclusively from meta['source_text']; D-14 closed.</done>
</task>

<task type="auto">
  <name>Task 3: Rewrite tests/test_export_cross_user_isolation.py (D-01, D-03, D-15)</name>
  <files>tests/test_export_cross_user_isolation.py</files>
  <read_first>
    - tests/test_export_cross_user_isolation.py (full current file)
    - tests/test_browse_state.py (canonical monkeypatch pattern, lines 5-80)
    - web/export_state.py (post-Task-1; the new module being tested)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-01, D-03, D-15)
  </read_first>
  <action>
Rewrite tests/test_export_cross_user_isolation.py to monkeypatch web.safe_storage.app directly (D-01) instead of swapping web.export_state._TEST_BACKEND. Add a new test asserting source_text cannot leak across sessions via the deleted fallback (D-15). Document the sequential-simulation caveat (D-03) in the module docstring.

Module docstring (rewrite verbatim):

```python
"""Cross-user isolation regression test for the export pipeline.

Bug 2026-05-12: User A's search query name appeared as the suggested
xlsx filename in User B's export dialog. They were on totally different
devices and networks; both shared the production process. Root cause:
``state.last_results`` / ``state.current_search_query`` / ``state.last_selected_uids``
on AppState (singleton) were the source of truth for the export handlers,
so the last writer won — User B's request to /api/export/excel read
whatever User A's search had just written to those fields.

Fix: handlers now read from ``web.export_state`` which routes through
``web.safe_storage`` to ``app.storage.user`` (per-session). This test
simulates two sessions with distinct storage dicts and asserts their
filenames + result counts are independent.

IMPORTANT (per Phase 88 D-03): this is SEQUENTIAL simulation, not true
concurrent coverage. We monkeypatch ``web.safe_storage.app`` to a stub
whose ``storage.user`` is a plain dict, swap the dict between requests
to model two sessions sharing one Python process, and verify
isolation. Real concurrency (two NiceGUI processes or fully-instantiated
``app.storage.user`` per request via the NiceGUI test harness) is
deferred to Phase 92 SWEEP-05 production smoke-test (two browser sessions,
manual checklist).
"""
```

Replace the existing fixture pattern with the monkeypatch-based pattern. Concrete shape (preserve User A vs. User B assertion semantics, change only the storage-injection mechanism):

```python
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock


class _StubApp:
    """Stub mirroring app.storage.user surface used by safe_storage helpers."""
    class storage:
        user: dict = {}


def _make_stub(initial_storage: dict) -> _StubApp:
    stub = _StubApp()
    stub.storage.user = initial_storage
    return stub


def _make_mock_meta_mgr():
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    return mgr


def _build_payload(results, query):
    return {
        'results': results,
        'query': query,
        'mode': 'text',
        'gap': None,
        'filters': None,
        'warnings': [],
        'selected_uids': None,
    }


def _user_results(uid_prefix, count=3):
    return [{
        'uid': f'{uid_prefix}_{i}',
        'display': {
            'shelfmark': f'T-S {uid_prefix}.{i}',
            'title': f'title {i}',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': f'header_99123456789012{i:02d}_IE99_P{i+1}',
        'snippet': f'a *match* {i}',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5 + i * 0.1,
    } for i in range(count)]


def test_two_sessions_get_independent_filenames(monkeypatch):
    """User A queries 'alpha', User B queries 'beta'. User B's xlsx must NOT
    carry User A's 'alpha' in its filename or User A's results.
    """
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # --- User A's session ---
        user_a_storage = {
            'export_search_payload': _build_payload(_user_results('A', count=3), 'alpha-query')
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_a_storage))
        r_a = client.get('/api/export/excel')
        assert r_a.status_code == 200
        cd_a = r_a.headers.get('content-disposition', '')
        assert 'alpha' in cd_a.lower(), f"User A's xlsx filename missing 'alpha': {cd_a}"

        # --- User B's session (different storage dict) ---
        user_b_storage = {
            'export_search_payload': _build_payload(_user_results('B', count=2), 'beta-query')
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_b_storage))
        r_b = client.get('/api/export/excel')
        assert r_b.status_code == 200
        cd_b = r_b.headers.get('content-disposition', '')
        assert 'beta' in cd_b.lower(), f"User B's xlsx filename missing 'beta': {cd_b}"
        assert 'alpha' not in cd_b.lower(), (
            f"CROSS-USER LEAK: User A's 'alpha' query appeared in User B's filename: {cd_b}"
        )
    finally:
        state.meta_mgr = saved_meta


def test_parallels_source_text_cannot_leak_via_deleted_fallback(monkeypatch):
    """Phase 88 D-15: prove the parallels_source_text legacy fallback is dead.

    User A's session writes 'alpha-text' to the deleted-fallback key
    ``app.storage.user['parallels_source_text']``. User B's session has
    NO parallels export payload and NO source_text in any form.

    Before Phase 88 D-14: User B's /api/export/parallels/excel would have
    fallen back to safe_user_get('parallels_source_text', '') — but since
    both sessions share storage in the singleton-mirror world, B would
    have read A's value.

    After Phase 88 D-14: the fallback is deleted; User B's handler reads
    only meta['source_text'] from their own (absent) export payload, gets
    '', and returns 400 (no parallels results) without ever touching the
    legacy key.

    Even if the legacy key is set in User A's storage, the assertion below
    proves User B's response is independent.
    """
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()
    try:
        # User A: has source_text in legacy key, no parallels payload.
        user_a_storage = {'parallels_source_text': 'alpha-leak-bait'}
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_a_storage))
        r_a = client.get('/api/export/parallels/excel')
        # No parallels results -> 400 either way; the goal is to prove
        # the handler does NOT crash because it no longer reads the legacy key.
        assert r_a.status_code == 400, f"Expected 400 (no parallels results), got {r_a.status_code}"
        assert b'alpha-leak-bait' not in r_a.content, (
            "Legacy fallback should be dead — source_text must not appear in any response"
        )

        # User B: empty storage. Different session simulated by storage swap.
        user_b_storage = {}
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_b_storage))
        r_b = client.get('/api/export/parallels/excel')
        assert r_b.status_code == 400
        assert b'alpha-leak-bait' not in r_b.content, (
            "CROSS-USER LEAK: User A's source_text appeared in User B's response. "
            "D-14 fallback deletion is broken."
        )
    finally:
        state.meta_mgr = saved_meta
```

Preserve any other tests already in the file by porting them to the same monkeypatch pattern. The two tests above are illustrative; the executor should retain coverage parity with the pre-rewrite file (read it first to enumerate existing test functions and port each one).

CRITICAL: do NOT import or reference `web.export_state._TEST_BACKEND` anywhere in the rewritten file. Do NOT call `state.last_results = ...` anywhere in fixtures — the goal is to keep tests state.*-free so Plan 88-03 can delete the fields without test churn.
  </action>
  <verify>
    <automated>python -m pytest tests/test_export_cross_user_isolation.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "_TEST_BACKEND" tests/test_export_cross_user_isolation.py` returns 0 matches.
    - `grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*=" tests/test_export_cross_user_isolation.py` returns 0 matches.
    - `grep -n "web.safe_storage.app" tests/test_export_cross_user_isolation.py` returns at least 4 matches (one per monkeypatch call in the 2 tests; more if existing tests are ported).
    - `python -m pytest tests/test_export_cross_user_isolation.py -v` exits 0 with at least 2 tests passing (D-01 cross-user + D-15 source_text leak).
    - `python -m ruff check tests/test_export_cross_user_isolation.py` exits 0.
  </acceptance_criteria>
  <done>tests/test_export_cross_user_isolation.py rewritten: monkeypatches web.safe_storage.app, no _TEST_BACKEND, no state.X fixture setup, source_text leak regression test added (D-15), module docstring documents sequential-simulation caveat (D-03).</done>
</task>

<task type="auto">
  <name>Task 4: Rewrite tests/test_export_state_selection.py (D-02)</name>
  <files>tests/test_export_state_selection.py</files>
  <read_first>
    - tests/test_export_state_selection.py (full current file)
    - tests/test_browse_state.py (canonical monkeypatch pattern)
    - web/export_state.py (post-Task-1)
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-02)
  </read_first>
  <action>
Rewrite tests/test_export_state_selection.py. Per D-02: delete the _StateProxy wrapper class entirely. Tests call export_state helpers directly to drive selection state, no shim, no state.X = fixture setup.

Module docstring (rewrite verbatim):

```python
"""
Phase 77 Plan 06 -- gap-closure regression tests.

Covers:
  Gap #1 (UAT test 8): _reset_search clears the export payload so
                       post-reset exports return 400.
  Gap #2 (UAT test 9): exports honor session_payload['selected_uids']:
                       - None  -> full set
                       - list  -> uid-filtered subset
                       - []    -> defensive -- treated as None
                       Filename gets '-selected-N' suffix when filtered.

Updated 2026-05-13 (Phase 88 D-02): tests now monkeypatch
``web.safe_storage.app`` to a stub whose ``storage.user`` is a plain
dict. The pre-Phase-88 ``_StateProxy`` wrapper and the
``web.export_state._TEST_BACKEND`` shim are deleted; tests call
``export_state.set_search_export(...)`` and
``export_state.update_search_export_selection(...)`` directly.

Builds a bare FastAPI app per fixture (mirrors test_api_export_json.py
HIGH-08 pattern) so handler logic can be exercised without NiceGUI.
"""
```

Core fixture pattern:

```python
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient


class _StubApp:
    class storage:
        user: dict = {}


def _make_stub(initial_storage: dict) -> _StubApp:
    stub = _StubApp()
    stub.storage.user = initial_storage
    return stub


@pytest.fixture(scope='module')
def bare_app_with_routes():
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app_with_routes):
    return TestClient(bare_app_with_routes)


@pytest.fixture
def mock_meta_mgr():
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    return mgr


@pytest.fixture
def session_with_5_results(mock_meta_mgr, monkeypatch):
    """Populate per-session export payload with 5 results via export_state helpers.

    No state.* setup; no _TEST_BACKEND. The fixture yields a (storage_dict, state)
    tuple. Tests drive selection by calling export_state.update_search_export_selection(...)
    OR by mutating storage_dict['export_search_payload']['selected_uids'] directly
    — both are equivalent because the helper is just a thin wrapper.
    """
    from web.api import state
    from web import export_state

    saved_meta = state.meta_mgr
    state.meta_mgr = mock_meta_mgr

    results = [{
        'uid': f'u{i}',
        'display': {
            'shelfmark': f'T-S 12.34{i}',
            'title': f'title {i}',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': f'header_99123456789012{i:02d}_IE99_P{i+1}',
        'snippet': f'a *match* {i}',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5 + i * 0.1,
    } for i in range(5)]

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Populate via the helper itself — proves the helper round-trips correctly.
    export_state.set_search_export(
        results=results,
        query='foo',
        mode='text',
        gap=None,
        filters=None,
        warnings=[],
        selected_uids=None,
    )

    yield storage  # tests mutate selected_uids via export_state.update_search_export_selection

    state.meta_mgr = saved_meta
```

Then port each existing test to use the new fixture. Examples:

```python
def test_export_json_no_selection_returns_full_set(client, session_with_5_results):
    # Already None from fixture; no extra setup needed.
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5
    assert len(body['results']) == 5


def test_export_json_with_selection_filters_by_uid(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(['u1', 'u3'])
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 2
    assert {item.get('uid') for item in body['results']} == {'u1', 'u3'}


def test_export_json_empty_selection_treated_as_none(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection([])
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5


def test_reset_clears_per_session_payload_then_export_returns_400(client, session_with_5_results):
    from web import export_state
    export_state.clear_search_export()
    r = client.get('/api/export/json')
    assert r.status_code == 400
    assert b'No results to export' in r.content
```

CRITICAL deletions in this rewrite:
- Delete `_StateProxy` class entirely (D-02).
- Delete every line that does `state.last_results = ...`, `state.current_search_query = ...`, etc.
- Delete `monkeypatch.setattr(export_state, '_TEST_BACKEND', ...)` lines.
- Delete the `saved = {...}` save/restore block that captured state.* values (no longer needed).

Port ALL existing test functions in the file. Read the file first to enumerate them; each existing test must have a ported equivalent.
  </action>
  <verify>
    <automated>python -m pytest tests/test_export_state_selection.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "_StateProxy" tests/test_export_state_selection.py` returns 0 matches.
    - `grep -n "_TEST_BACKEND" tests/test_export_state_selection.py` returns 0 matches.
    - `grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids)\\s*=" tests/test_export_state_selection.py` returns 0 matches.
    - `grep -n "web.safe_storage.app" tests/test_export_state_selection.py` returns at least 1 match (in the fixture).
    - `python -m pytest tests/test_export_state_selection.py -v` exits 0 with all original tests still passing (read pre-rewrite test names and confirm same count).
    - `python -m ruff check tests/test_export_state_selection.py` exits 0.
  </acceptance_criteria>
  <done>tests/test_export_state_selection.py rewritten: _StateProxy deleted, no _TEST_BACKEND, no state.X fixture setup, monkeypatch.setattr web.safe_storage.app pattern used; all ported tests pass.</done>
</task>

<task type="auto">
  <name>Task 5: Rewrite tests/test_api_export_json.py and tests/test_api_legacy_unchanged.py</name>
  <files>tests/test_api_export_json.py, tests/test_api_legacy_unchanged.py</files>
  <read_first>
    - tests/test_api_export_json.py (full file)
    - tests/test_api_legacy_unchanged.py (full file)
    - tests/test_browse_state.py (canonical monkeypatch pattern)
    - web/export_state.py (post-Task-1)
  </read_first>
  <action>
Apply the same rewrite pattern from Task 4 to both files. The structural goals are identical:
1. Delete every `state.X = ...` line for the 10 deleted-in-Plan-88-03 fields.
2. Delete every `monkeypatch.setattr(export_state, '_TEST_BACKEND', ...)` line.
3. Replace with `monkeypatch.setattr('web.safe_storage.app', _make_stub(initial_storage))` plus `export_state.set_search_export(...)` / `export_state.set_parallels_export(...)` to populate the per-session payload.
4. Update module docstrings to reference Phase 88 D-04 and the monkeypatch pattern.

For both files, the steps:

Step A — Read the pre-rewrite file and enumerate ALL test functions. Record names.

Step B — Identify per-test fixture setup. For each test, determine what storage state must be present BEFORE the handler is hit:
- If the test exercises /api/export/json with X results -> storage has `export_search_payload` from a call to `export_state.set_search_export(...)`.
- If the test exercises /api/export/parallels/excel -> storage has `export_parallels_payload` from `export_state.set_parallels_export(...)`.
- If the test exercises a no-payload empty-state branch -> storage is `{}`.

Step C — Write the new fixture using the `_StubApp` + `_make_stub(initial_storage)` pattern from Task 3/4. Use `monkeypatch.setattr('web.safe_storage.app', _make_stub({}))` then call the relevant `export_state.set_X(...)` helper to populate. If existing test passes `state.last_selected_uids = [...]` to drive selection, replace with `export_state.update_search_export_selection([...])`.

Step D — Port each test. The handler-call lines (e.g., `r = client.get('/api/export/json')`) and the assertion lines do not change. Only the storage-injection step changes.

Step E — Preserve test count parity. Pre-rewrite tests/test_api_export_json.py has N tests; post-rewrite must have the same N tests with the same names (or with `_state_proxy_` removed from any name that contained that token). Same for test_api_legacy_unchanged.py.

CRITICAL: do NOT silently drop tests. If a test in the pre-rewrite version is hard to port because it leaned on a `_StateProxy` corner case, port it to the cleaner `export_state.update_search_export_selection(...)` form — the helper exposes the same behavior. If a test in the pre-rewrite version explicitly tested that `_TEST_BACKEND` swap propagates to handlers, that test's purpose evaporates with the shim deletion; in that case rewrite the test to assert the monkeypatch.setattr propagates to handlers (same intent, new mechanism).

Both files must end with:
- 0 matches for `_TEST_BACKEND`
- 0 matches for `state.last_results = ` and friends (the 10 fields)
- At least 1 match for `web.safe_storage.app`
- Same number of test functions as pre-rewrite (or with renamed tests covering the same scenarios)
  </action>
  <verify>
    <automated>python -m pytest tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "_TEST_BACKEND" tests/test_api_export_json.py tests/test_api_legacy_unchanged.py` returns 0 matches total.
    - `grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*=" tests/test_api_export_json.py tests/test_api_legacy_unchanged.py` returns 0 matches total.
    - `grep -n "web.safe_storage.app" tests/test_api_export_json.py tests/test_api_legacy_unchanged.py` returns at least 2 matches (at least one per file).
    - `python -m pytest tests/test_api_export_json.py tests/test_api_legacy_unchanged.py -v` exits 0 with original test count preserved.
    - `python -m ruff check tests/test_api_export_json.py tests/test_api_legacy_unchanged.py` exits 0.
  </acceptance_criteria>
  <done>Both test files rewritten: no _TEST_BACKEND, no state.X fixture setup, monkeypatch.setattr web.safe_storage.app pattern used; all original tests ported and passing.</done>
</task>

<task type="auto">
  <name>Task 6: Delete web/export_state.py entry from Phase 87 allowlist</name>
  <files>.planning/phase87_storage_allowlist.yaml</files>
  <read_first>
    - .planning/phase87_storage_allowlist.yaml (full file)
    - tests/test_no_raw_storage_access.py (full file — to confirm what schema fields the H1 enforcement test reads)
  </read_first>
  <action>
Delete the `web/export_state.py` entry from `.planning/phase87_storage_allowlist.yaml`. The entry currently has 1 pattern (`source: "app.storage.user"`, `expected_count: 1`, `enclosing: "_backend (production fallthrough for _TEST_BACKEND shim)"`) with a multi-line justification.

After Task 1 lands, web/export_state.py contains zero raw `app.storage.user` accesses (it imports from safe_storage). The allowlist entry MUST be deleted because:
1. `test_no_raw_storage_access_outside_allowlist` no longer needs to allowlist this file — the file has 0 violations.
2. `test_allowlist_counts_exact` with `expected_count=1` would FAIL after Task 1 because the actual count drops to 0. Fix 3 in test_no_raw_storage_access.py at line 309-318 explicitly catches this case: "expected_count={pat['expected_count']} but file has no nicegui app import (actual count = 0). Either remove this allowlist entry or restore the import." Deletion is the right resolution.

Concrete edit: delete the entire entry block, currently lines 116-135 of the YAML file:

```yaml
  - file: web/export_state.py
    patterns:
      - source: "app.storage.user"
        expected_count: 1
        enclosing: "_backend (production fallthrough for _TEST_BACKEND shim)"
    justification: |
      Line 48 (`return app.storage.user`) is the production fallthrough inside
      `_backend()`, which exists ONLY to support the `_TEST_BACKEND` test
      injection shim. The AST scanner records the source segment of the bare
      `Attribute` access — which is just `app.storage.user`, NOT the full
      return statement. The pattern must be a substring of the recorded
      segment, so we match on `app.storage.user` and scope it via
      `enclosing: _backend` to prevent any future bare `app.storage.user`
      access elsewhere in this file from being silently legalized.
      Phase 88 STATE-04 explicitly deletes `_TEST_BACKEND` and replaces it
      with proper fixture injection, making this allowlist entry
      self-eliminating. Migrating to safe_user_get/set/pop here would not
      match the function's contract (it returns the dict-like backend object
      itself, not a value-for-key). expected_count=1 + enclosing=_backend
      enforces strict scope.
```

After deletion the allowlist should contain exactly 3 entries (web/auth_state.py, web/main.py, web/supabase_client.py).

Schema-validation check: tests/test_no_raw_storage_access.py:test_allowlist_well_formed asserts `entries` is non-empty (line 200) — 3 entries satisfies this. test_allowlist_counts_exact iterates only over present entries — fewer entries means fewer items to check, still green.

No other allowlist edits in this task. The web/auth_state.py entry remains untouched (Phase 91 deletes it). The web/main.py entry remains untouched (Phase 91 deletes it). The web/supabase_client.py entry remains untouched (Phase 90 deletes it).
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_raw_storage_access.py -v --tb=short</automated>
  </verify>
  <acceptance_criteria>
    - `grep -n "web/export_state.py" .planning/phase87_storage_allowlist.yaml` returns 0 matches.
    - `grep -n "_TEST_BACKEND" .planning/phase87_storage_allowlist.yaml` returns 0 matches (the justification block referencing it is also gone).
    - `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml` returns 3 (auth_state, main, supabase_client).
    - `python -m pytest tests/test_no_raw_storage_access.py -v` exits 0 with all 5 tests passing (well_formed, lint_rejects_synthetic, lint_handles_aliased, lint_does_not_double_report, allowlist_counts_exact, no_raw_storage_access_outside_allowlist).
  </acceptance_criteria>
  <done>web/export_state.py allowlist entry deleted; Phase 87 lint scanner verifies the file has 0 raw accesses post-Task-1; 3 allowlist entries remain (all scoped to future-phase deletions).</done>
</task>

<task type="auto">
  <name>Task 7: Plan-boundary green verification (pytest + ruff + check_docs)</name>
  <files></files>
  <read_first>
    - .planning/phases/88-state-separation-by-deletion/88-CONTEXT.md (D-05: plan boundaries MUST stay green)
  </read_first>
  <action>
Run the full test/lint/docs verification trio. Fix any regressions surfaced before the plan can be considered complete.

Commands (each must exit 0):
1. `python -m pytest -q` (full suite — target: 1879+ tests passing, +at least 1 new test from D-15 source_text leak regression; expect ~1880 passed / 20 skipped).
2. `python -m ruff check .` (no new lint violations).
3. `python scripts/check_docs.py` (docs health check).

Cross-cutting verification (must all be true after Plan 88-02 lands):

- `grep -rn "_TEST_BACKEND" .` returns 0 matches (gone from production code AND tests AND .planning/).
- `grep -rn "_StateProxy" tests/` returns 0 matches.
- `grep -rn "from web.export_state import _backend" .` returns 0 matches.
- `grep -nE "state\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*=" tests/` returns 0 matches across the 4 rewritten test files (other tests outside the 4 are not in scope for Plan 88-02 — Plan 88-03 D-07 static scanner will catch them at Phase 88 close).
- AppState class shape unchanged: `grep -cE "^\\s+self\\.(last_results|current_search_query|current_search_mode|current_search_gap|last_filters_applied|last_search_warnings|last_selected_uids|parallels_results|parallels_filtered|parallels_search_meta)\\s*[:=]" web/state.py` returns 10 (Plan 88-03 deletes these).

If pytest surfaces a test failure outside the 4 export-specific tests and outside test_no_raw_storage_access, investigate — most likely cause is a missed reader site somewhere in web/ that was reading the old `_TEST_BACKEND` shim. Sanity-grep: `grep -rn "_TEST_BACKEND\\|export_state\\._backend" web/` MUST return 0 matches.
  </action>
  <verify>
    <automated>python -m pytest -q</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest -q` exits 0 with at least 1880 tests passing (Phase 87 baseline 1879 + at least 1 new D-15 test).
    - `python -m ruff check .` exits 0.
    - `python scripts/check_docs.py` exits 0.
    - `grep -rn "_TEST_BACKEND" .` returns 0 matches across the entire repo.
    - `grep -rn "_StateProxy" tests/` returns 0 matches.
    - `grep -rn "export_state._backend" .` returns 0 matches.
  </acceptance_criteria>
  <done>Plan 88-02 leaves the tree green: pytest at or above Phase 87 baseline +1 new test, ruff clean, check_docs clean. _TEST_BACKEND is gone from the entire repo. AppState fields physically still exist on the class (Plan 88-03 deletes).</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Test monkeypatch -> web.safe_storage.app | If a test monkeypatches the wrong import path (e.g., `web.export_state.app` which no longer exists after Task 1, or `nicegui.app` which the chokepoint doesn't read), the stub silently doesn't intercept and tests run against an unpatched backend. False-positive coverage. |
| Production reader (api.py) -> meta['source_text'] only | After D-14 deletion the legacy fallback is gone. If a writer in parallels.py somehow fails to populate meta['source_text'] on a code path Plan 88-01 missed, the export returns an empty source_text in the output document — silent feature regression, not security. |
| web/export_state.py -> web/safe_storage.py | The new sole dependency. If safe_storage helpers fail (prune-race AssertionError), the safe_user_set returns False internally — but the export_state setters return None regardless (D-10), preserving today's silent-failure contract for callers. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-88-02-01 | Information Disclosure | Test monkeypatch wrong target | mitigate | If a test does `monkeypatch.setattr('web.export_state.app', stub)` (wrong path — that module has no `.app` after Task 1) instead of `web.safe_storage.app`, the stub no-ops and the helper reads the real `app.storage.user` which is undefined in TestClient context — pytest fails loudly with `RuntimeError: app.storage.user can only be accessed within a request`. This is a fail-loud guard; the wrong-target mistake cannot silently pass. Acceptance criteria in Tasks 3-5 grep for `web.safe_storage.app` to catch the right pattern. |
| T-88-02-02 | Tampering | export_state ABI silent change | mitigate | D-10 explicitly wraps the safe_user_set boolean return so callers in search.py/search_results.py/parallels.py see the same `None` return as today. Task 1 acceptance criteria verifies `export_state.set_search_export([], 'q') is None`. If the wrap is broken, the unit test fails immediately. |
| T-88-02-03 | Information Disclosure | parallels_source_text legacy fallback | mitigate | D-15 regression test directly proves User A's source_text cannot leak into User B's response. The test puts `'alpha-leak-bait'` in User A's storage and asserts the string is absent from User B's response body. If the fallback is reintroduced (e.g., a future PR re-adds `safe_user_get('parallels_source_text', '')`), this test fails. |
| T-88-02-04 | Tampering | Poisoned-shape storage payload | mitigate | D-11 `isinstance(payload, dict)` guard in the 3 update_* functions defends against storage holding a non-dict value for `_SEARCH_KEY` or `_PARALLELS_KEY`. Without the guard, `payload['results'] = ...` would raise TypeError on, e.g., a corrupted None/list/str. With the guard, the function returns silently — same behavior as a missing payload (existing contract). |
| T-88-02-05 | Tampering | Shared-reference mutation race | accept | D-12 copy-on-update is defensive but not atomic. Two same-session requests interleaving read-modify-write on the same payload could still race because the read-modify-write is not under lock. Real atomicity requires CAS or a lock — explicitly deferred per CONTEXT.md "Deferred Ideas". For Phase 88 the copy-on-update narrows the window: each request gets its own copy of the dict, mutates it locally, then writes back. The race window is the time between read and write — narrow but nonzero. Accepted because Phase 88 is about state separation, not atomicity. |
| T-88-02-06 | Denial of Service | Allowlist count drift | mitigate | Task 6 deletes the `web/export_state.py` allowlist entry. test_allowlist_counts_exact enforces exact count match — if Task 1 fails to delete `_backend()` correctly and a `app.storage.user` raw access lingers, this test fails because the entry is gone but the access still exists. The acceptance criterion in Task 6 explicitly runs `test_no_raw_storage_access.py` to verify. |

**No HIGH-severity threats.** Plan 88-02 reduces attack surface by:
(a) Deleting the `_TEST_BACKEND` production-code shim — a test-only mechanism that lived in production code violating Phase 87 chokepoint discipline.
(b) Deleting the parallels_source_text reader-side fallback — a legacy fallback that read from a session-shared dict at a different key than the rest of the export state.

The mitigations (D-11 + D-12 hardening, D-15 leak test, Task 6 allowlist deletion) all reduce the attack surface from where Plan 88-01 left it.
</threat_model>

<verification>
1. All 7 tasks pass acceptance criteria.
2. Plan-boundary green (Task 7): full pytest at Phase 87 baseline + 1 new D-15 test, ruff clean, check_docs clean.
3. _TEST_BACKEND is gone from the entire repository (production + tests + .planning + docs).
4. _StateProxy is gone from tests/.
5. Phase 87 lint scanner still passes; web/export_state.py allowlist entry is deleted; 3 entries remain (auth_state, main, supabase_client — all scoped to Phase 90/91 deletion).
6. AppState class shape STILL has the 10 fields (Plan 88-03 deletes them). Tests no longer reference them in fixtures, so Plan 88-03's deletion will not regress any test.
7. parallels_source_text legacy fallback proven dead by D-15 regression test.
</verification>

<success_criteria>
- STATE-03 satisfied: reader sites in api.py (the 3 parallels export handlers + the previously-migrated search export handlers) all read exclusively through web.export_state helpers, which now route through safe_storage chokepoint. No fallback to legacy app.storage.user keys.
- STATE-04 satisfied: _TEST_BACKEND shim removed from web/export_state.py; tests use monkeypatch.setattr web.safe_storage.app fixture pattern.
- STATE-05 satisfied: tests/test_export_cross_user_isolation.py rewritten to assert against per-session storage directly (via monkeypatched stub), no _TEST_BACKEND reference.
- STATE-06 satisfied: tests/test_export_state_selection.py + tests/test_api_export_json.py + tests/test_api_legacy_unchanged.py rewritten, _StateProxy + state.* setup deleted, tests use only export_state helpers.
- Phase 87 invariants intact: lint scanner still passes; one fewer allowlist entry.
- Zero user-visible behavior change: export_state ABI unchanged; reader sites behave identically except they no longer fall back to the legacy parallels_source_text key (which Plan 88-01 already populated via meta dict on every writer path).
</success_criteria>

<output>
After completion, create `.planning/phases/88-state-separation-by-deletion/88-02-export-state-rewrite-SUMMARY.md` per @$HOME/.claude/get-shit-done/templates/summary.md.
</output>
