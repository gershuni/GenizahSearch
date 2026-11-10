"""Phase 91 AUTHW-06 -- retention guard for filter_panel.py:persist_value
safe-wrap (originally landed in commit cca23db3 / 2026-05-12 Codex 3rd-pass
CRITICAL fix). Prevents a future refactor from un-doing the safe-wrap.

Per Codex M5: grep is enough for the *raw* app.storage.user lint (which
tests/test_no_raw_storage_access.py covers at file scope). persist_value's
contract is *function-local* -- the function body must (a) import from
web.safe_storage, (b) gate on safe_user_get('session_persistence_enabled',
True), AND (c) write via safe_user_set(key, value). AST walking gives
function-body precision without false positives from comments/strings.

Test count (6 total):
  3 production-code AST assertions:
    T-1: test_persist_value_imports_safe_storage_helpers (imports check)
    T-2: test_persist_value_reads_persistence_flag (flag-read check)
    T-3: test_persist_value_writes_via_safe_user_set (STRICT write check
         per Revision SHOULD-6 -- verifies args[0]/args[1] are Name refs
         to persist_value's own parameters, not arbitrary string constants)
  1 BEHAVIORAL test (Revision MUST-5 / Codex MEDIUM catch in 91-REVIEWS.md):
    T-Beh: test_persist_value_respects_session_persistence_flag
           (monkeypatches safe_storage backend; asserts False suppresses
           write and True allows it)
  2 seed-trap snippet sanity tests:
    T-Trap-Pass: passing snippet exercises all 3 AST positives
    T-Trap-Fail: failing snippet trips raw-subscript check

WHY BOTH AST + BEHAVIORAL (Revision MUST-5):
  AST assertions are SHAPE-ONLY. A regression could pass them while
  behavior is broken -- e.g., persist_value could call
  safe_user_get('session_persistence_enabled', True) and IGNORE the
  result, then call safe_user_set(key, value) unconditionally. All 3 AST
  assertions would pass; the safe-wrap would be functionally undone.
  T-Beh closes this gap by EXERCISING the function and verifying the
  conditional actually conditions.

WHY STRICT AST args CHECK (Revision SHOULD-6):
  Without strict args check, a regression could pass T-3 by inserting
  ANY safe_user_set call -- e.g., `safe_user_set('unrelated_key', None)`
  followed by raw `app.storage.user[k] = v`. The strict check requires
  the call match `safe_user_set(<first_param>, <second_param>)`
  specifically, so the AST positive assertion is tied to the function's
  actual contract.

IMPORT-ALIASING IS INTENTIONALLY UNHANDLED:
  If a future refactor does `from web.safe_storage import safe_user_set
  as safe_set`, the AST test FAILS by design -- not because the code is
  wrong, but because the refactor would need to also update this test.
  Documented per Gemini concern in 91-REVIEWS.md. The Phase 87 file-scope
  scanner (tests/test_no_raw_storage_access.py) handles aliased imports
  for the raw-access lint; this test deliberately scopes to the literal
  name 'safe_user_set' for tight contract enforcement.

Mirrors Phase 88 D-07 (test_no_deleted_state_references.py) seed-trap idiom
and Phase 90 D-15 (test_no_set_session_outside_oauth.py) AST-Call shape.

Test discipline (Phase 89 D-09 / Phase 90 D-13; NEW-M3 round-2 cross-AI catch):
this is a STRICT single-test-file atomic commit. No production code touched.
No documentation touched. Closeout docs updates (STATE.md / ROADMAP.md /
CLAUDE.md / OPEN_ISSUES.md flips to "Phase 91 Complete") live in Plan 91-03,
not here. If this test produces a false positive in some future refactor,
revert this commit in isolation without affecting Plan 91-01's migration
commit or Plan 91-03's closeout commit.
"""
import ast
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_FILE = REPO_ROOT / 'web' / 'components' / 'filter_panel.py'


def _find_function_def(tree, name):
    """Return the first ast.FunctionDef whose .name == `name`, or None.

    Mirrors Phase 88's _scan_file pattern (test_no_deleted_state_references.py:
    151-155) but scoped to one named function instead of file-wide walk.
    Handles both sync (FunctionDef) and async (AsyncFunctionDef) variants
    even though persist_value is sync today -- defensive for future refactor.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _is_app_storage_user_subscript(target):
    """Return True iff `target` is an ast.Subscript matching `<app>.storage.user[...]`.

    Used to detect raw `app.storage.user[k] = v` writes inside persist_value.
    Handles any binding name for app (app, nicegui_app, _app) by NOT checking
    the leaf Name -- function-local imports inside persist_value would use
    `app` by convention, but the body could in theory bind to a different name.
    The check walks the Attribute chain inside-out: target.value must be
    Attribute(attr='user'), and its .value must be Attribute(attr='storage').
    """
    if not isinstance(target, ast.Subscript):
        return False
    val = target.value
    if not isinstance(val, ast.Attribute) or val.attr != 'user':
        return False
    if not isinstance(val.value, ast.Attribute) or val.value.attr != 'storage':
        return False
    return True


def _get_param_names(fn):
    """Return the tuple of parameter names from a FunctionDef.

    For `def persist_value(key, value):` returns ('key', 'value').
    Used by the STRICT safe_user_set args check (Revision SHOULD-6).
    """
    return tuple(arg.arg for arg in fn.args.args)


# ===========================================================================
# Production-code AST assertions (3) -- D-09 core contract
# ===========================================================================

def test_persist_value_imports_safe_storage_helpers():
    """AUTHW-06 T-1: persist_value must import safe_user_get + safe_user_set."""
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "persist_value() function disappeared from filter_panel.py"
    # Walk function body for ImportFrom from web.safe_storage.
    imports = [n for n in ast.walk(fn)
               if isinstance(n, ast.ImportFrom) and n.module == 'web.safe_storage']
    assert imports, (
        "persist_value() must import from web.safe_storage "
        "(AUTHW-06 retention -- commit cca23db3 safe-wrap)"
    )
    imported_names = {alias.name for imp in imports for alias in imp.names}
    assert 'safe_user_get' in imported_names, (
        f"persist_value() must import safe_user_get; imports were: {imported_names}"
    )
    assert 'safe_user_set' in imported_names, (
        f"persist_value() must import safe_user_set; imports were: {imported_names}"
    )


def test_persist_value_reads_persistence_flag():
    """AUTHW-06 T-2: persist_value must gate on session_persistence_enabled.

    The original safe-wrap (commit cca23db3) had two parts: (a) route through
    safe_storage helpers, (b) check session_persistence_enabled before writing.
    Part (b) is what makes the function persist OPT-IN -- removing it would
    silently turn every page on persistent state writes, breaking user
    expectations about session lifetime. AST scan ensures part (b) survives.
    """
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "persist_value() function disappeared from filter_panel.py"
    # Find Call to safe_user_get with first positional arg literal 'session_persistence_enabled'.
    calls = [n for n in ast.walk(fn)
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_get'
             and n.args
             and isinstance(n.args[0], ast.Constant)
             and n.args[0].value == 'session_persistence_enabled']
    assert calls, (
        "persist_value() must read 'session_persistence_enabled' "
        "via safe_user_get to gate persistence (AUTHW-06 retention)"
    )


def test_persist_value_writes_via_safe_user_set():
    """AUTHW-06 T-3 (STRICT per Revision SHOULD-6): persist_value must NOT use raw
    app.storage.user[...] = ... AND must call safe_user_set(<first_param>, <second_param>)
    -- not just any safe_user_set call. The STRICT args check forecloses the
    Codex-flagged edge case where someone could trick the test with
    `safe_user_set('unrelated_key', None)` plus a raw write.
    """
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "persist_value() function disappeared from filter_panel.py"
    # Negative: NO raw app.storage.user[...] = ... subscript writes.
    raw_writes = [n for n in ast.walk(fn)
                  if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert not raw_writes, (
        "persist_value() reintroduced raw app.storage.user[...] = ...; "
        "must use safe_user_set (AUTHW-06 retention -- commit cca23db3)"
    )
    # Positive STRICT (Revision SHOULD-6): safe_user_set called with the
    # function's first parameter as args[0] AND second parameter as args[1].
    param_names = _get_param_names(fn)
    assert len(param_names) >= 2, (
        f"persist_value() must take at least 2 positional params (key, value); "
        f"got: {param_names}. If the signature changed, this test needs updating."
    )
    first_param, second_param = param_names[0], param_names[1]
    matching_calls = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set'
        and len(n.args) >= 2
        and isinstance(n.args[0], ast.Name) and n.args[0].id == first_param
        and isinstance(n.args[1], ast.Name) and n.args[1].id == second_param
    ]
    assert matching_calls, (
        f"persist_value() must call safe_user_set({first_param}, {second_param}) -- "
        f"passing its own parameters through. Revision SHOULD-6 STRICT check: "
        f"safe_user_set with arbitrary string constants or wrong arg ordering "
        f"does NOT satisfy the AUTHW-06 contract."
    )
    # Documentation hint: import-aliasing (e.g., `safe_user_set as safe_set`)
    # is intentionally NOT handled. A refactor changing the imported alias
    # must also update this test. See module docstring for rationale.


# ===========================================================================
# Behavioral test (Revision MUST-5 -- Codex MEDIUM catch)
# ===========================================================================

def test_persist_value_respects_session_persistence_flag(monkeypatch):
    """AUTHW-06 T-Beh (Revision MUST-5): behavioral test that closes the
    AST shape-only gap. Calls the LIVE persist_value with monkeypatched
    safe_storage backend; verifies that session_persistence_enabled=False
    suppresses the write while True allows it.

    Without this test, a regression could pass T-1/T-2/T-3 (correct shape)
    while breaking the actual contract -- e.g., persist_value could call
    safe_user_get('session_persistence_enabled', True) and ignore the
    result, then unconditionally call safe_user_set(key, value). The AST
    assertions would not notice; T-Beh would.

    Uses the Phase 87 B3 monkeypatch idiom: setattr the bound name
    'web.safe_storage.app' to a SimpleNamespace whose storage.user is
    a plain dict pre-populated with the flag value. safe_user_get and
    safe_user_set both go through that dict via app.storage.user.get/[__setitem__].
    """
    from web.components.filter_panel import persist_value

    # --- Case 1: flag=False should suppress the write ------------------
    storage_false = {'session_persistence_enabled': False}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage_false)),
    )
    persist_value('test_key_off', 'test_value_off')
    assert 'test_key_off' not in storage_false, (
        f"persist_value wrote 'test_key_off' despite session_persistence_enabled=False; "
        f"AUTHW-06 contract violated. Final storage: {storage_false}"
    )

    # --- Case 2: flag=True should allow the write ----------------------
    storage_true = {'session_persistence_enabled': True}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage_true)),
    )
    persist_value('test_key_on', 'test_value_on')
    assert storage_true.get('test_key_on') == 'test_value_on', (
        f"persist_value did NOT write 'test_key_on' despite "
        f"session_persistence_enabled=True; AUTHW-06 contract violated. "
        f"Final storage: {storage_true}"
    )


# ===========================================================================
# Seed-trap snippets (2) -- sanity for the AST scanner logic itself
# ===========================================================================

# These snippets live as parsed in-test strings (Phase 88 D-07 idiom -- see
# tests/test_no_deleted_state_references.py:158-197 for the same pattern).
# Phase 90 D-15 uses the same pattern in tests/test_no_set_session_outside_oauth.py.

_PASSING_SNIPPET = (
    "def persist_value(key, value):\n"
    "    from web.safe_storage import safe_user_get, safe_user_set\n"
    "    if safe_user_get('session_persistence_enabled', True):\n"
    "        safe_user_set(key, value)\n"
)

_FAILING_SNIPPET = (
    "def persist_value(key, value):\n"
    "    from nicegui import app\n"
    "    app.storage.user[key] = value\n"
)


def test_seed_trap_passing_snippet_passes_all_three_ast_checks():
    """Sanity: the canonical correct shape passes all 3 AST assertions.

    Without this sanity, an over-eager production assertion could silently
    pass against the production file but be wrong on edge cases. Exercising
    the same checker logic against a known-good snippet validates the logic
    -- including the Revision SHOULD-6 STRICT args check.
    """
    tree = ast.parse(_PASSING_SNIPPET)
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "snippet didn't define persist_value (test setup bug)"
    # Import check (mirrors T-1 logic).
    imports = [n for n in ast.walk(fn)
               if isinstance(n, ast.ImportFrom) and n.module == 'web.safe_storage']
    imported_names = {alias.name for imp in imports for alias in imp.names}
    assert 'safe_user_get' in imported_names
    assert 'safe_user_set' in imported_names
    # Flag-read check (mirrors T-2 logic).
    flag_calls = [n for n in ast.walk(fn)
                  if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_get'
                  and n.args
                  and isinstance(n.args[0], ast.Constant)
                  and n.args[0].value == 'session_persistence_enabled']
    assert flag_calls
    # Write check STRICT (mirrors T-3 logic per Revision SHOULD-6).
    raw_writes = [n for n in ast.walk(fn)
                  if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert not raw_writes
    param_names = _get_param_names(fn)
    first_param, second_param = param_names[0], param_names[1]
    matching_calls = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set'
        and len(n.args) >= 2
        and isinstance(n.args[0], ast.Name) and n.args[0].id == first_param
        and isinstance(n.args[1], ast.Name) and n.args[1].id == second_param
    ]
    assert matching_calls, (
        "Seed-trap passing snippet should match the STRICT safe_user_set check"
    )


def test_seed_trap_failing_snippet_fails_raw_subscript_check():
    """Sanity: a deliberately-bad shape is flagged by the raw-subscript check.

    The failing snippet uses raw `app.storage.user[k] = v` -- this MUST
    trip test_persist_value_writes_via_safe_user_set. If the scanner has
    a false-negative gap (e.g., the _is_app_storage_user_subscript helper
    has a regression), this test catches it.
    """
    tree = ast.parse(_FAILING_SNIPPET)
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "snippet didn't define persist_value (test setup bug)"
    # The failing snippet must trip the raw-subscript check.
    raw_writes = [n for n in ast.walk(fn)
                  if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert raw_writes, (
        "Seed-trap failing snippet was NOT flagged by _is_app_storage_user_subscript; "
        "scanner has a false-negative regression."
    )
    # AND the failing snippet does NOT have a safe_user_set call (positive check
    # would also trip).
    safe_set_calls = [n for n in ast.walk(fn)
                      if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set']
    assert not safe_set_calls, (
        "Seed-trap failing snippet has BOTH raw write AND safe_user_set "
        "call -- that's not a clean failing case."
    )
