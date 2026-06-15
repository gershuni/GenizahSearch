# -*- coding: utf-8 -*-
"""Phase 113 Plan 02 — CRASH-04 payload key allowlist + frame-walk tests.

Covers: D-07 (frame-walk, no format_exception, no str(exc)), REVIEWS MEDIUM-9
(robust in-app frame classification by resolved source root, generic basenames
excluded), REVIEWS PASS2 (venv/site-packages frames classified as external).

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

from __future__ import annotations

import ast
import inspect
import os
import sys
import textwrap
import types

import pytest

import desktop.telemetry as tel


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# Helper: build a synthetic TracebackType from a real raise-in-helper pattern
# ---------------------------------------------------------------------------
def _make_tb_in_file(filepath: str, exc_type=ValueError) -> object:
    """Raise exc_type from a compiled code object whose co_filename == filepath.

    Returns the traceback object from sys.exc_info() after the raise.
    """
    # Compile a tiny snippet with the desired filename so co_filename is set
    code = compile(
        "raise exc_type('test')\n",
        filepath,
        'exec',
    )
    globs = {'exc_type': exc_type}
    try:
        exec(code, globs)  # noqa: S102
    except exc_type:
        return sys.exc_info()[2]
    return None


# ---------------------------------------------------------------------------
# CRASH-04 — payload key allowlist enforcement
# ---------------------------------------------------------------------------
def test_payload_keys_allowlisted():
    """CRASH-04: all _make_crash_props keys are in _ALLOWED_PROPS."""
    props = tel._make_crash_props(ValueError, None, is_background=False)
    for key in props:
        assert key in tel._ALLOWED_PROPS, f"Key {key!r} not in _ALLOWED_PROPS"


def test_payload_has_exactly_five_keys():
    """CRASH-04: _make_crash_props returns exactly the five specified keys."""
    props = tel._make_crash_props(ValueError, None, is_background=False)
    expected = {'exc_type', 'exc_module', 'exc_lineno', 'error_fingerprint', 'is_background_thread'}
    assert set(props.keys()) == expected, (
        f"Expected keys {expected}, got {set(props.keys())}"
    )


def test_no_forbidden_keys_in_payload():
    """CRASH-04: 'traceback_scrubbed', 'thread_name', 'message' never appear."""
    props = tel._make_crash_props(ValueError, None, is_background=False)
    assert 'traceback_scrubbed' not in props
    assert 'thread_name' not in props
    assert 'message' not in props


def test_allowlist_reconciled_traceback_scrubbed_removed():
    """CRASH-04 D-07: 'traceback_scrubbed' is NOT in _ALLOWED_PROPS (removed)."""
    assert 'traceback_scrubbed' not in tel._ALLOWED_PROPS, (
        "'traceback_scrubbed' must be removed from _ALLOWED_PROPS (D-07)"
    )


def test_allowlist_reconciled_thread_name_removed():
    """CRASH-04 D-07: 'thread_name' is NOT in _ALLOWED_PROPS (removed)."""
    assert 'thread_name' not in tel._ALLOWED_PROPS, (
        "'thread_name' must be removed from _ALLOWED_PROPS (D-07)"
    )


def test_allowlist_reconciled_new_keys_present():
    """CRASH-04 D-07: error_fingerprint, is_background_thread, fatal_error ARE in _ALLOWED_PROPS."""
    assert 'error_fingerprint' in tel._ALLOWED_PROPS, "'error_fingerprint' missing from _ALLOWED_PROPS"
    assert 'is_background_thread' in tel._ALLOWED_PROPS, "'is_background_thread' missing from _ALLOWED_PROPS"
    assert 'fatal_error' in tel._ALLOWED_PROPS, "'fatal_error' missing from _ALLOWED_PROPS"


# ---------------------------------------------------------------------------
# CRASH-04 D-07 — no path in crash props + in-app vs external
# ---------------------------------------------------------------------------
def test_no_path_in_crash_props():
    """CRASH-04: no file path leaks into crash props.

    Gets a real traceback from a file under the desktop/ source root.
    The exc_module should contain ONLY a basename (no directory separators).
    """
    # Get a real traceback from this very test file (or any in-app file)
    try:
        raise ValueError("test path leak check")
    except ValueError:
        tb = sys.exc_info()[2]

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    # exc_module must be a basename — no directory separators
    module_val = props['exc_module']
    assert os.sep not in module_val, (
        f"exc_module contains directory separator: {module_val!r}"
    )
    assert '/' not in module_val, (
        f"exc_module contains '/': {module_val!r}"
    )
    # Verify it's just a filename
    assert module_val == os.path.basename(module_val), (
        f"exc_module {module_val!r} is not a basename"
    )
    # error_fingerprint should also not contain directory separators (its format is type:module:lineno)
    fp = props['error_fingerprint']
    # The module portion in fingerprint should not contain os.sep or '/'
    parts = fp.split(':')
    assert len(parts) == 3, f"error_fingerprint has wrong format: {fp!r}"
    module_in_fp = parts[1]
    assert os.sep not in module_in_fp, (
        f"error_fingerprint module contains directory separator: {module_in_fp!r}"
    )


def test_external_module_fallback(tmp_path):
    """CRASH-04 D-07: a traceback from outside app source roots → error_module='external'."""
    # Create a temp file that is definitely outside the app roots
    ext_file = tmp_path / 'not_an_app_module.py'
    ext_file.write_text("raise ValueError('external')\n")

    tb = _make_tb_in_file(str(ext_file))
    assert tb is not None, "Failed to create external traceback"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'external', (
        f"Expected 'external' for non-app file, got {props['exc_module']!r}"
    )


def test_in_app_frame_gives_basename(tmp_path):
    """CRASH-04 D-07: a traceback from a desktop/ file → exc_module is the basename."""
    # Simulate an in-app frame by using a file under desktop/
    desktop_dir = os.path.dirname(os.path.abspath(tel.__file__))
    fake_app_file = os.path.join(desktop_dir, 'fake_app_module.py')

    tb = _make_tb_in_file(fake_app_file)
    if tb is None:
        pytest.skip("Could not create traceback from desktop/ path")

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    # Should be classified as in-app and give the basename
    assert props['exc_module'] == 'fake_app_module.py', (
        f"Expected 'fake_app_module.py' for in-app frame, got {props['exc_module']!r}"
    )
    assert props['exc_lineno'] > 0, "Expected non-zero lineno for in-app frame"


# ---------------------------------------------------------------------------
# REVIEWS MEDIUM-9 — generic basename exclusion
# ---------------------------------------------------------------------------
def test_generic_basename_not_in_app(tmp_path):
    """REVIEWS MEDIUM-9: __init__.py under an app root is NOT classified as in-app.

    A file named '__init__.py' sitting under desktop/ would be a generic name
    and must not be used as the in-app classifier (it appears in many packages).
    """
    # Create a fake __init__.py under the desktop/ directory to test exclusion
    desktop_dir = os.path.dirname(os.path.abspath(tel.__file__))
    init_file = os.path.join(desktop_dir, '__init__.py')

    tb = _make_tb_in_file(init_file)
    if tb is None:
        pytest.skip("Could not create traceback from __init__.py path")

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    # __init__.py must be excluded — even though it's under desktop/
    assert props['exc_module'] == 'external', (
        f"__init__.py under app root should be 'external', got {props['exc_module']!r}"
    )


def test_generic_basename_main_not_in_app(tmp_path):
    """REVIEWS MEDIUM-9: __main__.py under an app root is NOT classified as in-app."""
    desktop_dir = os.path.dirname(os.path.abspath(tel.__file__))
    main_file = os.path.join(desktop_dir, '__main__.py')

    tb = _make_tb_in_file(main_file)
    if tb is None:
        pytest.skip("Could not create traceback from __main__.py path")

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'external', (
        f"__main__.py under app root should be 'external', got {props['exc_module']!r}"
    )


# ---------------------------------------------------------------------------
# REVIEWS PASS2 — venv/site-packages frames classified as external
# ---------------------------------------------------------------------------
def test_venv_frame_external(tmp_path):
    """REVIEWS PASS2 / MEDIUM-9: a frame under venv/Lib/site-packages is 'external'.

    Even though such a path might sit under the repo root, the _EXCLUDED_PATH_SEGMENTS
    check must force it to 'external'.
    """
    # Construct a path that looks like it's under venv/Lib/site-packages
    # (tmp_path used so we don't need to create real files under repo root)
    venv_path = str(tmp_path / 'venv' / 'Lib' / 'site-packages' / 'pkg' / 'mod.py')

    tb = _make_tb_in_file(venv_path)
    if tb is None:
        pytest.skip("Could not create traceback from venv path")

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'external', (
        f"venv/Lib/site-packages frame should be 'external', got {props['exc_module']!r}"
    )


def test_site_packages_frame_external(tmp_path):
    """REVIEWS PASS2: a frame with 'site-packages' in its path is 'external'."""
    sp_path = str(tmp_path / 'site-packages' / 'somelib' / 'somemod.py')

    tb = _make_tb_in_file(sp_path)
    if tb is None:
        pytest.skip("Could not create traceback from site-packages path")

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'external', (
        f"site-packages frame should be 'external', got {props['exc_module']!r}"
    )


# ---------------------------------------------------------------------------
# CRASH-04 — static: no str(exc) and no format_exception in emit/make_crash paths
# ---------------------------------------------------------------------------
def test_no_str_exc_in_emit_crash():
    """CRASH-04: str(exc_value) is never read in _emit_crash_direct or _make_crash_props.

    Static/AST check — parses the source of both functions and verifies that
    neither calls str() on the exception value, nor calls format_exception.
    """
    for func_name, func in [
        ('_emit_crash_direct', tel._emit_crash_direct),
        ('_make_crash_props', tel._make_crash_props),
    ]:
        src = inspect.getsource(func)
        tree = ast.parse(src)
        func_def = tree.body[0]
        # Strip leading docstring
        body = func_def.body
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            body = body[1:]
        code_module = ast.Module(body=list(body), type_ignores=[])
        code_text = ast.unparse(code_module)

        assert 'format_exception' not in code_text, (
            f"{func_name} contains 'format_exception' — "
            "full traceback string must never be materialized (D-07)"
        )
        # 'str(exc' or 'str(e' patterns — check for str( applied to exc variable
        # We check that 'str(' is not followed by patterns that look like exception variables.
        # The simplest check: assert 'str(exc' not in code_text (the exc_type and exc_tb
        # params are exc_type and exc_tb; the exc value is never passed to these functions).
        assert 'str(exc' not in code_text, (
            f"{func_name} calls str(exc...) — exception message must never be read (D-07)"
        )
