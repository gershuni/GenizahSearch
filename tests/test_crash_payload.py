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
# Frozen-build regression — PyInstaller bakes the BUILD host's source path into
# co_filename. On the end-user's machine that path is neither extractable via
# realpath nor under this module's runtime __file__, so the OLD realpath-equality
# classifier returned 'external' for EVERY shipped crash (module + real line
# lost). Segment matching must classify these as in-app. Each path below is
# deliberately one that does NOT exist on the test machine and is NOT under the
# real desktop/ or shared/ dirs.
# ---------------------------------------------------------------------------
def test_frozen_build_path_package_module_in_app():
    """A build-host absolute path carrying '/desktop/' → in-app basename, not 'external'."""
    frozen_path = '/__frozen_build_host__/work/genizahsearch/desktop/puzzle.py'
    tb = _make_tb_in_file(frozen_path)
    assert tb is not None, "Failed to create traceback from frozen build path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'puzzle.py', (
        f"frozen-build /desktop/ frame should be 'puzzle.py', got {props['exc_module']!r}"
    )
    assert props['exc_lineno'] > 0, "Expected non-zero lineno for in-app frame"


def test_frozen_build_path_shared_package_in_app():
    """A build-host absolute path carrying '/shared/' → in-app basename, not 'external'."""
    frozen_path = 'C:\\ci\\build\\genizahsearch\\shared\\puzzle_service.py'
    tb = _make_tb_in_file(frozen_path)
    assert tb is not None, "Failed to create traceback from frozen build path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'puzzle_service.py', (
        f"frozen-build /shared/ frame should be 'puzzle_service.py', got {props['exc_module']!r}"
    )


def test_frozen_bundle_relative_package_module_in_app():
    """A bundle-relative co_filename ('desktop/...') → in-app basename, not 'external'."""
    tb = _make_tb_in_file('desktop/my_library_tab.py')
    assert tb is not None, "Failed to create traceback from bundle-relative path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'my_library_tab.py', (
        f"bundle-relative desktop/ frame should be 'my_library_tab.py', got {props['exc_module']!r}"
    )


def test_frozen_build_path_top_level_module_in_app():
    """A build-host path whose basename is a top-level app module → in-app, not 'external'."""
    frozen_path = '/__frozen_build_host__/work/genizahsearch/genizah_app.py'
    tb = _make_tb_in_file(frozen_path)
    assert tb is not None, "Failed to create traceback from frozen build path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'genizah_app.py', (
        f"frozen-build top-level frame should be 'genizah_app.py', got {props['exc_module']!r}"
    )


def test_third_party_shared_under_site_packages_stays_external(tmp_path):
    """False-positive guard: a 3rd-party package literally named 'shared' under
    site-packages must stay 'external' — the exclusion check must win over the
    '/shared/' package-segment match (ordering invariant)."""
    sp_path = str(tmp_path / 'lib' / 'site-packages' / 'shared' / 'thing.py')
    tb = _make_tb_in_file(sp_path)
    assert tb is not None, "Failed to create traceback from site-packages/shared path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'external', (
        f"site-packages/shared frame should be 'external', got {props['exc_module']!r}"
    )


def test_frozen_root_level_module_in_app():
    """A build-host path to a root-level app module (no '/desktop/' or '/shared/'
    segment) is classified in-app via its _APP_SOURCE_FILES basename."""
    frozen_path = '/__frozen_build_host__/work/genizahsearch/corrections_ui.py'
    tb = _make_tb_in_file(frozen_path)
    assert tb is not None, "Failed to create traceback from frozen root-level path"

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    assert props['exc_module'] == 'corrections_ui.py', (
        f"root-level app module should be 'corrections_ui.py', got {props['exc_module']!r}"
    )


# ---------------------------------------------------------------------------
# D-07 cross-platform basename — a frozen build's co_filename carries the BUILD
# host's separators (Windows '\\'), but the classifier may run on a POSIX host
# (Linux CI / dev). os.path.basename is host-dependent and would NOT split a
# Windows path on POSIX, leaking the FULL path into exc_module + the fingerprint.
# _frame_basename must normalize separators itself so only a basename ever leaves
# the process regardless of host OS.
# ---------------------------------------------------------------------------
def test_windows_style_inapp_path_yields_clean_basename():
    """A Windows '\\'-separated in-app co_filename → bare basename, no separators,
    on ANY host (regression: os.path.basename leaks the whole path on POSIX)."""
    win_path = 'C:\\ci\\build\\genizahsearch\\desktop\\puzzle.py'
    tb = _make_tb_in_file(win_path)
    assert tb is not None

    props = tel._make_crash_props(ValueError, tb, is_background=False)
    module_val = props['exc_module']
    assert module_val == 'puzzle.py', (
        f"Windows-style in-app path should reduce to 'puzzle.py', got {module_val!r}"
    )
    # D-07: no path separator of either flavour may survive into exc_module...
    assert '/' not in module_val and '\\' not in module_val, (
        f"exc_module leaked a separator: {module_val!r}"
    )
    # ...nor into the module component of the fingerprint.
    module_in_fp = props['error_fingerprint'].split(':')[1]
    assert '/' not in module_in_fp and '\\' not in module_in_fp, (
        f"fingerprint module component leaked a separator: {module_in_fp!r}"
    )


def test_emit_crash_direct_windows_path_no_separator_leak(monkeypatch):
    """Full _emit_crash_direct path: a Windows-style in-app frame must reach
    send_crash_event_direct with a separator-free exc_module + fingerprint
    (post-scrub), on any host OS (D-07)."""
    win_path = 'C:\\ci\\build\\genizahsearch\\shared\\puzzle_service.py'
    tb = _make_tb_in_file(win_path)
    assert tb is not None

    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'crash-test-id')
    captured: dict = {}

    def _fake_send(event, props, distinct_id=None, **kwargs):
        captured['props'] = dict(props)

    monkeypatch.setattr(tel, 'send_crash_event_direct', _fake_send)
    tel._emit_crash_direct(ValueError, tb, is_background=False)

    assert captured, "send_crash_event_direct was never called"
    module_val = captured['props'].get('exc_module', '')
    fp = captured['props'].get('error_fingerprint', '')
    assert module_val == 'puzzle_service.py', f"exc_module wrong/leaked: {module_val!r}"
    assert '/' not in module_val and '\\' not in module_val, f"exc_module leaked sep: {module_val!r}"
    assert '/' not in fp and '\\' not in fp, f"fingerprint leaked a path separator: {fp!r}"


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


# ---------------------------------------------------------------------------
# CR-01 regression — full _emit_crash_direct path must NOT scrub the trusted
# crash keys (exc_module / error_fingerprint).
#
# The generic value scrubber's bare-filename branch (\S+\.[A-Za-z]\w{0,7}\b)
# matches any "*.py" basename and would redact it to [REDACTED], collapsing
# every in-app crash to fingerprint "[REDACTED]:<lineno>" and destroying crash
# grouping (D-07 — the phase deliverable). Every other payload assertion checks
# _make_crash_props OUTPUT (pre-scrub); this test captures what actually reaches
# send_crash_event_direct (post-scrub), which is where the defect lived.
# ---------------------------------------------------------------------------
def test_emit_crash_direct_preserves_inapp_module_and_fingerprint(monkeypatch):
    """CR-01: scrubbed crash payload keeps the in-app basename + fingerprint intact."""
    desktop_dir = os.path.dirname(os.path.abspath(tel.__file__))
    fake_app_file = os.path.join(desktop_dir, 'fake_app_module.py')

    tb = _make_tb_in_file(fake_app_file)
    if tb is None:
        pytest.skip("Could not create traceback from desktop/ path")

    # Enable the lock-free crash path and give it a distinct id.
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'crash-test-id')

    captured: dict = {}

    def _fake_send(event, props, distinct_id=None, **kwargs):
        captured['event'] = event
        captured['props'] = dict(props)
        captured['distinct_id'] = distinct_id

    monkeypatch.setattr(tel, 'send_crash_event_direct', _fake_send)

    tel._emit_crash_direct(ValueError, tb, is_background=False)

    assert captured, "send_crash_event_direct was never called"
    props = captured['props']
    # Pre-fix, the scrubber turned these into '[REDACTED]' / '[REDACTED]:<lineno>'.
    assert props.get('exc_module') == 'fake_app_module.py', (
        f"in-app exc_module was scrubbed away: {props.get('exc_module')!r}"
    )
    fp = props.get('error_fingerprint', '')
    assert fp.startswith('ValueError:fake_app_module.py:'), (
        f"error_fingerprint was scrubbed: {fp!r}"
    )
    assert '[REDACTED]' not in fp, f"fingerprint still contains [REDACTED]: {fp!r}"
