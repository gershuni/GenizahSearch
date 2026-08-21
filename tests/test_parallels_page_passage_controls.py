# -*- coding: utf-8 -*-
"""Codex review finding #13(c): while passage matching is selected, the GUI
must disable chunk_size/mode_select/freq_threshold (in addition to the
pre-existing boundary_mode disable) -- web/search_api.py rejects a
non-default value of any of them with 400 'passage_option_unsupported' when
method='passage', so the UI must never be able to send one.

Source-text assertion, not a live NiceGUI render test: this repo has no
render-smoke harness for web/pages/parallels.py (create_parallels_page is
never imported by any existing test -- confirmed by grep before writing
this file), and building one from scratch is out of scope for this fix.
This mirrors the project's existing "source_text_assertions_pin_
misspellings" pattern (e.g. tests/test_discovery_flag.py's substring checks
against web/main.py's source) -- a lighter-weight but real guard against a
regression in the handler's own code, extracted and inspected at the AST
level rather than by import (create_parallels_page has heavy NiceGUI/page
side effects unsuited to a unit import).
"""
from __future__ import annotations

import ast
import os
import re

PARALLELS_PAGE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'web', 'pages', 'parallels.py',
)


def _read_source() -> str:
    with open(PARALLELS_PAGE_PATH, encoding='utf-8') as fh:
        return fh.read()


def _on_passage_mode_change_source() -> str:
    """Extract on_passage_mode_change's own source text via the AST (not a
    regex over lines), so nested defs/indentation changes elsewhere in the
    file cannot silently widen or narrow what this test inspects."""
    tree = ast.parse(_read_source())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'on_passage_mode_change':
            return ast.get_source_segment(_read_source(), node) or ''
    raise AssertionError('on_passage_mode_change not found in web/pages/parallels.py')


def test_on_passage_mode_change_exists_exactly_once():
    tree = ast.parse(_read_source())
    matches = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'on_passage_mode_change'
    ]
    assert len(matches) == 1, 'expected exactly one on_passage_mode_change definition'


def test_passage_mode_forces_and_disables_chunk_size():
    src = _on_passage_mode_change_source()
    assert re.search(r"chunk_size\.value\s*=\s*5\b", src), (
        "on_passage_mode_change must force chunk_size to the API default (5) "
        "-- web/search_api.py rejects any other value for method='passage'")
    assert 'chunk_size.disable()' in src, (
        'chunk_size must be disabled while passage mode is selected')
    assert 'chunk_size.enable()' in src, (
        'chunk_size must be re-enabled when passage mode is deselected')


def test_passage_mode_forces_and_disables_mode_select():
    src = _on_passage_mode_change_source()
    assert re.search(r"mode_select\.value\s*=\s*'exact'", src), (
        "on_passage_mode_change must force mode_select to the API default "
        "('exact') -- web/search_api.py rejects any other value for "
        "method='passage'")
    assert 'mode_select.disable()' in src, (
        'mode_select must be disabled while passage mode is selected')
    assert 'mode_select.enable()' in src, (
        'mode_select must be re-enabled when passage mode is deselected')
    # Forcing mode_select back to 'exact' must also hide the variant-only
    # controls if 'variants' was previously selected -- on_mode_change is
    # the SAME handler mode_select's own change event calls (a programmatic
    # .value assignment does not fire NiceGUI's update:model-value event,
    # so it must be invoked explicitly; the same lesson boundary_mode /
    # passage_mode's own mutual-exclusivity handling already applies).
    assert 'on_mode_change()' in src, (
        'forcing mode_select to exact must also call on_mode_change() to '
        'hide variant_controls_col if it was showing')


def test_passage_mode_forces_and_disables_freq_threshold():
    src = _on_passage_mode_change_source()
    assert re.search(r"freq_threshold\.value\s*=\s*50\b", src), (
        "on_passage_mode_change must force freq_threshold to the page's "
        "default (50) -- passage has no per-chunk frequency signal at all")
    assert 'freq_threshold.disable()' in src, (
        'freq_threshold must be disabled while passage mode is selected')
    assert 'freq_threshold.enable()' in src, (
        'freq_threshold must be re-enabled when passage mode is deselected')


def test_passage_mode_still_forces_and_disables_boundary_mode():
    """Regression guard for the PRE-EXISTING fix (adversarial review finding
    #2) that finding #13(c) sits alongside -- must not have been lost in the
    same edit."""
    src = _on_passage_mode_change_source()
    assert re.search(r"boundary_mode\.value\s*=\s*'full'", src)
    assert 'boundary_mode.disable()' in src
    assert 'boundary_mode.enable()' in src


def test_disable_calls_are_inside_the_passage_mode_true_branch():
    """The disable() calls must be gated on `if passage_mode.value:`, not
    unconditional -- an unconditional disable would lock these controls
    even when passage mode is off."""
    src = _on_passage_mode_change_source()
    lines = src.splitlines()
    if_true_idx = next(
        i for i, ln in enumerate(lines) if re.search(r'if passage_mode\.value\s*:', ln)
    )
    else_idx = next(i for i, ln in enumerate(lines) if ln.strip() == 'else:')
    assert if_true_idx < else_idx, 'expected an if passage_mode.value: ... else: shape'
    true_branch = '\n'.join(lines[if_true_idx:else_idx])
    false_branch = '\n'.join(lines[else_idx:])
    for widget in ('chunk_size', 'mode_select', 'freq_threshold', 'boundary_mode'):
        assert f'{widget}.disable()' in true_branch, (
            f'{widget}.disable() must be inside the passage_mode.value branch')
        assert f'{widget}.enable()' in false_branch, (
            f'{widget}.enable() must be inside the else (not-selected) branch')


# ---------------------------------------------------------------------------
# Codex review finding #15: the page must route passage searches through
# the shared execution budget (run_passage_search), never NiceGUI's
# generic, unbounded run.io_bound.
# ---------------------------------------------------------------------------

def test_page_imports_run_passage_search_from_search_api():
    src = _read_source()
    assert 'from web.search_api import run_passage_search' in src, (
        'web/pages/parallels.py must import the SAME run_passage_search '
        'web/search_api.py exposes -- not a separately reimplemented copy')


def test_passage_branch_calls_run_passage_search_not_io_bound():
    """The passage dispatch must go through run_passage_search (which
    itself routes through the shared semaphore + dedicated executor +
    timeout), never NiceGUI's generic run.io_bound -- the actual bug this
    finding fixes."""
    tree = ast.parse(_read_source())
    execute_parallels = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_parallels'
    )
    src = ast.get_source_segment(_read_source(), execute_parallels) or ''

    # Split on the captured_passage_mode dispatch branch.
    branch_idx = src.index('if captured_passage_mode:')
    passage_branch_and_after = src[branch_idx:]
    else_idx = passage_branch_and_after.index('\n        else:')
    passage_branch = passage_branch_and_after[:else_idx]
    chunk_lab_branch = passage_branch_and_after[else_idx:]

    assert 'run_passage_search(' in passage_branch, (
        'the passage_mode branch must call run_passage_search')
    # 'run.io_bound' alone would also match this branch's own explanatory
    # comment (which names it to say why it is NOT used) -- check for an
    # actual CALL (the opening paren) instead of the bare substring.
    assert 'run.io_bound(' not in passage_branch, (
        'the passage_mode branch must NOT dispatch through run.io_bound -- '
        'that bypasses the shared semaphore/executor/timeout entirely')
    # The lab/chunk branch is UNCHANGED by this finding -- still run.io_bound.
    assert 'run.io_bound(run_search)' in chunk_lab_branch


def test_passage_branch_handles_busy_and_timeout_with_translated_messages():
    """Codex review finding #15 explicitly requires 'a translated busy/
    timeout message in the UI' -- not a bare exception, and not English-only
    (tr() is this codebase's i18n mechanism, used throughout this page)."""
    tree = ast.parse(_read_source())
    execute_parallels = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_parallels'
    )
    src = ast.get_source_segment(_read_source(), execute_parallels) or ''
    branch_idx = src.index('if captured_passage_mode:')
    else_idx = src.index('\n        else:', branch_idx)
    passage_branch = src[branch_idx:else_idx]

    assert 'passage_search_busy' in passage_branch
    assert 'core_timeout' in passage_branch
    # Every ui.notify string literal in this branch must be wrapped in tr().
    notify_calls = re.findall(r"ui\.notify\(\s*(tr\(|[\"'])", passage_branch)
    assert notify_calls, 'expected at least one ui.notify call in the passage branch'
    assert all(call == 'tr(' for call in notify_calls), (
        f'every ui.notify message in the passage branch must go through '
        f'tr() -- found raw-string call(s): {notify_calls}')
