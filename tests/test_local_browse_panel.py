# -*- coding: utf-8 -*-
"""Phase 95 Category 3 — Browse panel support for LOCAL files.

Smoke-level test (NOT a full GUI test) asserting that:
  1. genizah_app.GenizahGUI._open_local_browse exists and has the right shape.
  2. genizah_app.GenizahGUI._get_local_full_text_for_sys_id exists.
  3. The text-source helper aggregates LOCAL pages from the side-index.
  4. ResultDialog has the "View in Browse" button + _rd_open_in_browse handler.
"""
from __future__ import annotations

import ast
import os

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path: str) -> str:
    src_path = os.path.join(REPO_ROOT, rel_path)
    if not os.path.exists(src_path):
        pytest.skip(f"{rel_path} not found")
    with open(src_path, "r", encoding="utf-8") as f:
        return f.read()


def _find_function(source: str, fn_name: str) -> ast.FunctionDef | None:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == fn_name:
            return node
    return None


# ---------------------------------------------------------------------------
# genizah_app side
# ---------------------------------------------------------------------------

def test_open_local_browse_defined():
    """genizah_app.GenizahGUI._open_local_browse must exist."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_open_local_browse")
    assert fn is not None, "_open_local_browse function must exist"


def test_open_local_browse_renders_text():
    """_open_local_browse must call apply_line_numbered_text (the gutter helper
    that actually paints text into the Browse text widget). Previously the
    method called browse_load which does NOT know how to fetch LOCAL text."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_open_local_browse")
    assert fn is not None
    fn_src = ast.get_source_segment(src, fn) or ""
    assert "apply_line_numbered_text" in fn_src, (
        "Category 3: _open_local_browse must render text via "
        "apply_line_numbered_text — the previous stub called browse_load "
        "which does not handle LOCAL text"
    )
    # Must NOT go through browse_load (which is Genizah-only).
    assert "self.browse_load()" not in fn_src, (
        "Category 3: _open_local_browse must bypass browse_load (Genizah-only path)"
    )


def test_open_local_browse_hides_image_pane():
    """D-27: image pane must be hidden for LOCAL files."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_open_local_browse")
    fn_src = ast.get_source_segment(src, fn) or ""
    assert "_set_browse_image_pane_visible(False)" in fn_src, (
        "D-27: _open_local_browse must hide the image pane"
    )


def test_open_local_browse_shows_open_file_button():
    """D-28: Open File button must be shown when filepath available."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_open_local_browse")
    fn_src = ast.get_source_segment(src, fn) or ""
    assert "browse_open_file_btn" in fn_src, (
        "D-28: _open_local_browse must surface the Open File button"
    )


def test_get_local_full_text_helper_defined():
    """_get_local_full_text_for_sys_id must exist."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_get_local_full_text_for_sys_id")
    assert fn is not None, (
        "Category 3: _get_local_full_text_for_sys_id helper required so "
        "_open_local_browse can aggregate pages when the search hit's "
        "full_text field is empty"
    )


def test_get_local_full_text_returns_aggregated_pages():
    """The helper sorts and joins pages from the LOCAL side-index."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_get_local_full_text_for_sys_id")
    fn_src = ast.get_source_segment(src, fn) or ""
    # Confirms the implementation paginates and joins (text aggregation),
    # not just returns the first hit.
    assert "local_searcher" in fn_src
    assert "page" in fn_src.lower() or "p_num" in fn_src.lower()
    assert "join" in fn_src or "\\n\\n" in fn_src or "sort" in fn_src.lower()


# ---------------------------------------------------------------------------
# ResultDialog side
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason="Plan 96-07 (NEW-1) will remove btn_rd_open_browse; this assertion "
           "flips to expecting absence. xfail(strict=True) ensures the flip is detected.",
    strict=True,
)
def test_result_dialog_has_view_in_browse_button():
    """NEW-1 (Phase 96): btn_rd_open_browse is REMOVED -- redundant with `עיין`.

    Once plan 96-07 lands, this xfail becomes xpassed -> strict=True turns
    that into a failure -> executor flips xfail off and the test goes green
    as a stable negative assertion.
    """
    src = _read_source("desktop/result_dialog.py")
    assert "btn_rd_open_browse" not in src, (
        "NEW-1: btn_rd_open_browse must be removed (redundant with `עיין` Browse)"
    )


@pytest.mark.xfail(
    reason="Plan 96-07 (NEW-1) will remove the _rd_open_in_browse handler.",
    strict=True,
)
def test_result_dialog_has_open_in_browse_handler():
    """NEW-1: _rd_open_in_browse handler is REMOVED."""
    src = _read_source("desktop/result_dialog.py")
    fn = _find_function(src, "_rd_open_in_browse")
    assert fn is None, "NEW-1: _rd_open_in_browse handler must be removed"


@pytest.mark.xfail(
    reason="Plan 96-07 (NEW-1) will remove the btn_rd_open_browse visibility branches.",
    strict=True,
)
def test_result_dialog_show_view_in_browse_for_local_only():
    """NEW-1: btn_rd_open_browse visibility code is REMOVED entirely."""
    src = _read_source("desktop/result_dialog.py")
    assert "btn_rd_open_browse.setVisible" not in src, (
        "NEW-1: btn_rd_open_browse visibility setters must be removed"
    )


# ---------------------------------------------------------------------------
# Defense-in-depth (WR-03)
# ---------------------------------------------------------------------------

def test_on_browse_open_file_clicked_has_extension_guard():
    """WR-03: _on_browse_open_file_clicked must refuse non-LOCAL extensions."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_on_browse_open_file_clicked")
    assert fn is not None
    fn_src = ast.get_source_segment(src, fn) or ""
    assert (
        ".docx" in fn_src and ".pdf" in fn_src and ".txt" in fn_src
    ), (
        "WR-03: _on_browse_open_file_clicked must check ext in "
        "{.docx, .pdf, .txt} before calling os.startfile"
    )
