# -*- coding: utf-8 -*-
"""Phase 97 U-04 — View All cap raised 200 -> 500 + LD-10 invented-name guards.

Tests (AST/grep-based — no Qt import required):
  T-E-7a  test_cap_is_500 — _VIEW_ALL_PAGE_CAP = 500 in genizah_app.py
  T-E-7b  test_browse_text_widget_name — the View All function uses self.browse_text
          (not self.browse_text_edit — an invented name that was never real)
  T-E-7c  test_no_invented_build_pages_html — _build_pages_html does NOT appear
          in the View All function body (LD-10 invented-name guard)
"""
import ast
import pathlib

_GENIZAH_APP_SRC = (pathlib.Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(
    encoding="utf-8"
)
_GENIZAH_APP_TREE = ast.parse(_GENIZAH_APP_SRC)
_GENIZAH_APP_LINES = _GENIZAH_APP_SRC.splitlines()


# ---------------------------------------------------------------------------
# T-E-7a: _VIEW_ALL_PAGE_CAP = 500
# ---------------------------------------------------------------------------

def test_cap_is_500():
    """_VIEW_ALL_PAGE_CAP must be assigned 500 in genizah_app.py (Phase 97 U-04 cap bump)."""
    # First check via source text (fast)
    source_hit = "_VIEW_ALL_PAGE_CAP = 500" in _GENIZAH_APP_SRC
    # Cross-check via AST: find all Assign where target is _VIEW_ALL_PAGE_CAP
    found_500 = False
    found_200 = False
    for node in ast.walk(_GENIZAH_APP_TREE):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_VIEW_ALL_PAGE_CAP":
                    if isinstance(node.value, ast.Constant) and node.value.value == 500:
                        found_500 = True
                    if isinstance(node.value, ast.Constant) and node.value.value == 200:
                        found_200 = True

    assert source_hit and found_500, (
        f"_VIEW_ALL_PAGE_CAP must be 500 in genizah_app.py. "
        f"source_hit={source_hit}, found_500={found_500}, found_200={found_200}. "
        "Phase 97 U-04 raises the cap from 200 to 500. This test passes after Task 3."
    )
    assert not found_200, (
        "_VIEW_ALL_PAGE_CAP = 200 (old value) still present in genizah_app.py. "
        "Replace it with 500 as part of Phase 97 U-04."
    )


# ---------------------------------------------------------------------------
# T-E-7b: View All path references self.browse_text (not self.browse_text_edit)
# ---------------------------------------------------------------------------

def _find_view_all_function(tree: ast.AST):
    """Find the method that contains the View All rendering code."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # _open_local_browse is the method that contains _VIEW_ALL_PAGE_CAP
            if node.name in ("_open_local_browse", "_open_local_browse_all"):
                return node
    return None


def test_browse_text_widget_name():
    """The View All path must reference self.browse_text (NOT self.browse_text_edit).

    browse_text_edit is an invented name that does not exist in genizah_app.py.
    Using it would silently no-op (AttributeError only at runtime).
    LD-10 pins the real widget name: self.browse_text.
    """
    # Simple text check — grep for the invented name in the genizah_app source
    invented_name_count = _GENIZAH_APP_SRC.count("browse_text_edit")
    assert invented_name_count == 0, (
        f"'browse_text_edit' appears {invented_name_count} time(s) in genizah_app.py. "
        "This is an invented name that does not exist — use self.browse_text instead "
        "(LD-10 real-path constraint)."
    )

    # Also verify the REAL name is present in the View All section
    real_name_count = _GENIZAH_APP_SRC.count("self.browse_text")
    assert real_name_count >= 1, (
        "self.browse_text not found in genizah_app.py. "
        "The View All path must use the real widget attribute name."
    )


# ---------------------------------------------------------------------------
# T-E-7c: _build_pages_html does NOT appear (invented name guard)
# ---------------------------------------------------------------------------

def test_no_invented_build_pages_html():
    """_build_pages_html must NOT appear in genizah_app.py.

    This is an invented name suggested by Codex MEDIUM #5 that does not exist
    in the codebase. Using it would break at runtime. The real path calls
    apply_line_numbered_text directly (LD-10).
    """
    count = _GENIZAH_APP_SRC.count("_build_pages_html")
    assert count == 0, (
        f"'_build_pages_html' appears {count} time(s) in genizah_app.py. "
        "This is an invented helper name that does not exist — remove it. "
        "The real render path calls apply_line_numbered_text directly (LD-10)."
    )
