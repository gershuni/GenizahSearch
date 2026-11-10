"""Phase 128 Space-scroll test scaffold — NON-gui (bulk slice).

Contains:
  - Six web source/static guards (read web/pages/search.py + web/pages/search_results.py source as strings).
  - One desktop source guard: Space in the desktop results ticks instead of paging (2026-10-30).

This file is intentionally NOT registered in conftest._GUI_TEST_FILES so these tests run in
the bulk `-m "not gui"` slice with no PyQt6 import required at collection time.

Web guards go GREEN once Task 2 lands (setup_space_scroll injected into search.py).
"""
from __future__ import annotations

import pathlib

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_SEARCH_PY = (_REPO_ROOT / "web" / "pages" / "search.py").read_text(encoding="utf-8")
_SEARCH_RESULTS_PY = (_REPO_ROOT / "web" / "pages" / "search_results.py").read_text(encoding="utf-8")


def _func_source(src: str, name: str) -> str:
    """Return the source slice of a (possibly nested) `def {name}` — its def line plus all
    more-indented body lines, stopping at the first line dedented to <= the def's indent.
    Lets source guards assert against a FUNCTION BODY, not the whole file (avoids tautologies
    where a comment elsewhere satisfies the substring)."""
    lines = src.splitlines()
    start = next(
        (i for i, ln in enumerate(lines) if ln.lstrip().startswith(f"def {name}")), None
    )
    assert start is not None, f"def {name} not found in source"
    indent = len(lines[start]) - len(lines[start].lstrip())
    body = [lines[start]]
    for ln in lines[start + 1:]:
        if ln.strip() and (len(ln) - len(ln.lstrip())) <= indent:
            break
        body.append(ln)
    return "\n".join(body)


# ---------------------------------------------------------------------------
# Web source / static guards (SCROLL-01, GUARD-02)
# ---------------------------------------------------------------------------

def test_web_space_scroll_js_installed():
    """web/pages/search.py must define setup_space_scroll and include the _gsSpaceScrollInstalled guard flag."""
    assert "setup_space_scroll" in _SEARCH_PY, (
        "Expected setup_space_scroll function in web/pages/search.py"
    )
    assert "_gsSpaceScrollInstalled" in _SEARCH_PY, (
        "Expected _gsSpaceScrollInstalled double-install guard in web/pages/search.py"
    )


def test_web_suppression_set_complete():
    """The injected JS must contain all required suppression conditions from D-01,
    including the anchor checks ('A' tagName + closest('a[href]'))."""
    # Tag-name checks
    assert "'INPUT'" in _SEARCH_PY, "Missing 'INPUT' tagName check in space-scroll JS"
    assert "'BUTTON'" in _SEARCH_PY, "Missing 'BUTTON' tagName check in space-scroll JS"
    assert "'TEXTAREA'" in _SEARCH_PY, "Missing 'TEXTAREA' tagName check in space-scroll JS"
    assert "'SELECT'" in _SEARCH_PY, "Missing 'SELECT' tagName check in space-scroll JS"
    # Anchor checks (MEDIUM fix — ui.link renders as <a href>)
    assert "'A'" in _SEARCH_PY, "Missing 'A' (anchor) tagName check in space-scroll JS"
    assert "closest('a[href]')" in _SEARCH_PY, (
        "Missing closest('a[href]') ancestor-anchor check in space-scroll JS"
    )
    # Role-button check
    assert "getAttribute('role')" in _SEARCH_PY, (
        "Missing getAttribute('role') check in space-scroll JS"
    )
    assert "'button'" in _SEARCH_PY, "Missing 'button' role value in space-scroll JS"
    # contentEditable check
    assert "isContentEditable" in _SEARCH_PY, (
        "Missing isContentEditable check in space-scroll JS"
    )


def test_web_dialog_guard():
    """The injected JS must guard against open Quasar dialogs via .q-dialog."""
    assert "document.querySelector('.q-dialog')" in _SEARCH_PY, (
        "Missing .q-dialog guard in space-scroll JS"
    )


def test_expand_toggle_space_prevent_intact():
    """Regression guard (GUARD-02): web/pages/search_results.py must still have
    keydown.space.self.prevent on the expand-toggle column (Finding W-2)."""
    assert "keydown.space.self.prevent" in _SEARCH_RESULTS_PY, (
        "keydown.space.self.prevent was removed from web/pages/search_results.py — "
        "this regresses the expand-toggle Space behavior (Finding W-2)"
    )


def test_web_no_double_install_guard():
    """The injected JS must set window._gsSpaceScrollInstalled to prevent double-install (D-03, Pitfall 6)."""
    assert "window._gsSpaceScrollInstalled" in _SEARCH_PY, (
        "Missing window._gsSpaceScrollInstalled double-install guard in space-scroll JS"
    )


def test_existing_shortcuts_preserved():
    """GUARD-02: handle_keyboard_shortcut must still contain Escape and '/' branches.

    Scoped to the function body (not the whole file) so a stray comment or unrelated
    string elsewhere in search.py cannot satisfy the guard (Codex CODE review LOW)."""
    body = _func_source(_SEARCH_PY, "handle_keyboard_shortcut")
    assert "Escape" in body, (
        "Escape branch missing from handle_keyboard_shortcut — keyboard shortcut regression"
    )
    assert "'/'" in body or '"/"' in body, (
        "'/' branch missing from handle_keyboard_shortcut — keyboard shortcut regression"
    )


# ---------------------------------------------------------------------------
# Desktop (owner, 2026-10-30): Space in the Search results ticks the result
# instead of paging -- Page Down pages. The behaviour is pinned in
# tests/test_quick_open.py and tests/test_space_scroll_gui.py; this guard keeps
# the old paging helper from coming back beside it. The website keeps its own
# Space paging (the guards above).
# ---------------------------------------------------------------------------

def test_desktop_space_ticks_instead_of_paging():
    src = (_REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8")
    assert "def space_scroll_action" not in src
    assert "SliderPageStepAdd" not in src
    assert "def _results_table_key(self, event):" in src
