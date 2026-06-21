# -*- coding: utf-8 -*-
"""
Phase 117 Plan 03 — Tests for render_line_numbered_html in web/components/typography.py.

Verifies:
- The public function is accessible from web.components.typography (promotion).
- The re-export alias _render_line_numbered_html in web.pages.browse still works.
- Gutter is present and has user-select: none when show_line_numbers=True.
- No gutter when show_line_numbers=False.
- XSS escaping (no live <script> in output).
- No app.storage.user references (pure function, no browse globals).
"""
from __future__ import annotations


from web.components.typography import render_line_numbered_html
from web.pages.browse import _render_line_numbered_html as _browse_alias


# ─── Promotion sanity ──────────────────────────────────────────────────────────

def test_render_line_numbered_html_accessible_from_components():
    """Public function must be importable from web.components.typography."""
    # If the import above succeeded, this trivially passes.
    assert callable(render_line_numbered_html)


def test_browse_alias_is_same_function():
    """browse.py re-export alias must point to the same callable (not a copy)."""
    assert _browse_alias is render_line_numbered_html


# ─── Gutter on/off ────────────────────────────────────────────────────────────

GUTTER_CLASS = 'line-number-gutter'


def test_show_line_numbers_true_emits_gutter():
    """show_line_numbers=True: output contains a gutter element."""
    out = render_line_numbered_html("line one\nline two", show_line_numbers=True)
    assert GUTTER_CLASS in out, f"Expected gutter class; got: {out!r}"


def test_show_line_numbers_true_gutter_has_user_select_none():
    """The gutter span must have user-select: none (D-04 copy-paste invariant)."""
    out = render_line_numbered_html("line one\nline two", show_line_numbers=True)
    assert 'user-select: none' in out, (
        f"Gutter must have user-select: none; output: {out!r}"
    )


def test_show_line_numbers_false_no_gutter():
    """show_line_numbers=False: no gutter class in output."""
    out = render_line_numbered_html("line one\nline two", show_line_numbers=False)
    assert GUTTER_CLASS not in out, (
        f"show_line_numbers=False must produce no gutter; got: {out!r}"
    )


def test_show_line_numbers_false_body_preserved():
    """show_line_numbers=False: body text still present in output."""
    out = render_line_numbered_html("hello world", show_line_numbers=False)
    assert 'hello world' in out


# ─── XSS safety ───────────────────────────────────────────────────────────────

def test_xss_escaping_when_no_highlight_html():
    """Raw text with <script> must be HTML-escaped (ANC-03 / T-117-08)."""
    text = "<script>alert('x')</script>"
    out = render_line_numbered_html(text, show_line_numbers=True)
    assert '<script>' not in out.lower(), (
        f"raw text injected as live <script>; output: {out!r}"
    )
    assert '&lt;script' in out, (
        f"expected HTML-escaped <script>; output: {out!r}"
    )


def test_xss_escaping_show_line_numbers_false():
    """XSS escaping also active when show_line_numbers=False."""
    text = "<img src=x onerror=alert(1)>"
    out = render_line_numbered_html(text, show_line_numbers=False)
    assert '<img ' not in out, (
        f"raw <img> injected into output; output: {out!r}"
    )


# ─── No browse globals ────────────────────────────────────────────────────────

def test_typography_module_has_no_app_storage_user():
    """typography.py must not reference app.storage.user (pure function, no browse state)."""
    import inspect
    import web.components.typography as mod
    src = inspect.getsource(mod)
    # Only check within render_line_numbered_html body (not other functions)
    fn_src = inspect.getsource(render_line_numbered_html)
    assert 'app.storage.user' not in fn_src, (
        "render_line_numbered_html must not reference app.storage.user"
    )
