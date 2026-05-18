# -*- coding: utf-8 -*-
"""Phase 999.4 Plan 01 — Structural tests for the web line-number gutter helper.

Covers D-01, D-04, D-05, D-07, D-08, D-10 of `.planning/phases/999.4-line-numbering/999.4-CONTEXT.md`.

The helper `_render_line_numbered_html(text, highlight_html, line_height, font_size,
show_line_numbers)` lives in `web/pages/browse.py` at module scope. It is a PURE
function (str -> str) so these tests do not spin up NiceGUI; they exercise the
helper directly.

Hard invariants:
  - D-04 copy-paste invariant: the gutter is a separate DOM element with
    `user-select: none` AND lives in a separate grid column. Test 3 strips the
    `<span class="line-number-gutter">...</span>` from the HTML output via regex
    and asserts the remainder contains no stray digits beyond what was in the
    source `text`.
  - D-10 split semantics: line count == `len(text.split('\\n'))`. Blank lines
    and trailing empties get their own number. We use `text.split('\\n')`, NOT
    `splitlines()` (which would silently drop a trailing empty line).
  - D-07: when `show_line_numbers=False`, NO `line-number-gutter` class anywhere
    in the output.
"""
from __future__ import annotations

import re


from web.pages.browse import _render_line_numbered_html


# ---------- Helpers ----------

GUTTER_CLASS = 'line-number-gutter'
GUTTER_DIGITS_RE = re.compile(
    r'class="' + GUTTER_CLASS + r'[^"]*"[^>]*>([^<]*)</span>',
    re.DOTALL,
)
GUTTER_SPAN_RE = re.compile(
    r'<span class="' + GUTTER_CLASS + r'[^"]*"[^>]*>.*?</span>',
    re.DOTALL,
)
TAG_RE = re.compile(r'<[^>]+>')


def _extract_gutter_numbers(html: str) -> list[str]:
    """Return the list of line-number tokens found inside the gutter span(s)."""
    matches = GUTTER_DIGITS_RE.findall(html)
    numbers: list[str] = []
    for blob in matches:
        # Numbers are newline-separated inside the span.
        for tok in blob.split('\n'):
            tok = tok.strip()
            if tok:
                numbers.append(tok)
    return numbers


def _strip_gutter(html: str) -> str:
    """Remove the gutter span entirely, leaving only body HTML."""
    return GUTTER_SPAN_RE.sub('', html)


def _strip_all_tags(html: str) -> str:
    return TAG_RE.sub('', html)


# ---------- Test 1: basic line count ----------

def test_render_line_numbered_html_basic():
    text = "alpha\nbeta\ngamma"
    out = _render_line_numbered_html(
        text=text,
        highlight_html=None,
        line_height="2.2",
        font_size="1.4rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1', '2', '3'], (
        f"expected gutter [1,2,3], got {numbers!r}\nFull HTML:\n{out}"
    )
    # Body text contains the three source words.
    body_text = _strip_all_tags(_strip_gutter(out))
    assert 'alpha' in body_text
    assert 'beta' in body_text
    assert 'gamma' in body_text


# ---------- Test 2: blank middle line is numbered ----------

def test_render_line_numbered_html_blank_line_numbered():
    """D-10: blank lines get their own number, preserving L<N>: alignment."""
    text = "alpha\n\ngamma"
    out = _render_line_numbered_html(
        text=text, highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1', '2', '3'], (
        f"blank middle line must still be numbered; got {numbers!r}"
    )


# ---------- Test 3: copy-paste invariant (D-04) ----------

def test_render_line_numbered_html_copy_paste_invariant():
    """D-04: stripping the gutter span yields body HTML that contains NO line-number digits.

    This proves the gutter is structurally separable — a browser user copying
    text from the body column will not get the gutter content.
    """
    text = "alpha\nbeta\ngamma\ndelta"
    out = _render_line_numbered_html(
        text=text, highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    body_html = _strip_gutter(out)
    # The gutter span has been removed entirely.
    assert GUTTER_CLASS not in body_html, (
        "gutter remnants in body after strip; gutter may not be a single span"
    )
    # Body text is now gutter-free.
    body_text = _strip_all_tags(body_html)
    # The source text contains zero digits, so the body text must also contain zero digits.
    digits = re.findall(r'\d', body_text)
    assert digits == [], (
        f"copy-paste invariant violated: gutter-stripped body still has digits {digits!r}\n"
        f"body_html: {body_html!r}"
    )
    # Also assert the gutter span has user-select: none (defense-in-depth).
    assert 'user-select: none' in out or 'user-select:none' in out, (
        "gutter span must have user-select: none for browser-level copy safety"
    )


# ---------- Test 4: disabled passthrough ----------

def test_render_line_numbered_html_disabled_passthrough():
    text = "alpha\nbeta"
    out = _render_line_numbered_html(
        text=text, highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=False,
    )
    assert GUTTER_CLASS not in out, (
        f"show_line_numbers=False must produce no gutter; got: {out!r}"
    )
    # Still must contain the body text.
    assert 'alpha' in out
    assert 'beta' in out


# ---------- Test 5: highlight HTML survives wrap ----------

def test_render_line_numbered_html_with_highlight():
    """Body HTML preserves <mark> tags from the caller's highlight pipeline."""
    text = "alpha beta"
    highlight = 'alpha <mark>beta</mark>'
    out = _render_line_numbered_html(
        text=text, highlight_html=highlight,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1'], f"single-line input should yield [1]; got {numbers!r}"
    body_html = _strip_gutter(out)
    assert '<mark>beta</mark>' in body_html, (
        "highlight <mark> tag must survive the gutter wrap"
    )


# ---------- Test 6: RTL direction ----------

def test_render_line_numbered_html_rtl_direction():
    out = _render_line_numbered_html(
        text="א\nב\nג",
        highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    assert 'direction: rtl' in out, (
        f"helper output must contain `direction: rtl` for Hebrew render; got: {out!r}"
    )


# ---------- Test 7: line_height parameter wired ----------

def test_render_line_numbered_html_line_height_param():
    out = _render_line_numbered_html(
        text="a\nb",
        highlight_html=None,
        line_height="2.4", font_size="1.2rem",
        show_line_numbers=True,
    )
    assert 'line-height: 2.4' in out, (
        f"line_height param must surface in CSS; got: {out!r}"
    )
    assert 'font-size: 1.2rem' in out, (
        f"font_size param must surface in CSS; got: {out!r}"
    )


# ---------- Test 8: blank-count matches text.split('\n') ----------

def test_render_line_numbered_html_blank_count_matches():
    """D-10 invariant: line count == len(text.split('\\n'))."""
    text = "a\n\n\nb"  # 3 newlines -> 4 lines (a, blank, blank, b)
    expected_count = len(text.split('\n'))
    assert expected_count == 4
    out = _render_line_numbered_html(
        text=text, highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1', '2', '3', '4'], (
        f"text.split('\\n') gives {expected_count} lines; gutter must show 1..{expected_count}; "
        f"got {numbers!r}"
    )


# ---------- Test 9: XSS safety when highlight_html is None ----------

def test_render_line_numbered_html_xss_safety():
    """When highlight_html is None, raw text MUST be HTML-escaped."""
    text = "<script>alert('x')</script>"
    out = _render_line_numbered_html(
        text=text, highlight_html=None,
        line_height="2.2", font_size="1.4rem",
        show_line_numbers=True,
    )
    # The literal <script> tag must NOT appear in the output.
    # (Note: a `<span class="line-number-gutter">` and `<div class="line-numbered-body">`
    # ARE expected — but no LIVE <script>.)
    assert '<script>' not in out.lower(), (
        f"raw text injected as live <script> tag; output: {out!r}"
    )
    # The escaped form should be present.
    assert '&lt;script&gt;' in out or '&lt;script' in out, (
        f"expected HTML-escaped <script>; output: {out!r}"
    )


# ---------- Test 10: Quick View pre-built-HTML highlight survives ----------

def test_quick_view_gutter_with_prebuilt_html():
    """Quick View always passes pre-built highlight_html; <mark> tags must survive."""
    text = "line one\nline two"
    highlight = "line <mark>one</mark>\nline two"
    out = _render_line_numbered_html(
        text=text, highlight_html=highlight,
        line_height="2.4", font_size="1.2rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1', '2'], f"expected [1,2]; got {numbers!r}"
    body_html = _strip_gutter(out)
    assert '<mark>one</mark>' in body_html, (
        f"<mark> in pre-built highlight_html must survive; body: {body_html!r}"
    )


# ---------- Test 11: <br>-normalization for callers that pre-converted ----------

def test_quick_view_normalizes_br_to_newline():
    """Defense-in-depth: callers that pre-converted \\n to <br> still get correct numbering."""
    text = "line one\nline two\nline three"
    highlight = "line one<br>line two<br>line three"
    out = _render_line_numbered_html(
        text=text, highlight_html=highlight,
        line_height="2.4", font_size="1.2rem",
        show_line_numbers=True,
    )
    numbers = _extract_gutter_numbers(out)
    assert numbers == ['1', '2', '3'], (
        f"<br>-pre-converted highlight_html must still yield [1,2,3]; got {numbers!r}"
    )


# ---------- Test 12: passthrough disabled with pre-built HTML ----------

def test_quick_view_passthrough_disabled():
    """show_line_numbers=False with pre-built highlight_html: no gutter, body preserved."""
    text = "line one\nline two"
    highlight = "line <mark>one</mark>\nline two"
    out = _render_line_numbered_html(
        text=text, highlight_html=highlight,
        line_height="2.4", font_size="1.2rem",
        show_line_numbers=False,
    )
    assert GUTTER_CLASS not in out, (
        f"show_line_numbers=False must produce no gutter; got: {out!r}"
    )
    # Highlight <mark> still preserved.
    assert '<mark>one</mark>' in out, (
        f"highlight <mark> tag must be preserved in passthrough; got: {out!r}"
    )
