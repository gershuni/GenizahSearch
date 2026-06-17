from __future__ import annotations

from nicegui import ui
import html

class SemanticHeading(ui.html):
    """
    A semantic heading element (h1-h6) that behaves like a UI element.
    It wraps the heading in a ui.html component but uses 'display: contents'
    to avoid layout issues with the wrapper div.
    It exposes a .text property to update content dynamically.
    """
    def __init__(self, tag: str, text: str, classes: str = '', style: str = ''):
        self.tag_name = tag
        self.heading_classes = classes
        self.heading_style = style
        self._text = text

        # Build initial HTML
        content = self._build_html(text)
        # We construct safe HTML manually, so we disable auto-sanitization to preserve our tags
        super().__init__(content, sanitize=False)

        # We DO NOT use display: contents here because it prevents styling (like text-align)
        # from working correctly on the element itself.
        # Instead, we let the wrapper div exist, and ensure it fills width if needed.
        self.classes('w-full')

    def _build_html(self, text):
        return f'<{self.tag_name} class="{self.heading_classes}" style="{self.heading_style}">{html.escape(text)}</{self.tag_name}>'

    @property
    def text(self):
        return self._text

    @text.setter
    def text(self, value):
        self._text = value
        self.content = self._build_html(value)

def h1(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 1 semantic heading."""
    return SemanticHeading('h1', text, classes, style)

def h2(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 2 semantic heading."""
    return SemanticHeading('h2', text, classes, style)

def h3(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 3 semantic heading."""
    return SemanticHeading('h3', text, classes, style)

def h4(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 4 semantic heading."""
    return SemanticHeading('h4', text, classes, style)

def h5(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 5 semantic heading."""
    return SemanticHeading('h5', text, classes, style)

def h6(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 6 semantic heading."""
    return SemanticHeading('h6', text, classes, style)


def render_line_numbered_html(
    text: str,
    highlight_html: str | None = None,
    line_height: str = "2.2",
    font_size: str = "1.4rem",
    show_line_numbers: bool = True,
) -> str:
    """Render transcription text with an RTL-aware line-number gutter (Phase 999.4).

    Per D-10, a "line" is one element of `text.split('\\n')`. Blank lines
    DO get a number — preserving 1-to-1 alignment with the Responsa
    `L<N>:word` search syntax in genizah_core.py:7679-7691 (parser) and
    :4970-5010 (line_constraints).

    When `show_line_numbers=False` the helper returns plain rendered HTML
    with NO gutter — the body still gets the same RTL+Hebrew-font styling.

    `highlight_html` (if provided) is the caller's pre-escaped, highlight-
    marked HTML (e.g. from `highlight_text(text)` at browse.py:1573-1597,
    or from `_apply_highlight_marks` in search_results.py). When None, the
    raw `text` is HTML-escaped locally so XSS-bearing source text cannot
    inject live tags.

    Defense-in-depth: if `highlight_html` contains `<br>` separators (some
    callers pre-convert `\\n` to `<br>` before rendering), the helper
    normalizes them back to `\\n` so line counting still matches
    `text.split('\\n')`.

    The gutter is a SEPARATE grid column with `user-select: none` so a user
    drag-selecting inside the body copies the body only, NOT the line
    numbers (D-04 invariant — tested structurally via
    tests/test_line_numbers_web.py::test_render_line_numbered_html_copy_paste_invariant
    and confirmed end-to-end in the Phase 999.4 Plan 01 human-verify smoke check).

    Promoted from web/pages/browse.py to this shared component (Phase 117 Plan 03)
    so the anchor pane and Phase 119 Compare can reuse it directly (ANC-03).
    No browse module globals or state dependencies — pure function.
    """
    import html as _html_module

    if not text and not highlight_html:
        return ""

    if not show_line_numbers:
        if highlight_html is not None:
            # Preserve visual line breaks for non-line-numbered mode.
            body = highlight_html.replace('\n', '<br>')
        else:
            body = _html_module.escape(text).replace('\n', '<br>')
        return (
            f'<div class="transcription-text" style="'
            f'font-size: {font_size}; line-height: {line_height}; '
            f'direction: rtl; text-align: right; '
            f'font-family: \'David\', \'Frank Ruehl\', \'Noto Sans Hebrew\', serif; '
            f'white-space: pre-wrap; overflow-wrap: break-word; word-break: break-word;'
            f'">{body}</div>'
        )

    # Line-numbered render — per-line grid rows for robust alignment.
    # Each logical line gets ONE grid row containing a (gutter, body) cell pair.
    # If a body line wraps to multiple visual lines, the row grows in height and
    # the gutter number stays anchored to the top via `align-self: start`. The
    # body cell carries an absolute `min-height` so blank lines reserve their
    # row height instead of collapsing to zero (D-10 visual parity with the
    # Responsa `L<N>:` parser).
    if highlight_html is not None:
        # Some callers pre-convert \n to <br>; normalize for counting.
        normalized = (
            highlight_html
            .replace('<br>', '\n')
            .replace('<br/>', '\n')
            .replace('<br />', '\n')
        )
        body_lines = normalized.split('\n')
    else:
        escaped = _html_module.escape(text)
        body_lines = escaped.split('\n')

    # Absolute row line-height computed from the unitless multiplier × the body
    # font-size. Using calc() keeps the gutter span's line-box exactly as tall
    # as the body cell, eliminating drift from unitless `line-height` inheriting
    # against the gutter's smaller (0.75em) own font-size.
    row_line_height = f"calc({line_height} * {font_size})"

    gutter_style = (
        "color: var(--text-muted, #9ca3af); "
        "font-size: 0.75em; "
        "text-align: left; "
        "font-family: 'Inter', system-ui, sans-serif; "
        "user-select: none; -webkit-user-select: none; "
        "direction: ltr; "
        "padding-left: 0.4em; "
        "border-left: 1px solid var(--border-light, #e5e7eb); "
        "align-self: start; "
        f"line-height: {row_line_height};"
    )
    body_row_style = (
        "white-space: pre-wrap; "
        "overflow-wrap: break-word; "
        "word-break: break-word; "
        f"min-height: {row_line_height};"
    )

    rows = []
    for idx, line_content in enumerate(body_lines):
        rows.append(
            f'<span class="line-number-gutter" style="{gutter_style}">{idx + 1}</span>'
            f'<div class="line-numbered-body-row" style="{body_row_style}">{line_content}</div>'
        )

    return (
        f'<div class="line-numbered-text" style="'
        f'direction: rtl; text-align: right; '
        f'line-height: {line_height}; font-size: {font_size}; '
        f'font-family: \'David\', \'Frank Ruehl\', \'Noto Sans Hebrew\', serif; '
        f'display: grid; grid-template-columns: max-content 1fr; '
        f'column-gap: 0.6em; row-gap: 0;'
        f'">'
        + ''.join(rows)
        + '</div>'
    )
