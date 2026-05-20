"""Tests for shared_export_utils.build_rich_snippet_cell (Phase 94 D-14).

Phase 94 Wave 1, Task 3.

The helper extracts the rich-text snippet rendering pattern from desktop's
``write_rich_cell`` inner closure at ``genizah_app.py:17988-18021`` so that
web's main-sheet Snippet column can adopt the same red+bold highlight render.

Contract:
  - Plain text (no ``*`` markers) -> returns ``str`` directly (after sanitize).
  - Marker-bracketed text (e.g. ``"foo *bar* baz"``) -> returns
    ``openpyxl.cell.rich_text.CellRichText`` whose odd-index parts are red+bold
    and even-index parts are normal black.
  - sanitize_fn is applied BEFORE the ``*``-split so formula-injection
    prefix prepending is preserved through the split (sanitize-first ordering).
  - Empty / None text -> ``''`` (no crash).
"""
from openpyxl.cell.rich_text import CellRichText

from shared_export_utils import build_rich_snippet_cell, sanitize_text_for_excel


def _block_color_rgb(block):
    """Extract the 6-char color string from a TextBlock's InlineFont.

    openpyxl stores Color objects with rgb attribute (8-char incl. alpha)
    such as ``'00FF0000'``. The helper writes the 6-char form (``'FF0000'``)
    via the InlineFont(color=...) ctor; openpyxl prepends ``'00'`` for the
    alpha channel. Strip the leading 2 chars for comparison.
    """
    rgb = block.font.color.rgb
    if rgb and len(rgb) == 8:
        return rgb[2:]
    return rgb


def test_plain_text_returns_str():
    result = build_rich_snippet_cell('hello world', sanitize_text_for_excel)
    assert isinstance(result, str)
    assert result == 'hello world'


def test_single_highlight_returns_rich_text():
    result = build_rich_snippet_cell('foo *bar* baz', sanitize_text_for_excel)
    assert isinstance(result, CellRichText)
    # 3 parts after split-on-'*': 'foo ' (normal), 'bar' (red), ' baz' (normal)
    blocks = list(result)
    assert len(blocks) == 3
    assert _block_color_rgb(blocks[0]) == '000000'
    assert blocks[0].font.b is False
    assert _block_color_rgb(blocks[1]) == 'FF0000'
    assert blocks[1].font.b is True
    assert _block_color_rgb(blocks[2]) == '000000'
    assert blocks[2].font.b is False
    # Text content preserved
    assert str(blocks[0].text) == 'foo '
    assert str(blocks[1].text) == 'bar'
    assert str(blocks[2].text) == ' baz'


def test_multiple_highlights_alternate_normal_red():
    result = build_rich_snippet_cell('a *b* c *d* e', sanitize_text_for_excel)
    assert isinstance(result, CellRichText)
    blocks = list(result)
    # 5 parts: 'a ', 'b', ' c ', 'd', ' e'
    assert len(blocks) == 5
    expected_colors = ['000000', 'FF0000', '000000', 'FF0000', '000000']
    expected_bold = [False, True, False, True, False]
    for i, block in enumerate(blocks):
        assert _block_color_rgb(block) == expected_colors[i], (
            f"block {i} color mismatch: got {_block_color_rgb(block)}, "
            f"expected {expected_colors[i]}"
        )
        assert block.font.b is expected_bold[i], (
            f"block {i} bold mismatch: got {block.font.b}, "
            f"expected {expected_bold[i]}"
        )


def test_formula_injection_sanitized_before_split():
    # T-94-01 sanitize-first ordering: sanitize_text_for_excel prefixes
    # '=' with a single quote, so '=cmd|*pwd*' becomes "'=cmd|*pwd*".
    # That split on '*' yields ["'=cmd|", 'pwd', ''] — the helper produces
    # 2 non-empty blocks: normal "'=cmd|" then red 'pwd'.
    result = build_rich_snippet_cell('=cmd|*pwd*', sanitize_text_for_excel)
    assert isinstance(result, CellRichText)
    blocks = list(result)
    # Empty trailing part (after the final '*') is filtered out by the helper.
    assert len(blocks) == 2
    # First block contains the sanitized prefix with leading single-quote.
    assert "'=cmd|" in str(blocks[0].text)
    assert _block_color_rgb(blocks[0]) == '000000'
    # Second block is the red highlight.
    assert _block_color_rgb(blocks[1]) == 'FF0000'
    assert blocks[1].font.b is True
    assert str(blocks[1].text) == 'pwd'


def test_empty_input_returns_empty_string():
    assert build_rich_snippet_cell('', sanitize_text_for_excel) == ''


def test_none_input_returns_empty_string():
    assert build_rich_snippet_cell(None, sanitize_text_for_excel) == ''


def test_no_sanitize_fn_does_not_crash():
    # When sanitize_fn=None, the helper skips sanitization and uses raw text.
    # Useful for callers that have already sanitized upstream.
    result = build_rich_snippet_cell('hello *world*', sanitize_fn=None)
    assert isinstance(result, CellRichText)
    blocks = list(result)
    assert len(blocks) == 2
    assert str(blocks[0].text) == 'hello '
    assert str(blocks[1].text) == 'world'


def test_no_sanitize_with_formula_prefix_does_not_inject_prefix():
    # Without sanitize_fn, the leading '=' is NOT prefixed with single-quote.
    result = build_rich_snippet_cell('=cmd', sanitize_fn=None)
    # No '*' in the (non-sanitized) text -> plain string return.
    assert result == '=cmd'
