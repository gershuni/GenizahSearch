# -*- coding: utf-8 -*-
"""The Browse "View whole document" dialog shows transcription notation literally.

The dialog renders the PGP transcription (``get_transcription_for_document``)
with ``ui.html(..., sanitize=False)``. It used to interpolate the raw text into
the HTML, so editorial notation that looks like a tag -- 'al-Ṣa<y>dalānī',
'Manj<ar>ūr', '<upside down>' -- was parsed by the browser as markup and
silently vanished. The text is now escaped by a module-level builder,
``_document_transcription_html``, whose only markup is its own wrapper div.

Two layers: the builder's output is parsed and its visible text compared with
the raw input (behaviour), and an AST guard checks that no ``ui.html`` f-string
in web/pages/browse.py interpolates ``full_text`` unescaped (call site).
"""

import ast
from html.parser import HTMLParser
from pathlib import Path

import pytest

BROWSE = Path(__file__).resolve().parent.parent / 'web' / 'pages' / 'browse.py'


class _Collect(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags = []
        self.text = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)

    def handle_data(self, data):
        self.text.append(data)


def _parse(html_str):
    p = _Collect()
    p.feed(html_str)
    p.close()
    return p.tags, ''.join(p.text)


@pytest.mark.parametrize('raw', [
    'al-Ṣa<y>dalānī',
    'Manj<ar>ūr',
    '<upside down> בשם רחמנא',
    '<b>x</b> & <span title=x>',
    'A &amp; B &lt;c&gt;',
    'שורה ראשונה\nשורה שנייה',
])
def test_whole_document_transcription_shows_notation_literally(raw):
    browse = pytest.importorskip('web.pages.browse')
    out = browse._document_transcription_html(raw)
    tags, text = _parse(out)
    assert tags == ['div'], 'transcription text produced markup: %r' % tags
    assert text.strip('\n ') == raw, (text, raw)


def _unescaped_full_text_in_ui_html(source):
    tree = ast.parse(source)
    hits = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'html' and node.args):
            continue
        first = node.args[0]
        if not isinstance(first, ast.JoinedStr):
            continue
        for part in ast.walk(first):
            if (isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name)
                    and part.value.id == 'full_text'):
                hits.append(part.lineno)
    return hits


def test_no_ui_html_fstring_interpolates_full_text_unescaped():
    hits = _unescaped_full_text_in_ui_html(BROWSE.read_text(encoding='utf-8'))
    assert hits == [], 'raw full_text interpolated into ui.html at lines %r' % hits


def test_whole_document_dialog_renders_through_the_builder():
    """Call-site pin: open_document_viewer passes full_text through the escaping builder."""
    tree = ast.parse(BROWSE.read_text(encoding='utf-8'))
    viewers = [n for n in ast.walk(tree)
               if isinstance(n, ast.FunctionDef) and n.name == 'open_document_viewer']
    assert viewers, 'open_document_viewer not found'
    for fn in viewers:
        builder_calls = [
            c for c in ast.walk(fn)
            if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
            and c.func.id == '_document_transcription_html'
            and c.args and isinstance(c.args[0], ast.Name) and c.args[0].id == 'full_text'
        ]
        assert builder_calls, 'open_document_viewer does not build the transcription HTML via the builder'


@pytest.mark.parametrize('snippet,expected', [
    ("ui.html(f'<div>{full_text}</div>', sanitize=False)", 1),
    ("ui.html(f'<div>{html_module.escape(full_text)}</div>', sanitize=False)", 0),
    ("ui.html(_document_transcription_html(full_text), sanitize=False)", 0),
    ("ui.label(f'{full_text}')", 0),
])
def test_detector_fires_on_synthetic_code(snippet, expected):
    """Regression guard, green before and after: proves the detector can fail."""
    assert len(_unescaped_full_text_in_ui_html(snippet)) == expected
