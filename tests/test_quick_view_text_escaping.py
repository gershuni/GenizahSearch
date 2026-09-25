"""Quick View shows transcription notation literally and keeps highlights intact.

The Quick View text panel (web/pages/search_results.py, open_advanced_dialog) is
rendered with ``ui.html(..., sanitize=False)``. Its text comes from the
transcription sources, which carry editorial notation such as ``<upside down>``, ``al-Ṣa<y>dalānī`` or
``Manj<ar>ūr``. Before this fix the highlighter inserted ``<mark>`` tags into the
UNESCAPED text, so the browser swallowed that notation as tags, and a second
search term could match inside the ``<mark class=...>`` markup the first term had
just inserted.

The highlighter lives inside a NiceGUI dialog closure that cannot be driven
headlessly, so these tests load ``_apply_highlight_marks`` straight from the
module source with ``ast`` (it works whether the function is nested, as it was,
or module-level, as it is now) and parse its output the way a browser would.
The call sites are pinned by an AST guard.
"""
from __future__ import annotations

import ast
import html
import re
import textwrap
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SEARCH_RESULTS = ROOT / "web" / "pages" / "search_results.py"


# ---------------------------------------------------------------------------
# Loading helpers from source
# ---------------------------------------------------------------------------

def _src() -> str:
    return SEARCH_RESULTS.read_text(encoding="utf-8")


def _tree() -> ast.Module:
    return ast.parse(_src())


def _load_function(name: str):
    """exec the (first) def called ``name`` found anywhere in search_results.py."""
    src = _src()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            ns = {"html": html, "re": re}
            exec(textwrap.dedent(ast.get_source_segment(src, node)), ns)
            return ns[name]
    raise AssertionError(f"{name} is not defined in web/pages/search_results.py")


def _highlight(text, terms):
    return _load_function("_apply_highlight_marks")(text, terms)


class _Probe(HTMLParser):
    """Parse HTML the way the browser's innerHTML would see it."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags: list[str] = []
        self.parts: list[str] = []
        self.marks: list[str] = []
        self._stack: list[int] = []
        self.max_depth = 0

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        if tag == "br":
            self.parts.append("\n")
        elif tag == "mark":
            self.marks.append("")
            self._stack.append(len(self.marks) - 1)
            self.max_depth = max(self.max_depth, len(self._stack))

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag == "mark":
            self._stack.pop()

    def handle_endtag(self, tag):
        if tag == "mark" and self._stack:
            self._stack.pop()

    def handle_data(self, data):
        self.parts.append(data)
        for i in self._stack:
            self.marks[i] += data

    @property
    def text(self) -> str:
        return "".join(self.parts)


def _parse(markup: str) -> _Probe:
    p = _Probe()
    p.feed(markup)
    p.close()
    return p


# ---------------------------------------------------------------------------
# Behaviour of the highlighter
# ---------------------------------------------------------------------------

# Each term lies OUTSIDE every tag of its text, so a tag-splitting accident on
# today's code cannot make a case pass by chance.
_NOTATION_CASES = [
    ("<b>x</b>", ["x"]),
    ("<span>note</span> (1)", ["(1)"]),
    ("<span title=x> caption", ["caption"]),
    ("al-Ṣa<y>dalānī", ["dal"]),
    ("בבגר <upside down> שלום", ["שלום"]),
    ("Manj<ar>ūr", ["ūr"]),
]


@pytest.mark.parametrize("text,term", _NOTATION_CASES)
@pytest.mark.parametrize("with_term", [False, True], ids=["no-terms", "with-term"])
def test_literal_markup_in_transcription_is_visible_text(text, term, with_term):
    terms = term if with_term else []
    parsed = _parse(_highlight(text, terms))
    assert set(parsed.tags) <= {"mark", "br"}, parsed.tags
    assert parsed.text == text
    if with_term:
        assert parsed.marks == term


def test_entities_in_source_text_are_shown_literally():
    text = "A &amp; B &lt;c&gt;"
    out = _highlight(text, ["B"])
    parsed = _parse(out)
    assert parsed.text == text
    assert "&amp;amp;" in out
    assert parsed.marks == ["B"]


def test_later_term_never_matches_inserted_markup():
    text = "mark the class"
    parsed = _parse(_highlight(text, ["mark", "class"]))
    assert parsed.marks == ["mark", "class"]
    assert parsed.max_depth == 1
    assert parsed.text == text


@pytest.mark.parametrize("terms", [["abc", "b"], ["b", "abc"]])
def test_overlapping_terms_do_not_nest_or_drop(terms):
    parsed = _parse(_highlight("abc b", terms))
    assert parsed.marks == ["abc", "b"]
    assert parsed.max_depth == 1
    assert parsed.text == "abc b"


def test_longest_term_wins_at_the_same_position():
    parsed = _parse(_highlight("xabc", ["ab", "abc"]))
    assert parsed.marks == ["abc"]


def test_highlight_terms_with_special_chars_and_hebrew_survive():
    """REGRESSION GUARD (green before and after the fix).

    Terms are matched on the RAW text, never on escaped text: 'amp' must match
    only inside 'example', and 'quot' must match nothing, even though the
    escaped output contains '&amp;' and '&quot;'. Browse-style escape-then-match
    would split those entities.
    """
    text = 'שלום <עולם> & "ציטוט" example'
    parsed = _parse(_highlight(text, ["<עולם>", "ציטוט", "amp", "quot"]))
    assert parsed.marks == ["<עולם>", "ציטוט", "amp"]
    assert parsed.text == text


def test_mark_spanning_newline_is_closed_per_line():
    """A mark over a line break is closed and reopened, so every rendered row
    of the line-numbered grid carries balanced tags."""
    from web.components.typography import render_line_numbered_html

    text = "ab\ncd"
    out = render_line_numbered_html(
        text=text, highlight_html=_highlight(text, ["b\nc"]), show_line_numbers=True)
    rows = re.findall(r'<div class="line-numbered-body-row"[^>]*>(.*?)</div>', out)
    assert len(rows) == 2
    for row in rows:
        assert row.count("<mark") == row.count("</mark>"), row
    assert [_parse(r).text for r in rows] == ["ab", "cd"]


# 'b' and 'r' must NOT be used as terms here: they occur inside the literal
# '<br>' of the text, and splitting that tag apart would hide the defect.
@pytest.mark.parametrize("terms", [[], ["c"]], ids=["no-terms", "term-c"])
def test_line_numbered_render_keeps_source_line_count(terms):
    from web.components.typography import render_line_numbered_html

    text = "a<br>b\nc"
    markup = _highlight(text, terms)

    out = render_line_numbered_html(text=text, highlight_html=markup, show_line_numbers=True)
    gutter = re.findall(r'<span class="line-number-gutter"[^>]*>([^<]*)</span>', out)
    assert gutter == ["1", "2"]
    rows = re.findall(r'<div class="line-numbered-body-row"[^>]*>(.*?)</div>', out)
    assert "\n".join(_parse(r).text for r in rows) == text

    flat = render_line_numbered_html(text=text, highlight_html=markup, show_line_numbers=False)
    assert _parse(flat).text == text


# ---------------------------------------------------------------------------
# FGP stored entities (decoded once, on the web side only)
# ---------------------------------------------------------------------------

def test_fgp_stored_entities_show_as_the_character():
    """FGP sidecar text stores some characters as entities ('&deg;' in 518 rows).
    Quick View showed them decoded before (the browser parsed them); it must keep
    showing the degree sign now that the text is escaped."""
    decode = _load_function("_fgp_text_for_display")
    parsed = _parse(_highlight(decode("בבגר&deg; שלום"), ["שלום"]))
    assert parsed.text == "בבגר° שלום"
    assert parsed.marks == ["שלום"]


def test_fgp_entities_are_decoded_exactly_once():
    decode = _load_function("_fgp_text_for_display")
    assert _parse(_highlight(decode("&lt;and other&gt;"), [])).text == "<and other>"
    assert _parse(_highlight(decode("Pass&amp;#40age"), [])).text == "Pass&#40age"
    assert decode("") == ""
    assert decode(None) == ""


def test_pgp_entity_text_stays_literal():
    """PGP text is NOT decoded: a literal '&amp;' there is shown as '&amp;'."""
    parsed = _parse(_highlight("salt &amp; pepper", []))
    assert parsed.text == "salt &amp; pepper"


# ---------------------------------------------------------------------------
# Call sites (AST): every Quick View html string goes through the helper
# ---------------------------------------------------------------------------

def _func(tree_or_node, name):
    for node in ast.walk(tree_or_node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"def {name} not found")


def _calls_to(node, name):
    return [
        n for n in ast.walk(node)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name
    ]


def _own_calls_to(fn, name):
    """Calls to ``name`` in ``fn``'s own body, not in defs nested inside it."""
    found = []
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        n = stack.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name:
            found.append(n)
        stack.extend(ast.iter_child_nodes(n))
    return found


def _check_call_sites(tree: ast.Module) -> list[str]:
    problems: list[str] = []
    module_level = [
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_highlight_marks"
    ]
    if not module_level:
        problems.append("_apply_highlight_marks is not a module-level function")

    dialog = _func(tree, "open_advanced_dialog")
    for n in ast.walk(dialog):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == "_apply_highlight_marks":
            problems.append(f"nested _apply_highlight_marks at line {n.lineno}")
        if isinstance(n, ast.Constant) and n.value == "<br>":
            problems.append(f"hand-built '<br>' at line {n.lineno}")
        if isinstance(n, ast.Assign):
            for tgt in n.targets:
                if isinstance(tgt, ast.Name) and tgt.id in ("text_html", "new_html"):
                    v = n.value
                    ok = (isinstance(v, ast.Call) and isinstance(v.func, ast.Name)
                          and v.func.id == "_apply_highlight_marks")
                    if not ok:
                        problems.append(f"{tgt.id} at line {n.lineno} is not built by the helper")

    render_content = _func(dialog, "render_content")
    version_change = _func(dialog, "handle_version_change")
    n_total = len(_calls_to(dialog, "_apply_highlight_marks"))
    if n_total != 2:
        problems.append(f"expected 2 _apply_highlight_marks calls in the dialog, found {n_total}")
    for fn in (render_content, version_change):
        if len(_own_calls_to(fn, "_apply_highlight_marks")) != 1:
            problems.append(f"{fn.name} must call _apply_highlight_marks exactly once")
        if not _own_calls_to(fn, "_fgp_text_for_display"):
            problems.append(f"{fn.name} never decodes FGP text")

    # In handle_version_change the FGP decode must come before the text is
    # stored or highlighted.
    decodes = sorted((c.lineno, c.col_offset) for c in _calls_to(version_change, "_fgp_text_for_display"))
    marks = sorted((c.lineno, c.col_offset) for c in _calls_to(version_change, "_apply_highlight_marks"))
    stores = sorted(
        (n.lineno, n.col_offset) for n in ast.walk(version_change)
        if isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name)
            and t.value.id == "current_display_text" for t in n.targets)
    )
    if decodes and marks and stores and not (decodes[0] < marks[0] and decodes[0] < stores[0]):
        problems.append("handle_version_change decodes FGP text after using it")
    return problems


def test_quick_view_call_sites_route_through_escaping_helper():
    problems = _check_call_sites(_tree())
    assert not problems, "\n".join(problems)


def test_call_site_guard_can_fail():
    """The guard catches a seeded hand-built '<br>' html string."""
    seeded = textwrap.dedent('''
        def _apply_highlight_marks(text, terms):
            return text
        def _fgp_text_for_display(text):
            return text
        def open_advanced_dialog():
            def render_content():
                t = _fgp_text_for_display('x')
                text_html = _apply_highlight_marks(t, [])
            def handle_version_change(new_text, info):
                new_text = _fgp_text_for_display(new_text)
                current_display_text['value'] = new_text
                if info:
                    new_html = _apply_highlight_marks(new_text, [])
                else:
                    new_html = new_text.replace('\\n', '<br>')
    ''')
    problems = _check_call_sites(ast.parse(seeded))
    assert any("'<br>'" in p for p in problems)
    assert any("new_html" in p for p in problems)


# ---------------------------------------------------------------------------
# Fallback text when the snippet is empty (operator precedence)
# ---------------------------------------------------------------------------

def test_display_text_fallback_keeps_page_text_when_snippet_is_empty():
    """Title/shelfmark results have an empty snippet. Quick View must still show
    the page text. Every ``display_text = <expr of current_text/snippet>``
    assignment in render_content is evaluated directly from the source."""
    render_content = _func(_func(_tree(), "open_advanced_dialog"), "render_content")
    exprs = []
    for n in ast.walk(render_content):
        if isinstance(n, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "display_text" for t in n.targets):
            names = {x.id for x in ast.walk(n.value) if isinstance(x, ast.Name)}
            if names and names <= {"current_text", "snippet"}:
                exprs.append(n)
    assert len(exprs) >= 2, "expected the fallback assignments in render_content"
    for n in exprs:
        code = compile(ast.Expression(n.value), "<display_text>", "eval")
        assert eval(code, {}, {"current_text": "page text", "snippet": ""}) == "page text", n.lineno
        assert eval(code, {}, {"current_text": "page text", "snippet": None}) == "page text", n.lineno
        assert eval(code, {}, {"current_text": "", "snippet": "a *b* c"}) == "a b c", n.lineno
        assert eval(code, {}, {"current_text": None, "snippet": ""}) == "", n.lineno
