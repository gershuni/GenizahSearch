# -*- coding: utf-8 -*-
"""The deferred /browse Related Fragments render leaves the UI language as it found it.

The UI language is a process global (web/translations.py ``_current_lang``)
that every page render sets. The Related Fragments block on /browse can render
later, after an off-loop fetch, when another visitor's render may be in
progress. It renders in the language captured when it was scheduled; it must
then put the previous value back, or the other visitor's remaining rendering
runs in this visitor's language.

``using_language`` sets the language for a synchronous block and restores the
previous value on exit (also on an exception). /browse must use it rather
than a bare ``set_language``.
"""

import ast
from pathlib import Path

import pytest

translations = pytest.importorskip('web.translations')

REPO_ROOT = Path(__file__).resolve().parent.parent
BROWSE = REPO_ROOT / 'web' / 'pages' / 'browse.py'


@pytest.fixture
def restore_language():
    saved = translations.get_language()
    yield
    translations.set_language(saved)


def test_using_language_sets_then_restores(restore_language):
    translations.set_language('en')
    seen = []
    with translations.using_language('he'):
        seen.append(translations.get_language())
    assert seen == ['he']
    assert translations.get_language() == 'en'


def test_using_language_restores_after_an_exception(restore_language):
    translations.set_language('he')
    with pytest.raises(RuntimeError):
        with translations.using_language('en'):
            raise RuntimeError('client deleted')
    assert translations.get_language() == 'he'


def _bare_set_language_calls(source):
    return sorted(n.lineno for n in ast.walk(ast.parse(source))
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                  and n.func.id == 'set_language')


def _deferred_render_uses_using_language(source):
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == '_render_joins_deferred':
            for w in ast.walk(node):
                if isinstance(w, ast.With):
                    for item in w.items:
                        ce = item.context_expr
                        if (isinstance(ce, ast.Call) and isinstance(ce.func, ast.Name)
                                and ce.func.id == 'using_language'):
                            return any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                                       and c.func.id == '_render_related_fragments'
                                       for s in w.body for c in ast.walk(s))
            return False
    raise AssertionError('_render_joins_deferred not found in web/pages/browse.py')


def test_browse_never_sets_the_language_without_restoring_it():
    src = BROWSE.read_text(encoding='utf-8')
    assert _bare_set_language_calls(src) == [], (
        "web/pages/browse.py calls set_language directly; a deferred render must use "
        "'with using_language(captured):' so the previous language is restored")
    assert _deferred_render_uses_using_language(src), (
        "_render_joins_deferred must render inside 'with using_language(...)'")


@pytest.mark.parametrize('snippet,bare,uses', [
    ("def _render_joins_deferred(d):\n    set_language(x)\n    _render_related_fragments(d)\n", 1, False),
    ("def _render_joins_deferred(d):\n    # with using_language(x):\n    _render_related_fragments(d)\n", 0, False),
    ("def _render_joins_deferred(d):\n    with using_language(x):\n        _render_related_fragments(d)\n", 0, True),
])
def test_language_detectors(snippet, bare, uses):
    """Regression guard, green before and after: proves the call-site pin can fail."""
    assert len(_bare_set_language_calls(snippet)) == bare
    assert _deferred_render_uses_using_language(snippet) is uses
