# -*- coding: utf-8 -*-
"""Every `tr()` literal in `web/main.py` must resolve to Hebrew.

Written after a live miss on 2026-08-26. The What's New banner's wording was
revised, the new English/Hebrew pairs were added to the translation table, and
the `tr()` calls in `web/main.py` were left asking for the OLD keys. `tr()`
does not raise on a missing key -- it returns its argument -- so nothing failed
anywhere. The banner rendered, in English, on a Hebrew page, and the only thing
that caught it was the owner looking at the screen.

That is the shape worth guarding: not a crash, not a wrong value, but a silent
fallback that looks like working software. `web/main.py` is where the shell
lives -- nav, banner, drawer -- so an untranslated string here is on every
page.

Scope is deliberately this one file. A tree-wide version would be better and is
a fair follow-up, but it would have to adopt whatever drift already exists
elsewhere; this file is at zero and can be held there.
"""
from __future__ import annotations

import ast
import io
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from genizah_translations import TRANSLATIONS  # noqa: E402

MAIN = os.path.join(REPO_ROOT, 'web', 'main.py')

# The Hebrew block, plus the presentation forms. A value that contains none of
# these is not a translation, whatever else it is.
_HEBREW = range(0x0590, 0x0600)


def _is_hebrew(text: str) -> bool:
    return any(ord(ch) in _HEBREW for ch in text)


def _literal_tr_keys():
    """Every `tr("...")` in web/main.py whose argument is a plain string.

    Implicit concatenation -- `tr("a " "b")` -- is folded by the parser into a
    single Constant before this sees it, which matters because the banner's
    longer entry is written that way to stay inside the line length. Calls
    whose argument is an f-string or a variable are skipped: they cannot be
    resolved without running the code, and pretending otherwise would make
    this gate lie about its own coverage.
    """
    tree = ast.parse(io.open(MAIN, encoding='utf-8').read())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Name) and func.id == 'tr'):
            continue
        if not node.args:
            continue
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            yield node.lineno, arg.value


_KEYS = sorted(set(_literal_tr_keys()))


def test_the_gate_is_looking_at_something():
    """A parser that silently matched nothing would pass every case below."""
    assert len(_KEYS) > 40, (
        'only %d literal tr() calls found in web/main.py -- the extractor is '
        'probably broken, and a gate over an empty set proves nothing'
        % len(_KEYS))


@pytest.mark.parametrize('lineno,key', _KEYS, ids=[k[:45] for _, k in _KEYS])
def test_every_literal_tr_key_has_hebrew(lineno, key):
    value = TRANSLATIONS.get(key)
    assert value is not None, (
        'web/main.py:%d asks tr() for a key that is not in TRANSLATIONS, so it '
        'renders as English on a Hebrew page and nothing raises:\n  %r\n'
        'Usually this means the wording was changed in one place and not the '
        'other.' % (lineno, key))
    assert _is_hebrew(value), (
        'web/main.py:%d has a TRANSLATIONS entry with no Hebrew in it:\n'
        '  %r -> %r' % (lineno, key, value))
