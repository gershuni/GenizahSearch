# -*- coding: utf-8 -*-
"""The sidebar announces letter-level search -- and only where it exists.

Owner request, 2026-08-25: promote the new method more loudly, "on the main
sidebar parallel item, something that will announce the new feature".

The badge slot already exists (the Atlas and Computed-Identifications entries
use it for "Beta"), so the only thing that can go wrong is the gate. It must be
`passage_available()` -- the flag ANDed with an index that actually loaded --
and not the flag alone, because a box without the index hides the method
selector entirely: the badge would advertise a control the page does not draw.
That is the same failure `web/main.py`'s own comments describe for the Atlas
nav entry, which is why this follows its shape exactly.

Read from the AST of web/main.py rather than by importing it: `create_layout`
has NiceGUI page side effects, and the whole assertion is about which
predicate appears in one tuple.
"""
from __future__ import annotations

import ast
import os

import pytest

MAIN_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'web', 'main.py',
)


def _source() -> str:
    with open(MAIN_PATH, encoding='utf-8') as fh:
        return fh.read()


def _nav_tuple(path: str) -> ast.Tuple:
    """The nav_items entry whose first element is `path`, from create_layout.

    The LENGTH check is load-bearing, not decoration. A nav entry is the
    4-tuple `(path, icon, label, badge)`. Matching on "first element is
    `path`" alone was ambiguous, and on 2026-08-26 it silently became wrong:
    `_WHATS_NEW_SUPPRESSED_ON = ('/parallels', '/help')` was added EARLIER in
    the same function, so `ast.walk` reached it first and every assertion below
    was made against the suppression list instead of the nav entry. Nothing
    about the badge had changed; all five tests failed anyway.
    """
    src = _source()
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.FunctionDef)
                and node.name == 'create_layout'):
            continue
        for sub in ast.walk(node):
            if (isinstance(sub, ast.Tuple) and len(sub.elts) == 4
                    and isinstance(sub.elts[0], ast.Constant)
                    and sub.elts[0].value == path):
                return sub
        raise AssertionError(f'no 4-element nav entry for {path} in create_layout')
    raise AssertionError('create_layout not found in web/main.py')


def _calls_in(node) -> set:
    return {
        sub.func.id for sub in ast.walk(node)
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
    }


def test_the_parallels_nav_entry_carries_a_badge():
    entry = _nav_tuple('/parallels')
    assert len(entry.elts) == 4, 'nav entries are (path, icon, label, badge)'
    badge = entry.elts[3]
    assert not (isinstance(badge, ast.Constant) and badge.value is None), (
        'the parallels entry has no badge -- nothing announces the new method'
    )


def test_the_badge_is_gated_on_a_loaded_index_not_on_the_flag():
    """A box with the flag on and no index hides the method selector, so the
    badge would announce a control that is not on screen."""
    badge = _nav_tuple('/parallels').elts[3]
    assert isinstance(badge, ast.IfExp), (
        'the badge must be conditional, not unconditional'
    )
    assert 'passage_available' in _calls_in(badge.test), (
        'the badge must gate on passage_available() -- flag AND a loaded '
        'index -- never on PASSAGE_PARALLELS_ENABLED alone'
    )


def test_the_badge_text_goes_through_tr():
    """The sidebar renders in Hebrew for most of this corpus's readers."""
    badge = _nav_tuple('/parallels').elts[3]
    assert isinstance(badge.body, ast.Call), 'badge text must be tr(...)'
    assert getattr(badge.body.func, 'id', None) == 'tr'


def test_the_predicate_is_actually_imported():
    """An AST test that only reads a name would pass on a NameError."""
    src = _source()
    assert 'passage_available' in src
    import web.passage_assets as pa
    assert callable(pa.passage_available)


@pytest.mark.parametrize('other', ['/atlas', '/computed-identifications'])
def test_the_other_gated_entries_are_untouched(other):
    """This change must not have widened or narrowed anyone else's gate."""
    entry = _nav_tuple(other)
    assert len(entry.elts) == 4


def test_the_badge_says_what_is_new():
    """"New" alone does not tell a returning reader what changed.

    The exclamation sits after "New", not at the end: it marks the novelty,
    not the feature, and it mirrors the Hebrew the badge renders in
    (owner, 2026-08-25).
    """
    badge = _nav_tuple('/parallels').elts[3]
    text = badge.body.args[0]
    assert isinstance(text, ast.Constant)
    assert text.value == 'New! Fast search feature'


def test_the_badge_actually_has_a_hebrew_translation():
    """`tr()` falls back to the ENGLISH string when a key is missing, so an
    untranslated badge is invisible to every other test here -- it renders,
    it just renders in the wrong language beside a Hebrew nav label.
    Owner-reported once already (2026-08-25)."""
    from web.translations import tr, set_language, get_language
    badge = _nav_tuple('/parallels').elts[3]
    english = badge.body.args[0].value

    saved = get_language()
    try:
        set_language('he')
        rendered = tr(english)
    finally:
        set_language(saved)

    assert rendered != english, f'{english!r} has no Hebrew entry'
    assert any('֐' <= ch <= 'ת' for ch in rendered), (
        f'{english!r} translates to {rendered!r}, which has no Hebrew letters'
    )
