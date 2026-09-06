# -*- coding: utf-8 -*-
"""No dict in `genizah_translations.py` may declare a key twice.

Python keeps the LAST value for a duplicated key and discards the rest in
silence. So a duplicate is not a tidiness problem: it means some Hebrew string
on the site is one nobody chose, and the entry a translator edited may be dead
code. 165 shadowed entries were removed on 2026-09-04 (141 keys, 140 of them in
`TRANSLATIONS`), and the dedup was behaviour-preserving by construction: the
winner stayed, so the site renders exactly what it rendered before.

THE TRAP, which cost a reverted first attempt
---------------------------------------------
"Duplicate" means duplicate WITHIN ONE dict. This module holds 42 dict literals
-- `TRANSLATIONS`, `LIBRARY_CODES_HE` and 40 smaller ones -- and the same string
is legitimately a key in more than one of them. `'Oxford'` is `'אוקספורד'` in
`TRANSLATIONS` and the full Bodleian name in `LIBRARY_CODES_HE`. A file-wide key
count says those are duplicates; deleting one empties a table. That is why this
test walks each dict separately, and why `test_a_key_may_appear_in_two_different
_tables` exists as its control -- without it, someone "simplifying" this file to
a single Counter would reintroduce the bug and still be green.
"""

from __future__ import annotations

import ast
import collections
from pathlib import Path

TABLE = Path(__file__).resolve().parents[1] / 'genizah_translations.py'


def _dicts():
    tree = ast.parse(TABLE.read_text(encoding='utf-8'))
    return [n for n in ast.walk(tree) if isinstance(n, ast.Dict)]


def _string_keys(node):
    return [k.value for k in node.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)]


def test_no_dict_declares_a_key_twice():
    offenders = []
    for node in _dicts():
        counts = collections.Counter(_string_keys(node))
        for key, n in counts.items():
            if n > 1:
                offenders.append((node.lineno, key, n))
    assert not offenders, (
        'a duplicated key means Python silently keeps the LAST value and the '
        'other entries are dead:\n' + '\n'.join(
            '  dict at line %d: %r appears %d times' % o for o in offenders[:20]))


def test_a_key_may_appear_in_two_different_tables():
    """The CONTROL for the test above, and the reason it is per-dict.

    If this ever fails, the two tables have been merged or one has been emptied
    -- and a file-wide duplicate check would call this pair a duplicate and
    delete one of them, which is exactly the mistake that got reverted while
    this dedup was being done.
    """
    import sys
    sys.path.insert(0, str(TABLE.parent))
    from genizah_translations import LIBRARY_CODES_HE, TRANSLATIONS
    assert TRANSLATIONS.get('Oxford'), 'TRANSLATIONS lost its Oxford entry'
    assert LIBRARY_CODES_HE.get('Oxford'), 'LIBRARY_CODES_HE lost its Oxford entry'
    assert TRANSLATIONS['Oxford'] != LIBRARY_CODES_HE['Oxford'], (
        'the two Oxford values are now identical, so this control no longer '
        'demonstrates that a cross-table key collision is legitimate')


def test_the_tables_are_still_the_expected_size():
    """A dedup that removed a LIVE entry would show up here.

    Not a style ceiling -- these are floors. The counts may grow; a sudden drop
    means entries were lost, which is the one way a mechanical dedup can go
    wrong.
    """
    import sys
    sys.path.insert(0, str(TABLE.parent))
    from genizah_translations import LIBRARY_CODES_HE, TRANSLATIONS
    assert len(TRANSLATIONS) >= 3758, len(TRANSLATIONS)
    assert len(LIBRARY_CODES_HE) >= 89, len(LIBRARY_CODES_HE)


# REMOVED: test_no_section_comment_is_left_heading_nothing.
#
# It asserted that no `# --- Section ---` header is followed by another header
# with nothing between them. The dedup DID orphan one such header
# ("# --- Community Tab ---", whose whole section turned out to be duplicates);
# that was found by the dedup script's own report and fixed by hand.
#
# As a STANDING test it was wrong. It failed on a pre-existing and deliberate
# pattern: a run of annotation comments around line 4110 that record why no keys
# were added ("Feature 9: material display (pure Python helper, no new tr()
# keys)", "'Zoom in' / 'Zoom out' already in TRANSLATIONS"). Those headers label
# nothing ON PURPOSE. Whitelisting them would have been tailoring the test to
# the fixture rather than to a property, and the property itself -- "this one
# edit did not leave a stray header" -- is not an invariant anything can hold.
