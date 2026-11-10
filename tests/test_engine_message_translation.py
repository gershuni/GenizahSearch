"""The search engine's numbered messages reach a Hebrew page in Hebrew.

On the website the engine writes its notes in English (the process's language, which the
API and MCP want), with the numbers already in them: "a query with * reached 5,252
manuscripts, more than the 2,000 ...". The page used to look each note up as it was, so a
filled-in note never matched its key and showed in English beside the Hebrew results line
(owner, 2026-10-23, v9.9.0). `web.translations.tr_engine_message` recognises the templates
and fills the visitor's language instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from shared.genizah_translations import TRANSLATIONS
from shared.responsa_match import GUARD_REFUSAL_TEXT, SHORT_STAR_TEXT
from shared.search_engine import (
    CROSSING_NOTE_FAILED,
    CROSSING_NOTE_LONG,
    CROSSING_NOTE_TOO_MANY,
    FILLED_MESSAGE_TEMPLATES,
)
from web.translations import tr_engine_message, using_language

ROOT = Path(__file__).resolve().parents[1]
_LATIN = re.compile(r'[A-Za-z]')


def _english_left(text):
    """Latin letters in a Hebrew text, but for the "N+" the Hebrew itself writes."""
    return _LATIN.search(text.replace('N+', ''))


def _fields(template):
    return set(re.findall(r'\{(\w+)(?::[^}]*)?\}', template))


@pytest.mark.parametrize('template', FILLED_MESSAGE_TEMPLATES)
def test_every_numbered_message_has_a_hebrew_text_with_the_same_numbers(template):
    assert template in TRANSLATIONS, template
    assert _fields(TRANSLATIONS[template]) == _fields(template)


def test_the_too_many_note_from_the_screenshot_is_hebrew_with_its_numbers():
    english = CROSSING_NOTE_TOO_MANY.format(n=5252, limit=2000)
    with using_language('he'):
        hebrew = tr_engine_message(english)
    assert hebrew == TRANSLATIONS[CROSSING_NOTE_TOO_MANY].format(n=5252, limit=2000)
    assert '5,252' in hebrew and '2,000' in hebrew
    assert not _english_left(hebrew), hebrew


def test_the_long_side_note_is_hebrew():
    english = CROSSING_NOTE_LONG.format(k=30)
    with using_language('he'):
        hebrew = tr_engine_message(english)
    assert hebrew == TRANSLATIONS[CROSSING_NOTE_LONG].format(k=30)


def test_the_refusal_is_hebrew_with_its_numbers():
    english = GUARD_REFUSAL_TEXT.format(limit=500, count='500+')
    with using_language('he'):
        hebrew = tr_engine_message(english)
    assert hebrew == TRANSLATIONS[GUARD_REFUSAL_TEXT].format(limit=500, count='500+')
    assert '500+' in hebrew


def test_a_joined_line_is_hebrew_in_every_part():
    """The results line joins the guard's note and the page-break notes with "; ", and
    CROSSING_NOTE_FAILED has a "; " of its own: splitting there left both halves English."""
    english = '; '.join([SHORT_STAR_TEXT, CROSSING_NOTE_TOO_MANY.format(n=5252, limit=2000),
                         CROSSING_NOTE_FAILED])
    with using_language('he'):
        hebrew = tr_engine_message(english)
    assert hebrew == '; '.join([TRANSLATIONS[SHORT_STAR_TEXT],
                                TRANSLATIONS[CROSSING_NOTE_TOO_MANY].format(n=5252, limit=2000),
                                TRANSLATIONS[CROSSING_NOTE_FAILED]])
    assert not _english_left(hebrew), hebrew


def test_english_pages_and_unknown_text_are_left_as_they_are():
    english = CROSSING_NOTE_TOO_MANY.format(n=5252, limit=2000)
    with using_language('en'):
        assert tr_engine_message(english) == english
    with using_language('he'):
        assert tr_engine_message('Something the engine never says') == 'Something the engine never says'
        assert tr_engine_message('') == ''


def test_every_numbered_engine_message_is_registered():
    """A message filled in after _tr() and missing from FILLED_MESSAGE_TEMPLATES would
    reach a Hebrew page in English again: no inline template, every name registered."""
    import shared.responsa_match as responsa_match
    import shared.search_engine as search_engine
    found = 0
    for module in (search_engine, responsa_match):
        source = Path(module.__file__).read_text(encoding='utf-8')
        assert not re.search(r'_tr\(\s*["\'][^)]*\)\s*\.format\(', source), module.__name__
        for name in re.findall(r'_tr\((\w+)\)\s*\.format\(', source):
            assert getattr(module, name) in FILLED_MESSAGE_TEMPLATES, (module.__name__, name)
            found += 1
    assert found >= 4, 'the scan found fewer call sites than exist today: is it still reading them?'


def test_the_search_page_translates_through_it():
    """The two places the page shows what the engine wrote: the results line's note and
    the refusal message."""
    source = (ROOT / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
    assert '(" · " + tr_engine_message(note)) if note' in source
    assert 'error_msg = tr_engine_message(error_msg)' in source
    assert "tr(part) for part in note.split('; ')" not in source
