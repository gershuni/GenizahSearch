# -*- coding: utf-8 -*-
"""The owner's Help corrections of 2026-11-05 (help_corrections.docx, tracked changes): the
parts that are behaviour rather than wording."""
from __future__ import annotations

from pathlib import Path

from web.pages import help as help_page

REPO = Path(__file__).resolve().parents[1]
HELP = (REPO / 'web' / 'pages' / 'help.py').read_text(encoding='utf-8')
HEBREW = HELP[HELP.index('def _create_hebrew_content('):]
ENGLISH = HELP[HELP.index('def _create_english_content('):HELP.index('def _create_hebrew_content(')]


def test_a_tier_shows_only_the_sample_numbers_that_were_measured():
    """Owner: "remove all these if we did not measure -- here and below". A graded tier printed
    a sample line of three "not yet measured" and an audit line repeating its status line."""
    parts = help_page._measured_sample_parts
    assert parts({}, 'he') == []
    assert parts({'draw_size': None, 'denominator': 'x'}, 'en') == []
    assert parts({'draw_size': 1200}, 'en') == [f"{help_page.DRAW_SIZE_LABEL['en']} 1,200"]
    assert len(parts({'draw_size': 40, 'denominator': 35, 'numerator': 30}, 'he')) == 3
    band = HELP[HELP.index('def _render_one_band('):HELP.index('def _measured_sample_parts(')]
    assert 'if sample_parts:' in band
    assert "for field in ('measurement_date', 'grader', 'report_id', 'audit_status'):" in band
    assert "ph['audit_pending']" not in band          # no placeholder audit line any more


def test_the_hebrew_syntax_table_shows_each_cell_in_its_own_direction():
    """In a left-to-right cell "#מילה" read as "מילה#" to a Hebrew reader: every prefix looked
    like a suffix (the owner moved the % to the end of the word to fix what the page showed;
    the parser takes it only in front). The cells take their direction from their text."""
    assert HEBREW.count(".props('dir=auto')") == 2
    assert "direction: ltr; text-align: left; white-space: nowrap;" not in HEBREW
    assert "('%מילה', 'כתיב מלא/חסר (הוספת/הסרת ו/י)'" in HEBREW      # still typed in front


def test_tags_not_chips_in_both_languages():
    assert 'chip' not in ENGLISH.lower()
    assert "צ'יפ" not in HEBREW and 'צ׳יפ' not in HEBREW
