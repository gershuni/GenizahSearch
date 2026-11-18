# -*- coding: utf-8 -*-
"""What the search box says in each search mode: a short lead and an example query.

The owner's design (2026-11-18): the lead, in small letters, says what to type in the mode
chosen; the box's grey placeholder holds only an example, drawn at random from the mode's
examples each time the page opens or the mode changes, so a reader who sees the box many times
a day meets a different one (and in Responsa and Regex mode, a different operator). On the
website the lead is the box's label, on its top border as the Mode and Gap fields have theirs
(web/pages/search.py); on the desktop it heads the box's tooltip above the shortcuts
(desktop/catalog_search.py::show_mode_examples). Every example finds results on the corpus (run
on the local index, libraries.csv and catalog_text.db the same day). PGP Tags has no text box:
it and any other mode without an entry here get the text modes'.

The placeholder lead "For example:" sets an example that is a query typed into a right-to-left
box, so the example is kept right to left (isolated with RLI ... PDI) whatever the language
around it, and an English placeholder is isolated left to right as a whole: the website's box is
always right to left, where "For example:" would otherwise show its colon on the wrong side.
"""
from __future__ import annotations

import random
import unicodedata
from typing import Callable, Sequence

LRI, RLI, PDI = '⁦', '⁧', '⁩'

# The lead of each kind of search. Translations in shared/genizah_translations.py, beside
# "Enter Hebrew text to search".
LEAD_TEXT = 'Type a term to search'
LEAD_ADVANCED = 'Type words with operators for an advanced search'
LEAD_TITLE = 'Type a term to search the titles'
LEAD_CATALOG = 'Type a term to search the catalog data'
LEAD_SHELFMARK = 'Type a string to search the shelfmarks'
EXAMPLE_TEMPLATE = 'For example: {query}'

# The examples the owner approved on 2026-11-18. Each found results on the local data that day
# (pages: Exact / Variants Basic / Fuzzy; manuscripts for Title and Catalog), and none took more
# than ~30 s; slower or near-empty candidates were left out.
TEXT_EXAMPLES = (
    'יוחנן כהן גדול',        # 101 / 114 / 161
    'נשמת כל חי',            # 145 / 190 / 203
    'רב סעדיה גאון',         # 51 / 59 / 76
    'יום הכיפורים',          # 254 / 270 / 5,370
    'שמע ישראל',             # 3,208 / 3,281 / 4,254
    'אלשיך אלאגל',           # 352 / 357 / 5,041
)
# Each shows a different operator (Help, Responsa-Style Search).
RESPONSA_EXAMPLES = (
    '#%יוחנן (כהן גדול/כ"ג)',   # prefixes, plene spelling, alternatives: 118
    'כהנ* גדו*',                # * inside a word: 3,743
    'ראובן [3] שמעון',          # a gap of N words: 1,077
    'פסח#',                     # suffixes: 11,937
    '|נשמת כל חי',              # at the start of a line: 54
    '(שטר/כתובה) [5] דינר*',    # alternatives, a gap and * together: 27
    'כהן גדול AND יוחנן',       # anywhere in the transcription: 1,418
)
REGEX_EXAMPLES = (
    r'יו?חנן\s+כהן\s+גדול',       # 116
    r'\bא[א-ת]{3}\b',              # Help's own example: 58,155
    r'כהנ(ים|י)\s+גדולים',         # 382
    r'אל(שיך|רב)\s+אל[א-ת]+',      # 5,451
    r'^ברוך אתה',                  # 233
    r'ש[מנ]ואל\s+הנגיד',           # 172
)
TITLE_EXAMPLES = ('יוסיפון', 'מורה נבוכים', 'סדר עולם', 'הלכות גדולות')   # 96, 69, 66, 797
SHELFMARK_EXAMPLES = ('T-S NS 13.15', 'ENA 2598.15', 'T-S AS 190.288', 'Or.1080 7.5')
CATALOG_EXAMPLES = (
    'פיוט "יוחנן הכהן"',    # 82
    '"כהן גדול"',           # 102
    'רב האיי גאון',         # 305
    '"שמואל הנגיד"',        # 329
    'כתובה פוסטאט',         # 181
)

#: mode (the website's and the engine's names) -> (lead, its examples)
MODE_EXAMPLES = {
    'exact': (LEAD_TEXT, TEXT_EXAMPLES),
    'variants': (LEAD_TEXT, TEXT_EXAMPLES),
    'variants_extended': (LEAD_TEXT, TEXT_EXAMPLES),
    'variants_maximum': (LEAD_TEXT, TEXT_EXAMPLES),
    'fuzzy': (LEAD_TEXT, TEXT_EXAMPLES),
    'responsa': (LEAD_ADVANCED, RESPONSA_EXAMPLES),
    'Regex': (LEAD_ADVANCED, REGEX_EXAMPLES),
    'Title': (LEAD_TITLE, TITLE_EXAMPLES),
    'Catalog': (LEAD_CATALOG, CATALOG_EXAMPLES),
    'Shelfmark': (LEAD_SHELFMARK, SHELFMARK_EXAMPLES),
}

# The desktop names Exact 'literal' (desktop/catalog_search.py::step_mode).
_ALIASES = {'literal': 'exact'}


def _entry(mode: str):
    return MODE_EXAMPLES.get(_ALIASES.get(mode, mode), MODE_EXAMPLES['exact'])


def _reads_right_to_left(text: str) -> bool:
    """Whether the first letter of *text* is a right-to-left one (its language's)."""
    for ch in text:
        kind = unicodedata.bidirectional(ch)
        if kind == 'L':
            return False
        if kind in ('R', 'AL'):
            return True
    return False


def mode_lead(mode: str, tr: Callable[[str], str]) -> str:
    """What to type in *mode*, in the language *tr* translates into."""
    return tr(_entry(mode)[0])


def mode_placeholder(mode: str, tr: Callable[[str], str],
                     choose: Callable[[Sequence[str]], str] = random.choice) -> str:
    """The box's placeholder in *mode*: one of its examples (*choose* picks it), after "For
    example:" in the language *tr* translates into."""
    template = tr(EXAMPLE_TEMPLATE)
    text = template.format(query=RLI + choose(_entry(mode)[1]) + PDI)
    return text if _reads_right_to_left(template) else LRI + text + PDI
