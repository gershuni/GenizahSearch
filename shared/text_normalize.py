# -*- coding: utf-8 -*-
"""Hebrew text normalization helpers -- nikud stripping and diacritic folding.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains permanent same-object re-export shims so all
existing ``from genizah_core import ...`` callers continue working unchanged.

Dependencies: stdlib only (re). Zero Config, zero LOGGER, zero genizah_core.
"""

import re

# Hebrew nikud (vowel marks) Unicode range: U+0591-U+05CF (excluding letters U+05D0-U+05EA)
NIKUD_PATTERN = re.compile("[֑-׏]")


def strip_nikud(text: str) -> str:
    """Remove Hebrew vowel marks (nikud) and cantillation marks from text.

    Keeps only Hebrew letters (alef-tav) and other characters.
    """
    if not text:
        return text
    return NIKUD_PATTERN.sub("", text)


# Matches combining diacritical marks (U+0300-U+036F), apostrophe variants (ASCII and curly),
# the ASCII double-quote (U+0022 -- the common typed substitute for Hebrew gershayim in
# abbreviations), and Hebrew geresh (U+05F3), gershayim (U+05F4), curly quotes (U+2018/U+2019).
# Does NOT match Hebrew nikud (U+05B0-U+05C7).
# Expressed via re.compile with explicit chr() to avoid source-encoding issues.
COMBINING_DIACRITICALS_PATTERN = re.compile(
    "[" + chr(0x0300) + "-" + chr(0x036F)
    + chr(0x0022)   # ASCII double-quote
    + chr(0x0027)   # ASCII apostrophe
    + chr(0x05F3)   # Hebrew geresh
    + chr(0x05F4)   # Hebrew gershayim
    + chr(0x2018)   # left single quotation mark
    + chr(0x2019)   # right single quotation mark
    + "]"
)


def strip_search_diacritics(text: str) -> str:
    """Strip combining diacritical marks, apostrophe variants, and geresh/gershayim from search text.

    Removes:
    - Combining diacritical marks (U+0300-U+036F)
    - ASCII double-quote (U+0022) -- the gershayim substitute in abbreviations
    - ASCII apostrophe (U+0027)
    - Hebrew geresh (U+05F3)
    - Hebrew gershayim (U+05F4)
    - Curly single quotes (U+2018, U+2019)

    Preserves:
    - Hebrew base letters, nikud/vowel points, Latin chars, digits, punctuation

    SEED-006 P2: folding U+0022 keeps the additive content_search field
    (built from strip_search_diacritics(content)) and the content_search:
    query clause symmetric for ASCII-quote abbreviations. The stored content
    field is unchanged (the hebword tokenizer still keeps the char inside
    the token), so display and the gershayim form are unaffected.
    """
    if not text:
        return text
    return COMBINING_DIACRITICALS_PATTERN.sub("", text)
