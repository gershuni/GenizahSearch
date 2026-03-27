# -*- coding: utf-8 -*-
"""
Pesach 2026 Easter Eggs -- time-limited features active through April 20, 2026.

Three features:
1. "Ma Nishtana" search easter egg -- special card for Pesach-related queries
2. Pesach Genizah Collection banner -- random Pesach fragment on home page
3. Four Sons of the Genizah -- playful session behavior classification
"""

from datetime import datetime

# Pesach 2026: evening April 12 - evening April 20
PESACH_END = datetime(2026, 4, 21)  # inclusive buffer (full day April 20)

EMOJI_WINE = '\U0001f377'      # wine glass
EMOJI_PAGE = '\U0001f4c4'      # page facing up


def is_pesach_season() -> bool:
    """Check if we're in the Pesach season (now through April 20, 2026)."""
    return datetime.now() < PESACH_END


# ============================================================================
# Pesach Genizah Fragments — 60 Haggadah shel Pesach manuscripts
# (verified sys_ids from libraries.csv, out of 488 available)
# ============================================================================

PESACH_FRAGMENTS = [
    {'shelfmark': 'T-S NS 123.78', 'sys_id': '990051484580205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 271.42', 'sys_id': '990051643790205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 271.19', 'sys_id': '990051643560205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 197.36', 'sys_id': '990051573410205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 159.207', 'sys_id': '990051539270205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Or. 9772C', 'sys_id': '990001235240205171', 'title': 'הגדה של פסח (קטעים)'},
    {'shelfmark': 'Ms. 1358', 'sys_id': '990001986190205171', 'title': 'הגדה של פסח (קטע)'},
    {'shelfmark': 'Ms. EVR II A 2246', 'sys_id': '990001457720205171', 'title': 'הגדה של פסח כמנהג קראים (קטע)'},
    {'shelfmark': 'Ms. EVR II A 2510', 'sys_id': '990001460360205171', 'title': 'הגדה של פסח כמנהג קראים'},
    {'shelfmark': 'T-S H2.137', 'sys_id': '990051184040205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. I 105', 'sys_id': '990053796840205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. EVR II A 1652', 'sys_id': '990001451730205171', 'title': 'הגדה של פסח כמנהג קראים (קטע)'},
    {'shelfmark': 'T-S AS 86.16', 'sys_id': '990051954390205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S AS 107.85', 'sys_id': '990052025830205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. EVR II A 2875', 'sys_id': '990001464150205171', 'title': 'הגדה של פסח כמנהג קראים'},
    {'shelfmark': 'Ms. I 102', 'sys_id': '990053796790205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 294.80', 'sys_id': '990051680110205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S Ar.36.135', 'sys_id': '990051294150205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. EVR II A 2314', 'sys_id': '990001458370205171', 'title': 'הגדה של פסח כמנהג קראים'},
    {'shelfmark': 'T-S NS 150.49', 'sys_id': '990051521040205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. IV A 248', 'sys_id': '990044280600205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. 9560', 'sys_id': '990000826620205171', 'title': 'הגדה של פסח (בלתי שלם)'},
    {'shelfmark': 'T-S Ar.36.17', 'sys_id': '990051292970205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 153.66', 'sys_id': '990051528160205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S AS 79.36', 'sys_id': '990051940340205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 32.3', 'sys_id': '990001989550205171', 'title': 'הגדה של פסח (קטע)'},
    {'shelfmark': 'Ms. 1784', 'sys_id': '990053523760205171', 'title': 'הגדה של פסח (קטע)'},
    {'shelfmark': 'T-S H2.126', 'sys_id': '990051183930205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. Genizah 183', 'sys_id': '990053956710205171', 'title': 'הגדה של פסח (קטע)'},
    {'shelfmark': 'MS heb. f.44/33', 'sys_id': '990053474970205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S NS 271.227', 'sys_id': '990051645650205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. B 5006', 'sys_id': '990024883490205171', 'title': 'הגדה של פסח (קטע)'},
    {'shelfmark': 'T-S Ar.36.24', 'sys_id': '990051293040205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. Or. 1080 10.20', 'sys_id': '990026202260205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S Ar.36.18', 'sys_id': '990051292980205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. IV A 266', 'sys_id': '990044286070205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. EVR II A 1402', 'sys_id': '990001448940205171', 'title': 'הגדה של פסח כמנהג קראים'},
    {'shelfmark': 'T-S NS 156.40', 'sys_id': '990051533030205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'Ms. EVR II A 2928', 'sys_id': '990001464740205171', 'title': 'הגדה של פסח כמנהג קראים'},
    {'shelfmark': 'T-S AS 62.10', 'sys_id': '990051892010205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S AS 107.236', 'sys_id': '990052027340205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S H2.158', 'sys_id': '990051184250205171', 'title': 'הגדה של פסח'},
    {'shelfmark': 'T-S H2.143', 'sys_id': '990051184100205171', 'title': 'הגדה של פסח'},
]


def get_random_pesach_fragments(n: int = 3) -> list:
    """Return n random Pesach fragments (no duplicates)."""
    import random
    return random.sample(PESACH_FRAGMENTS, min(n, len(PESACH_FRAGMENTS)))
