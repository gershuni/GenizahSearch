# -*- coding: utf-8 -*-
"""ONE definition of "these two strings are the same manuscript".

`libraries.csv` writes a shelfmark one way and the app's own exports write it
another -- 'Cambridge University Library Ms. T-S NS 312' against
'Ms. T-S NS 312', 'Or. 2116.12.A.2' against 'Or.2116.12a.2'. Joining two
result sets on the raw string therefore fails SILENTLY: the rows do not
collide, so a run looks like it found hundreds of new manuscripts and its
recall looks catastrophic. That is not hypothetical -- it cost two wrong
measurements in one session (2026-08-24), the first reporting 22% recall for
a policy whose real figure was 67%.

A broken join always fails in the flattering direction for a NEW method
(everything looks novel) and the damning direction for recall (nothing looks
recovered), so it is exactly the bug that survives review.

Two layers, deliberately separate:

* `shelfmark_key` -- mechanical. Case, punctuation and library-name prefixes
  carry no identity, so they go. Derivable, needs no maintenance.
* `ALIASES` -- hand-verified. Pairs no rule can derive, because the two
  catalogues genuinely disagree about the name ('Catalogue Halper,
  Philadelphia 62' is 'Ms. Genizah 62'). Every entry was checked against the
  catalogue record by eye; nothing belongs here that `shelfmark_key` can do.

Consumers: scripts/compare_passage_policies.py, scripts/score_antiochus_deck.py.
"""
from __future__ import annotations

import json
import os

# Library-name prefixes that libraries.csv embeds in `call_numbers` but the
# app's own exports do not.
_LIB_PREFIXES = (
    'cambridge university library', 'the bodleian libraries',
    'bodleian libraries', 'university of oxford', 'the british library',
    'british library', 'the jewish theological seminary of america',
    'jewish theological seminary of america', 'jewish theological seminary',
    'the national library of russia', 'national library of russia',
    'national library of israel', 'the university of manchester library',
    'university of manchester library', 'alliance israelite universelle',
    'westminster college', 'katz center', 'oriental studies library',
    'adler, elkan nathan', 'adler elkan nathan',
)

_ALIAS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'eval', 'antiochus', 'aliases.json',
)


def shelfmark_key(shelfmark: str) -> str:
    """Case/punctuation/library-prefix-free join key for a shelfmark.

    'Cambridge University Library Ms. T-S NS 312' and 'Ms. T-S NS 312' are
    the same manuscript written two ways; so are 'Or. 2116.12.A.2' and
    'Or.2116.12a.2'. Everything non-alphanumeric goes, because the variation
    is entirely in spacing, dots and case.
    """
    s = (shelfmark or '').lower().strip()
    for pref in sorted(_LIB_PREFIXES, key=len, reverse=True):
        s = s.replace(pref, ' ')
    s = s.replace('mss.', ' ').replace('ms.', ' ')
    return ''.join(ch for ch in s if ch.isalnum())


def load_aliases(path: str | None = None) -> dict:
    """Return {variant_key: canonical_key} from the audited alias table.

    Missing file is not an error: the mechanical key alone is a valid (just
    slightly lossier) join, and a scorer that refused to run without the
    table would be worse than one that runs and reports six fewer matches.
    """
    path = path or _ALIAS_FILE
    try:
        with open(path, encoding='utf-8') as fh:
            pairs = json.load(fh)['aliases']
    except (OSError, ValueError, KeyError):
        return {}
    out = {}
    for entry in pairs:
        canon = shelfmark_key(entry['canonical'])
        for variant in entry['variants']:
            vk = shelfmark_key(variant)
            if vk and vk != canon:
                out[vk] = canon
    return out


def canonical_key(shelfmark: str, aliases: dict | None = None) -> str:
    """`shelfmark_key`, then one alias hop. THE join key for eval work."""
    key = shelfmark_key(shelfmark)
    if aliases is None:
        aliases = load_aliases()
    return aliases.get(key, key)
