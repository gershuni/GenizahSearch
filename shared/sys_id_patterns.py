# -*- coding: utf-8 -*-
"""Canonical sys_id extraction patterns for BOTH namespaces.

Before this module existed, roughly two dozen sites hand-rolled a sys_id regex
inline and drifted into two incompatible dialects (``99\\d{8,}`` vs
``(?:99|97)\\d{8,}``). This module is the single definition; the repo-grep lint
in ``tests/test_sys_id_patterns.py`` fails CI if a new site hand-rolls one.

Two namespaces, deliberately disjoint
-------------------------------------
``99`` is the **Genizah corpus** namespace -- Alma MMS ids (mostly the NLI
institution suffix ``0205171``) plus the Phase-85 synthetics
(``99 + InventoryId.zfill(10) + 000000``).

``97`` is the **LOCAL "My Library" namespace** (Phase 95,
``shared/local_sys_id.py``): ``97 + machine_id(8) + content_hash(8)``, generated
client-side on the DESKTOP app for a user's own files. It is not a Genizah
record and never enters the corpus.

So ``97`` IS a real prefix -- but NOT a corpus prefix. Do not "unify" the two
constants below into one; they answer different questions.

Evidence (re-measure before changing any of this; the corpus grows)
-------------------------------------------------------------------
``libraries.csv`` at 2026-08-25, all 255,723 records:
  * 255,723 begin ``99``; ZERO begin ``97``
  * every one is exactly 18 digits
  * that includes all 473 ``NLI``-library rows -- NLI is NOT a source of ``97``
The owner's measurement of the live Tantivy index (759,224 records) likewise
found zero ``97``. Re-run ``scripts/check_sys_id_prefixes.py`` to re-verify.

Why the leading ``(?<!\\d)`` boundary is load-bearing
-----------------------------------------------------
``re.search`` scans anywhere, so a corpus-only pattern applied to a LOCAL header
does NOT fail cleanly -- it can match a ``99`` that happens to fall INSIDE the
LOCAL id's random digits and return a truncated, wrong sys_id. Measured over
200,000 synthetic LOCAL ids, the unanchored ``(99\\d{8,})`` mis-fires on 6.36%
of them, e.g. ``970993169503583183`` -> ``993169503583183``. Silent corruption,
not a silent drop.

The boundary makes the miss clean: a LOCAL id is 18 digits total, so an
interior ``99`` is always preceded by a digit and the lookbehind rejects it,
while the id itself starts ``97`` and never matches. A corpus header
(``{sys_id}_{IE..}_{P..}_{FL..}``) is unaffected -- its sys_id sits at a
non-digit boundary.

The trailing ``(?!\\d)`` is redundant while ``\\d{8,}`` stays greedy (greedy
already consumes the whole digit run). It is kept so the pattern stays correct
under composition and if the quantifier is ever made lazy.
"""
from __future__ import annotations

import re
from typing import Optional

#: Genizah CORPUS namespace only (``99``). Use this for anything that resolves a
#: record against corpus data: exports, grouping, allowlists, metadata lookups.
CORPUS_SYS_ID_PATTERN = r'(?<!\d)(99\d{8,})(?!\d)'

#: Corpus OR LOCAL (``99`` or ``97``). Use ONLY where a LOCAL "My Library"
#: header can genuinely arrive -- i.e. the desktop-side namespace-agnostic
#: parsers. On web, LOCAL cannot exist; prefer the corpus pattern there.
ANY_SYS_ID_PATTERN = r'(?<!\d)((?:99|97)\d{8,})(?!\d)'

CORPUS_SYS_ID_RE = re.compile(CORPUS_SYS_ID_PATTERN)
ANY_SYS_ID_RE = re.compile(ANY_SYS_ID_PATTERN)


def extract_corpus_sys_id(text: object) -> Optional[str]:
    """First CORPUS (``99``) sys_id in ``text``, or None.

    Returns None for a LOCAL header rather than a truncated mis-match.

        >>> extract_corpus_sys_id("990051620920205171_IE167198813_P000003_FL167198817")
        '990051620920205171'
        >>> extract_corpus_sys_id("970993169503583183_LOCAL_P3_F0042") is None
        True
    """
    if not text:
        return None
    m = CORPUS_SYS_ID_RE.search(str(text))
    return m.group(1) if m else None


def extract_any_sys_id(text: object) -> Optional[str]:
    """First sys_id in ``text`` from EITHER namespace, or None.

        >>> extract_any_sys_id("970012345601234567_LOCAL_P3_F0042")
        '970012345601234567'
        >>> extract_any_sys_id("990025143260205171_IE1_P5_FL2")
        '990025143260205171'
    """
    if not text:
        return None
    m = ANY_SYS_ID_RE.search(str(text))
    return m.group(1) if m else None
