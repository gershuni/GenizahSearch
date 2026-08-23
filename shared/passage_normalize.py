# -*- coding: utf-8 -*-
"""Normalization and gram coding for the passage matcher.

Contract: docs/specs/passage-matching-algorithm.md sections 3 and 4.

Ported from the gitignored research tree (`same_work_spike/probe/scripts/
normalize.py::norm_stream` / `project_span`, and `engine_np.py::_gram_codes`).
The port is byte-exact by design and by test: every calibrated constant in the
spec -- the acceptance boundaries, the Stage-0 thresholds, the DF findings --
was measured against THAT normalizer, so any divergence here silently
invalidates all of them. `tests/test_passage_normalize.py` proves parity
against the original implementation on real corpus text.

Two normalization entry points, deliberately:

  norm_stream_fast(text) -> str
      Stream only, no offset map. Whole-string `str.translate` + one regex
      sub, so it runs at C speed. This is the BUILDER path: the index needs
      the stream and nothing else, and it runs over ~602.6M letters.

  norm_stream(text) -> (stream, offsets)
      Stream plus the offset map needed to project a matched span back onto
      displayable text. Costs a Python-level loop over surviving letters, so
      it is the DISPLAY path only -- called for the handful of records
      actually rendered, never over the corpus.

Equivalence of the two is asserted by test, not assumed: the original walks
characters one at a time, folding then range-testing each; folding the whole
string and then dropping non-letters is the same function because FINAL_FOLD
maps single letters to single letters.

NORMALIZER_VERSION is an artifact-identity input. Bump it for ANY change to
the alphabet, the folding, or the offset semantics, and every built index
must then be rejected on load. `MIN_SPAN` and other query policy do NOT
belong here (spec section 8).
"""
from __future__ import annotations

import re
import unicodedata
from array import array

import numpy as np

# Bump on any change to alphabet / folding / offset semantics.
NORMALIZER_VERSION = 1

# Hebrew base letters, alef..tav. The only characters that survive.
HEB_MIN, HEB_MAX = 0x05D0, 0x05EA

# Final-letter fold. Applied BEFORE the range test, so a final letter is
# folded and kept rather than dropped.
FINAL_FOLD = str.maketrans({
    'ך': 'כ', 'ם': 'מ', 'ן': 'נ', 'ף': 'פ', 'ץ': 'צ',
})

# Everything that is not a surviving base letter is a separator and is
# dropped: nikud, cantillation, every combining mark (including the
# Judeo-Arabic upper dot U+0307), punctuation, brackets, geresh/gershayim,
# quotes, apostrophes, digits, Latin, and ALL whitespace.
_NON_LETTER_RE = re.compile(r'[^א-ת]+')
_LETTER_RE = re.compile(r'[א-ת]')

# Gram coding (spec section 4). 27 symbols -> base-27 positional code.
# Code space is 27**5 = 14,348,907, which fits in 24 bits.
K = 5
GRAM_BASE = 27
GRAM_CODE_SPACE = GRAM_BASE ** K


def nfc(text: str) -> str:
    """NFC-normalize. Offsets returned by norm_stream index THIS string."""
    return unicodedata.normalize('NFC', text)


def norm_stream_fast(text: str) -> str:
    """Space-free normalized Hebrew letter stream. No offset map.

    The builder path. Equivalent to norm_stream(text)[0] but without the
    per-letter Python loop.
    """
    return _NON_LETTER_RE.sub('', nfc(text).translate(FINAL_FOLD))


def norm_stream(text: str) -> tuple[str, array]:
    """Return (stream, offsets).

    stream  -- base letters alef..tav, finals folded, whitespace removed
    offsets -- offsets[i] is the index IN THE NFC TEXT of stream letter i

    The display path: only call this for records being rendered.
    """
    folded = nfc(text).translate(FINAL_FOLD)
    offs = array('i')
    out = []
    for m in _LETTER_RE.finditer(folded):
        out.append(m.group())
        offs.append(m.start())
    return ''.join(out), offs


def project_span(offsets, start: int, end: int, orig_text: str,
                 pad: int = 0) -> str:
    """Map stream span [start, end) back onto orig_text, with `pad` context.

    `orig_text` MUST be the NFC text the offsets were built from -- pass
    nfc(raw) if the raw text has not been normalized, or offsets will point
    into a differently-composed string.
    """
    if not len(offsets) or start >= len(offsets):
        return ""
    end = min(end, len(offsets))
    if end <= start:
        return ""
    a = max(0, offsets[start] - pad)
    b = min(len(orig_text), offsets[end - 1] + 1 + pad)
    return orig_text[a:b]


def gram_codes(stream: str) -> np.ndarray:
    """uint64 base-27 codes for every overlapping K-gram of `stream`.

    `stream` must already be normalized: the utf-16-le view below assumes
    every character is a single BMP code unit, which holds for alef..tav and
    would silently corrupt on anything outside it.
    """
    n = len(stream) - K + 1
    if n <= 0:
        return np.empty(0, dtype=np.uint64)
    a = (np.frombuffer(stream.encode('utf-16-le'), dtype=np.uint16)
         .astype(np.uint64) - np.uint64(HEB_MIN))
    c = np.zeros(n, dtype=np.uint64)
    base = np.uint64(GRAM_BASE)
    for j in range(K):
        c = c * base + a[j:j + n]
    return c
