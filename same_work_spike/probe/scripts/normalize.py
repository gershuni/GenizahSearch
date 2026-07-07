# -*- coding: utf-8 -*-
"""Union-view normalizer for the shared-passage probe.

Design (SEED-029 revised):
- NFC
- strip nikud / cantillation / all combining marks (incl. Judeo-Arabic upper dot U+0307)
- fold geresh/gershayim/quotes/apostrophes (dropped entirely -> letters only)
- final-letter fold: ךםןףץ -> כמנפצ
- SPACE-STRIPPED letter stream (HTR word segmentation is unreliable)
- everything that is not a Hebrew base letter is a separator and is dropped
- offset map kept so any span in the normalized stream can be projected back
  onto the original HTR text for human display.

TODO (post-probe, if recall low): matres-light shingle view unioned into the
same shingle set (do NOT create a second index).
"""
import unicodedata
from array import array

FINAL_FOLD = str.maketrans({
    'ך': 'כ', 'ם': 'מ', 'ן': 'נ', 'ף': 'פ', 'ץ': 'צ',
})

HEB_MIN, HEB_MAX = 0x05D0, 0x05EA  # א..ת


def norm_stream(text: str):
    """Return (stream, offsets): space-free normalized Hebrew letter stream.

    stream: str of base letters א-ת (finals folded)
    offsets: array of original character indices, len == len(stream)
    """
    nfc = unicodedata.normalize('NFC', text)
    # NFC may shift offsets vs the raw input; we map back to NFC-text offsets,
    # which is what we store/display anyway.
    out = []
    offs = array('i')
    for i, ch in enumerate(nfc):
        ch2 = ch.translate(FINAL_FOLD)
        o = ord(ch2)
        if HEB_MIN <= o <= HEB_MAX:
            out.append(ch2)
            offs.append(i)
        # combining marks, punctuation, brackets, spaces, digits, Latin: dropped
    return ''.join(out), offs


def norm_words(text: str):
    """Word-level normalization (letters-only words, finals folded).

    Words are defined by the ORIGINAL whitespace (unreliable under HTR,
    use only where word units are required)."""
    nfc = unicodedata.normalize('NFC', text)
    words = []
    for raw_w in nfc.split():
        w = []
        for ch in raw_w.translate(FINAL_FOLD):
            if HEB_MIN <= ord(ch) <= HEB_MAX:
                w.append(ch)
        if w:
            words.append(''.join(w))
    return words


def project_span(offsets, start: int, end: int, orig_text: str, pad: int = 0):
    """Map stream span [start,end) back to original text; pad = extra context chars."""
    if not len(offsets) or start >= len(offsets):
        return ""
    end = min(end, len(offsets))
    a = max(0, offsets[start] - pad)
    b = min(len(orig_text), offsets[end - 1] + 1 + pad)
    return orig_text[a:b]


if __name__ == '__main__':
    # smoke test
    sample = "בְּרוּךֶ אַתָּה [יי] אלהינו מלך העולם, הזן אותנו צ̇מאן ואת־העולם כלו..."
    s, offs = norm_stream(sample)
    assert 'ְ' not in s and '[' not in s and ',' not in s
    assert 'ך' not in s and 'ם' not in s
    assert s.startswith('ברוכאתה'), s
    w = norm_words(sample)
    assert w[0] == 'ברוכ', w
    back = project_span(offs, 0, 8, unicodedata.normalize('NFC', sample))
    print("stream:", s[:40])
    print("words:", ' '.join(w[:6]))
    print("back-projection ok:", repr(back[:20]))
    print("ALL OK")
