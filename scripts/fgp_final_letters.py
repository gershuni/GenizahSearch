# -*- coding: utf-8 -*-
"""Final letters the PDF font left as control characters (GS-276), inferred row by row from a vocabulary.

Why
---
FGP's PDF copies encode the final letters ``ך ם ן ף ץ`` with font glyph codes that text extraction turned
into control characters U+0001-U+001F, so a reader sees a box where the letter belongs. Gate 3 replaces
most of those rows with FGP's own Word or line text; 1,810 rows have no such source and keep the PDF text.
The code-to-letter mapping is not fixed -- U+0018 stands for ``ן`` in one PDF and ``ם`` in another -- but
within one row a code is one letter (99.92% of 156,315 aligned occurrences on the rows that exist both
ways). So each row's codes are read off the row's own words.

The rule (declared in ``docs/plans/FGP_TEXT_PIPELINE_RULES.md`` §8 before the run)
---------------------------------------------------------------------------------------
* Vocabulary: every word ending in a final letter in the texts gate 3 holds without a control code (the
  source texts it read, and today's clean texts), each distinct text counted once.
* A code is *word-final* when at least as many of its occurrences end a word as stand inside one; codes
  that stand inside words are vowel points and dots in those PDFs, which no vocabulary can infer, and
  are left as typed.
* Each word ending in the code (its stem has ``MIN_STEM`` letters or more and no other code) votes: one
  vote, split between the final letters that make it a vocabulary word, by how often each does.
* The row's codes are assigned together: the assignment with the most votes, less ``REUSE_PENALTY`` for
  each code beyond the first on one letter (96% of rows use a distinct code per letter).
* A code is replaced, everywhere in the row, only when its letter has at least ``MIN_VOTES`` votes and
  at least ``MIN_LEAD`` of the code's votes. Otherwise it stays as typed: a box is honest, a wrong final
  letter is not. Measured on the rows that exist both ways, each row's own clean words left out: 99.95%
  of 6,454 applied codes right (3 wrong); at a lead of 0.6 it was 99.74% (18 wrong of 6,823), and the
  codes below the bar 71-95%.

``evaluate`` repeats that measurement inside gate 3 on every run (check T10).
"""
from __future__ import annotations

import collections
import itertools
import re

CODE_RE = re.compile(r"[\x01-\x08\x0b\x0c\x0e-\x1f]")
FINALS = "ךםןףץ"
WORD_RE = re.compile(r"[א-ת\x01-\x08\x0b\x0c\x0e-\x1f]+")
CLEAN_WORD_RE = re.compile(r"[א-ת]+")

MIN_STEM = 2
REUSE_PENALTY = 1.0
MIN_VOTES = 2.0
MIN_LEAD = 0.7
ACCURACY_FLOOR = 0.995     # T10: the applied band on the rows that exist both ways
MIN_JUDGED = 1000          # T10: fewer judged codes vouch for nothing
_EXACT_UP_TO = 7           # codes per row assigned exhaustively; beyond, each takes its own best


def has_codes(text: str | None) -> bool:
    return bool(text) and CODE_RE.search(text) is not None


def final_words(text: str) -> collections.Counter:
    """The words of ``text`` that end in a final letter (a text with a control code contributes none)."""
    if not text or has_codes(text):
        return collections.Counter()
    return collections.Counter(w for w in CLEAN_WORD_RE.findall(text) if w[-1] in FINALS)


def build_vocabulary(texts) -> collections.Counter:
    """Final-letter words of the clean texts, each distinct text counted once."""
    vocab: collections.Counter = collections.Counter()
    seen = set()
    for text in texts:
        if not text or has_codes(text) or text in seen:
            continue
        seen.add(text)
        vocab.update(final_words(text))
    return vocab


def code_votes(text: str, vocab, minus=None) -> dict:
    """{code: (votes {letter: float}, word-end occurrences, inside-word occurrences)}."""
    stems = collections.defaultdict(list)
    ends = collections.Counter()
    inside = collections.Counter()
    for w in WORD_RE.findall(text or ""):
        for k, ch in enumerate(w):
            if not CODE_RE.match(ch):
                continue
            if k == len(w) - 1:
                ends[ch] += 1
                if not CODE_RE.search(w[:-1]):
                    stems[ch].append(w[:-1])
            else:
                inside[ch] += 1
    out = {}
    for code in set(ends) | set(inside):
        votes = dict.fromkeys(FINALS, 0.0)
        for stem in stems.get(code, ()):
            if len(stem) < MIN_STEM:
                continue
            freqs = {}
            for letter in FINALS:
                f = vocab.get(stem + letter, 0) - (minus.get(stem + letter, 0) if minus else 0)
                if f > 0:
                    freqs[letter] = f
            total = sum(freqs.values())
            for letter, f in freqs.items():
                votes[letter] += f / total
        out[code] = (votes, ends[code], inside[code])
    return out


def assign(votes: dict) -> dict:
    """{code: {"letter", "votes", "lead", "applied"}} for the word-final codes with any vote, assigned jointly."""
    codes = sorted(c for c, (v, ends, inside) in votes.items() if ends >= inside and sum(v.values()) > 0)
    if not codes:
        return {}
    options = {c: sorted(FINALS, key=lambda L: (-votes[c][0][L], L))[:3] for c in codes}
    if len(codes) <= _EXACT_UP_TO:
        space = itertools.product(*(options[c] for c in codes))
    else:
        space = [tuple(options[c][0] for c in codes)]
    best, best_score = None, None
    for combo in space:
        score = sum(votes[c][0][L] for c, L in zip(codes, combo))
        score -= REUSE_PENALTY * sum(n - 1 for n in collections.Counter(combo).values() if n > 1)
        if best_score is None or score > best_score + 1e-12:
            best, best_score = combo, score
    out = {}
    for c, letter in zip(codes, best):
        v = votes[c][0]
        total = sum(v.values())
        lead = v[letter] / total if total else 0.0
        out[c] = {"letter": letter, "votes": round(v[letter], 4), "lead": round(lead, 4),
                  "applied": bool(v[letter] >= MIN_VOTES and lead >= MIN_LEAD)}
    return out


def mapping_of(decisions: dict) -> dict:
    """{code: letter} of the applied decisions."""
    return {c: d["letter"] for c, d in decisions.items() if d["applied"]}


def apply_mapping(text: str | None, mapping: dict) -> str | None:
    if not text or not mapping:
        return text
    return text.translate(str.maketrans(mapping))


def truth(pdf_text: str, clean_text: str) -> dict:
    """{code: letter} read off the same row's clean text: a word ``stem+code`` whose stem takes exactly one
    final letter among the clean text's words votes for it; the code takes its majority."""
    clean = set(CLEAN_WORD_RE.findall(clean_text or ""))
    votes = collections.defaultdict(collections.Counter)
    for w in WORD_RE.findall(pdf_text or ""):
        if len(w) >= 2 and CODE_RE.match(w[-1]) and not CODE_RE.search(w[:-1]):
            hits = [L for L in FINALS if w[:-1] + L in clean]
            if len(hits) == 1:
                votes[w[-1]][hits[0]] += 1
    return {c: cnt.most_common(1)[0][0] for c, cnt in votes.items()}


def evaluate(pairs, vocab) -> collections.Counter:
    """Accuracy on ``pairs`` of (pdf_text, clean_text): each pair's own clean words are taken out of the
    vocabulary first. Counts ``judged`` / ``right`` / ``wrong`` for the applied codes, ``all_*`` for every
    assigned code."""
    C: collections.Counter = collections.Counter()
    for pdf_text, clean_text in pairs:
        known = truth(pdf_text, clean_text)
        decisions = assign(code_votes(pdf_text, vocab, minus=final_words(clean_text)))
        for code, d in decisions.items():
            if code not in known:
                continue
            ok = d["letter"] == known[code]
            C["all_judged"] += 1
            C["all_right"] += ok
            if d["applied"]:
                C["judged"] += 1
                C["right"] += ok
                C["wrong"] += not ok
    return C
