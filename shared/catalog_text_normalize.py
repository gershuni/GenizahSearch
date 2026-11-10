# -*- coding: utf-8 -*-
"""Words and query keys for the catalogue text search (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md).

One function decides what a word is, and it runs on both sides: on the catalogue text when
``scripts/build_catalog_text_sidecar.py`` builds ``catalog_data/catalog_text.db``, and on the
reader's query when ``shared/catalog_text_service.py`` searches it. ``NORMALIZER_VERSION`` is
written into the sidecar's ``meta``. A service whose version differs refuses the file, because
every lookup would then miss without saying so.

A word's key (plan §4.2):

- NFKD, then every combining mark removed: Hebrew nikud and cantillation, and Latin accents
  (Fusṭāṭ -> fustat).
- Quote marks dropped wherever they stand (ASCII ' and ", geresh, gershayim, curly single
  quotes), so the typed and the typographic abbreviation meet: רמב"ם, רמב״ם and רמבם are one key,
  as are Sa'adia and Saadia.
- Lower case, and the Hebrew final letters folded (ך->כ ם->מ ן->נ ף->פ ץ->צ).
- What is left is split into runs of letters and digits. Hebrew maqaf, punctuation and spaces
  separate words.
- A Latin-letter word is made singular (``english_singular``: letters -> letter, stories -> story).

The keys contain only letters and digits, so they can always be quoted safely inside an FTS5
query: no reader input reaches the FTS5 query syntax (``fts_match``).

Query rules (plan §4.2, owner D3, 2026-11-05):

- Every word must occur in the same catalogue record.
- A Hebrew word of two letters or more also matches itself after the common prefixes
  (``HEBREW_PREFIXES``: כתובה finds הכתובה, בכתובה, וכתובה...). The service ranks the
  manuscripts found by the bare word first.
- Text in double quotes is a phrase: its words next to each other, exactly as typed, with no
  prefixes. A double quote between two Hebrew letters is a gershayim, never a phrase mark.

Stdlib only; no Config, no logging.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

#: Written into the sidecar's ``meta.normalizer_version``; bump on ANY change to ``key`` or
#: ``tokens``, then rebuild the sidecar.
NORMALIZER_VERSION = 1

#: The quote family dropped from words (as ``shared.text_normalize.strip_search_diacritics`` and
#: ``shared.blau_normalize.QUOTE_MARKS`` treat it).
QUOTE_MARKS = frozenset("'\"׳״‘’")
FINAL_FOLD = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})

#: Prefixes tried before a bare Hebrew query word (D3). Singles, then the common pairs.
HEBREW_PREFIXES = (
    "ה", "ו", "ב", "ל", "מ", "ש", "כ",
    "וה", "וב", "ול", "ומ", "וש", "וכ", "שה", "שב", "של", "מה", "בה", "לה", "כש",
)

# A run that may be one written word in the source: letters and digits plus the marks and quote
# characters that sit inside words. Hebrew punctuation in the mark block (maqaf U+05BE, paseq
# U+05C0, sof pasuq U+05C3, nun hafukha U+05C6) is left out, so it separates words.
_RUN_RE = re.compile(
    "[\\ẁ-֑ͯ-ׇֽֿׁׂׅׄ"
    "'\"׳״‘’]+"
)
_KEY_PART_RE = re.compile(r"[^\W_]+")
_HEBREW_WORD_RE = re.compile("^[א-ת]+$")
_HEBREW_LETTER = re.compile("[א-ת]")
# A double quote (or gershayim) with a Hebrew letter on both sides is part of the word.
_INWORD_QUOTE_RE = re.compile("(?<=[א-ת])[\"״](?=[א-ת])")


def _fold(run: str) -> str:
    decomposed = unicodedata.normalize("NFKD", run)
    kept = "".join(ch for ch in decomposed if unicodedata.category(ch)[0] != "M" and ch not in QUOTE_MARKS)
    return kept.casefold().translate(FINAL_FOLD)


_LATIN_WORD_RE = re.compile("^[a-z]+$")
_KEEP_FINAL_S = ("ss", "us", "is")
_DROP_ES = ("sses", "shes", "ches", "xes")


def english_singular(word_key: str) -> str:
    """A Latin-letter key of four letters or more, made singular (plan §4.2, measured 2026-11-05:
    the plural alone adds 31-64% more manuscripts for letter, contract, prayer, poem, merchant).

    Our own small rule, not SQLite's Porter stemmer, so the snippet highlighter runs the very same
    function: -ies -> -y (stories -> story), -sses/-shes/-ches/-xes lose -es (glasses -> glass,
    churches -> church, boxes -> box), any other final -s goes, except after s, u or i (glass,
    Exodus, Genesis stay). The possessive meets the name: Ezra's -> ezras -> ezra. Hebrew, Arabic
    and digits are never touched."""
    if len(word_key) < 4 or not _LATIN_WORD_RE.match(word_key):
        return word_key
    if word_key.endswith("ies") and len(word_key) >= 5:
        return word_key[:-3] + "y"
    if word_key.endswith(_DROP_ES):
        return word_key[:-2]
    if word_key.endswith("s") and not word_key.endswith(_KEEP_FINAL_S):
        return word_key[:-1]
    return word_key


@dataclass(frozen=True)
class Token:
    """One word of a text: where it stands in the ORIGINAL text, and its key(s).

    ``keys`` is usually one key. A run the fold splits further (a digit glued to a letter is still
    one run, but a stray symbol inside it is not) yields several, all mapped to the same span."""
    start: int
    end: int
    keys: tuple[str, ...]


def tokens(text: str) -> list[Token]:
    """The words of ``text`` with their spans in ``text`` itself (for highlighting the original)."""
    out: list[Token] = []
    if not text:
        return out
    for match in _RUN_RE.finditer(text):
        start, end = match.start(), match.end()
        # A quote mark at the edge of a run is punctuation, not part of the word.
        while start < end and text[start] in QUOTE_MARKS:
            start += 1
        while end > start and text[end - 1] in QUOTE_MARKS:
            end -= 1
        if start >= end:
            continue
        keys = tuple(english_singular(part) for part in _KEY_PART_RE.findall(_fold(text[start:end])))
        if keys:
            out.append(Token(start, end, keys))
    return out


def key_words(text: str) -> list[str]:
    """Every key of ``text``, in order."""
    return [k for tok in tokens(text) for k in tok.keys]


def key(text: str) -> str:
    """The indexed form of a whole text: its keys joined by single spaces ('' when it has none)."""
    return " ".join(key_words(text))


def is_hebrew_text(text: str) -> bool:
    """True when ``text`` holds a Hebrew letter (the record's language label: he or en)."""
    return bool(_HEBREW_LETTER.search(text or ""))


def hebrew_forms(word_key: str) -> tuple[str, ...]:
    """The key itself, then the key after every prefix in ``HEBREW_PREFIXES`` (D3).

    Only for an all-Hebrew key of two letters or more; any other key is its own only form."""
    if len(word_key) < 2 or not _HEBREW_WORD_RE.match(word_key):
        return (word_key,)
    return (word_key,) + tuple(p + word_key for p in HEBREW_PREFIXES)


@dataclass(frozen=True)
class Term:
    """One condition of a query: a single word (``phrase`` False, one key) or a quoted phrase
    (``phrase`` True, its keys in order, matched next to each other without prefixes)."""
    keys: tuple[str, ...]
    phrase: bool


def parse_query(query: str) -> list[Term]:
    """The terms of a reader's query. Empty when it holds no word at all."""
    if not query:
        return []
    text = _INWORD_QUOTE_RE.sub("", query)
    # Typographic double quotes act as phrase marks too.
    text = text.replace("“", '"').replace("”", '"').replace("״", '"')
    parts = text.split('"')
    if len(parts) % 2 == 0:
        # An unmatched quote: the text after it is ordinary words.
        parts = parts[:-2] + [parts[-2] + " " + parts[-1]]
    terms: list[Term] = []
    for index, part in enumerate(parts):
        words = key_words(part)
        if not words:
            continue
        if index % 2 == 1:
            terms.append(Term(tuple(words), True))
        else:
            terms.extend(Term((w,), False) for w in words)
    return terms


def fts_match(terms: list[Term], expand_prefixes: bool) -> str:
    """The FTS5 MATCH expression for ``terms``: every term required.

    Each key is wrapped in double quotes; keys hold only letters and digits (``tokens``), so the
    reader cannot reach FTS5 operators (OR, NOT, NEAR, ``*``, ``^``, column filters) through them.
    ``expand_prefixes`` adds ``hebrew_forms`` for each single Hebrew word (never inside a phrase)."""
    clauses: list[str] = []
    for term in terms:
        if term.phrase:
            clauses.append('"' + " ".join(term.keys) + '"')
            continue
        forms = hebrew_forms(term.keys[0]) if expand_prefixes else (term.keys[0],)
        if len(forms) == 1:
            clauses.append(f'"{forms[0]}"')
        else:
            clauses.append("(" + " OR ".join(f'"{f}"' for f in forms) + ")")
    return " AND ".join(clauses)


def can_expand(terms: list[Term]) -> bool:
    """True when prefix expansion would change ``fts_match`` (a single Hebrew word is present)."""
    return any(not t.phrase and len(hebrew_forms(t.keys[0])) > 1 for t in terms)


def match_spans(text: str, terms: list[Term], expand_prefixes: bool) -> tuple[list[tuple[int, int]], int | None]:
    """Where ``terms`` match in ``text``: the (start, end) of every matched word, in order, and the
    offset a snippet should centre on.

    A single word matches wherever its key (or, with ``expand_prefixes``, any of its
    ``hebrew_forms``) stands. A phrase matches only where all its keys stand next to each other,
    in order, as FTS5 matched it; its words elsewhere are not marked. The snippet centres on the
    first phrase match when the query has a phrase (the part the reader asked for exactly), else on
    the first matched word; ``None`` when nothing matches."""
    toks = tokens(text)
    flat = [(index, k) for index, tok in enumerate(toks) for k in tok.keys]
    marked: set[int] = set()
    phrase_starts: list[int] = []
    singles: set[str] = set()
    for term in terms:
        if not term.phrase:
            singles.update(hebrew_forms(term.keys[0]) if expand_prefixes else term.keys)
            continue
        width = len(term.keys)
        for j in range(len(flat) - width + 1):
            if all(flat[j + m][1] == term.keys[m] for m in range(width)):
                marked.update(flat[j + m][0] for m in range(width))
                phrase_starts.append(flat[j][0])
    for index, tok in enumerate(toks):
        if any(k in singles for k in tok.keys):
            marked.add(index)
    spans = [(toks[i].start, toks[i].end) for i in sorted(marked)]
    if phrase_starts:
        anchor: int | None = toks[min(phrase_starts)].start
    else:
        anchor = spans[0][0] if spans else None
    return spans, anchor
