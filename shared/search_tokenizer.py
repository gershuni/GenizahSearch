"""SEED-006 — shared Hebrew-aware "hebword" Tantivy tokenizer.

This module is deliberately dependency-light (it imports only ``tantivy``) so
BOTH the main Genizah search engine (``genizah_core``) and the desktop LOCAL
"My Library" indexer (``shared/local_indexer``, which also pulls in PyMuPDF
``fitz``) can register the tokenizer without dragging heavy desktop-only
dependencies into the web process.

Why a custom tokenizer
----------------------
Both Tantivy indexes previously tokenized their searchable ``content`` field
with ``whitespace``, which splits ONLY on spaces — so any word adjacent to
punctuation was indexed *with* the punctuation (``בסגן,`` -> token ``בסגן,``)
and a clean ``בסגן`` query never matched it. ~3% of distinct Hebrew words in
the corpus were unretrievable this way (≈75% editorial punctuation, ≈25%
combining diacritics such as the Judeo-Arabic upper dot in ``צ̇מאן``).

``hebword`` is a regex tokenizer (it MATCHES token spans). Its character class
contains NO literal whitespace, so it KEEPS inside a token:
  * ``\\w`` (letters/digits/underscore),
  * Hebrew block U+0590-05FF (incl. nikud and maqaf),
  * combining diacritical marks U+0300-036F,
  * the ASCII apostrophe / double-quote / gershayim, and
  * square brackets ``[`` ``]`` (scholarly reconstruction markers),
while SPLITTING on space / comma / period / colon / parens / slash / ``⟦⟧``.

So ``מצותה בסגן,`` -> ``['מצותה', 'בסגן']`` and ``[סגן`` -> ``['[סגן']``.

NOTE: ``L{n}:word`` position markers (in line_starts/line_ends) embed a colon
that hebword would shatter, so those fields STAY on ``whitespace``. Only the
``content`` field (which carries no ``L:`` markers) and the additive
``content_search`` field use hebword.
"""

from __future__ import annotations

import tantivy

HEBWORD_TOKENIZER_NAME = "hebword"

# No literal whitespace in the class — that is what makes punctuation a token
# boundary while Hebrew letters / marks / apostrophe / gershayim / brackets are
# kept. tantivy's regex engine accepts Python-style \\uXXXX escapes.
HEBWORD_TOKENIZER_PATTERN = "[\\w\\u0590-\\u05FF\\u0300-\\u036F'\"\\[\\]]+"


def register_search_tokenizers(index) -> None:
    """Register the raw / whitespace / hebword analyzers on *index*.

    A freshly opened or created ``tantivy.Index`` does NOT know custom
    tokenizers, so every ``parse_query`` / ``writer.add_document`` against a
    field declared with ``tokenizer_name="hebword"`` raises
    ``ValueError("The tokenizer 'hebword' ... is unknown")`` unless it is
    registered FIRST. Call this immediately after each ``tantivy.Index(...)``
    / ``tantivy.Index.open(...)`` for the main / LOCAL content indexes, before
    acquiring a writer or running any query.

    Idempotent + defensive (mirrors ``SearchEngine._ensure_lab_tokenizers``):
    re-registering a built-in is harmless and any single failure is non-fatal
    (search degrades rather than crashing on reopen).
    """
    if index is None:
        return
    for name, factory in (
        ("raw", tantivy.Tokenizer.raw),
        ("whitespace", tantivy.Tokenizer.whitespace),
    ):
        try:
            index.register_tokenizer(
                name, tantivy.TextAnalyzerBuilder(factory()).build()
            )
        except Exception:
            pass  # Already registered / built-in clash — non-fatal.
    try:
        index.register_tokenizer(
            HEBWORD_TOKENIZER_NAME,
            tantivy.TextAnalyzerBuilder(
                tantivy.Tokenizer.regex(HEBWORD_TOKENIZER_PATTERN)
            ).build(),
        )
    except Exception:
        pass  # Non-fatal: search degrades rather than crashing on reopen.
