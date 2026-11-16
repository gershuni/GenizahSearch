## [Unreleased] -- Dictionaries: צ and ט read for one another (web + desktop, branch `claude/hebrew-diacritical-marks-befc6d`)

Judeo-Arabic writes ض (צ̇) and ظ (ט̇) for one another, so a word looked up in Blau's or Friedman's
dictionary now also reaches an entry spelled with the other letter: עצ̇ים reaches עט̇ם, אלקאט̇י
reaches קאצ̇י. One or two letters of a word are exchanged (`shared/blau_normalize.py::interchange_variants`),
in the same later pass as a spelling with a yod or waw added or dropped: an entry spelled as the
word is written comes first, and an exchanged one shows as "Possible spelling" (step `interchange`,
`spelling_variant`). Several words (the expression search) the same way: a second pass with each
word's exchanged spellings (`shared/ja_expression.py::interchange_candidates`), whose hits come
after the words' own and carry `possible_spelling`. The reader's side only: the stored keys,
`NORMALIZER_VERSION` and `KEYING_VERSION` are unchanged, so no sidecar is rebuilt.

Only the ض/ظ pair itself is exchanged: an exchanged spelling is shown only where the dictionary
prints the letter reached with the dot (ט̇ for a word's צ, צ̇ for its ט; `shared/ja_roots.py::
interchange_dotted`). The word's own dot is not needed. Without the rule, 73% of the exchanged
cards in Blau had a ط/ص root (טוב reached צוב, חוץ reached חוט), and Blau printed the dot on every
ض/ظ root reached, so the rule drops no real pair there.

`scripts/measure_dictionary_interchange.py` measures what the exchange gains and adds on the
automatic text, for every exchanged spelling and for the dotted letter (ض/ظ) only.

A ת at the end of a word was already read as the feminine ה (סלאמת, ובסלאמת reach סלאמה), not only
before a pronoun suffix; a test now pins it.
