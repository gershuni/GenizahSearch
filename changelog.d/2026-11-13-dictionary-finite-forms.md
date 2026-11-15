## [Unreleased] -- Dictionary verb forms and contextual quotations (web, branch `codex/ja-dictionary-improvements`)

Improve common inflected verb retrieval in the three dictionaries, retaining printed lexical
matches and marking generated analyses as possibilities. Recognize additional demonstrative
spellings, prefer noun entries for article-bearing queries, and allow a clicked verb to find
an adjacent preposition with a pronoun in dictionary quotations. Missing personal-pronoun
entries in Lane no longer fall back to unrelated verb roots.

Regression checks cover noun and particle ambiguity, existing lookup switches, source-key
stability, and quotation attribution. Dictionary data files are unchanged.
