## [Unreleased] -- Blau's dictionary: the reader turns it on, and can type a word (web, branch `claude/blau-reader-switch`)

The owner's rulings of 2026-11-04 on the phase-2 companion of `docs/plans/BLAU_DICTIONARY_PLAN.md`:
the page does not guess the text's language, a reader can type a word, and close spellings
are not offered.

- **The reader's switch.** On `/browse` a dictionary button (`translate`) sits in the version
  row beside the line-number toggle. It is OFF by default, and the choice is remembered per
  visitor (`ui.blau_lookup`). While it is off, the pane is not marked, there is no hint, and a
  click on a word does nothing. While it is on, the hint shows, the pane's words are looked up,
  and a "type a word" button (`keyboard`) opens the panel with an empty box.
- **The box.** Every panel now has a box at its top holding the word it shows. Enter, or the
  search button, looks up what the box holds, so a misread word can be corrected and looked up
  again. A word typed in Arabic script is looked up against the entries', sub-entries' and
  forms' Arabic spellings (`shared/blau_service.py::arabic_key`: `normalize_arabic`, the
  extraction's broken spacing undone, ة/ه, ى/ي and the hamza seats folded; the conjunction and
  the article peeled). This is an index built in memory on first use: the sidecar is unchanged.
- **The empty answer** now says why a word may have no entry: Blau records what is particular to
  Judeo-Arabic, so an ordinary Arabic word may not be in it, and the lookup does not always find
  the base of an inflected form.
- **The dot above.** A word now also reaches an entry whose name is several words and begins with
  it: דלך shows ذلك (the entry "ד̇לך ו") beside دلك. A word written with the dot (an apostrophe or
  geresh after ג ד ט צ ת כ, or U+0307) lists the dotted reading first and marks the others
  "spelled without the dot"; the box's placeholder says "ד' for ד̇".
- **The click script** asks the server to close only when something is open (the panel shown,
  or a word marked), so with the switch off a click on the page sends nothing.
- **Close spellings were measured and dropped.** Trying an unknown word with one letter swapped
  (the HTR confusion pairs of `shared/unified_variants.py`) was built and measured against the
  PGP Judeo-Arabic editions, aligned word by word with the automatic text of the same pages
  (`scripts/measure_blau_close_spellings.py`). About 1 offer in 20 was right, at every word
  length and pair count: most words with no entry were read correctly, and the dictionary simply
  lacks them. The owner dropped the feature; the script stays for a later attempt.
