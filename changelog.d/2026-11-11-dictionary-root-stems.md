## [Unreleased] -- Dictionaries: a root's verb stems, Blau's entries for the roots Friedman and Lane give, and an empty answer that says what was checked (web, branch `dictionary-lookup-quotations`)

A root's card in Blau's and Friedman's dictionaries now shows the verb stems that dictionary lists
under it, as buttons (דלל: I, II, IV, V, VII, X in Blau). Pressing one opens that stem's
paragraph: in Blau its senses, in Friedman its stem paragraph (or the entry itself when the stem
is the entry's own). Typing the root with a stem does the same: "דלל X" or "דלל 10", or the root
in the box and the stem in the field beside it. A stem the root does not list says so above the
root's cards. An entry headed by the word now comes before one that lists it as another spelling:
וצל shows the verb וצל before the noun אَצْל/אُצْל/וَצْל in Friedman (157 words change their first
entry there; in Blau, an entry whose first name is the word before one that gives it as a later
name). An apostrophe after a letter is its dot in
both dictionaries (ד'לל is ד̇לל): the dotted reading first, the other marked. Before, Blau's lookup
did this and Friedman's gave no preference.

Blau's tab now also shows Blau's entry for the root that Friedman or Lane gives for the word, after
Blau's own entries. Blau has no list of inflected forms, so קאל found no entry; Friedman's index and
Lane's reading both give קול, and Blau's tab now shows קול after its own entries. On
5,000 words from each dictionary's quotations (Friedman's, Blau's), Blau shows an entry for 77% and
73% of them (35% and 40% before); one word in seven that Blau already answered gets one more entry. A borrowed root is
taken only from a dictionary that is served. From Friedman, a root comes from its index of forms
or from an entry headed by the word or its base form, never from a sub-entry, a stem paragraph or an
entry that holds the word further in its head: such a head is mostly a phrase, and the word the
verb's complement (אלעלמא borrowed דרס from "עِלْמאً", a stem paragraph of דרס; it now shows עלם,
which Lane gives).

Nothing is left only on the dictionary's site or PDF page any more. A Blau card shows its first four
senses and a button "Show N more senses" that opens the rest in place (it linked to the site
before); a Friedman unit shows its first eight quotations, a sub-entry its first, and the first 30
sub-entries and stem paragraphs, each with "Show N more ..." in place of the link to the PDF page. A
single remaining item is shown without a button. A new word opens on the first dictionary that has
something for it; the tab chosen for an earlier word is no longer remembered. A stem button, a gap
control or a Lane reading stays on its own tab, and choosing a tab opens it at its top.

A verb form now opens on its stem when a dictionary reaches its root through another way than the
word itself: אסתקר opens קרר at stem X in Blau (a root Friedman and Lane gave) and in Friedman (its
index), טאלבתה opens טלב at stem III. The stem comes from the word's shape: every form of each stem
the dictionary lists is generated (`shared/ja_verb_forms.py`, the plan's rules), and a stem is chosen
only when exactly one listed stem holds the word. Friedman's tab now also borrows roots, from Blau's
own entries and Lane: ויכון, which reached nothing there, shows כון.
Among Blau's own entries, one another dictionary names comes first, whichever key reached it (ויכון:
כון before כ̇לו; ואלקול: קול, reached with the article peeled, before טלע, which prints אלקול as a
variant). Lane marks and puts first the entry a reading is cited by (أيضا under أيض, طالبته as
طالبه, استقر as استقرر), and shows all of a root's entries and a cut entry's whole text in place
("Show N more entries", "Show the full entry").

A word a dictionary does not list can now reach its root through the generated forms of a stem the
dictionary lists: אסתדלו reaches דלל, stem X, in both. When the word itself reaches nothing in a
dictionary, up to three such cards are shown, each saying how it was reached: "found by its verb
form", then the form, the root and the stems ("אסתדלו (דלל X)"). When the word itself does reach an
entry, the other roots its verb forms give are only suggested, as chips at the top of the tab
under "Other possible roots" ("קרב V"; a press opens that root on that stem): נדרי reaches נדר
("vow") by its letters, and the chips offer דרי I ("we know"). The chips stay while the reader
presses them one by one, the one shown marked and the word itself first, to go back to its own
answer (pressing the marked chip again does the same). On a graded sample of 425 words of
the corpus's Judeo-Arabic editions (Hebrew and Aramaic words left out), a card shown where nothing
else was found named a right root for 87% to 95% of words, depending on the dictionary and the kind;
beside an entry the word already reached, it was mostly wrong, hence the chips. Each card has a kind,
by the word's own key or a clitic peeled and by its stem (A to E, `shared/ja_verb_forms.py`). Four
switches name the kinds each dictionary shows and those that open on the stem
(`BLAU_VERB_FORM_KINDS`, `FRIEDMAN_VERB_FORM_KINDS`, `BLAU_VERB_FORM_STEM_KINDS`,
`FRIEDMAN_VERB_FORM_STEM_KINDS`): by default A, B and D are shown, opened on the stem where the guessed
stem was right in 85% or more of the sample (A and D in Blau; A, B and D in Friedman); set empty, a
switch turns them off. A root one dictionary found by verb form is not lent to the other: both run
the same rules. A verb whose final
weak letter is dropped takes an ending (רמת, ירמו, not a bare ירמ, the short form after לם): the bare
form mostly spells another word (תלתה, "three", read as stem VIII of להו; אסתקר as stem X of קרא
beside קרר). On the gate it cost a tenth of a point of reach and took 0.4 points off the forms that
get only wrong cards.

A borrowed Blau card for a word that fits none of the entry's stems shows the senses outside the
stem paragraphs first: אלחרוף, the plural of the noun חרף, opened on the verb's stem II sense.

Only the box, a stem button and a chip read a stem: words clicked or selected in the manuscript stay
words ("פרק 2" is the expression search, not פרק at stem II), and several words typed in the box stay
an expression search whatever the stem field beside it holds. Lane tries a reading's other shapes
(طالبته as طالبه) only when the reading itself names none of the root's entries (بنت no longer marks
بن). Where a kind of card found by verb form is shown, the verb forms are built in the background
after the server starts (`web/main.py`), not inside the first reader's lookup.

A dictionary can be shown to editors only, as a preview before it is released: each of
`BLAU_DICTIONARY_ENABLED`, `LANE_DICTIONARY_ENABLED` and `FRIEDMAN_DICTIONARY_ENABLED` also takes
`editors` (`shared/dictionary_switch.py`). A signed-in editor or admin then gets the dictionary on
/browse, in the `?` card and in Help; everyone else, and What's New, see the site as if it were off.
Each service's `is_available()` stays the public answer and `served_to(editor)` adds the preview, so a
surface that does not know the reader cannot show it by mistake.

- `shared/ja_roots.py` (new): the dot above (`dot_pattern`, moved from `shared/blau_service.py`),
  stem labels read as every numeral they name (`stem_numbers`), and the typed shortcut
  (`root_and_stem`).
- `shared/blau_service.py`: `stem_pairs`, `root_stems`. A stem row's number comes from its
  `binyan` column, its `binyan_shift` and its printed head together; 355 rows of release 1.1 have a
  column that disagrees with the printed head, and such a row answers to both numbers.
- `shared/friedman_service.py`: `stem_pairs`, `entry_stems`, `answer`, and the dot rule in the
  lookup. Noisy stem labels ("IV/II", "II(<IV)") are read as every numeral they name.
- `web/components/stem_buttons.py` (new); `web/components/blau_panel.py`,
  `web/components/friedman_panel.py`, `web/pages/browse.py` (a press stays on its tab; Lane gets
  the root alone); a Help sentence on the Browse page.
- The empty answer in Blau's and Friedman's tabs (step B) now reads "Not found as an entry or in
  the examples. A verb or an inflected form may be listed under its root: type the root." Lane's
  tab keeps its line.
- New strings approved by the owner (2026-11-11), English and Hebrew: the empty answer, "Stem
  {stem} is not listed under this root.", the Help sentence, "Show {n} more senses", "Show {n} more quotations" and "Show {n} more
  sub-entries and verb stems", "Show {n} more entries", "Show the full entry" (Hebrew "הצג ..."),
  "found by its verb form" ("נמצא לפי צורת הפועל") and "Other
  possible roots" ("שורשים אפשריים נוספים"). The stem buttons reuse "verb stem".
- `shared/ja_verb_forms.py` (new): the forms of a stem, the stem a word belongs to, and the cards
  found by verb form with their kinds (`verb_form_cards`); each service's `verb_form_index` (built
  once, on the first lookup with a kind switched on); and `scripts/measure_dictionary_stems.py`, the
  gate that measures the forms before they reach a root by themselves, on the lookup's own cards.
  `docs/guides/ENV_VARS.md` documents the four switches. `scripts/draw_dictionary_stems_sample.py`
  and `scripts/score_dictionary_stems_sample.py` draw the graded sample from the corpus's
  Judaeo-Arabic editions and apply the pass rule a kind must meet before it is switched on.
- `web/components/show_more.py` (new): the rest of a card behind one button. `MAX_SENSES` (Blau)
  and `MAX_CHILDREN` / `MAX_QUOTATIONS` (Friedman) are now bounds far above the largest entry (115
  senses; 44 sub-entries, 16 quotations), so a card carries everything.
- `shared/ja_roots.py`: `borrowed_roots`, `ja_root_of_arabic`; `shared/blau_service.py`:
  `BlauService.borrow`; `web/pages/browse.py` hands Blau the roots after the three lookups.

The stem lists are built in memory on first use (about 50 ms for Blau and 36 ms for Friedman on
the local sidecars); the verb forms, once a kind is switched on, in one to two seconds each and 75 MB
held for both. No
sidecar change. Steps A, B and the stems of the dictionary lookup plan; a clicked verb form reaches
its root's entry in Blau through Friedman and Lane, opened on its stem, and by itself in both
dictionaries once a kind of card found by verb form is switched on.
