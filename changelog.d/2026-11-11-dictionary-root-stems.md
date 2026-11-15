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
כון first). Lane marks and puts first the entry a reading is cited by (أيضا under أيض, طالبته as
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

A word printed in an entry only beside the entry's verb no longer reaches that entry. Blau writes
the words a verb takes after it ("אכ̇ד̇ לה"), so אללה reached אכ̇ד̇, חצל, כ̇רג̇ and עמל through "לה",
and ואלסלאם reached כ̇צץ. A form under an entry now counts only when the word holds the entry's root
letters in order (those other than א ו י ה; 293 of Blau's 680 one-word forms are such words). In
Friedman, a unit reached that way through a peeled key is left out, and one the word itself
reaches comes after the units of the word's own root (אלכלאם: the noun כלאם before the sub-entries
of טול). A word with the article reads as a noun: an entry it reaches by its name shows its noun
senses first in Blau (אלקול: 'saying' before 'to say'), Friedman shows the noun's entry before the
verb's, and a verb form is read only as a participle or a verbal noun (אלקאצ̇י is no longer stem III
of קצו).

A Blau card heads the senses of each of its sub-entries with the sub-entry's name, as the site does:
קרצת's card gave 'nettle' and 'pulp', the senses of the noun קריץ, as the verb's.

In Friedman's tab, a sub-entry or stem paragraph the book prints without its root reads root, stem,
then what the book prints: "VIII -הُ" under שכו is drawn "שכו VIII -הُ", where the card showed "-הُ"
as the word. That is 1,360 units: a head that opens with a pronoun suffix ("-ה", "-ה גרצה/אמלה"), one
made only of prepositions and placeholders ("עליה", "לה/להא", "אן יפעל"), or a stem paragraph with no
head; under its own entry's card the root is the entry's head. Such a unit is no longer reached as the
word itself: clicking עליה or לה reached a dozen verbs' stem paragraphs headed by it (13% of the
clicked words of four letters reached one). It is still reached through its verb (a verb form, a
stem, the root). A particle's own forms (אליה under אלי) stay words.

An article that lost its alif after a preposition is peeled apart: בלסלאם (bi-l-salām) is read
ב + (א)ל + סלאם, ללנאס ל + (א)ל + נאס, כלכתאב כ + (א)ל + כתאב (after ו or פ too; the stem of three
letters or more). A lookup reads it only when the word as written reaches nothing, so בלגני stays בלג;
on four letters, 11 clicked words had the shape and 6 now reach their entry (לליום, ללנחאס,
ללמגאובה...). A selected or typed word answers to the stem in the expression search, so "יכצך בלסלאם"
finds what "יכצך באלסלאם" finds. The dictionaries' words are keyed as before, so the sidecars are
unchanged.

A feminine noun before a pronoun suffix reaches its entry: a lookup reads a ת that a suffix was
peeled from also as ה, so סלאמתך, וסעאדתך and עזתך reach סלאמה, סעאדה and עזה in Blau and Friedman,
where they reached only a root borrowed from Lane. A verb with an object suffix can now also reach a
noun (קלתה, 'I said it', reaches קלה). The expression search keys words as before, so the sidecars
are unchanged.

A clicked word is also looked up with the words beside it in the text. The page sends up to two
words on each side (not across a sentence's end, nor past a word broken by a gap), and the runs of
three and then two words that hold the clicked one are searched with no word between; a run of
function words only (עלי כל) is not. An entry or sub-entry whose name holds one comes first in its
tab, under "As an expression with the words beside it", and the panel opens on the dictionary that
has one: אללה in "ואטאל אללה בקאך" now opens on Friedman's tab with "אטאל אללה בקאה" first, where
it opened on Blau's entry אללה אללה. This adds about 50 to 60 ms a dictionary to a click.

When no name holds such a run word for word, the units that hold one with up to two words between,
in a name or in a quotation, come first instead, at most three: ואדאם and סלאמתך in "ואדאם סלאמתך"
now bring Friedman's "אדאם אללה סלאמתה" (quoted under ג̇דד) first, as selecting both words did. Only
runs without a function word or a word of a name (בן, אבו) are searched this way: on four letters
(1,570 clicked words), any run with one word between reached quotations for nearly half the words,
about eight units each, mostly by chance ("פי אלבר", "אבו אלפרג"); these runs reach them for 14% in
Friedman and 9% in Blau. The gap is the selection's, at most two.

The panel now opens on the dictionary whose answer comes closest to the clicked word, not on the
first one with any answer: an expression with the words beside it, then the word itself (an entry,
a form or a head that is the word, or Friedman's index listing it), then an expression with the
words beside it that only quotations hold, then its base form (a clitic peeled), then a guessed root
(another dictionary's, a verb form, Lane's reading), then quotations only; a reading spelled without
the word's dot counts half a step further, and of equals the first tab opens. An action inside a tab
still stays on it.

Every dictionary tab now opens with a line saying the lookup is automatic: "Found automatically
from the word's letters: an entry may belong to another word, and the word's own entry may be
missing. Check the dictionary."

With the switch on, words selected by dragging over them are looked up together, as with the D or
M key; before, only the word under the pointer was.

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
- New strings, English and Hebrew: "As an expression with the
  words beside it" ("כצירוף עם המילים הסמוכות") and the line every tab opens with ("נמצא אוטומטית
  לפי אותיות המילה: ייתכן שערך שייך למילה אחרת, וייתכן שהערך של המילה חסר. בדקו במילון.").
- New strings, English and Hebrew: the empty answer, "Stem
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
- `shared/ja_roots.py`: `carries_root`; `shared/blau_normalize.py`: `article_candidates`;
  `shared/ja_verb_forms.py`: `nominal_forms`; `shared/ja_expression.py`: `context_windows`; each
  service's `context` and `answer(before=, after=)`; `BlauSense.sub_entry`;
  `web/components/expression_panel.py::render_context`; the click script's `wordsBeside`.
- `shared/friedman_service.py`: `construction_head`, `FRAME_TOKENS` (the unit builder's, kept equal by
  a test), `FriedmanUnit.entry_root`; `web/components/friedman_panel.py`: `ROOT_CLASS`,
  `CONSTRUCTION_CLASS`.
- `shared/blau_normalize.py`: `elided_article_stems`, `MIN_ELIDED_STEM`; `shared/ja_expression.py`:
  `query_keys` (the reader's words only).
- `shared/blau_normalize.py`: `lookup_stems`; `shared/ja_expression.py`: `quoted_runs`, `quoted_gap`,
  `MAX_CONTEXT_QUOTED`, `NAME_WORDS`; each lookup's `context_quoted`; `blau_panel.CLOSE_QUOTED`.

The stem lists are built in memory on first use (about 50 ms for Blau and 36 ms for Friedman on
the local sidecars); the verb forms, once a kind is switched on, in one to two seconds each and 75 MB
held for both. No
sidecar change. Steps A, B and the stems of the dictionary lookup plan; a clicked verb form reaches
its root's entry in Blau through Friedman and Lane, opened on its stem, and by itself in both
dictionaries once a kind of card found by verb form is switched on.
