# Blau's dictionary as a companion: a Judeo-Arabic dictionary in GenizahSearch

**Status:** plan **rev. 3** (2026-11-04). Phase 1 (the sidecar, its normalizer and its gate) is
**merged** (#472, `c4b7f8c8`, 2026-11-03). Phase 2 (the reader companion on `/browse`) is built
on branch `claude/blau-browse` and is this plan's second PR; phases 3-4 are described, not built.
**Both owner decisions are ruled (2026-11-03, §9):** the site's permission is treated as
covering GenizahSearch, and the companion is **web only first**. Nothing is deployed: the server
copy of the sidecar is the owner's (`scripts/deploy_blau_sidecar.ps1`, §8).
**Origin:** the PostHog usage review of 2026-11-03 (`_tmp/posthog_usage_review_2026-11-03.md`):
readers whose browser is English hit zero results in 53% of their Hebrew-letter searches, against
33% for readers whose browser is Hebrew. The suspected cause is Judeo-Arabic orthography: a large
share of the corpus is Arabic in Hebrew letters, and a reader who knows the Arabic word does not
know how the scribe (or the HTR) spelled it. The owner proposed Joshua Blau's *A Dictionary of
Mediaeval Judaeo-Arabic Texts* (2006) as a companion for every reader, and as help for English and
Hebrew readers searching Arabic terms, and pointed at its digital edition as the data source.
**Scope:** data layer now (web and desktop can both read a SQLite sidecar); the reader companion,
search by meaning and the reverse link later, on the surfaces ruling (b) names.
**Branches:** `claude/blau-sidecar` (phase 1, #472, merged 2026-11-03) and `claude/blau-browse`
(phase 2, PR against `master-main`); nothing deployed.

---

## 1. The problem in one paragraph

The corpus is 2.2 million pages of Hebrew-letter transcription, much of it Judeo-Arabic. A reader
who meets `אלמוצ̇ע` on a page has no way, inside GenizahSearch, to learn that it is الموضع "the
place"; a reader who wants every page about a *qāḍī* has to guess the spelling (קאצ̇י, קאצי, קאדי,
with or without the article) and gets zero results for most guesses, which is what the usage
review measured. Blau's dictionary is the standard reference for exactly this: 3,047 roots and
12,753 forms, each with the Judeo-Arabic spelling in Hebrew letters, the Arabic spelling, a Hebrew
and an English gloss, and citations into the very texts the corpus holds. Its digital edition
(blau-dict.netlify.app, release 1.1, 2026-08-25, MiDRASH / ERC, "published with permission") ships
the whole dictionary as one JSON file.

**Proposal:** one sidecar, `blau_data/blau.db`, built from that JSON by a script with a gate, like
every other `*_data/` sidecar; one normalizer, `shared/blau_normalize.py`, under which a dictionary
form and a corpus token meet; then, in order, a reader companion on `/browse`, search by meaning on
`/search`, and a count of corpus pages on each dictionary entry.

---

## 2. The source, as measured (2026-11-03)

`GET https://blau-dict.netlify.app/data/semantic_blau.json.gz` (4,089,743 bytes, sha256
`072144ca763ec9102e95128393d577b4a29a5184dbea3f4665c51461b4d6d8ea`; 47,018,827 bytes of JSON) is
the whole dictionary: an object keyed by the printed entry name (`אבד (أبد)`), every value
`schema_version` `v6`.

| Unit | Count | Shape |
|---|---|---|
| roots | 3,047 | `id` `blau-e-…`, `name_ja` (list; 7 roots have none and are keyed by their Arabic spelling in parentheses), `name_ja_clean`, `arabic_spelling`, `start_page_printed`, `is_cross_reference` (191, with `target_id`; 14 have none, the site's own unresolved pointers), `stems`, `sub_entries` |
| sub-entries | 3,665 | `blau-se-…`, `name_ja` (54 have none, only an Arabic spelling), `arabic_spelling`, `stems` |
| stems | 8,295 (4,630 under roots, 3,665 under sub-entries) | `blau-st-…`, `stem_head` with `binyan` ("I" … "X"), `binyan_shift`, `government`, `complement_arabic`, `complement_ja` (strings) and `segments` |
| forms | 12,753 | `blau-f-…`, `segments`, `is_classical` / `is_reconstructed` / `is_erroneous`, `variant_head`, `dictionary_citations` (13,465), `references` (11,709), `additional_notes` |
| segments | 27,612 on forms | `text`, `text_type`, `language`, `font` |
| parsed citations | 28,505 under references + 13,465 dictionary citations | `blau-c-…` / `blau-dc-…`, `citation_type` (full, ibid, …), `source_work`, `source_id` (`blau-bib-…`; 8,246 parsed citations have none; one names `blau-bib-000364`, which the bibliography does not list), `ja_text`, `translation`, `commentary` |
| bibliography | 337 entries (324 top-level), `data/frontmatter/hebrew_bibliography.json` | `id`, `abbreviation`, `full_reference`, `page`, `parent` |

Segment `text_type` values: `definition_hebrew` 7,090 · `definition_english` 6,891 ·
`freeform_note` 5,346 · `variant_arabic` 1,621 · `punctuation` 1,344 · `cross_reference` 1,075 ·
`classical` 1,046 · `variant_ja` 904 · `grammar_citation` 814 · `morph_note` 745 · `binyan` 379 ·
`erroneous_form` 110 · `complement_arabic` 102 · `complement_ja` 46 · `headword_ja` 44 ·
`hebrew_in_ja` 32 · `binyan_shift` 14 · `headword_arabic` 6 · `government` 3. The font
`Hebrew-Miriam` (1,891 segments) marks Judeo-Arabic text in Hebrew letters; `Hebrew-Frankrehul` is
the Hebrew of the glosses and notes; `Arabic` the Arabic script.

**Marks in the Judeo-Arabic text.** On the 3,047 + 3,665 names and the 1,891 Judeo-Arabic
segments: U+0307 combining dot above (3,240; ג̇ = ج, כ̇ = خ, ד̇ = ذ, צ̇ = ض, ט̇ = ظ), U+0308 diaeresis
(1,990; ה̈ = ة), a few Arabic harakat and shadda that leaked into Hebrew-letter text, nikud on a
handful of headwords (אֶבִל), and one Gujarati nukta (U+0AFE, six forms: an extraction artefact).
Fifteen root names carry punctuation (`מאכול,מצדר`, `אין=אן`, `חיט(ה̈)`, `נזאלה̈ (נזילה̈)`); three are a
lone hyphen.

**The site's own normalization** (`js/search.js`, release 1.1): an optional "strip diacritics"
(one regex: U+0591-05BD, 05BF-05C7, 064B-065F, 0670, 06D6-06DC, 06DF-06E4, 06E7-06E8, 06EA-06ED,
0300-036F), an optional NFKC with a hand-made Arabic presentation-forms map, an optional
cross-script folding (Hebrew and Arabic letters to one Latin skeleton, finals folded there only),
then lower-case. `js/dataLoader.js` only fetches and caches; it normalizes nothing.

**Permission.** `about.html` thanks "the copyright holders, the Academy of the Hebrew Language and
the Israel Academy of Sciences and Humanities, for permission to make the Dictionary digitally
available", and states: "As dictated by the copyright holders, the resources are made available for
research and educational use only. One may not, without written permission from the copyright
holders, employ the dataset or interface for commercial purposes beyond conventional scholarly
publication." The site's credit line is in §9 (a).

---

## 3. The corpus side, as measured (2026-11-03)

First 600,000 lines of `Transcriptions.txt` (Hebrew and Judeo-Arabic pages mixed):

| Fact | Figure | Bears on |
|---|---|---|
| ASCII apostrophe inside words | 164,480 | the HTR writes ג' for ج far more often than ג̇ |
| U+0307 combining dot | 9,256 | the dot does occur, so both must fold to the same key |
| U+0308 diaeresis | 499 | |
| Hebrew geresh U+05F3 | 314 | |
| letters before an apostrophe | י 25,667 · מ 20,237 · ו 13,815 · ר 13,010 · א 11,197 · ב 8,100 · **ג 6,481** | most apostrophes are abbreviations, not Judeo-Arabic dots: the normalizer drops the mark, it does not read it |
| tokens / distinct tokens (after `normalize_ja`) | 4,120,830 / 510,409 | |
| tokens that are a single-word lookup key as they stand | **32.3%** of occurrences, 1.4% of types | phase 2's floor |
| with one proclitic layer and one suffix peeled (`candidate_stems`) | **47.3%** of occurrences, 10.5% of types; 1,143 of the 2,000 most frequent tokens | phase 2's expectation without a lemmatizer |
| against headword-like keys only (roots, sub-entries, headwords, variants; 6,495 keys) | 20.7% direct, 33.2% with stems | the share that lands on an entry rather than inside a quoted phrase |

The sample mixes Hebrew pages in, so the Judeo-Arabic share is higher than these figures; the
figures are the floor of what a click on a word can answer today.

---

## 4. Data model (built: `blau_data/blau.db`, schema 1.0.0)

One SQLite file, 31.7 MB, read-only in use, built by `scripts/build_blau_sidecar.py` from the two
downloaded files. Row counts from the real build:

| Table | Rows | Columns |
|---|---|---|
| `meta` | key/value | `version` 1.0.0 · `source_url`, `source_release` 1.1, `source_release_date`, `source_schema_version` v6 · `source_sha256` (the .gz), `source_bytes`, `source_json_sha256`, `source_json_bytes`, `bibliography_sha256` · `normalizer_version` · `built_at`, `builder`, `builder_revision` · `credit_he`, `credit_en`, `permission_note` · `rows_<table>` for every table · `cross_references` 191, `cross_references_unresolved` 14, `roots_unnamed` 7, `sub_entries_unnamed` 54, `citations_dangling_source` 1 |
| `sources` | 337 | `id`, `abbreviation`, `full_reference`, `page`, `parent` |
| `roots` | 3,047 | `id`, `key` (the JSON key), `ordinal`, `name_ja` (names joined by ` \| `), `names_json`, `name_ja_clean`, `arabic_spelling`, `arabic_norm`, `start_page_printed`, `is_cross_reference`, `target_id`, `num_stems`, `num_sub_entries`, `num_forms` |
| `sub_entries` | 3,665 | `id`, `root_id`, `ordinal`, `name_ja`, `names_json`, `arabic_spelling`, `arabic_norm` |
| `stems` | 8,295 | `id`, `root_id`, `sub_entry_id`, `ordinal`, `binyan`, `binyan_shift`, `government`, `complement_arabic`, `complement_ja`, `head_json` |
| `forms` | 12,753 | `id`, `stem_id`, `root_id`, `sub_entry_id`, `ordinal`, `ja_text` (headword_ja / variant_ja segments), `arabic_spelling` (headword_arabic / variant_arabic), `gloss_he`, `gloss_en`, `notes` (freeform, morph, grammar), `classical_note`, `cross_reference` (the ← target text), `target_ids`, `binyan` (the variant head's, else the stem's), `is_classical`, `is_reconstructed`, `is_erroneous`, `page` (the root's printed page), `segments_json` (every segment, the variant head and the additional notes, for rendering) |
| `citations` | 41,970 | `id`, `form_id`, `kind` (`reference` 28,505 / `dictionary` 13,465), `reference_id`, `ordinal`, `citation_type`, `source_work`, `source_id`, `source_ids`, `citation`, `ja_text`, `translation`, `commentary` |
| `forms_norm` | 41,932 (34,851 distinct keys; 11,101 single-word) | `norm` (indexed), `raw`, `kind` (`root` 3,199 · `sub_entry` 3,959 · `headword` 40 · `variant` 1,652 · `phrase` 33,082: quoted Judeo-Arabic in notes, complements and cited text), `root_id`, `sub_entry_id`, `form_id`, `word_count` |

Rules the build follows:

1. **The download is untrusted data.** It is read with the stdlib `json` module, nothing in it is
   executed or used as a path, and the two files live in an empty directory of their own
   (`blau_data/download/`, gitignored with the artifact).
2. **Fail, never skip.** A missing, empty, truncated (gzip CRC) or malformed file, any entry that
   is not `v6`, any entry or form without an id, any duplicate id: exit 2, nothing written. An
   unnamed root or sub-entry is kept and counted (the dictionary has 7 and 54).
3. **Never shrink silently.** If the target already holds more roots or forms than the build
   would write, exit 3 and leave it, unless `--force`. Equal or larger replaces it.
4. **Atomic.** Written beside the target as `blau.db.building-<pid>`, `foreign_key_check`ed,
   vacuumed, then renamed into place; a failed write leaves the served file and no temp file.
   On Windows a file another process holds open cannot be renamed over (measured: a read-only
   connection from a second process made the rename fail with exit 4; the old file stayed).
5. **Facts about the source are pinned, not hidden.** The one dangling `source_id`, the unnamed
   entries and the cross-reference count go into `meta`, and the gate checks the file against
   them, so a rebuild from a different release says what changed.

---

## 5. The normalizer (built: `shared/blau_normalize.py`, version 1)

`normalize_ja(text)` is the one key. NFC; every combining mark dropped (category M: nikud,
cantillation, the Judeo-Arabic dot and diaeresis, Arabic harakat, the stray nukta) plus the three
punctuation marks the site's regex also strips (paseq, sof pasuq, nun hafukha); the apostrophe /
geresh / gershayim / curly-quote family dropped (`QUOTE_MARKS`, as
`shared.text_normalize.strip_search_diacritics` does on the search path); brackets dropped as
transparent (`חיט(ה̈)` → `חיטה`, the corpus's `]הנתשנ` → `הנתשנ`); finals folded (`ליכון ד̇לך אבהא להם` →
`ליכונ דלכ אבהא להמ`); everything else is a separator, and the key is the Hebrew-letter words joined
by one space. Idempotent; NFD input gives the same key. `tests/test_blau_normalize.py` proves it is
a superset of the site's "strip diacritics" on every code point that regex matches.

`normalize_arabic(text)` keys the Arabic spellings for phase 3: NFKC (which folds the presentation
forms the site maps by hand), marks and tatweel dropped, hamza-carrying alefs folded to bare alef.

`candidate_stems(word)` is the phase-2 heuristic: the word, then with a conjunction (ו, פ), a
preposition (ב, ל, כ) and the article (אל) peeled in that order, each also with one pronoun suffix
peeled (הא, הם, הן, כם, כן, נא, ני, ה, כ, י); never below two letters; the unpeeled word first. It
offers both readings of an ambiguous prefix (`כתאבה` → `כתאבה`, `כתאב`, `תאבה`, `תאב`); the UI shows
them as a list (§6.2). It is not a lemmatizer (§10).

`NORMALIZER_VERSION` is written to `meta`; the gate recomputes every `forms_norm` key with the
running code and fails on any difference, so a sidecar built under other rules is refused rather
than quietly missing matches. (Measured on the real file: changing the bracket rule after a build
made the gate fail with "415 forms_norm keys differ"; the rebuild passed.)

---

## 6. Phases

### 6.1 Phase 1 — the sidecar (this PR)

`scripts/build_blau_sidecar.py`, `scripts/verify_blau_sidecar.py`, `shared/blau_normalize.py`,
their tests, the `.gitignore` entry, one `DATA_LIFECYCLE.md` row, a `changelog.d/` fragment, this
plan. No reader, no route, no bundling: the file exists only where someone ran the build.

### 6.2 Phase 2 — the reader companion on `/browse` (built: branch `claude/blau-browse`)

A reader clicks a word on the transcription. `shared/blau_service.py` (read-only over the
sidecar, resolving its path like `shared/fgp_service.py`, failing closed when the file is absent
or fails a check: schema version, normalizer version, credit line, lookup index, row count,
`quick_check`) looks the word up: `normalize_ja`, then `candidate_stems` in order, each against
`forms_norm`, headword-like kinds first (root, sub-entry, headword, variant). Every entry a key
reaches is one card: the entry name in Hebrew letters with its marks, the Arabic spelling, the
printed page, the site's entry link (`entry/blau-e-…`, its own `openInReader`), then its senses
-- the matched form first, then the first forms under the matched sub-entry or root
(`MAX_SENSES` 4; the rest counted and linked), each with its text, binyan, Hebrew and English
glosses (the UI language's first) and flags (classical, reconstructed, erroneous); a
cross-reference root leads to its target ("via"). Several hits, or several candidates that hit
(`כתאב` and `תאב`), are several cards in candidate order, not merged: the reader decides. A key
found only inside the dictionary's own example phrases (kind `phrase`) is listed apart, "also in
the examples under", because the entry it sits in defines another word -- on the real file that
is most of what a common word (וקאל, פי, מן) reaches. Ambiguity is the norm here, not an error.
The credit line of ruling (a) is on the panel, always: the renderer draws nothing without it.

Web first (ruling b): `web/static/js/blau_lookup.js` finds the word under the pointer (caret
from point, widened over Hebrew letters, combining marks and the apostrophe family), wraps it in
a highlighted span, places the popover beside it and dispatches `blau-lookup` on it; the page
answers from the service off the loop and `web/components/blau_panel.py` draws the cards into
the popover (`position: fixed`: beside the word from 1024px, a sheet at the foot of a phone).
The next click on no word, or the panel's close, hides it; the word stays highlighted until
then. A page without a served sidecar (or with `BLAU_DICTIONARY_ENABLED=0`) carries no script,
no popover and no marked pane. Desktop (a later ruling): the same lookup in the Browse viewer's
context menu, same card, same credit.

Not in this PR: the "what is this word" entry point on `/search`'s result snippets (that page is
in another session's hands; it follows once the panel is live), and the root-skeleton fallback
of §10.

### 6.3 Phase 3 — search by meaning

A Hebrew, English or Arabic-script query that matches a gloss or a spelling offers the
Judeo-Arabic forms. Lookup: `gloss_he` / `gloss_en` by whole word (an FTS5 table over the glosses,
built in the sidecar in phase 3), `arabic_norm` by `normalize_arabic` of the query. The forms found
(their `raw` spellings, with the marks dropped the way the corpus drops them, plus the apostrophe
spelling the HTR uses) are shown as chips under the search box: "Judeo-Arabic forms for 'judge':
קאצ̇י · אלקאצ̇י · קצ̇א". Ticking chips runs them as one alternation through the existing variants
machinery (`shared/search_engine.py::build_regex_pattern`, the same path `#word#` and the Variants
slider use), so the result list, the highlighting and the exports change nothing. Nothing runs
without the reader's tick; the chips are an offer, as the zero-result fallback the review proposes.

### 6.4 Phase 4 — the reverse link

Each entry shows how many corpus pages carry its forms. A nightly job (where the search index
lives) runs each single-word key of kind root / sub-entry / headword / variant as a whole-word
search and writes `(norm, page_count, sample_sys_ids)` to a small table beside the sidecar (not
in it: the sidecar is a build artifact of the dictionary, the counts are a build artifact of the
corpus and change with every index rebuild). The companion card shows "on 1,284 pages", linking
to the search. Costs: 6,495 keys × one indexed whole-word search; measured on a later PR.

---

## 7. Code to change

| File | Change | Phase |
|---|---|---|
| `shared/blau_normalize.py` (new) | `normalize_ja`, `normalize_arabic`, `candidate_stems`, `NORMALIZER_VERSION` | 1 |
| `scripts/build_blau_sidecar.py` (new) | the build (§4) | 1 |
| `scripts/verify_blau_sidecar.py` (new) | the gate (§8) | 1 |
| `tests/test_blau_normalize.py`, `tests/test_build_blau_sidecar.py` (new) | 43 + 35 tests on a synthetic v6 fixture (no dictionary text in the repository) | 1 |
| `tests/test_no_back_edges_core.py`, `.gitignore`, `docs/architecture/DATA_LIFECYCLE.md`, `docs/CODE_INDEX.md`, `changelog.d/` | registration | 1 |
| `shared/blau_service.py` (new) | read-only lookups, fail-closed open, the card model; `tests/test_blau_service.py` | 2 (built) |
| `web/components/blau_panel.py`, `web/static/js/blau_lookup.js` (new), `web/pages/browse.py`, `web/static/common.css`, `web/main.py`, `shared/genizah_translations.py` | the word click, the popover and its cards, the credit line, the startup check; `tests/test_blau_panel.py`, `tests/render_smoke/test_blau_panel_render_smoke.py` | 2 (built) |
| `scripts/deploy_blau_sidecar.ps1` (new), `docs/guides/DEPLOYMENT_TECHNICAL.md`, `docs/guides/ENV_VARS.md`, `CLAUDE.md` | the server copy (owner); the kill switch | 2 (built) |
| `desktop/viewers.py`, `desktop/result_dialog.py`, `GenizahSearchPro.spec` | the context-menu lookup and the bundling (desktop: a later ruling) | 2 (later) |
| `web/pages/search.py`, `shared/search_engine.py` | gloss lookup, chips, one alternation into the variants path; FTS5 table in the build | 3 |
| a nightly script + table | page counts per key | 4 |

---

## 8. Gates

- **Build:** exits 2 on a bad download, 3 on a shrink without `--force`, 4 on a failed write;
  `tests/test_build_blau_sidecar.py` covers each path, the atomic rename, the temp-file cleanup and
  the unreadable-target case.
- **`scripts/verify_blau_sidecar.py --db blau_data/blau.db --expect-roots 3047 --expect-forms 12753`**
  (optionally `--expect-source-sha256`, `--expect-version`): file present and non-empty;
  `integrity_check`; every table; `foreign_key_check`; every required `meta` key; `normalizer_version`
  equals the running module; every `rows_<table>` equals the table; the expected roots and forms;
  no empty table; unnamed roots and dangling sources equal what `meta` pinned and stay under 1% /
  0.1%; cross-reference targets resolve; `num_forms` per root; every form renders; every lookup key
  recomputed; the lookup index present. Fails rather than skips on a missing file.
  **Proven to fail on the real file** (2026-11-03): ten forms deleted → "forms 12,743 != expected
  12,753", "meta.rows_forms", "num_forms that differs", foreign keys; one in 97 keys edited → "432
  forms_norm keys differ from normalize_ja(raw)"; `rows_forms` doctored → "meta.rows_forms = '3047'
  but the table holds 12,753 rows". The PR carries the three runs.
- **Before any deploy (phase 2):** the gate on the dev copy, then on the server copy, as
  `scripts/deploy_fgp_sidecar.ps1` does for the FGP sidecar; the server copy goes up before the
  code (owner).
- **Normalizer change:** bump `NORMALIZER_VERSION`, rebuild, re-run the gate; CI's
  `tests/test_blau_normalize.py` pins every rule with real spellings.

---

## 9. Owner rulings

| # | Decision | Recommendation / state |
|---|---|---|
| **(a)** | **Permission and credit.** Does the permission the site cites ("published with permission"; research and educational use only; no commercial use beyond conventional scholarly publication) cover bundling the data in GenizahSearch — a free research site and a free desktop app — and what credit line must every surface show? The site's own line is: יהושע בלאו, מילון לטקסטים ערביים־יהודיים מימי הביניים, ירושלים: האקדמיה ללשון העברית והאקדמיה הלאומית הישראלית למדעים, תשס"ו (2006) © האקדמיה ללשון העברית והאקדמיה הלאומית הישראלית למדעים. פורסם ברשות | **RULED 2026-11-03: "Treat as permission."** The site's permission is taken to cover GenizahSearch; the data may be served. The credit line every surface shows is the site's own line above (`meta.credit_he`), with the site linked; the English rendering in `meta.credit_en` is still a public string the owner approves when phase 2 first shows it |
| **(b)** | **Web-only or both apps?** | **RULED 2026-11-03: "Web only first."** Phase 2 ships on `/browse` on the website; the desktop context-menu lookup and the installer bundling (§7) wait for a later ruling |
| D1 | The lookup key drops the corpus's apostrophe family as well as the combining marks | decided (§3: 164,480 apostrophes against 9,256 dots) |
| D2 | Brackets are transparent, not separators | decided (`חיט(ה̈)`, `]הנתשנ`) |
| D3 | Unnamed roots (7) and sub-entries (54) are kept, counted and pinned, not dropped | decided |
| D4 | Quoted Judeo-Arabic phrases (notes, complements, cited text) are lookup keys of kind `phrase`, after the headword kinds | decided; phase 2 ranks them last |
| D5 | The bibliography is a required input of the build (citations carry its ids) | decided |
| D6 | No dictionary text in the repository: tests use a synthetic v6 fixture | decided (pending (a)) |

---

## 10. Risks

- **Judeo-Arabic morphology without a lemmatizer.** `candidate_stems` peels one proclitic layer
  and one suffix; it misses broken plurals (كتب for كتاب, רגאל for רגל), verb inflection (יאכ̇ד̇,
  אכ̇ד̇ת), the assimilated article (אלשמס written אשמס), and the doubled-letter stems. Measured
  floor: 47.3% of token occurrences reach some key, 33.2% an entry. The honest UI shows nothing
  rather than a wrong entry, and offers the site's own search for the rest. A root-skeleton fallback
  (three consonants against the root names, after the weak letters א ו י are dropped) is the next
  step if the owner wants recall over precision; it is not in this plan.
- **HTR dropping diacritic dots.** The corpus writes ג for both ג and ג̇ far more often than it
  marks the dot, so the key must be mark-free, and so `אכד` (أخذ) and any `אכד` without the dot
  share one key. Ambiguity between dotted and undotted readings is shown, not resolved (§6.2);
  phase 3's chips offer both the dotted and the apostrophe spellings so a search reaches every
  page.
- **The credit line.** Public strings are the owner's; the line must appear on every surface that
  shows a gloss (panel, chips, exports, the API if it ever serves this). Ruling (a) settled the
  permission; the English rendering of the line is approved when phase 2 first shows it.
- **The source may change.** Release 1.1 is pinned by digest in `meta`; a new release is a new
  build, a new gate run, and a diff of the counts the gate pins (§4 rule 5).
- **Windows cannot replace an open file.** The build's atomic rename fails (exit 4, old file kept)
  while another process holds the sidecar; the deploy script of phase 2 must stop the reader first
  on Windows, as the FGP deploy's swap-then-restart does on Linux.

---

## 11. Review log

### Phase 2 (PR #473)

**Round 1** (`a4b6796d`), Codex: two P2, both fixed.
- A lookup that finished after a later click or a close could overwrite the newer word's card
  or reopen a closed panel: the page keeps a request counter, a close bumps it, and a completion
  that is no longer the latest draws nothing (`test_a_slow_earlier_lookup_does_not_overwrite_the_newer_word`,
  `test_a_close_during_a_lookup_keeps_the_panel_closed`).
- The panel's own close control left the word highlighted (the script returned early on a click
  inside the popover): a click on `.blau-close` now unwraps the mark before the page hides the
  popover.

### Phase 1 (PR #472)

**Round 1** (PR #472, `4794cf69`), Codex: two P2, both fixed.
- The gate let a cross-reference root with a NULL `target_id` pass (the dangling-target check
  tested only non-NULL targets). The real file has 14 such roots, the site's own unresolved
  pointers, so the build now pins `meta.cross_references_unresolved` and the gate fails on any
  difference (a NULLed target is a mutation) and on more than 10% of the cross-references.
- A scalar `name_ja` (or any field of the wrong shape) raised an uncaught `TypeError` instead of
  exit 2: `name_ja` / `arabic_spelling` are validated as lists of strings, and `main` turns any
  remaining shape error from the walk into a source failure with the exception named.

**Round 2** (`19b39e05`), Codex: three P2, all fixed.
- The gate checked table names only, so a file with a dropped column passed: it now reads the
  builder's `DDL` into an in-memory database and compares every table's columns (name, type,
  NOT NULL, primary key) and the index list with the file, and requires `meta.version` to equal
  the schema this code reads, with `--expect-version` as an extra.
- Only unnamed roots were compared with their pinned count; an erased `sub_entries.name_ja`
  passed: the sub-entry count is compared with `meta.sub_entries_unnamed` too (ceiling 5%).
- A parent directory that cannot be created (a file in its place) raised before the output-error
  handler: directory creation and the temp file are inside the guarded block, exit 4.

**Round 3** (`1fb5d692`), Codex: three P2, all fixed; the budget ends here.
- A schema mismatch was recorded but the later checks still queried the missing column and
  raised: the gate now stops after the schema report ("no further checks were run").
- The cross-reference total was used only for a ceiling; a root whose flag was cleared passed:
  it is compared with `meta.cross_references` like the other pinned counts.
- A duplicate bibliography id surfaced as a write failure (exit 4): it is a source error (exit 2),
  and ids/abbreviations must be strings.

**Round 4** (`db23b348`), Codex: two P2, both two-line fixes, taken although the budget was spent
(2 -> 3 -> 3 -> 2): a stray table whose name needs quoting made the schema walk raise (the
table-valued `pragma_table_info(?)` takes the name as a parameter); `num_stems` and
`num_sub_entries` are compared with their rows as `num_forms` was. Anything a later review finds
is a `docs/open_issues.d/` fragment, not another round.

**Round 5** (`7fca394c`), Codex: one P2, recorded as
`docs/open_issues.d/2026-11-03-blau-gate-form-level-targets.md` (P3): the gate does not check the
form-level cross-reference targets in `forms.target_ids`. Measured on the real file: 862 targets,
688 to roots and 174 to stems, none unresolved, so the check must look beyond `roots`; the fragment
carries the fix sketch (check against every id table, pin the counts in `meta`).
