# Catalogue text search: find manuscripts by what their catalogue records say

**Status:** plan **rev. 3** (2026-11-05). **Settled by the owner, 2026-11-05: D1-D7 as
recommended, plus a `%` shortcut and a note that PGP and NLI records change (§4.6).** **PR 1 (the
data file) is built** on branch `claude/catalogue-text-data`: the real file and its numbers are in
§3.1, the FJMS translation audit D2 asked for is in §3.2. Nothing is served; PR 2 is next. The shape was approved on 2026-11-03 ("Yes, catalogue round"); this
document writes it down, measures it on a throwaway prototype (§3), and corrects one point against
a binding decision (§2.3).
**Origin:** the PostHog usage review of 2026-11-03 (90 days of web searches, repeated identical
searches collapsed per person and day): Latin-script searches got zero results 100% of the time
(65 searches by 37 English-browser readers, 26 by 19 Hebrew-browser readers), and a share of them
were English topic words and names ("cheese", "wine", "Baghdad", "Yannai") — the words PGP and
FJMS use in their descriptions. More broadly, English-browser readers' Hebrew-letter searches find
nothing 53% of the time against 33% for Hebrew-browser readers. Earlier prior art: the pending
todo `.planning/todos/pending/2026-03-09-unified-metadata-text-search-with-translations.md`
(option A, one FTS5 index over every source, preferred; "the user should be able to choose
whether to include translations") and `docs/FEATURE_IDEAS.md` v8.0 shortlist #2.
**Order:** the first of three rounds discussed 2026-11-05 — catalogue text search, then searching
editions (FGP, PGP, community corrections), then desktop users fetching the index instead of
building it. This round depends on neither of the others (§7).
**Scope:** web, desktop, the public Search API and MCP.

---

## 1. The problem in one paragraph

A reader who types כתובה, גבינה, "marriage contract" or "Maimonides" is asking which manuscripts
the catalogues *describe* that way. Every search mode looks in the transcriptions (or, for Title
mode, in the `libraries.csv` titles). The catalogues' own text — FJMS free descriptions, running
titles, textual frames, full catalogue entries, PGP's 36,641 English descriptions — is searchable
only inside Catalog Browse's text filter, which covers FJMS's original text alone, and the 759,312
automatic translations between Hebrew and English are searched nowhere. So an English reader
cannot find a Hebrew-only catalogue record, a Hebrew reader cannot find a PGP description, and a
Latin-script query always ends at zero.

## 2. What exists today

### 2.1 Sources (main checkout, read-only queries, 2026-11-05)

| Source | Text | Rows | Volume | Language |
|---|---|---|---|---|
| `fist_data/fjms_enrichment.db` (v5.1.0) `catalog` | `GenizahTitleOrgTitle` (work title), `GenizahTitleEngTitle`, `Comment`, `TitleHeb`/`Title`, `Colophon`, `AuthorText`, `CopyPlace` | 731,354 records over 226,555 manuscripts (1–8+ per manuscript) | work titles 2.3M + 2.2M chars, comments 4.2M | he / en |
| `catalog_running_titles` | `RunningTitle` | 317,779 | 13.0M | mostly he |
| `catalog_free_desc` | `FreeDesc` | 303,638 | 42.0M | he / en |
| `catalog_full_texts` | `FullText` | 95,037 | 26.9M | he / en |
| `catalog_textual_frames` | `TextualFrameHeb`, `TextualFrameEng` | 299,294 | 10.2M + 11.2M | he, en |
| `catalog_mentions` | `Mention`, `MentionDesc` (people, places) | 34,897 | — | he / en |
| `fjms_translations` | `translated_text` (model `dictalm2.0`) | 759,312 (he2en 462,629, en2he 296,683) | ~111M | automatic |
| `pgp_data/pgp.db` `documents` | `description`, reached per manuscript through `document_fragments.sys_id` | 36,641 | 10.8M | en |
| `libraries.csv` col 7 | Hebrew title | 192,427 | 14.1M | he |
| `libraries_translations.db` `title_translations` | `english_title` (source `dicta`) | 184,514 | 8.9M | automatic |

`AlmaId` **is** the sys_id: 226,554 of 226,555 FJMS manuscripts are in `libraries.csv`. No
remapping is needed.

### 2.2 Searches that already touch this text

- **FJMS `catalog_fts`** (`scripts/export_fist_enrichment.py::create_fts5`, read by
  `shared/fjms_service.py:1569-1623` and `:2839-2895`): the "catalogue text" filter in Catalog
  Browse and in the Manuscript features panel. Contentless (no snippets possible), default
  tokenizer, no nikud / final-letter / prefix handling (`_fts_escape` quotes each term), one row
  per manuscript built from the *first* catalogue record's title fields (harmless: only 14
  manuscripts have differing Hebrew titles across records), and it leaves out the
  work titles, comments, mentions, PGP, the library titles and every translation.
- **Title mode (`$`)**: a substring scan of `libraries.csv` titles (`shared/metadata_manager.py::search_by_meta`).
- **PGP Tags**: PGP tags only, not descriptions (`shared/document_service.py::get_fragments_by_tag`).

### 2.3 Correction to the 2026-11-03 shape: no PGP Hebrew translations

The 11-03 proposal said the build would read PGP's Hebrew descriptions (`pgp_translations`) from
the production copy or Supabase. **That table is withheld by the owner's decision of 2026-09-21**
(`docs/plans/PGP_TRANSLATION_QUALITY.md`: 27.8% of a 180-row sample wrong or garbage, shipped on
neither surface). This plan leaves them out. A Hebrew reader reaches PGP records through the FJMS
and library text of the same manuscripts, and through English words.

The FJMS translations have **never been graded** the way the PGP ones were. §8 D2 asks what to do
about that, because a large share of hits come only from them (§3).

## 3. Prototype measurements (throwaway, scratchpad, 2026-11-05)

One SQLite file: a `rows` table (sys_id, source, field, language, automatic flag, original text)
and a contentless FTS5 table over the normalized text (`unicode61 remove_diacritics 2`; nikud and
cantillation stripped, geresh/gershayim inside a word removed so רמב"ם = רמבם, final letters
folded). Placeholder titles ("Cannot be determined from the catalog") were left out.

- **2,779,318 rows, built in 97 s, 736 MB on disk, ~191 MB compressed** (deflate level 6; the
  installer's LZMA will do somewhat better).
- **Speed:** 2–63 ms for the FTS lookup, 4–160 ms including the rows; answered in the server
  process, no research worker.

| Query | Manuscripts | Of which by original text (no translation) | With Hebrew prefixes | Time |
|---|---|---|---|---|
| כתובה | 2,212 | 1,374 | 2,531 | 58–65 ms |
| גבינה | 37 | 19 | 54 | 4–6 ms |
| marriage contract | 966 | 728 | — | 33 ms |
| ketubba | 1,043 (PGP 948) | 1,016 | — | 27 ms |
| רמב"ם | 1,883 | 1,513 | 4,432 | 52–158 ms |
| Maimonides | 5,086 | 3,030 | — | 114 ms |
| Guide for the Perplexed | 523 | 349 | — | 34 ms |
| cheese | 123 (PGP 99) | 108 | — | 5 ms |
| תפילין | 368 | 342 | 393 | 14–15 ms |
| פיוט ליום כפור | 32 | 31 | 40 | 7 ms |

"With Hebrew prefixes" = the query also matches the word after ה ו ב ל מ ש כ and their common
pairs (וה, וב, ול, שה, מה, בה, לה). Three things the numbers say:
1. **Translations matter.** For כתובה, 38% of the manuscripts are found only through an automatic
   translation; for Maimonides, 40%.
2. **Prefixes matter for Hebrew:** +14% (כתובה) to ×2.4 (רמב"ם, mostly הרמב"ם).
3. **English and Hebrew do not meet by themselves.** כתובה finds 2,531, "marriage contract" 966,
   "ketubba" 1,043 — different sets. Synonyms and transliterations are out of scope for this round.

### 3.1 The first real build (PR 1, 2026-11-05)

`scripts/build_catalog_text_sidecar.py` on the main checkout's sidecars (FJMS 5.1.0, PGP of
2026-09-22, `libraries.csv`, `libraries_translations.db`), into the scratchpad:

- **2,456,035 records over 218,839 manuscripts, 732 MB, built in 3.5-5 minutes** (the dedup
  hashing and the key computed per record make it slower than the prototype's 97 s). By source:
  FJMS 1,455,314 original + 691,708 automatic, PGP 37,153, library titles 166,888 + 104,972
  automatic. Left out: 596,160 FJMS and 104,854 library texts that repeat a text already stored
  for the same manuscript, 3,761 placeholder titles and authors (לא ידוע, לא מזוהה, "Unknown",
  "Not Coded"), and 91 work-title translations whose title id no catalogue record carries.
- **Work-title and author translations are keyed by the title id and the author's text**, not by
  a manuscript (`scripts/translate_fjms_catalog.py`; found by the Codex review of PR 1, #483). The
  first build passed those ids as sys_ids and dropped all 871 of them (624 work titles, 247
  authors). Each one now goes to every
  manuscript whose catalogue record carries that title or author: 13,745 manuscripts through
  their work titles and 1,476 through their authors ("Nathan ben Nahorai" finds 35).
- **The gate passes** on it in 3 minutes (`--expect-rows 2456035`, 2,000 sampled records re-keyed,
  eight canaries, `quick_check`).
- **English plurals** were measured before the rule in §4.2 was added: the plural alone added
  31-64% more manuscripts (letter 18,449 -> 28,567 with letters; contract 1,914 -> 3,097; prayer
  13,123 -> 20,772; poem 15,412 -> 20,262; merchant 540 -> 884). With the rule, "letter" and
  "letters" find the same 28,567.
- **Speed on this laptop, warm:** 1-120 ms for ordinary words (כתובה 87 ms, ketubba 11 ms, cheese
  4 ms, Maimonides 82 ms), but **0.3-1.2 s for the commonest** (פיוט, 52,862 manuscripts, 1.2 s;
  תורה 0.4 s; piyyut 0.36 s; letter 0.28 s). PR 2 therefore runs the search off the event loop
  (`run.io_bound`) and measures it on the server; a bound on very common words is a PR 2 choice.

| Query | Manuscripts | Original text only |
|---|---|---|
| כתובה | 2,537 | 1,581 |
| גבינה | 54 | 27 |
| marriage contract | 1,008 | 754 |
| ketubba | 1,045 | 1,018 |
| רמב"ם | 4,438 | 3,530 |
| Maimonides | 5,082 | 3,030 |
| cheese | 126 | 111 |
| תפילין | 394 | 364 |
| פיוט ליום כפור | 40 | 38 |

### 3.2 How good are the FJMS automatic translations? (D2, 2026-11-05)

A seeded random sample of 150 of the translations the file searches
(`scripts/sample_fjms_translations.py`, seed 20261105: 97 he2en, 53 en2he; FreeDesc 58,
RunningTitle 55, TextualFrame 20, FullText 17), graded with the PGP audit's method: one reader
graded every row good / minor / wrong / garbage against its original, and a second reader, told
to overturn stylistic nitpicking, re-judged the 23 adverse grades (21 upheld, 2 downgraded to
minor, 1 wrong raised to garbage).

| Grade | Rows | Share |
|---|---|---|
| good | 100 | 66.7% |
| minor | 29 | 19.3% |
| **wrong** | 18 | 12.0% |
| **garbage** | 3 | 2.0% |

**14.0% wrong or garbage, 95% CI 8.4-19.6%**, against 27.8% (21.2-34.3%) for the withheld PGP
Hebrew translations: about half, and the intervals do not overlap. By direction: he2en 12.4%,
en2he 17.0%. **By field, FullText is the weak one: 7 of 17 wrong or garbage**, against 4 of 20
TextualFrame, 5 of 55 RunningTitle and 5 of 58 FreeDesc. The sample is small, so this is a
signal, not a rate. 18 of the 21 bad rows would mislead a word search: a wrong tractate, person,
folio or verse number, a dropped shelfmark, "cantillated" rendered as "vocalized" (three times),
and one translation that invents some thirty catalogue entries. **Under D2 (a) the translations
are searched, every such hit is labelled "automatic translation", and the switch starts ON.**
**Which translations the audit measured.** The sample was drawn from the `fjms_translations`
table, not from the built file, and the two populations differ (pointed out by the Codex review
of #483). 146 of the 150 sampled rows enter the sidecar; the 4 that do not were left out as
duplicates of an original of the same manuscript, including 1 of the 3 garbage rows (a
"translation" that is the English original verbatim). **Among the 146 indexed rows: 20 wrong or
garbage, 13.7% (95% CI 8.1-19.3%)**, so the ruling stands. Not measured: the work-title and author
translations, which reach many manuscripts each and make up 16,885 of the file's 691,708
automatic FJMS records (2.4%); the sample held none. A later audit should draw from the file
itself: `scripts/sample_fjms_translations.py --sidecar catalog_data/catalog_text.db` samples its
automatic records uniformly and traces each to its translation row. On today's file that draw
holds 2 work-title translations in 150. The verdict files are in the session scratchpad; the
table sample is reproduced by the same script without `--sidecar` (seed 20261105).

## 4. Design

### 4.1 The data file

- **`catalog_data/catalog_text.db`**, a derived sidecar built by
  `scripts/build_catalog_text_sidecar.py` from the four inputs (all opened `mode=ro`). It is a
  file of its own because FJMS (1.7 GB) is rebuilt by the owner, PGP is re-exported, and the
  libraries files change separately: a derived file rebuilt from all of them gets one row in
  `docs/architecture/DATA_LIFECYCLE.md`, its own gate, and a rebuild whenever an input changes
  (3.5-5 minutes, §3.1). The `catalog_data/` directory is gitignored like the other sidecar directories.
- **Rows:** one per (sys_id, source, field, language, automatic, text). A text identical after
  normalization to one already stored for the same manuscript is stored once, whatever field said
  it. Originals are read first, so an automatic translation that repeats an original adds nothing.
- **FTS5:** the normalized text in three columns by weight — titles (work title, running title,
  library title, `TitleHeb`), descriptions (free description, PGP description, textual frame,
  comment, mentions) and full catalogue text — so bm25 ranks a title match above a match deep in
  a full entry. bm25 weights 3 / 1.5 / 1 (`COLUMN_WEIGHTS` in the service).
- **Snippets:** the card shows ~200 characters of the original text around the first match, with
  the matched words marked the way result cards already mark them (`*word*`, rendered by
  `SearchEngine.format_snippet`). The original text is re-tokenized by the same normalizer, and
  each word whose key is a query key is marked. A literal `*` in the text becomes `∗`.
- **`meta`:** schema version, normalizer version, built-at, each input's version or fingerprint
  (FJMS `meta.version`, PGP `upstream_commit`, sha256 of `libraries.csv` and
  `libraries_translations.db`), row counts per source and field.
- **Left out:** PGP Hebrew translations (§2.3), PGP transcriptions (they belong to the editions
  round), FJMS person descriptions (`PersonEngDesc`/`PersonHebDesc` describe people, not
  manuscripts), placeholder titles.
- **Gate:** `scripts/verify_catalog_text_sidecar.py --db … --expect-rows N`. It fails rather than
  skips. It checks the schema, the counts per source against `meta`, every record's sys_id, field
  and language label, and a seeded sample of records (2,000 by default, all with `--sample 0`)
  re-keyed by today's normalizer, each of which must find itself in the index. It then runs eight
  canary searches with floors at about half of the first build's counts, and `quick_check`. Its
  tests show it failing on ten kinds of damage.

### 4.2 Normalization (one module, versioned)

`shared/catalog_text_normalize.py` (NFKD with every combining mark dropped, the same quote family
as `shared/text_normalize.py::strip_search_diacritics`, the final-letter fold of
`shared/blau_normalize.py`). Its
`NORMALIZER_VERSION` is pinned in `meta`, and the service refuses a file whose version differs. The
same function runs on both sides: on the text at build time, and on the query at search time.

- Hebrew: nikud and cantillation stripped, geresh/gershayim inside a word removed, final letters
  folded, prefixes expanded at query time (§8 D3).
- English: case and accents folded (Fusṭāṭ = Fustat), and **every Latin-letter word made singular**
  by a small rule of our own (`english_singular`: letters -> letter, stories -> story, churches ->
  church; glass, Exodus, Genesis stay), measured in §3.1. It is not SQLite's Porter stemmer,
  because the snippet highlighter must run the very same function, and it touches no Hebrew,
  Arabic or digit.
- Several words: every word must occur in the same catalogue record (row), as in the prototype.
  Quotation marks ask for the words next to each other.

### 4.3 Results

- **One result per manuscript**, ranked by its best-matching record. At an equal match, original
  text comes before automatic translation, and titles before descriptions before full text.
- **The card** reuses the standard result card (`web/pages/search_results.py::create_result_card`),
  showing shelfmark, library, title, the snippet and **where the text comes from**: "FJMS catalogue
  · free description", "Princeton Geniza Project description" or "Library catalogue title", plus
  "automatic translation" when it is one. The strings already used by the catalogue dialogs are
  reused where they exist.
- **Catalogue hits are never mixed into transcription results.** They are a separate mode or a
  separate offer (the catalogue is a yardstick, not evidence of what the text reads).
- The row cap and paging are the ones result lists already have (`render_results[:200]`, more on
  request), and the count is exact because the lookup is cheap.

### 4.4 Surfaces

- **Web search page:** a "Catalogue" entry in the mode selector (`web/pages/search.py:728-751`,
  both dicts; `web/search_bootstrap.py::VALID_SEARCH_MODES` for `&mode=catalog`). Rows come from
  the server-process path PR #468 built for metadata modes (`web/research_jobs.py`
  `METADATA_SEARCH_MODES`, `IsolatedEngine._metadata_search`), or a direct `run.io_bound` call to
  the service, whichever the PR finds simpler.
- **Shortcut `%` (owner, 2026-11-05):** `% כתובה` or `%cheese` searches the catalogue, as `$` does
  Title and `#` Shelfmark. The prefix map is written out in three places, and all three get it:
  the engine's `parse_query_syntax` (`shared/search_engine.py:3414`), the web live shortcut list
  (`web/pages/search.py:640`), and the desktop `_SHORTCUT_PREFIXES` (`genizah_app.py:18474`).
  The mode label shows it (`tr('Catalogue') + ' (%)'`), and so do the shortcut help, the search
  syntax help on both apps and `docs/SEARCH_API.md` where prefixes are described. `%` is free in
  every non-Responsa mode. **Inside Responsa mode `%word` keeps its Responsa meaning**
  (`shared/responsa.py:539`), exactly as `#word` does today, because `parse_query_syntax` returns
  before the prefix map when Responsa is on. A test pins both sides: `%word` switches to the
  catalogue in Exact mode and is left alone in Responsa. The query is URL-encoded on the way to
  `/search?q=` (`quote`, `web/pages/search.py::_search_url`, `web/home_query.py`), so `%` survives
  a link.
- **Zero results:** when a text search finds nothing, the catalogue search runs at once and one
  line offers "No matches in the transcriptions · N catalogue records mention X". It sits in the
  same row as the wrong-layout "Did you mean" (`input_notice_row`, `_show_layout_suggestion`) and
  sends PostHog `search_suggestion` with `kind='catalogue'`. When both apply, "Did you mean" comes
  first.
- **Latin-script queries** (§8 D6): searched in the catalogue directly, with a one-line note
  "Searched the catalogues. Search the transcriptions instead", the pattern the shelfmark notice
  set (`_show_shelfmark_notice`).
- **Header and home boxes:** `web/home_query.py::home_query_target` sends Latin-only words to
  `/search?mode=catalog&q=…`. Hebrew words keep going to text search, where the zero-result line
  catches them.
- **Public Search API:** `search_mode='catalog'` on `POST /api/search`, synchronous (fast, never a
  job): the `SearchRequest.search_mode` Literal (`web/search_api.py:290`),
  `_SEARCH_MODE_TO_INTERNAL` (`:386`, which also drives the capabilities list pinned by
  `tests/test_capabilities_api.py:139`), the gap validator (`:337`, a metadata mode), and
  `docs/SEARCH_API.md:287`. Each result carries `catalogue_source`, `field`, `automatic_translation`
  and the snippet.
- **MCP:** a read-only tool `search_catalogue` (pattern: `list_manuscript_features`,
  `web/mcp_server.py:617-631`, through a `/chatgpt/catalogue` route) and a line in the
  `SEARCH_MODES` prose (`:87`). The tool and annotation sets in `tests/test_mcp_tools.py:140` and
  `integrations/mcp/smoke_test.py` grow by one.
- **Desktop:** a "Catalogue" entry in the mode combo (`genizah_app.py:6152`). Its index-to-mode
  lists are written out six times (`:23349`, `:23368`, `:18499`, `:24584`, `:25011`, `:25094`),
  so PR 3 first folds them into one helper. Rows are built like the PGP Tags results
  (`_on_tag_search_results`, `:26203`: `display`, `snippet`, `raw_header`) and loaded by
  `load_next_batch`. The zero-result line goes beside `_install_layout_suggestion` (`:24409`), and
  the telemetry enum (`_SEARCH_MODE_ENUM`, `:23372`) gets the value.
- **Analytics:** `search_executed` with `mode='catalog'`, plus `search_suggestion`
  `kind='catalogue'` shown/taken on both apps, so we can see whether readers use it.
- **Not in this round:** Catalog Browse's text filter and the Manuscript features "catalogue text"
  rule stay on FJMS's own index (§8 D7). Title mode and PGP Tags stay (§8 D4).

### 4.5 Service, flag, deploy

- `shared/catalog_text_service.py` follows `shared/arabic_service.py`: LOCALAPPDATA path first
  (`default_sidecar_path`), read-only connection per thread, `is_ready()` (schema and normalizer
  versions, required tables, row counts, `quick_check`), opened once at web startup off the loop
  (like `open_blau_sidecar_on_startup`), a `get_/reset_` singleton, and registration in
  `tests/test_no_back_edges_core.py`.
- **`CATALOG_TEXT_SEARCH_ENABLED`** (default `false` until the file is on the server) ANDed with
  `catalog_text_available()`, per decision 0004, with a flag-ON/file-missing test. It gets a row in
  the CLAUDE.md flags table and `docs/guides/ENV_VARS.md`.
- **Server copy:** `scripts/deploy_catalog_text_sidecar.ps1 -Rows N` on the Blau script's pattern
  (gate locally, scp to `.new`, sha256 and gate on the server, keep `.prev-<stamp>`, swap,
  restart, gate the served file). The file goes up before the code. This step is the owner's.
- **Desktop copy:** `GenizahSearchPro.spec` `datas` + `build_app.bat` (WAL checkpoint,
  `check_shipping_sidecar.py`), §8 D5.
### 4.6 Keeping it current: PGP and NLI records change (owner, 2026-11-05)

The file is a snapshot of records that their owners keep editing. PGP publishes changes to its
metadata repository (our copy is the 2026-09-22 refresh, 36,642 documents, upstream commit of
2026-09-21), and NLI revises its catalogue records. Those changes reach us through
`libraries.csv`, the FIST/FJMS export and the title translations made from them. The catalogue
search answers from the records the site (or the installed desktop) currently has, and nothing
in the results says a date (owner, 2026-11-05: "of course it's as of current records"). What
matters is that the file is rebuilt whenever those records are refreshed, so it never lags
behind the rest of the site:

- **Inputs recorded, internally.** `meta` records each input's own version and date (PGP
  `upstream_commit`/`upstream_committed`, FJMS `version`/`created`, the `libraries.csv` and
  translations fingerprints). This is for the drift check below, not for display.
- **Rebuild with every refresh of an input.** One line in the external data refresh routine
  (and in `docs/architecture/DATA_LIFECYCLE.md`'s row): after a PGP refresh, an FJMS/FIST rebuild,
  or a `libraries.csv` / translations change, rebuild the file (100 s), gate it, deploy it. The PGP
  refresh pipeline gets this as its last step.
- **Drift is detected, not guessed.** At startup the service compares the PGP and FJMS versions
  recorded in `meta` with the sidecars actually installed and logs a warning naming both. It does
  not refuse, since a slightly old catalogue is still useful. The server's health/admin page
  shows the same comparison.
- **The desktop's copy ages with its installer.** It changes only with a new installer, the same
  as every bundled sidecar today, until an update channel for sidecars exists (SEED-005, §7), and
  it always matches the PGP and FJMS files that installer carries.
- **sys_ids that disappear:** a manuscript dropped from the catalogue between rebuilds is skipped
  when results are drawn (no card for a sys_id the metadata no longer knows), never shown as an
  error.

## 5. Phasing (three PRs)

1. **Data (built, branch `claude/catalogue-text-data`):** normalizer, builder, gate, service, tests
   on a small fixture file (built in the test from synthetic inputs, never the real file), the
   DATA_LIFECYCLE row, the env var. The real file was built locally and measured (§3.1), and the
   FJMS translation sample graded (§3.2). Nothing is served.
2. **Web, API, MCP:** mode, the `%` shortcut in the engine and web maps, rows, cards, the
   zero-result line, Latin routing, header/home box, `search_mode='catalog'`, MCP tool, the startup drift
   warning (§4.6), PostHog, flag + readiness, deploy script, docs, a `changelog.d/` fragment. Every
   new public string is listed in the PR for the owner's approval.
3. **Desktop:** the mode-list helper, the mode, the `%` shortcut in `_SHORTCUT_PREFIXES`, rows,
   the zero-result line, telemetry, bundling.

## 6. Risks

- **Translation quality** (§2.3, §3): unmeasured, and up to ~40% of hits rest on it. The
  "automatic translation" label is the minimum. D2 decides whether to measure before shipping.
- **Desktop size:** +736 MB on disk and ~190 MB in the installer (today ~300 MB) if bundled (D5).
- **Noise from prefixes:** מ + חלב is also "from Aleppo". That ambiguity is inherent to Hebrew;
  ranking puts whole-word matches first, and D3 decides the default.
- **Staleness:** PGP and NLI records change, and a refresh without a rebuild leaves the file
  behind. §4.6 covers it: a rebuild step in every refresh, and a startup warning on drift.

## 7. How this round relates to the next two

- **Editions search (FGP, PGP, community corrections)** is the next round. Its hits will say "this
  manuscript *reads* X", where a catalogue hit says "its record *says* X", so the two stay
  separate. PGP splits cleanly between them: descriptions here, transcriptions there. The source
  label on the card (§4.3) is designed so an edition hit can reuse it ("FGP edition", "PGP
  edition", "community correction").
- **Desktop fetching data instead of building it** (SEED-005): this file is an ordinary sidecar,
  bundled like the others. If an update channel for sidecars is ever made live, this file is one
  more entry in its list, and nothing here waits for it.

## 8. Decisions (ruled by the owner, 2026-11-05: (a) on all seven)

The options are kept as they were put, so the ruling can be read against what was declined.
Added at the same time: the `%` shortcut (§4.4) and the refresh note (§4.6).

- **D1 — Where it appears.** **Ruled (a).** (a) Its own "Catalogue" mode plus the zero-result line (recommended);
  (b) only the zero-result line; (c) only the mode.
- **D2 — Automatic translations.** **Ruled (a).** (a) Search them, label every such hit "automatic translation",
  give readers a switch to leave them out, and first grade a sample of ~150 FJMS translations the
  way PGP's were graded: if they are as bad as PGP's (28% wrong), the switch starts off
  (recommended); (b) search them without measuring; (c) leave them out (כתובה would then find
  1,374 manuscripts instead of 2,212).
- **D3 — Hebrew prefixes.** **Ruled (a).** (a) Also find the word after ה ו ב ל מ ש כ (כתובה finds הכתובה,
  בכתובה…; +14% to ×2.4), with whole-word matches ranked first and quotation marks to turn it off
  (recommended); (b) exact words only.
- **D4 — Title mode and PGP Tags.** **Ruled (a).** (a) Keep both as they are; the catalogue mode covers library
  titles too, and the two can be retired later on usage numbers (recommended); (b) fold Title mode
  into the catalogue mode now (an API change: `search_mode='title'` is public).
- **D5 — Desktop.** **Ruled (a).** (a) Bundle the file in the installer (+~190 MB download, nothing to build:
  your stated ideal) (recommended); (b) the desktop builds it on first use from files it already
  has (~2 minutes, no bigger download); (c) web first, desktop in a later release.
- **D6 — Latin-script queries.** **Ruled (a).** (a) Go straight to the catalogue with a note and a link back to
  text search (100% of them find nothing in the transcriptions today) (recommended); (b) run the
  text search first and offer the catalogue on zero results, like Hebrew.
- **D7 — Catalog Browse's text filter.** **Ruled (a).** (a) Leave it on FJMS's own index this round (recommended);
  (b) switch it, and the Manuscript features "catalogue text" rule, to the new file so they also
  find PGP descriptions, work titles and translations (more PRs, touches v9.13.0's panel).

## 9. Changes by revision

- **rev. 1 (2026-11-05):** first written; the 11-03 shape recovered from its session, PGP Hebrew
  translations dropped (§2.3), prototype numbers (§3), code map (§4.4), decisions D1-D7.
- **rev. 2 (2026-11-05):** the owner ruled D1-D7 (a), as recommended; the `%` shortcut added
  (§4.4, all three prefix maps, Responsa's `%word` untouched); §4.6 on PGP and NLI records changing
  (a rebuild in every refresh, a startup drift warning; no date shown in results, owner); PR 2 and 3 scope
  updated.
- **rev. 3 (2026-11-05):** PR 1 built. §3.1 records the first real build: 2,456,035 records, 218,839
  manuscripts, 730 MB, the gate passing, the plural measurement, and 0.3-1.2 s for the commonest
  words, so PR 2 runs the search off the event loop. §3.2 records the D2 audit: 14.0% wrong or
  garbage, FullText weakest, switch ON. §4.2: English words made singular by our own rule instead
  of Porter. §4.1 updated to what was built. The Codex review of #483 found the work-title and author translations keyed by
  entity, not manuscript; they now fan out to their manuscripts, and placeholder titles and
  authors are left out. The audit's population was the translation table, not the file: 146 of its 150 rows are
  indexed, 13.7% bad among them (§3.2), and the sampler gained `--sidecar` to draw what the file
  holds.
