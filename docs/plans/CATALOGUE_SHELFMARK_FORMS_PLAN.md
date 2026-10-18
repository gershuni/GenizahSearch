# Catalogue-form shelfmarks: accept shelfmarks as the NLI catalogue writes them (step 1)

**Status:** rev. 5, **implemented** on branch `feat/catalogue-shelfmark-forms` (2026-10-18), owner
defaults for all four §9 questions; PR not yet opened. Codex round 4 (on rev. 4): SHIP WITH FIXES,
folded in. What the implementation measured and where it departs from the text is in §11.
**Date:** 2026-10-18.
**Origin:** a user asked whether genizahsearch can take a shelfmark "as it appears in the library
catalogue", with two NLI screenshots: `Cambridge University Library, Ms. T-S K 08.026` and
`Mosseri, Jacques, Ms. VIII.402` (also `Mosseri, Jacques Paris France Ms. VIII.402`).

## 1. Problem

Both forms fail on every surface today, desktop and web, in Browse, Search, the Fragment Puzzle,
the Joins Lab, exclusion lists, the public API and MCP.

- **Cambridge.** Our record `990051208260205171` stores `Cambridge University Library Ms. T-S K 8.26
  | Ms. T-S K 8.26 | T-S K8.26`. NLI now pads the numbers (`K 08.026`). No normalizer in the
  resolver path removes a leading zero that follows a letter (`cudl_normalize` strips zeros only
  after a dot or at the start of the string).
- **Mosseri.** Our records are the two parts, `990053809560205171` `Moss. VIII,402.1` and
  `990053809570205171` `Moss. VIII,402.2`. No record stores a `Mosseri, Jacques Ms. VIII 402` form.
  `_strip_library_prefix` strips only the word `mosseri` and leaves `Jacques, Ms. VIII.402`. The
  right answer is to offer both parts.
- The two NLI record ids in the screenshot (`990054482690205171`, `990054121910205171`) are in none
  of `libraries.csv`, `nli_crossref.db` or `pgp.db`. They are separate NLI description records.
  Pasting those ids keeps failing. That is out of scope.

## 2. What exists today (verified 2026-10-18; re-grep line numbers before editing)

- **One shared submit-time resolver:** `MetadataManager.resolve_system_by_shelfmark`
  (`shared/metadata_manager.py`, ~1919). It strips a library prefix (overwriting `query`),
  normalizes with `normalize_shelfmark`, and scans `_get_shelfmark_index()` linearly. The index
  holds every `call_numbers` variant plus the `nli_cache` `shelfmark`/`shelfmark_alt`, deduplicated
  by `(sys_id, norm)`. The scan does exact, substring and dotless matching. The resolver then
  promotes a `lookup_cudl` bridge hit to exact, deduplicates exact matches by `sys_id`, and returns
  one exact `sys_id`, else exact options, else substring options (each capped at `limit`). It
  serves desktop Browse, the reading desk, Joins Lab cold start and Share a Discovery, and, through
  `web/services.py::search_by_shelfmark`, web Browse and the Joins Lab. Web Puzzle calls it
  directly. It runs only on submit, never per keystroke, and on the desktop it runs on the UI
  thread.
- **The index is built lazily.** Nothing warms it. A fresh build with the CSV already loaded took
  **3.1 s and 156.5 MiB** private memory (Codex, round 1). `_load_csv_bank` resets
  `_shelfmark_index = None`; the getter also rebuilds once if it cached an empty index before the
  web's background CSV load finished.
- **Weaker side paths that do not use it:**
  - `search_by_meta` (~1733): desktop and web Search in Shelfmark mode, `POST /api/search`, the
    search jobs, the ChatGPT facade and MCP `search_manuscripts`. It matches only the shortest
    variant, does no prefix stripping, and falls back to `lookup_cudl` only on zero results. On the
    web it runs in the research workers (`shared/research_worker.py`, each with its own
    `MetadataManager`; `GENIZAH_RESEARCH_WORKERS`, default 1, at most 4), which never build the
    shelfmark index today.
  - The desktop `_shelf_to_sys` map (`genizah_app.py::_ensure_shelf_map`): shortest variant only,
    first entry wins. Used by desktop Puzzle add.
  - The desktop Exclude dialog (`desktop/dialogs_filter.py`) has **three** paths: the report button
    (`_resolve_and_show_report` -> `exclusion_service.resolve_shelfmarks`), the per-keystroke sync
    (`_resolve_sys_from_shelves`, its own dot-stripping normalizer), and the submission fallback in
    `get_exclusion_sources` when no report was run (`normalize_shelfmark` against the same map).
  - The desktop `JoinsDialog` Fragment B inline scan (`desktop/corrections_ui.py::create_new_join`):
    shortest variant, first match wins.
  - `shared/exclusion_service.py::build_shelf_map`/`resolve_shelfmarks`: all variants, first entry
    wins, statuses `found`/`not_found`/`duplicate` only.
- **Why today's long forms half-work:** `_get_library_prefix_aliases` has `adler` in its extras, so
  every `Adler, Elkan Nathan Ms. …` form (28,662 stored variants) is stripped to `Elkan Nathan
  Ms. …` and reached only by a substring match. That is a single option, which most surfaces
  auto-select.
- **Ambiguity on the web is unsafe today.** `browse.py::toolbar_add_by_shelfmark` (reading desk)
  and `joins_lab.py::resolve_anchor_input` take `results[0]`. `puzzle.py::on_add_shelfmark` ignores
  `options` and reports "Shelfmark not found". Desktop `CreateDiscoveryDialog._add_shelfmark_to_list`
  squashes options to `sys_id=None` and deduplicates on `(typed text, page)`. Only web Browse's
  main box and the desktop Browse, reading-desk and Joins Lab boxes show a picker.
- **Autocomplete:** only the desktop `ShelfmarkCompleter` (`desktop/widgets/__init__.py`), fed by
  `genizah_app.py::setup_shelfmark_completer` with one shortest shelfmark per record plus Oxford
  parts (whose keys come from `shared/codicological.py`'s own part normalization, e.g.
  `heb. d. 29 part 2` -> `hebd29part2`). It is shared by Browse, Puzzle, Share a Discovery and the
  Joins dialog. The web has no shelfmark autocomplete. **No test pins the completer today.**
- **Lists have no shelfmark box** on either app. Items arrive by `sys_id`, and JSON import requires
  `sys_id`. Nothing to change there.
- **Pins to respect:** `tests/test_shelfmark_bridge.py` hashes the source of `normalize_shelfmark`
  (any edit fails CI). `cudl_normalize` carries "do not modify without the Phase 86 suite".
  `build_alias_index` writes `reports/cudl_alias_collisions.csv` unless given `report_path`.
  `libraries.csv` is tracked in git, so real-data tests run in CI. They self-skip below 100,000
  rows. The existing bridge fixtures also skip on initialization errors, so their green alone does
  not prove they ran.

## 3. Design: one catalogue-form fallback (T2), only where today finds no exact match

**Invariant.** Today's resolver logic ("T1") is not edited. T2 runs only when T1 produced **no
exact match**, including the bridge. Every input that resolves exactly today, uniquely or as exact
options, returns the identical result. T2 may change only inputs that today give substring options
or nothing.

**Where T2 is inserted (exactly).** In `resolve_system_by_shelfmark`:

1. Save `original_query = query` before the prefix-stripping block.
2. Hoist the index: `index = self._get_shelfmark_index()`, then `for … in index:`. Same object,
   same iteration, so T1's behavior is identical, and T2 sees the very index T1 scanned.
3. Insert T2 **after both exact-return branches** (single exact; exact options) and **before**
   `partial_matches.sort(...)`. So unique exact answers, exact options (whatever `limit` is, `0`
   included), bridge promotion and its deduplication are all untouched.
4. If T2 returns hits, return them; otherwise fall through to today's substring options.

**Building blocks** (computed from the typed text):

- *Institution heads* are derived from the data, not hand-listed: the text before ` Ms.`/` MS` in
  `csv_bank` `call_numbers_raw` variants matching `^(head)[,\s]+(Ms\.?|MS\.?)\s+(rest)$`
  (lowercased, trailing comma dropped), kept if used at least 3 times, longest first. They are
  read from the raw variants **before** any normalized deduplication (deriving them from the
  deduplicated index drops `library of geneva.`). Measured: 58 heads, 0.5-0.7 s. A query head
  matches a table head when it equals it or continues with a space or comma. That is how
  `Mosseri, Jacques Paris France` matches `mosseri, jacques`.
- *Authoritative library codes, never inferred from frequency* (Codex round 3: three Sassoon
  variants stored under `CUL` would have admitted every Cambridge record). A head's codes come from
  `LIBRARY_CODES` only: the head equals a code's name (a leading `the` ignored on both sides) or
  starts with it followed by `,` or a space, or the head's first word equals a code
  (`sassoon, david solomon` -> `Sassoon`, `gaster, moses collection` -> `Gaster`). Measured: 44 of the
  58 heads map (`state library of berlin` -> `SBB`, although `SBB` uses that head only twice); the
  other 14 (e.g. `vernadsky national library of ukraine`) have none.
- *The named institution* of a query is its recognized head, or else the prefix
  `_strip_library_prefix` removed, when that prefix is exactly a `LIBRARY_CODES` code or name,
  compared case-insensitively and mapped to **every** code it names. Codes that differ only in case
  (`BnF` "Bibliothèque nationale de France", `BNF` "National Library of France", the one such pair)
  are one equivalence class, and every alias and head mapping is expanded through it, so the
  English name, the French name and either code all reach a record stored under either code. A query whose prefix is any other alias (`cambridge`, `adler`, …) gets **no
  T2 at all** and keeps today's answer.
- *A record belongs to the named institution* when it stores a variant with that head
  (record-level provenance), or its library code is one of the institution's authoritative codes.
  A head without codes therefore admits only records that store it.
- *Zero-insensitive key* `zkey(s) = normalize_shelfmark(zs(s))`, where `zs` strips leading zeros
  from every digit run **in the raw text**, keeping a lone `0`. Order matters: `normalize_shelfmark`
  deletes `=`, spaces and commas and so joins digit runs. Stripping after it turned
  `Heb. 577.10.06=04` into `…604` and matched `577.10.60=4` (Codex round 1, HIGH 1); stripping first
  gives `…6=4` -> `…64`, the right record. On the stored side, a side table maps `zkey(variant)` to
  entries for the **1,352** variants where `zkey` differs from the plain normalized key. No new full
  index is built.
- *Numeric check on every candidate:* the integers in the query must equal, in order, the integers
  in the stored variant that matched (parent candidates: a strict prefix of them). This blocks any
  match that only exists because normalization glued two numbers together (`EVR II A 0721` must not
  reach `EVR II A 72, 1`).

**Candidates.** T2 takes the **union** of these sets (rev. 2 took the first non-empty tier; Codex
round 2 showed that hid same-library records, e.g. `Library of the Hungarian Academy of Sciences,
Ms. 06` gave only the record storing that long form, not the HAS record stored as plain `6`):

- `D0` **today's exact answer for the unpadded input:** when `zs(q)` differs from `q`, T1's three
  exact conditions (normalized equality, dotless equality for dotless input, bridge) applied to
  `zs(q)`, restricted to records that belong to the named institution when one is named. `T1` is not refactored to provide this: `catalog_shelfmark` re-states the three conditions
  and a test pins them equal to T1's exact set on a corpus sample (§7).
- `Da` **rebuilt long form** (head recognized): `zkey("<head> Ms. <rest>")`. It drops the comma and
  any words between the institution and `Ms.`, and matches only records that store that form.
- `Db` **Mosseri short form** (heads starting `mosseri`): `zkey("Moss. <rest>")`.
- `Dr` **the rest within the institution** (institution named): `zkey(<rest>)` matches whose record
  belongs to the named institution. This is what brings in same-library records stored without the
  long form (the HAS record stored as plain `6`).
- `Dc` **the whole input** (no institution named): `zkey(_strip_library_prefix(q))` matches, any
  library. A named institution is never dropped (Codex round 1, HIGH 2: `Cambridge University
  Library, Ms. F 002` must not reach Freer `Ms. F 02`).
- `Dd` **Mosseri parent**, only if all of the above are empty: stored keys equal to the `Db` key
  followed by `.` and more, checked per entry by testing the prefixes up to each `.` against a set.

**Result.** Candidates are deduplicated by `sys_id` (shortest shelfmark, as T1). A direct record beats
its parts (`Mosseri, Jacques Ms. IV 366` opens record IV 366; parts are offered only when nothing
direct exists). **Uniqueness is decided before truncation:** one record gives `sys_id`; several give
options sorted as today and capped at `limit`, with `total` carrying the full count.

**What is measured (§4), stated exactly:** over the adversarial sets, T2 returns a single record
different from the input's source record only where that record is today's exact answer for the
unpadded input (102 Oxford cases); and T2's answer always contains every record in **today's exact
candidates for the unpadded input, after the numeric check and restricted to the named
institution**. That is narrower than "today's whole answer": today's substring options are not
compared (`Ms. F 002` correctly gives Freer `F 02`, which today's 2,395 substring options for
`Ms. F 2` do not contain), and the numeric check deliberately drops glued matches (`EVR II A 721`
today also offers `72, 1`).

**Execution:** T2 is a **second linear pass** over the same index, run only on a T1 miss. It
compares each entry's normalized key with the few query keys, the side table and the parent
prefixes; it allocates nothing per entry.

**Lifecycle.** The T2 tables (heads with codes, side table) live in one immutable object that keeps
a **reference to the index list it was built from**. The resolver passes the index it scanned; if
`tables.index is not index`, the tables are rebuilt and published with one attribute assignment.
An integer `id()` is **not** used (Python reuses ids of freed lists; Codex reproduced it). Every way
the index is replaced (`_load_csv_bank` resetting it, the empty-index rebuild, a refresh) yields a
new list object, so stale tables cannot be used and the loader needs no edit. Two threads building at
once both produce the same tables; the later assignment wins and nothing is shared half-built. T2
adds **no** new build moments: the index is built exactly when it is today (on the first resolve).

**New methods on `MetadataManager`:**

- `resolve_catalog_form(query, limit=100) -> dict`: T2 only, never substring options. Returns
  `{'sys_id', 'options', 'selected_shelfmark', 'total', 'tier'}` so a caller can tell "one record"
  from "one shown of several".
- `resolve_catalog_forms(queries, limit=100) -> dict[str, dict]`: the same responses for many inputs
  in **one** pass (all query keys in one dict, one iteration over the index). Exclusion lists use it
  (taking only single-record answers); gate 4 uses it.

**Code location:** a new pure module, `shared/catalog_shelfmark.py`, holds head derivation,
splitting, `zs`, `zkey`, the numeric check and the tables object, with no Qt/NiceGUI imports.
Register it in `EXTRACTED_MODULES` (`tests/test_no_back_edges_core.py`). `metadata_manager.py`
calls it.

## 4. Measured on the real corpus (scratch prototypes, read-only, 2026-10-18)

The 563,957 indexed variants are those of 255,723 records. The prototypes
(`proto_v42_lib.py`/`proto_v42_measure.py` in the session scratchpad) run the **live**
`resolve_system_by_shelfmark` source as T1 (executed from the file, without loading the app) and
add T2 as specified above. They are not versioned; the PR's gate scripts (§7) become the one
versioned measurement and replace this table with their own output.

| Check | Today | With T2 (rev. 4 rules) |
|---|---|---|
| Golden: Cambridge, 3 forms (comma, no comma, `T-S K 08.026`) | not found | `990051208260205171` |
| Golden: Mosseri VIII.402, 4 forms (comma, no comma, `Mosseri Jacques,`, `Paris France`) | not found | 2 options, `.1` and `.2` |
| Golden: `GASTER 1201.17`, `Halper 381`, `T-S AS 31.1`/`3.11`, `T-S 12.123`, `T-S 12.0123`, `Or. 10578R.98`, `F 01`, `T-S K 8.026`, `Cambridge University Library, Ms. T-S K 8.26` | as today | **unchanged** |
| Codex counterexamples: `…Ms. Heb. 577.10.06=04`, `…0577.010.01=04` | not found | the right records (`…6=4`, `…1=4`), not `…60=4`/`…10=4` |
| Codex counterexamples: `Cambridge University Library, Ms. F 002`, `The British Library, Ms. F 002`, `British Library, Ms. F 002`, `CUL F 002` | not found | not found (no Freer record) |
| Institution-free padded: `Ms. F 002`, `F 002` | not found | Freer `Ms. F 02` (the only F 2) |
| Codex round 2: `Library of the Hungarian Academy of Sciences, Ms. 06` / `…, Ms. 037` | 100 / 84 substring options | 2 options (`6` and `990001261670205171`, which stores the long form) / 3 options including `990000951170205171` (`37`) |
| Codex round 2: `The National Library of Russia, Ms. EVR II A 0721` | not found | `990001442050205171` alone (not `72, 1`) |
| Library-code prefixes: `CUL T-S K 08.026`; `Halper 077`, `GASTER 0967.03` | not found; substring; not found | `990051208260205171`; unchanged; unchanged (no Halper/Gaster record matches) |
| Codex round 3: `Sassoon, David Solomon, Ms. T-S K 08.026`, `Vernadsky National Library of Ukraine, Ms. T-S K 08.026` | not found | not found (rev. 3 rules: Cambridge `990051208260205171`) |
| Codex rounds 3-4: `State Library of Berlin, Ms. or. qu. 0943`; `BnF Heb 01489.08` (also `BNF`, the French and the English full names) | not found | the 2 Berlin records; `990035991030205171` (rev. 3 rules: nothing; nothing) |
| **Exhaustive adversarial** (candidates, uncapped), every stored head variant rewritten as `<head>, Ms. <rest>` (40,905 not exact today) | — | 37,497 correct, 3,407 options containing the target, 1 not found, **0 wrong, 0 without the target** |
| Same, Mosseri without the comma (5,707) | — | 3,644 correct, 2,063 options containing the target (other Mosseri records storing the same number), **0 wrong** |
| Same, every digit run of the rest zero-padded (247,855) | — | 242,762 correct, 5,092 options containing the target, 1 not found, **0 wrong, 0 without target** (rev. 1 rules: 12 wrong) |
| Same, every bare variant (no head) zero-padded (467,594) | — | 449,580 correct, 11,742 options containing the target, 6,170 not found, **0 without target**; 102 single answers differ from the source record, **each identical to today's answer for the unpadded input** (Oxford `Arch. O.d. 8.N`, where a stub stores the short form) |
| All four sets: answers missing a record from today's exact candidates for the unpadded input (numeric-checked, within the named institution) | — | **0** |
| All four sets: returned list (cap 100) missing the target | — | 46 query-target cases, all with more than 100 candidates (`Gaster … Ms. 1358`, 110 identical; `Or. 10578R`, 108), truncated the same way today |
| Simulated `"<Institution>, Ms. <X>"`, 2,910 records across libraries | 2,341 correct (80.4%), 568 options containing the target, 1 without | **2,836 correct (97.5%)**, 73 options containing the target, 1 without (the Gaster cap case below), **0 wrong** |
| CUL zero-padded (`K 08.026` style), 842 | 659 correct, 183 not found | **841 correct (99.9%)**, 1 option list containing the target, 0 wrong |
| Mosseri `… Paris France Ms. …`, 42 | 0 (42 not found) | 27 correct, 15 options containing the target, 0 wrong |
| 4,000 random stored variants as input | n/a | 278 differ: 256 go from one option to the same record directly; 18 are shorter lists; 3 are longer lists (a same-library Mosseri record stored as the bare number joins, e.g. `Mosseri, Jacques Ms. Ch 2` now also offers the record stored `Ch 2`); 1 replaces substring options with exact ones. **All 278 keep the input's own record; 0 point to another record** |
| Zero-stripping collisions | n/a | `F 001` gives both Freer `Ms. F 01` and RNL `F 1` as options, never one silently |

The one simulation list without its target is the same `Gaster, Moses Collection, Ms. 1358` cap case.

Rules rejected on evidence: "original text exact first" (moved 2,095 Oxford `Arch. O.d. 8.N`
lookups and `GASTER 1201.17` to bare stubs, turned `Halper 381` into options); `lookup_cudl` on the
unstripped text (turned single hits into options); library-code scoping (heads span several codes:
Gaster BL/Manchester, Sassoon in several); zero stripping after normalization (HIGH 1); tier c after a
named institution (HIGH 2); a single-pass T1+T2 loop (2.3x slower on hits); first-tier-wins
(hid same-library records, round 2); a desktop warm-up thread (hid later NLI-cache records, round 2).

## 5. Surface changes

| # | Surface | Change | Effect on inputs that work today |
|---|---|---|---|
| A | `resolve_system_by_shelfmark` | T2 as §3, at the stated insertion point | exact answers identical; substring-only or empty inputs may now resolve |
| B | `search_by_meta`, field `shelfmark` (desktop/web Search, `/api/search`, jobs, ChatGPT, MCP) | after today's `lookup_cudl` fallback, if still empty **and** the query looks like a catalogue form (a `Ms.`/`MS` token after a comma or space, or a zero-padded number), add `resolve_catalog_form(query, limit=500)` ids. **Never** substring options. Return contract (list of ids) unchanged | none; only zero-result catalogue-shaped searches change. The gate keeps ordinary empty searches from building the index in a research worker |
| C | Desktop Puzzle `_on_add_shelfmark` | keep the `_shelf_to_sys` exact lookup first; on a miss call the resolver; one `sys_id` adds it; options open the picker (G); **cancel adds nothing**; not found keeps today's message | none |
| D | Desktop `JoinsDialog` Fragment B (`create_new_join`) | keep the inline scan first; on a miss call the resolver plus the picker; the **picked record's** `sys_id` and shelfmark go to `create_join`; **cancel returns before `create_join`** | none |
| E | Desktop `CreateDiscoveryDialog._add_shelfmark_to_list` | exact `sys_id` as today; options open the picker; the entry stores the **picked** record's shelfmark and `sys_id`; duplicates are keyed on `(sys_id, page)` when `sys_id` is known, else `(text, page)` as today; **cancel adds nothing** and keeps the typed text | exact hits unchanged; ambiguous input asks instead of adding an unresolved entry |
| F | Exclusion lists | `exclusion_service.resolve_shelfmarks` gets an optional `fallback` (a batch callable): its `not_found` lines go through `resolve_catalog_forms` in one call; exactly one `sys_id` means `found`; several stay `not_found` (Q2). Desktop: the report button **and** the submission fallback in `get_exclusion_sources` both use it (one batch over the lines the map missed); file import goes through the report path already; the per-keystroke `_resolve_sys_from_shelves` is **not** changed. Web: every caller of `resolve_shelfmarks` (paste and file) passes `state.meta_mgr.resolve_catalog_forms` | none for lines found today |
| G | New `desktop/shelfmark_picker.py::pick_shelfmark_option(parent, options) -> dict or None` | extracted from the `browse_load` `QInputDialog.getItem` block, keeping its numbered labels (they distinguish identical rows); `None` means cancelled, distinct from "not found". Used by C, D, E. The four existing picker sites are not refactored in this step. Register in `DESKTOP_MODULES` | none |
| H | Web reading desk | `toolbar_add_by_shelfmark` becomes `async` and calls `io_bound_result.io_bound`; `INTERRUPTED` returns silently; one result adds; several open the picker (K). Its caller `rd_toolbar_add` becomes `async` and **awaits** it (both Enter and the Add button use it); the input is cleared only after an add | ambiguous input asks instead of silently adding the first |
| I | Web Joins Lab | new `resolve_anchor_candidates(query)` returns `INTERRUPTED` or the candidate list (the sys-id fast path returns one); `_on_load_btn_click` uses it: one loads, several open the picker, none shows today's message, `INTERRUPTED` returns silently. `resolve_anchor_input` stays as a thin wrapper returning the first candidate, so the `url_shelfmark` deep link keeps today's behavior (Q3) | ambiguous typed input asks |
| J | Web Puzzle `on_add_shelfmark` | `io_bound_result.io_bound` instead of `run.io_bound`; `INTERRUPTED` returns silently; one `sys_id` adds; options (dicts) open the picker; none keeps "Shelfmark not found" | ambiguous input asks instead of "not found" |
| K | New `web/components/shelfmark_picker.py::open_shelfmark_picker(container, items, on_pick, query)` | the card list from `browse.py::show_shelfmark_suggestions` (library badge, LTR shelfmark, RTL title). `container` is entered explicitly (the dialog may open after an `await`); `items` are plain dicts `{sys_id, shelfmark, title, library_code}` (callers convert `ManuscriptInfo`); `on_pick` may be async and is awaited; closing the dialog does nothing. Browse's own dialog is not changed in this step | none |

**Not every box that accepts shelfmark text changes.** Out of scope, by design: web Share a
Discovery's extra-shelfmark field (it stores text and resolves nothing today), and the Search and
candidate-grid result filters (substring filters over rows already shown).

**Desktop threads.** No warm-up thread (rev. 2 had one; Codex round 2 showed that building the index
earlier hides NLI-cache records added after the warm-up, which a lookup today would find). The new
miss-path calls in C and D run the resolver in a small worker thread, `ShelfmarkResolveThread` in
`desktop/gui_threads.py` (retired with the existing `_keep_until_finished` pattern); the add
control is disabled while it runs, and the picker opens in its finished slot. So a cold first build
(3 s) never blocks the UI from these dialogs. The Puzzle captures `_canvas_gen` **before** starting
the resolver worker (as `_start_meta_resolve` does for its own worker) and checks it before showing a
picker, adding, or reporting "not found", so New/Open during a lookup cannot send the old request to
the new canvas. The Joins dialog ignores a result that arrives after it closed. E already resolves on the UI thread today and stays
there. The index is built exactly when it is today, on the first resolve, whichever thread runs it.

Autocomplete stays byte-identical. Selecting a completer row gives the shortest shelfmark, which
T1 resolves exactly, so T2 never runs on it.

## 6. Not changed

`desktop/widgets/__init__.py` (`ShelfmarkCompleter`), `genizah_app.py::setup_shelfmark_completer`,
`normalize_shelfmark` (SHA-pinned), `cudl_normalize`, `build_alias_index`/`lookup_cudl`,
`_strip_library_prefix` and its alias list (the `adler` alias stays; T2 covers its long forms), the
T1 matching logic, `browse.py::search_shelfmark`/`show_shelfmark_suggestions`, the four existing
desktop picker blocks, the desktop Exclude per-keystroke sync, and
`join_workbench.py::build_known_join_rows` (non-interactive; it keeps squashing options).

## 7. Tests and gates (each must be shown able to fail)

1. **Completer pin, written and green BEFORE any other change:**
   `tests/test_shelfmark_completer_model.py`. Call the real `setup_shelfmark_completer` on a
   `QWidget` carrying a stub `meta_mgr` (a plain object cannot parent a `QCompleter`) with a fixed
   `csv_bank` (a duplicate shelfmark, an Oxford `MS. Heb. a.1` row) and one Neubauer part. Assert
   the **literal** ordered rows `(display text, UserRole key, part id)`, e.g.
   `('heb. d. 29 part 2', 'hebd29part2', 'MS. Heb. d. 29/2')`, the exact `valid_shelf_keys` set,
   and that activating a row puts its display text in the line edit. Literal values, not
   `normalize(text)`: the part key comes from a different normalizer.
2. **Baseline gate.** `scripts/build_shelfmark_resolve_baseline.py` runs on `master-main` BEFORE
   the change and writes `tests/fixtures/shelfmark_resolve_baseline.json` with a header (source
   commit, `libraries.csv` sha256, seeds, `limit`) and, per input: the input, the source record it
   was generated from, and the full ordered response (`sys_id`, option ids in order,
   `selected_shelfmark`) plus whether T1 answered exactly. Inputs: 1,000 stored variants and 200
   simulated NLI forms (fixed seeds), plus `limit` 0 and 1 cases with exact options.
   `tests/test_shelfmark_resolve_baseline.py` (real data, `slow`) asserts:
   - every baseline exact answer (unique or options) is reproduced identically, order included;
   - every other entry is identical, or now resolves to its source record, or is a list that still
     contains its source record. A unique answer that is not the source record fails.
   It fails, not skips, when `libraries.csv` has 100,000+ rows and anything raises. The fixture's
   corpus hash must match, so a data refresh regenerates it on purpose.
   Mutation: editing T1's prefix stripping turns it red.
3. **Golden, real data:** `tests/test_catalog_shelfmark_real.py`, every golden and counterexample
   row of §4 with its **exact full response** (ids in order, `total`). Mutations, each shown to fail:
   stripping zeros after normalization fails `577.10.06=04`; letting `Dc` run after a named
   institution fails `Cambridge University Library, Ms. F 002`; dropping the numeric check fails
   `The National Library of Russia, Ms. EVR II A 0721` (with it: `990001442050205171` alone;
   without it `990000628420205171`, stored `72, 1`, joins); dropping `D0`/`Dr` (first-tier-wins)
   fails `Library of the Hungarian Academy of Sciences, Ms. 06` (must be the 2 records `6` and
   `990001261670205171`) and `…, Ms. 037` (must include `990000951170205171`, stored `37`);
   inferring library codes from frequency fails `Sassoon, David Solomon, Ms. T-S K 08.026` and
   `Vernadsky National Library of Ukraine, Ms. T-S K 08.026` (must find nothing; frequency codes
   reach Cambridge `990051208260205171`); a frequency threshold on codes fails `State Library of
   Berlin, Ms. or. qu. 0943` (must give `990001756390205171` and `990001756460205171`); a
   single-code alias map fails `BnF Heb 01489.08`, and a map without the case-equivalence class
   fails `National Library of France Heb 01489.08` and `The National Library of France, Ms. Heb
   01489.08` (all three must give `990035991030205171`);
   dropping direct-before-parent fails IV 366; **running T2 before the T1 exact check fails `F 01`**
   (T1 gives Freer `Ms. F 01`; T2 would give two options).
4. **Adversarial gate, real data, `slow`:** the four exhaustive sets of §4 (about 760,000 inputs).
   One linear pass per input would take hours, so the gate runs them through the batch method
   `resolve_catalog_forms` in chunks (one index pass per chunk), and a second test asserts that
   `resolve_catalog_form` and `resolve_catalog_forms` give identical full responses for 2,000 of
   those inputs, so the batch path cannot drift from the single path. The PR reports the gate's
   wall time (the prototype's 90 s used dict lookups, not the shipped scan). Two separate assertions:
   - **candidates (uncapped):** 0 new wrong single records (a single record that differs from the
     source is allowed only if it equals today's exact answer for the unpadded input, the Oxford
     `Arch. O.d. 8.N` pairs), 0 lists without the target, 0 answers missing a record of today's
     exact candidates for the unpadded input (numeric-checked, within the named institution); correct counts at or above §4 minus 0.5%;
   - **returned options (capped at `limit`):** a target may be missing only when `total > limit`,
     and then the list must be the first `limit` in today's sort order. Measured: 46 such
     query-target cases, all in two families of identical shelfmarks (`Gaster … Ms. 1358`, 110
     records; `Or. 10578R`, 108), which today's resolver truncates the same way.
   Plus the simulation: at least 97.0% correct for comma forms and 97.5% for CUL zero-padded forms.
   Mutation: removing `Da` fails the **target-retention** assertion (in each affected set, three
   query-target cases lose their target from a non-empty list); the correct counts alone would stay
   inside the 0.5% tolerance (Codex measured 16 + 16 + 1 lost), so the gate does not rely on them.
5. **Unit, synthetic:** `tests/test_catalog_shelfmark.py`: head derivation (including a raw
   duplicate that normalizes away, and synthetic heads that are prefixes of each other), head codes,
   alias-to-code mapping (code, full name, unmapped alias -> no T2), splitting, `zs` (lone `0`,
   `00`, letters around zeros, Roman `O` vs `0`), `zkey` order, the numeric check, union and
   parent-only-if-empty, uniqueness before truncation (`limit=1` with two hits gives no `sys_id` and
   `total=2`), and `limit=0`. **The raw exact-candidate helper behind `D0` (before the numeric check
   and the institution filter) equals T1's exact set** for 1,000 corpus inputs, so the re-stated
   conditions cannot drift from T1 unnoticed. Spies: T2 does not run when T1 has an exact match;
   `resolve_catalog_forms` iterates the index **once** for N inputs (an iteration counter equal to
   the index length, not N times it).
6. **Lifecycle:** a cache-only (`nli_cache`) exact and `shelfmark_alt` match stays exact; a bridge
   hit that disagrees with T2 keeps the bridge answer; duplicate variants of one record dedupe;
   an empty index (CSV not loaded) followed by the load rebuilds the T2 tables; resetting
   `_shelfmark_index` rebuilds them; the tables hold a reference to the very list they were built
   from (`tables.index is index`), and replacing it with a **distinct list of equal content** forces a
   rebuild (this catches both `id()` tagging and `==` comparison); two threads calling the
   resolver on a cold manager both get the same answer; no code path builds the index earlier than
   today (a spy on `_get_shelfmark_index` while opening the Puzzle, the Joins dialog and Share a
   Discovery).
7. **`search_by_meta`:** the fallback fires only on an empty result for a catalogue-shaped query,
   never returns substring options, and never builds the index for an ordinary empty query (spy).
   Exercise the real method, not the hand copy in `test_shelfmark_normalization.py`.
8. **Exclusions:** `tests/test_exclusion.py`: a unique catalogue form becomes `found`; an
   ambiguous one stays `not_found`; one batch call. Desktop (`gui`): the real Exclude dialog,
   both the report path and the no-report submission path, asserting the accepted `sys_ids`,
   the duplicate statuses and the unresolved lines.
9. **Desktop Qt (`gui`, native locally, offscreen in CI):** Puzzle (the
   `test_puzzle_canvas_integrity.py` add-shelfmark harness), Fragment B and Share a Discovery,
   with `QInputDialog.getItem` monkeypatched and the C/D worker awaited (`qtbot.waitSignal` on its
   finished signal): two options call the picker with both numbered labels; choosing the second adds the second; cancel adds nothing and (Fragment B) does not call
   `create_join`; in Share a Discovery, the same parent entered twice with different parts picked
   gives two entries; in the Puzzle, a lookup still running when New (or opening a join) replaces the
   canvas adds nothing to the new canvas and shows no picker or message.
10. **Web (NiceGUI `User`, the `test_web_io_bound_none_findings.py` harness):** through the real
    events (Enter and the button): reading desk, Joins Lab load button and Puzzle show the picker
    for two results; choosing one adds that one; closing the picker adds nothing; an interrupted
    lookup shows no "not found"; the Joins Lab `url_shelfmark` deep link still loads the first
    result.
11. **Existing suites stay green:** `test_shelfmark_bridge.py` (with the SHA pin),
    `test_exclusion*.py`, `test_puzzle_canvas_integrity.py`, `test_web_io_bound_none_findings.py`,
    `test_no_back_edges_core.py`/`_desktop.py`. Plus `ruff check .` and
    `python scripts/check_docs.py`. Every new real-data fixture passes a temporary `report_path`
    to `build_alias_index`.
12. **Performance, measured and reported in the PR (not a CI test):** cold and warm separately,
    on the real resolver: median/p95 for 200 hits and 200 misses before and after (warm hits within
    ±5%; warm misses at most 2.5x); cold first call; private memory of a research worker before and
    after its first catalogue-form search (budget: +200 MiB; over budget, B is limited to the
    desktop and the PR says so); 2 and 4 concurrent web lookups; a 200-line exclusion batch.

## 8. Delivery

One PR, branched from `master-main` (not `feat/composition-conditions`). Order: gate 1, then the
gate 2 baseline (on unchanged code), then §3, then the surfaces, then the remaining tests. Docs:
`docs/CODE_INDEX.md` (new modules and methods) and a `docs/OPEN_ISSUES.md` entry. At release:
`python scripts/bump_version.py`, a `CHANGELOG.md` section, the README "What's New" line and the
CLAUDE.md "Recently Changed" line. Both apps change, so web deploy plus desktop installer. The web
needs no data deploy.

## 9. Owner questions

1. **Mosseri unit vs parts.** When a record exists for the whole unit (`Mosseri, Jacques Ms. IV
   366`) and parts exist too (`.1`-`.3`): open the unit (default), or offer the unit and its parts?
2. **Exclusion lists.** A catalogue form matching several fragments (the Mosseri parent): leave it
   unresolved (default), or exclude all of them?
3. **Joins Lab deep link** (`/joins-lab?shelfmark=` that matches several): keep today's
   first-result behavior (default), or show the not-found message?
4. **Desktop Puzzle and Joins dialog misses:** offer Browse-style substring options too (default,
   same as Browse), or catalogue forms only?

## 10. Later (step 2, not in this plan)

A library-scoped canonical key index replacing the linear scan (Codex estimate: about 300K keys,
35-60 MB per process). NLI `nli_images` (about 54K literal alias pairs) and PGP aliases, with
provenance. A suggestion line when the completer has no match. Unifying the other normalizers
(the desktop Exclude dialog's dot-stripping normalizer, the `JoinsDialog`/web joins inline
normalizers). The dead `state.meta_mgr._shelf_to_sys` lookup in `web/pages/puzzle.py`
(`MetadataManager` has no such attribute). Web Share a Discovery resolving its extra shelfmarks.
Two T1 behaviours found while testing, left alone by the invariant: the index is never rebuilt
when NLI-cache shelfmarks arrive after its first build, so those records are invisible to lookups
until restart (T2 neither fixes nor worsens this); and a conflation: `…Ms. Heb. 577.10.6=04` resolves
today to `577.10.60=4`, because `normalize_shelfmark` glues `6=04` into `604`.

## 11. Implementation notes (2026-10-18)

**Where it lives.** `shared/catalog_shelfmark.py` (rules, tables, `resolve_many`, `t1_exact_entries`),
`MetadataManager.resolve_system_by_shelfmark` (T2 after both exact branches), `resolve_catalog_form(s)`,
`_get_catalog_tables`; `search_by_meta` (B); `exclusion_service.resolve_shelfmarks(fallback=)` +
`catalog_fallback` (F); `desktop/shelfmark_picker.py` (G), `ShelfmarkResolveThread` (C, D), Share a
Discovery (E); `web/components/shelfmark_picker.py` (K), `web/pages/browse.py::add_to_reading_desk_by_shelfmark`
(H), Joins Lab `resolve_anchor_candidates` (I), web Puzzle (J). Owner defaults: Mosseri unit opens
directly (Q1), ambiguous exclusion lines stay unresolved (Q2), the Joins Lab deep link loads the first
match (Q3), desktop Puzzle/Joins misses also offer substring options (Q4).

**Departures from the text above.**

- `resolve_catalog_form(s)` answers **exact-or-catalogue**: T1's exact conditions on the input first,
  then T2, never substring options. That is exactly what `resolve_system_by_shelfmark` returns minus
  its substring fallback, so the search fallback and exclusion lists agree with Browse.
- The reading desk's add logic is a module-level helper, so a test can drive it (the reading desk
  exists only inside a loaded Browse session); the toolbar's Enter/Add chain is pinned by an AST test.
- Zero-stripping order turned out to be a **recall** guard, not the correctness guard: with `D0` and
  the numeric check, stripping after normalizing gives no wrong answer, it loses the inputs only the
  long form can reach (words between the library and `Ms.`). The gate-3 mutation case is therefore
  `The National Library of Israel Jerusalem Ms. Heb. 577.10.06=04`; the long-form mutation case is
  `Cambridge University Library, Ms. T-S 12.661` (no short variant to match).

**Codex implementation review, round 1 (DO NOT SHIP, 4 findings, all fixed).** The Joins dialog's
worker was handed to the keeper before `start()`, which releases a thread that is not running, so
the running thread could be destroyed (0xC0000409): it now starts first and the dialog holds it until
its answer (a real-thread test). Share a Discovery deduplicated on text even for different records
(110 Gaster records read `Ms. 1358`): now `(sys_id, page)` when resolved. The Exclude dialog's first
catalogue lookup built the index on the UI thread (6 s once): a `CatalogFormsThread` now prefetches the
answers, Resolve reports when it answers, Apply waits for it, Cancel applies nothing. Cancelling the
reading-desk picker cleared the box: it now clears only after an add.

**Gates, as run.** Completer pin green on unchanged code first; baseline written on master-main
`ed05058f` (1,219 inputs, 1,092 exact) and held by the changed resolver in full (220 s); the full
adversarial gate over every stored variant (about 760,000 inputs) passes in 192 s with 0 new wrong
records, 0 lists without the target and 0 answers narrower than today's unpadded exact answer; 17 named
mutations each turn their gate red. CI's main job runs subsamples of the two slow gates; the `slow` job
runs them in full.

**Measured (gate 12, this machine).** Cold first lookup 5.4 s (index and tables). Warm hits: median
120 ms before and after (T2 never runs). Warm misses: median 112 -> 141 ms (1.25x; budget 2.5x).
2 / 4 concurrent misses: 248 / 485 ms wall. A 200-line exclusion batch: 169 ms. A research worker's
first catalogue-form search: 7.7 s once, +98 MiB (budget +200); an ordinary empty search builds nothing.

## Revision log

- **rev. 5 (2026-10-18), after Codex round 4 (SHIP WITH FIXES).** M1: `BnF`/`BNF` form one
  equivalence class across aliases and head mappings (the English name now reaches the `BnF`
  record; verified on the data, Sassoon/Vernadsky negatives unchanged). L2: the preservation claim
  restated as exactly what is measured, and gate 5 compares the raw exact-candidate helper with T1.
  L3: gate 4's mutation names the target-retention assertion that fails.
- **rev. 4 (2026-10-18), after Codex round 3 (DO NOT SHIP).** HIGH 1: library codes come only from
  `LIBRARY_CODES` (name or first-word match), never from frequency, and a record belongs to the
  named institution only if it stores the head or carries an authoritative code (Sassoon/Vernadsky +
  Cambridge shelfmark now find nothing). M2: authoritative codes regardless of frequency (Berlin ->
  `SBB`), case-colliding codes kept as a set (`BnF`/`BNF`). M3: the Puzzle carries `_canvas_gen`
  across the new resolver worker. L4: the identity test replaces the list with a distinct equal one.
  Also: gate 4 runs through the batch method (single-input scans would take hours) with a
  single-vs-batch equality test.
- **rev. 3 (2026-10-18), after Codex round 2 (DO NOT SHIP).** HIGH 1: candidates are a union, with
  today's exact answer for the unpadded input (`D0`) and the rest within the institution's library
  codes (`Dr`), so a padded catalogue form never answers narrower than its unpadded form within that
  institution (measured: 0 cases); library-code prefixes map to their code, other aliases get no T2.
  M2: warm-up thread dropped; C/D's miss path runs in a worker thread; no new build moments. M3:
  uncapped candidate correctness and capped returned lists are separate claims and separate gates
  (46 truncation cases, all in two identical-shelfmark families). M4: tables keep a reference to
  their index and compare with `is`, never `id()`. L5: `EVR II A 0721` is the numeric-check mutation.
- **rev. 2 (2026-10-18), after Codex round 1 (DO NOT SHIP).** HIGH 1: zeros stripped on raw text
  before normalization, plus a numeric check on every T2 hit. HIGH 2: tier c only when the query
  names no institution. M3: exact insertion point, original query kept, index hoisted, tables tied
  to the index identity, uniqueness before truncation, heads from raw variants. M4: desktop
  exclusion submission path covered. M5: web event chain, picker item type, explicit container,
  `INTERRUPTED`, both Joins Lab callers. M6: desktop identity, dedupe and cancel. M7: completer pin
  with a `QWidget` and literal rows. M8: one versioned measurement, baseline fields, `F 01`
  mutation, exhaustive gate, fail-not-skip. M9: cold/warm, worker memory, B gated on catalogue
  shape, desktop warm thread. L10: out-of-scope inputs named, release steps, temporary report path.
- **rev. 1 (2026-10-18).** First plan.
