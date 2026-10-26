# FGP chooser validation rules (gate 4)

Gate 4 of [`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) §8.4. Script
`scripts/fgp_gate4_chooser_validation.py`, wrapper `scripts/run_fgp_gate4_chooser_validation.ps1` (two runs, one
digest), tests `tests/test_fgp_gate4_chooser_validation.py`, expectations
`scripts/fgp_gate4_chooser_validation.expect.json`, approvals `scripts/fgp_gate4_approvals.json` (absent = none).
Inputs: today's `fgp_data/fgp_transcriptions.db` (read only), `gate2.db`, `gate3.db`, `inventory.db`, `FIST.db`,
`nli_data/nli_crossref.db`, `libraries.csv`, and the HTR (V0.8) pages of the reference manuscripts from the search
engine the apps use. Outputs under `fgp_data/inventory/`: the candidate sidecar `fgp_transcriptions.candidate.db`
(never written over today's), `gate4.db`, `gate4_summary.json`, `GATE4_REPORT.md`.

The gate fails (exit 1) when any check or declared expectation fails. Every limit below was declared before the
first run; an undeclared limit is itself a fail (§8.4).

## 1. The candidate sidecar

Today's schema plus `origin` (`today` / `new`), `text_provenance` (`today` / `gate3:<reader>`), `fist_unit_table`,
`fist_unit_id`, `durable_key`. `shared/fgp_service.py` discovers the table and reads every column defensively, so
both apps open the candidate unchanged.

| rule | what it says |
|---|---|
| R1 identity | every today row keeps its `id` (the public `fgp:<id>`); new rows take ids above today's maximum, in row-key order |
| R2 carried | a today row is carried iff gate 2 did not retire it, it is gate-2 publishable (or its only gate-2 hold is `xml_unverified` and gate 3 verified its XML text by the row's own record), it has a text, and the text is not a test or dummy file. Excluded with reason `retired_alias` / `gate2_hold:<reasons>` (since 2026-10-26 also gate 2's `fist_comment_unchecked`) / `xml_unverified` / `empty_text` (today's 13 PDF rows that yielded no text; 11 have a DOCX text gate 3 holds as `extra_line` until the owner approves it) / `test_or_dummy` |
| R2a test files | owner's ruling of 2026-10-26: a text of at most 40 characters is a test or dummy file when it is a transcription (`Digital Edition`) without a Hebrew or Arabic letter (`test`, `ggg`, `fgfghfg`, `241241`, `....`), or consists of test words and digits only (`בדיקה`, `נסיון`, `test`), or carries `טסט` or `נא להתעלם` ("please ignore") anywhere. Translations are judged by the word rules only (an English translation is never Latin-only junk by itself). Every row it holds is listed in `row_dispositions` |
| R3 text | gate 3's text where gate 3 replaced it (status accepted / approved), else today's; `sections` (the PDF page split) is cleared on a replaced text (neither app reads it for FGP rows) |
| R4 credit | gate 2's `credit_he` / `credit_en`; the generic Friedberg attribution where gate 2 ruled generic |
| R2b legacy fonts | owner's ruling of 2026-10-26: a DOCX typed in a pre-Unicode Hebrew font arrives as vowel points standing where letters should be (RNL Arab.-Yevr. 72: «ַָָ ם ַהו זַֽֿ בַ ַֻהם בו»). In Unicode a combining mark follows a letter; a mark that opens a word (follows a space, digit or punctuation and is itself followed by a letter or a mark) is such a font's letter. A gate-3 text is held (`legacy_font`) when it has at least 10 such orphan marks and at least a tenth as many as Hebrew and Arabic letters. A vocalised piyyut has none; a mark standing alone between spaces is a transcriber's placeholder for an illegible letter and is not counted; a Latin letter with a decomposed diacritic is a letter. Applied to new rows and to carried rows whose text gate 3 replaced, never to today's PDF text (its detached marks are gate 3's classes) |
| R2c stubs | owner's ruling of 2026-10-26: when two FIST units serve the same image, relation and language of one manuscript (team 200's sub-team 1 beside sub-team 4's `_Tc` file), today's chooser lists only the longer text; a shorter row with under 30 % of the longer's letters and not the same text (ratio < 0.95) is a stub of a transcription filed twice and is retired from the candidate (`stub_retired`, the longer row's uid recorded). Two rows of about one length that differ are two readings and both stay (listed in §5, hidden by the chooser until it shows both) |
| R5 new rows | under `--new-rows-policy plain` (the owner's ruling of 2026-10-26: these rows ship) a new (unit, AlmaId) row read by gate 3 enters iff its text is non-blank, not a test or dummy file (R2a), not a legacy-font text (R2b), its FIST unit comment does not say it was never checked against the source (`fist_comment_unchecked`, gate 2's rule of 2026-10-26), and it comes from folder B, or from a DOCX with no tables, footnotes, hidden runs, tracked changes or auto-numbering (`docx_structure` holds the rest); a folder-B row's `c_number` is the image number of its folder, only when `b_numbers` (the NLI gallery) lists that image under the row's manuscript (`image_alma_mismatch` / `image_unknown` hold the rest); a row today's chooser could never list is held: `whole_doc_shadowed` (a whole-document row on a manuscript whose candidate has per-image rows of the same relation and language — `dedupe_fgp_sources` drops it), `image_already_served` (another unit already serves the image — the chooser keeps one row per image), `shadows_whole_doc_row` (the manuscript has a whole-document row of the same relation and language, which the new per-image row would hide). Under `hold` no new row enters. Credits by gate 2's resolver on the FIST unit (generic when unresolved) |
| R6 meta | `fgp_meta.version` = `2.0.0`, `candidate`, `built_at`, `built_from_gate3_digest`, `new_rows_policy`, row counts |

## 2. Checks

| check | fails when |
|---|---|
| C1 two runs | the digest (candidate rows, every disposition, every default decision) differs from the previous run's |
| C2 selectability | a source the real `FgpService.get_fgp_sources_for_fragment` lists today is not listed from the candidate and carries none of: `retired_alias` (its same-meaning successor is listed), `gate2_hold`, `xml_unverified`, `empty_text`, `test_or_dummy`, `legacy_font`, `stub_retired`, `same_text_swap` (its image group is now won by a row whose text matches at ratio ≥ 0.95); or a new row is not listed; or a same-meaning successor is not listed. Every manuscript of either DB |
| C3 identity | a carried row's `id`, `uid`, `doc_relation` or `language` changed — the only keys a saved choice can rest on |
| C4 page assignment | a carried row's `sys_id`, `c_number`, `image_side`, `page_info` or `folio_num` changed without an approval; a new per-image row's image is not confirmed for its manuscript by `nli_crossref.db` |
| C5 defaults | on the reference set, page by page: the web shape (`filter_sources_for_page` with the labels `folio_label_for_displayed_page` / `fgp_image_number_for_displayed_page` resolve) and the desktop shape (`_select_fgp_sources_for_page` with `displayed_folio_label` / `displayed_fgp_image_number`) disagree on either DB; or `choose_default_source`'s decision changes between the DBs without a disposition: `new_edition` (the candidate defaults to, or the page gained, a new row), `row_excluded` (a row of today's page set left the candidate — chosen or demoted, since a demoted edition's absence changes the reason too), `text_replaced` (an edition on the page carries gate 3's text), `same_text_swap`, `approved` |
| C6 latency | over the budget (§3) |
| C7 memory, size | over the budget (§3) |
| C8 contract | the service does not discover the table, `get_version()` ≠ `2.0.0`, a row lacks `sys_id` or text, counts do not reconcile |
| E_* | a declared expectation is missed |

Both apps render the shared decision only (`web/components/version_selector.py`, `genizah_app.py::_auto_select_pgp_edition`;
pinned by `tests/test_fgp_chooser_integration.py`), so driving the shared functions with both argument shapes is the
validation of both apps. PGP editions pre-empt the FGP decision identically on both DBs and are left out of the
drive. The decision is measured with the page's HTR text and the whole-manuscript HTR getter, as both apps pass them.

## 3. Declared budgets (before the first run)

| budget | limit | why |
|---|---|---|
| `latency.load` (`get_fgp_sources_for_fragment`, candidate) | p95 ≤ 500 ms, max ≤ 2,000 ms | one manuscript load in the reading view; after one warm-up call per DB |
| `latency.page` (`filter_sources_for_page` + `choose_default_source`, candidate) | p95 ≤ 250 ms, max ≤ 2,000 ms | runs on every page turn, twice on the web |
| candidate vs today | candidate p95 ≤ 1.5 × today's p95 + 20 ms | the larger sidecar must not slow the chooser materially; the floor keeps sub-millisecond noise from failing |
| `memory.build_peak` | ≤ 3,072 MB | building the candidate on the 63 GB dev machine |
| `memory.app_open` | ≤ 128 MB | opening the candidate and reading the two full sys_id sets both apps read at startup (`get_sys_ids_with_fgp_editions`, `get_sys_ids_with_fgp_sources`); the server runs at a 5–6 GB baseline |
| `memory.heaviest_manuscript` | ≤ 64 MB | loading the reference manuscript with most chooser rows |
| `size.candidate` | ≤ 1,024 MB | today's file is 405 MB; the installer and the deploy copy it |

## 4. The reference set (declared)

The union of: A the 10 manuscripts with most today rows · B the 10 with most new rows · C MS heb. g.2
(`990053489970205171`, the text-match demotion case) · D 20 manuscripts with a replaced text, seeded sample
(`random.Random(20261026)`) · E 20 with both today rows and new rows, seeded · F up to 20 with an excluded today
row · G every manuscript of a `same_text_swap` plus up to 10 whose image groups hold rows of several FIST units.
Pages = the manuscript's HTR pages; a manuscript without HTR pages is counted (`reference.without_htr`) and driven
with empty text, as the apps do.

## 5. Findings the run must state (written before the run, from the probes of 2026-10-26)

- Today's chooser collapses every image group to one row (`dedupe_fgp_sources` keeps the longest). 3,603 image groups
  hold rows of two FIST units (7,223 rows, team 200 sub-teams 1 and 4 — the same transcription filed twice); 3,508
  are the same text, 57 a short stub (18–88 letters) against a full text, 38 differ. The 95 short or different ones
  are hidden today; this gate does not change that, it lists them.
- The reading desk (`web/pages/browse.py`) saves a source choice as `src_<position>` and restores it into state but
  never applies it to the dropdown; the version selector and the desktop save no choice. A saved choice can only
  ever rest on `fgp:<id>`, which C3 keeps.
- Gate 3's new rows carry no `c_number`; the candidate derives the image number from the folder-B path, and 1,153
  of the 2,768 folder-B rows name a manuscript the gallery does not list for that image (one row per AlmaId of a
  multi-AlmaId unit) — held, not published.

## 6. Run record

**2026-10-26 (fourth run, on the owner's rulings) — PASS, 15 checks + 58 declared expectations, two runs with one
digest** (`b5994fab1a88eb1a`; about 90 s per run on the dev machine, the search engine loaded once for the HTR pages).
Earlier records of the day: `d5b89673fd971ae3` (44,500 carried, 6,433 new, 51 expectations) before gate 2's
`fist_comment_unchecked` hold and R2a; `d2645076b9603797` (44,332 / 6,408 / 55) before R2b; `44adcc4e93a5bcf8`
(44,332 / 6,395 / 56) before the 10 gate-3 approvals and R2c. The numbers below replace them.

The candidate (`fgp_data/inventory/fgp_transcriptions.candidate.db`, 294 MB, 50,681 rows, `version` 2.0.0):

| rows | count | of which |
|---|---|---|
| today's rows carried | 44,286 | 27,530 with gate 3's text (10 of them by the owner's approval), 16,756 with today's (identical, XML verified, or held by gate 3) |
| today's rows excluded | 748 | 391 XML rows gate 3 could not verify · 245 gate-2 holds (165 `fist_comment_unchecked`, 23 `tmp_DoubleNotToDisplay`, 19 absent files, 26 XML region, 5 other edition, 7 unconfirmed associations) · 56 stubs retired (R2c; the 57th of §5's count is held for another reason) · 50 retired (17 with a same-meaning successor) · 3 without text (an empty DOCX too) · 3 test or dummy files (`טסט`, a dots-only row, a 36-character English line filed as an edition) · no legacy font among the replaced texts |
| new rows entered (`plain`) | 6,395 | 1,569 per image (folder B), 4,826 whole document (plain DOCX); credits: 2,168 books, 1,806 teams, 1,588 infrastructure, 659 named contributors, 117 catalogues, 39 generic, 18 others |
| new rows held | 18,884 | 17,124 structured DOCX · 1,138 image–manuscript mismatches · 322 without an AlmaId · 168 blank · 48 whole-document rows shadowed · 26 unknown images · 21 test or dummy files (`test`, `ggg`, `נסיון / נא להתעלם!!!`, `241241`, teams 240 and 850) · 19 per-image rows that would hide a whole-document row · 13 legacy-font texts (10 units: RNL Arab.-Yevr. 24–81 and EVR ARAB I 1802 / 4528 of team 107, two Katz Center rows of team 131) · 4 units FIST says were never checked against the source · 1 image another unit serves |

Checks: selectability over 27,606 manuscripts through the real service — 40,447 sources kept, 555 lost and each
dispositioned (389 `xml_unverified`, 135 `gate2_hold`, 22 `retired_alias` with the successor listed, 3
`empty_text`, 3 `test_or_dummy`, 1 `stub_retired` — a stub that won its image today because the longer row's PDF
text was the shorter —, 2 `same_text_swap`), all 6,395 new rows listed; identity and page keys unchanged on all
44,286 carried rows, 1,569 new image numbers confirmed by `nli_crossref.db`; on the 88 reference manuscripts (1,237
pages; 6 without HTR) the web and desktop shapes agree everywhere and the 502 default changes are all dispositioned
(432 `new_edition`, 67 `row_excluded`, 1 `text_replaced`, 2 `same_text_swap`), 735 pages unchanged.

| budget | measured | limit |
|---|---|---|
| manuscript load, candidate | p50 0.3 ms · p95 4.2 ms · max 8.5 ms | p95 500 ms, max 2,000 ms, ≤ 1.5× today |
| page filter + default, candidate | p50 0.6 ms · p95 6.6 ms · max 45.0 ms | p95 250 ms, max 2,000 ms, ≤ 1.5× today |
| build peak working set | 950 MB | 3,072 MB |
| app-side open + full sys_id sets | 11.2 MB | 128 MB |
| heaviest reference manuscript | 0.1 MB | 64 MB |
| candidate file | 294 MB | 1,024 MB |

The first full run of the day failed twice before passing, both on the rules as first written: 13 of today's rows
carry no text (the PDF yielded none; 11 have a DOCX text gate 3 holds as `extra_line`) and are now excluded by R2;
and 30 default changes were a demoted XML edition leaving the candidate, which the `row_excluded` disposition now
covers whether or not the row was the default. R2b was first measured as "marks outnumber half the letters", which
would have held 34 rows including five vocalised piyyutim (Or. 4856) and, applied to carried rows, 24 English
translations with decomposed diacritics; the word-opening orphan count holds the 13 garbled texts and none of those.

**The owner's rulings of 2026-10-26 (by phone, "yes to all")**: the `plain` new-rows policy ships; the text-less
rows take their DOCX texts (10 approvals in `scripts/fgp_gate3_approvals.json`, keyed on the durable key and both
hashes; the three rows whose DOCX is also empty stay excluded); the stubs of the same-image pairs are retired (R2c)
while the pairs of two readings stay as they are; the 1,138 folder-B rows whose manuscript the gallery does not
list for the image stay held. Gates 3 and 4 were rerun on these rulings the same day (record below).

### Drafts and short texts in the delta (owner's question of 2026-10-26; `_tmp/fgp_delta_drafts_report.md`)

The delta holds drafts and short rows, at a lower rate than today's rows, with three exceptions of its own. Short
texts: 8.0 % of the delta's rows are under 200 non-whitespace characters against 18.0 % of today's (medians 861 vs
553); on the same format (DOCX) 6.3 % vs 10.4 %; folder-B per-image rows are as short on both sides (median ≈ 290
characters). The short delta rows come from team 200 (677 units), team 107's Firkovitch `העתקות:` excerpts (391) and
team 500's catalogue rows (123). Declared drafts: `לפני הגהה אחרונה` ("before final proofreading", team 151) is on
291 of today's rows and 298 of the delta's — today's sidecar already publishes them; `!DRAFT` (team 133, 7 delta
rows), `טרם הוגה` (1 each side), `הקריאה לא סופית` (1 delta row). FIST marks more draft-like units among today's
rows than the delta's: `PartOfTranscription` = partial on 1,600 of today's units vs 214; the unit comment `לא נבדק
מול המקור` / `אין לפרסם עד לקריאה מול מקור` on 168 of today's rows (166 publishable under gate 2 — open-issue fragment
2026-10-26) vs 4 in the delta; `PermissionCommentCode` 5309 on 156 of today's units vs 8. The delta's own exceptions:
168 empty files (56 units; 98 rows are two team-600 translations fanned out over 49 manuscripts) — gate 4 holds them
as `blank_text`; 19 test or dummy units (23 rows, teams 240 and 850: "test", "ggg", "נסיון") — held since the same
day by R2a (`test_or_dummy`), while team 850's non-manuscript documents (a contact list, a recipe, software notes)
are longer texts no rule catches; and `dbo_Signature.RecordStatus` 1 or 5 on 6,586 delta units (35 %) against one
of today's 40,416, with no code table to say what the values mean. The owner ruled the same day: gate 2 holds the
unchecked comments (`fist_comment_unchecked`) and gate 4 the test units; later the same day the owner ruled that
the `plain` rows ship. Still without a ruling, and worth one before the sidecar is deployed: the meaning of
`RecordStatus` 1 / 5 and of code 5309 (open-issue fragment 2026-10-26, a question for FGP), team 850's
non-manuscript documents, and whether team 107's excerpt rows are editions at all. `content_length` in today's sidecar is
`len(content)`, whitespace included, not the non-whitespace count the schema comment claims.
