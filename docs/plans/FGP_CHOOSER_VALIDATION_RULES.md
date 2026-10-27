# FGP chooser validation rules (gate 4)

Gate 4 of [`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) §8.4. Script
`scripts/fgp_gate4_chooser_validation.py`, wrapper `scripts/run_fgp_gate4_chooser_validation.ps1` (two runs, one
digest), tests `tests/test_fgp_gate4_chooser_validation.py`, expectations
`scripts/fgp_gate4_chooser_validation.expect.json`, approvals `scripts/fgp_gate4_approvals.json` (absent = none).
Inputs: today's `fgp_data/fgp_transcriptions.db` (read only), `gate2.db`, `gate3.db`, `inventory.db`, `FIST.db`,
`nli_data/nli_crossref.db`, `libraries.csv`, the owner-ruled exclusions `scripts/fgp_gate4_exclusions.json` (R2d), and the HTR (V0.8) pages of the reference manuscripts from the search
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
| R2 carried | a today row is carried iff gate 2 did not retire it, it is gate-2 publishable (or its only gate-2 hold is `xml_unverified` and gate 3 verified its XML text by the row's own record), it has a text, and the text is not a test or dummy file. Excluded with reason `retired_alias` / `gate2_hold:<reasons>` (since 2026-10-26 also gate 2's `fist_comment_no_publish`, narrowed the same day from `fist_comment_unchecked`) / `xml_unverified` / `empty_text` (today's 13 PDF rows that yielded no text; 11 have a DOCX text gate 3 holds as `extra_line` until the owner approves it) / `personal_data` (R2e) / the reason listed for it (R2d) / `test_or_dummy` |
| R2a test files | owner's ruling of 2026-10-26: a text of at most 40 characters is a test or dummy file when it is a transcription (`Digital Edition`) without a Hebrew or Arabic letter (`test`, `ggg`, `fgfghfg`, `241241`, `....`), or consists of test words and digits only (`בדיקה`, `נסיון`, `test`), or carries `טסט` or `נא להתעלם` ("please ignore") anywhere. Translations are judged by the word rules only (an English translation is never Latin-only junk by itself). Every row it holds is listed in `row_dispositions` |
| R3 text | gate 3's text where gate 3 replaced it (status accepted / approved), else today's; `sections` (the PDF page split) is cleared on a replaced text (neither app reads it for FGP rows) |
| R4 credit | gate 2's `credit_he` / `credit_en`; the generic Friedberg attribution where gate 2 ruled generic |
| R2b legacy fonts | owner's ruling of 2026-10-26: a DOCX typed in a pre-Unicode Hebrew font arrives as vowel points standing where letters should be (RNL Arab.-Yevr. 72: «ַָָ ם ַהו זַֽֿ בַ ַֻהם בו»). In Unicode a combining mark follows a letter; a mark that opens a word (follows a space, digit or punctuation and is itself followed by a letter or a mark) is such a font's letter. A gate-3 text is held (`legacy_font`) when it has at least 10 such orphan marks and at least a tenth as many as Hebrew and Arabic letters. A vocalised piyyut has none; a mark standing alone between spaces is a transcriber's placeholder for an illegible letter and is not counted; a Latin letter with a decomposed diacritic is a letter. Applied to new rows and to carried rows whose text gate 3 replaced, never to today's PDF text (its detached marks are gate 3's classes) |
| R2c stubs | owner's ruling of 2026-10-26, **corrected the same evening**: when two FIST units serve the same image, relation and language of one manuscript, today's chooser lists only the longer text; a shorter row is retired as a stub (`stub_retired`, the longer row's uid and the contained share recorded) only when it has under 30 % of the longer's letters **and is a part of the longer text: at least 80 % of its Hebrew and Arabic letters lie, in order, in runs of six or more letters of the longer text** (`contained_share`; transcription marks, brackets and dots are not letters). A shorter row that is not contained is another text and stays, like two readings of about one length (hidden by the chooser until it shows several editions per image). The first form of the rule ("under 30 % and not the same text") retired 56 of today's rows on length alone; the Codex review of the candidate and a check of all 56 showed that none is a part of its "longer" text (the highest share 0.33 in runs of six letters; 36 share under half their letters even letter by letter): every one is team 200's `צוות FGP להעתקות -גנוזות` row (sub-team 4) on a different photograph of the same C-number as sub-team 1's text — e.g. `fgp:16363`, a Talmud passage (Yoma, «כל עבירות שבתורה… ונקה»), beside `fgp:16362`, a halakhic text. A real excerpt of the longer text typed with small differences scores 0.996 |
| R2d listed exclusions | owner's ruling of 2026-10-26 evening: rows that are not a manuscript's text and that no general rule catches are listed in `scripts/fgp_gate4_exclusions.json` by durable key, with a reason — `non_manuscript` (team 850's recipe `fgp:52436`, the Polus system specification `fgp:52195`, the Babylonian Talmud project's entry list `fgp:52445`) and `keyboard_dummy` (six of today's rows of keyboard noise: `fgp:34819` «אא בב גג דד», 24925, 34818, 34814, 34815, 33761). A listed today row is excluded, a listed new row held, under the listed reason; a reason outside these two is an error; every listed key must name a row of today's sidecar or of gate 3's new rows (C9) |
| R2f superseded versions | 2026-10-26 evening, after Codex asked that the unexplained status codes be settled before shipping: FIST keeps each version of a unit as its own signature (same `SourceId`, `SubId`, `UnitNumInSource`; `Version` 1, 2, …; the newer one's `VersionSignature` names the older). The export has no code table for `dbo_Signature.RecordStatus`, but of the 3,945 new rows that entered with status 5, 3,926 have a newer status-0 version of the same unit and 3,920 of those newer versions are already in the candidate (2,181 among today's rows, 1,739 among the new ones); in a sample of 300 old/new pairs on one manuscript 250 have identical letters and 37 more ≥ 0.95. Status 5 is therefore "replaced by a newer version": such a unit is held (new row) or excluded (today's row; none today) as `superseded_version`, the newest status-0 signature recorded. Status 1 (1,982 new rows and 19 of today's, no newer version, mostly team 200's 2018 delivery) is not read: its meaning is not in the data. `ViewLevelCode` is 310 ("רגיל", regular) on every candidate row, against 2310 "authorised only" and 3310 "not for publication"; `PermissionCommentCode` 5309 ("being worked on … temporarily inaccessible") and 4309 ("not available for viewing") are on no new row (5309 is on 180 of today's rows, served today) |
| R2g status-1 translations | owner's ruling of 2026-10-26 evening, after looking up a sample on FJMS (`_tmp/fgp_status1_lookup.md`): FGP shows the status-1 *transcriptions* sampled from teams 200 (both rows), 107 (both) and 500 (one) with the same text, and neither status-1 *translation* (team 600's `fgp:59398` on T-S NS J136, whose text is not that fragment's; team 133's English `fgp:59386`); the AIU rows could not be found. Status-1 transcriptions enter; a new translation whose signature has `RecordStatus` 1 is held (`status1_translation`) — 26 rows that had entered, 50 under this first reason. Today's 19 status-1 rows (transcriptions, team 132) stay as served |
| R2e personal data | owner's ruling of 2026-10-26 evening: a text with at least three Israeli telephone numbers (`05x` mobile, `0x` landline, `+972`, hyphen optional; never a digit run inside a longer number) or an e-mail address (a Latin address with a known top-level domain — the transcribers' `@…@` notation is not one) is held (new row) or excluded (today's row) as `personal_data`. It holds team 850's contact list (`fgp:52439`, `52441`, `52443`: one file on three images, 107 numbers each, credited to a named site user); a sweep of both sidecars found no other row with three numbers or an address |
| R5 new rows | under `--new-rows-policy plain` (the owner's ruling of 2026-10-26: these rows ship) a new (unit, AlmaId) row read by gate 3 enters iff its text is non-blank, not a test or dummy file (R2a), not a legacy-font text (R2b), its FIST unit comment does not say it must not be published until read against the source (`fist_comment_no_publish`, gate 2's rule of 2026-10-26, narrowed the same day from the broader `fist_comment_unchecked`), and it comes from folder B, or from a DOCX whose rendering gate 3 checked clean against FGP's PDF of the file, or that has no PDF text (gate 3 status `checked` / `no_comparator`; a `pdf_held` rendering holds the row `pdf_held`), and whose tables are one-column line blocks (`table_max_columns` < 2; a real table holds the row `docx_multicolumn_table`) — the owner's rulings of 2026-10-26 in [`FGP_DOCX_STRUCTURE_RULES.md`](FGP_DOCX_STRUCTURE_RULES.md), which replaced the earlier `docx_structure` hold on any of tables, footnotes, hidden runs, tracked changes or auto-numbering (census: 17,124 rows, 15,638 of them held for hidden runs alone); a folder-B row's `c_number` is the image number of its folder, only when `b_numbers` (the NLI gallery) lists that image under the row's manuscript (`image_alma_mismatch` / `image_unknown` hold the rest); a row today's chooser could never list is held: `whole_doc_shadowed` (a whole-document row on a manuscript whose candidate has per-image rows of the same relation and language — `dedupe_fgp_sources` drops it), `image_already_served` (another unit already serves the image — the chooser keeps one row per image), `shadows_whole_doc_row` (the manuscript has a whole-document row of the same relation and language, which the new per-image row would hide). Under `hold` no new row enters. Credits by gate 2's resolver on the FIST unit (generic when unresolved) |
| R6 meta | `fgp_meta.version` = `2.0.0`, `candidate`, `built_at`, `built_from_gate3_digest`, `new_rows_policy`, row counts |

## 2. Checks

| check | fails when |
|---|---|
| C1 two runs | the digest (candidate rows, every disposition, every default decision) differs from the previous run's |
| C2 selectability | a source the real `FgpService.get_fgp_sources_for_fragment` lists today is not listed from the candidate and carries none of: `retired_alias` (its same-meaning successor is listed), `gate2_hold`, `xml_unverified`, `empty_text`, `test_or_dummy`, `legacy_font`, `stub_retired`, `personal_data`, `non_manuscript`, `keyboard_dummy`, `superseded_version`, `same_text_swap` (its image group is now won by a row whose text matches at ratio ≥ 0.95, `text_ratio` over the whole text — until 2026-10-26 evening it read only the first 4,000 letters, which scored two texts with one beginning and different endings as the same; the two dispositions of the sixth run hold at full length, 0.995 and 1.000); or a new row is not listed; or a same-meaning successor is not listed. Every manuscript of either DB |
| C3 identity | a carried row's `id`, `uid`, `doc_relation` or `language` changed — the only keys a saved choice can rest on |
| C4 page assignment | a carried row's `sys_id`, `c_number`, `image_side`, `page_info` or `folio_num` changed without an approval; a new per-image row's image is not confirmed for its manuscript by `nli_crossref.db` |
| C5 defaults | on the reference set, page by page: the web shape (`filter_sources_for_page` with the labels `folio_label_for_displayed_page` / `fgp_image_number_for_displayed_page` resolve) and the desktop shape (`_select_fgp_sources_for_page` with `displayed_folio_label` / `displayed_fgp_image_number`) disagree on either DB; or `choose_default_source`'s decision changes between the DBs without a disposition: `new_edition` (the candidate defaults to, or the page gained, a new row), `row_excluded` (a row of today's page set left the candidate — chosen or demoted, since a demoted edition's absence changes the reason too), `text_replaced` (an edition on the page carries gate 3's text — also when the page's edition changed hands between two whole-document rows and either row's text was replaced, since coverage decides which of them serves the page), `same_text_swap`, `approved` |
| C6 latency | over the budget (§3) |
| C7 memory, size | over the budget (§3) |
| C8 contract | the service does not discover the table, `get_version()` ≠ `2.0.0`, a row lacks `sys_id` or text, counts do not reconcile |
| C9 exclusions | a durable key listed in `scripts/fgp_gate4_exclusions.json` names no row of today's sidecar or of gate 3's new rows (`exclusions.unmatched` > 0), so the list cannot go stale unnoticed |
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
  are hidden today; this gate does not change that, it lists them. **Corrected 2026-10-26 evening:** the 57 "short
  stubs" are not parts of the longer text but other texts on a different photograph of the same C-number (R2c);
  sub-teams 1 and 4 file the same transcription twice only where the texts agree.
- The reading desk (`web/pages/browse.py`) saves a source choice as `src_<position>` and restores it into state but
  never applies it to the dropdown; the version selector and the desktop save no choice. A saved choice can only
  ever rest on `fgp:<id>`, which C3 keeps.
- Gate 3's new rows carry no `c_number`; the candidate derives the image number from the folder-B path, and 1,153
  of the 2,768 folder-B rows name a manuscript the gallery does not list for that image (one row per AlmaId of a
  multi-AlmaId unit) — held, not published.

## 6. Run record

**2026-10-27 morning (eleventh run, final letters, GS-276) — PASS, 16 checks + 66 declared expectations, two runs
with one digest** (`07dd7d1f048713ea`; about 95 s per run), on gate 3's rerun with the final-letter step (digest
`3f166188e2ebcc56`, `FGP_TEXT_PIPELINE_RULES.md` §8). The same 59,056 rows, carried / excluded / new / held as in
the tenth run, and every declared expectation held, the default decisions included (1,625 pages; 6 `text_replaced`).
**1,174 carried rows keep today's PDF text with their final letters restored** (`text_provenance`
`gate3:final_letters`, `text_source` `<today's>+final_letters`; two of gate 3's 1,176 repaired rows are excluded by
R2); rows still carrying a control code 1,810 → 1,168 (codes inside words, and codes below the bar). The checker
passes the rebuilt candidate (sha256 `0b1fb5faddff0bf6…`).

**2026-10-26 late evening (tenth run, the checksum in the summary) — PASS, 16 checks + 66 declared expectations, two
runs with one digest** (`14a8cfb929633f56`, the ninth run's: the same 59,056 rows; about 60 s per run). Codex found
that `scripts/check_fgp_sidecar.py` tied the file to the run only by `built_at`, so a transcription edited after the
run still passed; gate 4 now records the file's own checksum as `meta.candidate_sha256` (taken after every handle
on the file is closed; not part of the digest, since `built_at` differs between the two runs) and the checker
refuses a file that does not match it, or a summary that records none. The checker passes the rebuilt candidate
(sha256 `be9f5d84a9677b22…`), and so does the deploy script's dry run.

**2026-10-26 evening (ninth run, R2g) — PASS, 16 checks + 66 declared expectations, two runs with one digest**
(`14a8cfb929633f56`; about 60 s per run). The candidate holds **59,056 rows: 44,484 of today's carried (27,673 with
gate 3's text, as before) and 14,572 new** (12,986 whole document); 550 of today's rows excluded and 10,707 new rows
held, 50 of them `status1_translation` (the 26 that had entered and 24 `pdf_held` now reach first). Against the
eighth run: new −26 (whole document), held +26, one manuscript fewer in the selectability sweep (30,654); the
reference draw moved again (1,625 pages, 7 without HTR; 943 default changes all dispositioned: 868 `new_edition`,
68 `row_excluded`, 6 `text_replaced`, 3 `same_text_swap`; 680 unchanged). `scripts/check_fgp_sidecar.py` passes the
file as the one this run built (sha256 `ce53c4010b797f62…`, `integrity_check` ok, version 2.0.0).

**2026-10-26 evening (eighth run, R2f) — PASS, 16 checks + 65 declared expectations, two runs with one digest**
(`9b3305335cb57b79`; about 113 s per run). The candidate holds **59,082 rows: 44,484 of today's carried (27,673 with
gate 3's text) and 14,598 new** (1,586 per image, 13,012 whole document); 550 of today's rows excluded (as in the
seventh run) and 10,681 new rows held, 4,506 of them `superseded_version` — the 3,945 status-5 rows that had entered
and 561 that other holds had already stopped and that R2f now reaches first (pdf_held 1,153 → 812, whole-document
shadowed 3,858 → 3,666, real tables 28 → 12, test files 21 → 15, legacy fonts 13 → 9, gallery mismatches 1,138 →
1,136). Every lost source dispositioned as in the seventh run; all 14,598 new rows listed; 88 reference manuscripts
(1,483 pages, 6 without HTR; the draw moved), 933 default changes all dispositioned (860 `new_edition`, 68
`row_excluded`, 3 `text_replaced`, 2 `same_text_swap`), 550 unchanged. No candidate row has status 5 (a check of the
built file against FIST: today's rows 44,465 status 0 and 19 status 1, new rows 12,616 status 0 and 1,982 status 1);
`integrity_check` ok; 326 MB; `scripts/check_fgp_sidecar.py` passes it as the file this run built (sha256
`ef9068b7…a00300e4`).

Of the 860 `new_edition` default changes, most leave the default unchanged in what the page shows: the chooser lists
the new edition but demotes it for low coverage of the page's HTR text (`demote_low_coverage`), so the HTR text stays
the default. The ones that make a new FGP edition the default passed the same coverage test against the page's HTR
text, except a few on pages whose HTR text is too short to test (`htr_too_short`). (Counted on the seventh run: 556
demoted, 252 by coverage, 15 too short.)

**2026-10-26 evening (seventh run, after the Codex review of the candidate) — PASS, 16 checks + 64 declared
expectations, two runs with one digest** (`1a5d7558fa8eac09`; about 135–170 s per run), over gate 3 rerun from the
committed code the same evening (PASS 64/64, digest `0088ba547d3dfc83`, identical to the sixth run's input). Three
changes: R2c retires a stub only when it is contained in the longer text, R2d excludes the nine listed rows, R2e
holds personal data; `text_ratio` reads the whole text; C9 is new. The candidate holds **63,027 rows: 44,484 of
today's carried (27,673 with gate 3's text) and 18,543 new**; 550 of today's rows excluded and 6,736 new rows held.
Against the sixth run: the 56 rows R2c had retired are back (none is contained in its "longer" text; 2 carry gate 3's
text), the 6 keyboard-dummy rows are excluded (all 6 had gate 3's text) — carried +50, replaced −4; 6 new
whole-document rows are held (3 `personal_data`: the contact list on three images; 3 `non_manuscript`) — new −6,
5 of them credited to a site user and 1 generic; 3 manuscripts that only those rows served leave the selectability
sweep (30,799). Lost sources all dispositioned (389 `xml_unverified`, 59 `gate2_hold`, 22 `retired_alias`, 6
`keyboard_dummy`, 3 `empty_text`, 3 `test_or_dummy`, 2 `same_text_swap`; no `stub_retired`); all 18,543 new rows
listed; the 9 listed keys all matched (C9). On the 88 reference manuscripts (1,418 pages; the seeded draw moved with
the row set) 895 default changes, all dispositioned (823 `new_edition`, 68 `row_excluded`, 2 `text_replaced`, 2
`same_text_swap`), 523 unchanged. Load p95 4.3 ms, page p95 12.2 ms, build peak 988 MB, app-side 12.6 MB, file
331 MB (the recorded second run). A sweep of the built candidate finds no row with three telephone numbers and none of the listed texts.

**2026-10-26 (sixth run, on the Word-file rulings) — PASS, 15 checks + 60 declared expectations, two runs with one
digest** (`058a0c2690d68b4f`; about 111 s per run). The candidate holds **62,983 rows: 44,434 of today's carried as in the fifth run
and 18,549 new** (1,586 per image, 16,963 whole document; 16,952 checked against FGP's PDF of their file); 6,730
new rows held — 3,858 whole-document rows the chooser would drop beside per-image rows (3,811 Nuscha), 1,153
`pdf_held`, 1,138 image–manuscript mismatches, 322 without an AlmaId, 168 blank, 28 real tables
(`docx_multicolumn_table`), 26 unknown images, 21 test files, 13 legacy fonts, 2 + 1 shadowing cases. Folder-B rows
are planned before whole-document DOCX rows. Selectability over 30,802 manuscripts, every lost source dispositioned
as before and all 18,549 new rows listed; 88 reference manuscripts (1,416 pages, 8 without HTR), 896 default changes
all dispositioned (822 `new_edition`, 68 `row_excluded`, 4 `text_replaced`, 2 `same_text_swap`), 520 unchanged;
load p95 3.7 ms, page p95 10.3 ms, build peak 989 MB, app-side
12.6 MB, file 331 MB. The full record is `FGP_DOCX_STRUCTURE_RULES.md` §7; the
fifth run's table below stands for today's rows, whose numbers did not move.

**2026-10-26 (fifth run, after gate 2 narrowed its comment hold) — PASS, 15 checks + 57 declared expectations,
two runs with one digest** (`20df7caf8dda1d74`; about 80 s per run on the dev machine, the search engine loaded once
for the HTR pages). Earlier records of the day: `d5b89673fd971ae3` (44,500 carried, 6,433 new, 51 expectations)
before gate 2's `fist_comment_unchecked` hold and R2a; `d2645076b9603797` (44,332 / 6,408 / 55) before R2b;
`44adcc4e93a5bcf8` (44,332 / 6,395 / 56) before the 10 gate-3 approvals and R2c; `b5994fab1a88eb1a` (44,286 /
6,395 / 58) with the owner's rulings, before the hold was narrowed to `fist_comment_no_publish` (the owner: a
transcription not checked against the source is a legitimate if imperfect transcription; 148 carried rows and 4
new rows came back). The numbers below replace them.

The candidate (`fgp_data/inventory/fgp_transcriptions.candidate.db`, 294 MB, 50,833 rows, `version` 2.0.0):

| rows | count | of which |
|---|---|---|
| today's rows carried | 44,434 | 27,677 with gate 3's text (10 of them by the owner's approval), 16,757 with today's (identical, XML verified, or held by gate 3) |
| today's rows excluded | 600 | 391 XML rows gate 3 could not verify · 97 gate-2 holds (17 `fist_comment_no_publish`, 23 `tmp_DoubleNotToDisplay`, 19 absent files, 26 XML region, 5 other edition, 7 unconfirmed associations) · 56 stubs retired (R2c; the 57th of §5's count is held for another reason) · 50 retired (17 with a same-meaning successor) · 3 without text (an empty DOCX too) · 3 test or dummy files (`טסט`, a dots-only row, a 36-character English line filed as an edition) · no legacy font among the replaced texts |
| new rows entered (`plain`) | 6,399 | 1,569 per image (folder B), 4,830 whole document (plain DOCX); credits: 2,168 books, 1,810 teams, 1,588 infrastructure, 659 named contributors, 117 catalogues, 39 generic, 18 others |
| new rows held | 18,880 | 17,124 structured DOCX · 1,138 image–manuscript mismatches · 322 without an AlmaId · 168 blank · 48 whole-document rows shadowed · 26 unknown images · 21 test or dummy files (`test`, `ggg`, `נסיון / נא להתעלם!!!`, `241241`, teams 240 and 850) · 19 per-image rows that would hide a whole-document row · 13 legacy-font texts (10 units: RNL Arab.-Yevr. 24–81 and EVR ARAB I 1802 / 4528 of team 107, two Katz Center rows of team 131) · 1 image another unit serves (the 4 units FIST says were never checked against the source entered when the hold was narrowed; no new unit carries the do-not-publish instruction) |

Checks: selectability over 27,609 manuscripts through the real service — 40,523 sources kept, 479 lost and each
dispositioned (389 `xml_unverified`, 59 `gate2_hold`, 22 `retired_alias` with the successor listed, 3
`empty_text`, 3 `test_or_dummy`, 1 `stub_retired` — a stub that won its image today because the longer row's PDF
text was the shorter —, 2 `same_text_swap`), all 6,399 new rows listed; identity and page keys unchanged on all
44,434 carried rows, 1,569 new image numbers confirmed by `nli_crossref.db`; on the 88 reference manuscripts (1,236
pages; 6 without HTR; the seeded draw changed 21 of the 88 with the candidate's row set) the web and desktop shapes
agree everywhere and the 506 default changes are all dispositioned (432 `new_edition`, 68 `row_excluded`, 4
`text_replaced`, 2 `same_text_swap`), 730 pages unchanged. One of the four `text_replaced` pages first failed C5
as `unexplained`: page 2 of MS 990053490500205171 has two whole-document editions (a site user's, Ginzei Qedem's)
and coverage against the HTR text decides which serves the page; gate 3 replaced both texts and the page changed
hands, which the hand-over branch of C5 now dispositions as `text_replaced` (§2) instead of only "same text".

| budget | measured | limit |
|---|---|---|
| manuscript load, candidate | p50 0.2 ms · p95 3.2 ms · max 5.5 ms | p95 500 ms, max 2,000 ms, ≤ 1.5× today |
| page filter + default, candidate | p50 0.3 ms · p95 4.3 ms · max 24.0 ms | p95 250 ms, max 2,000 ms, ≤ 1.5× today |
| build peak working set | 952 MB | 3,072 MB |
| app-side open + full sys_id sets | 11.0 MB | 128 MB |
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
the `plain` rows ship, and narrowed the comment hold to the 17 rows whose comment is the explicit do-not-publish
instruction (`fist_comment_no_publish`): "not checked against the source" is a quality note on a transcription FGP
itself shows. Still without a ruling, and worth one before the sidecar is deployed: the meaning of
`RecordStatus` 1 / 5 and of code 5309 (open-issue fragment 2026-10-26, a question for FGP), team 850's
non-manuscript documents, and whether team 107's excerpt rows are editions at all. `content_length` in today's sidecar is
`len(content)`, whitespace included, not the non-whitespace count the schema comment claims.
