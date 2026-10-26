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
| R2 carried | a today row is carried iff gate 2 did not retire it, it is gate-2 publishable (or its only gate-2 hold is `xml_unverified` and gate 3 verified its XML text by the row's own record), and it has a text. Excluded with reason `retired_alias` / `gate2_hold:<reasons>` / `xml_unverified` / `empty_text` (today's 13 PDF rows that yielded no text; 11 have a DOCX text gate 3 holds as `extra_line` until the owner approves it) |
| R3 text | gate 3's text where gate 3 replaced it (status accepted / approved), else today's; `sections` (the PDF page split) is cleared on a replaced text (neither app reads it for FGP rows) |
| R4 credit | gate 2's `credit_he` / `credit_en`; the generic Friedberg attribution where gate 2 ruled generic |
| R5 new rows | under `--new-rows-policy plain` a new (unit, AlmaId) row read by gate 3 enters iff its text is non-blank and comes from folder B, or from a DOCX with no tables, footnotes, hidden runs, tracked changes or auto-numbering (`docx_structure` holds the rest); a folder-B row's `c_number` is the image number of its folder, only when `b_numbers` (the NLI gallery) lists that image under the row's manuscript (`image_alma_mismatch` / `image_unknown` hold the rest); a row today's chooser could never list is held: `whole_doc_shadowed` (a whole-document row on a manuscript whose candidate has per-image rows of the same relation and language — `dedupe_fgp_sources` drops it), `image_already_served` (another unit already serves the image — the chooser keeps one row per image), `shadows_whole_doc_row` (the manuscript has a whole-document row of the same relation and language, which the new per-image row would hide). Under `hold` no new row enters. Credits by gate 2's resolver on the FIST unit (generic when unresolved) |
| R6 meta | `fgp_meta.version` = `2.0.0`, `candidate`, `built_at`, `built_from_gate3_digest`, `new_rows_policy`, row counts |

## 2. Checks

| check | fails when |
|---|---|
| C1 two runs | the digest (candidate rows, every disposition, every default decision) differs from the previous run's |
| C2 selectability | a source the real `FgpService.get_fgp_sources_for_fragment` lists today is not listed from the candidate and carries none of: `retired_alias` (its same-meaning successor is listed), `gate2_hold`, `xml_unverified`, `empty_text`, `same_text_swap` (its image group is now won by a row whose text matches at ratio ≥ 0.95); or a new row is not listed; or a same-meaning successor is not listed. Every manuscript of either DB |
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

**2026-10-26 — PASS, 15 checks + 51 declared expectations, two runs with one digest** (`d5b89673fd971ae3`; 56 s per
run on the dev machine, the search engine loaded once for the HTR pages).

The candidate (`fgp_data/inventory/fgp_transcriptions.candidate.db`, 295 MB, 50,933 rows, `version` 2.0.0):

| rows | count | of which |
|---|---|---|
| today's rows carried | 44,500 | 27,686 with gate 3's text, 16,814 with today's (10,780 identical, 2,677 XML verified, the held rows) |
| today's rows excluded | 534 | 391 XML rows gate 3 could not verify · 80 gate-2 holds (23 `tmp_DoubleNotToDisplay`, 19 absent files, 26 XML region, 5 other edition, 7 unconfirmed associations) · 50 retired (17 with a same-meaning successor) · 13 without text |
| new rows entered (`plain`) | 6,433 | 1,569 per image (folder B), 4,864 whole document (plain DOCX); credits: 2,168 books, 1,819 teams, 1,588 infrastructure, 663 named contributors, 118 catalogues, 53 generic, 24 others |
| new rows held | 18,846 | 17,124 structured DOCX · 1,138 image–manuscript mismatches · 322 without an AlmaId · 168 blank · 48 whole-document rows shadowed · 26 unknown images · 19 per-image rows that would hide a whole-document row · 1 image another unit serves |

Checks: selectability over 27,625 manuscripts through the real service — 40,528 sources kept, 474 lost and each
dispositioned (389 `xml_unverified`, 48 `gate2_hold`, 22 `retired_alias` with the successor listed, 13
`empty_text`, 2 `same_text_swap`), all 6,433 new rows listed; identity and page keys unchanged on all 44,500
carried rows, 1,569 new image numbers confirmed by `nli_crossref.db`; on the 88 reference manuscripts (1,130 pages; 6
without HTR) the web and desktop shapes agree everywhere and the 497 default changes are all dispositioned (430
`new_edition`, 62 `row_excluded`, 3 `text_replaced`, 2 `same_text_swap`), 633 pages unchanged.

| budget | measured | limit |
|---|---|---|
| manuscript load, candidate | p50 2.0 ms · p95 2.7 ms · max 6.0 ms | p95 500 ms, max 2,000 ms, ≤ 1.5× today |
| page filter + default, candidate | p50 1.2 ms · p95 4.4 ms · max 24.7 ms | p95 250 ms, max 2,000 ms, ≤ 1.5× today |
| build peak working set | 950 MB | 3,072 MB |
| app-side open + full sys_id sets | 10.6 MB | 128 MB |
| heaviest reference manuscript | 0.1 MB | 64 MB |
| candidate file | 295 MB | 1,024 MB |

The first full run failed twice before passing, both on the rules as first written: 13 of today's rows carry no
text (the PDF yielded none; 11 have a DOCX text gate 3 holds as `extra_line`) and are now excluded by R2; and 30
default changes were a demoted XML edition leaving the candidate, which the `row_excluded` disposition now covers
whether or not the row was the default.

Open for the owner (beside gate 3's rulings): whether the 11 text-less rows' DOCX texts are approved; whether the
95 shorter rows of the same-image pairs (§5) are drafts to retire; the 1,138 folder-B rows whose manuscript the
gallery does not list for the image; the `plain` policy itself.
