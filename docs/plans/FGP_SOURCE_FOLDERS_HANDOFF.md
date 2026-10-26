# FGP source folders: handoff, plan and recommendations

Written 2026-10-26 at the end of the session that ran gates 1–4 and the Word-file release, for the session that
continues the work. It says where things stand, what exists and how to rerun it, what the owner has ruled and what
is still open, and the order I recommend for what comes next. Everything here is in the repository or in gitignored
data on the dev machine; nothing is deployed.

The owning records, in reading order: the assessment
[`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) (§8 the gates, Revisions 12–17 the day's
rulings), the four rules documents [`FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md`](FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md)
(gate 2), [`FGP_TEXT_PIPELINE_RULES.md`](FGP_TEXT_PIPELINE_RULES.md) (gate 3),
[`FGP_CHOOSER_VALIDATION_RULES.md`](FGP_CHOOSER_VALIDATION_RULES.md) (gate 4),
[`FGP_POSITIONS_RULES.md`](FGP_POSITIONS_RULES.md) (gate 5), and the Word-file census and rules
[`FGP_DOCX_STRUCTURE_RULES.md`](FGP_DOCX_STRUCTURE_RULES.md).

## 1. Where things stand

FGP delivered two folders: their file store (the original Word and PDF of every transcription, keyed by FIST unit)
and word positions for part of the images. Today's site serves 45,034 FGP transcriptions whose text came from the
PDFs, with known losses (line starts before a gershayim, final and vocalised letters, line order, mixed-direction
lines, Arabic). Four gates have been run on the new material and a **candidate sidecar** built and checked against
the real chooser of both apps; a fifth gate measured the positions.

| the candidate `fgp_data/inventory/fgp_transcriptions.candidate.db` (gitignored, 2026-10-26 evening) | rows |
|---|---|
| today's rows carried under their ids | 44,434 of 45,034 |
| of which with a cleaner text (the Word file or the per-line position text instead of the PDF) | 27,677 |
| today's rows dropped | 600 |
| new transcriptions added | 18,549 |
| new rows held, with a reason each | 6,730 |
| total | 62,983 |

The 600 drops: 391 XML rows gate 3 could not verify by their own record, 97 gate-2 holds (17 the explicit
"do not publish until read against the source" comment, 23 `tmp_DoubleNotToDisplay`, 19 absent files, 26 wrong
XML region, 5 another edition, 7 unconfirmed associations), 56 short stubs beside a fuller text of the same image,
50 retired duplicates, 3 without text, 3 test files.

The 6,730 holds: 3,858 whole-document rows on manuscripts another unit already serves per image (3,811 Nuscha —
the chooser keeps one row per image and would drop them), 1,153 Word files whose rendering differs from FGP's own
PDF in a way the classes do not explain, 1,138 folder-B rows whose image the NLI gallery does not list under the
row's manuscript, 322 without an AlmaId, 168 blank files, 28 real tables (several columns), 26 unknown images, 21
test or dummy files, 13 legacy-font texts, 3 shadowing cases.

Every number above is a declared expectation reproduced by two runs with one digest (gate 3 `0088ba547d3dfc83`,
gate 4 `058a0c2690d68b4f`); the budgets (latency, memory, size) are far under their limits. The chooser's checks
drive the real `FgpService`, `filter_sources_for_page` / `_select_fgp_sources_for_page` and
`choose_default_source` over both databases.

**Rulings taken (all 2026-10-26, all applied and committed):** generic credit where FIST names no one; the `plain`
new-rows policy ships; the 10 text-less rows take their Word text (the first approvals in
`scripts/fgp_gate3_approvals.json`); stubs of same-image pairs retired, pairs of two readings both kept; the 1,138
gallery-mismatch rows held; the FIST comment hold narrowed to the 17 explicit do-not-publish rows (the 150 "not
checked against the source" rows are a quality note and stay published); legacy-font Word texts held; test files
held; the Word-file rulings of [`FGP_DOCX_STRUCTURE_RULES.md`](FGP_DOCX_STRUCTURE_RULES.md) §5 — superscript
footnote markers, real tables held, every new Word row checked against FGP's PDF, everything in one run and **one
release for web and desktop together**, the Nuscha hidden layer kept.

**Commits on `master-main`** (fast-forward, no PRs, all pushed except the last): `c38a7e79` gate 4 · `5f6160a1`
drafts study · `acd26177` comment and test holds · `99c90650` legacy fonts · `adbccb24` the owner's rulings ·
`02341c77` gate 5 (the positions session) · `a50230a6` the comment hold narrowed · `83e65fee` the Word-file census ·
`8ba4111c` the Word-file rulings implemented (pushed with this handoff `766fbfa7`; checked 2026-10-26 18:25).

## 2. What exists

| piece | where | notes |
|---|---|---|
| FIST file store | `C:\FGPTranscriptions_NoPOSInfo\<SourceId(3)+SubId(4)>\{pdf,word,word_old}\` | 54,528 transcriptions, 3,912 translations, 10,890 free descriptions (never transcriptions); the 14-digit id is not unique; basename = FIST `FileName` |
| positions folders | `C:\FGPTranscriptions_WithPOSInfoAndImages\` | B1 (`N.xml`, `N.js`, 4,171 dirs) and B2 (`C<n>\data.js`, 19,314 dirs); FIST `ImageTranscriptionFolder` names the dir |
| FIST | `fist_data/FIST.db` | `dbo_Unit{Transcription,Translation,FreeDescription}`, `dbo_Signature.SourceId` = `dbo_CodeSource.TeamCode`, AlmaId via `dbo_InventoryAlma` |
| gate 1 inventory | `fgp_data/inventory/inventory.db` | `scripts/run_fgp_association_inventory.ps1` |
| gate 2 outcomes | `fgp_data/inventory/gate2.db` | `scripts/run_fgp_gate2_rules.ps1`; one run; 7 checks + 36 expectations |
| gate 3 texts | `fgp_data/inventory/gate3.db` (+ `gate3_run1/`) | `scripts/run_fgp_gate3_text_pipeline.ps1`; two runs; 9 + 55; **about 15 minutes a run with the PDF cache `fgp_data/inventory/pdf_text_cache.db`, 28 cold** |
| gate 4 candidate | `fgp_data/inventory/fgp_transcriptions.candidate.db`, `gate4.db` (+ `gate4_run1/`) | `scripts/run_fgp_gate4_chooser_validation.ps1`; two runs; 15 + 60; about 2 minutes a run; never written over today's `fgp_data/fgp_transcriptions.db` |
| gate 5 positions | `fgp_data/inventory/gate5*.db` and the positions store | `scripts/run_fgp_gate5_positions.ps1`; 68 minutes a run; verdict FAIL on G1 (17 of 20 images) by findings, not tuning |
| Word-file census | `fgp_data/inventory/docx_structure_census.json` | `scripts/fgp_docx_structure_census.py` |
| Nuscha hidden layer | `fgp_data/nuscha_hidden_layer.db` (155 MB) | `scripts/fgp_nuscha_hidden_layer.py`; 38,790 files, 796,854 lines; nothing reads it yet |
| approvals | `scripts/fgp_gate3_approvals.json`, `scripts/fgp_gate4_approvals.json` (absent) | keyed on the durable key `<unit_table>:<unit_id>\|<sys_id>\|<c_number>` and both text hashes |
| expectations | `scripts/fgp_gate{2,3,4,5}_*.expect.json` | re-declared from a run only on purpose, with the rules document; count the keys programmatically |
| tests | `tests/test_fgp_{association_inventory,gate2_rules,gate3_text_pipeline,gate4_chooser_validation,gate5_positions}.py` over `tests/fgp_fixture_world.py` | 101 tests, about 10 minutes together (the gate-5 file is the slow one) |
| open issues | GS-268 (text loss), GS-269 (XML by image), GS-270 (credit heuristic), GS-272 (FIST codes), GS-273 (reading-desk save key) | collected into the tracker 2026-10-26 evening |

How to rerun, in order, from the repository root in PowerShell: the four wrappers above. Gate 4 reads gate 3's
output, gate 3 reads gate 2's, gate 2 reads gate 1's; a change in any gate's rules reruns everything downstream.
Each wrapper runs twice and compares digests; a single run is for development (`--allow-single-run`). After a
change of rules: run once without expectations, read `*_summary.json`, re-declare the expectations in the
`.expect.json` with the rules document, run the wrapper. Gate 3's first pass after any change to the PDF cache's
inputs takes 28 minutes; later passes about 15. The reference set of gate 4 is a seeded draw that changes with the
candidate's row set (about 21 of 88 manuscripts swap each time the candidate changes), so default-change counts
move between runs for that reason alone.

## 3. Open decisions and questions

For the owner:

1. **Deploy.** The candidate is built and checked; nothing on the site changes until it is copied to the server. The
   ruling is one release, web and desktop together (§4 step A).
2. **The chooser's one-row-per-image rule** (`shared/fgp_service.py::dedupe_fgp_sources`) hides a second edition of
   the same image today (95 pairs with different texts, of which the 38 pairs of two readings are both kept in the
   candidate but only one shows) and keeps 3,858 whole-document rows out of the candidate. Whether the chooser should
   list several editions per image is a product decision (§4 step B).
3. **The 1,153 Word rows held against their PDF.** Mostly the PDF's own faults the classes do not yet name (a final
   letter printed as a digit, a line the PDF has and the Word file lacks); by a new declared class, by the owner's
   eye, or left held (§4 step C).
4. **Positions** (gate 5, [`FGP_POSITIONS_RULES.md`](FGP_POSITIONS_RULES.md) §7 "Open for the owner", six items):
   whether the pass bar stands as declared, how to clear 19,591 unchecked images, whether an overlay needs a
   confirmed page, the NLI 503s, the 46 `hash_mismatch` folders, the Strasbourg offset (§4 step D).
5. **The FGP codes nobody will ask FGP about**: `RecordStatus` 1 / 5 (on 6,586 delta units, one of today's),
   `PermissionCommentCode` 5309 ("being worked on by a team", 156 of today's units), `tmp_DoubleNotToDisplay` (23 rows
   held). The agreed way to guess: look up twenty units carrying each code on FGP's own public site; if FGP shows the
   transcription, we show it (§4 step E).
6. **Team 850's non-manuscript documents** (a contact list, a recipe, software notes — longer texts no rule catches)
   and **team 107's excerpt rows** (`העתקות:` Firkovitch excerpts, 391 short rows): editions or not.

## 4. Recommended plan, in order

**A. The release (web and desktop, one release).** Effort: a day plus the installer build.
1. Web: copy the candidate to the server as `fgp_data/fgp_transcriptions.db` **before** the code (the deploy rule:
   sidecars by `scp` first, then `git pull`; the auto-mode classifier blocks `scp` from a Claude session, so the owner
   runs it). The app code needs no change for the candidate's schema (the gates drove the real service over it; the
   extra columns `origin`, `text_provenance`, `fist_unit_*`, `durable_key` and `fgp_meta.version` 2.0.0 are
   harmless), but verify `FgpService.get_version()` / any version check in the web and desktop code before copying.
   Keep today's file beside it on the server for a rollback. Smoke-check a reading-desk page, a search hit with an
   FGP source and the `/api` transcription endpoint.
2. Desktop. **Corrected by the next session (2026-10-26 evening): `fgp_transcriptions.db` IS bundled in the
   installer** (`GenizahSearchPro.spec` datas, `('fgp_data\\fgp_transcriptions.db', 'fgp_data')`, since v8.2.0
   `3dc62d5d`), and `build_app.bat` builds from that spec, reading the file at the project root. So the desktop half
   of the release is: put the candidate at `fgp_data/fgp_transcriptions.db` (today's file kept beside it for a
   rollback — this writes over the file the gates never touch, so it waits for the owner's go-ahead) and build the
   installer. The sidecar updater is **not** a route: its GitHub release `data-latest` does not exist (`gh release
   view data-latest` → "release not found"; the tag 404s), so `SidecarUpdateThread` has never offered anything, and
   `FgpService.get_version()` is read only by that updater (today's file has no `version` key; the candidate's is
   2.0.0). Only `FgpService` opens the file (grep of `shared/`, `web/`, `desktop/`). The candidate is 347 MB
   (346,939,392 bytes), today's 405 MB. Its `fgp_meta` still carries today's build keys (`sysid_resolved` 45006 /
   45034, `empty_text` 13, `source` D:\…, `extractor` PyMuPDF) beside the new ones; nothing reads them, but they
   misdescribe the file.
3. The changelog fragment (`changelog.d/`) and the release notes should say what users see: about 18,500 new
   transcriptions, cleaner text on about 27,700, about 600 removed and why (`feedback_release_texts_gentle_on_past_gaps`).
4. Before any of it: push `8ba4111c`; run `python scripts/check_docs.py` and `ruff check .`; collect the three
   open-issue fragments into the tracker.

**B. The chooser: several editions per image.** Effort: two to three days, both apps. The biggest remaining gain
(3,858 held rows, the 95 hidden pairs today) and a defect in its own right. Design: `dedupe_fgp_sources` keeps one row
per (image, relation, language) — change the key to include the FIST unit (or the credit), order the group with the
fullest text first so `choose_default_source` keeps today's defaults, and let the version chooser list the rest. Then
drop gate 4's `whole_doc_shadowed` / `image_already_served` / `shadows_whole_doc_row` holds (they encode today's
dedupe) and rerun gate 4; its C5 check will show every default change the new rule causes, each needing a
disposition. The positional save key of the web reading desk (open-issue fragment 2026-10-26) must be fixed in the
same change, since more rows per page make the position less stable.

**C. The 1,153 PDF-held Word rows.** Effort: a day. Sample fifty held blocks by team (the Books team is 699 rows);
name the recurring PDF faults as classes in gate 3 (§2 of its rules: a candidate is "a final letter printed as an ASCII
digit", `0` for `ף`), declare them, rerun gate 3 and 4; what is left goes to the owner by eye through
`scripts/fgp_gate3_approvals.json` (the approval form exists) or stays held. Not before step A — these rows are a
small share and the release should not wait.

**D. Positions.** Effort: a week or more, in stages, after the owner's rulings on gate 5's six items. My
recommendation stands: keep the pass bar; register only the cheap half now (FGP's photo and NLI's the same size: 10
of 20 in the sample, a light landmark check); the rest on first view of a page, cached; an overlay only on a
confirmed page; the first visible feature is the search hit highlighted on the photograph. The Nuscha hidden layer
(a 12-digit image/line code per line) may give a second route to positions for the Nuscha files; nothing has been
tried.

**E. The guesses and the small items.** Effort: hours each. The FGP-site check for the three codes (§3.5) and a
gate-2 hold for whichever means "not for publication"; a ruling on team 850 and team 107 (§3.6), implemented as
gate-4 holds like `test_or_dummy`; the bullet rendering of 16 Word files by eye; the 1,138 gallery-mismatch rows
(folder-B rows whose image the gallery lists under another manuscript — the positions geometry work may explain
them, so leave them until D).

**What I would not do:** release in tranches (ruled out), rebuild today's sidecar from the PDFs again (the Word
route is cleaner on every measured class), or deploy the candidate to the web alone and leave the desktop on the old
file for long — the two apps would show different editions of the same manuscript.

## 5. Risks and lessons for the next session

- **Never write over `fgp_data/fgp_transcriptions.db`** (gate 4 refuses to overwrite anything that is not a
  candidate; keep it that way). Open every SQLite database read-only (`mode=ro`); a bare `sqlite3.connect` on a
  missing path makes a 0-byte stub.
- **Rerun, do not remember.** Every number in the documents came from a run; the next session should rerun the
  wrappers rather than trust the figures here, and count expectation keys programmatically (they were miscounted four
  times by hand).
- **A rules change must prove it can fail** (`feedback_gates_must_be_proven_able_to_fail`): each new hold or class
  got a fixture case; the gate-5 geometry check failed for real and that was its value.
- **Two traps that cost time today**: a global superscript-to-digit fold broke 306 verified XML rows because the
  XML editions use `²` as a siglum (the fold now runs only when today's side has no superscript); a verify-only
  agreement is a *check* for a new row, which has no text to replace. And one accident: a patch script that opened
  the gate-3 file for writing before a bad argument raised truncated it to zero bytes — restored from git; write to a
  temporary and rename, or edit with the Edit tool.
- **Windows and tooling**: Bash heredocs mangle Hebrew (write patch scripts with the Write tool and run them);
  `with sqlite3.connect(...)` does not close the file; `ruff` parses any path you pass it, `.ps1` included; wrapper
  logs through `Tee-Object` are UTF-16; background Bash jobs do survive past ten minutes (gate 3's 28-minute run did).
- **The reference draw moves**: gate 4's seeded reference set changes with the candidate, so a default-change count
  that moves is not by itself a regression; C5 dispositions every change and fails on an unexplained one.
- **Sessions sharing this folder**: the gate-5 session committed from the same checkout while this one worked; before
  staging, check `git status` for another session's uncommitted edits and never `git add -A`.

## 6. Pointers

Memory: `project_fgp_source_folders_assessment` (the day's state, rulings and lessons), `project_fgp_gate5_positions`,
`project_fgp_transcriptions_sidecar`, `reference_desktop_data_provisioning`, `feedback_deploy_db_sync`,
`reference_server_operations`. Documents: this file, the assessment and the five rules documents above, the plans
index [`PLANS_INDEX.md`](PLANS_INDEX.md). Logs of the day's runs are in `_tmp/fgp_*.log`.
