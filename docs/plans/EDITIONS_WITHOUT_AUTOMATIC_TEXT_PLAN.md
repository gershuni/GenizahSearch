# Editions on pages without an automatic transcription: show them, and edit them

Status: **decided and built for the web, 2026-10-27** (see "The owner's decisions" at the end). Owner, 2026-10-27:
"P2 will need fix, also for users that will want to edit an edition."

## What happens today (read from the code)

| Where | Today |
|---|---|
| Web reading view, `web/pages/browse.py:5225` | The version selector -- the only way to choose an FGP or PGP edition -- is built only `if page.text:` (the automatic transcription). A page without one says "No text available" and offers nothing, even when the manuscript has an FGP edition (T-S NS 243.146 p. 1, row 22952). |
| Web toolbar, `browse.py:4616` | Edit, Comment, Notes and Joins are built under the same `if page.text:`. |
| Web Edit, `browse.py:1987` (`toggle_edit_mode`) | Starts from `state.current_page.text` (the automatic text) or the user's own latest correction, never from the edition on screen; `original_text` is always the automatic text. Saved through `web/components/text_editor.py:384` -> `web/supabase_client.py::create_correction`. |
| Desktop Edit, `genizah_app.py:3692` | Starts from the text on screen (`browse_text.toPlainText()`), so an edition can already be edited -- but nothing records that it was an edition. The version list is built whenever there are sources (`genizah_app.py:8957`), so the desktop probably shows editions on such pages (not run to confirm). |
| Supabase `corrections` (`docs/guides/SUPABASE_GUIDE.md`) | No column says which text a correction was made on; `original_text` holds that text. The privileged-column guard (`migrations/add_privileged_column_guards.sql`) watches `status`, `reviewed_by`, `reviewed_at`, `author_id` only, so a new author-set column is not affected. |
| Approved corrections in the web menu, `web/components/version_selector.py::fetch_document_corrections` | Listed by author for the page; nothing says what they correct. |

Not measured yet: how many pages have an edition and no automatic text (step 1 measures it from the index).

## Proposed changes

1. **Measure** the pages with an FGP or PGP edition for the page and no automatic text (web route, from the index
   and the sidecars), so the fix and its release note carry a number.
2. **Web: show the editions.** Build the version selector whenever the page has an FGP or PGP source (or an
   approved correction), not only when it has automatic text; with no automatic text the default is the edition
   (`choose_default_source` already falls toward FGP when the automatic text is too short to compare). "No text
   available" stays for pages with nothing at all. The same gate lifts for Comment, Notes and Joins (decision 4).
3. **Edit from the text on screen (both apps).** Web Edit starts from the displayed text (`enrichment_refs
   ['current_version_text']` and its `version_info`), as the desktop already does; `original_text` is that text.
4. **Record what was edited.** New column `corrections.base_source text null`: `null` = the automatic text (every
   existing row), `fgp:<row id>`, `pgp:<edition id>`, `correction:<id>`. Migration with its GRANT check (the table's
   grants cover a new column; RLS and the guard unchanged); applied by the owner BEFORE the code that writes it
   (PostgREST rejects an unknown column). `SUPABASE_GUIDE.md` updated.
5. **Show it.** An approved correction of an edition appears in the version menu as a correction of that edition
   ("correction of FGP: <credit>"), in both apps and on the corrections review page; whether it becomes the page's
   default is decision 3.
6. **Tests** at the call sites: the selector on a page without automatic text, Edit's base on both apps, the
   stored `base_source`, the menu label; a render-smoke run for the browse page.

## Decisions for you

1. **Default on a page without automatic text:** the edition (recommended), or nothing until the reader chooses?
2. **What Edit starts from:** the text on screen, recorded as the base (recommended; the desktop already does it), or
   always the automatic text (today's web)?
3. **An approved correction of an edition:** listed as its own version while the edition stays the default
   (recommended: the edition is a scholar's published text), or it becomes the default as a correction of the
   automatic text does?
4. **Comment, Notes, Joins** on a page without automatic text: show them too (recommended), or only the editions?
5. **Timing:** steps 1-2 (show the editions; web only, no schema change) with v9.11.0, so the re-read texts on
   such pages can be reached (recommended), and steps 3-6 (editing, schema) after it; or all after the release?

## The owner's decisions (2026-10-27) and what was built

1. **"If no text, show empty text instead of HTR."** A page without an automatic transcription takes its automatic
   text as empty, so the version menu is built and the usual default rule chooses -- an edition when there is one.
   Built: `web/pages/browse_state.py::page_has_reading_view` gates the toolbar (`browse.py`), the version row and
   the version selector after enrichment (`browse_enrichment.py`); the selector gets `page.text or ''`; the citation
   chip cites an edition (or the reader's approved version) shown on such a page. A synthetic catalogue-only record
   is unchanged.
2. **"Keep it simplest"**: the web's Edit keeps starting from the automatic text -- empty on such a page. The
   question of editing an edition is logged as GS-277 (backlog).
3. **Recorded as the user's version**, as every correction today: no schema change, no `base_source` column.
4. **Comment, Notes and Joins** show on such pages too.
5. **Now**, with v9.11.0. Steps 3-5 of the proposal above (edit from the text on screen, `base_source`, labels) are
   not built; they wait on GS-277. Step 1's measurement was not run.

Verified on a local server with the rebuilt FGP candidate: T-S NS 243.146 page 1 opens on its FGP edition (row 22952)
with the version menu, Edit and Comment, and the citation credits the FGP edition; a page with an automatic text
(MS heb. e.77/6) is unchanged. Not checked live: Edit's save on such a page (needs a signed-in account).

## Quick View (2026-10-29)

Quick View (`web/pages/search_results.py::open_advanced_dialog`) had the same gate: its version chooser, Edit, Comment
and Joins were built only `if sys_id and current_text`. A reader reaches such a page there by paging from a hit on
another page of the manuscript, or from a shelfmark or title search for a manuscript with no automatic text at all
(the result opens on page 1). Unlike Browse, Quick View already showed the edition as the page's text -- it runs
`choose_default_source` itself -- but unlabelled, with no way to switch versions and none of those buttons. It now
takes Browse's decision (`_quick_view_has_reading_view` wraps `page_has_reading_view`; with no page fetched it keeps
the result's own text, as before), builds its text card on such a page so the chooser's default renders into it, and
says "No text available" when there is nothing -- the result's snippet now stands in for a missing text only on the
result's own page; paging to another page used to show the hit page's snippet there. Edit starts from the empty
automatic text, as in Browse (decision 2).
Quick View has no citation of its own: the "How to cite" chip (z-index 2000) sits beneath dialogs (6000), so
there is no Quick View citation to credit the edition. Tests:
`tests/render_smoke/test_quick_view_without_automatic_text_render_smoke.py` (both reach paths, and a catalogue-only
record) and `tests/test_browse_page_without_automatic_text.py`.
