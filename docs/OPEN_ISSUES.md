# GenizahSearch - Open Issues Tracker

> **Last Updated:** 2026-03-24 (PostHog-driven UX fixes: parallels rageclicks, login tracking, OAuth implicit flow, discoverability)
> **Status:** Active working document

---

## AI Assistant Maintenance Protocol

**IMPORTANT:** This document must be kept current. Follow these rules:

### When to Update This Document

1. **After fixing any issue** - Mark as completed with date
2. **After discovering new issues** - Add to appropriate section
3. **After completing a work session** - Review and update status
4. **When starting work** - Check current status first

### How to Update

```markdown
# When completing an issue:
| Issue | Status | Notes |
| Old text | ג Open | Description |
ג†“
| Old text | ג… Fixed (2026-02-03) | Description |

# When adding new issue:
| **NEW: Issue description** | ג Open | Details |

# When removing (only after verified in production):
Move to "Completed Issues" section at bottom with date
```

### Update Checklist (run after each session)

- [ ] Update "Last Updated" timestamp at top
- [ ] Mark any fixed issues with date
- [ ] Add any newly discovered issues
- [ ] Move verified-complete items to archive section
- [ ] Update summary counts if changed

---

## Quick Summary

| Category | Open | Fixed/Implemented | Total |
|----------|------|-------------------|-------|
| P1 Critical Bugs | 0 | 6 | 6 |
| P2 Medium Bugs | 10 | 34 | 44 |
| P3 Low Priority | 1 | 4 | 5 |
| Documentation Issues | 0 | 8 | 8 |
| Documentation Gaps | 0 | 4 | 4 |
| Code Quality Debt | 0 | 6 | 6 |
| Untested Areas | 6 | 1 | 7 |
| Implemented Plans | 0 | 5 | 5 |
| Archive Candidates | 0 | 4 | 4 |
| **Total** | **18** | **72** | **90** |

---

## 1. Outstanding Bugs

### P1 - Critical

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Puzzle process endpoint allows unauthenticated cache poisoning / arbitrary uploads** | `web/api.py`, `web/puzzle_tokens.py`, `shared/puzzle_image_service.py` | ✅ Fixed (2026-03-18) | Fixed via HMAC upload tokens: `GET /api/puzzle_image` returns a signed token on cache miss; `POST /api/puzzle_process` and `POST /api/puzzle_upload_derivative` require valid token (5-min expiry, fl_id-bound). Also added size limit (10MB), content-type validation (JPEG/PNG), and rate limiting (60/min/IP). |
| **Desktop Path Traversal** | `filter_text_dialog.py:16-23,58` | ג… Fixed (2026-02-03) | Already fixed - uses `_sanitize_cache_filename()` whitelist approach |

| **Desktop FJMS catalog HTML injection** | `genizah_app.py:6712-6716,6879-6883,6921-6932` | ג… Fixed (2026-03-10) | Added `html.escape()` to all three catalog toggle sections (RunningTitle, FreeDesc, FullText) |
| **Parallels metadata dialog NameError** | `web/pages/parallels.py:3387` | ג… Fixed (2026-03-10) | Changed `_md_show_trans` ג†’ `_par_show_trans` |
| **Search advanced header NameError** | `web/pages/search.py:4656` | ג… Fixed (2026-03-10) | Replaced `_adv_tt` with safe `_adv_tt_resolved` lookup |
| **Shared manuscript viewer script is deferred but used immediately by inline page scripts** | `web/pages/search.py`, `web/pages/browse.py`, `web/static/manuscript_viewer.js` | ✅ Fixed (2026-03-21) | Follow-up review confirmed `c2734168` removes `defer`, so the shared viewer factory now loads synchronously before the inline `createManuscriptViewer(...)` calls on both pages. |
### P2 - Medium

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Join finder v7/v8 is not app-ready: left-only vertical search, mixed scope duplicates, and 80-100s runtime** | `scripts/join_finder_v7.py`, `scripts/join_finder_v8.py`, `genizah_core.py` | ❌ Open | Research review on 2026-03-15 found 3 concrete gaps before manuscript-view integration: (1) v7/v8 only processes `]` torn lines, so it supports LEFT→RIGHT but not RIGHT→LEFT; (2) the scripts search mixed `page`/`system`/`part` scopes, producing duplicate candidates for the same manuscript; (3) Phase 3 fan-out over continuation words searched against `content` takes ~83-101s on the two benchmark cases. Recommended fix path: route by direction, restrict to/dedupe at `scope="system"`, use existing `line_starts` / `line_ends` / `L{n}:word` positional fields, and treat FIST-only visual hits as a separate bucket/fallback. See `docs/JOIN_FINDER_REPORT.md` and `docs/plans/JOIN_FINDER_IMPLEMENTATION_PLAN.md`. |
| **Shared filter recompute generation guard still allows stale results after clearing the last active filter** | `web/components/filter_panel.py` | ✅ Fixed (2026-03-21) | Follow-up review confirmed `c2734168` increments `_filter_recompute_gen` before the empty-filter early return, so older in-flight recomputes now discard their stale results instead of repopulating cleared filters. |
| **Shared filter option builders still read UI language inside `run.io_bound()` wrappers** | `web/pages/search.py`, `web/pages/parallels.py`, `web/components/filter_panel.py` | ✅ Fixed (2026-03-21) | Follow-up review confirmed both pages now capture `lang = get_language()` in client context and pass the shared `build_*_options` functions directly into `run.io_bound()`, removing the worker-thread language lookup. |
| **Fullscreen browse fallback now targets `fsEditViewer`, but the shared fallback handler assumes every viewer has `init()`** | `web/pages/browse.py`, `web/static/manuscript_viewer.js` | ✅ Fixed (2026-03-21) | Follow-up review confirmed `7dda767d` guards all three `window[viewerName].init()` calls with `typeof ... === 'function'`, so fallback success no longer throws for fullscreen `fsEditViewer` while main-page viewers still reinitialize normally. |
| **Browse enrichment worker cleanup no longer disconnects stale callbacks after generation-guard refactor** | `genizah_app.py:14691-14699` | ✅ Fixed (2026-03-20) | Follow-up review confirmed `_start_browse_enrichment()` now stores the lambda in `_browse_enrich_slot` and disconnects that exact callable before replacing the worker, so the generation-guard refactor once again removes old browse enrichment connections correctly. |
| **Parallels page extreme rageclicks (365/822 total, 44% of all site frustration)** | `web/pages/parallels.py` | ✅ Fixed (2026-03-24) | PostHog showed 365 rageclicks on /parallels out of 822 site-wide. Root causes: "Find Parallels" button had 50ms gap before disable (silent re-click suppression), shelfmark headings styled like links but not clickable, expansion rows had no visible chevron, filter dropdowns opened empty during async load, export buttons always active. Fixes: immediate button disable, clickable shelfmark links to /browse, expand_more icons, loading spinners on filter selects, export buttons disabled until results exist. |
| **Google OAuth PKCE code_verifier lost between redirect and callback** | `web/supabase_client.py` | ✅ Fixed (2026-03-24) | Supabase Python client defaulted to PKCE flow but stored code_verifier in singleton memory storage that was lost on client reset or multi-user race. Switched to implicit flow (`ClientOptions(flow_type='implicit')`) — callback Method 2 (hash token parsing) handles this correctly. Also added PostHog tracking for OAuth login_success/login_failed on the callback page (with POSTHOG_SCRIPT injection). |
| **Login PostHog tracking gaps: 67% failure rate appeared inflated** | `web/auth_state.py`, `web/supabase_client.py`, `web/main.py` | ✅ Fixed (2026-03-24) | Multiple tracking gaps: (1) "no user returned" path never fired login_failed, (2) login_success fired right before page reload (JS injection race), (3) Google OAuth completely untracked, (4) login_failed lacked error_code/status_code, (5) sign_in blocked event loop. Fixes: added missing login_failed event, 300ms delay before reload for PostHog flush, OAuth callback tracking, enriched error properties, wrapped sign_in in run.io_bound(). |
| **Login-gated write actions used silent disappearing toasts** | `web/pages/discoveries.py`, `web/pages/puzzle.py` | ✅ Fixed (2026-03-24) | Anonymous users clicking "New Discovery", vote, share, or Puzzle "Publish" got a 3-second toast with no actionable path. Replaced all 5 instances with `create_login_dialog().open()` — opens the actual login/register dialog. |
| **_posthog_identify JS injection on names with apostrophes** | `web/auth_state.py` | ✅ Fixed (2026-03-24) | Pre-existing: f-string interpolation of uid/email/name broke for names like O'Brien. Switched to json.dumps() for all interpolated values, matching the safe pattern in posthog_capture(). |
| **Dev mode zombie processes block port on Windows** | `web/main.py` | ✅ Fixed (2026-03-24) | Added _find_free_port() that auto-finds next available port (8081→8090) in dev mode (reload=true). Production mode uses configured port strictly. |
| **Web auth can get stuck retrying an already-used refresh token** | `web/supabase_client.py`, `web/auth_state.py` | ❌ Open | `get_user_client()` logs and falls back to the anonymous client when Supabase returns `Invalid Refresh Token: Already Used`, but it leaves `auth_session` and cached auth UI state in NiceGUI storage. Subsequent requests keep retrying the same dead token and spam logs while the UI may still look signed in. Fix path: treat this as terminal auth failure, clear stored auth state, and prompt re-login. |
| **NLI manifest failures are retried immediately and can hammer iiif.nli.org.il during thumbnail bursts** | `web/api.py` | ❌ Open | `fetch_fl_ids_from_nli()` does blocking 15s manifest fetches with success-only caching. When NLI is slow or rate-limits, the same `sys_id`s are retried on every request with no failure TTL/backoff and no concurrency cap, producing synchronized timeout bursts in production logs. Fix path: add a short negative cache/cooldown and optionally a small concurrency limit plus connection reuse. |
| **Search logs expose raw queries and regex internals at INFO in production** | `genizah_core.py` | ❌ Open | `search_text_tantivy()` and the line-break search path emit `[DEBUG]` messages via `LOGGER.info`, including raw user query text, generated Tantivy query strings, regex patterns, and hit counts. This bloats production logs and captures user searches unnecessarily. Fix path: gate these logs behind a debug flag or DEBUG level and default them off in production. |
| **Web puzzle image acquisition solved via browser extension; staged rollout in progress** | `extension/`, `web/pages/puzzle.py`, `web/api.py`, `web/puzzle_tokens.py` | ✅ Fixed (2026-03-18) | Browser extension ("GenizahSearch Image Helper") fetches NLI images via user's own IP, sends to server for bg removal + disk caching. Unified `_loadImageWithFallbacks()` fallback chain: server cache → extension → localhost helper → direct NLI. Extension submitted to Chrome Web Store. `WEB_PUZZLE_ENABLED=true` set on production for staged testing. Feature flag kept for rollback. |
| **Desktop EXE puzzle image loading fails with `No module named numpy`** | `build_app.bat`, `GenizahSearchPro.spec` | ✅ Fixed (2026-03-17) | The desktop build explicitly added `numpy` as a hidden import for puzzle background removal, but then also excluded `numpy` from the PyInstaller bundle. Running from source worked because the venv had NumPy installed; the packaged EXE failed only when puzzle image processing first imported `shared.background_removal`. Fixed by removing the contradictory `numpy` exclusion from both build entry points. |
| **Puzzle folio navigation still fails on production after initial client-side fallback** | `web/pages/puzzle.py:717-806`, `web/pages/puzzle.py:2685-2741` | ✅ Fixed (2026-03-17) | `navigateFolio()` now shares the same localhost-helper fallback chain as initial add and reload. Production was verified live: when `/api/puzzle_image` fails due to blocked server-side IIIF fetch, the browser falls back to the user's local helper and preserves correct `fl_id`/`folio_label` updates instead of saving mismatched metadata. |
| **Desktop rebuild index can fail with WinError 5 on Tantivy `.fast` files** | `genizah_core.py`, `genizah_app.py`, `gui_threads.py` | ✅ Fixed (2026-03-17) | Added `SearchEngine.close_index()` to release Tantivy index/searcher + gc.collect() before rebuild. Called in `run_indexing()` before `IndexerThread` starts. `reload_index()` reopens after rebuild completes. |
| **Puzzle state persistence/sync is incomplete across reloads and delete cleanup** | `web/pages/puzzle.py`, `shared/session_persistence.py`, `genizah_app.py` | ✅ Fixed (2026-03-17) | Verified that web now persists/restores `puzzle_doc_id`, refreshes the fragment selector on `puzzle-fragment-meta`, and clears `app.storage.tab['puzzle_doc_id']` when deleting the active local document. Targeted tests for the puzzle publish/service stack passed. Desktop restart persistence remains a separate UAT question, but the specific web reload/delete issue is fixed. |
| **Publishing can create broken community joins when export/render fails** | `shared/puzzle_publish_service.py`, `shared/puzzle_export.py`, `shared/puzzle_image_service.py` | ✅ Fixed (2026-03-17) | `publish_join()` now fails fast with `ValueError` when `compose_puzzle_export()` returns `None`, before any storage upload or Supabase upsert. Test coverage was added in `tests/test_puzzle_publish.py::test_publish_join_fails_on_null_composite`. |
| **Puzzle browsing help/parity drift across web and desktop** | `web/pages/discoveries.py`, `web/pages/help.py`, `corrections_ui.py`, `genizah_app.py`, `Help.html` | ✅ Fixed (2026-03-17) | No code change was needed: `web/pages/help.py` is the web help surface and now matches the web feed/filter model, while `Help.html` is desktop-only and correctly documents the desktop `All Puzzles` / `My Puzzles` tabs. The earlier review issue came from treating the two help surfaces as if they were shared. |
| **v7 puzzle/community rollout still has remaining Hebrew translation gaps** | `web/pages/discoveries.py`, `genizah_app.py`, `corrections_ui.py`, `genizah_translations.py` | ✅ Fixed (2026-03-17) | Verified the previously missing active puzzle/community strings now exist in `genizah_translations.py`, including the desktop publish-confirm prompt with embedded newlines, `(no original text)`, `View all joins...`, `No joined fragments found.`, and `Could not resolve fragment identifiers.` |
| **Manchester LUNA recto/verso shows same image for both sides** | `genizah_core.py`, `shared/nli_crossref_service.py` | ✅ Fixed (2026-03-18) | Each Manchester page (recto/verso) has a separate luna_id with its own 1-canvas IIIF manifest, but code only fetched the first image's luna_id. Added `get_manchester_canvases()` which resolves ALL crossref images to individual canvas entries with distinct IIIF URLs. Example: Ms. B 2091 (sys_id 990002081410205171) now correctly shows 2 pages. |
| **Mosseri collection missing CUDL high-res images — only NLI Rosetta thumbnails shown** | `genizah_core.py`, `shared/nli_crossref_service.py` | ✅ Fixed (2026-03-18) | Added `construct_mosseri_cudl_label()` that converts Mosseri shelfmark variants to CUDL labels (MS-MOSSERI-{SERIES}-{NUM}), wired as fallback in `enrich_metadata` after crossref normalized-shelfmark lookup fails. Iterates all call_number variants per record (stored in csv_bank for Mosseri). Covers 98.3% of 3,194 Mosseri records (3,141/3,194). |
| **Desktop puzzle auto-fit makes new fragments look like tiny thumbnails and resets expected zoom** | `genizah_app.py:3925-3949`, `genizah_app.py:3129-3148` | ✅ Fixed (2026-03-18) | `_fit_all_fragments()` called `fitInView()` after every newly loaded fragment, zooming the whole `QGraphicsView` out as the scene widened. Fix: auto-fit now only runs when a saved document finishes loading all fragments (last fragment triggers fit). Individual fragment adds use `ensureVisible()` instead. User can still Ctrl+0 to fit all manually. |
| **Home-page hero search did not navigate (respond=False unsupported)** | `web/pages/home.py` | ✅ Fixed (2026-03-19) | Reverted to `ui.navigate.to()`. The deferred task slot context bug was the real root cause — fixed by capturing `ui.context.client` at page creation and entering it in `_after_delay`. |
| **Search accordion thumbnails did not load** | `web/pages/search.py`, `web/api.py` | ✅ Fixed (2026-03-19) | Removed direct NLI IIIF URL approach. Now uses same `/api/nli_image_by_sysid/{sys_id}?page={idx}&width=300` server proxy pattern as Advanced View, with `advHandleImageError` JS fallback. Added `width` param to API for 300px thumbnails. |
| **Browse rotation slider TypeError: JS string passed as Python handler** | `web/pages/browse.py:4289-4292` | ✅ Fixed (2026-03-19) | `.on('update:model-value', 'if(window.manuscriptViewer)...')` passed a JS string where NiceGUI expects a callable. Removed — the Python `on_change=handle_rotation_slider` already handles rotation updates. |
| **Puzzle background removal still misses some CUL blue conservation mats** | `shared/background_removal.py`, `shared/puzzle_image_service.py` | ❌ Open | Current algorithm learns the background color from the four image corners. That works for most scans, but some Cambridge images have parchment touching the corners while the bright blue conservation mat dominates the center/background. In those cases the detector learns the parchment instead of the mat, so removal keeps the blue and can even attack the fragment. Likely fix path is smarter background sampling or multiple candidate backgrounds rather than more threshold tuning. |
| **FIST bibliography re-export is blocked by a placeholder-count mismatch in `export_bibliography()`** | `scripts/export_fist_enrichment.py` | ✅ Fixed (2026-03-22) | Follow-up review confirmed `export_bibliography()` now defines 22 bibliography columns, selects 22 values, and uses `n_cols = 22`, so the planned bibliography sidecar re-export is no longer blocked by the earlier insert mismatch. |
| **Auto-detected blue-mat union can attack parchment when the corner sample learns fragment pixels** | `shared/background_removal.py` | ✅ Fixed (2026-03-22) | Re-review confirmed the auto-detect branch now uses the same blue-only mask as explicit `is_cul=True` and no longer unions in the corner-sampled normal mask, so the specific parchment-hole regression path is closed. |
| **Puzzle external fragments persist wrong restore metadata in `initial_add` auto-add flow** | `web/pages/puzzle.py` | ✅ Fixed (2026-03-21) | Final re-review confirmed the `initial_add` external branch now computes `ext_processed`, `ext_threshold`, and `ext_is_cul` the same way as the regular add path and persists the effective values into pending/session metadata, so Oxford/Cambridge external fragments restore consistently across all three entry paths. |
| **Puzzle session restore can keep skipped fragments as ghost metadata** | `web/pages/puzzle.py` | ✅ Fixed (2026-03-21) | Re-review confirmed `init_canvas()` now prunes unrestorable entries from `puzzle_meta` and writes the filtered state back to `app.storage.tab`, so save/export stays aligned with the actual canvas contents after restore. |
| **Web puzzle canvas v49 is not app-ready: fragment-meta race, dead threshold slider, delete not persisted, and processed-image response can advertise the wrong MIME type** | `web/pages/puzzle.py`, `web/api.py`, `shared/puzzle_image_service.py` | ✅ Fixed (2026-03-16) | Fixed in multiple passes on 2026-03-16. The final root cause for “images are not loading” was that `ui.html('<canvas id="puzzleCanvas"></canvas>')` rendered as an empty `<div>` in NiceGUI, so Fabric never found a canvas element and every add was queued forever. Replaced it with a real `ui.element('canvas').props('id=puzzleCanvas')`, then verified in a live headless browser that Fabric initializes, `/api/puzzle_image` loads, and the fragment is present on the canvas. Earlier same-day fixes also added JS→Python add-result/meta events, persisted delete handling, selection sync, processed/original toggle reloads, threshold reprocessing wiring, and MIME-safe API responses. |
| **Puzzle export mismatched web positions, reprocessed processed images, and froze the desktop UI** | `web/pages/puzzle.py`, `shared/puzzle_export.py`, `genizah_app.py` | ✅ Fixed (2026-03-17) | Fixed 3 Phase-50 export bugs together. Web save/export now serializes Fabric objects from `getCenterPoint()` plus unscaled cropped size and restores persisted docs via center placement instead of brittle `left/top` math, bringing it in line with the desktop/QGraphics model. Shared export now reuses the same 800px processed image the canvas shows for bg-removed fragments, so exported output matches the on-canvas appearance instead of re-running background removal at 3000px. Desktop export now runs in `PuzzleExportThread` with cancelable progress UI and resolution choices (draft 1000px / standard 2000px / full 3000px) instead of blocking the main thread for ~20s. Follow-up same-day web fixes also corrected `build_fragments_list()` regressions, center-preserving folio swaps/reloads, session restore viewport fitting, and duplicate fragment creation during `Flip Puzzle`. |
| **Translation batch script rewires stdio on import and breaks pytest capture** | scripts/translate_pgp_descriptions.py, scripts/translate_libraries_titles.py | ג… Fixed (2026-03-11) | Moved UTF-8 stdio setup from import-time into `_configure_utf8_stdio()` called only from `if __name__ == "__main__":`. Uses `reconfigure()` when available. Fixed in both translation scripts. |
| **FJMS export drops ~38K catalog records (MAX(Version) filter)** | scripts/export_fist_enrichment.py | ✅ Fixed (2026-03-11) | MAX(Version) join on dbo_Signature drops child records when latest version lacks data but earlier versions have it. Affects 6 functions (catalog, running_titles, sizes, fields, textual_frames, mentions). 37,962 catalog recs lost (9.2%), 33,410 AlmaIds affected. 3 other functions (free_desc, full_texts, bibliography) already fixed. Fix: remove MAX(Version) from 6 functions. UnitCatalogRecId never on multiple versions — verified 0 for all 6 child tables (UnitCatalogRec, CatalogMultiRunningTitle, CatalogMultiSize, CatalogMultiField, CatalogMultiMention). See `docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md`. |
| **FJMS catalog translation toggle shows wrong language by default (desktop + web RT)** | genizah_app.py:6784-7000, web/components/catalog_dialog.py:274-286, shared/translation_service.py:392-420 | ✅ Fixed (2026-03-11) | Translation directions are mixed: RunningTitle/FullText are en2he, FreeDesc is he2en. Service layer drops direction column. Desktop: 3 confirmed wrong-default toggle sections (RunningTitle en2he, FreeDesc he2en, FullText en2he). Web: RunningTitle replacement confirmed broken (replaces EN with HE in EN UI); FreeDesc works by coincidence; FullText has no translation logic. Fix: (1) return direction from translation_service, (2) make desktop renderer + web RT replacement direction-aware. See `docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md`. |
| **PGP verso-only transcription shown on recto page** | shared/document_service.py, web/pages/browse.py, genizah_app.py | ✅ Fixed (2026-03-13) | 3 bugs: (1) get_section_for_page uses fragment_page_info to suppress unmarked transcriptions on wrong page, (2) desktop NLI image nav used images_ext instead of active_list, (3) desktop _browse_refresh_pgp_for_page bailed on empty sources list. Affects ENA and other NLI-only manuscripts with page-specific PGP links. |
| **Fragment joins broken — CHECK constraint, no attribution, no notes** | supabase_corrections_client.py, corrections_ui.py, web/supabase_client.py, web/components/joins_panel.py, supabase_setup.sql | ✅ Fixed (2026-03-13) | 5 bugs: (1) UI sent `physical_join`/`same_composition` but DB CHECK only allows `physical`/`content`/`uncertain`, (2) creator name blank — SELECT didn't resolve profiles, INSERT didn't fill from current user, (3) notes column missing from desktop detail table, (4) RLS policy hid proposed joins from other users, (5) community stats "User Joins" always 0 — query was missing. Also: display label maps updated in 8 files to handle both UI and DB join type keys. |
| **Web first-load drawer appears on wrong side before settling after reload/navigation** | `web/main.py` | ✅ Fixed (2026-03-15) | Root cause was bootstrap drift: `apply_theme_immediately()` could use stale/default language state and only tried Quasar RTL activation once. On cold load the first paint could render the drawer on the wrong side until a later navigation/reload. Fix: unify language resolution via `_resolve_ui_language()` and retry Quasar layout activation until the framework is ready. |
| **Browse title toggle keeps RTL classes** | web/pages/browse.py:2093,2098,2253,2257 | ג… Fixed (2026-03-10) | Changed `.classes()` to use `remove=/add=` for proper class swapping |
| **Debug prints in code** | `genizah_app.py`, `parallels.py` | ג… Fixed (2026-02-03) | Removed all `[DEBUG]` print statements |
| **List Rename** | `web/pages/lists.py:414-423` | ג… Fixed (2026-02-03) | Uses `create_inline_edit_label` for inline editing |
| **Missing CSV/Word exports for Lists** | `lists.py:612-631` | ג­ן¸ Won't Fix | Excel export sufficient for needs |
| **Bare `except:` statements** | Multiple files | ג… Fixed (2026-02-03) | Changed all 16 instances to `except Exception:` |
| **Shelfmark normalization inconsistency** | 5 implementations | ג… Fixed (2026-02-04) | Unified to single `normalize_shelfmark()` in `genizah_core.py` |
| **Star button visual feedback** | `browse.py`, `search.py` | ג… Fixed (2026-02-03) | Shows `star` when in list, `star_border` when not |

### P3 - Low Priority

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Auto-save not working** | `text_editor.py:374` | ג… Fixed (2026-02-03) | Auto-save implemented at lines 443-454 using NiceGUI timer |
| **Browse extended-info open state is not restored when content arrives only from the later PGP worker** | `genizah_app.py:14923-14930,14966-14975` | ✅ Fixed (2026-03-20) | Follow-up review confirmed the restore flag now has a complete lifecycle: it is set when browse enrichment resets the panel, consumed by either enrichment HTML or `_on_browse_pgp_loaded()` when that is the first content making the panel visible, and then cleared unconditionally at the end of `_on_browse_pgp_loaded()` so it cannot leak into a later manuscript. |
| **Race conditions in UI timers** | `parallels.py`, `search.py` | ג… Fixed (2026-02-04) | Added timer tracking and deactivation to prevent duplicates |
| **Cache thread-safety** | `joins_panel.py:17-19` | ג… Fixed (2026-02-04) | Added threading.Lock for cache access |
| **Filter panel overlap with progress bar** | `web/pages/search.py`, `parallels.py` | ג… Fixed (2026-03-03) | Chip bar, progress bar, results overlapped when filter panel open. Auto-collapse panel on search start + scroll to progress + spacing/z-index fix |
| **Pre-search domain filter: bilingual, "Other" ambiguous, missing 3rd level** | `search.py`, `parallels.py`, `genizah_app.py`, `fjms_service.py` | ג… Fixed (2026-03-03) | Dropdown showed bilingual labels (should be current lang only), "Other" had no parent disambiguation, sub-sub-domains missing. Chips also lost qualified names. Fixed all 3 issues + recursive checkbox propagation + qualified-name SQL filtering |
| **CSRF protection missing** | API endpoints | ג Deferred | Low risk - NiceGUI uses WebSocket |
| **Puzzle BG removal: brown backing page not removed on glued manuscripts** | `shared/background_removal.py`, `web/pages/puzzle.py` | ⏳ Deferred | BL manuscripts have brown backing over blue mat; Oxford has full brown background. Color segmentation alone can't distinguish brown backing from parchment. Planned solution: interactive click-to-remove eraser tool (user clicks background areas, BFS flood fill removes connected region, additive with per-step undo). Requires persistence plumbing (save/load/export eraser steps), auth on endpoint, proper canvas coordinate transforms, Fabric event model integration. Full design: `docs/plans/INTERACTIVE_BG_REMOVAL_DESIGN.md`. Recommend implementing as a full GSD phase. |
| **Session restore is not pixel-perfect** | genizah_app.py | ❌ Open | v6.5.1 added restore for browse tabs, catalog filters, composition results, and active tab. But composition restores flat (grouping/appendix lost), catalog sidebar doesn't highlight the selected author/work in the list widget, and browse-by-shelfmark skips full resolution (loads directly by sys_id). Could be improved to persist grouping state or re-run grouping more reliably. |
| **BrowseState.meta_mgr AttributeError in joined view** | `web/pages/browse.py:3255` | ✅ Fixed (2026-03-17) | `state.meta_mgr` accessed without guard in Oxford detection code path. Fixed with `getattr(state, 'meta_mgr', None)`. |
| **Desktop discovery stats all zeros** | `supabase_corrections_client.py` | ✅ Fixed (2026-03-17) | `get_discovery_stats()` only queried discoveries table type column, returning keys that didn't match UI stat_labels. Now queries corrections, profiles, fragment_joins tables. |
| **Reading Desk from joined view: "Could not resolve fragment identifiers"** | `genizah_app.py:15956` | ✅ Fixed (2026-03-17) | Added `meta_mgr.resolve_system_by_shelfmark()` as third fallback in `_browse_open_joins_in_reading_desk`. |
| **Deleting local puzzle join leaves orphaned Supabase published join** | `web/pages/puzzle.py`, `genizah_app.py` | ✅ Fixed (2026-03-17) | Auto-calls `unpublish_join` before deleting local document. |

---

## 2. Documentation Issues

### Stale/Outdated Content

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Lists Unification Plan references removed backend** | `LISTS_UNIFICATION_PLAN.md` | ג… Fixed (2026-02-03) | Added deprecation note |
| **Joins Feed Plan references removed backend** | `JOINS_FEED_PLAN.md` | ג… Fixed (2026-02-03) | Added deprecation note |
| **Plans Index stale status** | `PLANS_INDEX.md` | ג… Fixed (2026-02-03) | Updated with current status |
| **Duplicate bug tracking** | `PRE_LAUNCH_CHECKLIST.md` + `FIX_PLAN.md` | ג­ן¸ Deferred | OPEN_ISSUES.md is now canonical |

### Version Number Mismatches

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **README says 5.3** | `README.md` | ג… Fixed (2026-02-03) | Updated to 5.4 |
| **Desktop download reference** | `README.md` | ג… Fixed (2026-02-03) | Updated to V5.4.1 |
| **Pre-launch checklist version** | `PRE_LAUNCH_CHECKLIST.md` | ג… Fixed (2026-02-03) | Updated to 5.4 |
| **Code Quality Audit version** | `CODE_QUALITY_AUDIT_2026-01-30.md` | ג­ן¸ N/A | Already in archive |

---

## 3. Untested Areas

These items from `PRE_LAUNCH_CHECKLIST.md` need verification:

| Area | Status | Notes |
|------|--------|-------|
| **End-to-End Integration** | ❌ Not Tested | Full flows: Search→View→Edit→Submit→Approve |
| **Concurrency** | ❌ Not Tested | Two users editing same correction simultaneously |
| **Browser Compatibility** | ❌ Not Tested | Chrome, Firefox, Safari, Edge, Mobile |
| **Performance** | ✅ Fixed (2026-03-16) | Staged search enrichment: render results before metadata loads. FJMS indexes moved to build-time. Perf timing spans added to logger (`first_render_ms`, `visible_enrichment_ms`, `background_enrichment_ms`). Needs rebuild of `fjms_enrichment.db` to include new indexes. |
| **In-App Update (Desktop)** | ❌ Test on Next Release | Build test version with 5.0.0, verify full update flow works (download → install → auto-restart) |
| **Translation QA / hallucination audit** | ✅ Fixed (2026-03-11) | QC module (`shared/translation_qc.py`), audit script, report component, disclaimers added. 12,827 rows fixed: Piyyut (10,256), Bible (979), Mahzor (317), Selihot (347), Kinot (218), stuttering nulled (257), FJMS hallucinations deleted (445), PGP collapsed nulled (8). DBs uploaded to server. |
| **MARC field translations (Date/Subjects/People)** | ❌ Needs Testing | Added translate badges for Date, Subjects, People in ResultDialog and Browse extended info. Hebrew dates use direct gematria converter (`_translate_hebrew_date`) to avoid Dicta errors (e.g. "מאה ט״ו" → "15th century"). Subjects/People use Dicta on-demand. Test: open records with Hebrew dates, subjects, people in EN UI with translations ON. Verify badges appear, translations are correct, toggle works. Test record: sys_id 990001430180205171. |

---

## 4. Pending Plans (Implemented)

| Plan | File | Status | Notes |
|------|------|--------|-------|
| **Mobile Responsive Design** | `MOBILE_RESPONSIVE_PLAN.md` | ג… Implemented | Responsive design completed |
| **Lists/Projects Unification** | `LISTS_UNIFICATION_PLAN.md` | ג… Implemented | Lists and projects unified |
| **Joins in Discovery Feed** | `JOINS_FEED_PLAN.md` | ג… Implemented | Joins appear in discovery feed |
| **Desktop Cloud Sync** | Multiple docs | ג… Implemented | Desktop syncs with Supabase |

> Note: All plans implemented as of 2026-02-04

---

## 5. Code Quality Debt

### Duplication to Address

| Issue | Files | Status | Notes |
|-------|-------|--------|-------|
| **Excel export duplication** | `genizah_app.py` + `export_service.py` | ג… Fixed (2026-02-04) | Unified via `shared_export_utils.py` |
| **Word export duplication** | `genizah_app.py` + `export_service.py` | ג… Fixed (2026-02-04) | Unified via `shared_export_utils.py` |
| **Text sanitization inconsistency** | Desktop vs Web | ג… Fixed (2026-02-04) | Single `sanitize_text_for_excel()` in `shared_export_utils.py` |

### Hardcoded Values

| Value | File | Status | Should Be |
|-------|------|--------|-----------|
| `_CACHE_TTL = 30` | `joins_panel.py:19` | ג… Fixed (2026-02-04) | Now uses `JOINS_CACHE_TTL` env var |
| `CACHE_TTL = 300` | `api.py:46` | ג… Fixed (2026-02-04) | Now uses `NLI_CACHE_TTL` / `IMAGE_CACHE_TTL` env vars |
| Timeouts & retries | `auth_state.py:17-20` | ג Deferred | Low priority - defaults are reasonable |

---

## 6. Documentation Gaps

| Topic | Status | Notes |
|-------|--------|-------|
| **Supabase RLS policies detail** | ג… Fixed (2026-02-03) | Added detailed policy SQL examples to `SUPABASE_GUIDE.md` |
| **OAuth callback handling** | ג… Fixed (2026-02-03) | Documented implicit flow and token extraction in `SUPABASE_GUIDE.md` |
| **Cloudflare rate limiting config** | ג… Fixed (2026-02-03) | Added configuration guide to `DEPLOYMENT_TECHNICAL.md` |
| **Desktop Supabase client** | ג… Fixed (2026-02-03) | Added `supabase_corrections_client.py` to `CODE_INDEX.md` |

---

## 7. Archive Candidates

All completed items have been moved to `docs/archive/`:

| File | Reason | Status |
|------|--------|--------|
| `SUPABASE_MIGRATION_PLAN.md` | Marked COMPLETED | ג… Archived (2026-02-03) |
| `LIBRARY_LOCATION_PLAN.md` | Marked ג… Implemented | ג… Archived (2026-02-03) |
| `LIBRARY_LOCATION_TEST_CHECKLIST.md` | Testing complete | ג… Archived (2026-02-03) |
| `BOUNDARY_SEARCH_SPEC.md` | COMPLETED (Web + Desktop) | ג… Archived (2026-02-03) |

---

## 8. Completed Issues (Archive)

*Move verified-complete items here with completion date*

| Issue | Completed | Notes |
|-------|-----------|-------|
| *None yet* | - | - |

---

## Change Log

| Date | Change | By |
|------|--------|-----|
| 2026-03-22 | Re-reviewed the blue-mat auto-detection update. Confirmed the follow-up issue is fixed: auto-detected blue mats now take the same blue-only path as explicit `is_cul=True`, so the corner-sampled normal mask is no longer unioned in and the earlier parchment-hole regression path is closed. Focused `tests/test_background_removal.py` re-run passed. | Codex |
| 2026-03-22 | Reviewed the implemented FIST bibliography enhancement changes. Confirmed the earlier export follow-up is fixed: `export_bibliography()` now aligns the bibliography schema, SELECT list, and placeholder count at 22, so the planned re-export path is unblocked. No new review findings in this pass. | Codex |
| 2026-03-22 | Reviewed the FIST bibliography enhancement plan and added 1 new open P2 issue: `export_bibliography()` now defines 22 bibliography columns but still generates 23 `?` placeholders, so the planned sidecar re-export would fail before populating the new fields. | Codex |
| 2026-03-22 | Reviewed the blue-mat auto-detection follow-up. Added 1 new open P2 issue: the new auto-detect branch unions the blue-only mask with the same corner-sampled normal mask that already fails on blue-mat scans when parchment reaches the corners, so unhinted blue-mat images can still lose real fragment pixels. | Codex |
| 2026-03-21 | Final re-review of `web/pages/puzzle.py` confirmed the remaining external-fragment follow-up is fixed: `initial_add` now uses the same effective external metadata (`processed`, `threshold`, `is_cul`) as the other add/load paths, so all three external entry points are aligned. No new findings in this pass. | Codex |
| 2026-03-21 | Re-reviewed the puzzle follow-up fixes. Confirmed the ghost-fragment restore issue is fixed: skipped entries are now pruned from `puzzle_meta` and session storage. Narrowed the remaining external-metadata issue to the `initial_add` auto-add path, which still hard-codes `processed=true` for external fragments. | Codex |
| 2026-03-21 | Reviewed the latest `web/pages/puzzle.py` restore/banner/_after_delay diff. Added 2 new P2 issues: (1) external Oxford/Cambridge fragments still persist the wrong `processed` / `threshold` / `is_cul` metadata, so the new restore path can reload the wrong image variant; (2) skipped restore entries are still copied into `puzzle_meta`, so `load_pending` is fixed but stale fragments can survive as ghost metadata and be saved back out later. | Codex |
| 2026-03-20 | Re-reviewed the final browse-tab follow-up. Confirmed the remaining P3 is fixed: `_browse_ext_info_restore` is now consumed by whichever path first makes the panel visible (enrichment or PGP) and cleared unconditionally at the end of `_on_browse_pgp_loaded()`. | Codex |
| 2026-03-20 | Re-reviewed the browse-tab follow-up fixes. Confirmed the lambda disconnect issue is fixed by storing/disconnecting `_browse_enrich_slot`. Narrowed the remaining P3: the restore flag no longer leaks forward, but manuscripts with no enrichment HTML still do not reopen the panel when content appears later only via `_on_browse_pgp_loaded()`. | Codex |
| 2026-03-20 | Reviewed the 4 recent Browse by Shelfmark desktop commits. Added 2 new issues: (1) `_start_browse_enrichment()` now connects `finished_signal` through a lambda but still tries to disconnect `self.on_browse_enriched_loaded`, so stale worker callbacks are not actually disconnected; (2) the new `_browse_ext_info_restore` flag is only consumed when enrichment HTML is non-empty, so it can leak and reopen the wrong manuscript's info panel on a later navigation. | Codex |
| 2026-03-20 | Reviewed production server logs and added 3 open issues: (1) `get_user_client()` leaves stale auth state after `Invalid Refresh Token: Already Used`, causing repeated retries and log spam; (2) `fetch_fl_ids_from_nli()` retries slow NLI IIIF manifest fetches immediately with no negative cache/backoff, causing timeout bursts; (3) search code still logs raw queries and regex internals at INFO with `[DEBUG]` labels in production. | Codex |
| 2026-03-21 | Reviewed quick task 260321-tiv (shared filter panel + manuscript viewer extraction) and added 4 follow-up issues: (1) deferred shared viewer script is used before load on cold page renders, (2) filter recompute generation guard still allows stale updates after clearing the last filter, (3) option-builder language lookup still happens inside `run.io_bound()` wrappers, and (4) fullscreen browse fallback reinitializes the main viewer instead of the fullscreen viewer. | Codex |
| 2026-03-21 | Reviewed follow-up commit `c2734168`. Marked 3 of the 4 quick-task follow-up issues fixed (script load order, recompute generation invalidation, and worker-thread language lookup). Kept 1 P2 issue open in narrower form: fullscreen fallback now targets `fsEditViewer`, but the shared handler still assumes the target viewer exposes `init()`. | Codex |
| 2026-03-21 | Reviewed follow-up commit `7dda767d`. Confirmed the remaining shared-viewer follow-up issue is fixed: `handleImageError()` now guards `window[viewerName].init()` with a function check, so fullscreen fallback success no longer throws when the target viewer lacks `init()`. | Codex |
| 2026-03-18 | Fixed Manchester LUNA recto/verso bug: each page has its own luna_id but code only fetched the first. Added `get_manchester_canvases()` to `NliCrossrefService` which resolves ALL crossref images to individual IIIF canvas entries. Confirmed with Ms. B 2091 (sys_id 990002081410205171): recto and verso now show distinct images. 6 new tests. | Claude |
| 2026-03-18 | Investigated the handoff claim that NLI Vienna/Rainer images became "tiny" in the desktop puzzle after the Mosseri CUDL session. Static trace confirmed the Mosseri/Cambridge changes do not affect non-Cambridge records, and `tests/test_mosseri_cudl.py` still passes (17/17). Added a new open P2 issue for the more likely root cause: desktop puzzle `_fit_all_fragments()` always calls `fitInView()` after each new fragment, so perfectly normal 800px images can appear thumbnail-sized due to view zoom rather than bad source data. | Codex |
| 2026-03-17 | Verified the localhost-helper web puzzle path against `genizahsearch.com` and switched it from opt-in to default-on fallback. The browser now tries the user's local helper after `/api/puzzle_image` fails, across initial add, reload/toggle, and folio navigation. Also removed the fragile preflight `/health` fetch gate because it could block the helper attempt from the live HTTPS site even when image loading itself worked. | Codex |
| 2026-03-17 | Reviewed commit `d6395f17` (`fix: client-side IIIF fallback when server can't fetch images`). Found 2 follow-up issues: (1) new `POST /api/puzzle_process` accepts arbitrary unauthenticated bytes and writes them into the shared puzzle-image cache keyed only by caller-supplied query params, enabling cache poisoning / upload abuse; (2) folio navigation still uses `/api/puzzle_image` without the new client-side fallback, so prev/next folio remains broken on production and can persist mismatched `fl_id`/`folio_label` metadata after a failed load. | Codex |
| 2026-03-17 | Fixed desktop packaging regression for puzzle image processing: both `build_app.bat` and `GenizahSearchPro.spec` listed `numpy` as required, but also excluded it from the PyInstaller bundle. That contradiction produced `No module named numpy` only in the packaged EXE when the puzzle imported `shared.background_removal`. Removed the `numpy` exclusion from both build paths. | Codex |
| 2026-03-17 | Investigated a desktop user report that "Build / Rebuild Index" fails with `[WinError 5] Access is denied` on `tantivy_db\\*.fast`. Static trace points to a Windows self-lock: startup opens the existing Tantivy index in `SearchEngine.reload_index()`, and the rebuild path later calls `shutil.rmtree()` on that same directory without first releasing the live searcher/index handles. Added this as a new open P2 issue with the likely fix path. | Codex |
| 2026-03-17 | Verified the final follow-up fixes from the release review: deleting the active web puzzle document now clears `puzzle_doc_id`, and the remaining puzzle/community translation gaps were filled (`This will make your puzzle join visible to all users.\n\nPublish now?`, `(no original text)`, `View all joins...`, `No joined fragments found.`, `Could not resolve fragment identifiers.`). Targeted regression tests passed for `tests/test_puzzle_publish.py` and `tests/test_puzzle_service.py` (23/23). | Codex |
| 2026-03-17 | Verified the user-reported follow-up fixes. Confirmed that web puzzle reload now persists/restores `puzzle_doc_id`, the five named translation keys were added, and the stale schema-version test now expects `2`. Also narrowed the earlier help-surface concern: `web/pages/help.py` and `Help.html` are app-specific and each now matches its own UI. Found one remaining regression in the new persistence path: deleting the active puzzle doc does not clear `puzzle_doc_id`, so reload can restore a deleted doc id. Static audit also still found additional untranslated puzzle/community strings beyond the five named fixes. | Codex |
| 2026-03-17 | Re-checked the v7.0.0 ship-readiness findings after follow-up fixes. Marked the empty-publish bug fixed: `publish_join()` now aborts on null composite and has a regression test. Narrowed the parity issue to a documentation/help drift (`web/pages/help.py` now matches the feed filter, `Help.html` still advertises `All Puzzles` / `My Puzzles`). Confirmed state persistence/sync and a smaller set of translation gaps are still open. | Codex |
| 2026-03-17 | Added v7.0.0 ship-readiness findings after static review of puzzle/community code: `publish_join()` can publish empty/broken composites when export fails; puzzle state persistence/sync is incomplete across reloads, folio changes, and desktop restart; web is missing the promised `All Puzzles` / `My Puzzles` browser that desktop/help already expose; and the new puzzle/community rollout still has many missing Hebrew translations. | Codex |
| 2026-03-17 | Follow-up web puzzle fixes after the initial Phase-50 export patch: fixed `build_fragments_list()` crash, made folio swaps and image reloads preserve visual centers, restored saved sessions with viewport reset/fit, synced `folio_label` back into Python state, and removed stale keyed Fabric objects so `Flip Puzzle` no longer created duplicate fragments. | Codex |
| 2026-03-17 | Fixed Phase-50 puzzle export issues across web + desktop: web export/save now derives fragment positions from Fabric visual centers (`getCenterPoint()`) instead of brittle `left/top` math, shared export reuses the same 800px processed image shown on-canvas for bg-removed fragments, and desktop export now runs in a cancelable `PuzzleExportThread` with 1000/2000/3000px resolution choices instead of freezing the UI. Added a separate open issue for remaining CUL blue-mat failures in `shared/background_removal.py`. | Codex |
| 2026-03-16 | Fixed the web puzzle canvas image-loading failure in `web/pages/puzzle.py`: NiceGUI `ui.html('<canvas...>')` was rendering an empty `<div>`, so Fabric never found `#puzzleCanvas`. Replaced it with `ui.element('canvas').props('id=puzzleCanvas')` and verified via live headless Edge/CDP that the canvas initializes and a fragment image loads onto it. Also landed same-day state-sync fixes for add-result persistence, delete persistence, selection sync, threshold reloads, and processed/original toggling. | Codex |
| 2026-03-15 | Added open join-finder issue after reviewing `docs/JOIN_FINDER_REPORT.md` and active scripts: current v7/v8 is a strong prototype but is not ready for manuscript-view embedding because it is effectively LEFT-only, mixes page/system scopes, and takes ~83-101s on the two benchmark cases. Added implementation-plan docs. | Codex |
| 2026-03-16 | Added open P2 web puzzle canvas v49 issue after reviewing `web/pages/puzzle.py`, `web/api.py`, and `shared/puzzle_image_service.py`: async fragment-meta race, dead threshold slider, JS-only deletion persistence bug, wrong MIME type on processed-image fallback, dead selection bridge, and no-op context-menu background toggle. | Codex |
| 2026-03-15 | Fixed web first-load sidebar/drawer initialization race in `web/main.py`: unified persisted language resolution for head bootstrap + layout, and made Quasar RTL activation retry on cold load so Hebrew UI no longer paints the drawer on the wrong side before navigation/reload. | Codex |
| 2026-03-13 | Updated all 4 docs/guides/ files: WEBSITE_ADMIN_GUIDE (added sidecars, translations, PostHog), DEPLOYMENT_TECHNICAL (fixed DB versions/sizes, expanded shared/ listing, added libraries_translations.db), DEVELOPER_GUIDE (full project structure with pages/components/shared), SUPABASE_GUIDE (fixed author_id column in query). Web citation bar updated to full author list. | Claude |
| 2026-03-11 | Added P2: FJMS export MAX(Version) drops ~38K catalog records (9.2%); P2: Desktop translation toggle shows wrong language by default. Full report in docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md | Claude |
| 2026-03-11 | MARC field translations: added translate badges for Date, Subjects, People; Hebrew date gematria converter avoids Dicta errors; marked for testing | Claude |
| 2026-03-11 | Added open untested-area item for translation QA / hallucination audit after reviewing the Phase 46 translation rollout | Codex |
| 2026-03-01 | v6.1.1 ג€” async desktop catalog browse (QThread), 100x faster domain queries (35s->0.8s via IN+UNION subquery + dedup CTE), 3-level domain hierarchy, canonical FJMS ordering, thread-safe FjmsService, browse cache v2 | Claude |
| 2026-02-22 | Closed v6.0.0 milestone ג€” local data architecture (pgp.db sidecar, FJMS catalog descriptions, offline browsing), bug fixes (desktop crashes, pagination), performance optimization (parallel NLI, crossref, variant cache), IsNotGenizah badge removed | Claude |
| 2026-02-16 | Closed v5.9.0 milestone ג€” multi-source image & metadata integration (NLI crossref, Cambridge/Manchester/JTS IIIF, bibliography, catalog refs), version bump to 5.9.0 | Claude |
| 2026-02-15 | Closed v5.8.0 milestone ג€” FJMS integration (domains, scientific joins, catalog enrichment), version bump to 5.8.0 | Claude |
| 2026-02-11 | Closed v5.7.2 milestone ג€” version bump to 5.7.2, AI code removed, search normalization, full green test suite, structural sections | Claude |
| 2026-02-09 | Closed v5.6.0 milestone ג€” version bump to 5.6.0, updated CHANGELOG.md and STATE.md | Claude |
| 2026-02-09 | Created `pgp_tag_translations.py` ג€” 251 PGP tags with curated Hebrew translations in 16 categories | Claude |
| 2026-02-09 | Added categorized tag dropdowns with category headers in both web and desktop apps | Claude |
| 2026-02-09 | Language-aware tag display: Hebrew UI shows "׳¢׳‘׳¨׳™׳× (English)", English UI shows English only | Claude |
| 2026-02-09 | Fixed desktop PGP Tags mode layout ג€” hides row1, shows tag combo in row2 after Mode | Claude |
| 2026-02-09 | Fixed web [object Object] in tag dropdown ג€” switched to NiceGUI native dict format | Claude |
| 2026-02-09 | Corrected ~12 tag category misassignments (e.g., Ibn Yiju moved from India Book to People) | Claude |
| 2026-02-09 | PGP Tags search mode added to Mode dropdown in both apps | Claude |
| 2026-02-09 | PGP column sorting, simplified PGP controls, user-friendly labels | Claude |
| 2026-02-09 | Reverted Phase 13 (Transcription Search) ג€” index build too slow for desktop | Claude |
| 2026-02-04 | Improved connection indicator UX - yellow pulsing dot for loading, no alarming text messages | Claude |
| 2026-02-04 | Fixed sidebar opening on mobile - now closes by default on screens < 768px | Claude |
| 2026-02-04 | Improved connection stability - added continuous heartbeat monitoring and reconnect_timeout | Claude |
| 2026-02-04 | Added Hebrew translations for "Reconnecting...", "Connecting..." | Claude |
| 2026-02-04 | Implemented in-app software updates - downloads and runs installer silently via Inno Setup | Claude |
| 2026-02-04 | Added translation button for comments and community messages (Hebrew ג†” English using MyMemory API) | Claude |
| 2026-02-04 | Marked pending plans as implemented: Mobile, Lists Unification, Joins Feed, Desktop Sync | Claude |
| 2026-02-04 | Created `shared_export_utils.py` - unified text sanitization, filename helpers for Desktop & Web | Claude |
| 2026-02-04 | Unified shelfmark normalization - single `normalize_shelfmark()` in genizah_core.py | Claude |
| 2026-02-04 | Made TTL values configurable via environment variables (JOINS_CACHE_TTL, NLI_CACHE_TTL, IMAGE_CACHE_TTL) | Claude |
| 2026-02-04 | Fixed UI timer race conditions in parallels.py and search.py - added timer tracking | Claude |
| 2026-02-04 | Fixed cache thread-safety in joins_panel.py - added threading.Lock | Claude |
| 2026-02-03 | Fixed bare `except:` statements - changed all 16 to `except Exception:` | Claude |
| 2026-02-03 | Verified auto-save in text_editor.py is working (lines 443-454) | Claude |
| 2026-02-03 | Fixed all 4 documentation gaps: RLS policies, OAuth callback, Cloudflare config, Desktop client | Claude |
| 2026-02-03 | Moved 4 completed plans to archive (Supabase, Library Location, Boundary Search) | Claude |
| 2026-02-03 | Updated README.md version to 5.4, fixed download reference to V5.4.1 | Claude |
| 2026-02-03 | Updated PRE_LAUNCH_CHECKLIST.md version to 5.4 | Claude |
| 2026-02-03 | Added deprecation notes to LISTS_UNIFICATION_PLAN.md and JOINS_FEED_PLAN.md | Claude |
| 2026-02-03 | Updated PLANS_INDEX.md with current implementation status | Claude |
| 2026-02-03 | Marked all pending plans as deferred per user request | Claude |
| 2026-02-03 | Fixed star button visual feedback in browse.py, search.py, viewer.py, parallels.py | Claude |
| 2026-02-03 | Removed all `[DEBUG]` print statements from production code | Claude |
| 2026-02-03 | Marked CSV/Word exports as "Won't Fix" per user request | Claude |
| 2026-02-03 | Verified all P2 bugs - list rename already fixed, updated counts | Claude |
| 2026-02-03 | Verified P1 path traversal bug already fixed in `filter_text_dialog.py` | Claude |
| 2026-03-16 | Search performance: staged enrichment (3-phase render), FJMS build-time indexes (6 added to export script, runtime DDL removed), generation guard race fix, Stage-2 re-render fix. Reviewed by GPT Codex. | Claude |
| 2026-02-03 | Initial creation from documentation audit | Claude |

---

## Related Documents

- `PRE_LAUNCH_CHECKLIST.md` - Detailed test checklist
- `FIX_PLAN.md` - Bug fix tracking
- `CODE_QUALITY_AUDIT_2026-01-30.md` - Full code audit
- `PLANS_INDEX.md` - Implementation plans overview

