---
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
verified: 2026-06-04T10:00:00Z
status: human_needed
score: 6/6 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Open a result from the desktop ResultDialog, click Find joins. Confirm the Join Workbench opens as a modeless window anchored on the folio you were viewing, and the ResultDialog closes."
    expected: "JoinWorkbenchWindow opens showing the anchor's shelfmark, image, and numbered transcription. ResultDialog is gone."
    why_human: "PyQt6 UI cannot be headlessly activated — window show/raise/close flow requires a live QApplication."
  - test: "Open Browse tab, load a manuscript, click Find joins. Confirm the Workbench opens and Browse stays open."
    expected: "JoinWorkbenchWindow opens with anchor from current_browse_sid/current_browse_p. Browse tab is still visible."
    why_human: "Requires a live desktop session to exercise the Browse tab and confirm tab state."
  - test: "Click Find joins twice on two different fragments. Confirm only ONE Workbench window exists; the second call re-anchors it rather than opening a second window."
    expected: "Single window raises/re-anchors on the second call (D-01/D-02)."
    why_human: "Single-instance lifecycle cannot be exercised headlessly."
  - test: "With a fragment that has known joins (user + PGP or FJMS), open the Workbench. Confirm the Known Joins panel appears with correct per-member rows, source badges (PGP=blue, FJMS=purple, user=green, community=green), and the panel is hidden when the fragment has no known joins."
    expected: "Panel visible with rows; badge colors match badge_for_source; panel setVisible(count>0) works correctly."
    why_human: "Four-source join loading requires live DB/service connections; visual rendering requires human inspection."
  - test: "Click Add as Join in the Workbench action row. Confirm JoinsDialog opens with Fragment A pre-filled with the anchor and Fragment B empty. Create a join and confirm the Known Joins panel refreshes to include the new join."
    expected: "JoinsDialog opens anchor-only; after closing, _reload_known_joins fires and the new join appears in the panel (SC#4)."
    why_human: "Requires live Supabase/local joins persistence and visual confirmation."
  - test: "With a dark-mode desktop theme active, confirm the anchor image area has a dark loading background (#374151), the ANCHOR tag is teal (#14b8a6), and Hebrew strings display correctly under lang=he."
    expected: "No hardcoded English text visible; all new strings appear in Hebrew when lang=he."
    why_human: "Dark-mode palette and language-switch must be exercised in a live app session."
  - test: "Zoom in/out on the anchor image and navigate folio prev/next. Confirm zoom rescales the image without re-fetching from network; folio nav pages the SAME fragment without reloading the known-joins panel."
    expected: "Zoom changes image scale; folio nav changes page number displayed; known-joins panel is unchanged (D-07)."
    why_human: "Image rendering and folio navigation require a live display session."
---

# Phase 107: Desktop Join Workbench Verification Report

**Phase Goal:** A dedicated desktop "Join Workbench" opens with a fragment pinned as anchor (image + numbered transcription, zoom + folio nav, brief metadata, dark-mode/RTL safe), shows the anchor's already-known joins as a connected GROUP (pairwise→group BFS), and exposes public action APIs (Browse / Puzzle / Add-to-List / Add-as-Join) that persist a confirmed join via the existing pairwise path and refresh the group. Bilingual from the first line. No candidate search yet.
**Verified:** 2026-06-04T10:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A "Join Workbench" window exists and opens via Find joins from ResultDialog and Browse, and by shelfmark for cold start (JWB-01/02) | VERIFIED | `def open_joins_workbench` at genizah_app.py:15427; `self._join_workbench` single-instance pattern; `isVisible()` re-anchor guard confirmed; ResultDialog `_open_join_workbench` closes dialog; Browse `_browse_open_join_workbench` stays open; `_cold_start_open` with `len(opts)==1` guard present |
| 2 | Anchor image via enrich_metadata→meta['images']→iiif_full→ImageLoaderThread (not get_thumbnail, not NLI-first); numbered transcription; zoom; folio nav; dark-mode/RTL safe (JWB-03) | VERIFIED | `_AnchorLoadWorker.run()` uses `meta.get("images")` (not images_nli/images_ext directly); `_image_url_for_idx` passes to `iiif_full`; `ImageLoaderThread` import confirmed; `apply_line_numbered_text` call confirmed; `_clamp_zoom` bounds [0.25, 4.0] verified; folio nav does NOT call `_reload_known_joins` (D-07 confirmed) |
| 3 | Known-joins panel from JoinsManager + PGP + FJMS + community (all four sources, deduped, per-member rows) (JWB-04) | VERIFIED | `_KnownJoinsLoadWorker` loads all four sources; `get_connected_fragments_by_id`, `get_document_for_fragment`, `get_fjms_service`, `get_published_joins_for_fragment` (hasattr-guarded) all present; `build_known_join_rows` dedupes and produces one row per connected member; `setVisible(count > 0)` present |
| 4 | Add as Join persists via JoinsDialog→corrections_client.create_join / JoinsManager.create_join_local; known-joins group refreshes (JWB-09) | VERIFIED | `_on_add_as_join` calls `self._app.open_anchor_as_join(...)` then `self._reload_known_joins(self._gen)`; `open_anchor_as_join` opens JoinsDialog with anchor as Fragment A, frag_b_input left empty (R-02), `dialog.exec()` confirmed; JoinsDialog internally uses `create_join` / `create_join_local` (confirmed in corrections_ui.py) |
| 5 | All four actions dispatched via public named methods (no _vs_* on workbench path) (SC#5) | VERIFIED | `grep -c "_vs_" desktop/join_workbench.py` = 0; AST guard test_join_workbench_no_private.py passes; `open_anchor_in_puzzle` and `open_anchor_as_join` public wrappers exist in genizah_app.py; workbench calls `self._app.open_result_in_browse_from_table`, `self._app.open_anchor_in_puzzle`, `self._app.show_add_to_list_menu`, `self._app.open_anchor_as_join` |
| 6 | All new strings wrapped in tr(); fully bilingual under lang=he (SC#6) | VERIFIED | 11 NEW i18n keys added in Plan 01 closed bootstrap; all 18 phase keys (11 new + 7 pre-existing reused) present in TRANSLATIONS; AST guard test_join_workbench_i18n.py passes 78/78 including `test_phase107_host_keys_translated_and_wrapped` (was xfail in Plan 01, now passes after Plan 03 added tr("Find joins") to host files) |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/join_workbench.py` | JoinWorkbenchWindow + workers + pure helpers | VERIFIED | 1299 lines; contains JoinWorkbenchWindow(QDialog), _AnchorLoadWorker, _PageTextWorker, _KnownJoinsLoadWorker, ThumbBatchWorker, plus all pure helpers; imports headlessly |
| `tests/test_join_workbench.py` | Tier-1 unit tests | VERIFIED | 78/78 tests passing |
| `tests/test_join_workbench_no_private.py` | AST guard SC#5 | VERIFIED | Passes; _vs_ count = 0 in join_workbench.py |
| `tests/test_join_workbench_i18n.py` | AST guard SC#6 | VERIFIED | Passes 78/78 (xfail flipped to pass after Plan 03) |
| `genizah_translations.py` | 11 NEW i18n keys | VERIFIED | All 18 phase keys resolve in TRANSLATIONS |
| `genizah_app.py` | open_joins_workbench + public wrappers + Browse button | VERIFIED | def open_joins_workbench at :15427, def open_anchor_in_puzzle at :15439, def open_anchor_as_join at :15443, btn_b_find_joins at :6946, _browse_open_join_workbench at :9856 |
| `desktop/result_dialog.py` | Find joins button + callback | VERIFIED | btn_rd_find_joins QPushButton, _open_join_workbench callback, action_row.addWidget(self.btn_rd_find_joins) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `desktop/result_dialog.py _open_join_workbench` | `GenizahGUI.open_joins_workbench` | `self._app.open_joins_workbench(res); self.close()` | WIRED | Both calls confirmed in _open_join_workbench body |
| `genizah_app.py _browse_open_join_workbench` | `GenizahGUI.open_joins_workbench` | builds anchor dict from current_browse_sid + current_browse_p | WIRED | `current_browse_p` used (not self.p), `self.open_joins_workbench(res)` confirmed |
| `JoinWorkbenchWindow._start_anchor_load` | `meta_mgr.enrich_metadata` | `_AnchorLoadWorker` QThread emitting gen token | WIRED | `enrich_metadata` call in _AnchorLoadWorker.run() confirmed |
| `JoinWorkbenchWindow anchor image` | `ImageLoaderThread` | `iiif_full(meta['images'][idx]['url'])` passed to ImageLoaderThread | WIRED | `_image_url_for_idx` + `ImageLoaderThread(url)` call confirmed; NEVER calls get_thumbnail for anchor |
| `JoinWorkbenchWindow _reload_known_joins` | `JoinsManager.get_connected_fragments_by_id` + PGP + FJMS + community | `_KnownJoinsLoadWorker` four-source | WIRED | All four import paths confirmed in worker; community hasattr-guarded |
| `genizah_app.py open_anchor_in_puzzle` | `_vs_add_to_puzzle` | thin public wrapper | WIRED | `def open_anchor_in_puzzle(self, sys_id): self._vs_add_to_puzzle(sys_id)` |
| `genizah_app.py open_anchor_as_join` | `JoinsDialog.exec()` | opens JoinsDialog anchor-only (frag_b_input empty) | WIRED | `from corrections_ui import JoinsDialog; dialog.exec()` confirmed; `frag_b_input.setText` NOT present in method body |
| `JoinWorkbenchWindow _on_add_as_join` | `_reload_known_joins` | called after dialog.exec() returns (SC#4) | WIRED | `self._reload_known_joins(self._gen)` immediately after `open_anchor_as_join` call |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `JoinWorkbenchWindow._build_anchor_pane` (anchor image) | `self._anchor_images` | `enrich_metadata(sys_id)['images']` → IIIF URLs via NLI/ext catalog | Yes — enrich_metadata queries NLI/crossref/MARC via live network | FLOWING |
| `JoinWorkbenchWindow._on_anchor_loaded` (transcription) | `out["text"]` | `searcher.get_browse_page(sys_id, page)['text']` | Yes — real Tantivy index query | FLOWING |
| `JoinWorkbenchWindow._on_known_joins_loaded` (known joins) | `rows` list | `_KnownJoinsLoadWorker` four-source load (JoinsManager + PGP + FJMS + community) | Yes — real DB queries against joins.db / pgp.db / fjms_enrichment.db / Supabase | FLOWING |
| `meta_brief` (image count display) | `n_img` | `meta.get("images_nli") or meta.get("images_ext")` from enrich_metadata return dict | Yes — enrich_metadata sets both sub-keys; count is read-only display | FLOWING (note: uses images_nli/images_ext sub-keys for count only; anchor image route correctly uses meta['images']) |

### Behavioral Spot-Checks

Step 7b: SKIPPED — PyQt6 desktop window (JoinWorkbenchWindow is a QDialog); no headless runnable entry point. Verified via code inspection + test suite (78/78).

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| JWB-01 | 107-03 | Dedicated Join Workbench tab/window exists | SATISFIED | JoinWorkbenchWindow(QDialog) in desktop/join_workbench.py; open_joins_workbench single-instance pattern |
| JWB-02 | 107-03 | Opens with fragment pinned via "Find joins" from ResultDialog + Browse; openable by shelfmark | SATISFIED | Both entry buttons confirmed (direct QPushButtons); cold-start _cold_start_open with shelfmark input |
| JWB-03 | 107-02 | Anchor image + numbered transcription in view during search | SATISFIED | _AnchorLoadWorker→enrich_metadata→meta['images']→iiif_full; apply_line_numbered_text; zoom ± and folio nav not reloading joins |
| JWB-04 | 107-02 | Shows already-known joins for anchor (PGP + FJMS + user + community) | SATISFIED | _KnownJoinsLoadWorker four-source; dedup_join_rows; build_known_join_rows per-member rows |
| JWB-09 | 107-02/03 | Scholar can add a join via existing joins button; group refreshes | SATISFIED | _on_add_as_join→open_anchor_as_join→JoinsDialog→create_join path; _reload_known_joins after exec() |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `genizah_translations.py` | 1719, 1899, 3348 | "coming soon" strings | Info | Pre-existing strings unrelated to Phase 107; not in Phase 107 code paths |
| `desktop/result_dialog.py` | 3353 | `on_placeholder=lambda` | Info | Pre-existing parameter in ms_viewer — not a placeholder anti-pattern, it's a callback param name |

No blockers or warnings in Phase 107 code. All anti-pattern hits are pre-existing and outside Phase 107 scope.

### Human Verification Required

The automated verification is complete. The following behaviors require a live desktop session to confirm:

**1. ResultDialog Entry Point**
**Test:** Open a search result in ResultDialog, click Find joins.
**Expected:** JoinWorkbenchWindow opens modeless on the viewed folio; ResultDialog closes.
**Why human:** PyQt6 window show/close flow requires a live QApplication.

**2. Browse Entry Point**
**Test:** Load a manuscript in Browse, click Find joins.
**Expected:** Workbench opens anchored on current_browse_sid/current_browse_p; Browse tab stays open.
**Why human:** Browse tab state requires a live desktop session.

**3. Single-Instance Re-anchor (D-01/D-02)**
**Test:** Click Find joins on two different fragments in sequence.
**Expected:** One window; second call re-anchors and raises, no second window.
**Why human:** Window lifecycle cannot be exercised headlessly.

**4. Four-Source Known Joins Display (JWB-04)**
**Test:** Anchor on a fragment with joins from multiple sources.
**Expected:** Known Joins panel shows per-member rows with correct source badges; panel hidden when anchor has no joins.
**Why human:** Requires live DB/service connections and visual badge inspection.

**5. Add as Join + Group Refresh (JWB-09/SC#4)**
**Test:** Click Add as Join, create a join in JoinsDialog, close it.
**Expected:** Known Joins panel refreshes to include the new join.
**Why human:** Requires live Supabase/joins.db write and visual refresh confirmation.

**6. Dark Mode + Hebrew Bilingual (SC#6)**
**Test:** Run with a dark system theme and lang=he.
**Expected:** ANCHOR tag teal, loading bg dark, all new strings in Hebrew.
**Why human:** Theme and language switch must be exercised in a live app.

**7. Zoom + Folio Nav (JWB-03/D-07)**
**Test:** Zoom in/out; navigate folio prev/next.
**Expected:** Zoom rescales without network fetch; folio nav does not reload known-joins panel.
**Why human:** Image rendering and network-fetch suppression require live observation.

### Gaps Summary

No gaps found. All six observable truths are VERIFIED by code inspection. The 78/78 test suite passes including both AST guards (SC#5 no _vs_*, SC#6 i18n coverage) and the scoped host-key check. The seven items above are standard human verification for PyQt6 UI behavior — they are not gaps, they are confirmation items that cannot be exercised headlessly.

---

_Verified: 2026-06-04T10:00:00Z_
_Verifier: Claude (gsd-verifier)_
