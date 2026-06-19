---
phase: 118-joins-entry-full-builders
verified: 2026-06-18T12:00:00Z
human_confirmed: 2026-06-19T04:10:00Z
status: verified
score: 5/5 must-haves verified (7/7 human-UAT tests passed after fixes)
overrides_applied: 0
human_verification:
  - test: "Load /joins-lab?sys_id=<known-fragment> with PGP + community joins. Confirm the Known Joins expansion shows source badges with correct colors (PGP blue, FJMS purple, user emerald, community grey). Click the push_pin on a member — confirm the anchor pane reloads to that fragment (ANC-04 re-anchor, D-16). Confirm builder rows are preserved after re-anchor."
    expected: "Source badges render with four distinct colors; re-anchor loads the new fragment in the sticky pane without clearing typed builder rows."
    why_human: "Color rendering and NiceGUI interactive state cannot be verified by grep or unit tests."
  - test: "From a /search result card for a fragment WITH known joins: click the 'link' icon. Confirm a joins dialog opens with a 'Find more joins' button. Click 'Find more joins' — confirm a new browser tab opens to /joins-lab pre-loaded on that fragment."
    expected: "Dialog appears (not a new tab directly), 'Find more joins' button inside dialog navigates to /joins-lab in a NEW tab. Original search page is preserved."
    why_human: "New-tab navigation and dialog-with-button interaction are browser-level behaviors not verifiable headlessly."
  - test: "From a /search result card for a fragment WITHOUT known joins: click the 'link' icon. Confirm no empty dialog appears — clicking goes straight to /joins-lab in a NEW tab."
    expected: "New tab opens directly to /joins-lab with sys_id in URL. No near-empty dialog."
    why_human: "Whether the card count load correctly detected zero joins and reconfigured the click handler requires live Supabase data + browser interaction."
  - test: "Open /browse on a fragment with known joins. Confirm the joins button color (green when joins, neutral when none). Click it — joins exist path should show dialog with 'Find more joins'; no-joins path should open /joins-lab in a NEW tab."
    expected: "Button tint correct (var(--primary-600) when joins, neutral otherwise). New tab behavior works. Original /browse context preserved."
    why_human: "Color rendering and new-tab navigation need a live browser."
  - test: "Build ≥2 lines in the Joins Lab builder, run search. Confirm builder collapses to a summary bar showing mode and line count (WR-04: in Hebrew UI the summary bar must be in Hebrew, not English). Click 'Edit' — confirm builder re-expands showing the typed rows."
    expected: "Summary bar is bilingual (tr() applied). Auto-collapse works. Edit re-expands without losing typed content."
    why_human: "Visual/interaction state collapse + bilingual rendering require browser render."
  - test: "Toggle 'Flexible spacing' ON in the Advanced options. Run a search. Check server logs or network inspector to confirm the executed responsa_options contain flex_spacing=True (BLD-04)."
    expected: "flex_spacing in the search options is True when the toggle is checked."
    why_human: "Network inspection of request payload is not possible via static analysis; the unit test (test_merge_globals_web) proves the function works, but end-to-end wiring through the UI requires browser."
---

# Phase 118: Joins Entry + Full Builders Verification Report

**Phase Goal:** Scholars see the anchor's known scientific joins (PGP + FJMS + user + community) as a connected group, can reach the Joins Lab directly from `/search` cards and `/browse`, and can build complete line-by-line queries for BOTH sides of the leaf — with per-line modifiers and global toggles that are never silently dropped.

**Verified:** 2026-06-18
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Anchor pane displays known joins as a connected group with per-join source attribution (PGP / FJMS / user / community), pairwise→group BFS model, no new schema (ANC-04). | VERIFIED | `web/components/known_joins_group.py` — `render_known_joins_group()` renders source badges via `badge_for_source()` with exact UI-SPEC colors. `web/components/joins_panel.py:294-358` — community merge block gated to `confirmed_only` Lab path, merges published puzzle joins as `sources=['community']`. Test `test_community_member_appears_in_lab_group` PASSES. `test_multi_source_dedup_merges_sources` PASSES. `badge_for_source('PGP')='#1d4ed8'`, `badge_for_source('FJMS')='#7e22ce'`, `badge_for_source('user')='#047857'`, `badge_for_source('community')='#525252'` confirmed via direct Python invocation. |
| 2 | Known-joins display is multitenant-safe: only public/confirmed joins in the process-global path (status='confirmed' app-layer filter + isolated ':confirmed' cache key), so User A's unconfirmed proposed joins cannot appear in User B's session (ANC-05). | VERIFIED | `joins_panel.py:113-114` — `get_fragment_joins(fragment_sys_id=document_id, status='confirmed')` on the `confirmed_only=True` path. Cache key at `joins_panel.py:96` — `f"{base_key}:confirmed"` suffix when `confirmed_only`. `invalidate_joins_cache` prefix-match covers both keys. All 5 `test_joins_anc05_rls.py` tests PASS including `test_confirmed_path_passes_status_confirmed_to_get_fragment_joins` and `test_no_cross_user_poisoning`. `fragment_joins.status` column confirmed present via `supabase_setup.sql:162` + `get_fragment_joins` signature. |
| 3 | "Find joins" actions on `/search` result cards and `/browse` open Joins Lab pre-loaded with that fragment as anchor (FND-08 deep-link: sys_id param; `volume_ie` for multi-IE; no builder/triage state in URL). | VERIFIED | `search_results.py:635` — `f'/joins-lab?sys_id={sys_id}'` + volume_ie appended when `_joins_ie_id` set. `browse.py:3904-3906` — `f'/joins-lab?sys_id={page.sys_id}'` + `&volume_ie={page.volume_ie}` when set. Quick View at `search_results.py:2038-2045` — same pattern. URLs contain ONLY sys_id + optional volume_ie. `new_tab=True` confirmed at `search_results.py:673` and inside `joins_panel.py:470`. `joins_panel.py:432,511` — `find_joins_url` added backward-compatibly to both `create_joins_button` and `create_joins_dialog`. |
| 4 | Other-side builder drives `resolve_other_side_pages` over a web page contract (`p_num`-based not internal index, multi-IE-aware, defined behavior for unknown total-page counts and sparse pages); cross-side narrow/widen via `apply_cross_side` produces a proper subset (AND narrow) / superset (OR widen). | VERIFIED | `joins_lab.py:933-944` — `run_cross_side_core` closure calls `apply_cross_side(executor, base, b_query, b_ro, _combine_mode_snap)` via `run.io_bound`. Contract uses `p_num` not `internal_index` (verified by `test_p_num_used_not_internal_index` PASS). `total_pages=0` → None graceful degradation (verified by `test_total_pages_zero_treated_as_none` PASS). `volume_ie` forwarded to `get_browse_page` (verified by `test_cross_side_volume_ie_forwarded` PASS). `_other_side['combine']` controls AND/OR (Narrow/Widen). `test_cross_side_uses_p_num_and_handles_metadata_only` PASSES. |
| 5 | Per-line modifiers (line-start ⊢ / line-end ⊣ / plene-defective) hoist into Responsa-syntax forms `compose()` recognises; global toggles (variants, Judeo-Arabic, flexible spacing, bidirectional) apply to BOTH sides via `_merge_globals`-equivalent — a test enables each toggle and asserts the executed query string changes. | VERIFIED | `joins_builder.py:39-84` — `_apply_modifiers_to_term()` implements all modifiers including slash-group detection and RR-13 wildcard rule. `joins_lab.py:95-111` — `_merge_globals_web()` sets `flex_spacing` and `bidirectional`, explicitly excludes `ja` (D-10), does not touch `variants`. Applied at `joins_lab.py:829` (anchor) and `joins_lab.py:937` (other-side b_ro). All 34 phase tests PASS: `test_flex_spacing_injected_into_ro`, `test_ja_not_injected`, `test_merge_applies_to_other_side_ro`, `test_negation_slash_group`, `test_wildcard_prefix_not_applied_to_slash_group`. |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/components/known_joins_group.py` | Source-badged known-joins renderer (ANC-04) | VERIFIED | Defines `render_known_joins_group()` and `badge_for_source()`. 5 tr() calls. CR-02 fix: re-anchor pin hidden when `member_sys_id` is falsy (line 191). |
| `web/components/joins_builder.py` | Line builder widget (BLD-03) with modifier hoist | VERIFIED | Defines `_apply_modifiers_to_term()`, `build_side_query()`, `create_joins_builder()`. 22 tr() calls. WR-03 fix: `_text_position_options()` built at request time. WR-04 fix: `_get_summary()` uses tr(). WR-05 fix: gap border updated in place. |
| `web/components/joins_panel.py` | confirmed_only fetch path + find_joins_url (ANC-05, FND-04/05) | VERIFIED | `fetch_connected_fragments()` has `confirmed_only: bool = False` kwarg. `create_joins_button()` and `create_joins_dialog()` both have `find_joins_url: Optional[str] = None`. Community merge block at lines 294-358 gated to Lab path. |
| `web/pages/joins_lab.py` | _merge_globals_web + cross-side + known-joins wiring (BLD-02/04, ANC-04) | VERIFIED | `_merge_globals_web()` at line 95. `run_search_core` literal name at line 855. `run_cross_side_core` literal name at line 933. `_load_known_joins()` at line 515 called from `load_anchor` at line 652. CR-01 fix: `compose()` ValueError caught at line 817-825. CR-02 fix: empty sys_id guard at line 590. WR-01 fix: single outer try/finally. WR-02 fix: `candidates_container.clear()` in `load_anchor`. |
| `web/pages/search_results.py` | Joins icon on result cards + Quick View (FND-04) | VERIFIED | Card joins icon at lines 633-700. Off-loop count load via `run.io_bound` at line 686. Quick View `find_joins_url` at lines 2038-2045. WR-06 fix: `_joins_pgpid` threaded through at line 655. |
| `web/pages/browse.py` | find_joins_url on browse (FND-05) | VERIFIED | `_joins_lab_url` built at line 3904 with `volume_ie` appended. Passed as `find_joins_url=_joins_lab_url` at line 3914. |
| `tests/test_merge_globals_web.py` | BLD-04 test suite | VERIFIED | 5 tests, all PASS. |
| `tests/test_other_side_page_contract.py` | BLD-02 test suite | VERIFIED | 8 tests including fake-executor integration, all PASS. |
| `tests/test_builder_modifier_hoist.py` | BLD-03 test suite | VERIFIED | 11 tests, all PASS. |
| `tests/test_known_joins_group.py` | ANC-04 test suite | VERIFIED | 5 tests including community path, all PASS. |
| `tests/test_joins_anc05_rls.py` | ANC-05 test suite | VERIFIED | 5 tests including cross-user poisoning guard, all PASS. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `joins_lab.py run_search_core` | `_merge_globals_web(ro, _global_opts)` | Called after `compose(anchor_side)` at line 829 | VERIFIED | `grep -n "_merge_globals_web(ro" joins_lab.py` matches line 829 |
| `joins_lab.py run_cross_side_core` | `_merge_globals_web(b_ro, _global_opts_snap)` | Inside sync closure at line 937 | VERIFIED | Both sides get globals re-injected |
| `joins_lab.py` | `render_known_joins_group` | `fetch_connected_fragments(confirmed_only=True)` off-loop via `run.io_bound` | VERIFIED | `joins_lab.py:530-535` — `await run.io_bound(fetch_connected_fragments, ..., confirmed_only=True)` |
| `joins_lab.py run.io_bound(run_cross_side_core)` | `apply_cross_side` over web page contract | `run.io_bound(run_cross_side_core)` at line 946; literal name preserves CI guard | VERIFIED | `test_joins_lab_off_loop.py` PASSES |
| `search_results.py card` | `/joins-lab?sys_id=` | `ui.navigate.to(url, new_tab=True)` on no-joins path | VERIFIED | Line 673; `find_joins_url` forwarded to `create_joins_dialog` on joins path |
| `browse.py` | `create_joins_button(find_joins_url=...)` | `_joins_lab_url = f'/joins-lab?sys_id={page.sys_id}'` | VERIFIED | Line 3904 + 3914 |
| `joins_panel.py` confirmed_only | `get_fragment_joins(status='confirmed')` | `if confirmed_only: joins = get_fragment_joins(..., status='confirmed')` at line 113-114 | VERIFIED | Tested by `test_confirmed_path_passes_status_confirmed_to_get_fragment_joins` |
| `joins_panel.py` cache key | isolated `:confirmed` suffix | `cache_key = f"{base_key}:confirmed" if confirmed_only else base_key` at line 96 | VERIFIED | Tested by `test_confirmed_only_uses_separate_cache_key` |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `known_joins_group.py` | `data['joins']` | `fetch_connected_fragments(confirmed_only=True)` via Supabase + SQLite | Supabase query at `joins_panel.py:114` + FJMS + community joins | FLOWING — data comes from real DB queries, not stubs |
| `joins_lab.py` | `raw_results` | `executor.execute_search(...)` inside `run_search_core` | Real Tantivy + regex engine | FLOWING — off-loop search |
| `joins_lab.py` | `merge_result.candidates` | `apply_cross_side(executor, base, b_query, b_ro, combine)` inside `run_cross_side_core` | Real search via executor | FLOWING |
| `search_results.py` joins icon | `data.get('total_joins', 0)` | `fetch_connected_fragments(shelfmark=sm, document_id=s)` via `run.io_bound` | Supabase query | FLOWING — count loaded off-loop |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `_apply_modifiers_to_term` hoist rules | `python -c "from web.components.joins_builder import _apply_modifiers_to_term as f; assert f('שלום', {'negation': True}) == '-שלום'; print('ok')"` | ok | PASS |
| `_merge_globals_web` sets flex_spacing/bidirectional, not ja | `python -c "from web.pages.joins_lab import _merge_globals_web; ro={'flex_spacing':False,'bidirectional':False,'ja':False,'variants':True}; _merge_globals_web(ro,{'flex_spacing':True}); assert ro['flex_spacing']==True and ro.get('ja')==False; print('ok')"` | ok | PASS |
| `badge_for_source` colors match UI-SPEC | `python -c "from web.components.known_joins_group import badge_for_source; assert badge_for_source('PGP')[1]=='#1d4ed8' and badge_for_source('community')[1]=='#525252'; print('ok')"` | ok | PASS |
| Phase 118 full test suite (34 tests) | `pytest tests/test_merge_globals_web.py tests/test_other_side_page_contract.py tests/test_builder_modifier_hoist.py tests/test_known_joins_group.py tests/test_joins_anc05_rls.py` | 34 passed in 3.22s | PASS |
| CI guards (off-loop + multitenant) | `pytest tests/test_no_raw_storage_access.py tests/test_joins_lab_off_loop.py tests/test_joins_lab_page.py` | 49 passed in 2.98s | PASS |

### Probe Execution

Step 7c: SKIPPED — no conventional `scripts/*/tests/probe-*.sh` files for this phase. Phase 118 is a web UI phase; its automated verification is the test suite above.

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| ANC-04 | Plans 02, 04 | Anchor pane shows known joins as connected group with source attribution (PGP/FJMS/user/community) | SATISFIED | `known_joins_group.py` + `joins_panel.py` community merge + `test_known_joins_group.py` 5/5 PASS |
| ANC-05 | Plans 01, 02, 04 | Multitenant-safe: status='confirmed' filter + isolated cache key | SATISFIED | `joins_panel.py:96,113-114` + `test_joins_anc05_rls.py` 5/5 PASS |
| FND-04 | Plan 05 | "Find joins" action on /search result cards | SATISFIED | `search_results.py:635,686` — icon + off-loop count + find_joins_url wired |
| FND-05 | Plan 05 | "Find joins" action on /browse | SATISFIED | `browse.py:3904-3914` — find_joins_url with volume_ie |
| BLD-02 | Plans 01, 04 | Other-side builder over web page contract (p_num, multi-IE, metadata-only) | SATISFIED | `joins_lab.py:933-946` + `test_other_side_page_contract.py` 8/8 PASS |
| BLD-03 | Plans 01, 03, 04 | Per-line modifiers hoist into compose()-recognised forms | SATISFIED | `joins_builder.py:39-84` + `test_builder_modifier_hoist.py` 11/11 PASS |
| BLD-04 | Plans 01, 04 | Global toggles (flex_spacing/bidirectional, not JA) applied to BOTH sides via _merge_globals_web | SATISFIED | `joins_lab.py:95-111,829,937` + `test_merge_globals_web.py` 5/5 PASS |

No orphaned requirement IDs — all 7 phase requirement IDs are accounted for.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found in phase-modified files | — | — | — | All scanned files clean: no TBD/FIXME/XXX markers, no placeholder returns, no hardcoded empty state flowing to renders |

Full anti-pattern scan run on: `joins_builder.py`, `known_joins_group.py`, `joins_panel.py`, `joins_lab.py`, `search_results.py`, `browse.py`. Zero hits on `TBD|FIXME|XXX`. Gap-control WR-05 fix confirmed: `_on_gap_change` updates border in-place (not `_render_rows`). Dead community resolution block (CR-02/IN-01) confirmed removed — replaced by `_resolve_shelfmark_to_sys_id(member_shelfmark)` at `joins_panel.py:327`.

### Review Findings Remediation Check

All 8 Critical + Warning findings from `118-REVIEW.md` are confirmed fixed:

| Finding | Description | Fixed Evidence |
|---------|-------------|----------------|
| CR-01 | `compose()` uncaught ValueError crashes Run Search handler | `joins_lab.py:817-825` — `try/except ValueError` with `ui.notify` |
| CR-02 | Community member empty sys_id corrupts re-anchor | `joins_lab.py:590` — `if not sys_id: return`; `joins_panel.py:327` — real resolution; `known_joins_group.py:191` — pin hidden when no sys_id |
| WR-01 | Button re-enabled before cross-side leg completes | `joins_lab.py:844-988` — single outer try/finally wraps both legs |
| WR-02 | Stale candidates/summary bar after re-anchor | `joins_lab.py:600-610` — `candidates_container.clear()` + visibility resets in `load_anchor` |
| WR-03 | Text Position labels frozen at import time (wrong language) | `joins_builder.py:148-150` — `_text_position_options()` function called at render time |
| WR-04 | Summary bar hardcoded English bypasses tr() | `joins_builder.py:197-212` — all literals through `tr()` with plural forms |
| WR-05 | Gap input re-renders all rows on keystroke | `joins_builder.py:329-340` — in-place border style update only |
| WR-06 | create_joins_dialog from search card omits pgpid | `search_results.py:649-660` — `_joins_pgpid` resolved and passed |

3 Info items (IN-02/IN-03/IN-04) remain as optional follow-up per the REVIEW.md declaration — none are blockers.

### Human Verification Required

Items requiring live browser and/or Supabase data:

### 1. Known-Joins Group Visual Rendering + Re-Anchor Interaction

**Test:** Load `/joins-lab?sys_id=<fragment-with-joins>`. Confirm the Known Joins expansion shows source badges with correct colors (PGP blue #1d4ed8, FJMS purple #7e22ce, user emerald #047857, community grey #525252). Click a member's push_pin icon.
**Expected:** Four distinct colored badges render. Clicking push_pin reloads the anchor pane to the member fragment. Builder rows are preserved (D-16).
**Why human:** Color rendering and NiceGUI interactive navigation state cannot be verified headlessly.

### 2. Find Joins from Search Card — Joins Exist Path

**Test:** Open `/search`, find a result card for a fragment WITH known joins. Click the link icon.
**Expected:** A joins dialog opens. Inside it, a "Find more joins" button appears. Clicking it opens `/joins-lab` in a NEW browser tab pre-loaded on that fragment. Original search page is preserved.
**Why human:** Dialog rendering + new-tab navigation require a live browser.

### 3. Find Joins from Search Card — No Joins Path

**Test:** Open `/search`, find a result card for a fragment WITHOUT known joins. Click the link icon.
**Expected:** No near-empty dialog — clicking goes straight to `/joins-lab` in a NEW browser tab. The card's link icon should be neutral color (not green).
**Why human:** Whether the lazy joins-count load correctly detected zero joins (requiring a live Supabase call after the 0.15s timer) and reconfigured the click handler requires browser + data.

### 4. Find Joins from Browse

**Test:** Open `/browse` on a fragment. Observe the joins button color (green when joins exist, neutral when none). Click the button with joins present — confirm dialog + "Find more joins" button. With no joins — confirm new tab to /joins-lab.
**Expected:** Button tint correct. New tab behavior works. No duplicate joins button added.
**Why human:** Color rendering and new-tab navigation need a live browser.

### 5. Builder Summary Bar Bilingual Collapse

**Test:** Open Joins Lab with Hebrew UI. Build 2+ lines and run search. Observe the collapsed summary bar.
**Expected:** Summary bar text is in Hebrew (not English), showing mode, line count, and text position in Hebrew. Clicking "Edit" (or Hebrew equivalent) re-expands the builder showing the previously typed rows.
**Why human:** Bilingual rendering (Hebrew RTL) requires a browser render with the Hebrew locale active.

### 6. Global Toggle End-to-End (BLD-04)

**Test:** In Advanced search options, enable "Flexible spacing". Run a search. Inspect network request or server logs to confirm `flex_spacing=True` in the executed `responsa_options`.
**Expected:** The search request carries `flex_spacing: true` in its options.
**Why human:** Network payload inspection requires browser dev tools; the unit test confirms the function works in isolation but not the full browser→server wiring.

---

## Gaps Summary

No gaps found. All 5 observable truths are VERIFIED by codebase evidence. All 7 requirement IDs are SATISFIED. All 8 Critical+Warning code review findings are confirmed remediated. The phase test suite (34 tests) and all CI guards pass.

The `human_needed` status reflects 6 visual/interactive/browser-level behaviors that require a live browser session to confirm. These are normal UI verification items that cannot be checked headlessly — they do not indicate any code defect.

---

_Verified: 2026-06-18_
_Verifier: Claude (gsd-verifier)_
