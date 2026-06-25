# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- **v8.1.0 Desktop Telemetry** -- Phases 111-116 (shipped 2026-06-16; closed 2026-06-16). See `milestones/v8.1.0-ROADMAP.md`
- ✅ **v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search** -- Phases 117-121 (shipped 2026-06-23, both apps)
- 🚧 **v8.3.0 God-File Decomposition** -- Phases 122-127 (in progress)

## Phases

<details>
<summary>✅ v8.1.0 Desktop Telemetry (Phases 111-116) — SHIPPED 2026-06-16, closed 2026-06-16</summary>

See: .planning/milestones/v8.1.0-ROADMAP.md

6 phases (111-116), 20 plans, 32 tasks. Opt-in, privacy-preserving desktop telemetry for "Dicta Genizah Search Pro" — anonymous usage analytics, crash reports, and per-session performance summaries flow to the shared web PostHog project (id 134161, EU), identity-aligned with the web app (logged-in users → same Supabase `user.id`), split by `platform=desktop`. Default OFF until the user consents via a bilingual first-run dialog; never transmits search content or My Library data. Also bundled: desktop "Public API & AI Tools" advertising and web Search API enhancements (quick task 260616-p9x) + the `platform=web` super-property.

</details>

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). Desktop Joins Lab: shared core (`shared/joins_lab.py`) + anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + Visual Similarity toggle. Component B (JSA-01/02/03 + JWB-05) and web Joins Lab UI deferred.

</details>

---

### ✅ v8.2.0 Web Joins Lab (Phases 117-121) — SHIPPED 2026-06-23 (both apps)

> Shipped 2026-06-23 as **v8.2.0: Web Joins Lab, FGP Transcriptions & Hebrew Search**. Desktop: GitHub Release `v8.2.0` @ `e01bcf40` + installer (FGP DB bundled). Web: deployed `e01bcf40` + GENIZAH index rebuilt with the SEED-006 `content_search` schema + FGP DB uploaded. Bundled beyond the milestone's web-Joins-Lab scope: FGP transcriptions go-live (both apps), the SEED-006 Hebrew/Judeo-Arabic search rollout, Responsa-operators-over-My-Library (desktop), and fixes (My Library re-index crash, Fragment Puzzle per-machine crash, telemetry focus-crash).

**Milestone goal:** Port the desktop Joins Lab (Component A) to the web at full parity — a human-in-the-loop join-hunting workbench where a scholar pins one anchor fragment (image + numbered transcription) and drives GenizahSearch's existing search tools to find the fragments that physically join it. Rides the complete, web-reusable `shared/joins_lab.py` core (v8.0.0 Phase 106). Human-in-the-loop only — no automated join-finder. Bilingual from line one. Persistence device-local (browser-session, server-side via `safe_storage`) for anonymous users with no login wall.

**Hard constraints across all phases:**

- All per-user state through `web/safe_storage.py` chokepoint (zero raw `app.storage.user`, CI-guarded by `tests/test_no_raw_storage_access.py`)
- All NLI/IIIF image fetches through existing per-provider proxies + Phase-98 circuit breaker (never unguarded)
- Search always off the event loop (`run.io_bound`-style); event loop never blocked
- Bilingual EN/HE with correct RTL layout throughout
- No new Supabase schema; no automated join-finder

## Summary Checklist

- [x] **Phase 117: Vertical Spine** - `/joins-lab` route + safe_storage schema + `WebSearchExecutor` adapter (off-loop) + anchor image/transcription + minimal anchor-side builder + search → candidate grid. Proves the riskiest seam (FND-01 adapter) end-to-end. **COMPLETE 2026-06-17**
- [x] **Phase 118: Joins, Entry & Full Builders** - Known-joins group (PGP + FJMS + user + community, multitenant-safe) + "Find joins" from `/search` and `/browse` + other-side builder with web page contract + per-line modifiers + global toggles (variants/JA/spacing/bidirectional) applied correctly. (completed 2026-06-18)
- [x] **Phase 119: Candidates, Compare & Visual Similarity** - Full candidate surface (grid+table, sys_id-keyed triage, self-match, filters, pagination, off-loop enrichment) + side-by-side Compare (per-pane zoom/nav, verdict sync) + VS toggle (merge look-alikes, eye badge everywhere). (completed 2026-06-19)
- [x] **Phase 120: Actions & Persistence** - Add-as-Join (login-gated) + bulk puzzle handoff + add-to-list / export + builder/triage/view state survive refresh (server-side per-session; restore prefers an instant per-user results snapshot, re-run as fallback) + clear/reset. **COMPLETE 2026-06-21** — 5/5 success criteria verified (120-VERIFICATION.md); ~6 rounds live UAT folded in (selection bulk toolbar, results-persist-across-navigation, builder/triage/single_text/search_type restore, RTL prev/next in Compare+Grid, HE string fixes, Compare outer-scroll layout).
- [x] **Phase 121: i18n Polish** - Complete bilingual EN/HE coverage + RTL layout verification + Hebrew-leak audit across all surfaces. (completed 2026-06-21)

## Phase Details

### Phase 117: Vertical Spine

**Goal**: Scholars can navigate to `/joins-lab`, load an anchor fragment by shelfmark or sys_id, see its image and numbered transcription, type lines into a minimal query builder, run a search, and see a deduped candidate grid — all without login, all with state correctly isolated through `safe_storage`. This end-to-end working slice proves the riskiest seam (the `WebSearchExecutor` adapter) at the start of the milestone.
**Depends on**: Nothing (first phase of this milestone)
**Requirements**: FND-01, FND-02, FND-03, FND-06, FND-08, ANC-01, ANC-02, ANC-03, BLD-01, BLD-05, CND-01, CND-02
**Success Criteria** (what must be TRUE):

  1. A scholar opens `/joins-lab?sys_id=T-S+12.123` (or equivalent shelfmark param) in a browser without logging in and sees the anchor fragment's image with zoom/pan controls and folio navigation — loaded through the existing per-provider image-proxy resolution (NLI/Oxford/Cambridge/Manchester/JTS), never via a direct unguarded IIIF URL.
  2. The anchor pane shows the fragment's transcription as right-aligned (RTL) numbered lines alongside the image.
  3. The `WebSearchExecutor` adapter satisfies the `shared/joins_lab.py` `SearchExecutor` Protocol by wrapping `state.searcher.execute_search` directly off the event loop (`run.io_bound`-style with timeout, cancellation, and stale-generation handling); a CI test asserts no raw `app.storage.user` access is added under `web/` (`tests/test_no_raw_storage_access.py` allowlist stays `[]`), and a second CI test asserts the search call is not made on the event loop.
  4. A scholar types text into the minimal anchor-side line builder, triggers a search, and sees a deduped one-per-image candidate grid with thumbnail and key metadata — the compose + execute + dedup pipeline is wired end-to-end.
  5. The `safe_storage` schema for Joins Lab state is defined and versioned (schema-version invalidation field present); all state reads/writes go through `safe_user_*` helpers; the page loads for two different anonymous sessions without any state bleed between them.
  6. The deep-link URL contract is explicit and documented: anchor identified by `sys_id` (with optional shelfmark / `fl_id` / page / `volume_ie`); `/search`, `/browse`, and cold-start all resolve the same anchor; builder/candidate/triage state is not encoded in the URL (device-local only).

**Plans**: 6 plans (2 waves)

- [x] 117-01-PLAN.md — WebSearchExecutor adapter (wraps state.searcher off-loop, not /api/search) + Protocol & off-loop-guard CI tests [Wave 1]
- [x] 117-02-PLAN.md — Versioned `joins_lab` safe_storage schema + invalidation / round-trip / no-state-bleed tests [Wave 1]
- [x] 117-03-PLAN.md — Extract per-provider image-URL resolver + promote RTL transcription helper from browse.py (browse stays green) [Wave 1]
- [x] 117-05-PLAN.md — Read-only candidate grid component (thumbnail + shelfmark + library chip + title + View-in-Browse) [Wave 1]
- [x] 117-06-PLAN.md — AnchorViewer component (image zoom/pan + folio nav + RTL transcription, proxy-only, idempotent head HTML) [Wave 2]
- [x] 117-04-PLAN.md — /joins-lab route + cold-start + builder + off-loop search → compose → dedup → grid + anchor persistence [Wave 2]

**UI hint**: yes

### Phase 118: Joins, Entry & Full Builders

**Goal**: Scholars see the anchor's known scientific joins (PGP + FJMS + user + community) as a connected group, can reach the Joins Lab directly from `/search` cards and `/browse`, and can build complete line-by-line queries for BOTH sides of the leaf — with per-line modifiers and global toggles that are never silently dropped.
**Depends on**: Phase 117
**Requirements**: ANC-04, ANC-05, FND-04, FND-05, BLD-02, BLD-03, BLD-04
**Success Criteria** (what must be TRUE):

  1. The anchor pane displays known joins as a connected group with per-join source attribution (PGP / FJMS / user-submitted / community), using the pairwise→group BFS model (no new schema).
  2. The known-joins display is multitenant-safe: only public/confirmed joins appear in the process-global path (or the cache is user/status-aware and isolated), so User A's unconfirmed (creator-only, RLS-scoped) proposed joins cannot appear in User B's session.
  3. "Find joins" actions appear on `/search` result cards and on `/browse` and open the Joins Lab pre-loaded with that fragment as the anchor, satisfying the FND-08 deep-link contract (sys_id param; `volume_ie` passed for multi-IE manuscripts; no builder/triage state in the URL).
  4. The other-side builder drives `resolve_other_side_pages` over a web-defined page contract (`p_num`-based not internal index, multi-IE-aware, defined behavior for unknown total-page counts and sparse/metadata-only pages); cross-side narrow/widen via `apply_cross_side` produces a proper subset (AND narrow) or superset (OR widen) of the anchor-only search.
  5. Per-line modifiers (line-start ⊢ / line-end ⊣ / plene-defective) on either side hoist into the Responsa-syntax forms `compose()` recognises; global toggles (variants, Judeo-Arabic, flexible spacing, bidirectional) apply to BOTH sides via a `_merge_globals`-equivalent (since `compose()` hardcodes JA/flex/bidirectional false) — a test enables each toggle and asserts the executed query string changes.

**Plans**: 5 plans (3 waves)

- [x] 118-01-PLAN.md — Wave-0 scaffolds: 5 RED test stubs + `fragment_joins.status` schema probe [Wave 0]
- [x] 118-02-PLAN.md — ANC-04/ANC-05: confirmed-only isolated-cache join path + source-badged known-joins group component [Wave 1]
- [x] 118-03-PLAN.md — BLD-03: line-builder widget (per-row modifiers + gap + prominent Text Position + Exact/Variants/Fuzzy) [Wave 1]
- [x] 118-04-PLAN.md — Integration: `_merge_globals_web` (BLD-04) + other-side cross-side (BLD-02) + known-joins wiring (ANC-04) into `/joins-lab` [Wave 2]
- [x] 118-05-PLAN.md — FND-04/FND-05: "Find joins" entry from `/search` cards + Quick View + `/browse` (new-tab deep link) [Wave 2]

**UI hint**: yes

### Phase 119: Candidates, Compare & Visual Similarity

**Goal**: Scholars can work a large candidate set efficiently — grid/table with persistent triage, filters, bounded rendering, off-loop enrichment — pull up a side-by-side Compare of anchor vs candidate, and toggle Visual Similarity to merge FIST look-alikes, with look-alikes badged consistently everywhere.
**Depends on**: Phase 117
**Requirements**: CND-03, CND-04, CND-05, CND-06, CND-07, CND-08, CMP-01, CMP-02, CMP-03, VSM-01, VSM-02
**Success Criteria** (what must be TRUE):

  1. The candidate surface offers grid and table views (table sortable, multi-select); triage verdicts and eye badges show consistently in both simultaneously — switching view never resets or hides per-candidate state.
  2. Triage verdicts (Yes / Maybe / No) are keyed by `sys_id`, reflected immediately in grid and table, and cleared on re-anchor; a self-match banner appears when `detect_self_match` finds the anchor among its own candidates.
  3. Candidate filters (material / dimensions / size-mismatch / triage state) narrow the display (persisting across grid/table, matching desktop Phase-108 semantics); a result cap and/or pagination prevents unbounded render, and neither the render loop nor enrichment blocks the NiceGUI event loop.
  4. Candidate metadata (shelfmark / title / library / material / dimensions / thumbnail) is enriched asynchronously off the event loop, in batches, with image/network lookups through the Phase-98 NLI circuit breaker — an NLI outage degrades thumbnails gracefully without stalling the surface.
  5. Opening Compare from any candidate (grid card, table row, or shortcut) shows a side-by-side anchor↔candidate panel (image + numbered transcription) with independent per-pane zoom and folio navigation; recording a Y/?/N verdict in Compare syncs back to the `sys_id`-keyed triage in grid and table with no refresh.
  6. A single Visual Similarity (👁) toggle merges FIST look-alikes for the loaded anchor via the web VS-service adapter + `merge_candidates` (off = text-only; on = merged / intersection); it tracks the loaded anchor sid so look-alikes invalidate on re-anchor, has explicit disabled / no-data / empty-intersection states, and badges look-alikes consistently across grid, table, and Compare.

**Plans**: 11 plans (4 waves) — 04 original + 04 round-1 gap closure (05-08) + 03 round-2 live-UAT gap closure (09-11)

- [x] 119-01-PLAN.md — Wave-0 foundation: badge_and_tooltip() in shared core + extend off-loop guard (VS + enrichment) + RED test scaffolds [Wave 0]
- [x] 119-02-PLAN.md — Candidate surface: large-thumbnail grid + sortable multi-select table + triage + filters + pagination + 👁 badge (candidate_grid.py) [Wave 1]
- [x] 119-03-PLAN.md — Compare modal: full-screen two-pane (two independent AnchorViewers) + flip-through + verdict auto-advance + badges (compare_modal.py) [Wave 1]
- [x] 119-04-PLAN.md — Page integration: off-loop VS adapter + enrichment batch + conditional merge + 👁 toggle + wire surface/Compare into joins_lab.py [Wave 2]
- [x] 119-05-PLAN.md — Round-1 gaps: candidate card snippet+highlight + image-click Compare + triage fill (G1/G3/G4) [Wave 1]
- [x] 119-06-PLAN.md — Round-1 gaps: Compare both-panes async load + verdict refresh + line-safe highlight (G5/G3-compare) [Wave 1]
- [x] 119-07-PLAN.md — Round-1 gaps: VS raw-baseline recompute + table render branch + anchor page/shelfmark + VS-meta enrichment (G2/A1/A2/A3/A4) [Wave 1]
- [x] 119-08-PLAN.md — Round-1 gaps: render-smoke harness (NiceGUI User, /joins-lab, mocked engine) [Wave 2]
- [x] 119-09-PLAN.md — Round-2 gap closure: HE Compare translations (R2-1) + shared TRIAGE_ICONS mapping [Wave 1]
- [x] 119-10-PLAN.md — Round-2 gap closure: card icon triage ✓/?/✗ + browse/compare icon buttons (R2-4/R2-9) + VS-only transcription (R2-8) + dark table (R2-10) + render-smoke [Wave 2]
- [x] 119-11-PLAN.md — Round-2 gap closure: Compare LTR counter+arrows (R2-2) + image-height cap (R2-3) + verdict icons (R2-4) + verdict border (R2-5) + single shelfmark (R2-6) + Esc-close (R2-7) + render-smoke [Wave 3]

**UI hint**: yes

### Phase 120: Actions & Persistence

**Goal**: Scholars can act on confirmed candidates (add-as-join, add anchor+candidates to the Puzzle, add-to-list / export) and find their builder inputs, triage, filters, and view restored on return — search re-run from persisted inputs — with a clear/reset escape hatch.
**Depends on**: Phase 118, Phase 119
**Requirements**: ACT-01, ACT-02, ACT-03, PST-01, PST-02, PST-03
**Success Criteria** (what must be TRUE):

  1. The "Add as Join" action connects to the existing pairwise-join community-write path (login-gated): an unauthenticated scholar sees a login prompt (not a silent failure or error page), and after login the new join appears in the anchor's known-joins group.
  2. The "Add to Puzzle" action sends the anchor AND all selected candidates to `/puzzle` via a NEW multi-fragment bulk staging payload/API (the prior `/puzzle?add=` accepted one fragment at a time); the anchor is always included regardless of which candidates are selected.
  3. The "Add to List" action adds selected candidates to a saved list (login-gated cloud write via the existing `/lists`); an "Export" action produces a downloadable CSV or XLSX of the candidate set with shelfmark, library, triage verdict, and key metadata columns.
  4. After a page refresh (or return to `/joins-lab` in the same browser session without logging in), the anchor, builder inputs (line text, modifiers, global toggles), triage verdicts, active filter, and view mode are all restored; the search is automatically re-run from the persisted inputs (not a stale result blob) — consistent with the desktop `join_workbench` restore pattern.
  5. Persistence uses server-side per-browser-session state through `web/safe_storage.py` (`safe_user_*`, keyed by the NiceGUI session cookie); the stored schema has an explicit version field for clean invalidation; the payload contains no `full_text` / image / result blobs (size cap enforced) and never leaks across sessions; a "Clear / Reset" control wipes all Joins Lab working state, and a test confirms the `safe_storage` keys are empty after reset.

Also folds in (user-directed, 2026-06-19/20): SEED-007 workbench actions (Make-an-anchor D-07, Browse-in-Compare D-08, Compare info buttons D-09, image prefetch D-10, Stop-with-partials D-11, hide VS toggle D-12), Lists↔Joins-Lab integration (picker D-17, sign-in fix D-18, /lists entry D-19), and stability close-out (SEED-008 D-20, deferred Phase-119 verification D-21).

**Plans**: 8 plans (7 waves — serialized on the shared `web/pages/joins_lab.py` page file)

- [x] 120-01-PLAN.md — PST storage extension: write_full_state/read_full_state under schema_version 1 + size caps + extended clear (PST-01/02/03, D-13/16) [Wave 1]
- [x] 120-02-PLAN.md — Page hardening: SEED-008 fire-and-forget guards (D-20) + sign-in route fix (D-18) + Stop-with-partials (D-11) [Wave 1]
- [x] 120-03-PLAN.md — Persistence wiring: save-on-change + auto re-run restore + restoring indicator + re-attach by sys_id (D-14/15) + Clear/Reset (D-16) [Wave 2]
- [x] 120-04-PLAN.md — ACT-01 Add-as-Join (proposed status, confirmed_only=False + force_refresh per D-02) + D-03 remove-my-join [Wave 3]
- [x] 120-05-PLAN.md — ACT-02 bulk Add-to-Puzzle: new multi-fragment safe_storage staging handoff (D-04) [Wave 4]
- [x] 120-06-PLAN.md — ACT-03 Add-to-List (D-05) + flat CSV/XLSX Export with off-loop batched transcription (D-06) [Wave 5]
- [x] 120-07-PLAN.md — SEED-007 Compare/workbench: Make-an-anchor (D-07) + Browse-in-Compare (D-08) + info buttons (D-09) + image prefetch (D-10) + hide VS toggle (D-12) [Wave 6]
- [x] 120-08-PLAN.md — Lists integration: choose-from-lists picker (D-17) + /lists entry (D-19) + deferred Phase-119 verification close-out (D-21) [Wave 7]

**UI hint**: yes

### Phase 121: i18n Polish

**Goal**: Every visible string in the Joins Lab is available in both English and Hebrew; RTL layout is correct throughout — anchor transcription, builder rows, candidate grid/table, Compare panes, dialogs and toasts; no Hebrew string leaks to the English interface and no English string to the Hebrew interface.
**Depends on**: Phases 117, 118, 119, 120
**Requirements**: FND-07
**Success Criteria** (what must be TRUE):

  1. Every UI string in the Joins Lab — labels, buttons, tooltips, toasts, error messages, placeholders — has both an EN and HE key in the project's `tr()` system; switching the app language updates all Joins Lab strings without a page reload.
  2. The Hebrew-interface layout is fully RTL: anchor transcription right-aligned, builder rows right-to-left, candidate grid/table headers + cells in RTL, Compare panes mirrored, no element clipped or overlapping from a direction mismatch.
  3. A static / AST audit confirms no raw Hebrew string literal appears in the Joins Lab page/component Python files outside the `tr()` system — every Hebrew string goes through `tr()` so it cannot leak into the English interface.

**Plans**: 3 plans (2 waves)

- [x] 121-01-PLAN.md — TRANSLATIONS gap closure: add 17 missing HE keys + fix 'Open in Joins Lab' glossary drift + tr()-wrap the XLSX sheet name [Wave 1]
- [x] 121-02-PLAN.md — Permanent CI guard tests/test_joins_lab_i18n.py (no-raw-Hebrew + tr()-coverage + badge-strings + scoped entry-point keys) + render-smoke RTL structural assertions [Wave 2]
- [x] 121-03-PLAN.md — HE-mode HUMAN-UAT checklist artifact + sign-off (load-bearing SC#2 visual-RTL acceptance) [Wave 2]

**UI hint**: yes

---

### 🚧 v8.3.0 God-File Decomposition (Phases 122-127) — In Progress

**Milestone goal:** Split the two god-files — `genizah_app.py` (~28k lines, desktop) and `genizah_core.py` (~12.5k lines, shared by both apps) — into cohesive modules using the proven v7.9 extract-behind-tests / one-atomic-commit-per-cluster recipe. Zero behavior change. Pure internal maintainability work — no user-facing change, no GitHub Release (label-only version bump). Strategy in `.planning/seeds/SEED-020-decomposition-map.md` (§7 "Codex review corrections" authoritative).

**Hard constraints across all phases (GUARD invariants):**

- GUARD-01: No module-level import back-edges — no cycle through any extracted `shared/` module. AST/import guard installed in Phase 122 and enforced at every phase boundary.
- GUARD-02: Zero behavior change — full existing pytest suite (search / browse / responsa / joins / lists / composition parity, web + desktop import paths) passes at every phase boundary.
- GUARD-03: Every source-scanning / AST test that reads `genizah_core.py` or `genizah_app.py` is retargeted to the new module location before the original implementation is deleted (5 named files: `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`). Retarget during the additive phase; flip at deletion.
- GUARD-04: `genizah_core.py` remains a permanent compatibility facade (re-export shims preserved); `genizah_app.py` implementation shims are removed in a clean final deletion pass.
- Per-commit discipline: copy-not-move → retarget importers → add `# noqa: F401` re-export shim → (next phase) delete original + add AST guard, one atomic commit per cluster; per-file ruff review only (never repo-wide `ruff --fix`).

## Summary Checklist (v8.3.0)

- [ ] **Phase 122: Config Enabler** - Extract `Config` to `shared/config.py`; install back-edge AST guard (GUARD-01). Breaks the import-cycle pivot that blocks all subsequent core moves.
- [ ] **Phase 123: Core Leaf Modules** - Extract seven low-risk, well-tested clusters: `shared/variants.py`, `shared/codicological.py`, `shared/responsa.py`, `shared/joins_manager.py`, `shared/lists_manager.py`, `shared/browse_map_utils.py`, `shared/text_normalize.py`. Proves the shim+guard pipeline on core before touching engines.
- [ ] **Phase 124: Core Metadata & Index** - Extract `shared/metadata_manager.py` (+ `_BoundedLRUCache`) and `shared/indexer.py`; retarget the `shared/local_indexer.py` lazy back-edges into `genizah_core` helpers.
- [ ] **Phase 125: Core Engines** - SEED-011 composition dedup first (125a), then extract `shared/search_engine.py` (DI + BrowseMap cache + SEED-006 gates + `_LAST_RESPONSA_DOWNGRADE` preserved), `shared/lab_settings.py`, `shared/lab_engine.py` (LOCAL-LAB mirror preserved), and model `_my_library_tab_ref` as an injected optional interface for both engines.
- [ ] **Phase 126: Desktop Panels** - Extract seven desktop panel clusters to `desktop/`: `settings_dialogs.py`, `ui_widgets.py`, `catalog_browse.py`, `search_results_panel.py`, `browse_panel.py`, `reading_desk_panel.py`, `lists_tab.py`.
- [ ] **Phase 127: Update UI & Final Cleanup** - Extract `desktop/update_ui.py` + new direct behavioral tests for sidecar reset/download coordination; remove all desktop shims from `genizah_app.py`; confirm `genizah_core.py` permanent facade; full-suite-green sign-off.

## Phase Details (v8.3.0)

### Phase 122: Config Enabler

**Goal**: `Config` lives in `shared/config.py`; all existing callers continue working via the `genizah_core.Config` re-export facade; and a permanent AST guard (GUARD-01) is installed to catch any future module-level back-edges from extracted `shared/` modules back into `genizah_core`.
**Depends on**: Phase 121 (v8.2.0 complete — no active code changes in flight)
**Requirements**: CONFIG-01, GUARD-01, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `shared/config.py` exists and defines the `Config` class; `genizah_core.Config` is a re-export of the same class object (not a copy); a test imports both and asserts `shared.config.Config is genizah_core.Config`.
  2. All existing callers of `from genizah_core import Config` (including `shared/session_persistence.py:32`) continue to work without modification — the full existing pytest suite passes.
  3. A permanent CI test (`tests/test_no_back_edges_core.py`) is installed and green: it asserts no extracted `shared/` module imports `genizah_core` at module level (AST scan); it is parametrized so adding a new `shared/` module automatically enters the scan.
  4. Per-file ruff review on the extraction commit shows zero unintended F401 removals; the `# noqa: F401` shim in `genizah_core.py` is present and ruff-clean.

**Plans**: TBD

### Phase 123: Core Leaf Modules

**Goal**: Seven low-risk, well-tested core clusters are extracted to `shared/` behind re-export shims: `shared/variants.py` (`VariantManager`), `shared/codicological.py` (`CodicologicalManager`), `shared/responsa.py` (responsa parsing/expansion), `shared/joins_manager.py` (`JoinsManager`), `shared/lists_manager.py` (`ListsManager`), `shared/browse_map_utils.py` (browse-map + shelfmark utilities), and `shared/text_normalize.py` (`strip_nikud`, `strip_search_diacritics`, normalization constants). The lazy back-edges in `shared/local_indexer.py` that point at these helpers via `genizah_core` are retargeted to the new modules.
**Depends on**: Phase 122
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, CORE-05, CORE-06, CORE-07, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `from shared.responsa import ...`, `from shared.variants import VariantManager`, and `from shared.codicological import CodicologicalManager` all resolve; `from genizah_core import ...` for each of these names also resolves via the re-export shims; both paths produce the same class/function objects.
  2. The full responsa test suite (`tests/test_responsa_*.py`, ~3,271 test lines) passes — importing through either `genizah_core` or the new `shared.responsa` module.
  3. `shared/local_indexer.py` no longer imports these helpers via `genizah_core` at module level; the retargeted imports point directly at the new `shared/` modules; the GUARD-01 back-edge CI test remains green.
  4. All five source-scanning / AST tests that read `genizah_core.py` or `genizah_app.py` (`test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`) are retargeted to both locations during the additive phase and remain green — no deletion of originals until Phase 127.
  5. Per-file ruff review on each extraction commit shows no unintended stripping of `# noqa: F401` re-export shims in `genizah_core.py`; the full existing pytest suite passes at every cluster commit boundary.

**Plans**: TBD

### Phase 124: Core Metadata & Index

**Goal**: `MetadataManager` (and `_BoundedLRUCache`) are extracted to `shared/metadata_manager.py`, and `Indexer` is extracted to `shared/indexer.py`. These depend on `shared/config.py` (Phase 122) and are prerequisites for the engine moves in Phase 125.
**Depends on**: Phase 122, Phase 123
**Requirements**: CORE-08, CORE-09, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `from shared.metadata_manager import MetadataManager` and `from shared.indexer import Indexer` both resolve; `from genizah_core import MetadataManager` and `from genizah_core import Indexer` also resolve via re-export shims; both paths yield the same class objects.
  2. `_BoundedLRUCache` is co-located in `shared/metadata_manager.py` (not left as an orphan in `genizah_core.py`); the existing browse/API/NLI-breaker integration tests that exercise `MetadataManager` pass unchanged (`tests/test_browse_synthetic.py`, `tests/test_audit_followup_2026_05_29.py`, `tests/test_api_nli_breaker_integration.py`).
  3. `build_index.py` continues to resolve `Indexer.create_index` (smoke-importable); any existing direct `Indexer` coverage tests pass.
  4. No new module-level back-edge from `shared/metadata_manager.py` or `shared/indexer.py` into `genizah_core`; the GUARD-01 CI test remains green; per-file ruff review shows shims intact.

**Plans**: TBD

### Phase 125: Core Engines

**Goal**: The hardest core phase. SEED-011 composition double-prep dedup lands first (125a, before the engine code moves). Then `SearchEngine` is extracted intact to `shared/search_engine.py` with formalized `meta_mgr`/`var_mgr` dependency injection, and the three critical hazards explicitly preserved: the BrowseMap class-level cache migration, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local downgrade channel. `LabSettings` and `LabEngine` follow in the same phase, with the SearchEngine↔LabEngine LOCAL-LAB mirror (`_lab_weights_hash_override`, CR-01/CR-02) preserved intact. The `_my_library_tab_ref` coupling is modeled as an injected optional "local-search-gate" interface on both engines so no `shared/` module imports desktop code.
**Depends on**: Phase 124
**Requirements**: PREP-01, CORE-10, CORE-11, CORE-12, CORE-13, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. SEED-011 composition dedup is in place before any engine code moves: `corpus_scope='all'` composition no longer builds 2× the query set; the relevant composition tests pass.
  2. `from shared.search_engine import SearchEngine`, `from shared.lab_engine import LabEngine`, and `from shared.lab_settings import LabSettings` all resolve; `from genizah_core import SearchEngine` (and the other names) resolve via re-export shims; both paths yield the same class objects.
  3. The full search test suite passes — including `tests/test_search_api.py`, `tests/test_corpus_scope_routing.py`, `tests/test_cross_side_contract.py`, `tests/test_comp_corpus_scope.py`, `tests/test_lab_composition_chunk_hits.py`, and `tests/test_local_lab_invalidation.py` — with no behavior change in any search mode (keyword / Responsa / composition / parallels / Local / ALL).
  4. The BrowseMap class-level cache, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local channel all work identically after the move — no cache invalidation, no compat gate regression, no Responsa downgrade loss.
  5. `LabEngine.lab_composition_search()` and `SearchEngine.attach_my_library_tab()` accept the injected optional local-search-gate interface and function correctly; no `shared/` module imports `desktop/` or `genizah_app`; the GUARD-01 back-edge CI test remains green.

**Plans**: TBD

### Phase 126: Desktop Panels

**Goal**: Seven desktop panel clusters are extracted from `genizah_app.py` to `desktop/` modules: `desktop/settings_dialogs.py` (Settings/Help/Tabular-builder dialogs, D-07b telemetry snapshot stripping preserved), `desktop/ui_widgets.py` (table/header/scroll widget classes), `desktop/catalog_browse.py` (catalog Browse-by-Identification tab), `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`), `desktop/browse_panel.py` (browse panel), `desktop/reading_desk_panel.py` (reading desk), and `desktop/lists_tab.py` (lists tab + cloud-sync coordination). The v7.9 proven recipe (copy-not-move; shim; delete+guard next phase) applies. `pyqtSignal` worker classes stay at module level. `D3` (search results) is sequenced before `D4` (browse) because `browse_text` is shared.
**Depends on**: Phase 125
**Requirements**: DESK-01, DESK-02, DESK-03, DESK-04, DESK-05, DESK-06, DESK-07, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. Each of the seven `desktop/` modules exists and imports cleanly in isolation (no import of `genizah_app` at module level); `genizah_app.py` re-exports each panel class so all current `from genizah_app import ...` call sites (test files + `desktop/join_workbench.py` + 16+ other files) continue to work unchanged.
  2. The existing desktop panel test suites pass via the re-export shims: `tests/test_telemetry_consent_ux.py`, `tests/test_tabular_builder_rtl.py`, `tests/test_seed023_catalog_filters.py`, `tests/test_catalog_availability_filter.py`, `tests/test_browse_state.py`, `tests/test_browse_synthetic.py`, `tests/test_local_browse_panel.py`, `tests/test_wr01_open_local_browse_page_ast.py`, `tests/test_add_to_list_dialog_ui_context.py`, `tests/test_user_lists_*.py`.
  3. A new `tests/test_search_results_panel.py` (mock `SearchThread`) exercises `SearchResultsPanel` directly, imported from `desktop/search_results_panel.py` — the first panel to have a direct-module test.
  4. `pyqtSignal`-bearing worker classes (e.g. `_CatalogRefreshWorker`) remain at module level in their new `desktop/` home; the desktop app starts and the affected tabs are fully functional (desktop smoke-import + headless PyQt6 construction test green).
  5. The full existing pytest suite passes; per-file ruff review on each extraction commit shows no unintended shim stripping in `genizah_app.py`.

**Plans**: TBD

### Phase 127: Update UI & Final Cleanup

**Goal**: The last extractable desktop cluster — `desktop/update_ui.py` (notification bar, What's-New bar/dialog, update progress dialog, sidecar reset/download coordination) — lands with new direct behavioral tests for the sidecar reset/download coordination methods. Then: all implementation shims are removed from `genizah_app.py` (it keeps only thin `import ... as ...` re-exports for the deleted clusters); `genizah_core.py` permanent facade is confirmed intact; the GUARD-01 back-edge test and a new `tests/test_no_back_edges_desktop.py` guard are both green; the full pytest suite passes as the final sign-off.
**Depends on**: Phase 126
**Requirements**: DESK-08, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `desktop/update_ui.py` exists and imports cleanly; `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, and `UpdateProgressDialog` are importable from it; new direct behavioral tests covering the GUI sidecar reset/download coordination methods pass (SEED-020 §7 C-6 requirement).
  2. All `genizah_app.py` implementation shims (the desktop panel cluster code that was copied but not yet deleted in Phase 126) are removed in one clean commit; `genizah_app.py` contains only thin re-export lines for each extracted cluster; the file shrinks by at least 70% from its pre-milestone 28,033 lines.
  3. `genizah_core.py` permanent re-export facade is confirmed: `from genizah_core import Config`, `from genizah_core import SearchEngine`, and all other extracted names continue to resolve; `tests/test_genizah_core_facade.py` (new or updated) asserts the facade exports the same objects as the `shared/` modules.
  4. Both back-edge guards are green: `tests/test_no_back_edges_core.py` (GUARD-01, installed Phase 122) confirms no `shared/` module imports `genizah_core` at module level; `tests/test_no_back_edges_desktop.py` (new, this phase) confirms no `desktop/` module imports `genizah_app` at module level.
  5. The full existing pytest suite (all categories: search, browse, responsa, joins, lists, composition parity, web + desktop import paths) is green — the milestone's final zero-behavior-change sign-off.

**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 117. Vertical Spine | 6/6 | Complete | 2026-06-18 |
| 118. Joins, Entry & Full Builders | 6/6 | Complete | 2026-06-19 |
| 119. Candidates, Compare & Visual Similarity | 11/11 | Complete | 2026-06-19 |
| 120. Actions & Persistence | 7/8 | Complete | 2026-06-21 |
| 121. i18n Polish | 3/3 | Complete | 2026-06-21 |
| 122. Config Enabler | 0/TBD | Not started | - |
| 123. Core Leaf Modules | 0/TBD | Not started | - |
| 124. Core Metadata & Index | 0/TBD | Not started | - |
| 125. Core Engines | 0/TBD | Not started | - |
| 126. Desktop Panels | 0/TBD | Not started | - |
| 127. Update UI & Final Cleanup | 0/TBD | Not started | - |
