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
- 🚧 **v8.2.0 Web Joins Lab** -- Phases 117-121 (in progress)

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

### 🚧 v8.2.0 Web Joins Lab (Phases 117-121)

**Milestone goal:** Port the desktop Joins Lab (Component A) to the web at full parity — a human-in-the-loop join-hunting workbench where a scholar pins one anchor fragment (image + numbered transcription) and drives GenizahSearch's existing search tools to find the fragments that physically join it. Rides the complete, web-reusable `shared/joins_lab.py` core (v8.0.0 Phase 106). Human-in-the-loop only — no automated join-finder. Bilingual from line one. Persistence device-local (browser-session, server-side via `safe_storage`) for anonymous users with no login wall.

**Hard constraints across all phases:**

- All per-user state through `web/safe_storage.py` chokepoint (zero raw `app.storage.user`, CI-guarded by `tests/test_no_raw_storage_access.py`)
- All NLI/IIIF image fetches through existing per-provider proxies + Phase-98 circuit breaker (never unguarded)
- Search always off the event loop (`run.io_bound`-style); event loop never blocked
- Bilingual EN/HE with correct RTL layout throughout
- No new Supabase schema; no automated join-finder

## Summary Checklist

- [x] **Phase 117: Vertical Spine** - `/joins-lab` route + safe_storage schema + `WebSearchExecutor` adapter (off-loop) + anchor image/transcription + minimal anchor-side builder + search → candidate grid. Proves the riskiest seam (FND-01 adapter) end-to-end. **COMPLETE 2026-06-17**
- [ ] **Phase 118: Joins, Entry & Full Builders** - Known-joins group (PGP + FJMS + user + community, multitenant-safe) + "Find joins" from `/search` and `/browse` + other-side builder with web page contract + per-line modifiers + global toggles (variants/JA/spacing/bidirectional) applied correctly.
- [ ] **Phase 119: Candidates, Compare & Visual Similarity** - Full candidate surface (grid+table, sys_id-keyed triage, self-match, filters, pagination, off-loop enrichment) + side-by-side Compare (per-pane zoom/nav, verdict sync) + VS toggle (merge look-alikes, eye badge everywhere).
- [ ] **Phase 120: Actions & Persistence** - Add-as-Join (login-gated) + bulk puzzle handoff + add-to-list / export + builder/triage/view state survive refresh (server-side per-session, re-run on restore) + clear/reset.
- [ ] **Phase 121: i18n Polish** - Complete bilingual EN/HE coverage + RTL layout verification + Hebrew-leak audit across all surfaces.

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
- [ ] 118-03-PLAN.md — BLD-03: line-builder widget (per-row modifiers + gap + prominent Text Position + Exact/Variants/Fuzzy) [Wave 1]
- [ ] 118-04-PLAN.md — Integration: `_merge_globals_web` (BLD-04) + other-side cross-side (BLD-02) + known-joins wiring (ANC-04) into `/joins-lab` [Wave 2]
- [ ] 118-05-PLAN.md — FND-04/FND-05: "Find joins" entry from `/search` cards + Quick View + `/browse` (new-tab deep link) [Wave 2]

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

**Plans**: TBD
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

**Plans**: TBD
**UI hint**: yes

### Phase 121: i18n Polish

**Goal**: Every visible string in the Joins Lab is available in both English and Hebrew; RTL layout is correct throughout — anchor transcription, builder rows, candidate grid/table, Compare panes, dialogs and toasts; no Hebrew string leaks to the English interface and no English string to the Hebrew interface.
**Depends on**: Phases 117, 118, 119, 120
**Requirements**: FND-07
**Success Criteria** (what must be TRUE):

  1. Every UI string in the Joins Lab — labels, buttons, tooltips, toasts, error messages, placeholders — has both an EN and HE key in the project's `tr()` system; switching the app language updates all Joins Lab strings without a page reload.
  2. The Hebrew-interface layout is fully RTL: anchor transcription right-aligned, builder rows right-to-left, candidate grid/table headers + cells in RTL, Compare panes mirrored, no element clipped or overlapping from a direction mismatch.
  3. A static / AST audit confirms no raw Hebrew string literal appears in the Joins Lab page/component Python files outside the `tr()` system — every Hebrew string goes through `tr()` so it cannot leak into the English interface.

**Plans**: TBD
**UI hint**: yes

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 117. Vertical Spine | 6/6 | Complete    | 2026-06-18 |
| 118. Joins, Entry & Full Builders | 2/5 | In Progress|  |
| 119. Candidates, Compare & Visual Similarity | 0/TBD | Not started | - |
| 120. Actions & Persistence | 0/TBD | Not started | - |
| 121. i18n Polish | 0/TBD | Not started | - |
