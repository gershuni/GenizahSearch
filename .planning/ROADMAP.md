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
- ✅ **v8.3.0 God-File Decomposition + Search & Browse UX** -- Phases 122-129 (shipped 2026-06-29, both apps; closed 2026-06-30). Decomposition (122-127, zero behavior change) + SEED-025 Space-scroll + SEED-026 library filter. See `.planning/milestones/v8.3.0-ROADMAP.md`.
- 🚧 **v8.4.0 Dual-Mode Library Filter** -- Phases 130-132 (in progress). Evolve the v8.3.0 inclusion-only allowlist into a dual-mode (Show-only / Hide) library filter persisted across searches, at full web + desktop parity, plus a `mode` (include/exclude) on the public API. Evolution of SEED-026.

## Phases

<details>
<summary>✅ v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search (Phases 117-121) — SHIPPED 2026-06-23, both apps</summary>

See: .planning/milestones/v8.2.0-ROADMAP.md

5 phases (117-121). Ported the desktop Joins Lab (Component A) to the web at `/joins-lab` at full parity — anchor pane + line-by-line builders for both leaf sides + deduped candidate grid/table + side-by-side Compare + Visual Similarity toggle + Add-as-Join/Puzzle/list; bilingual EN/HE + RTL, no login, server-side per-session state via `safe_storage`. Bundled beyond scope: FGP transcriptions go-live (both apps), SEED-006 Hebrew/Judeo-Arabic search, Responsa-operators-over-My-Library (desktop). Phase dirs archived to `.planning/milestones/v8.2.0-phases/`.

</details>

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

<details>
<summary>✅ v8.3.0 God-File Decomposition + Search & Browse UX (Phases 122-129) — SHIPPED 2026-06-29, both apps; closed 2026-06-30</summary>

See: .planning/milestones/v8.3.0-ROADMAP.md

8 phases (122-129). Two strands shipped together as the public 8.2.2→8.3.0 release: (1) god-file decomposition (122-127) — split genizah_app.py + genizah_core.py into cohesive shared/+desktop/ modules behind permanent re-export facades, zero behavior change; (2) Search & Browse UX (128-129) — SEED-025 Space-key results scroll + SEED-026 library filter (web /search + Browse-by-Identification + desktop catalog + filters.library API), both apps. Also shipped: SEED-017 Joins-Lab viewer rotate/fullscreen, SEED-024 desktop Joins-Lab parity + XLSX export, SEED-015 desktop image NLI breaker.
</details>

---

### 🚧 v8.4.0 Dual-Mode Library Filter (Phases 130-132) — In Progress

**Milestone goal:** Library filtering can express BOTH "show only these libraries" and "hide these libraries" intents, persisted so each survives across searches, at full web + desktop parity — closing the v8.3.0 gap where the inclusion-only allowlist (over a result-derived universe) could not represent a sticky "exclude library X". Inclusion = allowlist; Hide = denylist (consistent with the existing `domain_exclusions` / printed-filter exclusion semantics). Evolution of the shipped SEED-026 — no new domain research needed. Spec lineage: `.planning/seeds/SEED-026-*.md` + the 2026-06-29 web-Parallels library-filter gap in `docs/OPEN_ISSUES.md`.

**Hard constraints across all phases (carry into every phase's success criteria):**

- **D-46 / D-NEW-7 `'LOCAL'` guard (DMF-10):** `'LOCAL'` (My Library) never appears as a web library-filter option in ANY mode or surface; `tests/test_web_library_options_no_local.py` + `tests/test_phase_97_invariants.py` stay green. (This tripped the release-commit CI in v8.3.0 — do not regress.)
- **Phase 87 multitenant invariant:** all per-user web state goes through the `web/safe_storage.py` chokepoint; `tests/test_no_raw_storage_access.py` allowlist stays `[]`.
- **Web + desktop parity:** the (mode + set) model defined by the lead phase (Phase 130) is the shared shape mirrored by the desktop catalog dialog and the web parity surfaces; both apps must stay in parity for the model.
- **Backward compatibility:** existing v8.3.0 persisted allowlist values and the existing API `filters.library` allowlist behavior must keep working unchanged (omitted mode = include = today's behavior).

## Summary Checklist

- [x] **Phase 130: Dual-Mode Filter Core — Web `/search`** *(lead)* - Define the shared (mode + set) state shape, add the Show-only / Hide mode toggle to the `/search` library-filter dialog, persist (mode + set) via `safe_storage`, migrate the existing allowlist cleanly, handle edge states, and make the button/label communicate the active mode + count. Settles the model the other surfaces mirror. (completed 2026-06-30)

- [ ] **Phase 131: Dual-Mode Parity — Desktop Catalog + Web Browse-by-Identification + Web `/parallels`** - Extend the Phase-130 (mode + set) model to the three remaining UI surfaces: the desktop catalog `LibraryFilterDialog` (Browse-by-Identification), the web Browse-by-Identification catalog filter, and a NEW web `/parallels` library-filter control (scoping via the existing `restrict_sys_ids` path) — each persisted, each at parity with the lead. (executed 2026-06-30 — code-verified 6/6; awaiting human UAT — see 131-HUMAN-UAT.md)

- [ ] **Phase 132: Public API Dual-Mode (`/api/search` + `/api/parallels`)** - Add an optional library-filter `mode` (include / exclude) alongside `filters.library` on both public endpoints; backward-compatible (omitted = include); `exclude` resolves to the complement (sys_ids whose `library_code` is not in the set) intersected into `restrict_sys_ids`. Documented in `docs/SEARCH_API.md` + the skill `api_contract.md`.

## Phase Details

### Phase 130: Dual-Mode Filter Core — Web `/search`

**Goal**: The web `/search` library filter can express BOTH "show only these libraries" (allowlist) and "hide these libraries" (denylist) — chosen via a mode toggle in the filter dialog, persisted (mode + set) across searches and reloads, with the existing allowlist migrated cleanly and edge states handled predictably. This phase defines the shared (mode + set) state shape and the dialog UX that Phase 131 mirrors on every other surface.
**Depends on**: Phase 129 (v8.3.0 SEED-026 inclusion-only allowlist shipped — this evolves it)
**Requirements**: DMF-01, DMF-02, DMF-03, DMF-04, DMF-05, DMF-06, DMF-10
**Success Criteria** (what must be TRUE):

  1. In the `/search` library-filter dialog the user can choose between **"Show only selected"** and **"Hide selected"** (bilingual EN/HE). In Hide mode, a library that surfaces in a later result set but is NOT in the hidden set is shown by default (the "hide RNL" intent persists as new libraries appear); in Show-only mode, only the selected libraries are shown. (DMF-01, DMF-02)
  2. The chosen mode AND the selected set survive across searches and a full page reload — persisted through the `web/safe_storage.py` chokepoint (no raw `app.storage.user`; `tests/test_no_raw_storage_access.py` allowlist stays `[]`). (DMF-03)
  3. The `/search` library-filter button (and any chip/label) clearly communicates the active mode and count — e.g. "Hiding N" vs "Showing N/total" — and shows a neutral state when no filter is active. (DMF-04)
  4. An existing v8.3.0 persisted allowlist (`search_library_filter`) loads cleanly into the new (mode + set) model without error, interpreted as **Show-only with the existing set** — verified by a migration test feeding the legacy value shape. (DMF-05)
  5. Edge states behave predictably: an empty selection in Show-only means "show all" (no collision with the all-unchecked sentinel), and a fully-populated Hide set (everything hidden) is handled without crash or contradictory display. (DMF-06)
  6. `'LOCAL'` is absent from the library-filter options in BOTH modes; `tests/test_web_library_options_no_local.py` + `tests/test_phase_97_invariants.py` stay green. (DMF-10)

**Plans**: 3 plans
Plans:
**Wave 1**

- [x] 130-01-PLAN.md — Model layer: `library_mode` field on `SearchUIState` + mode-aware `clear_search_snapshot` reset default

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 130-02-PLAN.md — `search.py` runtime: mode-aware restore + legacy migration, mode-branch filter, dict persist shape, redesigned dialog (mode toggle + count-shortlist + expand-all + text search), 3-state button

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 130-03-PLAN.md — Tests: new dual-mode behavior/migration/edge-state/LOCAL-guard suite + revise stale inclusion-only assertions in the existing libfilter test

**UI hint**: yes

### Phase 131: Dual-Mode Parity — Desktop Catalog + Web Browse-by-Identification + Web `/parallels`

**Goal**: The (mode + set) model from Phase 130 reaches the three remaining filter surfaces at parity: the desktop catalog `LibraryFilterDialog` (Browse-by-Identification), the web Browse-by-Identification catalog filter, and a NEW library-filter control on the web `/parallels` page (which scopes results through the existing `restrict_sys_ids` path) — each persisted for its surface.
**Depends on**: Phase 130 (shared (mode + set) model settled; `library_codes_with_manuscripts()` utility built)
**Requirements**: DMF-07, DMF-08, DMF-09, DMF-10, DMF-12, DMF-13
**Success Criteria** (what must be TRUE):

  1. The desktop catalog `LibraryFilterDialog` (`desktop/dialogs_filter.py`, Browse-by-Identification) offers the same Show-only / Hide modes as web `/search`; the chosen mode + set persist and re-apply on reopen, at model parity with the web lead. (DMF-07)
  2. The web Browse-by-Identification catalog filter offers the same Show-only / Hide modes over the full canonical library list, persisted, composing with the existing SEED-023 PGP/Editions filters without regression. (DMF-08)
  3. The web `/parallels` page has a library-filter control using the same dual-mode model; selecting libraries (Show-only) or hiding them (Hide) scopes the parallels results via the existing `restrict_sys_ids` compute path, and the selection persists for the page — closing the v8.3.0 deferred gap logged in `docs/OPEN_ISSUES.md` (2026-06-29). (DMF-09)
  4. On every web surface, `'LOCAL'` is absent from the options in BOTH modes; `tests/test_web_library_options_no_local.py` + `tests/test_phase_97_invariants.py` stay green; all web per-surface persistence goes through `web/safe_storage.py` (allowlist `[]`). (DMF-10)
  5. The web Browse-by-Identification catalog library filter gains the `/search` dialog affordances: a client-side text-search input, a per-library fragment/result count on the shortlist, and sort-by-count / sort-A–Z. (DMF-12)
  6. On every surface (catalog / `/parallels` / desktop), libraries with zero corpus manuscripts are excluded from the filter universe, reusing the shared `library_codes_with_manuscripts()` built in Phase 130 (web `/search` already done). (DMF-13)

**Plans**: 7 plans (5 base + 2 gap-closure)
Plans:
**Wave 1**

- [x] 131-01-PLAN.md — Wave-0 test scaffolds: new catalog + parallels dual-mode test files + extend desktop libfilter tests (pure mirrors + AST source contracts)
- [x] 131-02-PLAN.md — Shared `get_browse_results` `library_mode` param (EXISTS/NOT EXISTS, backward-compatible) for the server-side catalog/desktop surfaces

**Wave 2** *(blocked on Wave 1)*

- [x] 131-03-PLAN.md — Desktop catalog `LibraryFilterDialog` dual-mode (toggle + D-04 reset + mode-aware OK + 3-state button + `_catalog_library_mode` threaded into the worker) (DMF-07/10/13)
- [x] 131-04-PLAN.md — Web Browse-by-Identification catalog dual-mode dialog (toggle + text-search + count-shortlist + expand-all + sort + dict persist + migration) (DMF-08/12/10/13)
- [x] 131-05-PLAN.md — Web `/parallels` NEW dual-mode library control (button + dialog + post-fetch filter + `parallels_library_filter` persistence) (DMF-09/10/13)

**Gap closure** *(UAT 2026-06-30 — DMF-07/DMF-12 desktop facet parity)*

- [x] 131-06-PLAN.md — Desktop catalog `LibraryFilterDialog` dynamic per-library COUNTS (`Name (count)`) via `fjms.get_browse_library_facets`, computed off the UI thread, honoring the active PGP/Editions/domain filters — at parity with web /catalog (DMF-07/DMF-12)
- [x] 131-07-PLAN.md — Desktop catalog `LibraryFilterDialog` type-to-find search box + A-Z / By-count sort toggle (mirroring web `catLibFilterSearch` + `catLibFilterSort`; By count via `self._facets` desc, A-Z fallback; check-state preserved, Select All ignores filter) — at parity with web /catalog (DMF-07)

**UI hint**: yes

### Phase 132: Public API Dual-Mode (`/api/search` + `/api/parallels`)

**Goal**: Programmatic callers can express "hide these libraries" as well as "only these" — the public `POST /api/search` and `POST /api/parallels` accept an optional library-filter `mode` (include / exclude) alongside `filters.library`, backward-compatibly. `exclude` resolves to the complement (sys_ids whose `library_code` is not in the given set) intersected into `restrict_sys_ids`, mirroring the UI semantics.
**Depends on**: Phase 130 (mode semantics + complement resolution defined)
**Requirements**: DMF-11
**Success Criteria** (what must be TRUE):

  1. `POST /api/search` and `POST /api/parallels` accept an optional library-filter `mode` of `include` or `exclude` alongside `filters.library`; an omitted mode defaults to `include` and returns byte-for-byte the same behavior as today (backward-compatible — existing callers unaffected).
  2. With `mode=exclude` and a set of library codes, results are scoped to sys_ids whose `library_code` is NOT in the given set (the complement), intersected into the existing `restrict_sys_ids` path on both endpoints — verified by an API test that a `library` set returns disjoint result libraries under `include` vs `exclude`.
  3. An invalid `mode` value (anything other than `include`/`exclude`) is rejected with the standard 400 invalid-request envelope (fail-closed, consistent with the existing filter validation).
  4. The behavior is documented in `docs/SEARCH_API.md` and the skill `api_contract.md` (skills/cairo-genizah-research/), including the omitted-mode default and the exclude/complement semantics.

**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 130. Dual-Mode Filter Core — Web /search | 3/3 | Complete    | 2026-06-30 |
| 131. Dual-Mode Parity — Desktop + Browse + Parallels | 7/7 | Complete   | 2026-06-30 |
| 132. Public API Dual-Mode | 0/? | Not started | - |
