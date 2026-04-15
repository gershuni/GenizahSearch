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
- **v7.8 Structural Foundation** -- Phases 63-66 (in progress)

## Phases

<details>
<summary>v1 External Data Integration (Phases 1-7) -- SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

<details>
<summary>v5.6.0 Desktop Parity & PGP Integration (Phases 8-12) -- SHIPPED 2026-02-09</summary>

See: .planning/milestones/v5.6.0-ROADMAP.md

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

</details>

<details>
<summary>v5.7.0 Responsa Search (Phases 14-17) -- SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

</details>

<details>
<summary>v5.7.2 Cleanup, Normalization & Sections (Phases 18-21) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.2-ROADMAP.md

4 phases, 11 plans.
Dead AI code removed, Unicode search normalization, full green test suite (447 tests),
structural HTML section parser for PGP transcriptions.
13/13 requirements satisfied.

</details>

<details>
<summary>v5.7.3 Pending Corrections Visibility (Phases 22-24) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.3-ROADMAP.md

3 phases, 3 plans.
Pending corrections visible as selectable version in web and desktop version selectors.
Shared corrections service, amber styling (web), emoji labels (desktop).
6/6 requirements satisfied. 20 milestone-specific tests.

</details>

<details>
<summary>v5.8.0 FJMS Integration (Phases 25-28) -- SHIPPED 2026-02-15</summary>

See: .planning/milestones/v5.8.0-ROADMAP.md

4 phases, 12 plans.
FJMS scholarly metadata (domains, joins, catalog) integrated via SQLite sidecar.
Domain filtering, scientific joins with scholar attribution, catalog enrichment in both apps.
19/19 requirements satisfied. 38+ tests covering service layer and integration.

</details>

<details>
<summary>v5.9.0 Multi-Source Image & Metadata Integration (Phases 29-34) -- SHIPPED 2026-02-16</summary>

See: .planning/milestones/v5.9.0-ROADMAP.md

6 phases, 22 plans (including 3 gap closure plans), 76 commits.
NLI crossref sidecar (815K records), Cambridge IIIF (141K), Manchester LUNA (28K), JTS/Princeton Figgy (453).
Multi-source image viewing with folio navigation, bibliography (542K), catalog refs (64K), physical metadata.
11/14 requirements satisfied, 1 invalidated (FGP!=FL), 2 deferred (REL-01/REL-02).

</details>

<details>
<summary>v6.0.0 Local Data Architecture (Phases 35-40) -- SHIPPED 2026-02-22</summary>

See: .planning/milestones/v6.0.0-ROADMAP.md

6 phases, 21 plans (8 core + 8 bug-fix/cleanup + 5 performance optimization), 155 commits.
PGP data migrated to local pgp.db sidecar (147MB). FJMS catalog descriptions expanded (4 new tables, ~1.7M rows).
Desktop offline PGP browsing. All desktop crashes fixed. Paginated search (PAGE_SIZE=50).
Performance: parallel NLI fetch, browse crossref parallelization, FL ID index, variant cache unification.
14/14 requirements satisfied (audit passed).

</details>

<details>
<summary>v6.1.0 Catalog Browse & Navigation (Phase 41) -- SHIPPED 2026-02-27</summary>

1 phase, 4 plans.
Faceted browsing by domain hierarchy, author, and work title in both apps.
FIST v5.0.0 enrichment (genizah_persons, genizah_titles, code_values), FTS5+domain text filter,
cross-links between browse and catalog browse pages. 72 tests.

</details>

<details>
<summary>v6.5.0 Search UX & Filtered Search (Phases 42-46) -- SHIPPED 2026-03-14</summary>

See: .planning/milestones/v6.5.0-ROADMAP.md

5 phases, 26 plans, 244 commits.
Search UX overhaul (timer, ETA, partial results, printed filter), session persistence,
Hebrew library names, bidirectional filtered search (domain/author/work/date/material),
~580K Dicta translations for multilingual access. Origin: power user feedback letter (17 requests).

</details>

<details>
<summary>v7.0.0 Fragment Puzzle (Phases 47-52) -- SHIPPED 2026-03-17</summary>

6 phases, 15 plans.
Visual jigsaw tool for assembling physical joins from manuscript fragment images with background removal,
DPI calibration, recto/verso views, join document persistence, and community publishing --
in both web (NiceGUI + Fabric.js) and desktop (PyQt6 + QGraphicsScene).

</details>

<details>
<summary>v7.1.0 FIST Gap Fill (Phase 53) -- SHIPPED 2026-03-19</summary>

1 phase, 2 plans.
Added 38,673 Genizah manuscripts from FIST.db that were missing from libraries.csv.
Browsable with images and FJMS enrichment. Metadata search guard fix. 7 new library codes.

</details>

<details>
<summary>v7.6 Search Refinement & Scholarly Joins (Phases 54-57) -- SHIPPED 2026-03-31</summary>

See: .planning/milestones/v7.6-ROADMAP.md

5 phases (+ 55.1 inserted), 17 plans, 206 commits, 151 files changed (+28K/-3.7K lines).
Manuscript dimensions display + filtering, search within results with breadcrumb chain,
exclude known manuscripts (lists/files/paste), FIST visual similarity browse + search mode,
lightweight browse first-render. 14/14 requirements satisfied.

</details>

<details>
<summary>v7.7 Volume-Aware Browse (Phases 58-61) -- SHIPPED 2026-04-01</summary>

4 phases, 8 plans, 13 commits.
Fixed multi-IE image/text mismatch for 3,193 manuscripts (1.5%) by making search->browse->paging
IE-aware across both apps. IE volume data infrastructure, web + desktop volume selector dropdown,
per-IE paging, volume-correct images for external providers (Manchester/Oxford/Cambridge/JTS),
auto-default to external sources when NLI is down, session persistence for active volume,
community writes (corrections/comments) include IE context.

</details>

### v7.8 Structural Foundation (In Progress)

**Milestone Goal:** Reduce structural debt identified by dual code review (Claude Opus + Codex) -- pin dependencies, add CI, migrate deprecated auth, clean up repo hygiene, update docs -- without changing any user-visible behavior. Establishes CI safety net for v7.9 Decomposition.
**Per-phase gate:** `pytest tests/` green, `scripts/check_docs.py` green, CI green on Windows after each phase.

- [x] **Phase 63: CI & Dependency Pinning** - Pin all deps, add GitHub Actions CI with pytest + ruff + check_docs, configure scoped ruff ruleset (completed 2026-04-14)
- [x] **Phase 64: Auth Migration** - Migrate deprecated gotrue auth to current Supabase API across both apps (completed 2026-04-14)
- [ ] **Phase 65: Repo Hygiene** - Audit silent exceptions, encapsulate monkey-patches, clean root debris, update gitignore
- [ ] **Phase 66: Documentation Update** - Update CODE_INDEX, OPEN_ISSUES, DEVELOPER_GUIDE; ensure check_docs green

## Phase Details

### Phase 63: CI & Dependency Pinning
**Goal**: The project has reproducible builds and an automated safety net that catches regressions on every push
**Depends on**: Nothing (first phase of milestone)
**Requirements**: BLDG-01, BLDG-02, BLDG-04
**Success Criteria** (what must be TRUE):
  1. Every push and PR triggers a GitHub Actions workflow that runs pytest, ruff, and check_docs.py -- and the workflow passes on the current codebase
  2. The CI workflow includes at least one Windows runner (matching the development/deployment platform)
  3. Ruff enforces syntax errors and import hygiene (scoped ruleset) with zero violations on the current codebase
  4. `pip install -r requirements.txt` on a fresh venv produces a deterministic environment with exact package versions (pinning done last in phase, after CI is green)
  5. DEVELOPER_GUIDE.md documents how to add/upgrade a dependency
**Phase gate**: `pytest tests/` green, `scripts/check_docs.py` green, CI green on Windows
**Plans**: 2 plans
Plans:
- [x] 63-01-PLAN.md — CI workflow + ruff config + fix violations
- [x] 63-02-PLAN.md — Dependency pinning + DEVELOPER_GUIDE docs

### Phase 64: Auth Migration
**Goal**: Supabase authentication uses the current supported API with zero behavior change for users
**Depends on**: Phase 63 (CI safety net catches any auth regression)
**Requirements**: BLDG-03
**Success Criteria** (what must be TRUE):
  1. Desktop login with email/password works identically to before the migration
  2. Web login with email/password and OAuth callback work identically to before the migration
  3. Token refresh (session persistence across restarts) works in both apps
  4. Current pytest baseline remains green after migration
  5. requirements.txt re-pinned to reflect any dependency changes from auth migration
**Phase gate**: `pytest tests/` green, `scripts/check_docs.py` green, CI green on Windows
**Plans**: 2 plans
Plans:
- [x] 64-01-PLAN.md — Auth import migration + PKCE flow + callback cleanup
- [x] 64-02-PLAN.md — Remove gotrue dependency + update test guards + manual verification

### Phase 65: Repo Hygiene
**Goal**: The codebase follows consistent error handling patterns, framework patches are discoverable and version-guarded, and the repo root is clean
**Depends on**: Phase 63 (CI catches any hygiene change that breaks tests)
**Requirements**: HYGN-01, HYGN-02, HYGN-03, HYGN-04
**Success Criteria** (what must be TRUE):
  1. Every bare `except:` and `except Exception: pass` in first-party code either logs at an appropriate level or has an inline comment justifying the suppression
  2. All NiceGUI monkey-patches live in `web/framework_patches.py` with version guards (using `packaging.version.Version()` or equivalent, not string comparison) and a comment explaining why each patch exists
  3. Non-source generated/temp artifacts in the repo root are either gitignored or relocated; intentional root assets explicitly exempted
  4. `.gitignore` covers the patterns that caused root debris accumulation, preventing future recurrence
**Phase gate**: `pytest tests/` green, `scripts/check_docs.py` green, CI green on Windows
**Note**: Update `docs/OPEN_ISSUES.md` incrementally as issues are resolved during this phase, not only at end
**Plans**: 3 plans
Plans:
- [x] 65-01-PLAN.md — Monkey-patch isolation into web/framework_patches.py with version guards
- [ ] 65-02-PLAN.md — Full repo-wide silent exception handler audit (genizah_core, genizah_app, web/, shared/)
- [ ] 65-03-PLAN.md — Gitignore extension for root debris prevention with full asset verification

### Phase 66: Documentation Update
**Goal**: Project documentation accurately reflects the current codebase state after all structural changes
**Depends on**: Phase 65 (all code changes complete before documenting)
**Requirements**: DOCS-01, DOCS-02, DOCS-03, DOCS-04
**Success Criteria** (what must be TRUE):
  1. `docs/CODE_INDEX.md` reflects any file moves or new files created during this milestone (framework_patches.py, etc.)
  2. `docs/OPEN_ISSUES.md` includes structural debt items from the code review with their resolution status (includes any items already updated in earlier phases)
  3. `scripts/check_docs.py` passes green with zero warnings
  4. `docs/guides/DEVELOPER_GUIDE.md` documents the CI workflow, ruff configuration, and dependency upgrade process
**Phase gate**: `pytest tests/` green, `scripts/check_docs.py` green, CI green on Windows
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 63 -> 64 -> 65 -> 66

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 63. CI & Dependency Pinning | 2/2 | Complete    | 2026-04-14 |
| 64. Auth Migration | 2/2 | Complete    | 2026-04-14 |
| 65. Repo Hygiene | 1/3 | In Progress|  |
| 66. Documentation Update | 0/TBD | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-04-15 after Phase 65 replanning with review feedback (3 plans, Wave 1 parallel)*
