# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & Transcription Search - Phases 8-13 (in progress)

## Phases

<details>
<summary>v1 External Data Integration (Phases 1-7) - SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

### v5.6.0 Desktop Parity & Transcription Search (In Progress)

**Milestone Goal:** Bring all PGP features to the desktop app via a shared service layer, import remaining PGP documents, make transcriptions searchable in Tantivy with filter controls, and introduce a Virtual Reading Desk for multi-manuscript viewing in both apps.

**Phase Numbering:** Continues from v1 milestone (Phase 7). Integer phases 8-13.

- [x] **Phase 8: Foundation** - Extract shared service layer from web-only module
- [x] **Phase 9: Data Import** - Import remaining ~34K PGP documents to Supabase
- [x] **Phase 10: Desktop PGP Core** - Transcription display and version selector in desktop app
- [x] **Phase 11: Virtual Reading Desk** - Multi-manuscript viewer for both web and desktop
- [ ] **Phase 12: Desktop PGP Discovery** - Metadata, search indicators, tag search, and joins in desktop app
- [ ] **Phase 13: Transcription Search** - Index transcriptions in Tantivy with filter controls

## Phase Details

### Phase 8: Foundation
**Goal**: Both web and desktop apps consume PGP data through a shared service layer, with zero breakage to existing web functionality
**Depends on**: Nothing (first phase of v5.6.0)
**Requirements**: INFRA-01, INFRA-02, INFRA-03
**Success Criteria** (what must be TRUE):
  1. `shared/document_service.py` exists and both apps can import all 12 PGP data functions from it
  2. Web app starts and all PGP features (transcriptions, metadata, tag search, joins) work exactly as before extraction
  3. Desktop app can import and call shared service functions without import errors or path hacks
  4. A single `shared/supabase_provider.py` provides the Supabase client to both apps (no duplicate client initialization for PGP reads)
**Plans:** 2 plans

Plans:
- [x] 08-01-PLAN.md -- Create shared/ package with supabase_provider and extract document_service
- [x] 08-02-PLAN.md -- Web re-export shim, test updates, and smoke tests confirming zero breakage

### Phase 9: Data Import
**Goal**: All ~35,839 PGP documents are available in Supabase (full upsert of documents.csv), completing the dataset for transcription search indexing and desktop display. Also imports footnotes/bibliography and fragment metadata from fragments.csv.
**Depends on**: Phase 8 (uses shared service for verification)
**Requirements**: DATA-01, DATA-02
**Success Criteria** (what must be TRUE):
  1. Supabase documents table contains all ~35,839 PGP documents (existing 7,090 updated + ~28,749 new)
  2. Multi-fragment documents in the new batch have correct entries in document_fragments linking them to sys_ids, with fragment metadata from fragments.csv
  3. Existing 7,090 documents and 9,364 sources are unchanged (no data corruption from import)
**Plans:** 2 plans

Plans:
- [x] 09-01-PLAN.md -- Schema migrations and comprehensive import script
- [x] 09-02-PLAN.md -- Execute import and verify data integrity

### Phase 10: Desktop PGP Core
**Goal**: Desktop users can view PGP transcriptions in the manuscript viewer and switch between scholars' editions and translations
**Depends on**: Phase 8 (shared service layer)
**Requirements**: DESK-01, DESK-02
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript with a PGP transcription, the desktop app automatically displays the PGP edition (preferred over HTR V0.8)
  2. The desktop version selector shows PGP editions grouped by scholar and translations grouped by language, with visual separators between groups
  3. Switching between version sources updates the transcription display without freezing the UI (QThread workers handle all Supabase calls)
**Plans:** 2 plans

Plans:
- [x] 10-01-PLAN.md -- PGPSourceWorker QThread, shared helpers, and Browse tab PGP integration
- [x] 10-02-PLAN.md -- ResultDialog PGP integration and human verification

### Phase 11: Virtual Reading Desk
**Goal**: Users can view multiple manuscripts together in a reading desk, populated from joins, personal lists, or manual entry, in both web and desktop apps
**Depends on**: Phase 8 (shared service); Phase 10 (desktop PGP display)
**Requirements**: VIEW-01, VIEW-02, VIEW-03, VIEW-04
**Success Criteria** (what must be TRUE):
  1. User can open a joined document and see all its fragments displayed together with images and transcriptions in sequence
  2. User can add any manuscript to the reading desk by typing a shelfmark or sys_id
  3. User can populate the reading desk from a personal list (selecting which manuscripts to include)
  4. The reading desk feature works in both the web app and the desktop app with equivalent functionality
**Plans:** 11 plans (5 original + 2 gap closure + 4 UAT gap closure)

Plans:
- [x] 11-01-PLAN.md -- Shared ReadingDeskModel + web dual-pane synchronized reading desk (stacked images/texts, per-image controls, per-fragment version selectors)
- [x] 11-02-PLAN.md -- Web entry points and dynamic management (Add to View, toolbar, Add from List, remove, state preservation)
- [x] 11-03-PLAN.md -- Desktop dual-pane reading desk rendering (stacked images in viewer, stacked texts with version selectors, synchronized scrolling)
- [x] 11-04-PLAN.md -- Desktop entry points and dynamic management (Add to View, toolbar, joins integration, list panel integration)
- [x] 11-05-PLAN.md -- Human verification checkpoint (both web and desktop)
- [x] 11-06-PLAN.md -- Gap closure: Web reading desk fixes (W1-W5: list dialog, button visibility, badge visibility, word wrap, state persistence)
- [x] 11-07-PLAN.md -- Gap closure: Desktop reading desk fixes (D1, D2, D4: scroll sync, button label, button position)
- [x] 11-08-PLAN.md -- UAT gap closure: Web visual/CSS fixes (Light Mode header, word wrap, RuntimeError)
- [x] 11-09-PLAN.md -- UAT gap closure: Web state management (language switch + navigation stale state)
- [x] 11-10-PLAN.md -- UAT gap closure: Web Add from List individual selection
- [x] 11-11-PLAN.md -- UAT gap closure: Desktop scroll area lifecycle and sync signal management

### Phase 12: Desktop PGP Discovery
**Goal**: Desktop users can discover PGP content through metadata panels, search indicators, tag search, and fragment join relationships
**Depends on**: Phase 10 (desktop viewer must display PGP content before adding metadata/discovery)
**Requirements**: DESK-03, DESK-04, DESK-05, DESK-06
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript with PGP data, a collapsible panel shows document type, tags, dates, and description
  2. Search results show a green indicator column for manuscripts that have PGP transcriptions available
  3. User can search by PGP tag either by clicking a tag in the metadata panel or by entering a tag in a dedicated search mode
  4. Related Fragments dialog shows PGP-sourced joins (multi-fragment documents) alongside existing user-created joins
**Plans**: TBD

Plans:
- [ ] 12-01: PGP metadata panel (collapsible QGroupBox with type, tags, dates, description)
- [ ] 12-02: Search result transcription indicators and tag-based search mode
- [ ] 12-03: PGP joins integration in Related Fragments dialog

### Phase 13: Transcription Search
**Goal**: Users can search within PGP and user-corrected transcriptions via Tantivy, with filter controls to scope results
**Depends on**: Phase 8 (shared service for fetching transcriptions during indexing); Phase 9 preferred (complete dataset indexed)
**Requirements**: SRCH-01, SRCH-02, SRCH-03, SRCH-04
**Success Criteria** (what must be TRUE):
  1. Searching for Hebrew text that appears only in PGP transcriptions returns matching manuscripts
  2. User corrections are also indexed and searchable alongside PGP transcriptions
  3. User can toggle search filter between "All content" (default), "Transcriptions only", and "Exclude transcriptions" in both web and desktop
  4. Index rebuild uses a safe temp-then-swap pattern: existing index remains usable until new index is verified, with automatic rollback on failure
**Plans**: TBD

Plans:
- [ ] 13-01: Tantivy schema extension with transcription and content_type fields
- [ ] 13-02: Index builder fetches PGP + user transcriptions during rebuild
- [ ] 13-03: Search filter UI in both web and desktop apps

## Progress

**Execution Order:** 8 -> 9 -> 10 -> 11 -> 12 -> 13

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 8. Foundation | v5.6.0 | 2/2 | Complete | 2026-02-08 |
| 9. Data Import | v5.6.0 | 2/2 | Complete | 2026-02-08 |
| 10. Desktop PGP Core | v5.6.0 | 2/2 | Complete | 2026-02-08 |
| 11. Virtual Reading Desk | v5.6.0 | 11/11 | Complete | 2026-02-08 |
| 12. Desktop PGP Discovery | v5.6.0 | 0/3 | Not started | - |
| 13. Transcription Search | v5.6.0 | 0/3 | Not started | - |

---
*Roadmap created: 2026-02-07*
*Last updated: 2026-02-08 (Phase 11 complete -- all 11 plans including UAT gap closure, verified 12/12)*
