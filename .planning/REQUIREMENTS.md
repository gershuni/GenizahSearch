# Requirements: GenizahSearch

**Defined:** 2026-04-15
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.9 Requirements — Decomposition

Reduce structural debt by decomposing the two largest source files (`genizah_app.py` ~18,500 lines, `web/pages/search.py` ~3,200 lines plus `web/pages/browse.py`) into focused modules. Zero user-visible behavior changes. Leverages the v7.8 CI safety net (ruff + pytest on Ubuntu + Windows matrix).

Current pytest baseline (1067 passed, 8 skipped, 1 warning on 2026-04-14) must remain green throughout. Documentation refreshes required at milestone close are tracked in the roadmap but are not standalone requirements.

### Desktop Decomposition

- [ ] **DESK-01**: `ResultDialog` class extracted to a dedicated module, imported by `genizah_app.py`
- [ ] **DESK-02**: `PuzzleCanvasWindow` and puzzle-related classes extracted to a dedicated module
- [ ] **DESK-03**: `ManuscriptViewerWidget` and image viewer classes (including `FullscreenImageWindow`) extracted to a dedicated module
- [ ] **DESK-04**: `ExcludeDialog` and filter dialog classes extracted to a dedicated module
- [ ] **DESK-05**: FJMS, NLI, and bibliography dialog classes extracted to a dedicated module
- [ ] **DESK-06**: `GenizahGUI` remains in `genizah_app.py` as the top-level application coordinator; extracted UI/dialog/viewer implementations are imported from dedicated modules (small coordination helpers on `GenizahGUI` are acceptable)
- [ ] **DESK-07**: Current pytest baseline remains green after each extraction step; desktop smoke tests pass — minimum smoke-test suite: desktop app starts; basic search executes; browse navigation changes pages; ResultDialog opens/closes; puzzle window opens and loads a fragment

### Web Decomposition

- [ ] **WEBM-01**: `web/pages/search.py` split into search state, UI, and results modules
- [ ] **WEBM-02**: `web/pages/browse.py` split into browse state, UI, and enrichment modules
- [ ] **WEBM-03**: search and browse reduce reliance on `app.storage.user` for live page state and reduce detached `asyncio.ensure_future` flows by using page-scoped state / handlers where practical

### Non-Regression

- [ ] **NREG-01**: Manual non-regression check (executor + user) on search and browse responsiveness versus pre-refactor behavior — no obvious slowdown in initial render, result paging, or result interaction (qualitative; benchmark harness not required)

### Milestone-Close Documentation (not a standalone requirement)

At milestone close, the following docs must be refreshed to reflect the decomposition — these are deliverables of the close-out phase rather than separate numbered requirements:
- `docs/CODE_INDEX.md` — updated for all moved/extracted files with new module paths
- `docs/OPEN_ISSUES.md` — decomposition-related findings and deferred items recorded
- Any other docs that reference specific file paths or line numbers within the decomposed files
- `scripts/check_docs.py` passes green

## Future Requirements

Deferred to later milestones.

| Requirement | Defer reason |
|-------------|--------------|
| `genizah_core.py` (~8,300 lines) decomposition | Defer to v8.0 / dedicated Core Modularization milestone. Highly shared, test-heavy, behavior-critical — mixing into v7.9 would blur the risk boundary and make regressions harder to isolate |
| CUT-01: Remove read-only PGP tables from Supabase | Legacy data/infrastructure cleanup, not decomposition |
| Desktop corrections fetch → shared `corrections_service` | Allowed only as an implementation detail if it directly supports a desktop extraction task; not a standalone v7.9 requirement |
| SEED-001: Server-side IIIF image cache | Reliability infrastructure work — dormant for v8.0 / dedicated reliability milestone |

## Out of Scope

| Feature | Reason |
|---------|--------|
| New user-facing features | v7.9 is strictly structural — zero user-visible behavior changes |
| `genizah_core.py` decomposition | Explicitly deferred to v8.0 (risk boundary isolation) |
| Type checking (mypy / pyright) | Large investment, defer to future milestone |
| Full ruff ruleset enforcement | Scoped ruleset (E9/F401/F811/F821) remains baseline for v7.9 |
| Rewriting NiceGUI to another framework | Monkey-patches remain encapsulated in `web/framework_patches.py`, not eliminated |
| Desktop app modular packaging | PyInstaller spec unchanged; decomposition is source-level only |
| Web state management framework | Page-scoped objects only; not a full state library (Redux/Vuex-style) |
| Quantitative performance benchmarks | NREG-01 stays qualitative; no benchmark harness introduced for v7.9 |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DESK-01 | Phase 67 | Not started |
| DESK-02 | Phase 70 | Not started |
| DESK-03 | Phase 69 | Not started |
| DESK-04 | Phase 68 | Not started |
| DESK-05 | Phase 68 | Not started |
| DESK-06 | Phase 71 | Not started |
| DESK-07 | Phase 71 | Not started |
| WEBM-01 | Phase 72 | Not started |
| WEBM-02 | Phase 73 | Not started |
| WEBM-03 | Phase 74 | Not started |
| NREG-01 | Phase 75 | Not started |

**Coverage:**
- v7.9 requirements: 11 total
- Mapped to phases: 11
- Unmapped: 0

---
*Requirements defined: 2026-04-15*
*Traceability updated: 2026-04-15 -- all 11 requirements mapped to phases 67-75*
