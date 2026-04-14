# Requirements: GenizahSearch

**Defined:** 2026-04-14
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.8 Requirements — Structural Foundation

Requirements for the structural quality milestone. Zero user-visible behavior changes.
Current pytest baseline (1067 passed, 8 skipped, 1 warning on 2026-04-14) must remain green throughout.

### Build & Dependencies

- [ ] **BLDG-01**: All Python dependencies pinned to exact versions in requirements.txt, with dependency upgrade workflow documented in docs/guides/DEVELOPER_GUIDE.md
- [ ] **BLDG-02**: Single GitHub Actions workflow runs pytest tests/, ruff, and scripts/check_docs.py on push and PR, including at least one Windows runner
- [ ] **BLDG-03**: Supabase auth migrated to the current supported API — desktop login, web login, OAuth callback, token refresh preserved — current pytest baseline green
- [ ] **BLDG-04**: Ruff runs in CI with initial scoped ruleset (syntax errors, import hygiene only), expandable over time

### Repo Hygiene

- [ ] **HYGN-01**: Silent exception handlers audited — each either logs at appropriate level or is explicitly justified in a code comment; third-party/generated code excluded from audit
- [ ] **HYGN-02**: Framework monkey-patches isolated in web/framework_patches.py with NiceGUI version guards and justification comments for why each patch still exists
- [ ] **HYGN-03**: Non-source generated/temp artifacts in repo root gitignored or relocated; intentional root assets explicitly exempted
- [ ] **HYGN-04**: .gitignore updated to prevent future accumulation of generated artifacts

### Documentation

- [ ] **DOCS-01**: docs/CODE_INDEX.md updated to reflect any file moves; README.md updated if contributor workflow changes
- [ ] **DOCS-02**: docs/OPEN_ISSUES.md updated with structural debt status from code review findings
- [ ] **DOCS-03**: scripts/check_docs.py passes green after all changes
- [ ] **DOCS-04**: docs/guides/DEVELOPER_GUIDE.md updated for CI, lint, and dependency upgrade workflow

## v7.9 Requirements — Decomposition (Future)

Deferred to next milestone. Leverages CI safety net from v7.8.

### Desktop Decomposition

- **DESK-01**: ResultDialog class extracted to dedicated module
- **DESK-02**: PuzzleCanvasWindow and puzzle-related classes extracted to dedicated module
- **DESK-03**: ManuscriptViewerWidget and image viewer classes extracted to dedicated module
- **DESK-04**: ExcludeDialog and filter dialog classes extracted to dedicated module
- **DESK-05**: FJMS, NLI, and bibliography dialog classes extracted to dedicated module
- **DESK-06**: GenizahGUI remains in genizah_app.py, importing from extracted modules
- **DESK-07**: Current pytest baseline remains green after each extraction step; desktop app smoke-tests (startup, basic navigation) pass

### Web Decomposition

- **WEBM-01**: web/pages/search.py split into search state, UI, and results modules
- **WEBM-02**: web/pages/browse.py split into browse state, UI, and enrichment modules
- **WEBM-03**: app.storage.user sprawl and detached asyncio.ensure_future flows in search/browse reduced with page-scoped state objects

### Non-Regression

- **NREG-01**: Search and browse performance non-regression verified after decomposition (no measurable responsiveness degradation)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Server-Side Image Cache | Deferred to v7.9+ (blocked on NLI TOS outreach) |
| Type checking (mypy/pyright) | Large investment, defer to future milestone |
| Full ruff ruleset enforcement | Would be a side quest on legacy codebase; start scoped in v7.8 |
| Rewriting NiceGUI to another framework | Monkey-patches are encapsulated, not eliminated |
| Desktop app modular packaging | PyInstaller spec unchanged; decomposition is source-level only |
| Web state management framework | Page-scoped objects in v7.9, not a full state management library |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| BLDG-01 | Phase 63 | Pending |
| BLDG-02 | Phase 63 | Pending |
| BLDG-03 | Phase 64 | Pending |
| BLDG-04 | Phase 63 | Pending |
| HYGN-01 | Phase 65 | Pending |
| HYGN-02 | Phase 65 | Pending |
| HYGN-03 | Phase 65 | Pending |
| HYGN-04 | Phase 65 | Pending |
| DOCS-01 | Phase 66 | Pending |
| DOCS-02 | Phase 66 | Pending |
| DOCS-03 | Phase 66 | Pending |
| DOCS-04 | Phase 66 | Pending |

**Coverage:**
- v7.8 requirements: 12 total
- Mapped to phases: 12
- Unmapped: 0

---
*Requirements defined: 2026-04-14*
*Last updated: 2026-04-14 after roadmap creation (12/12 mapped)*
