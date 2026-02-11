# Requirements: GenizahSearch

**Defined:** 2026-02-11
**Core Value:** Researchers can find what they need in the Genizah corpus

## v5.7.1 Requirements

Requirements for v5.7.1 Cleanup & Polish. Each maps to roadmap phases.

### Code Cleanup

- [ ] **CLEAN-01**: AI Search component removed from desktop app (AIManager, AIDialog, AIWorkerThread, Settings panel, button, all wiring)
- [ ] **CLEAN-02**: AI Search instantiation removed from web app (unused import + initialization)
- [ ] **CLEAN-03**: AI Search references removed from help documentation (both apps)
- [ ] **CLEAN-04**: google-genai import and AI_PROVIDER_ENDPOINTS constants removed from genizah_core.py

### Search Normalization

- [ ] **NORM-01**: Combining diacritical marks (U+0300-U+036F) stripped from search queries at query time
- [ ] **NORM-02**: Hebrew geresh (U+05F3) and gershayim (U+05F4) stripped from search queries at query time
- [ ] **NORM-03**: Search result highlighting is mark-tolerant (regex matches base letters even when text contains combining marks)
- [ ] **NORM-04**: Existing search modes unaffected (normalization globally safe or gated appropriately)

### Test Fixes

- [ ] **TEST-01**: Export filename tests updated to expect underscore-separated filenames (4 tests)
- [ ] **TEST-02**: Boundary search tests fixed (algorithm or expectations aligned, 2 tests)
- [ ] **TEST-03**: Excel column index assertion fixed (1 test)
- [ ] **TEST-04**: Obsolete backend test files deleted (3 files: test_api_flow.py, test_corrections_api.py, test_corrections_integration.py)
- [ ] **TEST-05**: All tests pass with zero failures after fixes

## Future Requirements

### Search Enhancement

- **NORM-05**: User can intentionally search WITH diacritical marks (opt-in for marked letter matching)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Search WITH diacritical marks intentionally | Future feature -- needs UX design for mark input |
| Re-index Tantivy for diacritics | Query-time normalization sufficient for now |
| Rewrite obsolete backend tests for Supabase | Separate effort, different scope |
| Transcription search (Phase 13) | Needs server-side index architecture |
| NLI joins import | Separate milestone |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| CLEAN-01 | TBD | Pending |
| CLEAN-02 | TBD | Pending |
| CLEAN-03 | TBD | Pending |
| CLEAN-04 | TBD | Pending |
| NORM-01 | TBD | Pending |
| NORM-02 | TBD | Pending |
| NORM-03 | TBD | Pending |
| NORM-04 | TBD | Pending |
| TEST-01 | TBD | Pending |
| TEST-02 | TBD | Pending |
| TEST-03 | TBD | Pending |
| TEST-04 | TBD | Pending |
| TEST-05 | TBD | Pending |

**Coverage:**
- v5.7.1 requirements: 13 total
- Mapped to phases: 0
- Unmapped: 13

---
*Requirements defined: 2026-02-11*
*Last updated: 2026-02-11 after initial definition*
