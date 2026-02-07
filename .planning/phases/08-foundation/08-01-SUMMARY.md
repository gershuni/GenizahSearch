---
phase: 08-foundation
plan: 01
subsystem: shared-services
tags: [extraction, supabase, document-service, shared-layer]
dependency-graph:
  requires: []
  provides: [shared-package, supabase-provider, shared-document-service]
  affects: [08-02, 10-01, 12-01, 13-02]
tech-stack:
  added: []
  patterns: [singleton-client, service-extraction, additive-only-migration]
key-files:
  created:
    - shared/__init__.py
    - shared/supabase_provider.py
    - shared/document_service.py
  modified: []
decisions:
  - id: DEC-08-01-01
    description: "Keep shared/document_service.py API identical to web/document_service.py during extraction (no reshaping)"
    rationale: "Minimizes risk: identical API surface means web re-export shim in 08-02 is trivial"
metrics:
  duration: ~2 min
  completed: 2026-02-08
---

# Phase 8 Plan 1: Create shared/ package with supabase_provider and document_service

**One-liner:** Extracted shared/ Python package with supabase_provider singleton and all 12 PGP data functions from web/document_service.py -- additive-only, zero existing files modified.

## Performance

- **Start:** 2026-02-07T23:40:51Z
- **End:** 2026-02-07T23:43:08Z
- **Duration:** ~2 min
- **Tasks:** 2/2

## Accomplishments

1. Created `shared/` package at project root with `__init__.py` (empty marker) and `supabase_provider.py` (unified Supabase client singleton)
2. Extracted `web/document_service.py` (508 lines) to `shared/document_service.py` with single import change (`from shared.supabase_provider import get_client`)
3. Verified all 12 PGP data functions importable from `shared.document_service`
4. Verified `get_client()` returns working `supabase._sync.client.Client` singleton
5. Confirmed existing web tests still pass (16/17, 1 pre-existing failure unrelated to this change)

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Create shared/ package with supabase_provider | 69a8caf | shared/__init__.py, shared/supabase_provider.py |
| 2 | Extract document_service to shared/ | 3524969 | shared/document_service.py |

## Files Created

| File | Purpose | Lines |
|------|---------|-------|
| `shared/__init__.py` | Package marker (empty) | 0 |
| `shared/supabase_provider.py` | Unified Supabase client singleton for both apps | 44 |
| `shared/document_service.py` | All 12 PGP data functions (extracted from web/) | 507 |

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-08-01-01 | Keep API surface identical during extraction | Zero-risk migration: web re-export shim in 08-02 will be trivial, no call sites change |

## Deviations from Plan

None -- plan executed exactly as written.

## Issues & Risks

- **Pre-existing test failure:** `test_get_document_for_fragment_not_found` fails in existing `tests/test_document_service.py` (not caused by this change, was failing before). Tracked as pre-existing issue.
- **TODO at line 268:** `shared/document_service.py` contains existing TODO for multi-fragment page handling. Intentionally preserved per plan instructions.

## Next Phase Readiness

Plan 08-02 can proceed immediately. It will:
1. Create a re-export shim at `web/document_service.py` that imports from `shared.document_service`
2. Update tests to verify the shared module
3. Run smoke tests confirming zero breakage in web app

**Blockers:** None
**Concerns:** None

## Self-Check: PASSED
