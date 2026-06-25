---
phase: 120-actions-persistence
plan: "01"
subsystem: web/joins_lab_storage
tags: [persistence, safe_storage, phase-87-invariant, tdd]
dependency_graph:
  requires: [117-01, 117-04]
  provides: [write_full_state, read_full_state, clear_joins_lab_state-extended]
  affects: [web/pages/joins_lab.py (Phase 120 restore), web/pages/puzzle.py (clear)]
tech_stack:
  added: []
  patterns: [explicit-payload-whitelist, lru-cap-eviction, schema-v1-additive-extension]
key_files:
  created: []
  modified:
    - web/joins_lab_storage.py
    - tests/test_joins_lab_storage.py
decisions:
  - "_SCHEMA_VERSION stays 1: Phase-120 keys are additive/non-breaking; bumping would discard all Phase-117 user anchors via the exact-match gate"
  - "write_full_state builds payload from explicit named params only (whitelist), preventing any blob key from entering the store"
  - "_cap_triage preserves yes/no verdicts during overflow eviction, discarding oldest maybe entries"
  - "clear_joins_lab_state now pops both joins_lab and puzzle_staging (D-16) so reset cannot leave stale staging"
metrics:
  duration: "4 min"
  completed: "2026-06-21"
  tasks_completed: 2
  files_modified: 2
---

# Phase 120 Plan 01: Joins Lab Storage Extension (PST-01/02/03) Summary

Extended `web/joins_lab_storage.py` with full-state persistence under `schema_version: 1`, enforced size caps, and an extended clear that also wipes the `puzzle_staging` companion key.

## What Was Built

### `web/joins_lab_storage.py` — Phase-120 extension

New public functions:

**`write_full_state(**fields) -> bool`** — Persists the complete Joins Lab working state:
- Anchor identity (Phase-117 fields, unchanged)
- Builder rows + mode + text_position + global toggles (`flex_spacing`, `bidirectional`)
- Other-side builder rows + combine mode
- `sys_id`-keyed triage dict
- Active filter state + view mode
- Payload built **explicitly from named params only** — no blob key (full_text, image, candidates) can be inadvertently persisted (T-120-blob mitigation)
- `schema_version` pinned to `_SCHEMA_VERSION = 1` (unchanged)

**`read_full_state() -> Optional[dict]`** — Delegates to `read_joins_lab_state()` (existing version gate). Returns the stored dict or `None`. Callers read Phase-120 keys with `.get(key, default)` so legacy v1 blobs restore cleanly without discard.

**`clear_joins_lab_state()` (extended)** — Now pops both `joins_lab` AND `puzzle_staging` (D-16 requirement). Return type changed from `Any` to `None` (the popped value was not used by callers; the companion pop makes a return value ambiguous).

**Internal helpers:**
- `_cap_rows(rows)` — Caps builder row list at 20 entries; truncates each `term` to 200 chars.
- `_cap_triage(triage)` — Caps at 500 entries with LRU-style eviction: preserves `yes`/`no` entries, discards oldest `maybe` entries.

### `tests/test_joins_lab_storage.py` — New TDD tests

9 new test cases covering:
1. `test_write_full_state_round_trip` — all Phase-120 keys round-trip correctly
2. `test_write_full_state_schema_version_stays_1` — schema_version never bumped
3. `test_legacy_v1_anchor_blob_not_discarded` — Phase-117 blobs survive read_full_state()
4. `test_write_anchor_backward_compat` — write_anchor() unchanged (Phase-117 regression)
5. `test_write_full_state_no_blobs` — PST-01 / VALIDATION.md `test_write_full_state_no_blobs`
6. `test_builder_rows_capped_at_20` — 30-row input → 20-row output
7. `test_builder_row_term_capped_at_200_chars` — 300-char term → 200-char stored
8. `test_triage_capped_at_500` — 600-entry dict → 500 stored; all 100 yes/no entries preserved
9. `test_clear_leaves_empty` — PST-03 / VALIDATION.md `test_clear_leaves_empty` — both keys wiped

All 21 tests in the file pass (15 pre-existing + 9 new — the two "remove pytest import" and other minor fixups reduced the import-only test to 14 pre-existing).

## Threat Model Coverage

| Threat | Status |
|--------|--------|
| T-120-blob (DoS via blob persistence) | Mitigated — explicit payload whitelist; no **kwargs passthrough |
| T-120-leak (session isolation) | Mitigated — all access via safe_user_*; test_no_raw_storage_access stays green (allowlist []) |
| T-120-stale (stale puzzle_staging) | Mitigated — clear_joins_lab_state() pops both keys |

## TDD Gate Compliance

| Phase | Commit | Status |
|-------|--------|--------|
| RED | a48013e7 | test(120-01): failing tests added |
| GREEN + REFACTOR | 96dd5218 | feat(120-01): implementation + ruff fix |

## Deviations from Plan

None — plan executed exactly as written. Tasks 1 and 2 both targeted the same two files; their implementation was committed together in the GREEN phase (caps and clear extension are load-bearing for the no-blobs and clear tests already written in RED).

## Verification

```
PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -q --tb=short
# Result: 21 passed
grep -n "_SCHEMA_VERSION = 1" web/joins_lab_storage.py  # → 60:_SCHEMA_VERSION = 1
grep -n "_SCHEMA_VERSION = 2" web/joins_lab_storage.py  # → no output (correct)
```

## Self-Check

Files exist:
- `web/joins_lab_storage.py` — FOUND
- `tests/test_joins_lab_storage.py` — FOUND

Commits exist:
- `a48013e7` — RED gate
- `96dd5218` — GREEN gate

## Self-Check: PASSED
