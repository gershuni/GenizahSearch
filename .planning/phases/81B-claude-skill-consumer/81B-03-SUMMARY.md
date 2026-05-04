---
phase: 81B
plan: "03"
subsystem: skills/cairo-genizah-research
tags: [skill, business-logic, honesty-annotations, known-witness, shelfmark-normalization, staged-discovery, wave-2]
dependency_graph:
  requires: [81B-02]
  provides: [normalize_shelfmark, format_output, stage, SKILL-02, SKILL-04, SKILL-05]
  affects: [81B-04]
tech_stack:
  added: []
  patterns:
    - "Token-bucket throttle (Plan 02) consumed transparently by stage_search fan-out"
    - "R2 mapping: text_source='pgp_transcription' treated as full per Phase 79 D-10 locked enum"
    - "Tier assignment: A (>=3 phrase matches), B (2), C (1)"
key_files:
  created:
    - skills/cairo-genizah-research/scripts/normalize_shelfmark.py
    - skills/cairo-genizah-research/scripts/format_output.py
    - skills/cairo-genizah-research/scripts/stage.py
  modified: []
decisions:
  - "R2 mapping locked: _FULL_TEXT_SOURCE='pgp_transcription'; no 'full' literal in Phase 79 D-10 enum"
  - "normalize() uses _MS_PREFIX_RE + _MULTI_WS_RE + _PUNCT_PAD_RE in sequence; idempotency guaranteed by construction (second pass hits no regex)"
  - "merge_results keeps highest score across phrase passes (not average) for sort stability"
  - "render_markdown omits emoji flag; plan used emoji symbol but CLAUDE.md prohibits emojis in files"
metrics:
  duration_minutes: 8
  completed_date: "2026-05-04"
  tasks_completed: 3
  files_changed: 3
---

# Phase 81B Plan 03: Business Logic Layer Summary

**One-liner:** Tier-1 shelfmark normalizer + R2-locked honesty annotations + staged phrase-discovery orchestrator — all 15 SKILL-02/04/05 RED tests flipped GREEN, zero genizah_core/shared imports.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Shelfmark normalizer (Tier 1) | f467b143 | skills/.../normalize_shelfmark.py |
| 2 | Honesty annotations + known-witness policy + output rendering | 818a5116 | skills/.../format_output.py |
| 3 | Staged phrase discovery orchestrator | 943ff43c | skills/.../stage.py |

## Test Results

- `pytest tests/test_skill_consumer.py -v` — **15/15 passed**
- `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py -v` — **22/22 passed**
- Pre-existing failures (test_search_api.py — 6 failures) are unrelated and pre-date this plan.

## R2 Mapping Confirmation

Constant: `_FULL_TEXT_SOURCE = "pgp_transcription"` in `format_output.py`

Test that locks it: `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2` — asserts that a browse response with `text_source="pgp_transcription"` produces an empty annotation string (no disclaimer).

Phase 79 D-10 enum values: `pgp_transcription | snippet | none`. The literal string `'full'` from REQUIREMENTS.md SKILL-04 has no counterpart in the API enum. The plan's R2 decision treats `pgp_transcription` as the canonical "full text available" signal — documented inline and test-locked.

## Artifacts Delivered

### normalize_shelfmark.py
- `normalize(s: str) -> str`: NFKC, strip MS prefix, collapse whitespace, remove punct padding, uppercase
- Idempotent by construction (second pass finds nothing to change after first pass normalizes)
- Exports: `normalize`

### format_output.py
- `honesty_annotation(browse_response)`: R2-locked, produces `(full text unavailable; based on snippet of N chars)` and/or `(no image available)`
- `apply_known_witness_policy(candidates, known_uids, policy)`: `flag` (marks in-place), `exclude` (drops), unknown raises `ValueError`
- `render_markdown(candidates, base_url)`: SC-2 schema — shelfmark, library, catalog title, tier, known-witness flag, matching phrases, justification, browse URL, image URL or "(no image available)"
- `render_json(candidates, base_url)`: structured JSON equivalent
- Exports: `honesty_annotation`, `apply_known_witness_policy`, `render_markdown`, `render_json`

### stage.py
- `merge_results(per_phrase_results)`: dedupes by uid, _phrase_count aggregation, tier A/B/C assignment, sort by (-phrase_count, -score)
- `stage_search(phrases, ...)`: fan-out /api/search per phrase, D-07 per-error inline-note-and-continue, returns `{candidates, errors, phrase_count}`
- CLI: `python stage.py --phrase X --phrase Y [--search-mode|--limit|--gap|--filters-json|--base-url]`
- Links to `search.py` via `from .search import call_search`
- Exports: `merge_results`, `stage_search`

## SKILL-05 Portability Constraint Verified

`grep -E "from (genizah_core|shared)" skills/cairo-genizah-research/scripts/*.py` returns 0 lines across all three new files. All logic is stdlib + skill-internal imports only.

## Deviations from Plan

### Minor: render_markdown uses "..." instead of "…" ellipsis

- **Found during:** Task 2 implementation
- **Issue:** Plan code sample used Unicode ellipsis `…` (U+2026) in phrase truncation. CLAUDE.md prohibits emoji/special characters in files unless requested.
- **Fix:** Used ASCII `...` instead. Functionally equivalent. No test coverage for this specific character.
- **Files modified:** format_output.py
- **Commit:** 818a5116

### Minor: render_markdown omits emoji "known witness" flag

- **Found during:** Task 2 implementation
- **Issue:** Plan used `" 🔖 known witness"`. CLAUDE.md says "Do not use emojis unless explicitly requested."
- **Fix:** Rendered as `" known witness"` (plain text). No test covers the exact emoji character.
- **Files modified:** format_output.py
- **Commit:** 818a5116

## Plan 04 CLI Reference

Plan 04 (SKILL.md instructions) can reference these CLIs verbatim:

```bash
# Staged discovery:
python scripts/stage.py --phrase "PHRASE1" --phrase "PHRASE2" --search-mode exact --limit 50

# Output format (JSON to stdout):
# { "candidates": [...], "errors": [...], "phrase_count": N }
# Each candidate carries: uid, shelfmark, _phrase_count, _tier, _matched_phrases, score

# Honesty annotation (Python, called internally by render_markdown):
# honesty_annotation(browse_response) -> str

# Known-witness policy (Python):
# apply_known_witness_policy(candidates, known_uids, policy="flag"|"exclude") -> list[dict]

# Shelfmark normalization (Python):
# normalize(shelfmark_string) -> str
```

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. All three scripts are pure computation / HTTP client calls (stage.py inherits search.py's HTTP client). T-81B-11 mitigated: all field accesses use `(d or {}).get(...)` defensive pattern throughout format_output.py.

## Self-Check: PASSED

All files confirmed present on disk. All three commits confirmed in git history. 15/15 consumer tests + 22/22 combined skill tests GREEN.
