# Phase 81A: Minimal API Contract Expansion — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `81A-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-03
**Phase:** 81A-api-contract-expansion
**Areas discussed:** Regex mode (OQ-4), Hashed IP in echo (OQ-5), Variant tiers in responsa_options (OQ-6), Test organization & migration tactic

---

## Regex mode (OQ-4)

| Option | Description | Selected |
|--------|-------------|----------|
| Keep regex (Recommended) | All 6 values: exact/variants/regex/responsa/title/shelfmark. UI already supports it. 256-char pattern cap + executor timeout. Skill avoids it by default. | |
| Drop regex from v7.10 | 5 values only. Defers regex to v7.11. Smaller test surface, smaller ReDoS attack surface. Power users lose regex via API until v7.11. | ✓ |

**User's choice:** Drop regex from v7.10.
**Notes:** Deviates from RESCOPE §3 recommendation. Cascading effects captured in CONTEXT.md D-09 (no 256-char cap, no `regex_pattern_too_long` code, validation matrix simplifies, AC2 reduces from 6 to 5 modes). Second follow-up question on regex bounds was not asked because regex was dropped.

---

## Hashed IP in request echo (OQ-5)

| Option | Description | Selected |
|--------|-------------|----------|
| No (Recommended) | Echo block contains only what the client sent + what the server applied. Skills detect rate-limit state from 429 responses. Avoids hash-inversion leak surface and keeps the response a pure echo. | ✓ |
| Yes, include hashed IP | Skill authors can correlate their requests across runs for self-debugging. Adds a small information-disclosure surface (hash inversion via dictionary attacks). | |

**User's choice:** No.
**Notes:** Matches RESCOPE recommendation. Captured as D-10.

---

## Variant tiers in responsa_options (OQ-6)

| Option | Description | Selected |
|--------|-------------|----------|
| Boolean only (Recommended) | `variants: bool` mirrors the desktop UI checkbox exactly. Server derives internal `variant_mode` ('exact' vs 'variants') from the boolean. Extended/maximum tiers are deferred for both main mode and Responsa — consistent. | ✓ |
| Expose variants tier enum | `variants: 'off' \| 'basic' \| 'extended' \| 'maximum'`. Inconsistent with main-mode deferral. Adds Responsa cascade complexity. Defeats the deferral rationale. | |

**User's choice:** Boolean only.
**Notes:** Matches RESCOPE recommendation. Captured as D-11.

---

## Test organization & migration tactic

### Sub-question A: Where should the new 81A test cases live?

| Option | Description | Selected |
|--------|-------------|----------|
| New file: `tests/test_search_api_v2.py` (Recommended) | Per memo §8. Clean separation; new file owns the search_mode × responsa_options × invalid-combination matrix. | ✓ |
| Extend `tests/test_search_api.py` | Single file owns the surface. Tighter, but mixing old and new in one file invites confusion during the migration commit. | |
| Delete-then-replace `tests/test_search_api.py` | Wipe old file and rewrite as one canonical file. Loses git history continuity. | |

**User's choice:** New file `tests/test_search_api_v2.py`.

### Sub-question B: How should the old `mode` field be retired?

| Option | Description | Selected |
|--------|-------------|----------|
| Hard reject via `extra='forbid'` (Recommended) | Old `mode` returns 400 `invalid_request`. API is internal/undocumented; no external clients to break. Simple, atomic, auditable. | ✓ |
| Brief deprecation: accept both for one milestone | Map old `mode` to `search_mode` server-side, emit a `warnings[]` deprecation notice. Adds a mapping table and complicates validation. | |

**User's choice:** Hard reject via `extra='forbid'`.

### Sub-question C: Update existing `tests/test_search_api.py` cases — which approach?

| Option | Description | Selected |
|--------|-------------|----------|
| Rewrite in-place to new shape (Recommended) | Existing 78/79 hardening tests get migrated to `search_mode`. Keeps the hardening regression coverage. Old `mode` references swap to the appropriate `search_mode` value. | ✓ |
| Delete obsolete tests, add equivalents in v2 file | Old file shrinks to non-mode-related tests; equivalents land in the new v2 file. Cleaner separation but risks dropping a hardening assertion. | |
| Keep old tests, mark xfail until removal in v7.11 | Lazy migration. Adds tech debt and false-failing CI signal. | |

**User's choice:** Rewrite in-place to new shape.
**Notes:** All three sub-question outcomes captured as D-12 (test file structure) and D-13 (hard cutover) in CONTEXT.md.

---

## Claude's Discretion

- Pydantic model file location (`web/search_api.py` vs split file).
- Exact wording of `invalid_combination` messages, as long as both offending field names appear.
- Test fixture queries the planner picks for each `search_mode` value (must yield non-empty results per AC2).
- Whether to introduce a small `_apply_request_echo()` helper in `shared/search_serializer.py` or inline at endpoint sites.

## Deferred Ideas

- `search_mode='regex'` (D-09) — moved from 81A scope to v7.11.
- `'fuzzy'` re-introduction under a name that matches the actual variant tier (per RESCOPE §3.1).
- `variants_extended` / `variants_maximum` — v7.11.
- `text_position` (start/end of text/line) — v7.11 (join-finding).
- Global `judeo_arabic` / `plene_defective` / grammatical prefix/suffix / `exclude_words[]` — v7.11.
- Rename `/api/parallels.mode` → `search_mode` — v7.11 (OQ-2).
- Brief deprecation window for old `mode` field — explicitly rejected (D-13).
- Hashed IP in `request` echo — explicitly rejected (D-10).
