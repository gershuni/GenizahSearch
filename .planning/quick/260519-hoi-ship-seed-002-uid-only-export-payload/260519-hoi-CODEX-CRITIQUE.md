# Codex critique of commit 2a7440d6 (SEED-002 uid-only export payload)

**Date:** 2026-05-19
**Reviewer:** Codex CLI (gpt-5)
**HEAD reviewed:** 2a7440d6 (review run from a5240053; the 5 source files are unchanged from 2a7440d6)
**Verdict:** ship with 4 adjustments

## Codex's findings

### 1. ISSUE — Search allowlist is not sufficient

Real `uid` values are often `IE..._P..._FL...` from `MetadataManager.extract_unique_id` (verified at `genizah_core.py:3617-3638`), not sys-id-bearing strings. Metadata-only rows have `uid=''`. So `_resolve_result_display` tier 2 (`parse_full_id_components(uid)['sys_id']`) returns `None` for the production-common uid shape. Tier 3 (raw_header regex) only fires when `raw_header` is in the row — search rows don't have it. Excel/Word degrade to `"Unknown"` for every shelfmark in production search exports.

Also: search JSON loses `locator` page data, `image_url`, `excerpt`, `domains`, and `dating` that the previous `display` dict carried.

**Proposed fix:** store compact identity fields such as `sys_id` and/or `raw_header` in the row, and preserve or lazily rehydrate a bounded excerpt for JSON.

### 2. ISSUE — `chunk_hits` is read by the public API serializer

Verified at `shared/search_serializer.py:828` — `sub.get('chunk_hits')` drives the per-row match details in `/api/export/parallels/json`. Our fix drops `chunk_hits` from the parallels export-payload allowlist → API falls back to one degenerate match per row.

**Proposed fix:** split live-UI compaction from export compaction. Drop `chunk_hits` from `p_state.results` (live UI). KEEP a capped/sanitized `chunk_hits` in the parallels export payload (cap = 100 entries × 1000 chars per `ed6f89c4`).

### 3. ISSUE — `_resolve_result_display` tier 2 is built on a false assumption

`parse_full_id_components` looks for `(99\d{8,})` (sys_id pattern) in its input. When fed an `IE..._P..._FL...` uid, `sys_match` is None → `result['sys_id'] = None`. Tier 2 collapses to None → falls through to tier 3 → on search rows (no raw_header), falls to 'Unknown'.

**Proposed fix:** rehydrate from explicit `sys_id` (stored in row) or `raw_header` (already kept on parallels). Don't rely on uid parsing.

### 4. ISSUE — Parallels JSON byte-equivalence is not guaranteed after discarding `display`

JSON key shape is preserved, but compact rehydration reconstructs from current `meta_mgr` and does not exactly mirror `get_display_data` semantics (e.g., the fallback `"ID: {sys_id}"` when meta_mgr returns Unknown).

**Proposed fix:** centralize the display resolver to call `get_display_data`'s exact path (or mirror its fallbacks), with explicit legacy-vs-compact JSON equivalence tests.

### 5. CONFIRM — No cross-user leak

`get_meta_for_id` / `get_library_for_id` read process-global public metadata caches (`csv_bank` / `nli_cache`), not user-scoped state. Shared but not user-specific.

### 6. CONFIRM — Do not batch yet

Display lookup is in-memory. Full-text lookup is explicit export-time Tantivy work. No latency data to justify batching in this P1 fix.

### 7. CONFIRM — `<11 MB` is a defensible floor

5000 Hebrew snippets near 500 chars create a ~5 MB raw-text floor before JSON keys/UIDs/lists. Dropping redundant `match_terms` could shave overhead but isn't required.

### 8. ISSUE — New tests use sys-id-bearing fake uids

Test fixtures embed `99...` digits in the fake `uid`, so `parse_full_id_components(uid)` happens to extract a sys_id and tier 2 passes. Production uids are `IE..._P..._FL...` with NO `99...` digits → tier 2 returns None → tier 3 fails (no raw_header in search rows) → 'Unknown'. The tests pass while the production code path silently breaks.

**Proposed fix:** add regression fixtures sourced from actual index shapes:
- normal page uid: `IE188433865_P1_FL1` (no sys_id in uid)
- continuous `sys:` uid: `990012345678901_xxx`
- raw_header-bearing row (parallels)
- metadata-only row (uid='')

## Verdict: ship with 4 adjustments

NOT ready to deploy. The fix design is sound but the rehydration plumbing assumes a uid shape that production doesn't use.

## Concrete revision plan

1. **Add `sys_id` to both row allowlists** (~12 bytes/row — negligible vs 500-byte target).
   - Search: `{uid, sys_id, sort_score, snippet, match_terms}`
   - Parallels: `{uid, sys_id, sort_score, score, snippet, match_terms, source_ctx, text, raw_header, chunk_hits}` (`chunk_hits` re-added)
2. **Re-add `chunk_hits` to parallels EXPORT payload** with the existing cap (100 entries × 1000 chars).
3. **Rewrite `_resolve_result_display` to prefer `sys_id` from row over uid parsing.** Fallback chain becomes:
   - Tier 1: legacy `display` dict
   - Tier 2: `result['sys_id']` → meta_mgr
   - Tier 3: `parse_full_id_components(uid)['sys_id']` (legacy fallback for old data)
   - Tier 4: `raw_header` regex (parallels rows)
   - Tier 5: 'Unknown'
4. **Update tests with realistic uid fixtures:** at least one `IE..._P..._FL...` uid, one continuous sys uid, one metadata-only row (uid=''). Add a `test_resolve_display_with_production_uid_shape` regression test.
5. **Run full pytest again + ruff.** Expected count: similar to 2072.
6. **(Optional) Re-review with Codex** if any of #1-4 introduce design questions.

After these adjustments, the fix should be safe to deploy.
