---
id: SEED-002
status: shipped
planted: 2026-05-19
planted_during: v7.13 — roadmap locked (Phases 93 + 94), pre-Phase-93
shipped: 2026-05-19
shipped_commits: 2a7440d6 + 43a0fa0a + 7633a40e + 0aa92f82 (initial fix + 3 fixup commits per Codex round-1 review + self-audit catch)
trigger_when: SHIPPED
scope: medium
predecessor_fix: commit ed6f89c4 (2026-05-19 morning) — field-strip fix that capped export payloads at 110 MB/user worst case but did not eliminate them
successor_investigation: .planning/quick/260519-hoi-investigate-framework-retention/INVESTIGATION.md + PROPOSED-FIX.md — post-deploy investigation confirmed SEED-002 reduced per-row payload from 22 KB → 1.6 KB (in-RAM) and ~500 B (logical), and attributed the residual ~200 MB/heavy-search RSS pressure to NiceGUI ObservableDict wrapping work on the storage write path (NOT a logical leak; pymalloc high-water mark). Future-phase fix sketched: store payloads as JSON-encoded strings to bypass NiceGUI's auto-wrapping entirely.
---

# SEED-002: uid-only export payload — store {uid, sort_score, snippet, match_terms} per row and rehydrate the rest at export time

## Why This Matters

The 2026-05-19 field-strip fix (commit `ed6f89c4`) made `export_search_payload` / `export_parallels_payload` **bounded** but not negligible. Each compacted row still weighs ~22 KB (Hebrew text + bidi markers + chunk_hits + 500-char excerpt + display dict + match_terms), so the worst case per heavy user is 5000 rows × 22 KB ≈ **110 MB**. Five concurrent heavy users → 550 MB just for export payloads; twenty → 2.2 GB. That's the new ceiling, observed 2026-05-19 with one heavy session at 118 MB total / 112 MB in `export_search_payload`.

Most of the 22 KB/row is **redundant** — `shelfmark`, `library`, `title`, `library_code` can all be re-derived from `uid` (or `sys_id`) at export time via `metadata_manager`, and `full_text` is already rehydrated from Tantivy lazily in `web/export_service.py:_resolve_result_full_text`. Only `{uid, sort_score, snippet, match_terms}` are genuinely query-specific and must live in the payload (the snippet has query-dependent highlight markers; match_terms drives Excel highlight fill).

Storing uid-only would drop each row from 22 KB → ~500 bytes (~44× reduction):

| Scenario | Current ceiling | After SEED-002 |
|---|---:|---:|
| Per heavy user | 110 MB | ~2.5 MB |
| 5 concurrent heavy users | 550 MB | 12.5 MB |
| 20 concurrent heavy users | 2.2 GB | 50 MB |

The current fix made export payloads BOUNDED. This follow-up would make them NEGLIGIBLE.

## When to Surface

**Trigger:** v7.13 milestone close (natural fit with Phase 94, which already touches the export paths for research-grade metadata) OR earlier if memstat soak shows export payloads becoming the dominant top_key under real production traffic.

This seed should be presented during `/gsd-new-milestone` when the milestone scope matches any of these conditions:
- Memory/RSS work on the web service
- Export-related changes (xlsx, JSON, Word, parallels)
- v7.14+ scope review when v7.13 closes
- Any phase touching `web/export_state.py` or `web/export_service.py`

**Early-surface override:** if `/_internal/memstat` post-deploy soak shows `export_search_payload` >100 MB as the dominant top_key on multiple sessions (verifiable via `top_keys` array per session — diagnostic shipped in `web/storage_diagnostics.py` in commit `ed6f89c4`), insert this work ahead of v7.13 via `/gsd-insert-phase`.

## Scope Estimate

**Medium** — a phase or two; needs planning. Risk is LOW: the rehydration plumbing is already wired up and tested for `full_text`. Extending it to display fields is mechanical.

### Implementation outline (not authoritative — formalize via /gsd-plan-phase when surfaced)

1. **`web/export_state.py`** — extend `_compact_search_result_row` / `_compact_parallels_result_row` to keep only `{uid, sort_score, snippet, match_terms}`. Drop the `display` dict entirely (re-derived at export time). Drop `full_text_excerpt` (re-derived from Tantivy if needed).
2. **`web/export_service.py`** — extend `_resolve_result_full_text` into a family: `_resolve_result_display(row)` returns `{shelfmark, title, library_code, library_name}` via `meta_mgr.get_meta_for_id(uid_to_sysid) + get_library_for_id + get_library_display`. Apply at every export site (Excel, JSON, Word, parallels).
3. **`shared/search_serializer.py`** — same rehydration in `serialize_search_payload` / `serialize_parallels_payload` so the public API JSON output is unchanged.
4. **`web/api.py`** — verify no handler reads `display` fields directly from `safe_user_get('export_search_payload')` rows; all reads go through the rehydrated row.
5. **Tests** — extend `tests/test_export_state_cap.py` to assert per-row size ≤ 1 KB after compaction; extend `tests/test_export_service.py` to assert Excel output is byte-identical (or content-equivalent) before vs after the uid-only switch. Cross-user isolation tests stay green.

### What's tricky

- The `snippet` field has query-specific bidi-marker highlighting that can't be re-derived from Tantivy alone. Must stay in the payload (small — typically ~1-4 KB per row, much less than the full row).
- The `match_terms` array drives Excel highlight fill in `export_service.py:export_search_results_excel` — must stay (small).
- `sort_score` is search-specific (Tantivy BM25 result), must stay (~8 bytes).
- For parallels: `chunk_hits` is the heavy field (cap 100 × 1 KB = up to 100 KB/row worst case in current fix). Decision needed: keep capped, or also rehydrate? Codex's diff has chunk_hits at 1000 chars/hit which is already aggressively truncated. Leave as-is unless data shows it's still dominant.

## Breadcrumbs

Related code and decisions in the current codebase as of 2026-05-19:

- **`web/export_state.py`** — fix shipped in `ed6f89c4` (row compactor for search + parallels; cap at 5000; `_compact_results` helper; `compact_user_storage_export_payloads` for startup compaction).
- **`web/export_service.py:55`** — `_resolve_result_full_text` is the existing lazy-rehydration pattern. The model to extend.
- **`web/storage_diagnostics.py`** — `top_keys` per-payload diagnostic shipped in `ed6f89c4`; this is how we'll detect whether the early-surface trigger fires.
- **`shared/search_serializer.py`** — public API JSON output. Already falls back to `full_text_excerpt` when `full_text` is absent (line 270, post `ed6f89c4`).
- **`genizah_core.py:8452`** — `Searcher.get_full_text_by_id(uid)` — Tantivy lookup by unique_id. The rehydration source.
- **`metadata_manager.get_meta_for_id(sys_id)`** and **`get_library_for_id(sys_id)`** — already used in `web/export_service.py:ExportService.get_metadata` and `.get_library_code`. The rehydration source for display fields.
- **`docs/OPEN_ISSUES.md` P1 row** ("Web server memory leak") — current status notes the field-strip fix as 🟡 awaiting soak. If soak passes (RSS < 30 MB/hr, export payloads no longer dominant), this seed remains dormant until v7.13 close. If soak fails, the seed's early-surface trigger fires.
- **`.planning/todos/pending/2026-05-19-leak-attribution-phase.md`** — provisionally superseded by `ed6f89c4`; would re-activate if SEED-002 isn't enough OR if a non-export surface is the leak. SEED-002 is the OPTIMIZATION path; that todo is the ATTRIBUTION path.
- **Commit `f2e456d4`** (2026-05-18) — the count-cap fix that preceded `ed6f89c4`. Sequence: count cap → field strip → uid-only (SEED-002).
- **v7.13 Phase 94 scope (`.planning/PROJECT.md`, `.planning/STATE.md`)** — research-grade export metadata, web + desktop xlsx. SEED-002 is a natural fit to absorb here if the planner sees the cohesion.

## Notes

- **Why a seed and not a v7.13 phase?** v7.13 roadmap is LOCKED at Phases 93 + 94 with 14/14 reqs mapped (2026-05-19). Adding a 3rd phase would re-litigate the lock; not warranted unless production data forces it. Phase 94 (research-grade export metadata) is the natural absorption point but planning hasn't started — the seed surfaces at v7.13 close so the next planner has the context.
- **Why not just do it now?** Today's fix already capped the worst case at 110 MB/user. Sustainable for ≤20 concurrent heavy users on an 8 GB box. We need to *observe* whether 110 MB/user is actually the bottleneck (vs. some other surface) before pursuing 44× reduction. If post-deploy soak shows RSS plateaus comfortably under 6 GB, this can wait for Phase 94. If RSS keeps climbing, the early-surface trigger fires.
- **Composability with the leak-attribution todo:** SEED-002 (optimization) and `2026-05-19-leak-attribution-phase.md` (attribution) are NOT redundant. If the soak passes, only SEED-002 survives as a future optimization. If the soak fails, the attribution todo runs first to identify which surface is leaking; SEED-002 may or may not be the answer depending on what objgraph attributes.
- **Risk reminder:** the existing `_resolve_result_full_text` returns `''` if Tantivy can't find the uid (e.g., index rebuilt mid-session). The display-field rehydration should follow the same pattern — graceful degradation to a "[unknown shelfmark]" placeholder, not a crash. metadata_manager already does this via the `('Unknown', '')` fallback in `ExportService.get_metadata`.
