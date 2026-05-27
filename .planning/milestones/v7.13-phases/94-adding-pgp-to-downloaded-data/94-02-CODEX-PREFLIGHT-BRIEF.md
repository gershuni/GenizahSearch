# Codex Pre-Flight Brief — Phase 94 Wave 2 (web state plumbing + JSON)

You are doing **integration archaeology** on Wave 2 of GenizahSearch Phase 94 *before* it executes. Scope: Wave 2 only (`web/export_state.py` extensions + `shared/search_serializer.py:_serialize_item` JSON additions + 3 `set_search_export` call sites + 1 `update_search_export_enrichment` patch site).

Plan: `.planning/phases/94-adding-pgp-to-downloaded-data/94-02-PLAN.md`.
Context: `94-CONTEXT.md` (especially D-08, D-10, D-11) + `94-CODEX-CRITIQUE.md`.
Live code: `web/export_state.py`, `shared/search_serializer.py`, `web/pages/search.py`, `web/api.py`.

The earlier bilateral review (`94-REVIEWS.md`) raised these HIGH concerns on Wave 2:
- (a) JSON `domains` will be empty on compacted rows because `serialize_search_payload` batches domains keyed off `display.id` and compacted rows lack `display`.
- (b) History-restore reseeds `transcription_sys_ids=set()` / `printed_ids=set()` / `result_domains={}`, so restored exports carry false/empty metadata.
- (c) Wave 2 self-consistency: must_haves say `export_excel` passes new kwargs in Wave 2, but Task 4 defers activation to Wave 3.

Verify each against the real code. The key signal you may have missed in the bilateral review: `_compact_search_result_row` at `web/export_state.py:196-227` (SEED-002 fixup, 2026-05-19) synthesizes a top-level `sys_id` from `display.id` / `raw_header` / `uid` BEFORE dropping `display`. And `_serialize_item` at `shared/search_serializer.py:232-313` has a matching SEED-002 fixup that rehydrates `display` and computes `final_sys_id` for compacted rows. So concern (a) above MAY be stale.

## What to verify

Read the real code first, then answer each question as **PROBLEM** (concrete plan bug — explain what breaks and what to change), **OK** (verified against real code), or **UNCERTAIN** (need more info — say what). Cite file:line evidence.

### Q1 — Is concern (a) actually real after SEED-002?

Trace through `web/api.py:export_json` → `shared/search_serializer.py:serialize_search_payload` → `_serialize_item` with a COMPACTED row as input (i.e. `row = {'uid': 'IE...', 'raw_header': '...', 'sys_id': '12345678901', ...}` with no `display`). Does `final_sys_id` at `:262` resolve to the synthesized sys_id, and does `domain_batch.get(final_sys_id)` actually receive a populated dict for that sys_id from the path `serialize_search_payload` builds? If NOT, where is the break? If YES, the bilateral concern was stale and Wave 2's plan does not need to touch sys_id extraction.

### Q2 — `domain_batch` population path

Trace where `domain_batch` is constructed inside `serialize_search_payload` (around `:419+`). Does it iterate over `result['display']['id']` (which would break for compacted rows), or over a resolved sys_id that has the same fallback chain as `_serialize_item`? If the former, the bilateral concern IS real for the batch-build step even if individual-item rehydration works. State which.

### Q3 — History-restore enrichment loss

Find every call site in `web/pages/search.py` that restores a search from history / saved state and ALSO calls `set_search_export(...)`. Does it call `set_search_export(..., transcription_sys_ids=set(), printed_ids=set(), result_domains={})` with empty sets, or does it skip those kwargs entirely, or does it pass the restored sets from session storage? What's the actual data state of these 3 sets at history-restore time? If the restored exports really do carry false/empty metadata, propose ONE of: recompute on restore / mark restored exports as metadata-incomplete / scope restored exports out of EXPORT-META scope.

### Q4 — `export_excel` kwarg activation timing

Open Plan 94-02 and find both the `must_haves` list and Task 4. Are they really internally inconsistent about whether `export_excel` activates the new kwargs in Wave 2, or is the apparent contradiction a misreading (e.g. Wave 2 plumbs them into the payload but Wave 3 reads them out)? State exactly which the plan intends and where the wording needs to change.

### Q5 — Stage-1 vs Stage-2 enrichment race

The plan's `update_search_export_enrichment` is called at both Stage-1 (visible-page enrichment) and Stage-2 (full-result enrichment) completion sites in `web/pages/search.py`. If a user clicks Export between Stage-1 and Stage-2 completion, what data lands in the export payload — the partial Stage-1 set, or nothing? Is the partial-data state safe (no false positives — `is_printed=False` is correct under "we haven't enriched this row yet"), or does it produce a misleading export (sys_id is in PGP but Stage-2 hasn't run, so `has_pgp=False` for a row that actually has PGP)? Concrete file:line evidence of what state exists when.

### Q6 — Parallels envelope D-10 negative test reality

CONTEXT D-10 + Plan 94-02 add a negative regression test asserting `serialize_parallels_payload` does NOT emit `has_pgp` / `is_printed` keys. Verify: does `serialize_parallels_payload` use `_serialize_item` directly (via `_to_parallels_envelope_item`)? If yes, adding `has_pgp`/`is_printed` to `_serialize_item` MUST be conditional on something (a kwarg, a context flag), or the parallels envelope will inherit them silently. State whether the plan handles this correctly — if it adds the keys unconditionally to `_serialize_item`, the negative test will fail and Wave 2 will need a code path split.

### Q7 — Any other Wave 2 data-flow trap?

Anything you'd flag HIGH given real-code archaeology that the bilateral review missed. Especially: monkeypatch targets for the new tests (does `_serialize_item` import `_safe_library_name` and the `domain_batch` parameters in a way that lets tests intercept?), and whether `update_search_export_enrichment` preserves the Phase 88 copy-on-update + isinstance discipline correctly.

## Output format

For each question (Q1..Q7): one labeled section, verdict tag (PROBLEM / OK / UNCERTAIN / STALE — use STALE when a bilateral concern turned out to be already handled), 2-5 sentence finding with file:line evidence, and (if PROBLEM) a one-sentence concrete fix. No general summary.
