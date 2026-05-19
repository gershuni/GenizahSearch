# SEED-002 Fixup Handoff — for Fresh Context

**Created:** 2026-05-19 (after Codex review of commit `2a7440d6` returned VERDICT: ship with 4 adjustments)
**Status:** Awaiting fixup commit before deploy
**Predecessor context budget:** 51% used when handing off
**Branch:** master-main (working tree clean, ready to commit fixup on top of `a5240053`)

## TL;DR — what you need to do

Land a single fixup commit on master-main that addresses Codex's 4 adjustments. Then re-review with Codex. Then deploy. Then verify post-deploy soak (~2h memstat poll). Then flip OPEN_ISSUES.md P1 to ✅ Fixed and SEED-002 to `shipped`.

**The fix is narrow and mechanical** — the broken assumption is that production `uid` values contain `sys_id` digits. They don't (they look like `IE188433865_P1_FL1`). The fix is to **keep `raw_header` in both row allowlists** — that single change makes all downstream code work because the serializer + resolver already have raw_header-aware paths.

## Current state on disk

```
HEAD:           a5240053 docs(quick-260519-hoi): SEED-002 ship — plan + summary + verification + STATE
                2a7440d6 fix(web): SEED-002 uid-only export payload (~44x per-row reduction)   ← REVIEWED
                899fe7af fix(memstat): tracemalloc explicit start, hard-cap depth at 5
                77483a65 feat(memstat): add /_internal/objgraph + /_internal/tracemalloc endpoints
                4688e497 docs: plant SEED-002 — uid-only export payload (44x reduction)
                3272edfa docs: note ed6f89c4 field-strip fix in P1 row; mark attribution todo provisionally superseded
                7d2b2480 docs(quick-260519-9pk): re-open P1 web memory leak — investigate secondary leak
                ed6f89c4 fix(memory): strip heavyweight row fields from export payloads   ← Production deploy point
                ...
```

**Deployed on EC2 (production):** `3272edfa` — i.e. only the doc-update is live, NOT today's afternoon work. The objgraph/tracemalloc endpoints were live briefly but the service was restarted; the new endpoints are at `899fe7af`+ which has NOT been pushed/deployed. Let me check: actually `899fe7af` was deployed at 09:20:24 UTC. Verify with `git log origin/master-main` vs `git log` to see drift. As of handoff: HEAD is `a5240053` (local), origin probably at `3272edfa`-ish but possibly at `899fe7af` after the morning ssh deploys. Run `git log origin/master-main -1` to check.

**Today's full session timeline:**
- 05:25 UTC: `ed6f89c4` field-strip fix deployed
- 09:11 UTC: I introduced a tracemalloc(25) wedge — site down ~9 min
- 09:20 UTC: `899fe7af` safety fix deployed
- 09:25 UTC: tracemalloc/objgraph attribution capture (production)
- 09:43 UTC: SEED-002 quick task started
- 13:26 UTC: `2a7440d6` SEED-002 fix committed (LOCAL ONLY, never deployed)
- 14:00 UTC (~now): Codex review returned 4 issues; user wants fixup before deploy

## What Codex caught (full critique at `260519-hoi-CODEX-CRITIQUE.md`)

| Issue | Severity | Status | Confirmed by reading code? |
|---|---|---|---|
| 1+3+8: `_resolve_result_display` tier 2 broken for production uids | CRITICAL | Need fix | YES — `parse_full_id_components` looks for `(99\d{8,})` but production uids are `IE..._P..._FL...` with no `99...` digits. Verified at `genizah_core.py:3652-3666`. |
| 2: `chunk_hits` is read by public API | HIGH | Need fix | YES — `shared/search_serializer.py:828` reads `sub.get('chunk_hits')` for `/api/export/parallels/json`. |
| 4: JSON byte-equivalence drift | MEDIUM | Need fix | Yes — `get_display_data` returns `'shelfmark': shelfmark or f"ID: {sys_id}"` (line 4925). Our resolver returns `'Unknown'`. Edge-case but real. |
| 5,6,7: cross-user / batching / 11MB floor | — | CONFIRMED OK | No changes needed. |

## Why option 1 (fixup commit) was chosen over option 2 (revert + redesign)

Probe summary (from predecessor context):
- Production result rows ALREADY carry `raw_header` at `genizah_core.py:7458, 7471` — it's a passthrough, not a new producer requirement.
- `shared/search_serializer.py:_serialize_item:253-262` ALREADY has correct raw_header-aware code (`parse_full_id_components(raw_header)`). It works fine for legacy rows. The SEED-002 fallback we added at lines 268-304 tries `parse_full_id_components(uid)` instead, which doesn't work for production `IE..._P..._FL...` uids.
- **Single change unblocks everything: keep `raw_header` in the search row allowlist.** (Parallels already keeps it.)

Per-row size impact of the fixup:
- Search row: `{uid, sort_score, snippet, match_terms, raw_header}` ≈ 30 + 8 + 1000-4000 + 50 + 50 = **~1.1-4.1 KB/row** (still 5-20× reduction from 22 KB pre-fix)
- Parallels row (chunk_hits re-added): + `score, source_ctx, text, chunk_hits` ≈ **~13 KB/row** (1.7× reduction from 22 KB; chunk_hits eats most of the budget but is genuinely required by the API)

The leak-fix goal is preserved. Trade-off accepted.

## The concrete fixup design (5 steps)

### Step 1: `web/export_state.py`

**Search row allowlist:** add `raw_header` to `_SEARCH_ROW_ALLOWLIST`. Current is `{'uid', 'sort_score', 'snippet', 'match_terms'}`. New: `{'uid', 'sort_score', 'snippet', 'match_terms', 'raw_header'}`.

**Parallels row allowlist:** add `chunk_hits` back to `_PARALLELS_ROW_ALLOWLIST`. Current is `{'uid', 'sort_score', 'score', 'snippet', 'match_terms', 'source_ctx', 'text', 'raw_header'}`. New: `{'uid', 'sort_score', 'score', 'snippet', 'match_terms', 'source_ctx', 'text', 'raw_header', 'chunk_hits'}`.

**Parallels chunk_hits compaction:** the parallels compactor must still CAP chunk_hits at 100 entries × 1000 chars (matching `ed6f89c4`'s cap). Reuse the `_PARALLELS_CHUNK_HITS_CAP = 100` and `_PARALLELS_CHUNK_TEXT_STORAGE_CHARS = 1000` constants if they were deleted; otherwise restore. The `_compact_chunk_hit` helper (from `ed6f89c4`) is also needed. Read `ed6f89c4` for reference: `git show ed6f89c4:web/export_state.py`.

### Step 2: `web/export_service.py`

Rewrite `_resolve_result_display` to swap tier priority. Current order is:
1. legacy `display` dict
2. `parse_full_id_components(uid)['sys_id']` ← broken for production uids
3. `raw_header` regex `(99\d{8,})`
4. `'Unknown'` fallback

New order:
1. legacy `display` dict (back-compat for non-compact rows)
2. `raw_header` regex `(99\d{8,})` ← PROMOTE TO TIER 2 since raw_header is now always present
3. `parse_full_id_components(uid)['sys_id']` ← demote to legacy-data fallback
4. `'Unknown'` fallback

**Also (Issue 4):** mirror `get_display_data`'s `"ID: {sys_id}"` fallback when meta_mgr returns Unknown. After resolving sys_id but failing meta_mgr lookup, return `(f"ID: {sys_id}", '', '', '')` instead of `('Unknown', '', '', '')`. This preserves API JSON byte-equivalence for the edge case where sys_id is known but the manuscript isn't in libraries.csv.

### Step 3: `shared/search_serializer.py`

The SEED-002 fallback at lines 268-304 currently does:
```python
uid_for_parse = result.get('uid', '') or ''
if uid_for_parse:
    parsed_uid = meta_mgr.parse_full_id_components(uid_for_parse) or {}
    _sid = parsed_uid.get('sys_id') or ''
if not _sid and raw_header:
    m = re.search(r'(99\d{8,})', raw_header)
    if m:
        _sid = m.group(1)
```

Swap the order: try `raw_header` regex FIRST, then `parse_full_id_components(uid)` as legacy fallback. (Or just delete the uid-parsing fallback since raw_header is now always present in compact rows.)

Also mirror the `"ID: {sys_id}"` fallback for the rehydrated display dict's `shelfmark` field (matches Step 2's MEDIUM #4 fix).

### Step 4: `tests/test_export_state_cap.py`

Update the 5 SEED-002 tests added in `2a7440d6`:

- `test_search_export_row_has_only_uid_keys`: update expected key set to `{uid, sort_score, snippet, match_terms, raw_header}`.
- `test_parallels_export_row_keeps_safe_allowlist`: update expected key set to `{uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header, chunk_hits}`.
- `test_per_row_bytes_drops_to_under_2kb`: search row may now be slightly larger (~30 extra bytes for raw_header). Confirm still under 2 KB with 500-char Hebrew snippet.
- `test_5000_row_payload_under_5mb` (or whatever the executor named it as `<11 MB`): re-verify; size impact of 30 bytes/row × 5000 rows = 150 KB, well within tolerance.
- `test_field_strip_fix_still_works`: should still pass (full_text, raw_file_hl, content still stripped).

**ADD new regression tests with production uid shapes (Codex Issue 8):**
- `test_resolve_display_with_production_uid_shape`: pass a row with `uid='IE188433865_P1_FL1', raw_header='99001234567890 IE188433865 P1 FL1'`, mock `meta_mgr.get_meta_for_id('99001234567890')` to return `('T-S K1.1', 'Test Title')`. Assert Excel cell shows `'T-S K1.1'`.
- `test_resolve_display_metadata_only_row`: pass a row with `uid='', raw_header=''` and `display={'shelfmark': 'T-S NS 329.96', 'title': 'Synthetic'}`. Assert Excel cell shows `'T-S NS 329.96'` (tier 1 back-compat works).
- `test_resolve_display_id_prefix_fallback`: pass a row with `raw_header='99001234567890 ...'` but mock `meta_mgr.get_meta_for_id` to return `('Unknown', '')`. Assert Excel cell shows `'ID: 99001234567890'` (the `"ID: {sys_id}"` fallback from `get_display_data`).

### Step 5: `tests/test_export_service.py`

Update the 3 rehydration tests added in `2a7440d6` to use realistic uids (mostly: where they used `uid='990...'`, change to `uid='IE..._P..._FL...'` and add `raw_header='99... IE... P... FL...'`). The `test_excel_output_content_equivalent_pre_vs_post_uid_only` test in particular needs to exercise both uid shapes.

## Quality gates before re-review

1. **Full pytest:** `python -m pytest tests/ -x -q --ignore=tests/test_line_numbers_desktop.py` — expect ≥2072 passing (current baseline) + new regression tests. No regressions in: `test_export_state_cap.py`, `test_export_service.py`, `test_export_cross_user_isolation.py`, `test_api_export_json.py`, `test_api_legacy_unchanged.py`, `test_no_raw_storage_access.py`, `test_no_appstate_export_fields.py`, `test_no_deleted_state_references.py`.

2. **ruff:** `python -m ruff check web/export_state.py web/export_service.py shared/search_serializer.py tests/test_export_state_cap.py tests/test_export_service.py` — all checks passed.

3. **Live API smoke (optional, before deploy):** if you have a way to test `/api/export/parallels/json` locally with a sample payload, confirm `chunk_hits` populates per-row match details.

## Re-review with Codex after the fixup

Once the fixup commit lands, re-invoke Codex with a brief that:
1. Cites the new commit hash.
2. Specifically asks: did the 4 adjustments land correctly? Are there NEW issues? Any regressions in the original 5 confirmations?
3. Asks for VERDICT: 'ship as written' / 'ship with N adjustments' / 'redesign'.

Brief location: `_tmp/seed002-fixup-claude-take-for-codex.md` (mirror the structure of `_tmp/seed002-claude-take-for-codex.md`).

Codex invocation pattern (from `memory/feedback_codex_during_discuss_phase.md`):
```bash
codex exec --skip-git-repo-check "Read C:/Genizahsearch/_tmp/seed002-fixup-claude-take-for-codex.md. Read the actual repo files at HEAD = <new fixup commit hash>. Verify the 4 adjustments from the previous critique landed. Answer the N questions. VERDICT: 'ship as written' / 'ship with N adjustments' / 'redesign'."
```

Save Codex's output as `260519-hoi-CODEX-CRITIQUE-2.md` in the quick task dir.

## After Codex approves: deploy

```bash
git push origin master-main
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd GenizahSearch && ./deploy.sh"
```

## Post-deploy verification (~2h soak)

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com 'PID=$(systemctl show genizah-web.service -p MainPID --value); SECRET=$(sudo bash -c "tr \"\\0\" \"\\n\" < /proc/$PID/environ" | grep -a "^MEMSTAT_SECRET=" | cut -d= -f2-); curl -s -H "X-Memstat-Secret: $SECRET" http://localhost:8081/_internal/memstat | python3 -m json.tool | head -40'
```

**Verdict gate:**
- ✅ Pass: `export_search_payload` per session is in KB range (not MB); RSS growth < 30 MB/hr over the soak window.
- ⚠️ Fail: investigate further — possibly the NiceGUI Observable subscription retention is the residual leak (out of scope for SEED-002 — separate framework-bug investigation).

Optional bonus diagnostic:
```bash
# Start tracemalloc(depth=1), do a representative heavy search, then:
curl ... "/_internal/tracemalloc?action=snapshot&limit=30"
# Top allocators in web/pages/search_state.py:262/258/267 should be DRAMATICALLY smaller.
# (Note: web/pages/search_state.py is INTENTIONALLY untouched in this fix —
#  the tab-restore contract preserves display dict for live UI use.
#  Its allocator pressure is a separate follow-up, not blocking.)
```

## Documents to update post-verification (separate doc-only quick task)

If soak passes:
1. `docs/OPEN_ISSUES.md` P1 row: flip from `🟡 Field-strip fix in code` to `✅ Fixed (2026-05-19, ed6f89c4 + 2a7440d6 + <fixup hash>)`. Decrement P1 Open 1→0; Total Open 34→33.
2. `.planning/seeds/SEED-002-uid-only-export-payload.md`: flip `status: dormant` → `status: shipped`. Add commit references in frontmatter or footer.
3. `.planning/todos/pending/2026-05-19-leak-attribution-phase.md`: move to `.planning/todos/done/` with verdict block. The attribution work is now complete — production tracemalloc data named the surface, fix shipped, soak confirmed.
4. `.planning/STATE.md`: update last_activity to reflect ✅ Fixed status.

## Key files & line refs (for the next context)

| File | Where | Why |
|---|---|---|
| `_tmp/seed002-claude-take-for-codex.md` | full brief that went to Codex | reference for the fixup brief shape |
| `.planning/quick/260519-hoi-ship-seed-002-uid-only-export-payload/260519-hoi-PLAN.md` | the original plan (revised once) | reference, don't rewrite |
| `.planning/quick/260519-hoi-ship-seed-002-uid-only-export-payload/260519-hoi-CODEX-CRITIQUE.md` | Codex's 4 issues with proposed fixes | authoritative source for what to fix |
| `web/export_state.py` | `_SEARCH_ROW_ALLOWLIST` + `_PARALLELS_ROW_ALLOWLIST` near line 69-73 | Step 1 |
| `web/export_state.py` | `_compact_parallels_result_row` | Step 1 — chunk_hits handling |
| `web/export_service.py` | `_resolve_result_display` ~line 76 | Step 2 |
| `shared/search_serializer.py` | `_serialize_item` lines 268-304 (SEED-002 fallback block) | Step 3 |
| `genizah_core.py:3617-3666` | `extract_unique_id` + `parse_full_id_components` | reference — confirms production uid shape |
| `genizah_core.py:4915-4931` | `get_display_data` | reference — the `"ID: {sys_id}"` fallback to mirror |
| `genizah_core.py:7458, 7471` | result row construction | reference — confirms `raw_header` always present |
| `shared/search_serializer.py:828` | `chunk_hits` read site | reference — confirms public API requires it |

## What NOT to do in the fixup

- Don't touch `web/pages/search_state.py` (still out of scope — tab-restore contract).
- Don't touch `/_internal/objgraph` or `/_internal/tracemalloc` endpoints — already shipped and working.
- Don't restart `genizah-web.service` — separate user decision.
- Don't fix NiceGUI Observable subscription retention — framework bug, out of scope.
- Don't update `docs/OPEN_ISSUES.md` or `SEED-002` status in this fixup — those happen in a separate doc-only quick task AFTER post-deploy verification.

## Estimated effort

- Fixup commit: 30-45 min (mechanical changes + test updates)
- Codex re-review: 5 min (CLI invocation + verdict read)
- Apply any new Codex adjustments: 0-15 min (depends on what's caught)
- Deploy + initial smoke: 5 min
- Soak window: 2 hours (passive — you don't need to do anything, but the gate is gated on real traffic)
- Post-soak doc updates: 10 min

**Total active work: ~1 hour. Total wall-clock to ✅: ~3 hours.**
