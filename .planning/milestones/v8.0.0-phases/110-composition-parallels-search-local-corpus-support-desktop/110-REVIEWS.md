---
phase: 110
reviewers: [gemini, codex]
reviewed_at: 2026-06-08T15:51:39Z
plans_reviewed: [110-01-PLAN.md, 110-02-PLAN.md, 110-03-PLAN.md, 110-04-PLAN.md]
note: claude CLI skipped for independence (review run from inside Claude Code)
---

# Cross-AI Plan Review — Phase 110

> Composition / Parallels Search — LOCAL Corpus Support (desktop). Two independent external
> reviewers (Gemini, Codex). Codex notes its sandbox could not spawn PowerShell/Node to verify
> against live source, so its review is based on plan text + embedded code references — a few
> HIGH findings may already be satisfied in the plans and should be confirmed during `--reviews`.

## Gemini Review

# Cross-AI Plan Review: Phase 110 — Composition / Parallels LOCAL Corpus Support

## 1. Summary
The implementation plans for Phase 110 are exceptionally thorough, demonstrating a deep understanding of the existing codebase and the specific "gotchas" of the GenizahSearch architecture. The strategy of mirroring the Search-tab corpus selector pattern onto the Composition/Parallels tab is the correct approach for user familiarity and architectural consistency. The plans successfully decouple "Lab Mode" from the "LOCAL" corpus, fulfilling a key requirement. The fix for the long-standing "weights-hash mismatch" in Plan 02 is a standout architectural improvement that unblocks standard composition for LOCAL hits. The export strategy in Plan 04 correctly navigates the data-shape differences between regular search results and grouped composition items.

## 2. Strengths
- **Pattern Consistency:** Rigorous adherence to the established `Genizah / Local / ALL` selector pattern from the Search tab ensures a seamless user experience.
- **Root Cause Resolution:** The `_lab_weights_hash_override` in Plan 02 (RF-4) elegantly fixes the "silent drop" bug in standard composition without introducing risky re-indexing logic on the UI thread.
- **Lab Mode Orthogonality:** Correctly identifies that Lab Mode is an algorithmic axis (fingerprint vs. BM25), not just a corpus proxy, and preserves it as an independent toggle.
- **Export Parity:** Reuses the Phase 103 export primitives (`build_local_document_row`, etc.), ensuring consistent output across all desktop surfaces.
- **Nyquist-Compliant Verification:** Plan 01 establishes a solid Wave-0 test scaffold, ensuring every commit can be validated against requirements from the start.

## 3. Concerns
- **Staleness Signal Visibility (Plan 03):**
  - **Concern:** The staleness signal (`lbl_comp_local_stale`) is only updated in `on_comp_scan_finished`.
  - **Risk [LOW]:** If a user selects "Local" or "ALL" but doesn't run a search, they won't see the staleness warning until *after* a potentially empty/incomplete search completes.
- **Rebuild Hook Coverage (Plan 03, Task 1.5):**
  - **Concern:** The plan mentions re-injecting the weights-hash override after a LabEngine rebuild.
  - **Risk [MEDIUM]:** If the user rebuilds the index in the "My Library" tab and then switches back to Composition without a restart, the `SearchEngine`'s override might still hold the *old* hash until a search is run or the app is restarted (depending on when the injection fires).
- **Grouped Item Detection (Plan 04, Task 1):**
  - **Concern:** Plan 04 correctly identifies that grouped items lack a `display` dict.
  - **Risk [LOW]:** Relying on `item.get('src_lbl') == 'LOCAL'` assumes this field is consistently populated across both `search_composition_logic` and `lab_composition_search`. The plan includes `is_local_sys_id` as a fallback, which is robust.

## 4. Suggestions
- **Proactive Staleness Check:** In Plan 03, consider calling the freshness check and updating `lbl_comp_local_stale` when the `comp_corpus_scope_combo` index changes (guarded by `try/except`), not just when a scan finishes. This gives the user immediate feedback that their selected scope is stale.
- **Explicit Rebuild Signal:** In `genizah_app.py`, wire the `_on_lab_rebuild_finished` signal (if it exists) to trigger the weights-hash re-injection into `self.searcher`. This ensures the `SearchEngine` stays in sync with `LabEngine` after a manual rebuild.
- **Export Item Partitioning:** In Plan 04 Task 1, ensure the `all_items` collection for filepath priming correctly handles both the "Main" and "Appendix" lists of the composition report to avoid missing local path resolutions in the appendix.

## 5. Risk Assessment: LOW
The overall risk is **LOW**. The plans are surgical, reuse proven components, and explicitly include regression tests for the Genizah default path. The separation of concerns between the engine (`genizah_core`), threading (`gui_threads`), and UI (`genizah_app`) is maintained perfectly. The adversarial checks (i18n, cloud-write gates, and non-regression) are all accounted for in the Wave 0 test scaffold.

---
**Verdict:** Ready for execution. The plans satisfy all 4 Roadmap Success Criteria and the COMP-LOC-01/02 and EXP-F3 requirements.

---

## Codex Review

Note: I attempted direct local source checks, but the sandbox could not spawn PowerShell/Node (`windows sandbox: spawn setup refresh`). The review below is based on the supplied plan text and embedded code references.

### 110-01

**Summary**
Strong TDD-first scaffold, but it overreaches: several tests depend on helpers/UI behavior that later plans do not actually expose cleanly. The biggest risk is creating tests that either cannot be collected or cannot be satisfied by the planned implementation shape.

**Strengths**
- Creates requirement-named tests before implementation, which is good for wave sampling.
- Correctly calls out grouped composition items lacking `display`.
- Pre-seeds the key D-08 staleness string in EN/HE.
- Keeps corpus combo item labels aligned with the existing hardcoded Search-tab pattern.

**Concerns**
- **HIGH:** `tests/test_comp_export_local.py` is told to test a "pure function" Plan 04 will introduce, but Plan 04 only edits local closures inside `export_comp_report` in `genizah_app.py:20447`. Local closures are not testable without driving QFileDialog/export side effects.
- **HIGH:** Plan 02 verification runs all `test_comp_corpus_scope.py`, but `test_no_cloud_write_on_local_comp` and some routing tests may require UI/app wiring from Plan 03.
- **MEDIUM:** `test_genizah_default_nonregression` comparing explicit `corpus_scope='genizah'` to omitted `corpus_scope` only proves new default equivalence, not byte-for-byte parity with the old Genizah path.
- **MEDIUM:** Missing pre-seeded i18n keys if Plan 03 adds `tr("Corpus:")` or Plan 04 adds `tr("Local Documents")`.
- **LOW:** "Does not silently drop" is ambiguous for stale LAB. Correct behavior is likely "skip stale LOCAL hits but set/surface a stale flag."

**Suggestions**
- Define a real module-level export helper in Plan 04, e.g. `_build_comp_export_tables(...)`, and point tests at that.
- Mark UI-dependent/cloud-write tests `xfail` until Plan 03, or scope them to pure engine behavior.
- Add a fixture that asserts the LOCAL hook is not called for Genizah, not just default-call equality.
- Pre-seed any new visible labels used later, including `Corpus:` / `Local Documents` if not already translated.

**Risk Assessment: MEDIUM**
Good scaffolding intent, but high risk of tests drifting from the implementation surface.

### 110-02

**Summary**
The core engine direction is right: make corpus scope orthogonal and gate Genizah vs LOCAL loops. The plan's main hazards are signature compatibility, stale-state races, and the weights-hash override being only half-fixed until Plan 03.

**Strengths**
- Correctly preserves Lab Mode as a separate algorithmic mode, not a LOCAL proxy.
- Correctly avoids RRF for composition and uses existing accumulators.
- Correctly identifies the live RF-4 hash mismatch.
- Threads `corpus_scope` through both composition QThreads.

**Concerns**
- **HIGH:** `lab_composition_search` is instructed to add `corpus_scope` after `scan_limit=50000` (`genizah_core.py:1402` area). That can break any positional callers for `boundary_mode` and later args. Add it at the end.
- **HIGH:** `_lab_weights_hash_override` read path lands in Plan 02, but injection is Plan 03. Plan 02's "ALL includes LOCAL hits" tests will only pass if tests manually set the override.
- **HIGH:** Invalid `corpus_scope` currently behaves like ALL under `if corpus_scope != 'local'` and `!= 'genizah'`, which can expose LOCAL data on typo. Normalize/validate and fail closed to `genizah`.
- **MEDIUM:** `local_lab_searcher_stale` is shared mutable engine state. Concurrent or changed-in-flight composition runs can show stale status for the wrong run.
- **MEDIUM:** Local-only branches may rely on variables initialized in the Genizah loop. The plan should explicitly require accumulator/progress initialization before both branches.
- **MEDIUM:** "Stale" vs "no LOCAL index/library" is conflated. Do not show outdated-index warnings for a user with no LOCAL index.

**Suggestions**
- Append `corpus_scope='genizah'` as the final parameter in both engine methods.
- Add a small normalizer: unknown/falsey scope -> `genizah`, or raise before querying.
- Return staleness metadata in the result dict, rather than only mutating `engine.local_lab_searcher_stale`.
- In tests for Plan 02, set `_lab_weights_hash_override` explicitly.
- Reset stale flags at the start of local/all searches, and distinguish stale/missing/unsearchable states.

**Risk Assessment: HIGH**
The idea is sound, but signature placement and stale/hash lifecycle issues are serious enough to cause regressions or privacy-adjacent scope mistakes.

### 110-03

**Summary**
This plan covers the required desktop UI wiring, but the staleness display and weights-hash injection are too stateful. It also assumes language toggling will update hardcoded combo labels without specifying how.

**Strengths**
- Mirrors the Search-tab selector pattern at `genizah_app.py:5953`.
- Keeps composition and Search-tab corpus selectors independent.
- Preserves Lab Mode checkbox and passes scope to both standard and Lab threads.
- Correctly avoids activating the dormant post-search LOCAL filter.

**Concerns**
- **HIGH:** `on_comp_scan_finished` reads current UI state to decide which engine/scope ran. If the user changes Lab Mode or corpus while a thread is running, the stale label can reflect the wrong run.
- **HIGH:** Startup-only `_lab_weights_hash_override` injection is insufficient. If Lab weights change or the LAB index rebuilds, the override can become stale and mask true staleness.
- **MEDIUM:** Human verification asks language toggle to update combo labels, but the plan only creates labels based on `CURRENT_LANG` at construction.
- **MEDIUM:** `logger.warning(...)` may drift if `genizah_app.py` uses a different logger name.
- **MEDIUM:** If `tr("Corpus:")` is added, Plan 01 did not guarantee the key exists in HE.
- **LOW:** Staleness label placement is underspecified; composition controls are already dense.

**Suggestions**
- Capture `_active_comp_scope` and `_active_comp_lab_mode` when launching the thread; use those in the finish slot.
- Better: include `corpus_scope`, `lab_mode`, and `local_lab_stale` in the thread result payload.
- Refresh/inject the LabEngine hash immediately before any standard composition local/all run, and after every LAB rebuild or dynamic-weight change.
- Either implement combo relabeling in the app's language-refresh path or remove that from the human checkpoint.
- Add/confirm i18n keys for any new `tr(...)` labels.

**Risk Assessment: MEDIUM-HIGH**
UI wiring is straightforward, but stale/hash state can be wrong in realistic user interaction unless captured per run.

### 110-04

**Summary**
The export goal is well scoped, but this is the riskiest plan. It relies on major surgery inside a large dialog/export method while promising byte-for-byte Genizah parity and pure tests that the plan does not actually expose.

**Strengths**
- Correctly uses `is_local_sys_id` / `src_lbl == 'LOCAL'`, not `item['display']['source']`.
- Correctly maps LOCAL matched text from `source_ctx`.
- Correctly reuses Phase 103 helpers instead of inventing a new LOCAL schema.
- Correctly insists on batched filepath lookup.

**Concerns**
- **HIGH:** Testability mismatch: Plan 01 wants pure helper tests, but Plan 04 adds closures inside `export_comp_report`. Make helpers module-level or tests will need brittle GUI/export mocking.
- **HIGH:** "Byte-for-byte unchanged" for XLSX/DOCX may be unrealistic because those formats often contain ZIP metadata/order differences. Structural parity is safer unless existing exporters are deterministic.
- **HIGH:** Plan must be more precise about LOCAL page number extraction. `_get_meta_for_header(page['raw_header'])` may not parse LOCAL headers reliably.
- **MEDIUM:** Key-link says use `_prime_local_filepath_cache`, but Task 1 says directly call `self.indexer.get_filepaths(...)`. Pick one.
- **MEDIUM:** DOCX `write_docx_result_block` may need a richer search-result-shaped dict than `display={'source':'LOCAL','id':sid}`.
- **MEDIUM:** Category/order preservation is underspecified when mixing Genizah tables and LOCAL DOCX blocks.
- **LOW:** `Local Documents` sheet/section labels need confirmed existing i18n keys and Excel-safe sheet names.

**Suggestions**
- Extract pure helpers: `_flatten_comp_export_pages`, `_is_local_comp_item`, `_build_local_comp_row`, `_partition_comp_export_rows`.
- Pin Genizah parity structurally for XLSX/DOCX, not byte-for-byte, unless current tests already normalize generated artifacts.
- Resolve LOCAL page from page-level fields first, then header parsing as fallback.
- Reuse the exact Phase 103 CSV/TXT/XLSX conventions, including sheet/section names and translations.
- Build a full `result_dict` for DOCX matching the helper's expected search result contract.

**Risk Assessment: HIGH**
EXP-F3 is achievable, but the current plan underestimates export shape complexity and testability.

### Overall

The four plans cover the roadmap success criteria in intent, but I would not green-light them unchanged. The main fixes are: append engine params safely, validate scope fail-closed, make stale/hash status per-run instead of global mutable UI state, update the weights hash beyond startup, expose pure export helpers, and relax "byte-for-byte" export parity where file formats make that brittle.

---

## Consensus Summary

The two reviewers **diverge on overall risk** — Gemini rates the plan **LOW** ("ready for execution"), Codex rates it **HIGH/MEDIUM** ("would not green-light unchanged"). The divergence is mostly explained by depth: Codex pushed on test↔implementation surface, signature ABI, fail-open scope handling, and export-format determinism — areas Gemini treated as resolved. Codex could not verify against live source (sandbox blocked), so several of its HIGH findings should be **confirmed against the actual plan text/code** before acting; but most are cheap to honor regardless.

### Agreed Strengths
- Mirroring the shipping Search-tab Genizah/Local/ALL selector is the right pattern (familiarity + consistency).
- Lab Mode correctly preserved as an **orthogonal** algorithmic axis (fingerprint vs BM25) — not retired, not hardwired to LOCAL (RF-1/D-06).
- Correctly **avoids RRF** for composition and reuses the existing score-interleaved accumulators (RF-2).
- RF-4 weights-hash mismatch correctly identified as the live root cause of silent LOCAL-LAB drops.
- EXP-F3 correctly uses `is_local_sys_id` (NOT `item['display']['source']`) and maps `matched_text_raw ← source_ctx`; reuses Phase 103 helpers.
- Wave-0 TDD scaffold gives every later commit a sampling target.

### Agreed Concerns (highest priority — both reviewers)
1. **Weights-hash override staleness beyond startup** *(Gemini MEDIUM, Codex HIGH)* — startup-only injection goes stale after a My-Library LAB rebuild / dynamic-weight change. **Both** recommend refreshing the override **after every LAB rebuild and immediately before each Local/ALL standard-composition run**, not just at startup. (Gemini: wire an `_on_lab_rebuild_finished` signal.)
2. **Stale-signal correctness/timing** *(Gemini LOW, Codex HIGH)* — `lbl_comp_local_stale` updates only in `on_comp_scan_finished` (Gemini: no feedback until a search finishes) AND reads **current** UI state, so a scope/Lab-Mode change mid-run can label the wrong run (Codex). **Both** point toward making staleness **per-run** (capture scope/lab-mode at thread launch, or carry `local_lab_stale` in the result payload) and refreshing the label proactively on combo change.

### Codex-only HIGH concerns (divergent — investigate during `--reviews`)
- **C1 — Test↔implementation surface mismatch (110-01 ↔ 110-04):** Wave-0 export tests assume a *pure* helper, but Plan 04 implements LOCAL export as **closures inside `export_comp_report`** — not unit-testable without driving QFileDialog. → Extract module-level helpers (`_partition_comp_export_rows`, `_build_local_comp_row`, etc.) and point tests there.
- **C2 — Wave-2 tests need Wave-3 wiring:** Plan 02's full-file pytest run includes `test_no_cloud_write_on_local_comp` + routing tests that may depend on Plan 03 UI wiring → mark `xfail` until Wave 3 or scope to pure engine behavior, else Wave 2 sampling goes red.
- **C3 — `corpus_scope` parameter placement:** adding it mid-signature (after `scan_limit=50000`) in `lab_composition_search` risks breaking positional callers → **append at the end** of both engine methods.
- **C4 — Invalid scope fails OPEN → privacy:** `if corpus_scope != 'local'` / `!= 'genizah'` means a typo behaves like ALL and can surface LOCAL data → **normalize/validate, fail closed to `genizah`** (relevant to D-12).
- **C5 — "Byte-for-byte" XLSX/DOCX parity is brittle:** ZIP container metadata/ordering → assert **structural** parity unless the existing exporters are already deterministic / artifacts are normalized in tests.
- **C6 — LOCAL page-number extraction:** `_get_meta_for_header(page['raw_header'])` may not parse LOCAL headers → resolve page from page-level fields first, header parsing as fallback.

### Divergent Views
- **Overall risk:** Gemini LOW / "ready" vs Codex HIGH / "do not green-light unchanged." Recommended posture: the plan's *architecture* is sound (both agree); the actionable deltas are mostly **test-surface and state-lifecycle hardening**, not a redesign.
- **D-13 parity strength:** Codex argues the non-regression test only proves new-default == omitted-default, not parity with the historical Genizah path; Gemini accepted it as sufficient.

### Recommended next step
Run `/gsd-plan-phase 110 --reviews` to fold these in. Cheap, high-value, honor-regardless deltas: **C3** (param at end), **C4** (fail-closed scope normalizer), **C1** (module-level export helpers), **C2** (xfail Wave-2 UI/cloud tests until Wave 3), the **agreed weights-hash refresh** (#1), and **per-run staleness** (#2). Confirm **C5/C6** and the **D-13 parity** wording against the actual plan text before changing — Codex reviewed plan text only and some may already be handled.


---

## Round 2 — Codex (LIVE SOURCE, sandbox bypassed) — 2026-06-08

> Re-run of the Codex review with `--dangerously-bypass-approvals-and-sandbox` so it could open and ripgrep the live source it was blocked from in round 1. Verdict: **needs another revision** — all round-1 findings CONFIRMED-FIXED, but 6 new (1 HIGH, 4 MEDIUM, 1 LOW) source-grounded issues found.

# Codex Review - Phase 110 revised plans, round 2

Repo: `C:\Genizahsearch`
Date: 2026-06-08

## Live-source confirmations

- Current `SearchEngine.search_composition_logic` signature ends at `restrict_sys_ids: set = None`; there is no `corpus_scope` yet. Evidence: `genizah_core.py:8892-8895`.
- Current `LabEngine.lab_composition_search` signature ends at `min_delimiter_distance=3`; the trailing params after `scan_limit=50000` are `boundary_mode`, `boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`. Evidence: `genizah_core.py:1402-1405`.
- Current `CompositionThread` and `LabCompositionThread` do not accept/forward `corpus_scope`; `SearchThread` already does, and is the right pattern. Evidence: `gui_threads.py:86-112`, `gui_threads.py:171-210`, `gui_threads.py:224-272`.
- RF-4 premise is confirmed. `SearchEngine.__init__` does not initialize `dynamic_rank_map` or `settings` (`genizah_core.py:6890-6907`), and `SearchEngine._current_lab_weights_hash()` therefore hashes `dynamic_rank_map=None` / `use_dynamic_weights=False` unless an override is added (`genizah_core.py:7079-7109`). `LabEngine` owns `settings` and `dynamic_rank_map` (`genizah_core.py:698-705`), loads LAB weights (`genizah_core.py:718-723`), and hashes that state (`genizah_core.py:808-823`). The LOCAL LAB `.meta.json` is written from the build-time `lab_weights` (`shared/local_indexer.py:4447-4458`), so the standard composition path can miscompare without an override.
- `self.indexer` in `GenizahGUI` is the startup Genizah indexer (`genizah_app.py:3259-3265`). The LOCAL indexer is `self.my_library_tab._indexer`, initialized as a `LocalIndexer` (`desktop/my_library_tab.py:1471-1480`), and it exposes `get_filepaths()` (`shared/local_indexer.py:1954-1975`).
- `MyLibraryTab._parent_window` exists (`desktop/my_library_tab.py:1022-1037`) and `_on_lab_rebuild_finished()` exists (`desktop/my_library_tab.py:1207-1217`), so the A1 post-rebuild callback is wireable.
- `genizah_app.py` uses module-level `logger`, not `LOGGER`. Evidence: `genizah_app.py:107`.

## Round-1 HIGH/agreed findings

### A1 - weights-hash override refresh beyond startup

Status: **CONFIRMED-FIXED** in the revised plan, with one residual issue listed below.

The source confirms the original RF-4 bug premise: standard composition freshness uses `SearchEngine._current_lab_weights_hash()` but the real LAB weights live on `LabEngine`. Plan 02 adds the `_lab_weights_hash_override` read path (`110-02-PLAN.md:197-205`). Plan 03 adds a shared `GenizahGUI._refresh_lab_weights_hash_override()` helper (`110-03-PLAN.md:194-204`) and calls it at startup, post-LOCAL-LAB rebuild, and pre-run (`110-03-PLAN.md:205-217`, `110-03-PLAN.md:253-256`). The My Library hook is source-valid (`desktop/my_library_tab.py:1036`, `desktop/my_library_tab.py:1207-1217`).

### A2 - per-run stale payload, not live UI/engine state

Status: **CONFIRMED-FIXED** in the revised plan, with an early-return payload gap listed below.

Plan 02 requires both composition result dicts to carry `corpus_scope` and `local_lab_stale` (`110-02-PLAN.md:142-143`, `110-02-PLAN.md:175-176`, `110-02-PLAN.md:190-192`). Plan 03 updates `on_comp_scan_finished()` to read `result_obj.get('corpus_scope')` and `result_obj.get('local_lab_stale')`, not the live combo or engine flag (`110-03-PLAN.md:262-271`). This fits the current slot shape: `on_comp_scan_finished(self, result_obj)` receives the thread payload (`genizah_app.py:21892-21910`).

### C1 - export helper testability mismatch

Status: **CONFIRMED-FIXED** in the revised plan.

Live source currently has `local_documents_header_row()` and `build_local_document_row()` (`shared/export_dossier.py:418-427`, `shared/export_dossier.py:1239-1268`) but does not define `_build_local_comp_row` or `_partition_comp_export_rows`. The revision explicitly moves those to module-level helpers and points tests at them (`110-01-PLAN.md:232-236`, `110-04-PLAN.md:166-173`, `110-04-PLAN.md:213-214`), which fixes the original closure-testability problem.

### C2 - Wave-2 tests depending on Wave-3 UI wiring

Status: **CONFIRMED-FIXED** in the revised plan.

Plan 01 now requires all `tests/test_comp_corpus_scope.py` tests to be pure-engine: direct calls to `search_composition_logic` / `lab_composition_search`, no `genizah_app`, no `run_composition` (`110-01-PLAN.md:150-152`, `110-01-PLAN.md:204-210`). Plan 02's whole-file verify is therefore coherent (`110-02-PLAN.md:211-223`). The test files do not exist yet in live source, which is expected before plan execution.

### C3 - `corpus_scope` parameter placement

Status: **CONFIRMED-FIXED** in the revised plan.

Live source confirms the ABI risk: `search_composition_logic` currently ends at `restrict_sys_ids` (`genizah_core.py:8892-8895`), and `lab_composition_search` currently ends at `min_delimiter_distance=3`, not `scan_limit` (`genizah_core.py:1402-1405`). Plan 02 now appends `corpus_scope` as the final param in both methods (`110-02-PLAN.md:79-93`, `110-02-PLAN.md:155`, `110-02-PLAN.md:181`) and mirrors that in both thread constructors (`110-02-PLAN.md:240-246`).

### C4 - invalid scope fails open

Status: **CONFIRMED-FIXED** in the revised plan.

The revised Plan 02 requires the fail-closed normalizer at the top of both engine methods: `if corpus_scope not in ('genizah', 'local', 'all'): corpus_scope = 'genizah'` (`110-02-PLAN.md:149-153`), and pins it with `test_invalid_scope_fails_closed` (`110-01-PLAN.md:146`, `110-02-PLAN.md:222`). This addresses the privacy failure mode that a typo could otherwise take both `!= 'local'` and `!= 'genizah'` branches.

### C5 - byte-for-byte XLSX/DOCX parity

Status: **CONFIRMED-FIXED** in the revised plan.

Plan 01 and Plan 04 now assert structural parity, not byte-for-byte parity, for XLSX/DOCX (`110-01-PLAN.md:252-258`, `110-04-PLAN.md:254-258`). That is the right bar for this source: XLSX is written through `openpyxl` (`genizah_app.py:20939`) and DOCX through `python-docx` (`genizah_app.py:21102`), both ZIP-container formats where byte-level stability is brittle.

### C6 - LOCAL page-number extraction

Status: **CONFIRMED-FIXED**, with source narrowing.

The strongest version of my first concern was too broad: live LOCAL headers are parseable in the basic page-number case. `_make_full_header()` creates `{sys_id}_LOCAL_P{page_num}_F...` (`shared/local_indexer.py:939-941`), and `parse_header_smart()` recognizes `97...` plus `_P(\d+)_` (`genizah_core.py:3976-3987`). However, page-level fields are still the better export source because LOCAL rows store `chunk_locator` separately (`shared/local_indexer.py:2978-2985`) and existing composition export currently falls back to header parsing in multiple branches (`genizah_app.py:20603`, `genizah_app.py:20837`, `genizah_app.py:21143`). Plan 04 now prefers `page.get('p_num')` / `page.get('chunk_locator')` and only then header parsing (`110-04-PLAN.md:198-204`, `110-04-PLAN.md:247-250`).

## New issues in the revised plans

### HIGH - Plan 04 leaves LOCAL sys_ids in the metadata prefetch path

`export_comp_report()` currently collects all IDs and then calls `_fetch_metadata_with_dialog(missing)` before any format-specific write (`genizah_app.py:20472-20495`). That loader takes the supplied system IDs and starts a `ShelfmarkLoaderThread` for anything not in `nli_cache` (`genizah_app.py:23420-23437`). Plan 04 inserts LOCAL detection/cache priming before this loop (`110-04-PLAN.md:176-187`) but never says to remove LOCAL ids from `unique_ids` / `missing`.

Impact: a Local/ALL composition export can try to fetch NLI metadata for a private LOCAL `97...` id before writing the local export rows. At best this is wasted UI blocking; at worst it is an unintended metadata/network lookup for LOCAL data and violates the spirit of D-12.

Suggested fix: after `unique_ids`, compute `genizah_ids = [uid for uid in unique_ids if not is_local_sys_id(uid)]` and build `missing` from `genizah_ids` only. Add a test that a LOCAL-only composition export does not call `_fetch_metadata_with_dialog`.

### MEDIUM - Composition corpus scope is not restored on the persistent-preferences path

Plan 03 adds `comp_corpus_scope` to the `composition_search` dict and restores it in `_restore_session` (`110-03-PLAN.md:186-192`). But live `_restore_session()` applies persistent preferences before early returns (`genizah_app.py:25157-25168`) and can return for `restore_mode == 'never'`, no restorable data, or user decline before the composition restore block (`genizah_app.py:25168-25202`, composition block starts at `genizah_app.py:25283`). The existing persistent-preferences helper only restores the regular search scope (`genizah_app.py:24968-25004`).

Impact: "selected corpus scope persists across sessions" is false when full session restore is disabled or declined. Regular search already handles this correctly; composition should match it.

Suggested fix: store/read `composition_search.comp_corpus_scope` inside `_apply_persistent_session_preferences()` as well, validate against `{'genizah','local','all'}`, set `self._comp_corpus_scope`, and blockSignals-update `comp_corpus_scope_combo` if it exists.

### MEDIUM - Composition history re-runs still lose the corpus scope

Regular search history stores and restores `corpus_scope` (`genizah_app.py:24770-24780`, `genizah_app.py:24891-24900`). Composition history currently stores only `chunk_size`, `max_freq`, and `mode_index` (`genizah_app.py:24932-24936`), and restore only applies those (`genizah_app.py:24828-24833`). Plan 03 only covers session persistence, not `_add_comp_search_to_history()` / `_restore_comp_search_from_state()`.

Impact: clicking a saved Local/ALL composition history entry can re-run under the current/default Genizah scope, producing different results from the original run.

Suggested fix: add `comp_corpus_scope` to composition history `search_params`, restore it with the same `blockSignals` combo update before `run_composition()`, and add a small history restore test or source-grep validation.

### MEDIUM - A2 payload keys can be missing on early returns

Plan 02 says both returned dicts carry `corpus_scope` and `local_lab_stale` (`110-02-PLAN.md:142-143`, `110-02-PLAN.md:175-176`, `110-02-PLAN.md:190-192`). Live source has early returns before the main return dicts: Lab composition returns immediately on empty text (`genizah_core.py:1421-1422`), and standard composition returns immediately when the token count is below `chunk_size` (`genizah_core.py:8914-8915`).

Impact: `on_comp_scan_finished()` will default missing payload fields to Genizah/False, so a Local/ALL short-text run against a stale LOCAL LAB index can hide the warning and break the advertised result-dict contract.

Suggested fix: normalize `corpus_scope` and initialize `_local_lab_stale = False` before any return; include `corpus_scope` and `local_lab_stale` in all early-return dicts. Add tests for empty/too-short inputs.

### MEDIUM - proactive stale-label check can use a stale weights override

Plan 03's `_refresh_comp_stale_label_for_scope()` checks `eng._check_local_lab_freshness()` directly (`110-03-PLAN.md:171-180`). For standard mode, that check depends on the `SearchEngine._lab_weights_hash_override` added by Plan 02/03. The helper is refreshed at startup, after LOCAL LAB rebuild, and pre-run (`110-03-PLAN.md:205-217`, `110-03-PLAN.md:253-256`), but the LabPanel "Rebuild Lab Index" path can update `LabEngine.dynamic_rank_map` through `LabEngine.rebuild_lab_index()` (`genizah_app.py:1024-1068`, `genizah_core.py:852-857`) without refreshing the override until the next run.

Impact: the proactive scope-change label can report "fresh" using an old override even after LAB weights changed and before the pre-run refresh corrects the actual run payload.

Suggested fix: call `_refresh_lab_weights_hash_override()` inside `_refresh_comp_stale_label_for_scope()` before checking `SearchEngine`, or wire `LabPanel.on_rebuild_finished()` to refresh the parent override. Prefer both; the first is cheap and robust.

### LOW - Plan text still uses a misleading `LOCAL_...` namespace in test examples

Actual LOCAL sys_ids are 18-digit `97...` values (`shared/local_sys_id.py:53-79`), and LOCAL headers are built from that numeric sys_id (`shared/local_indexer.py:939-941`). Plan 04 even says to "confirm the LOCAL_ prefix" (`110-04-PLAN.md:158`), which is wrong. Plan 01's fake LOCAL item also uses `LOCAL_...` (`110-01-PLAN.md:240-241`).

Impact: low, because the sample lambda also checks `src_lbl == 'LOCAL'`, but tests involving `is_local_sys_id()` should use realistic `97...` ids or they will not exercise the actual discriminator.

Suggested fix: replace `LOCAL_...` fixtures with an 18-digit `97` id such as `970012345601234567`.

## Overall verdict

**Needs another revision before execution.**

The revised plans mostly fix the original C1-C6/A1/A2 findings and are much closer to executable. I would not execute unchanged because Plan 04 can still route LOCAL ids through the metadata-fetch path, and Plan 03 leaves two real re-run/persistence paths unsafeguarded. The fixes are small and mechanical: filter LOCAL ids out of metadata prefetch, persist/restore composition scope through persistent prefs and history, add A2 keys to early returns, refresh the override before proactive stale checks, and clean up LOCAL test fixtures.

