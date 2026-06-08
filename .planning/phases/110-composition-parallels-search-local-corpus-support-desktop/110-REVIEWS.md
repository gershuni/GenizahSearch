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
